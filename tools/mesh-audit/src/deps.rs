//! Dependency-rule gate (i143.e0.s4, PRD §4 "Dependency rules").
//!
//! Reads `cargo metadata` for a workspace and checks:
//!
//! 1. no RDM generic package depends (transitively, any kind) on a rafka-v2
//!    domain crate;
//! 2. `rafka-node-rpc-contract` has no Iroh dependency (normal/build closure);
//! 3. `rafka-mesh-entity` has no Application EF dependency (normal/build closure);
//! 4. the scenario and chaos crates reach the rest of RDM only through the
//!    public control/probe interfaces (direct normal/build deps on workspace
//!    members are limited to an allow-list).
//!
//! A rule over a package that does not exist yet is vacuous; it starts biting
//! the commit that adds the package.

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::Path;
use std::process::Command;

/// PRD §4 target packages plus the generic crates already in the workspace.
pub const GENERIC_PACKAGES: &[&str] = &[
    "rafka-node-rpc-contract",
    "rafka-mesh-entity",
    "rafka-mesh-transport",
    "rafka-node-rpc",
    "rafka-node-admin-core",
    "rafka-node-admin-client",
    "rafka-test-scenario",
    "rafka-node-rpc-testkit",
    "rafka-chaos",
    "rafka-telemetry",
];

pub const CONTRACT: &str = "rafka-node-rpc-contract";
pub const MESH_ENTITY: &str = "rafka-mesh-entity";

/// Crates that may only reach RDM through public control/probe interfaces.
pub const PUBLIC_INTERFACE_CONSUMERS: &[&str] = &["rafka-test-scenario", "rafka-chaos"];

/// The public control/probe interfaces those crates may depend on.
pub const PUBLIC_INTERFACES: &[&str] = &[
    "rafka-node-admin-client",
    "rafka-node-rpc-contract",
    "rafka-node-rpc-testkit",
    "rafka-test-scenario",
    "rafka-telemetry",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Rule 1: `package` reaches rafka-v2 domain crate `domain` via `path`.
    DomainCrate { package: String, domain: String, path: Vec<String> },
    /// Rule 2: the contract reaches an Iroh crate.
    ContractIroh { iroh: String, path: Vec<String> },
    /// Rule 3: the Mesh EF reaches an Application EF crate.
    MeshEntityApplicationEf { crate_name: String, path: Vec<String> },
    /// Rule 4: a scenario/chaos crate depends directly on a non-public RDM package.
    PrivateInterface { package: String, dependency: String },
    /// `cargo metadata` could not run.
    Metadata(String),
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DomainCrate { package, domain, path } => write!(
                f,
                "rule 1: generic package `{package}` depends on rafka-v2 domain crate `{domain}` ({})",
                path.join(" -> ")
            ),
            Self::ContractIroh { iroh, path } => {
                write!(f, "rule 2: `{CONTRACT}` depends on Iroh crate `{iroh}` ({})", path.join(" -> "))
            }
            Self::MeshEntityApplicationEf { crate_name, path } => write!(
                f,
                "rule 3: `{MESH_ENTITY}` depends on Application EF crate `{crate_name}` ({})",
                path.join(" -> ")
            ),
            Self::PrivateInterface { package, dependency } => write!(
                f,
                "rule 4: `{package}` depends on `{dependency}`, which is not a public control/probe interface ({})",
                PUBLIC_INTERFACES.join(", ")
            ),
            Self::Metadata(e) => write!(f, "cargo metadata failed: {e}"),
        }
    }
}

/// The dependency graph the rules need, built from `cargo metadata` JSON.
pub struct Graph {
    names: BTreeMap<String, String>,
    sources: BTreeMap<String, Option<String>>,
    members: BTreeSet<String>,
    /// id -> [(dep id, kinds)] where kinds ⊆ {"normal","build","dev"}.
    edges: BTreeMap<String, Vec<(String, BTreeSet<String>)>>,
}

impl Graph {
    pub fn from_metadata(v: &Value) -> Result<Self, String> {
        let mut names = BTreeMap::new();
        let mut sources = BTreeMap::new();
        for p in v["packages"].as_array().ok_or("metadata has no packages")? {
            let id = p["id"].as_str().unwrap_or_default().to_string();
            names.insert(id.clone(), p["name"].as_str().unwrap_or_default().to_string());
            sources.insert(id, p["source"].as_str().map(str::to_string));
        }
        let members = v["workspace_members"]
            .as_array()
            .ok_or("metadata has no workspace_members")?
            .iter()
            .filter_map(|m| m.as_str().map(str::to_string))
            .collect();
        let mut edges = BTreeMap::new();
        for n in v["resolve"]["nodes"].as_array().ok_or("metadata has no resolve graph")? {
            let id = n["id"].as_str().unwrap_or_default().to_string();
            let deps = n["deps"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|d| {
                    let kinds = d["dep_kinds"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| k["kind"].as_str().unwrap_or("normal").to_string())
                        .collect();
                    (d["pkg"].as_str().unwrap_or_default().to_string(), kinds)
                })
                .collect();
            edges.insert(id, deps);
        }
        Ok(Self { names, sources, members, edges })
    }

    pub fn load(root: &Path) -> Result<Self, String> {
        let out = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["metadata", "--format-version", "1", "--manifest-path"])
            .arg(root.join("Cargo.toml"))
            .output()
            .map_err(|e| e.to_string())?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
        }
        let v: Value = serde_json::from_slice(&out.stdout).map_err(|e| e.to_string())?;
        Self::from_metadata(&v)
    }

    fn name<'a>(&'a self, id: &'a str) -> &'a str {
        self.names.get(id).map(String::as_str).unwrap_or(id)
    }

    fn member_id(&self, name: &str) -> Option<&String> {
        self.members.iter().find(|id| self.name(id) == name)
    }

    /// A non-member `rafka-*` crate, or any crate sourced from the rafka-v2 repo.
    fn is_rafka_v2_domain(&self, id: &str) -> bool {
        if self.members.contains(id) {
            return false;
        }
        let from_rafka_v2 = self.sources.get(id).cloned().flatten().is_some_and(|s| s.contains("rafka-v2"));
        from_rafka_v2 || self.name(id).starts_with("rafka-")
    }

    /// Breadth-first closure from `start` over edges whose kinds intersect
    /// `kinds`; yields (dep id, path of names from start).
    fn closure(&self, start: &str, kinds: &[&str]) -> Vec<(String, Vec<String>)> {
        let mut seen = BTreeSet::from([start.to_string()]);
        let mut q = VecDeque::from([(start.to_string(), vec![self.name(start).to_string()])]);
        let mut out = Vec::new();
        while let Some((id, path)) = q.pop_front() {
            for (dep, dk) in self.edges.get(&id).into_iter().flatten() {
                if !dk.iter().any(|k| kinds.contains(&k.as_str())) || !seen.insert(dep.clone()) {
                    continue;
                }
                let mut p = path.clone();
                p.push(self.name(dep).to_string());
                out.push((dep.clone(), p.clone()));
                q.push_back((dep.clone(), p));
            }
        }
        out
    }
}

const ALL_KINDS: &[&str] = &["normal", "build", "dev"];
const SHIPPED_KINDS: &[&str] = &["normal", "build"];

pub fn check_graph(g: &Graph) -> Vec<Violation> {
    let mut out = Vec::new();
    // Rule 1.
    for &pkg in GENERIC_PACKAGES {
        let Some(id) = g.member_id(pkg) else { continue };
        for (dep, path) in g.closure(id, ALL_KINDS) {
            if g.is_rafka_v2_domain(&dep) {
                out.push(Violation::DomainCrate { package: pkg.into(), domain: g.name(&dep).into(), path });
            }
        }
    }
    // Rule 2.
    if let Some(id) = g.member_id(CONTRACT) {
        for (dep, path) in g.closure(id, SHIPPED_KINDS) {
            let n = g.name(&dep);
            if n == "iroh" || n.starts_with("iroh-") {
                out.push(Violation::ContractIroh { iroh: n.into(), path });
            }
        }
    }
    // Rule 3.
    if let Some(id) = g.member_id(MESH_ENTITY) {
        for (dep, path) in g.closure(id, SHIPPED_KINDS) {
            let n = g.name(&dep);
            if n == "rafka-entity-framework" || n.starts_with("rafka-entity-") {
                out.push(Violation::MeshEntityApplicationEf { crate_name: n.into(), path });
            }
        }
    }
    // Rule 4.
    for &pkg in PUBLIC_INTERFACE_CONSUMERS {
        let Some(id) = g.member_id(pkg) else { continue };
        for (dep, kinds) in g.edges.get(id).into_iter().flatten() {
            let shipped = kinds.iter().any(|k| SHIPPED_KINDS.contains(&k.as_str()));
            let n = g.name(dep);
            if shipped && g.members.contains(dep) && !PUBLIC_INTERFACES.contains(&n) {
                out.push(Violation::PrivateInterface { package: pkg.into(), dependency: n.into() });
            }
        }
    }
    out
}

/// Run every rule against the workspace at `root`.
pub fn check(root: &Path) -> Vec<Violation> {
    match Graph::load(root) {
        Ok(g) => check_graph(&g),
        Err(e) => vec![Violation::Metadata(e)],
    }
}
