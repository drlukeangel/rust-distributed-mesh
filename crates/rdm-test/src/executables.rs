//! The harness's executables and its complete-topology requests (proposal
//! executable-bindings-at-create.md rule 1 and §2, Q157): every Build the harness submits names each
//! path's executable, `build.create { Change(Reconcile { based_on, nodes }) }`. A cell's counts are
//! expanded here into named paths with their configs, reading the current topology, as Rafka's door
//! expands its per-type input into per-path entries before the typed call: RDM is never handed a count
//! by the harness, and nothing it launches is taken from a directory with no sha.
//!
//! [`Executables`] is where each type's executable comes from (the testkit's own binaries, each file
//! hashed once per process, or an explicit set); [`Current`] is the read a request is built from (the
//! current Build at the fabric-primary, its accepted topology and every node's row) and expands counts
//! the way the compiler's count form resized them (`accepted::resize`): a grow takes the lowest free
//! ordinal of the type, a shrink drops the highest ordinal that is not the type's primary.

use rdm_client::binding::{sha256_file, Binding, BindingSet};
use rdm_client::{BuildCarrier, BuildId, Builds, Filter, MeshDesired, NodeStatus, NodeView, PathName};
use rdm_core::meta::{NodeSpec, StoragePersistence};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

/// Where each type's executable comes from.
#[derive(Debug, Clone)]
pub enum Executables {
    /// The testkit's own binaries in [`crate::estate::bin_dir`]: `rdm-node-admin`, `testkit-node`, and
    /// `rdm-node` for every other kind (it takes its type from the launch). `container` binds each
    /// to the container provider's runtime image.
    Testkit {
        /// The fabric's provider is the container provider.
        container: bool,
    },
    /// An explicit set: a consumer's executables, the faulted node-admin's.
    Set(BindingSet),
}

impl Executables {
    /// The testkit's binaries under a provider named `provider` (`process` or `container`).
    pub fn testkit(provider: &str) -> Self {
        Self::Testkit { container: provider == "container" }
    }

    /// The binding a created node of `kind` runs. A set that binds no `kind` is a defect of the cell,
    /// named: nothing falls back to another executable.
    pub fn of(&self, r#type: &str) -> Binding {
        self.binding(r#type).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Self::of`], an unbound `kind` named in the error.
    pub fn binding(&self, r#type: &str) -> Result<Binding, String> {
        match self {
            Self::Testkit { container } => {
                let name = match r#type {
                    "node-admin" => "rdm-node-admin",
                    "node" => "testkit-node",
                    _ => "rdm-node",
                };
                let executable = crate::estate::binary(name);
                let sha256 = sha256_once(&executable)?;
                Ok(Binding { launch_id: r#type.into(), executable, sha256, image: container.then(|| crate::estate::RUNTIME_IMAGE.to_string()) })
            }
            Self::Set(set) => set.of(r#type).cloned().ok_or_else(|| format!("the executable set binds no `{}`: a {} path cannot be named (launch ids {:?})", r#type, r#type, set.launch_ids)),
        }
    }
}

/// The sha256 of the file at `path`, hashed once per process: the testkit binaries are hundreds of
/// megabytes, and every request names them.
fn sha256_once(path: &Path) -> Result<String, String> {
    static HASHED: OnceLock<Mutex<BTreeMap<PathBuf, Arc<OnceLock<Result<String, String>>>>>> = OnceLock::new();
    let cell = HASHED.get_or_init(Default::default).lock().unwrap().entry(path.to_path_buf()).or_default().clone();
    cell.get_or_init(|| sha256_file(path).map_err(|e| format!("{} could not be hashed: {e}", path.display()))).clone()
}

/// The create spec of `path`, a path the request creates: its storage its type's, written out (the
/// testkit application's declared type's, [`crate::test_app::types`], else RDM's
/// [`StoragePersistence::of_type`]), no tags, no config, and `executable`.
pub fn created_spec(path: PathName, mesh: &str, r#type: &str, executable: Binding) -> NodeSpec {
    let storage_persistence = crate::test_app::storage_of(r#type);
    NodeSpec { path: path.path.clone(), name: path.name.clone(), mesh: mesh.into(), r#type: r#type.to_string(), storage_persistence, executable: Some(executable), tags: Default::default(), config: Default::default() }
}

/// The counts of `mesh` by type, `node-admin` first: every type it names, and at 0 every type `held`
/// holds that it does not name.
fn counts(mesh: &MeshDesired, held: &BTreeMap<PathName, NodeSpec>) -> Vec<(String, u32)> {
    let mut types: Vec<String> = mesh.counts.keys().cloned().chain(held.values().map(|s| s.r#type.clone())).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    types.sort_by_key(|t| !rdm_core::is_node_admin(t));
    types.into_iter().map(|t| { let n = mesh.count(&t); (t, n) }).collect()
}

/// What a complete-topology request is built from: the current Build at the fabric-primary with its
/// accepted topology, and every node's row as `fabric.nodes.get` answers it.
#[derive(Debug, Clone)]
pub struct Current {
    /// The current Build: the request's `based_on`.
    pub based_on: BuildId,
    /// That Build's accepted topology: every node's create spec, by mesh and `path.name`.
    pub topology: BTreeMap<String, BTreeMap<PathName, NodeSpec>>,
    /// Every node of the fabric, from the rows.
    pub rows: Vec<NodeView>,
}

impl Current {
    /// Read it through `carrier`: `build.get` of the current Build (its topology, every node's create
    /// spec), and `node.get` with no pair.
    pub async fn read(carrier: &BuildCarrier) -> Result<Self, String> {
        let current = Builds::new(carrier.clone()).get(&Filter::new().eq("current", ["true"])).await.map_err(|e| format!("build.get of the current Build at {:?}: {e}", carrier.target()))?;
        let [build] = <[rdm_client::Build; 1]>::try_from(current).map_err(|all| format!("build.get of the current Build at {:?} answered {} Builds, not one", carrier.target(), all.len()))?;
        let mut topology: BTreeMap<String, BTreeMap<PathName, NodeSpec>> = BTreeMap::new();
        for spec in build.nodes {
            topology.entry(spec.mesh.clone()).or_default().insert(spec.location(), spec);
        }
        // Every row in one read: node.get streams its rows on op 0x23 (Q285).
        let rows = rdm_client::Nodes::new(carrier.clone()).get(&rdm_client::Filter::new()).await.map_err(|e| format!("node.get at {:?}: {e}", carrier.target()))?;
        Ok(Self { based_on: build.build_id, topology, rows })
    }

    /// The executable the row at `path` records, a live birth's first.
    fn row_executable(&self, path: &PathName) -> Option<Binding> {
        let at = || self.rows.iter().filter(|n| &n.location() == path);
        at().find(|n| !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)).or_else(|| at().next()).and_then(|n| n.executable.clone())
    }

    /// The live birth's row at `path`, if one stands.
    fn live_row(&self, path: &PathName) -> Option<&NodeView> {
        self.rows.iter().find(|n| &n.location() == path && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead))
    }

    /// The ordinal of `kind`'s primary in `mesh`, as the rows say.
    fn primary_path(&self, mesh: &str, r#type: &str) -> Option<PathName> {
        self.rows.iter().find(|n| n.mesh == mesh && n.r#type == r#type && n.is_primary && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Dead)).map(|n| n.location())
    }

    /// The executable a path the request creates runs: `executables`' for its type.
    fn created_executable(&self, r#type: &str, executables: &Executables) -> Result<Binding, String> {
        executables.binding(r#type)
    }

    /// Every path the topology holds, with its create spec: its entry's, the executable its entry names,
    /// else the one its row records, else `executables`' for its type (a path no birth was claimed at); the
    /// tags and the config a live birth's row carries, so the request names what the node holds and the
    /// Build, which applies every property that differs from the row (build.md §1), changes neither.
    pub fn held(&self, executables: &Executables) -> Result<BTreeMap<String, BTreeMap<PathName, NodeSpec>>, String> {
        let mut out: BTreeMap<String, BTreeMap<PathName, NodeSpec>> = BTreeMap::new();
        for (name, mesh) in &self.topology {
            let paths = out.entry(name.clone()).or_default();
            for (path, spec) in mesh {
                let mut spec = spec.clone();
                if spec.executable.is_none() {
                    spec.executable = Some(match self.row_executable(path) {
                        Some(b) => b,
                        None => self.created_executable(&spec.r#type, executables)?,
                    });
                }
                if let Some(row) = self.live_row(path) {
                    (spec.tags, spec.config) = (row.tags.clone(), row.config.clone());
                }
                paths.insert(path.clone(), spec);
            }
        }
        Ok(out)
    }

    /// The complete request that resizes each of `meshes` to its counts, as the compiler's count form
    /// resized it: a grow takes the lowest free ordinal of the kind, a shrink drops the highest ordinal
    /// that is not the type's primary (else the highest). Every path the topology holds keeps its spec
    /// ([`Self::held`]); every created path is named on its executable. A mesh the topology holds that
    /// `meshes` leaves out is left out of the request (its mesh.delete) when `whole`, the fabric's
    /// complete shape, and kept as it is otherwise.
    pub fn resized(&self, meshes: &[MeshDesired], whole: bool, executables: &Executables) -> Vec<NodeSpec> {
        self.try_resized(meshes, whole, executables).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Self::resized`], a path that cannot be named (its type unbound) named in the error.
    pub fn try_resized(&self, meshes: &[MeshDesired], whole: bool, executables: &Executables) -> Result<Vec<NodeSpec>, String> {
        let mut by_mesh = self.held(executables)?;
        if whole {
            by_mesh.retain(|name, _| meshes.iter().any(|m| &m.name == name));
        }
        for desired in meshes {
            let paths = by_mesh.entry(desired.name.clone()).or_default();
            for (r#type, want) in counts(desired, paths) {
                let mut have: Vec<PathName> = paths.values().filter(|spec| spec.r#type == r#type).map(|spec| spec.location()).collect();
                have.sort();
                let keep = self.primary_path(&desired.name, &r#type);
                while (have.len() as u32) < want {
                    let path = (1..).map(|ordinal| PathName::numbered(&desired.name, &r#type, ordinal)).find(|p| !paths.contains_key(p)).expect("a free generated pair");
                    have.push(path.clone());
                    have.sort();
                    paths.insert(path.clone(), created_spec(path, &desired.name, &r#type, self.created_executable(&r#type, executables)?));
                }
                while (have.len() as u32) > want {
                    let drop = have.iter().rev().find(|p| Some(*p) != keep.as_ref()).or_else(|| have.last()).expect("a member to drop").clone();
                    have.retain(|p| p != &drop);
                    paths.remove(&drop);
                }
            }
        }
        Ok(by_mesh.into_values().flat_map(BTreeMap::into_values).collect())
    }

    /// The counts the topology holds of `mesh` (zero for a mesh it does not hold).
    pub fn counts_of(&self, mesh: &str) -> MeshDesired {
        let mut counts = BTreeMap::new();
        for spec in self.topology.get(mesh).into_iter().flat_map(|m| m.values()) {
            *counts.entry(spec.r#type.clone()).or_insert(0) += 1;
        }
        MeshDesired { name: mesh.into(), counts }
    }

    /// The complete request that adds one node of `kind` to `mesh` (`node.create`), at the lowest free
    /// ordinal, as the count form's `AddNode` did.
    pub fn with_one_more(&self, mesh: &str, r#type: &str, executables: &Executables) -> Vec<NodeSpec> {
        self.try_with_one_more(mesh, r#type, executables).unwrap_or_else(|e| panic!("{e}"))
    }

    /// The nodes a Create adds (build.md §1): each of `specs` at a path the topology does not hold, by
    /// its Nodes schema.
    pub fn added(&self, specs: Vec<NodeSpec>) -> Vec<rdm_core::build::Nodes> {
        let held = |p: &PathName| self.topology.values().any(|m| m.contains_key(p));
        specs
            .into_iter()
            .filter(|s| !held(&s.path))
            .map(|s| rdm_core::build::Nodes { path: s.path.path, name: s.path.name, mesh: s.mesh, r#type: s.r#type, storage_persistence: Some(s.storage_persistence), executable: s.executable, tags: s.tags, config: s.config })
            .collect()
    }

    /// The one node a Create adds for one more `type` in `mesh`: the path [`Self::with_one_more`] adds,
    /// at the lowest free ordinal of its type, on `executables`' binding for it.
    pub fn one_more(&self, mesh: &str, r#type: &str, executables: &Executables) -> Result<rdm_core::build::Nodes, String> {
        let specs = self.try_with_one_more(mesh, r#type, executables)?;
        self.added(specs).into_iter().next().ok_or_else(|| format!("one more {type} in {mesh} names no path the topology does not hold", type = r#type))
    }

    /// [`Self::with_one_more`], a path that cannot be named named in the error.
    pub fn try_with_one_more(&self, mesh: &str, r#type: &str, executables: &Executables) -> Result<Vec<NodeSpec>, String> {
        let mut grown = self.counts_of(mesh);
        *grown.counts.entry(r#type.to_string()).or_insert(0) += 1;
        self.try_resized(&[grown], false, executables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(id: &str, sha: &str) -> Binding {
        Binding { launch_id: id.into(), executable: format!("/b/{id}").into(), sha256: sha.into(), image: None }
    }

    fn set() -> Executables {
        let bindings = vec![binding("node-admin", "a"), binding("node", "r")];
        Executables::Set(BindingSet { candidate: rdm_client::binding::Candidate { sha: "c".into(), build: String::new() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings })
    }

    fn row(name: PathName, mesh: &str, r#type: &str, primary: bool, sha: &str) -> NodeView {
        let mut v: NodeView = serde_json::from_value(serde_json::json!({
            "path": name.path, "name": name.name, "type": r#type, "mesh": mesh, "node_id": rdm_core::NodeId::mint(), "status": "ready",
            "is_primary": primary, "is_fabric_primary": false, "tags": [], "config": {}
        }))
        .unwrap();
        v.executable = Some(binding(r#type, sha));
        v
    }

    /// A topology holding `mesh1.node-admin.1` (entry naming none) and `mesh1.node.1..=3` (entries naming `r0`).
    fn current() -> Current {
        let mut mesh: BTreeMap<PathName, NodeSpec> = BTreeMap::new();
        let admin: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut admin_spec = created_spec(admin.clone(), "mesh1", "node-admin", binding("node-admin", "x"));
        admin_spec.executable = None;
        mesh.insert(admin, admin_spec);
        for o in 1..=3 {
            let p = PathName::numbered("mesh1", "node", o);
            mesh.insert(p.clone(), created_spec(p, "mesh1", "node", binding("node", "r0")));
        }
        let topology = [("mesh1".to_string(), mesh)].into_iter().collect();
        Current { based_on: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), topology, rows: vec![row(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, "a0"), row(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", true, "r0")] }
    }

    fn names(request: &[NodeSpec]) -> Vec<PathName> {
        request.iter().map(|s| s.location()).collect()
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1, "the door expands a count into named
    /// paths with configs, reading the current topology for the ordinals"; `accepted::resize`): a grow
    /// takes the lowest free ordinals and names each created path's executable; a held path keeps its
    /// entry's executable, else the one its row records; a shrink drops the highest ordinal that is not
    /// the type's primary; a whole-fabric shape leaves out a mesh it does not list, a mesh's own shape
    /// keeps the others.
    #[test]
    fn counts_expand_into_named_paths_the_way_the_compiler_resized_them() {
        let c = current();
        let grown = c.resized(&[MeshDesired::of("mesh1", [("node-admin", 2), ("node", 3)]), MeshDesired::of("mesh2", [("node-admin", 1), ("node", 1)])], true, &set());
        assert_eq!(names(&grown), [PathName::new("mesh1.node", "1").unwrap(), PathName::new("mesh1.node", "2").unwrap(), PathName::new("mesh1.node", "3").unwrap(), PathName::new("mesh1.node-admin", "1").unwrap(), PathName::new("mesh1.node-admin", "2").unwrap(), PathName::new("mesh2.node", "1").unwrap(), PathName::new("mesh2.node-admin", "1").unwrap()], "the request lists the paths in order");
        let sha = |p: PathName| grown.iter().find(|s| s.location() == p).and_then(|s| s.executable.clone()).map(|b| b.sha256).unwrap();
        assert_eq!((sha(PathName::new("mesh1.node-admin", "1").unwrap()), sha(PathName::new("mesh1.node-admin", "2").unwrap()), sha(PathName::new("mesh1.node", "2").unwrap()), sha(PathName::new("mesh2.node", "1").unwrap())), ("a0".into(), "a".into(), "r0".into(), "r".into()));
        let shrunk = c.resized(&[MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])], true, &set());
        assert_eq!(names(&shrunk), [PathName::new("mesh1.node", "3").unwrap(), PathName::new("mesh1.node-admin", "1").unwrap()], "the primary mesh1.node.3 is kept");
        let one_more = c.with_one_more("mesh1", "node", &set());
        assert_eq!(names(&one_more), [PathName::new("mesh1.node", "1").unwrap(), PathName::new("mesh1.node", "2").unwrap(), PathName::new("mesh1.node", "3").unwrap(), PathName::new("mesh1.node", "4").unwrap(), PathName::new("mesh1.node-admin", "1").unwrap()]);
        let other = c.resized(&[MeshDesired::of("mesh2", [("node-admin", 1)])], false, &set());
        assert_eq!(names(&other), [PathName::new("mesh1.node", "1").unwrap(), PathName::new("mesh1.node", "2").unwrap(), PathName::new("mesh1.node", "3").unwrap(), PathName::new("mesh1.node-admin", "1").unwrap(), PathName::new("mesh2.node-admin", "1").unwrap()]);
    }
}
