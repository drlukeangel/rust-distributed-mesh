//! The fabric topology projection and its invariants (PRD §1.14, §1.16).
//!
//! A converged topology has exactly one `is_primary` per declared cohort
//! (one `(mesh, kind)` pair) and exactly one `is_fabric_primary` across the
//! fabric, held by a node-admin that is also its own mesh's admin primary.
//! Mesh and Fabric views publish the live owning node-admin's control API
//! base; callers switch control only from these views.

use crate::model::{Fabric, Mesh, Node, NodeKind, PathName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    pub fabric: Fabric,
    pub meshes: Vec<Mesh>,
    pub nodes: Vec<Node>,
}

/// A topology that is not (yet) converged, named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyViolation {
    DuplicateNode(PathName),
    DuplicateMesh(String),
    NodeInUnknownMesh { node: PathName, mesh: String },
    KindMismatch(PathName),
    NoCohortPrimary { mesh: String, kind: NodeKind },
    SplitCohortPrimary { mesh: String, kind: NodeKind, primaries: Vec<PathName> },
    PrimaryNotLive(PathName),
    FabricPrimaryCount(usize),
    FabricPrimaryNotAdmin(PathName),
    FabricPrimaryNotMeshPrimary(PathName),
    AdminPrimaryWithoutControlEndpoint(PathName),
}

impl fmt::Display for TopologyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode(p) => write!(f, "node {p} appears twice"),
            Self::DuplicateMesh(m) => write!(f, "mesh {m} appears twice"),
            Self::NodeInUnknownMesh { node, mesh } => write!(f, "node {node} names mesh {mesh}, which is not in the fabric"),
            Self::KindMismatch(p) => write!(f, "node {p}: kind or mesh field disagrees with its path.name"),
            Self::NoCohortPrimary { mesh, kind } => write!(f, "cohort ({mesh}, {kind:?}) has no primary"),
            Self::SplitCohortPrimary { mesh, kind, primaries } => {
                write!(f, "cohort ({mesh}, {kind:?}) has {} primaries: {primaries:?}", primaries.len())
            }
            Self::PrimaryNotLive(p) => write!(f, "primary {p} is not live"),
            Self::FabricPrimaryCount(n) => write!(f, "{n} fabric primaries, expected exactly one"),
            Self::FabricPrimaryNotAdmin(p) => write!(f, "fabric primary {p} is not a node-admin"),
            Self::FabricPrimaryNotMeshPrimary(p) => {
                write!(f, "fabric primary {p} is not the node-admin primary of its own mesh")
            }
            Self::AdminPrimaryWithoutControlEndpoint(p) => {
                write!(f, "node-admin primary {p} advertises no control endpoint")
            }
        }
    }
}

/// `MeshView` (`docs/i143/design.md` §4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshView {
    pub id: Option<crate::model::MeshId>,
    pub name: String,
    pub status: crate::model::ScopeStatus,
    pub primary_admin: Option<PathName>,
    pub admin_api_base: Option<String>,
    pub nodes: Vec<PathName>,
}

/// `FabricView` (`docs/i143/design.md` §4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricView {
    pub id: crate::model::FabricId,
    pub name: String,
    pub status: crate::model::ScopeStatus,
    pub provider: crate::model::ProviderKind,
    pub fabric_primary: Option<PathName>,
    pub admin_api_base: Option<String>,
    pub meshes: Vec<MeshView>,
}

impl Topology {
    /// Every node of one cohort.
    pub fn cohort(&self, mesh: &str, kind: NodeKind) -> impl Iterator<Item = &Node> {
        let mesh = mesh.to_string();
        self.nodes.iter().filter(move |n| n.mesh == mesh && n.kind == kind)
    }

    /// The single primary of a cohort, when exactly one live node claims it.
    pub fn cohort_primary(&self, mesh: &str, kind: NodeKind) -> Option<&Node> {
        let mut p = self.cohort(mesh, kind).filter(|n| n.is_primary && n.status.is_live());
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    pub fn fabric_primary(&self) -> Option<&Node> {
        let mut p = self.nodes.iter().filter(|n| n.is_fabric_primary && n.status.is_live());
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The live owning node-admin control endpoint of `mesh` (its admin primary's).
    pub fn mesh_control_endpoint(&self, mesh: &str) -> Option<&str> {
        self.cohort_primary(mesh, NodeKind::NodeAdmin)?.admin_api_base.as_deref()
    }

    /// The fabric-primary's control endpoint.
    pub fn fabric_control_endpoint(&self) -> Option<&str> {
        self.fabric_primary()?.admin_api_base.as_deref()
    }

    pub fn node(&self, name: &PathName) -> Option<&Node> {
        self.nodes.iter().find(|n| &n.name == name)
    }

    /// Every invariant a converged topology satisfies, as named violations.
    pub fn violations(&self) -> Vec<TopologyViolation> {
        use TopologyViolation as V;
        let mut out = Vec::new();
        let mut seen_mesh = BTreeMap::new();
        for m in &self.meshes {
            if seen_mesh.insert(m.name.clone(), ()).is_some() {
                out.push(V::DuplicateMesh(m.name.clone()));
            }
        }
        let mut seen = BTreeMap::new();
        for n in &self.nodes {
            if seen.insert(n.name.clone(), ()).is_some() {
                out.push(V::DuplicateNode(n.name.clone()));
            }
            if n.kind != n.name.kind || n.mesh != n.name.mesh {
                out.push(V::KindMismatch(n.name.clone()));
            }
            if !seen_mesh.contains_key(&n.mesh) {
                out.push(V::NodeInUnknownMesh { node: n.name.clone(), mesh: n.mesh.clone() });
            }
            if (n.is_primary || n.is_fabric_primary) && !n.status.is_live() {
                out.push(V::PrimaryNotLive(n.name.clone()));
            }
        }
        // One primary per declared cohort that has a live member.
        let mut cohorts: BTreeMap<(String, NodeKind), Vec<&Node>> = BTreeMap::new();
        for n in self.nodes.iter().filter(|n| n.status.is_live()) {
            cohorts.entry((n.mesh.clone(), n.kind)).or_default().push(n);
        }
        for ((mesh, kind), members) in &cohorts {
            let primaries: Vec<PathName> = members.iter().filter(|n| n.is_primary).map(|n| n.name.clone()).collect();
            match primaries.len() {
                0 => out.push(V::NoCohortPrimary { mesh: mesh.clone(), kind: *kind }),
                1 => {
                    let p = members.iter().find(|n| n.is_primary).unwrap();
                    if *kind == NodeKind::NodeAdmin && p.admin_api_base.is_none() {
                        out.push(V::AdminPrimaryWithoutControlEndpoint(p.name.clone()));
                    }
                }
                _ => out.push(V::SplitCohortPrimary { mesh: mesh.clone(), kind: *kind, primaries }),
            }
        }
        // Exactly one fabric primary, a node-admin that is its mesh's admin primary.
        let fps: Vec<&Node> = self.nodes.iter().filter(|n| n.is_fabric_primary).collect();
        if fps.len() != 1 {
            out.push(V::FabricPrimaryCount(fps.len()));
        }
        for fp in fps {
            if fp.kind != NodeKind::NodeAdmin {
                out.push(V::FabricPrimaryNotAdmin(fp.name.clone()));
            } else if !fp.is_primary {
                out.push(V::FabricPrimaryNotMeshPrimary(fp.name.clone()));
            }
        }
        out
    }

    pub fn is_converged(&self) -> bool {
        self.violations().is_empty()
    }

    pub fn mesh_view(&self, id_or_name: &str) -> Option<MeshView> {
        let m = self.meshes.iter().find(|m| m.name == id_or_name || m.id.as_ref().is_some_and(|i| i.as_str() == id_or_name))?;
        let mut nodes: Vec<PathName> = self.nodes.iter().filter(|n| n.mesh == m.name).map(|n| n.name.clone()).collect();
        nodes.sort();
        Some(MeshView {
            id: m.id.clone(),
            name: m.name.clone(),
            status: m.status,
            primary_admin: self.cohort_primary(&m.name, NodeKind::NodeAdmin).map(|n| n.name.clone()),
            admin_api_base: self.mesh_control_endpoint(&m.name).map(str::to_string),
            nodes,
        })
    }

    pub fn fabric_view(&self) -> FabricView {
        FabricView {
            id: self.fabric.id.clone(),
            name: self.fabric.name.clone(),
            status: self.fabric.status,
            provider: self.fabric.provider,
            fabric_primary: self.fabric_primary().map(|n| n.name.clone()),
            admin_api_base: self.fabric_control_endpoint().map(str::to_string),
            meshes: self.meshes.iter().filter_map(|m| self.mesh_view(&m.name)).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn node(name: &str, status: NodeStatus, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = status;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        if n.kind == NodeKind::NodeAdmin {
            n.admin_api_base = Some(format!("http://127.0.0.1:{}", 18000 + n.name.ordinal));
        }
        n
    }

    fn mn() -> Topology {
        use NodeStatus::ReadyForTraffic as R;
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
            nodes: vec![
                node("mesh1.admin.1", R, true, true),
                node("mesh1.admin.2", R, false, false),
                node("mesh1.rpc.1", R, true, false),
                node("mesh1.rpc.2", R, false, false),
                node("mesh1.rpc.3", R, false, false),
            ],
        }
    }

    #[test]
    fn settled_mn_is_converged_with_one_primary_per_cohort() {
        let t = mn();
        assert_eq!(t.violations(), vec![]);
        assert_eq!(t.cohort_primary("mesh1", NodeKind::NodeAdmin).unwrap().name.to_string(), "mesh1.admin.1");
        assert_eq!(t.cohort_primary("mesh1", NodeKind::RpcNode).unwrap().name.to_string(), "mesh1.rpc.1");
        assert_eq!(t.fabric_primary().unwrap().name.to_string(), "mesh1.admin.1");
    }

    #[test]
    fn split_and_missing_cohort_primaries_are_named() {
        let mut t = mn();
        t.nodes[3].is_primary = true; // second rpc primary
        t.nodes[0].is_primary = false; // admin cohort loses its primary
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::NoCohortPrimary { mesh: "mesh1".into(), kind: NodeKind::NodeAdmin }), "{v:?}");
        assert!(
            v.contains(&TopologyViolation::SplitCohortPrimary {
                mesh: "mesh1".into(),
                kind: NodeKind::RpcNode,
                primaries: vec!["mesh1.rpc.1".parse().unwrap(), "mesh1.rpc.2".parse().unwrap()],
            }),
            "{v:?}"
        );
        assert!(v.contains(&TopologyViolation::FabricPrimaryNotMeshPrimary("mesh1.admin.1".parse().unwrap())), "{v:?}");
        assert_eq!(t.cohort_primary("mesh1", NodeKind::RpcNode), None, "a split cohort has no single primary");
    }

    #[test]
    fn exactly_one_fabric_primary_and_it_is_an_admin() {
        let mut t = mn();
        t.nodes[1].is_fabric_primary = true;
        assert!(t.violations().contains(&TopologyViolation::FabricPrimaryCount(2)));
        assert_eq!(t.fabric_primary(), None);
        let mut t = mn();
        t.nodes[0].is_fabric_primary = false;
        t.nodes[2].is_fabric_primary = true;
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::FabricPrimaryNotAdmin("mesh1.rpc.1".parse().unwrap())), "{v:?}");
        let mut t = mn();
        t.nodes[0].is_fabric_primary = false;
        assert!(t.violations().contains(&TopologyViolation::FabricPrimaryCount(0)));
    }

    #[test]
    fn dead_nodes_neither_hold_nor_need_a_primary() {
        let mut t = mn();
        t.nodes[0].status = NodeStatus::Dead;
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::PrimaryNotLive("mesh1.admin.1".parse().unwrap())), "{v:?}");
        assert!(v.contains(&TopologyViolation::NoCohortPrimary { mesh: "mesh1".into(), kind: NodeKind::NodeAdmin }), "{v:?}");
        assert_eq!(t.mesh_control_endpoint("mesh1"), None, "a dead admin's endpoint is never advertised");
    }

    #[test]
    fn views_advertise_the_live_owning_admin_endpoint() {
        let t = mn();
        let f = t.fabric_view();
        assert_eq!(f.fabric_primary.as_ref().unwrap().to_string(), "mesh1.admin.1");
        assert_eq!(f.admin_api_base.as_deref(), Some("http://127.0.0.1:18001"));
        assert_eq!(f.meshes.len(), 1);
        assert_eq!(f.meshes[0].admin_api_base.as_deref(), Some("http://127.0.0.1:18001"));
        assert_eq!(f.meshes[0].nodes.len(), 5);
        let id = t.meshes[0].id.clone().unwrap().to_string();
        assert_eq!(t.mesh_view(&id), t.mesh_view("mesh1"));
        // Control moves with the primary, never to a hidden map.
        let mut t = mn();
        t.nodes[0].is_primary = false;
        t.nodes[0].is_fabric_primary = false;
        t.nodes[1].is_primary = true;
        t.nodes[1].is_fabric_primary = true;
        assert_eq!(t.fabric_view().admin_api_base.as_deref(), Some("http://127.0.0.1:18002"));
        assert!(t.is_converged());
    }

    #[test]
    fn structural_mismatches_are_named() {
        let mut t = mn();
        t.nodes.push(t.nodes[4].clone());
        t.nodes[2].mesh = "mesh9".into();
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::DuplicateNode("mesh1.rpc.3".parse().unwrap())), "{v:?}");
        assert!(v.contains(&TopologyViolation::KindMismatch("mesh1.rpc.1".parse().unwrap())), "{v:?}");
        assert!(v.contains(&TopologyViolation::NodeInUnknownMesh { node: "mesh1.rpc.1".parse().unwrap(), mesh: "mesh9".into() }), "{v:?}");
    }

    #[test]
    fn primary_admin_without_endpoint_is_named() {
        let mut t = mn();
        t.nodes[0].admin_api_base = None;
        assert!(t.violations().contains(&TopologyViolation::AdminPrimaryWithoutControlEndpoint("mesh1.admin.1".parse().unwrap())));
    }
}
