//! The fabric topology projection and its invariants (`docs/architecture/gossip.md`,
//! `docs/architecture/query.md`, `docs/architecture/fabric-node-lifecycle-elections.md`).
//!
//! A converged topology has exactly one `is_primary` per declared `(mesh, kind)` group
//! and exactly one `is_fabric_primary` across the fabric, held by a `node-admin` that is also its
//! own mesh's `mesh-primary`. Mesh and Fabric views publish the primary's control API base; callers
//! switch control only from these views.

use crate::model::{Fabric, Mesh, Node, NodeId, NodeKind, NodeStatus, PathName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// The observed topology of one fabric (`docs/architecture/gossip.md` §1,
/// `docs/architecture/query.md`): its record, its meshes and its nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// The fabric record.
    pub fabric: Fabric,
    /// The fabric's meshes.
    pub meshes: Vec<Mesh>,
    /// The fabric's nodes.
    pub nodes: Vec<Node>,
}

/// A topology that is not (yet) converged, named (`docs/architecture/fabric-node-lifecycle-elections.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyViolation {
    /// A node path appears twice.
    DuplicateNode(PathName),
    /// A mesh name appears twice.
    DuplicateMesh(String),
    /// A node names a mesh the fabric does not hold.
    NodeInUnknownMesh {
        /// The node concerned.
        node: PathName,
        /// The mesh concerned.
        mesh: String,
    },
    /// A node's kind or mesh field disagrees with its `path.name`.
    KindMismatch(PathName),
    /// A `(mesh, kind)` group has no primary.
    NoCohortPrimary {
        /// The mesh concerned.
        mesh: String,
        /// The kind of the group.
        kind: NodeKind,
    },
    /// A `(mesh, kind)` group has more than one primary.
    SplitCohortPrimary {
        /// The mesh concerned.
        mesh: String,
        /// The kind of the group.
        kind: NodeKind,
        /// The nodes that hold the primary role.
        primaries: Vec<PathName>,
    },
    /// A primary is not live.
    PrimaryNotLive(PathName),
    /// The fabric does not hold exactly one fabric primary.
    FabricPrimaryCount(usize),
    /// The fabric primary is not a node-admin.
    FabricPrimaryNotAdmin(PathName),
    /// The fabric primary is not the node-admin primary of its own mesh.
    FabricPrimaryNotMeshPrimary(PathName),
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
        }
    }
}

/// A mesh as clients see it (`MeshView`, `docs/architecture/query.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshView {
    /// The mesh's minted id, when it has one.
    pub id: Option<crate::model::MeshId>,
    /// The mesh's name.
    pub name: String,
    /// The mesh's lifecycle state.
    pub status: crate::model::ScopeStatus,
    /// The mesh's primary node-admin, when one is elected.
    pub primary_admin: Option<PathName>,
    /// The control API base of the mesh's primary.
    pub admin_api_base: Option<String>,
    /// The `path.name` of every node of the mesh.
    pub nodes: Vec<PathName>,
}

/// The fabric as clients see it (`FabricView`, `docs/architecture/query.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricView {
    /// The fabric's minted id.
    pub id: crate::model::FabricId,
    /// The fabric's name.
    pub name: String,
    /// The fabric's lifecycle state.
    pub status: crate::model::ScopeStatus,
    /// The provider that runs the fabric's nodes.
    pub provider: crate::model::ProviderKind,
    /// The fabric's primary node-admin, when one is elected.
    pub fabric_primary: Option<PathName>,
    /// The control API base of the fabric primary.
    pub admin_api_base: Option<String>,
    /// The fabric's meshes.
    pub meshes: Vec<MeshView>,
}

impl Topology {
    /// The nodes that hold their paths (`docs/architecture/node-identity-and-state.md` §2.7). Every
    /// decision (planning, drift, election, primaries, executors) reads the view through this. A
    /// replaced predecessor (`<path>.old`) does not hold its path: its path is its successor's. This
    /// is the one place that rule lives.
    pub fn members(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| !n.name.old)
    }

    /// Every birth the view holds, replaced predecessors included. For what names or waits on a
    /// birth wherever it is: the REST views, an identity lookup by node id or endpoint, and the
    /// shutdown that stops every runtime still present.
    pub fn births(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// The replaced predecessors (`<path>.old`) still held, until their departure.
    pub fn predecessors(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.name.old)
    }

    /// Whether the birth `node_id` is the only node of `mesh` (R-MT1: the delete of a mesh's only node
    /// skips node.primary.update). A member that is silent, or `Dead` by the true-offline mark alone,
    /// is still a node of the mesh; a deleted one has left the view. The one place that rule lives: the
    /// Build door (`http`) and the drive (`executor::seat_move`) read it.
    pub fn only_node(&self, mesh: &str, node_id: &NodeId) -> bool {
        self.members().filter(|n| n.mesh == mesh).all(|n| n.node_id == *node_id)
    }

    /// Every node of one cohort, among the members.
    pub fn nodes_of_type(&self, mesh: &str, kind: NodeKind) -> impl Iterator<Item = &Node> {
        let mesh = mesh.to_string();
        self.members().filter(move |n| n.mesh == mesh && n.kind == kind)
    }

    /// Whether a node.create of the node-admin `node` makes it its mesh's first admin: it is a node-admin
    /// and no OTHER node-admin of its mesh is live, a live birth the view holds at its own path (a
    /// view that has not heard the mesh lost, or the exited birth a recovery creates over) never
    /// counting against it. The one rule of every node.create of a node-admin (e4.s11;
    /// mesh-investigation.md "A node-admin joins the live mesh").
    pub fn first_admin_of(&self, node: &PathName) -> bool {
        node.kind == NodeKind::NodeAdmin && self.nodes_of_type(&node.mesh, NodeKind::NodeAdmin).filter(|n| n.name != *node).all(|n| !n.status.is_live())
    }

    /// Why `mesh` is not ready: a mesh is ready when it has a primary node-admin and
    /// every node of the mesh that is not stopped is ready (states.md:16, Q240): a stopped birth is at
    /// rest by command and holds neither its mesh nor a checklist, so a mesh whose every birth but its
    /// admin is stopped is ready. The nodes of the mesh are the
    /// accepted Build's planned births (`planned`), a planned birth the view does not hold yet
    /// counting as not ready; with no accepted Build held, the members the view holds. Empty: the
    /// mesh is ready. The one derivation of a mesh's state: the projection and the fabric's entry
    /// check both read it.
    pub fn mesh_unready(&self, mesh: &str, planned: Option<&std::collections::BTreeSet<PathName>>) -> Vec<crate::checklist::Missing> {
        use crate::checklist::Missing;
        let mut out = Vec::new();
        if self.node_type_primary(mesh, NodeKind::NodeAdmin).is_none() {
            out.push(Missing { who: mesh.to_string(), why: "no-primary" });
        }
        let paths: Vec<PathName> = match planned {
            Some(p) => p.iter().cloned().collect(),
            None => self.members().filter(|n| n.mesh == mesh).map(|n| n.name.clone()).collect(),
        };
        for path in paths {
            match self.members().find(|n| n.name == path) {
                None => out.push(Missing { who: path.to_string(), why: "no-birth-heard" }),
                Some(n) if n.stopped => {}
                Some(n) if n.status != NodeStatus::Ready => out.push(Missing { who: path.to_string(), why: "not-ready" }),
                Some(_) => {}
            }
        }
        out
    }

    /// The single primary of a cohort, when exactly one member holds it: the seat the election
    /// resolved from the seat records this view was decided against, whatever the holder's status.
    /// Until the next election over this view, a ConnectionsDeleting, Leaving or silent holder is still
    /// the one named here. A caller that needs an admin it can reach checks the holder's
    /// status itself.
    pub fn node_type_primary(&self, mesh: &str, kind: NodeKind) -> Option<&Node> {
        let mut p = self.nodes_of_type(mesh, kind).filter(|n| n.is_primary);
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The fabric primary, when exactly one member holds the seat: the holder the election resolved
    /// from the seat records, whatever its status, as `Self::cohort_primary`.
    pub fn fabric_primary(&self) -> Option<&Node> {
        let mut p = self.members().filter(|n| n.is_fabric_primary);
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The row of the node born as `node_id`, among every birth the view holds: how a node finds
    /// itself (and a seat holder is told apart from another birth). Never by path.name, its shape
    /// included: a replace or a recovery keeps the path.name and mints a new node id, the one row
    /// this view holds at a path ([`crate::admin::project_at`] keys it by `PathName`) can be the
    /// deleted birth's, and a node whose row its mesh-primary renamed `<path>.old` is still that
    /// exact birth, which reports its own status (states.md "Who sets each state").
    pub fn own(&self, node_id: &NodeId) -> Option<&Node> {
        self.births().find(|n| n.node_id == *node_id)
    }

    /// Whether the node born as `node_id` is the fabric primary of this view.
    pub fn is_fabric_primary(&self, node_id: &NodeId) -> bool {
        self.fabric_primary().is_some_and(|n| n.node_id == *node_id)
    }

    /// Whether the node born as `node_id` is `mesh`'s primary node-admin in this view.
    pub fn is_mesh_primary(&self, mesh: &str, node_id: &NodeId) -> bool {
        self.node_type_primary(mesh, NodeKind::NodeAdmin).is_some_and(|n| n.node_id == *node_id)
    }

    /// The control endpoint of `mesh`'s seat holder (its admin primary's).
    pub(crate) fn mesh_control_endpoint(&self, mesh: &str) -> Option<&str> {
        self.node_type_primary(mesh, NodeKind::NodeAdmin)?.admin_api_base.as_deref()
    }

    /// The fabric-primary's control endpoint.
    pub(crate) fn fabric_control_endpoint(&self) -> Option<&str> {
        self.fabric_primary()?.admin_api_base.as_deref()
    }

    /// The node named `name`.
    pub fn node(&self, name: &PathName) -> Option<&Node> {
        self.births().find(|n| &n.name == name)
    }

    /// The `path.name` the birth `node_id` (born as `birth`) currently holds in this view (R12: a
    /// live or rejoin `node.name.update` moves the birth to its new name while structs built at boot
    /// still hold `birth`). Resolves by `node_id` first when given, then by `birth`, else the mesh's
    /// primary or sole node-admin when `birth` is a node-admin not listed under `birth`, falling back
    /// to `birth` before the view holds it.
    pub fn held_name_of(&self, birth: &PathName, node_id: Option<&NodeId>) -> PathName {
        if let Some(n) = node_id.and_then(|id| self.own(id)) {
            return n.name.clone();
        }
        if self.node(birth).is_some() {
            return birth.clone();
        }
        if birth.kind == NodeKind::NodeAdmin {
            if let Some(p) = self.node_type_primary(&birth.mesh, NodeKind::NodeAdmin) {
                return p.name.clone();
            }
            let mut admins = self.nodes_of_type(&birth.mesh, NodeKind::NodeAdmin);
            if let Some(only) = admins.next().filter(|_| admins.next().is_none()) {
                return only.name.clone();
            }
        }
        birth.clone()
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
        for n in self.births() {
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
        for n in self.members().filter(|n| n.status.is_live()) {
            cohorts.entry((n.mesh.clone(), n.kind)).or_default().push(n);
        }
        for ((mesh, kind), members) in &cohorts {
            let primaries: Vec<PathName> = members.iter().filter(|n| n.is_primary).map(|n| n.name.clone()).collect();
            match primaries.len() {
                0 => out.push(V::NoCohortPrimary { mesh: mesh.clone(), kind: *kind }),
                1 => {}
                _ => out.push(V::SplitCohortPrimary { mesh: mesh.clone(), kind: *kind, primaries }),
            }
        }
        // Exactly one fabric primary, a node-admin that is its mesh's admin primary.
        let fps: Vec<&Node> = self.members().filter(|n| n.is_fabric_primary).collect();
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

    /// Whether the topology has no violation.
    pub fn is_converged(&self) -> bool {
        self.violations().is_empty()
    }

    /// The mesh named or identified by `id_or_name`, as clients see it.
    pub fn mesh_view(&self, id_or_name: &str) -> Option<MeshView> {
        let m = self.meshes.iter().find(|m| m.name == id_or_name || m.id.as_ref().is_some_and(|i| i.as_str() == id_or_name))?;
        let mut nodes: Vec<PathName> = self.births().filter(|n| n.mesh == m.name).map(|n| n.name.clone()).collect();
        nodes.sort();
        Some(MeshView {
            id: m.id.clone(),
            name: m.name.clone(),
            status: m.status,
            primary_admin: self.node_type_primary(&m.name, NodeKind::NodeAdmin).map(|n| n.name.clone()),
            admin_api_base: self.mesh_control_endpoint(&m.name).map(str::to_string),
            nodes,
        })
    }

    /// The fabric as clients see it.
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
        use NodeStatus::Ready as R;
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![
                node("mesh1.admin.1", R, true, true),
                node("mesh1.admin.2", R, false, false),
                node("mesh1.rpc.1", R, true, false),
                node("mesh1.rpc.2", R, false, false),
                node("mesh1.rpc.3", R, false, false),
            ],
        }
    }

    /// CONTRACT: a node.create of a node-admin makes it its mesh's first admin when no OTHER node-admin
    /// of the mesh is live: the node being created never counts against itself, whether the view
    /// still holds a live birth at its path (a view that has not heard the mesh lost, or the exited
    /// birth a node recovery creates over) or holds none. A live sibling makes it not the first; a
    /// node that is not a node-admin is never one. Must NOT happen: the two node.create paths
    /// (the Build's create and node recovery's) answering differently for one view.
    #[test]
    fn a_node_admin_is_the_first_admin_when_no_other_node_admin_of_its_mesh_is_live() {
        use NodeStatus::{Dead, Ready as R};
        let mut t = mn();
        let admin1: PathName = "mesh1.admin.1".parse().unwrap();
        let admin2: PathName = "mesh1.admin.2".parse().unwrap();
        assert!(!t.first_admin_of(&admin1), "admin.2 is live: admin.1 is not the first");
        t.nodes[1].status = Dead;
        assert!(t.first_admin_of(&admin1), "admin.1's own live birth does not count against it: no other node-admin lives");
        assert!(!t.first_admin_of(&admin2), "admin.1 is live: admin.2 is not the first");
        t.nodes[0].status = Dead;
        assert!(t.first_admin_of(&admin1) && t.first_admin_of(&admin2), "no node-admin lives: either create is the first");
        t.nodes[0].status = R;
        t.nodes.retain(|n| n.name != admin2);
        assert!(!t.first_admin_of(&admin2), "a path the view holds nothing at, beside a live admin.1: not the first");
        assert!(!t.first_admin_of(&"mesh1.rpc.1".parse().unwrap()), "a node that is not a node-admin is never a mesh's first admin");
    }

    #[test]
    fn settled_mn_is_converged_with_one_primary_per_node_type() {
        let t = mn();
        assert_eq!(t.violations(), vec![]);
        assert_eq!(t.node_type_primary("mesh1", NodeKind::NodeAdmin).unwrap().name.to_string(), "mesh1.admin.1");
        assert_eq!(t.node_type_primary("mesh1", NodeKind::RpcNode).unwrap().name.to_string(), "mesh1.rpc.1");
        assert_eq!(t.fabric_primary().unwrap().name.to_string(), "mesh1.admin.1");
    }

    #[test]
    fn split_and_missing_node_type_primaries_are_named() {
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
        assert_eq!(t.node_type_primary("mesh1", NodeKind::RpcNode), None, "a split cohort has no single primary");
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

    /// CONTRACT: a seat holder marked silent after the election that named it is a view that has not
    /// converged, and until the next election it is still the holder the view names and advertises. Must NOT happen: the readers dropping a silent holder,
    /// so the mesh shows no primary while its seat record names one.
    #[test]
    fn a_silent_primary_is_unconverged_and_still_the_named_primary() {
        let mut t = mn();
        t.nodes[0].status = NodeStatus::PendingReconnect;
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::PrimaryNotLive("mesh1.admin.1".parse().unwrap())), "{v:?}");
        assert!(v.contains(&TopologyViolation::NoCohortPrimary { mesh: "mesh1".into(), kind: NodeKind::NodeAdmin }), "{v:?}");
        assert_eq!(t.node_type_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"), "the silent holder is still the one named");
        assert_eq!(t.mesh_control_endpoint("mesh1"), Some("http://127.0.0.1:18001"), "the holder's endpoint is the mesh's control endpoint");
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

    /// CONTRACT (R-OW1: "`admin_api_base` is config, never checked"; lifecycles/node-name-update.md: "Config on the
    /// row, `admin_api_base` included, is stored and gossiped as given; nothing checks it"): a
    /// fabric whose node-admins advertise no `admin_api_base` names no violation and is converged.
    /// What must NOT happen: a topology held unconverged for a config value RDM never reads.
    #[test]
    fn a_node_admin_primary_with_no_admin_api_base_names_no_violation() {
        let mut t = mn();
        for n in t.nodes.iter_mut() {
            n.admin_api_base = None;
        }
        assert_eq!(t.violations(), vec![]);
        assert!(t.is_converged());
    }
}
