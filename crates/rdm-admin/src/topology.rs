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
    /// A `(mesh, kind)` group has no primary.
    NoNodeTypePrimary {
        /// The mesh concerned.
        mesh: String,
        /// The kind of the group.
        kind: NodeKind,
    },
    /// A `(mesh, kind)` group has more than one primary.
    SplitNodeTypePrimary {
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
            Self::DuplicateNode(p) => write!(f, "node {p:?} appears twice"),
            Self::DuplicateMesh(m) => write!(f, "mesh {m} appears twice"),
            Self::NodeInUnknownMesh { node, mesh } => write!(f, "node {node:?} names mesh {mesh}, which is not in the fabric"),
            Self::NoNodeTypePrimary { mesh, kind } => write!(f, "node type ({mesh}, {kind:?}) has no primary"),
            Self::SplitNodeTypePrimary { mesh, kind, primaries } => {
                write!(f, "node type ({mesh}, {kind:?}) has {} primaries: {primaries:?}", primaries.len())
            }
            Self::PrimaryNotLive(p) => write!(f, "primary {p:?} is not live"),
            Self::FabricPrimaryCount(n) => write!(f, "{n} fabric primaries, expected exactly one"),
            Self::FabricPrimaryNotAdmin(p) => write!(f, "fabric primary {p:?} is not a node-admin"),
            Self::FabricPrimaryNotMeshPrimary(p) => {
                write!(f, "fabric primary {p:?} is not the node-admin primary of its own mesh")
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
    pub primary_admin: Option<NodeId>,
    /// The control API base of the mesh's primary.
    pub admin_api_base: Option<String>,
    /// The globally unique IDs of every node of the mesh.
    pub nodes: Vec<NodeId>,
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
    pub fabric_primary: Option<NodeId>,
    /// The control API base of the fabric primary.
    pub admin_api_base: Option<String>,
    /// The fabric's meshes.
    pub meshes: Vec<MeshView>,
}

impl Topology {
    /// The active members used by planning, drift, election, primaries and executors.
    /// Explicit replacement provenance excludes a predecessor, whose path stays unchanged
    /// and whose name becomes `<name>-old`. Neither field's spelling determines membership.
    pub fn members(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.renamed_from.is_none())
    }

    /// Every birth the view holds, replaced predecessors included. For what names or waits on a
    /// birth wherever it is: the REST views, an identity lookup by node id or endpoint, and the
    /// shutdown that stops every runtime still present.
    pub fn births(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// The replaced predecessors (the predecessor name `<name>-old` at the same path) still held, until their departure.
    pub fn predecessors(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.renamed_from.is_some())
    }

    /// Whether the birth `node_id` is the only node of `mesh` (R-MT1: the delete of a mesh's only node
    /// skips node.primary.update). A member that is silent, or `Dead` by the true-offline mark alone,
    /// is still a node of the mesh; a deleted one has left the view. The one place that rule lives: the
    /// Build door (`http`) and the drive (`executor::role_move`) read it.
    pub fn only_node(&self, mesh: &str, node_id: &NodeId) -> bool {
        self.members().filter(|n| n.mesh == mesh).all(|n| n.node_id == *node_id)
    }

    /// Every node of one type in `mesh`, among the members.
    pub fn nodes_of_type(&self, mesh: &str, kind: NodeKind) -> impl Iterator<Item = &Node> {
        let mesh = mesh.to_string();
        self.members().filter(move |n| n.mesh == mesh && n.kind == kind)
    }

    /// Whether a node.create of the node-admin `node` makes it its mesh's first admin: it is a node-admin
    /// and no OTHER node-admin of its mesh is live, a live birth the view holds at its own path (a
    /// view that has not heard the mesh lost, or the exited birth a recovery creates over) never
    /// counting against it. The one rule of every node.create of a node-admin (e4.s11;
    /// node-create.md "A node-admin created into a live mesh").
    pub fn first_admin_of(&self, node: &PathName, mesh: &str, kind: NodeKind) -> bool {
        kind == NodeKind::NodeAdmin && self.nodes_of_type(mesh, NodeKind::NodeAdmin).filter(|n| n.name != *node).all(|n| !n.status.is_live())
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
            out.push(Missing { who: mesh.to_string().into(), why: "no-primary" });
        }
        let paths: Vec<PathName> = match planned {
            Some(p) => p.iter().cloned().collect(),
            None => self.members().filter(|n| n.mesh == mesh).map(|n| n.name.clone()).collect(),
        };
        for path in paths {
            match self.members().find(|n| n.name == path) {
                None => out.push(Missing { who: crate::checklist::MissingSubject::Location(path.clone()), why: "no-birth-heard" }),
                Some(n) if n.stopped => {}
                Some(n) if n.status != NodeStatus::Ready => out.push(Missing { who: crate::checklist::MissingSubject::Node(n.node_id.clone()), why: "not-ready" }),
                Some(_) => {}
            }
        }
        out
    }

    /// The single primary of a node type in `mesh`, when exactly one member holds it: the role the election
    /// resolved from the primary records this view was decided against, whatever the holder's status.
    /// Until the next election over this view, a ConnectionsDeleting, Leaving or silent primary is still
    /// the one named here. A caller that needs an admin it can reach checks the holder's
    /// status itself.
    pub fn node_type_primary(&self, mesh: &str, kind: NodeKind) -> Option<&Node> {
        let mut p = self.nodes_of_type(mesh, kind).filter(|n| n.is_primary);
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The fabric primary, when exactly one member holds the role: the holder the election resolved
    /// from the primary records, whatever its status, as [`Self::node_type_primary`].
    pub fn fabric_primary(&self) -> Option<&Node> {
        let mut p = self.members().filter(|n| n.is_fabric_primary);
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The row of the node born as `node_id`, among every birth the view holds: how a node finds
    /// itself (and a primary is told apart from another birth). Never by path and name, its shape
    /// included: a replace or a recovery keeps the path and name and mints a new node id, the one row
    /// this view holds at a path ([`crate::admin::project_at`] keys it by `PathName`) can be the
    /// deleted birth's, and a node whose row its mesh-primary renamed the predecessor name `<name>-old` at the same path is still that
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

    /// Whether the node born as `me` is the primary above the node `node_id` of `mesh` in this view
    /// (node-investigation.md, Caller): the fabric-primary above a mesh-primary, the mesh-primary above
    /// every other birth of its mesh. Nothing is above the fabric-primary, and no node is above itself.
    pub fn is_primary_above(&self, me: &NodeId, mesh: &str, node_id: &NodeId) -> bool {
        if node_id == me || self.is_fabric_primary(node_id) {
            return false;
        }
        if self.is_mesh_primary(mesh, node_id) {
            self.is_fabric_primary(me)
        } else {
            self.is_mesh_primary(mesh, me)
        }
    }

    /// The control endpoint of `mesh`'s primary (its admin primary's).
    pub(crate) fn mesh_control_endpoint(&self, mesh: &str) -> Option<&str> {
        self.node_type_primary(mesh, NodeKind::NodeAdmin)?.admin_api_base.as_deref()
    }

    /// The fabric-primary's control endpoint.
    pub(crate) fn fabric_control_endpoint(&self) -> Option<&str> {
        self.fabric_primary()?.admin_api_base.as_deref()
    }

    /// Resolve an admitted node reference without falling back from identity to location.
    /// Non-node subjects do not select a node.
    pub fn node_subject(&self, subject: &rdm_core::OperationSubject) -> Option<&Node> {
        match subject {
            rdm_core::OperationSubject::Node(id) => self.own(id),
            rdm_core::OperationSubject::Birth { node_id, incarnation } => self.own(node_id).filter(|node| node.incarnation_id.as_ref() == Some(incarnation)),
            rdm_core::OperationSubject::Location(location) => self.node(location),
            _ => None,
        }
    }

    /// The node named `name`.
    pub fn node(&self, name: &PathName) -> Option<&Node> {
        self.births().find(|n| &n.name == name)
    }

    /// The `(path, name)` the birth `node_id` (born as `birth`) currently holds in this view (R12: a
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
            if !seen_mesh.contains_key(&n.mesh) {
                out.push(V::NodeInUnknownMesh { node: n.name.clone(), mesh: n.mesh.clone() });
            }
            if (n.is_primary || n.is_fabric_primary) && !n.status.is_live() {
                out.push(V::PrimaryNotLive(n.name.clone()));
            }
        }
        // One primary per declared node type in a mesh that has a live member.
        let mut node_types: BTreeMap<(String, NodeKind), Vec<&Node>> = BTreeMap::new();
        for n in self.members().filter(|n| n.status.is_live()) {
            node_types.entry((n.mesh.clone(), n.kind)).or_default().push(n);
        }
        for ((mesh, kind), members) in &node_types {
            let primaries: Vec<PathName> = members.iter().filter(|n| n.is_primary).map(|n| n.name.clone()).collect();
            match primaries.len() {
                0 => out.push(V::NoNodeTypePrimary { mesh: mesh.clone(), kind: *kind }),
                1 => {}
                _ => out.push(V::SplitNodeTypePrimary { mesh: mesh.clone(), kind: *kind, primaries }),
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
        let mut nodes: Vec<NodeId> = self.births().filter(|n| n.mesh == m.name).map(|n| n.node_id.clone()).collect();
        nodes.sort();
        Some(MeshView {
            id: m.id.clone(),
            name: m.name.clone(),
            status: m.status,
            primary_admin: self.node_type_primary(&m.name, NodeKind::NodeAdmin).map(|n| n.node_id.clone()),
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
            fabric_primary: self.fabric_primary().map(|n| n.node_id.clone()),
            admin_api_base: self.fabric_control_endpoint().map(str::to_string),
            meshes: self.meshes.iter().filter_map(|m| self.mesh_view(&m.name)).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn node(name: PathName, mesh: &str, kind: rdm_core::NodeKind, status: NodeStatus, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name, mesh, kind);
        n.status = status;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        if n.kind == NodeKind::NodeAdmin {
            // Each admin its own port in the fixture: mesh1.admin.1 serves 18001, mesh1.admin.2 18002.
            n.admin_api_base = Some(format!("http://127.0.0.1:{}", 18000 + n.name.name.parse::<u16>().expect("the fixture's admins are numbered")));
        }
        n
    }

    fn mn() -> Topology {
        use NodeStatus::Ready as R;
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![
                node(PathName::new("mesh1.admin", "1").unwrap(), "mesh1", rdm_core::NodeKind::NodeAdmin, R, true, true),
                node(PathName::new("mesh1.admin", "2").unwrap(), "mesh1", rdm_core::NodeKind::NodeAdmin, R, false, false),
                node(PathName::new("mesh1.rpc", "1").unwrap(), "mesh1", rdm_core::NodeKind::RpcNode, R, true, false),
                node(PathName::new("mesh1.rpc", "2").unwrap(), "mesh1", rdm_core::NodeKind::RpcNode, R, false, false),
                node(PathName::new("mesh1.rpc", "3").unwrap(), "mesh1", rdm_core::NodeKind::RpcNode, R, false, false),
            ],
        }
    }

    /// CONTRACT: selection follows the admitted ID and incarnation across a rename;
    /// the replacement at the original location is never substituted for that birth.
    // @feature: node-records
    #[test]
    fn admitted_subject_survives_rename_and_rejects_another_incarnation() {
        let mut view = mn();
        let location = view.nodes[0].name.clone();
        let incarnation = IncarnationId::mint();
        view.nodes[0].incarnation_id = Some(incarnation.clone());
        let id = view.nodes[0].node_id.clone();
        let op = crate::build::BuildOperation::StopNode { node: location.clone(), mesh: "mesh1".into(), kind: NodeKind::NodeAdmin, node_id: id.clone(), from_incarnation: incarnation.clone() };
        let subject = op.subject().unwrap();
        view.nodes[0].name = PathName::new(&location.path, format!("{}-old", location.name)).unwrap();
        let mut successor = Node::allocated(location.clone(), "mesh1", NodeKind::NodeAdmin);
        successor.incarnation_id = Some(IncarnationId::mint());
        let successor_id = successor.node_id.clone();
        view.nodes.push(successor);
        assert_eq!(view.node_subject(&subject).map(|n| &n.node_id), Some(&id));
        assert_eq!(view.node_subject(&rdm_core::OperationSubject::Location(location)).map(|n| &n.node_id), Some(&successor_id));
        assert_eq!(crate::executor::role_move(&op, &view).map(|m| m.primary.node_id), Some(id.clone()));
        let updates = crate::build_drive::preceding_updates(&op, &rdm_core::BuildId::mint(), 2);
        assert_eq!(updates.len(), 2);
        assert!(updates.iter().all(|update| update.primary.as_ref() == Some(&subject)));
        view.nodes[0].incarnation_id = Some(IncarnationId::mint());
        assert!(view.node_subject(&subject).is_none());
        assert!(crate::executor::role_move(&op, &view).is_none());
        view.nodes.retain(|n| n.node_id != id);
        assert!(view.node_subject(&subject).is_none());
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
        let admin1: PathName = rdm_core::PathName::new("mesh1.admin", "1").unwrap();
        let admin2: PathName = rdm_core::PathName::new("mesh1.admin", "2").unwrap();
        assert!(!t.first_admin_of(&admin1, "mesh1", NodeKind::NodeAdmin), "admin.2 is live: admin.1 is not the first");
        t.nodes[1].status = Dead;
        assert!(t.first_admin_of(&admin1, "mesh1", NodeKind::NodeAdmin), "admin.1's own live birth does not count against it: no other node-admin lives");
        assert!(!t.first_admin_of(&admin2, "mesh1", NodeKind::NodeAdmin), "admin.1 is live: admin.2 is not the first");
        t.nodes[0].status = Dead;
        assert!(t.first_admin_of(&admin1, "mesh1", NodeKind::NodeAdmin) && t.first_admin_of(&admin2, "mesh1", NodeKind::NodeAdmin), "no node-admin lives: either create is the first");
        t.nodes[0].status = R;
        t.nodes.retain(|n| n.name != admin2);
        assert!(!t.first_admin_of(&admin2, "mesh1", NodeKind::NodeAdmin), "a path the view holds nothing at, beside a live admin.1: not the first");
        assert!(!t.first_admin_of(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap(), "mesh1", NodeKind::RpcNode), "a node that is not a node-admin is never a mesh's first admin");
    }

    #[test]
    fn settled_mn_is_converged_with_one_primary_per_node_type() {
        let t = mn();
        assert_eq!(t.violations(), vec![]);
        assert_eq!(t.node_type_primary("mesh1", NodeKind::NodeAdmin).unwrap().name, PathName::new("mesh1.admin", "1").unwrap());
        assert_eq!(t.node_type_primary("mesh1", NodeKind::RpcNode).unwrap().name, PathName::new("mesh1.rpc", "1").unwrap());
        assert_eq!(t.fabric_primary().unwrap().name, PathName::new("mesh1.admin", "1").unwrap());
    }

    #[test]
    fn split_and_missing_node_type_primaries_are_named() {
        let mut t = mn();
        t.nodes[3].is_primary = true; // second rpc primary
        t.nodes[0].is_primary = false; // node-admin type loses its primary
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::NoNodeTypePrimary { mesh: "mesh1".into(), kind: NodeKind::NodeAdmin }), "{v:?}");
        assert!(
            v.contains(&TopologyViolation::SplitNodeTypePrimary {
                mesh: "mesh1".into(),
                kind: NodeKind::RpcNode,
                primaries: vec![rdm_core::PathName::new("mesh1.rpc", "1").unwrap(), rdm_core::PathName::new("mesh1.rpc", "2").unwrap()],
            }),
            "{v:?}"
        );
        assert!(v.contains(&TopologyViolation::FabricPrimaryNotMeshPrimary(rdm_core::PathName::new("mesh1.admin", "1").unwrap())), "{v:?}");
        assert_eq!(t.node_type_primary("mesh1", NodeKind::RpcNode), None, "a split node type has no single primary");
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
        assert!(v.contains(&TopologyViolation::FabricPrimaryNotAdmin(rdm_core::PathName::new("mesh1.rpc", "1").unwrap())), "{v:?}");
        let mut t = mn();
        t.nodes[0].is_fabric_primary = false;
        assert!(t.violations().contains(&TopologyViolation::FabricPrimaryCount(0)));
    }

    /// CONTRACT: a primary marked silent after the election that named it is a view that has not
    /// converged, and until the next election it is still the holder the view names and advertises. Must NOT happen: the readers dropping a silent primary,
    /// so the mesh shows no primary while its primary record names one.
    #[test]
    fn a_silent_primary_is_unconverged_and_still_the_named_primary() {
        let mut t = mn();
        t.nodes[0].status = NodeStatus::PendingReconnect;
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::PrimaryNotLive(rdm_core::PathName::new("mesh1.admin", "1").unwrap())), "{v:?}");
        assert!(v.contains(&TopologyViolation::NoNodeTypePrimary { mesh: "mesh1".into(), kind: NodeKind::NodeAdmin }), "{v:?}");
        assert_eq!(t.node_type_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.node_id.clone()), Some(t.nodes[0].node_id.clone()), "the silent primary is still the one named");
        assert_eq!(t.mesh_control_endpoint("mesh1"), Some("http://127.0.0.1:18001"), "the holder's endpoint is the mesh's control endpoint");
    }

    #[test]
    fn views_advertise_the_live_owning_admin_endpoint() {
        let t = mn();
        let f = t.fabric_view();
        assert_eq!(f.fabric_primary.as_ref(), Some(&t.nodes[0].node_id));
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
        let mut explicit = mn();
        explicit.nodes[2].name = rdm_core::PathName::new("elsewhere.gateway", "north").unwrap();
        assert!(explicit.is_converged(), "a free path/name does not select mesh or kind: {:?}", explicit.violations());
        let mut t = mn();
        t.nodes.push(t.nodes[4].clone());
        t.nodes[2].mesh = "mesh9".into();
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::DuplicateNode(rdm_core::PathName::new("mesh1.rpc", "3").unwrap())), "{v:?}");
        assert!(v.contains(&TopologyViolation::NodeInUnknownMesh { node: rdm_core::PathName::new("mesh1.rpc", "1").unwrap(), mesh: "mesh9".into() }), "{v:?}");
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
