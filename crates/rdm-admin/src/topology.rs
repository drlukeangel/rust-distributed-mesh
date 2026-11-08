//! The fabric topology projection and its invariants (`docs/architecture/gossip.md`,
//! `docs/architecture/query.md`, `docs/architecture/fabric-node-lifecycle-elections.md`).
//!
//! A converged topology has exactly one `is_primary` per declared `(mesh, kind)` group
//! and exactly one `is_fabric_primary` across the fabric, held by a `node-admin` that is also its
//! own mesh's `mesh-primary`. Mesh and Fabric views publish the primary's control API base; callers
//! switch control only from these views.

use crate::model::{Fabric, Mesh, Node, NodeId, NodeStatus, PathName};
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
    DuplicateNode {
        /// The node's path.
        path: String,
        /// The node's name, beside its path.
        name: String,
    },
    /// A mesh name appears twice.
    DuplicateMesh(String),
    /// A node names a mesh the fabric does not hold.
    NodeInUnknownMesh {
        /// The node concerned.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The mesh concerned.
        mesh: String,
    },
    /// A `(mesh, kind)` group has no primary.
    NoNodePrimary {
        /// The mesh concerned.
        mesh: String,
        /// The kind of the group.
        r#type: String,
    },
    /// A `(mesh, kind)` group has more than one primary.
    SplitNodePrimary {
        /// The mesh concerned.
        mesh: String,
        /// The kind of the group.
        r#type: String,
        /// The nodes that hold the primary role.
        primaries: Vec<PathName>,
    },
    /// A primary is not live.
    PrimaryNotLive {
        /// The node's path.
        path: String,
        /// The node's name, beside its path.
        name: String,
    },
    /// The fabric does not hold exactly one fabric primary.
    FabricPrimaryCount(usize),
    /// The fabric primary is not a node-admin.
    FabricPrimaryNotAdmin {
        /// The node's path.
        path: String,
        /// The node's name, beside its path.
        name: String,
    },
    /// The fabric primary is not the node-admin primary of its own mesh.
    FabricPrimaryNotMeshPrimary {
        /// The node's path.
        path: String,
        /// The node's name, beside its path.
        name: String,
    },
}

impl fmt::Display for TopologyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode { path, name } => write!(f, "node (path {path:?}, name {name:?}) appears twice"),
            Self::DuplicateMesh(m) => write!(f, "mesh {m} appears twice"),
            Self::NodeInUnknownMesh { path, name, mesh } => write!(f, "node (path {path:?}, name {name:?}) names mesh {mesh}, which is not in the fabric"),
            Self::NoNodePrimary { mesh, r#type } => write!(f, "node type ({mesh}, {:?}) has no primary", r#type),
            Self::SplitNodePrimary { mesh, r#type, primaries } => {
                write!(f, "node type ({mesh}, {:?}) has {} primaries: {primaries:?}", r#type, primaries.len())
            }
            Self::PrimaryNotLive { path, name } => write!(f, "primary (path {path:?}, name {name:?}) is not live"),
            Self::FabricPrimaryCount(n) => write!(f, "{n} fabric primaries, expected exactly one"),
            Self::FabricPrimaryNotAdmin { path, name } => write!(f, "fabric primary (path {path:?}, name {name:?}) is not a node-admin"),
            Self::FabricPrimaryNotMeshPrimary { path, name } => {
                write!(f, "fabric primary (path {path:?}, name {name:?}) is not the node-admin primary of its own mesh")
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
    /// The fabric's meshes.
    pub meshes: Vec<MeshView>,
}

impl Topology {
    /// Every birth the view holds, stopped renamed nodes included: what planning, the election, the
    /// primaries, the executors, the REST views, an identity lookup by node id or endpoint and the
    /// shutdown that stops every runtime still present read (D63: the one list of the view's births).
    pub fn births(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter()
    }

    /// Whether the birth `node_id` is the only node of `mesh` (R-MT1: the delete of a mesh's only node
    /// skips node.primary.update). A member that is silent, or `Dead` by the dead mark alone,
    /// is still a node of the mesh; a deleted one has left the view. The one place that rule lives: the
    /// `BuildService` (`http`) and the drive (`executor::role_move`) read it.
    pub fn only_node(&self, mesh: &str, node_id: &NodeId) -> bool {
        self.births().filter(|n| n.mesh == mesh).all(|n| n.node_id == *node_id)
    }

    /// Every node of one type in `mesh`, among the members.
    pub fn nodes_of_type(&self, mesh: &str, r#type: &str) -> impl Iterator<Item = &Node> + '_ {
        let (mesh, r#type) = (mesh.to_string(), r#type.to_string());
        self.births().filter(move |n| n.mesh == mesh && n.r#type == r#type)
    }

    /// Whether a node.create of the node-admin `node` makes it its mesh's first admin: it is a node-admin
    /// and no OTHER node-admin of its mesh is live, a live birth the view holds at its own path (a
    /// view that has not heard the mesh lost, or the exited birth a recovery creates over) never
    /// counting against it. The one rule of every node.create of a node-admin (e4.s11;
    /// node-create.md "A node-admin created into a live mesh").
    pub fn first_admin_of(&self, node: &PathName, mesh: &str, r#type: &str) -> bool {
        rdm_core::is_node_admin(&r#type) && self.nodes_of_type(mesh, "node-admin").filter(|n| n.location() != *node).all(|n| !n.status.is_live())
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
        if self.node_type_primary(mesh, "node-admin").is_none() {
            out.push(Missing { who: mesh.to_string().into(), why: "no-primary" });
        }
        let paths: Vec<PathName> = match planned {
            Some(p) => p.iter().cloned().collect(),
            None => self.births().filter(|n| n.mesh == mesh).map(|n| n.location()).collect(),
        };
        for path in paths {
            match self.births().find(|n| n.location() == path) {
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
    pub fn node_type_primary(&self, mesh: &str, r#type: &str) -> Option<&Node> {
        let mut p = self.nodes_of_type(mesh, r#type).filter(|n| n.is_primary);
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The fabric primary, when exactly one member holds the role: the holder the election resolved
    /// from the primary records, whatever its status, as [`Self::node_type_primary`].
    pub fn fabric_primary(&self) -> Option<&Node> {
        let mut p = self.births().filter(|n| n.is_fabric_primary);
        let first = p.next()?;
        p.next().is_none().then_some(first)
    }

    /// The row of the node born as `node_id`, among every birth the view holds: how a node finds
    /// itself (and a primary is told apart from another birth). Never by path and name, its shape
    /// included: a recovery keeps the path and name and mints a new node id, the one row
    /// this view holds at a path ([`crate::admin::project_at`] keys it by `PathName`) can be the
    /// deleted birth's, and a node whose row its mesh-primary renamed a name node.name.update gave it is still that
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
        self.node_type_primary(mesh, "node-admin").is_some_and(|n| n.node_id == *node_id)
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

    /// Resolve an admitted node reference without falling back from identity to location.
    /// Non-node subjects do not select a node.
    pub fn node_subject(&self, subject: &rdm_core::OperationSubject) -> Option<&Node> {
        match subject {
            rdm_core::OperationSubject::Node(id) => self.own(id),
            rdm_core::OperationSubject::Birth { node_id, incarnation } => self.own(node_id).filter(|node| node.incarnation_id.as_ref() == Some(incarnation)),
            _ => None,
        }
    }

    /// The node named `name`.
    pub fn node(&self, name: &PathName) -> Option<&Node> {
        self.births().find(|n| &n.location() == name)
    }

    /// The `(path, name)` the birth `node_id` (born as `birth`) currently holds in this view (R12: a
    /// live or rejoin `node.name.update` moves the birth to its new name while structs built at boot
    /// still hold `birth`). Resolves by `node_id` first when given, then by `birth`, else the mesh's
    /// primary or sole node-admin when `birth` is a node-admin not listed under `birth`, falling back
    /// to `birth` before the view holds it.
    pub fn held_name_of(&self, birth: &PathName, node_id: Option<&NodeId>) -> PathName {
        if let Some(n) = node_id.and_then(|id| self.own(id)) {
            return n.location();
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
            if seen.insert(n.location(), ()).is_some() {
                out.push(V::DuplicateNode { path: n.path.clone(), name: n.name.clone() });
            }
            if !seen_mesh.contains_key(&n.mesh) {
                out.push(V::NodeInUnknownMesh { path: n.path.clone(), name: n.name.clone(), mesh: n.mesh.clone() });
            }
            if (n.is_primary || n.is_fabric_primary) && !n.status.is_live() {
                out.push(V::PrimaryNotLive { path: n.path.clone(), name: n.name.clone() });
            }
        }
        // One primary per declared node type in a mesh that has a live member.
        let mut node_types: BTreeMap<(String, String), Vec<&Node>> = BTreeMap::new();
        for n in self.births().filter(|n| n.status.is_live()) {
            node_types.entry((n.mesh.clone(), n.r#type.clone())).or_default().push(n);
        }
        for ((mesh, r#type), members) in &node_types {
            let primaries: Vec<PathName> = members.iter().filter(|n| n.is_primary).map(|n| n.location()).collect();
            match primaries.len() {
                0 => out.push(V::NoNodePrimary { mesh: mesh.clone(), r#type: r#type.clone() }),
                1 => {}
                _ => out.push(V::SplitNodePrimary { mesh: mesh.clone(), r#type: r#type.clone(), primaries }),
            }
        }
        // Exactly one fabric primary, a node-admin that is its mesh's admin primary.
        let fps: Vec<&Node> = self.births().filter(|n| n.is_fabric_primary).collect();
        if fps.len() != 1 {
            out.push(V::FabricPrimaryCount(fps.len()));
        }
        for fp in fps {
            if !rdm_core::is_node_admin(&fp.r#type) {
                out.push(V::FabricPrimaryNotAdmin { path: fp.path.clone(), name: fp.name.clone() });
            } else if !fp.is_primary {
                out.push(V::FabricPrimaryNotMeshPrimary { path: fp.path.clone(), name: fp.name.clone() });
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
            primary_admin: self.node_type_primary(&m.name, "node-admin").map(|n| n.node_id.clone()),
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
            meshes: self.meshes.iter().filter_map(|m| self.mesh_view(&m.name)).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn node(name: PathName, mesh: &str, r#type: &str, status: NodeStatus, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name, mesh, r#type);
        n.status = status;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    fn mn() -> Topology {
        use NodeStatus::Ready as R;
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![
                node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", R, true, true),
                node(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", R, false, false),
                node(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", R, true, false),
                node(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", R, false, false),
                node(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", R, false, false),
            ],
        }
    }

    /// CONTRACT: selection follows the admitted ID and incarnation across a rename;
    /// the replacement at the original location is never substituted for that birth.
    // @feature: node-records
    #[test]
    fn admitted_subject_survives_rename_and_rejects_another_incarnation() {
        let mut view = mn();
        let location = view.nodes[0].location();
        let incarnation = IncarnationId::mint();
        view.nodes[0].incarnation_id = Some(incarnation.clone());
        let id = view.nodes[0].node_id.clone();
        let op = crate::build::BuildOperation::DeleteLiveNode { path: location.path.clone(), name: location.name.clone(), mesh: "mesh1".into(), r#type: "node-admin".into(), exited: None, birth: Some(crate::build::PinnedBirth { mesh: "mesh1".into(), r#type: "node-admin".into(), node_id: id.clone(), incarnation: incarnation.clone() }) };
        let subject = op.subject().unwrap();
        { let at = PathName::new(&location.path, format!("{}-old", location.name)).unwrap(); view.nodes[0].path = at.path; view.nodes[0].name = at.name; }
        let mut successor = Node::allocated(location.clone(), "mesh1", "node-admin");
        successor.incarnation_id = Some(IncarnationId::mint());
        let successor_id = successor.node_id.clone();
        view.nodes.push(successor);
        assert_eq!(view.node_subject(&subject).map(|n| &n.node_id), Some(&id));
        assert_eq!(view.node_subject(&rdm_core::OperationSubject::Node(successor_id.clone())).map(|n| &n.node_id), Some(&successor_id));
        assert_eq!(crate::executor::role_move(&op, &view).map(|m| m.primary.node_id), Some(id.clone()));
        let updates = crate::build_drive::preceding_updates(&op);
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
        let admin1: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let admin2: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        assert!(!t.first_admin_of(&admin1, "mesh1", "node-admin"), "admin.2 is live: admin.1 is not the first");
        t.nodes[1].status = Dead;
        assert!(t.first_admin_of(&admin1, "mesh1", "node-admin"), "admin.1's own live birth does not count against it: no other node-admin lives");
        assert!(!t.first_admin_of(&admin2, "mesh1", "node-admin"), "admin.1 is live: admin.2 is not the first");
        t.nodes[0].status = Dead;
        assert!(t.first_admin_of(&admin1, "mesh1", "node-admin") && t.first_admin_of(&admin2, "mesh1", "node-admin"), "no node-admin lives: either create is the first");
        t.nodes[0].status = R;
        t.nodes.retain(|n| n.location() != admin2);
        assert!(!t.first_admin_of(&admin2, "mesh1", "node-admin"), "a path the view holds nothing at, beside a live admin.1: not the first");
        assert!(!t.first_admin_of(&rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), "a node that is not a node-admin is never a mesh's first admin");
    }

    #[test]
    fn settled_mn_is_converged_with_one_primary_per_node_type() {
        let t = mn();
        assert_eq!(t.violations(), vec![]);
        assert_eq!(t.node_type_primary("mesh1", "node-admin").unwrap().location(), PathName::new("mesh1.node-admin", "1").unwrap());
        assert_eq!(t.node_type_primary("mesh1", "node").unwrap().location(), PathName::new("mesh1.node", "1").unwrap());
        assert_eq!(t.fabric_primary().unwrap().location(), PathName::new("mesh1.node-admin", "1").unwrap());
    }

    #[test]
    fn split_and_missing_node_type_primaries_are_named() {
        let mut t = mn();
        t.nodes[3].is_primary = true; // second rpc primary
        t.nodes[0].is_primary = false; // node-admin type loses its primary
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::NoNodePrimary { mesh: "mesh1".into(), r#type: "node-admin".into() }), "{v:?}");
        assert!(
            v.contains(&TopologyViolation::SplitNodePrimary {
                mesh: "mesh1".into(),
                r#type: "node".into(),
                primaries: vec![rdm_core::PathName::new("mesh1.node", "1").unwrap(), rdm_core::PathName::new("mesh1.node", "2").unwrap()],
            }),
            "{v:?}"
        );
        assert!(v.contains(&TopologyViolation::FabricPrimaryNotMeshPrimary { path: "mesh1.node-admin".into(), name: "1".into() }), "{v:?}");
        assert_eq!(t.node_type_primary("mesh1", "node"), None, "a split node type has no single primary");
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
        assert!(v.contains(&TopologyViolation::FabricPrimaryNotAdmin { path: "mesh1.node".into(), name: "1".into() }), "{v:?}");
        let mut t = mn();
        t.nodes[0].is_fabric_primary = false;
        assert!(t.violations().contains(&TopologyViolation::FabricPrimaryCount(0)));
    }

    /// CONTRACT: a primary marked silent after the election that named it is a view that has not
    /// converged, and until the next election it is still the primary the view names, `mesh.get`'s
    /// `primary_admin` included. Must NOT happen: the readers dropping a silent primary,
    /// so the mesh shows no primary while its primary record names one.
    #[test]
    fn a_silent_primary_is_unconverged_and_still_the_named_primary() {
        let mut t = mn();
        t.nodes[0].status = NodeStatus::PendingReconnect;
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::PrimaryNotLive { path: "mesh1.node-admin".into(), name: "1".into() }), "{v:?}");
        assert!(v.contains(&TopologyViolation::NoNodePrimary { mesh: "mesh1".into(), r#type: "node-admin".into() }), "{v:?}");
        assert_eq!(t.node_type_primary("mesh1", "node-admin").map(|n| n.node_id.clone()), Some(t.nodes[0].node_id.clone()), "the silent primary is still the one named");
        assert_eq!(t.mesh_view("mesh1").and_then(|m| m.primary_admin), Some(t.nodes[0].node_id.clone()), "mesh.get names the silent primary");
    }

    /// CONTRACT: `fabric.get` and `mesh.get` name the live primary node-admin by its node id, and
    /// the name moves with the primary, never to a hidden map. RDM carries no contact field (Q402,
    /// Q403): a node's contact is one of its named listeners, read with `node.get`.
    #[test]
    fn views_name_the_live_owning_admin() {
        let t = mn();
        let f = t.fabric_view();
        assert_eq!(f.fabric_primary.as_ref(), Some(&t.nodes[0].node_id));
        assert_eq!(f.meshes.len(), 1);
        assert_eq!(f.meshes[0].primary_admin.as_ref(), Some(&t.nodes[0].node_id));
        assert_eq!(f.meshes[0].nodes.len(), 5);
        let id = t.meshes[0].id.clone().unwrap().to_string();
        assert_eq!(t.mesh_view(&id), t.mesh_view("mesh1"));
        // Control moves with the primary, never to a hidden map.
        let mut t = mn();
        t.nodes[0].is_primary = false;
        t.nodes[0].is_fabric_primary = false;
        t.nodes[1].is_primary = true;
        t.nodes[1].is_fabric_primary = true;
        let f = t.fabric_view();
        assert_eq!(f.fabric_primary.as_ref(), Some(&t.nodes[1].node_id));
        assert_eq!(f.meshes[0].primary_admin.as_ref(), Some(&t.nodes[1].node_id));
        assert!(t.is_converged());
    }

    #[test]
    fn structural_mismatches_are_named() {
        let mut explicit = mn();
        { let at = rdm_core::PathName::new("elsewhere.gateway", "north").unwrap(); explicit.nodes[2].path = at.path; explicit.nodes[2].name = at.name; }
        assert!(explicit.is_converged(), "a free path/name does not select mesh or kind: {:?}", explicit.violations());
        let mut t = mn();
        t.nodes.push(t.nodes[4].clone());
        t.nodes[2].mesh = "mesh9".into();
        let v = t.violations();
        assert!(v.contains(&TopologyViolation::DuplicateNode { path: "mesh1.node".into(), name: "3".into() }), "{v:?}");
        assert!(v.contains(&TopologyViolation::NodeInUnknownMesh { path: "mesh1.node".into(), name: "1".into(), mesh: "mesh9".into() }), "{v:?}");
    }
}
