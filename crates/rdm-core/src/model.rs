//! Fabric → Mesh → Node model (i143 PRD §1, §13; ownership §4, §5.1).
//!
//! Every identity is opaque and compared by equality only. Incarnation ids
//! are never ordered (PRD §22).

use serde::{Deserialize, Serialize};

// Identity, path and port types are the Mesh EF's (rafka-mesh-entity);
// node-admin uses them, it does not redefine them.
pub use crate::path::is_valid_mesh_name;
pub use crate::{DeploymentId, EndpointId, FabricId, IncarnationId, MeshId, NodeId, PathName, PathNameError};

/// Node lifecycle status as published on views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Created and not yet ready.
    ///
    /// Position 0 on the wire; serialized as `pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Position 1 on the wire; serialized as `ready`.
    Ready,
    /// Connections-deleting: it takes no new work.
    ///
    /// Position 2 on the wire; serialized as `connections-deleting`.
    ConnectionsDeleting,
    /// Leaving: it has announced its departure.
    ///
    /// Position 3 on the wire; serialized as `leaving`.
    Leaving,
    /// Under its mesh-primary's self-heal (Q393): unheard past the staleness floor (`RDM_STALENESS_MS`,
    /// 30 s), held silent by the pruner, and written on its record by its mesh-primary as the self-heal
    /// starts; its rejoin (pending, then ready) ends it (node-recovery.md step 1). Never death, and never
    /// a reason to restart or delete it: the investigation's verdict is.
    ///
    /// Position 4 on the wire; serialized as `pending-reconnect`.
    PendingReconnect,
    /// Reserved position: the removed restart's status (Q183; node.restart and its NodeRestarting
    /// notice are removed). Never set; kept so every later status keeps its position.
    ///
    /// Position 5 on the wire; serialized as `reserved5`.
    Reserved5,
    /// Dead: an investigation of the node concluded in failure (Q393: `exited`, not proven by the end
    /// of the retries, or a join or start unanswered at the end of its attempts), written on its record
    /// by its mesh-primary; its recovery follows at once (node-recovery.md step 3). The node never
    /// publishes it.
    ///
    /// Position 6 on the wire; serialized as `dead`.
    Dead,
    /// Deleted: the node's identity and name are released. Only the node's row carries it, written
    /// by the node's mesh-primary (the fabric-primary for a mesh with no primary) from node.delete's
    /// step receipt; the record and its config stay for the next node created at the same path and name.
    ///
    /// Position 7 on the wire; serialized as `deleted`.
    Deleted,
    /// Withdrawn: the create that claimed the name was withdrawn before its node ran (R-SR2). Only the
    /// node's record carries it, appended where the record was once removed, and a name claim wins
    /// over it as over a `Deleted` one. Appended last: no variant before it moves.
    ///
    /// Position 8 on the wire; serialized as `withdrawn`.
    Withdrawn,
    /// Starting: the node's row from a stopped node's `CommandAccepted` on its start's `StartNode`
    /// stream (a create's from its launch, where no process can acknowledge) until its join is
    /// accepted (states.md "Who sets each state", Q233). A row status the node's mesh-primary writes;
    /// appended after `Withdrawn`, as are the four below: no variant before them moves.
    ///
    /// Position 9 on the wire; serialized as `starting`.
    Starting,
    /// Connections-deleted: the node's row once its node.connection.delete { All } completed
    /// `Established`.
    ///
    /// Position 10 on the wire; serialized as `connections-deleted`.
    ConnectionsDeleted,
    /// Stopping: the node's row once its stop's `CommandAccepted` admission frame arrived.
    ///
    /// Position 11 on the wire; serialized as `stopping`.
    Stopping,
    /// Stopped: the node's row once the matching stopped frame arrived: a live, stopped process.
    ///
    /// Position 12 on the wire; serialized as `stopped`.
    Stopped,
    /// Deleting: the node's row once node.delete's claim was adopted (published as NodeDeleting); the
    /// frames of the stop the delete composes move the row on from it (Q233).
    ///
    /// Position 13 on the wire; serialized as `deleting`.
    Deleting,
    /// Left: the node's row once its node.mesh.leave's completion arrived (`MeshLeft`, or `StopMeshLeft`
    /// on its stop's stream; Q205), the node off its mesh with its endpoint bound. Appended after
    /// `Deleting`: no variant before it moves.
    ///
    /// Position 14 on the wire; serialized as `left`.
    Left,
}

impl NodeStatus {
    /// Whether a node at `self` moves to `to`: the one order its record and its Digest follow. A
    /// `Deleted` or `Withdrawn` node stands. A start of a stopped node (`Starting`) and a new join
    /// (`Pending`, from any status but `Deleting`) begin its next cycle. `Deleting` is the
    /// mesh-primary's, from node.delete's admission, over any status (states.md). A stop's
    /// acknowledgement (`Stopping`) sets stopping over any status but `Stopped`. The mesh-primary writes
    /// `PendingReconnect` as its self-heal of the node starts, over a live status only (starting,
    /// pending, ready), and `Dead` when an investigation of it concludes in failure, over any status but
    /// deleting, deleted, withdrawn and dead (Q393); the rejoin moves `PendingReconnect` to pending, then
    /// ready. Every other status only moves forward along node.stop's frames (node-stop.md steps 1-4,
    /// Q205: starting, pending, ready, deleting, stopping, connections-deleting, connections-deleted,
    /// leaving, left, stopped).
    pub fn moves_to(self, to: NodeStatus) -> bool {
        use NodeStatus as S;
        fn order(s: S) -> u8 {
            match s {
                S::Starting => 0,
                S::Pending => 1,
                S::Ready | S::PendingReconnect | S::Dead | S::Reserved5 => 2,
                S::Deleting => 3,
                S::Stopping => 4,
                S::ConnectionsDeleting => 5,
                S::ConnectionsDeleted => 6,
                S::Leaving => 7,
                S::Left => 8,
                S::Stopped => 9,
                S::Deleted | S::Withdrawn => 10,
            }
        }
        match to {
            _ if matches!(self, S::Deleted | S::Withdrawn) => false,
            S::Starting => matches!(self, S::Stopped | S::Leaving),
            S::Pending => self != S::Deleting,
            S::Deleting => true,
            S::Stopping => self != S::Stopped,
            S::PendingReconnect => matches!(self, S::Starting | S::Pending | S::Ready),
            S::Dead => !matches!(self, S::Deleting | S::Deleted | S::Withdrawn | S::Dead),
            _ => order(to) > order(self),
        }
    }

    /// A node that may hold or contend for a node-type primary.
    pub fn is_live(self) -> bool {
        matches!(self, Self::Pending | Self::Ready | Self::ConnectionsDeleting)
    }
}

/// Mesh and Fabric lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    /// Created and not yet ready.
    ///
    /// Serialized as `pending`; `name()` is `pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Serialized as `ready`; `name()` is `ready`.
    Ready,
    /// Leaving: a mesh whose shutdown was accepted; it is not selected for new work.
    ///
    /// Serialized as `leaving`; `name()` is `leaving`.
    Leaving,
    /// Dead: a mesh every one of whose births was proven exited; final, its id never reissued.
    ///
    /// Serialized as `dead`; `name()` is `dead`.
    Dead,
}

impl ScopeStatus {
    /// The name the control API and the spans carry.
    pub fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Leaving => "leaving",
            Self::Dead => "dead",
        }
    }
}

/// Deployment provider kind, fabric policy (PRD §1.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Each node is an OS process.
    Process,
    /// Each node is a container.
    Container,
}

/// One node as the fabric control projection knows it (`NodeView`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// The exact birth's original claimed name, at its unchanged path, while listed as a moved
    /// predecessor.
    #[serde(default)]
    pub renamed_from: Option<String>,
    /// The node's path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The node's immutable type, set at creation, independent of its path and name.
    pub r#type: String,
    /// The name of the node's mesh.
    pub mesh: String,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's fabric endpoint id, once known.
    pub endpoint_id: Option<EndpointId>,
    /// The incarnation of the node's current birth, once launched.
    pub incarnation_id: Option<IncarnationId>,
    /// The deployment that runs the node, once created.
    pub deployment_id: Option<DeploymentId>,
    /// The provider that runs the node.
    pub provider: Option<ProviderKind>,
    /// The node's data directory.
    pub data_dir: Option<String>,
    /// The node's lifecycle state.
    pub status: NodeStatus,
    /// Whether the node holds its node-type primary role.
    pub is_primary: bool,
    /// Whether the node holds the fabric-primary role.
    pub is_fabric_primary: bool,
    /// The control API base a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    /// The one address of the birth's Iroh endpoint; `None` until it is born.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// Non-Iroh listeners the birth binds, by name.
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// May application routing select this node (states.md "Routing"): only while it is
    /// ready and its mesh is ready; anything else is a routing stop. The
    /// resolver and the Node RPC client never read it.
    #[serde(default = "routable_default")]
    pub routable: bool,
    /// The lifecycle state this birth declared to its authority and the authority applied
    /// (`status_rpc`); `None` until it declares. Not `status`: that is what the topology view hears.
    #[serde(default)]
    pub declared: Option<String>,
    /// Stopped: `Leaving` with a live process, waiting for a start or a delete.
    #[serde(default)]
    pub stopped: bool,
}

impl Node {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> crate::PathName { crate::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// A node read without the fact held is not selected (states.md "Routing": selection needs the
/// held ready of the birth and its mesh).
fn routable_default() -> bool {
    false
}

impl Node {
    /// The exact runtime birth, selected by globally unique node ID and incarnation.
    pub fn birth(&self) -> Option<crate::framing::Birth> {
        self.incarnation_id.clone().map(|incarnation| crate::framing::Birth { node_id: self.node_id.clone(), incarnation })
    }

    /// A newly allocated node: identity only, nothing realised yet.
    pub fn allocated(name: PathName, mesh: impl Into<String>, r#type: &str) -> Self {
        Self {
            renamed_from: None,
            r#type: r#type.to_string(),
            mesh: mesh.into(),
            path: name.path.clone(), name: name.name.clone(),
            node_id: NodeId::mint(),
            endpoint_id: None,
            incarnation_id: None,
            deployment_id: None,
            provider: None,
            data_dir: None,
            status: NodeStatus::Pending,
            is_primary: false,
            is_fabric_primary: false,
            admin_api_base: None,
            transport_addr: None,
            listeners: Vec::new(),
            // Nothing realised yet: not ready, so not selected.
            routable: false,
            declared: None,
            stopped: false,
        }
    }

    /// The node's immutable path, separate from its name and explicit mesh and type.
    pub fn path(&self) -> String {
        self.path.clone()
    }

    /// The node's name within its path (it changes only by node.name.update).
    pub fn node_name(&self) -> String {
        self.name.clone()
    }

    /// Whether the node is a node-admin.
    pub fn is_node_admin(&self) -> bool {
        crate::meta::is_node_admin(&self.r#type)
    }

    /// Set the node's `type` at creation (`node.create`), normalizing an empty string to `node` and
    /// refusing any value outside the shared identifier grammar by name (`type`).
    pub fn with_type(mut self, r#type: &str) -> Result<Self, String> {
        let normalized = crate::status::normalize_type(r#type);
        if let Some(reason) = crate::status::type_refusal(&normalized) {
            return Err(reason);
        }
        self.r#type = normalized;
        Ok(self)
    }

    /// Attempt to update `type` after creation (`node.update`): always refused by name (`type` is
    /// immutable once set at `node.create`).
    pub fn update_type(&mut self, requested: &str) -> Result<(), String> {
        Err(crate::status::type_update_refusal(&self.r#type, requested))
    }
}

/// A mesh as the fabric record holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mesh {
    /// `None` until a member of the mesh has said which id it carries.
    pub id: Option<MeshId>,
    /// The mesh's name.
    pub name: String,
    /// The mesh's lifecycle state.
    pub status: ScopeStatus,
}

/// The fabric record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fabric {
    /// The logical Fabric's identity, kept for its lifetime; the name is its label.
    pub id: FabricId,
    /// The fabric's name.
    pub name: String,
    /// The fabric's lifecycle state.
    pub status: ScopeStatus,
    /// The provider that runs the fabric's nodes.
    pub provider: ProviderKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_name_round_trips() {
        for (path, name) in [("mesh1.node", "2"), ("mesh1.node-admin", "1"), ("m-2.node", "17")] {
            let p = PathName::new(path, name).unwrap();
            assert_eq!((&*p.path, &*p.name), (path, name));
            let json = serde_json::to_string(&p).unwrap();
            assert_eq!(serde_json::from_str::<PathName>(&json).unwrap(), p);
        }
        let p: PathName = crate::PathName::new("mesh1.node", "2").unwrap();
        assert_eq!(p, PathName::numbered("mesh1", "node", 2));
    }

    #[test]
    fn path_name_refuses_invalid_fields_and_accepts_free_text() {
        assert!(PathName::new("Mesh1.node", "1").is_err());
        assert!(PathName::new("mesh1..rpc", "1").is_err());
        assert!(PathName::new("mesh1.node", "a.b").is_err());
        for (path, name) in [("mesh1.registry", "1"), ("mesh1.node", "0"), ("mesh1.node", "01"), ("arbitrary.path", "north")] {
            let location = PathName::new(path, name).unwrap();
            assert_eq!((&*location.path, &*location.name), (path, name));
        }
    }

    #[test]
    fn minted_ids_are_distinct_and_opaque() {
        let a = IncarnationId::mint();
        let b = IncarnationId::mint();
        assert_ne!(a, b);
        assert_eq!(a.to_string().len(), 12, "an incarnation id is a 12-character Crockford id");
    }

    #[test]
    fn view_spelling_matches_the_design_contract() {
        assert_eq!(serde_json::to_value(NodeStatus::Ready).unwrap(), "ready");
        assert_eq!(serde_json::to_value(crate::views::NodeStatus::ConnectionsDeleting).unwrap(), "connections-deleting");
        assert_eq!(serde_json::to_value(crate::model::NodeStatus::ConnectionsDeleting).unwrap(), "connections-deleting");
        assert_eq!(serde_json::from_str::<crate::views::NodeStatus>("\"connections-deleting\"").unwrap(), crate::views::NodeStatus::ConnectionsDeleting);
        assert_eq!(serde_json::from_str::<crate::model::NodeStatus>("\"connections-deleting\"").unwrap(), crate::model::NodeStatus::ConnectionsDeleting);
        assert_eq!(postcard::to_allocvec(&crate::views::NodeStatus::ConnectionsDeleting).unwrap(), [2], "the view enum position stands");
        assert_eq!(postcard::to_allocvec(&crate::model::NodeStatus::ConnectionsDeleting).unwrap(), [2], "the projection enum position stands");
        for (status, spelled, at) in [(NodeStatus::Starting, "starting", 9u8), (NodeStatus::ConnectionsDeleted, "connections-deleted", 10), (NodeStatus::Stopping, "stopping", 11), (NodeStatus::Stopped, "stopped", 12), (NodeStatus::Deleting, "deleting", 13)] {
            assert_eq!(serde_json::to_value(status).unwrap(), spelled, "the row status is spelled as states.md names it");
            assert_eq!(postcard::to_allocvec(&status).unwrap(), [at], "appended after Withdrawn: no position moves");
        }
        let n = Node::allocated(crate::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node");
        let v = serde_json::to_value(&n).unwrap();
        assert_eq!((&v["path"], &v["name"]), (&serde_json::json!("mesh1.node"), &serde_json::json!("2")), "path and name are two fields");
        assert_eq!(v["type"], "node");
        assert!(v.get("kind").is_none() && v.get("node_type").is_none(), "one field names the type: {v}");
        assert_eq!(v["mesh"], "mesh1");
        assert_eq!(v["status"], "pending");
    }

    /// CONTRACT (Q393; lead note 0001 C2: "`PendingReconnect` from Starting | Pending | Ready; `Dead`
    /// from every status but Deleting, Deleted, Withdrawn and Dead. PendingReconnect -> Pending -> Ready
    /// (the rejoin) already moves"): the record order takes the mesh-primary's two investigation
    /// statuses in exactly those arms. Must NOT happen: `PendingReconnect` over a lifecycle state the node
    /// reported (stopping, stopped, ...), `Dead` over a deleting or deleted node, or a rejoin that
    /// cannot leave `PendingReconnect`.
    // @feature: node-investigation
    #[test]
    fn the_mesh_primary_writes_pending_reconnect_over_a_live_status_and_dead_over_any_but_a_deleting_one() {
        use NodeStatus as S;
        let all = [S::Starting, S::Pending, S::Ready, S::ConnectionsDeleting, S::ConnectionsDeleted, S::Leaving, S::Left, S::Stopping, S::Stopped, S::PendingReconnect, S::Reserved5, S::Dead, S::Deleting, S::Deleted, S::Withdrawn];
        for from in all {
            assert_eq!(from.moves_to(S::PendingReconnect), matches!(from, S::Starting | S::Pending | S::Ready), "{from:?} -> PendingReconnect");
            assert_eq!(from.moves_to(S::Dead), !matches!(from, S::Deleting | S::Deleted | S::Withdrawn | S::Dead), "{from:?} -> Dead");
        }
        assert!(S::PendingReconnect.moves_to(S::Pending) && S::Pending.moves_to(S::Ready), "the rejoin: pending, then ready");
        assert!(S::Dead.moves_to(S::Deleting), "the recovery's delete follows dead");
        assert!(!S::PendingReconnect.moves_to(S::Ready), "a rejoin passes through pending");
    }

    #[test]
    fn only_pending_ready_and_connections_deleting_are_live() {
        assert!(NodeStatus::Ready.is_live());
        assert!(NodeStatus::ConnectionsDeleting.is_live());
        assert!(!NodeStatus::Leaving.is_live());
        assert!(!NodeStatus::PendingReconnect.is_live());
    }
}

/// A fabric's durable identity: its Crockford ULID and its human-readable name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricIdentity {
    /// The fabric's Crockford ULID.
    pub fabric_id: FabricId,
    /// The fabric's name.
    pub name: String,
}

#[cfg(test)]
mod type_tests {
    use super::*;

    // @feature: node-records
    /// CONTRACT (i150 R1): a node's type is stored as its own field; changing the
    /// path/name cannot change that type, and a later type update is refused.
    #[test]
    fn a_nodes_type_is_independent_of_the_path() {
        let mut node = Node::allocated(crate::PathName::new("mesh1.node", "north").unwrap(), "mesh1", "node").with_type("application-worker").unwrap();
        assert_eq!(node.r#type, "application-worker");
        { let at = crate::PathName::new("mesh2.gateway", "south").unwrap(); node.path = at.path; node.name = at.name; }
        assert_eq!(node.r#type, "application-worker");
        assert!(node.update_type("gateway").unwrap_err().contains("application-worker"));
        let encoded = serde_json::to_vec(&node).unwrap();
        let decoded: Node = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.r#type, "application-worker");
        assert_eq!(Node::allocated(node.location(), "mesh1", "node").with_type("").unwrap().r#type, crate::meta::NODE);
    }
}
