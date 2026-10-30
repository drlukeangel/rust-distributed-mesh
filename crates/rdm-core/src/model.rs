//! Fabric → Mesh → Node model (i143 PRD §1, §13; ownership §4, §5.1).
//!
//! Every identity is opaque and compared by equality only. Incarnation ids
//! are never ordered (PRD §22).

use serde::{Deserialize, Serialize};

// Identity, path and port types are the Mesh EF's (rafka-mesh-entity);
// node-admin uses them, it does not redefine them.
pub use crate::path::is_valid_mesh_name;
pub use crate::{DeploymentId, EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName, PathNameError};

/// Node lifecycle status as published on views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Created and not yet ready.
    ///
    /// Serialized as `pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Serialized as `ready`.
    Ready,
    /// Connections-deleting: it takes no new work.
    ///
    /// Serialized as `connections-deleting`.
    ConnectionsDeleting,
    /// Leaving: it has announced its departure.
    ///
    /// Serialized as `leaving`.
    Leaving,
    /// Unheard past the staleness floor (`RDM_STALENESS_MS`, 30 s): this node's own pruner marks
    /// the silent peer in place and keeps it; its next digest flips it back (node-identity-and-state.md
    /// §7.3, i77 PRD row 18). Never death, and never a reason to restart or delete it.
    ///
    /// Serialized as `pending-reconnect`.
    PendingReconnect,
    /// Reserved position: the removed restart's status (Q183; node.restart and its NodeRestarting
    /// notice are removed). Never set; kept so every later status keeps its position.
    ///
    /// Serialized as `reserved5`.
    Reserved5,
    /// True offline: half a floor after the mark the mesh primary's one tickle ladder reached nobody,
    /// and its inspection and the recovery on its verdict follow at once (node-recovery.md steps 2-3).
    /// Observer inferred only; never sent, never itself the reason to act: the verdict is.
    ///
    /// Serialized as `dead`.
    Dead,
    /// Deleted: the node's identity and name are released. Only the node's row carries it, written
    /// by the node's mesh-primary (the fabric-primary for a mesh with no primary) from node.delete's
    /// step receipt; the record and its config stay for the next node created at the same `path.name`.
    ///
    /// Serialized as `deleted`.
    Deleted,
    /// Withdrawn: the create that claimed the name was withdrawn before its node ran (R-SR2). Only the
    /// node's record carries it, appended where the record was once removed, and a name claim wins
    /// over it as over a `Deleted` one. Appended last: no variant before it moves.
    ///
    /// Serialized as `withdrawn`.
    Withdrawn,
    /// Starting: the node's row from a stopped node's `CommandAccepted` on its start's `StartNode`
    /// stream (a create's from its launch, where no process can acknowledge) until its join is
    /// accepted (states.md "Who sets each state", Q233). A row status the node's mesh-primary writes;
    /// appended after `Withdrawn`, as are the four below: no variant before them moves.
    ///
    /// Serialized as `starting`.
    Starting,
    /// Connections-deleted: the node's row once its node.connection.delete { All } completed
    /// `Established`.
    ///
    /// Serialized as `connections-deleted`.
    ConnectionsDeleted,
    /// Stopping: the node's row once its stop's `CommandAccepted` admission frame arrived.
    ///
    /// Serialized as `stopping`.
    Stopping,
    /// Stopped: the node's row once the matching stopped frame arrived: a live, stopped process.
    ///
    /// Serialized as `stopped`.
    Stopped,
    /// Deleting: the node's row once node.delete's claim was adopted (published as NodeDeleting); the
    /// frames of the stop the delete composes move the row on from it (Q233).
    ///
    /// Serialized as `deleting`.
    Deleting,
    /// Left: the node's row once its node.mesh.leave's completion arrived (`MeshLeft`, or `StopMeshLeft`
    /// on its stop's stream; Q205), the node off its mesh with its endpoint bound. Appended after
    /// `Deleting`: no variant before it moves.
    ///
    /// Serialized as `left`.
    Left,
}

impl NodeStatus {
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
    /// The fabric primary has decided a peer mesh is re-born (`investigate`).
    ///
    /// Serialized as `degraded`; `name()` is `degraded`.
    Degraded,
}

impl ScopeStatus {
    /// The name the control API and the spans carry.
    pub fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Leaving => "leaving",
            Self::Dead => "dead",
            Self::Degraded => "degraded",
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
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's kind.
    pub kind: NodeKind,
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

/// A node read without the fact held is not selected (states.md "Routing": selection needs the
/// held ready of the birth and its mesh).
fn routable_default() -> bool {
    false
}

impl Node {
    /// A newly allocated node: identity only, nothing realised yet.
    pub fn allocated(name: PathName) -> Self {
        Self {
            kind: name.kind,
            mesh: name.mesh.clone(),
            name,
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

    /// The node's path (`<mesh>.<kind>`), separate from its name within that path.
    pub fn path(&self) -> String {
        self.name.path()
    }

    /// The node's name within its path (`<base>[.old]`).
    pub fn node_name(&self) -> String {
        self.name.name()
    }

    /// The node's `node_type`.
    pub fn node_type(&self) -> &str {
        self.name.node_type()
    }

    /// Set the node's `node_type` at creation (`node.create`), normalizing an empty string to
    /// [`crate::status::DEFAULT_NODE_TYPE`] (`"node"`) and refusing any value
    /// outside the shared identifier grammar by name (`node_type`).
    pub fn with_node_type(self, node_type: &str) -> Result<Self, String> {
        let normalized = crate::status::normalize_node_type(node_type);
        if let Some(reason) = crate::status::node_type_refusal(&normalized) {
            return Err(reason);
        }
        Ok(self)
    }

    /// Attempt to update `node_type` after creation (`node.update`): always refused by name
    /// (`node_type` is immutable once set at `node.create`).
    pub fn update_node_type(&mut self, requested_node_type: &str) -> Result<(), String> {
        Err(crate::status::node_type_update_refusal(self.node_type(), requested_node_type))
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
        for s in ["mesh1.rpc.2", "mesh1.admin.1", "m-2.rpc.17"] {
            let p: PathName = s.parse().unwrap();
            assert_eq!(p.to_string(), s);
            let json = serde_json::to_string(&p).unwrap();
            assert_eq!(serde_json::from_str::<PathName>(&json).unwrap(), p);
        }
        let p: PathName = "mesh1.rpc.2".parse().unwrap();
        assert_eq!(p, PathName::new("mesh1", NodeKind::RpcNode, 2));
    }

    #[test]
    fn path_name_refuses_by_name() {
        assert_eq!("mesh1.rpc".parse::<PathName>(), Err(PathNameError::Shape("mesh1.rpc".into())));
        assert_eq!("Mesh1.rpc.1".parse::<PathName>(), Err(PathNameError::MeshName("Mesh1".into())));
        assert_eq!("mesh1.registry.1".parse::<PathName>(), Err(PathNameError::Kind("registry".into())));
        assert_eq!("mesh1.rpc.0".parse::<PathName>(), Err(PathNameError::Ordinal("0".into())));
        assert_eq!("mesh1.rpc.01".parse::<PathName>(), Err(PathNameError::Ordinal("01".into())));
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
        assert_eq!(serde_json::to_value(crate::MemberStatus::ConnectionsDeleting).unwrap(), "connections-deleting");
        assert_eq!(serde_json::from_str::<crate::views::NodeStatus>("\"connections-deleting\"").unwrap(), crate::views::NodeStatus::ConnectionsDeleting);
        assert_eq!(serde_json::from_str::<crate::MemberStatus>("\"connections-deleting\"").unwrap(), crate::MemberStatus::ConnectionsDeleting);
        assert_eq!(postcard::to_allocvec(&crate::views::NodeStatus::ConnectionsDeleting).unwrap(), [2], "the view enum position stands");
        assert_eq!(postcard::to_allocvec(&crate::MemberStatus::ConnectionsDeleting).unwrap(), [2], "the projection enum position stands");
        for (status, spelled, at) in [(NodeStatus::Starting, "starting", 9u8), (NodeStatus::ConnectionsDeleted, "connections-deleted", 10), (NodeStatus::Stopping, "stopping", 11), (NodeStatus::Stopped, "stopped", 12), (NodeStatus::Deleting, "deleting", 13)] {
            assert_eq!(serde_json::to_value(status).unwrap(), spelled, "the row status is spelled as states.md names it");
            assert_eq!(postcard::to_allocvec(&status).unwrap(), [at], "appended after Withdrawn: no position moves");
        }
        assert_eq!(serde_json::to_value(NodeKind::RpcNode).unwrap(), "rpc_node");
        assert_eq!(serde_json::to_value(NodeKind::NodeAdmin).unwrap(), "node_admin");
        let n = Node::allocated("mesh1.rpc.2".parse().unwrap());
        let v = serde_json::to_value(&n).unwrap();
        assert_eq!(v["name"], "mesh1.rpc.2");
        assert_eq!(v["kind"], "rpc_node");
        assert_eq!(v["mesh"], "mesh1");
        assert_eq!(v["status"], "pending");
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
