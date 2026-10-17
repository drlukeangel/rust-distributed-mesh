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
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work.
    Draining,
    /// Leaving: it has announced its departure.
    Leaving,
    /// Unheard past the staleness floor (`RDM_STALENESS_MS`, 30 s): this node's own pruner marks
    /// the silent peer in place and keeps it; its next digest flips it back (node-identity-and-state.md
    /// §7.3, i77 PRD row 18). Never death, and never a reason to restart or delete it.
    PendingReconnect,
    /// Commanded silence: its mesh executor is restarting this exact birth (`NodeRestarting`,
    /// node-identity-and-state.md) and owns bringing it back. Held through its `Leaving` and
    /// silence, exempt from the staleness marks, never routable; the later birth's own digest
    /// clears it.
    Restarting,
    /// True offline: half a floor after the mark the mesh primary's one QUIC connect found no path,
    /// on two rounds at least one staleness floor apart. Observer inferred only; never sent, never a
    /// reason to restart or delete the node.
    Dead,
    /// Deleted: the node's identity and name are released. Only the node's row carries it, written
    /// by the fabric-primary from the exit proof; the record and its config stay for the next node
    /// created at the same `path.name`.
    Deleted,
}

impl NodeStatus {
    /// A node that may hold or contend for a cohort primary.
    pub fn is_live(self) -> bool {
        matches!(self, Self::Pending | Self::ReadyForTraffic | Self::Draining)
    }
}

/// Mesh and Fabric lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Leaving: a mesh whose shutdown was accepted; it is not selected for new work.
    Leaving,
    /// Dead: a mesh every one of whose births was proven exited; final, its id never reissued.
    Dead,
    /// The fabric primary has decided a peer mesh is reborn (`investigate`).
    Degraded,
    /// A fabric state: every mesh is ready-for-traffic and the application does its day-0 and seed
    /// work (fabric-state-sync.md).
    StateSync,
    /// A fabric state: the commit-state round publishes what the scratchpads hold
    /// (fabric-state-commit.md); the open-traffic round runs inside it.
    StateCommit,
}

impl ScopeStatus {
    /// The name the control API and the spans carry.
    pub fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ReadyForTraffic => "ready-for-traffic",
            Self::Leaving => "leaving",
            Self::Dead => "dead",
            Self::Degraded => "degraded",
            Self::StateSync => "state-sync",
            Self::StateCommit => "state-commit",
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
    /// Whether the node holds its cohort's primary seat.
    pub is_primary: bool,
    /// Whether the node holds the fabric-primary seat.
    pub is_fabric_primary: bool,
    /// The control API base a node-admin serves; `None` for other kinds.
    pub admin_api_base: Option<String>,
    /// The one address of the birth's Iroh endpoint; `None` until it is born.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// Non-Iroh listeners the birth binds, by name.
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// May application routing select this node (states.md "Routing"): only while it is
    /// ready-for-traffic and its mesh is ready-for-traffic; anything else is a routing stop. The
    /// resolver and the Node RPC client never read it.
    #[serde(default = "routable_default")]
    pub routable: bool,
    /// The lifecycle state this birth declared to its authority and the authority applied
    /// (`status_rpc`); `None` until it declares. Not `status`: that is what membership hears.
    #[serde(default)]
    pub declared: Option<String>,
    /// Stopped and parked: `Leaving` with a live process, waiting for a start or a delete.
    #[serde(default)]
    pub parked: bool,
    /// The provider's proof that this exact birth exited is recorded: its exit-proof row was read
    /// (`Records::read_exits`), so the birth is `Dead` and gone from every seat. `false` for a
    /// `Dead` the observer inferred (true offline), which proves nothing (elections §4.1: only the
    /// provider's exit proof vacates a seat).
    #[serde(default)]
    pub exit_proven: bool,
    /// The operating system holds this exact birth's runtime suspended, as its mesh-primary recorded
    /// it (deployment-provider.md §2.5: the `os-stopped { since }` status-row fact): read for a silent
    /// birth only, in the view's read phase (`Records::read_os_stopped`), since an os-stopped runtime
    /// answers nothing. `None` for a birth that is heard. What node.get, node.delete's door and the
    /// plan read of it: an os-stopped birth may be deleted; a birth that is only silent may not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os_stopped: Option<crate::views::OsStopped>,
    /// The node's config as its digest carries it: the application's own key/values, opaque to RDM.
    #[serde(default)]
    pub config: std::collections::BTreeMap<String, String>,
}

/// A node read without the fact held is not selected (states.md "Routing": selection needs the
/// held ready-for-traffic of the birth and its mesh).
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
            // Nothing realised yet: not ready-for-traffic, so not selected.
            routable: false,
            declared: None,
            parked: false,
            exit_proven: false,
            os_stopped: None,
            config: Default::default(),
        }
    }

    /// The node's path (`<mesh>.<kind>`), separate from its name within that path.
    pub fn path(&self) -> String {
        self.name.path()
    }

    /// The node's name within its path (`<base>[.old|.new]`).
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
        assert_eq!(serde_json::to_value(NodeStatus::ReadyForTraffic).unwrap(), "ready-for-traffic");
        assert_eq!(serde_json::to_value(NodeStatus::Draining).unwrap(), "connections-deleting");
        assert_eq!(serde_json::to_value(crate::MemberStatus::Draining).unwrap(), "connections-deleting");
        assert_eq!(serde_json::from_str::<NodeStatus>("\"connections-deleting\"").unwrap(), NodeStatus::Draining);
        assert_eq!(serde_json::from_str::<crate::MemberStatus>("\"connections-deleting\"").unwrap(), crate::MemberStatus::Draining);
        assert_eq!(postcard::to_allocvec(&NodeStatus::Draining).unwrap(), [2], "the view enum position stands");
        assert_eq!(postcard::to_allocvec(&crate::MemberStatus::Draining).unwrap(), [2], "the projection enum position stands");
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
    fn only_pending_ready_and_draining_are_live() {
        assert!(NodeStatus::ReadyForTraffic.is_live());
        assert!(NodeStatus::Draining.is_live());
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
