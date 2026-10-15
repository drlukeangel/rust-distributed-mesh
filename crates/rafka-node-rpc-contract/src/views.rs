//! What the reads answer (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"): `node.get` answers [`NodeView`]s, `mesh.get` [`MeshView`]s, `fabric.get` a
//! [`FabricView`], each as the answering node-admin's view holds it, and `node.connections.get` a
//! node's held connections, a [`ConnectionsView`]. They travel on op `0x1B`
//! ([`crate::status::StatusReply::Nodes`] and the rest). Postcard is positional: a field's place
//! and a variant's place are the wire format, pinned by `tests/status_wire.rs`.

use rafka_mesh_entity::{FabricId, GossipStats, IncarnationId, MeshId, NodeId, NodeKind, NodeLoad, PathName};
use serde::{Deserialize, Serialize};

/// A node's lifecycle state as its node-admin's view holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Born and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work while it finishes what it holds.
    Draining,
    /// Leaving: it has announced its departure and is closing.
    Leaving,
    /// Unheard past the staleness floor: held, never death, and never a reason to restart or
    /// delete it; its next digest flips it back.
    PendingReconnect,
    /// Commanded silence: its mesh executor is restarting this exact birth and owns bringing it
    /// back.
    Restarting,
    /// True offline: the mesh primary's connect found no path on two rounds a staleness floor
    /// apart. Inferred by an observer, never announced by the node.
    Dead,
    /// Deleted: identity and name released; only the node's row carries it.
    Deleted,
}

/// The lifecycle state of a mesh or of a fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Leaving: a mesh whose shutdown was accepted; it is not selected for new work.
    Leaving,
    /// Dead: a mesh every one of whose births was proven exited; final.
    Dead,
    /// Degraded: the fabric primary has decided a peer mesh is reborn.
    Degraded,
    /// A fabric state: every mesh is ready-for-traffic and the application does its work.
    StateSync,
    /// A fabric state: the commit-state and open-traffic rounds run.
    StateCommit,
}

/// The deployment provider that runs a fabric's nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Each node is an OS process.
    Process,
    /// Each node is a container.
    Container,
}

/// The durable `held-running` fact on a birth's status row (R-FM1): the provider reported its
/// exact runtime `Running` while the birth is silent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldRunning {
    /// When the first `Running` verdict was observed, ms since the Unix epoch.
    pub since: u64,
    /// How many `Running` verdicts have been observed for this birth during the hold.
    pub verdicts: u64,
}

/// One node as the answering node-admin's view holds it (`node.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's kind.
    pub kind: NodeKind,
    /// The name of the mesh the node belongs to.
    pub mesh: String,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's fabric endpoint id, once known.
    pub endpoint_id: Option<String>,
    /// The incarnation of the node's current birth, once launched.
    pub incarnation_id: Option<IncarnationId>,
    /// The deployment that runs the node, once created.
    pub deployment_id: Option<String>,
    /// The provider that runs the node.
    pub provider: Option<ProviderKind>,
    /// The node's data directory.
    pub data_dir: Option<String>,
    /// The node's lifecycle state.
    pub status: NodeStatus,
    /// Whether the node holds its mesh's primary seat.
    pub is_primary: bool,
    /// Whether the node holds the fabric-primary seat.
    pub is_fabric_primary: bool,
    /// The control API base the node advertises, for a node-admin.
    pub admin_api_base: Option<String>,
    /// The address the node's mesh transport is bound to.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// The named listeners the node serves, each with its bound address.
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The lifecycle state the birth declared to its authority, once applied.
    #[serde(default)]
    pub declared: Option<String>,
    /// Stopped and parked: `leaving` with a live process, waiting for a start or a delete.
    #[serde(default)]
    pub parked: bool,
    /// The node's CPU and RAM from its latest digest, as the answering admin holds it.
    #[serde(default)]
    pub load: Option<NodeLoad>,
    /// The node's mesh-channel counts from its latest digest: heard, neighbours, frames.
    #[serde(default)]
    pub gossip: Option<GossipStats>,
    /// The node's tag labels (R-TG1): caller-owned meta set through `node.update`, read from the
    /// node's row at its `path.name`, never from its digest.
    #[serde(default)]
    pub tags: std::collections::BTreeSet<String>,
    /// The node's config (R-TG1, node-rpc-envelope.md "Meta and config"): the application's property
    /// bag, a property of the node (node.get answers the whole node), as the node's digest carries
    /// the live copy. RDM never reads it.
    #[serde(default)]
    pub config: std::collections::BTreeMap<String, String>,
    /// The durable `held-running` fact on this birth's status row (R-FM1), when the provider
    /// reported its exact runtime `Running` while silent.
    #[serde(default)]
    pub held_running: Option<HeldRunning>,
}

impl NodeView {
    /// The node's path (`<mesh>.<kind>`), separate from its name within that path.
    pub fn path(&self) -> String {
        self.name.path()
    }

    /// The node's name within its path (`<base>[.old|.new]`).
    pub fn node_name(&self) -> String {
        self.name.name()
    }

    /// The node's `node_type`.
    pub fn node_type(&self) -> &'static str {
        self.name.node_type()
    }
}

/// One mesh as the answering node-admin's view holds it (`mesh.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshView {
    /// The mesh's minted id, once a member of the mesh has said which id it carries.
    pub id: Option<MeshId>,
    /// The mesh's name.
    pub name: String,
    /// The mesh's lifecycle state.
    pub status: ScopeStatus,
    /// The node-admin that is the mesh's primary, when one is seated.
    pub primary_admin: Option<PathName>,
    /// The control API base of the mesh's primary.
    pub admin_api_base: Option<String>,
    /// The `path.name` of every node of the mesh.
    pub nodes: Vec<PathName>,
}

/// The fabric as the answering node-admin's view holds it (`fabric.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricView {
    /// The fabric's minted id.
    pub id: FabricId,
    /// The fabric's name.
    pub name: String,
    /// The fabric's lifecycle state.
    pub status: ScopeStatus,
    /// The provider that runs the fabric's nodes.
    pub provider: ProviderKind,
    /// The node-admin that is the fabric primary, when one is seated.
    pub fabric_primary: Option<PathName>,
    /// The control API base of the fabric primary.
    pub admin_api_base: Option<String>,
    /// The fabric's meshes.
    pub meshes: Vec<MeshView>,
    /// `Fabric.build_id` (node-identity-and-state.md §2.8, a field of the fabric moved only by the
    /// fabric-primary): the Build whose topology the fabric accepted, as the answering node-admin's
    /// fabric record holds it; `None` while it holds no accepted Build.
    pub build_id: Option<String>,
    /// The durable `held-running` fact on the fabric-primary's status row (R-FM1), when watching
    /// admins observed its exact runtime `Running` while silent.
    #[serde(default)]
    pub held_running: Option<HeldRunning>,
}

/// One connection fact a node holds (`node.connections.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionView {
    /// The entry's minted id: an open entry's id names its connection to `node.connections.delete`.
    pub id: String,
    /// The source node's `path.name`.
    pub source: String,
    /// The destination node's `path.name`.
    pub destination: String,
    /// `direct` or `proxy`.
    pub kind: String,
    /// `connected`, `disconnected` or `failed`.
    pub state: String,
    /// The carrier's `path.name`, for a proxy.
    #[serde(default)]
    pub carrier: Option<String>,
    /// Why the connection is in its state, when known.
    #[serde(default)]
    pub reason: Option<String>,
    /// When the fact was observed, in rafka-time milliseconds.
    #[serde(default)]
    pub logged_at_ms: u64,
}

/// The connection facts one node holds (`node.connections.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionsView {
    /// The answering node's `path.name`, the name it holds.
    pub node: String,
    /// Whether the node holds the complete set.
    pub complete: bool,
    /// The facts.
    pub connections: Vec<ConnectionView>,
}
