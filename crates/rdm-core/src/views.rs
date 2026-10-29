//! What the reads answer (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"): `node.get` answers [`NodeView`]s, `mesh.get` [`MeshView`]s, `fabric.get` a
//! [`FabricView`], each as the answering node-admin's view holds it, and `node.connection.get` a
//! node's held connections, a [`ConnectionsView`]. They travel on op `0x1B`
//! ([`crate::status::StatusReply::Nodes`] and the rest). Postcard is positional: a field's place
//! and a variant's place are the wire format, pinned by `tests/status_wire.rs`.

use crate::{FabricId, GossipStats, IncarnationId, MeshId, NodeId, NodeKind, NodeLoad, PathName};
use serde::{Deserialize, Serialize};

/// A node's lifecycle state as its node-admin's view holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Born and not yet ready.
    ///
    /// Position 0 on the wire; serialized as `pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Position 1 on the wire; serialized as `ready`.
    Ready,
    /// Connections-deleting: it takes no new work while it finishes what it holds.
    ///
    /// Position 2 on the wire; serialized as `connections-deleting`.
    #[serde(rename = "connections-deleting")]
    ConnectionsDeleting,
    /// Leaving: it has announced its departure and is closing.
    ///
    /// Position 3 on the wire; serialized as `leaving`.
    Leaving,
    /// Unheard past the staleness floor: held, never death, and never a reason to restart or
    /// delete it; its next digest flips it back.
    ///
    /// Position 4 on the wire; serialized as `pending-reconnect`.
    PendingReconnect,
    /// Reserved position: the removed restart's status (Q183; node.restart and its NodeRestarting
    /// notice are removed). Never set; kept so every later status keeps its position.
    ///
    /// Position 5 on the wire; serialized as `reserved5`.
    Reserved5,
    /// True offline: the mesh primary's connect found no path on two ladders a staleness floor
    /// apart. Inferred by an observer, never announced by the node.
    ///
    /// Position 6 on the wire; serialized as `dead`.
    Dead,
    /// Deleted: identity and name released; only the node's row carries it.
    ///
    /// Position 7 on the wire; serialized as `deleted`.
    Deleted,
    /// Starting: from a stopped node's acknowledgement of its start (a create's from its launch) until
    /// its join is accepted (states.md "Who sets each state", Q233). Appended after `Deleted`, as are
    /// the four below: no variant before them moves.
    ///
    /// Position 8 on the wire; serialized as `starting`.
    Starting,
    /// Connections-deleted: its node.connection.delete { All } completed `Established`.
    ///
    /// Position 9 on the wire; serialized as `connections-deleted`.
    ConnectionsDeleted,
    /// Stopping: its stop's admission arrived.
    ///
    /// Position 10 on the wire; serialized as `stopping`.
    Stopping,
    /// Stopped: a live, stopped process.
    ///
    /// Position 11 on the wire; serialized as `stopped`.
    Stopped,
    /// Deleting: node.delete's claim was adopted.
    ///
    /// Position 12 on the wire; serialized as `deleting`.
    Deleting,
    /// Left: its node.mesh.leave completed (Q205); off its mesh, its endpoint bound. Appended after
    /// `Deleting`: no variant before it moves.
    ///
    /// Position 13 on the wire; serialized as `left`.
    Left,
}

/// The lifecycle state of a mesh or of a fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopeStatus {
    /// Created and not yet ready.
    ///
    /// Position 0 on the wire; serialized as `pending`; `name()` is `pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Position 1 on the wire; serialized as `ready`; `name()` is `ready`.
    Ready,
    /// Leaving: a mesh whose shutdown was accepted; it is not selected for new work.
    ///
    /// Position 2 on the wire; serialized as `leaving`; `name()` is `leaving`.
    Leaving,
    /// Dead: a mesh every one of whose births was proven exited; final.
    ///
    /// Position 3 on the wire; serialized as `dead`; `name()` is `dead`.
    Dead,
    /// Degraded: the fabric primary has decided a peer mesh is re-born.
    ///
    /// Position 4 on the wire; serialized as `degraded`; `name()` is `degraded`.
    Degraded,
}

/// The deployment provider that runs a fabric's nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Each node is an OS process.
    ///
    /// Position 0 on the wire.
    Process,
    /// Each node is a container.
    ///
    /// Position 1 on the wire.
    Container,
}

/// The durable `data-dir-lost` fact on a birth's status row (R-FM2): the inherited data dir was
/// absent for `RDM_DATA_DIR_GONE_FLOORS` staleness floors while the provider reported the birth
/// `Exited`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirLost {
    /// The absent data dir path.
    pub path: String,
    /// When the absent data dir was first observed, ms since the Unix epoch.
    pub since: u64,
}

/// The durable `data-dir-unreadable` fact on a birth's status row (R-FM2): the inherited data dir
/// exists on disk, but `node-key` is missing, unreadable, or corrupt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirUnreadable {
    /// The broken data dir path.
    pub path: String,
    /// Why `node-key` could not be loaded from the data dir.
    pub cause: String,
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
    /// A stopped node with a live process, `leaving`, waiting for a start or a delete.
    #[serde(default)]
    pub stopped: bool,
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
    /// The durable `data-dir-lost` fact on this birth's status row (R-FM2), when the inherited
    /// data dir was absent for `RDM_DATA_DIR_GONE_FLOORS` staleness floors while `Exited`.
    #[serde(default)]
    pub data_dir_lost: Option<DataDirLost>,
    /// The durable `data-dir-unreadable` fact on this birth's status row (R-FM2), when the
    /// inherited data dir exists on disk with a missing, unreadable, or corrupt `node-key`.
    #[serde(default)]
    pub data_dir_unreadable: Option<DataDirUnreadable>,
    /// This exact birth's immutable app declaration.
    #[serde(default)]
    pub types: std::collections::BTreeSet<String>,
    /// Types this birth joined, including members and peers on a node-admin.
    #[serde(default)]
    pub held: std::collections::BTreeSet<String>,
    /// Canonical held-set hash carried by this birth's digest.
    #[serde(default)]
    pub held_hash: [u8; 32],
    /// The birth's member cert as its newest cert row holds it (fabric-certs.md §Tracking): `None`
    /// when no row names it (an admin configured with no certs issues none).
    #[serde(default)]
    pub cert: Option<NodeCertView>,
    /// The executable this birth runs, as its node row records it (proposal
    /// executable-bindings-at-create.md rule 7): read from the row, never from gossip; `None` for a row
    /// that records none (written before the field).
    #[serde(default)]
    pub executable: Option<crate::binding::Binding>,
    /// The node's `storage_persistence` (storage.md §1), as its row records it from its create spec:
    /// what becomes of the node's own data dir across its births and once it is gone. `None` for a row
    /// that records none.
    #[serde(default)]
    pub storage_persistence: Option<crate::meta::StorageMeta>,
    /// The `topology_storage_provider` the node writes (storage.md §2), as its row records it: the
    /// fact, beside the fabric's intent on the Fabric row. `None` for a row that records none.
    #[serde(default)]
    pub topology_storage_provider: Option<String>,
}

/// A birth's member cert as `node.get` surfaces it (fabric-certs.md §Tracking: `cert { issued_at,
/// expires_at, issuer_expires_at }`), each rafka-time ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCertView {
    /// When the cert the row names was issued.
    pub issued_at: u64,
    /// When it expires.
    pub expires_at: u64,
    /// When its issuing cert expires.
    pub issuer_expires_at: u64,
}

/// A mesh whose member renewals stall (fabric-certs.md §Tracking: a row past `renew_at` by more than
/// a floor with no `node.cert.renewed`), surfaced on `fabric.get` as `renewals: stalled`, the way
/// `held-running` is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Renewals {
    /// At least one birth's renewal is past its point by more than a floor with no receipt.
    ///
    /// Position 0 on the wire.
    Stalled {
        /// The births whose renewals stall.
        nodes: Vec<PathName>,
    },
}

impl NodeView {
    /// The node's path (`<mesh>.<kind>`), separate from its name within that path.
    pub fn path(&self) -> String {
        self.name.path()
    }

    /// The node's name within its path (`<base>[.old]`).
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
    /// The mesh's member renewals, when they stall (fabric-certs.md §Tracking).
    #[serde(default)]
    pub renewals: Option<Renewals>,
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
    /// The next member cert expiry across the fabric's cert rows, rafka-time ms (fabric-certs.md
    /// §Tracking); `None` while no row names one.
    #[serde(default)]
    pub next_expiry: Option<u64>,
    /// The fabric's `topology_storage_provider` (storage.md §2), as its Fabric row names it: the
    /// registered name of the backend RDM's five record stores are written to. `None` while the
    /// answering admin holds no Fabric row.
    #[serde(default)]
    pub topology_storage_provider: Option<String>,
    /// Each provider the fabric left, in order, as its Fabric row names them (storage.md §2).
    #[serde(default)]
    pub previous_topology_storage_providers: Vec<String>,
    /// The root rotation as the Fabric row's roots row names it (fabric-ca-renew.md): its phase, the
    /// old anchor's bound, and whether that bound passed with the rotation not retired. `None` while no
    /// rotation was accepted.
    #[serde(default)]
    pub roots: Option<RootsView>,
    /// The fabric CA as the Fabric row's CA row names it: its expiry. `None` while no CA renewal
    /// wrote the row.
    #[serde(default)]
    pub ca: Option<CaView>,
}

/// `fabric.get`'s `roots { phase, expires_at }` (fabric-ca-renew.md "fabric.root.update, the caller
/// op"): a rotation not retired by the old anchor's bound is `stalled`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootsView {
    /// The rotation's phase.
    pub phase: crate::status::RootsPhase,
    /// The old anchor's bound, rafka-time ms.
    pub expires_at: u64,
    /// The bound passed and the rotation is not retired (`roots: stalled`).
    pub stalled: bool,
}

/// `fabric.get`'s `ca { expires_at }` (node-rpc-envelope.md, fabric.root.update's row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaView {
    /// When the fabric CA expires, rafka-time ms.
    pub expires_at: u64,
}

/// The fabric-primary's atlas (`fabric.atlas.get`; docs/plans/proposal/fabric-atlas.md):
/// one row for every node the current Build's topology names, in `path.name` order. Each row's birth
/// is the one its node row names current, and its lifecycle is what the ops left it: the
/// fabric-primary's own (what it accepted and the receipts its attempts returned) and the effects the
/// node row records of any admin's op. Gossip is context only (`observed`) and decides nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasView {
    /// The rows, one per node, in `path.name` order.
    pub nodes: Vec<AtlasNodeView>,
}

impl AtlasView {
    /// The row of the node at `name`, when the atlas holds one.
    pub fn node(&self, name: &str) -> Option<&AtlasNodeView> {
        self.nodes.iter().find(|n| n.name.to_string() == name)
    }
}

/// One node of the [`AtlasView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasNodeView {
    /// The node's `path.name`.
    pub name: PathName,
    /// The minted id of the birth the node row names current.
    pub node_id: NodeId,
    /// The incarnation of that birth, once the row records one.
    pub incarnation_id: Option<IncarnationId>,
    /// The birth's lifecycle state as the ops left it.
    pub status: NodeStatus,
    /// A stopped birth with a live process (a completed node.stop), waiting for a start or a delete.
    pub stopped: bool,
    /// The operation key (`node.stop:mesh2.rpc.1`) of the command accepted on the node whose attempt
    /// has not completed; `None` once its receipts are in.
    pub accepted: Option<String>,
    /// Context only: the status the answering admin's gossip view holds for the same birth, when it
    /// holds it. Never read by acceptance.
    pub observed: Option<NodeStatus>,
}

/// One connection fact a node holds (`node.connection.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionView {
    /// The entry's minted id: an open entry's id names its connection to `node.connection.delete`.
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

/// The connection facts one node holds (`node.connection.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionsView {
    /// The answering node's `path.name`, the name it holds.
    pub node: String,
    /// Whether the node holds the complete set.
    pub complete: bool,
    /// The facts.
    pub connections: Vec<ConnectionView>,
}
