//! What the reads answer (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"): `node.get` answers [`NodeView`]s, `mesh.get` [`MeshView`]s, `fabric.get` a
//! [`FabricView`], each as the answering node-admin's view holds it, and `node.connection.get` a
//! node's held connections, a [`ConnectionsView`]. They travel on op `0x1B`
//! ([`crate::status::StatusReply::Nodes`] and the rest). Postcard is positional: a field's place
//! and a variant's place are the wire format, pinned by `tests/status_wire.rs`.

use crate::{FabricId, GossipStats, IncarnationId, MeshId, NodeId, NodeLoad, PathName};
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

/// One node as `node.get` answers it: the node record's fields under its names (Q366), each from the
/// node's record at its node id, and, when the answering node is the node itself, its own live sample
/// (`load`, `gossip`), never stored and never carried on gossip (Q394). A node's mesh is the first
/// location of its `path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's path; its first location is its mesh.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The node's type.
    pub r#type: String,
    /// The node's incarnation, once launched.
    pub incarnation: Option<IncarnationId>,
    /// The node's lifecycle state.
    pub state: NodeStatus,
    /// The node's fabric endpoint id, once bound.
    pub endpoint_id: Option<String>,
    /// The address the node's mesh transport is bound to.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// The named listeners the node serves, each with its bound address.
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The runtime the node runs in, once launched.
    pub runtime: Option<crate::RuntimeFact>,
    /// Whether the node is its type's primary in its mesh (a node-admin's: the mesh-primary).
    pub is_primary: bool,
    /// Whether the node is the fabric-primary.
    pub is_fabric_primary: bool,
    /// The node's member cert dates (fabric-certs.md §Tracking); `None` when it holds none.
    pub cert: Option<crate::certs::CertDates>,
    /// A node-admin's signing cert dates; `None` for every other node.
    pub signing_cert: Option<crate::certs::SigningCertDates>,
    /// The node's own storage (storage.md §1).
    pub storage: crate::meta::Storage,
    /// The executable the node runs; `None` when its create named none.
    pub executable: Option<crate::binding::Binding>,
    /// The node's config (R-TG1, node-rpc-envelope.md "Meta and config"): the application's property
    /// bag. RDM never reads it.
    pub config: std::collections::BTreeMap<String, String>,
    /// The node's tag labels (R-TG1).
    pub tags: std::collections::BTreeSet<String>,
    /// The node's CPU and RAM, its own sample: present only on the row of the node that answers the
    /// read ([`own_sample_on`], Q394).
    pub load: Option<NodeLoad>,
    /// The node's mesh-channel counts (heard, neighbours, frames), its own sample: present only on the
    /// row of the node that answers the read ([`own_sample_on`], Q394).
    pub gossip: Option<GossipStats>,
}

/// `node_id`'s own load and mesh-channel counts on its own row of `nodes`, and on no other: what the
/// node that answers a `node.get` serves of itself (Q394; node-identity-and-state.md §2.5: in-memory
/// diagnostics of the node itself, served only for the node that answers).
pub fn own_sample_on(nodes: &mut [NodeView], node_id: &NodeId, load: NodeLoad, gossip: GossipStats) {
    for n in nodes.iter_mut().filter(|n| n.node_id == *node_id) {
        (n.load, n.gossip) = (Some(load), Some(gossip));
    }
}

impl NodeView {
    /// The node `node_id` of `r#type` at `name` in `state`, every other field absent or empty: what an
    /// answer fills field by field.
    pub fn new(name: &crate::PathName, r#type: &str, node_id: NodeId, state: NodeStatus) -> Self {
        Self {
            node_id,
            path: name.path.clone(),
            name: name.name.clone(),
            r#type: r#type.to_string(),
            incarnation: None,
            state,
            endpoint_id: None,
            transport_addr: None,
            listeners: Vec::new(),
            runtime: None,
            is_primary: false,
            is_fabric_primary: false,
            cert: None,
            signing_cert: None,
            storage: Default::default(),
            executable: None,
            config: Default::default(),
            tags: Default::default(),
            load: None,
            gossip: None,
        }
    }

    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> crate::PathName { crate::PathName { path: self.path.clone(), name: self.name.clone() } }

    /// The node's mesh: the first location of its path.
    pub fn mesh(&self) -> String {
        crate::path::mesh_of(&self.path)
    }
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
        self.path.clone()
    }

    /// The node's name within its path (it changes only by node.name.update).
    pub fn node_name(&self) -> String {
        self.name.clone()
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
    /// The node-admin that is the mesh's primary, when one is elected.
    pub primary_admin: Option<NodeId>,
    /// The globally unique ID of every node of the mesh.
    pub nodes: Vec<NodeId>,
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
    /// The node-admin that is the fabric primary, when one is elected.
    pub fabric_primary: Option<NodeId>,
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
    /// old anchor's bound, and whether that bound passed with the rotation not updated. `None` while no
    /// rotation was accepted.
    #[serde(default)]
    pub roots: Option<RootsView>,
    /// The fabric CA as the Fabric row's CA row names it: its expiry. `None` while no CA renewal
    /// wrote the row.
    #[serde(default)]
    pub ca: Option<CaView>,
}

/// `fabric.get`'s `roots { phase, expires_at }` (fabric-ca-renew.md "fabric.root.update, the caller
/// op"): a rotation not updated by the old anchor's bound is `stalled`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootsView {
    /// The rotation's phase.
    pub phase: crate::status::RootsPhase,
    /// The old anchor's bound, rafka-time ms.
    pub expires_at: u64,
    /// The bound passed and the rotation is not updated (`roots: stalled`).
    pub stalled: bool,
}

/// `fabric.get`'s `ca { expires_at }` (node-rpc-envelope.md, fabric.root.update's row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaView {
    /// When the fabric CA expires, rafka-time ms.
    pub expires_at: u64,
}

/// The fabric-primary's atlas (`fabric.atlas.get`; docs/plans/proposal/fabric-atlas.md):
/// one row for every node the current Build's topology names, in `(path, name)` order. Each row's birth
/// is the one its node row names current, and its lifecycle is what the ops left it: the
/// fabric-primary's own (what it accepted and the step events its runs returned) and the effects the
/// node row records of any admin's op. Gossip is context only (`observed`) and decides nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasView {
    /// The rows, one per node, in `(path, name)` order.
    pub nodes: Vec<AtlasNodeView>,
}

impl AtlasView {
    /// The row of the node at `name`, when the atlas holds one.
    pub fn node(&self, node_id: &NodeId) -> Option<&AtlasNodeView> {
        self.nodes.iter().find(|n| n.node_id == *node_id)
    }
}

/// One node of the [`AtlasView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasNodeView {
    /// The node's path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The minted id of the birth the node row names current.
    pub node_id: NodeId,
    /// The incarnation of that birth, once the row records one.
    pub incarnation_id: Option<IncarnationId>,
    /// The birth's lifecycle state as the ops left it.
    pub status: NodeStatus,
    /// A stopped birth with a live process (a completed node.stop), waiting for a start or a delete.
    pub stopped: bool,
    /// The operation key (`node.stop:mesh2.node.1`) of the command accepted on the node that has not
    /// completed; `None` once it completes.
    pub accepted: Option<crate::OperationId>,
    /// Context only: the status the answering admin's gossip view holds for the same birth, when it
    /// holds it. Never read by acceptance.
    pub observed: Option<NodeStatus>,
}

impl AtlasNodeView {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> crate::PathName { crate::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// One connection fact a node holds (`node.connection.get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionView {
    /// The entry's minted id: an open entry's id names its connection to `node.connection.delete`.
    pub id: String,
    /// The source node's globally unique ID.
    pub source: String,
    /// The destination node's globally unique ID.
    pub destination: String,
    /// `direct` or `proxy`.
    pub kind: String,
    /// `connected`, `disconnected` or `failed`.
    pub state: String,
    /// The carrier's globally unique node ID, for a proxy.
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
    /// The answering node's globally unique ID.
    pub node: String,
    /// Whether the node holds the complete set.
    pub complete: bool,
    /// The facts.
    pub connections: Vec<ConnectionView>,
}
