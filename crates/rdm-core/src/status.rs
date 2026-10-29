//! The lifecycle/status declaration protocol on op `0x1B`, RDM's control family (the core
//! families stay exactly Ping and Forward) (node-rpc.md §38; node-rpc-envelope.md §3; i143.e6.s7).
//!
//! A node or a primary declares a committed state upward to its authority and gets the typed
//! result of applying it. Gossip stays the dissemination plane; this is certainty that one
//! declared state was applied by the authority that holds it. There is no transition id:
//! an operation-named node effect is idempotent by `(birth, build_id, attempt, operation)` (Q49),
//! held across cycle begins and forgotten only with the birth. Without an operation,
//! idempotency is the natural key of the thing declared — `(NodeId, IncarnationId, status)`,
//! `(MeshId, status)`, `(FabricId, event)` — and the entity's current state is the result.
//!
//! The sender is never read from the request: the receiver takes it from the authenticated
//! `PeerContext` and resolves it through its own view. Who may declare what:
//!
//! ```text
//! DeclareNodeState   the subject itself (ordinary node -> its mesh-primary;
//!                    node-admin -> the fabric-primary)
//! DeclareMeshState   the mesh's current primary -> the fabric-primary
//! ApplyMeshState     the fabric-primary -> the exact bootstrap admin of that mesh (Pending)
//! ApplyFabricEvent   the fabric-primary -> a mesh primary
//! ```
//!
//! The reads (`node.get`, `mesh.get`, `fabric.get`, `node.connections.get`) ride the same op until
//! the node, mesh and fabric families have op numbers of their own: a caller asks, the receiver
//! answers from what it holds, and nothing changes. Their answers are [`crate::views`].
//!
//! Legal moves run forward along `Pending -> Ready -> ConnectionsDeleting -> Stopped` (and
//! `Pending -> Ready -> Leaving -> Dead` for a Mesh), skips included; the same
//! state again is `AlreadyApplied`, a backward move `RejectedInvalidNodeTransition` or `RejectedInvalidMeshTransition`. A declaration
//! for a birth the receiver holds a newer incarnation of, or holds as deleted, is
//! `RejectedStaleIncarnation`. A receiver that is not the authority, or a sender that is not the
//! subject or its primary, is `RejectedNotPrimary`, the reply naming which.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::views::{ConnectionsView, FabricView, MeshView, NodeStatus, NodeView};
use crate::{ConnectionId, FabricId, IncarnationId, MeshId, NodeId, RuntimeFact, CROCKFORD60_LEN};
use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};

/// The Status protocol: declare, apply and probe lifecycle state.
pub struct Status;

/// A node birth's lifecycle state, as declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NodeState {
    /// Born and not yet ready.
    ///
    /// Position 0 on the wire; serialized as `Pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Position 1 on the wire; serialized as `Ready`.
    Ready,
    /// Connections-deleting: it takes no new work.
    ///
    /// Position 2 on the wire; serialized as `ConnectionsDeleting`.
    ConnectionsDeleting,
    /// Stopped: declared at node.stop's end, after its connections are deleted and its transport cut
    /// (node-stop.md step 4); "in the system with its identity kept, receiving no traffic, silent on
    /// gossip and not advancing" (states.md, Stopped nodes).
    ///
    /// Position 3 on the wire; serialized as `Stopped`.
    Stopped,
}

/// A Mesh's status, as declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MeshState {
    /// Created and not yet ready.
    ///
    /// Position 0 on the wire; serialized as `Pending`.
    Pending,
    /// Ready: it takes traffic.
    ///
    /// Position 1 on the wire; serialized as `Ready`.
    Ready,
    /// Leaving: it is closing.
    ///
    /// Position 2 on the wire; serialized as `Leaving`.
    Leaving,
    /// Dead: every member is gone.
    ///
    /// Position 3 on the wire; serialized as `Dead`.
    Dead,
}

/// A Fabric event the fabric-primary applies at a mesh primary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FabricEvent {
    /// The fabric is ready.
    ///
    /// Position 0 on the wire.
    Ready,
    /// A fabric shutdown was initiated by the named admin.
    ///
    /// Position 1 on the wire.
    ShutdownInitiated {
        /// The admin that initiated the shutdown.
        initiated_by: String,
    },
}

impl FabricEvent {
    /// The event's name as it appears in spans and replies.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::ShutdownInitiated { .. } => "shutdown-initiated",
        }
    }
}

/// The operation that produced a declared state (i146 Q36): the Build, its attempt and the
/// operation's idempotency key, as the command that produced the state named them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredOperation {
    /// The Build the operation belongs to.
    pub build_id: String,
    /// The attempt of the Build.
    pub attempt: u32,
    /// The operation's idempotency key (`node.stop:mesh1.rpc.1`).
    pub operation: String,
}

/// A Status call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusRequest {
    /// Upward: the sender's own birth declares the lifecycle state it committed.
    ///
    /// Position 0 on the wire.
    DeclareNodeState {
        /// The declaring birth's logical node.
        node_id: NodeId,
        /// The declaring birth's incarnation.
        incarnation: IncarnationId,
        /// The state the birth committed.
        state: NodeState,
        /// The operation that produced the state, when one did (a stop's `Leaving`, a start's
        /// `Pending` and `Ready`); `None` for a state the birth measured itself.
        operation: Option<DeclaredOperation>,
    },
    /// Upward: the mesh's current primary declares the Mesh's state to the fabric-primary.
    ///
    /// Position 1 on the wire.
    DeclareMeshState {
        /// The mesh declared.
        mesh_id: MeshId,
        /// The mesh's state.
        state: MeshState,
        /// The Rafka-time of the change, as the declaring primary's `MeshStatus` carries it: the
        /// instant the mesh entered `state`, which every later primary republishes unchanged
        /// (mesh-investigation.md step 7); 0 when the declarer holds none.
        changed_at_rafka_ms: u64,
    },
    /// Downward: an authority tells the exact birth to enter `state`.
    ///
    /// Position 2 on the wire.
    ApplyNodeState {
        /// The birth told to change state.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The state it is told to enter.
        state: NodeState,
    },
    /// Downward: an authority asks the exact birth to reassert its presence and answer its
    /// current state; no transition.
    ///
    /// Position 3 on the wire.
    ProbeNodeState {
        /// The birth probed.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
    },
    /// Downward: the fabric-primary applies a Mesh state at that mesh's bootstrap admin.
    ///
    /// Position 4 on the wire.
    ApplyMeshState {
        /// The mesh applied to.
        mesh_id: MeshId,
        /// The mesh's name.
        mesh_name: String,
        /// The state applied.
        state: MeshState,
        /// The Rafka-time of the change the fabric-primary holds for `state`: the receiving admin
        /// holds its MeshStatus at that instant and republishes it unchanged (mesh-investigation.md
        /// step 7); 0 when the fabric-primary holds none.
        changed_at_rafka_ms: u64,
    },
    /// Downward: the fabric-primary applies a Fabric event at a mesh primary.
    ///
    /// Position 5 on the wire.
    ApplyFabricEvent {
        /// The fabric the event belongs to.
        fabric_id: FabricId,
        /// The event applied.
        event: FabricEvent,
    },
    /// Reserved position (R-CR4): never sent. A node's connections are deleted by
    /// [`Self::DeleteConnections`]; a receiver refuses this by name.
    ///
    /// Position 6 on the wire.
    Reserved6 {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation.
        operation: String,
    },
    /// Reserved position (R-CR4): never sent; a receiver refuses this by name.
    ///
    /// Position 7 on the wire.
    Reserved7 {
        /// The birth reported.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation.
        operation: String,
    },
    /// Reserved at position 8: refused by name, never executed. Stop and start use NodeCommand 0x22.
    ///
    /// Position 8 on the wire.
    StopNode {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `node.stop:<path>`.
        operation: String,
    },
    /// Reserved: a stopped birth reports `stopped` on the reply of its `StopNode` call, so this call is
    /// never sent and a receiver refuses it by name.
    ///
    /// Position 9 on the wire.
    NodeLeft {
        /// The birth that is leaving.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `node.stop:<path>`.
        operation: String,
    },
    /// Downward: the fabric-primary tells the deleting mesh's primary to run its mesh.delete
    /// (`delete-mesh`; mesh-delete.md). `Applied` / `AlreadyApplied` admit the command; mesh.deleted is
    /// reported only by the `MeshDeleted` call that follows.
    ///
    /// Position 10 on the wire.
    DeleteMesh {
        /// The mesh that leaves.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `mesh.delete:<mesh_id>`.
        operation: String,
    },
    /// Upward: the still-running mesh-primary hands the fabric-primary the proof that every other
    /// member exited and its own exact runtime (`mesh-deleted`). The manifest is a reference to
    /// immutable receipts in the accepted Build's records; the fabric-primary reads and validates
    /// them before it accepts. The primary keeps running: the outside owner stops it next.
    ///
    /// Position 11 on the wire.
    MeshDeleted {
        /// The mesh that leaves.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `mesh.delete:<mesh_id>`.
        operation: String,
        /// The final mesh-primary's node.
        final_node_id: NodeId,
        /// The final mesh-primary's incarnation.
        final_incarnation: IncarnationId,
        /// The final mesh-primary's exact runtime, still running.
        #[serde(with = "crate::wire::runtime")]
        final_runtime: RuntimeFact,
        /// A bounded reference (at most [`MAX_RECEIPT_MANIFEST_BYTES`]) to the other members'
        /// terminal receipts.
        receipt_manifest: String,
    },
    /// Reserved position (Q171): never sent; a receiver refuses this by name.
    ///
    /// Position 12 on the wire.
    Reserved12 {
        /// The fabric the command belongs to.
        fabric_id: FabricId,
        /// The birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build.
        build_id: String,
        /// The Build attempt.
        attempt: u32,
        /// The operation.
        operation: String,
    },
    /// Reserved position (Q171): never sent; a receiver refuses this by name.
    ///
    /// Position 13 on the wire.
    Reserved13 {
        /// The fabric the command belongs to.
        fabric_id: FabricId,
        /// The birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build.
        build_id: String,
        /// The Build attempt.
        attempt: u32,
        /// The operation.
        operation: String,
    },
    /// Reserved position (Q174): never sent; a receiver refuses this by name.
    ///
    /// Position 14 on the wire.
    Reserved14 {
        /// The fabric the command belonged to.
        fabric_id: FabricId,
        /// The birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build.
        build_id: String,
        /// The Build attempt.
        attempt: u32,
        /// The operation.
        operation: String,
    },
    /// Reserved position (Q174): never sent; a receiver refuses this by name.
    ///
    /// Position 15 on the wire.
    Reserved15 {
        /// The fabric the command belonged to.
        fabric_id: FabricId,
        /// The birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build.
        build_id: String,
        /// The Build attempt.
        attempt: u32,
        /// The operation.
        operation: String,
    },
    /// Reserved at position 16: refused by name, never executed. Stop and start use NodeCommand 0x22.
    ///
    /// Position 16 on the wire.
    StartNode {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `node.start:<path>`.
        operation: String,
    },
    /// Upward: a caller asks the fabric-primary to put the exact birth's whole config
    /// (`node.config.update`, node-config-update.md), through a carrier (Forward `0x1A`) when it cannot
    /// reach it directly; the fabric-primary writes the map to the node in its atlas and sends
    /// the command to the node's mesh-primary, the writer of that mesh's rows, which writes the node's
    /// record and tells the node. [`StatusReply::ConfigUpdated`] (`node.config.updated`) once the node
    /// applied it. A single call, never a Build.
    ///
    /// Position 17 on the wire.
    UpdateNodeConfig {
        /// The birth whose config is put.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The whole config: a put replaces, it never merges.
        config: BTreeMap<String, String>,
    },
    /// Downward: the birth's mesh-primary tells the exact birth its config, the row's as written, and
    /// the row's revision; the birth installs it over an older revision (P1) and holds it on its own
    /// entry. Answered `Applied`.
    ///
    /// Position 18 on the wire.
    ApplyNodeConfig {
        /// The birth told.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The whole config.
        config: BTreeMap<String, String>,
        /// The row's revision the config was written at.
        revision: u64,
    },
    /// Downward: the node's mesh-primary, having written the renamed row, tells the exact birth
    /// its name (proposal node-properties-own-commands.md §2.2 step 6; R12: the node owns its name and
    /// publishes only the row's name). The birth holds the name and publishes its digest carrying it,
    /// then answers [`StatusReply::Applied`]; [`StatusReply::AlreadyApplied`] for the name it already
    /// holds; [`StatusReply::BusyHoldingConnections`] naming each connection it still holds, with
    /// nothing held. A stopped birth is sent nothing: its join answer hands it the name.
    ///
    /// Position 19 on the wire.
    ApplyNodeName {
        /// The birth told.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The `path.name` the row now holds.
        name: String,
    },
    /// Upward: `node.name.update` (proposal node-properties-own-commands.md §2; R13, R-CR1): a caller
    /// asks the node's mesh-primary, the only writer of its mesh's rows, through a carrier (Forward
    /// `0x1A`) when it cannot reach it directly, to rename the exact birth. The request carries the
    /// name only, so the path cannot move: the mesh-primary joins the held record's own path and
    /// `name` for the `path.name` index key. It validates from the record it holds, in order: the
    /// exact birth ([`StatusReply::RejectedStaleIncarnation`]), its own writer role
    /// ([`NotPrimary::ReceiverNotPrimary`]), the node admitting no connection
    /// ([`StatusReply::RejectedAdmittingConnections`]), the name grammar ([`StatusReply::RejectedName`]);
    /// then it reads the new name's row and claims it ([`StatusReply::NameHeld`]),
    /// moves the row (put at the new key carrying `renamed_from`, release of the old key), sends a live
    /// birth `ApplyNodeName` (a stopped one nothing) and answers [`StatusReply::NameUpdated`]
    /// (`node.name.updated`): for a stopped node after both writes of the move, for a live one after
    /// the birth answered `Applied` or `AlreadyApplied`. A single call, never a Build.
    ///
    /// Position 20 on the wire.
    UpdateName {
        /// The birth renamed.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The new name, without its path (`2`, `2.old`, `9`).
        name: String,
    },
    /// `node.connections.delete` (R-CR3; node-stop.md step 3 is its `All`): a caller asks the exact
    /// birth's mesh-primary, which commands the birth with the same request and holds its completion
    /// for the birth's rename precondition (R-CR2); the birth itself hard-cuts the connections the
    /// scope names, writing each drop under the name it holds, and answers
    /// [`StatusReply::ConnectionsDeleted`] (`node.connections.deleted`). Its endpoint stays bound and
    /// the call's own stream stays open. An empty `Connections` list is refused by name
    /// ([`StatusReply::RejectedConnectionScope`]).
    ///
    /// Position 21 on the wire.
    DeleteConnections {
        /// The birth whose connections are cut.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// Which connections.
        scope: ConnectionScope,
    },
    /// The node reads (node-rpc-envelope.md "Typed objects mapped to wire and execution owners",
    /// Q165): `node.get { id }`, `mesh.nodes.get { tags? }` and `fabric.nodes.get { tags? }`, one
    /// request: a caller asks a node-admin for the nodes its view holds that `select` selects,
    /// answered [`StatusReply::Nodes`]; an id the view does not hold selects nothing, and the caller's
    /// `node.get` ends `NOT_FOUND`. A read: nothing changes and nothing is published.
    ///
    /// Position 22 on the wire.
    GetNodes {
        /// Which nodes.
        select: NodeSelect,
    },
    /// `mesh.get { name }` (`Some`): a caller asks a node-admin for one mesh its view holds, by the
    /// mesh's minted id or its name; `fabric.meshes.get` (`None`): every mesh (Q165). Answered
    /// [`StatusReply::Meshes`]; a mesh the view does not hold is [`StatusReply::UnknownMesh`]. A read.
    ///
    /// Position 23 on the wire.
    GetMeshes {
        /// The mesh's minted id or name; `None` for every mesh of the fabric.
        mesh: Option<String>,
    },
    /// `fabric.get`: a caller asks a node-admin for the fabric as its view holds it, answered
    /// [`StatusReply::Fabric`]. A read.
    ///
    /// Position 24 on the wire.
    GetFabric,
    /// `node.connections.get` (fabric-mesh-node-lifecycles.md: "Reading or cutting a node's held
    /// connections"): a caller asks the exact birth for the connections it holds as its own, its
    /// latest Direct per destination and its active Proxies, each named by the minted id
    /// `DeleteConnections` names it by, answered [`StatusReply::Connections`]. A read.
    ///
    /// Position 25 on the wire.
    GetConnections {
        /// The birth asked.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
    },
    /// `node.tags.create` (node-rpc-envelope.md node.tags; R-TG1): a caller asks the exact birth's
    /// mesh-primary, the only writer of its mesh's rows, through a carrier (Forward `0x1A`) when it
    /// cannot reach it directly, to add `labels` to the node's tags. Each label is its own key of the
    /// node store's tags index, a blind put: a label the node already carries is ignored and named in
    /// the reply. Answered [`StatusReply::TagsCreated`] (`node.tags.created`) once the row and its
    /// index entries are written; a non-label, an empty list, or labels that would carry the node past
    /// [`MAX_NODE_TAGS`] are [`StatusReply::RejectedTags`] with nothing written. No precondition but
    /// the fabric having first reached ready; the node is told nothing. A single call,
    /// never a Build.
    ///
    /// Position 26 on the wire.
    CreateTags {
        /// The birth whose tags grow.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The labels added: one or a list, at most [`MAX_NODE_TAGS`], each a label ([`tag_refusal`]).
        labels: BTreeSet<String>,
    },
    /// `node.tags.delete` (node-rpc-envelope.md node.tags; R-TG1): as [`StatusRequest::CreateTags`],
    /// removing every label (`All`) or the named ones; each removed label's index key is tombstoned.
    /// Answered [`StatusReply::TagsDeleted`] (`node.tags.deleted`) naming any named label the node
    /// did not carry.
    ///
    /// Position 27 on the wire.
    DeleteTags {
        /// The birth whose tags shrink.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// Which labels.
        scope: TagScope,
    },
    /// Downward: the commanding authority tells the exact birth to join its mesh channel(s)
    /// (`node.mesh.join`; node-mesh-join.md), reporting `JoinNode` back to the commander,
    /// installing the handed name, cert, config and rafka-time, opening the mesh channel
    /// (`WhichMesh::Own`, and `WhichMesh::Backbone` for a node-admin) and publishing its
    /// first `Digest` heartbeat, answered [`StatusReply::MeshJoined`].
    ///
    /// Position 28 on the wire.
    JoinMesh {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `join-mesh:<path.name>` or `node.start:<path.name>`.
        operation: String,
    },
    /// Reserved position (Q205): node.mesh.leave is a node command on NodeCommand 0x22
    /// ([`crate::node_command::NodeCommandRequest::LeaveMesh`]). A receiver refuses this by name and
    /// executes nothing; the position and its fields stay so no later variant moves.
    ///
    /// Position 29 on the wire.
    LeaveMesh {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `leave-mesh:<path.name>` or `node.stop:<path.name>`.
        operation: String,
    },
    /// Downward (`node.cert.renew`; node-cert-renew.md): the birth's issuing authority installs a
    /// fresh member cert on the exact running birth, answered [`StatusReply::CertRenewed`], or
    /// [`StatusReply::RejectedCert`] for a cert that does not decode or names another birth.
    ///
    /// Position 30 on the wire.
    RenewCert {
        /// The birth renewed.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The member cert, as the join carries it (`rdm_core::certs::cert_to_bytes`).
        cert: Vec<u8>,
    },
    /// Downward (`node.issuer.update`; node-issuer-update.md): the fabric-primary installs its mesh's
    /// issuing material on one node-admin of that mesh, answered [`StatusReply::IssuerUpdated`], or
    /// [`StatusReply::RejectedCert`] for material of another mesh or birth, or one the root does not
    /// verify.
    ///
    /// Position 31 on the wire.
    UpdateIssuer {
        /// The node-admin.
        node_id: NodeId,
        /// The incarnation of that node-admin.
        incarnation: IncarnationId,
        /// The issuing key's secret, as hex.
        key: IssuingKeyHex,
        /// The root-signed issuing cert, as hex (`rdm_core::certs::encode_issuing_cert`); it carries the
        /// signed cert of the fabric CA it is minted under.
        cert: String,
        /// The key of that fabric CA, as hex: installed with the issuing material, as the launch writes
        /// it (`RDM_ISSUING_KEY` beside `RDM_FABRIC_CA_KEY`; fabric.ca.renew step 2, Q275).
        ca_key: CaKeyHex,
    },
    /// `fabrics.atlas.get` (docs/plans/proposal/fabric-atlas.md R-FA4): a caller asks the
    /// fabric-primary for its atlas, answered [`StatusReply::Atlas`]. A read. Appended last: no
    /// variant before it moves.
    ///
    /// Position 32 on the wire.
    GetAtlas,
    /// The reads (Q167, R-RD1 to R-RD3): one read per entity, plural, `fabrics.get`, `meshes.get`,
    /// `nodes.get` and `connections.get { node }`, each a list of the rows the receiver holds that
    /// match every pair of `filter` ([`crate::filter`]), in a stable order, the empty list when none
    /// matches; a filter the entity's schema refuses is [`StatusReply::RejectedFilter`] naming the
    /// pair. Answered [`StatusReply::Fabrics`], [`StatusReply::Meshes`], [`StatusReply::Nodes`] or
    /// [`StatusReply::Connections`]. `node` is `connections.get`'s address, the exact birth whose
    /// connections log is read, required for `Connections` and refused for any other entity. A read:
    /// nothing changes. Appended last: no variant before it moves; `GetNodes`, `GetMeshes`,
    /// `GetFabric` and `GetConnections` keep their positions and are read as this request
    /// ([`StatusRequest::as_read`]).
    ///
    /// Position 33 on the wire.
    Get {
        /// The entity read.
        entity: crate::filter::Entity,
        /// The exact birth a `connections.get` reads; `None` for every other entity.
        node: Option<(NodeId, IncarnationId)>,
        /// The pairs every answered row matches.
        filter: crate::filter::Filter,
    },
    /// Upward (node-config-update.md, a node's own update, step 3; Q235): a mesh-primary that is not
    /// the fabric-primary owes the node's row as written, its config and the row's revision, to the
    /// fabric-primary, which writes it into its atlas over an older revision. Owed until answered:
    /// `Applied` when the atlas took it, `AlreadyApplied` when the atlas holds that revision or a newer
    /// one; at an admin that is not the fabric-primary, `RejectedNotPrimary` naming it. Appended last:
    /// no variant before it moves.
    ///
    /// Position 34 on the wire.
    NodeConfigUpdated {
        /// The node whose row was written.
        node_id: NodeId,
        /// The row's config as written.
        config: BTreeMap<String, String>,
        /// The row's revision as written (P1: every install is ordered by it).
        revision: u64,
    },
    /// Downward (`node.roots.update`; fabric-ca-renew.md "node.roots.update, the primitive"): the
    /// fabric-primary installs the fabric's trusted root set on one exact birth, answered
    /// [`StatusReply::RootsUpdated`], or [`StatusReply::RejectedRootStillChained`] for a set that drops a
    /// root the birth still admits a chain under. One or two roots, never none; the root secret only to
    /// a node-admin. Appended last: no variant before it moves.
    ///
    /// Position 35 on the wire.
    UpdateRoots {
        /// The node.
        node_id: NodeId,
        /// Its incarnation: the exact birth.
        incarnation: IncarnationId,
        /// The root public keys the birth trusts from now on, each an ed25519 public key as hex.
        roots: Vec<String>,
        /// The rotation's phase this set belongs to.
        phase: RootsPhase,
        /// The new root's secret, carried to a node-admin only.
        secret: Option<RootSecretHex>,
    },
    /// Caller (`fabric.root.update`; fabric-ca-renew.md "fabric.root.update, the caller op"): the app or a
    /// person hands the fabric-primary a new root secret, the trust anchor, and the rotation's phases
    /// begin; answered [`StatusReply::RootUpdated`], or refused by name while a rotation is in flight
    /// ([`StatusReply::RejectedRotationInProgress`]) and for a root already trusted
    /// ([`StatusReply::RejectedRootAlreadyTrusted`]). Appended last: no variant before it moves.
    ///
    /// Position 36 on the wire.
    UpdateRoot {
        /// The new root's secret.
        secret: RootSecretHex,
    },
}

/// A read as one shape (Q167): the entity, the exact birth a `connections.get` reads, and the filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Read {
    /// The entity read.
    pub entity: crate::filter::Entity,
    /// The exact birth a `connections.get` reads.
    pub node: Option<(NodeId, IncarnationId)>,
    /// The pairs every answered row matches.
    pub filter: crate::filter::Filter,
}

impl StatusRequest {
    /// The read this request is, `None` for any other request: a [`StatusRequest::Get`] as it is,
    /// and each retained read variant as the pairs it names (`NodeSelect::Id(x)` is `[id: x]`,
    /// `Mesh(m)` `[mesh: m]`, `Tags(All[a, b])` `[tags: a][tags: b]`, `Tags(Any[a, b])` `[tags: a, b]`,
    /// `GetMeshes { Some(m) }` `[id: m]` for a canonical id and `[name: m]` otherwise).
    pub fn as_read(&self) -> Option<Read> {
        use crate::filter::{Entity, Filter};
        let tags = |f: Filter, t: &TagFilter| match t {
            TagFilter::All(labels) => labels.iter().fold(f, |f, l| f.eq("tags", [l.clone()])),
            TagFilter::Any(labels) => f.eq("tags", labels.iter().cloned()),
        };
        let (entity, node, filter) = match self {
            Self::Get { entity, node, filter } => (*entity, node.clone(), filter.clone()),
            Self::GetNodes { select } => (Entity::Nodes, None, match select {
                NodeSelect::Id(id) => Filter::new().eq("id", [id.to_string()]),
                NodeSelect::Mesh(m) => Filter::new().eq("mesh", [m.clone()]),
                NodeSelect::Tags(t) => tags(Filter::new(), t),
                NodeSelect::Fabric => Filter::new(),
                NodeSelect::MeshTags(m, t) => tags(Filter::new().eq("mesh", [m.clone()]), t),
            }),
            Self::GetMeshes { mesh: None } => (Entity::Meshes, None, Filter::new()),
            Self::GetMeshes { mesh: Some(m) } if crate::ids::decode_crockford60(m).is_some() => (Entity::Meshes, None, Filter::new().eq("id", [m.clone()])),
            Self::GetMeshes { mesh: Some(m) } => (Entity::Meshes, None, Filter::new().eq("name", [m.clone()])),
            Self::GetFabric => (Entity::Fabrics, None, Filter::new()),
            Self::GetConnections { node_id, incarnation } => (Entity::Connections, Some((node_id.clone(), incarnation.clone())), Filter::new()),
            _ => return None,
        };
        Some(Read { entity, node, filter })
    }
}

/// An issuing key's secret carried by [`StatusRequest::UpdateIssuer`], as hex: its debug form names its
/// length, never its value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IssuingKeyHex(pub String);

impl std::fmt::Debug for IssuingKeyHex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IssuingKeyHex({} hex digits)", self.0.len())
    }
}

/// A fabric CA's key carried by [`StatusRequest::UpdateIssuer`], as hex: its debug form names its
/// length, never its value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CaKeyHex(pub String);

impl std::fmt::Debug for CaKeyHex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CaKeyHex({} hex digits)", self.0.len())
    }
}

/// A root's secret carried by [`StatusRequest::UpdateRoots`] to a node-admin, as hex: its debug form
/// names its length, never its value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RootSecretHex(pub String);

impl std::fmt::Debug for RootSecretHex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RootSecretHex({} hex digits)", self.0.len())
    }
}

/// A root rotation's phase (fabric-ca-renew.md "fabric.root.update"): `minted` -> `distributed` (both
/// anchors trusted everywhere) -> `ca-rotated` -> `retired` (one anchor again). Positional order is
/// part of the wire schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RootsPhase {
    /// The new root's secret is written beside the old one on the fabric-primary.
    ///
    /// Position 0 on the wire.
    Minted,
    /// Every node trusts both roots.
    ///
    /// Position 1 on the wire.
    Distributed,
    /// The CA is renewed under the new root, every issuer re-minted.
    ///
    /// Position 2 on the wire.
    CaRotated,
    /// Every node trusts the new root alone; the old secret is destroyed.
    ///
    /// Position 3 on the wire.
    Retired,
}

/// Which mesh channel a `node.mesh.join` joined or a `node.mesh.leave` left
/// (node-rpc-envelope.md, "Node mesh join and leave command pairs"). Positional order is part
/// of the wire schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WhichMesh {
    /// The node's own mesh channel (`<fabric_id>/<mesh_id>`).
    ///
    /// Position 0 on the wire.
    Own,
    /// The fabric's admin-backbone channel (`<fabric_id>/admin-backbone`), carried by a node-admin.
    ///
    /// Position 1 on the wire.
    Backbone,
}

impl WhichMesh {
    /// The channel's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Own => "own",
            Self::Backbone => "backbone",
        }
    }
}

/// Which nodes a `GetNodes` selects (Q165). Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeSelect {
    /// The node with this minted id (`node.get { id }`).
    ///
    /// Position 0 on the wire.
    Id(NodeId),
    /// Every node of the mesh with this name (`mesh.nodes.get`, no filter).
    ///
    /// Position 1 on the wire.
    Mesh(String),
    /// Every node of the fabric carrying the tag labels the filter names (R-TG1;
    /// `fabric.nodes.get { tags }`), read through the node store's tags index: one point read per
    /// label.
    ///
    /// Position 2 on the wire.
    Tags(TagFilter),
    /// Every node of every mesh (`fabric.nodes.get`, no filter): the mesh selector's answer over
    /// every mesh, read from the rows. Appended last: positional.
    ///
    /// Position 3 on the wire.
    Fabric,
    /// Every node of the mesh with this name carrying the tag labels the filter names
    /// (`mesh.nodes.get { tags }`), read through the tags index as `Tags` is. Appended last:
    /// positional.
    ///
    /// Position 4 on the wire.
    MeshTags(String, TagFilter),
}

impl NodeSelect {
    /// The selection's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Id(_) => "id",
            Self::Mesh(_) => "mesh",
            Self::Tags(_) => "tags",
            Self::Fabric => "fabric",
            Self::MeshTags(..) => "mesh-tags",
        }
    }

    /// The call this selection is, by its name in node-rpc-envelope.md (Q165).
    pub fn read(&self) -> &'static str {
        match self {
            Self::Id(_) => "node.get",
            Self::Mesh(_) | Self::MeshTags(..) => "mesh.nodes.get",
            Self::Tags(_) | Self::Fabric => "fabric.nodes.get",
        }
    }
}

/// Which tag labels a `mesh.nodes.get` or `fabric.nodes.get` filter requires (node-rpc-envelope.md:
/// "all of the given labels or any of them"). Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TagFilter {
    /// The node carries every one of these labels.
    ///
    /// Position 0 on the wire.
    All(BTreeSet<String>),
    /// The node carries at least one of these labels.
    ///
    /// Position 1 on the wire.
    Any(BTreeSet<String>),
}

impl TagFilter {
    /// The labels the filter names.
    pub fn labels(&self) -> &BTreeSet<String> {
        match self {
            Self::All(l) | Self::Any(l) => l,
        }
    }

    /// The filter's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::All(_) => "all",
            Self::Any(_) => "any",
        }
    }

    /// Why the filter is refused, by name, or `None` when it names labels a node can carry: it names
    /// at least one label, at most [`MAX_NODE_TAGS`], each a label ([`tag_refusal`]).
    pub fn refusal(&self) -> Option<String> {
        if self.labels().is_empty() {
            return Some(format!("the tag filter ({}) names no label: it needs at least one", self.name()));
        }
        tags_refusal(self.labels())
    }
}

/// The most tag labels one node carries (R-TG1): a set at the cap, each label at
/// [`MAX_TAG_BYTES`], fits the Status request bound with a `CreateTags`'s other fields.
pub const MAX_NODE_TAGS: usize = 32;

/// The longest tag label, in bytes.
pub const MAX_TAG_BYTES: usize = 64;

/// Why `label` is not a tag label, by name, or `None` when it is one (node-rpc-envelope.md node.tags:
/// "A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading '.'"): 1 to [`MAX_TAG_BYTES`] bytes
/// of lowercase ASCII letters, digits, `-`, `_`, `.` and `~`, not starting with `.` (`rabbit`,
/// `zone_a`, `v1.2~rc`). A label keys one row of the node store's tags index, so it is a key every
/// store can hold: no separator, no whitespace, and never `.` or `..`.
pub fn tag_refusal(label: &str) -> Option<String> {
    grammar_refusal("tag label", label)
}

/// Why `name` is not a node name, by name, or `None` when it is one (proposal
/// node-properties-own-commands.md §9, ruled A: "a rename may take any name within the name grammar
/// (the tag-label grammar ...: 1-64 of `a-z 0-9 - _ . ~`, no leading `.`, whitespace refused)").
pub fn name_refusal(name: &str) -> Option<String> {
    grammar_refusal("name", name)
}

/// The default `node_type` when `node.create` omits it or passes an empty string.
pub const DEFAULT_NODE_TYPE: &str = "node";

/// Normalize a `node_type` supplied at `node.create`: an empty string becomes [`DEFAULT_NODE_TYPE`].
/// Whitespace is not trimmed, so `" "` stays `" "` and [`node_type_refusal`] refuses it by name.
pub fn normalize_node_type(node_type: &str) -> String {
    if node_type.is_empty() {
        DEFAULT_NODE_TYPE.to_string()
    } else {
        node_type.to_string()
    }
}

/// Why `node_type` is not a valid node type, by name (`node_type`), or `None` when it is within
/// the shared identifier grammar (`1..=64` of `a-z 0-9 - _ . ~`, no leading `.`, whitespace refused).
pub fn node_type_refusal(node_type: &str) -> Option<String> {
    grammar_refusal("node_type", node_type)
}

/// Why `node.update` of `node_type` is refused, by name (`node_type` is immutable after `node.create`).
pub fn node_type_update_refusal(held_node_type: &str, requested_node_type: &str) -> String {
    if let Some(reason) = node_type_refusal(requested_node_type) {
        return format!("node_type is immutable (set at node.create, held {held_node_type:?}); node.update cannot change node_type ({reason})");
    }
    format!("node_type is immutable (set at node.create, held {held_node_type:?}); node.update cannot change node_type to {requested_node_type:?}")
}

/// Why `value`, a `noun` (a tag label, a node name), is outside the one grammar both take, by name, or
/// `None` when it is within it: 1 to [`MAX_TAG_BYTES`] bytes of lowercase ASCII letters, digits, `-`,
/// `_`, `.` and `~`, not starting with `.`.
fn grammar_refusal(noun: &str, value: &str) -> Option<String> {
    let b = value.as_bytes();
    if b.is_empty() {
        return Some(format!("the {noun} is empty"));
    }
    if b.len() > MAX_TAG_BYTES {
        return Some(format!("the {noun} {:?} is {} bytes, over the {MAX_TAG_BYTES}-byte cap", within(value, MAX_TAG_BYTES), b.len()));
    }
    if b[0] == b'.' {
        return Some(format!("the {noun} {value:?} starts with '.'"));
    }
    if !b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'-' | b'_' | b'.' | b'~')) {
        return Some(format!("the {noun} {value:?} is not lowercase ASCII letters, digits, '-', '_', '.' and '~'"));
    }
    None
}

/// Why the tag set `tags` is refused, by name, or `None`: at most [`MAX_NODE_TAGS`] labels, each a
/// label ([`tag_refusal`]).
pub fn tags_refusal(tags: &BTreeSet<String>) -> Option<String> {
    if tags.len() > MAX_NODE_TAGS {
        return Some(format!("{} tag labels, over the {MAX_NODE_TAGS} one node carries", tags.len()));
    }
    tags.iter().find_map(|t| tag_refusal(t))
}

/// The most connection ids one `node.connections.delete { scope: Connections }` names: a request at
/// the cap fits the Status request bound, and more is refused by name.
pub const MAX_CONNECTION_IDS: usize = 64;

/// Which of a birth's connections `node.connections.delete` cuts (R-CR3). Positional order is part
/// of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionScope {
    /// Every connection, exactly the stop's cut: the gossip neighbours and the pooled peer
    /// connections, never the endpoint or the call's own stream. A tag of its own, never a value in
    /// the list.
    ///
    /// Position 0 on the wire.
    All,
    /// The exact connections named by the minted id of their open entry, as `node.connections.get`
    /// returns them: only those pooled peer connections are cut. At least one, at most
    /// [`MAX_CONNECTION_IDS`].
    ///
    /// Position 1 on the wire.
    Connections(Vec<ConnectionId>),
}

impl ConnectionScope {
    /// The scope's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Connections(_) => "connections",
        }
    }

    /// Why the scope is refused, by name, or `None` when it names a cut: an empty list or one past
    /// [`MAX_CONNECTION_IDS`] names nothing to cut that the request can carry.
    pub fn refusal(&self) -> Option<String> {
        match self {
            Self::All => None,
            Self::Connections(ids) if ids.is_empty() => Some("node.connections.delete names no connection: Connections needs at least one connection id (All is its own scope)".into()),
            Self::Connections(ids) if ids.len() > MAX_CONNECTION_IDS => Some(format!("node.connections.delete names {} connections, over the {MAX_CONNECTION_IDS} one request carries", ids.len())),
            Self::Connections(_) => None,
        }
    }
}

/// Which of a node's tag labels `node.tags.delete` removes (node-rpc-envelope.md, node.tags: "the
/// same scope shape as `node.connections.delete`"). Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TagScope {
    /// Every label the node carries. A tag of its own, never a value in the list.
    ///
    /// Position 0 on the wire.
    All,
    /// The named labels: at least one, at most [`MAX_NODE_TAGS`], each a label ([`tag_refusal`]).
    ///
    /// Position 1 on the wire.
    Labels(BTreeSet<String>),
}

impl TagScope {
    /// The scope's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Labels(_) => "labels",
        }
    }

    /// Why the scope is refused, by name, or `None` when it names labels a node can carry: an empty
    /// list names nothing to remove (`All` is its own scope); a list past [`MAX_NODE_TAGS`] or naming
    /// a non-label is refused as [`tags_refusal`] refuses it.
    pub fn refusal(&self) -> Option<String> {
        match self {
            Self::All => None,
            Self::Labels(labels) if labels.is_empty() => Some("node.tags.delete names no label: Labels needs at least one label (All is its own scope)".into()),
            Self::Labels(labels) => tags_refusal(labels),
        }
    }
}

/// Why a `node.tags.create`'s labels are refused, by name, or `None`: at least one label, at most
/// [`MAX_NODE_TAGS`], each a label ([`tag_refusal`]). Whether the node may carry them all is the
/// writer's decision, from the labels its row already carries.
pub fn create_tags_refusal(labels: &BTreeSet<String>) -> Option<String> {
    if labels.is_empty() {
        return Some("node.tags.create names no label: it needs at least one".into());
    }
    tags_refusal(labels)
}

/// The longest `receipt_manifest` reference a `MeshDeleted` carries.
pub const MAX_RECEIPT_MANIFEST_BYTES: usize = 128;

/// The bound every Status request fits in (node-rpc.md "Each protocol declares
/// `MAX_REQUEST_FRAME_BYTES`"): a request past it, a config put's whole map included, is refused
/// `Malformed(TooLarge)` before its body is read.
const MAX_STATUS_REQUEST_BYTES: usize = 4 * 1024;

/// The bound a declaration fits in, well within [`MAX_STATUS_REQUEST_BYTES`].
#[cfg(test)]
const MAX_DECLARATION_REQUEST_BYTES: usize = 1024;

impl StatusRequest {
    /// The request's operation name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::DeclareNodeState { .. } => "declare-node-state",
            Self::DeclareMeshState { .. } => "declare-mesh-state",
            Self::ApplyNodeState { .. } => "apply-node-state",
            Self::ProbeNodeState { .. } => "probe-node-state",
            Self::ApplyMeshState { .. } => "apply-mesh-state",
            Self::ApplyFabricEvent { .. } => "apply-fabric-event",
            Self::Reserved6 { .. } => "reserved-6",
            Self::Reserved7 { .. } => "reserved-7",
            Self::StopNode { .. } => "stop-node",
            Self::NodeLeft { .. } => "node-left",
            Self::DeleteMesh { .. } => "delete-mesh",
            Self::MeshDeleted { .. } => "mesh-deleted",
            Self::Reserved12 { .. } => "reserved-12",
            Self::Reserved13 { .. } => "reserved-13",
            Self::Reserved14 { .. } => "reserved-14",
            Self::Reserved15 { .. } => "reserved-15",
            Self::StartNode { .. } => "start-node",
            Self::UpdateNodeConfig { .. } => "update-node-config",
            Self::ApplyNodeConfig { .. } => "apply-node-config",
            Self::ApplyNodeName { .. } => "apply-node-name",
            Self::UpdateName { .. } => "node.name.update",
            Self::DeleteConnections { .. } => "delete-connections",
            Self::GetNodes { select } => select.read(),
            Self::GetMeshes { .. } => "meshes.get",
            Self::GetFabric => "fabrics.get",
            Self::GetAtlas => "fabrics.atlas.get",
            Self::GetConnections { .. } => "connections.get",
            Self::Get { entity, .. } => entity.read(),
            Self::NodeConfigUpdated { .. } => "node-config-updated",
            Self::CreateTags { .. } => "node.tags.create",
            Self::DeleteTags { .. } => "node.tags.delete",
            Self::JoinMesh { .. } => "node.mesh.join",
            Self::LeaveMesh { .. } => "node.mesh.leave",
            Self::RenewCert { .. } => "node.cert.renew",
            Self::UpdateIssuer { .. } => "node.issuer.update",
            Self::UpdateRoots { .. } => "node.roots.update",
            Self::UpdateRoot { .. } => "fabric.root.update",
        }
    }
}

/// Why a declaration was not applied, by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotPrimary {
    /// The receiver does not hold the seat the declaration needs.
    ///
    /// Position 0 on the wire.
    ReceiverNotPrimary {
        /// The seat the receiver would have to hold.
        needed: String,
    },
    /// The sender is not the subject (or the subject's current primary).
    ///
    /// Position 1 on the wire.
    SenderNotSubject {
        /// The sender, as the receiver resolved it.
        sender: String,
    },
    /// The receiver does not hold the subject at all.
    ///
    /// Position 2 on the wire.
    SubjectUnknown,
}

impl NotPrimary {
    /// The reason's name as it appears in spans and replies.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReceiverNotPrimary { .. } => "receiver-not-primary",
            Self::SenderNotSubject { .. } => "sender-not-subject",
            Self::SubjectUnknown => "subject-unknown",
        }
    }
}

/// What a `node.connections.delete { All }` established (node-connections-delete.md, "Receipt"): the
/// in-flight count it measured and the connections it closed. `Deadline` records the last observed
/// count and the connections still open; it never claims zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ConnectionsReceipt {
    /// The eligible in-flight work reached zero and every open connection was closed.
    ///
    /// Position 0 on the wire.
    Established {
        /// The in-flight count when the closure was admitted.
        in_flight: u64,
        /// The open entries closed.
        closed: Vec<ConnectionId>,
    },
    /// `RDM_CONNECTIONS_DELETE_DEADLINE_MS` passed with work still in flight.
    ///
    /// Position 1 on the wire.
    Deadline {
        /// The last in-flight count observed before the bound.
        last_in_flight: u64,
        /// The open entries closed.
        closed: Vec<ConnectionId>,
        /// The open entries still held.
        held: Vec<ConnectionId>,
    },
}

impl ConnectionsReceipt {
    /// The receipt's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Established { .. } => "established",
            Self::Deadline { .. } => "deadline",
        }
    }
}

/// The workflow identity of one mesh.delete, as a `DeleteMesh` or `MeshDeleted` names it:
/// `(mesh_id, build_id, attempt, operation)` (node-rpc-envelope.md, "Mesh delete command pair") less
/// the mesh id, which the call itself names. A workflow refusal carries the workflow the receiver holds and
/// the one offered, so the mismatch is named whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDeleteIdentity {
    /// The Build the workflow belongs to.
    pub build_id: String,
    /// The Build attempt.
    pub attempt: u32,
    /// The operation key, `<op>:<mesh_id>`.
    pub operation: String,
}

impl std::fmt::Display for MeshDeleteIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (build {}, attempt {})", self.operation, self.build_id, self.attempt)
    }
}

/// The typed result of a Status call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusReply {
    /// Applied now by the authority, and durable where the ruling asks it to be.
    ///
    /// Position 0 on the wire.
    Applied,
    /// The same natural key was already at that state: one logical event.
    ///
    /// Position 1 on the wire.
    AlreadyApplied,
    /// Reserved position (R-CR4): never sent.
    ///
    /// Position 2 on the wire.
    Reserved2 {
        /// The work still in flight.
        in_flight: u64,
    },
    /// The exact birth's own answer to a probe: it reasserted its presence, and this is its state.
    ///
    /// Position 3 on the wire.
    Current {
        /// The answering birth's logical node.
        node_id: NodeId,
        /// The answering birth's incarnation.
        incarnation: IncarnationId,
        /// The birth's current state.
        state: NodeState,
    },
    /// The receiver holds a newer incarnation of that NodeId, or holds it as deleted.
    ///
    /// Position 4 on the wire.
    RejectedStaleIncarnation {
        /// The incarnation held.
        held: IncarnationId,
    },
    /// The receiver holds another mesh id under that mesh name.
    ///
    /// Position 5 on the wire.
    RejectedStaleMesh {
        /// The mesh id held.
        held: MeshId,
    },
    /// The receiver holds another fabric id.
    ///
    /// Position 6 on the wire.
    RejectedStaleFabric {
        /// The fabric id held.
        held: FabricId,
    },
    /// The receiver or sender is not the authority for the call.
    ///
    /// Position 7 on the wire.
    RejectedNotPrimary {
        /// Which authority condition failed.
        why: NotPrimary,
    },
    /// A node move backward along the legal order.
    ///
    /// Position 8 on the wire.
    RejectedInvalidNodeTransition {
        /// The state the node holds.
        current: NodeState,
    },
    /// A mesh move backward along the legal order.
    ///
    /// Position 9 on the wire.
    RejectedInvalidMeshTransition {
        /// The state the mesh holds.
        current: MeshState,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 10 on the wire.
    PeerUnresolved {
        /// Why the call is refused.
        reason: String,
    },
    /// The node is not ready to serve.
    ///
    /// Position 11 on the wire.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    ///
    /// Position 12 on the wire.
    Busy {
        /// Which bound the node is at.
        reason: String,
    },
    /// The node is connections-deleting and takes no new work.
    ///
    /// Position 13 on the wire.
    ConnectionsDeleting {
        /// Why the node refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 14 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 15 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// A completion call (`node.connections.deleted`) names no open command of the receiver: the first field
    /// that differs from the command the receiver holds for that birth is named, with the value the
    /// receiver holds and the value the completion reported. A `node-left` is never a call and
    /// completes no command, whatever is open for the birth: it is refused with `command`.
    ///
    /// Position 16 on the wire.
    RejectedUnmatchedCompletion {
        /// The field that differs: `build_id`, `attempt`, `operation`, or `command` when the
        /// receiver holds no open command the call completes (none for the birth at all, or the call
        /// is a `node-left`).
        field: String,
        /// What the receiver holds for it.
        expected: String,
        /// What the completion reported.
        reported: String,
    },
    /// The reply of a `StopNode` call, sent when the stop is done (`node.stopped`): the birth's
    /// `node.connections.delete { All }` completed, it cut its transport and is stopped, its process
    /// alive. The receipt is what that `All` established.
    ///
    /// Position 17 on the wire.
    Stopped {
        /// What the stop's `node.connections.delete { All }` established.
        receipt: ConnectionsReceipt,
    },
    /// The reply of a `StartNode` call: the stopped birth rejoined as itself.
    ///
    /// Position 18 on the wire.
    Started,
    /// The reply of a `StartNode` call whose start stopped at `step`: nothing after that step ran.
    ///
    /// Position 19 on the wire.
    StartFailed {
        /// The step that failed: `node.join`, `node.topology` or `node.pre-node-ready`.
        step: String,
        /// Why it failed.
        reason: String,
    },
    /// The exact birth's own answer to a probe while it is stopped: `Leaving`, its process alive and
    /// its endpoint bound, waiting for `StartNode` or a delete.
    ///
    /// Position 20 on the wire.
    CurrentStopped {
        /// The answering birth's logical node.
        node_id: NodeId,
        /// The answering birth's incarnation.
        incarnation: IncarnationId,
    },
    /// The reply of an `UpdateNodeConfig`: the birth's mesh-primary wrote the node's row and the exact
    /// birth holds the config (`node.config.updated`). It carries the row as written, its config and
    /// revision: a node's own update installs it over an older revision, and the fabric-primary writes
    /// it into its atlas over an older revision (P1, P2).
    ///
    /// Position 21 on the wire.
    ConfigUpdated {
        /// The birth whose config was put.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The row's config as written.
        config: BTreeMap<String, String>,
        /// The row's revision as written.
        revision: u64,
    },
    /// Reserved position (Q203): never sent.
    ///
    /// Position 22 on the wire.
    Reserved22 {
        /// Never read.
        keys: u32,
        /// Never read.
        bytes: u32,
        /// Never read.
        max_keys: u32,
        /// Never read.
        max_bytes: u32,
    },
    /// The config put was not made: the nodes store refused the node's row, so nothing was told to
    /// the birth and the node's record is what it was.
    ///
    /// Position 23 on the wire.
    ConfigRowRefused {
        /// The node's `path.name`, the row the store refused.
        node: String,
        /// The store's refusal.
        reason: String,
        /// The row's config as it stands (the write refused, it is what it was): a node's own update
        /// installs it over an older revision (P2); empty when the writer holds no row.
        config: BTreeMap<String, String>,
        /// The row's revision as it stands; 0 when the writer holds no row.
        revision: u64,
    },
    /// The mesh-primary wrote the node's row, and the exact birth did not apply the config: the
    /// record holds the new config, the birth does not.
    ///
    /// Position 24 on the wire.
    ConfigNotApplied {
        /// The node's `path.name`.
        node: String,
        /// The birth's reply to `apply-node-config` by name, or the call's end by name.
        answered: String,
        /// Its detail.
        reason: String,
    },
    /// The exact birth's own answer to a probe while its `pre_node_ready` hook is Blocked: the
    /// probe served that birth and found an active readiness blocker. `reason` is the hook's own
    /// text and `retry_on` the event it runs again on. It is never a `NotReady`: a `NotReady` is the
    /// door or the handler not serving yet, and says nothing of the hook.
    ///
    /// Position 25 on the wire.
    Blocked {
        /// The answering birth's logical node.
        node_id: NodeId,
        /// The answering birth's incarnation.
        incarnation: IncarnationId,
        /// The hook's own blocker, in words.
        reason: String,
        /// The event the hook runs again on.
        retry_on: RetryOn,
    },
    /// The reply of an `UpdateName` (`node.name.updated`): the mesh-primary moved the node's row to
    /// the new name, and the exact birth holds the name (it answered `Applied` or `AlreadyApplied`),
    /// or is stopped and reads it in its next join answer.
    ///
    /// Position 26 on the wire.
    NameUpdated {
        /// The birth renamed.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
    },
    /// The `UpdateName` was not made: the node admits connections (R-CR1). Its status is not
    /// stopped, and its mesh-primary holds no `node.connections.deleted` of an `All` made while it was
    /// not ready that left it holding nothing. Nothing was written.
    ///
    /// Position 27 on the wire.
    RejectedAdmittingConnections {
        /// The node's status as its mesh-primary holds it (at the cut, when it holds one).
        status: NodeStatus,
        /// The connections the node still held after its last `node.connections.delete`, at most
        /// [`MAX_REPORTED_CONNECTIONS`]; empty when it holds no cut of every connection.
        held_connections: Vec<ConnectionId>,
    },
    /// The `UpdateName` was not made: another node holds the name (the row writer read its row).
    /// Nothing was written.
    ///
    /// Position 28 on the wire.
    NameHeld {
        /// The name claimed.
        name: String,
        /// The node that holds it.
        by: NodeId,
    },
    /// The `UpdateName` was not made: the nodes store refused the claim or the put at the new key, so
    /// nothing was written and the old name stands.
    ///
    /// Position 29 on the wire.
    NodeRowRefused {
        /// The node's `path.name`, the row the store refused.
        node: String,
        /// The store's refusal.
        reason: String,
    },
    /// The `UpdateName` ended incomplete after its put at the new key (proposal
    /// node-properties-own-commands.md §2.3, §2.4): the old key's release is pending
    /// (`answered: release-pending`), or the live birth did not apply the name (its answer). The row
    /// holds the new name; a resubmit finishes what is pending and completes on the node's answer.
    ///
    /// Position 30 on the wire.
    NameNotApplied {
        /// The node's `path.name` the row now holds.
        node: String,
        /// `release-pending`, or the birth's reply to `apply-node-name` by name, or the call's end by
        /// name.
        answered: String,
        /// Its detail.
        reason: String,
    },
    /// `node.connections.deleted`: the cut is complete. Every connection named is accounted for by
    /// its entry id: `closed` were open and are cut, their drops written under the name the birth
    /// holds; `not_open` were named and are unknown or already closed; `held` are the connections
    /// the birth still holds after the cut (none after `All`).
    ///
    /// Position 31 on the wire.
    ConnectionsDeleted {
        /// The open entries cut.
        closed: Vec<ConnectionId>,
        /// The ids named that are no open entry of the birth: unknown, or already closed.
        not_open: Vec<ConnectionId>,
        /// The open entries the birth still holds.
        held: Vec<ConnectionId>,
        /// For `All`, what the closure established; `None` for `Connections`.
        receipt: Option<ConnectionsReceipt>,
    },
    /// The `DeleteConnections` scope names nothing the birth can cut (an empty list, or more ids
    /// than one request carries); nothing is cut.
    ///
    /// Position 32 on the wire.
    RejectedConnectionScope {
        /// Why, by name.
        reason: String,
    },
    /// The answer to a `GetNodes`: the nodes the answering admin's view holds that the request
    /// selected, sorted by `path.name`.
    ///
    /// Position 33 on the wire.
    Nodes {
        /// The nodes.
        nodes: Vec<NodeView>,
    },
    /// The answer to a `GetMeshes`: the mesh named, or every mesh the answering admin's view holds.
    ///
    /// Position 34 on the wire.
    Meshes {
        /// The meshes.
        meshes: Vec<MeshView>,
    },
    /// The answer to a `GetFabric`: the fabric as the answering admin's view holds it.
    ///
    /// Position 35 on the wire.
    Fabric {
        /// The fabric.
        fabric: FabricView,
    },
    /// The answer to a `GetConnections`: the connections the exact birth holds as its own.
    ///
    /// Position 36 on the wire.
    Connections {
        /// The connections.
        connections: ConnectionsView,
    },
    /// The `GetMeshes` named a mesh the answering admin's view does not hold, by id or by name.
    ///
    /// Position 37 on the wire.
    UnknownMesh {
        /// The mesh as the request named it.
        mesh: String,
    },
    /// A read's answer encodes past the Status reply bound: nothing of it is sent, and the reply
    /// names the read, the answer's size and the bound.
    ///
    /// Position 38 on the wire.
    ReadOverBound {
        /// The read (`node.get`, `mesh.get`, `fabric.get`, `node.connections.get`).
        read: String,
        /// The answer's encoded size, in bytes.
        bytes: u64,
        /// The Status reply bound, in bytes.
        max: u64,
    },
    /// A `DeleteMesh` or `MeshDeleted` names an earlier attempt of the workflow the receiver holds for the
    /// mesh: same Build and operation, a lower attempt (node-rpc-envelope.md, "Mesh delete command
    /// pair": "A stale/conflicting workflow identity ... is refused by name"). Nothing is admitted or accepted.
    ///
    /// Position 39 on the wire.
    RejectedStaleWorkflow {
        /// The workflow the receiver holds for the mesh.
        held: MeshDeleteIdentity,
        /// The workflow the call names.
        offered: MeshDeleteIdentity,
    },
    /// A `DeleteMesh` or `MeshDeleted` names a workflow of the mesh the receiver does not hold and that is
    /// no earlier attempt of the one it holds: another Build or operation, or (for `MeshDeleted`) an
    /// attempt the receiver never opened. Nothing is admitted or accepted.
    ///
    /// Position 40 on the wire.
    RejectedConflictingWorkflow {
        /// The workflow the receiver holds for the mesh.
        held: MeshDeleteIdentity,
        /// The workflow the call names.
        offered: MeshDeleteIdentity,
    },
    /// A `MeshDeleted` whose manifest is incomplete: the leaving mesh-primary recorded no exit manifest
    /// for the workflow, so the member workflows are incomplete and the open leave ends
    /// (node-rpc-envelope.md, "Mesh delete command pair": "A ... incomplete manifest is refused by
    /// name with a typed Status refusal naming the mismatch").
    ///
    /// Position 41 on the wire.
    RejectedIncompleteManifest {
        /// How many roster births, the final primary aside, hold no exact Exited receipt.
        missing: u32,
        /// `<path.name> <incarnation>` of each such birth, at most [`MAX_WITHOUT_EXIT_BIRTHS`] of
        /// them (each within [`MAX_WITHOUT_EXIT_BIRTH_BYTES`]); fewer than `missing` when cut.
        without_exit: Vec<String>,
        /// What the receiver read and found missing, within [`MAX_INCOMPLETE_REASON_BYTES`].
        reason: String,
    },
    /// The call is refused because what the receiver had to read or write to decide or commit it
    /// failed: a durable store refused a put or could not be read, its view names no party the call
    /// needs, its provider refused the runtime, or a peer's Build log could not be read. It is a runtime fact of the receiver, never a readiness answer
    /// (`NotReady` is the door's, node-rpc-envelope.md "Node RPC readiness gate"), and nothing the call
    /// would have done was done.
    ///
    /// Position 42 on the wire.
    RejectedSourceFailed {
        /// What failed, named with its owner and row: `fabric.storage of mesh2.admin.1, handover row
        /// <operation>`, `the view of mesh2.admin.1`, `the Build log of mesh1.admin.1 (the final
        /// primary), read over fetch-build-facts`. Within [`MAX_SOURCE_FAILED_SOURCE_BYTES`].
        source: String,
        /// The operation on it that failed (`put`, `read`, `fetch-build-facts`, `adopt`, `resolve`),
        /// with its subject. Within [`MAX_SOURCE_FAILED_OP_BYTES`].
        op: String,
        /// Why it failed: the store's, the provider's or the peer's own error, or the party the view
        /// does not name. Within [`MAX_SOURCE_FAILED_CAUSE_BYTES`].
        cause: String,
    },
    /// A tags command's labels, or a `GetNodes` tag filter, name something that is not a node's tag
    /// set (R-TG1): an empty filter, more than [`MAX_NODE_TAGS`] labels, or a label that is not a
    /// label ([`tag_refusal`]). Nothing is written or read.
    ///
    /// Position 43 on the wire.
    RejectedTags {
        /// Why, by name, naming the label; within [`MAX_REFUSAL_REASON_BYTES`].
        reason: String,
    },
    /// Reserved position (Q171): never sent.
    ///
    /// Position 44 on the wire.
    Reserved44 {
        /// Never read.
        workflow: String,
        /// Never read.
        status: String,
    },
    /// The reply of a `CreateTags` (`node.tags.created`): the mesh-primary wrote the node's row
    /// carrying the labels and their tags index entries.
    ///
    /// Position 45 on the wire.
    TagsCreated {
        /// The birth whose tags grew.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The labels the node already carried: ignored, nothing written for them
        /// (`AlreadyApplied` for those).
        already_carried: BTreeSet<String>,
    },
    /// The reply of a `DeleteTags` (`node.tags.deleted`): the mesh-primary wrote the node's row
    /// without the labels and tombstoned their tags index entries.
    ///
    /// Position 46 on the wire.
    TagsDeleted {
        /// The birth whose tags shrank.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The named labels the node did not carry: nothing was removed for them.
        not_carried: BTreeSet<String>,
    },
    /// The `UpdateName` was not made: `name` is outside the name grammar ([`name_refusal`]), or the
    /// `path.name` its join with the held record's path makes is not one this build can key. Nothing
    /// was written.
    ///
    /// Position 47 on the wire.
    RejectedName {
        /// Why, by name, naming the name; within [`MAX_REFUSAL_REASON_BYTES`].
        reason: String,
    },
    /// The exact birth's answer to `ApplyNodeName` or `LeaveMesh` while it holds connections (R-CR1;
    /// proposal node-properties-own-commands.md §2.2 step 6, "`Busy { connections }` naming each
    /// connection it still holds"): it keeps its name and holds nothing. Not [`StatusReply::Busy`],
    /// the protocol's admission bound.
    ///
    /// Position 48 on the wire.
    BusyHoldingConnections {
        /// Each connection it holds, by the minted id of its open entry; at most
        /// [`MAX_REPORTED_CONNECTIONS`].
        connections: Vec<ConnectionId>,
    },
    /// `node.mesh.joined`: the exact birth joined its mesh channel(s) and published its first
    /// `Digest` heartbeat (`[Own]` for an rpc-node or storage-node, `[Own, Backbone]` for a
    /// node-admin).
    ///
    /// Position 49 on the wire.
    MeshJoined {
        /// The birth that joined.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The mesh channels joined, in order.
        joined: Vec<WhichMesh>,
    },
    /// `node.mesh.left`: the exact birth left its mesh channel(s), its endpoint still bound
    /// (`[Own]` for an rpc-node or storage-node, `[Own, Backbone]` for a node-admin). Never sent on
    /// this op since node.mesh.leave moved to NodeCommand 0x22 (Q205): the mesh-primary's in-process
    /// carry of the leave answers it ([`crate::node_command::NodeCommandReply::MeshLeft`] on the wire).
    ///
    /// Position 50 on the wire.
    MeshLeft {
        /// The birth that left.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The mesh channels left, in order.
        left: Vec<WhichMesh>,
    },
    /// The receiving endpoint has no handler for the request (R24, Luke Q11): decided by dispatch
    /// alone, never by node kind, seat or sender; nothing is done.
    ///
    /// Position 51 on the wire.
    RequestNotHandled {
        /// The request's op name.
        request: String,
    },
    /// `node.cert.renewed`: the exact birth holds and presents the new member cert.
    ///
    /// Position 52 on the wire.
    CertRenewed {
        /// The birth renewed.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// When the installed cert expires, rafka-time ms.
        expires_at: u64,
    },
    /// `node.issuer.updated`: the node-admin holds its mesh's new issuing material and signs member
    /// certs with it from now.
    ///
    /// Position 53 on the wire.
    IssuerUpdated {
        /// The node-admin.
        node_id: NodeId,
        /// The incarnation of that node-admin.
        incarnation: IncarnationId,
        /// When the installed issuing cert expires, rafka-time ms.
        issuer_expires_at: u64,
    },
    /// A cert or issuing material refused by name (node-cert-renew.md, node-issuer-update.md): it does
    /// not decode, names another birth or mesh, or the root does not verify it. The receiver keeps
    /// what it holds.
    ///
    /// Position 54 on the wire.
    RejectedCert {
        /// Why, by name; within [`MAX_REFUSAL_REASON_BYTES`].
        reason: String,
    },
    /// The answer to a `GetAtlas`: the fabric-primary's atlas. Appended last: no variant
    /// before it moves.
    ///
    /// Position 55 on the wire.
    Atlas {
        /// The atlas.
        atlas: crate::views::AtlasView,
    },
    /// The answer to a `fabrics.get` ([`StatusRequest::Get`] of `Fabrics`): the fabric the answering
    /// admin holds when it matches every pair, else nothing. Appended last: no variant before it moves.
    ///
    /// Position 56 on the wire.
    Fabrics {
        /// The fabrics.
        fabrics: Vec<FabricView>,
    },
    /// A read's filter the entity's schema refuses (Q167 R-RD2): an unknown property, a value the
    /// field cannot take or an empty values list, the pair named; or a `connections.get` without its
    /// node, or a node on another read. Nothing is read. Appended last: no variant before it moves.
    ///
    /// Position 57 on the wire.
    RejectedFilter {
        /// Why, by name, naming the pair; within [`MAX_REFUSAL_REASON_BYTES`].
        reason: String,
    },
    /// The receipt of `UpdateRoots` (`node.roots.update`): the exact birth trusts `roots` from now on.
    /// Appended last: no variant before it moves.
    ///
    /// Position 58 on the wire.
    RootsUpdated {
        /// The node.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
        /// The roots it trusts now, as hex.
        roots: Vec<String>,
    },
    /// `UpdateRoots` refused by name (`root-still-chained`): the set drops a root the birth still
    /// admits a chain under. Nothing is installed. Appended last: no variant before it moves.
    ///
    /// Position 59 on the wire.
    RejectedRootStillChained {
        /// The root, as hex.
        root: String,
    },
    /// `fabric.root.updated`: the rotation is accepted, its roots row `minted`; its progress is
    /// `fabrics.get`'s roots. Appended last: no variant before it moves.
    ///
    /// Position 60 on the wire.
    RootUpdated {
        /// The rotation's phase now.
        phase: RootsPhase,
    },
    /// `fabric.root.update` refused by name (`rotation-in-progress`): a rotation is in flight, at `phase`.
    /// Appended last: no variant before it moves.
    ///
    /// Position 61 on the wire.
    RejectedRotationInProgress {
        /// The phase of the rotation in flight.
        phase: RootsPhase,
    },
    /// `fabric.root.update` refused by name (`root-already-trusted`): the root is one the fabric trusts.
    /// Appended last: no variant before it moves.
    ///
    /// Position 62 on the wire.
    RejectedRootAlreadyTrusted {
        /// The root's public key, as hex.
        root: String,
    },
}

/// The event a Blocked `pre_node_ready` hook runs again on (fabric-node-hooks.md, "Outcomes
/// and the gate"). Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryOn {
    /// The authority's birth changed or deleted.
    ///
    /// Position 0 on the wire.
    PrimaryChanged,
    /// The named node became reachable (heard after silence, or first heard).
    ///
    /// Position 1 on the wire.
    TargetReachable {
        /// The node waited for.
        node_id: NodeId,
    },
    /// The authority published Ready.
    ///
    /// Position 2 on the wire.
    PrimaryReady,
    /// The authority addressed this node (it sent a down op).
    ///
    /// Position 3 on the wire.
    PrimaryAddressed,
}

impl RetryOn {
    /// The event's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::PrimaryChanged => "primary-changed",
            Self::TargetReachable { .. } => "target-reachable",
            Self::PrimaryReady => "primary-ready",
            Self::PrimaryAddressed => "primary-addressed",
        }
    }
}

/// The most bytes of a hook's blocker a `Blocked` reply carries: what is left of the reply bound
/// after the variant (one byte), the node id (a one-byte length and [`CROCKFORD60_LEN`] characters),
/// the incarnation (a one-byte length and [`CROCKFORD60_LEN`] characters), the retry event at its
/// largest (a variant byte and a node id), and a two-byte length prefix on the reason.
pub const MAX_BLOCKED_REASON_BYTES: usize = 1024 - 1 - (1 + CROCKFORD60_LEN) - (1 + CROCKFORD60_LEN) - (1 + 1 + CROCKFORD60_LEN) - 2;

/// The most connection ids one `node.connections.deleted` carries across its three lists: each id
/// is a one-byte length and [`CROCKFORD60_LEN`] characters, after the variant (one byte) and three
/// list lengths (at most three bytes each). A birth holds one entry per peer and kind, far fewer.
pub const MAX_REPORTED_CONNECTIONS: usize = (64 * 1024 - 1 - 3 * 3) / (1 + CROCKFORD60_LEN);

/// The most bytes of a node's `path.name` a config refusal carries.
pub const MAX_REFUSAL_NODE_BYTES: usize = 128;
/// The most bytes of the reply or call-end name a `ConfigNotApplied` carries.
pub const MAX_REFUSAL_ANSWERED_BYTES: usize = 64;
/// The most bytes of detail a config refusal carries: what is left of the reply bound after the
/// variant (one byte), the other fields at their budgets and a two-byte length prefix on each string.
pub const MAX_REFUSAL_REASON_BYTES: usize = 1024 - 1 - (2 + MAX_REFUSAL_NODE_BYTES) - (2 + MAX_REFUSAL_ANSWERED_BYTES) - 2;

/// The most bytes of detail a `RejectedIncompleteManifest` carries.
pub const MAX_INCOMPLETE_REASON_BYTES: usize = 4096;
/// The most bytes of one birth a `RejectedIncompleteManifest` names (`<path.name> <incarnation>`).
pub const MAX_WITHOUT_EXIT_BIRTH_BYTES: usize = 192;
/// The most births a `RejectedIncompleteManifest` names: what is left of the reply bound after the
/// variant (one byte), the count (a varint, at most five bytes), the list length (a varint, at most
/// three bytes) and the reason at its budget with a two-byte length prefix, each birth at its budget
/// with a two-byte length prefix.
pub const MAX_WITHOUT_EXIT_BIRTHS: usize = (64 * 1024 - 1 - 5 - 3 - (2 + MAX_INCOMPLETE_REASON_BYTES)) / (2 + MAX_WITHOUT_EXIT_BIRTH_BYTES);

/// The most bytes of the `source` a `RejectedSourceFailed` names.
pub const MAX_SOURCE_FAILED_SOURCE_BYTES: usize = 256;
/// The most bytes of the `op` a `RejectedSourceFailed` names.
pub const MAX_SOURCE_FAILED_OP_BYTES: usize = 192;
/// The most bytes of the `cause` a `RejectedSourceFailed` carries: what is left of a 1024-byte reply
/// after the variant (one byte), the other two fields at their budgets and a two-byte length prefix on
/// each string. The fabric-primary handover's reply bound is 1024 bytes, so the same three fields fit
/// both families.
pub const MAX_SOURCE_FAILED_CAUSE_BYTES: usize = 1024 - 1 - (2 + MAX_SOURCE_FAILED_SOURCE_BYTES) - (2 + MAX_SOURCE_FAILED_OP_BYTES) - 2;

/// `(source, op, cause)` each cut to its `RejectedSourceFailed` budget.
pub fn source_failed_fields(source: &str, op: &str, cause: &str) -> (String, String, String) {
    (within(source, MAX_SOURCE_FAILED_SOURCE_BYTES), within(op, MAX_SOURCE_FAILED_OP_BYTES), within(cause, MAX_SOURCE_FAILED_CAUSE_BYTES))
}

/// `s` cut to at most `max` bytes at a character boundary; a cut detail ends with `…` so the reader
/// knows it was cut.
pub(crate) fn within(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max - '…'.len_utf8();
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

impl StatusReply {
    /// `ConfigRowRefused` for `node`, the store's refusal `reason`, each within its budget so the
    /// reply always fits the Status reply bound.
    pub fn config_row_refused(node: &str, reason: &str) -> Self {
        Self::config_row_refused_at(node, reason, BTreeMap::new(), 0)
    }

    /// [`Self::config_row_refused`] carrying the row as it stands, its `config` at `revision` (P2).
    pub fn config_row_refused_at(node: &str, reason: &str, config: BTreeMap<String, String>, revision: u64) -> Self {
        Self::ConfigRowRefused { node: within(node, MAX_REFUSAL_NODE_BYTES), reason: within(reason, MAX_REFUSAL_REASON_BYTES), config, revision }
    }

    /// `ConfigNotApplied` for `node`: the birth `answered` (a reply or call-end name), and why, each
    /// within its budget so the reply always fits the Status reply bound.
    pub fn config_not_applied(node: &str, answered: &str, reason: &str) -> Self {
        Self::ConfigNotApplied { node: within(node, MAX_REFUSAL_NODE_BYTES), answered: within(answered, MAX_REFUSAL_ANSWERED_BYTES), reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// `Blocked` for the exact birth (`node_id`, `incarnation`) whose hook is blocked for `reason`
    /// until `retry_on`, the reason within its budget so the reply always fits the Status reply bound.
    pub fn blocked(node_id: NodeId, incarnation: IncarnationId, reason: &str, retry_on: RetryOn) -> Self {
        Self::Blocked { node_id, incarnation, reason: within(reason, MAX_BLOCKED_REASON_BYTES), retry_on }
    }

    /// `RejectedTags` naming why, within its budget so the reply always fits the Status reply bound.
    pub fn rejected_tags(reason: &str) -> Self {
        Self::RejectedTags { reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// `RejectedFilter` naming why, within [`MAX_REFUSAL_REASON_BYTES`].
    pub fn rejected_filter(reason: &str) -> Self {
        Self::RejectedFilter { reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }


    /// `NodeRowRefused` for `node`, each field within its budget (as [`Self::config_row_refused`]).
    pub fn node_row_refused(node: &str, reason: &str) -> Self {
        Self::NodeRowRefused { node: within(node, MAX_REFUSAL_NODE_BYTES), reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// `RejectedName` naming why, within its budget so the reply always fits the Status reply bound.
    pub fn rejected_name(reason: &str) -> Self {
        Self::RejectedName { reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// `RejectedAdmittingConnections` for a node of `status` still holding `held`, at most
    /// [`MAX_REPORTED_CONNECTIONS`] of them so the reply always fits the Status reply bound.
    pub fn rejected_admitting_connections(status: NodeStatus, held: &[ConnectionId]) -> Self {
        Self::RejectedAdmittingConnections { status, held_connections: held.iter().take(MAX_REPORTED_CONNECTIONS).cloned().collect() }
    }

    /// `BusyHoldingConnections` naming `held`, at most [`MAX_REPORTED_CONNECTIONS`] of them.
    pub fn busy_holding_connections(held: &[ConnectionId]) -> Self {
        Self::BusyHoldingConnections { connections: held.iter().take(MAX_REPORTED_CONNECTIONS).cloned().collect() }
    }

    /// `NameNotApplied` for `node`, each field within its budget (as [`Self::config_not_applied`]).
    pub fn name_not_applied(node: &str, answered: &str, reason: &str) -> Self {
        Self::NameNotApplied { node: within(node, MAX_REFUSAL_NODE_BYTES), answered: within(answered, MAX_REFUSAL_ANSWERED_BYTES), reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// `answer` to the read `read` when it encodes within the Status reply bound; past it,
    /// [`StatusReply::ReadOverBound`] naming the read, the answer's size and the bound, so the
    /// caller gets a named refusal and never a reset. An answer that does not encode at all is
    /// returned as it is: the server's own encode refuses it as a handler fault.
    pub fn within_reply_bound(answer: Self, read: &str) -> Self {
        match Status::encode_reply(&answer) {
            Ok(bytes) if bytes.len() > Status::MAX_REPLY_FRAME_BYTES => Self::ReadOverBound { read: read.to_string(), bytes: bytes.len() as u64, max: Status::MAX_REPLY_FRAME_BYTES as u64 },
            _ => answer,
        }
    }

    /// `RejectedSourceFailed` naming what failed (`source`), the operation on it (`op`) and why
    /// (`cause`), each within its budget so the reply always fits the reply bound.
    pub fn source_failed(source: &str, op: &str, cause: &str) -> Self {
        let (source, op, cause) = source_failed_fields(source, op, cause);
        Self::RejectedSourceFailed { source, op, cause }
    }

    /// `RejectedIncompleteManifest` naming every birth in `without_exit` (each a roster birth
    /// without an exact Exited receipt), cut to its budgets so the reply always fits the Status reply
    /// bound; `missing` keeps the whole count when the list is cut.
    pub fn incomplete_manifest(without_exit: &[String], reason: &str) -> Self {
        Self::RejectedIncompleteManifest {
            missing: u32::try_from(without_exit.len()).unwrap_or(u32::MAX),
            without_exit: without_exit.iter().take(MAX_WITHOUT_EXIT_BIRTHS).map(|b| within(b, MAX_WITHOUT_EXIT_BIRTH_BYTES)).collect(),
            reason: within(reason, MAX_INCOMPLETE_REASON_BYTES),
        }
    }

    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::AlreadyApplied => "already-applied",
            Self::Reserved2 { .. } => "reserved-2",
            Self::Current { .. } => "current",
            Self::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            Self::RejectedStaleMesh { .. } => "rejected-stale-mesh",
            Self::RejectedStaleFabric { .. } => "rejected-stale-fabric",
            Self::RejectedNotPrimary { .. } => "rejected-not-primary",
            Self::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            Self::RejectedInvalidMeshTransition { .. } => "rejected-invalid-mesh-transition",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::RejectedUnmatchedCompletion { .. } => "rejected-unmatched-completion",
            Self::Stopped { .. } => "stopped",
            Self::Started => "started",
            Self::StartFailed { .. } => "start-failed",
            Self::CurrentStopped { .. } => "current-stopped",
            Self::ConfigUpdated { .. } => "config-updated",
            Self::Reserved22 { .. } => "reserved-22",
            Self::ConfigRowRefused { .. } => "config-row-refused",
            Self::ConfigNotApplied { .. } => "config-not-applied",
            Self::Blocked { .. } => "blocked",
            Self::NameUpdated { .. } => "name-updated",
            Self::RejectedAdmittingConnections { .. } => "rejected-admitting-connections",
            Self::NameHeld { .. } => "name-held",
            Self::NodeRowRefused { .. } => "node-row-refused",
            Self::NameNotApplied { .. } => "name-not-applied",
            Self::ConnectionsDeleted { .. } => "connections-deleted",
            Self::RejectedConnectionScope { .. } => "rejected-connection-scope",
            Self::Nodes { .. } => "nodes",
            Self::Meshes { .. } => "meshes",
            Self::Fabric { .. } => "fabric",
            Self::Atlas { .. } => "atlas",
            Self::Connections { .. } => "connections",
            Self::UnknownMesh { .. } => "unknown-mesh",
            Self::ReadOverBound { .. } => "read-over-bound",
            Self::RejectedStaleWorkflow { .. } => "rejected-stale-workflow",
            Self::RejectedConflictingWorkflow { .. } => "rejected-conflicting-workflow",
            Self::RejectedIncompleteManifest { .. } => "rejected-incomplete-manifest",
            Self::RejectedSourceFailed { .. } => "rejected-source-failed",
            Self::RejectedTags { .. } => "rejected-tags",
            Self::Reserved44 { .. } => "reserved-44",
            Self::TagsCreated { .. } => "tags-created",
            Self::TagsDeleted { .. } => "tags-deleted",
            Self::RejectedName { .. } => "rejected-name",
            Self::BusyHoldingConnections { .. } => "busy-holding-connections",
            Self::MeshJoined { .. } => "mesh-joined",
            Self::MeshLeft { .. } => "mesh-left",
            Self::RequestNotHandled { .. } => "request-not-handled",
            Self::CertRenewed { .. } => "cert-renewed",
            Self::IssuerUpdated { .. } => "issuer-updated",
            Self::RejectedCert { .. } => "rejected-cert",
            Self::Fabrics { .. } => "fabrics",
            Self::RejectedFilter { .. } => "rejected-filter",
            Self::RootsUpdated { .. } => "roots-updated",
            Self::RejectedRootStillChained { .. } => "root-still-chained",
            Self::RootUpdated { .. } => "fabric.root.updated",
            Self::RejectedRotationInProgress { .. } => "rotation-in-progress",
            Self::RejectedRootAlreadyTrusted { .. } => "root-already-trusted",
        }
    }
}

impl StatusReply {
    /// The result for a label in the served request. An unlisted requested label was applied;
    /// the existing exception lists prove already-carried and not-carried without adding bytes.
    pub fn tag_outcome<'a>(&self, label: &'a str) -> Option<crate::outcome::ElementOutcome<'a, str>> {
        use crate::outcome::{Code, ElementDetail, ElementOutcome};
        let (code, detail) = match self {
            Self::TagsCreated { already_carried, .. } if already_carried.contains(label) => (Code::AlreadyExists, ElementDetail::AlreadyCarried),
            Self::TagsDeleted { not_carried, .. } if not_carried.contains(label) => (Code::NotFound, ElementDetail::NotCarried),
            Self::TagsCreated { .. } | Self::TagsDeleted { .. } => (Code::Ok, ElementDetail::Applied),
            _ => return None,
        };
        Some(ElementOutcome { element: label, code, detail })
    }

    /// Codes for the ids the served named-connection answer accounts for, in its existing lists.
    pub fn connection_outcomes(&self) -> Vec<crate::outcome::ElementOutcome<'_, ConnectionId>> {
        match self {
            Self::ConnectionsDeleted { closed, not_open, .. } => crate::outcome::connection_outcomes(closed, not_open),
            _ => Vec::new(),
        }
    }
}

impl NodeProtocol for Status {
    const OP: u8 = 0x1B;
    const NAME: &'static str = "status";
    /// `MAX_STATUS_REQUEST_BYTES`.
    const MAX_REQUEST_FRAME_BYTES: usize = MAX_STATUS_REQUEST_BYTES;
    /// A `node.connections.deleted` names every connection it accounts for by id: the bound holds
    /// [`MAX_REPORTED_CONNECTIONS`] of them. A read's answer past it is
    /// [`StatusReply::ReadOverBound`] ([`StatusReply::within_reply_bound`]).
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    /// A declaration travels the ordinary Direct/ViaPeer route like any other call.
    const FORWARDABLE: bool = true;
    /// A connections-deleting node still answers its authority's probe and apply.
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = true;
    const REQUEST_VARIANTS: u32 = 37;
    const REPLY_VARIANTS: u32 = 63;

    type Request = StatusRequest;
    type Reply = StatusReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            StatusReply::Applied => "applied",
            StatusReply::AlreadyApplied => "already-applied",
            StatusReply::Reserved2 { .. } => "reserved2",
            StatusReply::Current { .. } => "current",
            StatusReply::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            StatusReply::RejectedStaleMesh { .. } => "rejected-stale-mesh",
            StatusReply::RejectedStaleFabric { .. } => "rejected-stale-fabric",
            StatusReply::RejectedNotPrimary { .. } => "rejected-not-primary",
            StatusReply::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            StatusReply::RejectedInvalidMeshTransition { .. } => "rejected-invalid-mesh-transition",
            StatusReply::PeerUnresolved { .. } => "peer-unresolved",
            StatusReply::NotReady { .. } => "not-ready",
            StatusReply::Busy { .. } => "busy",
            StatusReply::ConnectionsDeleting { .. } => "connections-deleting",
            StatusReply::Malformed { .. } => "malformed",
            StatusReply::Unauthorized { .. } => "unauthorized",
            StatusReply::RejectedUnmatchedCompletion { .. } => "rejected-unmatched-completion",
            StatusReply::Stopped { .. } => "stopped",
            StatusReply::Started => "started",
            StatusReply::StartFailed { .. } => "start-failed",
            StatusReply::CurrentStopped { .. } => "current-stopped",
            StatusReply::ConfigUpdated { .. } => "config-updated",
            StatusReply::Reserved22 { .. } => "reserved22",
            StatusReply::ConfigRowRefused { .. } => "config-row-refused",
            StatusReply::ConfigNotApplied { .. } => "config-not-applied",
            StatusReply::Blocked { .. } => "blocked",
            StatusReply::NameUpdated { .. } => "name-updated",
            StatusReply::RejectedAdmittingConnections { .. } => "rejected-admitting-connections",
            StatusReply::NameHeld { .. } => "name-held",
            StatusReply::NodeRowRefused { .. } => "node-row-refused",
            StatusReply::NameNotApplied { .. } => "name-not-applied",
            StatusReply::ConnectionsDeleted { .. } => "connections-deleted",
            StatusReply::RejectedConnectionScope { .. } => "rejected-connection-scope",
            StatusReply::Nodes { .. } => "nodes",
            StatusReply::Meshes { .. } => "meshes",
            StatusReply::Fabric { .. } => "fabric",
            StatusReply::Atlas { .. } => "atlas",
            StatusReply::Connections { .. } => "connections",
            StatusReply::UnknownMesh { .. } => "unknown-mesh",
            StatusReply::ReadOverBound { .. } => "read-over-bound",
            StatusReply::RejectedStaleWorkflow { .. } => "rejected-stale-workflow",
            StatusReply::RejectedConflictingWorkflow { .. } => "rejected-conflicting-workflow",
            StatusReply::RejectedIncompleteManifest { .. } => "rejected-incomplete-manifest",
            StatusReply::RejectedSourceFailed { .. } => "rejected-source-failed",
            StatusReply::RejectedTags { .. } => "rejected-tags",
            StatusReply::Reserved44 { .. } => "reserved44",
            StatusReply::TagsCreated { .. } => "tags-created",
            StatusReply::TagsDeleted { .. } => "tags-deleted",
            StatusReply::RejectedName { .. } => "rejected-name",
            StatusReply::BusyHoldingConnections { .. } => "busy-holding-connections",
            StatusReply::MeshJoined { .. } => "mesh-joined",
            StatusReply::MeshLeft { .. } => "mesh-left",
            StatusReply::RequestNotHandled { .. } => "request-not-handled",
            StatusReply::CertRenewed { .. } => "cert-renewed",
            StatusReply::IssuerUpdated { .. } => "issuer-updated",
            StatusReply::RejectedCert { .. } => "rejected-cert",
            StatusReply::Fabrics { .. } => "fabrics",
            StatusReply::RejectedFilter { .. } => "rejected-filter",
            StatusReply::RootsUpdated { .. } => "roots-updated",
            StatusReply::RejectedRootStillChained { .. } => "root-still-chained",
            StatusReply::RootUpdated { .. } => "fabric.root.updated",
            StatusReply::RejectedRotationInProgress { .. } => "rotation-in-progress",
            StatusReply::RejectedRootAlreadyTrusted { .. } => "root-already-trusted",
        }
    }

    fn classify_reply(reply: &StatusReply) -> ReplyKind {
        match reply {
            StatusReply::Applied  => ReplyKind::Success,
            StatusReply::AlreadyApplied  => ReplyKind::new(crate::outcome::Code::AlreadyExists),
            StatusReply::Reserved2 { .. } => ReplyKind::Success,
            StatusReply::Current { .. } => ReplyKind::Success,
            StatusReply::RejectedStaleIncarnation { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            StatusReply::RejectedStaleMesh { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            StatusReply::RejectedStaleFabric { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown { .. } } => ReplyKind::new(crate::outcome::Code::NotFound),
            StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::RejectedNotPrimary { why: NotPrimary::SenderNotSubject { .. } } => ReplyKind::new(crate::outcome::Code::PermissionDenied),
            StatusReply::RejectedInvalidNodeTransition { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::RejectedInvalidMeshTransition { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            StatusReply::NotReady { .. } => ReplyKind::NotReady,
            StatusReply::Busy { .. } => ReplyKind::Busy,
            StatusReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            StatusReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            StatusReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            StatusReply::RejectedUnmatchedCompletion { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::Stopped { receipt: ConnectionsReceipt::Deadline { .. } }
            | StatusReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Deadline { .. }), .. } => ReplyKind::new(crate::outcome::Code::DeadlineExceeded),
            StatusReply::Stopped { .. } => ReplyKind::Success,
            StatusReply::Started  => ReplyKind::Success,
            StatusReply::StartFailed { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::CurrentStopped { .. } => ReplyKind::Success,
            StatusReply::ConfigUpdated { .. } => ReplyKind::Success,
            StatusReply::Reserved22 { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::ConfigRowRefused { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::ConfigNotApplied { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::Blocked { .. } => ReplyKind::Success,
            StatusReply::NameUpdated { .. } => ReplyKind::Success,
            StatusReply::RejectedAdmittingConnections { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::NameHeld { .. } => ReplyKind::new(crate::outcome::Code::AlreadyExists),
            StatusReply::NodeRowRefused { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::NameNotApplied { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::ConnectionsDeleted { .. } => ReplyKind::Success,
            StatusReply::RejectedConnectionScope { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            StatusReply::Nodes { .. } => ReplyKind::Success,
            StatusReply::Meshes { .. } => ReplyKind::Success,
            StatusReply::Fabric { .. } => ReplyKind::Success,
            StatusReply::Atlas { .. } => ReplyKind::Success,
            StatusReply::Connections { .. } => ReplyKind::Success,
            StatusReply::UnknownMesh { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
            StatusReply::ReadOverBound { .. } => ReplyKind::new(crate::outcome::Code::ResourceExhausted),
            StatusReply::RejectedStaleWorkflow { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            StatusReply::RejectedConflictingWorkflow { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::RejectedIncompleteManifest { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::RejectedSourceFailed { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::RejectedTags { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            StatusReply::Reserved44 { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::TagsCreated { .. } => ReplyKind::Success,
            StatusReply::TagsDeleted { .. } => ReplyKind::Success,
            StatusReply::RejectedName { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            StatusReply::BusyHoldingConnections { .. } => ReplyKind::new(crate::outcome::Code::Unavailable),
            StatusReply::MeshJoined { .. } => ReplyKind::Success,
            StatusReply::MeshLeft { .. } => ReplyKind::Success,
            StatusReply::RequestNotHandled { .. } => ReplyKind::new(crate::outcome::Code::Unimplemented),
            StatusReply::CertRenewed { .. } => ReplyKind::Success,
            StatusReply::IssuerUpdated { .. } => ReplyKind::Success,
            StatusReply::RejectedCert { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            StatusReply::Fabrics { .. } => ReplyKind::Success,
            StatusReply::RejectedFilter { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            StatusReply::RootsUpdated { .. } => ReplyKind::Success,
            StatusReply::RejectedRootStillChained { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            StatusReply::RootUpdated { .. } => ReplyKind::Success,
            StatusReply::RejectedRotationInProgress { .. } | StatusReply::RejectedRootAlreadyTrusted { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
        }
    }

    fn peer_unresolved(reason: String) -> StatusReply {
        StatusReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> StatusReply {
        StatusReply::NotReady { reason }
    }
    fn busy(reason: String) -> StatusReply {
        StatusReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> StatusReply {
        StatusReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> StatusReply {
        StatusReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> StatusReply {
        StatusReply::Unauthorized { reason }
    }
}

/// The one transition rule, for every scope: the same state is already applied, a forward move
/// (skips included) applies, a backward move is refused naming the current state.
pub fn transition<S: Ord + Copy>(current: Option<S>, declared: S) -> Transition<S> {
    match current {
        None => Transition::Apply,
        Some(c) if c == declared => Transition::AlreadyApplied,
        Some(c) if c < declared => Transition::Apply,
        Some(c) => Transition::Backward { current: c },
    }
}

/// The result of checking a declared state against the held one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition<S> {
    /// The declared state is a legal forward move; apply it.
    Apply,
    /// The declared state is already held.
    AlreadyApplied,
    /// The declared state is a move backward.
    Backward {
        /// The state held.
        current: S,
    },
}


/// What a node declared, as its mesh-primary's view holds it: the one mapping from the declaration
/// vocabulary to the member status ([`declared_of_member`] is its inverse).
pub fn member_of_declared(s: NodeState) -> crate::MemberStatus {
    use crate::MemberStatus as M;
    match s {
        NodeState::Pending => M::Pending,
        NodeState::Ready => M::Ready,
        NodeState::ConnectionsDeleting => M::ConnectionsDeleting,
        NodeState::Stopped => M::Leaving,
    }
}

/// A member status in the declaration vocabulary: the inverse of [`member_of_declared`].
pub fn declared_of_member(s: crate::MemberStatus) -> NodeState {
    use crate::MemberStatus as M;
    match s {
        M::Pending => NodeState::Pending,
        M::Ready => NodeState::Ready,
        M::ConnectionsDeleting => NodeState::ConnectionsDeleting,
        M::Leaving => NodeState::Stopped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip() {
        let all = [
            (StatusReply::Applied, ReplyKind::Success),
            (StatusReply::AlreadyApplied, ReplyKind::new(crate::outcome::Code::AlreadyExists)),
            (StatusReply::Reserved2 { in_flight: 3 }, ReplyKind::Success),
            (StatusReply::Current { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::Ready }, ReplyKind::Success),
            (StatusReply::RejectedStaleIncarnation { held: IncarnationId::mint() }, ReplyKind::new(crate::outcome::Code::Aborted)),
            (StatusReply::RejectedStaleMesh { held: MeshId::mint() }, ReplyKind::new(crate::outcome::Code::Aborted)),
            (StatusReply::RejectedStaleFabric { held: FabricId::mint() }, ReplyKind::new(crate::outcome::Code::Aborted)),
            (StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, ReplyKind::new(crate::outcome::Code::NotFound)),
            (StatusReply::RejectedInvalidNodeTransition { current: NodeState::Stopped }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::RejectedInvalidMeshTransition { current: MeshState::Dead }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (Status::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (Status::not_ready("n".into()), ReplyKind::NotReady),
            (Status::busy("b".into()), ReplyKind::Busy),
            (Status::connections_deleting("d".into()), ReplyKind::ConnectionsDeleting),
            (Status::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (Status::unauthorized("u".into()), ReplyKind::Unauthorized),
            (StatusReply::RejectedUnmatchedCompletion { field: "attempt".into(), expected: "1".into(), reported: "2".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::Stopped { receipt: ConnectionsReceipt::Deadline { last_in_flight: 2, closed: vec![ConnectionId::mint()], held: vec![ConnectionId::mint()] } }, ReplyKind::new(crate::outcome::Code::DeadlineExceeded)),
            (StatusReply::Started, ReplyKind::Success),
            (StatusReply::StartFailed { step: "node.join".into(), reason: "refused".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::CurrentStopped { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, ReplyKind::Success),
            (StatusReply::ConfigUpdated { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: Default::default(), revision: 1 }, ReplyKind::Success),
            (StatusReply::Reserved22 { keys: 33, bytes: 10, max_keys: 32, max_bytes: 2048 }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::ConfigRowRefused { node: "mesh1.rpc.1".into(), reason: "io".into(), config: Default::default(), revision: 0 }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::ConfigNotApplied { node: "mesh1.rpc.1".into(), answered: "busy".into(), reason: "b".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::Blocked { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), reason: "rows not yet published".into(), retry_on: RetryOn::TargetReachable { node_id: NodeId::mint() } }, ReplyKind::Success),
            (StatusReply::NameUpdated { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, ReplyKind::Success),
            (StatusReply::RejectedAdmittingConnections { status: crate::views::NodeStatus::ConnectionsDeleting, held_connections: vec![ConnectionId::mint()] }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::NameHeld { name: "mesh1.rpc.1".into(), by: NodeId::mint() }, ReplyKind::new(crate::outcome::Code::AlreadyExists)),
            (StatusReply::NodeRowRefused { node: "mesh1.rpc.1".into(), reason: "io".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::NameNotApplied { node: "mesh1.rpc.1".into(), answered: "busy".into(), reason: "b".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::ConnectionsDeleted { closed: vec![ConnectionId::mint()], not_open: vec![ConnectionId::mint()], held: vec![], receipt: None }, ReplyKind::Success),
            (StatusReply::RejectedConnectionScope { reason: "empty".into() }, ReplyKind::new(crate::outcome::Code::InvalidArgument)),
            (StatusReply::Nodes { nodes: vec![node_view()] }, ReplyKind::Success),
            (StatusReply::Meshes { meshes: vec![mesh_view()] }, ReplyKind::Success),
            (StatusReply::Fabric { fabric: crate::views::FabricView { next_expiry: None, topology_storage_provider: None, roots: None, ca: None, previous_topology_storage_providers: Vec::new(), id: FabricId::mint(), name: "fabric1".into(), status: crate::views::ScopeStatus::Pending, provider: crate::views::ProviderKind::Process, fabric_primary: Some("mesh1.admin.1".parse().unwrap()), admin_api_base: None, meshes: vec![mesh_view()], build_id: Some("3qcfk1kq47m9".into()) } }, ReplyKind::Success),
            (StatusReply::Connections { connections: crate::views::ConnectionsView { node: "mesh1.rpc.1".into(), complete: true, connections: vec![connection_view()] } }, ReplyKind::Success),
            (StatusReply::UnknownMesh { mesh: "mesh9".into() }, ReplyKind::new(crate::outcome::Code::NotFound)),
            (StatusReply::ReadOverBound { read: "nodes.get".into(), bytes: 70_000, max: 65_536 }, ReplyKind::new(crate::outcome::Code::ResourceExhausted)),
            (StatusReply::RejectedStaleWorkflow { held: MeshDeleteIdentity { build_id: "66h8qrqvaajk".into(), attempt: 2, operation: "mesh.delete:m".into() }, offered: MeshDeleteIdentity { build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "mesh.delete:m".into() } }, ReplyKind::new(crate::outcome::Code::Aborted)),
            (StatusReply::RejectedConflictingWorkflow { held: MeshDeleteIdentity { build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "mesh.delete:m".into() }, offered: MeshDeleteIdentity { build_id: "h5g70m03ktnb".into(), attempt: 1, operation: "mesh.delete:m".into() } }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::incomplete_manifest(&["mesh2.rpc.1 i1".into()], "no exit manifest"), ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::source_failed("the Build log of mesh2.admin.1", "fetch-build-facts 66h8qrqvaajk", "unreached: no path"), ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::rejected_tags("the tag label \"Rabbit\" is not lowercase ASCII letters, digits and '-'"), ReplyKind::new(crate::outcome::Code::InvalidArgument)),
            (StatusReply::Reserved44 { workflow: String::new(), status: String::new() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::TagsCreated { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), already_carried: ["red".to_string()].into() }, ReplyKind::Success),
            (StatusReply::TagsDeleted { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), not_carried: ["blue".to_string()].into() }, ReplyKind::Success),
            (StatusReply::RejectedName { reason: "r".into() }, ReplyKind::new(crate::outcome::Code::InvalidArgument)),
            (StatusReply::BusyHoldingConnections { connections: vec![ConnectionId::mint()] }, ReplyKind::new(crate::outcome::Code::Unavailable)),
            (StatusReply::MeshJoined { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), joined: vec![WhichMesh::Own, WhichMesh::Backbone] }, ReplyKind::Success),
            (StatusReply::MeshLeft { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), left: vec![WhichMesh::Own] }, ReplyKind::Success),
            (StatusReply::RequestNotHandled { request: "declare-node-state".into() }, ReplyKind::new(crate::outcome::Code::Unimplemented)),
            (StatusReply::CertRenewed { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), expires_at: 7 }, ReplyKind::Success),
            (StatusReply::IssuerUpdated { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), issuer_expires_at: 7 }, ReplyKind::Success),
            (StatusReply::RejectedCert { reason: "x".into() }, ReplyKind::new(crate::outcome::Code::InvalidArgument)),
            (StatusReply::Atlas { atlas: crate::views::AtlasView::default() }, ReplyKind::Success),
            (StatusReply::Fabrics { fabrics: vec![] }, ReplyKind::Success),
            (StatusReply::rejected_filter("nodes.get: the pair (zone, [eu-1]) names no property of a node row"), ReplyKind::new(crate::outcome::Code::InvalidArgument)),
            (StatusReply::RootsUpdated { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), roots: vec!["ab".into()] }, ReplyKind::Success),
            (StatusReply::RejectedRootStillChained { root: "ab".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::RootUpdated { phase: RootsPhase::Minted }, ReplyKind::Success),
            (StatusReply::RejectedRotationInProgress { phase: RootsPhase::Distributed }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (StatusReply::RejectedRootAlreadyTrusted { root: "ab".into() }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
        ];
        assert_eq!(all.len() as u32, Status::REPLY_VARIANTS);
        for (r, kind) in all {
            let back = Status::decode_reply(&Status::encode_reply(&r).unwrap()).unwrap();
            assert_eq!(back, r);
            assert_eq!(Status::classify_reply(&back), kind);
        }
        let reqs = [
            StatusRequest::DeclareNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::Ready, operation: None },
            StatusRequest::ApplyNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::ConnectionsDeleting },
            StatusRequest::ProbeNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::Ready, changed_at_rafka_ms: 0 },
            StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh2".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 },
            StatusRequest::ApplyFabricEvent { fabric_id: FabricId::mint(), event: FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() } },
            StatusRequest::Reserved6 { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "reserved-6:mesh1.rpc.1".into() },
            StatusRequest::Reserved7 { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "reserved-7:mesh1.rpc.1".into() },
            StatusRequest::StopNode { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() },
            StatusRequest::NodeLeft { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() },
            StatusRequest::DeleteMesh { mesh_id: MeshId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "mesh.delete:m".into() },
            StatusRequest::MeshDeleted {
                mesh_id: MeshId::mint(),
                build_id: "66h8qrqvaajk".into(),
                attempt: 1,
                operation: "mesh.delete:m".into(),
                final_node_id: NodeId::mint(),
                final_incarnation: IncarnationId::mint(),
                final_runtime: RuntimeFact { deployment_id: crate::fixture_crockford60("dep"), provider: crate::RuntimeProvider::Process, control_domain: "process:boot-a:pidns-a".into(), locator: crate::RuntimeLocator::Process { pid: 4321, start: 123456 } },
                receipt_manifest: "66h8qrqvaajk/1/mesh.delete:m/other-members-exited".into(),
            },
            StatusRequest::Reserved12 { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "commit-state:fabric1".into() },
            StatusRequest::Reserved13 { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "commit-state:fabric1".into() },
            StatusRequest::Reserved14 { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "open-traffic:fabric1".into() },
            StatusRequest::Reserved15 { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "open-traffic:fabric1".into() },
            StatusRequest::StartNode { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.start:mesh1.rpc.1".into() },
            StatusRequest::UpdateNodeConfig { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: [("k".to_string(), "v".to_string())].into() },
            StatusRequest::ApplyNodeConfig { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: Default::default(), revision: 1 },
            StatusRequest::NodeConfigUpdated { node_id: NodeId::mint(), config: Default::default(), revision: 1 },
            StatusRequest::ApplyNodeName { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.1".into() },
            StatusRequest::UpdateName { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "1.old".into() },
            StatusRequest::DeleteConnections { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), scope: ConnectionScope::Connections(vec![ConnectionId::mint()]) },
            StatusRequest::GetNodes { select: NodeSelect::Tags(TagFilter::All(["rabbit".to_string(), "red".to_string()].into())) },
            StatusRequest::GetMeshes { mesh: None },
            StatusRequest::GetFabric,
            StatusRequest::GetAtlas,
            StatusRequest::GetConnections { node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            StatusRequest::CreateTags { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), labels: ["rabbit".to_string()].into() },
            StatusRequest::DeleteTags { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), scope: TagScope::Labels(["rabbit".to_string()].into()) },
            StatusRequest::JoinMesh { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "join-mesh:mesh1.rpc.1".into() },
            StatusRequest::LeaveMesh { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "leave-mesh:mesh1.rpc.1".into() },
            StatusRequest::RenewCert { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), cert: vec![1, 2, 3] },
            StatusRequest::UpdateIssuer { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), key: IssuingKeyHex("ab".into()), cert: "cd".into(), ca_key: CaKeyHex("ef".into()) },
            StatusRequest::Get { entity: crate::filter::Entity::Nodes, node: None, filter: crate::filter::Filter::new().eq("config.zone", ["eu-1"]).eq("tags", ["gold", "silver"]) },
            StatusRequest::UpdateRoots { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), roots: vec!["ab".into()], phase: RootsPhase::Distributed, secret: Some(RootSecretHex("cd".into())) },
            StatusRequest::UpdateRoot { secret: RootSecretHex("ab".into()) },
        ];
        assert_eq!(reqs.len() as u32, Status::REQUEST_VARIANTS);
        for q in reqs {
            assert_eq!(Status::decode_request(&Status::encode_request(&q).unwrap()).unwrap(), q);
        }
        // A declaration naming the operation that produced its state (i146 Q36).
        let named = StatusRequest::DeclareNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::Stopped, operation: Some(DeclaredOperation { build_id: "66h8qrqvaajk".into(), attempt: 2, operation: "node.stop:mesh1.rpc.1".into() }) };
        assert_eq!(Status::decode_request(&Status::encode_request(&named).unwrap()).unwrap(), named);
    }

    /// CONTRACT (R-TG1; node-rpc-envelope.md node.tags: "A label is 1-64 characters of a-z 0-9 - _ .
    /// ~, with no leading '.', and a node carries at most 32"): a `CreateTags` and a `DeleteTags`
    /// carrying the most labels one node carries, each at its longest, encode within the Status request
    /// bound, as do a `GetNodes` filter naming as many and an `UpdateName` at the longest name.
    /// Anything outside the grammar, a set past `MAX_NODE_TAGS`, an empty filter, an empty create and
    /// an empty `Labels` delete are refused by name, the label named.
    #[test]
    fn a_tag_set_at_its_cap_encodes_within_the_status_request_bound_and_a_non_label_is_refused_by_name() {
        let at_cap: BTreeSet<String> = (0..MAX_NODE_TAGS).map(|i| format!("{i:0>width$}", width = MAX_TAG_BYTES)).collect();
        assert_eq!((at_cap.len(), tags_refusal(&at_cap)), (MAX_NODE_TAGS, None));
        let longest_name = "n".repeat(MAX_TAG_BYTES);
        assert_eq!(name_refusal(&longest_name), None);
        for q in [
            StatusRequest::UpdateName { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: longest_name.clone() },
            StatusRequest::CreateTags { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), labels: at_cap.clone() },
            StatusRequest::DeleteTags { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), scope: TagScope::Labels(at_cap.clone()) },
            StatusRequest::GetNodes { select: NodeSelect::Tags(TagFilter::Any(at_cap.clone())) },
            StatusRequest::GetNodes { select: NodeSelect::MeshTags("m".repeat(MAX_TAG_BYTES), TagFilter::Any(at_cap.clone())) },
        ] {
            let len = Status::encode_request(&q).unwrap().len();
            assert!(len <= Status::MAX_REQUEST_FRAME_BYTES, "{} encodes to {len}, over {}", q.op(), Status::MAX_REQUEST_FRAME_BYTES);
        }
        for ok in ["rabbit", "red", "zone-a", "7", "a-1-b", "zone_a", "a.b", "v1.2~rc", "-red", "~x", "_x"] {
            assert_eq!(tag_refusal(ok), None, "{ok}");
            assert_eq!(name_refusal(ok), None, "a name takes the tag-label grammar (§9 A): {ok}");
        }
        for (bad, says) in [("", "empty"), ("Rpc", "\"Rpc\""), (".old", "\".old\""), ("a b", "\"a b\"")] {
            assert!(name_refusal(bad).is_some_and(|r| r.contains(says)), "{bad:?}: {:?}", name_refusal(bad));
        }
        assert!(name_refusal(&"x".repeat(MAX_TAG_BYTES + 1)).is_some_and(|r| r.contains("over the 64-byte cap")));
        for (bad, says) in [("", "empty"), ("Rabbit", "\"Rabbit\""), (".red", "\".red\""), ("a/b", "\"a/b\""), ("a b", "\"a b\""), ("zone\ta", "\"zone\\ta\"")] {
            assert!(tag_refusal(bad).is_some_and(|r| r.contains(says)), "{bad:?}: {:?}", tag_refusal(bad));
        }
        assert!(tag_refusal(&"x".repeat(MAX_TAG_BYTES + 1)).is_some_and(|r| r.contains("over the 64-byte cap")));
        let over: BTreeSet<String> = (0..=MAX_NODE_TAGS).map(|i| format!("t{i}")).collect();
        assert!(tags_refusal(&over).is_some_and(|r| r.contains("33 tag labels, over the 32")));
        assert!(TagFilter::All(BTreeSet::new()).refusal().is_some_and(|r| r.contains("names no label")));
        assert!(TagFilter::Any(["Red".to_string()].into()).refusal().is_some_and(|r| r.contains("\"Red\"")));
        assert!(create_tags_refusal(&BTreeSet::new()).is_some_and(|r| r.contains("node.tags.create names no label")));
        assert!(create_tags_refusal(&["Red".to_string()].into()).is_some_and(|r| r.contains("\"Red\"")));
        assert_eq!(TagScope::All.refusal(), None);
        assert!(TagScope::Labels(BTreeSet::new()).refusal().is_some_and(|r| r.contains("node.tags.delete names no label")));
        assert!(TagScope::Labels(over.clone()).refusal().is_some_and(|r| r.contains("33 tag labels, over the 32")));
        let long = "x".repeat(4 * Status::MAX_REPLY_FRAME_BYTES);
        let len = Status::encode_reply(&StatusReply::rejected_tags(&long)).unwrap().len();
        assert!(len <= 1024, "rejected-tags encodes to {len}, over 1024");
    }

    /// CONTRACT (an error carries every detail it can, and still reaches the caller): a config refusal
    /// whose detail is longer than the reply bound is still a reply within `MAX_REPLY_FRAME_BYTES`, so
    /// the server never resets it as a protocol violation and the caller gets the named arm.
    #[test]
    fn a_config_refusal_with_a_long_detail_encodes_within_the_reply_bound() {
        let long = "x".repeat(4 * Status::MAX_REPLY_FRAME_BYTES);
        for r in [StatusReply::config_row_refused(&long, &long), StatusReply::config_not_applied(&long, &long, &long)] {
            let len = Status::encode_reply(&r).unwrap().len();
            assert!(len <= Status::MAX_REPLY_FRAME_BYTES, "{} encodes to {len}, over {}", r.name(), Status::MAX_REPLY_FRAME_BYTES);
        }
        let short = StatusReply::config_not_applied("mesh1.rpc.1", "busy", "at its admission bound");
        assert_eq!(short, StatusReply::ConfigNotApplied { node: "mesh1.rpc.1".into(), answered: "busy".into(), reason: "at its admission bound".into() }, "a detail within its budget is kept whole");
    }

    /// CONTRACT (an error carries every detail it can, and still reaches the caller): a
    /// `RejectedSourceFailed` whose source, operation and cause are each longer than the reply bound
    /// is still a reply within 1024 bytes (the smaller of the two families that carry it), each field
    /// cut at its budget and ending `…`; fields within their budgets are kept whole.
    #[test]
    fn a_source_failure_with_long_fields_encodes_within_the_reply_bound() {
        let long = "x".repeat(4 * Status::MAX_REPLY_FRAME_BYTES);
        let r = StatusReply::source_failed(&long, &long, &long);
        let len = Status::encode_reply(&r).unwrap().len();
        assert!(len <= 1024, "{} encodes to {len}, over 1024", r.name());
        let StatusReply::RejectedSourceFailed { source, op, cause } = &r else { unreachable!() };
        assert!(source.ends_with('…') && op.ends_with('…') && cause.ends_with('…'), "a cut field says it was cut");
        let short = StatusReply::source_failed("fabric.storage of mesh1.admin.1", "put", "disk full");
        assert_eq!(short, StatusReply::RejectedSourceFailed { source: "fabric.storage of mesh1.admin.1".into(), op: "put".into(), cause: "disk full".into() }, "fields within their budgets are kept whole");
    }

    /// CONTRACT (a hook's blocker reaches its executor whole or cut, never as a reset): a `Blocked`
    /// answer whose hook reason is longer than the reply bound, from a birth with minted ids and the
    /// largest retry event, is still a reply within `MAX_REPLY_FRAME_BYTES`; a reason within its budget
    /// is kept whole.
    #[test]
    fn a_blocked_answer_with_a_long_hook_reason_encodes_within_the_reply_bound() {
        let long = "x".repeat(4 * Status::MAX_REPLY_FRAME_BYTES);
        let r = StatusReply::blocked(NodeId::mint(), IncarnationId::mint(), &long, RetryOn::TargetReachable { node_id: NodeId::mint() });
        let len = Status::encode_reply(&r).unwrap().len();
        assert!(len <= Status::MAX_REPLY_FRAME_BYTES, "blocked encodes to {len}, over {}", Status::MAX_REPLY_FRAME_BYTES);
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let short = StatusReply::blocked(node_id.clone(), incarnation.clone(), "rows not yet published", RetryOn::PrimaryAddressed);
        assert_eq!(short, StatusReply::Blocked { node_id, incarnation, reason: "rows not yet published".into(), retry_on: RetryOn::PrimaryAddressed }, "a reason within its budget is kept whole");
    }

    /// CONTRACT (R-CR3: the completion names every id it accounts for; a request names at most
    /// `MAX_CONNECTION_IDS`): a `DeleteConnections` naming the most ids one request carries fits the
    /// Status request bound, and a `ConnectionsDeleted` naming `MAX_REPORTED_CONNECTIONS` ids fits the
    /// reply bound. An empty list and one past the cap are refused by name; `All` never is.
    #[test]
    fn a_connections_delete_at_its_caps_encodes_within_the_status_bounds() {
        let ids = |n: usize| (0..n).map(|_| ConnectionId::mint()).collect::<Vec<_>>();
        let q = StatusRequest::DeleteConnections { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), scope: ConnectionScope::Connections(ids(MAX_CONNECTION_IDS)) };
        let len = Status::encode_request(&q).unwrap().len();
        assert!(len <= MAX_DECLARATION_REQUEST_BYTES, "{MAX_CONNECTION_IDS} ids encode to {len}, over {MAX_DECLARATION_REQUEST_BYTES}");
        let r = StatusReply::ConnectionsDeleted { closed: ids(MAX_REPORTED_CONNECTIONS - 2), not_open: ids(1), held: ids(1), receipt: None };
        let len = Status::encode_reply(&r).unwrap().len();
        assert!(len <= Status::MAX_REPLY_FRAME_BYTES, "{MAX_REPORTED_CONNECTIONS} ids encode to {len}, over {}", Status::MAX_REPLY_FRAME_BYTES);
        // The rename's two refusals naming held connections are capped at the same count.
        for r in [StatusReply::rejected_admitting_connections(crate::views::NodeStatus::PendingReconnect, &ids(MAX_REPORTED_CONNECTIONS + 1)), StatusReply::busy_holding_connections(&ids(MAX_REPORTED_CONNECTIONS + 1))] {
            let len = Status::encode_reply(&r).unwrap().len();
            assert!(len <= Status::MAX_REPLY_FRAME_BYTES, "{} encodes to {len}, over {}", r.name(), Status::MAX_REPLY_FRAME_BYTES);
        }
        assert!(ConnectionScope::Connections(vec![]).refusal().is_some_and(|r| r.contains("at least one")));
        assert!(ConnectionScope::Connections(ids(MAX_CONNECTION_IDS + 1)).refusal().is_some_and(|r| r.contains("over the")));
        assert_eq!(ConnectionScope::All.refusal(), None);
        assert_eq!(ConnectionScope::Connections(ids(1)).refusal(), None);
    }

    fn node_view() -> crate::views::NodeView {
        crate::views::NodeView { cert: None, executable: None, storage_persistence: None, topology_storage_provider: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
            name: "mesh1.rpc.12".parse().unwrap(),
            kind: crate::NodeKind::RpcNode,
            mesh: "mesh1".into(),
            node_id: NodeId::mint(),
            endpoint_id: Some("e".repeat(64)),
            incarnation_id: Some(IncarnationId::mint()),
            deployment_id: Some("d".repeat(32)),
            provider: Some(crate::views::ProviderKind::Process),
            data_dir: Some("/var/lib/rdm/fabric1/mesh1.rpc.12".into()),
            status: crate::views::NodeStatus::Ready,
            is_primary: false,
            is_fabric_primary: false,
            admin_api_base: None,
            transport_addr: Some("127.0.0.1:41235".parse().unwrap()),
            listeners: vec![("control".into(), "127.0.0.1:41236".parse().unwrap())],
            declared: Some("Ready".into()),
            stopped: false,
            load: Some(crate::NodeLoad { cpu_used_millicores: 2400, cpu_budget_millicores: 16000, ram_used_bytes: 123_456_789, ram_budget_bytes: 64_000_000_000 }),
            gossip: Some(crate::GossipStats { heard: 20, neighbours: 5, frames_sent: 123_456, frames_received: 654_321 }),
            tags: ["rabbit".to_string(), "zone-a".to_string()].into(),
            config: [("tier".to_string(), "gold".to_string())].into(),
            data_dir_lost: None,
            data_dir_unreadable: None,
        }
    }

    fn mesh_view() -> crate::views::MeshView {
        crate::views::MeshView { renewals: None, id: Some(MeshId::mint()), name: "mesh1".into(), status: crate::views::ScopeStatus::Ready, primary_admin: Some("mesh1.admin.1".parse().unwrap()), admin_api_base: None, nodes: vec!["mesh1.admin.1".parse().unwrap(), "mesh1.rpc.1".parse().unwrap()] }
    }

    fn connection_view() -> crate::views::ConnectionView {
        crate::views::ConnectionView { id: ConnectionId::mint().to_string(), source: "mesh1.rpc.1".into(), destination: "mesh1.admin.1".into(), kind: "direct".into(), state: "connected".into(), carrier: None, reason: Some("dialled".into()), logged_at_ms: 1_791_332_804_963 }
    }

    /// CONTRACT (a read past the reply bound is a named refusal, never a reset): an answer within
    /// the Status reply bound is answered as it is; one past it is `ReadOverBound` naming the read,
    /// the answer's encoded size and the bound, and that refusal itself fits the bound. One mesh of
    /// 150 nodes at a realistic size each fits.
    #[test]
    fn a_read_answer_past_the_reply_bound_is_refused_naming_its_size() {
        let mesh = StatusReply::Nodes { nodes: (0..150).map(|_| node_view()).collect() };
        assert_eq!(StatusReply::within_reply_bound(mesh.clone(), "nodes.get"), mesh, "150 nodes fit the bound");
        let over = StatusReply::Nodes { nodes: (0..400).map(|_| node_view()).collect() };
        let size = Status::encode_reply(&over).unwrap().len() as u64;
        let refused = StatusReply::within_reply_bound(over, "nodes.get");
        assert_eq!(refused, StatusReply::ReadOverBound { read: "nodes.get".into(), bytes: size, max: Status::MAX_REPLY_FRAME_BYTES as u64 });
        assert!(Status::encode_reply(&refused).unwrap().len() <= Status::MAX_REPLY_FRAME_BYTES);
    }

    /// CONTRACT (an error carries every detail it can, and still reaches the caller): an incomplete
    /// manifest naming more births than one reply carries, each and the reason longer than its
    /// budget, is still a reply within `MAX_REPLY_FRAME_BYTES`, its count whole; a short one is kept
    /// whole.
    #[test]
    fn an_incomplete_manifest_naming_every_birth_encodes_within_the_reply_bound() {
        let long = |i: usize| format!("{i} {}", "x".repeat(4 * MAX_WITHOUT_EXIT_BIRTH_BYTES));
        let births: Vec<String> = (0..2 * MAX_WITHOUT_EXIT_BIRTHS).map(long).collect();
        let r = StatusReply::incomplete_manifest(&births, &"r".repeat(4 * Status::MAX_REPLY_FRAME_BYTES));
        let len = Status::encode_reply(&r).unwrap().len();
        assert!(len <= Status::MAX_REPLY_FRAME_BYTES, "rejected-incomplete-manifest encodes to {len}, over {}", Status::MAX_REPLY_FRAME_BYTES);
        let StatusReply::RejectedIncompleteManifest { missing, without_exit, .. } = r else { unreachable!() };
        assert_eq!((missing as usize, without_exit.len()), (births.len(), MAX_WITHOUT_EXIT_BIRTHS), "the count stays whole when the list is cut");
        let short = StatusReply::incomplete_manifest(&["mesh2.rpc.1 i1".into(), "mesh2.admin.2 i2".into()], "no exit manifest");
        assert_eq!(short, StatusReply::RejectedIncompleteManifest { missing: 2, without_exit: vec!["mesh2.rpc.1 i1".into(), "mesh2.admin.2 i2".into()], reason: "no exit manifest".into() }, "a list and reason within budget are kept whole");
    }

    #[test]
    fn transitions_move_forward_only_and_the_same_state_is_already_applied() {
        use NodeState::*;
        assert_eq!(transition(None, Pending), Transition::Apply);
        assert_eq!(transition(Some(Pending), Ready), Transition::Apply);
        assert_eq!(transition(Some(Pending), Stopped), Transition::Apply, "a skip is a forward move");
        assert_eq!(transition(Some(Ready), Ready), Transition::AlreadyApplied);
        assert_eq!(transition(Some(ConnectionsDeleting), Ready), Transition::Backward { current: ConnectionsDeleting });
        assert_eq!(transition(Some(MeshState::Dead), MeshState::Pending), Transition::Backward { current: MeshState::Dead });
    }
}
