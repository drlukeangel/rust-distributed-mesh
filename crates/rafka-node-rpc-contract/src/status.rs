//! The lifecycle/status declaration protocol on op `0x1B`, RDM's control family (the core
//! families stay exactly Ping and Forward) (node-rpc.md §38; fabric-mesh-ops.md §3; i143.e6.s7).
//!
//! A node or a primary declares a committed state upward to its authority and gets the typed
//! result of applying it. Gossip stays the dissemination plane; this is certainty that one
//! declared state was applied by the authority that holds it. There is no transition id:
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
//! Legal moves run forward along `Pending -> ReadyForTraffic -> Draining -> Leaving` (and
//! `Pending -> ReadyForTraffic -> Leaving -> Dead` for a Mesh), skips included; the same
//! state again is `AlreadyApplied`, a backward move `RejectedInvalidNodeTransition` or `RejectedInvalidMeshTransition`. A declaration
//! for a birth the receiver holds a newer incarnation of, or holds as departed, is
//! `RejectedStaleIncarnation`. A receiver that is not the authority, or a sender that is not the
//! subject or its primary, is `RejectedNotAuthority`, the reply naming which.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use rafka_mesh_entity::{ConfigOverCap, FabricId, IncarnationId, MeshId, NodeId, RuntimeFact, CROCKFORD60_LEN, MAX_CONFIG_BYTES, MAX_CONFIG_KEYS, OPAQUE_ID_LEN};
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

/// The Status protocol: declare, apply and probe lifecycle state.
pub struct Status;

/// A node birth's lifecycle state, as declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NodeState {
    /// Born and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work.
    Draining,
    /// Leaving: it has announced its departure.
    Leaving,
}

/// A Mesh's status, as declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MeshState {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Leaving: it is closing.
    Leaving,
    /// Dead: every member is gone.
    Dead,
}

/// A Fabric event the fabric-primary applies at a mesh primary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FabricEvent {
    /// The fabric is ready for traffic.
    ReadyForTraffic,
    /// A fabric shutdown was initiated by the named admin.
    ShutdownInitiated {
        /// The admin that initiated the shutdown.
        initiated_by: String,
    },
}

impl FabricEvent {
    /// The event's name as it appears in spans and replies.
    pub fn name(&self) -> &'static str {
        match self {
            Self::ReadyForTraffic => "ready-for-traffic",
            Self::ShutdownInitiated { .. } => "shutdown-initiated",
        }
    }
}

/// A Status call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusRequest {
    /// Upward: the sender's own birth declares the lifecycle state it committed.
    DeclareNodeState {
        /// The declaring birth's logical node.
        node_id: NodeId,
        /// The declaring birth's incarnation.
        incarnation: IncarnationId,
        /// The state the birth committed.
        state: NodeState,
    },
    /// Upward: the mesh's current primary declares the Mesh's state to the fabric-primary.
    DeclareMeshState {
        /// The mesh declared.
        mesh_id: MeshId,
        /// The mesh's state.
        state: MeshState,
    },
    /// Downward: an authority tells the exact birth to enter `state`.
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
    ProbeNodeState {
        /// The birth probed.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
    },
    /// Downward: the fabric-primary applies a Mesh state at that mesh's bootstrap admin.
    ApplyMeshState {
        /// The mesh applied to.
        mesh_id: MeshId,
        /// The mesh's name.
        mesh_name: String,
        /// The state applied.
        state: MeshState,
    },
    /// Downward: the fabric-primary applies a Fabric event at a mesh primary.
    ApplyFabricEvent {
        /// The fabric the event belongs to.
        fabric_id: FabricId,
        /// The event applied.
        event: FabricEvent,
    },
    /// Downward: the owning mesh-admin tells the exact birth to refuse new work and finish the
    /// eligible in-flight work, and to keep running (`drain-node`; node-drain.md). The reply
    /// (`Applied` / `AlreadyApplied`) admits the command; completion is [`Self::NodeDrained`].
    DrainNode {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `drain-node:<path>`.
        operation: String,
    },
    /// Upward: the exact birth tells its owning mesh-admin that no eligible work remains in
    /// flight (`node-drained`). The subject is the sender; the fence names the admin.
    NodeDrained {
        /// The birth that drained.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `drain-node:<path>`.
        operation: String,
    },
    /// Downward: the owning mesh-admin tells the exact birth to stop (`stop-node`; node-stop.md):
    /// drain, hard-cut its mesh connections, enter `Leaving` and park, its process alive with its
    /// endpoint bound. The reply is held until the stop is done and is [`StatusReply::Stopped`]:
    /// `stopped` rides the stop call's own reply, never a gossip frame and never a second call.
    StopNode {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `stop-node:<path>`.
        operation: String,
    },
    /// Reserved: a stopped birth reports `stopped` on the reply of its `StopNode` call, so this call is
    /// never sent and a receiver refuses it by name.
    NodeLeft {
        /// The birth that is leaving.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `stop-node:<path>`.
        operation: String,
    },
    /// Downward: the fabric-primary tells the leaving mesh's primary to run the mesh-leave
    /// workflow (`leave-mesh`; mesh-leave.md). `Applied` / `AlreadyApplied` admit the command; the
    /// departure is complete only when the fabric-primary records `MeshLeft`.
    LeaveMesh {
        /// The mesh that leaves.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `shutdown-mesh:<mesh_id>`.
        operation: String,
    },
    /// Upward: the still-running mesh-primary hands the fabric-primary the proof that every other
    /// member exited and its own exact runtime (`mesh-leave`). The manifest is a reference to
    /// immutable receipts in the accepted Build's records; the fabric-primary reads and validates
    /// them before it accepts. The primary keeps running: the outside owner stops it next.
    MeshLeave {
        /// The mesh that leaves.
        mesh_id: MeshId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `shutdown-mesh:<mesh_id>`.
        operation: String,
        /// The final mesh-primary's node.
        final_node_id: NodeId,
        /// The final mesh-primary's incarnation.
        final_incarnation: IncarnationId,
        /// The final mesh-primary's exact runtime, still running.
        #[serde(with = "rafka_mesh_entity::wire::runtime")]
        final_runtime: RuntimeFact,
        /// A bounded reference (at most [`MAX_RECEIPT_MANIFEST_BYTES`]) to the other members'
        /// terminal receipts.
        receipt_manifest: String,
    },
    /// Downward: the commanding authority tells the exact birth to publish what its scratchpad holds (`commit-state`; fabric-state-commit.md). The reply admits the command; completion is [`Self::StateCommitted`].
    CommitState {
        /// The fabric the round belongs to.
        fabric_id: FabricId,
        /// The subject: the commanded or reporting birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the round belongs to.
        build_id: String,
        /// The Build attempt that holds the round.
        attempt: u32,
        /// The operation: `commit-state:<fabric_id>`.
        operation: String,
    },
    /// Upward: the exact birth, or a mesh primary for its mesh, checks in for the state-commit round (`state-committed`) after its required scratchpad writes are stored. A check-in, never a node or mesh state.
    StateCommitted {
        /// The fabric the round belongs to.
        fabric_id: FabricId,
        /// The subject: the commanded or reporting birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the round belongs to.
        build_id: String,
        /// The Build attempt that holds the round.
        attempt: u32,
        /// The operation: `commit-state:<fabric_id>`.
        operation: String,
    },
    /// Downward: the commanding authority tells the exact birth to open traffic (`open-traffic`; fabric-open-traffic.md). The reply admits the command; completion is [`Self::TrafficOpened`].
    OpenTraffic {
        /// The fabric the round belongs to.
        fabric_id: FabricId,
        /// The subject: the commanded or reporting birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the round belongs to.
        build_id: String,
        /// The Build attempt that holds the round.
        attempt: u32,
        /// The operation: `open-traffic:<fabric_id>`.
        operation: String,
    },
    /// Upward: the exact birth, or a mesh primary for its mesh, checks in for the open-traffic round (`traffic-opened`) after it completes traffic opening.
    TrafficOpened {
        /// The fabric the round belongs to.
        fabric_id: FabricId,
        /// The subject: the commanded or reporting birth.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the round belongs to.
        build_id: String,
        /// The Build attempt that holds the round.
        attempt: u32,
        /// The operation: `open-traffic:<fabric_id>`.
        operation: String,
    },
    /// Downward: the owning mesh-admin tells the parked exact birth to rejoin as itself
    /// (`start-node`; node-start.md): the same node id, incarnation, endpoint key and port. The
    /// birth joins its authority, rejoins its mesh channel and takes its topology, then runs its
    /// `hydrate_before_ready` hook with `AfterStop` in its context and declares itself ready. The
    /// reply is [`StatusReply::Started`] once it has rejoined, or [`StatusReply::StartFailed`]
    /// naming the step that failed. A birth that is not parked refuses it.
    StartNode {
        /// The birth commanded.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The Build the operation belongs to.
        build_id: String,
        /// The Build attempt that holds the operation.
        attempt: u32,
        /// The operation: `start-node:<path>`.
        operation: String,
    },
    /// Upward: a caller asks the fabric-primary to put the exact birth's whole config (`node.config.update`).
    /// The fabric-primary writes the node's row, tells the node, and answers [`StatusReply::ConfigUpdated`]
    /// (`node.config.updated`); a config past its cap is [`StatusReply::RejectedConfig`]. A single call, never a Build.
    UpdateNodeConfig {
        /// The birth whose config is put.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The whole config: a put replaces, it never merges.
        config: BTreeMap<String, String>,
    },
    /// Downward: the fabric-primary tells the exact birth its config; the birth holds it and carries it in
    /// its next digest. Answered `Applied`.
    ApplyNodeConfig {
        /// The birth told.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// The whole config.
        config: BTreeMap<String, String>,
    },
}

/// The longest `receipt_manifest` reference a `MeshLeave` carries.
pub const MAX_RECEIPT_MANIFEST_BYTES: usize = 128;

/// The most bytes postcard spends on one varint length prefix of a config key or value: one string
/// is at most [`MAX_CONFIG_BYTES`] long, under `2^14`, so its prefix is at most two bytes.
const CONFIG_STRING_PREFIX_BYTES: usize = 2;
const _: () = assert!(MAX_CONFIG_BYTES < 1 << 14, "a config string's length prefix is assumed to be at most two bytes");

/// The most bytes an `UpdateNodeConfig` (or `ApplyNodeConfig`) at the config cap encodes to: the
/// variant (a varint, at most five bytes), the node id (a one-byte length and [`CROCKFORD60_LEN`]
/// characters), the incarnation (a one-byte length and [`OPAQUE_ID_LEN`] characters), the map's length (a varint under the key cap, at most two bytes), and every key and
/// value at the cap with its length prefix. Derived from the cap, so the two cannot drift.
pub const MAX_CONFIG_REQUEST_BYTES: usize = 5 + (1 + CROCKFORD60_LEN) + (1 + OPAQUE_ID_LEN) + 2 + MAX_CONFIG_BYTES + MAX_CONFIG_KEYS * 2 * CONFIG_STRING_PREFIX_BYTES;

/// The bound every other Status request fits in.
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
            Self::DrainNode { .. } => "drain-node",
            Self::NodeDrained { .. } => "node-drained",
            Self::StopNode { .. } => "stop-node",
            Self::NodeLeft { .. } => "node-left",
            Self::LeaveMesh { .. } => "leave-mesh",
            Self::MeshLeave { .. } => "mesh-leave",
            Self::CommitState { .. } => "commit-state",
            Self::StateCommitted { .. } => "state-committed",
            Self::OpenTraffic { .. } => "open-traffic",
            Self::TrafficOpened { .. } => "traffic-opened",
            Self::StartNode { .. } => "start-node",
            Self::UpdateNodeConfig { .. } => "update-node-config",
            Self::ApplyNodeConfig { .. } => "apply-node-config",
        }
    }
}

/// Why a declaration was not applied, by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotAuthority {
    /// The receiver does not hold the seat the declaration needs.
    ReceiverNotPrimary {
        /// The seat the receiver would have to hold.
        needed: String,
    },
    /// The sender is not the subject (or the subject's current primary).
    SenderNotSubject {
        /// The sender, as the receiver resolved it.
        sender: String,
    },
    /// The receiver does not hold the subject at all.
    SubjectUnknown,
}

impl NotAuthority {
    /// The reason's name as it appears in spans and replies.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReceiverNotPrimary { .. } => "receiver-not-primary",
            Self::SenderNotSubject { .. } => "sender-not-subject",
            Self::SubjectUnknown => "subject-unknown",
        }
    }
}

/// What a drain established (node-drain.md): the count it measured, and whether the work finished
/// inside the drain bound. `Deadline` records the last observed count and never claims zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrainReceipt {
    /// The eligible in-flight work finished: `in_flight` is the count at the moment it was
    /// established.
    Established {
        /// The in-flight count when the drain was established.
        in_flight: u64,
    },
    /// The drain bound passed with work still in flight: `last_in_flight` is the last count seen.
    Deadline {
        /// The last in-flight count observed before the bound.
        last_in_flight: u64,
    },
}

impl DrainReceipt {
    /// The receipt's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Established { .. } => "established",
            Self::Deadline { .. } => "deadline",
        }
    }
}

/// The typed result of a Status call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusReply {
    /// Applied now by the authority, and durable where the ruling asks it to be.
    Applied,
    /// The same natural key was already at that state: one logical event.
    AlreadyApplied,
    /// The exact birth entered `Draining` (or already was), with the work still in flight now.
    NodeDrainingApplied {
        /// The work still in flight.
        in_flight: u64,
    },
    /// The exact birth's own answer to a probe: it reasserted its presence, and this is its state.
    Current {
        /// The answering birth's logical node.
        node_id: NodeId,
        /// The answering birth's incarnation.
        incarnation: IncarnationId,
        /// The birth's current state.
        state: NodeState,
    },
    /// The receiver holds a newer incarnation of that NodeId, or holds it as departed.
    RejectedStaleIncarnation {
        /// The incarnation held.
        held: IncarnationId,
    },
    /// The receiver holds another mesh id under that mesh name.
    RejectedStaleMesh {
        /// The mesh id held.
        held: MeshId,
    },
    /// The receiver holds another fabric id.
    RejectedStaleFabric {
        /// The fabric id held.
        held: FabricId,
    },
    /// The receiver or sender is not the authority for the call.
    RejectedNotAuthority {
        /// Which authority condition failed.
        why: NotAuthority,
    },
    /// A node move backward along the legal order.
    RejectedInvalidNodeTransition {
        /// The state the node holds.
        current: NodeState,
    },
    /// A mesh move backward along the legal order.
    RejectedInvalidMeshTransition {
        /// The state the mesh holds.
        current: MeshState,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why the call is refused.
        reason: String,
    },
    /// The node is not ready to serve.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    Busy {
        /// Which bound the node is at.
        reason: String,
    },
    /// The node is draining and takes no new work.
    Draining {
        /// Why the node refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// A completion call (`node-drained`, `node-left`) names no open command of the receiver: the
    /// first field that differs from the command the receiver holds for that birth is named, with
    /// the value the receiver holds and the value the completion reported.
    RejectedUnmatchedCompletion {
        /// The field that differs: `build_id`, `attempt`, `operation`, or `command` when the
        /// receiver holds no open command for the birth at all.
        field: String,
        /// What the receiver holds for it.
        expected: String,
        /// What the completion reported.
        reported: String,
    },
    /// The reply of a `StopNode` call, sent when the stop is done (`stopped`): the birth drained, cut
    /// its mesh connections and is `Leaving`, parked with its process alive. The receipt is what
    /// its drain established.
    Stopped {
        /// What the drain established.
        receipt: DrainReceipt,
    },
    /// The reply of a `StartNode` call: the parked birth rejoined as itself.
    Started,
    /// The reply of a `StartNode` call whose start stopped at `step`: nothing after that step ran.
    StartFailed {
        /// The step that failed: `node.join`, `node.topology` or `node.hydrate`.
        step: String,
        /// Why it failed.
        reason: String,
    },
    /// The exact birth's own answer to a probe while it is parked: `Leaving`, its process alive and
    /// its endpoint bound, waiting for `StartNode` or a delete.
    CurrentParked {
        /// The answering birth's logical node.
        node_id: NodeId,
        /// The answering birth's incarnation.
        incarnation: IncarnationId,
    },
    /// The reply of an `UpdateNodeConfig`: the fabric-primary wrote the node's row and the exact birth holds
    /// the config (`node.config.updated`).
    ConfigUpdated {
        /// The birth whose config was put.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
    },
    /// The config put was refused: the cap it crossed.
    RejectedConfig {
        /// The keys and bytes of the refused config and the bounds.
        over: ConfigOverCap,
    },
    /// The config put was not made: the nodes store refused the node's row, so nothing was told to
    /// the birth and the node's record is what it was.
    ConfigRowRefused {
        /// The node's `path.name`, the row the store refused.
        node: String,
        /// The store's refusal.
        reason: String,
    },
    /// The fabric-primary wrote the node's row, and the exact birth did not apply the config: the
    /// record holds the new config, the birth does not.
    ConfigNotApplied {
        /// The node's `path.name`.
        node: String,
        /// The birth's reply to `apply-node-config` by name, or the call's end by name.
        answered: String,
        /// Its detail.
        reason: String,
    },
}

/// The most bytes of a node's `path.name` a config refusal carries.
pub const MAX_REFUSAL_NODE_BYTES: usize = 128;
/// The most bytes of the reply or call-end name a `ConfigNotApplied` carries.
pub const MAX_REFUSAL_ANSWERED_BYTES: usize = 64;
/// The most bytes of detail a config refusal carries: what is left of the reply bound after the
/// variant (one byte), the other fields at their budgets and a two-byte length prefix on each string.
pub const MAX_REFUSAL_REASON_BYTES: usize = 1024 - 1 - (2 + MAX_REFUSAL_NODE_BYTES) - (2 + MAX_REFUSAL_ANSWERED_BYTES) - 2;

/// `s` cut to at most `max` bytes at a character boundary; a cut detail ends with `…` so the reader
/// knows it was cut.
fn within(s: &str, max: usize) -> String {
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
        Self::ConfigRowRefused { node: within(node, MAX_REFUSAL_NODE_BYTES), reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// `ConfigNotApplied` for `node`: the birth `answered` (a reply or call-end name), and why, each
    /// within its budget so the reply always fits the Status reply bound.
    pub fn config_not_applied(node: &str, answered: &str, reason: &str) -> Self {
        Self::ConfigNotApplied { node: within(node, MAX_REFUSAL_NODE_BYTES), answered: within(answered, MAX_REFUSAL_ANSWERED_BYTES), reason: within(reason, MAX_REFUSAL_REASON_BYTES) }
    }

    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::AlreadyApplied => "already-applied",
            Self::NodeDrainingApplied { .. } => "node-draining-applied",
            Self::Current { .. } => "current",
            Self::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            Self::RejectedStaleMesh { .. } => "rejected-stale-mesh",
            Self::RejectedStaleFabric { .. } => "rejected-stale-fabric",
            Self::RejectedNotAuthority { .. } => "rejected-not-authority",
            Self::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            Self::RejectedInvalidMeshTransition { .. } => "rejected-invalid-mesh-transition",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::Draining { .. } => "draining",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::RejectedUnmatchedCompletion { .. } => "rejected-unmatched-completion",
            Self::Stopped { .. } => "stopped",
            Self::Started => "started",
            Self::StartFailed { .. } => "start-failed",
            Self::CurrentParked { .. } => "current-parked",
            Self::ConfigUpdated { .. } => "config-updated",
            Self::RejectedConfig { .. } => "rejected-config",
            Self::ConfigRowRefused { .. } => "config-row-refused",
            Self::ConfigNotApplied { .. } => "config-not-applied",
        }
    }
}

impl NodeProtocol for Status {
    const OP: u8 = 0x1B;
    const NAME: &'static str = "status";
    /// The larger of a declaration's bound and a config put at its cap ([`MAX_CONFIG_REQUEST_BYTES`]).
    const MAX_REQUEST_FRAME_BYTES: usize = if MAX_CONFIG_REQUEST_BYTES > MAX_DECLARATION_REQUEST_BYTES { MAX_CONFIG_REQUEST_BYTES } else { MAX_DECLARATION_REQUEST_BYTES };
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// A declaration travels the ordinary Direct/ViaPeer route like any other call.
    const FORWARDABLE: bool = true;
    /// A draining node still answers its authority's probe and apply.
    const SERVED_WHILE_DRAINING: bool = true;
    const REQUEST_VARIANTS: u32 = 19;
    const REPLY_VARIANTS: u32 = 25;

    type Request = StatusRequest;
    type Reply = StatusReply;

    fn classify_reply(reply: &StatusReply) -> ReplyKind {
        match reply {
            StatusReply::Applied
            | StatusReply::AlreadyApplied
            | StatusReply::NodeDrainingApplied { .. }
            | StatusReply::Current { .. }
            | StatusReply::CurrentParked { .. }
            | StatusReply::ConfigUpdated { .. }
            | StatusReply::Stopped { .. }
            | StatusReply::Started => ReplyKind::Success,
            StatusReply::RejectedStaleIncarnation { .. }
            | StatusReply::RejectedStaleMesh { .. }
            | StatusReply::RejectedStaleFabric { .. }
            | StatusReply::RejectedNotAuthority { .. }
            | StatusReply::RejectedInvalidNodeTransition { .. }
            | StatusReply::RejectedInvalidMeshTransition { .. }
            | StatusReply::RejectedUnmatchedCompletion { .. }
            | StatusReply::StartFailed { .. }
            | StatusReply::RejectedConfig { .. }
            | StatusReply::ConfigRowRefused { .. }
            | StatusReply::ConfigNotApplied { .. } => ReplyKind::ProtocolRefusal,
            StatusReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            StatusReply::NotReady { .. } => ReplyKind::NotReady,
            StatusReply::Busy { .. } => ReplyKind::Busy,
            StatusReply::Draining { .. } => ReplyKind::Draining,
            StatusReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            StatusReply::Unauthorized { .. } => ReplyKind::Unauthorized,
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
    fn draining(reason: String) -> StatusReply {
        StatusReply::Draining { reason }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip() {
        let all = [
            (StatusReply::Applied, ReplyKind::Success),
            (StatusReply::AlreadyApplied, ReplyKind::Success),
            (StatusReply::NodeDrainingApplied { in_flight: 3 }, ReplyKind::Success),
            (StatusReply::Current { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::ReadyForTraffic }, ReplyKind::Success),
            (StatusReply::RejectedStaleIncarnation { held: IncarnationId::mint() }, ReplyKind::ProtocolRefusal),
            (StatusReply::RejectedStaleMesh { held: MeshId::mint() }, ReplyKind::ProtocolRefusal),
            (StatusReply::RejectedStaleFabric { held: FabricId::mint() }, ReplyKind::ProtocolRefusal),
            (StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, ReplyKind::ProtocolRefusal),
            (StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving }, ReplyKind::ProtocolRefusal),
            (StatusReply::RejectedInvalidMeshTransition { current: MeshState::Dead }, ReplyKind::ProtocolRefusal),
            (Status::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (Status::not_ready("n".into()), ReplyKind::NotReady),
            (Status::busy("b".into()), ReplyKind::Busy),
            (Status::draining("d".into()), ReplyKind::Draining),
            (Status::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (Status::unauthorized("u".into()), ReplyKind::Unauthorized),
            (StatusReply::RejectedUnmatchedCompletion { field: "attempt".into(), expected: "1".into(), reported: "2".into() }, ReplyKind::ProtocolRefusal),
            (StatusReply::Stopped { receipt: DrainReceipt::Deadline { last_in_flight: 2 } }, ReplyKind::Success),
            (StatusReply::Started, ReplyKind::Success),
            (StatusReply::StartFailed { step: "node.join".into(), reason: "refused".into() }, ReplyKind::ProtocolRefusal),
            (StatusReply::CurrentParked { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, ReplyKind::Success),
            (StatusReply::ConfigUpdated { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, ReplyKind::Success),
            (StatusReply::RejectedConfig { over: ConfigOverCap { keys: 33, bytes: 10, max_keys: 32, max_bytes: 2048 } }, ReplyKind::ProtocolRefusal),
            (StatusReply::ConfigRowRefused { node: "mesh1.rpc.1".into(), reason: "io".into() }, ReplyKind::ProtocolRefusal),
            (StatusReply::ConfigNotApplied { node: "mesh1.rpc.1".into(), answered: "busy".into(), reason: "b".into() }, ReplyKind::ProtocolRefusal),
        ];
        assert_eq!(all.len() as u32, Status::REPLY_VARIANTS);
        for (r, kind) in all {
            let back = Status::decode_reply(&Status::encode_reply(&r).unwrap()).unwrap();
            assert_eq!(back, r);
            assert_eq!(Status::classify_reply(&back), kind);
        }
        let reqs = [
            StatusRequest::DeclareNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::ReadyForTraffic },
            StatusRequest::ApplyNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), state: NodeState::Draining },
            StatusRequest::ProbeNodeState { node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::ReadyForTraffic },
            StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh2".into(), state: MeshState::Pending },
            StatusRequest::ApplyFabricEvent { fabric_id: FabricId::mint(), event: FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() } },
            StatusRequest::DrainNode { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() },
            StatusRequest::NodeDrained { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() },
            StatusRequest::StopNode { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() },
            StatusRequest::NodeLeft { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() },
            StatusRequest::LeaveMesh { mesh_id: MeshId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "shutdown-mesh:m".into() },
            StatusRequest::MeshLeave {
                mesh_id: MeshId::mint(),
                build_id: "bld_1".into(),
                attempt: 1,
                operation: "shutdown-mesh:m".into(),
                final_node_id: NodeId::mint(),
                final_incarnation: IncarnationId::mint(),
                final_runtime: RuntimeFact { deployment_id: "dep".into(), provider: rafka_mesh_entity::RuntimeProvider::Process, control_domain: "process:boot-a:pidns-a".into(), locator: rafka_mesh_entity::RuntimeLocator::Process { pid: 4321, start: 123456 } },
                receipt_manifest: "bld_1/1/shutdown-mesh:m/other-members-exited".into(),
            },
            StatusRequest::CommitState { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "commit-state:fabric1".into() },
            StatusRequest::StateCommitted { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "commit-state:fabric1".into() },
            StatusRequest::OpenTraffic { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "open-traffic:fabric1".into() },
            StatusRequest::TrafficOpened { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "open-traffic:fabric1".into() },
            StatusRequest::StartNode { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "bld_1".into(), attempt: 1, operation: "start-node:mesh1.rpc.1".into() },
            StatusRequest::UpdateNodeConfig { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: [("k".to_string(), "v".to_string())].into() },
            StatusRequest::ApplyNodeConfig { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: Default::default() },
        ];
        assert_eq!(reqs.len() as u32, Status::REQUEST_VARIANTS);
        for q in reqs {
            assert_eq!(Status::decode_request(&Status::encode_request(&q).unwrap()).unwrap(), q);
        }
    }

    /// CONTRACT (ops-naming "Node data": config is capped in key count and bytes): a config put at
    /// its cap encodes within the Status request bound, whichever shape fills the cap: the most keys,
    /// the longest strings (two-byte length prefixes), or both prefixes long on every entry.
    #[test]
    fn a_config_put_at_its_cap_encodes_within_the_status_request_bound() {
        let shapes: [(usize, usize, usize); 3] = [(MAX_CONFIG_KEYS, 8, MAX_CONFIG_BYTES / MAX_CONFIG_KEYS - 8), (MAX_CONFIG_BYTES / 256, 128, 128), (MAX_CONFIG_BYTES / 136, 8, 128)];
        for (keys, key_len, value_len) in shapes {
            let config: BTreeMap<String, String> = (0..keys).map(|i| (format!("{i:0key_len$}"), "v".repeat(value_len))).collect();
            assert_eq!(config.len(), keys);
            rafka_mesh_entity::check_config(&config).expect("the shape is within the cap");
            for q in [
                StatusRequest::UpdateNodeConfig { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: config.clone() },
                StatusRequest::ApplyNodeConfig { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), config: config.clone() },
            ] {
                let len = Status::encode_request(&q).unwrap().len();
                assert!(len <= Status::MAX_REQUEST_FRAME_BYTES, "{keys} keys of {key_len}+{value_len} bytes encode to {len}, over {}", Status::MAX_REQUEST_FRAME_BYTES);
                assert!(len <= MAX_CONFIG_REQUEST_BYTES, "{len} over the derived {MAX_CONFIG_REQUEST_BYTES}");
            }
        }
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

    #[test]
    fn transitions_move_forward_only_and_the_same_state_is_already_applied() {
        use NodeState::*;
        assert_eq!(transition(None, Pending), Transition::Apply);
        assert_eq!(transition(Some(Pending), ReadyForTraffic), Transition::Apply);
        assert_eq!(transition(Some(Pending), Leaving), Transition::Apply, "a skip is a forward move");
        assert_eq!(transition(Some(ReadyForTraffic), ReadyForTraffic), Transition::AlreadyApplied);
        assert_eq!(transition(Some(Draining), ReadyForTraffic), Transition::Backward { current: Draining });
        assert_eq!(transition(Some(MeshState::Dead), MeshState::Pending), Transition::Backward { current: MeshState::Dead });
    }
}
