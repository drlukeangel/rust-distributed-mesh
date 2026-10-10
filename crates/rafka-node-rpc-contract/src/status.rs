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
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId, RuntimeFact};
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
}

/// The longest `receipt_manifest` reference a `MeshLeave` carries.
pub const MAX_RECEIPT_MANIFEST_BYTES: usize = 128;

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
}

impl StatusReply {
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
        }
    }
}

impl NodeProtocol for Status {
    const OP: u8 = 0x1B;
    const NAME: &'static str = "status";
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// A declaration travels the ordinary Direct/ViaPeer route like any other call.
    const FORWARDABLE: bool = true;
    /// A draining node still answers its authority's probe and apply.
    const SERVED_WHILE_DRAINING: bool = true;
    const REQUEST_VARIANTS: u32 = 17;
    const REPLY_VARIANTS: u32 = 21;

    type Request = StatusRequest;
    type Reply = StatusReply;

    fn classify_reply(reply: &StatusReply) -> ReplyKind {
        match reply {
            StatusReply::Applied
            | StatusReply::AlreadyApplied
            | StatusReply::NodeDrainingApplied { .. }
            | StatusReply::Current { .. }
            | StatusReply::CurrentParked { .. }
            | StatusReply::Stopped { .. }
            | StatusReply::Started => ReplyKind::Success,
            StatusReply::RejectedStaleIncarnation { .. }
            | StatusReply::RejectedStaleMesh { .. }
            | StatusReply::RejectedStaleFabric { .. }
            | StatusReply::RejectedNotAuthority { .. }
            | StatusReply::RejectedInvalidNodeTransition { .. }
            | StatusReply::RejectedInvalidMeshTransition { .. }
            | StatusReply::RejectedUnmatchedCompletion { .. }
            | StatusReply::StartFailed { .. } => ReplyKind::ProtocolRefusal,
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
        ];
        assert_eq!(reqs.len() as u32, Status::REQUEST_VARIANTS);
        for q in reqs {
            assert_eq!(Status::decode_request(&Status::encode_request(&q).unwrap()).unwrap(), q);
        }
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
