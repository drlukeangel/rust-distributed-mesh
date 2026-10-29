//! The node command protocol on op `0x22` (i144 R22; Luke's rulings Q6 and Q7, 2026-10-16): the
//! mesh-primary's command to a node of its own mesh, answered on that command's own ordered reply
//! stream, so the node reports what it accepted, the state it entered and its completion, in order,
//! on one channel. Server-streaming and never forwarded: the hop is the mesh-primary's direct call to
//! the node, the hop its carrier makes; the caller-to-mesh-primary routing is the Build carrier's,
//! unchanged. Nothing checks who sent a command (node-rpc.md §14).
//!
//! `node.connection.delete` (node-connections-delete.md) is its first command:
//!
//! ```text
//! DeleteConnections { All }
//!   <- CommandAccepted        Started: the node validated the command; no lifecycle state
//!   <- ConnectionsDeleting    Data: the node's block on new application work is in force
//!   <- ConnectionsDeleted     Terminal: Established or Deadline
//! DeleteConnections { Connections([ids]) }
//!   <- CommandAccepted        Started; no node-wide state
//!   <- ConnectionsDeleted     Terminal
//! refused before acceptance: one refusal frame
//! ```
//!
//! Q47 appends `StopNode` and `StartNode`; Q205 makes the stop a workflow that calls its primitives
//! and streams each one's acknowledgement and completion: stop reports `CommandAccepted`, then
//! `ConnectionsDeleting` and `StopConnectionsDeleted` (node.connection.delete's, Data), then
//! `Leaving` and `StopMeshLeft` (node.mesh.leave's, Data), then `NodeStopped` (Terminal). Q205 appends
//! `LeaveMesh`, standalone node.mesh.leave: `CommandAccepted` (leaving), then `MeshLeft` (Terminal), or
//! the refusal `BusyHoldingConnections` before acceptance. Start reports `CommandAccepted`,
//! `NodeJoined` (Data), then `NodeStarted` (Terminal), or a typed `Failed` terminal after acceptance.
//! A lifecycle state refusal before acceptance is `RejectedInvalidNodeTransition`.
//! `CommandAccepted` carries no lifecycle state: on a stop, its admission records stopping.
//! `ConnectionsDeleting`, `StopConnectionsDeleted`, `Leaving`, `StopMeshLeft`, `MeshLeft`, `NodeJoined`,
//! `NodeStopped` and `NodeStarted` record their own effects; a leave's `CommandAccepted` records leaving. A stream that breaks after
//! `CommandAccepted` is incomplete: whatever the frames before the break reported stands, and no
//! completion is claimed. The record is of the transport header's `(invocation_id, seq)`: delivery
//! orders and suppresses repeats before handing progress to the mesh-primary, which records it
//! before acknowledging recovery. This adds no identity to the body and no receipt on the node.
//! A Build reissuing an uncertain DeleteConnections step opens a new invocation: the node executes
//! the naturally idempotent closure again. Completed steps come from the Build's step receipts.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::status::{ConnectionScope, ConnectionsReceipt, WhichMesh, MAX_CONNECTION_IDS};
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::{ConnectionId, IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// The node command protocol on op `0x22`.
#[derive(Debug, Clone, Copy)]
pub struct NodeCommand;

/// A command to the exact birth it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCommandRequest {
    /// `node.connection.delete` (R-CR3): the exact birth hard-cuts the connections the scope
    /// names, writing each drop under the name it holds. `All` is graceful node-wide closure: the
    /// birth blocks new application work, lets admitted work finish under its bound, then closes
    /// every open connection (node-connections-delete.md step 3). Its endpoint stays bound and the
    /// call's own stream stays open.
    ///
    /// Position 0 on the wire.
    DeleteConnections {
        /// The birth whose connections are cut.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// Which connections.
        scope: ConnectionScope,
    },
    /// Stop this exact birth, reporting closure and completion on this stream (Q47).
    ///
    /// Position 1 on the wire.
    StopNode {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The Build that owns this operation.
        build_id: String,
        /// Its executing attempt.
        attempt: u32,
        /// The operation's declared identity.
        operation: String,
    },
    /// Start this stopped exact birth, reporting join and readiness on this stream (Q47).
    ///
    /// Position 2 on the wire.
    StartNode {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The Build that owns this operation.
        build_id: String,
        /// Its executing attempt.
        attempt: u32,
        /// The operation's declared identity.
        operation: String,
    },
    /// A storage change's move of the exact birth to the provider its change decided (storage.md §2
    /// "Changing it", step 4: "Each mesh-primary moves its nodes to the new provider"), carried by its
    /// mesh-primary: answered [`NodeCommandReply::StorageStepped`] once the birth writes `provider`, or
    /// [`NodeCommandReply::RejectedStaleChange`] for a command of a change older than the one it acted on.
    ///
    /// Position 3 on the wire.
    StorageStep {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The storage change's Build.
        build_id: String,
        /// The change.
        change_id: crate::ChangeId,
        /// The provider the birth writes from here on.
        provider: String,
    },
    /// `node.mesh.leave` (node-mesh-leave.md; Q205): the exact birth, its connections deleted, leaves
    /// its mesh (and the backbone, a node-admin) and cuts its Node RPC pool, reporting its
    /// acknowledgement (`CommandAccepted`, leaving) and its completion (`MeshLeft`) on this stream.
    /// Appended: no variant before it moves.
    ///
    /// Position 4 on the wire.
    LeaveMesh {
        /// The commanded node.
        node_id: NodeId,
        /// Its exact incarnation.
        incarnation: IncarnationId,
        /// The Build that owns this operation.
        build_id: String,
        /// Its executing attempt.
        attempt: u32,
        /// The operation's declared identity.
        operation: String,
    },
}

impl NodeCommandRequest {
    /// The command's name in spans and evidence.
    pub fn op(&self) -> &'static str {
        match self {
            Self::DeleteConnections { .. } => "delete-connections",
            Self::StopNode { .. } => "stop-node",
            Self::StartNode { .. } => "start-node",
            Self::StorageStep { .. } => "storage-step",
            Self::LeaveMesh { .. } => "leave-mesh",
        }
    }

    /// The birth the command names.
    pub fn subject(&self) -> (&NodeId, &IncarnationId) {
        match self {
            Self::DeleteConnections { node_id, incarnation, .. } | Self::StopNode { node_id, incarnation, .. } | Self::StartNode { node_id, incarnation, .. } | Self::StorageStep { node_id, incarnation, .. } | Self::LeaveMesh { node_id, incarnation, .. } => (node_id, incarnation),
        }
    }
}

/// One reply frame of a node command's stream. Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCommandReply {
    /// The node validated the command and runs it: the stream's first frame. It carries no
    /// lifecycle state.
    ///
    /// Position 0 on the wire.
    CommandAccepted {
        /// The birth that accepted it.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// `All` only: the node's block on new application work is in force; it is connections-deleting
    /// until its completion. The state the mesh-primary records and publishes for the birth.
    ///
    /// Position 1 on the wire.
    ConnectionsDeleting {
        /// The birth that entered connections-deleting.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// `node.connections.deleted`: the cut is complete. Every connection named is accounted for by
    /// its entry id: `closed` were open and are cut, their drops written under the name the birth
    /// holds; `not_open` were named and are unknown or already closed; `held` are the connections
    /// the birth still holds after the cut (none after an `Established` `All`).
    ///
    /// Position 2 on the wire.
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
    /// The scope names nothing the birth can cut (an empty list, or more ids than one request
    /// carries); nothing is cut.
    ///
    /// Position 3 on the wire.
    RejectedConnectionScope {
        /// Why, by name.
        reason: String,
    },
    /// The command names another birth than the one it reached: the incarnation this one holds.
    ///
    /// Position 4 on the wire.
    RejectedStaleIncarnation {
        /// The incarnation the receiver holds.
        held: IncarnationId,
    },
    /// The receiver cannot take the command yet (no connections writer, its server not ready).
    ///
    /// Position 5 on the wire.
    NotReady {
        /// Why, by name.
        reason: String,
    },
    /// The receiver could not resolve the peer the call came over.
    ///
    /// Position 6 on the wire.
    PeerUnresolved {
        /// Why, by name.
        reason: String,
    },
    /// The receiver is at its admission bound for the op.
    ///
    /// Position 7 on the wire.
    Busy {
        /// Why, by name.
        reason: String,
    },
    /// The receiver refuses new non-status calls while connections-deleting. Never sent for this op,
    /// which a connections-deleting node serves (a second `All` reaches it, step 5).
    ///
    /// Position 8 on the wire.
    RejectedConnectionsDeleting {
        /// Why, by name.
        reason: String,
    },
    /// The request did not decode or broke the protocol's bounds.
    ///
    /// Position 9 on the wire.
    Malformed {
        /// What was malformed.
        kind: MalformedKind,
    },
    /// The caller is not authorized for the op.
    ///
    /// Position 10 on the wire.
    Unauthorized {
        /// Why, by name.
        reason: String,
    },
    /// The stop's whole All closure; progress, never the stop's terminal.
    ///
    /// Position 11 on the wire.
    StopConnectionsDeleted {
        /// The closure's Established or Deadline evidence.
        receipt: ConnectionsReceipt,
    },
    /// The stop's transport cut is complete; the live process is stopped.
    ///
    /// Position 12 on the wire.
    NodeStopped,
    /// The accepted join and channel join completed; start gates remain.
    ///
    /// Position 13 on the wire.
    NodeJoined,
    /// Every start gate completed; the receiver records Ready.
    ///
    /// Position 14 on the wire.
    NodeStarted,
    /// An accepted command failed at this named step.
    ///
    /// Position 15 on the wire.
    Failed {
        /// The step that failed.
        step: String,
        /// Its explanation.
        reason: String,
        /// The step's typed outcome code.
        code: crate::outcome::Code,
    },
    /// The lifecycle command is invalid for this living birth's held state.
    ///
    /// Position 16 on the wire.
    RejectedInvalidNodeTransition {
        /// The state it holds, named by the refusal.
        current: crate::status::NodeState,
    },
    /// The birth writes `provider`: it moved onto it, or wrote it already. The command's terminal.
    ///
    /// Position 17 on the wire.
    StorageStepped {
        /// The provider the birth writes.
        provider: String,
    },
    /// The command names a storage change older than the one the birth acted on: refused by name,
    /// nothing done (storage.md §2 "Changing it").
    ///
    /// Position 18 on the wire.
    RejectedStaleChange {
        /// The change the birth acted on last.
        held: crate::ChangeId,
    },
    /// Inside a stop, node.mesh.leave's acknowledgement (Q205): the birth is leaving its mesh; the
    /// state the mesh-primary writes on its row. Appended, as are the three below: no variant before
    /// them moves.
    ///
    /// Position 19 on the wire.
    Leaving {
        /// The birth that is leaving.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// Inside a stop, node.mesh.leave's completion, `node.mesh.left`: progress, never the stop's
    /// terminal (the twin of [`Self::MeshLeft`], as `StopConnectionsDeleted` is of `ConnectionsDeleted`).
    ///
    /// Position 20 on the wire.
    StopMeshLeft {
        /// Which meshes the birth left: its own, and the backbone for a node-admin.
        left: Vec<WhichMesh>,
    },
    /// Standalone `node.mesh.left`: the birth left its mesh (and the backbone, a node-admin), its
    /// endpoint still bound. The `LeaveMesh` command's terminal.
    ///
    /// Position 21 on the wire.
    MeshLeft {
        /// The birth that left.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
        /// Which meshes it left.
        left: Vec<WhichMesh>,
    },
    /// `LeaveMesh` refused before acceptance: the birth still holds these connections, and leaves
    /// nothing (node-mesh-leave.md step 2).
    ///
    /// Position 22 on the wire.
    BusyHoldingConnections {
        /// The connections it still holds.
        connections: Vec<ConnectionId>,
    },
}

impl NodeCommandReply {
    /// The reply's name in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CommandAccepted { .. } => "command-accepted",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::ConnectionsDeleted { .. } => "connections-deleted",
            Self::RejectedConnectionScope { .. } => "rejected-connection-scope",
            Self::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            Self::NotReady { .. } => "not-ready",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::RejectedConnectionsDeleting { .. } => "rejected-connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::StopConnectionsDeleted { .. } => "stop-connections-deleted",
            Self::NodeStopped => "node-stopped",
            Self::NodeJoined => "node-joined",
            Self::NodeStarted => "node-started",
            Self::Failed { .. } => "failed",
            Self::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            Self::StorageStepped { .. } => "storage-stepped",
            Self::RejectedStaleChange { .. } => "rejected-stale-change",
            Self::Leaving { .. } => "leaving",
            Self::StopMeshLeft { .. } => "stop-mesh-left",
            Self::MeshLeft { .. } => "mesh-left",
            Self::BusyHoldingConnections { .. } => "busy-holding-connections",
        }
    }
}

impl NodeCommandReply {
    /// The per-element code view of a served named-connection answer, shared with Status.
    pub fn connection_outcomes(&self) -> Vec<crate::outcome::ElementOutcome<'_, crate::ConnectionId>> {
        match self {
            Self::ConnectionsDeleted { closed, not_open, .. } => crate::outcome::connection_outcomes(closed, not_open),
            _ => Vec::new(),
        }
    }
}

impl NodeProtocol for NodeCommand {
    const OP: u8 = 0x22;
    const NAME: &'static str = "node-command";
    /// A command naming the most connection ids one request carries ([`MAX_CONNECTION_IDS`]).
    const MAX_REQUEST_FRAME_BYTES: usize = 4 * 1024;
    /// A `node.connections.deleted` names every connection it accounts for by id
    /// ([`crate::status::MAX_REPORTED_CONNECTIONS`] of them).
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    /// The mesh-primary's direct hop to its node: never carried by a peer.
    const FORWARDABLE: bool = false;
    /// A connections-deleting node still takes a command: a second `All` leaves an incomplete
    /// closure (node-connections-delete.md step 5).
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = true;
    const REQUEST_VARIANTS: u32 = 5;
    const REPLY_VARIANTS: u32 = 23;

    type Request = NodeCommandRequest;
    type Reply = NodeCommandReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            NodeCommandReply::CommandAccepted { .. } => "command-accepted",
            NodeCommandReply::ConnectionsDeleting { .. } => "connections-deleting",
            NodeCommandReply::ConnectionsDeleted { .. } => "connections-deleted",
            NodeCommandReply::RejectedConnectionScope { .. } => "rejected-connection-scope",
            NodeCommandReply::RejectedStaleIncarnation { .. } => "rejected-stale-incarnation",
            NodeCommandReply::NotReady { .. } => "not-ready",
            NodeCommandReply::PeerUnresolved { .. } => "peer-unresolved",
            NodeCommandReply::Busy { .. } => "busy",
            NodeCommandReply::RejectedConnectionsDeleting { .. } => "rejected-connections-deleting",
            NodeCommandReply::Malformed { .. } => "malformed",
            NodeCommandReply::Unauthorized { .. } => "unauthorized",
            NodeCommandReply::StopConnectionsDeleted { .. } => "stop-connections-deleted",
            NodeCommandReply::NodeStopped => "node-stopped",
            NodeCommandReply::NodeJoined => "node-joined",
            NodeCommandReply::NodeStarted => "node-started",
            NodeCommandReply::Failed { .. } => "failed",
            NodeCommandReply::RejectedInvalidNodeTransition { .. } => "rejected-invalid-node-transition",
            NodeCommandReply::StorageStepped { .. } => "storage-stepped",
            NodeCommandReply::RejectedStaleChange { .. } => "rejected-stale-change",
            NodeCommandReply::Leaving { .. } => "leaving",
            NodeCommandReply::StopMeshLeft { .. } => "stop-mesh-left",
            NodeCommandReply::MeshLeft { .. } => "mesh-left",
            NodeCommandReply::BusyHoldingConnections { .. } => "busy-holding-connections",
        }
    }

    fn classify_reply(reply: &NodeCommandReply) -> ReplyKind {
        match reply {
            NodeCommandReply::CommandAccepted { .. } | NodeCommandReply::NodeStopped | NodeCommandReply::NodeJoined | NodeCommandReply::NodeStarted | NodeCommandReply::StorageStepped { .. } => ReplyKind::Success,
            NodeCommandReply::Leaving { .. } | NodeCommandReply::StopMeshLeft { .. } | NodeCommandReply::MeshLeft { .. } => ReplyKind::Success,
            // As Status 0x1B classifies the same refusal.
            NodeCommandReply::BusyHoldingConnections { .. } => ReplyKind::new(crate::outcome::Code::Unavailable),
            NodeCommandReply::RejectedStaleChange { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodeCommandReply::Failed { code, .. } => ReplyKind::new(*code),
            NodeCommandReply::RejectedInvalidNodeTransition { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodeCommandReply::StopConnectionsDeleted { receipt: ConnectionsReceipt::Deadline { .. } } => ReplyKind::new(crate::outcome::Code::DeadlineExceeded),
            NodeCommandReply::StopConnectionsDeleted { .. } => ReplyKind::Success,
            NodeCommandReply::ConnectionsDeleting { .. } => ReplyKind::Success,
            NodeCommandReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Deadline { .. }), .. } => ReplyKind::new(crate::outcome::Code::DeadlineExceeded),
            NodeCommandReply::ConnectionsDeleted { .. } => ReplyKind::Success,
            NodeCommandReply::RejectedConnectionScope { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            NodeCommandReply::RejectedStaleIncarnation { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodeCommandReply::NotReady { .. } => ReplyKind::NotReady,
            NodeCommandReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            NodeCommandReply::Busy { .. } => ReplyKind::Busy,
            NodeCommandReply::RejectedConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            NodeCommandReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            NodeCommandReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> NodeCommandReply {
        NodeCommandReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> NodeCommandReply {
        NodeCommandReply::NotReady { reason }
    }
    fn busy(reason: String) -> NodeCommandReply {
        NodeCommandReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> NodeCommandReply {
        NodeCommandReply::RejectedConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> NodeCommandReply {
        NodeCommandReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> NodeCommandReply {
        NodeCommandReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for NodeCommand {
    fn frame_kind(frame: &NodeCommandReply) -> FrameKind {
        match frame {
            NodeCommandReply::CommandAccepted { .. } => FrameKind::Started,
            NodeCommandReply::ConnectionsDeleting { .. } | NodeCommandReply::StopConnectionsDeleted { .. } | NodeCommandReply::Leaving { .. } | NodeCommandReply::StopMeshLeft { .. } | NodeCommandReply::NodeJoined => FrameKind::Data,
            NodeCommandReply::ConnectionsDeleted { .. } | NodeCommandReply::MeshLeft { .. } | NodeCommandReply::NodeStopped | NodeCommandReply::NodeStarted | NodeCommandReply::Failed { .. } | NodeCommandReply::StorageStepped { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(NodeCommand::classify_reply(other)),
        }
    }
}

const _: () = assert!(MAX_CONNECTION_IDS * (1 + crate::CROCKFORD60_LEN) + 64 <= NodeCommand::MAX_REQUEST_FRAME_BYTES, "a DeleteConnections naming the most ids one request carries fits the request bound");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::FrameOrder;

    fn accepted() -> NodeCommandReply {
        NodeCommandReply::CommandAccepted { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }
    }

    fn deleted(receipt: Option<ConnectionsReceipt>) -> NodeCommandReply {
        NodeCommandReply::ConnectionsDeleted { closed: vec![ConnectionId::mint()], not_open: vec![], held: vec![], receipt }
    }

    /// CONTRACT (Luke Q7: "Replies (NodeCommand replies, classified into the existing stream order)"):
    /// `All` streams `CommandAccepted`, `ConnectionsDeleting`, then `ConnectionsDeleted`;
    /// `Connections([ids])` streams `CommandAccepted` then `ConnectionsDeleted`; a refusal before
    /// acceptance is one frame. Each is a legal order of the transport's existing enforcer. Must NOT
    /// happen: a progress frame before acceptance, a refusal after it, or a second completion.
    // @feature: node-lifecycle
    #[test]
    fn both_scopes_follow_the_stream_order_and_a_refusal_is_one_frame() {
        let mut all = FrameOrder::new();
        for f in [accepted(), NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, deleted(Some(ConnectionsReceipt::Established { in_flight: 0, closed: vec![] }))] {
            all.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("All: {f:?}: {e}"));
        }
        let mut some = FrameOrder::new();
        for f in [accepted(), deleted(None)] {
            some.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("Connections: {f:?}: {e}"));
        }
        for refusal in [NodeCommandReply::RejectedConnectionScope { reason: "empty".into() }, NodeCommandReply::RejectedStaleIncarnation { held: IncarnationId::mint() }, NodeCommandReply::NotReady { reason: "not yet".into() }] {
            let mut one = FrameOrder::new();
            one.admit(NodeCommand::frame_kind(&refusal)).unwrap_or_else(|e| panic!("{refusal:?} alone: {e}"));
            assert!(one.admit(NodeCommand::frame_kind(&accepted())).is_err(), "nothing follows a refusal");
        }
        let mut early = FrameOrder::new();
        assert!(early.admit(NodeCommand::frame_kind(&NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() })).is_err(), "no progress before acceptance");
        let mut late = FrameOrder::new();
        late.admit(NodeCommand::frame_kind(&accepted())).unwrap();
        assert!(late.admit(NodeCommand::frame_kind(&NodeCommandReply::RejectedConnectionScope { reason: "late".into() })).is_err(), "no refusal after acceptance");
    }

    /// CONTRACT (Q205; node-stop.md steps 1-4; node-mesh-leave.md Wire): a stop streams each of its
    /// primitives' acknowledgement and completion, `CommandAccepted`, `ConnectionsDeleting`,
    /// `StopConnectionsDeleted`, `Leaving`, `StopMeshLeft`, `NodeStopped`, a legal order ending in its one
    /// terminal; a standalone leave streams `CommandAccepted` then `MeshLeft`, its terminal, or the one
    /// refusal `BusyHoldingConnections`. Must NOT happen: a primitive's completion inside a stop ending
    /// the stop's stream, or a frame after a standalone leave's `MeshLeft`.
    // @feature: node-lifecycle
    #[test]
    fn a_stop_streams_each_primitives_frames_and_a_standalone_leave_ends_on_mesh_left() {
        let mut stop = FrameOrder::new();
        let receipt = ConnectionsReceipt::Established { in_flight: 0, closed: vec![] };
        for f in [accepted(), NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, NodeCommandReply::StopConnectionsDeleted { receipt }, NodeCommandReply::Leaving { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, NodeCommandReply::StopMeshLeft { left: vec![WhichMesh::Own] }, NodeCommandReply::NodeStopped] {
            stop.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("stop: {f:?}: {e}"));
        }
        assert!(stop.admit(NodeCommand::frame_kind(&NodeCommandReply::NodeStopped)).is_err(), "one terminal");
        let mut leave = FrameOrder::new();
        let left = NodeCommandReply::MeshLeft { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), left: vec![WhichMesh::Own, WhichMesh::Backbone] };
        for f in [accepted(), left.clone()] {
            leave.admit(NodeCommand::frame_kind(&f)).unwrap_or_else(|e| panic!("leave: {f:?}: {e}"));
        }
        assert!(leave.admit(NodeCommand::frame_kind(&left)).is_err(), "nothing follows MeshLeft");
        let mut busy = FrameOrder::new();
        busy.admit(NodeCommand::frame_kind(&NodeCommandReply::BusyHoldingConnections { connections: vec![ConnectionId::mint()] })).expect("a refusal alone");
        assert_eq!(NodeCommand::classify_reply(&NodeCommandReply::BusyHoldingConnections { connections: vec![] }).code(), crate::outcome::Code::Unavailable, "as on Status 0x1B");
        for f in [NodeCommandReply::Leaving { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, NodeCommandReply::StopMeshLeft { left: vec![WhichMesh::Own] }, left, NodeCommandReply::BusyHoldingConnections { connections: vec![ConnectionId::mint()] }] {
            assert_eq!(NodeCommand::decode_reply(&NodeCommand::encode_reply(&f).unwrap()).unwrap(), f, "{}", f.name());
        }
        let req = NodeCommandRequest::LeaveMesh { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "3vzt99cnx0xe".into(), attempt: 1, operation: "node.mesh.leave:mesh1.rpc.1".into() };
        assert_eq!(NodeCommand::decode_request(&NodeCommand::encode_request(&req).unwrap()).unwrap(), req);
    }

    /// CONTRACT (node-rpc-envelope.md, the wire is postcard): every request and reply of op 0x22
    /// round-trips, and a `node.connections.deleted` naming the most ids one reply carries fits the
    /// reply bound.
    // @feature: node-lifecycle
    #[test]
    fn every_node_command_frame_round_trips_under_its_bounds() {
        let req = NodeCommandRequest::DeleteConnections { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), scope: ConnectionScope::Connections((0..MAX_CONNECTION_IDS).map(|_| ConnectionId::mint()).collect()) };
        let bytes = NodeCommand::encode_request(&req).unwrap();
        assert!(bytes.len() <= NodeCommand::MAX_REQUEST_FRAME_BYTES, "{} bytes", bytes.len());
        assert_eq!(NodeCommand::decode_request(&bytes).unwrap(), req);
        let ids = |n: usize| (0..n).map(|_| ConnectionId::mint()).collect::<Vec<_>>();
        let big = NodeCommandReply::ConnectionsDeleted { closed: ids(crate::status::MAX_REPORTED_CONNECTIONS - 2), not_open: ids(1), held: ids(1), receipt: None };
        for reply in [accepted(), NodeCommandReply::ConnectionsDeleting { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, big, deleted(Some(ConnectionsReceipt::Deadline { last_in_flight: 1, closed: ids(1), held: ids(1) }))] {
            let bytes = NodeCommand::encode_reply(&reply).unwrap();
            assert!(bytes.len() <= NodeCommand::MAX_REPLY_FRAME_BYTES, "{}: {} bytes", reply.name(), bytes.len());
            assert_eq!(NodeCommand::decode_reply(&bytes).unwrap(), reply);
        }
    }
}
