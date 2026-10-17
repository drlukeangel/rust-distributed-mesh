//! The node command protocol on op `0x22` (i144 R22; Luke's rulings Q6 and Q7, 2026-10-16): the
//! mesh-primary's command to a node of its own mesh, answered on that command's own ordered reply
//! stream, so the node reports what it accepted, the state it entered and its completion, in order,
//! on one channel. Server-streaming and never forwarded: the hop is the mesh-primary's direct call to
//! the node, the hop its carrier makes; the caller-to-mesh-primary routing is the Build carrier's,
//! unchanged. Nothing checks who sent a command (node-rpc.md §14).
//!
//! `node.connections.delete` (node-connections-delete.md) is its first command:
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
//! `CommandAccepted` is neither a transport envelope nor node.start's `Started`; only
//! `ConnectionsDeleting` makes the mesh-primary record that state. A stream that breaks after
//! `CommandAccepted` is incomplete: whatever the frames before the break reported stands, and no
//! completion is claimed.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::status::{ConnectionScope, ConnectionsReceipt, MAX_CONNECTION_IDS};
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::{ConnectionId, IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// The node command protocol on op `0x22`.
#[derive(Debug, Clone, Copy)]
pub struct NodeCommand;

/// A command to the exact birth it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCommandRequest {
    /// `node.connections.delete` (R-CR3): the exact birth hard-cuts the connections the scope
    /// names, writing each drop under the name it holds. `All` is graceful node-wide closure: the
    /// birth blocks new application work, lets admitted work finish under its bound, then closes
    /// every open connection (node-connections-delete.md step 3). Its endpoint stays bound and the
    /// call's own stream stays open.
    DeleteConnections {
        /// The birth whose connections are cut.
        node_id: NodeId,
        /// The incarnation of that birth.
        incarnation: IncarnationId,
        /// Which connections.
        scope: ConnectionScope,
    },
}

impl NodeCommandRequest {
    /// The command's name in spans and evidence.
    pub fn op(&self) -> &'static str {
        match self {
            Self::DeleteConnections { .. } => "delete-connections",
        }
    }

    /// The birth the command names.
    pub fn subject(&self) -> (&NodeId, &IncarnationId) {
        match self {
            Self::DeleteConnections { node_id, incarnation, .. } => (node_id, incarnation),
        }
    }
}

/// One reply frame of a node command's stream. Positional order is part of the wire schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeCommandReply {
    /// The node validated the command and runs it: the stream's first frame. It carries no
    /// lifecycle state.
    CommandAccepted {
        /// The birth that accepted it.
        node_id: NodeId,
        /// Its incarnation.
        incarnation: IncarnationId,
    },
    /// `All` only: the node's block on new application work is in force; it is connections-deleting
    /// until its completion. The state the mesh-primary records and publishes for the birth.
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
    RejectedConnectionScope {
        /// Why, by name.
        reason: String,
    },
    /// The command names another birth than the one it reached: the incarnation this one holds.
    RejectedStaleIncarnation {
        /// The incarnation the receiver holds.
        held: IncarnationId,
    },
    /// The receiver cannot take the command yet (no connections writer, its server not ready).
    NotReady {
        /// Why, by name.
        reason: String,
    },
    /// The receiver could not resolve the peer the call came over.
    PeerUnresolved {
        /// Why, by name.
        reason: String,
    },
    /// The receiver is at its admission bound for the op.
    Busy {
        /// Why, by name.
        reason: String,
    },
    /// The receiver refuses new non-status calls while connections-deleting. Never sent for this op,
    /// which a connections-deleting node serves (a second `All` reaches it, step 5).
    RejectedConnectionsDeleting {
        /// Why, by name.
        reason: String,
    },
    /// The request did not decode or broke the protocol's bounds.
    Malformed {
        /// What was malformed.
        kind: MalformedKind,
    },
    /// The caller is not authorized for the op.
    Unauthorized {
        /// Why, by name.
        reason: String,
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
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 11;

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
        }
    }

    fn classify_reply(reply: &NodeCommandReply) -> ReplyKind {
        match reply {
            NodeCommandReply::CommandAccepted { .. } => ReplyKind::Success,
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
            NodeCommandReply::ConnectionsDeleting { .. } => FrameKind::Data,
            NodeCommandReply::ConnectionsDeleted { .. } => FrameKind::Terminal,
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
    fn both_scopes_obey_the_stream_order_and_a_refusal_is_one_frame() {
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
