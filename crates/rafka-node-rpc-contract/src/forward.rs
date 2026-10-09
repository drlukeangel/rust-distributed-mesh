//! Generic one-hop carried execution (node-rpc.md §36.1; i143.e6.s4) on core op `0x1A`.
//!
//! The origin asks a carrier to make exactly one direct inner call to an exact final target. The
//! request carries the target, the inner protocol op and the opaque inner request bytes; no origin
//! identity rides in it, because the target authenticates the carrier, never a copied origin.
//!
//! The carrier verifies the inner protocol is forwardable, makes one direct inner call, never
//! forwards again, and hands back the inner outcome without inventing domain semantics. The
//! forward protocol itself is never forwardable.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use rafka_mesh_entity::NodeId;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub struct Forward;

/// The largest inner request or reply a carrier hands across.
pub const MAX_CARRIED_BYTES: usize = 1024 * 1024;

/// What the carrier keeps back from the origin's remaining budget so its reply still reaches the
/// origin inside that budget: the request's transit from origin to carrier (the origin measures
/// before it writes), the carrier's work after its inner call ends (edge lookup, reply encoding),
/// the reply's write and transit back, and the origin's read and decode. It is a bounded
/// transport and serialization allowance, not a policy margin.
pub const FORWARD_REPLY_RESERVE: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardRequest {
    Forward {
        /// The exact final target's logical NodeId.
        target: NodeId,
        inner_op: u8,
        inner: Vec<u8>,
        /// The origin's remaining time budget for this call, in whole milliseconds, measured
        /// immediately before this frame is written to the carrier (after the carrier was
        /// resolved and dialed). For an overall budget it is deadline minus now; for a split
        /// budget it is the reply budget, which starts at the commit that follows the write. The
        /// carrier bounds its one inner call by this minus [`FORWARD_REPLY_RESERVE`].
        remaining_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardReply {
    /// The inner call reached the target, which answered: its reply payload, verbatim.
    Relayed { inner: Vec<u8> },
    /// The carrier proved the inner call never committed at the target.
    InnerNotSent { reason: String },
    /// The target does not serve the inner op.
    InnerUnserved { op: u8 },
    /// The target refused the inner call's fence (`425 STALE_TARGET`): it is not that node.
    InnerRejectedStale { target_node_id: NodeId },
    /// The inner call committed at the target and its outcome is unknown.
    InnerIndeterminate { reason: String },
    /// The inner protocol is not forwardable through this carrier.
    NotForwardable { op: u8 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    /// The carrier made no inner call it could prove arrived: its own Direct edge to the final
    /// target is not Active (connections.md §8). `reason` names the carrier's latest Direct fact
    /// toward the target. The origin retires its Proxy through this carrier with the structural
    /// reason `carrier-edge-lost`.
    CarrierEdgeLost { reason: String },
    /// The origin's remaining budget does not exceed [`FORWARD_REPLY_RESERVE`], so the carrier
    /// made no inner call. Both are whole milliseconds.
    NoInnerBudget { remaining_ms: u64, reserve_ms: u64 },
}

impl NodeProtocol for Forward {
    const OP: u8 = 0x1A;
    const NAME: &'static str = "forward";
    const MAX_REQUEST_FRAME_BYTES: usize = MAX_CARRIED_BYTES + 256;
    const MAX_REPLY_FRAME_BYTES: usize = MAX_CARRIED_BYTES + 256;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 14;

    type Request = ForwardRequest;
    type Reply = ForwardReply;

    fn classify_reply(reply: &ForwardReply) -> ReplyKind {
        match reply {
            ForwardReply::Relayed { .. } => ReplyKind::Success,
            ForwardReply::InnerNotSent { .. }
            | ForwardReply::InnerUnserved { .. }
            | ForwardReply::InnerRejectedStale { .. }
            | ForwardReply::InnerIndeterminate { .. }
            | ForwardReply::NotForwardable { .. }
            | ForwardReply::CarrierEdgeLost { .. }
            | ForwardReply::NoInnerBudget { .. } => ReplyKind::ProtocolRefusal,
            ForwardReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ForwardReply::NotReady { .. } => ReplyKind::NotReady,
            ForwardReply::Busy { .. } => ReplyKind::Busy,
            ForwardReply::Draining { .. } => ReplyKind::Draining,
            ForwardReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            ForwardReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> ForwardReply {
        ForwardReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> ForwardReply {
        ForwardReply::NotReady { reason }
    }
    fn busy(reason: String) -> ForwardReply {
        ForwardReply::Busy { reason }
    }
    fn draining(reason: String) -> ForwardReply {
        ForwardReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> ForwardReply {
        ForwardReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> ForwardReply {
        ForwardReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_forward_protocol_is_never_itself_forwardable() {
        assert!(!Forward::FORWARDABLE);
        assert_eq!(Forward::OP, 0x1A);
    }

    #[test]
    fn every_reply_variant_round_trips() {
        let replies = [
            ForwardReply::Relayed { inner: vec![1] },
            ForwardReply::InnerNotSent { reason: "r".into() },
            ForwardReply::InnerUnserved { op: 7 },
            ForwardReply::InnerRejectedStale { target_node_id: NodeId::mint() },
            ForwardReply::InnerIndeterminate { reason: "r".into() },
            ForwardReply::NotForwardable { op: 0x01 },
            ForwardReply::PeerUnresolved { reason: "p".into() },
            ForwardReply::NotReady { reason: "n".into() },
            ForwardReply::Busy { reason: "b".into() },
            ForwardReply::Draining { reason: "d".into() },
            ForwardReply::Malformed { kind: MalformedKind::Corrupt },
            ForwardReply::Unauthorized { reason: "u".into() },
            ForwardReply::CarrierEdgeLost { reason: "e".into() },
            ForwardReply::NoInnerBudget { remaining_ms: 5, reserve_ms: 100 },
        ];
        assert_eq!(replies.len() as u32, Forward::REPLY_VARIANTS);
        for r in replies {
            assert_eq!(Forward::decode_reply(&Forward::encode_reply(&r).unwrap()).unwrap(), r);
        }
    }
}
