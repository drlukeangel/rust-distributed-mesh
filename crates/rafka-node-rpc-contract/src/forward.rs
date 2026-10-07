//! Generic one-hop carried execution (node-rpc.md §36.1; i143.e6.s4) on core tag `0x1A`.
//!
//! The origin asks a carrier to make exactly one direct inner call to an exact final target. The
//! request carries the target, the inner protocol tag and the opaque inner request bytes; no origin
//! identity rides in it, because the target authenticates the carrier, never a copied origin.
//!
//! The carrier verifies the inner protocol is forwardable, makes one direct inner call, never
//! forwards again, and hands back the inner outcome without inventing domain semantics. The
//! forward protocol itself is never forwardable.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};

pub struct Forward;

/// The largest inner request or reply a carrier hands across.
pub const MAX_CARRIED_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardRequest {
    Forward {
        /// The exact final target's logical NodeId.
        target: String,
        inner_tag: u8,
        inner: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardReply {
    /// The inner call reached the target, which answered: its reply payload, verbatim.
    Relayed { inner: Vec<u8> },
    /// The carrier proved the inner call never committed at the target.
    InnerNotSent { reason: String },
    /// The target does not serve the inner tag.
    InnerUnserved { tag: u8 },
    /// The target refused the inner call's fence (`425 STALE_TARGET`): it is not that node.
    InnerRejectedStale { target_node_id: String },
    /// The inner call committed at the target and its outcome is unknown.
    InnerIndeterminate { reason: String },
    /// The inner protocol is not forwardable through this carrier.
    NotForwardable { tag: u8 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Forward {
    const TAG: u8 = 0x1A;
    const NAME: &'static str = "forward";
    const MAX_REQUEST_FRAME_BYTES: usize = MAX_CARRIED_BYTES + 256;
    const MAX_REPLY_FRAME_BYTES: usize = MAX_CARRIED_BYTES + 256;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 12;

    type Request = ForwardRequest;
    type Reply = ForwardReply;

    fn classify_reply(reply: &ForwardReply) -> ReplyKind {
        match reply {
            ForwardReply::Relayed { .. } => ReplyKind::Success,
            ForwardReply::InnerNotSent { .. }
            | ForwardReply::InnerUnserved { .. }
            | ForwardReply::InnerRejectedStale { .. }
            | ForwardReply::InnerIndeterminate { .. }
            | ForwardReply::NotForwardable { .. } => ReplyKind::ProtocolRefusal,
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
        assert_eq!(Forward::TAG, 0x1A);
    }

    #[test]
    fn every_reply_variant_round_trips() {
        let replies = [
            ForwardReply::Relayed { inner: vec![1] },
            ForwardReply::InnerNotSent { reason: "r".into() },
            ForwardReply::InnerUnserved { tag: 7 },
            ForwardReply::InnerRejectedStale { target_node_id: "n1".into() },
            ForwardReply::InnerIndeterminate { reason: "r".into() },
            ForwardReply::NotForwardable { tag: 0x11 },
            ForwardReply::PeerUnresolved { reason: "p".into() },
            ForwardReply::NotReady { reason: "n".into() },
            ForwardReply::Busy { reason: "b".into() },
            ForwardReply::Draining { reason: "d".into() },
            ForwardReply::Malformed { kind: MalformedKind::Corrupt },
            ForwardReply::Unauthorized { reason: "u".into() },
        ];
        assert_eq!(replies.len() as u32, Forward::REPLY_VARIANTS);
        for r in replies {
            assert_eq!(Forward::decode_reply(&Forward::encode_reply(&r).unwrap()).unwrap(), r);
        }
    }
}
