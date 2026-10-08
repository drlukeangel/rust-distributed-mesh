//! The join on op `0x1D`, RDM's third control family (node-rpc.md §9; node-rpc-envelope.md tag
//! table; i143 R-J1).
//!
//! A node binds its transport on port 0 and the operating system assigns the port. Its first
//! call is `JoinNode` to the node-admin that deployed it: the request carries the node's full
//! membership digest (the same struct its gossip `Frame::Digest` carries) with the transport
//! address read from the bound endpoint. The admin verifies the digest against what it
//! deployed, installs that address for the node's key and answers what it holds.
//!
//! ```text
//! Joined { answer }     the digest matches the deployment; `answer` is the admin's entry answer
//! JoinMismatch          the digest disagrees with the deployment in `field`
//! NotAuthority          the receiver did not deploy this birth and holds no member of that name
//! ```
//!
//! The request carries the digest typed, as `WireDigest` (`rafka_mesh_entity::wire`, the same
//! positional shape a gossip frame carries). The answer is the postcard of the admin's entry
//! answer (`rafka-node-admin-core` `wire::WireEntryAnswer`): its topology projection, its
//! membership frames and its source snapshots are types of crates this contract does not depend
//! on, so they travel as one postcard frame of their own, never JSON. The family is not
//! forwardable: the deploying admin decides its own joins.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use rafka_mesh_entity::wire::WireDigest;
use serde::{Deserialize, Serialize};

pub struct Join;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinRequest {
    /// The node reports its full digest, with the address it really bound.
    JoinNode { digest: WireDigest },
}

impl JoinRequest {
    pub fn op(&self) -> &'static str {
        match self {
            Self::JoinNode { .. } => "join-node",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinReply {
    /// The join is taken. `answer` is what the admin holds now.
    /// `answer` is one postcard frame of the admin's `WireEntryAnswer`.
    Joined { answer: Vec<u8> },
    /// The reported digest disagrees with what the admin deployed.
    JoinMismatch { field: String, deployed: String, reported: String },
    /// The receiver did not deploy this birth; `primary` is the mesh primary it sees, if any.
    NotAuthority { primary: Option<String> },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl JoinReply {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Joined { .. } => "joined",
            Self::JoinMismatch { .. } => "join-mismatch",
            Self::NotAuthority { .. } => "not-authority",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::Draining { .. } => "draining",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
        }
    }
}

impl NodeProtocol for Join {
    const OP: u8 = 0x1D;
    const NAME: &'static str = "join";
    /// One digest, with its runtime fact and labels.
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    /// The admin's topology projection, members and statuses.
    const MAX_REPLY_FRAME_BYTES: usize = 4 * 1024 * 1024;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 9;

    type Request = JoinRequest;
    type Reply = JoinReply;

    fn classify_reply(reply: &JoinReply) -> ReplyKind {
        match reply {
            JoinReply::Joined { .. } => ReplyKind::Success,
            JoinReply::JoinMismatch { .. } | JoinReply::NotAuthority { .. } => ReplyKind::ProtocolRefusal,
            JoinReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            JoinReply::NotReady { .. } => ReplyKind::NotReady,
            JoinReply::Busy { .. } => ReplyKind::Busy,
            JoinReply::Draining { .. } => ReplyKind::Draining,
            JoinReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            JoinReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> JoinReply {
        JoinReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> JoinReply {
        JoinReply::NotReady { reason }
    }
    fn busy(reason: String) -> JoinReply {
        JoinReply::Busy { reason }
    }
    fn draining(reason: String) -> JoinReply {
        JoinReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> JoinReply {
        JoinReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> JoinReply {
        JoinReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip() {
        let all = [
            (JoinReply::Joined { answer: vec![1, 2, 3] }, ReplyKind::Success),
            (JoinReply::JoinMismatch { field: "incarnation".into(), deployed: "a".into(), reported: "b".into() }, ReplyKind::ProtocolRefusal),
            (JoinReply::NotAuthority { primary: Some("mesh1.admin.1".into()) }, ReplyKind::ProtocolRefusal),
            (Join::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (Join::not_ready("n".into()), ReplyKind::NotReady),
            (Join::busy("b".into()), ReplyKind::Busy),
            (Join::draining("d".into()), ReplyKind::Draining),
            (Join::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (Join::unauthorized("u".into()), ReplyKind::Unauthorized),
        ];
        assert_eq!(all.len() as u32, Join::REPLY_VARIANTS);
        for (r, kind) in all {
            let back = Join::decode_reply(&Join::encode_reply(&r).unwrap()).unwrap();
            assert_eq!(back, r);
            assert_eq!(Join::classify_reply(&back), kind);
        }
        
    }

    #[test]
    fn a_variant_past_the_declared_count_is_an_unknown_variant() {
        use crate::protocol::DecodeFailure;
        assert_eq!(Join::decode_reply(&[Join::REPLY_VARIANTS as u8]), Err(DecodeFailure::UnknownVariant));
        assert_eq!(Join::decode_request(&[Join::REQUEST_VARIANTS as u8]), Err(DecodeFailure::UnknownVariant));
    }
}
