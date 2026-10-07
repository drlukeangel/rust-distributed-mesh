//! The core ping on op `1`: the transport's liveness and sanity surface, and the
//! mesh primary's 45 s offline tickle (fabric-node-lifecycle.md §7.3). Op `0` is
//! reserved as invalid, so a zeroed fence is never served.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};

pub struct Ping;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PingRequest {
    Ping { payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PingReply {
    Pong { payload: Vec<u8> },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Ping {
    const OP: u8 = 0x01;
    const NAME: &'static str = "ping";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024 + 64;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;

    type Request = PingRequest;
    type Reply = PingReply;

    fn classify_reply(reply: &PingReply) -> ReplyKind {
        match reply {
            PingReply::Pong { .. } => ReplyKind::Success,
            PingReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            PingReply::NotReady { .. } => ReplyKind::NotReady,
            PingReply::Busy { .. } => ReplyKind::Busy,
            PingReply::Draining { .. } => ReplyKind::Draining,
            PingReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            PingReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> PingReply {
        PingReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> PingReply {
        PingReply::NotReady { reason }
    }
    fn busy(reason: String) -> PingReply {
        PingReply::Busy { reason }
    }
    fn draining(reason: String) -> PingReply {
        PingReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> PingReply {
        PingReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> PingReply {
        PingReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{decode_request, decode_single_frame, encode_frame, encode_request, encode_varint};
    use crate::protocol::DecodeFailure;

    #[test]
    fn request_codec_round_trips_through_the_frame() {
        let req = PingRequest::Ping { payload: b"ping".to_vec() };
        let header = crate::framing::RequestHeader::fence(crate::framing::Fence { target_node_id: "n1".into(), op: 0x01 });
        let frame = encode_request(&header, &Ping::encode_request(&req).unwrap());
        let (op, at, body) = decode_request(&frame, |t| (t == Ping::OP).then_some(Ping::MAX_REQUEST_FRAME_BYTES)).unwrap();
        assert_eq!((op, at), (0x01, header));
        let back = Ping::decode_request(body).unwrap();
        assert_eq!(back, req);
    }

    /// node-rpc.md §25: every shared constructor survives constructor -> encode
    /// -> decode -> classify with the same class.
    #[test]
    fn every_shared_constructor_classifies_the_same_after_a_round_trip() {
        let cases = [
            (Ping::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (Ping::not_ready("n".into()), ReplyKind::NotReady),
            (Ping::busy("b".into()), ReplyKind::Busy),
            (Ping::draining("d".into()), ReplyKind::Draining),
            (Ping::malformed(MalformedKind::TooLarge), ReplyKind::Malformed(MalformedKind::TooLarge)),
            (Ping::malformed(MalformedKind::UnknownVariant), ReplyKind::Malformed(MalformedKind::UnknownVariant)),
            (Ping::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (Ping::unauthorized("u".into()), ReplyKind::Unauthorized),
            (PingReply::Pong { payload: vec![1, 2] }, ReplyKind::Success),
        ];
        for (reply, class) in cases {
            assert_eq!(Ping::classify_reply(&reply), class);
            let framed = encode_frame(&Ping::encode_reply(&reply).unwrap());
            let back = Ping::decode_reply(decode_single_frame(&framed, Ping::MAX_REPLY_FRAME_BYTES).unwrap()).unwrap();
            assert_eq!(back, reply);
            assert_eq!(Ping::classify_reply(&back), class);
        }
    }

    #[test]
    fn unknown_operation_variant_and_corrupt_bytes_are_distinct() {
        let mut unknown = Vec::new();
        encode_varint(1, &mut unknown); // variant 1 does not exist in this build
        assert_eq!(Ping::decode_request(&unknown), Err(DecodeFailure::UnknownVariant));
        assert_eq!(Ping::decode_request(&[0, 1]), Err(DecodeFailure::Corrupt));
        assert_eq!(Ping::decode_request(&[]), Err(DecodeFailure::Corrupt));
    }
}
