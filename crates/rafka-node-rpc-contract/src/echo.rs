//! The RDM core Echo protocol on tag `0x11` (ownership amendment §10): the
//! first live Node RPC family and the transport's sanity surface.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};

pub struct Echo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EchoRequest {
    Echo { traceparent: Option<String>, payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EchoReply {
    Echoed { payload: Vec<u8> },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Echo {
    const TAG: u8 = 0x11;
    const NAME: &'static str = "echo";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024 + 64;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;

    type Request = EchoRequest;
    type Reply = EchoReply;

    fn traceparent(req: &EchoRequest) -> Option<&str> {
        let EchoRequest::Echo { traceparent, .. } = req;
        traceparent.as_deref()
    }

    fn classify_reply(reply: &EchoReply) -> ReplyKind {
        match reply {
            EchoReply::Echoed { .. } => ReplyKind::Success,
            EchoReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            EchoReply::NotReady { .. } => ReplyKind::NotReady,
            EchoReply::Busy { .. } => ReplyKind::Busy,
            EchoReply::Draining { .. } => ReplyKind::Draining,
            EchoReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            EchoReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> EchoReply {
        EchoReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> EchoReply {
        EchoReply::NotReady { reason }
    }
    fn busy(reason: String) -> EchoReply {
        EchoReply::Busy { reason }
    }
    fn draining(reason: String) -> EchoReply {
        EchoReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> EchoReply {
        EchoReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> EchoReply {
        EchoReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{decode_request, decode_single_frame, encode_frame, encode_request, encode_varint};
    use crate::protocol::DecodeFailure;

    #[test]
    fn request_codec_round_trips_through_the_frame() {
        let req = EchoRequest::Echo { traceparent: Some("00-ab-cd-01".into()), payload: b"ping".to_vec() };
        let target = crate::framing::RequestTarget { node_id: "n1".into(), incarnation: "i1".into(), slot: "rpc".into(), freshness: "f".into() };
        let frame = encode_request(Echo::TAG, &target, &Echo::encode_request(&req).unwrap());
        let (tag, at, body) = decode_request(&frame, |t| (t == Echo::TAG).then_some(Echo::MAX_REQUEST_FRAME_BYTES)).unwrap();
        assert_eq!((tag, at), (0x11, target));
        let back = Echo::decode_request(body).unwrap();
        assert_eq!(back, req);
        assert_eq!(Echo::traceparent(&back), Some("00-ab-cd-01"));
    }

    /// node-rpc.md §25: every shared constructor survives constructor -> encode
    /// -> decode -> classify with the same class.
    #[test]
    fn every_shared_constructor_classifies_the_same_after_a_round_trip() {
        let cases = [
            (Echo::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (Echo::not_ready("n".into()), ReplyKind::NotReady),
            (Echo::busy("b".into()), ReplyKind::Busy),
            (Echo::draining("d".into()), ReplyKind::Draining),
            (Echo::malformed(MalformedKind::TooLarge), ReplyKind::Malformed(MalformedKind::TooLarge)),
            (Echo::malformed(MalformedKind::UnknownVariant), ReplyKind::Malformed(MalformedKind::UnknownVariant)),
            (Echo::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (Echo::unauthorized("u".into()), ReplyKind::Unauthorized),
            (EchoReply::Echoed { payload: vec![1, 2] }, ReplyKind::Success),
        ];
        for (reply, class) in cases {
            assert_eq!(Echo::classify_reply(&reply), class);
            let framed = encode_frame(&Echo::encode_reply(&reply).unwrap());
            let back = Echo::decode_reply(decode_single_frame(&framed, Echo::MAX_REPLY_FRAME_BYTES).unwrap()).unwrap();
            assert_eq!(back, reply);
            assert_eq!(Echo::classify_reply(&back), class);
        }
    }

    #[test]
    fn unknown_operation_variant_and_corrupt_bytes_are_distinct() {
        let mut unknown = Vec::new();
        encode_varint(1, &mut unknown); // variant 1 does not exist in this build
        assert_eq!(Echo::decode_request(&unknown), Err(DecodeFailure::UnknownVariant));
        assert_eq!(Echo::decode_request(&[0, 1]), Err(DecodeFailure::Corrupt));
        assert_eq!(Echo::decode_request(&[]), Err(DecodeFailure::Corrupt));
    }
}
