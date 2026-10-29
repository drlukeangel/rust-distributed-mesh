//! Fixed bytes for the `0x23` read family (Q285). These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::filter::{Entity, Filter};
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::read::{Read, ReadReply, ReadRequest};
use rdm_core::streaming::{FrameKind, StreamingProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn requests_match_the_frozen_one_variant_wire_schema() {
    for (q, hex) in [
        (ReadRequest::Get { entity: Entity::Nodes, filter: Filter::new() }, "00 02 00"),
        (ReadRequest::Get { entity: Entity::Fabrics, filter: Filter::new().eq("mesh", ["m"]) }, "00 00 01 04 6d657368 01 01 6d"),
    ] {
        assert_eq!(Read::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Read::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Read::REQUEST_VARIANTS, 1);
    assert_eq!(Read::decode_request(&[1]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_fourteen_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use ReadReply::*;
    let fixtures = [
        (Started { read: "r".into() }, "00 01 72", FrameKind::Started),
        (Nodes { nodes: Vec::new() }, "01 00", FrameKind::Data),
        (Meshes { meshes: Vec::new() }, "02 00", FrameKind::Data),
        (Fabrics { fabrics: Vec::new() }, "03 00", FrameKind::Data),
        (End { rows: 3, frames: 2 }, "04 03 02", FrameKind::Terminal),
    ];
    for (r, hex, kind) in fixtures {
        variants.insert(Read::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Read>(&r);
        assert_eq!(Read::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Read::decode_reply(&bytes(hex)).unwrap(), r);
        assert_eq!(Read::frame_kind(&r), kind, "{r:?}");
    }
    let refusals = [
        (RejectedFilter { reason: "f".into() }, "05 01 66"),
        (ReadOverBound { read: "r".into(), row: "w".into(), bytes: 5, max: 4 }, "06 01 72 01 77 05 04"),
        (RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() }, "07 01 73 01 6f 01 63"),
        (NotReady { reason: "n".into() }, "08 01 6e"),
        (PeerUnresolved { reason: "p".into() }, "09 01 70"),
        (Busy { reason: "b".into() }, "0a 01 62"),
        (ConnectionsDeleting { reason: "d".into() }, "0b 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "0c 02"),
        (Unauthorized { reason: "u".into() }, "0d 01 75"),
    ];
    for (r, hex) in refusals {
        variants.insert(Read::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Read>(&r);
        assert_eq!(Read::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Read::decode_reply(&bytes(hex)).unwrap(), r);
        assert!(matches!(Read::frame_kind(&r), FrameKind::Refusal(_)), "{r:?}");
    }
    assert_eq!(Read::REPLY_VARIANTS, 14);
    assert_eq!(Read::decode_reply(&[14]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), Read::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<Read>();
}
