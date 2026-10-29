//! Fixed bytes for the `0x1F` Build-facts family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::build_facts::{BuildFacts, BuildFactsReply, BuildFactsRequest};
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::streaming::{FrameKind, StreamingProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn requests_match_the_frozen_wire_schema() {
    let fixtures = [
        (BuildFactsRequest::FetchBuildFacts { build_id: "b".into() }, "00 01 62"),
        (BuildFactsRequest::FetchBuildFacts { build_id: "3qcfk1kq47m9".into() }, "00 0c 337163666b316b7134376d39"),
        (BuildFactsRequest::FetchCurrentBuild, "01"),
        (BuildFactsRequest::FetchBuildSection { build_id: Some("b".into()), mesh: "m".into() }, "02 01 01 62 01 6d"),
        (BuildFactsRequest::FetchBuildSection { build_id: None, mesh: "m".into() }, "02 00 01 6d"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(BuildFacts::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(BuildFacts::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(BuildFacts::REQUEST_VARIANTS, 3);
    assert_eq!(BuildFacts::decode_request(&[3]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_eleven_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use BuildFactsReply::*;
    let fixtures = [
        (Facts { build_id: "b".into(), chunk_index: 1, chunk_count: 2, facts: vec![0xAA, 0xBB] }, "00 01 62 01 02 02 aabb"),
        (End { build_id: "b".into(), facts: 3, chunks: 2, complete: true }, "01 01 62 03 02 01"),
        (End { build_id: "b".into(), facts: 0, chunks: 0, complete: false }, "01 01 62 00 00 00"),
        (NotReady { reason: "n".into() }, "02 01 6e"),
        (UnknownBuild { build_id: "b".into() }, "03 01 62"),
        (PeerUnresolved { reason: "p".into() }, "04 01 70"),
        (Busy { reason: "b".into() }, "05 01 62"),
        (ConnectionsDeleting { reason: "d".into() }, "06 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "07 02"),
        (Unauthorized { reason: "u".into() }, "08 01 75"),
        (Started, "09"),
        (NoCurrentBuild { reason: "n".into() }, "0a 01 6e"),
    ];
    for (r, hex) in fixtures {
        variants.insert(BuildFacts::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<BuildFacts>(&r);
        assert_eq!(BuildFacts::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(BuildFacts::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(BuildFacts::REPLY_VARIANTS, 11);
    assert_eq!(BuildFacts::decode_reply(&[11]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), BuildFacts::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<BuildFacts>();
}

#[test]
fn every_frame_is_classified_for_stream_order_and_the_family_is_not_forwardable() {
    use BuildFactsReply::*;
    assert!(!BuildFacts::FORWARDABLE);
    assert_eq!(BuildFacts::OP, 0x1F);
    assert_eq!(BuildFacts::frame_kind(&Started), FrameKind::Started);
    assert_eq!(BuildFacts::frame_kind(&Facts { build_id: "b".into(), chunk_index: 0, chunk_count: 1, facts: vec![] }), FrameKind::Data);
    assert_eq!(BuildFacts::frame_kind(&End { build_id: "b".into(), facts: 0, chunks: 0, complete: false }), FrameKind::Terminal);
    assert_eq!(BuildFacts::frame_kind(&NotReady { reason: "n".into() }), FrameKind::Refusal(ReplyKind::NotReady));
    assert_eq!(BuildFacts::frame_kind(&UnknownBuild { build_id: "b".into() }), FrameKind::Refusal(ReplyKind::new(rdm_core::outcome::Code::NotFound)));
    assert_eq!(BuildFacts::frame_kind(&NoCurrentBuild { reason: "n".into() }), FrameKind::Refusal(ReplyKind::new(rdm_core::outcome::Code::NotFound)));
}

#[test]
fn a_full_chunk_fits_the_reply_frame_ceiling() {
    let r = BuildFactsReply::Facts { build_id: "cq2tyb03krac".into(), chunk_index: u32::MAX, chunk_count: u32::MAX, facts: vec![0u8; rafka_mesh_transport_bound()] };
    assert!(BuildFacts::encode_reply(&r).unwrap().len() <= BuildFacts::MAX_REPLY_FRAME_BYTES);
}

/// The Build topic's message bound (`rafka-mesh-transport` `chunking::MAX_MESSAGE_BYTES`, 4096 - 64).
fn rafka_mesh_transport_bound() -> usize {
    4096 - 64
}

#[test]
fn every_reply_variant_of_build_facts_classifies_to_one_code() {
    replies_match_the_frozen_eleven_variant_wire_schema();
}
