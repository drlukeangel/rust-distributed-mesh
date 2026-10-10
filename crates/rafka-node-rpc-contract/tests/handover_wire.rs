//! Fixed bytes for the `0x21` fabric-primary-handover family. These literals lock discriminants,
//! the nested birth fields and the positional field order independently of codec round trips;
//! enums are append-only. Never regenerate expectations from the Rust serializer to make a schema
//! change pass.
//!
//! Hand derivation (postcard): an enum is its variant index as a varint; a string is its byte
//! length as a varint then its bytes; a u64 is a varint; a NodeId and an IncarnationId are strings.
//! A birth is `mesh | node_id | incarnation`.

use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc_contract::handover::{FabricPrimaryHandover, FabricPrimaryHandoverReply, FabricPrimaryHandoverRequest, HandoverBirth};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::{DecodeFailure, NodeProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn incumbent() -> HandoverBirth {
    HandoverBirth { mesh: "m".into(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId("i".into()) }
}

fn successor() -> HandoverBirth {
    HandoverBirth { mesh: "n".into(), node_id: NodeId::parse("0123456789cd").unwrap(), incarnation: IncarnationId("j".into()) }
}

/// `m`, `0123456789ab`, `i`.
const INCUMBENT: &str = "01 6d  0c 303132333435363738396162  01 69";
/// `n`, `0123456789cd`, `j`.
const SUCCESSOR: &str = "01 6e  0c 303132333435363738396364  01 6a";

#[test]
fn requests_match_the_frozen_two_variant_wire_schema() {
    let take = |epoch: u64| FabricPrimaryHandoverRequest::TakeFabricPrimary { fabric: "f".into(), incumbent: incumbent(), successor: successor(), expected_epoch: epoch, operation: "o".into() };
    let taken = FabricPrimaryHandoverRequest::FabricPrimaryTaken { fabric: "f".into(), incumbent: incumbent(), successor: successor(), expected_epoch: 7, committed_epoch: 8, operation: "o".into() };
    let fixtures = [
        // variant 0 | fabric | incumbent | successor | expected_epoch | operation
        (take(7), format!("00 0166 {INCUMBENT} {SUCCESSOR} 07 016f")),
        // 300 is the two-byte varint ac 02.
        (take(300), format!("00 0166 {INCUMBENT} {SUCCESSOR} ac02 016f")),
        // variant 1 | fabric | incumbent | successor | expected_epoch | committed_epoch | operation
        (taken, format!("01 0166 {INCUMBENT} {SUCCESSOR} 07 08 016f")),
    ];
    for (q, hex) in fixtures {
        assert_eq!(FabricPrimaryHandover::encode_request(&q).unwrap(), bytes(&hex), "{q:?}");
        assert_eq!(FabricPrimaryHandover::decode_request(&bytes(&hex)).unwrap(), q);
    }
    assert_eq!(FabricPrimaryHandover::decode_request(&[2]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_thirteen_variant_wire_schema() {
    use FabricPrimaryHandoverReply::*;
    let fixtures = [
        (Applied, "00"),
        (AlreadyApplied, "01"),
        (RejectedStaleEpoch { expected: 7, current: 8 }, "02 07 08"),
        (RejectedWrongBirth { role: "i".into() }, "03 01 69"),
        (RejectedNotAuthority, "04"),
        (RejectedNotEligible { reason: "e".into() }, "05 01 65"),
        (RejectedConflict { operation: "o".into() }, "06 01 6f"),
        (NotReady { reason: "n".into() }, "07 01 6e"),
        (Malformed { kind: MalformedKind::Corrupt }, "08 02"),
        (PeerUnresolved { reason: "p".into() }, "09 01 70"),
        (Busy { reason: "b".into() }, "0a 01 62"),
        (Draining { reason: "d".into() }, "0b 01 64"),
        (Unauthorized { reason: "u".into() }, "0c 01 75"),
    ];
    for (r, hex) in fixtures {
        assert_eq!(FabricPrimaryHandover::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(FabricPrimaryHandover::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(FabricPrimaryHandover::REPLY_VARIANTS, 13);
    assert_eq!(FabricPrimaryHandover::decode_reply(&[13]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn the_family_is_op_0x21_unary_and_not_forwardable_and_every_refusal_is_classified() {
    use FabricPrimaryHandoverReply::*;
    assert_eq!(FabricPrimaryHandover::OP, 0x21);
    assert!(!FabricPrimaryHandover::FORWARDABLE);
    assert_eq!(FabricPrimaryHandover::classify_reply(&Applied), ReplyKind::Success);
    assert_eq!(FabricPrimaryHandover::classify_reply(&AlreadyApplied), ReplyKind::Success);
    for r in [RejectedStaleEpoch { expected: 1, current: 2 }, RejectedWrongBirth { role: "i".into() }, RejectedNotAuthority, RejectedNotEligible { reason: "e".into() }, RejectedConflict { operation: "o".into() }] {
        assert_eq!(FabricPrimaryHandover::classify_reply(&r), ReplyKind::ProtocolRefusal, "{r:?}");
    }
}

#[test]
fn the_largest_request_fits_the_frame_ceiling() {
    let long = "x".repeat(128);
    let b = || HandoverBirth { mesh: long.clone(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId(long.clone()) };
    let q = FabricPrimaryHandoverRequest::FabricPrimaryTaken { fabric: long.clone(), incumbent: b(), successor: b(), expected_epoch: u64::MAX, committed_epoch: u64::MAX, operation: long.clone() };
    assert!(FabricPrimaryHandover::encode_request(&q).unwrap().len() <= FabricPrimaryHandover::MAX_REQUEST_FRAME_BYTES);
}
