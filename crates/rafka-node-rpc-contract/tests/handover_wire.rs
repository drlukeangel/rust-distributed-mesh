//! Fixed bytes for the `0x21` fabric-primary-handover family. These literals lock discriminants,
//! the nested birth fields and the positional field order independently of codec round trips;
//! enums are append-only. Never regenerate expectations from the Rust serializer to make a schema
//! change pass.
//!
//! Hand derivation (postcard): an enum is its variant index as a varint; a string is its byte
//! length as a varint then its bytes; a u64 is a varint; a NodeId and an IncarnationId are strings.
//! A birth is `mesh | node_id | incarnation`.

use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc_contract::handover::{CommittedSeat, FabricPrimaryHandover, FabricPrimaryHandoverReply, FabricPrimaryHandoverRequest, HandoverBirth, Seat, SeatEpoch};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::{DecodeFailure, NodeProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn incumbent() -> HandoverBirth {
    HandoverBirth { mesh: "m".into(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap() }
}

fn successor() -> HandoverBirth {
    HandoverBirth { mesh: "n".into(), node_id: NodeId::parse("0123456789cd").unwrap(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("j")).unwrap() }
}

fn take_seats() -> FabricPrimaryHandoverRequest {
    FabricPrimaryHandoverRequest::TakeSeats {
        fabric: "f".into(),
        holder: incumbent(),
        successor: successor(),
        seats: vec![SeatEpoch { seat: Seat::MeshPrimary { mesh: "m".into() }, expected_epoch: 3 }, SeatEpoch { seat: Seat::FabricPrimary, expected_epoch: 7 }],
        build_id: "b".into(),
        attempt: 2,
        operation: "o".into(),
    }
}

/// The successor holding the fabric seat at epoch 8.
fn committed() -> CommittedSeat {
    CommittedSeat { seat: Seat::FabricPrimary, holder: successor(), epoch: 8 }
}

/// `m`, `0123456789ab`, `i`.
const INCUMBENT: &str = "01 6d  0c 303132333435363738396162  0c 79727a34396a333033796834";
/// `n`, `0123456789cd`, `j`.
const SUCCESSOR: &str = "01 6e  0c 303132333435363738396364  0c 79727a37396a3330337a7378";

#[test]
fn requests_match_the_frozen_three_variant_wire_schema() {
    let take = |epoch: u64| FabricPrimaryHandoverRequest::TakeFabricPrimary { fabric: "f".into(), incumbent: incumbent(), successor: successor(), expected_epoch: epoch, operation: "o".into() };
    let taken = FabricPrimaryHandoverRequest::FabricPrimaryTaken { fabric: "f".into(), incumbent: incumbent(), successor: successor(), expected_epoch: 7, committed_epoch: 8, operation: "o".into() };
    let fixtures = [
        // variant 0 | fabric | incumbent | successor | expected_epoch | operation
        (take(7), format!("00 0166 {INCUMBENT} {SUCCESSOR} 07 016f")),
        // 300 is the two-byte varint ac 02.
        (take(300), format!("00 0166 {INCUMBENT} {SUCCESSOR} ac02 016f")),
        // variant 1 | fabric | incumbent | successor | expected_epoch | committed_epoch | operation
        (taken, format!("01 0166 {INCUMBENT} {SUCCESSOR} 07 08 016f")),
        // variant 2 | fabric | holder | successor | seats (len 2: MeshPrimary=1 { mesh "m" } epoch 3,
        // FabricPrimary=0 epoch 7) | build_id | attempt | operation
        (take_seats(), format!("02 0166 {INCUMBENT} {SUCCESSOR} 02 01 016d 03 00 07 0162 02 016f")),
    ];
    for (q, hex) in fixtures {
        assert_eq!(FabricPrimaryHandover::encode_request(&q).unwrap(), bytes(&hex), "{q:?}");
        assert_eq!(FabricPrimaryHandover::decode_request(&bytes(&hex)).unwrap(), q);
    }
    assert_eq!(FabricPrimaryHandover::REQUEST_VARIANTS, 3);
    assert_eq!(FabricPrimaryHandover::decode_request(&[3]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_nineteen_variant_wire_schema() {
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
        // seats (len 1) | seat FabricPrimary=0 | holder birth | epoch 8
        (SeatsCommitted { seats: vec![committed()] }, "0d 01 00  01 6e  0c 303132333435363738396364  0c 79727a37396a3330337a7378  08"),
        // seat MeshPrimary=1 { mesh "m" } | expected | current
        (RejectedStaleSeat { seat: Seat::MeshPrimary { mesh: "m".into() }, expected: 3, current: 4 }, "0e 01 016d 03 04"),
        // seat MeshPrimary=1 { mesh "m" } | epoch u64::MAX, the ten-byte varint ff×9 01
        (RejectedEpochOverflow { seat: Seat::MeshPrimary { mesh: "m".into() }, epoch: u64::MAX }, "0f 01 016d ffffffffffffffffff01"),
        // seat FabricPrimary=0 | current 7
        (RejectedSeatOmitted { seat: Seat::FabricPrimary, current: 7 }, "10 00 07"),
        // source "s" | op "o" | cause "c"
        (RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() }, "11 0173 016f 0163"),
        // record "r" | holder "h"
        (RejectedRecordAbsent { record: "r".into(), holder: "h".into() }, "12 0172 0168"),
    ];
    for (r, hex) in fixtures {
        assert_eq!(FabricPrimaryHandover::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(FabricPrimaryHandover::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(FabricPrimaryHandover::REPLY_VARIANTS, 19);
    assert_eq!(FabricPrimaryHandover::decode_reply(&[19]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn the_family_is_op_0x21_unary_and_not_forwardable_and_every_refusal_is_classified() {
    use FabricPrimaryHandoverReply::*;
    assert_eq!(FabricPrimaryHandover::OP, 0x21);
    assert!(!FabricPrimaryHandover::FORWARDABLE);
    assert_eq!(FabricPrimaryHandover::classify_reply(&Applied), ReplyKind::Success);
    assert_eq!(FabricPrimaryHandover::classify_reply(&AlreadyApplied), ReplyKind::Success);
    assert_eq!(FabricPrimaryHandover::classify_reply(&SeatsCommitted { seats: vec![committed()] }), ReplyKind::Success);
    for r in [RejectedStaleEpoch { expected: 1, current: 2 }, RejectedWrongBirth { role: "i".into() }, RejectedNotAuthority, RejectedNotEligible { reason: "e".into() }, RejectedConflict { operation: "o".into() }, RejectedStaleSeat { seat: Seat::FabricPrimary, expected: 1, current: 2 }, RejectedEpochOverflow { seat: Seat::FabricPrimary, epoch: u64::MAX }, RejectedSeatOmitted { seat: Seat::FabricPrimary, current: 7 }, RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() }, RejectedRecordAbsent { record: "r".into(), holder: "h".into() }] {
        assert_eq!(FabricPrimaryHandover::classify_reply(&r), ReplyKind::ProtocolRefusal, "{r:?}");
    }
}

#[test]
fn the_largest_request_fits_the_frame_ceiling() {
    let long = "x".repeat(128);
    let b = || HandoverBirth { mesh: long.clone(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&"z".repeat(12)).unwrap() };
    let q = FabricPrimaryHandoverRequest::FabricPrimaryTaken { fabric: long.clone(), incumbent: b(), successor: b(), expected_epoch: u64::MAX, committed_epoch: u64::MAX, operation: long.clone() };
    assert!(FabricPrimaryHandover::encode_request(&q).unwrap().len() <= FabricPrimaryHandover::MAX_REQUEST_FRAME_BYTES);
    let seats = vec![SeatEpoch { seat: Seat::MeshPrimary { mesh: long.clone() }, expected_epoch: u64::MAX }, SeatEpoch { seat: Seat::FabricPrimary, expected_epoch: u64::MAX }];
    let q = FabricPrimaryHandoverRequest::TakeSeats { fabric: long.clone(), holder: b(), successor: b(), seats, build_id: long.clone(), attempt: u32::MAX, operation: long.clone() };
    assert!(FabricPrimaryHandover::encode_request(&q).unwrap().len() <= FabricPrimaryHandover::MAX_REQUEST_FRAME_BYTES);
    let r = FabricPrimaryHandoverReply::SeatsCommitted { seats: vec![CommittedSeat { seat: Seat::MeshPrimary { mesh: long.clone() }, holder: b(), epoch: u64::MAX }, CommittedSeat { seat: Seat::FabricPrimary, holder: b(), epoch: u64::MAX }] };
    assert!(FabricPrimaryHandover::encode_reply(&r).unwrap().len() <= FabricPrimaryHandover::MAX_REPLY_FRAME_BYTES);
    // A source failure whose fields are each longer than the bound is cut to its budgets, never reset.
    let huge = "x".repeat(4 * FabricPrimaryHandover::MAX_REPLY_FRAME_BYTES);
    let r = FabricPrimaryHandoverReply::source_failed(&huge, &huge, &huge);
    assert!(FabricPrimaryHandover::encode_reply(&r).unwrap().len() <= FabricPrimaryHandover::MAX_REPLY_FRAME_BYTES, "{r:?}");
    // So is an absent record whose record and holder are each longer than the bound.
    let r = FabricPrimaryHandoverReply::record_absent(&huge, &huge);
    assert!(FabricPrimaryHandover::encode_reply(&r).unwrap().len() <= FabricPrimaryHandover::MAX_REPLY_FRAME_BYTES, "{r:?}");
    let FabricPrimaryHandoverReply::RejectedRecordAbsent { record, holder } = &r else { unreachable!() };
    assert!(record.ends_with('…') && holder.ends_with('…'), "a cut field says it was cut: {r:?}");
    let short = FabricPrimaryHandoverReply::record_absent("the fabric-primary seat record of fabric1", "mesh2.admin.1");
    assert_eq!(short, FabricPrimaryHandoverReply::RejectedRecordAbsent { record: "the fabric-primary seat record of fabric1".into(), holder: "mesh2.admin.1".into() }, "fields within their budgets are kept whole");
}
