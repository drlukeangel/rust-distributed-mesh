//! Fixed bytes for the `0x21` node-primary-update family (the approved proposal
//! node-primary-update-fabric-mesh-and-node-type.md Part C3 edit 5 and Part D: "0x21 carries the
//! primitive's `PrimaryUpdate` / `PrimaryUpdated` for both seats; the incumbent writes the row").
//! These literals lock discriminants, the nested birth fields and the positional field order
//! independently of codec round trips; enums are append-only. Never regenerate expectations from the
//! Rust serializer to make a schema change pass.
//!
//! Hand derivation (postcard): an enum is its variant index as a varint; a string is its byte
//! length as a varint then its bytes; a u64 is a varint; a NodeId and an IncarnationId are strings.
//! A birth is `mesh | node_id | incarnation`.

use rdm_core::{IncarnationId, NodeId};
use rdm_core::handover::{CommittedSeat, HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply, NodePrimaryUpdateRequest, Seat, SeatRevision, SeatUpdate};
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::{DecodeFailure, NodeProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn holder() -> HandoverBirth {
    HandoverBirth { mesh: "m".into(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }
}

fn successor() -> HandoverBirth {
    HandoverBirth { mesh: "m".into(), node_id: NodeId::parse("0123456789cd").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("j")).unwrap() }
}

/// The holder's call on its successor moving both seats in one row (Q1): the mesh seat of `m` to
/// revision 4 and the fabric seat to revision 8.
fn primary_update() -> NodePrimaryUpdateRequest {
    NodePrimaryUpdateRequest::PrimaryUpdate {
        seats: vec![SeatUpdate { seat: Seat::MeshPrimary { mesh: "m".into() }, holder: holder(), successor: successor(), revision: 4 }, SeatUpdate { seat: Seat::FabricPrimary, holder: holder(), successor: successor(), revision: 8 }],
        build_id: "b".into(),
        attempt: 2,
        operation: "o".into(),
    }
}

/// The successor holding the fabric seat at revision 8.
fn committed() -> CommittedSeat {
    CommittedSeat { seat: Seat::FabricPrimary, holder: successor(), revision: 8 }
}

/// `m`, `0123456789ab`, `i`.
const HOLDER: &str = "01 6d  0c 303132333435363738396162  0c 79727a34396a333033796834";
/// `m`, `0123456789cd`, `j`.
const SUCCESSOR: &str = "01 6d  0c 303132333435363738396364  0c 79727a37396a3330337a7378";

/// CONTRACT: the family's three requests. `PrimaryUpdate` (variant 0) is the holder's call on its
/// successor carrying the committed row's content: every seat with its cohort, both exact births and
/// the committed revision, then the Build, its attempt and the operation. `UpdatePrimary` (variant 1) is
/// the commander's command to the holder of a node-admin seat, or to the mesh-primary of any other
/// cohort. `UpdatePrimaryFromRow` (variant 2) is a mesh-primary's command to the holder of a cohort
/// that is not node-admin, after its row and carrying the row's content (proposal Part C1: "its
/// command to the holder B1 comes after the row and carries it"). What must NOT happen: a name in
/// place of a birth, a field moved, a variant inserted before the end.
#[test]
fn requests_match_the_frozen_three_variant_wire_schema() {
    let fixtures = [
        // variant 0 | seats (len 2: MeshPrimary=1 { mesh "m" } holder successor revision 4, FabricPrimary=0
        // holder successor revision 8) | build_id | attempt | operation
        (primary_update(), format!("00 02 01 016d {HOLDER} {SUCCESSOR} 04 00 {HOLDER} {SUCCESSOR} 08 0162 02 016f")),
        // variant 1 | fabric | holder | seats (len 1: MeshPrimary=1 { mesh "m" } revision 3) | build_id | attempt | operation
        (
            NodePrimaryUpdateRequest::UpdatePrimary { fabric: "f".into(), holder: holder(), seats: vec![SeatRevision { seat: Seat::MeshPrimary { mesh: "m".into() }, expected_revision: 3 }], build_id: "b".into(), attempt: 2, operation: "o".into() },
            format!("01 0166 {HOLDER} 01 01 016d 03 0162 02 016f"),
        ),
        // variant 2 | fabric | seats (len 1: Cohort=2 { mesh "m", kind RpcNode=1 } holder successor revision 3) | build_id | attempt | operation
        (
            NodePrimaryUpdateRequest::UpdatePrimaryFromRow {
                fabric: "f".into(),
                seats: vec![SeatUpdate { seat: Seat::Cohort { mesh: "m".into(), kind: rdm_core::NodeKind::RpcNode }, holder: holder(), successor: successor(), revision: 3 }],
                build_id: "b".into(),
                attempt: 2,
                operation: "o".into(),
            },
            format!("02 0166 01 02 016d 01 {HOLDER} {SUCCESSOR} 03 0162 02 016f"),
        ),
    ];
    for (q, hex) in fixtures {
        assert_eq!(NodePrimaryUpdate::encode_request(&q).unwrap(), bytes(&hex), "{q:?}");
        assert_eq!(NodePrimaryUpdate::decode_request(&bytes(&hex)).unwrap(), q);
    }
    assert_eq!(NodePrimaryUpdate::REQUEST_VARIANTS, 3);
    assert_eq!(NodePrimaryUpdate::decode_request(&[3]), Err(DecodeFailure::UnknownVariant));
}

/// CONTRACT: the family's seventeen replies. `PrimaryUpdated` (node.primary.updated) first, then the
/// successor's two refusals (a conflicting payload, the initial fabric not ready), the holder's
/// refusals before the row, the runtime facts, `Incomplete` (the call after the row did not answer),
/// the framework's outcomes, and `Failed` (the step ended failed after the committed row). There is no
/// wrong-birth refusal (R-UC1a, R-RX5).
#[test]
fn replies_match_the_frozen_seventeen_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use NodePrimaryUpdateReply::*;
    let fixtures = [
        // revisions (len 1) | seat FabricPrimary=0 | holder birth | revision 8
        (PrimaryUpdated { revisions: vec![committed()] }, format!("00 01 00 {SUCCESSOR} 08")),
        (RejectedConflict { operation: "o".into() }, "01 01 6f".to_string()),
        (RejectedFabricNotReady { reason: "r".into() }, "02 01 72".to_string()),
        (RejectedNoSuccessor { reason: "n".into() }, "03 01 6e".to_string()),
        (RejectedNotAuthority { reason: "a".into() }, "04 01 61".to_string()),
        // seat MeshPrimary=1 { mesh "m" } | expected | current
        (RejectedStaleSeat { seat: Seat::MeshPrimary { mesh: "m".into() }, expected: 3, current: 4 }, "05 01 016d 03 04".to_string()),
        // seat MeshPrimary=1 { mesh "m" } | revision u64::MAX, the ten-byte varint ff×9 01
        (RejectedRevisionOverflow { seat: Seat::MeshPrimary { mesh: "m".into() }, revision: u64::MAX }, "06 01 016d ffffffffffffffffff01".to_string()),
        // source "s" | op "o" | cause "c"
        (RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() }, "07 0173 016f 0163".to_string()),
        // record "r" | holder "h"
        (RejectedRecordAbsent { record: "r".into(), holder: "h".into() }, "08 0172 0168".to_string()),
        (Incomplete { reason: "i".into() }, "09 01 69".to_string()),
        (NotReady { reason: "n".into() }, "0a 01 6e".to_string()),
        (Malformed { kind: MalformedKind::Corrupt }, "0b 02".to_string()),
        (PeerUnresolved { reason: "p".into() }, "0c 01 70".to_string()),
        (Busy { reason: "b".into() }, "0d 01 62".to_string()),
        (ConnectionsDeleting { reason: "d".into() }, "0e 01 64".to_string()),
        (Unauthorized { reason: "u".into() }, "0f 01 75".to_string()),
        // step "s" | reason "r"
        (Failed { step: "s".into(), reason: "r".into() }, "10 0173 0172".to_string()),
    ];
    for (r, hex) in fixtures {
        variants.insert(NodePrimaryUpdate::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<NodePrimaryUpdate>(&r);
        assert_eq!(NodePrimaryUpdate::encode_reply(&r).unwrap(), bytes(&hex), "{r:?}");
        assert_eq!(NodePrimaryUpdate::decode_reply(&bytes(&hex)).unwrap(), r);
    }
    assert_eq!(NodePrimaryUpdate::REPLY_VARIANTS, 17);
    assert_eq!(NodePrimaryUpdate::decode_reply(&[17]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), NodePrimaryUpdate::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<NodePrimaryUpdate>();
}

#[test]
fn the_family_is_op_0x21_unary_and_not_forwardable_and_every_refusal_is_classified() {
    use NodePrimaryUpdateReply::*;
    assert_eq!(NodePrimaryUpdate::OP, 0x21);
    assert_eq!(NodePrimaryUpdate::NAME, "node-primary-update");
    assert!(!NodePrimaryUpdate::FORWARDABLE);
    assert_eq!(NodePrimaryUpdate::classify_reply(&PrimaryUpdated { revisions: vec![committed()] }), ReplyKind::Success);
    for r in [
        RejectedConflict { operation: "o".into() },
        RejectedFabricNotReady { reason: "r".into() },
        RejectedNoSuccessor { reason: "n".into() },
        RejectedNotAuthority { reason: "a".into() },
        RejectedStaleSeat { seat: Seat::MeshPrimary { mesh: "m".into() }, expected: 1, current: 2 },
        RejectedRevisionOverflow { seat: Seat::FabricPrimary, revision: u64::MAX },
        RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() },
        RejectedRecordAbsent { record: "r".into(), holder: "h".into() },
        Incomplete { reason: "i".into() },
        Failed { step: "s".into(), reason: "r".into() },
    ] {
        assert_eq!(NodePrimaryUpdate::classify_reply(&r).code(), match r { RejectedStaleSeat { .. } | RejectedConflict { .. } => rdm_core::outcome::Code::Aborted, RejectedRecordAbsent { .. } => rdm_core::outcome::Code::NotFound, _ => rdm_core::outcome::Code::FailedPrecondition }, "{r:?}");
    }
}

#[test]
fn the_largest_request_fits_the_frame_ceiling() {
    let long = "x".repeat(128);
    let b = || HandoverBirth { mesh: long.clone(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&"z".repeat(12)).unwrap() };
    let seats = vec![SeatUpdate { seat: Seat::MeshPrimary { mesh: long.clone() }, holder: b(), successor: b(), revision: u64::MAX }, SeatUpdate { seat: Seat::FabricPrimary, holder: b(), successor: b(), revision: u64::MAX }];
    let q = NodePrimaryUpdateRequest::PrimaryUpdate { seats, build_id: long.clone(), attempt: u32::MAX, operation: long.clone() };
    assert!(NodePrimaryUpdate::encode_request(&q).unwrap().len() <= NodePrimaryUpdate::MAX_REQUEST_FRAME_BYTES);
    let seats = vec![SeatRevision { seat: Seat::MeshPrimary { mesh: long.clone() }, expected_revision: u64::MAX }, SeatRevision { seat: Seat::FabricPrimary, expected_revision: u64::MAX }];
    let q = NodePrimaryUpdateRequest::UpdatePrimary { fabric: long.clone(), holder: b(), seats, build_id: long.clone(), attempt: u32::MAX, operation: long.clone() };
    assert!(NodePrimaryUpdate::encode_request(&q).unwrap().len() <= NodePrimaryUpdate::MAX_REQUEST_FRAME_BYTES);
    let r = NodePrimaryUpdateReply::PrimaryUpdated { revisions: vec![CommittedSeat { seat: Seat::MeshPrimary { mesh: long.clone() }, holder: b(), revision: u64::MAX }, CommittedSeat { seat: Seat::FabricPrimary, holder: b(), revision: u64::MAX }] };
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES);
    // A source failure whose fields are each longer than the bound is cut to its budgets, never reset.
    let huge = "x".repeat(4 * NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES);
    let r = NodePrimaryUpdateReply::source_failed(&huge, &huge, &huge);
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES, "{r:?}");
    // So is an absent record whose record and holder are each longer than the bound.
    let r = NodePrimaryUpdateReply::record_absent(&huge, &huge);
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES, "{r:?}");
    let NodePrimaryUpdateReply::RejectedRecordAbsent { record, holder } = &r else { unreachable!() };
    assert!(record.ends_with('…') && holder.ends_with('…'), "a cut field says it was cut: {r:?}");
    let short = NodePrimaryUpdateReply::record_absent("the seat-transfer row o under mesh m", "mesh2.admin.1");
    assert_eq!(short, NodePrimaryUpdateReply::RejectedRecordAbsent { record: "the seat-transfer row o under mesh m".into(), holder: "mesh2.admin.1".into() }, "fields within their budgets are kept whole");
    // A failed step whose step and reason are each longer than the bound is cut to its budgets, never reset.
    let r = NodePrimaryUpdateReply::failed(&huge, &huge);
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES, "{r:?}");
    // Each refusal's reason is cut to the reply's bound, never reset.
    let r = NodePrimaryUpdateReply::no_successor(&huge);
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES, "{r:?}");
}

#[test]
fn every_reply_variant_of_node_primary_update_classifies_to_one_code() {
    replies_match_the_frozen_seventeen_variant_wire_schema();
}
