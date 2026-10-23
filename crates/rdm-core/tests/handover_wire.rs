//! Fixed bytes for the `0x21` node-primary-update family (node-rpc-envelope.md "PrimaryUpdate, op
//! `0x21`, rdm"). These literals lock discriminants, the nested birth fields and the positional field
//! order independently of codec round trips; enums are append-only. Never regenerate expectations from
//! the Rust serializer to make a schema change pass.
//!
//! Hand derivation (postcard): an enum is its variant index as a varint; a string is its byte
//! length as a varint then its bytes; a u64 and a u32 are varints; a NodeId and an IncarnationId are
//! strings; a tuple is its fields in order. A birth is `node_id | incarnation`; a take-over's birth is
//! `mesh | node_id | incarnation`.

use rdm_core::framing::Birth;
use rdm_core::handover::{NodePrimaryUpdate, NodePrimaryUpdateReply, NodePrimaryUpdateRequest};
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::{IncarnationId, NodeId};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn primary() -> Birth {
    Birth { node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }
}

fn successor() -> Birth {
    Birth { node_id: NodeId::parse("0123456789cd").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("j")).unwrap() }
}

/// The fabric-primary's entry: the primary's and the successor's exact births at revision 8.
fn fabric_entry() -> (String, Birth, Birth, u64) {
    ("fabric".into(), primary(), successor(), 8)
}

/// `0123456789ab`, `i`.
const PRIMARY: &str = "0c 303132333435363738396162  0c 79727a34396a333033796834";
/// `0123456789cd`, `j`.
const SUCCESSOR: &str = "0c 303132333435363738396364  0c 79727a37396a3330337a7378";

/// CONTRACT: the family's two requests. `PrimaryUpdate` (variant 0) is the primary's call on its
/// successor carrying the election's result: one entry per primary that moves, `(node_type, primary,
/// successor, revision)`, the fabric-primary an entry of node type `fabric`, then the Build, its run and
/// the operation. `UpdatePrimary` (variant 1) is the command from above: the node type whose primary
/// moves, the Build, its run, the operation and the successor's exact birth the mesh-primary named, none
/// to a node-admin (Q129). There is no take-over call (Q122). What must NOT happen: a name in place of a birth, a field moved, a variant inserted before
/// the end.
#[test]
fn requests_match_the_frozen_two_variant_wire_schema() {
    let fixtures = [
        // variant 0 | primaries (len 2: "node-admin" primary successor 4, "fabric" primary successor 8) | build_id | run | operation
        (
            NodePrimaryUpdateRequest::PrimaryUpdate { primaries: vec![("node-admin".into(), primary(), successor(), 4), fabric_entry()], build_id: "b".into(), run: 2, operation: "o".into() },
            format!("00 02 0a 6e6f64652d61646d696e {PRIMARY} {SUCCESSOR} 04 06 666162726963 {PRIMARY} {SUCCESSOR} 08 0162 02 016f"),
        ),
        // variant 1 | node_type "rpc-node" | build_id | run | operation | successor none (a node-admin's command)
        (NodePrimaryUpdateRequest::UpdatePrimary { node_type: "rpc-node".into(), build_id: "b".into(), run: 2, operation: "o".into(), successor: None }, "01 08 7270632d6e6f6465 0162 02 016f 00".to_string()),
        // variant 1 | ... | successor some: the birth the mesh-primary's pre-check named (Q129)
        (NodePrimaryUpdateRequest::UpdatePrimary { node_type: "rpc-node".into(), build_id: "b".into(), run: 2, operation: "o".into(), successor: Some(successor()) }, format!("01 08 7270632d6e6f6465 0162 02 016f 01 {SUCCESSOR}")),
    ];
    for (q, hex) in fixtures {
        assert_eq!(NodePrimaryUpdate::encode_request(&q).unwrap(), bytes(&hex), "{q:?}");
        assert_eq!(NodePrimaryUpdate::decode_request(&bytes(&hex)).unwrap(), q);
    }
    assert_eq!(NodePrimaryUpdate::REQUEST_VARIANTS, 2);
    assert_eq!(NodePrimaryUpdate::decode_request(&[2]), Err(DecodeFailure::UnknownVariant));
}

/// CONTRACT: the family's thirteen replies. `PrimaryUpdated` (node.primary.updated) first, then the
/// receiver's refusals (a conflicting payload, the initial fabric not ready, not the primary the call
/// needs, a take-over at a stale revision), the framework's outcomes, `Failed` (the commanded update
/// ended failed) and `AlreadyApplied` (a take-over applied already to the successor named). There is no
/// wrong-birth refusal (R-UC1a, R-RX5).
#[test]
fn replies_match_the_frozen_thirteen_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use NodePrimaryUpdateReply::*;
    let fixtures = [
        // revisions (len 1) | "fabric" | primary | successor | revision 8
        (PrimaryUpdated { revisions: vec![fabric_entry()] }, format!("00 01 06 666162726963 {PRIMARY} {SUCCESSOR} 08")),
        (RejectedConflictingPayload { operation: "o".into() }, "01 01 6f".to_string()),
        (RejectedFabricNotReady { reason: "r".into() }, "02 01 72".to_string()),
        (RejectedNotPrimary { reason: "a".into() }, "03 01 61".to_string()),
        // node_type "fabric" | expected | current
        (RejectedStaleRevision { node_type: "fabric".into(), expected: 3, current: 4 }, "04 06 666162726963 03 04".to_string()),
        (NotReady { reason: "n".into() }, "05 01 6e".to_string()),
        (Malformed { kind: MalformedKind::Corrupt }, "06 02".to_string()),
        (PeerUnresolved { reason: "p".into() }, "07 01 70".to_string()),
        (Busy { reason: "b".into() }, "08 01 62".to_string()),
        (ConnectionsDeleting { reason: "d".into() }, "09 01 64".to_string()),
        (Unauthorized { reason: "u".into() }, "0a 01 75".to_string()),
        // step "s" | reason "r" | code FailedPrecondition 9
        (Failed { step: "s".into(), reason: "r".into(), code: rdm_core::outcome::Code::FailedPrecondition }, "0b 0173 0172 09".to_string()),
        (AlreadyApplied { revisions: vec![fabric_entry()] }, format!("0c 01 06 666162726963 {PRIMARY} {SUCCESSOR} 08")),
    ];
    for (r, hex) in fixtures {
        variants.insert(NodePrimaryUpdate::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<NodePrimaryUpdate>(&r);
        assert_eq!(NodePrimaryUpdate::encode_reply(&r).unwrap(), bytes(&hex), "{r:?}");
        assert_eq!(NodePrimaryUpdate::decode_reply(&bytes(&hex)).unwrap(), r);
    }
    assert_eq!(NodePrimaryUpdate::REPLY_VARIANTS, 13);
    assert_eq!(NodePrimaryUpdate::decode_reply(&[13]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), NodePrimaryUpdate::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<NodePrimaryUpdate>();
}

#[test]
fn the_family_is_op_0x21_unary_and_not_forwardable_and_every_refusal_is_classified() {
    use NodePrimaryUpdateReply::*;
    use rdm_core::outcome::Code;
    assert_eq!(NodePrimaryUpdate::OP, 0x21);
    assert_eq!(NodePrimaryUpdate::NAME, "node-primary-update");
    assert!(!NodePrimaryUpdate::FORWARDABLE);
    assert_eq!(NodePrimaryUpdate::classify_reply(&PrimaryUpdated { revisions: vec![fabric_entry()] }), ReplyKind::Success);
    for (r, code) in [
        (RejectedConflictingPayload { operation: "o".into() }, Code::Aborted),
        (RejectedFabricNotReady { reason: "r".into() }, Code::FailedPrecondition),
        (RejectedNotPrimary { reason: "a".into() }, Code::FailedPrecondition),
        (RejectedStaleRevision { node_type: "fabric".into(), expected: 1, current: 2 }, Code::Aborted),
        (Failed { step: "s".into(), reason: "r".into(), code: Code::FailedPrecondition }, Code::FailedPrecondition),
        (AlreadyApplied { revisions: vec![fabric_entry()] }, Code::AlreadyExists),
    ] {
        assert_eq!(NodePrimaryUpdate::classify_reply(&r).code(), code, "{r:?}");
    }
}

#[test]
fn the_largest_request_fits_the_frame_ceiling() {
    let long = "x".repeat(128);
    let b = || Birth { node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&"z".repeat(12)).unwrap() };
    let primaries = vec![(long.clone(), b(), b(), u64::MAX), (long.clone(), b(), b(), u64::MAX)];
    let q = NodePrimaryUpdateRequest::PrimaryUpdate { primaries: primaries.clone(), build_id: long.clone(), run: u32::MAX, operation: long.clone() };
    assert!(NodePrimaryUpdate::encode_request(&q).unwrap().len() <= NodePrimaryUpdate::MAX_REQUEST_FRAME_BYTES);
    let r = NodePrimaryUpdateReply::PrimaryUpdated { revisions: primaries };
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES);
    // A failed step whose step and reason are each longer than the bound is cut to its budgets, never reset.
    let huge = "x".repeat(4 * NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES);
    let r = NodePrimaryUpdateReply::failed(&huge, &huge, rdm_core::outcome::Code::FailedPrecondition);
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES, "{r:?}");
    // Each refusal's reason is cut to the reply's bound, never reset.
    let r = NodePrimaryUpdateReply::not_primary(&huge);
    assert!(NodePrimaryUpdate::encode_reply(&r).unwrap().len() <= NodePrimaryUpdate::MAX_REPLY_FRAME_BYTES, "{r:?}");
}

#[test]
fn every_reply_variant_of_node_primary_update_classifies_to_one_code() {
    replies_match_the_frozen_thirteen_variant_wire_schema();
}

#[test]
fn failed_primary_update_retains_the_typed_build_code_in_its_appended_position() {
    use rdm_core::outcome::Code;
    for (code, n) in [(Code::Unknown, 2), (Code::DeadlineExceeded, 4), (Code::Unavailable, 14)] {
        let frame = NodePrimaryUpdateReply::failed("s", "r", code);
        let golden = vec![11, 1, b's', 1, b'r', n];
        assert_eq!(NodePrimaryUpdate::encode_reply(&frame).unwrap(), golden);
        assert_eq!(NodePrimaryUpdate::decode_reply(&golden).unwrap(), frame);
        assert_eq!(NodePrimaryUpdate::classify_reply(&frame).code(), code);
    }
    assert_eq!(NodePrimaryUpdate::decode_reply(&[11, 1, b's', 1, b'r']), Err(rdm_core::protocol::DecodeFailure::Corrupt));
}
