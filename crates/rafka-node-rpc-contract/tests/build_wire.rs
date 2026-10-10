//! Fixed bytes for the `0x20` Build family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::NodeKind;
use rafka_node_rpc_contract::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepReceipt, StepResult};
use rafka_node_rpc_contract::context::CallContext;
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::{DecodeFailure, NodeProtocol};
use rafka_node_rpc_contract::streaming::{FrameKind, StreamingProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn requests_match_the_frozen_four_variant_wire_schema() {
    let fixtures = [
        (BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::AddNode { mesh: "m".into(), node_kind: NodeKind::RpcNode }) }, "00 00 02 01 6d 01"),
        (BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Restart { node: "mesh1.rpc.1".parse().unwrap() }) }, "00 00 06 0b 6d657368312e7270632e31"),
        (BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: "b".into(), from_attempt: 3 } }, "00 01 01 62 03"),
        (BuildRequest::AttemptRun { build_id: "b".into(), attempt: 2, executor: "e".into(), context: CallContext::default(), intent: vec![vec![0xAA, 0xBB], vec![]] }, "01 01 62 02 01 65 00 00 00 00 02 02 aabb 00"),
        (BuildRequest::Get { build_id: "b".into() }, "02 01 62"),
        (BuildRequest::Delete { build_id: "b".into() }, "03 01 62"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(Build::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Build::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Build::REQUEST_VARIANTS, 4);
    assert_eq!(Build::decode_request(&[4]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_twenty_four_variant_wire_schema() {
    use BuildReply::*;
    let b = || "b".to_string();
    let s = |v: &str| v.to_string();
    let fixtures = [
        (Started { build_id: b(), attempt: 1, disposition: Disposition::Created }, "00 01 62 01 00"),
        (Started { build_id: b(), attempt: 1, disposition: Disposition::Reattached }, "00 01 62 01 05"),
        (Step { build_id: b(), attempt: 1, operation: s("o"), step: s("s") }, "01 01 62 01 01 6f 01 73"),
        (Blocked { build_id: b(), attempt: 1, operation: s("o"), step: s("s"), reason: s("r") }, "02 01 62 01 01 6f 01 73 01 72"),
        (Steps { build_id: b(), chunk_index: 0, chunk_count: 1, steps: vec![StepReceipt { attempt: 1, operation: s("o"), step: s("s"), result: StepResult::Complete }] }, "03 01 62 00 01 01 01 01 6f 01 73 00"),
        (Steps { build_id: b(), chunk_index: 0, chunk_count: 1, steps: vec![StepReceipt { attempt: 1, operation: s("o"), step: s("s"), result: StepResult::Failed { reason: s("r") } }] }, "03 01 62 00 01 01 01 01 6f 01 73 01 01 72"),
        (Complete { build_id: b(), attempt: 2 }, "04 01 62 02"),
        (Failed { build_id: b(), attempt: 2, operation: s("o"), step: s("s"), reason: s("r") }, "05 01 62 02 01 6f 01 73 01 72"),
        (HandedOff { build_id: b(), attempt: 2, to: s("t") }, "06 01 62 02 01 74"),
        (Got { build_id: b(), phase: BuildPhase::Complete, attempt: 2, executor: Some(s("e")), reason: s("requested"), last_failure: None, steps: 3, chunks: 1 }, "07 01 62 02 02 01 01 65 09 726571756573746564 00 03 01"),
        (Deleted { build_id: b() }, "08 01 62"),
        (NotFabricPrimary { fabric_primary: Some(s("p")) }, "09 01 01 70"),
        (NotExecutor { named: s("n"), recipient: s("r") }, "0a 01 6e 01 72"),
        (StaleClaim { held_attempt: 3, held_executor: None, carried_attempt: 2 }, "0b 03 00 02"),
        (UnknownBuild { build_id: b() }, "0c 01 62"),
        (Rejected { reason: s("r"), detail: s("d") }, "0d 01 72 01 64"),
        (BuildInProgress { current_build_id: s("c") }, "0e 01 63"),
        (AttemptTaken { detail: s("d") }, "0f 01 64"),
        (Fenced { node: s("n"), by: s("b") }, "10 01 6e 01 62"),
        (CannotDelete { build_id: b(), reason: s("r") }, "11 01 62 01 72"),
        (PeerUnresolved { reason: s("p") }, "12 01 70"),
        (NotReady { reason: s("n") }, "13 01 6e"),
        (Busy { reason: s("b") }, "14 01 62"),
        (Draining { reason: s("d") }, "15 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "16 02"),
        (Unauthorized { reason: s("u") }, "17 01 75"),
    ];
    for (r, hex) in fixtures {
        assert_eq!(Build::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Build::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(Build::REPLY_VARIANTS, 24);
    assert_eq!(Build::decode_reply(&[24]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn every_frame_is_classified_for_stream_order_and_the_family_is_not_forwardable() {
    use BuildReply::*;
    assert!(!Build::FORWARDABLE);
    assert_eq!(Build::OP, 0x20);
    assert_eq!(Build::frame_kind(&Started { build_id: "b".into(), attempt: 1, disposition: Disposition::Created }), FrameKind::Started);
    for data in [
        Step { build_id: "b".into(), attempt: 1, operation: "o".into(), step: "s".into() },
        Blocked { build_id: "b".into(), attempt: 1, operation: "o".into(), step: "s".into(), reason: "r".into() },
        Steps { build_id: "b".into(), chunk_index: 0, chunk_count: 1, steps: vec![] },
    ] {
        assert_eq!(Build::frame_kind(&data), FrameKind::Data, "{data:?}");
    }
    for terminal in [
        Complete { build_id: "b".into(), attempt: 1 },
        Failed { build_id: "b".into(), attempt: 1, operation: "o".into(), step: "s".into(), reason: "r".into() },
        HandedOff { build_id: "b".into(), attempt: 1, to: "t".into() },
        Got { build_id: "b".into(), phase: BuildPhase::Pending, attempt: 0, executor: None, reason: "requested".into(), last_failure: None, steps: 0, chunks: 0 },
        Deleted { build_id: "b".into() },
    ] {
        assert_eq!(Build::frame_kind(&terminal), FrameKind::Terminal, "{terminal:?}");
    }
    for refusal in [
        NotFabricPrimary { fabric_primary: None },
        NotExecutor { named: "n".into(), recipient: "r".into() },
        StaleClaim { held_attempt: 1, held_executor: None, carried_attempt: 2 },
        UnknownBuild { build_id: "b".into() },
        Rejected { reason: "r".into(), detail: "d".into() },
        BuildInProgress { current_build_id: "c".into() },
        AttemptTaken { detail: "d".into() },
        Fenced { node: "n".into(), by: "b".into() },
        CannotDelete { build_id: "b".into(), reason: "r".into() },
    ] {
        assert_eq!(Build::frame_kind(&refusal), FrameKind::Refusal(ReplyKind::ProtocolRefusal), "{refusal:?}");
    }
    assert_eq!(Build::frame_kind(&NotReady { reason: "n".into() }), FrameKind::Refusal(ReplyKind::NotReady));
}

#[test]
fn a_failed_step_reason_within_the_senders_bound_fits_the_reply_frame_ceiling() {
    let r = BuildReply::Failed { build_id: "bld-0123456789abcdef01234567".into(), attempt: u32::MAX, operation: "o".repeat(256), step: "s".repeat(256), reason: "r".repeat(16 * 1024) };
    assert!(Build::encode_reply(&r).unwrap().len() <= Build::MAX_REPLY_FRAME_BYTES);
    let q = BuildRequest::AttemptRun {
        build_id: "bld-0123456789abcdef01234567".into(),
        attempt: u32::MAX,
        executor: "mesh1.admin.1".into(),
        context: CallContext { caller_system: Some("rdm".into()), traceparent: Some("t".repeat(55)), tracestate: Some("s".repeat(512)), baggage: Some("b".repeat(8192)) },
        intent: (0..32).map(|_| vec![0u8; 4096 - 64]).collect(),
    };
    assert!(Build::encode_request(&q).unwrap().len() <= Build::MAX_REQUEST_FRAME_BYTES);
}
