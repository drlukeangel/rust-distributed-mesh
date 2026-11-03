//! Fixed bytes for the `0x20` Build family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepResult};
use rdm_core::context::CallContext;
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::streaming::{FrameKind, StreamingProtocol};

fn executor_birth() -> rdm_core::framing::Birth {
    rdm_core::framing::Birth {
        node_id: rdm_core::NodeId::parse("0123456789ab").unwrap(),
        incarnation: rdm_core::IncarnationId::parse("0123456789ac").unwrap(),
    }
}

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn requests_match_the_frozen_five_variant_wire_schema() {
    let fixtures = [
        // positions 0, 1 and 2 (Create, Delete, DeleteFabric): `a_change_is_a_create_a_delete_by_node_id_or_a_fabric_delete`
        (BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: "b".into() } }, "00 01 01 62"),
        // build.attempt.run names the Build and the mesh, never a copy of either (Q292)
        (BuildRequest::AttemptRun { build_id: "b".into(), mesh: "m".into(), context: CallContext::default() }, "01 01 62 01 6d 00 00 00 00"),
        (BuildRequest::Get { build_id: "b".into() }, "02 01 62"),
        (BuildRequest::Delete { build_id: "b".into() }, "03 01 62"),
        // builds.get (Q167 R-RD5): no pair, then one pair (phase, [failed]).
        (BuildRequest::GetBuilds { filter: rdm_core::filter::Filter::new() }, "04 00"),
        (BuildRequest::GetBuilds { filter: rdm_core::filter::Filter::new().eq("phase", ["failed"]) }, "04 01 05 7068617365 01 06 6661696c6564"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(Build::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Build::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Build::REQUEST_VARIANTS, 5);
    assert_eq!(Build::decode_request(&[5]), Err(DecodeFailure::UnknownVariant));
    // a change past DeleteFabric (position 2) does not decode
    assert_eq!(Build::decode_request(&bytes("00 00 03")), Err(DecodeFailure::Corrupt));
}

#[test]
fn replies_match_the_frozen_twenty_seven_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use BuildReply::*;
    use rdm_core::build::Build;
    let b = || "b".to_string();
    let s = |v: &str| v.to_string();
    let node = || rdm_core::NodeId::parse("0123456789ab").unwrap();
    let fixtures = [
        (Started { build_id: b(), disposition: Disposition::AlreadyApplied }, "00 01 62 02"),
        (Started { build_id: b(), disposition: Disposition::Created }, "00 01 62 00"),
        (Started { build_id: b(), disposition: Disposition::Reattached }, "00 01 62 04"),
        (Step { build_id: b(), mesh: s("m"), operation: s("o"), step: s("s"), result: StepResult::Complete }, "01 01 62 01 6d 01 6f 01 73 00"),
        (Step { build_id: b(), mesh: s("m"), operation: s("o"), step: s("s"), result: StepResult::Failed { reason: s("r"), code: rdm_core::outcome::Code::FailedPrecondition } }, "01 01 62 01 6d 01 6f 01 73 01 01 72 09"),
        (Pending { build_id: b(), mesh: s("m"), operation: s("o"), step: s("s"), reason: s("r") }, "02 01 62 01 6d 01 6f 01 73 01 72"),
        (Complete { build_id: b() }, "03 01 62"),
        // the Build could not start (Q346): build_id | reason | code
        (Failed { build_id: b(), reason: s("r"), code: rdm_core::outcome::Code::FailedPrecondition }, "04 01 62 01 72 09"),
        (Got { build_id: b(), phase: BuildPhase::Complete, reason: s("requested"), last_failure: None, events: 3, chunks: 1 }, "05 01 62 02 09 726571756573746564 00 03 01"),
        (Deleted { build_id: b() }, "06 01 62"),
        (NotFabricPrimary { fabric_primary: Some(node()) }, "07 01 0c 303132333435363738396162"),
        (NotExecutor { named: executor_birth(), recipient: executor_birth() }, "08 0c 303132333435363738396162 0c 303132333435363738396163 0c 303132333435363738396162 0c 303132333435363738396163"),
        (UnknownBuild { build_id: b() }, "09 01 62"),
        (Rejected { reason: s("r"), detail: s("d") }, "0a 01 72 01 64"),
        (BuildInProgress { current_build_id: s("c") }, "0b 01 63"),
        (RoleYielded { node: executor_birth().node_id, by: s("b") }, "0c 0c 303132333435363738396162 01 62"),
        (CannotDelete { build_id: b(), reason: s("r") }, "0d 01 62 01 72"),
        (PeerUnresolved { reason: s("p") }, "0e 01 70"),
        (NotReady { reason: s("n") }, "0f 01 6e"),
        (Busy { reason: s("b") }, "10 01 62"),
        (ConnectionsDeleting { reason: s("d") }, "11 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "12 02"),
        (Unauthorized { reason: s("u") }, "13 01 75"),
        (FabricPrimaryMoved { build_id: b(), fabric_primary: Some(node()) }, "14 01 62 01 0c 303132333435363738396162"),
        (FabricPrimaryMoved { build_id: b(), fabric_primary: None }, "14 01 62 00"),
        // build.get's frames (Q167 R-RD5)
        (Topology { build_id: b(), chunk_index: 0, chunk_count: 1, nodes: vec![] }, "15 01 62 00 01 00"),
        (Events { build_id: b(), chunk_index: 0, chunk_count: 1, events: vec![s("{}")] }, "16 01 62 00 01 01 02 7b7d"),
        (BuildReply::Build { build_id: b(), phase: BuildPhase::Failed, current: true, events: 1, chunks: 1, change: BuildChange::DeleteFabric, time: 5 }, "17 01 62 03 01 01 01 02 05"),
        (BuildReply::Build { build_id: b(), phase: BuildPhase::Incomplete, current: false, events: 1, chunks: 1, change: BuildChange::Delete { meshes: vec![s("m")], node_ids: vec![] }, time: 300 }, "17 01 62 04 00 01 01 01 01 01 6d 00 ac02"),
        (End { builds: 3 }, "18 03"),
        (RejectedFilter { reason: s("r") }, "19 01 72"),
        // appended (Q346): build_id | sections (len 1: mesh | operation | step | reason | code)
        (Incomplete { build_id: b(), sections: vec![rdm_core::build::UnfinishedSection { mesh: s("m"), operation: s("o"), step: s("s"), reason: s("r"), code: rdm_core::outcome::Code::FailedPrecondition }] }, "1a 01 62 01 01 6d 01 6f 01 73 01 72 09"),
    ];
    for (r, hex) in fixtures {
        variants.insert(Build::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Build>(&r);
        assert_eq!(Build::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Build::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(Build::REPLY_VARIANTS, 27);
    assert_eq!(Build::decode_reply(&[27]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), Build::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<Build>();
}

#[test]
fn every_frame_is_classified_for_stream_order_and_the_family_is_not_forwardable() {
    use BuildReply::*;
    use rdm_core::build::Build;
    assert!(!Build::FORWARDABLE);
    assert_eq!(Build::OP, 0x20);
    assert_eq!(Build::frame_kind(&Started { build_id: "b".into(), disposition: Disposition::Created }), FrameKind::Started);
    for data in [
        Step { build_id: "b".into(), mesh: "m".into(), operation: "o".into(), step: "s".into(), result: StepResult::Complete },
        Pending { build_id: "b".into(), mesh: "m".into(), operation: "o".into(), step: "s".into(), reason: "r".into() },
        Events { build_id: "b".into(), chunk_index: 0, chunk_count: 1, events: vec![] },
        Topology { build_id: "b".into(), chunk_index: 0, chunk_count: 1, nodes: vec![] },
        // build.get's Build header closes one Build of several: data, never the read's terminal.
        BuildReply::Build { build_id: "b".into(), phase: BuildPhase::Complete, current: true, events: 0, chunks: 1, change: BuildChange::DeleteFabric, time: 0 },
    ] {
        assert_eq!(Build::frame_kind(&data), FrameKind::Data, "{data:?}");
    }
    for terminal in [
        Complete { build_id: "b".into() },
        Failed { build_id: "b".into(), reason: "r".into(), code: rdm_core::outcome::Code::FailedPrecondition },
        Incomplete { build_id: "b".into(), sections: vec![] },
        FabricPrimaryMoved { build_id: "b".into(), fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) },
        Got { build_id: "b".into(), phase: BuildPhase::Pending, reason: "requested".into(), last_failure: None, events: 0, chunks: 0 },
        Deleted { build_id: "b".into() },
        End { builds: 0 },
    ] {
        assert_eq!(Build::frame_kind(&terminal), FrameKind::Terminal, "{terminal:?}");
    }
    for refusal in [
        NotFabricPrimary { fabric_primary: None },
        NotExecutor { named: executor_birth(), recipient: executor_birth() },
        UnknownBuild { build_id: "b".into() },
        Rejected { reason: "r".into(), detail: "d".into() },
        BuildInProgress { current_build_id: "c".into() },
        RoleYielded { node: executor_birth().node_id, by: "b".into() },
        CannotDelete { build_id: "b".into(), reason: "r".into() },
        RejectedFilter { reason: "build.get: the pair (zone, [eu]) names no property of a Build header row".into() },
    ] {
        assert!(matches!(Build::frame_kind(&refusal), FrameKind::Refusal(_)), "{refusal:?}");
        crate::outcome_codes::check::<Build>(&refusal);
    }
    assert_eq!(Build::frame_kind(&NotReady { reason: "n".into() }), FrameKind::Refusal(ReplyKind::NotReady));
}

#[test]
fn a_failed_step_reason_within_the_senders_bound_fits_the_reply_frame_ceiling() {
    let r = BuildReply::Failed { build_id: "cq2tyb03krac".into(), reason: "r".repeat(16 * 1024), code: rdm_core::outcome::Code::FailedPrecondition };
    assert!(Build::encode_reply(&r).unwrap().len() <= Build::MAX_REPLY_FRAME_BYTES);
    let q = BuildRequest::AttemptRun {
        build_id: "cq2tyb03krac".into(),
        mesh: "mesh1".into(),
        context: CallContext { caller_system: Some("rdm".into()), traceparent: Some("t".repeat(55)), tracestate: Some("s".repeat(512)), baggage: Some("b".repeat(8192)) },
    };
    assert!(Build::encode_request(&q).unwrap().len() <= Build::MAX_REQUEST_FRAME_BYTES);
}

#[test]
fn every_reply_variant_of_build_classifies_to_one_code() {
    replies_match_the_frozen_twenty_seven_variant_wire_schema();
}

/// The failed-state code is retained by the terminal and the step frame, with no old decoder.
#[test]
fn failure_codes_are_appended_and_retained_by_the_build_wire() {
    use rdm_core::outcome::Code;
    for (code, suffix) in [(Code::Unknown, 2), (Code::DeadlineExceeded, 4), (Code::Unavailable, 14)] {
        let terminal = BuildReply::Failed { build_id: "b".into(), reason: "r".into(), code };
        let mut golden = bytes("04 01 62 01 72");
        golden.push(suffix);
        assert_eq!(Build::encode_reply(&terminal).unwrap(), golden);
        assert_eq!(Build::decode_reply(&golden).unwrap(), terminal);
        assert_eq!(Build::classify_reply(&terminal).code(), code);
        let step = BuildReply::Step { build_id: "b".into(), mesh: "m".into(), operation: "o".into(), step: "s".into(), result: StepResult::Failed { reason: "r".into(), code } };
        let mut golden = bytes("01 01 62 01 6d 01 6f 01 73 01 01 72");
        golden.push(suffix);
        assert_eq!(Build::encode_reply(&step).unwrap(), golden);
        assert_eq!(Build::decode_reply(&golden).unwrap(), step);
    }
    // A terminal without its code (build_id | reason, no code) is refused, never read as one.
    assert_eq!(Build::decode_reply(&bytes("04 01 62 01 72")), Err(DecodeFailure::Corrupt));
}

/// A create spec carries its owning mesh and its type explicitly, followed by the complete
/// properties. The full rebirth cut refuses a request written with the former entry shape.
#[test]
fn a_create_spec_carries_explicit_ownership_and_the_complete_config_map() {
    let fixtures = [
        ("m.node-admin.1", "0c 6d 2e 6e 6f 64 65 2d 61 64 6d 69 6e 01 31 01 6d 0a 6e 6f 64 65 2d 61 64 6d 69 6e 01 00 01 0a 6e 6f 64 65 2d 61 64 6d 69 6e 02 2f 78 01 73 00 01 01 74 00"),
        ("m.node.1", "06 6d2e6e6f64650131 01 6d 04 6e6f6465 00 00 00 00"),
    ];
    let admin = rdm_core::meta::NodeSpec {
        path: "m.node-admin".into(), name: "1".into(), mesh: "m".into(), r#type: "node-admin".into(),
        storage_persistence: rdm_core::meta::StoragePersistence::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve },
        
        executable: Some(rdm_core::binding::Binding { launch_id: "node-admin".into(), executable: "/x".into(), sha256: "s".into(), image: None }),
        tags: ["t".to_string()].into(),
        config: Default::default(),
    };
    let node = rdm_core::meta::NodeSpec::default_for(rdm_core::PathName::new("m.node", "1").unwrap(), "m", "node");
    for ((path, expected), spec) in fixtures.iter().zip([&admin, &node]) {
        assert_eq!(postcard::to_allocvec(spec).unwrap(), bytes(expected), "{path}: explicit mesh, type and complete properties");
    }
    let configured = rdm_core::meta::NodeSpec { config: [("k".to_string(), "v".to_string())].into(), ..node.clone() };
    assert_eq!(postcard::to_allocvec(&configured).unwrap(), bytes("06 6d2e6e6f64650131 01 6d 04 6e6f6465 00 00 00 01 01 6b 01 76"));
    let old_request = bytes("00 00 03 0c 303132333435363738396162 02 09 6d2e61646d696e2e31 01 00 01 0a 6e6f64655f61646d696e 02 2f78 01 73 00 01 01 74 07 6d2e7270632e31 00 00 00");
    assert!(Build::decode_request(&old_request).is_err(), "a request written before Q166 does not decode against the create spec");
}

/// CONTRACT (node-rpc-envelope.md "Build schema"; build.md §1, Q342): a Build's change is one of
/// three batches, at positions 0, 1 and 2: `Create { meshes, nodes }`, each mesh by its Meshes schema
/// (its name) and each node by its Nodes schema, its `path` and `name` two strings and its `type` a
/// string, its storage and executable `None` where its type's apply; `Delete { meshes, node_ids }`,
/// each mesh by name and each node by its node.id, never by a path and name; and `DeleteFabric`.
#[test]
fn a_change_is_a_create_a_delete_by_node_id_or_a_fabric_delete() {
    use rdm_core::build::{Meshes, Nodes};
    let admin = Nodes {
        path: "m2.admin".into(),
        name: "1".into(),
        mesh: "m2".into(),
        r#type: "node-admin".into(),
        storage_persistence: None,
        executable: Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/x".into(), sha256: "s".into(), image: None }),
        tags: Default::default(),
        config: Default::default(),
    };
    let rpc = Nodes {
        path: "m2.rpc".into(),
        name: "1".into(),
        mesh: "m2".into(),
        r#type: "rpc-node".into(),
        storage_persistence: Some(rdm_core::meta::StoragePersistence::Ephemeral),
        executable: None,
        tags: ["t".to_string()].into(),
        config: [("k".to_string(), "v".to_string())].into(),
    };
    let fixtures = [
        (
            BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Create { meshes: vec![Meshes { name: "m2".into() }], nodes: vec![admin, rpc] }) },
            "00 00 00 01 02 6d32 02 \
             08 6d322e61646d696e 01 31 02 6d32 0a 6e6f64652d61646d696e 00 01 0a 6e6f64655f61646d696e 02 2f78 01 73 00 00 00 \
             06 6d322e727063 01 31 02 6d32 08 7270632d6e6f6465 01 00 00 01 01 74 01 01 6b 01 76",
        ),
        (
            BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Delete { meshes: vec!["m3".into()], node_ids: vec![rdm_core::NodeId::parse("0123456789ab").unwrap()] }) },
            "00 00 01 01 02 6d33 01 0c 303132333435363738396162",
        ),
        (BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::DeleteFabric) }, "00 00 02"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(Build::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Build::decode_request(&bytes(hex)).unwrap(), q);
    }
}
