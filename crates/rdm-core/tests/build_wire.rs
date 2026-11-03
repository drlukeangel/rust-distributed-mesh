//! Fixed bytes for the `0x20` Build family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepReceipt, StepResult};
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
fn requests_match_the_frozen_four_variant_wire_schema() {
    let fixtures = [
        // positions 0, 1 and 2 (Create, Delete, DeleteFabric): `a_change_is_a_create_a_delete_by_node_id_or_a_fabric_delete`
        (BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: "b".into(), from_attempt: 3 } }, "00 01 01 62 03"),
        (BuildRequest::AttemptRun { build_id: "b".into(), attempt: 2, executor: executor_birth(), context: CallContext::default(), operations: vec![b"{}".to_vec()], hand_off_to: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()), mesh: None }, "01 01 62 02 0c 303132333435363738396162 0c 303132333435363738396163 00 00 00 00 01 02 7b7d 01 0c 303132333435363738396162 00"),
        // a drift attempt names its mesh (Q118): mesh "m"
        (BuildRequest::AttemptRun { build_id: "b".into(), attempt: 2, executor: executor_birth(), context: CallContext::default(), operations: vec![], hand_off_to: None, mesh: Some("m".into()) }, "01 01 62 02 0c 303132333435363738396162 0c 303132333435363738396163 00 00 00 00 00 00 01 01 6d"),
        (BuildRequest::Get { build_id: "b".into() }, "02 01 62"),
        (BuildRequest::Delete { build_id: "b".into() }, "03 01 62"),
        // builds.get (Q167 R-RD5), appended: no pair, then one pair (phase, [failed]).
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
fn replies_match_the_frozen_thirty_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use BuildReply::*;
    use rdm_core::build::Build;
    let b = || "b".to_string();
    let s = |v: &str| v.to_string();
    let fixtures = [
        (
            Started {
                build_id: b(),
                attempt: 1,
                disposition: Disposition::AlreadyApplied,
            },
            "00 01 62 01 03",
        ),
        (
            Started {
                build_id: b(),
                attempt: 1,
                disposition: Disposition::Created,
            },
            "00 01 62 01 00",
        ),
        (
            Started {
                build_id: b(),
                attempt: 1,
                disposition: Disposition::Reattached,
            },
            "00 01 62 01 05",
        ),
        (
            Step {
                build_id: b(),
                attempt: 1,
                operation: rdm_core::OperationId::unscoped("o"),
                step: s("s"),
                receipt: None,
            },
            "01 01 62 01 01 6f 00 00 00 01 73 00",
        ),
        // the inner stream's Step carries its receipt for the fabric-primary to write (Q118)
        (
            Step {
                build_id: b(),
                attempt: 1,
                operation: rdm_core::OperationId::unscoped("o"),
                step: s("s"),
                receipt: Some(vec![0x7b, 0x7d]),
            },
            "01 01 62 01 01 6f 00 00 00 01 73 01 02 7b7d",
        ),
        (
            Blocked {
                build_id: b(),
                attempt: 1,
                operation: rdm_core::OperationId::unscoped("o"),
                step: s("s"),
                reason: s("r"),
            },
            "02 01 62 01 01 6f 00 00 00 01 73 01 72",
        ),
        (
            Steps {
                build_id: b(),
                chunk_index: 0,
                chunk_count: 1,
                steps: vec![StepReceipt {
                    attempt: 1,
                    operation: rdm_core::OperationId::unscoped("o"),
                    step: s("s"),
                    result: StepResult::Complete,
                }],
            },
            "03 01 62 00 01 01 01 01 6f 00 00 00 01 73 00",
        ),
        (
            Steps {
                build_id: b(),
                chunk_index: 0,
                chunk_count: 1,
                steps: vec![StepReceipt {
                    attempt: 1,
                    operation: rdm_core::OperationId::unscoped("o"),
                    step: s("s"),
                    result: StepResult::Failed {
                        reason: s("r"),
                        code: rdm_core::outcome::Code::FailedPrecondition,
                    },
                }],
            },
            "03 01 62 00 01 01 01 01 6f 00 00 00 01 73 01 01 72 09",
        ),
        (
            Complete {
                build_id: b(),
                attempt: 2,
            },
            "04 01 62 02",
        ),
        (
            Failed {
                build_id: b(),
                attempt: 2,
                operation: rdm_core::OperationId::unscoped("o"),
                step: s("s"),
                reason: s("r"),
                code: rdm_core::outcome::Code::FailedPrecondition,
            },
            "05 01 62 02 01 6f 00 00 00 01 73 01 72 09",
        ),
        (
            HandedOff {
                build_id: b(),
                attempt: 2,
                to: rdm_core::NodeId::parse("0123456789ab").unwrap(),
            },
            "06 01 62 02 0c 303132333435363738396162",
        ),
        (
            Got {
                build_id: b(),
                phase: BuildPhase::Complete,
                attempt: 2,
                executor: Some(executor_birth()),
                reason: s("requested"),
                last_failure: None,
                steps: 3,
                chunks: 1,
            },
            "07 01 62 02 02 01 0c 303132333435363738396162 0c 303132333435363738396163 09 726571756573746564 00 03 01",
        ),
        (Deleted { build_id: b() }, "08 01 62"),
        (NotFabricPrimary { fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) }, "09 01 0c 303132333435363738396162"),
        (NotExecutor { named: executor_birth(), recipient: executor_birth() }, "0a 0c 303132333435363738396162 0c 303132333435363738396163 0c 303132333435363738396162 0c 303132333435363738396163"),
        (StaleClaim { held_attempt: 3, held_executor: None, carried_attempt: 2 }, "0b 03 00 02"),
        (UnknownBuild { build_id: b() }, "0c 01 62"),
        (Rejected { reason: s("r"), detail: s("d") }, "0d 01 72 01 64"),
        (BuildInProgress { current_build_id: s("c"), mesh: Some(s("m")), held_by: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) }, "0e 01 63 01 01 6d 01 0c 303132333435363738396162"),
        (BuildInProgress { current_build_id: s("c"), mesh: None, held_by: None }, "0e 01 63 00 00"),
        (AttemptTaken { detail: s("d") }, "0f 01 64"),
        (RoleYielded { node: executor_birth().node_id, by: s("b") }, "10 0c 303132333435363738396162 01 62"),
        (CannotDelete { build_id: b(), reason: s("r") }, "11 01 62 01 72"),
        (PeerUnresolved { reason: s("p") }, "12 01 70"),
        (NotReady { reason: s("n") }, "13 01 6e"),
        (Busy { reason: s("b") }, "14 01 62"),
        (ConnectionsDeleting { reason: s("d") }, "15 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "16 02"),
        (Unauthorized { reason: s("u") }, "17 01 75"),
        (FabricPrimaryMoved { build_id: b(), fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) }, "18 01 62 01 0c 303132333435363738396162"),
        (FabricPrimaryMoved { build_id: b(), fabric_primary: None }, "18 01 62 00"),
        (NotClaimant { mesh: Some(s("m")), fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()), mesh_primary: None }, "19 01 01 6d 01 0c 303132333435363738396162 00"),
        // build.get's frames (Q167 R-RD5), appended after NotClaimant.
        (Topology { build_id: b(), chunk_index: 0, chunk_count: 1, nodes: vec![] }, "1a 01 62 00 01 00"),
        (BuildReply::Build { build_id: b(), phase: BuildPhase::Failed, attempt: 2, executor: None, reason: s("r"), last_failure: Some(s("x")), current: true, steps: 1, chunks: 1 }, "1b 01 62 03 02 00 01 72 01 01 78 01 01 01"),
        (End { builds: 3 }, "1c 03"),
        (RejectedFilter { reason: s("r") }, "1d 01 72"),
    ];
    for (r, hex) in fixtures {
        variants.insert(Build::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Build>(&r);
        assert_eq!(Build::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Build::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(Build::REPLY_VARIANTS, 30);
    assert_eq!(Build::decode_reply(&[30]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), Build::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<Build>();
}

#[test]
fn every_frame_is_classified_for_stream_order_and_the_family_is_not_forwardable() {
    use BuildReply::*;
    use rdm_core::build::Build;
    assert!(!Build::FORWARDABLE);
    assert_eq!(Build::OP, 0x20);
    assert_eq!(Build::frame_kind(&Started { build_id: "b".into(), attempt: 1, disposition: Disposition::Created }), FrameKind::Started);
    for data in [
        Step {
            build_id: "b".into(),
            attempt: 1,
            operation: rdm_core::OperationId::unscoped("o"),
            step: "s".into(),
            receipt: None,
        },
        Blocked {
            build_id: "b".into(),
            attempt: 1,
            operation: rdm_core::OperationId::unscoped("o"),
            step: "s".into(),
            reason: "r".into(),
        },
        Steps {
            build_id: "b".into(),
            chunk_index: 0,
            chunk_count: 1,
            steps: vec![],
        },
        Topology { build_id: "b".into(), chunk_index: 0, chunk_count: 1, nodes: vec![] },
        // build.get's Build header closes one Build of several: data, never the read's terminal.
        BuildReply::Build { build_id: "b".into(), phase: BuildPhase::Complete, attempt: 1, executor: None, reason: "requested".into(), last_failure: None, current: true, steps: 0, chunks: 1 },
    ] {
        assert_eq!(Build::frame_kind(&data), FrameKind::Data, "{data:?}");
    }
    for terminal in [
        Complete { build_id: "b".into(), attempt: 1 },
        Failed { build_id: "b".into(), attempt: 1, operation: rdm_core::OperationId::unscoped("o"), step: "s".into(), reason: "r".into() , code: rdm_core::outcome::Code::FailedPrecondition },
        HandedOff { build_id: "b".into(), attempt: 1, to: rdm_core::NodeId::parse("0123456789ab").unwrap() },
        FabricPrimaryMoved { build_id: "b".into(), fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) },
        Got { build_id: "b".into(), phase: BuildPhase::Pending, attempt: 0, executor: None, reason: "requested".into(), last_failure: None, steps: 0, chunks: 0 },
        Deleted { build_id: "b".into() },
        End { builds: 0 },
    ] {
        assert_eq!(Build::frame_kind(&terminal), FrameKind::Terminal, "{terminal:?}");
    }
    for refusal in [
        NotFabricPrimary { fabric_primary: None },
        NotExecutor { named: executor_birth(), recipient: executor_birth() },
        StaleClaim { held_attempt: 1, held_executor: None, carried_attempt: 2 },
        UnknownBuild { build_id: "b".into() },
        Rejected { reason: "r".into(), detail: "d".into() },
        BuildInProgress { current_build_id: "c".into(), mesh: None, held_by: None },
        NotClaimant { mesh: Some("m".into()), fabric_primary: None, mesh_primary: None },
        AttemptTaken { detail: "d".into() },
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
    let r = BuildReply::Failed { build_id: "cq2tyb03krac".into(), attempt: u32::MAX, operation: rdm_core::OperationId::unscoped("o".repeat(256)), step: "s".repeat(256), reason: "r".repeat(16 * 1024) , code: rdm_core::outcome::Code::FailedPrecondition };
    assert!(Build::encode_reply(&r).unwrap().len() <= Build::MAX_REPLY_FRAME_BYTES);
    let q = BuildRequest::AttemptRun {
        build_id: "cq2tyb03krac".into(),
        attempt: u32::MAX,
        executor: executor_birth(),
        context: CallContext { caller_system: Some("rdm".into()), traceparent: Some("t".repeat(55)), tracestate: Some("s".repeat(512)), baggage: Some("b".repeat(8192)) },
        operations: (0..64).map(|_| vec![0u8; 256]).collect(),
        hand_off_to: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()),
        mesh: Some("mesh1".into()),
    };
    assert!(Build::encode_request(&q).unwrap().len() <= Build::MAX_REQUEST_FRAME_BYTES);
}

#[test]
fn every_reply_variant_of_build_classifies_to_one_code() {
    replies_match_the_frozen_thirty_variant_wire_schema();
}

/// The failed-state field is appended in place on both receipt and terminal, with no old decoder.
#[test]
fn failure_codes_are_appended_and_retained_by_the_build_wire() {
    use rdm_core::outcome::Code;
    for (code, suffix) in [(Code::Unknown, 2), (Code::DeadlineExceeded, 4), (Code::Unavailable, 14)] {
        let terminal = BuildReply::Failed { build_id: "b".into(), attempt: 2, operation: rdm_core::OperationId::unscoped("o"), step: "s".into(), reason: "r".into(), code };
        let mut golden = bytes("05 01 62 02 01 6f 00 00 00 01 73 01 72"); golden.push(suffix);
        assert_eq!(Build::encode_reply(&terminal).unwrap(), golden);
        assert_eq!(Build::decode_reply(&golden).unwrap(), terminal);
        assert_eq!(Build::classify_reply(&terminal).code(), code);
        let receipt = BuildReply::Steps { build_id: "b".into(), chunk_index: 0, chunk_count: 1, steps: vec![StepReceipt { attempt: 1, operation: rdm_core::OperationId::unscoped("o"), step: "s".into(), result: StepResult::Failed { reason: "r".into(), code } }] };
        let mut golden = bytes("03 01 62 00 01 01 01 01 6f 00 00 00 01 73 01 01 72"); golden.push(suffix);
        assert_eq!(Build::encode_reply(&receipt).unwrap(), golden);
        assert_eq!(Build::decode_reply(&golden).unwrap(), receipt);
    }
    assert_eq!(Build::decode_reply(&bytes("05 01 62 02 01 6f 00 00 00 01 73 01 72")), Err(DecodeFailure::Corrupt));
    assert_eq!(Build::decode_reply(&bytes("05 01 62 02 01 6f 01 73 01 72 11")), Err(DecodeFailure::Corrupt));
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
