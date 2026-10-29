//! Fixed bytes for the `0x20` Build family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::build::{Build, BuildChange, Step, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepReceipt, StepResult};
use rdm_core::context::CallContext;
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::streaming::{FrameKind, StreamingProtocol};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn requests_match_the_frozen_four_variant_wire_schema() {
    let fixtures = [
        // positions 0, 1, 2 and 4 are reserved (the count forms): `a_request_at_a_reserved_count_position_is_an_unknown_variant`; position 6 (the removed restart): `a_request_at_the_removed_restart_position_is_an_unknown_variant`
        (BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::RemoveNode { node: "m.rpc.1".parse().unwrap() }) }, "00 00 03 07 6d2e7270632e31"),
        (
            BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Recover { node: "mesh1.rpc.1".parse().unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap(), incarnation: rdm_core::IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }) },
            "00 00 0a 0b 6d657368312e7270632e31 0c 303132333435363738396162 0c 79727a34396a333033796834",
        ),
        (
            BuildRequest::Create {
                submit: BuildSubmit::Change(BuildChange::Steps {
                    steps: vec![
                        Step::DeleteConnections { node: "m.rpc.1".parse().unwrap() },
                        Step::LeaveMesh { node: "m.rpc.1".parse().unwrap() },
                        Step::Update { node: "m.rpc.1".parse().unwrap(), name: "m.rpc.1.old".parse().unwrap() },
                        Step::Stop { node: "m.rpc.1.old".parse().unwrap() },
                        Step::Create { node: "m.rpc.1".parse().unwrap(), executable: None },
                        Step::Start { node: "m.rpc.1.old".parse().unwrap() },
                        Step::Delete { node: "m.rpc.1.old".parse().unwrap() },
                    ],
                }),
            },
            "00 00 0b 07 05 07 6d2e7270632e31 06 07 6d2e7270632e31 02 07 6d2e7270632e31 0b 6d2e7270632e312e6f6c64 01 0b 6d2e7270632e312e6f6c64 00 07 6d2e7270632e31 00 03 0b 6d2e7270632e312e6f6c64 04 0b 6d2e7270632e312e6f6c64",
        ),
        (BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::DeleteFabric) }, "00 00 0c"),
        // position 14 is reserved (the removed change): `a_request_at_the_removed_change_position_is_an_unknown_variant`
        // the complete topology (proposal executable-bindings-at-create.md rule 1): the change's last
        // variant; based_on, then each node's create spec positional (Q166, NodeSpec): its path.name,
        // storage's variant and its disposition, placement empty, the executable an Option of the binding,
        // its image written even when none, the tag labels, then the application's config map. The bytes
        // before each config are the entry's bytes before Q166 ((PathName, { storage, placement,
        // executable, tags })); the map is appended: `00` when empty, else its length and each key and value.
        (
            BuildRequest::Create {
                submit: BuildSubmit::Change(BuildChange::Reconcile {
                    based_on: rdm_core::BuildId::parse("0123456789ab").unwrap(),
                    nodes: vec![
                        rdm_core::meta::NodeSpec {
                            path: "m.admin.1".parse().unwrap(),
                            storage_persistence: rdm_core::meta::StorageMeta::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve },
                            placement: Default::default(),
                            executable: Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/x".into(), sha256: "s".into(), image: None }),
                            tags: ["t".to_string()].into(),
                            config: Default::default(),
                        },
                        rdm_core::meta::NodeSpec { path: "m.rpc.1".parse().unwrap(), storage_persistence: rdm_core::meta::StorageMeta::Ephemeral, placement: Default::default(), executable: None, tags: Default::default(), config: [("k".to_string(), "v".to_string())].into() },
                    ],
                    topology_storage_provider: None,
                }),
            },
            // the fabric's topology_storage_provider appended last (storage.md §2): `00`, none named.
            "00 00 0f 0c 303132333435363738396162 02 09 6d2e61646d696e2e31 01 00 01 0a 6e6f64655f61646d696e 02 2f78 01 73 00 01 01 74 00 07 6d2e7270632e31 00 00 00 01 01 6b 01 76 00",
        ),
        // a complete topology naming the provider `file`: `01`, then the name.
        (
            BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Reconcile { based_on: rdm_core::BuildId::parse("0123456789ab").unwrap(), nodes: vec![], topology_storage_provider: Some("file".into()) }) },
            "00 00 0f 0c 303132333435363738396162 00 01 04 66696c65",
        ),
        (BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: "b".into(), from_attempt: 3 } }, "00 01 01 62 03"),
        (BuildRequest::AttemptRun { build_id: "b".into(), attempt: 2, executor: "e".into(), context: CallContext::default(), intent: vec![vec![0xAA, 0xBB], vec![]], operations: vec![b"{}".to_vec()], hand_off_to: Some("n".into()), mesh: None }, "01 01 62 02 01 65 00 00 00 00 02 02 aabb 00 01 02 7b7d 01 01 6e 00"),
        // a drift attempt names its mesh (Q118): mesh "m"
        (BuildRequest::AttemptRun { build_id: "b".into(), attempt: 2, executor: "e".into(), context: CallContext::default(), intent: vec![], operations: vec![], hand_off_to: None, mesh: Some("m".into()) }, "01 01 62 02 01 65 00 00 00 00 00 00 00 01 01 6d"),
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
                operation: s("o"),
                step: s("s"),
                receipt: None,
            },
            "01 01 62 01 01 6f 01 73 00",
        ),
        // the inner stream's Step carries its receipt for the fabric-primary to write (Q118)
        (
            Step {
                build_id: b(),
                attempt: 1,
                operation: s("o"),
                step: s("s"),
                receipt: Some(vec![0x7b, 0x7d]),
            },
            "01 01 62 01 01 6f 01 73 01 02 7b7d",
        ),
        (
            Blocked {
                build_id: b(),
                attempt: 1,
                operation: s("o"),
                step: s("s"),
                reason: s("r"),
            },
            "02 01 62 01 01 6f 01 73 01 72",
        ),
        (
            Steps {
                build_id: b(),
                chunk_index: 0,
                chunk_count: 1,
                steps: vec![StepReceipt {
                    attempt: 1,
                    operation: s("o"),
                    step: s("s"),
                    result: StepResult::Complete,
                }],
            },
            "03 01 62 00 01 01 01 01 6f 01 73 00",
        ),
        (
            Steps {
                build_id: b(),
                chunk_index: 0,
                chunk_count: 1,
                steps: vec![StepReceipt {
                    attempt: 1,
                    operation: s("o"),
                    step: s("s"),
                    result: StepResult::Failed {
                        reason: s("r"),
                        code: rdm_core::outcome::Code::FailedPrecondition,
                    },
                }],
            },
            "03 01 62 00 01 01 01 01 6f 01 73 01 01 72 09",
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
                operation: s("o"),
                step: s("s"),
                reason: s("r"),
                code: rdm_core::outcome::Code::FailedPrecondition,
            },
            "05 01 62 02 01 6f 01 73 01 72 09",
        ),
        (
            HandedOff {
                build_id: b(),
                attempt: 2,
                to: s("t"),
            },
            "06 01 62 02 01 74",
        ),
        (
            Got {
                build_id: b(),
                phase: BuildPhase::Complete,
                attempt: 2,
                executor: Some(s("e")),
                reason: s("requested"),
                last_failure: None,
                steps: 3,
                chunks: 1,
            },
            "07 01 62 02 02 01 01 65 09 726571756573746564 00 03 01",
        ),
        (Deleted { build_id: b() }, "08 01 62"),
        (NotFabricPrimary { fabric_primary: Some(s("p")) }, "09 01 01 70"),
        (NotExecutor { named: s("n"), recipient: s("r") }, "0a 01 6e 01 72"),
        (StaleClaim { held_attempt: 3, held_executor: None, carried_attempt: 2 }, "0b 03 00 02"),
        (UnknownBuild { build_id: b() }, "0c 01 62"),
        (Rejected { reason: s("r"), detail: s("d") }, "0d 01 72 01 64"),
        (BuildInProgress { current_build_id: s("c"), mesh: Some(s("m")), held_by: Some(s("h")) }, "0e 01 63 01 01 6d 01 01 68"),
        (BuildInProgress { current_build_id: s("c"), mesh: None, held_by: None }, "0e 01 63 00 00"),
        (AttemptTaken { detail: s("d") }, "0f 01 64"),
        (SeatYielded { node: s("n"), by: s("b") }, "10 01 6e 01 62"),
        (CannotDelete { build_id: b(), reason: s("r") }, "11 01 62 01 72"),
        (PeerUnresolved { reason: s("p") }, "12 01 70"),
        (NotReady { reason: s("n") }, "13 01 6e"),
        (Busy { reason: s("b") }, "14 01 62"),
        (ConnectionsDeleting { reason: s("d") }, "15 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "16 02"),
        (Unauthorized { reason: s("u") }, "17 01 75"),
        (FabricPrimaryMoved { build_id: b(), fabric_primary: Some(s("p")) }, "18 01 62 01 01 70"),
        (FabricPrimaryMoved { build_id: b(), fabric_primary: None }, "18 01 62 00"),
        (NotClaimant { mesh: Some(s("m")), fabric_primary: Some(s("p")), mesh_primary: None }, "19 01 01 6d 01 01 70 00"),
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
            operation: "o".into(),
            step: "s".into(),
            receipt: None,
        },
        Blocked {
            build_id: "b".into(),
            attempt: 1,
            operation: "o".into(),
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
        Failed { build_id: "b".into(), attempt: 1, operation: "o".into(), step: "s".into(), reason: "r".into() , code: rdm_core::outcome::Code::FailedPrecondition },
        HandedOff { build_id: "b".into(), attempt: 1, to: "t".into() },
        FabricPrimaryMoved { build_id: "b".into(), fabric_primary: Some("p".into()) },
        Got { build_id: "b".into(), phase: BuildPhase::Pending, attempt: 0, executor: None, reason: "requested".into(), last_failure: None, steps: 0, chunks: 0 },
        Deleted { build_id: "b".into() },
        End { builds: 0 },
    ] {
        assert_eq!(Build::frame_kind(&terminal), FrameKind::Terminal, "{terminal:?}");
    }
    for refusal in [
        NotFabricPrimary { fabric_primary: None },
        NotExecutor { named: "n".into(), recipient: "r".into() },
        StaleClaim { held_attempt: 1, held_executor: None, carried_attempt: 2 },
        UnknownBuild { build_id: "b".into() },
        Rejected { reason: "r".into(), detail: "d".into() },
        BuildInProgress { current_build_id: "c".into(), mesh: None, held_by: None },
        NotClaimant { mesh: Some("m".into()), fabric_primary: None, mesh_primary: None },
        AttemptTaken { detail: "d".into() },
        SeatYielded { node: "n".into(), by: "b".into() },
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
    let r = BuildReply::Failed { build_id: "cq2tyb03krac".into(), attempt: u32::MAX, operation: "o".repeat(256), step: "s".repeat(256), reason: "r".repeat(16 * 1024) , code: rdm_core::outcome::Code::FailedPrecondition };
    assert!(Build::encode_reply(&r).unwrap().len() <= Build::MAX_REPLY_FRAME_BYTES);
    let q = BuildRequest::AttemptRun {
        build_id: "cq2tyb03krac".into(),
        attempt: u32::MAX,
        executor: "mesh1.admin.1".into(),
        context: CallContext { caller_system: Some("rdm".into()), traceparent: Some("t".repeat(55)), tracestate: Some("s".repeat(512)), baggage: Some("b".repeat(8192)) },
        intent: (0..32).map(|_| vec![0u8; 4096 - 64]).collect(),
        operations: (0..64).map(|_| vec![0u8; 256]).collect(),
        hand_off_to: Some("mesh1.admin.1".into()),
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
        let terminal = BuildReply::Failed { build_id: "b".into(), attempt: 2, operation: "o".into(), step: "s".into(), reason: "r".into(), code };
        let mut golden = bytes("05 01 62 02 01 6f 01 73 01 72"); golden.push(suffix);
        assert_eq!(Build::encode_reply(&terminal).unwrap(), golden);
        assert_eq!(Build::decode_reply(&golden).unwrap(), terminal);
        assert_eq!(Build::classify_reply(&terminal).code(), code);
        let receipt = BuildReply::Steps { build_id: "b".into(), chunk_index: 0, chunk_count: 1, steps: vec![StepReceipt { attempt: 1, operation: "o".into(), step: "s".into(), result: StepResult::Failed { reason: "r".into(), code } }] };
        let mut golden = bytes("03 01 62 00 01 01 01 01 6f 01 73 01 01 72"); golden.push(suffix);
        assert_eq!(Build::encode_reply(&receipt).unwrap(), golden);
        assert_eq!(Build::decode_reply(&golden).unwrap(), receipt);
    }
    assert_eq!(Build::decode_reply(&bytes("05 01 62 02 01 6f 01 73 01 72")), Err(DecodeFailure::Corrupt));
    assert_eq!(Build::decode_reply(&bytes("05 01 62 02 01 6f 01 73 01 72 11")), Err(DecodeFailure::Corrupt));
}

/// CONTRACT (proposal executable-bindings-at-create.md §1: the removed change's
/// positional slots reserved, no format move; §4: the removed change refused as unknown, the Build wire's
/// reserved slots decode as before): a request a caller of the removed operation sends (change position
/// 14, its set after it) decodes as `UnknownVariant`, never a change; the change after it keeps its
/// position (`Reconcile` = 0f).
#[test]
fn a_request_at_the_removed_change_position_is_an_unknown_variant() {
    let removed = bytes("00 00 0e 01 63 01 62 01 0a 6e6f64655f61646d696e 01 0a 6e6f64655f61646d696e 02 2f78 01 73 00");
    assert_eq!(Build::decode_request(&removed), Err(DecodeFailure::UnknownVariant));
    assert_eq!(Build::decode_request(&bytes("00 00 0e")), Err(DecodeFailure::UnknownVariant), "the bare position too");
    let reconcile = bytes("00 00 0f 0c 303132333435363738396162 00 00");
    assert!(matches!(Build::decode_request(&reconcile), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Reconcile { .. }) })), "Reconcile keeps position 15");
}

/// CONTRACT (Q183, Luke 2026-10-26: node.restart is removed; callers run node.stop then node.start):
/// a request a caller of the removed restart sends (change position 6, its node after it) decodes as
/// `UnknownVariant`, never a change, and so does the bare position; the changes after it keep their
/// positions (`DeleteConnections` = 07, `Stop` = 08, `Start` = 09).
#[test]
fn a_request_at_the_removed_restart_position_is_an_unknown_variant() {
    assert_eq!(Build::decode_request(&bytes("00 00 06 0b 6d657368312e7270632e31")), Err(DecodeFailure::UnknownVariant), "a node.restart of mesh1.rpc.1");
    assert_eq!(Build::decode_request(&bytes("00 00 06")), Err(DecodeFailure::UnknownVariant), "the bare position too");
    assert!(matches!(Build::decode_request(&bytes("00 00 07 0b 6d657368312e7270632e31")), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::DeleteConnections { .. }) })), "DeleteConnections keeps position 7");
    assert!(matches!(Build::decode_request(&bytes("00 00 08 0b 6d657368312e7270632e31")), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Stop { .. }) })), "Stop keeps position 8");
    assert!(matches!(Build::decode_request(&bytes("00 00 09 0b 6d657368312e7270632e31")), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Start { .. }) })), "Start keeps position 9");
}

/// CONTRACT (proposal executable-bindings-at-create.md §3.10(a): "the count forms `ReconcileFabric`,
/// `ReconcileMesh`, `AddNode` and `CreateMesh` are reserved (positional, no format move)"): a request a
/// caller of a count form sends (change positions 0, 1, 2 and 4, with the counts after them, or the bare
/// position) decodes as `UnknownVariant`, never a change; every change between and after them keeps its
/// position (`RemoveNode` = 03, `RemoveMesh` = 05, `Reconcile` = 0f).
#[test]
fn a_request_at_a_reserved_count_position_is_an_unknown_variant() {
    for (what, sent) in [
        ("ReconcileFabric { fabric: f, meshes: [m 1 admin 1 rpc] }", "00 00 00 01 66 01 01 6d 01 01 00 00 00"),
        ("ReconcileMesh { m 1 admin 1 rpc }", "00 00 01 01 6d 01 01 00 00 00"),
        ("AddNode { m, RpcNode }", "00 00 02 01 6d 01"),
        ("CreateMesh { m 1 admin 1 rpc }", "00 00 04 01 6d 01 01 00 00 00"),
        ("the bare position 0", "00 00 00"),
        ("the bare position 1", "00 00 01"),
        ("the bare position 2", "00 00 02"),
        ("the bare position 4", "00 00 04"),
    ] {
        assert_eq!(Build::decode_request(&bytes(sent)), Err(DecodeFailure::UnknownVariant), "{what}");
    }
    assert!(matches!(Build::decode_request(&bytes("00 00 03 07 6d2e7270632e31")), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::RemoveNode { .. }) })), "RemoveNode keeps position 3");
    assert!(matches!(Build::decode_request(&bytes("00 00 05 01 6d")), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::RemoveMesh { .. }) })), "RemoveMesh keeps position 5");
    assert!(matches!(Build::decode_request(&bytes("00 00 0f 0c 303132333435363738396162 00 00")), Ok(BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Reconcile { .. }) })), "Reconcile keeps position 15");
}

/// CONTRACT (Q166; node-rpc-envelope.md wire block: `Reconcile { based_on, nodes: Vec<NodeSpec> }`,
/// `NodeSpec { path, storage_persistence, placement, executable, tags, config }`): a create spec with no config
/// writes exactly the bytes the entry wrote before Q166 (`(PathName, { storage, placement, executable,
/// tags })`, frozen below from the fixture of that shape) plus `00`, the empty map; a spec with a
/// config appends the map's length and each key and value. A request written before Q166 does not
/// decode against the spec: the positional codec has no trailing default, so a caller and the admin
/// it calls carry the same spec.
#[test]
fn a_create_spec_without_config_writes_the_old_entry_bytes_and_a_config_appends_its_map() {
    let before_q166 = [
        ("m.admin.1", "09 6d2e61646d696e2e31 01 00 01 0a 6e6f64655f61646d696e 02 2f78 01 73 00 01 01 74"),
        ("m.rpc.1", "07 6d2e7270632e31 00 00 00"),
    ];
    let admin = rdm_core::meta::NodeSpec {
        path: "m.admin.1".parse().unwrap(),
        storage_persistence: rdm_core::meta::StorageMeta::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve },
        placement: Default::default(),
        executable: Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/x".into(), sha256: "s".into(), image: None }),
        tags: ["t".to_string()].into(),
        config: Default::default(),
    };
    let rpc = rdm_core::meta::NodeSpec::default_for("m.rpc.1".parse().unwrap());
    for ((path, old), spec) in before_q166.iter().zip([&admin, &rpc]) {
        let mut expected = bytes(old);
        expected.push(0x00);
        assert_eq!(postcard::to_allocvec(spec).unwrap(), expected, "{path}: the entry's bytes before Q166, then the empty map");
    }
    let configured = rdm_core::meta::NodeSpec { config: [("k".to_string(), "v".to_string())].into(), ..rpc.clone() };
    assert_eq!(postcard::to_allocvec(&configured).unwrap(), bytes("07 6d2e7270632e31 00 00 00 01 01 6b 01 76"));
    let old_request = bytes("00 00 0f 0c 303132333435363738396162 02 09 6d2e61646d696e2e31 01 00 01 0a 6e6f64655f61646d696e 02 2f78 01 73 00 01 01 74 07 6d2e7270632e31 00 00 00");
    assert!(Build::decode_request(&old_request).is_err(), "a request written before Q166 does not decode against the create spec");
}
