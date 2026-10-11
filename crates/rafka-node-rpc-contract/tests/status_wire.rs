//! Fixed bytes for the coordinated 2026-10-07 status reseal (#2923); the four drain/stop command
//! variants (6..=9) are appended after it, their bytes derived from the postcard rules by hand, as are
//! `node.connections.delete`'s request (21) and its two replies (31, 32), and the mesh.delete round
//! and manifest refusals (33, 34, 35).
//! These literals lock discriminants and positional fields independently of codec round trips.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::{ConnectionId, FabricId, IncarnationId, MeshId, NodeId, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_contract::status::{ConnectionScope, DrainReceipt, MAX_RECEIPT_MANIFEST_BYTES, FabricEvent, MeshDeleteRound, MeshState, NodeState, NotAuthority, RetryOn, Status, StatusReply, StatusRequest};

fn node() -> NodeId { NodeId::parse("0123456789ab").unwrap() }
fn mesh() -> MeshId { MeshId::parse("1123456789ab").unwrap() }
fn fabric() -> FabricId { FabricId::parse("2123456789ab").unwrap() }
fn birth() -> IncarnationId { IncarnationId("birth".into()) }
fn connection() -> ConnectionId { ConnectionId::parse("3123456789ab").unwrap() }

fn round(build_id: &str, attempt: u32) -> MeshDeleteRound { MeshDeleteRound { build_id: build_id.into(), attempt, operation: "o".into() } }

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| {
        u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
    }).collect()
}

#[test]
fn requests_match_the_frozen_twenty_two_variant_wire_schema() {
    use StatusRequest::*;
    let fixtures = [
        (DeclareNodeState { node_id: node(), incarnation: birth(), state: NodeState::Leaving }, "000c30313233343536373839616205626972746803"),
        (DeclareMeshState { mesh_id: mesh(), state: MeshState::Leaving }, "010c31313233343536373839616202"),
        (ApplyNodeState { node_id: node(), incarnation: birth(), state: NodeState::Draining }, "020c30313233343536373839616205626972746802"),
        (ProbeNodeState { node_id: node(), incarnation: birth() }, "030c303132333435363738396162056269727468"),
        (ApplyMeshState { mesh_id: mesh(), mesh_name: "mesh1".into(), state: MeshState::Pending }, "040c313132333435363738396162056d6573683100"),
        (ApplyFabricEvent { fabric_id: fabric(), event: FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() } }, "050c323132333435363738396162010d6d657368312e61646d696e2e31"),
        (DrainNode { node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() }, "060c3031323334353637383961620562697274680c626c645f31613262336334640116647261696e2d6e6f64653a6d657368312e7270632e31"),
        (NodeDrained { node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() }, "070c3031323334353637383961620562697274680c626c645f31613262336334640116647261696e2d6e6f64653a6d657368312e7270632e31"),
        (StopNode { node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() }, "080c3031323334353637383961620562697274680c626c645f3161326233633464011573746f702d6e6f64653a6d657368312e7270632e31"),
        (NodeLeft { node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() }, "090c3031323334353637383961620562697274680c626c645f3161326233633464011573746f702d6e6f64653a6d657368312e7270632e31"),
        (LeaveMesh { mesh_id: mesh(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "shutdown-mesh:1123456789ab".into() }, "0a0c3131323334353637383961620c626c645f3161326233633464011a73687574646f776e2d6d6573683a313132333435363738396162"),
        (
            MeshLeave {
                mesh_id: mesh(),
                build_id: "bld_1a2b3c4d".into(),
                attempt: 1,
                operation: "shutdown-mesh:1123456789ab".into(),
                final_node_id: node(),
                final_incarnation: birth(),
                final_runtime: RuntimeFact { deployment_id: "dep_mesh1_admin1".into(), provider: RuntimeProvider::Process, control_domain: "process:boot-a:pidns-a".into(), locator: RuntimeLocator::Process { pid: 4321, start: 123456 } },
                receipt_manifest: "bld_1a2b3c4d/1/shutdown-mesh:1123456789ab/other-members-exited".into(),
            },
            "0b0c3131323334353637383961620c626c645f3161326233633464011a73687574646f776e2d6d6573683a3131323334353637383961620c303132333435363738396162056269727468106465705f6d657368315f61646d696e31001670726f636573733a626f6f742d613a7069646e732d6100e121c0c4073e626c645f31613262336334642f312f73687574646f776e2d6d6573683a3131323334353637383961622f6f746865722d6d656d626572732d657869746564",
        ),
        (CommitState { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "commit-state:fabric1".into() }, "0c0c3231323334353637383961620c3031323334353637383961620562697274680c626c645f31613262336334640114636f6d6d69742d73746174653a66616272696331"),
        (StateCommitted { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "commit-state:fabric1".into() }, "0d0c3231323334353637383961620c3031323334353637383961620562697274680c626c645f31613262336334640114636f6d6d69742d73746174653a66616272696331"),
        (OpenTraffic { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "open-traffic:fabric1".into() }, "0e0c3231323334353637383961620c3031323334353637383961620562697274680c626c645f316132623363346401146f70656e2d747261666669633a66616272696331"),
        (TrafficOpened { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "open-traffic:fabric1".into() }, "0f0c3231323334353637383961620c3031323334353637383961620562697274680c626c645f316132623363346401146f70656e2d747261666669633a66616272696331"),
        (StartNode { node_id: node(), incarnation: birth(), build_id: "bld_1a2b3c4d".into(), attempt: 1, operation: "start-node:mesh1.rpc.1".into() }, "100c3031323334353637383961620562697274680c626c645f3161326233633464011673746172742d6e6f64653a6d657368312e7270632e31"),
        (UpdateNodeConfig { node_id: node(), incarnation: birth(), config: [("k".to_string(), "v".to_string())].into() }, "110c30313233343536373839616205626972746801016b0176"),
        (ApplyNodeConfig { node_id: node(), incarnation: birth(), config: [("k".to_string(), "v".to_string())].into() }, "120c30313233343536373839616205626972746801016b0176"),
        (ApplyNodeName { node_id: node(), incarnation: birth(), name: "mesh1.rpc.1".into() }, "130c3031323334353637383961620562697274680b6d657368312e7270632e31"),
        (UpdateNode { node_id: node(), incarnation: birth(), name: "mesh1.rpc.1".into() }, "140c3031323334353637383961620562697274680b6d657368312e7270632e31"),
        (DeleteConnections { node_id: node(), incarnation: birth(), scope: ConnectionScope::Connections(vec![connection()]) }, "150c30313233343536373839616205626972746801010c333132333435363738396162"),
    ];
    assert_eq!(fixtures.len() as u32, Status::REQUEST_VARIANTS);
    for (request, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(Status::encode_request(&request).unwrap(), expected, "{request:?}");
        assert_eq!(Status::decode_request(&expected).unwrap(), request);
    }
}

#[test]
fn replies_match_the_frozen_thirty_six_variant_wire_schema() {
    use StatusReply::*;
    let fixtures = [
        (Applied, "00"),
        (AlreadyApplied, "01"),
        (NodeDrainingApplied { in_flight: 300 }, "02ac02"),
        (Current { node_id: node(), incarnation: birth(), state: NodeState::ReadyForTraffic }, "030c30313233343536373839616205626972746801"),
        (RejectedStaleIncarnation { held: birth() }, "04056269727468"),
        (RejectedStaleMesh { held: mesh() }, "050c313132333435363738396162"),
        (RejectedStaleFabric { held: fabric() }, "060c323132333435363738396162"),
        (RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "peer-path".into() } }, "070109706565722d70617468"),
        (RejectedInvalidNodeTransition { current: NodeState::Leaving }, "0803"),
        (RejectedInvalidMeshTransition { current: MeshState::Dead }, "0903"),
        (PeerUnresolved { reason: "p".into() }, "0a0170"),
        (NotReady { reason: "n".into() }, "0b016e"),
        (Busy { reason: "b".into() }, "0c0162"),
        (Draining { reason: "d".into() }, "0d0164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0e02"),
        (Unauthorized { reason: "u".into() }, "0f0175"),
        (RejectedUnmatchedCompletion { field: "attempt".into(), expected: "1".into(), reported: "2".into() }, "1007617474656d707401310132"),
        (Stopped { receipt: DrainReceipt::Established { in_flight: 0 } }, "110000"),
        (Started, "12"),
        (StartFailed { step: "node.join".into(), reason: "refused".into() }, "13096e6f64652e6a6f696e0772656675736564"),
        (CurrentParked { node_id: node(), incarnation: birth() }, "140c303132333435363738396162056269727468"),
        (ConfigUpdated { node_id: node(), incarnation: birth() }, "150c303132333435363738396162056269727468"),
        (RejectedConfig { over: rafka_mesh_entity::ConfigOverCap { keys: 33, bytes: 10, max_keys: 32, max_bytes: 2048 } }, "16210a208010"),
        (ConfigRowRefused { node: "n".into(), reason: "r".into() }, "17016e0172"),
        (ConfigNotApplied { node: "n".into(), answered: "a".into(), reason: "r".into() }, "18016e01610172"),
        (Blocked { node_id: node(), incarnation: birth(), reason: "r".into(), retry_on: RetryOn::TargetReachable { node_id: node() } }, "190c3031323334353637383961620562697274680172010c303132333435363738396162"),
        (NodeUpdated { node_id: node(), incarnation: birth() }, "1a0c303132333435363738396162056269727468"),
        (RejectedMetaFieldIsRdmOwned { field: "name".into() }, "1b046e616d65"),
        (NameHeld { name: "n".into(), by: node() }, "1c016e0c303132333435363738396162"),
        (NodeRowRefused { node: "n".into(), reason: "r".into() }, "1d016e0172"),
        (NameNotApplied { node: "n".into(), answered: "a".into(), reason: "r".into() }, "1e016e01610172"),
        (ConnectionsDeleted { closed: vec![connection()], not_open: vec![], held: vec![] }, "1f010c3331323334353637383961620000"),
        (RejectedConnectionScope { reason: "r".into() }, "200172"),
        // held { build_id "b" | attempt 2 | operation "o" } | offered { "b" | 1 | "o" }
        (RejectedStaleRound { held: round("b", 2), offered: round("b", 1) }, "21016202016f016201016f"),
        // held { "b" | 1 | "o" } | offered { "c" | 1 | "o" }
        (RejectedConflictingRound { held: round("b", 1), offered: round("c", 1) }, "22016201016f016301016f"),
        // missing 1 | without_exit (len 1: "n i") | reason "r"
        (RejectedIncompleteManifest { missing: 1, without_exit: vec!["n i".into()], reason: "r".into() }, "230101036e20690172"),
    ];
    assert_eq!(fixtures.len() as u32, Status::REPLY_VARIANTS);
    for (reply, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(Status::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Status::decode_reply(&expected).unwrap(), reply);
    }
}

/// CONTRACT: the drain receipt a `Stopped` carries is frozen: `Established` is variant 0, `Deadline` variant 1,
/// each with its count as a varint.
#[test]
fn a_left_reply_carries_its_drain_receipt_in_frozen_bytes() {
    use StatusReply::Stopped;
    for (reply, hex) in [
        (Stopped { receipt: DrainReceipt::Established { in_flight: 0 } }, "110000"),
        (Stopped { receipt: DrainReceipt::Established { in_flight: 3 } }, "110003"),
        (Stopped { receipt: DrainReceipt::Deadline { last_in_flight: 300 } }, "1101ac02"),
    ] {
        let expected = bytes(hex);
        assert_eq!(Status::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Status::decode_reply(&expected).unwrap(), reply);
    }
}

/// CONTRACT: the largest `MeshLeave` (a container's 64-character id and a manifest reference at its
/// 128-byte bound) fits the Status request ceiling with room for every other field; a manifest
/// reference over its bound is the caller's to refuse before it sends.
#[test]
fn the_largest_mesh_leave_fits_the_request_ceiling() {
    let request = StatusRequest::MeshLeave {
        mesh_id: mesh(),
        build_id: "bld_1a2b3c4d".into(),
        attempt: u32::MAX,
        operation: "shutdown-mesh:1123456789ab".into(),
        final_node_id: node(),
        final_incarnation: IncarnationId("i".repeat(32)),
        final_runtime: RuntimeFact { deployment_id: "d".repeat(64), provider: RuntimeProvider::Container, control_domain: "c".repeat(128), locator: RuntimeLocator::Container { id: "a".repeat(64) } },
        receipt_manifest: "m".repeat(MAX_RECEIPT_MANIFEST_BYTES),
    };
    let bytes = Status::encode_request(&request).unwrap();
    assert!(bytes.len() + 512 <= Status::MAX_REQUEST_FRAME_BYTES, "{} bytes of {}", bytes.len(), Status::MAX_REQUEST_FRAME_BYTES);
    assert_eq!(Status::decode_request(&bytes).unwrap(), request);
}

fn golden<T: serde::Serialize>(value: T, expected: &str) {
    assert_eq!(postcard::to_allocvec(&value).unwrap(), bytes(expected));
}

#[test]
fn nested_enum_discriminants_and_fields_are_frozen_too() {
    for (state, expected) in [
        (NodeState::Pending, "00"), (NodeState::ReadyForTraffic, "01"),
        (NodeState::Draining, "02"), (NodeState::Leaving, "03"),
    ] { golden(state, expected); }
    for (state, expected) in [
        (MeshState::Pending, "00"), (MeshState::ReadyForTraffic, "01"),
        (MeshState::Leaving, "02"), (MeshState::Dead, "03"),
    ] { golden(state, expected); }
    golden(FabricEvent::ReadyForTraffic, "00");
    golden(FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() }, "010d6d657368312e61646d696e2e31");
    golden(NotAuthority::ReceiverNotPrimary { needed: "mesh-primary".into() }, "000c6d6573682d7072696d617279");
    golden(NotAuthority::SenderNotSubject { sender: "peer-path".into() }, "0109706565722d70617468");
    golden(NotAuthority::SubjectUnknown, "02");
    golden(MalformedKind::TooLarge, "00");
    golden(MalformedKind::UnknownVariant, "01");
    golden(MalformedKind::Corrupt, "02");
    golden(RetryOn::AuthorityChanged, "00");
    golden(RetryOn::TargetReachable { node_id: node() }, "010c303132333435363738396162");
    golden(RetryOn::AuthorityReady, "02");
    golden(RetryOn::AuthorityAddressed, "03");
    golden(ConnectionScope::All, "00");
    golden(ConnectionScope::Connections(vec![connection()]), "01010c333132333435363738396162");
    // build_id "b" | attempt 300 (ac 02) | operation "o"
    golden(MeshDeleteRound { build_id: "b".into(), attempt: 300, operation: "o".into() }, "0162ac02016f");
}
