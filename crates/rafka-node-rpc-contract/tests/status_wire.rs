//! Fixed bytes for the coordinated 2026-10-07 status reseal (#2923); the four drain/stop command
//! variants (6..=9) are appended after it, their bytes derived from the postcard rules by hand.
//! These literals lock discriminants and positional fields independently of codec round trips.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_contract::status::{MAX_RECEIPT_MANIFEST_BYTES, FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest};

fn node() -> NodeId { NodeId::parse("0123456789ab").unwrap() }
fn mesh() -> MeshId { MeshId::parse("1123456789ab").unwrap() }
fn fabric() -> FabricId { FabricId::parse("2123456789ab").unwrap() }
fn birth() -> IncarnationId { IncarnationId("birth".into()) }

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| {
        u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
    }).collect()
}

#[test]
fn requests_match_the_frozen_twelve_variant_wire_schema() {
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
    ];
    assert_eq!(fixtures.len() as u32, Status::REQUEST_VARIANTS);
    for (request, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(Status::encode_request(&request).unwrap(), expected, "{request:?}");
        assert_eq!(Status::decode_request(&expected).unwrap(), request);
    }
}

#[test]
fn replies_match_the_frozen_seventeen_variant_wire_schema() {
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
    ];
    assert_eq!(fixtures.len() as u32, Status::REPLY_VARIANTS);
    for (reply, hex) in fixtures {
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
}
