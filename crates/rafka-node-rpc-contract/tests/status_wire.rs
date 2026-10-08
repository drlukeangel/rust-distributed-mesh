//! Fixed bytes for the coordinated 2026-10-07 status reseal (#2923).
//! These literals lock discriminants and positional fields independently of codec round trips.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_contract::status::{FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest};

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
fn requests_match_the_frozen_six_variant_wire_schema() {
    use StatusRequest::*;
    let fixtures = [
        (DeclareNodeState { node_id: node(), incarnation: birth(), state: NodeState::Leaving }, "000c30313233343536373839616205626972746803"),
        (DeclareMeshState { mesh_id: mesh(), state: MeshState::Leaving }, "010c31313233343536373839616202"),
        (ApplyNodeState { node_id: node(), incarnation: birth(), state: NodeState::Draining }, "020c30313233343536373839616205626972746802"),
        (ProbeNodeState { node_id: node(), incarnation: birth() }, "030c303132333435363738396162056269727468"),
        (ApplyMeshState { mesh_id: mesh(), mesh_name: "mesh1".into(), state: MeshState::Pending }, "040c313132333435363738396162056d6573683100"),
        (ApplyFabricEvent { fabric_id: fabric(), event: FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() } }, "050c323132333435363738396162010d6d657368312e61646d696e2e31"),
    ];
    assert_eq!(fixtures.len() as u32, Status::REQUEST_VARIANTS);
    for (request, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(Status::encode_request(&request).unwrap(), expected, "{request:?}");
        assert_eq!(Status::decode_request(&expected).unwrap(), request);
    }
}

#[test]
fn replies_match_the_frozen_sixteen_variant_wire_schema() {
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
    ];
    assert_eq!(fixtures.len() as u32, Status::REPLY_VARIANTS);
    for (reply, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(Status::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Status::decode_reply(&expected).unwrap(), reply);
    }
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
