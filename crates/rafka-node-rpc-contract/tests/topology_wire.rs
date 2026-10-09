//! Fixed bytes for the `0x1E` topology family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::wire::WireDigest;
use rafka_mesh_entity::{IncarnationId, LifecycleOp, PublisherId, Seat, SeatHolder};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::{DecodeFailure, NodeProtocol};
use rafka_node_rpc_contract::streaming::{FrameKind, StreamingProtocol};
use rafka_node_rpc_contract::topology::{SourceVersion, StoredNode, Topology, TopologyReply, TopologyRequest};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn publisher() -> PublisherId {
    PublisherId { node: "a".into(), incarnation: IncarnationId("b".into()) }
}

fn digest() -> rafka_mesh_entity::MeshDigest {
    use rafka_mesh_entity::*;
    MeshDigest {
        fabric_id: FabricId::mint(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:34567".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
        load: None,
    }
}

#[test]
fn requests_match_the_frozen_wire_schema() {
    let fixtures = [
        (TopologyRequest::GetTopology { mesh: None, since: None }, "00 00 00"),
        (TopologyRequest::GetTopology { mesh: Some("mesh1".into()), since: None }, "00 01 05 6d65736831 00"),
        (TopologyRequest::GetTopology { mesh: None, since: Some(SourceVersion { publisher: publisher(), topology_version: 7 }) }, "00 00 01 0161 0162 07"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(Topology::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Topology::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Topology::decode_request(&[1]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_thirteen_variant_wire_schema() {
    use TopologyReply::*;
    let fixtures = [
        (Unchanged { mesh: "m".into(), publisher: publisher(), topology_version: 7 }, "01 016d 0161 0162 07"),
        (End { meshes: 2 }, "0202"),
        (NotReady { reason: "n".into() }, "03016e"),
        (UnknownMesh { mesh: "m".into() }, "04016d"),
        (PeerUnresolved { reason: "p".into() }, "050170"),
        (Busy { reason: "b".into() }, "060162"),
        (Draining { reason: "d".into() }, "070164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0802"),
        (Unauthorized { reason: "u".into() }, "090175"),
        (Started, "0a"),
        (Stored { mesh: "m".into(), mesh_id: None, nodes: vec![StoredNode { node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), name: "m.rpc.1".into(), endpoint_id: rafka_mesh_entity::EndpointId("k".into()), incarnation: IncarnationId("i".into()), transport_addr: "127.0.0.1:80".parse().unwrap() }] }, "0b 016d 00 01 0c 303472616a303970337a7037 07 6d2e7270632e31 016b 0169 00 7f000001 50"),
        (Seats { seat: Seat::FabricPrimary, holder: SeatHolder { mesh: "m".into(), node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId("i".into()), epoch: 2 }, gone: false }, "0c 01 016d 0c 303472616a303970337a7037 0169 02 00"),
        (Seats { seat: Seat::MeshPrimary, holder: SeatHolder { mesh: "m".into(), node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId("i".into()), epoch: 300 }, gone: true }, "0c 00 016d 0c 303472616a303970337a7037 0169 ac02 01"),
        (Stored { mesh: "m".into(), mesh_id: Some(rafka_mesh_entity::MeshId::parse("04raj09p3zp7").unwrap()), nodes: vec![StoredNode { node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), name: "m.rpc.1".into(), endpoint_id: rafka_mesh_entity::EndpointId("k".into()), incarnation: IncarnationId("i".into()), transport_addr: "127.0.0.1:80".parse().unwrap() }] }, "0b 016d 01 0c 303472616a303970337a7037 01 0c 303472616a303970337a7037 07 6d2e7270632e31 016b 0169 00 7f000001 50"),
    ];
    for (r, hex) in fixtures {
        assert_eq!(Topology::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Topology::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(Topology::REPLY_VARIANTS, 13);
    assert_eq!(Topology::decode_reply(&[13]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn a_snapshot_chunk_round_trips_with_its_digests_and_overlays() {
    let d = digest();
    let op = LifecycleOp {
        build_id: "b".into(),
        attempt: 1,
        operation: "retire-node:mesh1.rpc.2".into(),
        node_id: rafka_mesh_entity::NodeId::mint(),
        incarnation: IncarnationId::mint(),
        name: "mesh1.rpc.2".parse().unwrap(),
        event_at_rafka_ms: 5,
    };
    let r = TopologyReply::Snapshot {
        mesh: "mesh1".into(),
        publisher: publisher(),
        topology_version: 3,
        snapshot_id: 9,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![WireDigest::from(&d)],
        in_flight: vec![op.clone()],
        departed: vec![op],
    };
    let bytes = Topology::encode_reply(&r).unwrap();
    assert_eq!(bytes[0], 0x00, "variant 0");
    assert!(bytes.len() <= Topology::MAX_REPLY_FRAME_BYTES);
    assert_eq!(Topology::decode_reply(&bytes).unwrap(), r);
}

#[test]
fn every_frame_is_classified_for_stream_order_and_the_family_is_not_forwardable() {
    use TopologyReply::*;
    assert!(!Topology::FORWARDABLE);
    assert_eq!(Topology::OP, 0x1E);
    assert_eq!(Topology::frame_kind(&Started), FrameKind::Started);
    assert_eq!(Topology::frame_kind(&Unchanged { mesh: "m".into(), publisher: publisher(), topology_version: 1 }), FrameKind::Data);
    assert_eq!(Topology::frame_kind(&End { meshes: 0 }), FrameKind::Terminal);
    let holder = SeatHolder { mesh: "m".into(), node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId("i".into()), epoch: 1 };
    assert_eq!(Topology::frame_kind(&Seats { seat: Seat::FabricPrimary, holder, gone: false }), FrameKind::Data);
    assert_eq!(Topology::frame_kind(&NotReady { reason: "n".into() }), FrameKind::Refusal(ReplyKind::NotReady));
    assert_eq!(Topology::frame_kind(&UnknownMesh { mesh: "m".into() }), FrameKind::Refusal(ReplyKind::ProtocolRefusal));
}
