//! Fixed bytes for the `0x1E` topology family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::wire::ProjectionEntry;
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
    PublisherId { node: "a".into(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("b")).unwrap() }
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
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

#[test]
fn requests_match_the_frozen_wire_schema() {
    let fixtures = [
        (TopologyRequest::GetTopology { mesh: None, since: None }, "00 00 00"),
        (TopologyRequest::GetTopology { mesh: Some("mesh1".into()), since: None }, "00 01 05 6d65736831 00"),
        (TopologyRequest::GetTopology { mesh: None, since: Some(SourceVersion { publisher: publisher(), topology_version: 7 }) }, "00 00 01 0161 0c 7972797a396a333033776435 07"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(Topology::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Topology::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Topology::decode_request(&[1]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_fourteen_variant_wire_schema() {
    use TopologyReply::*;
    let fixtures = [
        (Unchanged { mesh: "m".into(), publisher: publisher(), topology_version: 7 }, "01 016d 0161 0c 7972797a396a333033776435 07"),
        (End { meshes: 2 }, "0202"),
        (NotReady { reason: "n".into() }, "03016e"),
        (UnknownMesh { mesh: "m".into() }, "04016d"),
        (PeerUnresolved { reason: "p".into() }, "050170"),
        (Busy { reason: "b".into() }, "060162"),
        (ConnectionsDeleting { reason: "d".into() }, "070164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0802"),
        (Unauthorized { reason: "u".into() }, "090175"),
        (Started, "0a"),
        (Stored { mesh: "m".into(), mesh_id: None, nodes: vec![StoredNode { node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), name: "m.rpc.1".into(), endpoint_id: rafka_mesh_entity::EndpointId("k".into()), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), transport_addr: "127.0.0.1:80".parse().unwrap() }] }, "0b 016d 00 01 0c 303472616a303970337a7037 07 6d2e7270632e31 016b 0c79727a34396a333033796834 00 7f000001 50"),
        (Seats { seat: Seat::FabricPrimary, holder: SeatHolder { mesh: "m".into(), node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), revision: 2 }, gone: false }, "0c 01 016d 0c 303472616a303970337a7037 0c79727a34396a333033796834 02 00"),
        (Seats { seat: Seat::MeshPrimary, holder: SeatHolder { mesh: "m".into(), node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), revision: 300 }, gone: true }, "0c 00 016d 0c 303472616a303970337a7037 0c79727a34396a333033796834 ac02 01"),
        (Stored { mesh: "m".into(), mesh_id: Some(rafka_mesh_entity::MeshId::parse("04raj09p3zp7").unwrap()), nodes: vec![StoredNode { node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), name: "m.rpc.1".into(), endpoint_id: rafka_mesh_entity::EndpointId("k".into()), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), transport_addr: "127.0.0.1:80".parse().unwrap() }] }, "0b 016d 01 0c 303472616a303970337a7037 01 0c 303472616a303970337a7037 07 6d2e7270632e31 016b 0c79727a34396a333033796834 00 7f000001 50"),
        (RafkaTime { ms: 1_000, seat: None }, "0d e807 00"),
        (RafkaTime { ms: 300, seat: Some(Seat::FabricPrimary) }, "0d ac02 01 01"),
        (RafkaTime { ms: 7, seat: Some(Seat::MeshPrimary) }, "0d 07 01 00"),
    ];
    for (r, hex) in fixtures {
        assert_eq!(Topology::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Topology::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(Topology::REPLY_VARIANTS, 14);
    assert_eq!(Topology::decode_reply(&[14]), Err(DecodeFailure::UnknownVariant));
}

/// CONTRACT (gossip.md §3.1: the topology read serves the member projection a node holds): a
/// `Snapshot` chunk (variant 0) carries each member as a projection entry, its digest (position 2 the
/// self-published liveness `node_state`, `Alive` = 1) then the status held for it (`Draining` = 2).
/// What must NOT happen: an entry without its status, or a status inside the digest.
#[test]
fn a_snapshot_chunk_carries_each_member_as_its_digest_then_its_status() {
    use rafka_mesh_entity::*;
    let d = MeshDigest {
        fabric_id: FabricId::parse("1123456789ab").unwrap(),
        node: MeshNode {
            node_id: NodeId::parse("0123456789ab").unwrap(),
            name: "mesh1.rpc.1".parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:80".parse().unwrap(),
            incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Draining,
        node_state: NodeState::Alive,
        admin_api_base: None,
        digest_seq: 7,
        emitted_at_rafka_ms: 9,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    };
    let r = TopologyReply::Snapshot { mesh: "mesh1".into(), publisher: publisher(), topology_version: 3, snapshot_id: 9, chunk_index: 0, chunk_count: 1, digests: vec![ProjectionEntry::from(&d)], in_flight: vec![], departed: vec![] };
    let hex = "00 05 6d65736831 0161 0c 7972797a396a333033776435 03 09 00 01 01 \
        0c 313132333435363738396162 0c 303132333435363738396162 0b 6d657368312e7270632e31 01 6b 00 7f000001 50 0c 79727a34396a333033796834 00 00 \
        01 00 07 09 00 00 00 00 00 00 \
        02 \
        00 00";
    assert_eq!(Topology::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
    let TopologyReply::Snapshot { digests, .. } = Topology::decode_reply(&bytes(hex)).unwrap() else { panic!("not a Snapshot") };
    let back: Vec<MeshDigest> = digests.into_iter().map(MeshDigest::from).collect();
    assert_eq!(back, vec![d], "the member decodes with its liveness and its status");
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
        digests: vec![ProjectionEntry::from(&d)],
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
    let holder = SeatHolder { mesh: "m".into(), node_id: rafka_mesh_entity::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), revision: 1 };
    assert_eq!(Topology::frame_kind(&Seats { seat: Seat::FabricPrimary, holder, gone: false }), FrameKind::Data);
    assert_eq!(Topology::frame_kind(&RafkaTime { ms: 1, seat: None }), FrameKind::Data);
    assert_eq!(Topology::frame_kind(&NotReady { reason: "n".into() }), FrameKind::Refusal(ReplyKind::NotReady));
    assert_eq!(Topology::frame_kind(&UnknownMesh { mesh: "m".into() }), FrameKind::Refusal(ReplyKind::ProtocolRefusal));
}
