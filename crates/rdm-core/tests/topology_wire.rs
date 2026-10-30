//! Fixed bytes for the `0x1E` topology family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::wire::ProjectionEntry;
use rdm_core::{IncarnationId, LifecycleOp, PublisherId, Primary};
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use rdm_core::topology::{PrimaryKind, SourceVersion, StoredNode, Topology, TopologyReply, TopologyRequest};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn publisher() -> PublisherId {
    PublisherId { node: "a".into(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("b")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() }
}

fn digest() -> rdm_core::MeshDigest {
    use rdm_core::*;
    MeshDigest { held_hash: Default::default(),
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
        status: MemberStatus::Ready,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

#[test]
fn requests_match_the_frozen_wire_schema() {
    let fixtures = [
        (TopologyRequest::GetTopology { mesh: None, since: None }, "00 00 00"),
        (TopologyRequest::GetTopology { mesh: Some("mesh1".into()), since: None }, "00 01 05 6d65736831 00"),
        (TopologyRequest::GetTopology { mesh: None, since: Some(SourceVersion { publisher: publisher(), topology_version: 7 }) }, "00 00 01 0161 0c 7972797a396a333033776435 0c 303132333435363738396162 07"),
    ];
    for (q, hex) in fixtures {
        assert_eq!(Topology::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Topology::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Topology::decode_request(&[1]), Err(DecodeFailure::UnknownVariant));
}

/// The smallest own row: `m.rpc.1`, ready, one config key, every optional field absent.
fn own_row() -> rdm_core::views::NodeView {
    rdm_core::views::NodeView {
        name: "m.rpc.1".parse().unwrap(),
        kind: rdm_core::NodeKind::RpcNode,
        mesh: "m".into(),
        node_id: rdm_core::NodeId::parse("04raj09p3zp7").unwrap(),
        endpoint_id: None,
        incarnation_id: None,
        deployment_id: None,
        provider: None,
        data_dir: None,
        status: rdm_core::views::NodeStatus::Ready,
        is_primary: false,
        is_fabric_primary: false,
        admin_api_base: None,
        transport_addr: None,
        listeners: vec![],
        declared: None,
        stopped: false,
        load: None,
        gossip: None,
        tags: Default::default(),
        config: [("k".to_string(), "v".to_string())].into(),
        data_dir_lost: None,
        data_dir_unreadable: None,
        types: Default::default(),
        held: Default::default(),
        held_hash: [0; 32],
        cert: None,
        executable: None,
        storage_persistence: None,
        topology_storage_provider: None,
    }
}

#[test]
fn replies_match_the_frozen_fifteen_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use TopologyReply::*;
    let fixtures = [
        (Snapshot { mesh: "m".into(), publisher: publisher(), topology_version: 3, snapshot_id: 9, chunk_index: 0, chunk_count: 1, digests: vec![], in_flight: vec![], deleted: vec![] }, "00 016d 0161 0c 7972797a396a333033776435 0c 303132333435363738396162 03 09 00 01 00 00 00"),
        (Unchanged { mesh: "m".into(), publisher: publisher(), topology_version: 7 }, "01 016d 0161 0c 7972797a396a333033776435 0c 303132333435363738396162 07"),
        (End { meshes: 2 }, "0202"),
        (NotReady { reason: "n".into() }, "03016e"),
        (UnknownMesh { mesh: "m".into() }, "04016d"),
        (PeerUnresolved { reason: "p".into() }, "050170"),
        (Busy { reason: "b".into() }, "060162"),
        (ConnectionsDeleting { reason: "d".into() }, "070164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0802"),
        (Unauthorized { reason: "u".into() }, "090175"),
        (Started, "0a"),
        (Stored { mesh: "m".into(), mesh_id: None, nodes: vec![StoredNode { node_id: rdm_core::NodeId::parse("04raj09p3zp7").unwrap(), name: "m.rpc.1".into(), endpoint_id: rdm_core::EndpointId("k".into()), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), transport_addr: "127.0.0.1:80".parse().unwrap() }] }, "0b 016d 00 01 0c 303472616a303970337a7037 07 6d2e7270632e31 016b 0c79727a34396a333033796834 00 7f000001 50"),
        (Primaries { kind: PrimaryKind::FabricPrimary, primary: Primary { mesh: "m".into(), node_id: rdm_core::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), revision: 2 }, gone: false }, "0c 01 016d 0c 303472616a303970337a7037 0c79727a34396a333033796834 02 00"),
        (Primaries { kind: PrimaryKind::MeshPrimary, primary: Primary { mesh: "m".into(), node_id: rdm_core::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), revision: 300 }, gone: true }, "0c 00 016d 0c 303472616a303970337a7037 0c79727a34396a333033796834 ac02 01"),
        (Stored { mesh: "m".into(), mesh_id: Some(rdm_core::MeshId::parse("04raj09p3zp7").unwrap()), nodes: vec![StoredNode { node_id: rdm_core::NodeId::parse("04raj09p3zp7").unwrap(), name: "m.rpc.1".into(), endpoint_id: rdm_core::EndpointId("k".into()), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), transport_addr: "127.0.0.1:80".parse().unwrap() }] }, "0b 016d 01 0c 303472616a303970337a7037 01 0c 303472616a303970337a7037 07 6d2e7270632e31 016b 0c79727a34396a333033796834 00 7f000001 50"),
        (RafkaTime { ms: 1_000, kind: None }, "0d e807 00"),
        (RafkaTime { ms: 300, kind: Some(PrimaryKind::FabricPrimary) }, "0d ac02 01 01"),
        (RafkaTime { ms: 7, kind: Some(PrimaryKind::MeshPrimary) }, "0d 07 01 00"),
        // Q234: the reader's own whole row, a `NodeView` in its field order, then the row's revision (P1).
        (OwnRow { node: own_row(), revision: 3 }, "0e 07 6d2e7270632e31 01 016d 0c 303472616a303970337a7037 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 01 016b 0176 00 00 00 00 \
            0000000000000000000000000000000000000000000000000000000000000000 00 00 00 00 03"),
    ];
    for (r, hex) in fixtures {
        variants.insert(Topology::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Topology>(&r);
        assert_eq!(Topology::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Topology::decode_reply(&bytes(hex)).unwrap(), r);
    }
    assert_eq!(Topology::REPLY_VARIANTS, 15);
    assert_eq!(Topology::decode_reply(&[15]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), Topology::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<Topology>();
}

/// CONTRACT (gossip.md §3.1: the topology read serves the member projection a node holds): a
/// `Snapshot` chunk (variant 0) carries each member as a projection entry, its digest (position 2 the
/// self-published liveness `node_state`, `Alive` = 1) then the status held for it (`ConnectionsDeleting` = 2).
/// What must NOT happen: an entry without its status, or a status inside the digest.
#[test]
fn a_snapshot_chunk_carries_each_member_as_its_digest_then_its_status() {
    use rdm_core::*;
    let d = MeshDigest { held_hash: Default::default(),
        fabric_id: FabricId::parse("1123456789ab").unwrap(),
        node: MeshNode {
            node_id: NodeId::parse("0123456789ab").unwrap(),
            name: "mesh1.rpc.1".parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:80".parse().unwrap(),
            incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::ConnectionsDeleting,
        node_state: NodeState::Alive,
        admin_api_base: None,
        digest_seq: 7,
        emitted_at_rafka_ms: 9,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    };
    let r = TopologyReply::Snapshot { mesh: "mesh1".into(), publisher: publisher(), topology_version: 3, snapshot_id: 9, chunk_index: 0, chunk_count: 1, digests: vec![ProjectionEntry::from(&d)], in_flight: vec![], deleted: vec![] };
    let hex = "00 05 6d65736831 0161 0c 7972797a396a333033776435 0c 303132333435363738396162 03 09 00 01 01 \
        0c 313132333435363738396162 0c 303132333435363738396162 0b 6d657368312e7270632e31 01 6b 00 7f000001 50 0c 79727a34396a333033796834 00 00 \
        01 00 07 09 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
        00 \
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
        operation: "node.delete:mesh1.rpc.2".into(),
        node_id: rdm_core::NodeId::mint(),
        incarnation: IncarnationId::mint(),
        name: "mesh1.rpc.2".parse().unwrap(),
        event_at_rafka_ms: 5,
        traceparent: None,
        tracestate: None,
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
        deleted: vec![op],
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
    let primary = Primary { mesh: "m".into(), node_id: rdm_core::NodeId::parse("04raj09p3zp7").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), revision: 1 };
    assert_eq!(Topology::frame_kind(&Primaries { kind: PrimaryKind::FabricPrimary, primary, gone: false }), FrameKind::Data);
    assert_eq!(Topology::frame_kind(&RafkaTime { ms: 1, kind: None }), FrameKind::Data);
    assert_eq!(Topology::frame_kind(&NotReady { reason: "n".into() }), FrameKind::Refusal(ReplyKind::NotReady));
    assert_eq!(Topology::frame_kind(&UnknownMesh { mesh: "m".into() }), FrameKind::Refusal(ReplyKind::new(rdm_core::outcome::Code::NotFound)));
}

#[test]
fn every_reply_variant_of_topology_classifies_to_one_code() {
    replies_match_the_frozen_fifteen_variant_wire_schema();
}
