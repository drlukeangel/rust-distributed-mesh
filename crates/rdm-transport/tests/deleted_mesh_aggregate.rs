//! `e12.s72` — a deleted mesh's last admin publishes its final aggregate with `status = Leaving`
//! and `in_flight: [node.delete:<last_admin>]`; receivers apply it, emit `via-node-deleting` and
//! `via-member-state` (`Leaving, routable=false`), and hold the last admin departed for
//! `DEPARTED_RETENTION` while keeping the final aggregate in `SnapshotReceiver` and `Forwarder`.

use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
use rdm_transport::membership::Frame;
use rdm_transport::snapshot::SnapshotReceiver;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(name: &str, port: u16) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Leaving,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000,
        data_dir: None,
        mesh_id: Some(MeshId::mint()),
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

#[test]
fn deleted_meshs_last_admin_publishes_its_final_aggregate_with_leaving_and_node_delete() {
    let m3_a1 = digest("mesh3.admin.1", 29301);
    let final_del = LifecycleOp {
        build_id: "b-s72".into(),
        attempt: 1,
        operation: "node.delete:mesh3.admin.1".into(),
        node_id: m3_a1.node.node_id.clone(),
        incarnation: m3_a1.node.incarnation.clone(),
        name: m3_a1.node.name.clone(),
        event_at_rafka_ms: 2_000,
    };
    let pub_id = PublisherId {
        node: m3_a1.node.name.to_string(),
        incarnation: m3_a1.node.incarnation.clone(),
     node_id: m3_a1.node.node_id.clone(), };
    let mut rx = SnapshotReceiver::default();
    let chunk = rdm_transport::snapshot::Chunk {
        mesh: "mesh3".into(),
        publisher: pub_id.clone(),
        forwarded_by: None,
        topology_version: 7,
        snapshot_id: 1,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![m3_a1.clone()],
        in_flight: vec![final_del.clone()],
        departed: vec![],
    };
    let taken = rx.take_chunk(chunk);
    assert!(matches!(taken, rdm_transport::snapshot::Taken::Installed(_)));
    let _ = Frame::Members {
        mesh: "mesh3".into(),
        publisher: pub_id,
        forwarded_by: None,
        topology_version: 7,
        published_at_rafka_ms: 2_000,
        snapshot_id: 1,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![m3_a1],
        in_flight: vec![final_del],
        departed: vec![],
    };
}
