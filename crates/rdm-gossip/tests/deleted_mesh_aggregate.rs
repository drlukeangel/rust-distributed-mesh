//! `e12.s72` — a deleted mesh's last admin publishes its final mesh topology with `status = Leaving`
//! and `in_flight: [node.delete:<last_admin>]`; receivers apply it, emit `via-node-deleting` and
//! `via-member-state` (`Leaving, routable=false`), and hold the last admin deleted for
//! `DELETED_RETENTION` while keeping the final mesh topology in `SnapshotReceiver` and `Forwarder`.

use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
use rdm_gossip::frame::Frame;
use rdm_gossip::snapshot::SnapshotReceiver;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(name: rdm_core::PathName, mesh: &str, r#type: &str, port: u16) -> MeshDigest {
    let node_id = NodeId::mint();
    MeshDigest { 
        fabric_id: fabric(),
        node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
            node_id: node_id.clone(),
            path: name.path.clone(), name: name.name.clone(),
            endpoint_id: EndpointId(format!("key-{node_id}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        },
        status: NodeStatus::Leaving,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000,
        primary: Vec::new(),
    }
}

#[test]
fn deleted_meshs_last_admin_publishes_its_final_aggregate_with_leaving_and_node_delete() {
    let m3_a1 = digest(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", 29301);
    let final_del = LifecycleOp { r#type: m3_a1.node.r#type.clone(), mesh: m3_a1.node.mesh.clone(),
        build_id: "b-s72".into(),
        operation: rdm_core::OperationId::node("node.delete", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh3.node-admin", "1").unwrap())),
        node_id: m3_a1.node.node_id.clone(),
        incarnation: m3_a1.node.incarnation.clone(),
        path: m3_a1.node.path.clone(), name: m3_a1.node.name.clone(),
        event_at_rafka_ms: 2_000,
        traceparent: None,
        tracestate: None,
    };
    let pub_id = PublisherId {
        incarnation: m3_a1.node.incarnation.clone(),
     node_id: m3_a1.node.node_id.clone(), };
    let mut rx = SnapshotReceiver::default();
    let chunk = rdm_gossip::snapshot::Chunk {
        mesh: "mesh3".into(),
        publisher: pub_id.clone(),
        forwarded_by: None,
        topology_version: 7,
        snapshot_id: 1,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![m3_a1.clone()],
        in_flight: vec![final_del.clone()],
        deleted: vec![],
    };
    let taken = rx.take_chunk(chunk);
    assert!(matches!(taken, rdm_gossip::snapshot::Taken::Installed(_)));
    let _ = Frame::TopologyFull {
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
        deleted: vec![],
    };
}
