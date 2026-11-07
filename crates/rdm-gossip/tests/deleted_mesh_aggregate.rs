//! `e12.s72` — a deleted mesh's last admin publishes its final mesh topology, itself the only node and
//! listed `deleting` (gossip.md "Mesh delete": "the mesh deleted and itself deleting"; Q390: a removal is
//! the node's status); receivers apply it, emit `via-node-deleting` and `via-member-state`
//! (`routable=false`), and hold the last admin deleted for `DELETED_RETENTION` while keeping the final
//! mesh topology in `SnapshotReceiver` and `Forwarder`.

use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
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
fn deleted_meshs_last_admin_publishes_its_final_aggregate_with_itself_deleting() {
    let m3_a1 = digest(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", 29301);
    let author = rdm_gossip::envelope::Birth::publisher(&fabric(), &PublisherId { incarnation: m3_a1.node.incarnation.clone(), node_id: m3_a1.node.node_id.clone() });
    let mut rx = SnapshotReceiver::default();
    // The deleted mesh's id rides its aggregate beside its name; the last admin is its only node,
    // listed deleting.
    let mesh3_id = MeshId::mint();
    let last = MeshDigest { status: NodeStatus::Deleting, ..m3_a1.clone() };
    let frame = Frame::aggregate("mesh3", Some(mesh3_id.clone()), &author, 7, 2_000, 0, 1, vec![last]);
    let Frame::TopologyFull { nodes, mesh_id, .. } = frame.data() else { panic!("not TopologyFull") };
    assert_eq!(mesh_id.as_ref(), Some(&mesh3_id));
    assert_eq!(nodes.iter().map(|n| (n.node_id.clone(), n.status)).collect::<Vec<_>>(), vec![(m3_a1.node.node_id.clone(), NodeStatus::Deleting)], "its one node, the last admin, deleting");
    let chunk = rdm_gossip::snapshot::Chunk {
        mesh: "mesh3".into(),
        mesh_id: Some(mesh3_id),
        author: Some(author),
        topology_version: 7,
        time_ms: 2_000,
        chunk_index: 0,
        chunk_count: 1,
        digests: nodes.iter().map(|n| n.digest(&fabric(), "mesh3")).collect(),
        runtimes: Vec::new(),
    };
    let taken = rx.take_chunk(chunk);
    assert!(matches!(taken, rdm_gossip::snapshot::Taken::Installed(_)));
}
