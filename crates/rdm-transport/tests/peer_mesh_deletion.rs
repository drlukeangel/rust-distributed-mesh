//! A deletion executed outside the deleted birth's mesh reaches that mesh (gossip.md §3.1, the
//! `NodeDeleted` row: published on the "executor's own mesh channel; backbone Members.deleted",
//! forwarded by "peer mesh primaries, onto their mesh channels", and after it "the current topology
//! no longer lists it"; §2 rules: "lifecycle events are published by the executor on its own mesh
//! channel and reach the backbone only inside Members.in_flight and Members.deleted"; §6.1: the
//! executor "carries it in `Members.deleted` on the backbone ... peer primaries forward it").
//!
//! The defect this cell pins (rshape `i143-rshape-authority-chaos-process`
//! `mock_primary_handoffs_preserve_one_effective_writer` at 1ecc14207): the fabric-primary
//! mesh1.admin.2 executed the recovery delete of mesh2.admin.1 and published `NodeDeleted` on mesh1's
//! channel; its mesh1 aggregate carried only mesh1's own deletions, so no mesh2 process took the
//! deletion, and mesh2's primary later took the deleted birth back as an arrival.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{learn_addresses, Backbone, Frame, TopologyView};
use serde_json::Value;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(name: &str, node_id: &NodeId, incarnation: &IncarnationId, port: u16) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: incarnation.clone(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::ReadyForTraffic,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// CONTRACT (gossip.md §3.1 `NodeDeleted`; §2 rules; §6.1): the fabric-primary mesh1.admin.1, mesh1's
/// primary, executes the delete of mesh2.admin.1, a node of a peer mesh, and publishes `NodeDeleted`
/// on mesh1's channel. mesh2's primary mesh2.admin.2 hears mesh1's aggregate on the backbone, and
/// mesh2.rpc.1 is on mesh2's channel through mesh2.admin.2 alone. Both mesh2 nodes take the deletion:
/// each holds mesh2.admin.1 deleted and no longer lists it, and each emits
/// `rdm.mesh.topology.remove.via-node-deleted` for the exact birth. Must NOT happen: the deletion
/// reaching only the executor's mesh, so the deleted birth's own mesh keeps listing it.
#[test]
fn a_deletion_executed_in_another_mesh_is_taken_by_the_deleted_births_mesh() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-peer-mesh-deletion").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (fp, m2, rpc) = (node().await, node().await, node().await);
        let (fp_addr, m2_addr) = (addr_of(&fp.ep), addr_of(&m2.ep));
        learn_addresses(&m2.ep, &[fp_addr.clone()]).unwrap();
        learn_addresses(&rpc.ep, &[m2_addr.clone()]).unwrap();
        let fab = fabric();
        let (mesh1, mesh2) = (MeshId::mint(), MeshId::mint());
        let clock = rdm_transport::clock::os_clock;
        let fp_m = TopologyView::join(&fp.gossip, &fp.ep, &fab, "mesh1", &mesh1, "mesh1.admin.1", clock(), vec![], &[]).await.unwrap();
        let m2_m = TopologyView::join(&m2.gossip, &m2.ep, &fab, "mesh2", &mesh2, "mesh2.admin.2", clock(), vec![], &[]).await.unwrap();
        let rpc_m = TopologyView::join(&rpc.gossip, &rpc.ep, &fab, "mesh2", &mesh2, "mesh2.rpc.1", clock(), vec![m2_addr.clone()], &[]).await.unwrap();
        let fp_bb = Backbone::join(&fp.gossip, &fp.ep, &fp_m, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: NodeId::mint() }, vec![]).await.unwrap();
        let m2_bb = Backbone::join(&m2.gossip, &m2.ep, &m2_m, "mesh2", rdm_core::PublisherId { node: "mesh2.admin.2".to_string(), incarnation: IncarnationId::mint(), node_id: NodeId::mint() }, vec![fp_addr.clone()]).await.unwrap();
        fp_bb.set_mesh_primary(true);
        fp_bb.set_fabric_primary(true);
        m2_bb.set_mesh_primary(true);
        rpc_m.wait_for_neighbour().await;

        // mesh2.admin.1, the previous mesh-primary of mesh2: every view holds it.
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let subject = digest("mesh2.admin.1", &id, &inc, 42_001);
        for v in [&fp_m, &m2_m, &rpc_m] {
            assert!(v.digest_view.record(subject.clone()), "the view holds mesh2.admin.1");
        }

        // The fabric-primary, as executor, proves the exit and publishes NodeDeleted on its own mesh channel.
        let op = LifecycleOp {
            build_id: "b-peer-mesh-deletion".into(),
            attempt: 3,
            operation: "node.delete:mesh2.admin.1".into(),
            node_id: id.clone(),
            incarnation: inc.clone(),
            name: subject.node.name.clone(),
            event_at_rafka_ms: 2_000,
        };
        fp_m.publish_lifecycle(&Frame::NodeDeleted { op: op.clone(), forwarded_by: None }).await.unwrap();
        assert!(fp_m.digest_view.is_deleted(id.as_str()), "the executor holds its own deletion");

        // Each primary publishes as a node-admin's publication does, until mesh2's nodes hold the deletion.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !(m2_m.digest_view.is_deleted(id.as_str()) && rpc_m.digest_view.is_deleted(id.as_str())) {
            assert!(
                std::time::Instant::now() < deadline,
                "mesh2's nodes never took the deletion of mesh2.admin.1 the fabric-primary executed: mesh2.admin.2 deleted={} mesh2.rpc.1 deleted={}",
                m2_m.digest_view.is_deleted(id.as_str()),
                rpc_m.digest_view.is_deleted(id.as_str()),
            );
            fp_bb.publish(&fp_m, fp_bb.own_members(&fp_m)).await;
            m2_bb.publish(&m2_m, m2_bb.own_members(&m2_m)).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for v in [&m2_m, &rpc_m] {
            assert!(v.digest_view.get(id.as_str()).is_none(), "the current topology no longer lists mesh2.admin.1");
        }

        fp_bb.set_fabric_primary(false);
        for n in [&fp, &m2, &rpc] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        std::fs::write(dir.join("peer-mesh-deletion.spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        for observer in ["mesh2.admin.2", "mesh2.rpc.1"] {
            let taken: Vec<&Value> = spans
                .iter()
                .filter(|s| s["name"] == "rdm.mesh.topology.remove.via-node-deleted" && attr(s, "observer") == observer && attr(s, "node_id") == id.as_str() && attr(s, "incarnation_id") == inc.as_str())
                .collect();
            assert_eq!(taken.len(), 1, "{observer} takes the deletion of the exact birth once: {taken:#?}");
            println!("peer-mesh-deletion: {observer} took node.delete:mesh2.admin.1 trace {} span {}", taken[0]["trace_id"], taken[0]["span_id"]);
        }
    });
}
