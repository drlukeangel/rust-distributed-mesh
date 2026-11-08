//! A deletion the fabric-primary executes of a peer mesh's node reaches that mesh (gossip.md, the
//! `NodeDeleted` section: "A deletion the fabric-primary executes of another mesh's node is served, the
//! node listed `deleted`, in its reads of that mesh; that mesh's next mesh-primary holds it from its start
//! read and lists it in its own aggregate (Q335)"; Q390: a mesh's aggregate lists only its own mesh's
//! nodes, each by its status).
//!
//! The defect this cell pins (rshape `i143-rshape-authority-chaos-process`
//! `mock_primary_handoffs_preserve_one_effective_writer` at 1ecc14207): the fabric-primary
//! mesh1.node-admin.2 executed the recovery delete of mesh2.node-admin.1 and published `NodeDeleted` on mesh1's
//! channel; its mesh1 aggregate carried only mesh1's own deletions, so no mesh2 process took the
//! deletion, and mesh2's primary later took the deleted birth back as an arrival.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::channel::{ChannelId, Channels};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::frame::Frame;
use rdm_gossip::aggregate::{Chunk, HeldAggregate, Taken};
use rdm_gossip::topology::TopologyView;
use serde_json::Value;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(name: rdm_core::PathName, mesh: &str, r#type: &str, node_id: &NodeId, incarnation: &IncarnationId, port: u16) -> MeshDigest {
    MeshDigest { 
        fabric_id: fabric(),
        node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
            node_id: node_id.clone(),
            path: name.path.clone(), name: name.name.clone(),
            endpoint_id: EndpointId(format!("key-{node_id}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: incarnation.clone(),
            supersedes: None,
        },
        status: NodeStatus::Ready,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000,
        primary: Vec::new(),
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

/// CONTRACT (gossip.md `NodeDeleted`; Q335, Q390; lead 0002 item 1): mesh2's mesh-primary
/// mesh2.node-admin.1 is lost; its last aggregate (v7) is what the fabric-primary holds of mesh2. The
/// fabric-primary mesh1.node-admin.1 executes its delete and publishes `NodeDeleted` on mesh1's channel;
/// its read of mesh2 then lists mesh2.node-admin.1 deleted. mesh2.node-admin.2, mesh2's next
/// mesh-primary, holds the deletion from its start read of the fabric-primary, and its first aggregate,
/// which continues mesh2's version, lists the node deleted; the fabric-primary installs it from the
/// backbone. mesh2.node.1 holds the deletion from its read of its mesh-primary. Each mesh2 node emits
/// `rdm.mesh.topology.remove.via-node-deleted` for the exact birth once and no longer lists it. Must NOT
/// happen: the deletion reaching only the executor's mesh, so the deleted birth's own mesh keeps listing it.
#[test]
fn a_deletion_executed_in_another_mesh_is_taken_by_the_deleted_births_mesh() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-peer-mesh-deletion").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (fp, m2, rpc, lost) = (node().await, node().await, node().await, node().await);
        let (fp_addr, m2_addr) = (addr_of(&fp.ep), addr_of(&m2.ep));
        learn_addresses(&m2.ep, &[fp_addr.clone()]).unwrap();
        learn_addresses(&rpc.ep, &[m2_addr.clone()]).unwrap();
        learn_addresses(&lost.ep, &[fp_addr.clone()]).unwrap();
        let fab = fabric();
        let (mesh1, mesh2) = (MeshId::mint(), MeshId::mint());
        let clock = rdm_transport::clock::os_clock;
        let fp_m = TopologyView::new(&fab, "mesh1", &mesh1, "mesh1.node-admin.1", clock(), Default::default());
        let fp_m_channels = Channels::new(&fp.gossip, &fp.ep, &fp_m).unwrap();
        fp_m_channels.join(ChannelId::mesh(fp_m.fabric_id(), fp_m.mesh_id()), vec![]).await.unwrap();
        let m2_m = TopologyView::new(&fab, "mesh2", &mesh2, "mesh2.node-admin.2", clock(), Default::default());
        let m2_m_channels = Channels::new(&m2.gossip, &m2.ep, &m2_m).unwrap();
        m2_m_channels.join(ChannelId::mesh(m2_m.fabric_id(), m2_m.mesh_id()), vec![]).await.unwrap();
        let rpc_m = TopologyView::new(&fab, "mesh2", &mesh2, "mesh2.node.1", clock(), Default::default());
        let rpc_m_channels = Channels::new(&rpc.gossip, &rpc.ep, &rpc_m).unwrap();
        rpc_m_channels.join(ChannelId::mesh(rpc_m.fabric_id(), rpc_m.mesh_id()), vec![m2_addr.clone()]).await.unwrap();
        fp_m_channels.set_publisher(&rdm_core::PublisherId { incarnation: IncarnationId::mint(), node_id: NodeId::mint() });
        fp_m_channels.join(ChannelId::backbone(fp_m.fabric_id()), vec![]).await.unwrap();
        let fp_bb = fp_m_channels.backbone().expect("the backbone is joined");
        fp_bb.set_mesh_primary(true);
        fp_bb.set_fabric_primary(true);
        rpc_m_channels.wait_for_neighbour().await;

        // mesh2's nodes as its lost mesh-primary mesh2.node-admin.1 last listed them; every view holds it.
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let subject = digest(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", &id, &inc, 42_001);
        let (m2_id, m2_inc) = (NodeId::mint(), IncarnationId::mint());
        let m2_own = digest(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", &m2_id, &m2_inc, 42_002);
        let rpc_own = digest(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", &NodeId::mint(), &IncarnationId::mint(), 42_003);
        for v in [&fp_m, &m2_m, &rpc_m] {
            assert!(v.digest_view.record(subject.clone()), "the view holds mesh2.node-admin.1");
        }
        assert!(m2_m.digest_view.record(rpc_own.clone()), "mesh2.node-admin.2 hears mesh2.node.1");

        // The lost mesh-primary's last aggregate of mesh2, v7, on the backbone: the fabric-primary holds it.
        let author = rdm_gossip::envelope::Birth { fabric: fab.clone(), node: id.clone(), incarnation: inc.clone() };
        let last = Frame::aggregate("mesh2", Some(mesh2.clone()), &author, 7, 1_000, 0, 1, vec![subject.clone(), m2_own.clone(), rpc_own.clone()]).encode();
        let mut lost_bb = lost.gossip.subscribe(ChannelId::backbone(&fab).topic(), vec![fp.ep.id()]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), lost_bb.joined()).await.expect("the lost mesh-primary's backbone joins").unwrap();
        let (lost_sender, _lost_rx) = lost_bb.split();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while fp_bb.held_source("mesh2").is_none() {
            assert!(std::time::Instant::now() < deadline, "the fabric-primary never held mesh2's last aggregate");
            lost_sender.broadcast_neighbors(last.clone().into()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(fp_bb.held_source("mesh2").map(|(_, v)| v), Some(7), "the fabric-primary holds mesh2 at v7");

        // The fabric-primary, as executor (node recovery of the lost mesh-primary, Q335), publishes
        // NodeDeleted on its own mesh channel; its read of mesh2 lists the node deleted.
        let op = LifecycleOp { r#type: subject.node.r#type.clone(), mesh: subject.node.mesh.clone(),
            build_id: "b-peer-mesh-deletion".into(),
            operation: rdm_core::OperationId::node("node.delete", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap())),
            node_id: id.clone(),
            incarnation: inc.clone(),
            path: subject.node.path.clone(), name: subject.node.name.clone(),
            event_at_rafka_ms: 2_000,
            traceparent: None,
            tracestate: None,
        };
        fp_m_channels.publish_lifecycle(&Frame::NodeDeleted { op: op.clone(), forwarded_by: None }).await.unwrap();
        assert!(fp_m.digest_view.is_deleted(id.as_str()), "the executor holds its own deletion");
        let fp_own = digest(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", &NodeId::mint(), &IncarnationId::mint(), 41_001);
        let listed = |s: &[HeldAggregate]| s.iter().find(|s| s.mesh == "mesh2").and_then(|s| s.digests.iter().find(|d| d.node.node_id == id).map(|d| (s.topology_version, d.status)));
        let served = fp_m.held_topology_of(&fp_own, Some("mesh2"));
        assert_eq!(listed(&served), Some((7, NodeStatus::Deleted)), "the fabric-primary's read of mesh2 lists the node it deleted, deleted");
        let read = |s: &[HeldAggregate], time_ms: u64| {
            let s = s.iter().find(|s| s.mesh == "mesh2").expect("the read holds mesh2");
            Chunk { mesh: s.mesh.clone(), mesh_id: s.mesh_id.clone(), author: None, topology_version: s.topology_version, time_ms, chunk_index: 0, chunk_count: 1, digests: s.digests.clone(), runtimes: Vec::new() }
        };

        // mesh2.node-admin.2, mesh2's next mesh-primary, reads the fabric-primary at its start (its mesh
        // holds no mesh-primary): it holds the node deleted.
        assert!(matches!(m2_m_channels.take_read_chunk(read(&served, 2_000), "mesh1"), Taken::Installed(_)), "its start read installs mesh2");
        assert!(m2_m.digest_view.is_deleted(id.as_str()), "its start read holds mesh2.node-admin.1 deleted");

        // It takes the role: its first aggregate continues mesh2's version (Q391) and lists the node
        // deleted; the fabric-primary installs it from the backbone.
        m2_m_channels.set_publisher(&rdm_core::PublisherId { incarnation: m2_inc.clone(), node_id: m2_id.clone() });
        m2_m_channels.join(ChannelId::backbone(m2_m.fabric_id()), vec![fp_addr.clone()]).await.unwrap();
        let m2_bb = m2_m_channels.backbone().expect("the backbone is joined");
        m2_bb.set_mesh_primary(true);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while fp_bb.held_source("mesh2").is_none_or(|(_, v)| v <= 7) {
            assert!(std::time::Instant::now() < deadline, "the fabric-primary never installed the aggregate of mesh2's next mesh-primary: it holds {:?}", fp_bb.held_source("mesh2").map(|(_, v)| v));
            m2_bb.publish(&m2_m, m2_bb.own_members(&m2_m)).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // mesh2.node.1 reads its mesh-primary: it holds the node deleted.
        let from_primary = m2_m.held_topology_of(&m2_own, Some("mesh2"));
        assert_eq!(listed(&from_primary).map(|(_, status)| status), Some(NodeStatus::Deleted), "mesh2's mesh-primary serves the node deleted");
        assert!(matches!(rpc_m_channels.take_read_chunk(read(&from_primary, 3_000), "mesh2"), Taken::Installed(_)), "mesh2.node.1's read installs mesh2");
        for v in [&m2_m, &rpc_m] {
            assert!(v.digest_view.is_deleted(id.as_str()), "mesh2's nodes hold mesh2.node-admin.1 deleted");
            assert!(v.digest_view.get(id.as_str()).is_none(), "the current topology no longer lists mesh2.node-admin.1");
        }

        fp_bb.set_fabric_primary(false);
        drop(m2_bb);
        for n in [&fp, &m2, &rpc, &lost] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        std::fs::write(dir.join("peer-mesh-deletion.spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        for observer in ["mesh2.node-admin.2", "mesh2.node.1"] {
            let taken: Vec<&Value> = spans
                .iter()
                .filter(|s| s["name"] == "rdm.mesh.topology.remove.via-node-deleted" && attr(s, "observer") == observer && attr(s, "node_id") == id.as_str() && attr(s, "incarnation_id") == inc.as_str())
                .collect();
            assert_eq!(taken.len(), 1, "{observer} takes the deletion of the exact birth once: {taken:#?}");
            println!("peer-mesh-deletion: {observer} took node.delete:mesh2.node-admin.1 trace {} span {}", taken[0]["trace_id"], taken[0]["span_id"]);
        }
        let installed: Vec<&Value> = spans
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.topology.update.via-aggregate-installed" && attr(s, "node") == "mesh1.node-admin.1" && attr(s, "channel") == "backbone" && attr(s, "mesh") == "mesh2" && attr(s, "topology_version").parse::<u64>().is_ok_and(|v| v > 7))
            .collect();
        assert!(installed.first().is_some_and(|s| attr(s, "deleted") == "1"), "the next mesh-primary's first aggregate lists the node deleted: {installed:#?}");
        println!("peer-mesh-deletion: mesh1.node-admin.1 installed mesh2's next aggregate trace {} span {}", installed[0]["trace_id"], installed[0]["span_id"]);
    });
}
