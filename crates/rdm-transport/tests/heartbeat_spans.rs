//! A node's heartbeat and a member going stale are readable from spans.
//!
//! CONTRACT: every node's publish loop marks its heartbeat in the trace once per
//! `HEARTBEAT_EVERY` (not once per digest), as a root span carrying the peer count its view holds
//! and its clock reading beside the host's wall clock; and a member the view still holds but no
//! longer hears within the staleness floor is named by one span on every node that held it heard.
//! What must NOT happen: a heartbeat span per 500 ms digest, a peer count that includes the node
//! itself, or a silent member that leaves the view with no span.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::TopologyView;
use serde_json::Value;
use std::time::Duration;

fn digest(name: &str, ep: &iroh::Endpoint) -> MeshDigest {
    let port = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    MeshDigest { held_hash: Default::default(),
        fabric_id: FabricId::parse("fab000000001").unwrap(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(ep.id().to_string()),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
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
        gossip: None,
        is_primary: None, is_fabric_primary: None
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

fn num(s: &Value, k: &str) -> u64 {
    s["attributes"][k].as_str().and_then(|v| v.parse().ok()).unwrap_or(0)
}

#[test]
fn node_publish_loop_marks_heartbeat_and_stale_member_in_spans() {
    if crate::own_process::delegated_with_env(module_path!(), "node_publish_loop_marks_heartbeat_and_stale_member_in_spans", &[("RDM_STALENESS_MS", Some(std::ffi::OsStr::new("2000")))]) {
        return;
    }
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-heartbeat").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (a, b) = (node().await, node().await);
        rdm_transport::topology::learn_addresses(&b.ep, &[iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).map(|s| std::net::SocketAddr::from(([127, 0, 0, 1], s.port()))).unwrap())]).unwrap();
        let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
        let ma = TopologyView::join(&a.gossip, &a.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let seed = iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())));
        let mb = TopologyView::join(&b.gossip, &b.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.2", rdm_transport::clock::os_clock(), vec![seed], &[]).await.unwrap();
        let (da, db) = (digest("mesh1.rpc.1", &a.ep), digest("mesh1.rpc.2", &b.ep));
        let started = std::time::Instant::now();
        let pa = ma.publish_every(Duration::from_millis(500), move || da.clone());
        let pb = mb.publish_every(Duration::from_millis(500), move || db.clone());

        // Past one heartbeat period, so each loop has marked twice; then B falls silent past the floor.
        tokio::time::sleep(rdm_transport::topology::HEARTBEAT_EVERY + Duration::from_millis(1500)).await;
        pb.abort();
        tokio::time::sleep(Duration::from_millis(3500)).await;
        pa.abort();
        // The heartbeat marks at the loop's start and every HEARTBEAT_EVERY after: as many as the window
        // the loop ran holds, read from the clock, never from the sleeps (a loaded host stretches them).
        let ran = started.elapsed();

        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let beats: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.node.update.via-heartbeat" && s["attributes"]["node"] == "mesh1.rpc.1").collect();
        // ~6.5 s of 500 ms digests is 13 publishes: the heartbeat is the 5 s cadence, not the digest cadence.
        let due = (ran.as_millis() / rdm_transport::topology::HEARTBEAT_EVERY.as_millis()) as usize + 1;
        assert!(beats.len() == due || beats.len() + 1 == due, "heartbeat spans of mesh1.rpc.1 over {ran:?} ({due} due at most, one fewer when the last mark falls at the window's edge): {beats:#?}");
        let last = beats.iter().max_by_key(|s| num(s, "digest_seq")).unwrap();
        assert_eq!(num(last, "peer_count"), 1, "the heartbeat counts the other member, not itself: {last}");
        assert!(num(last, "wall_time_ms") > 0 && last["attributes"].get("clock_skew_ms").is_some(), "{last}");
        assert!(last["parent_span_id"] == "", "a heartbeat is a root span: {last}");
        let stale: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-member-stale" && s["attributes"]["node"] == "mesh1.rpc.1").collect();
        assert_eq!(stale.len(), 1, "one span names the member that fell silent: {stale:#?}");
        assert_eq!(stale[0]["attributes"]["member"], "mesh1.rpc.2");
    });
}
