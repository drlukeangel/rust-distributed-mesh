//! A node's heartbeat and a member going stale are readable from spans.
//!
//! CONTRACT: every node's publish loop marks its heartbeat in the trace once per
//! `HEARTBEAT_EVERY` (not once per digest), as a root span carrying the peer count its view holds
//! and its clock reading beside the host's wall clock; and a member the view still holds but no
//! longer hears within the staleness floor is named by one span on every node that held it heard.
//! What must NOT happen: a heartbeat span per 500 ms digest, a peer count that includes the node
//! itself, or a silent member that leaves the view with no span.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::topology::TopologyView;
use serde_json::Value;
use std::time::Duration;

fn digest(name: rdm_core::PathName, mesh: &str, r#type: &str, ep: &iroh::Endpoint) -> MeshDigest {
    let port = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    MeshDigest { 
        fabric_id: FabricId::parse("fab000000001").unwrap(),
        node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
            node_id: NodeId::mint(),
            path: name.path.clone(), name: name.name.clone(),
            endpoint_id: EndpointId(ep.id().to_string()),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        },
        status: NodeStatus::Ready,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
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
        rdm_gossip::location::learn_addresses(&b.ep, &[iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).map(|s| std::net::SocketAddr::from(([127, 0, 0, 1], s.port()))).unwrap())]).unwrap();
        let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
        // Each node joins under its node id, as every node does; its digest names its (path, name).
        let (da, db) = (digest(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &a.ep), digest(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", &b.ep));
        let (a_id, b_id) = (da.node.node_id.clone(), db.node.node_id.clone());
        let ma = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh_id, a_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let ma_channels = rdm_gossip::channel::Channels::new(&a.gossip, &a.ep, &ma).unwrap();
        ma_channels.join(rdm_gossip::channel::ChannelId::mesh(ma.fabric_id(), ma.mesh_id()), vec![]).await.unwrap();
        let seed = iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())));
        let mb = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh_id, b_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let mb_channels = rdm_gossip::channel::Channels::new(&b.gossip, &b.ep, &mb).unwrap();
        mb_channels.join(rdm_gossip::channel::ChannelId::mesh(mb.fabric_id(), mb.mesh_id()), vec![seed]).await.unwrap();
        let started = std::time::Instant::now();
        let pa = ma_channels.publish_every(Duration::from_millis(500), move || da.clone());
        let pb = mb_channels.publish_every(Duration::from_millis(500), move || db.clone());

        // Past one heartbeat period, so each loop has marked twice; then B falls silent past the floor.
        tokio::time::sleep(rdm_gossip::topology::HEARTBEAT_EVERY + Duration::from_millis(1500)).await;
        pb.abort();
        tokio::time::sleep(Duration::from_millis(3500)).await;
        pa.abort();
        // The heartbeat marks at the loop's start and every HEARTBEAT_EVERY after: as many as the window
        // the loop ran holds, read from the clock, never from the sleeps (a loaded host stretches them).
        let ran = started.elapsed();

        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let beats: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.node.update.via-heartbeat" && s["attributes"]["node_path"] == "mesh1.node" && s["attributes"]["node_name"] == "1").collect();
        // ~6.5 s of 500 ms digests is 13 publishes: the heartbeat is the 5 s cadence, not the digest cadence.
        let due = (ran.as_millis() / rdm_gossip::topology::HEARTBEAT_EVERY.as_millis()) as usize + 1;
        assert!(beats.len() == due || beats.len() + 1 == due, "heartbeat spans of mesh1.node.1 over {ran:?} ({due} due at most, one fewer when the last mark falls at the window's edge): {beats:#?}");
        let last = beats.iter().max_by_key(|s| num(s, "digest_seq")).unwrap();
        assert_eq!(num(last, "peer_count"), 1, "the heartbeat counts the other member, not itself: {last}");
        assert!(num(last, "wall_time_ms") > 0 && last["attributes"].get("clock_skew_ms").is_some(), "{last}");
        assert!(last["parent_span_id"] == "", "a heartbeat is a root span: {last}");
        let stale: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-member-stale" && s["attributes"]["node"] == a_id.as_str()).collect();
        assert_eq!(stale.len(), 1, "one span names the member that fell silent: {stale:#?}");
        assert_eq!(stale[0]["attributes"]["member_id"], b_id.as_str());
    });
}

/// CONTRACT (node-mesh-leave.md: "the node publishes nothing in that state and nothing after it",
/// R-ON1; node-stop.md step 4: "The node gossips nothing for this step"): a node whose status moves to
/// Leaving stops publishing its Digest. Its peer holds it at the last status it published, Ready, and
/// never hears it Leaving. Must NOT happen: the publish loop sending the Leaving Digest until the cut.
#[test]
fn a_node_leaving_publishes_no_digest_in_that_state() {
    if crate::own_process::delegated(module_path!(), "a_node_leaving_publishes_no_digest_in_that_state") {
        return;
    }
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-leaving").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (a, b) = (node().await, node().await);
        rdm_gossip::location::learn_addresses(&b.ep, &[iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).map(|s| std::net::SocketAddr::from(([127, 0, 0, 1], s.port()))).unwrap())]).unwrap();
        let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
        let (da, db) = (digest(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &a.ep), digest(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", &b.ep));
        let (a_id, b_id) = (da.node.node_id.clone(), db.node.node_id.clone());
        let ma = TopologyView::new(&fab, "mesh1", &mesh_id, a_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let ma_channels = rdm_gossip::channel::Channels::new(&a.gossip, &a.ep, &ma).unwrap();
        ma_channels.join(rdm_gossip::channel::ChannelId::mesh(ma.fabric_id(), ma.mesh_id()), vec![]).await.unwrap();
        let seed = iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())));
        let mb = TopologyView::new(&fab, "mesh1", &mesh_id, b_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let mb_channels = rdm_gossip::channel::Channels::new(&b.gossip, &b.ep, &mb).unwrap();
        mb_channels.join(rdm_gossip::channel::ChannelId::mesh(mb.fabric_id(), mb.mesh_id()), vec![seed]).await.unwrap();
        let status = std::sync::Arc::new(std::sync::Mutex::new(NodeStatus::Ready));
        let (pa, pb) = {
            let s = status.clone();
            let db = db.clone();
            let seq = std::sync::atomic::AtomicU64::new(0);
            (ma_channels.publish_every(Duration::from_millis(200), move || da.clone()), mb_channels.publish_every(Duration::from_millis(200), move || {
                let mut d = db.clone();
                d.status = *s.lock().unwrap();
                d.digest_seq = seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                d
            }))
        };
        let heard = |want: NodeStatus| ma.digest_view.get(b_id.as_str()).is_some_and(|(d, _)| d.status == want);
        let t0 = std::time::Instant::now();
        while !heard(NodeStatus::Ready) && t0.elapsed() < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(heard(NodeStatus::Ready), "the peer hears the node Ready first");
        *status.lock().unwrap() = NodeStatus::Leaving;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        pb.abort();
        pa.abort();
        assert!(heard(NodeStatus::Ready), "the peer holds the node at its last published status, Ready, never Leaving: {:?}", ma.digest_view.get(b_id.as_str()).map(|(d, _)| d.status));
    });
}
