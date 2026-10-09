//! A node's heartbeat and a member going stale are readable from spans.
//!
//! CONTRACT: every node's publish loop marks its heartbeat in the trace once per
//! `HEARTBEAT_EVERY` (not once per digest), as a root span carrying the peer count its book holds
//! and its clock reading beside the host's wall clock; and a member the book still holds but no
//! longer hears within the staleness floor is named by one span on every node that held it heard.
//! What must NOT happen: a heartbeat span per 500 ms digest, a peer count that includes the node
//! itself, or a silent member that leaves the view with no span.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::Membership;
use serde_json::Value;
use std::time::Duration;

fn digest(name: &str, ep: &iroh::Endpoint) -> MeshDigest {
    let port = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    MeshDigest {
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
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        extra: Default::default(),
    }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

fn num(s: &Value, k: &str) -> u64 {
    s["attributes"][k].as_str().and_then(|v| v.parse().ok()).unwrap_or(0)
}

fn spans_of(dir: &std::path::Path) -> Vec<Value> {
    let mine = format!(".{}-", std::process::id());
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with(".spans.jsonl") && n.contains(&mine) {
            spans.extend(std::fs::read_to_string(e.path()).unwrap().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    spans
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_publish_loop_marks_heartbeat_and_stale_member_in_spans() {
    if crate::own_process::delegated(module_path!(), "node_publish_loop_marks_heartbeat_and_stale_member_in_spans") {
        return;
    }
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/restore-lost/heartbeat");
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    std::env::set_var("RDM_STALENESS_MS", "2000");
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-heartbeat");

    let (a, b) = (node().await, node().await);
    rafka_mesh_transport::membership::learn_addresses(&b.ep, &[iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).map(|s| std::net::SocketAddr::from(([127, 0, 0, 1], s.port()))).unwrap())]).unwrap();
    let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
    let ma = Membership::join(&a.gossip, &a.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.1", rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let seed = iroh::EndpointAddr::new(a.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())));
    let mb = Membership::join(&b.gossip, &b.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.2", rafka_mesh_transport::clock::os_clock(), vec![seed]).await.unwrap();
    let (da, db) = (digest("mesh1.rpc.1", &a.ep), digest("mesh1.rpc.2", &b.ep));
    let pa = ma.publish_every(Duration::from_millis(500), move || da.clone());
    let pb = mb.publish_every(Duration::from_millis(500), move || db.clone());

    // Past one heartbeat period, so each loop has marked twice; then B falls silent past the floor.
    tokio::time::sleep(rafka_mesh_transport::membership::HEARTBEAT_EVERY + Duration::from_millis(1500)).await;
    pb.abort();
    tokio::time::sleep(Duration::from_millis(3500)).await;
    pa.abort();
    drop(telemetry);

    let spans = spans_of(&dir);
    let beats: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.node.update.via-heartbeat" && s["attributes"]["node"] == "mesh1.rpc.1").collect();
    // ~6.5 s of 500 ms digests is 13 publishes: the heartbeat is the 5 s cadence, not the digest cadence.
    assert_eq!(beats.len(), 2, "heartbeat spans of mesh1.rpc.1: {beats:#?}");
    let last = beats.iter().max_by_key(|s| num(s, "digest_seq")).unwrap();
    assert_eq!(num(last, "peer_count"), 1, "the heartbeat counts the other member, not itself: {last}");
    assert!(num(last, "wall_time_ms") > 0 && last["attributes"].get("clock_skew_ms").is_some(), "{last}");
    assert!(last["parent_span_id"] == "", "a heartbeat is a root span: {last}");
    let stale: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-member-stale" && s["attributes"]["node"] == "mesh1.rpc.1").collect();
    assert_eq!(stale.len(), 1, "one span names the member that fell silent: {stale:#?}");
    assert_eq!(stale[0]["attributes"]["member"], "mesh1.rpc.2");
}
