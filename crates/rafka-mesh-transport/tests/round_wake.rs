//! i143.e12.s40 functional: a mesh primary publishes its Mesh's aggregate "on its anti-entropy timer
//! and at once on a material change" (gossip.md, the `Members` row). A stop overlay that parks a
//! member is such a change ("an in_flight overlay added or cleared" bumps `topology_version`), so the
//! round that publishes it is woken by the change itself, not by the next anti-entropy tick.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{learn_addresses, Backbone, Membership, StatusEvent};
use serde_json::Value;
use std::time::Duration;

const CELL: &str = "a_member_stop_overlay_between_two_rounds_reaches_the_backbone_at_once";

/// The anti-entropy interval the round runs on in this cell: far longer than the bound below, so a
/// publication inside the bound can only be the material change's own.
const ANTI_ENTROPY: Duration = Duration::from_secs(30);

/// How long a peer may wait for the overlay: a publication woken by the change, plus gossip.
const AT_ONCE: Duration = Duration::from_secs(5);

fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mine = format!(".{}-", std::process::id());
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl") && n.to_string_lossy().contains(&mine)) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
}

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn member(mesh: &str, ordinal: u32, node_id: &NodeId, birth: &IncarnationId) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: format!("{mesh}.rpc.{ordinal}").parse().unwrap(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 42_000 + ordinal).parse().unwrap(),
            incarnation: birth.clone(),
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
        config: Default::default(),
        load: None,
        gossip: None,
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

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

/// CONTRACT: the source mesh primary runs its publication round as the node-admin does (publish,
/// then wait for the round's wake with a 30 s anti-entropy deadline). After a round has published,
/// its mesh channel takes a member's `NodeStopped`: the overlay parks the member in the primary's
/// book, the normalized projection moves, and the woken round publishes it on the backbone at once.
/// A peer mesh's admin holds the member parked (its stop overlay open, `via-member-state
/// parked=true`) within 5 s, long before the next anti-entropy round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_stop_overlay_between_two_rounds_reaches_the_backbone_at_once() {
    if crate::own_process::delegated(module_path!(), CELL) {
        return;
    }
    let dir = crate::evidence_dir();
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-round-wake");

    let (src_node, peer_node) = (node().await, node().await);
    let src_addr = addr_of(&src_node.ep);
    learn_addresses(&peer_node.ep, &[src_addr.clone()]).unwrap();
    let fab = fabric();
    let src_m = Membership::join(&src_node.gossip, &src_node.ep, &fab, "mesh2", &MeshId::mint(), "mesh2.admin.1", rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let peer_m = Membership::join(&peer_node.gossip, &peer_node.ep, &fab, "mesh1", &MeshId::mint(), "mesh1.admin.1", rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let src_bb = Backbone::join(&src_node.gossip, &src_node.ep, &src_m, "mesh2", "mesh2.admin.1", IncarnationId::mint(), vec![]).await.unwrap();
    let peer_bb = Backbone::join(&peer_node.gossip, &peer_node.ep, &peer_m, "mesh1", "mesh1.admin.1", IncarnationId::mint(), vec![src_addr]).await.unwrap();
    src_bb.set_mesh_primary(true);
    peer_bb.set_mesh_primary(true);

    // Two members of the source Mesh, heard by its primary.
    let births: Vec<(NodeId, IncarnationId)> = (0..2).map(|_| (NodeId::mint(), IncarnationId::mint())).collect();
    let digests: Vec<MeshDigest> = births.iter().enumerate().map(|(k, (id, b))| member("mesh2", k as u32 + 1, id, b)).collect();
    for d in &digests {
        assert!(src_m.book.record(d.clone()));
    }

    // The peer holds the source's first projection: rounds published by hand until the backbone
    // carries it (the neighbour forms after the first broadcasts).
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let v1 = loop {
        src_bb.publish(&src_m, src_bb.own_members(&src_m)).await;
        if let Some((_, v)) = peer_bb.held_source("mesh2") {
            break v;
        }
        assert!(std::time::Instant::now() < deadline, "the peer never held the source's first projection");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // The source primary's round, as the node-admin runs it: publish, then wait for the wake.
    let rounds = {
        let (bb, m) = (src_bb.clone(), src_m.clone());
        tokio::spawn(async move {
            loop {
                bb.publish(&m, bb.own_members(&m)).await;
                bb.round_wake(&m, tokio::time::Instant::now() + ANTI_ENTROPY).await;
            }
        })
    };
    // Let the first round publish and settle into its wait.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Between two rounds: the member's stop, as the mesh primary takes its own NodeStopped.
    let stopped = &digests[0];
    let op = LifecycleOp {
        build_id: "b-s40".into(),
        attempt: 1,
        operation: format!("stop-node:{}", stopped.node.name),
        node_id: stopped.node.node_id.clone(),
        incarnation: stopped.node.incarnation.clone(),
        name: stopped.node.name.clone(),
        event_at_rafka_ms: 2_000,
    };
    let stopped_at = std::time::Instant::now();
    src_bb.publish_status_event(StatusEvent::Stopped, &op).await;

    let held = loop {
        let parked = peer_m.book.in_flight().iter().any(|o| o.key() == op.key());
        if parked {
            break peer_bb.held_source("mesh2").map(|(_, v)| v);
        }
        assert!(
            stopped_at.elapsed() < AT_ONCE,
            "the peer mesh did not hold {} parked within {AT_ONCE:?} of its NodeStopped: the source primary's round was not woken by its own projection's move (held source version {:?}, first {v1})",
            op.name,
            peer_bb.held_source("mesh2").map(|(_, v)| v)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let took = stopped_at.elapsed();
    assert!(held.is_some_and(|v| v > v1), "the overlay arrived in a newer version of the source's projection: {held:?} after {v1}");

    rounds.abort();
    drop(src_bb);
    drop(peer_bb);
    for n in [&src_node, &peer_node] {
        n.ep.close().await;
    }
    drop(telemetry);
    let spans = collect_spans(&dir);
    let names = |n: &str| spans.iter().filter(|s| s["name"] == n).count();
    let parked_at_peer = spans
        .iter()
        .filter(|s| s["name"] == "rdm.mesh.membership.update.via-member-state" && s["attributes"]["member"].as_str() == Some(op.name.to_string().as_str()) && (s["attributes"]["parked"] == Value::Bool(true) || s["attributes"]["parked"] == "true"))
        .count();
    println!("{CELL}: overlay held at the peer {took:?} after NodeStopped; via-topology-version spans {}; via-member-state parked {}", names("rdm.mesh.backbone.update.via-topology-version"), parked_at_peer);
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    assert!(parked_at_peer >= 1, "the peer spans the member parked (via-member-state parked=true)");
}
