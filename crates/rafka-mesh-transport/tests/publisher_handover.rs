//! i143.e12.s52 (rafka-v2 #3082), R-HB1 part 2, §A.3: the handover of a mesh's projection between
//! publishers, over real gossip.
//!
//! The design (rafka-v2 docs/plans/proposal/node-status-heartbeat-connections.md §A.3): "Author
//! ordering reuses publisher epochs and the LeadershipView. A new source primary begins a new epoch;
//! a receiver installs a full of that epoch before its deltas; a forwarder that takes the seat
//! publishes a full first. The one new contract is the handover step: the successor acquires the
//! held member statuses and publishes a complete projection under its own epoch before any delta,
//! and a delayed predecessor publication cannot switch receivers back." gossip.md §3.3: "the source
//! publisher's exact identity is its epoch".

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{learn_addresses, Backbone, Membership};
use rafka_mesh_transport::snapshot::PublisherId;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn evidence(cell: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-s52").join(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    dir
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
    spans.sort_by_key(|s| s["start_unix_nano"].as_u64().unwrap_or(0));
    spans
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
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
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(),
    }
}

fn seat_of(d: &MeshDigest, epoch: u64) -> SeatHolder {
    SeatHolder { mesh: d.node.name.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), epoch }
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

/// Wait, bounded, for `what` to hold: gossip delivers on its own time.
async fn until(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !holds() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// CONTRACT (§A.3; gossip.md §3.3): through real gossip on mesh1's channel, the ordinary member
/// mesh1.rpc.1 holds mesh1's projection from mesh1.admin.1, its seated primary. The seat moves to
/// mesh1.admin.2 (epoch 2, announced on the channel). mesh1.admin.2's first publication into the
/// mesh is a complete projection under its own epoch (a full, `full-due`), before any delta of its
/// own; the member installs it, then its delta. mesh1.admin.1, its publication delayed (it still
/// publishes as a primary would: a full, then a delta), is refused by name at the member, naming the
/// seat holder; the member never holds mesh1.admin.1's epoch again and no source is desynchronized.
/// Must NOT happen: the member switched back to the predecessor's epoch, a delta of the successor
/// before its full, or a predecessor's delta read as a version gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_successor_publishes_a_full_first_and_a_delayed_predecessor_publication_is_refused_by_name() {
    let cell = "the_successor_publishes_a_full_first_and_a_delayed_predecessor_publication_is_refused_by_name";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let dir = evidence(cell);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-s52-handover");

    let (n1, n2, nm) = (node().await, node().await, node().await);
    let a1 = addr_of(&n1.ep);
    for n in [&n2, &nm] {
        learn_addresses(&n.ep, &[a1.clone()]).unwrap();
    }
    let mesh_id = MeshId::mint();
    let clock = rafka_mesh_transport::clock::os_clock;
    let (p1_d, p2_d, rpc1, rpc2) = (digest("mesh1.admin.1", 41_001), digest("mesh1.admin.2", 41_002), digest("mesh1.rpc.1", 41_011), digest("mesh1.rpc.2", 41_012));
    let m1 = Membership::join(&n1.gossip, &n1.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![]).await.unwrap();
    let m2 = Membership::join(&n2.gossip, &n2.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.2", clock(), vec![a1.clone()]).await.unwrap();
    let m = Membership::join(&nm.gossip, &nm.ep, &fabric(), "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![a1.clone()]).await.unwrap();
    let bb1 = Backbone::join(&n1.gossip, &n1.ep, &m1, "mesh1", "mesh1.admin.1", p1_d.node.incarnation.clone(), vec![]).await.unwrap();
    let bb2 = Backbone::join(&n2.gossip, &n2.ep, &m2, "mesh1", "mesh1.admin.2", p2_d.node.incarnation.clone(), vec![a1.clone()]).await.unwrap();
    m2.wait_for_neighbour().await;
    m.wait_for_neighbour().await;
    let (pub1, pub2) = (PublisherId { node: "mesh1.admin.1".into(), incarnation: p1_d.node.incarnation.clone() }, PublisherId { node: "mesh1.admin.2".into(), incarnation: p2_d.node.incarnation.clone() });
    let held_at_member = || m.held_source_version("mesh1");
    for book in [&m1.book, &m2.book] {
        for d in [&p1_d, &p2_d, &rpc1] {
            book.record(d.clone());
        }
    }

    // mesh1.admin.1 is seated and publishes: the member holds its projection.
    bb1.announce_seat(Seat::MeshPrimary, seat_of(&p1_d, 1)).await;
    until("the member to hold mesh1.admin.1's seat", || m.book.seats.mesh("mesh1") == Some(seat_of(&p1_d, 1))).await;
    bb1.set_mesh_primary(true);
    bb1.publish(&m1, bb1.own_members(&m1)).await;
    until("the member to hold mesh1.admin.1's projection", || held_at_member().is_some_and(|(p, _)| p == pub1)).await;

    // The seat moves to mesh1.admin.2: its first publication is a full under its own epoch, then a delta.
    bb2.announce_seat(Seat::MeshPrimary, seat_of(&p2_d, 2)).await;
    until("the member to hold mesh1.admin.2's seat", || m.book.seats.mesh("mesh1") == Some(seat_of(&p2_d, 2))).await;
    bb2.set_mesh_primary(true);
    bb2.publish(&m2, bb2.own_members(&m2)).await;
    until("the member to install the successor's full", || held_at_member().is_some_and(|(p, _)| p == pub2)).await;
    let (_, v1) = held_at_member().unwrap();
    m2.book.record(rpc2.clone());
    bb2.publish(&m2, bb2.own_members(&m2)).await;
    until("the member to apply the successor's delta", || held_at_member().is_some_and(|(p, v)| p == pub2 && v > v1)).await;
    let (_, v2) = held_at_member().unwrap();

    // mesh1.admin.1's delayed publications: a full (it publishes as a primary that just took the seat
    // would), then a delta of a change it holds. The member keeps the successor's epoch throughout.
    for round in 0..5u16 {
        bb1.set_mesh_primary(false);
        bb1.set_mesh_primary(true);
        bb1.publish(&m1, bb1.own_members(&m1)).await;
        m1.book.record(digest(&format!("mesh1.rpc.{}", 20 + round), 41_020 + round));
        bb1.publish(&m1, bb1.own_members(&m1)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(held_at_member(), Some((pub2.clone(), v2)), "round {round}: the member keeps the successor's epoch");
    }
    assert!(m.desynced_sources().is_empty(), "a predecessor's delta is refused, not a version gap: {:?}", m.desynced_sources());

    drop((bb1, bb2));
    for n in [&n1, &n2, &nm] {
        n.ep.close().await;
    }
    drop(telemetry);
    let spans = spans_of(&dir);
    let at = |name: &str, node: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && attr(s, "node") == node).collect() };
    // The successor: a full of its own mesh under its epoch before any delta of its own.
    let fulls = at("rdm.mesh.membership.update.via-forwarded-full", "mesh1.admin.2");
    let deltas: Vec<&Value> = at("rdm.mesh.membership.update.via-forwarded-delta", "mesh1.admin.2").into_iter().filter(|s| attr(s, "source_publisher") == pub2.to_string()).collect();
    assert!(!fulls.is_empty() && !deltas.is_empty(), "the successor published a full and a delta: {fulls:#?} {deltas:#?}");
    assert_eq!(attr(fulls[0], "reason"), "full-due", "the successor's first publication into the mesh is the full owed on taking the seat");
    assert!(fulls[0]["start_unix_nano"].as_u64() < deltas[0]["start_unix_nano"].as_u64(), "its full comes before any delta of its own");
    assert_eq!((attr(deltas[0], "base_version"), attr(deltas[0], "topology_version")), (v1.to_string(), v2.to_string()));
    // The member: the successor's full installed as a new epoch.
    let installed: Vec<&Value> = at("rdm.mesh.membership.update.via-snapshot-installed", "mesh1.rpc.1").into_iter().filter(|s| attr(s, "publisher") == pub2.to_string()).collect();
    assert!(installed.first().is_some_and(|s| attr(s, "new_epoch") == "true"), "{installed:#?}");
    // The member: each delayed predecessor publication refused by name, naming the seat holder.
    let refused = at("rdm.mesh.membership.reject.via-predecessor-publisher", "mesh1.rpc.1");
    let kinds: std::collections::BTreeSet<String> = refused.iter().map(|s| attr(s, "frame")).collect();
    assert_eq!(kinds, ["members".to_string(), "members-delta".to_string()].into_iter().collect(), "the predecessor's full and delta are each refused: {refused:#?}");
    assert!(
        refused.iter().all(|s| attr(s, "publisher") == pub1.to_string() && attr(s, "held_publisher") == pub2.to_string() && attr(s, "reason") == "not-seated" && attr(s, "seat_holder") == seat_of(&p2_d, 2).to_string() && attr(s, "channel") == "mesh-channel"),
        "{refused:#?}"
    );
    let mut lines = String::new();
    for (what, s) in fulls.iter().take(1).map(|s| ("successor-full", *s)).chain(deltas.iter().take(1).map(|s| ("successor-delta", *s))).chain(installed.iter().take(1).map(|s| ("member-installed", *s))).chain(refused.iter().map(|s| ("refused", *s))) {
        lines.push_str(&format!("{what} {} {} frame={} reason={} topology_version={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "frame"), attr(s, "reason"), attr(s, "topology_version")));
    }
    std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
}
