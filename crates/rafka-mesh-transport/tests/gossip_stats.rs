//! A node's own digest tells how its mesh channel is doing.
//!
//! CONTRACT: the digest a node publishes carries the members it holds as heard, its ACTIVE gossip
//! neighbours on the mesh channel (never the seeded peer list), and the membership frames it sent
//! and decoded since it started. A node whose neighbours all went away publishes `neighbours = 0`
//! while still holding the members it heard; when delivery returns it publishes neighbours again.
//! What must NOT happen: a neighbour count read from the seeded peer list (it would stay 1 through
//! the isolation), or counters that do not move when frames flow.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, GossipStats, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::Membership;
use std::time::{Duration, Instant};

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
        gossip: None,
        extra: Default::default(),
    }
}

struct Node {
    ep: iroh::Endpoint,
    router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, router, gossip }
}

fn addr_of(n: &Node) -> iroh::EndpointAddr {
    iroh::EndpointAddr::new(n.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], n.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())))
}

/// Publish `m` repeatedly until its own sampled stats satisfy `ok`; the last sampled stats.
async fn published_until(m: &Membership, own: &MeshDigest, ok: impl Fn(&GossipStats) -> bool) -> GossipStats {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        m.publish(own).await.unwrap();
        let mine = m.book.current(m.book.staleness_floor()).into_iter().find(|d| d.node.node_id == own.node.node_id).expect("the node holds its own digest");
        let g = mine.gossip.expect("a published digest carries gossip stats");
        if ok(&g) {
            return g;
        }
        assert!(Instant::now() < deadline, "stats never satisfied the condition; last {g:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_digest_counts_active_neighbours_through_isolation_and_return() {
    if crate::own_process::delegated(module_path!(), "node_digest_counts_active_neighbours_through_isolation_and_return") {
        return;
    }
    std::env::set_var("RDM_STALENESS_MS", "60000");
    let (a, b) = (node().await, node().await);
    let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
    let clock = rafka_mesh_transport::clock::os_clock;
    let ma = Membership::join(&a.gossip, &a.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![]).await.unwrap();
    let mb = Membership::join(&b.gossip, &b.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.2", clock(), vec![addr_of(&a)]).await.unwrap();
    let (da, db) = (digest("mesh1.rpc.1", &a.ep), digest("mesh1.rpc.2", &b.ep));

    // Joined: A has a neighbour, has heard B, and frames flowed both ways.
    let pb = mb.publish_every(Duration::from_millis(200), move || db.clone());
    let joined = published_until(&ma, &da, |g| g.neighbours == 1 && g.heard >= 2 && g.frames_received >= 1).await;
    assert!(joined.frames_sent >= 1, "{joined:?}");

    // B's transport goes away: A's mesh channel has no neighbour, yet A still holds B as heard.
    pb.abort();
    b.router.shutdown().await.unwrap();
    drop(mb);
    let isolated = published_until(&ma, &da, |g| g.neighbours == 0).await;
    assert!(isolated.heard >= 1, "A still holds the members it heard: {isolated:?}");
    assert!(isolated.frames_sent > joined.frames_sent, "A kept publishing: {isolated:?} after {joined:?}");

    // Delivery returns: a new node reaches A and A counts it as a neighbour again.
    let c = node().await;
    let mc = Membership::join(&c.gossip, &c.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.3", clock(), vec![addr_of(&a)]).await.unwrap();
    let dc = digest("mesh1.rpc.3", &c.ep);
    let pc = mc.publish_every(Duration::from_millis(200), move || dc.clone());
    let returned = published_until(&ma, &da, |g| g.neighbours >= 1 && g.frames_received > isolated.frames_received).await;
    assert!(returned.heard >= 1, "{returned:?}");
    pc.abort();
}
