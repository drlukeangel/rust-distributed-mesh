//! A node's own digest tells how its mesh channel is doing.
//!
//! CONTRACT: the digest a node publishes carries the members it holds as heard, its ACTIVE gossip
//! neighbours on the mesh channel (never the seeded peer list), and the topology frames it sent
//! and decoded since it started. A node whose neighbours all went away publishes `neighbours = 0`
//! while still holding the members it heard; when delivery returns it publishes neighbours again.
//! What must NOT happen: a neighbour count read from the seeded peer list (it would stay 1 through
//! the isolation), or counters that do not move when frames flow.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, GossipStats, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::topology::TopologyView;
use std::time::{Duration, Instant};

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
            runtime: None,
        },
        status: NodeStatus::Ready,
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
    router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, router, gossip }
}

fn addr_of(n: &Node) -> iroh::EndpointAddr {
    iroh::EndpointAddr::new(n.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], n.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())))
}

/// Publish `m` repeatedly until its own sampled stats satisfy `ok`; the last sampled stats.
async fn published_until(m: &rdm_gossip::channel::Channels, own: &MeshDigest, ok: impl Fn(&GossipStats) -> bool) -> GossipStats {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        m.publish(own).await.unwrap();
        let mine = m.topology_view().digest_view.current(m.topology_view().digest_view.staleness_floor()).into_iter().find(|d| d.node.node_id == own.node.node_id).expect("the node holds its own digest");
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
    if crate::own_process::delegated_with_env(module_path!(), "node_digest_counts_active_neighbours_through_isolation_and_return", &[("RDM_STALENESS_MS", Some(std::ffi::OsStr::new("60000")))]) {
        return;
    }
    let (a, b) = (node().await, node().await);
    let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
    let clock = rdm_transport::clock::os_clock;
    let ma = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh_id, "mesh1.node.1", clock(), Default::default());
    let ma_channels = rdm_gossip::channel::Channels::new(&a.gossip, &a.ep, &ma).unwrap();
    ma_channels.join(rdm_gossip::channel::ChannelId::mesh(ma.fabric_id(), ma.mesh_id()), vec![]).await.unwrap();
    let mb = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh_id, "mesh1.node.2", clock(), Default::default());
    let mb_channels = rdm_gossip::channel::Channels::new(&b.gossip, &b.ep, &mb).unwrap();
    mb_channels.join(rdm_gossip::channel::ChannelId::mesh(mb.fabric_id(), mb.mesh_id()), vec![addr_of(&a)]).await.unwrap();
    let (da, db) = (digest(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &a.ep), digest(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", &b.ep));

    // Joined: A has a neighbour, has heard B, and frames flowed both ways.
    let pb = mb_channels.publish_every(Duration::from_millis(200), move || db.clone());
    let joined = published_until(&ma_channels, &da, |g| g.neighbours == 1 && g.heard >= 2 && g.frames_received >= 1).await;
    assert!(joined.frames_sent >= 1, "{joined:?}");

    // B's transport goes away: A's mesh channel has no neighbour, yet A still holds B as heard.
    pb.abort();
    b.router.shutdown().await.unwrap();
    drop(mb);
    let isolated = published_until(&ma_channels, &da, |g| g.neighbours == 0).await;
    assert!(isolated.heard >= 1, "A still holds the members it heard: {isolated:?}");
    assert!(isolated.frames_sent > joined.frames_sent, "A kept publishing: {isolated:?} after {joined:?}");

    // Delivery returns: a new node reaches A and A counts it as a neighbour again.
    let c = node().await;
    let mc = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh_id, "mesh1.node.3", clock(), Default::default());
    let mc_channels = rdm_gossip::channel::Channels::new(&c.gossip, &c.ep, &mc).unwrap();
    mc_channels.join(rdm_gossip::channel::ChannelId::mesh(mc.fabric_id(), mc.mesh_id()), vec![addr_of(&a)]).await.unwrap();
    let dc = digest(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", &c.ep);
    let pc = mc_channels.publish_every(Duration::from_millis(200), move || dc.clone());
    let returned = published_until(&ma_channels, &da, |g| g.neighbours >= 1 && g.frames_received > isolated.frames_received).await;
    assert!(returned.heard >= 1, "{returned:?}");
    pc.abort();
}
