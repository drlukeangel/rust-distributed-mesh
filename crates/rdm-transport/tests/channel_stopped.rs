//! A node that stops leaves its mesh topic and a node that starts joins it again.
//!
//! CONTRACT: stopping a topology view drops its gossip sender and receiver, so no one counts it as a
//! neighbour and a publish on it is refused by name; nothing it would have said reaches the others.
//! Starting joins the topic again through the peers it knows, its neighbours return, and its digest
//! sequence continues from where it stopped. What must NOT happen: a stopped node still counted as a
//! neighbour, a frame leaving a stopped node, or an start that leaves the node deaf.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::TopologyView;
use std::time::{Duration, Instant};

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
        status: MemberStatus::ReadyForTraffic,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(), is_primary: None, is_fabric_primary: None
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

fn addr_of(n: &Node) -> iroh::EndpointAddr {
    iroh::EndpointAddr::new(n.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], n.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port())))
}

async fn until(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ok() {
        assert!(Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_topology_view_is_no_neighbour_and_sends_nothing_until_it_starts() {
    if crate::own_process::delegated_with_env(module_path!(), "a_stopped_topology_view_is_no_neighbour_and_sends_nothing_until_it_starts", &[("RDM_STALENESS_MS", Some(std::ffi::OsStr::new("60000")))]) {
        return;
    }
    let (a, b) = (node().await, node().await);
    let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
    let clock = rdm_transport::clock::os_clock;
    let ma = TopologyView::join(&a.gossip, &a.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![], &[]).await.unwrap();
    let mb = TopologyView::join(&b.gossip, &b.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.2", clock(), vec![addr_of(&a)], &[]).await.unwrap();
    let db = digest("mesh1.rpc.2", &b.ep);

    // Joined: each holds the other as a neighbour and A hears B's digest.
    until("A and B are neighbours", || ma.neighbours() == 1 && mb.neighbours() == 1).await;
    mb.publish(&db).await.unwrap();
    until("A hears B", || ma.digest_view.get(db.node.node_id.as_str()).is_some()).await;
    let seq_before = ma.digest_view.get(db.node.node_id.as_str()).unwrap().0.digest_seq;

    // B stops: A no longer counts it a neighbour, and B refuses to send, by name.
    mb.stop().await;
    until("A has lost its neighbour", || ma.neighbours() == 0).await;
    assert_eq!(mb.neighbours(), 0, "a stopped node holds no neighbour");
    let refused = mb.publish(&db).await.expect_err("a publish on a stopped channel is refused");
    assert!(refused.to_string().contains("stopped"), "the refusal names why: {refused}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(ma.digest_view.get(db.node.node_id.as_str()).unwrap().0.digest_seq, seq_before, "nothing B would have said reached A while it was stopped");

    // B starts: the topic is joined again, the neighbours return and B's digest sequence continued.
    mb.start().await;
    until("A and B are neighbours again", || ma.neighbours() == 1 && mb.neighbours() == 1).await;
    mb.publish(&db).await.unwrap();
    until("A hears B again", || ma.digest_view.get(db.node.node_id.as_str()).is_some_and(|(d, _)| d.digest_seq > seq_before)).await;
}
