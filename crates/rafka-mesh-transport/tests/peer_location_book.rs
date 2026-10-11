//! A key that moved keeps its old socket in no address book of the endpoint, and no dial reaches it.
//!
//! CONTRACT (R-I1; node-rpc-envelope.md, JoinNode: "On acceptance it installs the reported address
//! for that exact key, drops pooled connections and pending work aimed at that key's old address";
//! gossip.md: "Contact changes continue through the birth's ordered digest updates"): a node-admin
//! holds a peer admin's key at socket X, on its mesh channel and on its backbone, and that key's old
//! birth is connected to it. The key re-binds (same key, new process) at socket Y and the admin
//! accepts the newer birth. From then on every address lookup the endpoint consults names the key
//! at Y alone, the endpoint holds no path to X, and a dial of the key by its id (what gossip does)
//! puts no path to X back. A second node of the same process that accepts the same newer birth
//! names the key at Y alone too. What must NOT happen: X left in a side book (the backbone's) and
//! read back when the endpoint dials the key; a second node keeping X because the first node of
//! the process consumed the newer birth.

use futures_lite::StreamExt;
use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{Backbone, Membership};
use serde_json::Value;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

struct Node {
    ep: iroh::Endpoint,
    router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node_with(key: iroh::SecretKey) -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rafka_node_rpc::endpoint::bind_exact(key, "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, router, gossip }
}

fn socket_of(n: &Node) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], n.ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port()))
}

/// The birth `incarnation` of node `node_id` at `socket`, under `key`.
fn birth(node_id: &NodeId, key: iroh::PublicKey, socket: SocketAddr, incarnation: &IncarnationId, supersedes: Option<IncarnationId>) -> MeshDigest {
    MeshDigest {
        fabric_id: FabricId::parse("fab000000001").unwrap(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: "mesh1.admin.2".parse().unwrap(),
            endpoint_id: EndpointId(key.to_string()),
            transport_addr: socket,
            incarnation: incarnation.clone(),
            supersedes,
            runtime: None,
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(),
    }
}

/// Every direct address the endpoint's address lookup services name for `key`, merged as iroh
/// merges them when it dials a key with no selected path.
async fn looked_up(ep: &iroh::Endpoint, key: iroh::PublicKey) -> BTreeSet<SocketAddr> {
    let mut items = Box::pin(ep.address_lookup().unwrap().resolve(key));
    let mut out = BTreeSet::new();
    while let Some(item) = items.next().await {
        if let Ok(Ok(item)) = item {
            out.extend(item.ip_addrs().cloned());
        }
    }
    out
}

/// Every direct address the endpoint's own state for `key` holds a path to in active use.
async fn active_paths(ep: &iroh::Endpoint, key: iroh::PublicKey) -> BTreeSet<SocketAddr> {
    let Some(info) = ep.remote_info(key).await else { return BTreeSet::new() };
    info.addrs()
        .filter(|a| matches!(a.usage(), iroh::endpoint::TransportAddrUsage::Active))
        .filter_map(|a| match a.addr() {
            iroh::TransportAddr::Ip(s) => Some(*s),
            _ => None,
        })
        .collect()
}

/// Every direct address the endpoint's own state for `key` holds a path to.
async fn paths(ep: &iroh::Endpoint, key: iroh::PublicKey) -> BTreeSet<SocketAddr> {
    let Some(info) = ep.remote_info(key).await else { return BTreeSet::new() };
    info.addrs()
        .filter_map(|a| match a.addr() {
            iroh::TransportAddr::Ip(s) => Some(*s),
            _ => None,
        })
        .collect()
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
async fn an_admin_accepting_a_moved_keys_newer_birth_names_only_the_new_socket_and_never_dials_the_old() {
    if crate::own_process::delegated(module_path!(), "an_admin_accepting_a_moved_keys_newer_birth_names_only_the_new_socket_and_never_dials_the_old") {
        return;
    }
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/restore-lost/peer-location-book");
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    std::env::set_var("RDM_STALENESS_MS", "60000");
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-peer-location-book");

    let (fab, mesh_id) = (FabricId::parse("fab000000001").unwrap(), MeshId::mint());
    let clock = rafka_mesh_transport::clock::os_clock;
    let (admin, member) = (node_with(iroh::SecretKey::generate()).await, node_with(iroh::SecretKey::generate()).await);
    let m_admin = Membership::join(&admin.gossip, &admin.ep, &fab, "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![]).await.unwrap();
    let backbone = Backbone::join(&admin.gossip, &admin.ep, &m_admin, "mesh1", "mesh1.admin.1", IncarnationId::mint(), vec![]).await.unwrap();
    let m_member = Membership::join(&member.gossip, &member.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![]).await.unwrap();

    // The peer admin's first birth at X: known to the admin's mesh channel and its backbone, known
    // to the member, and connected to the admin.
    let key = iroh::SecretKey::generate();
    let peer = key.public();
    let old = node_with(key.clone()).await;
    let x = socket_of(&old);
    let node_id = NodeId::mint();
    let first_birth = IncarnationId::mint();
    let first = birth(&node_id, peer, x, &first_birth, None);
    m_admin.learn(first.clone(), "join");
    m_member.learn(first, "join");
    backbone.join_admins(vec![iroh::EndpointAddr::new(peer).with_ip_addr(x)]).await;
    let held = old.ep.connect(iroh::EndpointAddr::new(admin.ep.id()).with_ip_addr(socket_of(&admin)), iroh_gossip::ALPN).await.unwrap();
    assert!(paths(&admin.ep, peer).await.contains(&x), "the old birth is connected to the admin at {x}");

    // The key re-binds: the old process dies without a word (no close reaches the admin, which keeps
    // its connection to X open and in use), and its successor binds a new socket Y.
    drop(held);
    drop(old);
    let open = active_paths(&admin.ep, peer).await;
    assert!(open.contains(&x), "the admin still holds the dead birth's path to {x} open: {open:?}");
    let new = node_with(key).await;
    let y = socket_of(&new);
    assert_ne!(x, y);
    let second = birth(&node_id, peer, y, &IncarnationId::mint(), Some(first_birth));
    m_admin.learn(second.clone(), "join");
    m_member.learn(second, "join");

    let (at_admin, at_member) = (looked_up(&admin.ep, peer).await, looked_up(&member.ep, peer).await);
    assert!(
        at_admin == BTreeSet::from([y]) && at_member == BTreeSet::from([y]),
        "after the newer birth is accepted every address book names the key at {y} alone (old {x}): admin {at_admin:?}, member {at_member:?}"
    );

    // The admin retires its path to X, and a dial of the key by its id puts none back.
    let deadline = Instant::now() + Duration::from_secs(10);
    while paths(&admin.ep, peer).await.contains(&x) {
        assert!(Instant::now() < deadline, "the admin still holds a path to the old socket {x}: {:?}", paths(&admin.ep, peer).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _dialed = admin.ep.connect(iroh::EndpointAddr::new(peer), iroh_gossip::ALPN).await.unwrap();
    let after = paths(&admin.ep, peer).await;
    assert!(!after.contains(&x), "a dial of the key by its id reached for the old socket {x} again: {after:?}");
    assert!(after.contains(&y), "the dial went to {y}: {after:?}");

    drop(telemetry);
    let spans = spans_of(&dir);
    let refreshed: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.connection.update.via-peer-location-refreshed").collect();
    for observer in ["mesh1.admin.1", "mesh1.rpc.1"] {
        assert!(
            refreshed.iter().any(|s| s["attributes"]["observer"] == observer
                && s["attributes"]["old_socket"] == x.to_string()
                && s["attributes"]["new_socket"] == y.to_string()
                && s["attributes"]["retired"] == "true"),
            "{observer} names the move from {x} to {y} and the retirement of the old paths: {refreshed:#?}"
        );
    }
}
