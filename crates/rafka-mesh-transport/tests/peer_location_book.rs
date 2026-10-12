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
//!
//! Every connection between the admin and the old birth is registered at the admin before the old
//! birth dies: iroh registers a connection with the remote's state only once its handshake has
//! completed on that side, and a registration puts the connection's socket back into the remote's
//! paths. A connection the admin registered only after it accepted the newer birth would put X back
//! from outside the move this cell is about.

use futures_lite::StreamExt;
use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{Backbone, Membership};
use serde_json::Value;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// The cell's own connection from the old birth to the admin: an ALPN only the admin serves.
const HELD: &[u8] = b"rdm-cell/held";

/// Hands every connection its router accepts to the cell. The router calls `accept` only once iroh's
/// `Accepting` resolved, and that resolves only once the endpoint registered the connection with the
/// remote's state (iroh `endpoint/connection.rs` `conn_from_noq_conn`). The cell holds the handle,
/// so the connection stays open.
#[derive(Debug, Clone)]
struct HandOver(tokio::sync::mpsc::UnboundedSender<iroh::endpoint::Connection>);

impl iroh::protocol::ProtocolHandler for HandOver {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), iroh::protocol::AcceptError> {
        let _ = self.0.send(connection);
        Ok(())
    }
}

struct Node {
    ep: iroh::Endpoint,
    router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn bind(key: iroh::SecretKey, alpns: &[&[u8]]) -> iroh::Endpoint {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    rafka_node_rpc::endpoint::bind_exact(key, "127.0.0.1:0".parse().unwrap(), alpns.iter().map(|a| a.to_vec()).collect(), transport).await.unwrap()
}

/// A gossip node; with `held`, it also serves the cell's [`HELD`] ALPN through it.
async fn node_with(key: iroh::SecretKey, held: Option<HandOver>) -> Node {
    let alpns: &[&[u8]] = if held.is_some() { &[iroh_gossip::ALPN, HELD] } else { &[iroh_gossip::ALPN] };
    let ep = bind(key, alpns).await;
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let mut router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone());
    if let Some(held) = held {
        router = router.accept(HELD, held);
    }
    Node { ep, router: router.spawn(), gossip }
}

/// The peer admin's first birth: it holds every gossip connection the admin dials to it and hands
/// each to the cell. It joins no gossip topic, so its own gossip would only drop what the admin sends
/// (iroh-gossip `proto/state.rs` `State::handle` passes a message only to a topic it joined).
async fn first_birth_at(key: iroh::SecretKey, dialed: HandOver) -> (iroh::Endpoint, Router) {
    let ep = bind(key, &[iroh_gossip::ALPN]).await;
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, dialed).spawn();
    (ep, router)
}

fn socket_of(ep: &iroh::Endpoint) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], ep.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap().port()))
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
        node_state: Default::default(),
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
    let (held_tx, mut held_at_admin) = tokio::sync::mpsc::unbounded_channel();
    let admin = node_with(iroh::SecretKey::generate(), Some(HandOver(held_tx))).await;
    let member = node_with(iroh::SecretKey::generate(), None).await;
    let m_admin = Membership::join(&admin.gossip, &admin.ep, &fab, "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![]).await.unwrap();
    let backbone = Backbone::join(&admin.gossip, &admin.ep, &m_admin, "mesh1", "mesh1.admin.1", IncarnationId::mint(), vec![]).await.unwrap();
    let m_member = Membership::join(&member.gossip, &member.ep, &fab, "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![]).await.unwrap();

    // The peer admin's first birth at X: known to the admin's mesh channel and its backbone, known
    // to the member, and connected to the admin.
    let key = iroh::SecretKey::generate();
    let peer = key.public();
    let (dialed_tx, mut dialed_at_old) = tokio::sync::mpsc::unbounded_channel();
    let old = first_birth_at(key.clone(), HandOver(dialed_tx)).await;
    let x = socket_of(&old.0);
    let node_id = NodeId::mint();
    let first_birth = IncarnationId::mint();
    let first = birth(&node_id, peer, x, &first_birth, None);
    m_admin.learn(first.clone(), "join");
    m_member.learn(first, "join");
    backbone.join_admins(vec![iroh::EndpointAddr::new(peer).with_ip_addr(x)]).await;
    // The admin's gossip dial to X is registered at the admin and in use: gossip writes on a
    // connection only once its connect returned, and a connect returns only once the endpoint
    // registered the connection.
    let dialed = dialed_at_old.recv().await.unwrap();
    let first_frames = dialed.accept_uni().await.unwrap();
    // The old birth's own connection to the admin, registered at the admin: its connect returning
    // says only that the old side completed the handshake.
    let held = old.0.connect(iroh::EndpointAddr::new(admin.ep.id()).with_ip_addr(socket_of(&admin.ep)), HELD).await.unwrap();
    let held_by_admin = held_at_admin.recv().await.unwrap();
    assert_eq!(held_by_admin.remote_id(), peer);
    assert!(paths(&admin.ep, peer).await.contains(&x), "the old birth is connected to the admin at {x}");

    // The key re-binds: the old process dies without a word (no close reaches the admin, which keeps
    // its connections to X open and in use), and its successor binds a new socket Y.
    drop(old);
    let open = active_paths(&admin.ep, peer).await;
    assert!(open.contains(&x), "the admin still holds the dead birth's path to {x} open: {open:?}");
    let new = node_with(key, None).await;
    let y = socket_of(&new.ep);
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
    assert_eq!(
        held_by_admin.close_reason(),
        Some(iroh::endpoint::ConnectionError::LocallyClosed),
        "the retire closed the admin's open connection from the old birth, whose last path X was"
    );
    let _dialed = admin.ep.connect(iroh::EndpointAddr::new(peer), iroh_gossip::ALPN).await.unwrap();
    let after = paths(&admin.ep, peer).await;
    assert!(!after.contains(&x), "a dial of the key by its id reached for the old socket {x} again: {after:?}");
    assert!(after.contains(&y), "the dial went to {y}: {after:?}");
    drop((held, held_by_admin, dialed, first_frames));

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
