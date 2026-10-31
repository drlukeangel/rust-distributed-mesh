//! A node-admin serves core Forward (op `0x1A`) like every node built on node-rpc: the generic
//! one-hop carried execution is core (the core families are exactly Ping and Forward), so a peer
//! that picks an admin as its carrier gets the inner call made and answered, never `Unserved`.
//!
//! The server under test is `admin::rpc_server`, the composition `admin::start` builds and
//! serves; the carried target is a real Node RPC server on loopback.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rdm_core::{IncarnationId, NodeId};
use rdm_admin::connections_writer::ConnectionsWriter;
use rdm_admin::storage::MemoryConnectionsStorage;
use rdm_transport::{CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::forward::{Forward, ForwardReply, ForwardRequest};
use rdm_core::outcome::{NotSentReason, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use rdm_core::status::{NodeState, Status, StatusReply, StatusRequest};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

struct Birth {
    node_id: NodeId,
    incarnation: IncarnationId,
    key: SecretKey,
}

fn birth() -> Birth {
    Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), key: SecretKey::generate() }
}

fn served(b: &Birth) -> ServedBirth {
    ServedBirth { node_id: b.node_id.to_string(), incarnation: b.incarnation.to_string() }
}

fn resolved(b: &Birth, name: &str, addr: std::net::SocketAddr) -> ResolvedNode {
    ResolvedNode { node_id: b.node_id.clone(), name: name.parse().unwrap(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.incarnation.clone() }
}

struct Rig {
    admin: ResolvedNode,
    admin_server: rdm_transport::NodeRpcServer,
    _admin_router: Router,
    admin_connections: Arc<ConnectionsWriter>,
    admin_live: Arc<LiveNodeResolver>,
    target: Birth,
    target_resolved: ResolvedNode,
    handled: Arc<AtomicU64>,
    _target_router: Router,
    origin: NodeRpcClient,
}

/// The admin (its composition, its one client dialling through its live resolver), a target that
/// serves Status on loopback, and an origin that knows only the admin and the target.
async fn rig() -> Rig {
    let (admin_birth, target) = (birth(), birth());

    let handled = Arc::new(AtomicU64::new(0));
    let (h, node_id, incarnation) = (handled.clone(), target.node_id.clone(), target.incarnation.clone());
    let target_server = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, _req: StatusRequest| {
            let (h, node_id, incarnation) = (h.clone(), node_id.clone(), incarnation.clone());
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                Ok(StatusReply::Current { node_id, incarnation, state: NodeState::Ready })
            }
        })
        .build(served(&target))
        .unwrap();
    let target_ep = rdm_transport::endpoint::bind(target.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let target_addr = target_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let target_router = Router::builder(target_ep).accept(rdm_transport::ALPN, target_server).spawn();
    let target_resolved = resolved(&target, "mesh1.rpc.1", target_addr);

    let admin_ep = rdm_transport::endpoint::bind(admin_birth.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let admin_addr = admin_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let admin = resolved(&admin_birth, "mesh1.admin.2", admin_addr);
    let live = Arc::new(LiveNodeResolver::default());
    live.apply(target_resolved.clone(), None);
    let connections = Arc::new(ConnectionsWriter::new(
        ConnectionEnd { name: admin.name.clone(), node_id: admin.node_id.clone(), incarnation: Some(admin.incarnation.clone()) },
        Arc::new(MemoryConnectionsStorage::default()),
        Arc::new(Mutex::new(ConnectionsHeld::new())),
    ));
    connections.held().lock().unwrap().set_topology_view(live.clone());
    let node_rpc = rdm_admin::node_rpc::ProcessNodeRpc::new(live.clone(), admin_ep.clone());
    let admin_server = rdm_admin::admin::rpc_server(connections.clone(), node_rpc.client.clone(), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()))
        .build(served(&admin_birth))
        .unwrap();
    let admin_router = Router::builder(admin_ep).accept(rdm_transport::ALPN, admin_server.clone()).spawn();

    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(admin.clone());
    resolver.insert(target_resolved.clone());
    let origin_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let origin = NodeRpcClient::new(origin_ep, resolver).with_caller_system("rdm");
    Rig { admin, admin_server, _admin_router: admin_router, admin_connections: connections, admin_live: live, target, target_resolved, handled, _target_router: target_router, origin }
}

fn probe(t: &Birth) -> StatusRequest {
    StatusRequest::ProbeNodeState { node_id: t.node_id.clone(), incarnation: t.incarnation.clone() }
}

fn admin_target(r: &Rig) -> NodeTarget {
    NodeTarget::ExactNode(r.admin.node_id.clone())
}

/// A status call whose carrier is a node-admin is answered by the final target: the admin made
/// the one inner call and handed back the target's own reply.
///
/// CONTRACT: Forward is core, so choosing a node-admin as the carrier never ends `Unserved`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_call_carried_by_a_node_admin_is_answered_by_the_target() {
    let r = rig().await;
    let (out, _) = r.origin.call_via::<Status>(&admin_target(&r), &r.target.node_id, &probe(&r.target), &CallOptions::default()).await;
    let reply = out.reply().unwrap_or_else(|| panic!("the target's reply through the admin carrier: {out:?}")).value().clone();
    assert_eq!(reply, StatusReply::Current { node_id: r.target.node_id.clone(), incarnation: r.target.incarnation.clone(), state: NodeState::Ready });
    assert_eq!(r.handled.load(Ordering::SeqCst), 1, "exactly one inner call");
}

/// A carried call to a target the admin cannot resolve is `NotSent`, naming why, and the admin
/// made no inner call.
///
/// CONTRACT: an admin carrier refuses what it cannot carry by name, never `Unserved`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_call_carried_to_a_target_the_admin_cannot_resolve_is_not_sent_by_name() {
    let r = rig().await;
    let stranger = birth();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(r.admin.clone());
    resolver.insert(resolved(&stranger, "mesh1.rpc.9", "127.0.0.1:1".parse().unwrap()));
    let origin_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let origin = NodeRpcClient::new(origin_ep, resolver);
    let (out, _) = origin.call_via::<Status>(&admin_target(&r), &stranger.node_id, &probe(&stranger), &CallOptions::default()).await;
    match &out {
        RpcOutcome::NotSent(n) => {
            assert_eq!(n.reason(), &NotSentReason::Resolve(rdm_core::outcome::ResolveFailure::Unknown));
            assert_eq!(n.code(), rdm_core::outcome::Code::NotFound);
        },
        other => panic!("not sent, naming the unresolved target: {other:?}"),
    }
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// A target the admin cannot reach, while the admin's own Direct fact toward it is not Active, is
/// answered `ConnectionLost` naming that fact, and the target handled nothing.
///
/// CONTRACT: the admin carrier reports its own connection to a dead target, the same as any carrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_call_carried_to_a_dead_target_names_the_admins_lost_connection() {
    let r = rig().await;
    // The dead target: the admin dials the live target's address for it and the handshake names another key.
    let dead = birth();
    let mut dead_resolved = resolved(&dead, "mesh1.rpc.2", r.target_resolved.transport_addr);
    dead_resolved.endpoint_id = SecretKey::generate().public();
    r.admin_live.apply(dead_resolved.clone(), None);
    // The admin's own latest Direct fact toward it: Disconnected.
    let own = ConnectionEnd { name: r.admin.name.clone(), node_id: r.admin.node_id.clone(), incarnation: Some(r.admin.incarnation.clone()) };
    let destination = ConnectionEnd { name: dead_resolved.name.clone(), node_id: dead.node_id.clone(), incarnation: Some(dead.incarnation.clone()) };
    r.admin_connections
        .record(NodeConnection { id: rdm_core::ConnectionId::mint(), source: own, destination, kind: ConnectionKind::Direct, alpn: rdm_core::connections::ALPN.to_vec(), state: ConnectionState::Disconnected, carrier: None, recovery: None, reason: Some("pooled connection broke".into()), logged_at_ms: 1 })
        .await
        .unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(r.admin.clone());
    resolver.insert(dead_resolved);
    let origin_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let origin = NodeRpcClient::new(origin_ep, resolver);
    let (out, _) = origin.call_via::<Status>(&admin_target(&r), &dead.node_id, &probe(&dead), &CallOptions::default()).await;
    match &out {
        RpcOutcome::NotSent(n) => assert!(matches!(n.reason(), NotSentReason::ConnectionLost(why) if why.starts_with("mesh1.admin.2 -> mesh1.rpc.2 Direct ")), "{out:?}"),
        other => panic!("the admin names its lost connection to the dead target: {other:?}"),
    }
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// Forward is not itself forwardable: an inner op naming Forward is refused by the admin carrier
/// as `NotForwardable`, and no inner call is made.
///
/// CONTRACT: the one-hop rule holds at an admin: a carrier never carries a carry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forward_naming_forward_is_refused_not_forwardable_by_the_admin() {
    let r = rig().await;
    let nested = ForwardRequest::Forward { target: r.target.node_id.clone(), inner_op: Forward::OP, inner: Vec::new(), remaining_ms: 5_000, owed: None };
    let (out, _) = r.origin.call::<Forward>(&admin_target(&r), &nested, &CallOptions::default()).await;
    let reply = out.reply().unwrap_or_else(|| panic!("the admin answers a nested forward: {out:?}")).value().clone();
    assert_eq!(reply, ForwardReply::NotForwardable { op: Forward::OP });
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// An admin whose connections are being deleted refuses Forward like every op: the origin is told
/// `ConnectionsDeleting`, and no inner call is made.
///
/// CONTRACT: Forward is not served while connections are being deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connections_deleting_admin_refuses_forward_by_name() {
    let r = rig().await;
    r.admin_server.begin_connections_delete();
    let (out, _) = r.origin.call_via::<Status>(&admin_target(&r), &r.target.node_id, &probe(&r.target), &CallOptions::default()).await;
    match &out {
        RpcOutcome::NotSent(n) => assert!(matches!(n.reason(), NotSentReason::Carried(why) if why.contains("connections-deleting")), "{out:?}"),
        other => panic!("the connections-deleting admin refuses the forward by name: {other:?}"),
    }
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
    let _ = &r.target_resolved;
}
