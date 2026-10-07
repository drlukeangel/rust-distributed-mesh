//! i143.e6.s8 functional: one Iroh endpoint and one socket per process; the fence is the gate.
//!
//! A process binds one UDP socket; Node RPC and gossip share it by ALPN. Every request carries its
//! fence `(target_node_id, op)` first in its framing, and the server refuses a fence that is not
//! this node with `425 STALE_TARGET`, which the client reports as `RejectedStale`: never
//! dispatched, and not `NotSent`. The fence says nothing about the process birth: that is the
//! resolver's and the pool's knowledge.
//!
//! The adversarial cells are the hard gate: a wrong node id is refused on the pooled connection
//! while the right one keeps serving on it, a caller's stale birth is not the fence's business,
//! and the handler never runs for a refused request.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeRpcServer, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn echo() -> PingRequest {
    PingRequest::Ping { payload: b"ping".to_vec() }
}

/// A process serving Ping from one endpoint and one socket; it counts dispatched calls.
struct Process {
    _router: Router,
    endpoint: iroh::Endpoint,
    _server: NodeRpcServer,
    resolved: ResolvedNode,
    dispatched: Arc<AtomicU64>,
}

async fn process(key: &SecretKey) -> Process {
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let endpoint = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr: SocketAddr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let dispatched = Arc::new(AtomicU64::new(0));
    let d = dispatched.clone();
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| {
            d.fetch_add(1, Ordering::SeqCst);
            async move {
                let PingRequest::Ping { payload, .. } = req;
                Ok(PingReply::Pong { payload })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone()).accept(rafka_node_rpc::ALPN, server.clone()).accept(iroh_gossip::ALPN, gossip).spawn();
    let resolved = ResolvedNode { node_id, name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Process { _router: router, endpoint, _server: server, resolved, dispatched }
}

struct Caller {
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
}

async fn caller(records: &[&ResolvedNode]) -> Caller {
    let resolver = Arc::new(StaticResolver::new());
    for r in records {
        resolver.insert((*r).clone());
    }
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Caller { client: NodeRpcClient::new(ep, resolver.clone()), resolver }
}

impl Caller {
    async fn on(&self, node: &ResolvedNode) -> (RpcOutcome<PingReply>, Option<rafka_node_rpc::CallEvidence>) {
        self.client.call::<Ping>(&NodeTarget::ExactNode(node.node_id.clone()), &echo(), &CallOptions::default()).await
    }
}

fn stale(out: &RpcOutcome<PingReply>, node_id: &NodeId) -> bool {
    matches!(out, RpcOutcome::RejectedStale(s) if s.target_node_id() == node_id.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_endpoint_and_one_socket_serve_every_call_on_one_connection() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    assert_eq!(p.endpoint.id(), key.public(), "the process has one identity");
    let bound = p.endpoint.bound_sockets();
    assert_eq!(bound, vec![p.resolved.transport_addr], "exactly one socket, the transport address: {bound:?}");
    let c = caller(&[&p.resolved]).await;
    for _ in 0..2 {
        let (out, ev) = c.on(&p.resolved).await;
        assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
        assert_eq!(ev.unwrap().addr, p.resolved.transport_addr, "reached at the one address");
    }
    assert_eq!(p.dispatched.load(Ordering::SeqCst), 2);
    assert_eq!(c.client.pooled().len(), 1, "both calls rode one connection to the process: {:?}", c.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_node_id_is_425_on_the_pooled_connection_and_never_dispatches() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    // The caller's view maps a second node id to this very process (a replacement it has not
    // learned of, or a misroute): the fence names a node the process is not.
    let mut other = p.resolved.clone();
    other.node_id = NodeId::mint();
    let c = caller(&[&p.resolved, &other]).await;
    let (_, first) = c.on(&p.resolved).await;
    let first = first.unwrap();
    assert_eq!(c.client.pooled().len(), 1);

    let dispatched = p.dispatched.load(Ordering::SeqCst);
    let (out, ev) = c.on(&other).await;
    assert!(stale(&out, &other.node_id), "a wrong node id is 425 STALE_TARGET -> RejectedStale: {out:?}");
    assert!(out.proves_not_dispatched(), "RejectedStale is a definitive no-dispatch");
    assert_eq!(p.dispatched.load(Ordering::SeqCst), dispatched, "the refused call never reached the handler");
    let ev = ev.unwrap();
    assert!(ev.committed, "the request was sent and refused, not NotSent");
    assert_eq!(ev.connection, first.connection, "the refusal rode the pooled connection");

    // The right node id keeps serving on the very same connection; nothing was evicted.
    let (out, ev) = c.on(&p.resolved).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    let ev = ev.unwrap();
    assert!(ev.reused && ev.connection == first.connection, "a refused fence never touches the process connection");
    assert_eq!(c.client.pooled().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_callers_stale_birth_is_not_the_fences_business() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    // The caller's view holds another incarnation for the node: the fence carries no birth, so the
    // same node answers; the pool, not the wire, is where a birth matters.
    let mut other_birth = p.resolved.clone();
    other_birth.incarnation = IncarnationId::mint();
    let c = caller(&[&other_birth]).await;
    let (out, _) = c.on(&other_birth).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "the fence carries no birth: {out:?}");
    assert_eq!(p.dispatched.load(Ordering::SeqCst), 1);
    // The resolver moving the birth under the pool evicts the old connection and the next call
    // rides a new one to the same process.
    c.resolver.insert(p.resolved.clone());
    let (out, ev) = c.on(&p.resolved).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "{out:?}");
    assert!(!ev.unwrap().reused, "a new incarnation in the caller's view is a new connection");
}
