//! i143.e6.s8 functional: one Iroh endpoint and one socket per process.
//!
//! A process binds one UDP socket; Node RPC and gossip share it by ALPN. Every request carries its
//! target `(node_id, op)` first in its framing. No receiver refuses a call for the node id it
//! names (R-UC1a, Luke 2026-10-12: the receiver-side target check is a receiver deciding whether a
//! call was meant for it, and it goes with no replacement): the node id is the caller's record of
//! whom it called, and a call to the endpoint of a call that reached the caller names no node at
//! all. The target says nothing about the process birth: that is the resolver's and the pool's
//! knowledge.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::{CallOptions, NodeRpcClient, NodeRpcServer, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::outcome::RpcOutcome;
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
    let endpoint = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone()).accept(rdm_transport::ALPN, server.clone()).accept(iroh_gossip::ALPN, gossip).spawn();
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
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Caller { client: NodeRpcClient::new(ep, resolver.clone()), resolver }
}

impl Caller {
    async fn on(&self, node: &ResolvedNode) -> (RpcOutcome<PingReply>, Option<rdm_transport::CallEvidence>) {
        self.client.call::<Ping>(&NodeTarget::ExactNode(node.node_id.clone()), &echo(), &CallOptions::default()).await
    }
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

/// CONTRACT (R-UC1a: no receiver refuses a Node RPC call for the node id it names): the caller's
/// view maps a second node id to this very process (a replacement it has not learned of, or a
/// misroute). The call naming that id rides the pooled connection to the process and is served:
/// the handler runs and answers. Must NOT happen: a `425` reset, `RejectedStale` from the receiver,
/// or the call never dispatched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_naming_another_node_id_at_this_process_is_served_never_refused() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    let mut other = p.resolved.clone();
    other.node_id = NodeId::mint();
    let c = caller(&[&p.resolved, &other]).await;
    let (_, first) = c.on(&p.resolved).await;
    let first = first.unwrap();
    assert_eq!(c.client.pooled().len(), 1);

    let dispatched = p.dispatched.load(Ordering::SeqCst);
    let (out, ev) = c.on(&other).await;
    assert!(matches!(&out, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. })), "a call naming another node id is served: {out:?}");
    assert_eq!(p.dispatched.load(Ordering::SeqCst), dispatched + 1, "the call reached the handler");
    let ev = ev.unwrap();
    assert_eq!(ev.node_id, Some(other.node_id.clone()), "the evidence names the node the caller resolved");
    assert_eq!(ev.connection, first.connection, "it rode the pooled connection to the process");
}

/// CONTRACT (node-start.md step 2: a stopped node told to start "joins back over the endpoint and
/// path of the call that commanded it; it holds no record of the commander"; R-UC1a): a caller
/// whose resolver holds no record of the process calls it as `NodeTarget::Endpoint`, by its key at
/// the address it is reached on. The call is dialled as named and served; its evidence names no
/// node, and its pooled connection holds no birth. Must NOT happen: a `NotSent` resolve failure (no
/// resolver answers an endpoint), or the call refused for the empty node id its target carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_to_an_endpoint_no_record_names_is_dialled_as_named_and_served() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    let c = caller(&[]).await;
    let target = NodeTarget::Endpoint { endpoint_id: key.public(), addr: p.resolved.transport_addr };
    for _ in 0..2 {
        let (out, ev) = c.client.call::<Ping>(&target, &echo(), &CallOptions::default()).await;
        assert!(matches!(&out, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. })), "the endpoint is reached and served: {out:?}");
        let ev = ev.unwrap();
        assert_eq!(ev.node_id, None, "no record names the process: the evidence names no node");
        assert_eq!(ev.addr, p.resolved.transport_addr, "reached at the address the target names");
    }
    assert_eq!(p.dispatched.load(Ordering::SeqCst), 2);
    let pooled = c.client.pooled();
    assert!(pooled.len() == 1 && pooled[0].peer == key.public() && pooled[0].incarnation.is_none(), "one pooled connection to the process, holding no birth: {pooled:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_callers_stale_birth_is_not_the_targets_business() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    // The caller's view holds another incarnation for the node: the target carries no birth, so the
    // same node answers; the pool, not the wire, is where a birth matters.
    let mut other_birth = p.resolved.clone();
    other_birth.incarnation = IncarnationId::mint();
    let c = caller(&[&other_birth]).await;
    let (out, _) = c.on(&other_birth).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "the target carries no birth: {out:?}");
    assert_eq!(p.dispatched.load(Ordering::SeqCst), 1);
    // The resolver moving the birth under the pool evicts the old connection and the next call
    // rides a new one to the same process.
    c.resolver.insert(p.resolved.clone());
    let (out, ev) = c.on(&p.resolved).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "{out:?}");
    assert!(!ev.unwrap().reused, "a new incarnation in the caller's view is a new connection");
}

#[derive(Default)]
struct Observed(std::sync::Mutex<Vec<ResolvedNode>>);

impl rdm_transport::ConnectionObserver for Observed {
    fn direct_connected(&self, node: &ResolvedNode) {
        self.0.lock().unwrap().push(node.clone());
    }
    fn direct_failed(&self, _node: &ResolvedNode, _reason: &str) {}
    fn direct_broken(&self, _node: &ResolvedNode, _reason: &str) {}
}

/// CONTRACT (node-start.md step 2; connections.md §10; node-rpc-envelope.md §8–§9): a join
/// to a commander no record names dials `NodeTarget::Endpoint` and pools the connection with no
/// birth. Once the caller's resolver learns the commander's birth at that endpoint and address, a
/// later `ExactNode` call adopts that pooled connection under the birth's incarnation (reusing it
/// instead of opening a second connection to the same admin), records its `Direct` connection fact
/// under the resolved birth, and purges the connection when a later birth supersedes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_to_an_endpoint_no_record_names_is_adopted_by_the_birth_and_purged_when_superseded() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    let resolver = Arc::new(StaticResolver::new());
    let observed = Arc::new(Observed::default());
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver.clone()).with_connection_observer(observed.clone());

    let endpoint_target = NodeTarget::Endpoint { endpoint_id: key.public(), addr: p.resolved.transport_addr };
    let (out, join_ev) = client.call::<Ping>(&endpoint_target, &echo(), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "{out:?}");
    let join_ev = join_ev.unwrap();
    assert_eq!(client.pooled(), vec![rdm_transport::PoolKey { scope: None, peer: key.public(), incarnation: None }]);
    assert!(observed.0.lock().unwrap().is_empty(), "no record named the commander at the join");

    // The resolver learns the commander's birth (as GetTopology / the topology view installs it).
    resolver.insert(p.resolved.clone());
    let exact = NodeTarget::ExactNode(p.resolved.node_id.clone());
    let (out, exact_ev) = client.call::<Ping>(&exact, &echo(), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "{out:?}");
    let exact_ev = exact_ev.unwrap();
    assert!(exact_ev.reused, "the ExactNode call reuses the join's connection instead of opening a second one to the same admin");
    assert_eq!(exact_ev.connection, join_ev.connection, "same underlying connection");
    assert_eq!(
        client.pooled(),
        vec![rdm_transport::PoolKey { scope: None, peer: key.public(), incarnation: Some(p.resolved.incarnation.clone()) }],
        "the join's pooled connection is adopted under the resolved birth and no birthless entry remains"
    );
    assert_eq!(*observed.0.lock().unwrap(), vec![p.resolved.clone()], "the Direct connection fact is recorded once the birth is known");

    // Superseding the birth purges the adopted connection.
    let mut next_birth = p.resolved.clone();
    next_birth.incarnation = IncarnationId::mint();
    resolver.insert(next_birth.clone());
    let (out, next_ev) = client.call::<Ping>(&exact, &echo(), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::Reply(_)), "{out:?}");
    let next_ev = next_ev.unwrap();
    assert!(!next_ev.reused, "a new birth evicts the adopted join connection and dials a new one");
    assert_eq!(
        client.pooled(),
        vec![rdm_transport::PoolKey { scope: None, peer: key.public(), incarnation: Some(next_birth.incarnation.clone()) }],
        "only the new birth's connection remains pooled"
    );
}

