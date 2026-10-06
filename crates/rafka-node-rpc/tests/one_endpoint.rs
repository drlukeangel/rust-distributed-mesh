//! i143.e6.s8 functional: one Iroh endpoint and one socket per process; slots are fences.
//!
//! A process binds one UDP socket; Node RPC and gossip share it by ALPN. A slot is a logical
//! invocation fence, not a transport endpoint: it owns no socket, port or connection. Every
//! request names its exact target `(node_id, incarnation, slot, freshness)` in its framing, and
//! the server refuses any part that is not current with `425 STALE_SLOT`, which the client reports
//! as `RejectedStale`: never dispatched, and not `NotSent`. A slot moving never touches the
//! process's connection; a new incarnation does.
//!
//! The adversarial cells are the hard gate: a superseded `rpc-0` is refused while `rpc-1` keeps
//! serving on the same pooled connection, a stale fence is refused whichever part moved, and the
//! handler never runs for a refused request.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeRpcServer, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn echo() -> EchoRequest {
    EchoRequest::Echo { payload: b"ping".to_vec() }
}

/// A process serving Echo on logical slots `rpc-0` and `rpc-1` from one endpoint and one socket.
/// The handler records the slot each dispatched call named.
struct Process {
    _router: Router,
    endpoint: iroh::Endpoint,
    server: NodeRpcServer,
    resolved: ResolvedNode,
    handled: Arc<Mutex<Vec<String>>>,
    dispatched: Arc<AtomicU64>,
}

async fn process(key: &SecretKey) -> Process {
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let slots = vec![EndpointSlot::fresh("rpc-0"), EndpointSlot::fresh("rpc-1")];
    let endpoint = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr: SocketAddr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let (handled, dispatched) = (Arc::new(Mutex::new(Vec::new())), Arc::new(AtomicU64::new(0)));
    let (h, d) = (handled.clone(), dispatched.clone());
    let server = ServerBuilder::new()
        .serve::<Echo, _, _>(TagOwner::Core, move |peer, req: EchoRequest| {
            h.lock().unwrap().push(peer.slot.clone());
            d.fetch_add(1, Ordering::SeqCst);
            async move {
                let EchoRequest::Echo { payload, .. } = req;
                Ok(EchoReply::Echoed { payload })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }, slots.clone())
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone()).accept(rafka_node_rpc::ALPN, server.clone()).accept(iroh_gossip::ALPN, gossip).spawn();
    let resolved = ResolvedNode { node_id, name: "mesh1.rpc.1".parse().unwrap(), transport_id: key.public(), transport_addr: addr, incarnation, slots };
    Process { _router: router, endpoint, server, resolved, handled, dispatched }
}

struct Caller {
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
    record: ResolvedNode,
    target: NodeTarget,
}

async fn caller(record: ResolvedNode) -> Caller {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(record.clone());
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Caller { client: NodeRpcClient::new(ep, resolver.clone()), resolver, target: NodeTarget::ExactNode(record.node_id.clone()), record }
}

impl Caller {
    async fn on(&self, slot: &str) -> (RpcOutcome<EchoReply>, Option<rafka_node_rpc::CallEvidence>) {
        self.client.call::<Echo>(&self.target, &echo(), &CallOptions { slot: Some(slot.into()), ..Default::default() }).await
    }
}

fn stale(out: &RpcOutcome<EchoReply>, slot: &str, freshness: &str) -> bool {
    matches!(out, RpcOutcome::RejectedStale(s) if s.slot() == slot && s.freshness() == freshness)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_endpoint_and_one_socket_serve_every_slot_on_one_connection() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    assert_eq!(p.endpoint.id(), key.public(), "the process has one identity");
    let bound = p.endpoint.bound_sockets();
    assert_eq!(bound, vec![p.resolved.transport_addr], "exactly one socket, the transport address: {bound:?}");
    let c = caller(p.resolved.clone()).await;
    for slot in ["rpc-0", "rpc-1"] {
        let (out, ev) = c.on(slot).await;
        assert!(matches!(out, RpcOutcome::Reply(_)), "{slot} answers: {out:?}");
        assert_eq!(ev.unwrap().addr, p.resolved.transport_addr, "every slot is reached at the one address");
    }
    assert_eq!(*p.handled.lock().unwrap(), vec!["rpc-0".to_string(), "rpc-1".to_string()], "the handler sees the slot the request named");
    assert_eq!(c.client.pooled().len(), 1, "both slots rode one connection to the process: {:?}", c.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_slot_is_425_while_its_sibling_and_the_connection_survive() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    let mut c = caller(p.resolved.clone()).await;
    let (_, first) = c.on("rpc-0").await;
    let first = first.unwrap();
    assert!(matches!(c.on("rpc-1").await.0, RpcOutcome::Reply(_)));
    assert_eq!(c.client.pooled().len(), 1);

    // rpc-0 moves at the server; the caller still holds its old token.
    let old = p.resolved.slot("rpc-0").unwrap().freshness.to_string();
    let moved = EndpointSlot::fresh("rpc-0");
    p.server.assign(moved.clone());
    let dispatched = p.dispatched.load(Ordering::SeqCst);
    let (out, ev) = c.on("rpc-0").await;
    assert!(stale(&out, "rpc-0", &old), "a superseded token is 425 STALE_SLOT -> RejectedStale: {out:?}");
    assert!(out.proves_not_dispatched(), "RejectedStale is a definitive no-dispatch");
    assert_eq!(p.dispatched.load(Ordering::SeqCst), dispatched, "the stale call never reached the handler");
    assert_eq!(ev.unwrap().connection, first.connection, "the refusal rode the pooled connection");

    // The sibling keeps serving on the very same connection; nothing was evicted.
    let (out, ev) = c.on("rpc-1").await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    let ev = ev.unwrap();
    assert!(ev.reused && ev.connection == first.connection, "a slot moving never touches the process connection");
    assert_eq!(c.client.pooled().len(), 1);

    // Once the caller learns the new token, rpc-0 serves again on that same connection.
    c.record.slots[0] = moved;
    c.resolver.insert(c.record.clone());
    let (out, ev) = c.on("rpc-0").await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    let ev = ev.unwrap();
    assert!(ev.reused && ev.connection == first.connection, "the new token rides the existing connection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_fence_is_refused_whichever_part_moved_and_never_dispatches() {
    let key = SecretKey::generate();
    let p = process(&key).await;
    let (old_slot, old_tok) = (p.resolved.slots[0].clone(), p.resolved.slots[0].freshness.to_string());

    // The caller decides: a pin to a token its own view already knows is superseded.
    let mut record = p.resolved.clone();
    record.slots[0] = EndpointSlot::fresh("rpc-0");
    let c = caller(record).await;
    let pinned = CallOptions { pin: Some(("rpc-0".into(), old_slot.freshness.clone())), ..Default::default() };
    let (out, ev) = c.client.call::<Echo>(&c.target, &echo(), &pinned).await;
    assert!(stale(&out, "rpc-0", &old_tok), "the caller refuses a superseded pin before any dial: {out:?}");
    assert!(ev.is_none(), "nothing was dialled");

    // The caller decides: a slot the current birth does not hold.
    let (out, _) = c.on("rpc-9").await;
    assert!(matches!(&out, RpcOutcome::RejectedStale(s) if s.slot() == "rpc-9"), "{out:?}");

    // The target decides: the caller's view names a slot token the server moved past.
    let c = caller(p.resolved.clone()).await;
    p.server.assign(EndpointSlot::fresh("rpc-0"));
    let (out, _) = c.on("rpc-0").await;
    assert!(stale(&out, "rpc-0", &old_tok), "the target refuses with 425: {out:?}");

    // The target decides: a caller holding the right token under the wrong birth.
    let mut other_birth = p.resolved.clone();
    other_birth.incarnation = IncarnationId::mint();
    let c = caller(other_birth).await;
    let (out, _) = c.on("rpc-1").await;
    assert!(matches!(&out, RpcOutcome::RejectedStale(s) if s.slot() == "rpc-1"), "a stale incarnation cannot dispatch even with a current slot token: {out:?}");
    let mut other_node = p.resolved.clone();
    other_node.node_id = NodeId::mint();
    let c = caller(other_node).await;
    let (out, _) = c.on("rpc-1").await;
    assert!(matches!(&out, RpcOutcome::RejectedStale(_)), "a wrong node id cannot dispatch: {out:?}");
    assert_eq!(p.dispatched.load(Ordering::SeqCst), 0, "no refused request reached the handler");
}
