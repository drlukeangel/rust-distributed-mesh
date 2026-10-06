//! i143.e6.s2 functional: the scoped pool, keyed by process birth (PRD §1.19–20, §14;
//! node-rpc-rdm-ownership.md §8–§9).
//!
//! Pool identity is `(scope, peer, incarnation)`: a connection is to a process birth, never to
//! a slot. Slots are invocation fences in the framing: one moving never touches a connection,
//! every slot of a birth shares it, and a new incarnation evicts it. Every cell runs over real
//! Iroh endpoints on 127.0.0.1. A dial that must stay in flight targets a "blackhole": a UDP
//! socket that is bound and never read, so a QUIC handshake to it never completes.
//!
//! Freshness and incarnation are compared by equality only.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallOptions, Decode, Failpoint, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn echo(p: &[u8]) -> EchoRequest {
    EchoRequest::Echo { payload: p.to_vec() }
}

fn blackhole() -> (UdpSocket, SocketAddr) {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let a = s.local_addr().unwrap();
    (s, a)
}

/// A node serving Echo on logical slots `rpc-0` and `rpc-1` from one endpoint and one socket.
/// `hang` never replies; `slow` replies after 1.5 s; anything else echoes at once.
struct Node {
    _router: Router,
    server: rafka_node_rpc::NodeRpcServer,
    resolved: ResolvedNode,
}

impl Node {
    /// The node moves `slot` to a new freshness token.
    fn reassign(&self, slot: &str) -> EndpointSlot {
        let moved = EndpointSlot::fresh(slot);
        self.server.assign(moved.clone());
        moved
    }
}

async fn node() -> Node {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let slots = vec![EndpointSlot::fresh("rpc-0"), EndpointSlot::fresh("rpc-1")];
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let server = ServerBuilder::new()
        .serve::<Echo, _, _>(TagOwner::Core, |_peer, req: EchoRequest| async move {
            let EchoRequest::Echo { payload, .. } = req;
            match payload.as_slice() {
                b"hang" => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
                b"slow" => {
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    Ok(EchoReply::Echoed { payload })
                }
                _ => Ok(EchoReply::Echoed { payload }),
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }, slots.clone())
        .unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode { node_id, name: "mesh1.rpc.1".parse().unwrap(), transport_id: key.public(), transport_addr: addr, incarnation, slots };
    Node { _router: router, server, resolved }
}

struct Rig {
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
    node_id: NodeId,
    record: ResolvedNode,
    target: NodeTarget,
}

/// A client whose resolver holds `record`.
async fn rig(record: ResolvedNode) -> Rig {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(record.clone());
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let node_id = record.node_id.clone();
    Rig { client: NodeRpcClient::new(cep, resolver.clone()), resolver, node_id: node_id.clone(), record, target: NodeTarget::ExactNode(node_id) }
}

impl Rig {
    /// The node moves `slot` to a new token and the record follows.
    fn move_slot(&mut self, n: &Node, slot: &str) {
        let moved = n.reassign(slot);
        let e = self.record.slots.iter_mut().find(|e| e.slot == slot).unwrap();
        *e = moved;
        self.resolver.insert(self.record.clone());
    }

    fn on(&self, slot: &str) -> CallOptions {
        CallOptions { slot: Some(slot.into()), ..Default::default() }
    }

    async fn ping(&self, slot: &str) -> (RpcOutcome<EchoReply>, rafka_node_rpc::CallEvidence) {
        let (out, ev) = self.client.call::<Echo>(&self.target, &echo(b"ping"), &self.on(slot)).await;
        (out, ev.expect("a resolved call carries evidence"))
    }
}

/// The caller found the target stale: `RejectedStale`, never `NotSent`.
fn superseded(out: &RpcOutcome<EchoReply>, slot: &str) -> bool {
    matches!(out, RpcOutcome::RejectedStale(s) if s.slot() == slot) && out.proves_not_dispatched()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_incarnation_cancels_an_inflight_dial_and_a_slot_move_does_not() {
    let key = SecretKey::generate();
    let (_h0, a0) = blackhole();
    let record = ResolvedNode {
        node_id: NodeId::mint(),
        name: "mesh1.rpc.1".parse().unwrap(),
        transport_id: key.public(),
        transport_addr: a0,
        incarnation: IncarnationId::mint(),
        slots: vec![EndpointSlot::fresh("rpc-0"), EndpointSlot::fresh("rpc-1")],
    };
    // A slot moving while the dial is in flight: the dial runs on to its deadline.
    let r = rig(record.clone()).await;
    let budget = CallOptions { slot: Some("rpc-1".into()), budget: Budget::Overall(Duration::from_secs(3)), ..Default::default() };
    let started = Instant::now();
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Echo>(&rr.target, &echo(b"x"), &budget).await.0 }, async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut rec = rr.record.clone();
        rec.slots[0] = EndpointSlot::fresh("rpc-0");
        rr.resolver.insert(rec);
    });
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Deadline), "a sibling slot moving is not this dial's business: {out:?}");
    assert!(started.elapsed() >= Duration::from_secs(3), "the dial was not cancelled");
    assert!(r.client.pooled().is_empty());

    // The birth moving while the dial is in flight: released at once, as a stale target.
    let r = rig(record.clone()).await;
    let budget = CallOptions { slot: Some("rpc-0".into()), budget: Budget::Overall(Duration::from_secs(4)), ..Default::default() };
    let started = Instant::now();
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Echo>(&rr.target, &echo(b"x"), &budget).await.0 }, async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut rec = rr.record.clone();
        rec.incarnation = IncarnationId::mint();
        rr.resolver.insert(rec);
    });
    assert!(superseded(&out, "rpc-0"), "the stale dial ends as RejectedStale: {out:?}");
    assert!(started.elapsed() < Duration::from_millis(1500), "released at the move, not at the deadline");
    assert!(r.client.pooled().is_empty(), "nothing pooled from a dial that never completed: {:?}", r.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_slot_of_a_birth_shares_one_connection_and_a_slot_move_keeps_it() {
    let n = node().await;
    let mut r = rig(n.resolved.clone()).await;
    let (out0, e0) = r.ping("rpc-0").await;
    let (out1, e1) = r.ping("rpc-1").await;
    assert!(out0.reply().is_some() && out1.reply().is_some());
    assert!(e1.reused && e1.connection == e0.connection, "the second slot reused the first's connection");
    assert_eq!(r.client.pooled().len(), 1, "one pooled connection per birth");

    r.move_slot(&n, "rpc-0");
    let (out0b, e0b) = r.ping("rpc-0").await;
    assert!(out0b.reply().is_some(), "{out0b:?}");
    assert!(e0b.reused && e0b.connection == e0.connection, "the moved slot rides the same connection under its new token");
    assert_ne!(e0b.freshness, e0.freshness);
    let (out1b, e1b) = r.ping("rpc-1").await;
    assert!(out1b.reply().is_some() && e1b.reused && e1b.connection == e0.connection);
    assert_eq!(r.client.pooled().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_superseded_dial_never_pools() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let fp = Arc::new(Failpoint::default());
    let opts = CallOptions { slot: Some("rpc-0".into()), after_connect: Some(fp.clone()), ..Default::default() };
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Echo>(&rr.target, &echo(b"x"), &opts).await.0 }, async {
        // The handshake completed; before it is pooled, the birth moves.
        fp.reached.notified().await;
        let mut rec = rr.record.clone();
        rec.incarnation = IncarnationId::mint();
        rr.resolver.insert(rec);
        fp.release.notify_one();
    });
    assert!(superseded(&out, "rpc-0"), "a late connect to a superseded birth is RejectedStale: {out:?}");
    assert!(r.client.pooled().is_empty(), "the late connection is never pooled: {:?}", r.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_freshness_token_never_reenters_and_evicts_nothing() {
    let n = node().await;
    let mut r = rig(n.resolved.clone()).await;
    let (_, old) = r.ping("rpc-0").await;
    r.move_slot(&n, "rpc-0");
    // Pinned to the old token: refused before any dial.
    let pinned = CallOptions { pin: Some(("rpc-0".into(), old.freshness.clone())), ..Default::default() };
    let (out, ev) = r.client.call::<Echo>(&r.target, &echo(b"x"), &pinned).await;
    assert!(superseded(&out, "rpc-0"), "{out:?}");
    assert!(ev.is_none(), "nothing dialled");
    // The current token rides the connection the old one used: a slot is not a connection.
    let (out, now) = r.ping("rpc-0").await;
    assert!(out.reply().is_some());
    assert!(now.reused && now.connection == old.connection);
    assert_ne!(now.freshness, old.freshness);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_timeout_evicts_poisoned_connection_without_marking_node_unreachable() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let (_, first) = r.ping("rpc-0").await;
    // A long call already riding the pooled connection.
    let (slow, on0) = (echo(b"slow"), r.on("rpc-0"));
    let long = r.client.call::<Echo>(&r.target, &slow, &on0);
    let strikes = async {
        let opts = CallOptions { slot: Some("rpc-0".into()), budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(200) }, ..Default::default() };
        let mut seen = Vec::new();
        for _ in 0..2 {
            let (out, ev) = r.client.call::<Echo>(&r.target, &echo(b"hang"), &opts).await;
            assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline), "{out:?}");
            seen.push(ev.unwrap());
        }
        seen
    };
    let ((long_out, _), seen) = tokio::join!(long, strikes);
    assert!(seen.iter().all(|e| e.connection == first.connection), "both timeouts rode the pooled connection");
    assert!(long_out.reply().is_some(), "eviction never closes a connection a call is still using: {long_out:?}");
    // The node is still resolvable and served: the next call dials anew and gets a reply.
    let (out, next) = r.ping("rpc-0").await;
    assert!(out.reply().is_some(), "{out:?}");
    assert!(!next.reused && next.connection != first.connection, "the poisoned connection was evicted");
    assert_eq!(next.node_id, r.node_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusal_reply_keeps_healthy_connection_pooled() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let (_, first) = r.ping("rpc-0").await;
    for _ in 0..3 {
        let (out, ev) = r
            .client
            .invoke_raw::<EchoReply, _>(&r.target, 0x42, vec![1, 2, 3], 1024, &r.on("rpc-0"), |d| match d {
                Decode::Committed(c, b) => c.reply::<Echo>(b),
                Decode::Early(e, b) => e.reply::<Echo>(b),
            })
            .await;
        assert!(matches!(out, RpcOutcome::Unserved(_)), "{out:?}");
        assert_eq!(ev.unwrap().connection, first.connection);
    }
    let (out, after) = r.ping("rpc-0").await;
    assert!(out.reply().is_some());
    assert!(after.reused && after.connection == first.connection, "a refusal is a healthy reply: the connection stays pooled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_incarnation_evicts_its_predecessors_connection_without_a_failed_call() {
    let n = node().await;
    let mut r = rig(n.resolved.clone()).await;
    let (_, before) = r.ping("rpc-1").await;
    // Same key, same address, same tokens: only the process birth changed. The server still
    // answers for its own birth, so the new birth's first call is refused by the target's fence;
    // what matters here is the pool: the predecessor's connection is gone.
    r.record.incarnation = IncarnationId::mint();
    r.resolver.insert(r.record.clone());
    let (out, after) = r.ping("rpc-1").await;
    assert!(superseded(&out, "rpc-1"), "the old server refuses a fence naming a birth it is not: {out:?}");
    assert!(!after.reused && after.connection != before.connection, "it rode a connection of the new birth");
    assert!(r.client.pooled().iter().all(|k| k.incarnation == r.record.incarnation));
}
