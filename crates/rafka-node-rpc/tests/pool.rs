//! i143.e6.s2 functional: the scoped, slot-aware pool (PRD §1.19–20, §14;
//! node-rpc-rdm-ownership.md §8–§9).
//!
//! Pool identity is `(scope, peer, incarnation, endpoint slot, freshness)`.
//! Every cell runs over real Iroh endpoints on 127.0.0.1. A dial that must
//! stay in flight targets a "blackhole": a UDP socket that is bound and never
//! read, so a QUIC handshake to it never completes.
//!
//! Freshness and incarnation are compared by equality only.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallOptions, Decode, Failpoint, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn echo(p: &[u8]) -> EchoRequest {
    EchoRequest::Echo { traceparent: None, payload: p.to_vec() }
}

fn blackhole() -> (UdpSocket, SocketAddr) {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let a = s.local_addr().unwrap();
    (s, a)
}

/// A node serving Echo on two slots (`rpc-0`, `rpc-1`), one Iroh endpoint per
/// slot under the node's one key. `hang` never replies; `slow` replies after
/// 1.5 s; anything else echoes at once.
struct Node {
    _routers: Vec<Router>,
    key: SecretKey,
    addrs: Vec<SocketAddr>,
}

async fn node() -> Node {
    let key = SecretKey::generate();
    let (mut routers, mut addrs) = (Vec::new(), Vec::new());
    for slot in ["rpc-0", "rpc-1"] {
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
            .seal(slot)
            .unwrap();
        let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        addrs.push(ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap());
        routers.push(Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn());
    }
    Node { _routers: routers, key, addrs }
}

struct Rig {
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
    node_id: NodeId,
    record: ResolvedNode,
    target: NodeTarget,
}

/// A client whose resolver names `fabric` at `slots`.
async fn rig(fabric: &SecretKey, slots: Vec<EndpointSlot>) -> Rig {
    let resolver = Arc::new(StaticResolver::new());
    let node_id = NodeId::mint();
    let record = ResolvedNode {
        node_id: node_id.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        transport_id: fabric.public(),
        incarnation: IncarnationId::mint(),
        endpoints: slots,
    };
    resolver.insert(record.clone());
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Rig { client: NodeRpcClient::new(cep, resolver.clone()), resolver, node_id: node_id.clone(), record, target: NodeTarget::ExactNode(node_id) }
}

impl Rig {
    /// Republish the record with `slot` moved to `addr` under a new token.
    fn move_slot(&mut self, slot: &str, addr: SocketAddr) {
        let e = self.record.endpoints.iter_mut().find(|e| e.slot == slot).unwrap();
        *e = EndpointSlot::assign(slot, addr);
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

fn superseded(out: &RpcOutcome<EchoReply>, slot: &str) -> bool {
    matches!(out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Superseded { slot: slot.into() })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_slot_cancels_only_stale_inflight_dials() {
    let fabric = SecretKey::generate();
    let ((_h0, a0), (_h1, a1), (_h2, a2)) = (blackhole(), blackhole(), blackhole());
    let r = rig(&fabric, vec![EndpointSlot::assign("rpc-0", a0), EndpointSlot::assign("rpc-1", a1)]).await;
    {
        // Two dials in flight, one per slot; neither handshake can complete.
        let budget = CallOptions { budget: Budget::Overall(Duration::from_secs(4)), ..Default::default() };
        let opts0 = CallOptions { slot: Some("rpc-0".into()), ..budget.clone() };
        let opts1 = CallOptions { slot: Some("rpc-1".into()), ..budget };
        let rr = &r;
        let stale = async { rr.client.call::<Echo>(&rr.target, &echo(b"x"), &opts0).await };
        let sibling = async { rr.client.call::<Echo>(&rr.target, &echo(b"x"), &opts1).await };
        let started = Instant::now();
        let (stale, sibling, ()) = tokio::join!(
            async {
                let (out, _) = stale.await;
                (out, started.elapsed())
            },
            async {
                let (out, _) = sibling.await;
                (out, started.elapsed())
            },
            async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                // rpc-0 moves; rpc-1 does not.
                let mut rec = rr.record.clone();
                rec.endpoints[0] = EndpointSlot::assign("rpc-0", a2);
                rr.resolver.insert(rec);
            }
        );
        assert!(superseded(&stale.0, "rpc-0"), "the stale dial ends as typed supersession: {:?}", stale.0);
        assert!(stale.1 < Duration::from_millis(1500), "released at the move, not at the deadline: {:?}", stale.1);
        assert!(matches!(&sibling.0, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Deadline), "the sibling dial ran on: {:?}", sibling.0);
        assert!(sibling.1 >= Duration::from_secs(4), "the sibling dial was not cancelled: {:?}", sibling.1);
    }
    assert!(r.client.pooled().is_empty(), "nothing pooled from a dial that never completed: {:?}", r.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unchanged_sibling_slot_stays_usable() {
    let n = node().await;
    let mut r = rig(&n.key, vec![EndpointSlot::assign("rpc-0", n.addrs[0]), EndpointSlot::assign("rpc-1", n.addrs[1])]).await;
    let (out0, e0) = r.ping("rpc-0").await;
    let (out1, e1) = r.ping("rpc-1").await;
    assert!(out0.reply().is_some() && out1.reply().is_some());
    assert_eq!(r.client.pooled().len(), 2, "one pooled connection per slot");

    // rpc-0 moves (a new token at the same address: a fresh slot's restart).
    r.move_slot("rpc-0", n.addrs[0]);
    let (out1b, e1b) = r.ping("rpc-1").await;
    assert!(out1b.reply().is_some(), "{out1b:?}");
    assert!(e1b.reused, "the unchanged sibling's pooled connection is reused");
    assert_eq!(e1b.connection, e1.connection, "the very same connection");
    let (out0b, e0b) = r.ping("rpc-0").await;
    assert!(out0b.reply().is_some(), "{out0b:?}");
    assert!(!e0b.reused && e0b.connection != e0.connection, "the moved slot dials anew");
    assert_ne!(e0b.freshness, e0.freshness);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_superseded_dial_never_pools() {
    let n = node().await;
    let mut r = rig(&n.key, vec![EndpointSlot::assign("rpc-0", n.addrs[0]), EndpointSlot::assign("rpc-1", n.addrs[1])]).await;
    let fp = Arc::new(Failpoint::default());
    let opts = CallOptions { slot: Some("rpc-0".into()), after_connect: Some(fp.clone()), ..Default::default() };
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Echo>(&rr.target, &echo(b"x"), &opts).await.0 }, async {
        // The handshake completed; before it is pooled, the slot moves.
        fp.reached.notified().await;
        let mut rec = rr.record.clone();
        rec.endpoints[0] = EndpointSlot::assign("rpc-0", n.addrs[0]);
        rr.resolver.insert(rec);
        fp.release.notify_one();
    });
    assert!(superseded(&out, "rpc-0"), "a late connect to a superseded slot is typed supersession: {out:?}");
    assert!(r.client.pooled().is_empty(), "the late connection is never pooled: {:?}", r.client.pooled());
    r.record = r.resolver_record();
    let (out, ev) = r.ping("rpc-0").await;
    assert!(out.reply().is_some() && !ev.reused, "the current slot dials its own connection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_freshness_token_never_reenters() {
    let n = node().await;
    let mut r = rig(&n.key, vec![EndpointSlot::assign("rpc-0", n.addrs[0]), EndpointSlot::assign("rpc-1", n.addrs[1])]).await;
    let (_, old) = r.ping("rpc-0").await;
    r.move_slot("rpc-0", n.addrs[0]);
    // Pinned to the old token: refused before any dial.
    let pinned = CallOptions { pin: Some(("rpc-0".into(), old.freshness.clone())), ..Default::default() };
    let (out, _) = r.client.call::<Echo>(&r.target, &echo(b"x"), &pinned).await;
    assert!(superseded(&out, "rpc-0"), "{out:?}");
    // The current token never rides the old token's connection, even at the same address.
    let (out, now) = r.ping("rpc-0").await;
    assert!(out.reply().is_some());
    assert!(!now.reused && now.connection != old.connection);
    assert!(
        r.client.pooled().iter().all(|k| k.freshness != old.freshness),
        "the old token's entry is gone from the pool: {:?}",
        r.client.pooled()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_timeout_evicts_poisoned_connection_without_marking_node_unreachable() {
    let n = node().await;
    let r = rig(&n.key, vec![EndpointSlot::assign("rpc-0", n.addrs[0]), EndpointSlot::assign("rpc-1", n.addrs[1])]).await;
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
    let r = rig(&n.key, vec![EndpointSlot::assign("rpc-0", n.addrs[0]), EndpointSlot::assign("rpc-1", n.addrs[1])]).await;
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
    let mut r = rig(&n.key, vec![EndpointSlot::assign("rpc-0", n.addrs[0]), EndpointSlot::assign("rpc-1", n.addrs[1])]).await;
    let (_, before) = r.ping("rpc-1").await;
    // Same key, same addresses, same tokens: only the process birth changed.
    r.record.incarnation = IncarnationId::mint();
    r.resolver.insert(r.record.clone());
    let (out, after) = r.ping("rpc-1").await;
    assert!(out.reply().is_some(), "the first call after the new birth succeeds: {out:?}");
    assert!(!after.reused && after.connection != before.connection, "it rides a connection of the new birth");
    assert!(r.client.pooled().iter().all(|k| k.incarnation == r.record.incarnation));
}

impl Rig {
    fn resolver_record(&self) -> ResolvedNode {
        use rafka_node_rpc::NodeResolver;
        self.resolver.resolve(&self.target).unwrap()
    }
}
