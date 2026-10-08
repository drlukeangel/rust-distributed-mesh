//! i143.e6.s2 functional: the scoped pool, keyed by process birth (PRD §1.19–20, §14;
//! node-rpc-rdm-ownership.md §8–§9).
//!
//! Pool identity is `(scope, peer, incarnation)`: a connection is to a process birth. The fence in
//! the framing names the node: every call to a birth shares the connection, and a new incarnation
//! evicts it. Every cell runs over real
//! Iroh endpoints on 127.0.0.1. A dial that must stay in flight targets a "blackhole": a UDP
//! socket that is bound and never read, so a QUIC handshake to it never completes.
//!
//! Incarnations are compared by equality only.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallOptions, Decode, Failpoint, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn echo(p: &[u8]) -> PingRequest {
    PingRequest::Ping { payload: p.to_vec() }
}

fn blackhole() -> (UdpSocket, SocketAddr) {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let a = s.local_addr().unwrap();
    (s, a)
}

/// A node serving Ping from one endpoint and one socket.
/// `hang` never replies; `slow` replies once the test sends `release`; anything else echoes at once.
struct Node {
    _router: Router,
    _server: rafka_node_rpc::NodeRpcServer,
    resolved: ResolvedNode,
    release: tokio::sync::watch::Sender<bool>,
}


async fn node() -> Node {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let (release, released) = tokio::sync::watch::channel(false);
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| {
            let mut released = released.clone();
            async move {
                let PingRequest::Ping { payload, .. } = req;
                match payload.as_slice() {
                    b"hang" => std::future::pending().await,
                    b"slow" => {
                        let _ = released.wait_for(|r| *r).await;
                        Ok(PingReply::Pong { payload })
                    }
                    _ => Ok(PingReply::Pong { payload }),
                }
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode { node_id, name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Node { _router: router, _server: server, resolved, release }
}

/// Until `client` holds a dial in flight (no timer: the dial is registered by the call's first poll).
async fn until_dialing(client: &NodeRpcClient) {
    while client.dialing().is_empty() {
        tokio::task::yield_now().await;
    }
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
    async fn ping(&self) -> (RpcOutcome<PingReply>, rafka_node_rpc::CallEvidence) {
        let (out, ev) = self.client.call::<Ping>(&self.target, &echo(b"ping"), &CallOptions::default()).await;
        (out, ev.expect("a resolved call carries evidence"))
    }
}

/// The caller found the target stale: `RejectedStale`, never `NotSent`.
fn superseded(out: &RpcOutcome<PingReply>) -> bool {
    matches!(out, RpcOutcome::RejectedStale(_)) && out.proves_not_dispatched()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_incarnation_cancels_an_inflight_dial_and_an_address_change_elsewhere_does_not() {
    let key = SecretKey::generate();
    let (_h0, a0) = blackhole();
    let record = ResolvedNode {
        node_id: NodeId::mint(),
        name: "mesh1.rpc.1".parse().unwrap(),
        endpoint_id: key.public(),
        transport_addr: a0,
        incarnation: IncarnationId::mint(),
    };
    // The same birth republished while the dial is in flight: the dial runs on to its deadline.
    let r = rig(record.clone()).await;
    let budget = CallOptions { budget: Budget::Overall(Duration::from_secs(3)), ..Default::default() };
    let started = Instant::now();
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Ping>(&rr.target, &echo(b"x"), &budget).await.0 }, async {
        until_dialing(&rr.client).await;
        let rec = rr.record.clone();
        rr.resolver.insert(rec);
    });
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Deadline), "the same birth republished is not this dial's business: {out:?}");
    assert!(started.elapsed() >= Duration::from_secs(3), "the dial was not cancelled");
    assert!(r.client.pooled().is_empty());

    // The birth moving while the dial is in flight: released at once, as a stale target.
    let r = rig(record.clone()).await;
    let budget = CallOptions { budget: Budget::Overall(Duration::from_secs(4)), ..Default::default() };
    let started = Instant::now();
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Ping>(&rr.target, &echo(b"x"), &budget).await.0 }, async {
        until_dialing(&rr.client).await;
        let mut rec = rr.record.clone();
        rec.incarnation = IncarnationId::mint();
        rr.resolver.insert(rec);
    });
    assert!(superseded(&out), "the stale dial ends as RejectedStale: {out:?}");
    assert!(started.elapsed() < Duration::from_millis(1500), "released at the move, not at the deadline");
    assert!(r.client.pooled().is_empty(), "nothing pooled from a dial that never completed: {:?}", r.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_call_to_a_birth_shares_one_connection() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let (out0, e0) = r.ping().await;
    let (out1, e1) = r.ping().await;
    assert!(out0.reply().is_some() && out1.reply().is_some());
    assert!(e1.reused && e1.connection == e0.connection, "the second call reused the first's connection");
    assert_eq!(r.client.pooled().len(), 1, "one pooled connection per birth");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_superseded_dial_never_pools() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let fp = Arc::new(Failpoint::default());
    let opts = CallOptions { after_connect: Some(fp.clone()), ..Default::default() };
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Ping>(&rr.target, &echo(b"x"), &opts).await.0 }, async {
        // The handshake completed; before it is pooled, the birth moves.
        fp.reached.notified().await;
        let mut rec = rr.record.clone();
        rec.incarnation = IncarnationId::mint();
        rr.resolver.insert(rec);
        fp.release.notify_one();
    });
    assert!(superseded(&out), "a late connect to a superseded birth is RejectedStale: {out:?}");
    assert!(r.client.pooled().is_empty(), "the late connection is never pooled: {:?}", r.client.pooled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_timeout_evicts_poisoned_connection_without_marking_node_unreachable() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let (_, first) = r.ping().await;
    // A long call already riding the pooled connection.
    let (slow, on0) = (echo(b"slow"), CallOptions::default());
    let long = r.client.call::<Ping>(&r.target, &slow, &on0);
    let strikes = async {
        let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(200) }, ..Default::default() };
        let mut seen = Vec::new();
        for _ in 0..2 {
            let (out, ev) = r.client.call::<Ping>(&r.target, &echo(b"hang"), &opts).await;
            assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline), "{out:?}");
            seen.push(ev.unwrap());
        }
        // The long call is answered only after both timeouts evicted the connection it rides.
        n.release.send_replace(true);
        seen
    };
    let ((long_out, _), seen) = tokio::join!(long, strikes);
    assert!(seen.iter().all(|e| e.connection == first.connection), "both timeouts rode the pooled connection");
    assert!(long_out.reply().is_some(), "eviction never closes a connection a call is still using: {long_out:?}");
    // The node is still resolvable and served: the next call dials anew and gets a reply.
    let (out, next) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    assert!(!next.reused && next.connection != first.connection, "the poisoned connection was evicted");
    assert_eq!(next.node_id, r.node_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusal_reply_keeps_healthy_connection_pooled() {
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let (_, first) = r.ping().await;
    for _ in 0..3 {
        let (out, ev) = r
            .client
            .invoke_raw::<PingReply, _>(&r.target, 0x42, vec![1, 2, 3], 1024, &CallOptions::default(), |d| match d {
                Decode::Committed(c, b) => c.reply::<Ping>(b),
                Decode::Early(e, b) => e.reply::<Ping>(b),
            })
            .await;
        assert!(matches!(out, RpcOutcome::Unserved(_)), "{out:?}");
        assert_eq!(ev.unwrap().connection, first.connection);
    }
    let (out, after) = r.ping().await;
    assert!(out.reply().is_some());
    assert!(after.reused && after.connection == first.connection, "a refusal is a healthy reply: the connection stays pooled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_incarnation_evicts_its_predecessors_connection_without_a_failed_call() {
    let n = node().await;
    let mut r = rig(n.resolved.clone()).await;
    let (_, before) = r.ping().await;
    // Same key, same address: only the process birth changed in the caller's view.
    // The fence says nothing about the birth, so the same process answers; what matters here is
    // the pool: the predecessor's connection is gone and the call rode a new one.
    r.record.incarnation = IncarnationId::mint();
    r.resolver.insert(r.record.clone());
    let (out, after) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    assert!(!after.reused && after.connection != before.connection, "it rode a connection of the new birth");
    assert!(r.client.pooled().iter().all(|k| k.incarnation == r.record.incarnation));
}

/// CONTRACT: a dial is shared by every caller of a birth, and each caller's own budget bounds its
/// own wait. A caller that joins a dial another caller started, with a longer budget, is not
/// ended by the starter's shorter one: the starter ends `NotSent(Deadline)` at its own deadline
/// and the joiner is answered once the network lets the dial complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caller_joining_a_dial_is_not_ended_by_the_starters_shorter_budget() {
    use rafka_test_scenario::sim::{Mode, Tap};
    let n = node().await;
    let tap = Tap::start(n.resolved.transport_addr).await.unwrap();
    tap.set_to_server(Mode::Drop);
    let mut behind = n.resolved.clone();
    behind.transport_addr = tap.addr();
    let r = rig(behind).await;
    let short = CallOptions { budget: Budget::Split { send: Duration::from_millis(300), reply: Duration::from_secs(1) }, ..Default::default() };
    let rr = &r;
    let tapr = &tap;
    let ((starter, starter_ended_ms), (joiner, joiner_ended_ms)) = tokio::join!(
        async {
            let started = Instant::now();
            let out = rr.client.call::<Ping>(&rr.target, &echo(b"starter"), &short).await.0;
            let ended = started.elapsed().as_millis();
            // The network heals once the starter's budget is spent.
            tapr.set_to_server(Mode::Pass);
            (out, ended)
        },
        async {
            until_dialing(&rr.client).await;
            let started = Instant::now();
            let out = rr.client.call::<Ping>(&rr.target, &echo(b"joiner"), &CallOptions::default()).await.0;
            (out, started.elapsed().as_millis())
        }
    );
    assert!(matches!(&starter, RpcOutcome::NotSent(x) if *x.reason() == NotSentReason::Deadline), "the starter ends at its own deadline: {starter:?}");
    assert!(starter_ended_ms >= 300, "the starter waited its whole budget: {starter_ended_ms} ms");
    assert!(joiner.reply().is_some(), "the joiner holds a 10 s budget; the starter's 300 ms does not end it (ended after {joiner_ended_ms} ms): {joiner:?} tap to_server (forwarded, held, dropped) = {:?}", tap.stats().to_server.snapshot());
}

/// What the client told its source-owned connections writer, in order.
#[derive(Default)]
struct Told(Mutex<Vec<String>>);

impl rafka_node_rpc::ConnectionObserver for Told {
    fn direct_connected(&self, n: &ResolvedNode) {
        self.0.lock().unwrap().push(format!("connected {}", n.incarnation.0));
    }
    fn direct_failed(&self, n: &ResolvedNode, reason: &str) {
        self.0.lock().unwrap().push(format!("failed {} ({reason})", n.incarnation.0));
    }
    fn direct_broken(&self, n: &ResolvedNode, reason: &str) {
        self.0.lock().unwrap().push(format!("broken {} ({reason})", n.incarnation.0));
    }
}

async fn observed_rig(record: ResolvedNode) -> (Rig, Arc<Told>) {
    let told = Arc::new(Told::default());
    let mut r = rig(record).await;
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    r.client = NodeRpcClient::new(cep, r.resolver.clone()).with_connection_observer(told.clone());
    (r, told)
}

/// CONTRACT: the connections writer is told a Direct connection is Connected by the dial that opened
/// it, exactly once, whether or not a caller is still waiting for it. A caller whose deadline ends
/// while the dial is in flight is told `failed`; the dial runs on (it is bounded by no caller's
/// budget); when it connects and enters the pool the writer hears `connected`, once; later calls
/// ride the pooled connection and report nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_that_outlives_its_callers_deadline_reports_connected_once_when_it_opens() {
    let n = node().await;
    let (r, told) = observed_rig(n.resolved.clone()).await;
    let fp = Arc::new(Failpoint::default());
    let opts = CallOptions { budget: Budget::Overall(Duration::from_millis(400)), after_connect: Some(fp.clone()), ..Default::default() };
    let (out, _) = r.client.call::<Ping>(&r.target, &echo(b"x"), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Deadline), "the caller's deadline ended while the dial was held: {out:?}");
    assert_eq!(told.0.lock().unwrap().len(), 1, "only the failure so far: {:?}", told.0.lock().unwrap());
    // The dial is released: the connection enters the pool with nobody waiting.
    fp.release.notify_one();
    let until = Instant::now() + Duration::from_secs(5);
    while r.client.pooled().is_empty() && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(r.client.pooled().len(), 1, "the late dial pooled its connection");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let told_open = told.0.lock().unwrap().clone();
    assert_eq!(told_open.iter().filter(|e| e.starts_with("connected")).count(), 1, "the dial that opened the connection reported it once: {told_open:?}");
    // A later call rides the pooled connection and reports nothing.
    let (out, ev) = r.client.call::<Ping>(&r.target, &echo(b"y"), &CallOptions::default()).await;
    assert!(out.reply().is_some() && ev.unwrap().reused, "{out:?}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(told.0.lock().unwrap().clone(), told_open, "reuse is not a new fact");
}
