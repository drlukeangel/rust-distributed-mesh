//! i143.e6.s2 functional: the scoped pool, keyed by process birth (PRD §1.19–20, §14;
//! node-rpc-envelope.md §8–§9).
//!
//! Pool identity is `(scope, peer, incarnation)`: a connection is to a process birth. The target in
//! the framing names the node: every call to a birth shares the connection, and a new incarnation
//! evicts it. Every cell runs over real
//! Iroh endpoints on 127.0.0.1. A dial that must stay in flight targets a "blackhole": a UDP
//! socket that is bound and never read, so a QUIC handshake to it never completes.
//!
//! Incarnations are compared by equality only.

use iroh::protocol::Router;
use iroh::SecretKey;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::trace::SpanData;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::{Budget, CallOptions, Decode, Failpoint, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
    _server: rdm_transport::NodeRpcServer,
    resolved: ResolvedNode,
    release: tokio::sync::watch::Sender<bool>,
}


async fn node() -> Node {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
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
    let cep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let node_id = record.node_id.clone();
    Rig { client: NodeRpcClient::new(cep, resolver.clone()), resolver, node_id: node_id.clone(), record, target: NodeTarget::ExactNode(node_id) }
}

impl Rig {
    async fn ping(&self) -> (RpcOutcome<PingReply>, rdm_transport::CallEvidence) {
        let (out, ev) = self.client.call::<Ping>(&self.target, &echo(b"ping"), &CallOptions::default()).await;
        (out, ev.expect("a resolved call carries evidence"))
    }
}

/// The caller found the target stale: `RejectedStale`, never `NotSent`.
fn superseded(out: &RpcOutcome<PingReply>) -> bool {
    matches!(out, RpcOutcome::RejectedStale(_)) && out.proves_not_dispatched()
}

/// A reply budget the `slow` and `hang` handlers outlast.
const REPLY_BUDGET: Budget = Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(200) };

fn spans() -> crate::common::Spans {
    crate::common::spans_exporter()
}

/// The finished spans named `named` in the trace `trace`.
fn in_trace(trace: &str, named: &str) -> Vec<SpanData> {
    spans().get_finished_spans().unwrap().into_iter().filter(|s| s.name == named && s.span_context.trace_id().to_string() == trace).collect()
}

/// The spans named `named` linked (follows_from) to a span of `trace`: the dials the trace's calls started.
fn linked_to_trace(trace: &str, named: &str) -> Vec<SpanData> {
    spans().get_finished_spans().unwrap().into_iter().filter(|s| s.name == named && s.links.iter().any(|l| l.span_context.trace_id().to_string() == trace)).collect()
}

fn attr(s: &SpanData, key: &str) -> Option<String> {
    s.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.as_str().into_owned())
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
    // The same birth republished while the dial is in flight: the dial runs on to its deadline. The deadline is
    // below the transport's idle timeout (the staleness floor, 3 s at gate cadence), which would otherwise end an
    // unanswered dial as `Connection("timed out")` at the same instant.
    let r = rig(record.clone()).await;
    let budget = CallOptions { budget: Budget::Overall(Duration::from_secs(2)), ..Default::default() };
    let started = Instant::now();
    let rr = &r;
    let (out, ()) = tokio::join!(async { rr.client.call::<Ping>(&rr.target, &echo(b"x"), &budget).await.0 }, async {
        until_dialing(&rr.client).await;
        let rec = rr.record.clone();
        rr.resolver.insert(rec);
    });
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Deadline), "the same birth republished is not this dial's business: {out:?}");
    assert!(started.elapsed() >= Duration::from_secs(2), "the dial was not cancelled");
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

/// CONTRACT (node-rpc session rulings 2026-10-03 §4.3; node-rpc.md §21): a reply deadline is not
/// evidence about the connection. With the same birth and a handler holding its reply past the
/// caller's reply budget, three calls in a row each end `Indeterminate(ReplyDeadline)` on the one
/// pooled connection, each of them reused; the connection stays pooled and the next call is
/// answered on it. The caller's trace holds no `evict.via-timeout-strikes` span and no
/// `create.via-dial` span linked to one of its calls (a dial is a root of its own trace, linked to the call
/// that started it): those reply timeouts evicted nothing and caused no redial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_reply_deadlines_leave_the_pooled_connection_in_place_and_reused() {
    spans();
    let n = node().await;
    let r = rig(n.resolved.clone()).await;
    let (out, first) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    let caller = tracing::info_span!("test.caller");
    let trace = caller.context().span().span_context().trace_id().to_string();
    let opts = CallOptions { budget: REPLY_BUDGET, ..Default::default() };
    let (late, (next_out, next)) = tracing::Instrument::instrument(
        async {
            let mut late = Vec::new();
            for _ in 0..3 {
                let (out, ev) = r.client.call::<Ping>(&r.target, &echo(b"slow"), &opts).await;
                late.push((out, ev.expect("a resolved call carries evidence")));
            }
            // The held replies go now, onto streams their callers left; then a call answered at once.
            n.release.send_replace(true);
            (late, r.ping().await)
        },
        caller.clone(),
    )
    .await;
    drop(caller);
    let calls = in_trace(&trace, "rdm.node_rpc.request.update.via-call");
    let strikes = in_trace(&trace, "rdm.node_rpc.connection.evict.via-timeout-strikes");
    let dials = linked_to_trace(&trace, "rdm.node_rpc.connection.create.via-dial");
    let rows: Vec<String> = calls.iter().map(|s| format!("via-call outcome={:?} reason={:?} reused={:?}", attr(s, "outcome"), attr(s, "reason"), attr(s, "reused"))).collect();
    eprintln!("trace {trace}: {rows:?}; evict.via-timeout-strikes={}; create.via-dial={}; pooled={}", strikes.len(), dials.len(), r.client.pooled().len());
    assert!(strikes.is_empty(), "no reply deadline evicts the pooled connection: trace {trace} {rows:?}");
    assert!(dials.is_empty(), "no redial caused by those reply timeouts: trace {trace} {rows:?}");
    for (i, (out, ev)) in late.iter().enumerate() {
        assert!(matches!(out, RpcOutcome::Indeterminate(x) if *x.reason() == IndeterminateReason::ReplyDeadline), "call {i}: {out:?}");
        assert!(ev.reused && ev.connection == first.connection, "call {i} rode the pooled connection: {ev:?} (first {:?})", first.connection);
    }
    assert!(next_out.reply().is_some(), "{next_out:?}");
    assert!(next.reused && next.connection == first.connection, "the next call is answered on the same pooled connection: {next:?}");
    assert_eq!(next.node_id, Some(r.node_id.clone()));
    assert_eq!(r.client.pooled().len(), 1, "the one connection is still pooled");
    assert_eq!(calls.len(), 4, "three late calls and the answered one: {rows:?}");
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
    // The target says nothing about the birth, so the same process answers; what matters here is
    // the pool: the predecessor's connection is gone and the call rode a new one.
    r.record.incarnation = IncarnationId::mint();
    r.resolver.insert(r.record.clone());
    let (out, after) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    assert!(!after.reused && after.connection != before.connection, "it rode a connection of the new birth");
    assert!(r.client.pooled().iter().all(|k| k.incarnation.as_ref() == Some(&r.record.incarnation)));
}

/// What the client told its source-owned connections writer, in order.
#[derive(Default)]
struct Told(Mutex<Vec<String>>);

impl rdm_transport::ConnectionObserver for Told {
    fn direct_connected(&self, n: &ResolvedNode) {
        self.0.lock().unwrap().push(format!("connected {}", n.incarnation.as_str()));
    }
    fn direct_failed(&self, n: &ResolvedNode, reason: &str) {
        self.0.lock().unwrap().push(format!("failed {} ({reason})", n.incarnation.as_str()));
    }
    fn direct_broken(&self, n: &ResolvedNode, reason: &str) {
        self.0.lock().unwrap().push(format!("broken {} ({reason})", n.incarnation.as_str()));
    }
}

async fn observed_rig(record: ResolvedNode) -> (Rig, Arc<Told>) {
    let told = Arc::new(Told::default());
    let mut r = rig(record).await;
    let cep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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

/// CONTRACT (node-rpc session rulings 2026-10-03 §4.3; connections.md sections 9 and 10): reply
/// deadlines on a pooled connection are not a broken Direct connection. After three calls end
/// `Indeterminate(ReplyDeadline)` the connections writer has heard `connected` once, from the dial
/// that opened the connection, and `broken` never; the connection is still the one pooled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_reply_deadlines_report_nothing_broken_to_the_connections_writer() {
    let n = node().await;
    let (r, told) = observed_rig(n.resolved.clone()).await;
    let (out, first) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    let opts = CallOptions { budget: REPLY_BUDGET, ..Default::default() };
    for i in 0..3 {
        let (out, ev) = r.client.call::<Ping>(&r.target, &echo(b"hang"), &opts).await;
        assert!(matches!(&out, RpcOutcome::Indeterminate(x) if *x.reason() == IndeterminateReason::ReplyDeadline), "call {i}: {out:?}");
        assert_eq!(ev.unwrap().connection, first.connection, "call {i} rode the pooled connection");
    }
    let told = told.0.lock().unwrap().clone();
    assert_eq!(told, vec![format!("connected {}", r.record.incarnation.as_str())], "the dial's connected, once, and nothing broken: {told:?}");
    assert_eq!(r.client.pooled().len(), 1, "the connection is still pooled");
}

/// CONTRACT (node-rpc.md §21: Iroh reports the connection closed/unusable; §43 and
/// node-rpc-envelope.md "Transport frames": a stream's end never ends an invocation): a pooled
/// connection the peer closes under a committed, acked call is structural evidence. The connection
/// leaves the pool and the connections writer hears `broken` once; the invocation stays open (its
/// response is owed to it) and ends at its own reply budget; the next call is answered on a
/// connection other than the closed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_closed_under_a_committed_call_is_evicted_and_reported_broken_once() {
    let n = node().await;
    let (r, told) = observed_rig(n.resolved.clone()).await;
    let (out, first) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    let stats = n._server.stats();
    let before = ServerStats::get(&stats.dispatched);
    let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(1) }, ..Default::default() };
    let hang = echo(b"hang");
    let held = r.client.call::<Ping>(&r.target, &hang, &opts);
    let cut = async {
        // The peer closes the connection once the held call is dispatched there (no timer).
        while ServerStats::get(&stats.dispatched) == before {
            tokio::task::yield_now().await;
        }
        n._server.close_connections_except(&[], "test.cut")
    };
    let ((out, _), closed) = tokio::join!(held, cut);
    assert_eq!(closed, 1, "the peer closed the one connection");
    assert!(matches!(&out, RpcOutcome::Indeterminate(x) if *x.reason() == IndeterminateReason::ReplyDeadline), "the invocation outlived its stream, to its own budget: {out:?}");
    let broken: Vec<String> = told.0.lock().unwrap().iter().filter(|e| e.starts_with("broken")).cloned().collect();
    assert_eq!(broken.len(), 1, "the closed connection is reported broken, once: {:?}", told.0.lock().unwrap());
    let (out, next) = r.ping().await;
    assert!(out.reply().is_some(), "{out:?}");
    assert_ne!(next.connection, first.connection, "the closed connection left the pool: {next:?}");
}
