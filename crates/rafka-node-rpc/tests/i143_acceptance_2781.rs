//! i143.e8.s3 acceptance (rafka-v2 #2781, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2781-unit`, which exports `I143_ACCEPTANCE_DIR`; each
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.
//!
//! A seeded [`Scheduler`] decides every choice of a cell (payload size, which cut runs first,
//! whether the held call is released before or after the stale dial ends) and records each step
//! with a logical tick. The cuts are driven through seams the product already takes: the
//! `cut_before_finish` and `after_connect` options, the resolver's `changes()`, and a testkit UDP
//! [`Tap`] between the caller and the node for held, lost and partitioned datagrams. Each cell runs
//! its whole scenario twice from one seed: the recorded events and typed outcomes must be equal.
//! Every cut is preceded by a completed healthy invocation in the same capture.

use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, RelayMode, SecretKey};
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallEvidence, CallOptions, Failpoint, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{IndeterminateReason, RpcOutcome};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_test_scenario::sim::{plan, Cut, Mode, Scheduler, Tap};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::Instrument;

const SEED: u64 = 0x2781;

/// `rafka_node_rpc::endpoint::bind`'s configuration.
async fn bind_behind_tap() -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .alpns(vec![rafka_node_rpc::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .portmapper_config(iroh::endpoint::PortmapperConfig::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap()
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2781/unit").join(cell),
    }
}

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on
/// every thread of the cell's own runtime, so two cells in one test process never share spans.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer as _;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2781-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    // Only this story's spans: the Node RPC runtime's own (`rdm.*`) and the cell's control calls.
    // Iroh's transport internals are not part of it.
    let layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("i143-2781"))
        .with_filter(tracing_subscriber::filter::filter_fn(|m| m.name().starts_with("rdm.")));
    let dbg = std::env::var("I2781_TRACE").ok().map(|_| {
        tracing_subscriber::fmt::layer()
            .with_test_writer()
            .with_target(true)
            .with_ansi(false)
            .with_filter(tracing_subscriber::EnvFilter::new(std::env::var("I2781_FILTER").unwrap_or_else(|_| "iroh::_events=debug,noq=info,iroh::socket::remote_map=debug".into())))
    });
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer).with(dbg)), service }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    /// Every finished span, as exported: name, TraceId, SpanId, ParentSpanId, resource, attributes.
    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }

    /// Write `spans.json` and `result.json` for `cell`.
    fn finish(&self, cell: &str, result: Value) -> Vec<Value> {
        let spans = self.spans();
        let dir = acceptance_dir(cell);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
        spans
    }
}

/// What a handler saw and held, shared with the test that drives the node.
struct Hooks {
    /// Payloads that reached the handler, in order.
    handled: Mutex<Vec<String>>,
    /// The "stored mutation": every `put:<v>`.
    store: Mutex<Vec<String>>,
    /// A `slow` or `put:` handler has run its effect and waits at the gate.
    reached: tokio::sync::Notify,
    gate: tokio::sync::watch::Sender<bool>,
}

impl Hooks {
    fn open(&self) {
        self.gate.send_replace(true);
    }
    fn handled(&self) -> Vec<String> {
        self.handled.lock().unwrap().clone()
    }
}

struct Node {
    _router: Router,
    stats: Arc<ServerStats>,
    hooks: Arc<Hooks>,
    record: ResolvedNode,
}

async fn node() -> Node {
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let ep = bind_behind_tap().await;
    let key = ep.secret_key().clone();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let (gate, gated) = tokio::sync::watch::channel(false);
    let hooks = Arc::new(Hooks { handled: Mutex::default(), store: Mutex::default(), reached: tokio::sync::Notify::new(), gate });
    let h = hooks.clone();
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| {
            let (h, mut gated) = (h.clone(), gated.clone());
            async move {
                let PingRequest::Ping { payload, .. } = req;
                let text = String::from_utf8_lossy(&payload).to_string();
                h.handled.lock().unwrap().push(text.clone());
                if let Some(v) = text.strip_prefix("put:") {
                    h.store.lock().unwrap().push(v.to_string());
                }
                if text == "slow" || text.starts_with("put:") {
                    h.reached.notify_one();
                    let _ = gated.wait_for(|g| *g).await;
                }
                if text == "get" {
                    return Ok(PingReply::Pong { payload: h.store.lock().unwrap().join(",").into_bytes() });
                }
                Ok(PingReply::Pong { payload })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let stats = server.stats();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let record = ResolvedNode { node_id, name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Node { _router: router, stats, hooks, record }
}

struct Caller {
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    target: NodeTarget,
}

async fn caller(record: ResolvedNode) -> Caller {
    let resolver = Arc::new(StaticResolver::new());
    let target = NodeTarget::ExactNode(record.node_id.clone());
    resolver.insert(record);
    let cep = bind_behind_tap().await;
    Caller { client: Arc::new(NodeRpcClient::new(cep, resolver.clone())), resolver, target }
}

type Called = (RpcOutcome<PingReply>, Option<CallEvidence>);

impl Caller {
    async fn call(&self, payload: &[u8], opts: &CallOptions) -> Called {
        self.client.call::<Ping>(&self.target, &PingRequest::Ping { payload: payload.to_vec() }, opts).await
    }
}

/// A typed outcome as a stable label (no ids, no addresses): the same on every replay.
fn label(o: &RpcOutcome<PingReply>) -> String {
    match o {
        RpcOutcome::NotSent(n) => format!("NotSent({:?})", n.reason()),
        RpcOutcome::Indeterminate(i) => format!("Indeterminate({:?})", i.reason()),
        RpcOutcome::Reply(r) => format!("Reply({:?})", r.class()),
        other => other.name().to_string(),
    }
}

/// What one run of a scenario leaves: the scheduler's events, every typed outcome in order and
/// the receiver's own counters. Two runs of one seed leave equal reports.
#[derive(Debug, PartialEq, Clone)]
struct Report {
    events: Vec<Value>,
    outcomes: Vec<String>,
    counters: Value,
}

impl Report {
    fn json(&self) -> Value {
        json!({"events": self.events, "outcomes": self.outcomes, "counters": self.counters})
    }
}

struct Run {
    s: Scheduler,
    outcomes: Vec<String>,
}

impl Run {
    fn new(seed: u64) -> Self {
        Self { s: Scheduler::new(seed), outcomes: Vec::new() }
    }
    fn saw(&mut self, step: &str, o: &RpcOutcome<PingReply>) -> String {
        let l = label(o);
        self.s.record(step, &l);
        self.outcomes.push(format!("{step}: {l}"));
        l
    }
    fn report(self, counters: Value) -> Report {
        Report { events: self.s.events_json(), outcomes: self.outcomes, counters }
    }
}

/// A healthy invocation under its own span: the control that proves the serve path ran.
async fn control(c: &Caller, payload: &[u8]) -> Called {
    c.call(payload, &CallOptions::default()).instrument(tracing::info_span!("rdm.node_rpc.call.create.via-control", payload = %String::from_utf8_lossy(payload))).await
}

async fn until(what: &str, f: impl Fn() -> bool) {
    for _ in 0..500 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never observed: {what}");
}

fn serve_spans(spans: &[Value]) -> Vec<&Value> {
    spans.iter().filter(|s| s["name"] == "rdm.node_rpc.request.serve.via-direct").collect()
}

/// Every `control` call's serve span descends from that call's own span through the client's
/// `request.update.via-call` span, in the same trace.
fn assert_control_parentage(spans: &[Value], controls: usize) {
    let calls: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.call.create.via-control").collect();
    assert_eq!(calls.len(), controls, "one control call span per control invocation");
    for call in calls {
        let served = serve_spans(spans).into_iter().filter(|s| s["trace_id"] == call["trace_id"]).collect::<Vec<_>>();
        assert_eq!(served.len(), 1, "the control invocation's trace holds exactly one serve span: {call}");
        let client = spans.iter().find(|s| s["name"] == "rdm.node_rpc.request.update.via-call" && s["span_id"] == served[0]["parent_span_id"]).expect("the serve span's parent is the client's call span (the transmitted traceparent)");
        assert_eq!(client["trace_id"], call["trace_id"]);
        assert_eq!(client["parent_span_id"], call["span_id"], "the client's call span is a child of the caller's own span");
        assert_eq!(served[0]["attributes"]["protocol"], "ping");
    }
}

// ---------------------------------------------------------------------------------------------
// Cell 1: a request cut before its full send.
// ---------------------------------------------------------------------------------------------

async fn incomplete_send(seed: u64) -> Report {
    let mut r = Run::new(seed);
    let n = node().await;
    let c = caller(n.record.clone()).await;
    let (o, ev) = control(&c, b"control").await;
    assert_eq!(r.saw("control", &o), "Reply(Success)");
    assert!(ev.unwrap().committed);
    let len = 512 + r.s.draw("payload_len", 7680) as usize;
    let mut expected_dispatched = 1;
    for cut in plan(seed, &[Cut::UnfinishedSend, Cut::PartitionedDial]) {
        r.s.record("cut", cut.name());
        match cut {
            Cut::UnfinishedSend => {
                let opts = CallOptions { cut_before_finish: true, ..Default::default() };
                let (o, ev) = c.call(&vec![7u8; len], &opts).await;
                assert_eq!(r.saw("unfinished_send", &o), "NotSent(FrameNotSent)");
                assert!(!ev.unwrap().committed, "the request never crossed the commit cut");
                until("the receiver dropped the unfinished request", || ServerStats::get(&n.stats.dropped_unfinished) == 1).await;
                r.s.record("receiver", "dropped_unfinished=1");
            }
            Cut::PartitionedDial => {
                let tap = Tap::start(n.record.transport_addr).await.unwrap();
                tap.set_to_server(Mode::Drop);
                r.s.record("tap", "to_server=drop");
                let mut behind = n.record.clone();
                behind.transport_addr = tap.addr();
                let p = caller(behind).await;
                let opts = CallOptions { budget: Budget::Split { send: Duration::from_millis(400), reply: Duration::from_secs(1) }, ..Default::default() };
                let (o, ev) = p.call(b"partitioned", &opts).await;
                assert_eq!(r.saw("partitioned_dial", &o), "NotSent(Deadline)");
                assert!(ev.is_none_or(|e| !e.committed));
                assert!(o.proves_not_dispatched());
                tap.set_to_server(Mode::Pass);
                r.s.record("tap", "to_server=pass");
                let (o, _) = p.call(b"healed", &CallOptions::default()).await;
                assert_eq!(r.saw("healed_sibling", &o), "Reply(Success)");
                expected_dispatched += 1;
            }
            _ => unreachable!("this cell plans only send cuts"),
        }
        assert_eq!(ServerStats::get(&n.stats.dispatched), expected_dispatched, "the cut request never reached dispatch");
    }
    let (o, _) = c.call(b"sibling", &CallOptions::default()).await;
    assert_eq!(r.saw("sibling", &o), "Reply(Success)");
    let handled = n.hooks.handled();
    assert!(!handled.iter().any(|h| h == "partitioned"), "the partitioned request never ran a handler: {handled:?}");
    assert_eq!(handled.len() as u64, ServerStats::get(&n.stats.dispatched));
    let counters = json!({
        "dispatched": ServerStats::get(&n.stats.dispatched),
        "dropped_unfinished": ServerStats::get(&n.stats.dropped_unfinished),
        "handled": handled,
        "payload_len": len,
    });
    r.report(counters)
}

/// CONTRACT: a request the scheduler cuts before its complete send and FIN (an unfinished stream
/// reset with 499, or a dial the network never completes) ends `NotSent`, the receiver drops the
/// unfinished request without dispatching it, and the healthy invocations before and after the
/// cut complete. The same seed replays the same cut order, payload and events.
#[test]
fn network_scheduler_replays_incomplete_send_as_not_sent() {
    let cell = "network_scheduler_replays_incomplete_send_as_not_sent";
    let cap = capture(cell);
    let (a, b, other) = cap.run(async { (incomplete_send(SEED).await, incomplete_send(SEED).await, incomplete_send(SEED + 1).await) });
    assert_eq!(a, b, "one seed replays the same events, outcomes and counters");
    assert_ne!(a.events, other.events, "another seed draws another schedule");
    let result = json!({"cell": cell, "seed": SEED, "run": a.json(), "replay_equal": true, "other_seed": SEED + 1, "other_seed_events": other.events});
    let spans = cap.finish(cell, result);
    // Three runs, each with a control and a sibling (plus the healed one when partitioned).
    assert_control_parentage(&spans, 3);
    let dispatched: u64 = [&a, &b, &other].iter().map(|r| r.counters["dispatched"].as_u64().unwrap()).sum();
    assert_eq!(serve_spans(&spans).len() as u64, dispatched, "a serve span exists for every dispatched call and for no cut one");
}

// ---------------------------------------------------------------------------------------------
// Cell 2: a reply held or lost after the complete send.
// ---------------------------------------------------------------------------------------------

async fn reply_cut(seed: u64) -> Report {
    let mut r = Run::new(seed);
    let mut counters = Vec::new();
    for cut in plan(seed, &[Cut::ReplyLost, Cut::ReplyHeld]) {
        r.s.record("cut", cut.name());
        let n = node().await;
        let tap = Tap::start(n.record.transport_addr).await.unwrap();
        let mut behind = n.record.clone();
        behind.transport_addr = tap.addr();
        let c = caller(behind).await;
        let (o, _) = control(&c, b"control").await;
        assert_eq!(r.saw("control", &o), "Reply(Success)");
        let value = format!("v{}", r.s.draw("value", 1000));
        let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(600) }, ..Default::default() };
        let mode = if cut == Cut::ReplyLost { Mode::Drop } else { Mode::Hold };
        let put = format!("put:{value}");
        let call = c.call(put.as_bytes(), &opts);
        let drive = async {
            n.hooks.reached.notified().await;
            r.s.record("handler", "applied");
            tap.set_to_client(mode);
            r.s.record("tap", &format!("to_client={mode:?}"));
            n.hooks.open();
            r.s.record("handler", "released");
        };
        let ((o, ev), ()) = tokio::join!(call, drive);
        assert_eq!(r.saw("put", &o), "Indeterminate(ReplyDeadline)");
        assert!(matches!(&o, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline));
        assert!(ev.unwrap().committed, "the request crossed the commit cut before the reply was cut");
        assert!(!o.proves_not_dispatched());
        // The reply crossed the tap and was cut there: nothing reached the caller another way.
        let (_, held, dropped) = tap.stats().to_client.snapshot();
        assert!(if cut == Cut::ReplyLost { dropped > 0 } else { held > 0 }, "the cut reply datagrams were seen by the tap: held {held}, dropped {dropped}");
        tap.set_to_client(Mode::Pass);
        r.s.record("tap", "to_client=pass");
        // The stored mutation reconciles with what the node holds, read by a new invocation.
        let (o, _) = c.call(b"get", &CallOptions::default()).await;
        assert_eq!(r.saw("reconcile", &o), "Reply(Success)");
        let held = match &o {
            RpcOutcome::Reply(rep) => rep.value().clone(),
            _ => unreachable!(),
        };
        assert_eq!(held, PingReply::Pong { payload: value.clone().into_bytes() }, "the lost-reply mutation is in the final state");
        let handled = n.hooks.handled();
        assert_eq!(handled.iter().filter(|h| h.starts_with("put:")).count(), 1, "the call was never replayed: {handled:?}");
        let (o, _) = c.call(b"sibling", &CallOptions::default()).await;
        assert_eq!(r.saw("sibling", &o), "Reply(Success)");
        counters.push(json!({"cut": cut.name(), "value": value, "dispatched": ServerStats::get(&n.stats.dispatched), "handled": n.hooks.handled(), "stored": n.hooks.store.lock().unwrap().clone()}));
    }
    r.report(Value::Array(counters))
}

/// CONTRACT: after a complete send the scheduler holds or drops the reply datagrams; the caller's
/// outcome is `Indeterminate`, never `NotSent` and never replayed, the handler ran exactly once,
/// and the mutation it stored is found in the final state read by a new invocation after the
/// network is released. The same seed replays the same event order.
#[test]
fn network_scheduler_replays_lost_reply_as_indeterminate() {
    let cell = "network_scheduler_replays_lost_reply_as_indeterminate";
    let cap = capture(cell);
    let (a, b) = cap.run(async { (reply_cut(SEED).await, reply_cut(SEED).await) });
    assert_eq!(a, b, "one seed replays the same events, outcomes and counters");
    let spans = cap.finish(cell, json!({"cell": cell, "seed": SEED, "run": a.json(), "replay_equal": true}));
    assert_control_parentage(&spans, 4);
}

// ---------------------------------------------------------------------------------------------
// Cell 3: a pool supersession while the old connection is in use.
// ---------------------------------------------------------------------------------------------

async fn supersession(seed: u64) -> Report {
    let mut r = Run::new(seed);
    let n = node().await;
    let c = caller(n.record.clone()).await;
    let (o, ev) = control(&c, b"control").await;
    assert_eq!(r.saw("control", &o), "Reply(Success)");
    let first = ev.unwrap();
    // A long call rides the old birth's pooled connection.
    let slow = {
        let (client, target) = (c.client.clone(), c.target.clone());
        tokio::spawn(async move { client.call::<Ping>(&target, &PingRequest::Ping { payload: b"slow".to_vec() }, &CallOptions::default()).await })
    };
    n.hooks.reached.notified().await;
    r.s.record("slow", "in_flight_on_old_birth");
    let release_first = r.s.draw("release_before_stale_dial", 2) == 1;
    // The birth moves: the old connection is evicted at the next call, the dial of the new birth stops at the failpoint.
    let mut second = n.record.clone();
    second.incarnation = IncarnationId::mint();
    c.resolver.insert(second.clone());
    r.s.record("resolver", "birth=2");
    if release_first {
        n.hooks.open();
        r.s.record("slow", "released_before_stale_dial");
    }
    let fp = Arc::new(Failpoint::default());
    let opts = CallOptions { after_connect: Some(fp.clone()), ..Default::default() };
    let stale = c.call(b"stale", &opts);
    let drive = async {
        fp.reached.notified().await;
        r.s.record("dial", "birth=2_connected");
        let mut third = second.clone();
        third.incarnation = IncarnationId::mint();
        c.resolver.insert(third.clone());
        r.s.record("resolver", "birth=3");
        fp.release.notify_one();
        third
    };
    let ((o, ev), third) = tokio::join!(stale, drive);
    assert_eq!(r.saw("stale_dial", &o), "RejectedStale");
    assert!(o.proves_not_dispatched());
    assert!(ev.is_none_or(|e| !e.committed));
    assert!(c.client.pooled().is_empty(), "nothing of a superseded birth is pooled: {:?}", c.client.pooled());
    assert_eq!(ServerStats::get(&n.stats.dispatched), 2, "the superseded call never dispatched (control and slow only)");
    if !release_first {
        n.hooks.open();
        r.s.record("slow", "released_after_stale_dial");
    }
    let (o, _) = slow.await.unwrap();
    assert_eq!(r.saw("slow", &o), "Reply(Success)", "eviction never closes a connection a call is still using");
    // The healthy current sibling.
    let (o, ev) = c.call(b"sibling", &CallOptions::default()).await;
    assert_eq!(r.saw("sibling", &o), "Reply(Success)");
    let ev = ev.unwrap();
    assert!(!ev.reused && ev.connection != first.connection, "the sibling rode a connection of the current birth");
    assert!(c.client.pooled().iter().all(|k| k.incarnation == third.incarnation), "{:?}", c.client.pooled());
    let handled = n.hooks.handled();
    assert!(!handled.iter().any(|h| h == "stale"), "the superseded birth never ran the call: {handled:?}");
    r.s.record("pool", "current_birth_only");
    let counters = json!({"dispatched": ServerStats::get(&n.stats.dispatched), "handled": handled, "release_before_stale_dial": release_first, "pooled": c.client.pooled().len()});
    r.report(counters)
}

/// CONTRACT: while a call rides the old birth's connection, the resolver supersedes the birth
/// twice; the stale dial is cancelled before it is pooled and ends `RejectedStale`, the superseded
/// call never dispatches, the call on the old connection still completes, and a call to the
/// current birth succeeds on a new connection. The seed decides whether the held call is released
/// before or after the stale dial; either order leaves the same typed outcomes.
#[test]
fn pool_scheduler_supersedes_birth_preserves_current_sibling() {
    let cell = "pool_scheduler_supersedes_birth_preserves_current_sibling";
    let cap = capture(cell);
    let (a, b) = cap.run(async { (supersession(SEED).await, supersession(SEED).await) });
    assert_eq!(a, b, "one seed replays the same events, outcomes and counters");
    // The other order of release, found by seed, leaves the same typed outcomes.
    let other_seed = (SEED + 1..).find(|s| Scheduler::new(*s).draw("release_before_stale_dial", 2) != Scheduler::new(SEED).draw("release_before_stale_dial", 2)).unwrap();
    let other = cap.run(supersession(other_seed));
    assert_ne!(a.counters["release_before_stale_dial"], other.counters["release_before_stale_dial"], "the seeds took opposite release orders");
    assert_eq!(a.outcomes.iter().map(|o| o.split(": ").last().unwrap().to_string()).collect::<Vec<_>>(), other.outcomes.iter().map(|o| o.split(": ").last().unwrap().to_string()).collect::<Vec<_>>());
    let spans = cap.finish(cell, json!({"cell": cell, "seed": SEED, "run": a.json(), "replay_equal": true, "other_order_seed": other_seed, "other_order_run": other.json()}));
    assert_control_parentage(&spans, 3);
    let evicts: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.connection.evict.via-incarnation-superseded").collect();
    let outcomes: Vec<&str> = evicts.iter().map(|s| s["attributes"]["outcome"].as_str().unwrap()).collect();
    assert_eq!(outcomes.iter().filter(|o| **o == "evicted").count(), 3, "each run evicted the old birth's pooled connection: {outcomes:?}");
    assert_eq!(outcomes.iter().filter(|o| **o == "late-connect-dropped").count(), 3, "each run dropped the stale dial before pooling: {outcomes:?}");
    for e in evicts {
        assert!(e["attributes"]["peer"].is_string() && e["attributes"]["incarnation_id"].is_string());
    }
}
