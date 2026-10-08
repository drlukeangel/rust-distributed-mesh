//! i143.e8.s2 acceptance (rafka-v2 #2780, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2780-unit`, which exports `I143_ACCEPTANCE_DIR`; each cell
//! leaves `result.json` (its direct observations, one row per explored cut) and `spans.json` (every
//! span it emitted, captured in-process by its own OTel exporter) there.
//!
//! The testkit proof store (op 0x70) is served by a real `NodeRpcServer` over a real store file; its
//! two apply failpoints (`ApplyCuts`: before the apply, after the apply and before the reply) are
//! the testkit-only seam, shaped like `rafka_node_rpc::Failpoint`. The request-side cut is the
//! client's own `CallOptions::cut_before_finish`. Every cut is explored against a healthy control
//! call of the same family in the same capture.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use rafka_node_rpc_testkit::proof_store::{self, ApplyCuts, FileProofStore, ProofOp, ProofReply, ProofRequest, ProofStore, Provenance};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Every request/apply cut of one proof-store call, in the order a call crosses them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cut {
    /// The request is written in part and reset with 499 before its direction finished.
    BeforeCompleteSend,
    /// The request is complete and finished; the handler holds before it applies.
    HandlerBeforeApply,
    /// The handler applied; it holds before it replies.
    AfterApplyBeforeReply,
}

impl Cut {
    const ALL: [Cut; 3] = [Cut::BeforeCompleteSend, Cut::HandlerBeforeApply, Cut::AfterApplyBeforeReply];
    fn name(self) -> &'static str {
        match self {
            Cut::BeforeCompleteSend => "before-complete-send",
            Cut::HandlerBeforeApply => "handler-before-apply",
            Cut::AfterApplyBeforeReply => "after-apply-before-reply",
        }
    }
}

/// The cuts each cell explores; together they are every cut.
const BEFORE_APPLY_CELL: [Cut; 2] = [Cut::BeforeCompleteSend, Cut::HandlerBeforeApply];
const AFTER_APPLY_CELL: [Cut; 1] = [Cut::AfterApplyBeforeReply];
const _: () = assert!(BEFORE_APPLY_CELL.len() + AFTER_APPLY_CELL.len() == Cut::ALL.len());

fn acceptance_dir(cell: &str) -> PathBuf {
    let dir = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2780/unit").join(cell),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on every
/// thread of the cell's own runtime, so two cells in one test process never share spans.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2780-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-2780"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
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
}

/// One real proof-store server and a real client naming its exact birth.
struct Rig {
    _router: Router,
    store: Arc<FileProofStore>,
    cuts: Arc<ApplyCuts>,
    stats: Arc<ServerStats>,
    client: NodeRpcClient,
    target: NodeTarget,
    launch: Launch,
}

async fn rig(dir: &std::path::Path) -> Rig {
    let data_dir = dir.join("data");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let key = SecretKey::generate();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: Vec::new(),
        seeds: Vec::new(),
        launcher: None,
        data_dir,
        mesh_id: None,
        mesh_primary: false,
        fabric_primary: false,
    };
    let store = Arc::new(FileProofStore::open(&launch.data_dir).expect("the store opens in its data dir"));
    let cuts = Arc::new(ApplyCuts::default());
    let server = proof_store::serve_with_cuts(ServerBuilder::new(), store.clone(), &launch, cuts.clone())
        .seal(ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
        .expect("the proof store seals");
    let stats = server.stats();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: launch.node_id.clone(), name: launch.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: launch.incarnation.clone() });
    let client = NodeRpcClient::new(rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap(), resolver).with_caller_system("rdm");
    Rig { _router: router, store, cuts, stats, client, target: NodeTarget::ExactNode(launch.node_id.clone()), launch }
}

impl Rig {
    fn at(&self, op: ProofOp) -> Provenance {
        Provenance {
            node_id: self.launch.node_id.to_string(),
            node: self.launch.name.to_string(),
            mesh: self.launch.name.mesh.clone(),
            incarnation_id: self.launch.incarnation.to_string(),
            op,
        }
    }

    /// What the store holds for `key` right now: read from the store itself, never through a call.
    fn held(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.store.apply(ProofRequest::Get { key: key.to_vec() }, self.at(ProofOp::Get)) {
            ProofReply::Value { value, .. } => Some(value),
            ProofReply::Absent { .. } => None,
            other => panic!("the store answered a direct read with {other:?}"),
        }
    }

    async fn put(&self, key: &[u8], value: &[u8], opts: &CallOptions) -> (RpcOutcome<ProofReply>, Option<rafka_node_rpc::CallEvidence>) {
        self.client.call::<ProofStore>(&self.target, &ProofRequest::Put { key: key.to_vec(), value: value.to_vec() }, opts).await
    }

    /// The server's own counters: what it dispatched, dropped unfinished, runs now, and how many
    /// handlers entered and applied.
    fn counters(&self) -> Value {
        json!({
            "dispatched": ServerStats::get(&self.stats.dispatched),
            "dropped_unfinished": ServerStats::get(&self.stats.dropped_unfinished),
            "in_flight": ServerStats::get(&self.stats.in_flight),
            "handler_entered": self.cuts.entered.load(Ordering::SeqCst),
            "applied": self.cuts.applied.load(Ordering::SeqCst),
        })
    }
}

async fn eventually(what: &str, f: impl Fn() -> bool) {
    let until = std::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(std::time::Instant::now() < until, "never observed: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn split(reply: Duration) -> CallOptions {
    CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply }, ..Default::default() }
}

fn spans_named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

/// The healthy same-family control: a completed Put emits the serve span and the proof-store span
/// under it, both under the caller's trace, and stores exactly its value.
async fn control(r: &Rig) -> Value {
    let (out, ev) = r.put(b"control", b"v-control", &CallOptions::default()).await;
    let reply = out.reply().expect("the healthy control call replies").value().clone();
    assert_eq!(reply, ProofReply::Stored { at: r.at(ProofOp::Put) });
    assert!(ev.expect("evidence").committed);
    assert_eq!(r.held(b"control"), Some(b"v-control".to_vec()));
    let c = r.counters();
    assert_eq!((c["dispatched"].as_u64(), c["applied"].as_u64()), (Some(1), Some(1)), "{c}");
    json!({"call": "put control", "outcome": out.name(), "counters": c})
}

/// Parent links the unit census requires: the serve span is the child of the caller's call span
/// (propagated context), and the proof-store span is the child of the serve span.
fn assert_serve_chain(spans: &[Value], want_serves: usize) -> Value {
    let serves = spans_named(spans, "rdm.node_rpc.request.serve.via-direct");
    let stores = spans_named(spans, "rdm.node_rpc.proof_store.serve.via-request");
    let calls = spans_named(spans, "rdm.node_rpc.request.update.via-call");
    assert_eq!(serves.len(), want_serves, "one serve span per dispatched call: {serves:#?}");
    assert_eq!(stores.len(), want_serves, "one proof-store span per dispatched call");
    let mut chain = Vec::new();
    for s in &serves {
        assert_eq!(s["attributes"]["protocol"], "proof-store");
        assert_eq!(s["attributes"]["op"], "112");
        let call = calls.iter().find(|c| c["trace_id"] == s["trace_id"]).unwrap_or_else(|| panic!("a serve span is under its caller's trace: {s}"));
        assert_eq!(s["parent_span_id"], call["span_id"], "the serve span's parent is the transmitted call span");
        let store = stores.iter().find(|p| p["parent_span_id"] == s["span_id"]).unwrap_or_else(|| panic!("the proof-store span is the child of its serve span: {s}"));
        assert_eq!(store["trace_id"], s["trace_id"]);
        chain.push(json!({
            "trace_id": s["trace_id"], "call_span_id": call["span_id"], "serve_span_id": s["span_id"], "serve_parent_span_id": s["parent_span_id"],
            "proof_store_span_id": store["span_id"], "proof_store_parent_span_id": store["parent_span_id"], "op": store["attributes"]["op"], "outcome": store["attributes"]["outcome"],
        }));
    }
    json!(chain)
}

/// CONTRACT (#2780): a request that never completed its send is not dispatched, and a request that
/// completed its send but whose handler has not applied is not known undelivered. Cut by cut: (1) an
/// unfinished send is reset with 499 and answers NotSent(FrameNotSent) with the server's dropped
/// counter at one and its dispatched and handler counters unmoved (no serve span for that call);
/// (2) a complete, finished request whose handler holds before the apply answers Indeterminate
/// (ReplyDeadline), never NotSent, with the store unchanged at that instant and the apply counter
/// unmoved; the held handler is observed parked by its own reached signal; release then applies it
/// exactly once and the caller's outcome stays Indeterminate with no second dispatch. What must NOT
/// happen: NotSent inferred from a post-commit timeout, a dispatch for the unfinished request, or a
/// replay by the client.
#[test]
fn failpoint_explorer_before_apply_proves_not_sent_or_unapplied() {
    let cell = "failpoint_explorer_before_apply_proves_not_sent_or_unapplied";
    let dir = acceptance_dir(cell);
    let cap = capture(cell);
    let rows = cap.run(async {
        let r = rig(&dir).await;
        let mut rows = vec![control(&r).await];
        for cut in BEFORE_APPLY_CELL {
            match cut {
                Cut::BeforeCompleteSend => {
                    let before = r.counters();
                    let (out, ev) = r.put(b"k-unfinished", &[7u8; 4096], &CallOptions { cut_before_finish: true, ..Default::default() }).await;
                    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent), "{out:?}");
                    assert!(out.proves_not_dispatched());
                    assert!(!ev.expect("evidence").committed, "the request never crossed the commit cut");
                    // The receiver saw the partial request and dropped it: the positive control for "absent".
                    eventually("the receiver dropped the unfinished request", || ServerStats::get(&r.stats.dropped_unfinished) == 1).await;
                    let after = r.counters();
                    assert_eq!(after["dispatched"], before["dispatched"], "the partial frame never reached dispatch");
                    assert_eq!(after["handler_entered"], before["handler_entered"], "no handler ran");
                    assert_eq!(after["applied"], before["applied"]);
                    assert_eq!(r.held(b"k-unfinished"), None, "no effect");
                    rows.push(json!({
                        "cut": cut.name(), "send_complete": false, "request_finished": false, "outcome": out.name(), "reason": format!("{:?}", NotSentReason::FrameNotSent),
                        "proves_not_dispatched": true, "committed": false, "counters_before": before, "counters_after": after, "effect_on_store": null,
                    }));
                }
                Cut::HandlerBeforeApply => {
                    let before = r.counters();
                    r.cuts.before_apply.arm();
                    let (out, ev) = r.put(b"k-held", b"v-held", &split(Duration::from_millis(300))).await;
                    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline), "{out:?}");
                    assert!(!out.proves_not_dispatched(), "a post-commit timeout is never NotSent");
                    assert!(ev.expect("evidence").committed, "the request was complete and finished");
                    r.cuts.before_apply.reached().await;
                    let at_timeout = r.counters();
                    assert_eq!(at_timeout["dispatched"].as_u64(), Some(before["dispatched"].as_u64().unwrap() + 1), "the complete request was dispatched");
                    assert_eq!(at_timeout["in_flight"], 1, "the handler is parked at the cut");
                    assert_eq!(at_timeout["applied"], before["applied"], "zero effects while it holds");
                    assert_eq!(r.held(b"k-held"), None, "the store is unchanged at the instant of Indeterminate");
                    r.cuts.before_apply.release();
                    eventually("the released handler finished", || ServerStats::get(&r.stats.in_flight) == 0).await;
                    let released = r.counters();
                    assert_eq!(released["applied"].as_u64(), Some(before["applied"].as_u64().unwrap() + 1), "release applies it exactly once");
                    assert_eq!(released["dispatched"], at_timeout["dispatched"], "no automatic replay: nothing was dispatched again");
                    assert_eq!(r.held(b"k-held"), Some(b"v-held".to_vec()));
                    rows.push(json!({
                        "cut": cut.name(), "send_complete": true, "request_finished": true, "outcome": out.name(), "reason": format!("{:?}", IndeterminateReason::ReplyDeadline),
                        "proves_not_dispatched": false, "committed": true, "counters_before": before, "counters_at_indeterminate": at_timeout, "counters_after_release": released,
                        "effect_at_indeterminate": null, "effect_after_release": "v-held", "holds": r.cuts.before_apply.holds(),
                    }));
                }
                Cut::AfterApplyBeforeReply => unreachable!("explored by the other cell"),
            }
        }
        rows
    });
    let spans = cap.spans();
    // The control and the held call were dispatched and served; the unfinished one left no serve span.
    let chain = assert_serve_chain(&spans, 2);
    let calls = spans_named(&spans, "rdm.node_rpc.request.update.via-call");
    let outcomes: Vec<(String, String)> = calls.iter().map(|c| (c["attributes"]["outcome"].as_str().unwrap_or("").to_string(), c["attributes"]["reason"].as_str().unwrap_or("").to_string())).collect();
    assert!(outcomes.iter().any(|(o, r)| o == "NotSent" && r == "FrameNotSent"), "the cut call is spanned as NotSent/FrameNotSent: {outcomes:?}");
    assert!(outcomes.iter().any(|(o, r)| o == "Indeterminate" && r == "ReplyDeadline"), "the held call is spanned as Indeterminate/ReplyDeadline: {outcomes:?}");
    let not_sent_call = calls.iter().find(|c| c["attributes"]["outcome"] == "NotSent").unwrap();
    assert!(spans_named(&spans, "rdm.node_rpc.request.serve.via-direct").iter().all(|s| s["trace_id"] != not_sent_call["trace_id"]), "the unfinished call has no serve span in its trace");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let result = json!({"cell": cell, "cuts_explored": BEFORE_APPLY_CELL.iter().map(|c| c.name()).collect::<Vec<_>>(), "rows": rows, "serve_chain": chain, "call_outcomes": outcomes});
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2780): a request whose handler applied and then lost its reply is Indeterminate at the
/// caller, never NotSent: the store holds exactly one mutation, the server dispatched the call once,
/// and nothing replays it. The held handler is observed parked after its apply by its own reached
/// signal and still in flight; release finishes it with the mutation still stored once and the
/// connection healthy for the next call. An explicit idempotent retry by the caller is a separate,
/// recorded act and the only second dispatch. What must NOT happen: NotSent after an apply, a
/// second stored mutation from the lost-reply call, or an automatic replay.
#[test]
fn failpoint_explorer_after_apply_lost_reply_proves_indeterminate() {
    let cell = "failpoint_explorer_after_apply_lost_reply_proves_indeterminate";
    let dir = acceptance_dir(cell);
    let cap = capture(cell);
    let rows = cap.run(async {
        let r = rig(&dir).await;
        let mut rows = vec![control(&r).await];
        for cut in AFTER_APPLY_CELL {
            assert_eq!(cut, Cut::AfterApplyBeforeReply);
            let before = r.counters();
            r.cuts.after_apply.arm();
            let (out, ev) = r.put(b"k-lost", b"v-lost", &split(Duration::from_millis(300))).await;
            assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline), "{out:?}");
            assert!(!out.proves_not_dispatched(), "a lost reply after an apply is never NotSent");
            assert!(ev.expect("evidence").committed);
            r.cuts.after_apply.reached().await;
            let at_timeout = r.counters();
            assert_eq!(at_timeout["dispatched"].as_u64(), Some(before["dispatched"].as_u64().unwrap() + 1));
            assert_eq!(at_timeout["applied"].as_u64(), Some(before["applied"].as_u64().unwrap() + 1), "exactly one stored mutation");
            assert_eq!(at_timeout["in_flight"], 1, "the handler applied and is parked before its reply");
            assert_eq!(r.held(b"k-lost"), Some(b"v-lost".to_vec()), "the mutation is stored though the caller saw no reply");
            r.cuts.after_apply.release();
            eventually("the released handler finished", || ServerStats::get(&r.stats.in_flight) == 0).await;
            let released = r.counters();
            assert_eq!(released["applied"], at_timeout["applied"], "release applies nothing more");
            assert_eq!(released["dispatched"], at_timeout["dispatched"], "no automatic replay");
            assert_eq!(r.held(b"k-lost"), Some(b"v-lost".to_vec()));
            // Terminal recovery: the connection and the server are healthy for the next call.
            let (next, _) = r.client.call::<ProofStore>(&r.target, &ProofRequest::Get { key: b"k-lost".to_vec() }, &CallOptions::default()).await;
            assert_eq!(next.reply().expect("the next call replies").value(), &ProofReply::Value { at: r.at(ProofOp::Get), value: b"v-lost".to_vec() });
            // A separate, explicit, domain-approved idempotent retry: the only second dispatch of this key.
            let (retry, _) = r.put(b"k-lost", b"v-lost", &CallOptions::default()).await;
            assert_eq!(retry.reply().expect("the explicit retry replies").value(), &ProofReply::Stored { at: r.at(ProofOp::Put) });
            let retried = r.counters();
            assert_eq!(r.held(b"k-lost"), Some(b"v-lost".to_vec()), "the idempotent retry leaves the same value");
            rows.push(json!({
                "cut": cut.name(), "send_complete": true, "request_finished": true, "outcome": out.name(), "reason": format!("{:?}", IndeterminateReason::ReplyDeadline),
                "proves_not_dispatched": false, "committed": true, "counters_before": before, "counters_at_indeterminate": at_timeout, "counters_after_release": released,
                "effect_at_indeterminate": "v-lost", "effect_after_release": "v-lost", "holds": r.cuts.after_apply.holds(),
                "terminal_recovery_get": "v-lost", "explicit_retry": {"outcome": retry.name(), "counters_after": retried},
            }));
        }
        rows
    });
    let spans = cap.spans();
    // Control, the lost-reply call, the recovery Get and the explicit retry were each dispatched and served.
    let chain = assert_serve_chain(&spans, 4);
    let calls = spans_named(&spans, "rdm.node_rpc.request.update.via-call");
    let outcomes: Vec<(String, String)> = calls.iter().map(|c| (c["attributes"]["outcome"].as_str().unwrap_or("").to_string(), c["attributes"]["reason"].as_str().unwrap_or("").to_string())).collect();
    assert!(outcomes.iter().any(|(o, r)| o == "Indeterminate" && r == "ReplyDeadline"), "the lost-reply call is spanned as Indeterminate/ReplyDeadline: {outcomes:?}");
    assert!(!outcomes.iter().any(|(o, _)| o == "NotSent"), "no call was NotSent: {outcomes:?}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let result = json!({"cell": cell, "cuts_explored": AFTER_APPLY_CELL.iter().map(|c| c.name()).collect::<Vec<_>>(), "rows": rows, "serve_chain": chain, "call_outcomes": outcomes});
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
