//! i143.e6.s4: the carrier's span-asserting forward cell, in its own test binary.
//!
//! The cell installs a process-global tracing subscriber whose span processor exports
//! synchronously on every span end of every thread. A binary that holds it pays that cost on
//! every other cell's hot path, and the reply-reserve cells in `forward.rs` are bounded by 100 ms
//! of wall clock, so this cell lives in a process of its own.

use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::testing::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::{SimpleSpanProcessor, TracerProvider};
use rafka_node_rpc::CallOptions;
use rafka_node_rpc_contract::forward::FORWARD_REPLY_RESERVE;
use rafka_node_rpc_contract::outcome::{NotSentReason, RpcOutcome};
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[allow(dead_code)]
#[path = "common/forward_rig.rs"]
mod rig;
use rig::*;

fn spans() -> InMemorySpanExporter {
    static EXPORTER: OnceLock<InMemorySpanExporter> = OnceLock::new();
    EXPORTER
        .get_or_init(|| {
            let exporter = InMemorySpanExporter::default();
            let provider = TracerProvider::builder().with_span_processor(SimpleSpanProcessor::new(Box::new(exporter.clone()))).build();
            let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "forward-test");
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)).init();
            exporter
        })
        .clone()
}

/// The names of the spans a cell's own trace finished.
fn trace_span_names(trace: &str) -> Vec<String> {
    spans().get_finished_spans().unwrap().into_iter().filter(|s| s.span_context.trace_id().to_string() == trace).map(|s| s.name.to_string()).collect()
}

/// A budget already spent down to the carrier's reserve when the frame is written.
///
/// CONTRACT: the carrier makes no inner call and refuses by name before dispatch: the origin
/// sees `NotSent(CarrierNoBudget)`, the target handled nothing, and the trace holds the
/// carrier's `reject.via-forward-budget-spent` span and no `serve.via-carried-inner` span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_budget_spent_to_the_reply_reserve_is_refused_by_name_with_no_inner_call() {
    spans();
    let r = rig().await;
    let (target, carrier) = (r.target.resolved.node_id.clone(), carrier_of(&r));
    // Warm the carrier connection so the budgeted call's dial is instant.
    let (warm, _) = r.origin.call_via::<Probe>(&carrier, &target, &probe(b"warm"), &CallOptions::default()).await;
    assert!(warm.reply().is_some(), "{warm:?}");
    let handled_before = r.handled.load(Ordering::SeqCst);
    let caller = tracing::info_span!("test.caller");
    let trace = caller.context().span().span_context().trace_id().to_string();
    let (out, _) = tracing::Instrument::instrument(r.origin.call_via::<Probe>(&carrier, &target, &probe(b"late"), &overall(FORWARD_REPLY_RESERVE)), caller).await;
    assert!(
        matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::CarrierNoBudget { reserve_ms, .. } if *reserve_ms == FORWARD_REPLY_RESERVE.as_millis() as u64)),
        "{out:?}"
    );
    assert_eq!(r.handled.load(Ordering::SeqCst), handled_before, "no inner call reached the target");
    let names = trace_span_names(&trace);
    assert!(names.iter().any(|n| n == "rdm.node_rpc.request.reject.via-forward-budget-spent"), "the refusal is spanned in the origin's trace: {names:?}");
    assert!(!names.iter().any(|n| n == "rdm.node_rpc.request.serve.via-carried-inner"), "no inner call span: {names:?}");
}
