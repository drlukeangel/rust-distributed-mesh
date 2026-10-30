//! One trace across the demo's own node processes, under the consumer's own tracing subscriber
//! (Q295): the consumer installs its own OpenTelemetry layer (no RDM telemetry set-up), opens its own
//! span, and produces a record through the gateway inside it. RDM's client sends the caller's
//! traceparent; the gateway's serve span takes it as its parent, and the gateway's forward to the broker
//! primary carries it on: the gateway's and the broker's serve of the records op are in the
//! consumer's trace (node-rpc.md, the call's context). Rafka's own subscriber reads RDM's spans this way.
//!
//! CONTRACT: the consumer's span, the gateway's serve of `Records` (op `0x52`) and the broker's serve
//! of it share one trace id, and each node's serve span has a parent in that trace. Must NOT happen: a
//! node that starts a trace of its own for the consumer's call.

mod common;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use rdm_core::NodeKind;
use rshape_consumer::replica::{RecordsReply, RecordsRequest};
use serde_json::Value;
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

/// The op the replica's records travel on, as a span's `op` attribute renders it.
const RECORDS_OP: &str = "82";

#[test]
fn a_produce_through_the_gateway_is_one_trace_across_the_gateway_and_the_broker_under_the_consumers_own_subscriber() {
    // The consumer's own subscriber: its OpenTelemetry layer over an in-memory exporter.
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let subscriber = tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("rshape-consumer")));
    tracing::subscriber::set_global_default(subscriber).expect("the consumer's subscriber is the process's");

    common::run(async {
        let test = "a_produce_through_the_gateway_is_one_trace_across_the_gateway_and_the_broker_under_the_consumers_own_subscriber";
        let mut estate = common::estate("demo-one-trace", test, &[(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 1), (NodeKind::Broker, 1)]).await;

        // The consumer's span, and the produce through the gateway inside it.
        let call = tracing::info_span!(target: "rshape", "rshape.demo.records.create.via-gateway");
        let reply = common::records(&estate, "mesh1.gateway.1", &RecordsRequest::Produce { operation: "traced-1".into(), topic: "orders".into(), record: "order 1".into() }).instrument(call).await;
        assert!(matches!(reply, RecordsReply::Produced { .. }), "the gateway's produce is acknowledged: {reply:?}");
        provider.force_flush().unwrap();

        let consumer = exporter.get_finished_spans().unwrap().into_iter().find(|s| s.name == "rshape.demo.records.create.via-gateway").expect("the consumer's span");
        let trace = consumer.span_context.trace_id().to_string();
        // Every process's spans are whole once it exited.
        estate.stop().await;
        let spans = estate.spans();
        let in_trace: Vec<&Value> = spans.iter().filter(|s| s["trace_id"] == trace.as_str()).collect();
        for service in ["rshape-gateway", "rshape-broker"] {
            let serve = in_trace
                .iter()
                .find(|s| s["service"] == service && s["name"] == "rdm.node_rpc.request.serve.via-direct" && s["attributes"]["op"] == RECORDS_OP)
                .unwrap_or_else(|| panic!("{service}'s serve of the records op is in the consumer's trace {trace}: {:?}", in_trace.iter().map(|s| (&s["service"], &s["name"])).collect::<Vec<_>>()));
            assert!(!serve["parent_span_id"].as_str().unwrap_or_default().is_empty(), "{service}'s serve span has a parent in the consumer's trace: {serve}");
        }
    });
}
