//! A unit cell's own span capture: an in-memory OTel exporter behind a dispatcher the cell installs
//! on its own thread (a `#[tokio::test]` runtime is that one thread), so the spans it reads are its
//! own and carry the trace ids a cell reports.

use opentelemetry::trace::TracerProvider as _;
use tracing_subscriber::layer::SubscriberExt as _;

/// One finished span: its name, trace id and attributes.
#[derive(Debug, Clone)]
pub(crate) struct Captured {
    /// The span name.
    pub name: String,
    /// The trace it belongs to.
    pub trace_id: String,
    /// Its attributes, rendered.
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// The capture and the dispatcher that feeds it.
pub(crate) struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    /// Install with `tracing::dispatcher::set_default` for the cell's span of work.
    pub dispatch: tracing::Dispatch,
}

impl Capture {
    /// A capture exporting under `service.name = cell`.
    pub(crate) fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", cell.to_string())]))
            .build();
        let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("rdm-unit-cell"));
        Self { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
    }

    /// Every finished span named `name`.
    pub(crate) fn named(&self, name: &str) -> Vec<Captured> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured { name: s.name.to_string(), trace_id: s.span_context.trace_id().to_string(), attributes: s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect() })
            .collect()
    }
}
