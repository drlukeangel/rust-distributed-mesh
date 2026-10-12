//! A unit cell's own span capture: the product span pipeline (`rafka_mesh_telemetry::span_capture`)
//! into an in-memory exporter, behind a dispatcher the cell installs on its own thread (a
//! `#[tokio::test]` runtime is that one thread), so the spans it reads are its own and carry the
//! trace ids a cell reports. The pipeline enables every callsite before the dispatcher exists
//! (`rafka_mesh_telemetry::enable_callsites`), so a callsite another thread hit first still reaches it.

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
    capture: rafka_mesh_telemetry::SpanCapture,
    /// Install with `tracing::dispatcher::set_default` for the cell's span of work.
    pub dispatch: tracing::Dispatch,
}

impl Capture {
    /// A capture exporting under `service.name = cell`.
    pub(crate) fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
        let (capture, layer) = rafka_mesh_telemetry::span_capture(cell, exporter.clone());
        Self { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
    }

    /// Every finished span named `name`.
    pub(crate) fn named(&self, name: &str) -> Vec<Captured> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured { name: s.name.to_string(), trace_id: s.span_context.trace_id().to_string(), attributes: s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect() })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Capture;

    /// Opens the span every thread of the cell opens, at one callsite.
    fn open() {
        tracing::info_span!("capture.shared-callsite").in_scope(|| {});
    }

    /// CONTRACT: a capture receives the span its own thread opens at a callsite that another thread,
    /// one with no dispatcher of its own (another cell's), opened first. tracing-core caches a
    /// callsite's interest for the whole process when the callsite is first hit, and with one
    /// dispatcher registered it asks only the thread that hit it (tracing-core 0.1.36 callsite.rs,
    /// `Rebuilder::JustOne`), which answers never. Must NOT happen: the capture's own span lost
    /// because a cell on another thread hit its callsite first (the peer-mesh probe-order cell lost
    /// both its probe spans so in a whole-crate run while passing alone).
    #[test]
    fn a_capture_receives_its_own_span_at_a_callsite_another_thread_hit_first() {
        let capture = Capture::new("span-capture-shared-callsite");
        std::thread::spawn(open).join().unwrap();
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            open();
        }
        assert_eq!(capture.named("capture.shared-callsite").len(), 1, "the capture's own span reached it");
    }
}
