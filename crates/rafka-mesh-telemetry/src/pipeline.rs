//! The one span pipeline every span consumer is built from: product (OTLP, the JSONL evidence
//! sink) and the test rigs that read spans back.
//!
//! A consumer supplies an exporter and gets, exactly as product has it, the span filter
//! ([`crate::span_export_filter`] plus [`crate::export::admits_source`], which keep iroh, noq and
//! the other internals off) and a `BatchSpanProcessor` with [`crate::batch_config`] on the export
//! runtime. Ending a span is one bounded-queue `try_send` on the ending thread: no lock shared
//! with other threads, no formatting, no I/O. Formatting and writing run on the export thread.
//!
//! The queue is bounded and the SDK drops a span when it is full (naming it once). Every span
//! handed to the processor is counted, and so is every span that reached the exporter; the
//! difference is reported by name when the processor shuts down and is readable as [`SpanLoss`].

use opentelemetry::trace::{TraceError, TraceResult};
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::trace::{BatchSpanProcessor, Span, SpanProcessor, TracerProvider};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Spans handed to a processor against spans that reached its exporter.
#[derive(Debug, Default)]
struct Tally {
    handed: AtomicU64,
    reached: AtomicU64,
}

/// Wraps an exporter to count the spans that reach it (a batch whose export timed out is dropped
/// by the SDK before it completes and is not counted: it is loss).
struct CountedExporter<E> {
    inner: E,
    tally: Arc<Tally>,
}

impl<E: std::fmt::Debug> std::fmt::Debug for CountedExporter<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<E: SpanExporter> SpanExporter for CountedExporter<E> {
    fn export(&mut self, batch: Vec<SpanData>) -> futures_util::future::BoxFuture<'static, ExportResult> {
        let n = batch.len() as u64;
        let fut = self.inner.export(batch);
        let tally = self.tally.clone();
        Box::pin(async move {
            let r = fut.await;
            tally.reached.fetch_add(n, Ordering::SeqCst);
            r
        })
    }

    fn shutdown(&mut self) {
        self.inner.shutdown();
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// How many spans a processor lost between `on_end` and its exporter.
#[derive(Debug, Clone)]
pub struct SpanLoss {
    name: String,
    tally: Arc<Tally>,
}

impl SpanLoss {
    /// Spans handed to the processor and not (yet) seen by its exporter. Zero once the processor
    /// has flushed with nothing dropped.
    pub fn lost(&self) -> u64 {
        self.tally.handed.load(Ordering::SeqCst).saturating_sub(self.tally.reached.load(Ordering::SeqCst))
    }

    /// Spans that reached the exporter so far.
    pub fn reached(&self) -> u64 {
        self.tally.reached.load(Ordering::SeqCst)
    }

    /// Spans handed to the processor so far.
    pub fn handed(&self) -> u64 {
        self.tally.handed.load(Ordering::SeqCst)
    }
}

/// The product span processor; see the module docs.
#[derive(Debug, Clone)]
pub struct CountingBatchProcessor {
    inner: Arc<BatchSpanProcessor<crate::export::ExportRuntime>>,
    loss: SpanLoss,
}

impl CountingBatchProcessor {
    /// The loss counters of this processor.
    pub fn loss(&self) -> SpanLoss {
        self.loss.clone()
    }
}

impl SpanProcessor for CountingBatchProcessor {
    fn on_start(&self, span: &mut Span, cx: &opentelemetry::Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        if span.span_context.is_sampled() {
            self.loss.tally.handed.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.on_end(span);
    }

    /// Blocks until the queue has drained into the exporter and returns the SDK's answer. It has
    /// no timeout of its own: call it on a path that has a deadline.
    fn force_flush(&self) -> TraceResult<()> {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> TraceResult<()> {
        let r = self.inner.shutdown();
        let lost = self.loss.lost();
        if lost > 0 {
            eprintln!("telemetry: {lost} of {} spans handed to the {} sink never reached its exporter (queue full or export timed out)", self.loss.handed(), self.loss.name);
        }
        r
    }

    fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
}

/// The product processor around `exporter`; `name` labels the loss report.
pub fn span_processor<E: SpanExporter + 'static>(name: &str, exporter: E) -> CountingBatchProcessor {
    let tally = Arc::new(Tally::default());
    let rt = crate::export::ExportRuntime::get();
    let _export = rt.enter();
    let processor = BatchSpanProcessor::builder(CountedExporter { inner: exporter, tally: tally.clone() }, rt.clone()).with_batch_config(crate::batch_config()).build();
    CountingBatchProcessor { inner: Arc::new(processor), loss: SpanLoss { name: name.to_string(), tally } }
}

/// The product `tracing` layer that feeds `provider`, behind product's span filter.
pub fn span_export_layer<S>(provider: &TracerProvider, service: &str) -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::filter::{filter_fn, FilterExt};
    use tracing_subscriber::Layer;
    let filter = crate::span_export_filter().and(filter_fn(crate::export::admits_source));
    tracing_opentelemetry::OpenTelemetryLayer::new(provider.tracer(service.to_string())).with_filter(filter)
}

/// Spans captured into a test's exporter through the product pipeline, installed as the process's
/// global subscriber.
pub struct SpanCapture {
    provider: TracerProvider,
    loss: SpanLoss,
}

/// Install the global subscriber of a test executable: product's filter and processor in front of
/// `exporter`. Fails when a global subscriber is already set.
pub fn install_span_capture<E: SpanExporter + 'static>(service: &str, exporter: E) -> Result<SpanCapture, String> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let processor = span_processor("capture", exporter);
    let loss = processor.loss();
    let provider = TracerProvider::builder().with_span_processor(processor).build();
    tracing_subscriber::registry().with(span_export_layer(&provider, service)).try_init().map_err(|e| format!("a global subscriber is already installed: {e}"))?;
    Ok(SpanCapture { provider, loss })
}

impl SpanCapture {
    /// Every span ended before this call has reached the exporter when it returns `Ok` (spans other threads end during the call are not owed by it). The SDK's
    /// flush has no timeout of its own, so the wait is bounded here and a flush that does not
    /// return, returns an error, or leaves a span behind is an `Err` naming which.
    pub fn flush(&self, bound: Duration) -> Result<(), String> {
        // Spans other threads end while the flush runs are not owed by it: only those handed over
        // before it began.
        let owed = self.loss.handed();
        let (tx, rx) = std::sync::mpsc::channel();
        let provider = self.provider.clone();
        std::thread::Builder::new()
            .name("span-capture-flush".into())
            .spawn(move || {
                let _ = tx.send(provider.force_flush());
            })
            .map_err(|e| format!("the flush thread could not be spawned: {e}"))?;
        let results = rx.recv_timeout(bound).map_err(|_| format!("the span flush did not return within {} ms", bound.as_millis()))?;
        for r in results {
            r.map_err(|e: TraceError| format!("the span flush failed: {e}"))?;
        }
        match owed.saturating_sub(self.loss.reached()) {
            0 => Ok(()),
            n => Err(format!("{n} of the {owed} spans ended before the flush never reached the exporter")),
        }
    }
}
