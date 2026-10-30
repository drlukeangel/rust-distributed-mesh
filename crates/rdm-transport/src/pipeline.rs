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

use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{SdkTracerProvider, Span, SpanData, SpanExporter, SpanProcessor};
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
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let n = batch.len() as u64;
        let r = self.inner.export(batch).await;
        self.tally.reached.fetch_add(n, Ordering::SeqCst);
        r
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
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

/// The product span processor; see the module docs. `inner` is the processor it counts in front
/// of: product's `BatchSpanProcessor` on the export runtime ([`span_processor`]).
#[derive(Debug)]
pub struct CountingBatchProcessor {
    inner: Box<dyn SpanProcessor>,
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

    /// The span is counted once the inner processor has taken it (queued, or refused because its
    /// queue is full: a loss the count then names). Counting it before would let a flush begun in
    /// between owe a span its flush request is queued ahead of, and report it lost.
    fn on_end(&self, span: SpanData) {
        let sampled = span.span_context.is_sampled();
        self.inner.on_end(span);
        if sampled {
            self.loss.tally.handed.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Blocks until the queue has emptied into the exporter and returns the SDK's answer. It has
    /// no timeout of its own: call it on a path that has a deadline.
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        let r = self.inner.shutdown_with_timeout(timeout);
        let lost = self.loss.lost();
        if lost > 0 {
            eprintln!("telemetry: {lost} of {} spans handed to the {} sink never reached its exporter (queue full or export timed out)", self.loss.handed(), self.loss.name);
        }
        r
    }

    /// The provider hands its resource to each processor as it is built; the inner processor passes it
    /// on to its exporter, which sends it with every request.
    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// The product processor around `exporter`; `name` labels the loss report.
pub fn span_processor<E: SpanExporter + 'static>(name: &str, exporter: E) -> CountingBatchProcessor {
    let tally = Arc::new(Tally::default());
    let rt = crate::export::ExportRuntime::get();
    let _export = rt.enter();
    let processor = BatchSpanProcessor::builder(CountedExporter { inner: exporter, tally: tally.clone() }, rt.clone()).with_batch_config(crate::batch_config()).build();
    CountingBatchProcessor { inner: Box::new(processor), loss: SpanLoss { name: name.to_string(), tally } }
}

/// The product `tracing` layer that feeds `provider`, behind product's span filter.
pub fn span_export_layer<S>(provider: &SdkTracerProvider, service: &str) -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use opentelemetry::trace::TracerProvider as _;
    tracer_export_layer(provider.tracer(service.to_string()))
}

/// [`span_export_layer`] in front of `tracer`, any tracer the SDK's spans come from.
pub(crate) fn tracer_export_layer<S, T>(tracer: T) -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    T: opentelemetry::trace::Tracer + 'static,
    T::Span: Send + Sync,
{
    use tracing_subscriber::filter::{filter_fn, FilterExt};
    use tracing_subscriber::Layer;
    let filter = crate::span_export_filter().and(filter_fn(crate::export::admits_source));
    tracing_opentelemetry::OpenTelemetryLayer::new(tracer).with_filter(filter)
}

/// Spans captured into a test's exporter through the product pipeline, installed as the process's
/// global subscriber.
pub struct SpanCapture {
    provider: SdkTracerProvider,
    loss: SpanLoss,
}

/// Install the global subscriber of a test executable: product's filter and processor in front of
/// `exporter`. Fails when a global subscriber is already set.
pub fn install_span_capture<E: SpanExporter + 'static>(service: &str, exporter: E) -> Result<SpanCapture, String> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let (capture, layer) = span_capture(service, exporter);
    tracing_subscriber::registry().with(layer).try_init().map_err(|e| format!("a global subscriber is already installed: {e}"))?;
    Ok(capture)
}

/// Make every callsite's interest come from every registered dispatcher, once for the whole process:
/// the one call a test makes before it installs a capture dispatcher of its own on its own threads
/// ([`span_capture`] makes it itself).
///
/// tracing-core caches a callsite's interest for the whole process when the callsite is first hit.
/// With exactly one dispatcher registered it asks only the thread that hit it (tracing-core 0.1.36
/// `callsite.rs`, `Rebuilder::JustOne`), and a thread with no dispatcher of its own (another test's)
/// answers never, so a capture would lose every span of a callsite such a thread hit first. A second
/// dispatcher, registered here and installed as no thread's default, makes tracing take every
/// callsite's interest from every live dispatcher; each span is then offered to the dispatcher of the
/// thread that opens it. It is never a default: a process-wide default would hand a worker thread a
/// registry that has not got the span a capturing test's endpoint stored as an explicit parent, and
/// creating the child would panic.
pub fn enable_callsites() {
    static HELD: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    HELD.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
}

/// The same capture as [`install_span_capture`], not installed: the layer goes into a subscriber the
/// caller builds (a test that dispatches to its own subscriber on every thread of its own runtime,
/// so two tests in one executable never share spans). Every callsite is enabled first
/// ([`enable_callsites`]), so the capture receives each span its own threads open.
pub fn span_capture<S, E>(service: &str, exporter: E) -> (SpanCapture, impl tracing_subscriber::Layer<S>)
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    E: SpanExporter + 'static,
{
    enable_callsites();
    let processor = span_processor("capture", exporter);
    let loss = processor.loss();
    let provider = SdkTracerProvider::builder().with_span_processor(processor).build();
    let layer = span_export_layer(&provider, service);
    (SpanCapture { provider, loss }, layer)
}

impl SpanCapture {
    /// Every span ended before this call has reached the exporter when it returns `Ok` (spans other threads end during the call are not owed by it). The SDK's
    /// flush has no timeout of its own, so the wait is bounded here and a flush that does not
    /// return, returns an error, or leaves a span behind is an `Err` naming which.
    pub fn flush(&self, bound: Duration) -> Result<(), String> {
        // Spans other threads end while the flush runs are not owed by it: only those handed over
        // before it began. A span is counted only once it is queued (`on_end`), so every span
        // counted here is in the queue ahead of the flush request below, which the queue serves in
        // order.
        let owed = self.loss.handed();
        let (tx, rx) = std::sync::mpsc::channel();
        let provider = self.provider.clone();
        std::thread::Builder::new()
            .name("span-capture-flush".into())
            .spawn(move || {
                let _ = tx.send(provider.force_flush());
            })
            .map_err(|e| format!("the flush thread could not be spawned: {e}"))?;
        let result = rx.recv_timeout(bound).map_err(|_| format!("the span flush did not return within {} ms", bound.as_millis()))?;
        result.map_err(|e: OTelSdkError| format!("the span flush failed: {e}"))?;
        match owed.saturating_sub(self.loss.reached()) {
            0 => Ok(()),
            n => Err(format!("{n} of the {owed} spans ended before the flush never reached the exporter")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{Tracer, TracerProvider as _};
    use std::sync::{mpsc, Mutex};

    /// A processor in front of product's batch processor that holds the thread ending a span inside
    /// `on_end`, before the span is handed on to the batch queue, until the cell releases it.
    #[derive(Debug)]
    struct HeldEnd<P> {
        inner: P,
        entered: Mutex<mpsc::Sender<()>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl<P: SpanProcessor> SpanProcessor for HeldEnd<P> {
        fn on_start(&self, span: &mut Span, cx: &opentelemetry::Context) {
            self.inner.on_start(span, cx);
        }

        fn on_end(&self, span: SpanData) {
            let _ = self.entered.lock().unwrap().send(());
            let _ = self.release.lock().unwrap().recv();
            self.inner.on_end(span);
        }

        fn force_flush(&self) -> OTelSdkResult {
            self.inner.force_flush()
        }

        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.inner.shutdown_with_timeout(timeout)
        }
    }

    /// CONTRACT: a flush owes exactly the spans whose end returned before it began. A span another
    /// thread is still ending when the flush starts (held inside the processor, not yet in the
    /// batch queue) is not owed by it, so the flush answers Ok; once that end returns, the next flush
    /// delivers the span to the exporter. Must NOT happen: the flush counting the span as ended
    /// before it and reporting it lost because it was not yet queued when the flush request was
    /// (the node-rpc gate's "1 of the 4286 spans ended before the flush never reached the exporter").
    #[test]
    fn a_span_still_ending_when_a_flush_begins_is_not_owed_by_that_flush() {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let tally = Arc::new(Tally::default());
        let rt = crate::export::ExportRuntime::get();
        let batch = {
            let _export = rt.enter();
            BatchSpanProcessor::builder(CountedExporter { inner: exporter.clone(), tally: tally.clone() }, rt.clone()).with_batch_config(crate::batch_config()).build()
        };
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held = HeldEnd { inner: batch, entered: Mutex::new(entered_tx), release: Mutex::new(release_rx) };
        let processor = CountingBatchProcessor { inner: Box::new(held), loss: SpanLoss { name: "held".into(), tally } };
        let loss = processor.loss();
        let provider = SdkTracerProvider::builder().with_span_processor(processor).build();
        let capture = SpanCapture { provider: provider.clone(), loss: loss.clone() };
        let tracer = provider.tracer("held-end");
        let ending = std::thread::spawn(move || drop(tracer.start("rdm.test.span.end.held")));
        entered_rx.recv_timeout(Duration::from_secs(10)).expect("the span's end reached the processor");
        let during = capture.flush(Duration::from_secs(10));
        // Release before asserting, so a red does not leave the ending thread held.
        release_tx.send(()).unwrap();
        ending.join().unwrap();
        let after = capture.flush(Duration::from_secs(10));
        assert_eq!(during, Ok(()), "a span still ending when the flush began is not owed by that flush");
        assert_eq!(after, Ok(()), "the span whose end returned is delivered by the next flush");
        assert_eq!(exporter.get_finished_spans().unwrap().iter().map(|s| s.name.to_string()).collect::<Vec<_>>(), vec!["rdm.test.span.end.held".to_string()]);
        assert_eq!((loss.handed(), loss.reached()), (1, 1), "the one span was handed over and reached the exporter");
    }
}
