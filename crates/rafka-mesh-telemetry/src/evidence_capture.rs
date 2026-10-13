//! A test cell's own span evidence, scoped to the cell's threads: the product span pipeline in
//! front of a JSONL file only this capture writes, and in front of the collector when one is named.
//!
//! One test binary is one process, so a cell never installs process-wide telemetry: no global
//! subscriber and no global propagator. The capture is the default dispatcher of the thread that
//! starts it (a `set_default` guard) and of every thread of the runtime it builds for the cell
//! ([`EvidenceCapture::run`]); a span another cell's thread opens never reaches it. The file is the
//! capture's own (`<service>.<pid>-<pid namespace inode>.<n>.spans.jsonl`), so two cells of one
//! process never read each other's spans. Every span also goes to the collector at the OTLP
//! endpoint, when one is set: the cell's trace is its deliverable.
//!
//! The capture owns its provider. Its `Drop` flushes the provider and then shuts it down, waiting
//! at most [`crate::export::DRAIN_BOUND`] (an unreachable collector never holds a cell), before
//! the dispatcher guard drops.

use crate::pipeline::{span_export_layer, span_processor, CountingBatchProcessor, SpanLoss};
use opentelemetry::trace::TraceResult;
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::trace::{Span, SpanProcessor, TracerProvider};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// How long [`EvidenceCapture::spans`] waits for the evidence file to hold every span ended
/// before it: the evidence processor's queue written to a local file.
const EVIDENCE_FLUSH_BOUND: Duration = Duration::from_secs(10);

/// The evidence processor, shared between the provider and the capture, so the capture flushes
/// the evidence file alone and never waits on the collector to read it.
#[derive(Debug, Clone)]
struct SharedEvidence(Arc<CountingBatchProcessor>);

impl SpanProcessor for SharedEvidence {
    fn on_start(&self, span: &mut Span, cx: &opentelemetry::Context) {
        self.0.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        self.0.on_end(span);
    }

    fn force_flush(&self) -> TraceResult<()> {
        self.0.force_flush()
    }

    fn shutdown(&self) -> TraceResult<()> {
        self.0.shutdown()
    }
}

/// A test cell's span capture; see the module docs.
pub struct EvidenceCapture {
    provider: TracerProvider,
    evidence: SharedEvidence,
    loss: SpanLoss,
    path: PathBuf,
    dispatch: tracing::Dispatch,
    /// Declared last: it drops after `Drop::drop` has flushed and shut the provider down.
    _default: tracing::dispatcher::DefaultGuard,
}

impl EvidenceCapture {
    /// The capture of one cell, writing under `dir` as `service`, and to the collector named by
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` when the process was started with one. It is the calling
    /// thread's default dispatcher until it drops.
    pub fn start(dir: &Path, service: &str) -> std::io::Result<Self> {
        Self::start_with_collector(dir, service, std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().as_deref())
    }

    /// [`EvidenceCapture::start`], exporting to the collector at `otlp_endpoint` (OTLP/HTTP base
    /// URL) when it is `Some`, whatever the process environment says.
    pub fn start_with_collector(dir: &Path, service: &str, otlp_endpoint: Option<&str>) -> std::io::Result<Self> {
        use tracing_subscriber::layer::SubscriberExt;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        crate::enable_callsites();
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _ = n;
        let path = dir.join(format!("{service}.{}.spans.jsonl", crate::process_tag()));
        let evidence = SharedEvidence(Arc::new(span_processor("evidence", crate::JsonlSpanExporter::create_at(&path, service)?)));
        let loss = evidence.0.loss();
        let mut resource = crate::build_resource(service);
        resource = resource.merge(&opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("rafka.evidence_file", path.display().to_string())]));
        let mut builder = TracerProvider::builder().with_resource(resource).with_span_processor(evidence.clone());
        let _ = otlp_endpoint;
        if let Some(endpoint) = None::<&str> {
            builder = builder.with_span_processor(span_processor("otlp", crate::build_exporter(endpoint.to_string())));
        }
        let provider = builder.build();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(span_export_layer(&provider, service)));
        let _ = tracing::dispatcher::set_global_default(dispatch.clone());
        let default = tracing::dispatcher::set_default(&dispatch);
        Ok(Self { provider, evidence, loss, path, dispatch, _default: default })
    }

    /// Run the cell's `future` to completion on a multi-thread runtime of `worker_threads` workers
    /// (timers and I/O enabled), every one of whose threads has this capture as its default
    /// dispatcher; the future itself runs on the calling thread, whose default it already is.
    pub fn run<F: std::future::Future>(&self, worker_threads: usize, future: F) -> F::Output {
        let dispatch = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&dispatch)))
            .build()
            .unwrap_or_else(|e| panic!("the cell's runtime of {worker_threads} workers could not be built: {e}"));
        rt.block_on(future)
    }

    /// The capture's own evidence file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Every span ended before this call, as the evidence file holds it: one JSON value per line,
    /// in the order written. `Err` names what failed: the flush not returning within its bound, the
    /// flush's own error, a span ended before the call that never reached the file, the file
    /// unreadable, or a line that is not JSON.
    pub fn spans(&self) -> Result<Vec<serde_json::Value>, String> {
        let owed = self.loss.handed();
        let (tx, rx) = std::sync::mpsc::channel();
        let evidence = self.evidence.clone();
        std::thread::Builder::new()
            .name("evidence-capture-flush".into())
            .spawn(move || {
                let _ = tx.send(evidence.force_flush());
            })
            .map_err(|e| format!("the flush of {} could not start a thread: {e}", self.path.display()))?;
        rx.recv_timeout(EVIDENCE_FLUSH_BOUND)
            .map_err(|_| format!("the flush of {} did not return within {} ms", self.path.display(), EVIDENCE_FLUSH_BOUND.as_millis()))?
            .map_err(|e| format!("the flush of {} failed: {e}", self.path.display()))?;
        if let n @ 1.. = owed.saturating_sub(self.loss.reached()) {
            return Err(format!("{n} of the {owed} spans ended before the read never reached {}", self.path.display()));
        }
        let text = std::fs::read_to_string(&self.path).map_err(|e| format!("{} could not be read: {e}", self.path.display()))?;
        text.lines()
            .enumerate()
            .map(|(i, line)| serde_json::from_str(line).map_err(|e| format!("{} line {} is not JSON ({e}): {line}", self.path.display(), i + 1)))
            .collect()
    }
}

impl Drop for EvidenceCapture {
    fn drop(&mut self) {
        let provider = self.provider.clone();
        let path = self.path.display().to_string();
        crate::export::bounded(
            "the evidence capture's shutdown",
            vec![Box::new(move || {
                for r in provider.force_flush() {
                    if let Err(e) = r {
                        eprintln!("telemetry: the capture writing {path} failed to flush: {e}");
                    }
                }
                if let Err(e) = provider.shutdown() {
                    eprintln!("telemetry: the capture writing {path} failed to shut down: {e}");
                }
            })],
        );
    }
}
