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
//! as long as [`crate::export::bounded`] allows (an unreachable collector never holds a cell), before
//! the dispatcher guard drops; it then takes the provider's tracer out of the dispatcher
//! ([`CaptureTracer`]), so the provider's end runs on the cell's thread and the dispatcher holds
//! nothing whose drop runs telemetry code. tracing-core drops a dispatcher wherever its last
//! reference goes, which can be inside its own dispatcher lock while another cell registers a
//! dispatcher (tracing-core 0.1.36 `callsite.rs:572`); a provider ended there emits an event whose
//! callsite registration waits on that lock, and every cell of the process waits with it (rdm#106).
//!
//! A held capture ([`EvidenceCapture::start_held`]) is for a cell that makes more calls than its
//! evidence can carry (a flood): each trace's spans wait until the cell decides the trace, and only
//! the traces it keeps reach the file and the collector.

use crate::pipeline::{span_processor, tracer_export_layer, CountingBatchProcessor, SpanLoss};
use opentelemetry::trace::TraceId;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider, Span, SpanData, SpanProcessor};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long [`EvidenceCapture::spans`] waits for the evidence file to hold every span ended
/// before it: the evidence processor's queue written to a local file.
const EVIDENCE_FLUSH_BOUND: Duration = Duration::from_secs(10);

/// The evidence processor, shared between the provider and its owner (a capture, a process's exit
/// flush), so the owner flushes the evidence file alone and never waits on the collector for it.
#[derive(Debug, Clone)]
pub(crate) struct SharedEvidence(pub(crate) Arc<CountingBatchProcessor>);

impl SpanProcessor for SharedEvidence {
    fn on_start(&self, span: &mut Span, cx: &opentelemetry::Context) {
        self.0.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        self.0.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
}

/// What a held capture holds: the spans of each trace its cell has not decided yet, and the traces it
/// kept or forgot. A span of a decided trace that ends after the decision follows it (a served call's
/// spans can end after its caller read the reply).
#[derive(Debug, Default)]
struct HeldState {
    waiting: HashMap<TraceId, Vec<SpanData>>,
    kept: HashSet<TraceId>,
    forgotten: HashSet<TraceId>,
}

/// A held capture's one processor: a span of a kept trace goes on to the capture's file and collector,
/// one of a forgotten trace is dropped, any other waits for its trace's decision.
#[derive(Debug, Clone)]
struct HeldTraces {
    state: Arc<Mutex<HeldState>>,
    sinks: Arc<Vec<Box<dyn SpanProcessor>>>,
}

impl HeldTraces {
    fn send(&self, span: SpanData) {
        for sink in self.sinks.iter() {
            sink.on_end(span.clone());
        }
    }

    /// Decide `trace` (its hex id): kept, its waiting spans go on and every later one follows;
    /// forgotten, they are dropped and every later one is too.
    fn decide(&self, trace: &str, keep: bool) {
        let id = TraceId::from_hex(trace).unwrap_or_else(|e| panic!("{trace:?} is not a trace id: {e}"));
        let waiting = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if keep {
                state.kept.insert(id);
            } else {
                state.forgotten.insert(id);
            }
            state.waiting.remove(&id).unwrap_or_default()
        };
        if keep {
            waiting.into_iter().for_each(|span| self.send(span));
        }
    }
}

impl SpanProcessor for HeldTraces {
    fn on_start(&self, span: &mut Span, cx: &opentelemetry::Context) {
        for sink in self.sinks.iter() {
            sink.on_start(span, cx);
        }
    }

    fn on_end(&self, span: SpanData) {
        let id = span.span_context.trace_id();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.kept.contains(&id) {
            drop(state);
            self.send(span);
        } else if !state.forgotten.contains(&id) {
            state.waiting.entry(id).or_default().push(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.sinks.iter().map(|sink| sink.force_flush()).fold(Ok(()), |all, r| all.and(r))
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.sinks.iter().map(|sink| sink.shutdown_with_timeout(timeout)).fold(Ok(()), |all, r| all.and(r))
    }
}

/// The tracer a capture's dispatcher holds: the capture provider's tracer until the capture's `Drop`
/// takes it out ([`CaptureTracer::take`]), then the tracer of a process-lifetime provider with no
/// processor, so a span opened on a thread still defaulting to the dispatcher goes nowhere.
///
/// Once taken, the dispatcher's drop runs no telemetry code: its registry holds no span then, since
/// every `tracing::Span` handle holds a reference to its dispatcher, so the last reference going
/// means no handle (and no started SDK span, which holds its own tracer) is left.
#[derive(Debug, Clone)]
struct CaptureTracer(Arc<std::sync::RwLock<Option<SdkTracer>>>);

impl CaptureTracer {
    fn new(tracer: SdkTracer) -> Self {
        Self(Arc::new(std::sync::RwLock::new(Some(tracer))))
    }

    /// The capture provider's tracer, leaving the dispatcher the provider-less one.
    fn take(&self) -> Option<SdkTracer> {
        self.0.write().unwrap_or_else(|e| e.into_inner()).take()
    }

    /// The tracer of a provider with no processor, built once and never dropped.
    fn taken() -> &'static SdkTracer {
        static TAKEN: std::sync::OnceLock<SdkTracer> = std::sync::OnceLock::new();
        TAKEN.get_or_init(|| {
            use opentelemetry::trace::TracerProvider as _;
            SdkTracerProvider::builder().build().tracer("rdm-evidence-capture-taken")
        })
    }
}

impl opentelemetry::trace::Tracer for CaptureTracer {
    type Span = opentelemetry_sdk::trace::Span;

    fn build_with_context(&self, builder: opentelemetry::trace::SpanBuilder, parent_cx: &opentelemetry::Context) -> Self::Span {
        match &*self.0.read().unwrap_or_else(|e| e.into_inner()) {
            Some(tracer) => tracer.build_with_context(builder, parent_cx),
            None => Self::taken().build_with_context(builder, parent_cx),
        }
    }
}

/// A test cell's span capture; see the module docs.
pub struct EvidenceCapture {
    provider: SdkTracerProvider,
    /// The tracer the dispatcher holds; `Drop` takes the provider's tracer out of it.
    tracer: CaptureTracer,
    evidence: SharedEvidence,
    loss: SpanLoss,
    path: PathBuf,
    dispatch: tracing::Dispatch,
    /// A held capture's decisions ([`EvidenceCapture::start_held`]); `None`: every span goes on.
    held: Option<HeldTraces>,
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
        Self::open(dir, service, otlp_endpoint, false)
    }

    /// [`EvidenceCapture::start`], holding each trace's spans until the cell decides the trace:
    /// [`EvidenceCapture::keep`] sends them on to the file and the collector, [`EvidenceCapture::forget`]
    /// drops them. A trace never decided reaches neither.
    pub fn start_held(dir: &Path, service: &str) -> std::io::Result<Self> {
        Self::open(dir, service, std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().as_deref(), true)
    }

    fn open(dir: &Path, service: &str, otlp_endpoint: Option<&str>, held: bool) -> std::io::Result<Self> {
        use tracing_subscriber::layer::SubscriberExt;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        crate::enable_callsites();
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = dir.join(format!("{service}.{}.{n}.spans.jsonl", crate::process_tag()));
        let evidence = SharedEvidence(Arc::new(span_processor("evidence", crate::JsonlSpanExporter::create_at(&path, service)?)));
        let loss = evidence.0.loss();
        let resource = crate::build_resource_with(service, vec![opentelemetry::KeyValue::new("rafka.evidence_file", path.display().to_string())]);
        let otlp = otlp_endpoint.map(|endpoint| span_processor("otlp", crate::build_exporter(endpoint.to_string())));
        let mut builder = SdkTracerProvider::builder().with_resource(resource.clone());
        let held = if held {
            // The held processor owns its sinks, so the provider's resource reaches the collector's
            // exporter here rather than through the provider.
            let mut sinks: Vec<Box<dyn SpanProcessor>> = vec![Box::new(evidence.clone())];
            if let Some(mut otlp) = otlp {
                otlp.set_resource(&resource);
                sinks.push(Box::new(otlp));
            }
            let h = HeldTraces { state: Arc::default(), sinks: Arc::new(sinks) };
            builder = builder.with_span_processor(h.clone());
            Some(h)
        } else {
            builder = builder.with_span_processor(evidence.clone());
            if let Some(otlp) = otlp {
                builder = builder.with_span_processor(otlp);
            }
            None
        };
        let provider = builder.build();
        let tracer = CaptureTracer::new({
            use opentelemetry::trace::TracerProvider as _;
            provider.tracer(service.to_string())
        });
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(tracer_export_layer(tracer.clone())).with(
            // Test tooling (Luke, 2026-10-19): with RDM_DEBUG_GOSSIP set, iroh-gossip's own DEBUG lines (the dial,
            // accept and close of every gossip connection) go to stderr on one clock with RDM's spans.
            std::env::var("RDM_DEBUG_GOSSIP").ok().map(|_| {
                use tracing_subscriber::Layer;
                tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_ansi(false).with_filter(tracing_subscriber::filter::filter_fn(|m| m.target().starts_with("iroh_gossip") && *m.level() <= tracing::Level::DEBUG))
            }),
        ));
        let default = tracing::dispatcher::set_default(&dispatch);
        Ok(Self { provider, tracer, evidence, loss, path, dispatch, held, _default: default })
    }

    /// Send the spans of `trace` (its hex id) on to the file and the collector, and every span of it
    /// that ends later. Panics on a capture not started held.
    pub fn keep(&self, trace: &str) {
        self.held.as_ref().expect("keep is a held capture's decision (EvidenceCapture::start_held)").decide(trace, true);
    }

    /// Drop the spans of `trace` (its hex id), and every span of it that ends later. Panics on a
    /// capture not started held.
    pub fn forget(&self, trace: &str) {
        self.held.as_ref().expect("forget is a held capture's decision (EvidenceCapture::start_held)").decide(trace, false);
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
                if let Err(e) = provider.force_flush() {
                    eprintln!("telemetry: the capture writing {path} failed to flush: {e}");
                }
                if let Err(e) = provider.shutdown() {
                    eprintln!("telemetry: the capture writing {path} failed to shut down: {e}");
                }
            })],
        );
        // The provider's tracer leaves the dispatcher here, on the cell's thread, so its end (with
        // the `provider` field, or the shutdown's thread past the bound) never runs where
        // tracing-core drops the dispatcher; see the module docs.
        drop(self.tracer.take());
    }
}
