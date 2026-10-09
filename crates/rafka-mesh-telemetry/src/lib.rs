//! Process telemetry for every mesh binary: OTLP span and log export, the JSONL evidence sink, and the
//! bounded drain a process runs before it exits.
//!
//! A process calls one `init_*` function ([`init_telemetry`], [`init_telemetry_for_cli`],
//! [`init_evidence_telemetry`]) and holds the returned [`TelemetryGuard`] for its lifetime. The
//! exporters run on their own thread ([`export`]), the stall watchdog is in [`watchdog`] and the log
//! adapter that turns `tracing` events into OTLP log records is in [`logs`].
#![deny(missing_docs)]


use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SimpleSpanProcessor, TracerProvider};
use tracing_subscriber::filter::{filter_fn, FilterExt};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub mod export;
pub mod logs;
mod log_sink;
pub mod watchdog;

use export::{ExportRuntime, WatchedLogs, WatchedSpans};

/// The exporters one `init_*` built: drained at most once, by whichever of the guard's `Drop` and
/// [`flush_before_exit`] comes first.
#[derive(Clone)]
struct Exporters {
    provider: TracerProvider,
    /// The OTLP log provider the log adapter emits through, when a collector is configured.
    logs: Option<opentelemetry_sdk::logs::LoggerProvider>,
    drained: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// The local evidence sink's writer, drained before anything waits on the collector.
    evidence: Option<std::sync::Arc<std::sync::Mutex<EvidenceWriter>>>,
}

impl Exporters {
    fn new(provider: TracerProvider, logs: Option<opentelemetry_sdk::logs::LoggerProvider>, evidence: Option<std::sync::Arc<std::sync::Mutex<EvidenceWriter>>>) -> Self {
        Self { provider, logs, drained: Default::default(), evidence }
    }

    /// Drain the local evidence sink (a file write; every ended span is already queued to it),
    /// then shut the providers down (the shutdown drains what is queued for the collector), each
    /// on a thread of its own: waited for at most [`export::DRAIN_BOUND`] together, and not at
    /// all while the collector is known to be down, because what is queued for it is dropped.
    fn drain(&self, what: &'static str) {
        if self.drained.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        if let Some(evidence) = &self.evidence {
            if let Ok(mut writer) = evidence.lock() {
                writer.shutdown();
            }
        }
        let provider = self.provider.clone();
        let mut jobs: Vec<Box<dyn FnOnce() + Send>> = vec![Box::new(move || {
            if let Err(e) = provider.shutdown() {
                eprintln!("telemetry shutdown error: {e}");
            }
        })];
        if let Some(logs) = self.logs.clone() {
            jobs.push(Box::new(move || {
                if let Err(e) = logs.shutdown() {
                    eprintln!("telemetry log shutdown error: {e}");
                }
            }));
        }
        export::bounded(what, jobs);
        if let Some(sink) = STDERR_SINK.get() {
            sink.flush();
        }
    }
}

/// The handle of the exporters one `init_*` call built. Dropping it drains the queued spans and
/// logs, bounded by [`export::DRAIN_BOUND`], and shuts the exporters down.
pub struct TelemetryGuard {
    exporters: Exporters,
}

/// What an intentional `process::exit` drains: the exporters the guard owns, which `exit` never drops.
/// The fmt log's writer, drained with the exporters: its lines are queued to a thread of their own.
static STDERR_SINK: std::sync::OnceLock<log_sink::LogSink> = std::sync::OnceLock::new();

static EXIT_FLUSH: std::sync::OnceLock<Exporters> = std::sync::OnceLock::new();

/// Drain and shut down the exporters before a deliberate `process::exit`, which skips the guard's
/// `Drop`: spans closed just before the exit (the reason it exits for) reach the evidence file and the
/// collector. A process that never initialised telemetry has nothing to drain. The wait is bounded
/// by [`export::DRAIN_BOUND`]: an unreachable collector never holds the exit.
pub fn flush_before_exit() {
    if let Some(exporters) = EXIT_FLUSH.get() {
        exporters.drain("the exit drain");
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.exporters.drain("the guard's drain");
    }
}

fn guard(provider: TracerProvider, logs: Option<opentelemetry_sdk::logs::LoggerProvider>, evidence: Option<std::sync::Arc<std::sync::Mutex<EvidenceWriter>>>) -> TelemetryGuard {
    let exporters = Exporters::new(provider, logs, evidence);
    let _ = EXIT_FLUSH.set(exporters.clone());
    TelemetryGuard { exporters }
}

/// Initialize OTLP tracing for long-running services (gateway, broker, compute,
/// registry, topology-ui). Uses BatchSpanProcessor with 200ms scheduled delay —
/// efficient batching, suitable for high span volume. Spans export within 200ms
/// of close, well before Jaeger's index window.
///
/// Also installs the W3C TraceContext propagator globally so cross-service trace
/// chains link via `traceparent` HTTP headers (e.g. a client → node-admin).
///
/// Returns a guard whose `Drop` flushes and shuts down the exporter.
pub fn init_telemetry(service_name: &str) -> TelemetryGuard {
    install_propagator();
    let (provider, tracer) = build_batch_provider(service_name);
    install_subscriber(tracer);
    guard(provider, None, None)
}

/// Initialize OTLP tracing for short-lived CLI processes.
/// Spans export in the background like a service's; the guard's `Drop` and
/// [`flush_before_exit`] drain what is queued, each waiting at most [`export::DRAIN_BOUND`],
/// so a CLI never waits on a collector that is down.
///
/// Also installs the W3C TraceContext propagator globally.
///
/// Returns a guard whose `Drop` flushes and shuts down the exporter.
pub fn init_telemetry_for_cli(service_name: &str) -> TelemetryGuard {
    install_propagator();
    let (provider, tracer) = build_batch_provider(service_name);
    install_subscriber(tracer);
    guard(provider, None, None)
}

fn install_propagator() {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
}

fn resolve_endpoint_and_service(service_name: &str) -> (String, String) {
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4316".to_string());
    let resolved_service_name = std::env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| service_name.to_string());
    (otlp_endpoint, resolved_service_name)
}

/// The process's resource: its service, and what tells this process apart from every other one
/// exporting beside it (many estates and births share one collector): its pid, and, where the
/// launcher set them, the node it runs as, its birth's data directory and its estate's evidence
/// directory.
fn build_resource(service_name: &str) -> opentelemetry_sdk::Resource {
    let mut attrs = vec![
        opentelemetry::KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_NAME, service_name.to_string()),
        opentelemetry::KeyValue::new("process.pid", std::process::id() as i64),
    ];
    for (env, key) in [("RDM_NODE_NAME", "rafka.node"), ("RDM_DATA_DIR", "rafka.data_dir"), ("RDM_EVIDENCE_DIR", "rafka.evidence_dir")] {
        if let Ok(v) = std::env::var(env) {
            if !v.is_empty() {
                attrs.push(opentelemetry::KeyValue::new(key, v));
            }
        }
    }
    opentelemetry_sdk::Resource::new(attrs)
}

/// The OTLP span exporter, built on the export runtime so its connection lives there, with
/// every request bounded by [`export::EXPORT_TIMEOUT`].
fn build_exporter(endpoint: String) -> WatchedSpans<SpanExporter> {
    let _export = ExportRuntime::get().enter();
    let inner = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.clone())
        .with_timeout(export::EXPORT_TIMEOUT)
        .build()
        .expect("OTLP span exporter");
    WatchedSpans::new(inner, &endpoint)
}

fn build_batch_provider(
    service_name: &str,
) -> (TracerProvider, opentelemetry_sdk::trace::Tracer) {
    let (endpoint, resolved) = resolve_endpoint_and_service(service_name);
    let exporter = build_exporter(endpoint);
    let resource = build_resource(&resolved);

    let processor = {
        let _export = ExportRuntime::get().enter();
        BatchSpanProcessor::builder(exporter, ExportRuntime::get().clone()).with_batch_config(batch_config()).build()
    };

    let provider = TracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();
    let tracer = provider.tracer(resolved);
    (provider, tracer)
}

/// The batch processors' bounds: 200 ms between exports, a full queue drops, and one export
/// request takes at most [`export::EXPORT_TIMEOUT`].
fn batch_config() -> opentelemetry_sdk::trace::BatchConfig {
    BatchConfigBuilder::default()
        .with_scheduled_delay(std::time::Duration::from_millis(200))
        .with_max_export_timeout(export::EXPORT_TIMEOUT)
        .build()
}

fn install_subscriber(tracer: opentelemetry_sdk::trace::Tracer) {
    // Per-layer filters. Both layers floor at INFO. stdout stays terse; OTLP
    // also floors at INFO (see the otel_filter note below for why DEBUG capture
    // was a memory leak under churn). RUST_LOG can still raise either layer via
    // EnvFilter::from_default_env() — a more-specific directive (e.g.
    // `iroh_quinn_proto=trace`) wins over the per-layer floor, so debug
    // visibility is opt-in per debugging session rather than always-on.
    let fmt_filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into())
        .and(filter_fn(export::admits_source));
    // OTLP layer floor is INFO, NOT debug. Under chaos churn, iroh/noq/gossip
    // emit a DEBUG firehose (binding / path selection / socket transports /
    // hyparview internals). tracing-opentelemetry's `on_event` appends every
    // captured event to the *currently-active* span's event buffer, which is
    // only freed when that span CLOSES. iroh's actor loops (magicsock, relay,
    // gossip-net) are long-lived `#[instrument]` spans that never close for the
    // process lifetime — so their event buffers grew without bound. dhat proved
    // it: with QUIC connections bounded (leak #1 fixed), tracing/otel still grew
    // 11.6 -> 29.2 MB (~1.6 MB/min, 68% of retained heap) via on_event, the
    // single 2 MB allocation being one long-lived span's event Vec. An INFO floor
    // drops the debug firehose at the filter, before it ever reaches on_event.
    //
    // SAFE: every span the topology UI consumes from Jaeger (node.ready,
    // heartbeat, peer.connected/discovered/disconnected, cross.peer_connected) is
    // emitted at INFO and survives. The per-frame frame.sent/received spans are
    // TRACE (already below the old DEBUG floor) and are NOT load-bearing — the
    // admin-ui edge-builder that once queried them is dead code behind an
    // unconditional return; edges derive from gossip mesh_id labels. The =off
    // directives below stay as belt-and-suspenders for any RUST_LOG=debug opt-in.
    let otel_filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into())
        .add_directive("h2=off".parse().expect("static directive"))
        .add_directive("hyper=off".parse().expect("static directive"))
        .add_directive("hyper_util=off".parse().expect("static directive"))
        .add_directive("tonic=off".parse().expect("static directive"))
        .add_directive("tower=off".parse().expect("static directive"))
        .add_directive("opentelemetry=off".parse().expect("static directive"))
        .add_directive("opentelemetry_sdk=off".parse().expect("static directive"))
        .add_directive("opentelemetry_otlp=off".parse().expect("static directive"))
        .and(filter_fn(export::admits_source));

    let otel_layer = OpenTelemetryLayer::new(tracer)
        .with_filter(otel_filter);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter))
        .with(otel_layer)
        .init();
}

/// W3C `traceparent` of the current tracing span, when an OpenTelemetry layer
/// is installed and the span context is valid. Carried inside protocol codecs
/// and Build intents so causality survives process and executor boundaries.
pub fn current_traceparent() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let cx = tracing::Span::current().context();
    let span = cx.span();
    let sc = span.span_context();
    sc.is_valid().then(|| format!("00-{}-{}-{:02x}", sc.trace_id(), sc.span_id(), sc.trace_flags().to_u8()))
}

/// Make `span` a child of the remote span named by `traceparent`.
/// A malformed value leaves `span` a root, never panics.
pub fn set_parent(span: &tracing::Span, traceparent: &str) {
    set_remote_parent(span, traceparent, None);
}

/// [`set_parent`] carrying the remote `tracestate` too; a `tracestate` that does not parse
/// is left out and the parent still applies.
pub fn set_remote_parent(span: &tracing::Span, traceparent: &str, tracestate: Option<&str>) {
    use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let parts: Vec<&str> = traceparent.split('-').collect();
    let [_, trace, span_id, flags] = parts[..] else { return };
    let (Ok(t), Ok(s), Ok(f)) = (TraceId::from_hex(trace), SpanId::from_hex(span_id), u8::from_str_radix(flags, 16)) else {
        return;
    };
    let state = tracestate
        .and_then(|ts| TraceState::from_key_value(ts.split(',').filter_map(|m| m.trim().split_once('=')).map(|(k, v)| (k.trim(), v.trim()))).ok())
        .unwrap_or_default();
    let sc = SpanContext::new(t, s, TraceFlags::new(f), true, state);
    if sc.is_valid() {
        span.set_parent(opentelemetry::Context::new().with_remote_span_context(sc));
    }
}

/// The W3C `tracestate` of the current tracing span, when one rides its context.
pub fn current_tracestate() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let cx = tracing::Span::current().context();
    let span = cx.span();
    let sc = span.span_context();
    let header = sc.trace_state().header();
    (sc.is_valid() && !header.is_empty()).then_some(header)
}

/// Writes every finished span as one JSON line to
/// `<RDM_EVIDENCE_DIR>/<service>.<pid>-<pid namespace inode>.spans.jsonl`:
/// a pid names one process only within its pid namespace, and every container has its own.
/// Causality is carried by `parent_span_id`; a consumer never infers it from
/// timestamps.
/// An evidence write at or beyond this length is named on stderr.
pub(crate) const EVIDENCE_WRITE_STALL: std::time::Duration = std::time::Duration::from_millis(50);

/// Writes evidence lines on its own OS thread: the thread that closes a span (a runtime worker)
/// only hands the lines over, so a write the kernel holds stalls this thread, never the runtime.
/// Lines are written in the order they were handed over; shutdown drains every line handed over
/// before it returns.
#[derive(Debug)]
struct EvidenceWriter {
    tx: Option<std::sync::mpsc::Sender<String>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl EvidenceWriter {
    fn start(mut file: std::fs::File) -> std::io::Result<Self> {
        use std::io::Write;
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let thread = std::thread::Builder::new().name("rdm-evidence".into()).spawn(move || {
            for out in rx {
                let started = std::time::Instant::now();
                if let Err(e) = file.write_all(out.as_bytes()) {
                    eprintln!("telemetry: evidence write failed: {e}");
                }
                let took = started.elapsed();
                if took >= EVIDENCE_WRITE_STALL {
                    eprintln!("telemetry: evidence write stalled {} ms ({} bytes) on the evidence thread", took.as_millis(), out.len());
                }
            }
        })?;
        Ok(Self { tx: Some(tx), thread: Some(thread) })
    }

    fn send(&self, out: String) -> Result<(), String> {
        self.tx.as_ref().ok_or_else(|| "the evidence writer is shut down".to_string())?.send(out).map_err(|e| e.to_string())
    }

    fn shutdown(&mut self) {
        self.tx.take();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[derive(Debug)]
pub(crate) struct JsonlSpanExporter {
    service: String,
    writer: std::sync::Arc<std::sync::Mutex<EvidenceWriter>>,
}

impl JsonlSpanExporter {
    pub fn create(dir: &std::path::Path, service: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let ns = std::fs::read_link("/proc/self/ns/pid").ok().and_then(|l| l.to_string_lossy().trim_start_matches("pid:[").trim_end_matches(']').parse::<u64>().ok()).unwrap_or(0);
        let path = dir.join(format!("{service}.{}-{ns}.spans.jsonl", std::process::id()));
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { service: service.to_string(), writer: std::sync::Arc::new(std::sync::Mutex::new(EvidenceWriter::start(file)?)) })
    }

    fn line(&self, s: &opentelemetry_sdk::export::trace::SpanData) -> String {
        let nanos = |t: std::time::SystemTime| t.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        let parent = if s.parent_span_id == opentelemetry::trace::SpanId::INVALID { String::new() } else { s.parent_span_id.to_string() };
        let attrs: serde_json::Map<String, serde_json::Value> =
            s.attributes.iter().map(|kv| (kv.key.to_string(), serde_json::Value::String(kv.value.as_str().into_owned()))).collect();
        serde_json::json!({
            "trace_id": s.span_context.trace_id().to_string(),
            "span_id": s.span_context.span_id().to_string(),
            "parent_span_id": parent,
            "name": s.name,
            "service": self.service,
            "start_unix_nano": nanos(s.start_time),
            "end_unix_nano": nanos(s.end_time),
            "attributes": attrs,
            // The log lines emitted inside this span, in order: what the span did, in its words.
            "events": s.events.iter().map(|e| serde_json::json!({
                "name": e.name,
                "time_unix_nano": nanos(e.timestamp),
                "attributes": e.attributes.iter().map(|kv| (kv.key.to_string(), serde_json::Value::String(kv.value.as_str().into_owned()))).collect::<serde_json::Map<_, _>>(),
            })).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

impl opentelemetry_sdk::export::trace::SpanExporter for JsonlSpanExporter {
    fn export(
        &mut self,
        batch: Vec<opentelemetry_sdk::export::trace::SpanData>,
    ) -> futures_util::future::BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let mut out = String::new();
        for s in &batch {
            out.push_str(&self.line(s));
            out.push('\n');
        }
        let r = self.writer.lock().map_err(|e| e.to_string()).and_then(|w| w.send(out));
        Box::pin(async move { r.map_err(|e| opentelemetry::trace::TraceError::Other(e.into())) })
    }

    fn shutdown(&mut self) {
        if let Ok(mut w) = self.writer.lock() {
            w.shutdown();
        }
    }
}

/// Telemetry for i143 binaries. Spans go to the JSONL evidence sink when
/// `RDM_EVIDENCE_DIR` is set and to OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT`
/// is set; with neither, only the fmt layer runs. Returns `None` when no
/// exporter is configured.
pub fn init_evidence_telemetry(service_name: &str) -> Option<TelemetryGuard> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    install_propagator();
    let service = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| service_name.to_string());
    let mut builder = TracerProvider::builder().with_resource(build_resource(&service));
    let mut any = false;
    let mut evidence = None;
    if let Ok(dir) = std::env::var("RDM_EVIDENCE_DIR") {
        match JsonlSpanExporter::create(std::path::Path::new(&dir), &service) {
            Ok(e) => {
                evidence = Some(e.writer.clone());
                builder = builder.with_span_processor(SimpleSpanProcessor::new(Box::new(e)));
                any = true;
            }
            Err(e) => eprintln!("telemetry: RDM_EVIDENCE_DIR={dir} is unusable: {e}"),
        }
    }
    let mut logs = None;
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        let export_rt = ExportRuntime::get();
        let _export = export_rt.enter();
        // The log adapter's provider: every log line to the collector's log store, beside spans.
        match opentelemetry_otlp::LogExporter::builder().with_tonic().with_endpoint(endpoint.clone()).with_timeout(export::EXPORT_TIMEOUT).build() {
            Ok(exporter) => {
                logs = Some(
                    opentelemetry_sdk::logs::LoggerProvider::builder()
                        .with_resource(build_resource(&service))
                        .with_batch_exporter(WatchedLogs::new(exporter, &endpoint), export_rt.clone())
                        .build(),
                );
            }
            Err(e) => eprintln!("telemetry: the OTLP log exporter for {endpoint} could not be built: {e}"),
        }
        let processor = BatchSpanProcessor::builder(build_exporter(endpoint), export_rt.clone()).with_batch_config(batch_config()).build();
        builder = builder.with_span_processor(processor);
        any = true;
    }
    let stderr_sink = log_sink::LogSink::start(std::io::stderr());
    let _ = STDERR_SINK.set(stderr_sink.clone());
    let fmt_filter = EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()).and(filter_fn(export::admits_source));
    if !any {
        let _ = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_writer(stderr_sink.clone()).with_filter(fmt_filter)).try_init();
        return None;
    }
    let provider = builder.build();
    let tracer = provider.tracer(service.clone());
    let otel_filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into())
        .add_directive("iroh=warn".parse().expect("static directive"))
        .add_directive("iroh_gossip=warn".parse().expect("static directive"))
        .add_directive("noq=warn".parse().expect("static directive"))
        .add_directive("noq_proto=warn".parse().expect("static directive"))
        .and(filter_fn(export::admits_source));
    let log_filter = log_env_filter().and(filter_fn(export::admits_source));
    use opentelemetry::logs::LoggerProvider as _;
    let log_layer = logs.as_ref().map(|p| logs::LogAdapter::new(p.logger("rafka-mesh")).with_filter(log_filter));
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(stderr_sink.clone()).with_filter(fmt_filter))
        .with(OpenTelemetryLayer::new(tracer).with_filter(otel_filter))
        .with(log_layer)
        .try_init();
    let _ = watchdog::spawn();
    Some(guard(provider, logs, evidence))
}

/// What the OTLP log bridge exports: INFO and above from RDM, WARN and above from iroh, iroh-gossip
/// and noq. A crate's DEBUG firehose (iroh-gossip's HyParView `rg3` diagnostics among them: one
/// event per neighbour message per node) never reaches the collector by default; RUST_LOG can
/// still admit it for one run.
pub(crate) fn log_env_filter() -> EnvFilter {
    EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into())
        .add_directive("iroh=warn".parse().expect("static directive"))
        .add_directive("iroh_gossip=warn".parse().expect("static directive"))
        .add_directive("noq=warn".parse().expect("static directive"))
        .add_directive("noq_proto=warn".parse().expect("static directive"))
}

