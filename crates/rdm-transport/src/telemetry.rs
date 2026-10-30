//! Process telemetry for every mesh binary: OTLP span and log export, the JSONL evidence sink, and the
//! bounded flush a process runs before it exits.
//!
//! A process calls one `init_*` function ([`init_telemetry`], [`init_telemetry_for_cli`],
//! [`init_evidence_telemetry`]) and holds the returned [`TelemetryGuard`] for its lifetime. The
//! exporters run on their own thread ([`export`]), the stall watchdog is in [`watchdog`] and the log
//! adapter that turns `tracing` events into OTLP log records is in [`logs`].


use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::hyper::HyperClient;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{BatchConfigBuilder, SdkTracerProvider};
use tracing_subscriber::filter::{filter_fn, FilterExt};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use crate::{export, log_sink, logs, transport_diag, watchdog};
pub use rdm_core::{TRANSPORT_DIAG_FILE, TRANSPORT_DIAG_TARGET};
pub use crate::evidence_capture::EvidenceCapture;
pub use crate::pipeline::{enable_callsites, install_span_capture, span_capture, span_export_layer, span_processor, CountingBatchProcessor, SpanCapture, SpanLoss};

use export::{ExportRuntime, WatchedLogs, WatchedSpans};

/// The exporters one `init_*` built: flushed at most once, by whichever of the guard's `Drop` and
/// [`flush_before_exit`] comes first.
#[derive(Clone)]
struct Exporters {
    provider: SdkTracerProvider,
    /// The OTLP log provider the log adapter emits through, when a collector is configured.
    logs: Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
    /// The evidence file's processor, when the process writes one: flushed first and on its own.
    evidence: Option<crate::evidence_capture::SharedEvidence>,
    flushed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Exporters {
    fn new(provider: SdkTracerProvider, logs: Option<opentelemetry_sdk::logs::SdkLoggerProvider>, evidence: Option<crate::evidence_capture::SharedEvidence>) -> Self {
        Self { provider, logs, evidence, flushed: Default::default() }
    }

    /// Flush the evidence file, then shut the providers down (the shutdown sends what is queued for
    /// the collector), each on a thread of its own: waited for at most [`export::EXIT_FLUSH_BOUND`]
    /// together, the collector's part not at all while the collector is known to be down, because
    /// what is queued for it is dropped. The bound holds the code's own wait (i146 Q60): it returns at
    /// the bound with any loss named, and a thread still inside a filesystem call is the kernel's to
    /// end.
    fn flush_before_exit(&self, what: &'static str) {
        if self.flushed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        // One deadline for the whole flush. The evidence file first, whatever the collector's state:
        // its spans are this process's own record, and no collector outage may cost them (i146 Q42).
        // Its first export opens the file.
        let deadline = std::time::Instant::now() + export::EXIT_FLUSH_BOUND;
        if let Some(evidence) = self.evidence.clone() {
            if !export::ExportRuntime::running() {
                export::name_on_stderr(format!("telemetry: {what}: the export thread has not begun running; the evidence flush waits for it within {} ms", export::EXIT_FLUSH_BOUND.as_millis()));
            }
            let loss = evidence.0.loss();
            let flushed = export::bounded_until(
                "the evidence flush",
                vec![Box::new(move || {
                    if let Err(e) = opentelemetry_sdk::trace::SpanProcessor::force_flush(&evidence) {
                        eprintln!("telemetry: the evidence flush failed: {e}");
                    }
                })],
                false,
                deadline,
            );
            // The bound is the contract: a flush the disk holds past it ends there, its loss named
            // (Luke 2026-10-17: a named, bounded loss, never an exit that waits on the disk).
            if !flushed {
                export::name_on_stderr(format!(
                    "telemetry: the evidence flush ended at its bound ({} ms): {} of {} spans not confirmed written to the evidence file",
                    export::EXIT_FLUSH_BOUND.as_millis(),
                    loss.lost(),
                    loss.handed()
                ));
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
                // Flushed first: opentelemetry_sdk 0.31's async-runtime BatchLogProcessor implements
                // `shutdown` but not `shutdown_with_timeout`, which the provider's shutdown calls, so a
                // shutdown alone exports nothing still queued.
                if let Err(e) = logs.force_flush() {
                    eprintln!("telemetry log flush error: {e}");
                }
                if let Err(e) = logs.shutdown() {
                    eprintln!("telemetry log shutdown error: {e}");
                }
            }));
        }
        if !export::bounded_until(what, jobs, true, deadline) {
            export::name_abandoned(what);
        }
        flush_log_sinks(deadline);
    }
}

/// The handle of the exporters one `init_*` call built. Dropping it flushes the queued spans and
/// logs, bounded by `export::EXIT_FLUSH_BOUND`, and shuts the exporters down.
pub struct TelemetryGuard {
    exporters: Exporters,
}

/// The fmt log's writer, flushed with the exporters: its lines are queued to a thread of their own.
static STDERR_SINK: std::sync::OnceLock<log_sink::LogSink> = std::sync::OnceLock::new();

/// What an intentional `process::exit` flushes: the exporters the guard owns, which `exit` never drops.
static EXIT_FLUSH: std::sync::OnceLock<Exporters> = std::sync::OnceLock::new();

/// The transport diagnostic file's writer ([`transport_diag`]), flushed with the fmt log's.
static DIAG_SINK: std::sync::OnceLock<log_sink::LogSink> = std::sync::OnceLock::new();

/// Flush both logs within the same process deadline as the evidence and collector. A flush that ends at
/// its bound is named through [`export::name_on_stderr`]: when the unconfirmed log is stderr itself, a
/// direct `eprintln!` would wait on the very writer that holds it.
fn flush_log_sinks(deadline: std::time::Instant) {
    for (name, sink) in [("stderr log", STDERR_SINK.get()), ("transport diagnostic log", DIAG_SINK.get())] {
        if let Some(sink) = sink {
            if !sink.flush_until(deadline) {
                export::name_on_stderr(format!("telemetry: the {name} flush ended at its bound ({} ms); queued lines not confirmed written", export::EXIT_FLUSH_BOUND.as_millis()));
            }
        }
    }
}

/// Flush and shut down the exporters before a deliberate `process::exit`, which skips the guard's
/// `Drop`: spans closed just before the exit (the reason it exits for) reach the evidence file and the
/// collector. A process that never initialised telemetry has only its logs to flush. The wait is
/// bounded by `export::EXIT_FLUSH_BOUND`: an unreachable collector never holds the exit.
pub fn flush_before_exit() {
    match EXIT_FLUSH.get() {
        Some(exporters) => exporters.flush_before_exit("the exit flush"),
        None => flush_log_sinks(std::time::Instant::now() + export::EXIT_FLUSH_BOUND),
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.exporters.flush_before_exit("the guard's flush");
    }
}

fn guard(provider: SdkTracerProvider, logs: Option<opentelemetry_sdk::logs::SdkLoggerProvider>, evidence: Option<crate::evidence_capture::SharedEvidence>) -> TelemetryGuard {
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
/// [`flush_before_exit`] flush what is queued, each waiting at most `export::EXIT_FLUSH_BOUND`,
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
        .unwrap_or_else(|_| "http://localhost:4318".to_string());
    let resolved_service_name = std::env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| service_name.to_string());
    (otlp_endpoint, resolved_service_name)
}

/// The process's resource: its service, and what tells this process apart from every other one
/// exporting beside it (many estates and births share one collector): its pid, and, where the
/// launcher set them, the node it runs as, its birth's data directory and its estate's evidence
/// directory.
pub(crate) fn build_resource(service_name: &str) -> opentelemetry_sdk::Resource {
    build_resource_with(service_name, Vec::new())
}

/// [`build_resource`] with `extra` attributes beside its own.
pub(crate) fn build_resource_with(service_name: &str, extra: Vec<opentelemetry::KeyValue>) -> opentelemetry_sdk::Resource {
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
    attrs.extend(extra);
    opentelemetry_sdk::Resource::builder_empty().with_attributes(attrs).build()
}

/// Where the collector takes one signal over OTLP/HTTP: the signal's own path under its base URL.
/// The exporter posts to exactly the URL it is given, so the path is composed here.
fn signal_url(base: &str, path: &str) -> String {
    format!("{}{path}", base.trim_end_matches('/'))
}

/// The HTTP client of both exporters: hyper, non-blocking, on the export runtime, with every request
/// bounded by [`export::EXPORT_TIMEOUT`] and addressed to the exact signal URL `url` the caller gave
/// (opentelemetry-otlp's `resolve_http_endpoint` otherwise overrides `with_endpoint` with
/// `OTEL_EXPORTER_OTLP_ENDPOINT` from the process environment).
#[derive(Debug)]
struct ExportClient {
    inner: HyperClient,
    uri: http::Uri,
}

#[async_trait::async_trait]
impl opentelemetry_http::HttpClient for ExportClient {
    async fn send_bytes(&self, mut request: http::Request<opentelemetry_http::Bytes>) -> Result<http::Response<opentelemetry_http::Bytes>, opentelemetry_http::HttpError> {
        *request.uri_mut() = self.uri.clone();
        self.inner.send_bytes(request).await
    }
}

fn export_client(url: &str) -> ExportClient {
    ExportClient {
        inner: HyperClient::with_default_connector(export::EXPORT_TIMEOUT, None),
        uri: url.parse().expect("valid OTLP signal URL"),
    }
}

/// The OTLP span exporter: protobuf over HTTP to `<base>/v1/traces`, built on the export runtime.
pub(crate) fn build_exporter(endpoint: String) -> WatchedSpans<SpanExporter> {
    let _export = ExportRuntime::get().enter();
    let url = signal_url(&endpoint, "/v1/traces");
    let inner = SpanExporter::builder()
        .with_http()
        .with_http_client(export_client(&url))
        // One attempt per batch, as before opentelemetry-otlp 0.33 added its own retry: a failed
        // batch is dropped and counted, and the outage is named once per transition (WatchedSpans).
        .with_retry_policy(opentelemetry_otlp::RetryPolicy::disabled())
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(url.clone())
        .build()
        .expect("OTLP span exporter");
    WatchedSpans::new(inner, &url)
}

fn build_batch_provider(
    service_name: &str,
) -> (SdkTracerProvider, opentelemetry_sdk::trace::SdkTracer) {
    let (endpoint, resolved) = resolve_endpoint_and_service(service_name);
    let exporter = build_exporter(endpoint);
    let resource = build_resource(&resolved);

    let processor = {
        let _export = ExportRuntime::get().enter();
        BatchSpanProcessor::builder(exporter, ExportRuntime::get().clone()).with_batch_config(batch_config()).build()
    };

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();
    let tracer = provider.tracer(resolved);
    (provider, tracer)
}

/// The batch processors' bounds: 200 ms between exports, a full queue drops, and one export
/// request takes at most [`export::EXPORT_TIMEOUT`].
pub(crate) fn batch_config() -> opentelemetry_sdk::trace::BatchConfig {
    BatchConfigBuilder::default()
        .with_scheduled_delay(std::time::Duration::from_millis(200))
        .with_max_export_timeout(export::EXPORT_TIMEOUT)
        .with_max_queue_size(65536)
        .with_max_export_batch_size(2048)
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
    // only freed when that span CLOSES. iroh's actor loops (magicsock,
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
    let otel_filter = span_export_filter().and(filter_fn(export::admits_source));

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
    traceparent_of(&tracing::Span::current())
}

/// W3C `traceparent` of `span`, when an OpenTelemetry layer is installed and its context is valid:
/// what ties a line written outside span export (the transport diagnostic file) to its trace.
pub fn traceparent_of(span: &tracing::Span) -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let cx = span.context();
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
        if let Err(e) = span.set_parent(opentelemetry::Context::new().with_remote_span_context(sc)) {
            // The span keeps a trace of its own: the remote parent is lost, which joins nothing
            // across the call. Named once per process on stderr, never in the pipeline it concerns.
            static NAMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
            if !NAMED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                eprintln!("telemetry: a span's remote parent {traceparent} could not be set ({e}); the span starts its own trace");
            }
        }
    }
}

/// The W3C `tracestate` of the current tracing span, when one rides its context.
pub fn current_tracestate() -> Option<String> {
    tracestate_of(&tracing::Span::current())
}

/// The W3C `tracestate` of `span`, when one rides its context.
pub fn tracestate_of(span: &tracing::Span) -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let cx = span.context();
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

/// This process among every process writing beside it: `<pid>-<pid namespace inode>` (a pid names
/// one process only within its pid namespace, and every container has its own).
pub(crate) fn process_tag() -> String {
    let ns = std::fs::read_link("/proc/self/ns/pid").ok().and_then(|l| l.to_string_lossy().trim_start_matches("pid:[").trim_end_matches(']').parse::<u64>().ok()).unwrap_or(0);
    format!("{}-{ns}", std::process::id())
}

#[derive(Debug)]
pub(crate) struct JsonlSpanExporter {
    service: String,
    file: std::sync::Mutex<EvidenceFile>,
}

/// Where an exporter's evidence file stands.
#[derive(Debug)]
enum EvidenceFile {
    Open(std::fs::File),
    /// Created and opened by the first export, on the export thread (i146 Q33).
    OnFirstExport(std::path::PathBuf),
    /// The create or the open failed, as named on stderr: every export fails with this.
    Unusable(String),
}

impl JsonlSpanExporter {
    /// The exporter appending to `<dir>/<service>.<pid>-<pid namespace inode>.spans.jsonl`, the file
    /// created and opened by its first export on the export thread: the caller (telemetry init, inside
    /// every node's start) never waits on a disk (i146 Q33).
    pub fn on_first_export(dir: &std::path::Path, service: &str) -> Self {
        Self { service: service.to_string(), file: std::sync::Mutex::new(EvidenceFile::OnFirstExport(dir.join(format!("{service}.{}.spans.jsonl", process_tag())))) }
    }

    pub fn create(dir: &std::path::Path, service: &str) -> std::io::Result<Self> {
        Self::create_at(&dir.join(format!("{service}.{}.spans.jsonl", process_tag())), service)
    }

    /// The exporter appending to exactly `path` (its directory created): a writer that owns a file of
    /// its own beside others of the same process ([`EvidenceCapture`]).
    pub fn create_at(path: &std::path::Path, service: &str) -> std::io::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { service: service.to_string(), file: std::sync::Mutex::new(EvidenceFile::Open(file)) })
    }

    /// The open file, opening it now when its first export is this one; an open that fails is named
    /// once on stderr and every export after fails with it.
    fn file(file: &mut EvidenceFile) -> Result<&mut std::fs::File, String> {
        if let EvidenceFile::OnFirstExport(path) = &*file {
            let opened = path.parent().map_or(Ok(()), std::fs::create_dir_all).and_then(|()| std::fs::OpenOptions::new().create(true).append(true).open(path));
            *file = match opened {
                Ok(f) => EvidenceFile::Open(f),
                Err(e) => {
                    let why = format!("the evidence file {} could not be opened: {e}", path.display());
                    eprintln!("telemetry: {why}; spans are not written to it");
                    EvidenceFile::Unusable(why)
                }
            };
        }
        match file {
            EvidenceFile::Open(f) => Ok(f),
            EvidenceFile::Unusable(why) => Err(why.clone()),
            EvidenceFile::OnFirstExport(_) => unreachable!("opened above"),
        }
    }

    fn line(&self, s: &opentelemetry_sdk::trace::SpanData) -> String {
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
            // The spans this one follows from without being their child (a pooled dial's link to the call
            // that started it): the causal edge a parent id does not carry.
            "links": s.links.iter().map(|l| serde_json::json!({ "trace_id": l.span_context.trace_id().to_string(), "span_id": l.span_context.span_id().to_string() })).collect::<Vec<_>>(),
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

impl opentelemetry_sdk::trace::SpanExporter for JsonlSpanExporter {
    /// Runs on the evidence thread of [`CountingBatchProcessor`]: formats the batch and appends
    /// it at once, so a killed process loses only what was queued behind the write in flight.
    async fn export(&self, batch: Vec<opentelemetry_sdk::trace::SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        use std::io::Write;
        let mut out = String::new();
        for s in &batch {
            out.push_str(&self.line(s));
            out.push('\n');
        }
        let started = std::time::Instant::now();
        let mut held = self.file.lock().unwrap_or_else(|e| e.into_inner());
        let file = Self::file(&mut held);
        let opened = started.elapsed();
        let r = match file {
            Ok(f) => f.write_all(out.as_bytes()).map_err(|e| format!("evidence write failed: {e}")),
            Err(why) => Err(why),
        };
        let took = started.elapsed();
        if took >= EVIDENCE_WRITE_STALL {
            eprintln!("telemetry: evidence write stalled {} ms ({} bytes) on the evidence thread; open={} ms, write={} ms", took.as_millis(), out.len(), opened.as_millis(), took.saturating_sub(opened).as_millis());
        }
        r.map_err(opentelemetry_sdk::error::OTelSdkError::InternalFailure)
    }
}

/// The JSONL evidence exporter [`init_evidence_telemetry`] writes with, for a consumer that builds
/// its own pipeline ([`span_capture`]): one line per span to
/// `<dir>/<service>.<pid>-<pid namespace inode>.spans.jsonl`, appended on the export thread.
pub fn jsonl_span_exporter(dir: &std::path::Path, service: &str) -> std::io::Result<impl opentelemetry_sdk::trace::SpanExporter + 'static> {
    JsonlSpanExporter::create(dir, service)
}

/// How long each synchronous step of [`init_evidence_telemetry`] took, in the order it ran: the
/// export runtime's first start, the evidence processor, the exporters, the stderr sink, the
/// transport diagnostic sink, the subscriber and the watchdog (i144 Q33). No step opens a file or
/// waits for a thread: the evidence file and the transport diagnostic file are opened on their own
/// threads.
/// `Display`: `step=<ms>ms`, space-separated.
#[derive(Debug, Clone, Default)]
pub struct InitSteps(pub Vec<(&'static str, std::time::Duration)>);

impl InitSteps {
    fn timed(&mut self, step: &'static str, from: std::time::Instant) {
        self.0.push((step, from.elapsed()));
    }

    /// The step that took longest.
    pub fn slowest(&self) -> Option<(&'static str, std::time::Duration)> {
        self.0.iter().copied().max_by_key(|(_, took)| *took)
    }
}

impl std::fmt::Display for InitSteps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let steps: Vec<String> = self.0.iter().map(|(step, took)| format!("{step}={}ms", took.as_millis())).collect();
        f.write_str(&steps.join(" "))
    }
}

static INIT_STEPS: std::sync::OnceLock<InitSteps> = std::sync::OnceLock::new();

/// The steps of this process's [`init_evidence_telemetry`], once it has run.
pub fn init_steps() -> Option<&'static InitSteps> {
    INIT_STEPS.get()
}

/// Each step of an init, once its subscriber is installed: one span per step carrying how long the
/// step took (the steps ran before any subscriber could record them), under one span naming the
/// slowest, in the current span.
fn emit_init_steps(steps: &InitSteps) {
    let (slowest, slowest_ms) = steps.slowest().map_or(("", 0), |(step, took)| (step, took.as_millis() as u64));
    tracing::info_span!("rdm.mesh.telemetry.create.via-init", steps = %steps, slowest, slowest_ms, "otel.kind" = "internal").in_scope(|| {
        for (step, took) in &steps.0 {
            tracing::info_span!("rdm.mesh.telemetry.create.via-init-step", step = *step, elapsed_ms = took.as_millis() as u64, "otel.kind" = "internal").in_scope(|| {});
        }
    });
}

/// The init's steps held for [`init_steps`], reported now unless the process reports them inside its
/// own call ([`init_evidence_telemetry_for_one_call`]).
fn report_init_steps(steps: InitSteps, now: bool) {
    if now {
        emit_init_steps(&steps);
    }
    let _ = INIT_STEPS.set(steps);
}

/// Report this process's init steps in the current span: a one-call process does it inside its
/// call's root, so the invocation is one trace ([`init_evidence_telemetry_for_one_call`]).
pub fn report_init_steps_here() {
    if let Some(steps) = INIT_STEPS.get() {
        emit_init_steps(steps);
    }
}

/// Telemetry for i143 binaries. Spans go to the JSONL evidence sink when
/// `RDM_EVIDENCE_DIR` is set and to OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT`
/// is set; with neither, only the fmt layer runs. Returns `None` when no
/// exporter is configured. Each step's time is reported ([`init_steps`]).
pub fn init_evidence_telemetry(service_name: &str) -> Option<TelemetryGuard> {
    init_evidence(service_name, true, Vec::new())
}

/// [`init_evidence_telemetry`] for a process that holds its node id before its telemetry (Q208: a
/// node-admin's `AdminConfig::node_id`, minted by the bootstrap's first step when no launcher gave
/// it): the resource carries `instance_id` as `service.instance.id` (the OpenTelemetry resource
/// convention), on every span and log record from the first.
pub fn init_evidence_telemetry_for_instance(service_name: &str, instance_id: &str) -> Option<TelemetryGuard> {
    init_evidence(service_name, true, vec![opentelemetry::KeyValue::new(SERVICE_INSTANCE_ID, instance_id.to_string())])
}

/// The OpenTelemetry resource attribute naming one instance of a service: a node's id.
pub const SERVICE_INSTANCE_ID: &str = "service.instance.id";

/// [`init_evidence_telemetry`] for a process that runs one call and ends (the probe): its init
/// steps are held, not reported, until it reports them inside the call's own root span
/// ([`report_init_steps_here`]), so each invocation is one trace.
pub fn init_evidence_telemetry_for_one_call(service_name: &str) -> Option<TelemetryGuard> {
    init_evidence(service_name, false, Vec::new())
}

fn init_evidence(service_name: &str, report_now: bool, extra: Vec<opentelemetry::KeyValue>) -> Option<TelemetryGuard> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    install_propagator();
    let service = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| service_name.to_string());
    let resource = build_resource_with(&service, extra);
    let mut builder = SdkTracerProvider::builder().with_resource(resource.clone());
    let mut any = false;
    let mut steps = InitSteps::default();
    // The export runtime's first start, its own step: every processor below runs on it.
    let step = std::time::Instant::now();
    if std::env::var_os("RDM_EVIDENCE_DIR").is_some() || std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some() {
        let _ = ExportRuntime::get();
    }
    steps.timed("export-runtime", step);
    let mut evidence = None;
    if let Ok(dir) = std::env::var("RDM_EVIDENCE_DIR") {
        let step = std::time::Instant::now();
        let shared = crate::evidence_capture::SharedEvidence(std::sync::Arc::new(span_processor("evidence", JsonlSpanExporter::on_first_export(std::path::Path::new(&dir), &service))));
        builder = builder.with_span_processor(shared.clone());
        evidence = Some(shared);
        any = true;
        steps.timed("evidence-processor", step);
    }
    let step = std::time::Instant::now();
    let mut logs = None;
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        let export_rt = ExportRuntime::get();
        let _export = export_rt.enter();
        // The log adapter's provider: every log line to the collector's log store, beside spans.
        let logs_url = signal_url(&endpoint, "/v1/logs");
        match opentelemetry_otlp::LogExporter::builder().with_http().with_http_client(export_client(&logs_url)).with_retry_policy(opentelemetry_otlp::RetryPolicy::disabled()).with_protocol(Protocol::HttpBinary).with_endpoint(logs_url.clone()).build() {
            Ok(exporter) => {
                logs = Some(
                    opentelemetry_sdk::logs::SdkLoggerProvider::builder()
                        .with_resource(resource.clone())
                        .with_log_processor(opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor::builder(WatchedLogs::new(exporter, &logs_url), export_rt.clone()).build())
                        .build(),
                );
            }
            Err(e) => eprintln!("telemetry: the OTLP log exporter for {logs_url} could not be built: {e}"),
        }
        builder = builder.with_span_processor(span_processor("otlp", build_exporter(endpoint)));
        any = true;
    }
    steps.timed("exporters", step);
    let step = std::time::Instant::now();
    let stderr_sink = log_sink::LogSink::start(std::io::stderr());
    let _ = STDERR_SINK.set(stderr_sink.clone());
    steps.timed("stderr-sink", step);
    // The stderr log: INFO, RUST_LOG raises it; never the transport diagnostic target.
    macro_rules! stderr_layer {
        () => {
            tracing_subscriber::fmt::layer()
                .with_writer(stderr_sink.clone())
                .with_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()).and(filter_fn(export::admits_source)).and(filter_fn(not_transport_diag)))
        };
    }
    // The transport diagnostic file (R-QH1), beside and apart from every other output, in an
    // evidence run only. Outside one the layer is not in the stack at all: added as `None`, a layer
    // that carries per-layer filters makes tracing-subscriber enable every callsite of the process.
    let step = std::time::Instant::now();
    let diag = transport_diag::start::<tracing_subscriber::Registry>(&service).map(|(layer, sink)| {
        let _ = DIAG_SINK.set(sink);
        layer
    });
    steps.timed("transport-diagnostic", step);
    let step = std::time::Instant::now();
    if !any {
        let _ = match diag {
            Some(diag) => tracing_subscriber::registry().with(diag).with(stderr_layer!()).try_init(),
            None => tracing_subscriber::registry().with(stderr_layer!()).try_init(),
        };
        steps.timed("subscriber", step);
        report_init_steps(steps, report_now);
        return None;
    }
    let provider = builder.build();
    use opentelemetry::logs::LoggerProvider as _;
    // The OTLP log bridge, when a collector is configured: from `otlp_log_export_level`, never the
    // transport diagnostic target.
    macro_rules! log_layer {
        () => {
            logs.as_ref().map(|p| {
                logs::LogAdapter::new(p.logger("rafka-mesh"))
                    .with_filter(tracing_subscriber::filter::LevelFilter::from_level(otlp_log_export_level()).and(filter_fn(export::admits_source)).and(filter_fn(not_transport_diag)))
            })
        };
    }
    let _ = match diag {
        Some(diag) => tracing_subscriber::registry().with(diag).with(stderr_layer!()).with(span_export_layer(&provider, &service)).with(log_layer!()).try_init(),
        None => tracing_subscriber::registry().with(stderr_layer!()).with(span_export_layer(&provider, &service)).with(log_layer!()).try_init(),
    };
    steps.timed("subscriber", step);
    let step = std::time::Instant::now();
    let _ = watchdog::spawn();
    steps.timed("watchdog", step);
    report_init_steps(steps, report_now);
    Some(guard(provider, logs, evidence))
}

/// A line of the transport diagnostic file's own target goes to that file only, never to stderr
/// or the OTLP log bridge.
fn not_transport_diag(meta: &tracing::Metadata<'_>) -> bool {
    meta.target() != TRANSPORT_DIAG_TARGET
}

/// What span export (OTLP and the evidence files) carries, as rafka-v2's telemetry
/// (`rafka-telemetry` `make_otel_filter`): nothing by default, then INFO from RDM's own crates
/// (`rafka_*`), the R-shape consumer (`rshape_*`), RDM tools (`rdm_*`) and the one test executable
/// per crate (`main`); DEBUG with `RDM_OTEL_DEBUG=1`. Every third-party crate (iroh, iroh-gossip
/// and its HyParView `rg3` diagnostics, noq, the HTTP and OTLP stacks) is off, whatever RUST_LOG
/// says; RUST_LOG drives stdout only.
pub(crate) fn span_export_filter() -> EnvFilter {
    let level = if std::env::var("RDM_OTEL_DEBUG").as_deref() == Ok("1") { "debug" } else { "info" };
    let mut f = EnvFilter::new("off");
    for own in ["rafka", "rshape", "rdm", "main"] {
        f = f.add_directive(format!("{own}={level}").parse().expect("static directive"));
    }
    for third in ["iroh", "iroh_gossip", "rg3", "noq", "noq_proto", "netwatch", "quinn", "h2", "hyper", "hyper_util", "rustls", "reqwest", "tower", "tower_http", "opentelemetry", "opentelemetry_sdk", "opentelemetry_otlp"] {
        f = f.add_directive(format!("{third}=off").parse().expect("static directive"));
    }
    f
}

/// The level the OTLP log bridge exports from, as rafka-v2's (`otlp_log_export_level`): WARN by
/// default, `RDM_OTLP_LOG_LEVEL` sets it; independent of RUST_LOG (stdout) and of span export.
pub(crate) fn otlp_log_export_level() -> tracing::Level {
    std::env::var("RDM_OTLP_LOG_LEVEL").ok().and_then(|v| v.parse::<tracing::Level>().ok()).unwrap_or(tracing::Level::WARN)
}


#[cfg(test)]
mod evidence_tests {
    use opentelemetry::trace::{Tracer, TracerProvider as _};

    /// The exit flush's one deadline includes a held fmt log, after the
    /// evidence and collector processors. No log writer can hold the exit forever.
    #[test]
    fn a_held_log_writer_ends_the_exit_flush_at_its_one_bound_named() {
        use std::io::{BufRead, Read, Write};
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};
        const CHILD: &str = "RDM_HELD_LOG_EXIT_CHILD";
        if std::env::var_os(CHILD).is_some() {
            struct HeldLog {
                entered: std::sync::mpsc::Sender<()>,
                held: std::sync::mpsc::Receiver<()>,
            }
            impl Write for HeldLog {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    let _ = self.entered.send(());
                    let _ = self.held.recv();
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
            }
            let (entered, writing) = std::sync::mpsc::channel();
            let (_hold, held) = std::sync::mpsc::channel();
            let sink = crate::log_sink::LogSink::start(HeldLog { entered, held });
            assert!(super::STDERR_SINK.set(sink.clone()).is_ok());
            {
                use tracing_subscriber::fmt::MakeWriter;
                let mut line = sink.make_writer();
                line.write_all(b"a held log line\n").unwrap();
            }
            writing.recv_timeout(Duration::from_secs(10)).expect("the writer entered before the exit flush's clock starts");
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
            let exporters = super::Exporters::new(provider, None, None);
            println!("FLUSH_STARTED");
            let started = Instant::now();
            exporters.flush_before_exit("the held-log exit flush");
            println!("FLUSH_DONE_MS={}", started.elapsed().as_millis());
            std::process::exit(0);
        }
        let name = concat!(module_path!(), "::a_held_log_writer_ends_the_exit_flush_at_its_one_bound_named");
        let name = name.split_once("::").unwrap().1;
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"]).env(CHILD, "1")
            .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let output = child.stdout.take().unwrap();
        let mut error = child.stderr.take().unwrap();
        let errors = std::thread::spawn(move || { let mut s = String::new(); error.read_to_string(&mut s).unwrap(); s });
        let (lines, received) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(output).lines() {
                if lines.send(line.unwrap()).is_err() { break; }
            }
        });
        let ready_by = Instant::now() + Duration::from_secs(10);
        let mut output = Vec::new();
        loop {
            match received.recv_timeout(ready_by.saturating_duration_since(Instant::now())) {
                Ok(line) => { let ready = line == "FLUSH_STARTED"; output.push(line); if ready { break; } }
                Err(e) => { let _ = child.kill(); let _ = child.wait(); panic!("the child did not enter its held-log exit flush: {e}; {output:?}; {}", errors.join().unwrap()); }
            }
        }
        let bound = super::export::EXIT_FLUSH_BOUND + Duration::from_millis(500);
        let end = Instant::now() + bound;
        let mut elapsed = None;
        while let Ok(line) = received.recv_timeout(end.saturating_duration_since(Instant::now())) {
            if let Some(value) = line.strip_prefix("FLUSH_DONE_MS=") {
                elapsed = value.parse::<u128>().ok();
                output.push(line);
                break;
            }
            output.push(line);
        }
        if elapsed.is_none() { let _ = child.kill(); }
        let status = child.wait().unwrap();
        let _ = reader.join();
        let error = errors.join().unwrap();
        assert!(elapsed.is_some(), "the held log kept the exit flush past its one bound: status={status:?}; stdout={output:?}; stderr={error}");
        assert!(status.success(), "the child failed: {status:?}; {output:?}; {error}");
        assert!(elapsed.unwrap() < bound.as_millis(), "the exit flush exceeded its bound: {output:?}; {error}");
        assert!(error.contains("stderr log flush ended at its bound"), "the unconfirmed log flush is named: {error}");
    }

    /// CONTRACT (CLAUDE.md `RDM_CONNECTIONS_DELETE_DEADLINE_MS`: a signal stop is bounded "before the node
    /// leaves"; `flush_before_exit`: "an unreachable collector never holds the exit"): a process whose
    /// stderr is a pipe nobody reads, full, ends its exit flush within its one bound plus the lines that
    /// name it. The fmt log's thread is blocked inside a stderr write, holding std's stderr lock; what
    /// must NOT happen is the exit naming that unconfirmed flush with an `eprintln!` that waits on the
    /// same lock for good (store_root_boot's Day-0 admin never exited after SIGTERM: its main thread
    /// in `flush_log_sinks`'s `eprintln!`, its `rdm-log` thread in `write(2)` on fd 2).
    #[test]
    fn an_exit_flush_on_a_full_stderr_pipe_nobody_reads_ends_at_its_bound() {
        use std::io::{BufRead, Read, Write};
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};
        const CHILD: &str = "RDM_FULL_STDERR_EXIT_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let sink = crate::log_sink::LogSink::start(std::io::stderr());
            assert!(super::STDERR_SINK.set(sink.clone()).is_ok());
            // Over a pipe's capacity (64 KiB on Linux) many times: the pipe fills and the log's thread
            // blocks inside its write. A flush that cannot finish within a second confirms it.
            let line = [b'x'; 1023];
            let held_by = Instant::now() + Duration::from_secs(10);
            loop {
                for _ in 0..256 {
                    use tracing_subscriber::fmt::MakeWriter;
                    let mut w = sink.make_writer();
                    w.write_all(&line).unwrap();
                    w.write_all(b"\n").unwrap();
                }
                if !sink.flush_until(Instant::now() + Duration::from_secs(1)) {
                    break;
                }
                assert!(Instant::now() < held_by, "stderr never filled: the parent reads it");
            }
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
            let exporters = super::Exporters::new(provider, None, None);
            println!("FLUSH_STARTED");
            let started = Instant::now();
            exporters.flush_before_exit("the full-stderr exit flush");
            println!("FLUSH_DONE_MS={}", started.elapsed().as_millis());
            std::process::exit(0);
        }
        let name = concat!(module_path!(), "::an_exit_flush_on_a_full_stderr_pipe_nobody_reads_ends_at_its_bound");
        let name = name.split_once("::").unwrap().1;
        // stderr is piped and not read until the child has ended, as a launcher that never reads it.
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"]).env(CHILD, "1")
            .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let output = child.stdout.take().unwrap();
        let (lines, received) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(output).lines() {
                if lines.send(line.unwrap()).is_err() { break; }
            }
        });
        let mut output = Vec::new();
        let ready_by = Instant::now() + Duration::from_secs(30);
        loop {
            match received.recv_timeout(ready_by.saturating_duration_since(Instant::now())) {
                Ok(line) => { let ready = line == "FLUSH_STARTED"; output.push(line); if ready { break; } }
                Err(e) => { let _ = child.kill(); let _ = child.wait(); panic!("the child did not fill its stderr and enter its exit flush: {e}; {output:?}"); }
            }
        }
        // The flush's one bound, plus the lines that name it: each waits a quarter second at most.
        let bound = super::export::EXIT_FLUSH_BOUND + Duration::from_millis(1500);
        let end = Instant::now() + bound;
        let mut elapsed = None;
        while let Ok(line) = received.recv_timeout(end.saturating_duration_since(Instant::now())) {
            if let Some(value) = line.strip_prefix("FLUSH_DONE_MS=") {
                elapsed = value.parse::<u128>().ok();
                break;
            }
            output.push(line);
        }
        if elapsed.is_none() { let _ = child.kill(); }
        let status = child.wait().unwrap();
        let _ = reader.join();
        let mut error = Vec::new();
        let _ = child.stderr.take().unwrap().read_to_end(&mut error);
        assert!(elapsed.is_some(), "the full stderr held the exit flush past its bound ({} ms): status={status:?}; stdout={output:?}; {} stderr bytes", bound.as_millis(), error.len());
        assert!(status.success(), "the child failed: {status:?}; {output:?}");
        assert!(elapsed.unwrap() < bound.as_millis(), "the exit flush exceeded its bound: {elapsed:?} ms; {output:?}");
    }


    /// CONTRACT: a span ended just before the provider shuts down (the guard's drop) is a complete
    /// JSON line of the evidence file, in order, and a flush alone already puts the spans ended
    /// before it on disk; nothing was lost on the way.
    #[test]
    fn evidence_file_holds_every_span_ended_before_shutdown() {
        let dir = std::env::temp_dir().join(format!("rdm-evidence-complete-{}", std::process::id()));
        let exporter = super::JsonlSpanExporter::create(&dir, "evidence-complete").unwrap();
        let processor = super::span_processor("evidence", exporter);
        let loss = processor.loss();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().with_span_processor(processor).build();
        let tracer = provider.tracer("t");
        let read = || {
            let file = std::fs::read_dir(&dir).unwrap().next().unwrap().unwrap().path();
            std::fs::read_to_string(file).unwrap()
        };
        for i in 0..300 {
            drop(tracer.start(format!("s{i}")));
        }
        provider.force_flush().expect("the flush returned an error");
        assert_eq!(read().lines().count(), 300, "a flush returned with spans still queued");
        for i in 300..600 {
            drop(tracer.start(format!("s{i}")));
        }
        provider.shutdown().unwrap();
        let names: Vec<String> = read().lines().map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["name"].as_str().unwrap().to_string()).collect();
        assert_eq!(names, (0..600).map(|i| format!("s{i}")).collect::<Vec<_>>());
        assert_eq!(loss.lost(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
