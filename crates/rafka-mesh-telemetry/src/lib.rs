//! Process telemetry for every mesh binary: OTLP span and log export, the JSONL evidence sink, and the
//! bounded drain a process runs before it exits.
//!
//! A process calls one `init_*` function ([`init_telemetry`], [`init_telemetry_for_cli`],
//! [`init_evidence_telemetry`]) and holds the returned [`TelemetryGuard`] for its lifetime. The
//! exporters run on their own thread ([`export`]), the stall watchdog is in [`watchdog`] and the log
//! adapter that turns `tracing` events into OTLP log records is in [`logs`].
#![deny(missing_docs)]


use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::hyper::HyperClient;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, TracerProvider};
use tracing_subscriber::filter::{filter_fn, FilterExt};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub mod export;
pub mod logs;
mod log_sink;
mod transport_diag;
pub use transport_diag::{TRANSPORT_DIAG_FILE, TRANSPORT_DIAG_TARGET};
pub mod watchdog;
mod pipeline;
mod evidence_capture;
pub use evidence_capture::EvidenceCapture;
pub use pipeline::{enable_callsites, install_span_capture, span_capture, span_export_layer, span_processor, CountingBatchProcessor, SpanCapture, SpanLoss};

use export::{ExportRuntime, WatchedLogs, WatchedSpans};

/// The exporters one `init_*` built: drained at most once, by whichever of the guard's `Drop` and
/// [`flush_before_exit`] comes first.
#[derive(Clone)]
struct Exporters {
    provider: TracerProvider,
    /// The OTLP log provider the log adapter emits through, when a collector is configured.
    logs: Option<opentelemetry_sdk::logs::LoggerProvider>,
    drained: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Exporters {
    fn new(provider: TracerProvider, logs: Option<opentelemetry_sdk::logs::LoggerProvider>) -> Self {
        Self { provider, logs, drained: Default::default() }
    }

    /// Shut the providers down (the shutdown drains what is queued for the collector), each
    /// on a thread of its own: waited for at most [`export::DRAIN_BOUND`] together, and not at
    /// all while the collector is known to be down, because what is queued for it is dropped.
    fn drain(&self, what: &'static str) {
        if self.drained.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
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
        flush_log_sinks();
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

/// The transport diagnostic file's writer ([`transport_diag`]), drained with the fmt log's.
static DIAG_SINK: std::sync::OnceLock<log_sink::LogSink> = std::sync::OnceLock::new();

/// Returns once every line handed to the stderr log and the transport diagnostic file is written.
fn flush_log_sinks() {
    for sink in [STDERR_SINK.get(), DIAG_SINK.get()].into_iter().flatten() {
        sink.flush();
    }
}

/// Drain and shut down the exporters before a deliberate `process::exit`, which skips the guard's
/// `Drop`: spans closed just before the exit (the reason it exits for) reach the evidence file and the
/// collector. A process that never initialised telemetry has nothing to drain. The wait is bounded
/// by [`export::DRAIN_BOUND`]: an unreachable collector never holds the exit.
pub fn flush_before_exit() {
    match EXIT_FLUSH.get() {
        Some(exporters) => exporters.drain("the exit drain"),
        None => flush_log_sinks(),
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.exporters.drain("the guard's drain");
    }
}

fn guard(provider: TracerProvider, logs: Option<opentelemetry_sdk::logs::LoggerProvider>) -> TelemetryGuard {
    let exporters = Exporters::new(provider, logs);
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
    guard(provider, None)
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
    guard(provider, None)
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

/// Where the collector takes one signal over OTLP/HTTP: the signal's own path under its base URL.
/// The exporter posts to exactly the URL it is given, so the path is composed here.
fn signal_url(base: &str, path: &str) -> String {
    format!("{}{path}", base.trim_end_matches('/'))
}

/// The HTTP client of both exporters: hyper, non-blocking, on the export runtime, with every request
/// bounded by [`export::EXPORT_TIMEOUT`]. The exporter builders hold the timeout they are given
/// but do not apply it to an HTTP request; the bound lives in the client.
fn export_client() -> HyperClient {
    HyperClient::with_default_connector(export::EXPORT_TIMEOUT, None)
}

/// The OTLP span exporter: protobuf over HTTP to `<base>/v1/traces`, built on the export runtime.
fn build_exporter(endpoint: String) -> WatchedSpans<SpanExporter> {
    let _export = ExportRuntime::get().enter();
    let url = signal_url(&endpoint, "/v1/traces");
    let inner = SpanExporter::builder()
        .with_http()
        .with_http_client(export_client())
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(url.clone())
        .build()
        .expect("OTLP span exporter");
    WatchedSpans::new(inner, &url)
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

/// This process among every process writing beside it: `<pid>-<pid namespace inode>` (a pid names
/// one process only within its pid namespace, and every container has its own).
pub(crate) fn process_tag() -> String {
    let ns = std::fs::read_link("/proc/self/ns/pid").ok().and_then(|l| l.to_string_lossy().trim_start_matches("pid:[").trim_end_matches(']').parse::<u64>().ok()).unwrap_or(0);
    format!("{}-{ns}", std::process::id())
}

#[derive(Debug)]
pub(crate) struct JsonlSpanExporter {
    service: String,
    file: std::fs::File,
}

impl JsonlSpanExporter {
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
        Ok(Self { service: service.to_string(), file })
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
    /// Runs on the evidence thread of [`CountingBatchProcessor`]: formats the batch and appends
    /// it at once, so a killed process loses only what was queued behind the write in flight.
    fn export(
        &mut self,
        batch: Vec<opentelemetry_sdk::export::trace::SpanData>,
    ) -> futures_util::future::BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        use std::io::Write;
        let mut out = String::new();
        for s in &batch {
            out.push_str(&self.line(s));
            out.push('\n');
        }
        let started = std::time::Instant::now();
        let r = self.file.write_all(out.as_bytes());
        let took = started.elapsed();
        if took >= EVIDENCE_WRITE_STALL {
            eprintln!("telemetry: evidence write stalled {} ms ({} bytes) on the evidence thread", took.as_millis(), out.len());
        }
        Box::pin(async move { r.map_err(|e| opentelemetry::trace::TraceError::Other(format!("evidence write failed: {e}").into())) })
    }
}

/// The JSONL evidence exporter [`init_evidence_telemetry`] writes with, for a consumer that builds
/// its own pipeline ([`span_capture`]): one line per span to
/// `<dir>/<service>.<pid>-<pid namespace inode>.spans.jsonl`, appended on the export thread.
pub fn jsonl_span_exporter(dir: &std::path::Path, service: &str) -> std::io::Result<impl opentelemetry_sdk::export::trace::SpanExporter + 'static> {
    JsonlSpanExporter::create(dir, service)
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
    if let Ok(dir) = std::env::var("RDM_EVIDENCE_DIR") {
        match JsonlSpanExporter::create(std::path::Path::new(&dir), &service) {
            Ok(e) => {
                builder = builder.with_span_processor(span_processor("evidence", e));
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
        let logs_url = signal_url(&endpoint, "/v1/logs");
        match opentelemetry_otlp::LogExporter::builder().with_http().with_http_client(export_client()).with_protocol(Protocol::HttpBinary).with_endpoint(logs_url.clone()).build() {
            Ok(exporter) => {
                logs = Some(
                    opentelemetry_sdk::logs::LoggerProvider::builder()
                        .with_resource(build_resource(&service))
                        .with_batch_exporter(WatchedLogs::new(exporter, &logs_url), export_rt.clone())
                        .build(),
                );
            }
            Err(e) => eprintln!("telemetry: the OTLP log exporter for {logs_url} could not be built: {e}"),
        }
        builder = builder.with_span_processor(span_processor("otlp", build_exporter(endpoint)));
        any = true;
    }
    let stderr_sink = log_sink::LogSink::start(std::io::stderr());
    let _ = STDERR_SINK.set(stderr_sink.clone());
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
    let diag = transport_diag::start::<tracing_subscriber::Registry>(&service).map(|(layer, sink)| {
        let _ = DIAG_SINK.set(sink);
        layer
    });
    if !any {
        let _ = match diag {
            Some(diag) => tracing_subscriber::registry().with(diag).with(stderr_layer!()).try_init(),
            None => tracing_subscriber::registry().with(stderr_layer!()).try_init(),
        };
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
    let _ = watchdog::spawn();
    Some(guard(provider, logs))
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

    /// CONTRACT: a span ended just before the provider shuts down (the guard's drop) is a complete
    /// JSON line of the evidence file, in order, and a flush alone already puts the spans ended
    /// before it on disk; nothing was lost on the way.
    #[test]
    fn evidence_file_holds_every_span_ended_before_shutdown() {
        let dir = std::env::temp_dir().join(format!("rdm-evidence-complete-{}", std::process::id()));
        let exporter = super::JsonlSpanExporter::create(&dir, "evidence-complete").unwrap();
        let processor = super::span_processor("evidence", exporter);
        let loss = processor.loss();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_span_processor(processor).build();
        let tracer = provider.tracer("t");
        let read = || {
            let file = std::fs::read_dir(&dir).unwrap().next().unwrap().unwrap().path();
            std::fs::read_to_string(file).unwrap()
        };
        for i in 0..300 {
            drop(tracer.start(format!("s{i}")));
        }
        for r in provider.force_flush() {
            r.expect("the flush returned an error");
        }
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
