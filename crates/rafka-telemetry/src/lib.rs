use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SimpleSpanProcessor, TracerProvider};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub struct TelemetryGuard {
    provider: TracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        for result in self.provider.force_flush() {
            if let Err(e) = result {
                eprintln!("telemetry flush error: {e}");
            }
        }
        if let Err(e) = self.provider.shutdown() {
            eprintln!("telemetry shutdown error: {e}");
        }
    }
}

/// Initialize OTLP tracing for long-running services (gateway, broker, compute,
/// registry, topology-ui). Uses BatchSpanProcessor with 200ms scheduled delay —
/// efficient batching, suitable for high span volume. Spans export within 200ms
/// of close, well before Jaeger's index window.
///
/// Also installs the W3C TraceContext propagator globally so cross-service trace
/// chains link via `traceparent` HTTP headers (e.g. rfa → topology-ui).
///
/// Returns a guard whose `Drop` flushes and shuts down the exporter.
pub fn init_telemetry(service_name: &str) -> TelemetryGuard {
    install_propagator();
    let (provider, tracer) = build_batch_provider(service_name);
    install_subscriber(tracer);
    TelemetryGuard { provider }
}

/// Initialize OTLP tracing for short-lived CLI processes (rfa, future rf).
/// Uses SimpleSpanProcessor — synchronous export on span close. No batch race,
/// no need for a pre-exit flush sleep. Cost: one export per span (higher
/// overhead than batching). Suitable for low span volume + short process lifetime.
///
/// Also installs the W3C TraceContext propagator globally.
///
/// Returns a guard whose `Drop` flushes and shuts down the exporter.
pub fn init_telemetry_for_cli(service_name: &str) -> TelemetryGuard {
    install_propagator();
    let (provider, tracer) = build_simple_provider(service_name);
    install_subscriber(tracer);
    TelemetryGuard { provider }
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

fn build_resource(service_name: &str) -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        service_name.to_string(),
    )])
}

fn build_exporter(endpoint: String) -> SpanExporter {
    SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("OTLP span exporter")
}

fn build_batch_provider(
    service_name: &str,
) -> (TracerProvider, opentelemetry_sdk::trace::Tracer) {
    let (endpoint, resolved) = resolve_endpoint_and_service(service_name);
    let exporter = build_exporter(endpoint);
    let resource = build_resource(&resolved);

    let batch_config = BatchConfigBuilder::default()
        .with_scheduled_delay(std::time::Duration::from_millis(200))
        .build();
    let processor = BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_batch_config(batch_config)
        .build();

    let provider = TracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();
    let tracer = provider.tracer(resolved);
    (provider, tracer)
}

fn build_simple_provider(
    service_name: &str,
) -> (TracerProvider, opentelemetry_sdk::trace::Tracer) {
    let (endpoint, resolved) = resolve_endpoint_and_service(service_name);
    let exporter = build_exporter(endpoint);
    let resource = build_resource(&resolved);

    let provider = TracerProvider::builder()
        .with_span_processor(SimpleSpanProcessor::new(Box::new(exporter)))
        .with_resource(resource)
        .build();
    let tracer = provider.tracer(resolved);
    (provider, tracer)
}

fn install_subscriber(tracer: opentelemetry_sdk::trace::Tracer) {
    // Per-layer filters. Both layers floor at INFO. stdout stays terse; OTLP
    // also floors at INFO (see the otel_filter note below for why DEBUG capture
    // was a memory leak under churn). RUST_LOG can still raise either layer via
    // EnvFilter::from_default_env() — a more-specific directive (e.g.
    // `iroh_quinn_proto=trace`) wins over the per-layer floor, so debug
    // visibility is opt-in per debugging session rather than always-on.
    let fmt_filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into());
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
        .add_directive("opentelemetry_otlp=off".parse().expect("static directive"));

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
    use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let parts: Vec<&str> = traceparent.split('-').collect();
    let [_, trace, span_id, flags] = parts[..] else { return };
    let (Ok(t), Ok(s), Ok(f)) = (TraceId::from_hex(trace), SpanId::from_hex(span_id), u8::from_str_radix(flags, 16)) else {
        return;
    };
    let sc = SpanContext::new(t, s, TraceFlags::new(f), true, TraceState::default());
    if sc.is_valid() {
        span.set_parent(opentelemetry::Context::new().with_remote_span_context(sc));
    }
}

/// Writes every finished span as one JSON line to
/// `<RAFKA_EVIDENCE_DIR>/<service>.<pid>.spans.jsonl` (`docs/i143/design.md` §6).
/// Causality is carried by `parent_span_id`; a consumer never infers it from
/// timestamps.
#[derive(Debug)]
pub struct JsonlSpanExporter {
    service: String,
    file: std::sync::Arc<std::sync::Mutex<std::fs::File>>,
}

impl JsonlSpanExporter {
    pub fn create(dir: &std::path::Path, service: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{service}.{}.spans.jsonl", std::process::id()));
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { service: service.to_string(), file: std::sync::Arc::new(std::sync::Mutex::new(file)) })
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
        })
        .to_string()
    }
}

impl opentelemetry_sdk::export::trace::SpanExporter for JsonlSpanExporter {
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
        let file = self.file.clone();
        let r = file.lock().map_err(|e| e.to_string()).and_then(|mut f| f.write_all(out.as_bytes()).map_err(|e| e.to_string()));
        Box::pin(async move { r.map_err(|e| opentelemetry::trace::TraceError::Other(e.into())) })
    }
}

/// Telemetry for i143 binaries. Spans go to the JSONL evidence sink when
/// `RAFKA_EVIDENCE_DIR` is set and to OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT`
/// is set; with neither, only the fmt layer runs. Returns `None` when no
/// exporter is configured.
pub fn init_evidence_telemetry(service_name: &str) -> Option<TelemetryGuard> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    install_propagator();
    let service = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| service_name.to_string());
    let mut builder = TracerProvider::builder().with_resource(build_resource(&service));
    let mut any = false;
    if let Ok(dir) = std::env::var("RAFKA_EVIDENCE_DIR") {
        match JsonlSpanExporter::create(std::path::Path::new(&dir), &service) {
            Ok(e) => {
                builder = builder.with_span_processor(SimpleSpanProcessor::new(Box::new(e)));
                any = true;
            }
            Err(e) => eprintln!("telemetry: RAFKA_EVIDENCE_DIR={dir} is unusable: {e}"),
        }
    }
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        let processor = BatchSpanProcessor::builder(build_exporter(endpoint), opentelemetry_sdk::runtime::Tokio).build();
        builder = builder.with_span_processor(processor);
        any = true;
    }
    let fmt_filter = EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into());
    if !any {
        let _ = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_filter(fmt_filter)).try_init();
        return None;
    }
    let provider = builder.build();
    let tracer = provider.tracer(service.clone());
    let otel_filter = EnvFilter::from_default_env()
        .add_directive(tracing::Level::INFO.into())
        .add_directive("iroh=warn".parse().expect("static directive"))
        .add_directive("iroh_gossip=warn".parse().expect("static directive"))
        .add_directive("noq=warn".parse().expect("static directive"))
        .add_directive("noq_proto=warn".parse().expect("static directive"));
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_filter(fmt_filter))
        .with(OpenTelemetryLayer::new(tracer).with_filter(otel_filter))
        .try_init();
    Some(TelemetryGuard { provider })
}
