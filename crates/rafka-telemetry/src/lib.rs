use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::TracerProvider;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub struct TelemetryGuard {
    provider: TracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Flush pending spans then shut down. Critical for node.stopping span.
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

/// Initialize OTLP tracing. Call once in `main()` before any other work.
/// Returns a guard whose `Drop` flushes and shuts down the exporter.
///
/// Uses `BatchSpanProcessor` to avoid blocking tokio worker threads on each
/// span export. The `TelemetryGuard` drop calls `force_flush()` + `shutdown()`
/// to ensure all spans are exported before process exit.
pub fn init_telemetry(service_name: &str) -> TelemetryGuard {
    let otlp_endpoint = std::env::var("RAFKA_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4327".to_string());

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()
        .expect("OTLP span exporter");

    let resource = opentelemetry_sdk::Resource::new(vec![
        opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            service_name.to_string(),
        ),
    ]);

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(service_name.to_string());
    let otel_layer = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();

    TelemetryGuard { provider }
}
