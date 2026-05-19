use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, TracerProvider};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

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

/// Initialize OTLP tracing. Call once in `main()` before any other work.
/// Returns a guard whose `Drop` flushes and shuts down the exporter.
///
/// Uses BatchSpanProcessor with a 200ms scheduled delay so boot root spans
/// (rafka.mesh.node.ready, rafka.mesh.boot.endpoint_created) export promptly
/// after closing, well before their children age out of Jaeger's index window.
pub fn init_telemetry(service_name: &str) -> TelemetryGuard {
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4316".to_string());

    let resolved_service_name = std::env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| service_name.to_string());

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .build()
        .expect("OTLP span exporter");

    let resource = opentelemetry_sdk::Resource::new(vec![
        opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            resolved_service_name.clone(),
        ),
    ]);

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

    let tracer = provider.tracer(resolved_service_name);
    let otel_layer = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();

    TelemetryGuard { provider }
}
