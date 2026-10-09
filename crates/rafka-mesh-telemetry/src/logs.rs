//! The log adapter: every `tracing` event a process emits becomes one OTLP log record sent to the
//! collector, stamped with the trace id and span id of the span it fired in, so the collector
//! shows each log line on the span that produced it. Events inside a span also travel on that
//! span as span events (the span exporter writes them); this adapter is what carries the ones
//! the span exporter does not see (no span, or a span the span filter drops) and what puts every
//! line in the collector's log store with its trace context.

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::TraceContextExt;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_opentelemetry::OtelData;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// Hands each event to an OTLP logger (see the module doc).
pub(crate) struct LogAdapter {
    logger: opentelemetry_sdk::logs::Logger,
}

impl LogAdapter {
    pub fn new(logger: opentelemetry_sdk::logs::Logger) -> Self {
        Self { logger }
    }
}

fn severity(level: &Level) -> (Severity, &'static str) {
    match *level {
        Level::ERROR => (Severity::Error, "ERROR"),
        Level::WARN => (Severity::Warn, "WARN"),
        Level::INFO => (Severity::Info, "INFO"),
        Level::DEBUG => (Severity::Debug, "DEBUG"),
        Level::TRACE => (Severity::Trace, "TRACE"),
    }
}

/// The event's fields: `message` becomes the body, every other field an attribute.
#[derive(Default)]
struct Fields {
    message: Option<String>,
    attributes: Vec<(&'static str, AnyValue)>,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.attributes.push((field.name(), AnyValue::from(value.to_string())));
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let v = format!("{value:?}");
        if field.name() == "message" {
            self.message = Some(v);
        } else {
            self.attributes.push((field.name(), AnyValue::from(v)));
        }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.attributes.push((field.name(), AnyValue::from(value)));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.attributes.push((field.name(), AnyValue::from(value as i64)));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.attributes.push((field.name(), AnyValue::from(value)));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.attributes.push((field.name(), AnyValue::from(value)));
    }
}

impl<S> Layer<S> for LogAdapter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut record = self.logger.create_log_record();
        let now = std::time::SystemTime::now();
        record.set_timestamp(now);
        record.set_observed_timestamp(now);
        let (number, text) = severity(meta.level());
        record.set_severity_number(number);
        record.set_severity_text(text);
        record.set_target(meta.target().to_string());
        record.set_body(AnyValue::from(fields.message.unwrap_or_default()));
        for (k, v) in fields.attributes {
            record.add_attribute(k, v);
        }
        if let Some(file) = meta.file() {
            record.add_attribute("code.filepath", file.to_string());
        }
        if let Some(line) = meta.line() {
            record.add_attribute("code.lineno", line as i64);
        }
        // The nearest enclosing span the span exporter records: its ids are what the collector
        // joins this line to.
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                let ext = span.extensions();
                if let Some(otel) = ext.get::<OtelData>() {
                    let trace_id = otel.builder.trace_id.unwrap_or_else(|| otel.parent_cx.span().span_context().trace_id());
                    if let Some(span_id) = otel.builder.span_id {
                        record.add_attribute("span.name", span.name().to_string());
                        record.set_trace_context(trace_id, span_id, None);
                    }
                    break;
                }
            }
        }
        self.logger.emit(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::logs::LoggerProvider as _;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::testing::logs::InMemoryLogExporter;
    use tracing_subscriber::layer::SubscriberExt;

    /// CONTRACT: an event inside a span reaches the log exporter with that span's trace id and
    /// span id, its message as the body and its fields as attributes; an event outside every span
    /// reaches it with no trace context.
    #[test]
    fn an_event_inside_a_span_carries_that_spans_ids_and_one_outside_carries_none() {
        let logs = InMemoryLogExporter::default();
        let log_provider = opentelemetry_sdk::logs::LoggerProvider::builder().with_simple_exporter(logs.clone()).build();
        let spans = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
        let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder().with_simple_exporter(spans.clone()).build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::OpenTelemetryLayer::new(tracer_provider.tracer("t")))
            .with(LogAdapter::new(log_provider.logger("t")));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("rdm.test.span.update.via-cell");
            span.in_scope(|| tracing::info!(node = "mesh1.admin.1", "inside the span"));
            drop(span);
            tracing::warn!("outside every span");
        });
        let _ = tracer_provider.force_flush();
        let finished = spans.get_finished_spans().unwrap();
        let span = finished.iter().find(|s| s.name == "rdm.test.span.update.via-cell").expect("the span was exported");
        let records = logs.get_emitted_logs().unwrap();
        assert_eq!(records.len(), 2, "both events became log records");
        let inside = records.iter().find(|r| matches!(&r.record.body, Some(AnyValue::String(s)) if s.as_str() == "inside the span")).expect("the inside event");
        let tc = inside.record.trace_context.as_ref().expect("the inside event carries trace context");
        assert_eq!(tc.trace_id, span.span_context.trace_id());
        assert_eq!(tc.span_id, span.span_context.span_id());
        assert!(inside.record.attributes_iter().any(|(k, v)| k.as_str() == "node" && matches!(v, AnyValue::String(s) if s.as_str() == "mesh1.admin.1")));
        let outside = records.iter().find(|r| matches!(&r.record.body, Some(AnyValue::String(s)) if s.as_str() == "outside every span")).expect("the outside event");
        assert!(outside.record.trace_context.is_none());
        assert_eq!(outside.record.severity_number, Some(Severity::Warn));
    }
}
