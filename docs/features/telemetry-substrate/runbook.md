# telemetry-substrate — runbook

## Failure modes

### Mode 1 — No spans reaching Jaeger

**Cause:** OTLP collector down OR endpoint URL wrong OR firewall blocking.

**Detection:**
```bash
podman ps --format "{{.Names}}\t{{.Status}}" | grep otel
# rafka-test-jaeger should be Up + healthy
# rafka-test-otel-collector should be Up

curl -v http://localhost:4316  # should connect, returns 405 for GET
```

**Recovery:**
```bash
podman compose -f E:/dev/rafka/deployment/dev/compose.test-otlp.yml up -d
# wait ~10s for jaeger healthcheck
```

### Mode 2 — Short-lived CLI loses spans on exit

**Cause:** Used `init_telemetry` (BatchSpanProcessor) instead of `init_telemetry_for_cli` (SimpleSpanProcessor). Batch processor's background task is torn down with the tokio runtime before flush completes.

**Recovery:** Switch the CLI binary to `init_telemetry_for_cli`. See `cli/rfa/src/main.rs` for reference.

### Mode 3 — Cross-service traces don't link (separate trace_ids per service)

**Cause:** W3C propagator not installed globally OR HTTP layer not injecting/extracting `traceparent` header.

**Detection:** rfa→topology-ui call should produce one trace spanning both services. If two separate traces: propagation broken.

**Recovery:** See [`cross-service-tracing runbook`](../cross-service-tracing/runbook.md).

### Mode 4 — Spans appear but lack expected attributes

**Cause:** Late-binding via `Span::current().record()` raced span close. OTel exported the span with empty fields before record() landed.

**Recovery:** Set attributes at span construction (in `info_span!` macro args) instead of after-the-fact via record(). Reserve record() for values that genuinely cannot be known at construction.

## Cross-references

* Parent: substrate.
* Sibling: [`cross-service-tracing runbook`](../cross-service-tracing/runbook.md).
