# cross-service-tracing — runbook

## Failure modes

### Mode 1 — Two separate traces per CLI call (no chain)

**Cause:** Either the client (rfa) isn't injecting `traceparent` OR the server (topology-ui) isn't extracting.

**Detection:**
```bash
TID=$(curl -s "http://localhost:16686/api/traces?service=rfa&operation=rafka.cli.command&limit=1&lookback=1m" | python -c "import sys,json; print(json.load(sys.stdin)['data'][0]['traceID'])")
curl -s "http://localhost:16686/api/traces/$TID" | python -c "import sys,json; d=json.load(sys.stdin); proc=d['data'][0]['processes']; print(sorted(set(proc.get(s.get('processID'),{}).get('serviceName','?') for s in d['data'][0]['spans'])))"
# expect ['rfa', 'topology-ui']; if only ['rfa'], propagation broken
```

**Recovery:**

- Confirm `install_propagator()` is called in both binaries' `init_telemetry*()`. Should be — both code paths invoke it.
- Confirm client injects: search for `current_traceparent_headers()` in `cli/rfa/src/main.rs::http_*`. The fn must be called INSIDE the `.instrument(span)` async block (not before), so `Span::current()` returns the http.request span at injection time.
- Confirm server extracts: `topology-ui/src/main.rs::trace_middleware` must call `propagator.extract(...)` + `span.set_parent(parent_ctx)`. Without `set_parent`, the inbound span is its own root.

### Mode 2 — Trace chains across services but root span attrs are missing

**Cause:** The PARENT span (cli.command) doesn't have its own context properly set when `current_traceparent_headers` runs — the child's traceparent is built from a default/empty context.

**Recovery:** Confirm `.instrument(cmd_span)` wraps the entire `run_command` call in main(). Headers should only be built inside http_* functions which are themselves inside `.instrument(span)` chains.

### Mode 3 — traceparent header looks OK on wire but Jaeger still shows separate traces

**Cause:** Jaeger received both spans but they have DIFFERENT trace_ids — `propagator.extract` returned a Context with no trace info because the header value was malformed.

**Recovery:** Curl the actual request from rfa with verbose mode to inspect the `traceparent` header:
```bash
# Modify rfa temporarily to print headers, OR use Wireshark / fiddler to capture
# Header format: `traceparent: 00-<32-hex-trace_id>-<16-hex-span_id>-<2-hex-flags>`
```
If the header looks valid, the bug is in topology-ui's extract path.

## Cross-references

* Parent: substrate.
* Sibling: [`telemetry-substrate runbook`](../telemetry-substrate/runbook.md), [`rfa-cli runbook`](../rfa-cli/runbook.md).
