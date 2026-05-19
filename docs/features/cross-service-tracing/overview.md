# cross-service-tracing — overview

> **Source:** Substrate observability feature. W3C TraceContext propagation across HTTP boundaries so a single trace_id links rfa → topology-ui → subprocess.spawned in Jaeger.

## What it is

When `rfa` (client) calls `topology-ui` (server), the outbound HTTP request carries a `traceparent` header encoding the current OTel context. topology-ui's axum middleware extracts the header on inbound, calls `set_parent` on the `rafka.ui.http.request` span, and all subsequent spans in topology-ui's handler chain inherit the same trace_id.

Result: an operator clicks any `rafka.cli.command` trace in Jaeger and sees the full causal chain through the HTTP boundary into the subprocess.spawned span — one trace, multiple services.

## How it works

`crates/rafka-telemetry/src/lib.rs::install_propagator()`:
```rust
opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
```
Called by both `init_telemetry()` (long-running) and `init_telemetry_for_cli()` (CLI). Without the global propagator, no automatic header injection/extraction happens.

`cli/rfa/src/main.rs::current_traceparent_headers()`:
```rust
let mut headers = reqwest::header::HeaderMap::new();
let ctx = tracing::Span::current().context();
global::get_text_map_propagator(|p| p.inject_context(&ctx, &mut HeaderInjector(&mut headers)));
headers
```
Called inside each `http_get`/`http_post`/`http_delete` async block (inside `.instrument(span)` so `current()` returns the http.request span).

`topology-ui/src/main.rs::trace_middleware`:
```rust
let parent_ctx = global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(req.headers())));
let span = info_span!("rafka.ui.http.request", ...);
span.set_parent(parent_ctx);
next.run(req).instrument(span).await
```
Axum middleware wraps every incoming request.

## Verified end-to-end trace

```
rafka.cli.command           svc=rfa            (parent — CLI invocation)
  rafka.cli.http.request    svc=rfa            (outbound POST)
    rafka.ui.http.request   svc=topology-ui    (inbound, parented via traceparent)
      rafka.ui.subprocess.spawned svc=topology-ui (spawned child process)
```

All four under one `trace_id`. Sprint-10 fix commit a618aba landed this.

## Invariants

1. **Global propagator must be installed in EVERY binary that talks HTTP cross-service.** rfa, topology-ui (and any future binary participating in HTTP chains).
2. **Inject on the client side, extract on the server side.** Asymmetric only-extract or only-inject = no propagation.
3. **`current_traceparent_headers()` must run INSIDE `.instrument(span)`.** Otherwise `Span::current()` returns the parent (or nothing) — the child span's context won't be in the headers.

## Cross-references

* Sibling: [`telemetry-substrate`](../telemetry-substrate/overview.md), [`rfa-cli`](../rfa-cli/overview.md), [`topology-ui-waterfall`](../topology-ui-waterfall/overview.md).
* Code: `crates/rafka-telemetry/src/lib.rs::install_propagator`, `cli/rfa/src/main.rs::current_traceparent_headers`, `topology-ui/src/main.rs::trace_middleware`.
* Decisions: sprint-10 fix (commit a618aba).
