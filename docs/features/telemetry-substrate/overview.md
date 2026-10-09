# telemetry-substrate — overview

> **Source:** Substrate library crate. OTLP pipeline initialization with two modes: BatchSpanProcessor (long-running services) and SimpleSpanProcessor (short-lived CLIs).

## What it is

`crates/rafka-mesh-telemetry/` exposes `init_telemetry(service_name)` and `init_telemetry_for_cli(service_name)`. Every binary calls one of these in `main()` before any other work; both return a `TelemetryGuard` whose `Drop` flushes + shuts down.

Also installs the W3C `TraceContextPropagator` globally so HTTP-injected `traceparent` headers chain spans across services.

## Namespace ownership (normative)

RDM-owned instrumentation MUST use `rdm.<component>.<entity>.<action>.<reason>`.
Rafka-owned instrumentation uses `rafka.<component>.<entity>.<action>.<reason>`;
third-party instrumentation retains its native namespace. The retained customer chaos
harness uses `rafka.chaos.*` only inside `crates/rafka-chaos`. RDM's administration UI
and CLI implementation use `rdm.ui.*` and `rdm.cli.*`; their existing suffixes are preserved.
Historical sprint records retain the names emitted at the time of the record.

The span prefix identifies implementation ownership. OTel resource `service.name` identifies
the host process/service. `CallContext.caller_system` identifies invocation origin and MUST
remain observability-only, with no routing, authorization, ownership or execution semantics.
W3C `traceparent` supplies causal trace propagation. These identities MUST NOT be derived
from one another: RDM can emit `rdm.node_rpc.request.serve.via-direct` in host
`rafka-gateway` for origin `compute`. Do not encode the host node role in an RDM root.
Acceptance checks MUST compare TraceId and child ParentSpanId to parent SpanId, never
infer parentage from timestamps.

The `rdm_spans_are_rdm_prefixed` lock ratchet parses runtime Rust span macros and named
`#[instrument]` attributes throughout crates, tools and binaries. Tests, comments, assertion
literals and historical documentation are excluded. Its planted customer-prefix span fails
that rule alone; `cargo test -p rafka-mesh-audit --test main lock_ratchets::` also checks the clean
workspace and the explicitly retained chaos exception. `cargo run -p rafka-mesh-audit
--bin span_census` emits the runtime source census as JSON.

## How it works

`init_telemetry()`:
- BatchSpanProcessor with `scheduled_delay=200ms` (production cadence per `8d99f528` v1 evidence)
- OTLP gRPC exporter pointed at `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4316` — the `rafka-test-jaeger` direct ingest)
- Sets `service.name` resource attribute from `OTEL_SERVICE_NAME` env var (defaulting to the arg)

`init_telemetry_for_cli()`:
- SimpleSpanProcessor — synchronous export on span close
- Same exporter + propagator setup
- Used by `rfa` and any future short-lived CLI

`TelemetryGuard::Drop` calls `force_flush()` then `shutdown()` to drain pending spans before process exit.

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4316` | OTLP gRPC ingest URL |
| `OTEL_SERVICE_NAME` | (arg to `init_telemetry`) | Override service name |
| `RUST_LOG` | `info` | tracing-subscriber filter (controls stdout + OTel both) |

## Invariants

1. **Long-running services use BatchSpanProcessor.** High span volume; batched export amortizes overhead.
2. **Short-lived CLIs use SimpleSpanProcessor.** No batch flush race vs runtime teardown.
3. **W3C propagator is installed globally.** Both `init_telemetry` and `init_telemetry_for_cli` install it; required for cross-service trace propagation.

## Cross-references

* Code: `crates/rafka-mesh-telemetry/src/lib.rs`.
* Sibling: [`cross-service-tracing`](../cross-service-tracing/overview.md).
* Decisions: D-024 (telemetry artifacts prove sprint scope), sprint-10 fix (commit a618aba) — added init_telemetry_for_cli.
