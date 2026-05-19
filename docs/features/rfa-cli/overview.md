# rfa-cli — overview

> **Source:** Operator CLI. `rfa` = "rafka admin". Thin REST client targeting `topology-ui`'s internal API per D-009; ships with the cluster deployment.

## What it is

`cli/rfa/` is a clap-derive CLI binary. All commands talk to `topology-ui` on `http://localhost:19090` by default (override via `--api-url`). Two grammar tiers:

**Read-only:**
- `rfa mesh node list` — node types currently visible in Jaeger services list
- `rfa mesh node describe <name>` — node_id + boot span timings as table
- `rfa mesh topology show [--format dot|json|table]` — adjacency view
- `rfa mesh status` — per-node summary (peer_count, last heartbeat age)

**Mutations:**
- `rfa mesh node add --type <type>` — spawn new subprocess
- `rfa mesh node remove <name>` — kill subprocess
- `rfa mesh wait-converged --target N --timeout 30s` — block until ≥N node types present

**Chaos:**
- `rfa mesh chaos kill [--target X]` — primitive: kill_node
- `rfa mesh chaos restart [--target X]` — primitive: restart_node
- `rfa mesh chaos soak --duration 5m --interval 30s --seed 42` — random primitive loop with JSON report

## How it works

Each command:
1. `init_telemetry_for_cli("rfa")` at startup (SimpleSpanProcessor — synchronous OTel export).
2. Open `rafka.cli.command{command, args, otel.kind="internal"}` root span.
3. Build reqwest HTTP request; inject W3C `traceparent` header from current OTel context (`current_traceparent_headers()`).
4. Send + parse response inside a `rafka.cli.http.request{method, path, otel.kind="client"}` child span.
5. Render response per `--format`.

Cross-service trace propagation means topology-ui's `rafka.ui.http.request` span lands under the same trace_id as `rafka.cli.command` — operators can click any CLI invocation in Jaeger and see the full chain through topology-ui into the subprocess spawn span.

## Locked spans

- `rafka.cli.command{command, args, otel.kind="internal"}` — root span per invocation
- `rafka.cli.http.request{method, path, otel.kind="client"}` — child per outbound HTTP
- `rafka.cli.wait_loop{poll_count, target, current_count}` — wait-converged poll iterations

## Invariants

1. **rfa only talks to topology-ui.** No direct Jaeger query, no direct mesh participation. Per D-009 — single backend means no drift between UI + CLI state.
2. **SimpleSpanProcessor, no pre-exit sleep.** Per sprint-10 fix; short-lived process needs synchronous OTel export.
3. **traceparent injection on every reqwest call.** Cross-service propagation requires it.

## Cross-references

* Sibling: [`topology-ui-waterfall`](../topology-ui-waterfall/overview.md), [`subprocess-control`](../subprocess-control/overview.md), [`cross-service-tracing`](../cross-service-tracing/overview.md).
* Code: `cli/rfa/src/main.rs`.
* Decisions: D-009 (rfa is thin REST client), D-030 (CLIs at `cli/<name>/`).
