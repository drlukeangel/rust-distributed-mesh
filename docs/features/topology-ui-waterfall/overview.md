# topology-ui-waterfall — overview

> **Source:** Operator UI feature. The boot-waterfall slice of the topology UI; renders any node's `rafka.mesh.node.ready` trace as a horizontal cascade.

## What it is

`topology-ui` binary serves a single HTML page at `http://localhost:19090`. The page polls `/api/nodes` to populate a node-selector dropdown; when an operator picks a node, the page fetches `/api/boot-trace?service=<name>` and renders the rafka.* spans as colored horizontal bars (left position = `start_offset`, width = `duration`).

Per D-008: vanilla HTML+CSS+JS only. No React/Vue/Svelte/transpilation. Inline `<style>` + inline `<script>`. A future graph-rendering lib (vis-network or cytoscape.js) loads via CDN if/when the topology graph view is added.

## How it works

`topology-ui/src/main.rs` axum server:
- `GET /` → static HTML (inline const)
- `GET /api/nodes` → proxies Jaeger `/api/services`, filters to `{gateway, broker, compute, registry}`
- `GET /api/boot-trace?service=X` → proxies Jaeger `/api/traces?service=X&operation=rafka.mesh.node.ready&limit=1`
- `GET /api/heartbeat?service=X` → proxies Jaeger heartbeat query, returns latest peer_count + age
- `GET /api/health` → trivial `{"status":"ok"}` for monitoring

Frontend JS:
- On load + every 30s: refetch /api/nodes, repopulate dropdown
- On dropdown change: fetch /api/boot-trace, render waterfall
- On Refresh button: refetch dropdown + currently-selected node's waterfall

## Locked spans

- `rafka.ui.http.request{method, path, otel.kind="server"}` — every inbound HTTP request, set_parent from W3C traceparent if present
- `rafka.ui.jaeger.query{endpoint, service, otel.kind="client"}` — every outbound Jaeger call

## Color palette (per phase prefix)

| Phase | Color |
|---|---|
| `rafka.mesh.node.ready` | dark blue (root) |
| `rafka.mesh.boot.identity_*` | green |
| `rafka.mesh.boot.endpoint_created` | amber |
| `rafka.mesh.boot.alpn_registered` | purple |
| `rafka.mesh.boot.gossip_started` | teal |
| `rafka.mesh.boot.accept_loop_started` | red |

## Invariants

1. **No SPA framework.** Per D-008.
2. **HTML/CSS/JS inline in main.rs** (small KISS — no static/ dir + ServeDir).
3. **Filter to known node types** when proxying Jaeger services list — stale services from prior naming attempts must not pollute the dropdown.

## Cross-references

* Sibling: [`subprocess-control`](../subprocess-control/overview.md), [`rfa-cli`](../rfa-cli/overview.md).
* Code: `topology-ui/src/main.rs`.
* Decisions: D-007 (topology-ui as separate process), D-008 (no SPA framework), D-026 (HTTP allowed on topology-ui as observability surface).
