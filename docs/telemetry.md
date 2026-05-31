# Telemetry — framework + the service-hierarchy correction

**Status:** current framework documented + the correction scoped for **sprint-13** (telemetry hierarchy).
**Date:** 2026-05-31.

This is the contract for how rafka-v2 emits and propagates traces. Principle #6: telemetry IS the
substrate. Principle #10: the span/metric/service vocabulary is locked once, not invented per sprint.

---

## Part A — the CURRENT framework (as built, sprint-01 … sprint-12)

### Crate: `rafka-telemetry`
- `init_telemetry(service_name)` — long-running nodes. `BatchSpanProcessor`, 200 ms scheduled delay.
- `init_telemetry_for_cli(service_name)` — short-lived CLIs (`rfa`). `SimpleSpanProcessor` (sync export).
- Exporter: **OTLP/gRPC (tonic)** → `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4316`,
  straight to Jaeger).
- `TelemetryGuard::drop` force-flushes + shuts down the provider.
- Subscriber: `tracing-subscriber` registry + `OpenTelemetryLayer`. **Both layers floor at INFO**
  (leak fix: a DEBUG firehose from iroh/gossip was retained on long-lived `#[instrument]` span event
  buffers). `RUST_LOG` can raise specific targets.

### Resource (the problem)
`build_resource()` constructs `Resource::new(vec![SERVICE_NAME = <name>])` — **only** the service name.
Because `Resource::new` *replaces* rather than *merges* env-detected attributes,
`OTEL_RESOURCE_ATTRIBUTES` is **silently ignored**. There is no `service.namespace`, no
`service.instance.id`.

### Service naming (the problem)
Each node is spawned with `OTEL_SERVICE_NAME = <node_type>` (`admin-ui/src/main.rs:2853`) — i.e. flat
`broker`, `gateway`, `compute`, `registry`. **Consequence:** `mesh1.broker1` and `mesh2.broker1` both
report `service.name = "broker"`, so **Jaeger's System Architecture graph collapses them into one
`broker` node** — the two-mesh write is invisible; you cannot tell intra-mesh from cross-mesh delivery.

### Trace-context propagation
Two different mechanisms today:
- **HTTP hops** (`rfa → topology-ui`): the global **W3C `TraceContextPropagator`**
  (`set_text_map_propagator`, `lib.rs:57`) via `traceparent` headers. Standard.
- **Mesh hops over QUIC** (gateway → broker, etc.): a **hand-rolled** binary struct, NOT W3C.
  `InternalMeshFrame::encode_with_context` (`rafka-mesh-ops/src/lib.rs:50`) packs
  `TracedFrame { trace_id, span_id, flags }` (postcard, tag `0x10`); `decode_with_context` rebuilds a
  `SpanContext` via `with_remote_span_context`. It works (cross-mesh parent↔child links correctly), but:
  - it is **not** the W3C `traceparent` wire format / does not go through the global propagator;
  - it **drops `tracestate`** (`decode_with_context` hardcodes `TraceState::default()`, line 74).

### Locked span / metric vocabulary
See CLAUDE.md §10 (`rafka.mesh.node.ready`, `boot.*`, `heartbeat`, `peer.*`, `frame.sent/received`,
`op_kind` enum, `node_type` enum, the metric table). The write-sim `frame.sent`/`frame.received` are
emitted at **INFO** (exported) — they are what draw the Jaeger dependency edges.

### Known observability artifact (the "smell")
Only the gateway runs the write-sim; it writes to `mesh1.broker1` + `mesh2.broker1`, and additionally
**cc's a copy of every write to `admin-ui`** (`rafka-node-base/src/lib.rs:494–499`) so the Messages tab
is non-empty (admin-ui's `message_ring` only captures frames it receives). That CC produces a
`gateway → admin-ui` edge in the dependency graph that is **not real data-plane topology** — it exists
only to populate a UI tab.

---

## Part B — the CORRECTION (sprint-13 scope)

Goal: Jaeger's System Architecture graph reflects **real two-mesh topology** — distinct per-mesh nodes,
standard W3C propagation, no observability artifacts.

### B1. Service hierarchy (OTel semantic conventions)

| node_type | abbrev | node_name (RAFKA_NODE_NAME) | service.name | service.namespace | service.instance.id |
|---|---|---|---|---|---|
| gateway   | gw | `mesh1.gateway.gw1`  | `mesh1.gateway`  | `mesh1` | `gw1` |
| broker    | br | `mesh2.broker.br1`   | `mesh2.broker`   | `mesh2` | `br1` |
| compute   | cp | `mesh1.compute.cp1`  | `mesh1.compute`  | `mesh1` | `cp1` |
| registry  | rg | `mesh1.registry.rg1` | `mesh1.registry` | `mesh1` | `rg1` |
| admin-ui  | —  | `admin-ui`           | `admin-ui`       | `admin` | (host) |

- **`service.name = <mesh>.<type>`** — this is what dictates the Jaeger graph node. Mesh-qualified so
  each mesh's broker/gateway is its own node.
- **`service.namespace = <mesh>`** — logical boundary (`mesh1` / `mesh2`).
- **`service.instance.id = <abbrev><N>`** — the specific replica (`gw1`, `br1`, `br2`).
- **`node_name` becomes the full dotted form** `mesh1.gateway.gw1` (replaces the old `mesh1.gateway1`);
  the admin-ui assigns it on spawn and derives the three OTel fields from its three segments.

### B2. Telemetry crate must emit the full resource
`rafka-telemetry::build_resource()` must add `service.namespace` and `service.instance.id` (read from
`OTEL_RESOURCE_ATTRIBUTES`, or set explicitly), not just `SERVICE_NAME`. Fix the `Resource::new` replace
vs merge so env attributes survive.

### B3. W3C propagation over QUIC (no half measure)
Replace the hand-rolled `TracedFrame` ("transaction vec") with the **global W3C propagator**:
`global::get_text_map_propagator().inject_context(&ctx, &mut carrier)` on send and `.extract(&carrier)`
on receive, with the carrier (traceparent + **tracestate**) embedded in the frame. Standard format,
`tracestate` preserved, single propagation mechanism for HTTP and mesh. The extracted context is then
**used as the parent of the broker's own action spans** (B5) — that's the whole point of propagating it.

### B4. Drop the admin-ui CC
Remove the gateway→admin-ui write CC. Derive the Messages tab from a legitimate observation path (the
gossiped `frames_recv_total` per node, or a real read subscription) so the dependency graph shows only
true edges. No node should special-case sending copies to the observer.

### B5. Broker records its own work + ACKs — REQUIRED (missing broker activity is a telemetry flaw)
A trace that shows the gateway sending but nothing of what the broker DID is lying. Once the W3C context
arrives (B3), the broker must **continue the trace**:
- The write-sim moves from `open_uni` (fire-and-forget) to **`open_bi`** (request/response).
- On receiving a produce, the broker **extracts the traceparent** and opens its own child span(s) for
  its actions (e.g. `rafka.mesh.produce.handle`), then writes an **ACK** back on the response stream.
- The ACK carries the broker's span context, so the gateway's ack-receive span is a child of the
  broker's ack span → the dependency graph shows **`broker → gateway`** (the reverse arrow), and the
  broker's handling is visible in the trace waterfall under the gateway's produce span.
- New span names / `op_kind` (e.g. an `"ack"` op_kind or `produce.handle`/`produce.ack` spans) are added
  to CLAUDE.md §10 in the same commit (B6, Principle #10).

### B6. CLAUDE.md §10 update
Add the **service-name contract** (the table in B1) to CLAUDE.md §10 as a locked vocabulary item, in the
same commit (Principle #10 — vocabulary changes land in CLAUDE.md before/with the emit code).

### Acceptance (sprint-13)
- Jaeger **System Architecture** shows distinct nodes: `mesh1.gateway`, `mesh1.broker`, `mesh2.broker`
  (+ compute/registry per mesh), NOT a single collapsed `broker`.
- The cross-mesh write is visible as `mesh1.gateway → mesh2.broker`, **and the ack as `mesh2.broker →
  mesh1.gateway`** (arrows both ways).
- **No `gateway → admin-ui` artifact** edge.
- A produce **trace** shows the broker's own work: `gateway produce → broker handle → broker ack`, all
  one trace, the broker spans parented by the propagated W3C context.
- QUIC frames carry W3C `traceparent` + `tracestate`; a cross-mesh trace stitches end-to-end.
- UI + Jaeger screenshots (System Architecture graph + a produce trace waterfall) in the sprint folder;
  team lead personally verified.
