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

| node_type | node_name (RAFKA_NODE_NAME) | service.name | service.namespace | service.instance.id |
|---|---|---|---|---|
| gateway   | `mesh1.gateway.f52746`  | `mesh1.gateway`  | `mesh1` | `f52746c5cdda…` (full node_id) |
| broker    | `mesh2.broker.ccff65`   | `mesh2.broker`   | `mesh2` | `ccff65329d41…bf42` (full node_id) |
| compute   | `mesh1.compute.3a23aa`  | `mesh1.compute`  | `mesh1` | `3a23aadeb87c…` (full node_id) |
| registry  | `mesh1.registry.05bd26` | `mesh1.registry` | `mesh1` | `05bd263e6e00…` (full node_id) |
| admin-ui  | `mesh1.admin-ui.bbc0f7` | `mesh1.admin-ui` | `mesh1` | `bbc0f7112f29…` (full node_id) |

**The admin-ui is a node like any other — no special treatment, and no invented mesh.** It is NOT a magic
`admin-ui` service or `admin`/`ops` mesh — there are only `mesh1` and `mesh2`. It declares a real
`RAFKA_MESH_ID` of an existing mesh (`mesh1` above — operator's choice between mesh1/mesh2, but it MUST be
set, no default), has `node_type = admin-ui` (append to the locked `node_type` enum, §10), and
**self-names `<mesh>.admin-ui.<6hex>`** exactly like every other node. It broadcasts its digest on its
home mesh and observes the other mesh via `RAFKA_OBSERVER_MESHES` — the same mechanism a gateway uses, not
a privileged path.

- **`service.name = <mesh>.<type>`** — this is what dictates the Jaeger graph node. Mesh-qualified so
  each mesh's broker/gateway is its own node.
- **`service.namespace = <mesh>`** — logical boundary (`mesh1` / `mesh2`).
- **`service.instance.id = <node_id>`** — the full unique physical replica (the iroh public key), the
  OTel-correct "pod-abc-123" analog.
- **`node_name = <mesh>.<type>.<first-6-hex-of-node_id>`** — e.g. `mesh2.broker.ccff65` (**6** hex
  digits, no `#`; keep the length as a single named constant). The tail IS the first 6 of
  `instance.id`, so a node is eyeball-matchable to its trace. **No ordinal counter.**
- **Every node SELF-NAMES (admin-ui included).** node_id is only known after the node mints/loads its
  identity at boot, so each node computes its own `node_name` from `{mesh_id}.{node_type}.{node_id[..6]}`
  and broadcasts it in the digest. The admin-ui assigns only `RAFKA_MESH_ID` + `node_type` to children
  and learns their final names from gossip — the per-`(mesh,type)` counter (`main.rs:1123`) is removed.
  The admin-ui names ITSELF the same way.
- **`mesh_id` is REQUIRED for EVERY node — there is no default, and no exception for the admin-ui.**
  Remove the `unwrap_or_else(|_| "default")` fallback (`rafka-node-base/src/lib.rs:122`). Any node
  (including the admin-ui) with no `RAFKA_MESH_ID` **fails fast** and refuses to boot — never a silent
  `"default"` (or magic `"admin"`) mesh. Drop the `|| 'default'` display
  fallbacks in the React UI too.

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
