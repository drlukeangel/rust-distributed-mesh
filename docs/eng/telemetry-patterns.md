# Telemetry patterns for RDM

How RDM code emits telemetry: which signals leave a process, how a process starts and ends its
telemetry, how trace context crosses a boundary, how spans are named, and what a test reads.
The contract is [docs/architecture/telemetry.md](../architecture/telemetry.md) (export, carriers,
where a trace starts) and [node-rpc-envelope.md §Runtime spans](../architecture/node-rpc-envelope.md#runtime-spans)
(the Node RPC spans and attributes); this page is how code meets it. The code is
`crates/rdm-transport/src/telemetry.rs` and the modules it names (`export`, `logs`, `watchdog`,
`transport_diag`, `evidence_capture`).

## 1. Signals

| signal | where it goes | source |
|---|---|---|
| spans | OTLP/HTTP protobuf to `<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/traces` | telemetry.md §1; `telemetry.rs::signal_url` |
| log records | OTLP/HTTP protobuf to `<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/logs`, each stamped with the trace and span it fired in | telemetry.md §1; `crates/rdm-transport/src/logs.rs` |
| blackbox evidence | with `RDM_EVIDENCE_DIR` set, every exported span also as JSONL in `<dir>/<service>.<pid>-<pid namespace inode>.spans.jsonl` | telemetry.md §1; `telemetry.rs` (the JSONL exporter) |
| the transport diagnostic file | only in an evidence run: the iroh and noq handshake and path lines and the node's own dial, accept and bind lines in `<RDM_DATA_DIR>/transport-diag.log`; never reaches span export or the log bridge | `crates/rdm-transport/src/transport_diag.rs` |
| stdout | the fmt layer, driven by `RUST_LOG` | `telemetry.rs::init_evidence` |

RDM exports no metrics signal. The runtime watchdog (`crates/rdm-transport/src/watchdog.rs`) logs
a window in which a process's async runtime ran nothing, with what every thread was doing; nothing
acts on it.

## 2. Starting and ending a process's telemetry

A process calls one `init_*` once, at its start, and holds the returned guard for its life:

| call | for | source |
|---|---|---|
| `rdm_transport::init_evidence_telemetry(service)` | every RDM binary: `rdm-node-admin` (`crates/rdm-admin/src/entry.rs`), every node built on `rdm-node` (`crates/rdm-node/src/lib.rs`), `rdm-rpc-node` | `telemetry.rs` |
| `rdm_transport::init_evidence_telemetry_for_one_call(service)` | a process that makes one call and ends (`rdm-rpc-probe`); it reports its init steps inside its call's root span (`report_init_steps_here`), so each run is one trace | `telemetry.rs` |
| `rdm_transport::telemetry::init_telemetry(service)` | a long-running service outside the node runtime (`demo/admin-ui`) | `telemetry.rs` |
| `init_telemetry_for_cli(service)` | a short-lived command-line process; no caller in the workspace today | `telemetry.rs` |

- `init_evidence_telemetry` exports to the evidence file when `RDM_EVIDENCE_DIR` is set and to
  the collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set; with neither, only the fmt layer runs
  and it returns `None`. It installs the W3C TraceContext propagator.
- Each step of the init is timed and reported as spans once the subscriber exists
  (`telemetry.rs::InitSteps`, `init_steps`).
- Exporters run on their own thread. The batch processors export every 200 ms, a full queue
  (65,536) drops, and one export request takes at most 3 s (`EXPORT_TIMEOUT`,
  `crates/rdm-transport/src/export.rs`).
- **Ending.** Dropping the `TelemetryGuard` flushes and shuts the exporters down. A deliberate
  `std::process::exit` skips `Drop`, so call `rdm_transport::flush_before_exit()` first: spans
  closed just before the exit (the reason it exits) reach the evidence file and the collector.
  Every exit flush waits at most `EXIT_FLUSH_BOUND` (2 s): an unreachable collector never holds an
  exit, and a flush that ends at its bound names what it dropped on stderr (telemetry.md §1
  "Exit"; `export.rs`).
- A test never calls an `init_*`: one test executable is one process, and a cell captures with a
  thread-scoped `EvidenceCapture` ([test-otlp-capture.md](test-otlp-capture.md) §3).

## 3. Service and instance identity

The resource every span and log record carries (`telemetry.rs::build_resource_with`):

| attribute | value |
|---|---|
| `service.name` | `OTEL_SERVICE_NAME`, else the name the binary passes to `init_*` (`rdm-rpc-node`, `rdm-rpc-probe`, the executable's own name for an `rdm-node` node). It names the host process, never the caller: `caller_system` names the caller (telemetry.md §1) |
| `process.pid` | the process's pid |
| `rafka.node` | `RDM_NODE_NAME` (the node's `path.name`), when the launcher set it |
| `service.instance.id` | a node-admin's node id: its launch's `RDM_NODE_ID`, else the one the bootstrap's first step minted (`init_evidence_telemetry_for_instance`, Q208) |
| `rafka.data_dir` | `RDM_DATA_DIR`, when set |
| `rafka.evidence_dir` | `RDM_EVIDENCE_DIR`, when set |

Many estates and births share one collector; these are what tell one process from another.

## 4. Carrying context across a boundary

Every carrier uses W3C `traceparent` and `tracestate`; a receiver's span is the sender's span's
child when the context arrived and parses, and a new root otherwise. Trace context is observability
only: it never decides dispatch, routing or execution, and a malformed part is dropped by name
while the call or frame continues (telemetry.md §2). The carriers are telemetry.md §3's table:

| boundary | what carries it | what the code does |
|---|---|---|
| a Node RPC call | the call's `CallContext` | the runtime does it: the caller's `rdm.node_rpc.request.update.via-call`, the server's `rdm.node_rpc.request.serve.via-direct` as its child; a dropped context is `rdm.node_rpc.request.update.via-context-dropped` |
| a forwarded call (Forward `0x1A`) | the inner call keeps the origin's `CallContext` | the runtime does it (`rdm.node_rpc.request.serve.via-carried-inner`) |
| a gossip frame | the envelope's `traceparent` and `tracestate` | publish from inside the span the frame belongs to; the receive span is its child ([gossip.md §3.4](../architecture/gossip.md#34-the-envelope)) |
| a node's launch | `TRACEPARENT` (and `TRACESTATE`) in the launch environment | the boot span `rdm.mesh.node.create.via-deployment` is the `DeployRuntime` step's child; the launcher's own values never reach the child |
| a data-plane connection | the connection's present frame | the tunnel span `rdm.node.connection.create.via-tunnel` is the opener's child |

For a carrier of your own, the helpers are `rdm_transport::current_traceparent()`,
`current_tracestate()`, `traceparent_of(&span)`, `set_parent(&span, traceparent)` and
`set_remote_parent(&span, traceparent, tracestate)`; a malformed value leaves the span a root and
never panics (`telemetry.rs`).

**Where no parent edge exists, identity joins the traces.** Every Build-backed span carries
`build_id` and, where it applies, `attempt`, `operation`, `step`, `node_id` and `incarnation`
(telemetry.md §2). The fabric's change, a timer's pass and a call that arrives without context
start roots of their own (telemetry.md §4), joined by those fields.

## 5. Async code

A span guard (`let _g = span.enter()`) held across an `.await` is refused: the
`no_span_guard_across_await` ratchet scans non-test code under `crates/`, `demo/` and `tools/`
(`tools/mesh-audit/src/lock.rs`, `Ratchet::NoSpanGuardAcrossAwait`; run by
`tools/mesh-audit/tests/lock_ratchets.rs::the_rdm_tree_is_green_under_every_ratchet` in the
gate's audit job). Attach the span to the future with `tracing::Instrument::instrument(span)`, or
scope the guard so it drops before the `.await`.

## 6. What span export carries

Span export is a fixed filter, independent of `RUST_LOG` (`telemetry.rs::span_export_filter`):

- off by default;
- INFO from RDM's own targets: `rafka*`, `rshape*`, `rdm*` and `main` (each crate's test
  executable); DEBUG instead with `RDM_OTEL_DEBUG=1`;
- iroh, iroh-gossip (and its `rg3` diagnostics), noq, netwatch, quinn, the HTTP stacks and the
  OpenTelemetry crates off, whatever `RUST_LOG` says.

So a span's level decides whether production exports it: an `info_span!` is exported, a
`debug_span!` only with `RDM_OTEL_DEBUG=1`. RDM configures no sampler: every span the filter
admits is exported.

**Hot paths.** Per-call pre-body INFO spans are forbidden; counters carry storm-rate information
and caller spans carry individual refusal evidence; protocol and domain spans nest below the
invocation span ([node-rpc-envelope.md §Runtime spans](../architecture/node-rpc-envelope.md#runtime-spans)).

**No payload in a span.** No byte of a frame's or a call's body, and never a declared secret or
RDM's own secret material, is written as a span attribute (telemetry.md §2).

## 7. Logs

- A `tracing` event inside a span travels on that span as a span event (the JSONL record's
  `events`), and every event also becomes one OTLP log record stamped with its trace and span id
  (`crates/rdm-transport/src/logs.rs`).
- The log bridge exports from WARN by default; `RDM_OTLP_LOG_LEVEL` sets it, independent of
  `RUST_LOG` and of span export (`telemetry.rs::otlp_log_export_level`).
- A fact a test or a reader needs belongs in a span or a span attribute; a log line is for a
  person reading one process.

## 8. Span names

Every rule below is stated on a page or in a canon file; the source is named with each. RDM's
generated census of every span name and its source line is
[docs/reference/rdm/spans.md](../reference/rdm/spans.md) (written by `rdm-docgen` from
`rdm-mesh-audit`'s scanner, `tools/mesh-audit/src/telemetry.rs::emitters`).

### 8.1 The shape

```text
<owner>.<component>.<entity>.<action>.<reason>
rdm   . node_rpc  . request. serve  . via-direct
```

Five dot-separated segments (telemetry.md §1, `<owner>.<domain>.<entity>.<action>.via-<reason>`;
`observability-conventions.md` in `rafka-v2`, §"Full span-name shape"):

| segment | rule | source |
|---|---|---|
| `<owner>` | `rdm` for every span RDM's code emits; an application's handlers keep their own owner (`rafka`), third-party spans their native namespace | telemetry.md §1; node-rpc-envelope.md §Runtime spans |
| `<component>` | one lowercase snake_case word: a node kind or a shared crate. RDM's are `node_admin`, `node_rpc`, `mesh`, `node`, and the test kit's `testkit`. There is no `ui` component: a span is named for its emitter, never for the surface that renders it | node-identity-and-state.md §9 rule 1; observability-conventions.md §"Full span-name shape" |
| `<entity>` | one lowercase snake_case word: what the action is about (`request`, `connection`, `node`, `topology`, `build`) | observability-conventions.md §"Full span-name shape" |
| `<action>` | one verb from the whitelist (§8.2) | node-identity-and-state.md §9 rule 2; observability-conventions.md §"Verb whitelist" |
| `<reason>` | kebab-case, `via-` for the direct trigger, `from-` for a derived one (§8.3) | node-identity-and-state.md §9; observability-conventions.md §"Reason prefixes" |

The owner is enforced: the `rdm_spans_are_rdm_prefixed` ratchet fails any span RDM's non-test
source emits that is not `rdm.*` (the chaos backends' `rafka.chaos.*` in `crates/rdm-test/src/`
excepted) and requires `rdm.node_rpc.request.serve.via-direct` in
`crates/rdm-transport/src/server.rs` (`tools/mesh-audit/src/lock.rs`,
`Ratchet::RdmSpansAreRdmPrefixed`). In a cell, `assert_own_story` fails a capture holding any
non-`rdm.*` span ([test-otlp-capture.md](test-otlp-capture.md) §3).

### 8.2 The verb whitelist

The action is one of these twelve; no compound verb, no synonym (observability-conventions.md
§"Verb whitelist", which node-identity-and-state.md §9 rule 2 names "the whitelist"):

| verb | use for | RDM example |
|---|---|---|
| `create` | an entity comes into being | `rdm.mesh.node.create.via-deployment` (`crates/rdm-node/src/lib.rs`) |
| `update` | an existing entity's persisted state changes | `rdm.node_rpc.request.update.via-call` (`crates/rdm-transport/src/stream.rs`) |
| `delete` | an entity is removed | `rdm.mesh.node.delete.via-signal` (`crates/rdm-admin/src/entry.rs`) |
| `add` | an item is added to a collection (a relationship, not a record) | `rdm.mesh.node.add.via-alpn-registered` (`crates/rdm-node/src/node.rs`) |
| `remove` | an item is removed from a collection | `rdm.node_rpc.node.remove.via-topology` (`crates/rdm-node/src/node_rpc.rs`) |
| `recompile` | a compile pipeline runs again | none in RDM |
| `resolve` | a lookup that produces an answer | `rdm.node_rpc.route.resolve.via-connections` (`crates/rdm-transport/src/route.rs`) |
| `returns-unknown` | the caller is answered exactly as for a target that does not exist | `rdm.node_admin.deployment.returns-unknown.via-proc-unreadable` (`crates/rdm-admin/src/deployment/process.rs`) |
| `match` | a predicate that returns hit or miss | none in RDM |
| `reject` | a request or input is refused | `rdm.node_rpc.request.reject.via-busy` (`crates/rdm-transport/src/server.rs`) |
| `evict` | derived state is dropped, the record it was derived from kept | `rdm.node_rpc.connection.evict.via-incarnation-superseded` (`crates/rdm-transport/src/pool.rs`) |
| `serve` | a request's execution from receipt through its response | `rdm.node_rpc.request.serve.via-direct` (`crates/rdm-transport/src/server.rs`) |

`add`/`remove` change a collection; `update` changes a record (observability-conventions.md
§"Verb whitelist"). A lookup is `resolve` only when it produces an answer; a milestone with no
honest verb of its own is a child span under the parent that holds the verb, never a whitelist
word forced onto it (observability-conventions.md, "No verb-laundering").

### 8.3 The reason

- **Kebab-case, with a prefix.** `via-<cause>` when the code this span is in caused the action
  directly (`rdm.node_rpc.request.reject.via-busy`); `from-<cause>` when it fires because
  something else happened (`rdm.mesh.topology.update.from-node-started`,
  `crates/rdm-transport/src/topology.rs`) (node-identity-and-state.md §9;
  observability-conventions.md §"Reason prefixes").
- **The reason carries the discriminator.** What distinguishes one outcome or path from another
  goes in the name, never in an attribute: `reject.via-busy` and `reject.via-not-ready` are two
  names, not one `reject` with a `kind` attribute. Attributes carry data: ids, counts, the typed
  `code` and `detail` (node-identity-and-state.md §9 rule 2; observability-conventions.md
  §"Don't use attrs as discriminators").

### 8.4 Rules every name keeps

1. **Every span name is a literal** (node-identity-and-state.md §9 rule 2): never formatted from a
   variable, so the census reads it from source and a reader can grep it.
2. **An attempt never reads as a success** (§9 rule 3): a span that names an attempt carries its
   outcome in an `outcome` attribute or in the reason, never in a name that is silent about which
   way it went (`rdm.mesh.node.update.via-pre-node-ready-attempt`).
3. **A node is named by its id and its `name`, never by its `endpoint_id`** (§9 rule 4). The id
   attribute is the same external form an API answer renders, from the same renderer. A span
   carries `endpoint_id` only when its subject is the routing key itself: a dial's target, the
   handshake, the identity load or mint before an id exists.
4. **Every lifecycle transition is observable as a span** (§9, "Design rule"), and every primitive
   and workflow step of node-admin emits `rdm.node_admin.node.<verb>.via-…`; each
   [lifecycle page](../architecture/fabric-mesh-node-lifecycles.md) lists its step spans
   (node-identity-and-state.md §9).
5. **A call is a pair.** Every Node RPC call emits `rdm.node_rpc.request.update.via-call` on the
   caller (`target`, `op`, `outcome`, `reason`, `reused`, `elapsed_ms`, `code`, `detail`) and
   `rdm.node_rpc.request.serve.via-direct` on the server (`protocol`, `op`, `peer`,
   `caller_system`, `context_dropped`, `test_case`, `scenario`, `operation`, `code`, `detail`);
   `code` is the standard gRPC name and `detail` names the typed result or refusal. A stream's end
   is `rdm.node_rpc.stream.serve.via-terminal`, a child of its serve span
   (node-rpc-envelope.md §Runtime spans).
6. **Build-backed spans carry the Build's identity:** `build_id` and, where it applies,
   `attempt`, `operation`, `step`, `node_id` and `incarnation` (telemetry.md §2;
   node-rpc-envelope.md §Runtime spans).
7. **No banned word in any segment.** CLAUDE.md's vocabulary bans hold in a span name as in any
   identifier: connections are `delete`d (`connections-deleting`), never drained; a primary is
   the fabric-primary, the mesh-primary or a node-type primary.
8. **Two names that look redundant may be kept apart on purpose**: a refusal because the store did
   not answer is a separate name from a read that found nothing. Check a merge of two names
   against what each one tells a reader before making it (observability-conventions.md
   §"Names that must stay distinct").

## 9. Reading telemetry in a test

A cell asserts on spans the code emits on its normal path: its own capture in-process, or an
estate's evidence files ([test-otlp-capture.md](test-otlp-capture.md)). Assert the parent edge
(`descends_from`), not only that one span's interval contains another's, and assert an absence
only after the work that would have emitted the span has finished.

## 10. The collector

The gate and the test runner export to the collector at `http://192.168.68.99:5318` (OTLP/HTTP);
Jaeger's UI is `http://192.168.68.99:16687`; ClickHouse is `http://192.168.68.99:18123`,
table `otel.otel_traces` ([test-otlp-capture.md](test-otlp-capture.md) §5). The binaries' own
default is `http://localhost:4318` (CLAUDE.md env table).

## Checklist for new instrumentation

- [ ] The process calls one `init_*` and holds the guard; a deliberate exit calls
      `flush_before_exit()` first.
- [ ] Every new span name is a literal of five segments, `rdm.`-owned, with a whitelist verb and a
      `via-`/`from-` reason that carries the discriminator.
- [ ] An attempt's outcome is in an attribute or the reason; a node is named by id and `name`.
- [ ] Build-backed work carries `build_id` and the identity fields that apply.
- [ ] No guard held across an `.await`; no payload or secret in an attribute.
- [ ] Production-volume spans are DEBUG; the lifecycle transition, the decision and the refusal
      are INFO.
- [ ] The lifecycle page that owns the step lists the span.
