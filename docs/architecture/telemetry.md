# Telemetry: how a trace runs through RDM

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node RPC envelope](node-rpc-envelope.md#runtime-spans) · [Gossip envelope](gossip.md#34-the-envelope)

This page owns how RDM's telemetry leaves a process and how one W3C trace crosses processes:
Node RPC calls and workflows, gossip frames, a launch, data-plane connections and the fabric's
change. The per-operation span lists stay on each lifecycle page; the naming grammar and the code
attribute stay on the Node RPC envelope's [Runtime spans](node-rpc-envelope.md#runtime-spans).

## 1. Export

| What | Where |
|---|---|
| Spans | OTLP/HTTP protobuf to `<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/traces` |
| Log records | OTLP/HTTP protobuf to `<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/logs` |
| `service.name` | `OTEL_SERVICE_NAME`, else the binary's own default: it names the host process, never the caller (`caller_system` names the caller) |
| Blackbox evidence | with `RDM_EVIDENCE_DIR` set, every span also as JSONL in `<dir>/<service>.<pid>.spans.jsonl` |
| Exit | the exit flush is bounded and names its bounds on stderr; a full stderr pipe never holds the exit |

Span names follow `<owner>.<domain>.<entity>.<action>.via-<reason>`. RDM's own spans are `rdm.*`;
an application's handlers keep their own owner (`rafka.*`), and third-party spans keep their native
namespace ([Runtime spans](node-rpc-envelope.md#runtime-spans)).

## 2. The rules

- **Trace context is observability only.** It never decides dispatch, routing, authorization or
  execution. A malformed or oversized part is dropped by name, and the call or frame continues.
- **One W3C context, one shape.** Every carrier below uses W3C `traceparent` and `tracestate`. A
  receiver's span is a child of the sender's span when the context arrived and parses, and a new
  root otherwise.
- **No payload enters a span.** No byte of a frame's or a call's body, and never a declared secret
  or RDM's own secret material, is written as a span attribute.
- **Identity correlates where no parent edge exists.** Every Build-backed span carries `build_id`
  and, where it applies, `attempt`, `operation`, `step`, `node_id` and `incarnation`. A trace that
  starts on its own (§4) is joined to the operation that caused it by those fields.

## 3. The carriers

| Carrier | Where the context rides | Sender | Receiver |
|---|---|---|---|
| Node RPC call | the call's `CallContext` (`traceparent`, `tracestate`, `baggage`; [Section 1](node-rpc-envelope.md#section-1-the-context)) | the caller's `rdm.node_rpc.request.update.via-call` span | `rdm.node_rpc.request.serve.via-direct` shares the TraceId; its parent is the call span; the handler's spans and the invocation's end are its children |
| Forwarded call (Forward `0x1A`, a carried inner call) | the inner call keeps the origin's `CallContext` | the origin's call span | the carrier's hop span and the destination's `rdm.node_rpc.request.serve.via-carried-inner` are children of the origin's trace |
| Workflow (one Build) | each child call's `CallContext` | the workflow's span | every child call of one workflow shares the operation's trace, each with its own caller span; a completion sent upward is its own call with its own target and trace context |
| Gossip frame | the envelope's `traceparent` and `tracestate` (the CloudEvents Distributed Tracing extension, [gossip.md §3.4](gossip.md#34-the-envelope)) | the span the publisher is in when it publishes | the receive span is a child of the publishing span; a frame published from a timer carries that timer's span, which is a root of its own |
| Launch of a node | `TRACEPARENT` (and `TRACESTATE`) in the launch environment | the pipeline's `DeployRuntime` step | the node's boot span `rdm.mesh.node.create.via-deployment` is that step's child. The launcher's own `TRACEPARENT` and `TRACESTATE` never reach the child ([deployment-provider.md](deployment-provider.md)) |
| Data-plane connection (Direct or Proxy, on an app protocol) | the connection's present frame | the opener's span around `open` | a carrier's tunnel span `rdm.node.connection.create.via-tunnel` is its child and passes its own context on to the destination; stream bytes carry no context, and an application's own spans on a stream are its own |
| Accepted connection (Direct or Proxy, handed to the application) | the accepted `Connection`'s `traceparent()` and `tracestate()`: the opener's context, as the connection's present frame carried it | the opener's span around `open`; a Proxy's destination takes the carrier's tunnel span, that span's child | an application span made from the accepted `Connection`'s context is the open's child and shares the opener's trace |
| Deletion operation (a `LifecycleOp` handed to the application, `DigestView::deleted()`) | the `LifecycleOp`'s `traceparent` and `tracestate`: the operation's context, taken from the envelope of the lifecycle frame that brought it ([gossip.md §3.4](gossip.md#34-the-envelope)); never written in the frame's body or a receipt | the executor's span when it publishes the operation, in the Build's trace under its `build.create` caller | an application span made from the `LifecycleOp` is the operation's child and shares the delete's trace; an operation learned only from a `MeshTopology` carries none |
| `pre_node_ready` and the `on_node_ready`, `on_mesh_ready`, `on_fabric_ready`, `on_build_started` and `on_build_ended` notices | the hook's context, and the span RDM calls it in (`rdm.mesh.node.update.via-pre-node-ready-attempt`, `rdm.mesh.node.update.via-on-node-ready`, `rdm.node_admin.mesh.update.via-on-mesh-ready`, `rdm.node_admin.fabric.update.via-on-fabric-ready`, `rdm.node_admin.build.update.via-on-build-started`, `rdm.node_admin.build.update.via-on-build-ended`) | the start step, the mesh's record write, the fabric change or the Build attempt's claim or terminal that calls it | the application's spans in the hook are children of that span |

## 4. Where a trace starts

These start a root of their own today:

- **A call that arrives without context.** A person's or an outside caller's call; its serve
  span is the root, and everything the call causes in that process is its child.
- **A Day-0 or person-started node-admin.** Its boot span takes `TRACEPARENT` when it is given,
  and is a root otherwise.
- **The fabric's change.** Each change from ready (or an adopted one) opens
  `rdm.node_admin.fabric.update.via-change` as a root. Its state entries and the
  `on_fabric_ready` notice are its children. It carries `build_id`. It is not a child of the Build's `build.create`: the Build that
  caused it and the change are two traces, joined by `build_id`.
- **Loops on a timer.** Each pass of the Build drive (`rdm.node_admin.build.update.via-drive`),
  the status turn (`rdm.mesh.fabric.update.via-status-send`), the heartbeat
  (`rdm.mesh.node.update.via-heartbeat`), the election's recompute and the topology view's own
  sweeps is a root per pass, joined by `build_id`, `node_id` or `publisher`.
- **Gossip receipts of a frame published from a timer.** They are children of that timer pass's
  root, not of an operation.
- **The dial of a connection.** The pooled Node RPC dial (`rdm.node_rpc.connection.create.via-dial`)
  and a data-plane connection's dial (`rdm.node.connection.create.via-dial`) are each a root,
  linked (follows_from, the OpenTelemetry link for causally related work that is not nested) to the
  span that started it: the call that started the pooled dial, and the opener's span around `open`.
  The transport's connect gives the connection's driver task the span current at that moment for
  as long as the connection lives, so a dial is never a child of the call or the open, and never the
  current span while it runs; the pooled dial also outlives the wait of the call that started it.

## 5. Following one operation

| Question | Read |
|---|---|
| A call and what it did on the target | one trace: the caller's call span, the target's serve span and its handler's spans |
| A Build from request to Complete | the `build.create` trace for acceptance; the drive passes and the attempt's calls by `build_id` and `attempt`; each step's span and receipt by `operation` and `step` |
| The fabric's change a Build caused | the `via-change` root whose `build_id` is the Build's |
| A node's birth | the `DeployRuntime` step's trace, through the node's boot span, to its join and `pre_node_ready` |
| A gossip fact | the receive span's parent (the publishing span), and the frame's `publisher`, `topology_version`, `snapshot_id` and identity fields |
| A connection | `rdm.node_admin.connection.update.via-observed` by `entry`, `source`, `destination` and `alpn`; a Proxy's tunnel span under the opener's trace |

The per-operation chains are on their pages:
- [fabric recovery](lifecycles/fabric-recovery.md#fabric-recovery-telemetry-chain);
- [mesh investigation](lifecycles/mesh-investigation.md#telemetry-chain);
- [the fabric's ready](lifecycles/states.md#the-fabrics-ready).

## 6. Not carried today

- The fabric's change is not a child of the Build that caused it (§4); they are joined by
  `build_id`.
- A destination's admission of a data-plane connection writes its connection fact
  (`via-observed`) without a parent edge to the opener's trace; it is joined by the connection's
  `source`, `destination` and `alpn`.
