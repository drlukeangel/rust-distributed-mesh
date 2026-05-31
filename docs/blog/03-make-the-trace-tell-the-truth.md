# 3 — Make the trace tell the truth

> Part 3 of *Building a multi-mesh substrate*. Three ways the distributed trace was lying, and the fixes.

By now there were two meshes and cross-mesh writes. But the moment you opened Jaeger's **System
Architecture** view, it lied to you three different ways. Each lie had a real cause.

## Lie 1: "there is one broker"

Every broker reported `service.name = "broker"`. Jaeger keys its dependency graph on service name, so
`mesh1.broker` and `mesh2.broker` **collapsed into a single node** — and the two-mesh write you'd just
shipped was invisible.

The fix is the standard OpenTelemetry resource hierarchy:

- `service.namespace` = the mesh (`mesh1`)
- `service.name` = `<mesh>.<type>` (`mesh1.broker`) — **this is what makes a node in the graph**
- `service.instance.id` = the full node id (the unique replica)

Each node derives all three from its own identity at boot (the friendly `node_name` carries the first
6 hex of that id). Suddenly the graph has the nodes that actually exist:

![System Architecture — distinct per-mesh nodes](../sprints/sprint-13/screenshots/jaeger-1-system-architecture.png)

![Cache with self-derived names](../sprints/sprint-13/screenshots/9-cache.png)

## Lie 2: the arrows only point one way

The write was fire-and-forget over a unidirectional stream, so the broker was a pure **sink** — it
received and recorded, but emitted nothing the trace could see. The graph showed `gateway → broker` and
nothing coming back, which isn't what a produce/ack actually is.

Two fixes, one mechanism. First, propagate **real W3C trace context** across the QUIC frame (we'd been
hand-rolling a `{trace_id, span_id, flags}` struct that dropped `tracestate`; now it's the standard
`traceparent`/`tracestate` carrier via the global propagator). Then the broker **continues the trace**:
it extracts the context, opens its own `produce.handle` span for its work, and sends an **ack** back.
One cross-mesh trace now reads:

![Cross-mesh produce → handle → ack trace](../sprints/sprint-13/screenshots/jaeger-2-produce-trace.png)

`frame.sent → produce.handle → produce.ack → frame.received` — the broker's work is in the trace, and
the ack gives the reverse edge. Arrows both ways, because the work genuinely goes both ways.

## Lie 3: the gateway "talks to" the console

The graph also showed a fat `gateway → admin-ui` edge. That edge wasn't data-plane topology at all — the
gateway had been **cc'ing a copy of every write to the console** so a UI tab would be non-empty. A
shortcut to light up a panel, masquerading as real traffic.

We deleted the cc. The message view now derives from the per-node frame counters that are already in the
gossip digests — a legitimate observation path — and the console stops appearing as a destination for
data it never receives.

![Messages from gossiped counters](../sprints/sprint-13/screenshots/3-messages.png)

## Takeaway

A dependency graph is only as honest as the spans under it. If a service name is too coarse, nodes
merge; if a receiver emits nothing, edges vanish; if you cc the observer, fake edges appear. Fix the
emission, not the picture. Next: making cross-mesh awareness itself scale — the backbone.
