# chaos-timeline — overview

> **Status:** SHIPPED. Live on the topology-ui Timeline tab.

## What it is

A real-time chronological view of every chaos primitive execution paired with its detection outcome. Operators see "what's happening now" and "is the substrate resolving it" in one column.

## How it works

`GET /api/chaos/timeline` queries Jaeger for `rafka.chaos.primitive.executed` and `rafka.chaos.primitive.detected` spans in the last 10 minutes. It matches them by `trace_id` (every chaos event runs in its own trace), then sorts newest-first.

Response shape:

```json
{"events": [
  {
    "when": "3s ago",
    "primitive": "nat_shift",
    "description": "Restart target with new random RAFKA_NODE_BIND_ADDR. iroh must re-discover the NodeId at the new ephemeral port.",
    "target": "bridge-6fde553d",
    "detection": "passed",
    "resolved_ms": 102
  }
]}
```

The Timeline tab in `topology-ui` polls this endpoint every 5s. Each row renders:

- **time-ago** (left, gray)
- **status symbol** (green ✓ resolved / amber … pending / red ✗ failed)
- **primitive name** (blue)
- **target node_name** (white)
- **resolution** ("resolved in Xms" green / "pending detection" amber / failure reason red)
- **description** (muted gray below the row)

## Locked spans

- `rafka.chaos.primitive.executed{name, target, otel.kind="internal"}` — at primitive `execute()` entry.
- `rafka.chaos.primitive.detected{name, result, waited_ms, otel.kind="internal"}` — at primitive `detect()` exit. `result ∈ {passed, failed_timeout, failed_assertion}`.

Both share trace_id by virtue of being awaited within a single span tree inside `cmd_chaos_primitive` / `run_soak`.

## Cross-references

* Spec: [`prd.md`](prd.md)
* How to use: [`how-to.md`](how-to.md)
* Runbook: [`runbook.md`](runbook.md)
* Related: [`chaos-harness/overview.md`](../chaos-harness/overview.md) (the primitives themselves)
