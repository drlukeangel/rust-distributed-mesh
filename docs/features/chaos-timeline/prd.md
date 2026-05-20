# PRD — Chaos Timeline

**Status:** SHIPPED (commits 1f0bdbf, 151fadc).
**Surface:** `topology-ui` Timeline tab + `/api/chaos/timeline` endpoint.

---

## Problem

Operators running the rafka substrate under chaos need to answer three questions, live, while the chaos is happening:

1. **What disturbance is the substrate enduring right now?** (which primitive, against which node)
2. **Is the substrate actually resolving it?** (or just absorbing failures silently?)
3. **How long did each resolution take?** (regression signal — a kill that used to detect in 200ms now takes 5s = real degradation)

The earlier Alerts tab only fired on `result != passed`, so a healthy soak showed an empty Alerts panel — visually indistinguishable from a soak that wasn't running at all. The Chaos tab showed lifetime counts per primitive but no time-axis or detection-pairing. Neither answered #2 or #3.

## Goals

- **G1.** Show every `rafka.chaos.primitive.executed` event in reverse-chronological order with its paired `rafka.chaos.primitive.detected` resolution (matched by `trace_id`).
- **G2.** Surface the detection outcome inline: green ✓ "resolved in Xms" / amber … "pending" / red ✗ "failed: <reason>".
- **G3.** Self-explaining: each row shows what the primitive does (no PRD cross-reference needed to understand the timeline).
- **G4.** Auto-refresh every 5s without operator action.
- **G5.** Real data only — must query Jaeger directly, no fixtures or stubbed events.

## Non-goals

- **NG1.** Long-term storage. Lookback window is 10m (operator dashboard, not audit log). For permanent records use the `docs/evidence/*.json` soak reports.
- **NG2.** Visual graph of cascading effects (e.g., "kill A → B's peer_count dropped"). That's a future Insights tab.
- **NG3.** Filtering / search. v1 is a flat list; if event volume exceeds usability, add filter controls later.

## Acceptance criteria

1. With no chaos running, Timeline tab shows "no chaos events in lookback window — soak idle?" — not an empty panel.
2. With chaos running, every `executed` span pairs to its matching `detected` span by `trace_id` within ≤5s of the detection completing.
3. The "resolved in Xms" value matches the `waited_ms` tag on the detected span.
4. Description shown matches `primitive_description(name)` (single source of truth in `topology-ui/src/main.rs`).
5. Tab auto-refreshes every 5s with no visible flicker.
6. Endpoint returns ≤200 traces per query (bounded cost on Jaeger side).
7. Empty response on jaeger-unreachable — never 500.

## Architecture

```
rfa mesh chaos {kill,restart,...}
        │ emits via OTLP
        ▼
  rafka.chaos.primitive.executed{name, target, …}    ┐
  rafka.chaos.primitive.detected{name, result, waited_ms}  ┴── same trace_id
        ▼
   Jaeger storage (10m retention for active session)
        ▲
        │ GET /api/traces?service=rfa&operation=...&lookback=10m
        │
  topology-ui handle_chaos_timeline()
        │ matches executed↔detected by trace_id
        │ adds primitive_description(name)
        │ returns sorted-newest-first
        ▼
   GET /api/chaos/timeline → { events: [{when, primitive, description, target, detection, resolved_ms}] }
        ▲
        │ fetch every 5s when Timeline tab active
        │
   topology-ui HTML/JS renders rows with color-coded status symbols
```

## Span contract (locked)

- **`rafka.chaos.primitive.executed{name, target, otel.kind="internal"}`** — emitted at the START of every chaos primitive invocation.
- **`rafka.chaos.primitive.detected{name, result, waited_ms, otel.kind="internal"}`** — emitted at the END of every primitive's `detect()` method. `result ∈ {"passed", "failed_timeout", "failed_assertion"}`. `waited_ms` is the elapsed time between execute start and detection success/fail.

Both spans share the same `trace_id` because both `execute()` and `detect()` are awaited within a single span tree inside `cmd_chaos_primitive` (one-shot) or `run_soak` (loop).

## Open follow-ups

- **F1.** Background-job mode for Timeline so a long-tail soak summary doesn't truncate to the 10m lookback. Likely: an SQLite persistence layer next to topology-ui.
- **F2.** "Cluster impact" column showing per-event delta in `mean_peer_count` from `/api/cluster/summary`. Requires snapshotting cluster state at every chaos event.
- **F3.** Filter by primitive / target / result. Only worth building once event volume routinely exceeds ~50 visible rows.

## Verification

- 4× one-shot primitives fired against a 7-node pool, all detected, all rendered with descriptions:
  ```
  [3s ago] nat_shift    → resolved in 102ms
    Restart target with new random RAFKA_NODE_BIND_ADDR. iroh must re-discover the NodeId at the new ephemeral port.
  [4s ago] clock_skew   → resolved in 73ms
    Restart target with RAFKA_CLOCK_SKEW_MS env. node-base adds that offset to wall_time_ms on every heartbeat span.
  [4s ago] restart_node → resolved in 51ms
  [4s ago] kill_node    → resolved in 26ms
  ```
- 46 events in lookback window after running a chain of one-shots — none dropped, all paired.
