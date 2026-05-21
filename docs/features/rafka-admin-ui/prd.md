# rafka-v2-mesh-ui — Product Requirements

## Problem

Operators need to observe and control a live rafka v2 mesh — adding/removing
nodes, watching chaos in real time, replaying boot waterfalls, running tests,
and reading alerts — without dropping into Jaeger UI, raw curl, or SSH. The
previous prototype was vanilla JS in a Rust raw-string template; it couldn't be
laid out properly and broke whenever a Jaeger query stalled.

## Goals

1. **One URL, one click to a working two-mesh demo.** A fresh launch → click
   `bootstrap 2-mesh` → eighteen nodes spin up across mesh-a, mesh-b, and a
   bridge pool, no manual spawn needed.
2. **Add and remove nodes at any time** via per-node kill button, per-type
   spawn buttons, and a mesh dropdown that pins each spawn to a specific mesh.
3. **See all nodes' status in real time** in Heartbeat + Topology tabs with
   ≤3 s refresh.
4. **Boot Waterfall** must show spans for ANY spawned node, not just the
   first-spawned of a given type.
5. **Topology** must group by mesh visually (separate circles per mesh) and
   draw the bridge spanning them.
6. **Alerts** must surface failed chaos primitives without hanging the UI.
7. **Chaos testing** must support both manual primitives and a continuous
   auto-chaos loop that kills + respawns one non-bridge node every 30 s.
8. **Timeline** must show spawn / kill / chaos / mesh events together,
   newest-first, even when Jaeger ingestion is lagging.
9. **Tests** must be runnable from the UI (per-test `run` button + `run all`)
   AND from the CLI (`rfa mesh test run/all`), with reports surfaced in the
   Tests tab.
10. **Backpressure** must be proven with a sustained-throughput test:
    32 concurrent bi-streams, 1 KiB × 10 s, 0 errors, ≥ 200 round-trips.

## Non-goals (this rev)

- Multi-user auth / RBAC. Single-operator local-only.
- Cross-mesh data-plane traffic beyond peer-to-peer iroh dialing. Bridges
  today have no special routing role — they're regular nodes tagged "bridge"
  that participate in normal peer discovery.

## What the topology view actually represents (post real-edge fix)

Edges in `/api/topology` are derived from Jaeger spans, NOT synthesized from
mesh_id labels:

1. `rafka.mesh.heartbeat` spans (every 5 s) provide a `node_id → node_name`
   resolver.
2. `rafka.mesh.peer.connected` spans (10-min lookback) carry `node_id` (self)
   + `peer_id` → resolved to (from_name, to_name); becomes an edge.
3. `rafka.mesh.frame.sent` spans (60-s lookback) provide the `frame_count`
   weighting per edge AND the `frames_per_min` per node.

If either endpoint of a pair hasn't emitted a heartbeat yet, the edge is
dropped (we don't fabricate names). All node types — gateway, broker,
compute, registry, bridge — emit pings every 10 s, so every connected pair
produces frame.sent spans in steady state.

Caveat: at startup, only nodes whose heartbeat AND ping cycles have already
landed in Jaeger appear with edges. First 10-15 s after bootstrap the view
is sparse by design.

## Stack

- Frontend: Vite + React 19 + react-flow 11 (`topology-ui/web/`)
- Backend: axum 0.8 + tower-http ServeDir + reqwest 0.13 + dashmap + tokio
- Telemetry: tracing + OpenTelemetry → OTLP → Jaeger (port 16686)
- Data plane: iroh 0.91 + iroh-gossip 0.91 + postcard 1 framer
- Control plane: same iroh QUIC connection, ALPN-multiplexed
  (`rafka-mesh-v1` + `/iroh-gossip/1`)
- CLI: `rfa.exe mesh test {list, run, all}` invokes cargo tests or chaos soak

## API surface

| method | path                    | source of truth | latency (warm / cold-loopback) |
|--------|-------------------------|-----------------|--------------------------------|
| GET    | /api/health             | local           | ~25 ms / ~300 ms cold |
| GET    | /api/cluster/summary    | local           | ~50 ms / ~800 ms cold |
| GET    | /api/topology           | local + Jaeger fan-out | up to 40 s (Jaeger-dependent) |
| GET    | /api/heartbeats         | Jaeger fan-out (parallel) | <6 s healthy / 4 s on miss |
| GET    | /api/boot-trace?service | Jaeger          | <2 s healthy / 4 s on miss |
| GET    | /api/timeline           | local + Jaeger merge | <6 s healthy / 4 s on miss |
| GET    | /api/alerts             | Jaeger          | <3 s healthy / 3 s on miss |
| GET    | /api/tests              | filesystem (E:/tmp/rafka-tests/) | <50 ms |
| GET    | /api/chaos/state        | local           | ~25 ms |
| POST   | /api/bootstrap          | local           | ~5 s |
| POST   | /api/nodes/spawn        | local           | <500 ms |
| DELETE | /api/nodes/{name}       | local           | <5 s |
| POST   | /api/chaos/start        | local           | ~50 ms |
| POST   | /api/chaos/stop         | local           | ~50 ms |
| POST   | /api/tests/run          | local subprocess | up to 600 s |

**SpawnRequest body**: `{ node_type: <known>, mesh_id?: <safe>, extra_env?: { ... } }`
where `mesh_id` matches `^[a-z0-9][a-z0-9-]{0,63}$` (slashes, spaces, unicode
rejected with 400). `mesh_id` and `extra_env.RAFKA_MESH_ID` both work;
`mesh_id` wins if both present.

Every Jaeger-backed endpoint MUST have a per-request reqwest timeout (4 s
default at the client, 3 s for `/api/alerts`) so a stalled Jaeger never
hangs the page. Local-only endpoints have a ~25 ms warm latency floor due
to tracing-middleware instrumentation; the previous PRD claim of <10 ms was
aspirational, not measured.

## Local state (never lost on Jaeger restart)

`AppState`:
- `processes: DashMap<node_name, Child>` — live subprocess handles
- `spawned_meta: DashMap<node_name, {node_type, mesh_id, pid}>` — ground
  truth for topology + summary; used in chaos loop for "pick a random
  non-bridge"
- `chaos: ChaosController` — running flag, cadence, total_events, last_ts
- `events: EventRing` (cap 500) — spawn, kill, chaos.kill, chaos.respawn,
  test.start, test.end. Surfaces in /api/timeline immediately.

## Bootstrap composition

`POST /api/bootstrap` spawns 18 children:
- 4 × {gateway, broker, compute, registry} in **mesh-a** (8 nodes)
- 4 × {gateway, broker, compute, registry} in **mesh-b** (8 nodes)
- 2 × bridge nodes (mesh_id = "bridge")

50 ms stagger between spawns to avoid Windows FS namespace collisions.

## Chaos auto-loop

`POST /api/chaos/start` arms a tokio task that, every `cadence_ms` (default
30 000):
1. Snapshot non-bridge entries in `spawned_meta`
2. Pick one at random
3. `kill_one(state, name)` — graceful SIGTERM, escalate to KILL after 5 s
4. `spawn_one(state, type, RAFKA_MESH_ID=same_mesh)` — same type, same mesh
5. Push `chaos.kill` + `chaos.respawn` events
6. Increment `total_events`, update `last_event_ts_us`

Bridges are protected; if a bridge dies (manual kill), the loop does NOT
respawn it. This keeps the mesh-spanning topology stable.

## Test registry (12 entries)

| name | kind | what it proves |
|------|------|----------------|
| framer-roundtrip | functional | tag+varint+postcard frame round-trips byte-for-byte |
| framer-truncation | functional | dropping last byte surfaces `FramerError::Truncated` |
| traced-frame-roundtrip | functional | TracedFrame preserves trace_id+span_id |
| unknown-tag-rejected | functional | non-0x10 tags do NOT deserialize as TracedFrame |
| bi-stream-echo | functional | 2 iroh endpoints exchange a tag=0x11 payload over QUIC bi-stream |
| backpressure-stream-flood | chaos | 32 concurrent bi-streams, 1 KiB × 10 s, 0 errors, ≥200 RTs |
| chaos-soak-9prim-1min | chaos | 1-min soak, 9-primitive pool, expects 100% pass |
| chaos-soak-9prim-5min | chaos | 5-min soak, balanced primitive distribution |
| mesh-five-types-present | chaos | spawn 5 types, all visible in topology + heartbeats fresh |
| remove-resilience | chaos | kill 3 of 6, survivors detect within 15 s |
| gossip-swarm-forms | chaos | `rafka.mesh.gossip.received` spans fire — Plumtree swarm formed |
| gossip-mesh-to-mesh | chaos | mesh-A and mesh-B gossip isolated; cross.peer_connected fires |

Every run writes `E:/tmp/rafka-tests/<name>-<seed>.json`. The Tests tab polls
the directory every 3 s.

## Non-functional

- **Time-to-first-screen** ≤ 1 s after the launcher prints `topology-ui
  listening`.
- **Bootstrap → all 18 nodes visible in Topology** ≤ 8 s.
- **Manual kill → row gone from Heartbeat** ≤ 5 s.
- **Chaos event → row appears in Timeline** ≤ 1 s (local ring).
- **No tab may hang the page** if Jaeger is down or slow — fail fast, show
  empty state with a hint.
