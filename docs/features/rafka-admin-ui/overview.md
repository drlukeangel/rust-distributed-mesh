# rafka-v2-mesh-ui — Overview

A live operator console for the rafka v2 mesh: spin up a two-mesh demo with
one click, watch nodes spawn/die in real time, drive chaos primitives, run
the test suite, and read the boot trace of any node — all in one page.

## What it is

- **Front end:** single-page React app (Vite + react-flow). Built artifact at
  `topology-ui/web/dist/`, served by the Rust binary under `/`.
- **Back end:** axum 0.8 HTTP server (`rafka-topology-ui` crate). Owns the
  spawned subprocess pool, the chaos auto-loop, and the local event ring.
  Defers to Jaeger for trace history but never blocks on it.
- **Test runner:** the existing `rfa.exe` CLI is invoked via
  `POST /api/tests/run` so the Tests tab can drive the same test runs an
  operator would do from a shell.

## Topology

```
┌──────────────────────────────────────────────────────────────────┐
│ Browser  ←  HTTP/2  →  Rust topology-ui  (port 19105)            │
│                          │                                       │
│                          ├── tokio subprocess pool (18+ children)│
│                          │     └── rafka-{gateway,broker,...}    │
│                          │                                       │
│                          ├── chaos auto-loop                     │
│                          ├── local event ring (cap 500)          │
│                          └── reqwest → Jaeger (port 16686)       │
│                                                                  │
│ Subprocesses → OTLP → otel-collector → Jaeger (separate process) │
└──────────────────────────────────────────────────────────────────┘
```

## Tabs

| tab | source | refresh |
|-----|--------|---------|
| Topology | `/api/topology` (local) | 2 s |
| Heartbeat | `/api/heartbeats` (Jaeger fan-out) | 2 s |
| Boot Waterfall | `/api/topology` for list, `/api/boot-trace` for spans | manual |
| Chaos | `/api/chaos/state` (local) | 2 s |
| Timeline | `/api/timeline` (local + Jaeger) | 2 s |
| Alerts | `/api/alerts` (Jaeger) | 3 s |
| Tests | `/api/tests` (filesystem) + `/api/tests/run` (subprocess) | 3 s |

## Persistent top bar

Always visible above the tabs:
- **Cluster summary line** — `N spawned · meshes: … · chaos: N/min · mean peers: F`
- **Mesh dropdown** — mesh-a (primary) / mesh-b (secondary) / + new mesh… —
  controls which mesh subsequent spawns join via `RAFKA_MESH_ID` env.
- **+ {type}** buttons (gateway, broker, compute, registry, bridge) — manual spawn
- **bootstrap 2-mesh** — POST /api/bootstrap, 18 children
- **start chaos / stop chaos** — toggle auto-loop

## Data flow for chaos auto-loop

```
chaos.start
  │
  └─→ tokio task (cadence 30 s)
        │
        ├── snapshot spawned_meta where node_type != "bridge"
        ├── pick random victim
        ├── kill_one(victim)
        │     ├── start_kill (SIGTERM)
        │     ├── 5 s timeout → escalate to KILL
        │     ├── remove from processes + spawned_meta
        │     └── push event "chaos.kill"
        │
        └── spawn_one(victim.type, RAFKA_MESH_ID=victim.mesh_id)
              ├── new random suffix → new node_name
              ├── tokio::process::Command::spawn
              ├── insert into processes + spawned_meta
              └── push event "chaos.respawn"
```

## Architectural truths the UI surfaces

- **Edges are real**, derived from `rafka.mesh.peer.connected` +
  `rafka.mesh.frame.sent` spans. No synthesis from labels. Pairs that
  haven't connected yet have no edge.
- **Frame counts are real**, accumulated from `rafka.mesh.frame.sent` spans
  in the last 60 s. Pairs with non-zero counts render thicker.
- **Every role pings every 10 s.** Previously only gateways pinged; brokers
  / compute / registry / bridges were silent on the data plane. Now all of
  them emit pings → all of them produce frame.sent spans → all of them
  appear with real edges + counts.
- **No broker-mediated routing.** v2 has no central broker. Control plane
  is iroh-gossip (Plumtree), data plane is bidirectional QUIC bi-streams
  multiplexed on the same connection. Bridges today are regular nodes
  tagged "bridge" — they participate in normal peer discovery, no special
  cross-mesh role.
- **Postcard wire format.** All frames use `tag(u8) + varint(length) +
  postcard(payload)` per the locked architecture spec in `docs/architecture/mesh.md`.

## What it does NOT show (yet)

- Per-stream MTU or QoS metrics. Out of scope this rev.
- Active alerts beyond chaos primitive detection failures.
- Predictive metrics (will-this-node-fail-soon style).
