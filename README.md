# rafkav2

A telemetry-first mesh substrate built on iroh (QUIC + NodeId crypto + mdns).
No JVM, no ZooKeeper. Every action is observable in Jaeger; every code path
emits a span. Chaos-tested under a 10-primitive catalog at multi-hour scale.

## Quick start

```bash
# 1. Bring up Jaeger + OTLP collector (podman or docker)
podman compose -f E:/dev/rafka/deployment/dev/compose.test-otlp.yml up -d

# 2. Build the binaries
cargo build -p rafka-gateway -p rafka-broker -p rafka-compute -p rafka-registry \
            -p rafka-bridge -p rafka-topology-ui -p rfa

# 3. Launch the topology UI (also spawns subprocess registry)
cargo run -p rafka-topology-ui
# → http://localhost:19090

# 4. Open Jaeger
# → http://localhost:16686
```

The topology UI has four tabs:
- **Boot Waterfall** — last boot trace per service
- **Topology** — live SVG mesh graph grouped by mesh_id (cross-mesh edges dashed)
- **Alerts** — chaos events with non-Passed result (auto-polls every 10s)
- **Heartbeat** — per-service peer count + staleness color

Spawn buttons inline (gateway / broker / compute / registry / bridge).

## CLI: `rfa`

```bash
# Spawn / kill / list nodes via topology-ui REST
rfa mesh node add --type broker
rfa mesh node list
rfa mesh node remove broker-XYZ

# Chaos primitives
rfa mesh chaos kill [--target NAME]
rfa mesh chaos restart [--target NAME]
rfa mesh chaos soak --duration 1h --interval 20s --seed 42
```

## Chaos catalog (10 of 13 shipped)

| Primitive | What it does |
|---|---|
| kill_node | terminate a node abruptly |
| restart_node | kill + immediate re-spawn |
| burst_kill | N back-to-back kills |
| disk_full | fill spawn data dir until writes fail |
| wedge_node | Windows NtSuspendProcess + revert |
| partition_pair | New-NetFirewallRule blocking outbound UDP (needs admin) |
| clock_skew | restart with RAFKA_CLOCK_SKEW_MS env injected |
| slow_link | restart with RAFKA_LINK_SLOW_MS env (per-frame sleep) |
| lossy_link | restart with RAFKA_LINK_LOSS_PCT env (per-frame drop dice) |
| nat_shift | restart with new RAFKA_NODE_BIND_ADDR (ephemeral port) |

Queued: partition_subset, flap_link, firewall_inbound (all admin-required).

## Robustness evidence

Reports in `docs/evidence/`:

| Run | Pool | Duration | Result |
|---|---|---|---|
| 30min-soak-seed-900 | 4 prim | 30m | 117/117 ✓ |
| 1h-soak-seed-800 | 4 prim | 1h | 177/177 ✓ |
| 1h-soak-seed-1400 | 4 prim | 1h | 178/178 ✓ |
| 1h-soak-seed-2100-full-pool | 8 prim | 1h | 175/175 ✓ |
| 2h-soak-seed-2200-full-pool | 8 prim | 2h | 349/349 ✓ |
| 1h-soak-seed-2400-9-prim | **9 prim** | 1h | **174/174 ✓** |

Long-soak cumulative (≥1h): **1053 chaos events, zero failures across 5 runs**.

## Features

Each user-visible feature has an `overview.md` + `how-to.md` + `runbook.md`
under `docs/features/<slug>/`. 12 features documented:

`boot-chain`, `peer-discovery`, `frame-exchange`, `heartbeat`, `node-base`,
`telemetry-substrate`, `topology-ui-waterfall`, `subprocess-control`,
`rfa-cli`, `cross-service-tracing`, `chaos-harness`, `mesh-to-mesh`,
`spawned-list`.

## Architecture decisions

`docs/plans/mesh-v1/06-decisions.md` locks 30+ decisions (D-001 ... D-030)
covering topology, naming, control vs data plane, observability contracts,
testing strategy, repo layout. PRDs in `docs/plans/mesh-v1/` cover the
substrate, topology-ui, CLI, and chaos harness.

## Telemetry ports

This repo runs against the same Jaeger/OTLP stack as v1:
- OTLP/gRPC: `4316`
- OTLP/HTTP: `4317`
- Jaeger UI: `16686`
- topology-ui: `19090`
