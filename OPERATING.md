# Operating rafkav2

Single-page operator reference: env vars, ports, common commands, troubleshooting.

## Ports

| Port | Service |
|---|---|
| 19090 | topology-ui REST + HTML |
| 16686 | Jaeger UI (`http://localhost:16686`) |
| 4316  | OTLP/gRPC ingest |
| 4317  | OTLP/HTTP ingest |
| 0     | rafka-* iroh endpoint (random ephemeral; override with `RDM_NODE_BIND_ADDR`) |

## Environment variables

### Every rafka-{gateway,broker,compute,registry,bridge} binary reads:

| Env var | Default | Effect |
|---|---|---|
| `RDM_DATA_DIR` | `./data/node-<rand>` | Where node-identity.json lives + chaos disk_full fills |
| `RDM_NODE_BIND_ADDR` | `0.0.0.0:0` | iroh endpoint bind. Random ephemeral = `0`. `nat_shift` chaos sets a fresh port |
| `RDM_MESH_ID` | `default` | Logical mesh tag on node.ready + heartbeat spans |
| `RDM_SEED_NODES` | `` | Comma-list of `<node_id>@<addr>` for explicit dial (mdns is the default discovery) |
| `RDM_GOSSIP_INTERVAL_MS` | `500` | (Reserved — gossip plane not yet implemented; ms placeholder for now) |
| `RDM_CLOCK_SKEW_MS` | `0` | Adds offset to `wall_time_ms` on every heartbeat span. Chaos `clock_skew` sets this at respawn |
| `RDM_LINK_SLOW_MS` | `0` | Sleep that many ms before each outbound ping `open_uni`. Chaos `slow_link` |
| `RDM_LINK_LOSS_PCT` | `0` | Per outbound ping, roll u8%100; if `<` this, emit drop span + skip write. Chaos `lossy_link` |
| `RDM_AUTO_SHUTDOWN_SECS` | unset (= wait for SIGINT) | Auto-exit after N seconds; used by e2e tests |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4316` | Where node-base + topology-ui ship spans |
| `OTEL_SERVICE_NAME` | (set per binary) | Jaeger service tag |
| `RUST_LOG` | `info` | tracing filter (e.g. `rafka_node_base=debug,info`) |

### Bridge (`rafka-bridge`) additionally reads:

| Env var | Default | Effect |
|---|---|---|
| `RDM_BRIDGE_TARGET_MESHES` | `` | Comma-list of mesh IDs this bridge announces it spans; surfaced on `rdm.mesh.bridge.boot_announced` |

### topology-ui reads:

| Env var | Default | Effect |
|---|---|---|
| `RDM_ADMIN_UI_BIND_ADDR` | `127.0.0.1:19090` | HTTP listen addr |
| `JAEGER_QUERY_URL` | `http://localhost:16686` | Where to ask "what's in the traces" |
| `CARGO_TARGET_DIR` | derived from own exe path | Where spawned `rafka-*.exe` binaries live |

## Demo admin UI tabs (`demo/admin-ui`)

| Tab | What it shows | Source |
|---|---|---|
| Topology | Nodes by mesh with their seat; connection edges: Direct solid, Proxy dashed with its carrier, failed or disconnected red | node-admin `GET /api/nodes`, `GET /api/connections` |
| Nodes | Per-node state; restart and remove (each a Build) | node-admin |
| Builds | The accepted Build and the Builds the UI submitted, with attempts and steps | node-admin `GET /api/builds` |
| Boot Waterfall | A node's boot trace | Jaeger |
| Chaos | Per-node stop, continue and kill of its exact runtime; network cut of a node or a peer mesh; each fault's typed outcome. The fabric-primary is never a target. | `rafka-chaos` |
| Timeline | The running log, newest first, from the nodes' own span records in `RDM_EVIDENCE_DIR`; high-volume events behind a toggle | evidence folder |

## Common operations

### Spawn a multi-mesh test cluster

```bash
# Default-mesh nodes (4 of them, full mesh peering via mdns)
for t in gateway broker compute registry; do
  curl -s -X POST http://localhost:19090/api/nodes/spawn \
    -H 'Content-Type: application/json' -d "{\"node_type\":\"$t\"}"
done

# A bridge that announces it spans mesh-A + mesh-B
curl -X POST http://localhost:19090/api/nodes/spawn \
  -H 'Content-Type: application/json' \
  -d '{"node_type":"bridge","extra_env":{"RDM_BRIDGE_TARGET_MESHES":"mesh-A,mesh-B"}}'

# A node deliberately in a different mesh — should show as cross-mesh edge in topology
curl -X POST http://localhost:19090/api/nodes/spawn \
  -H 'Content-Type: application/json' \
  -d '{"node_type":"compute","extra_env":{"RDM_MESH_ID":"mesh-A"}}'
```

### Drain the spawned pool

```bash
for n in $(curl -s http://localhost:19090/api/nodes/spawned | jq -r '.spawned[]'); do
  curl -s -X DELETE "http://localhost:19090/api/nodes/$n" > /dev/null
done
```

## Troubleshooting

### "Access is denied (os error 5)" during `cargo build`
One or more `rafka-*.exe` binaries are still running and holding the file lock.
```powershell
Get-Process rafka-* -ErrorAction SilentlyContinue | Stop-Process -Force
```
Then re-run the build.

### topology-ui spawns nodes from the wrong build
topology-ui derives the binary search root from its own exe path's grandparent
(typically `<repo>/target/debug` or `E:/cargo-target-v2/debug`). If you've moved
the topology-ui binary, set `CARGO_TARGET_DIR` env to match where the node
binaries live.

### Heartbeat tab shows "NaNs ago"
You're running an old topology-ui build. Rebuild — `/api/heartbeat` now returns
`age_ms` directly.

### Soak appears hung in its log file
Stdout is block-buffered when redirected to a file. The soak emits "soak progress:"
every 10 events with explicit flush, plus "soak end:" at completion. Either look
in the file for those lines, or query Jaeger for `rafka.chaos.primitive.detected`
spans to see live activity.

### orphan rafka-* processes accumulating
The topology-ui reaper polls each entry's `Child::try_wait()` every 5s and
removes exited ones. Spawned subprocesses are also killed when topology-ui
itself exits. If you have leftover orphans from a topology-ui crash:
```powershell
Get-Process rafka-broker,rafka-gateway,rafka-compute,rafka-registry,rafka-bridge `
  -ErrorAction SilentlyContinue | Stop-Process -Force
```

## Integration tests: one executable per crate

Every crate with a `tests/` directory declares one test target, `main` (`tests/main.rs`, `autotests = false`), and each file in `tests/` is a `mod` of it, so a crate links once. A file (a stem) runs alone as `cargo test -p <crate> --test main -- <stem>::` or as `<exe> <stem>::`; a single cell as `<exe> <stem>::<test> --exact`. A new test file is added to `tests/main.rs` as `mod <stem>;`.

A cell that changes process-wide state (the global tracing subscriber, evidence telemetry, an environment variable) calls `own_process::delegated(module_path!(), "<fn name>")` first (`tools/test-support/own_process.rs`, included from the crate's `tests/main.rs`): it re-runs that one cell in a fresh process of the same executable and returns. A cell that captures spans with a dispatcher on its own thread calls `enable_callsites()` first where the crate defines it. A cell that re-runs its own executable names itself with its module path (`module_path!()`).

## Where to find things

| Thing | Where |
|---|---|
| Feature specs | `docs/features/<slug>/{overview,how-to,runbook}.md` |
| PRDs | `docs/plans/mesh-v1/0*.md` |
| Architecture decisions | `docs/plans/mesh-v1/06-decisions.md` |
| Soak evidence reports | `docs/evidence/*.json` |
| Locked span vocabulary | `CLAUDE.md` (Principle #10) + per-feature `overview.md` |
