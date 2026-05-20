# Operating rafkav2

Single-page operator reference: env vars, ports, common commands, troubleshooting.

## Ports

| Port | Service |
|---|---|
| 19090 | topology-ui REST + HTML |
| 16686 | Jaeger UI (`http://localhost:16686`) |
| 4316  | OTLP/gRPC ingest |
| 4317  | OTLP/HTTP ingest |
| 0     | rafka-* iroh endpoint (random ephemeral; override with `RAFKA_NODE_BIND_ADDR`) |

## Environment variables

### Every rafka-{gateway,broker,compute,registry,bridge} binary reads:

| Env var | Default | Effect |
|---|---|---|
| `RAFKA_DATA_DIR` | `./data/node-<rand>` | Where node-identity.json lives + chaos disk_full fills |
| `RAFKA_NODE_BIND_ADDR` | `0.0.0.0:0` | iroh endpoint bind. Random ephemeral = `0`. `nat_shift` chaos sets a fresh port |
| `RAFKA_MESH_ID` | `default` | Logical mesh tag on node.ready + heartbeat spans |
| `RAFKA_SEED_NODES` | `` | Comma-list of `<node_id>@<addr>` for explicit dial (mdns is the default discovery) |
| `RAFKA_GOSSIP_INTERVAL_MS` | `500` | (Reserved — gossip plane not yet implemented; ms placeholder for now) |
| `RAFKA_CLOCK_SKEW_MS` | `0` | Adds offset to `wall_time_ms` on every heartbeat span. Chaos `clock_skew` sets this at respawn |
| `RAFKA_LINK_SLOW_MS` | `0` | Sleep that many ms before each outbound ping `open_uni`. Chaos `slow_link` |
| `RAFKA_LINK_LOSS_PCT` | `0` | Per outbound ping, roll u8%100; if `<` this, emit drop span + skip write. Chaos `lossy_link` |
| `RAFKA_AUTO_SHUTDOWN_SECS` | unset (= wait for SIGINT) | Auto-exit after N seconds; used by e2e tests |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4316` | Where node-base + topology-ui ship spans |
| `OTEL_SERVICE_NAME` | (set per binary) | Jaeger service tag |
| `RUST_LOG` | `info` | tracing filter (e.g. `rafka_node_base=debug,info`) |

### Bridge (`rafka-bridge`) additionally reads:

| Env var | Default | Effect |
|---|---|---|
| `RAFKA_BRIDGE_TARGET_MESHES` | `` | Comma-list of mesh IDs this bridge announces it spans; surfaced on `rafka.mesh.bridge.boot_announced` |

### topology-ui reads:

| Env var | Default | Effect |
|---|---|---|
| `RAFKA_TOPOLOGY_UI_BIND_ADDR` | `127.0.0.1:19090` | HTTP listen addr |
| `JAEGER_QUERY_URL` | `http://localhost:16686` | Where to ask "what's in the traces" |
| `CARGO_TARGET_DIR` | derived from own exe path | Where spawned `rafka-*.exe` binaries live |

## CLI: `rfa`

```bash
# Node management
rfa mesh node list                          # services known to Jaeger
rfa mesh node add --type broker             # spawn via topology-ui
rfa mesh node remove broker-XYZ             # DELETE via topology-ui
rfa mesh node wait-converged --target 4 --timeout 30s

# Chaos one-shots (every shipped primitive)
rfa mesh chaos kill          [--target T --deadline-ms M]
rfa mesh chaos restart       [--target T --deadline-ms M]
rfa mesh chaos burst-kill    [--count N --deadline-ms M]
rfa mesh chaos disk-full     [--target T --max-mb N --deadline-ms M]
rfa mesh chaos wedge         [--target-type X --duration-ms M]
rfa mesh chaos clock-skew    [--target T --skew-ms N --deadline-ms M]
rfa mesh chaos slow-link     [--target T --latency-ms N --deadline-ms M]
rfa mesh chaos lossy-link    [--target T --loss-pct N --deadline-ms M]
rfa mesh chaos nat-shift     [--target T --deadline-ms M]
rfa mesh chaos partition-pair --a NAME --b NAME [--duration-ms M]    # NEEDS ADMIN

# Soak runner (the long-running smoke test)
rfa mesh chaos soak --duration 1h --interval 20s --seed 42
# Writes report to E:/tmp/rafka-chaos-soak-<seed>.json
# Exit code 0 only on 100% pass
```

### Important: launching long-running soaks in the background

The Claude Code Bash tool kills `&` children when its subshell exits.
Use PowerShell `Start-Process` for true detach, or host the soak inside
a `Monitor` task:

```powershell
$proc = Start-Process -FilePath "E:\cargo-target-v2\debug\rfa.exe" `
  -ArgumentList "mesh","chaos","soak","--duration","1h","--interval","20s","--seed","42" `
  -RedirectStandardOutput "E:\tmp\soak.out" `
  -RedirectStandardError "E:\tmp\soak.err" `
  -PassThru -NoNewWindow
"soak pid: $($proc.Id)"
```

## Topology UI tabs

| Tab | What it shows | Auto-poll |
|---|---|---|
| Boot Waterfall | Last `rafka.mesh.node.ready` trace per service | manual |
| Topology | SVG mesh graph; nodes grouped by mesh_id; cross-mesh edges dashed gold; per-node `N fr/m` activity badge | 5s |
| Alerts | Chaos events with non-Passed result (last 10m) | 10s |
| Heartbeat | Per-service peer_count + age_ms with color-coded staleness | 5s |
| Chaos | Per-primitive count + 20 most recent .executed events (last 10m) | 10s |

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
  -d '{"node_type":"bridge","extra_env":{"RAFKA_BRIDGE_TARGET_MESHES":"mesh-A,mesh-B"}}'

# A node deliberately in a different mesh — should show as cross-mesh edge in topology
curl -X POST http://localhost:19090/api/nodes/spawn \
  -H 'Content-Type: application/json' \
  -d '{"node_type":"compute","extra_env":{"RAFKA_MESH_ID":"mesh-A"}}'
```

### Drain the spawned pool

```bash
for n in $(curl -s http://localhost:19090/api/nodes/spawned | jq -r '.spawned[]'); do
  curl -s -X DELETE "http://localhost:19090/api/nodes/$n" > /dev/null
done
```

### Verify a chaos primitive ran end-to-end via Jaeger

```bash
curl -s "http://localhost:16686/api/traces?service=rfa&operation=rafka.chaos.primitive.detected&limit=10&lookback=5m" \
  | jq '.data[].spans[].tags[] | select(.key=="name" or .key=="result")'
```

## Troubleshooting

### "Access is denied (os error 5)" during `cargo build`
One or more `rafka-*.exe` binaries are still running and holding the file lock.
```powershell
Get-Process rafka-*,rfa -ErrorAction SilentlyContinue | Stop-Process -Force
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

## Where to find things

| Thing | Where |
|---|---|
| Feature specs | `docs/features/<slug>/{overview,how-to,runbook}.md` |
| PRDs | `docs/plans/mesh-v1/0*.md` |
| Architecture decisions | `docs/plans/mesh-v1/06-decisions.md` |
| Soak evidence reports | `docs/evidence/*.json` |
| Locked span vocabulary | `CLAUDE.md` (Principle #10) + per-feature `overview.md` |
