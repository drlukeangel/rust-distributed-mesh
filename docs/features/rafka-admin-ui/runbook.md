# rafka-v2-mesh-ui — Runbook

## Quick diagnostics

| Symptom | First check |
|--------|-------------|
| Page won't load | `curl http://localhost:19106/api/health` |
| Page loads, all tabs blank | Check log `E:\tmp\topo-react-err.log` |
| Heartbeat / Alerts / Timeline hang | Jaeger down? `curl http://localhost:16686/api/services` |
| Boot Waterfall says "no nodes" | Topology has 0 nodes — bootstrap first |
| Topology empty after bootstrap | `curl http://localhost:19106/api/cluster/summary` — should show spawned=18 |
| Tests tab says "never run" for all | `ls E:\tmp\rafka-tests\` empty — first runs haven't completed |
| /api/tests/run returns 405 | Server binary is stale — rebuild + relaunch |

## Port 19105 already in use

Windows zombie socket. Kill the holder:
```powershell
Get-NetTCPConnection -LocalPort 19105 | Select-Object OwningProcess
Stop-Process -Id <pid> -Force
```
If no process matches the PID (true zombie), pick a different port via
`$env:RAFKA_TOPOLOGY_UI_BIND_ADDR = "127.0.0.1:19106"` and relaunch.

## Subprocess won't die after kill

`handle_kill` escalates from start_kill → 5 s wait → kill → wait. If a child
ignores even SIGKILL/TerminateProcess (rare on Windows for hung iroh):
```powershell
Get-Process -Name rafka-* | Stop-Process -Force
```
Then trigger reaper manually by hitting any endpoint that touches
`spawned_meta`.

## Rebuild + relaunch

```powershell
Get-Process rafka-topology-ui,rafka-gateway,rafka-broker,rafka-compute,rafka-registry,rafka-bridge,rfa `
  -ErrorAction SilentlyContinue | Stop-Process -Force

cd E:\dev\rafka-V2-new-mesh
$env:CARGO_TARGET_DIR = "E:\cargo-target-v2"
cargo build -p rafka-topology-ui -p rfa

cd topology-ui\web
npm run build
```

Then re-run the launcher block from `how-to.md` → "Start the UI".

## Jaeger restart loses heartbeats

Heartbeats use 2-minute lookback. After restart the cards will show
`age: -1s` briefly while spans re-flush. Spawned_meta still knows the nodes,
so Topology + Heartbeat node cards still render — only the `peer_count`
column will read 0 until the first heartbeat span returns.

## Chaos loop won't stop

`POST /api/chaos/stop` sets the running flag false; the loop checks it after
each sleep. Worst case wait = current cadence_ms. If you need a hard stop:
```powershell
Get-Process rafka-topology-ui | Stop-Process -Force
```
This also abandons live subprocesses — reaper picks them up on next launch.

## Bootstrap fails mid-way

`/api/bootstrap` returns `{spawned: [...], errors: [...]}`. If `errors` is
non-empty:
- Check disk: `Get-PSDrive E` (FS spawn dirs)
- Check binary exists: `dir E:\cargo-target-v2\debug\rafka-*.exe`
- Check CARGO_TARGET_DIR env (server side, not your shell)

Whatever spawned successfully stays alive. Either kill them all and retry,
or just hit bootstrap again — duplicates are fine, each child has a unique
random suffix.

## Test report missing after `run`

`/api/tests/run` waits for `rfa.exe` to exit (up to 10 min) then reads
`E:/tmp/rafka-tests/<name>-<seed>.json`. If the report is missing:
- Look at the returned `detail` field — contains last 400 chars of stdout +
  stderr from rfa
- Confirm `rfa.exe` exists at `${CARGO_TARGET_DIR}/debug/rfa.exe`
- Check the test name is in the registry (`rfa mesh test list`)

## Add a new test

1. If functional: add a `#[tokio::test]` in the owning crate.
2. Add entry to `TEST_REGISTRY` in `cli/rfa/src/main.rs`.
3. Add a match arm in `cmd_test_run` mapping the registry name to a runner
   (either `run_cargo_test_for` or a custom async fn).
4. Add the registry entry to `REGISTRY` in
   `topology-ui/web/src/tabs/Tests.tsx`.
5. `cargo build -p rfa` + `npm run build`.
