# rfa-cli — runbook

## Failure modes

### Mode 1 — `rfa: connection refused`

**Cause:** topology-ui not running on the assumed `--api-url` (default `http://localhost:19090`).

**Recovery:**
```bash
CARGO_TARGET_DIR=E:/cargo-target-v2 cargo run -p rafka-topology-ui &
# wait 3s, retry rfa
```

### Mode 2 — Commands work but no rfa spans in Jaeger

**Cause:** OTel export drop on short-lived rfa exit.

**Detection:** After running an rfa command, query:
```bash
curl -s "http://localhost:16686/api/traces?service=rfa&operation=rafka.cli.command&limit=5&lookback=2m" | python -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))"
```
Should be ≥ number of invocations.

**Recovery:** Confirm `init_telemetry_for_cli` is used (NOT `init_telemetry`). The SimpleSpanProcessor variant synchronously exports.

### Mode 3 — Cross-service trace_id doesn't link rfa → topology-ui

**Cause:** Either rfa isn't injecting `traceparent` (check `current_traceparent_headers()` is called in http_post/get/delete) OR topology-ui isn't extracting (check `trace_middleware` calls `propagator.extract` + `span.set_parent`).

**Recovery:** See [`cross-service-tracing runbook`](../cross-service-tracing/runbook.md).

### Mode 4 — chaos kill targets a node that's not UI-spawned → can't kill

**Cause:** rfa's kill targets the topology-ui `DELETE /api/nodes/{name}` endpoint, which only knows about UI-spawned subprocesses. Baseline nodes started directly (without `rfa mesh node add`) aren't in the DashMap.

**Recovery:** Either (a) only use `rfa mesh node add` to spawn nodes so they're killable via CLI, or (b) for OS-level kill of arbitrary processes: `Get-Process rafka-* | Stop-Process -Force`.

### Mode 5 — chaos soak reports `events=0`

**Cause:** No UI-spawned subprocesses available as random targets — the primitives' `pick_random_spawned()` raises `InvalidTarget`.

**Recovery:** Pre-populate before soak:
```bash
for t in broker compute gateway registry; do rfa mesh node add --type $t; done
sleep 5
rfa mesh chaos soak --duration 1m --interval 10s --seed 1
```

## Cross-references

* Parent: operator CLI.
* Sibling: [`subprocess-control runbook`](../subprocess-control/runbook.md), [`chaos-harness runbook`](../chaos-harness/runbook.md).
