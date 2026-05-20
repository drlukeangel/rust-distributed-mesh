# chaos-timeline — runbook

## Failure modes

### Mode 1 — Tab shows "no chaos events in lookback window — soak idle?"

**Cause:** No `rafka.chaos.primitive.executed` spans in the last 10 minutes. Either no soak is running, or all chaos was emitted >10m ago.

**Recovery:** Kick a soak (`rfa mesh chaos soak --duration 5m --interval 10s --seed 1`) or fire a single primitive (`rfa mesh chaos kill`). Within ~6s the event should appear.

### Mode 2 — Events all show "pending" indefinitely

**Cause:** `rafka.chaos.primitive.detected` spans aren't being emitted. Most likely: an OLD `rfa` binary built before the per-primitive `.detected` span additions (commit 80ba962 et al.). Could also be Jaeger ingestion lag if traffic is heavy.

**Detection:** `curl -s "http://localhost:16686/api/traces?service=rfa&operation=rafka.chaos.primitive.detected&limit=1" | jq '.data | length'` — should return ≥1 if the substrate is emitting detection spans.

**Recovery:** Rebuild rfa: `cargo build -p rfa` and retry the chaos one-shot. If Jaeger is the bottleneck (>500 spans/min from a heavy soak), drop the soak `--interval` to slow the rate.

### Mode 3 — "fetch failed" status in the Timeline tab

**Cause:** `/api/chaos/timeline` returned non-2xx or non-JSON. Usually topology-ui crashed or Jaeger is down.

**Detection:** `curl -s http://localhost:19092/api/health` (topology-ui), `curl -s http://localhost:16686/` (Jaeger).

**Recovery:** Restart whichever component is dead. topology-ui:
```powershell
Start-Process -FilePath E:\cargo-target-v2\debug\rafka-topology-ui.exe `
  -RedirectStandardOutput E:\tmp\rafka-run\topo.log `
  -RedirectStandardError E:\tmp\rafka-run\topo.err `
  -PassThru -NoNewWindow
```
Jaeger: re-up the podman/docker compose stack at `E:/dev/rafka/deployment/dev/compose.test-otlp.yml`.

### Mode 4 — Events show but `resolved_ms` always 0

**Cause:** Trace pairing succeeded but `waited_ms` tag missing from the detected span. Indicates a custom primitive that didn't follow the locked span contract.

**Detection:** Pick one event's trace_id, inspect in Jaeger: the `rafka.chaos.primitive.detected` span should have a `waited_ms` int tag.

**Recovery:** Add `waited_ms = waited as i64` to the offending primitive's `.detected` span emit. Required by the [PRD span contract](prd.md#span-contract-locked).

## Cross-references

* Spec: [`prd.md`](prd.md)
* Parent: [`chaos-harness/runbook.md`](../chaos-harness/runbook.md).
