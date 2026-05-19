# chaos-harness — runbook

## Failure modes

### Mode 1 — `rfa mesh chaos kill` → "no UI-spawned subprocesses available"

**Cause:** No targets in topology-ui's DashMap. Chaos primitives only operate on UI-spawned subprocesses (not baseline binaries launched directly).

**Recovery:**
```bash
rfa mesh node add --type broker
# now retry kill
```

### Mode 2 — Soak reports `failed_timeout > 0`

**Cause:** A primitive's detection criterion didn't meet within the 30s deadline. For `kill_node` this means `/api/nodes/spawned` still includes the target name after 30s — either the kill HTTP returned but the subprocess didn't actually die, OR the spawned list cache is stale.

**Detection:** Inspect the JSON report:
```bash
python -c "import json; r=json.load(open('E:/tmp/rafka-chaos-soak-42.json')); [print(e) for e in r['events'] if e['detection']!='Passed']"
```

**Recovery:**
- If kill_node: check `taskkill /F /PID <pid>` on the leaked subprocess and root-cause why topology-ui's `Child::kill()` didn't take.
- If restart_node detection timeout: subprocess re-spawn might be hanging on identity load (file locked from prior process). Bump deadline or clean spawn dirs.

### Mode 3 — Soak reports `failed_assertion > 0` from "execute: ..."

**Cause:** Primitive's execute() raised — topology-ui unreachable, or invalid target picked, or spawn failed.

**Recovery:** Inspect the assertion message in the SoakEvent. Most common: `pick_random_spawned` raised because between primitive picks, ALL subprocesses got killed and the registry is empty.

### Mode 4 — chaos spans missing from Jaeger after soak

**Cause:** Same shape as the sprint-10 rfa span drop — long-running rfa process emits many spans, but exit-time flush has a race.

**Detection:** Compare event count in report vs span count in Jaeger:
```bash
EVENTS=$(python -c "import json; print(json.load(open('E:/tmp/rafka-chaos-soak-42.json'))['event_count'])")
SPANS=$(curl -s "http://localhost:16686/api/traces?service=rfa&operation=rafka.chaos.primitive.executed&limit=200&lookback=10m" | python -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))")
echo "events=$EVENTS spans=$SPANS"
```

**Recovery:** Open follow-up; primitive execute() spans should be exported eagerly by SimpleSpanProcessor but apparently aren't always. Fix: explicit `force_flush()` between chaos events OR convert chaos span emission to ad-hoc OTel SDK Span (not via tracing layer) so it's flushed synchronously.

### Mode 5 — Soak hangs forever

**Cause:** A primitive's detect() loop has no deadline OR ignores the deadline param.

**Recovery:** All primitives must honor `deadline_ms` argument — early-return with `DetectionResult::FailedTimeout` when `Instant::elapsed() > deadline`. Audit `crates/rafka-chaos/src/primitives.rs`.

## Cross-references

* Parent: chaos engineering substrate.
* Sibling: [`subprocess-control runbook`](../subprocess-control/runbook.md), [`rfa-cli runbook`](../rfa-cli/runbook.md).
* PRD: `docs/plans/mesh-v1/04-chaos-harness-prd.md`.
