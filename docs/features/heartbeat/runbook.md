# heartbeat — runbook

## Failure modes

### Mode 1 — peer_count tag is always "0" string-typed

**Cause:** `run_heartbeat` is emitting peer_count as `u32` not `i64`. tracing-opentelemetry encodes u64 inconsistently with int64-typed Jaeger attributes; the result reads as string `"0"`.

**Detection:**
```bash
curl -s "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.heartbeat&limit=1&lookback=5m" | python -c "import sys,json; d=json.load(sys.stdin); print([(t['type'],t['value']) for t in d['data'][0]['spans'][0]['tags'] if t['key']=='peer_count'])"
```

Expected: `[('int64', N)]`. If `('string', '0')` — regression.

**Recovery:** Ensure `let peer_count = registry.len() as i64;` and the macro is `peer_count = peer_count,` (or just `peer_count,` shorthand with i64 binding). Fixed at sprint-06 commit `5ee222f`.

### Mode 2 — Only 1 heartbeat trace in Jaeger per node (should be ~12 per minute)

**Cause:** `run_heartbeat` is `#[instrument]`-decorated. The function span never closes (infinite loop), OTel batch processor holds child heartbeat spans waiting, only the first one exports before shutdown.

**Detection:**
```bash
# Should be ≥10 heartbeats over a 1-min run
COUNT=$(curl -s "http://localhost:16686/api/traces?service=gateway&operation=rafka.mesh.heartbeat&limit=100&lookback=1m" | python -c "import sys,json; print(len(json.load(sys.stdin).get('data',[])))")
echo "$COUNT heartbeats in 1m"
```

**Recovery:** Remove `#[instrument(skip_all)]` from `run_heartbeat` in `crates/rafka-node-base/src/lib.rs`. Each tick must be its own root span. Code comment in lib.rs explains the why — don't reintroduce the wrapper.

### Mode 3 — peer_count goes to 0 even when peers are connected

**Cause:** `PeerRegistry` is being recreated somewhere (deep DashMap clone instead of Arc clone), so the heartbeat reads from a different map than the discovery code inserts into.

**Recovery:** All three insert sites (`dial_seeds`, `watch_mdns`, `start_accept_loop`) and `run_heartbeat` MUST receive the same `Arc::clone(&peer_registry)` at boot. Grep for `peer_registry.clone()` (deep) vs `Arc::clone(&peer_registry)` (correct).

## Cross-references

* Parent: substrate.
* Sibling: [`peer-discovery runbook`](../peer-discovery/runbook.md), [`node-base runbook`](../node-base/runbook.md).
