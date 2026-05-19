# boot-chain — runbook

## Failure modes

### Mode 1 — Boot trace shows fewer than 6 rafka.* ops

**Cause:** Regression in `NodeRuntime::run()` where a `.instrument(info_span!(...))` chain was dropped, OR OTel export race during shutdown.

**Detection:** D-024 artifact set check after every sprint:
```bash
TID=<trace id>
curl -s "http://localhost:16686/api/traces/$TID" | python -c "import sys,json; ops=sorted(set(s['operationName'] for s in json.load(sys.stdin)['data'][0]['spans'] if s['operationName'].startswith('rafka.'))); print(len(ops), ops)"
```
Expected: 5–6 ops including `rafka.mesh.node.ready` AND `rafka.mesh.boot.endpoint_created`.

**Recovery:** Read `crates/rafka-node-base/src/lib.rs::NodeRuntime::run`. Confirm each child span is wrapped via `.instrument(info_span!("rafka.mesh.boot.<phase>"))`. If `node.ready` is missing, the boot chain was refactored such that iroh background tasks inherited the root span context — fix by moving the iroh endpoint creation OUTSIDE the root span (per sprint-06 regression-fix-v2 pattern).

### Mode 2 — Boot trace exists but `bind_addr` shows `0.0.0.0:0`

**Cause:** iroh bound IPv6-only on Windows wildcard, but the late-bind filter on `bind_sockets()` only matches IPv4.

**Recovery:** Take ALL bound sockets (drop the IPv4 filter):
```rust
let bound: Vec<String> = transport.endpoint.bound_sockets().into_iter().map(|a| a.to_string()).collect();
let actual_bind_addr = if bound.is_empty() { bind_addr.to_string() } else { bound.join(", ") };
```
Already fixed in sprint-02 (commit `fd1252d`).

### Mode 3 — No boot trace at all in Jaeger

**Cause:** OTLP collector down OR `OTEL_EXPORTER_OTLP_ENDPOINT` misconfigured.

**Recovery:**
```bash
podman ps --format "{{.Names}}\t{{.Status}}" | grep otel
curl -s http://localhost:4316/v1/traces  # should 405 not refused
podman compose -f E:/dev/rafka/deployment/dev/compose.test-otlp.yml up -d
```

## Cross-references

* Parent: substrate.
* Sibling: [`heartbeat runbook`](../heartbeat/runbook.md), [`peer-discovery runbook`](../peer-discovery/runbook.md).
