# topology-ui-waterfall — runbook

## Failure modes

### Mode 1 — `/api/nodes` returns empty

**Cause:** Jaeger has no spans for `gateway`/`broker`/`compute`/`registry` (services have aged out OR mesh isn't running).

**Recovery:**
```bash
# verify nodes are alive
ps  # or PowerShell Get-Process rafka-*
# if not, launch them
```

### Mode 2 — Browser shows "no boot trace found for X"

**Cause:** The chosen node hasn't booted recently (no `rafka.mesh.node.ready` trace in 10m lookback).

**Recovery:** Restart that node, or via CLI:
```bash
rfa mesh node remove <existing-instance>
rfa mesh node add --type <type>
# wait 5s; refresh UI
```

### Mode 3 — Stale node types in dropdown (`data-gateway`, `compute-gateway`, `schema`)

**Cause:** Jaeger services list includes services that no current binary emits under. Topology-ui's filter (`KNOWN_NODE_TYPES`) should drop them.

**Recovery:** Verify the constant in `topology-ui/src/main.rs` matches the locked `node_type` enum from CLAUDE.md (currently `[gateway, broker, compute, registry]`). If you see ghost services in the UI, either the filter regressed or Jaeger memory needs a wipe:
```bash
podman restart rafka-test-jaeger
# all services age out + repopulate as nodes re-emit
```

### Mode 4 — UI's own spans don't appear in Jaeger

**Cause:** topology-ui's `init_telemetry()` call failed silently OR OTLP endpoint unreachable from topology-ui process.

**Recovery:** Check topology-ui stdout for `telemetry flush error` messages. Verify `OTEL_EXPORTER_OTLP_ENDPOINT` env var is set or default reachable.

## Cross-references

* Parent: operator UI.
* Sibling: [`telemetry-substrate runbook`](../telemetry-substrate/runbook.md).
