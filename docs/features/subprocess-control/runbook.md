# subprocess-control — runbook

## Failure modes

### Mode 1 — spawn returns 500 "binary not found"

**Cause:** topology-ui resolves binary path via `CARGO_TARGET_DIR`. If unset or wrong, spawning fails.

**Recovery:**
```bash
CARGO_TARGET_DIR=E:/cargo-target-v2 cargo run -p rafka-topology-ui
# topology-ui will now find binaries at E:/cargo-target-v2/debug/rafka-{type}.exe
```

### Mode 2 — kill returns 200 but subprocess still alive

**Cause:** `child.start_kill()` is graceful on Unix (SIGTERM) but Windows treats it as `TerminateProcess` — graceful sends nothing meaningful. The escalation path (timeout → `child.kill()`) is needed.

**Recovery:** Already handled in `handle_kill`. If subprocess survives 10s, force-path fired and span shows `reason=forced`. If STILL alive: external task manager (`taskkill /F /PID <pid>`).

### Mode 3 — spawn data dir not deleted

**Cause:** Subprocess holds a file handle in the dir (likely the identity file). On Windows, deletion fails while file is locked.

**Recovery:** `handle_kill` logs the delete error and returns 200 anyway (it's best-effort cleanup). Stale dirs accumulate in `E:/tmp/rafka-ui-nodes/`; nuke periodically:
```powershell
Remove-Item -Recurse -Force E:/tmp/rafka-ui-nodes/*
```

### Mode 4 — `/api/nodes/spawned` shows entries for already-dead processes

**Cause:** topology-ui crashed mid-kill — Child was removed from registry but process didn't get killed cleanly.

**Recovery:** Restart topology-ui (registry resets); manually kill orphaned PIDs via `taskkill /F /IM rafka-*.exe`.

### Mode 5 — Spawn of `gateway` succeeds but two gateways collide on something

**Cause:** Today only one gateway pings; if two are spawned, both ping. Not a bug per se but ping-pong traffic doubles.

**Recovery:** Currently OK — extra gateway just emits extra pings. Future sprint may add a leader-election shim if needed.

## Cross-references

* Parent: operator UI.
* Sibling: [`topology-ui-waterfall runbook`](../topology-ui-waterfall/runbook.md), [`chaos-harness runbook`](../chaos-harness/runbook.md).
