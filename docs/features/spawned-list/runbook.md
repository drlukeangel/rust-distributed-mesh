# spawned-list — runbook

## Failure modes

### Mode 1 — Returns empty even though nodes were spawned

**Cause:** topology-ui restarted; DashMap is process-local and resets on restart. Spawned subprocesses keep running but are now orphaned (UI lost their handles).

**Recovery:** Orphaned subprocesses must be killed at OS level:
```powershell
Get-Process rafka-* | Stop-Process -Force
```
Then re-spawn fresh via `rfa mesh node add`.

### Mode 2 — Lists names but trying to DELETE returns 404

**Cause:** Race — entry was removed from DashMap (e.g., another kill in progress) between the list and the delete.

**Recovery:** Re-query `/api/nodes/spawned`, retry kill with current names. Self-correcting.

### Mode 3 — Lists names of subprocesses that have actually died

**Cause:** Subprocess crashed (OOM, panic) without `topology-ui` noticing — DashMap still holds the Child handle but the underlying PID is gone.

**Detection:** `Get-Process -Id <pid>` returns nothing for a name in the spawned list.

**Recovery:** Trigger DELETE on the name; `Child::kill()` will return success (process already dead) and the entry gets cleaned up. Could also add a background reaper that polls `Child::try_wait()` and removes exited subprocesses from the DashMap — queued cleanup.

## Cross-references

* Parent: operator visibility.
* Sibling: [`subprocess-control runbook`](../subprocess-control/runbook.md).
