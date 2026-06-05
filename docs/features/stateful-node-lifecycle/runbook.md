# stateful-node-lifecycle — runbook

## Symptom: `POST /restart` returns 404

**Cause:** `spawned_meta` does not have an entry for the node name. Either:
- The node was spawned on a different admin-ui instance (each admin-ui has its own `spawned_meta`).
- The node was already fully cleaned up (a previous `DELETE` with manual dir wipe, or admin-ui restarted).

**Fix:** The node must be re-spawned. If the data dir still exists at `E:/tmp/rafka-ui-nodes/<name>/`, spawn a new node with the same `mesh_id` and `stateful:true` — but note the `node_name` will be different (new key). To recover the original identity you need to re-pass the original `secret_key_hex` out-of-band, which is not currently supported via the REST API.

## Symptom: `POST /restart` returns 422

**Cause:** The node was spawned without `"stateful":true`. Restart preserves identity only for stateful nodes.

**Fix:** `DELETE` the node, then respawn with `"stateful":true`. Data dir from the non-stateful spawn was wiped on DELETE; the new spawn starts fresh.

## Symptom: After restart, `node_id` changed

**Cause:** `SpawnedMeta.secret_key_hex` was empty (node was spawned before this field was added — pre-sprint-stateful-node-restart builds). The restart fell back to `node-identity.json`, which should still give the same key (it was written by the original spawn). If the file was also absent, iroh minted a new key.

**Fix:** Upgrade to sprint-stateful-node-restart build. All nodes spawned on the new build store `secret_key_hex` in meta.

**Verification:** Check the restart span in Jaeger — `old_pid` and `new_pid` differ; `node_id` should be the same. If `node_id` changed, the span will show the NEW node_id (not the original).

## Symptom: Topology shows a stateful node stuck in `Joining` after restart

**Cause:** The restarted process hasn't connected to the mesh yet. This is normal for the first 2-5 seconds. Gossip interval is 2000ms; the node sends its first digest within ~1 gossip tick after boot + mesh connection.

**Fix:** Wait 5-10 seconds. If still stuck:
1. Check `E:/tmp/rafka-ui-nodes/<name>/` for a log file (admin-ui captures child stdout/stderr when `CARGO_CAPTURE_CHILD_IO` is set).
2. Query Jaeger for `rafka.mesh.node.ready` to confirm the restart ran the boot chain.
3. Query `/api/heartbeats` — the node should appear with `age_ms > 0` within 10s of restart.

## Symptom: `POST /restart` returns 500 "restart spawn failed: ..."

**Cause:** Binary not found at the expected path (wrong `CARGO_TARGET_DIR` or `RAFKA_CHILD_BUILD_PROFILE`), or the port is still held by the old process (OS takes a few seconds to release a TCP port after process exit).

**Fix:**
- Verify `CARGO_TARGET_DIR` and `RAFKA_CHILD_BUILD_PROFILE` env vars are set correctly on admin-ui.
- If it's a port-in-use error, wait 5s and retry — the restart loop waits up to 10s for the old process to exit, but the OS CLOSE_WAIT TTL may extend that window on Windows.

## Symptom: Reaper keeps wiping a stateful node's data dir on crash

**Cause:** The node was not actually spawned as stateful (`SpawnedMeta.stateful` is false). The topology UI badge and gossip `stateful:true` come from the `RAFKA_STATEFUL` env var on the CHILD; but if admin-ui spawned it without `"stateful":true` in the spawn request, `SpawnedMeta.stateful` is false and the reaper wipes the dir.

**Fix:** Always spawn via `POST /api/nodes/spawn { "stateful": true }` — do not set `RAFKA_STATEFUL` manually in `extra_env`. The `stateful` field on the spawn request is the authoritative source of truth for reaper behaviour.

## Symptom: `state.restarting` has a stuck entry after an admin-ui restart

**Cause:** Admin-ui crashed mid-restart after inserting into `restarting` but before clearing it. On restart `state.restarting` is a fresh empty `DashSet`.

**Impact:** None — `restarting` is ephemeral (process-global in-memory DashSet). After admin-ui restart the restarting state is gone; if the data dir survived it can be restarted normally.

## Jaeger queries for stateful restart investigation

```
# Find all restart spans in last 1h
http://localhost:16686/search?service=<mesh>.admin-ui&operation=rafka.ui.node.restart&lookback=1h

# Find reaped spans where a stateful node exited unexpectedly
http://localhost:16686/search?service=<mesh>.admin-ui&operation=rafka.ui.subprocess.reaped&lookback=1h
# Look for spans with stateful=true in attributes — these are crash exits on stateful nodes.
```
