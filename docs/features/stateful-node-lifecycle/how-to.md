# stateful-node-lifecycle — how-to

## Spawn a stateful node

Add `"stateful": true` to the spawn body. All other fields work the same.

```bash
curl -X POST http://localhost:19090/api/nodes/spawn \
  -H 'Content-Type: application/json' \
  -d '{"node_type":"broker","mesh_id":"mesh1","stateful":true}'
```

Response:
```json
{"node_name":"mesh1.broker.a1b2c3","pid":12345}
```

The UI Nodes tab shows a **Stateful** badge on this node. Topology also marks it `stateful:true`.

## Restart a stateful node (preserving identity)

```bash
curl -X POST http://localhost:19090/api/nodes/mesh1.broker.a1b2c3/restart
```

Response:
```json
{
  "node_name": "mesh1.broker.a1b2c3",
  "node_id": "<same as before>",
  "old_pid": 12345,
  "new_pid": 12367,
  "mesh_id": "mesh1"
}
```

Verify identity is preserved:
```bash
curl -s http://localhost:19090/api/topology | \
  jq '.nodes[] | select(.id=="mesh1.broker.a1b2c3") | {node_id, stateful, state}'
```

You should see the same `node_id` as before the restart, `stateful: true`, and state `Joining` briefly then `Alive`.

## Kill a stateful node (without restarting)

`DELETE` works as usual — the node is killed via the mesh Shutdown op. For a stateful node the data dir and `spawned_meta` entry are **kept** so the operator can call `/restart` later.

```bash
curl -X DELETE http://localhost:19090/api/nodes/mesh1.broker.a1b2c3
```

To verify the data dir was preserved:
```bash
ls "E:/tmp/rafka-ui-nodes/mesh1.broker.a1b2c3/"
# should still show node-identity.json
```

## Permanently delete a stateful node (wipe data)

To fully decommission: call DELETE, then manually remove the dir:

```bash
curl -X DELETE http://localhost:19090/api/nodes/mesh1.broker.a1b2c3
Remove-Item -Recurse -Force "E:/tmp/rafka-ui-nodes/mesh1.broker.a1b2c3"
```

Or just stop admin-ui and clean up all spawned dirs at once.

## Verify the restart span in Jaeger

After a `POST /restart`, the `rafka.ui.node.restart` span appears in Jaeger under the admin-ui service within 1-2 gossip intervals (Jaeger batch export delay ~2s):

```
http://localhost:16686/search?service=<mesh>.admin-ui&operation=rafka.ui.node.restart&lookback=5m
```

Expected span attributes:
- `node_name` = the node's name
- `node_id` = the iroh PublicKey (unchanged across restart)
- `old_pid` = OS PID before restart
- `new_pid` = OS PID after restart
- `mesh_id` = the node's mesh

## What does NOT work with stateful restart

- **Non-stateful nodes:** `POST /restart` returns `422 Unprocessable Entity`. Spawn with `stateful:true` first.
- **Unknown nodes:** returns `404 Not Found`.
- **Chaos loop targets:** the chaos loop skips stateful nodes (they are not in the chaos victim pool — chaos always uses `stateful:false` nodes from `spawn_one`). If you want chaos to kill your stateful node, use `DELETE` manually; note the data dir is preserved.

## Combining with mesh_id and extra_env

Stateful nodes support the same `mesh_id` and `extra_env` fields as non-stateful spawns. The restart path re-passes `RAFKA_MESH_ID` (from `spawned_meta.mesh_id`) and does NOT re-apply `extra_env` — the node boots with the same env it had originally (minus chaos-specific vars that aren't re-applied on restart). If you need the node to restart with different env (e.g. a new `RAFKA_LINK_SLOW_MS`), DELETE it and respawn.
