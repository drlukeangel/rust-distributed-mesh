# stateful-node-lifecycle — overview

> **Source:** Admin-ui feature (sprint-stateful-node-restart). Stateful nodes persist their iroh identity (NodeId) and data dir across operator-initiated restarts, enabling zero-identity-change rolling upgrades without re-bootstrapping the mesh.

## What it is

A node spawned with `"stateful": true` in the spawn request carries two extra guarantees:

1. **Data dir is never auto-wiped.** Neither `kill_one` nor the reaper calls `remove_dir_all` on a stateful node's spawn dir (`E:/tmp/rafka-ui-nodes/<node_name>/`). The dir — including `node-identity.json` — survives across kills, crashes, and restarts.

2. **`POST /api/nodes/{name}/restart` preserves NodeId.** The route kills the OS process (direct SIGKILL, no mesh Shutdown op) and immediately re-spawns the same binary in the same data dir with the same `RAFKA_NODE_SECRET_KEY`. Because the key is re-passed explicitly, the node boots with the identical iroh `PublicKey` → same `node_id` → same `node_name` on the mesh. Peers reconnect automatically via HyParView; the mesh doesn't notice a new node joined.

The chaos `restart_node` primitive is **unaffected** — it still spawns a fresh identity (new NodeId, new data dir). Stateful restart is the operator-plane path only.

## How it works

### Spawn path

`POST /api/nodes/spawn { "node_type": "broker", "mesh_id": "mesh1", "stateful": true }`:

1. `handle_spawn` reads `body.stateful` and passes it to `spawn_one`.
2. `spawn_one` generates a `SecretKey`, derives `node_id_hex` and `node_name` (unchanged from non-stateful flow).
3. Sets `RAFKA_STATEFUL=true` in the child's env so it broadcasts `stateful:true` in every `GossipDigest`.
4. Stores both `stateful: true` and `secret_key_hex` in `SpawnedMeta`.

### Restart path

`POST /api/nodes/{name}/restart`:

1. Looks up `SpawnedMeta` — returns 404 if not found, 422 if node is not stateful.
2. Adds `node_name` to `state.restarting` (DashSet) — prevents reaper from wiping dir/meta in the kill→respawn window.
3. Removes the old `Child` from `state.processes` and calls `start_kill()` + waits for genuine OS process exit (up to 10s).
4. Builds a new `Command` with the **same** `spawn_dir`, `bind_port`, `mesh_id`, and re-passes `RAFKA_NODE_SECRET_KEY=<original secret_key_hex>` so the node boots with the same NodeId.
5. Inserts the new `Child` into `state.processes`; updates `SpawnedMeta.pid` (all other fields unchanged).
6. Clears `node_name` from `state.restarting`.
7. Emits `rafka.ui.node.restart` span with `node_name`, `node_id`, `old_pid`, `new_pid`, `mesh_id`.

### Reaper-loop protection

The reaper runs every 5 seconds. For any node that has exited:
- Checks `spawned_meta.get(name).stateful` BEFORE removing meta.
- Checks `restarting.contains(name)` for the restart window.
- If stateful OR restarting: removes from `processes` but SKIPS `remove_dir_all` and `spawned_meta.remove`. The orphan-sweep pass (second pass) also skips dirs where `spawned_meta.contains_key(name)`.

## Locked spans

| Span | Attributes |
|---|---|
| `rafka.ui.node.restart` | `node_name`, `node_id`, `old_pid`, `new_pid`, `mesh_id`, `otel.kind="internal"` |

The `stateful` and `restarting` attributes are added to the existing `rafka.ui.subprocess.reaped` span so operators can see whether a reaped exit was from a stateful or restarting node.

## Topology and heartbeats surface

`/api/topology` and `/api/heartbeats` include `"stateful": true/false` per node (sourced from gossip digest for live nodes, from `spawned_meta` for pending nodes). The React UI renders a **Stateful** badge on stateful nodes in both the Topology and Nodes tabs.

## Invariants

1. **Stateful node data dir is never auto-wiped** — `kill_one`, the reaper first pass, and the reaper orphan-sweep all check `stateful`/`restarting` before any `remove_dir_all`.
2. **NodeId is preserved across stateful restarts** — `restart_one` re-passes `RAFKA_NODE_SECRET_KEY` from `SpawnedMeta.secret_key_hex`; the child ignores its file and uses the env key (takes precedence in `load_or_mint_identity`).
3. **Chaos restart is unaffected** — `chaos_loop` calls `spawn_one` with `stateful=false` and produces a new NodeId. No overlap with the stateful path.
4. **Non-stateful kill is unchanged** — `kill_one` behaviour for `stateful=false` nodes is identical to pre-sprint.
5. **Stateful semantics survive meta-anchor** — if the process exits unexpectedly (crash), the reaper keeps meta alive, so the operator can call `/restart` at any time to bring it back with the same identity.

## Cross-references

- `OPERATING.md` — operator runbook for stateful node management
- `docs/features/subprocess-control/overview.md` — parent feature (spawn/kill)
- `docs/features/stateful-node-lifecycle/how-to.md` — usage guide
- `docs/features/stateful-node-lifecycle/runbook.md` — incident runbook
- `CLAUDE.md §10` — locked span: `rafka.ui.node.restart`
- Code: `admin-ui/src/main.rs::{restart_one, handle_restart, reaper_loop, kill_one}`
- Code: `crates/rafka-node-base/src/lib.rs::GossipDigest.stateful`
