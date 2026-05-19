# subprocess-control — overview

> **Source:** Operator UI feature. topology-ui can spawn + kill node subprocesses on-demand; closes the loop where UI launches a node and then renders that node's boot waterfall in the same page.

## What it is

`POST /api/nodes/spawn { "node_type": "gateway" | "broker" | "compute" | "registry" }` forks a subprocess of the appropriate binary, tracks the `Child` handle in `Arc<DashMap<String, Mutex<Child>>>`, returns `{node_name, pid}`.

`DELETE /api/nodes/{node_name}` looks up the Child, calls `start_kill()`, waits 5s for graceful exit, escalates to SIGKILL if needed, removes from registry, deletes spawn data dir.

`GET /api/nodes/spawned` returns the registry's key list — operator visibility into what UI launched.

## How it works

`topology-ui/src/main.rs::handle_spawn`:
1. Generate `node_name = format!("{type}-{8hex_random}")`.
2. Create spawn dir `E:/tmp/rafka-ui-nodes/{node_name}/`.
3. Locate binary via `CARGO_TARGET_DIR` env var (default `./target`); resolve `{cargo_target_dir}/debug/rafka-{type}.exe`.
4. `tokio::process::Command::new(binary_path).envs([OTEL_*, RAFKA_DATA_DIR]).spawn()`.
5. Insert Child into DashMap; emit `rafka.ui.subprocess.spawned{node_name, node_type, pid, otel.kind="internal"}`.

`handle_kill`:
1. Remove Child from DashMap.
2. `Mutex::into_inner()` → `child.start_kill()` → `tokio::time::timeout(5s, child.wait())`.
3. On timeout: `child.kill().await` (force).
4. Delete spawn dir; emit `rafka.ui.subprocess.killed{node_name, pid, reason}` where `reason` ∈ {`graceful`, `forced`}.

`handle_spawned_list`:
1. Iterate DashMap keys → return as JSON array.
2. Emit `rafka.ui.spawned_list{count, otel.kind="internal"}`.

## Locked spans

- `rafka.ui.subprocess.spawned{node_name, node_type, pid, otel.kind="internal"}`
- `rafka.ui.subprocess.killed{node_name, pid, reason, otel.kind="internal"}` — `reason` ∈ {`graceful`, `forced`}
- `rafka.ui.subprocess.spawn_failed{node_type, error, otel.kind="internal"}` — fork/path errors
- `rafka.ui.spawned_list{count, otel.kind="internal"}` — visibility queries

## Invariants

1. **Spawn dirs live under `E:/tmp/rafka-ui-nodes/`.** Subprocesses are ephemeral; killing the node deletes the dir.
2. **OTEL_* env vars inherit from parent topology-ui.** Spawned subprocesses push spans to the same Jaeger.
3. **Spawn responses include both `node_name` (for kill) and `pid` (for OS-level inspection).**
4. **Child handle stays in registry until kill returns 200.** No orphaned subprocesses on UI restart (process-tree dies with parent).

## Cross-references

* Sibling: [`topology-ui-waterfall`](../topology-ui-waterfall/overview.md), [`chaos-harness`](../chaos-harness/overview.md) (chaos primitives consume these endpoints).
* Code: `topology-ui/src/main.rs::{handle_spawn, handle_kill, handle_spawned_list}`.
* Decisions: D-007 (topology-ui has HTTP), D-026 (axum allowed on topology-ui).
