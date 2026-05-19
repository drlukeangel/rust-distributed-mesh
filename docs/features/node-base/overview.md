# node-base — overview

> **Source:** Substrate library crate. The shared `rafka-node-base` per D-025; absorbs ALL boilerplate that was previously copy-pasted across the 4 node binaries.

## What it is

`crates/rafka-node-base/` exposes `NodeRuntime` + `Role` enum. Every node binary (`gateway`, `broker`, `compute`, `registry`) becomes a ~10-line `main.rs`:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    rafka_node_base::NodeRuntime::new("gateway")
        .with_role(Role::Gateway)
        .run()
        .await
}
```

All substrate work — identity, iroh endpoint, mdns + seed discovery, peer registry, accept loop, frame reader, ping sender (Role::Gateway only), heartbeat, shutdown — lives in `node_base::run()`.

## How it works

`NodeRuntime::run()`:

1. `init_telemetry(node_type)` — OTel pipeline up.
2. Load/mint identity from `RAFKA_DATA_DIR/node-identity.json`.
3. Build `IrohMeshTransport` with mdns discovery enabled.
4. Emit `rafka.mesh.node.ready` root span; nest the boot child spans under it.
5. Spawn background tasks: `dial_seeds`, `watch_mdns`, `start_accept_loop`, `run_heartbeat`, plus `run_ping_sender` if `Role::Gateway`.
6. `wait_for_signal()` — block on Ctrl+C or `RAFKA_AUTO_SHUTDOWN_SECS` timer.
7. Emit `rafka.mesh.node.stopping` and return.

## Role variants (locked enum)

```rust
pub enum Role { Gateway, Broker, Compute, Registry }
```

Today: only Gateway sends pings (`run_ping_sender`). All four accept incoming connections + reply to Pings with Pongs. Future per-role specialization (Broker storage, Compute jobs, Registry schema) layers on top of `NodeRuntime` without modifying it — added in each binary's `main.rs` as additional task spawns.

## Invariants

1. **No `axum` / HTTP server in node-base.** Per D-026.
2. **All async fns get `#[instrument]` EXCEPT infinite-loop functions emitting child spans** (per [`heartbeat`](../heartbeat/overview.md) — wrapping span never closes, children pile up).
3. **PeerRegistry keyed by String(node_id hex), never address.** Per D-028. Migration to typed `NodeId` key is queued cleanup.
4. **D-025 thin-shells.** Each binary's `main.rs` is ~10 lines. Growth happens in NEW files inside each binary's crate (e.g., future `gateway/src/serve_clients.rs`), NOT by re-inlining substrate code.

## Cross-references

* Code: `crates/rafka-node-base/src/lib.rs`.
* Sibling: [`boot-chain`](../boot-chain/overview.md), [`peer-discovery`](../peer-discovery/overview.md), [`heartbeat`](../heartbeat/overview.md), [`frame-exchange`](../frame-exchange/overview.md).
* Decisions: D-025 (extraction), D-026 (REST only on gateway), D-028 (NodeId-keyed registries).
