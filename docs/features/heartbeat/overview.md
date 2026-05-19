# heartbeat — overview

> **Source:** Substrate feature. Per-node periodic emission proving the runtime is alive AND seeing its peers.

## What it is

Every node fires a `rafka.mesh.heartbeat` span every 5 seconds, carrying the current peer count read from the in-process `PeerRegistry`. Operators (and the topology-ui status panel) detect a dead/silent node when heartbeats stop arriving in Jaeger.

## How it works

`crates/rafka-node-base/src/lib.rs::run_heartbeat` is a forever loop spawned at boot. Per iteration:

1. Sleep on `tokio::time::interval(5s)`.
2. `let peer_count = registry.len() as i64;` — read current peer count from the shared DashMap.
3. Emit `info_span!("rafka.mesh.heartbeat", node_id, peer_count)` and an `info!("heartbeat")` log event in scope.

**Critical:** the loop function is NOT `#[instrument]`-decorated. A wrapping span would never close (loop is infinite); OTel batch processor would hold child heartbeat spans waiting for the parent, dropping all but the first on shutdown. Each tick is its own root span instead.

## Locked spans (Principle #10)

- `rafka.mesh.heartbeat{node_id, peer_count}` — fires every 5s, no other attributes

## Invariants

1. **Each tick is a fresh root span.** Never wrap `run_heartbeat` in `#[instrument]`.
2. **`peer_count` is `i64`, not `u32`.** OTel SDK encodes i64 as `int64` attribute; the older u32 path got encoded as `string` (Jaeger displayed as `"0"`).
3. **5-second interval is fixed.** Configurability is deferred until ops needs it.

## Cross-references

* Sibling: [`peer-discovery`](../peer-discovery/overview.md).
* Code: `crates/rafka-node-base/src/lib.rs::run_heartbeat`.
* Decisions: D-025 (node-base extraction), sprint-06 heartbeat OTel export fix (commit 5ee222f).
