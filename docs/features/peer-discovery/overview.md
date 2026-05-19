# peer-discovery — overview

> **Source:** Substrate feature. Replaces v1's custom QUIC peer discovery with iroh's built-in mdns + manual seed list per D-002.

## What it is

How nodes find each other on the mesh. Two simultaneous paths:

1. **mdns** — iroh's `LocalSwarmDiscovery` announces NodeId + addresses on the local network; peers receive announcements and dial. No config needed.
2. **Seed list** — `RAFKA_SEED_NODES` env var (CSV of `<node_id_hex>@<host>:<port>`) for explicit cross-network or first-boot bootstrap.

## How it works

`crates/rafka-node-base/src/lib.rs` spawns three tasks per boot:
- `dial_seeds` — parses `RAFKA_SEED_NODES`, dials each via `endpoint.connect()`, emits `rafka.mesh.peer.discovered{source="seed"}` then `rafka.mesh.peer.connected{direction="outbound"}` on handshake.
- `watch_mdns` — subscribes to iroh's mdns channel; for each newly-announced peer, dials + emits `peer.discovered{source="mdns"}` + `peer.connected{direction="outbound"}`. Skips if already in registry.
- `start_accept_loop` — accepts inbound connections via `endpoint.accept()`, completes handshake, emits `peer.connected{direction="inbound"}`.

All three insert the live `iroh::endpoint::Connection` into a shared `PeerRegistry: Arc<DashMap<String, Connection>>` keyed by NodeId hex.

## Locked spans (Principle #10)

- `rafka.mesh.peer.discovered{node_id, peer_id, peer_node_type, source}` — `source` ∈ {`seed`, `mdns`}
- `rafka.mesh.peer.connected{node_id, peer_id, peer_node_type, direction}` — `direction` ∈ {`inbound`, `outbound`}
- `rafka.mesh.peer.disconnected{node_id, peer_id, reason}` — fires when iroh `Connection::closed()` resolves

## Invariants

1. **Registry keyed by NodeId, never address.** D-028 rule 1.
2. **Mutual handshake = 2 peer.connected spans.** One per side per pair, opposite `direction` tags.
3. **mdns dedup.** `watch_mdns` checks `registry.contains_key(peer_id)` before dialing — prevents re-connect storms from mdns republish.

## Cross-references

* Sibling: [`boot-chain`](../boot-chain/overview.md), [`frame-exchange`](../frame-exchange/overview.md).
* Code: `crates/rafka-node-base/src/lib.rs` (`dial_seeds`, `watch_mdns`, `start_accept_loop`).
* Decisions: D-002 (iroh substrate), D-028 (NodeId-keyed registry).
