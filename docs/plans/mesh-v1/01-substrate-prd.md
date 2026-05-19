# PRD — Mesh Substrate (iroh)

**Status:** Open
**Companion to:** `00-mesh-rebuild-prd.md`
**Default candidate:** [`iroh`](https://crates.io/crates/iroh) — QUIC-native rust mesh framework with peer discovery + relay support

---

## 1. Why iroh (default candidate)

- **QUIC-native** — matches the architectural lock that intermittent device support requires QUIC (0-RTT resume, connection migration). Rules out tonic/gRPC over HTTP/2.
- **Designed for direct peer connections + NAT traversal** — exactly the operational class rafka keeps hitting (Windows firewall, mobile network handoffs, branch-office NAT).
- **Built-in relay tier** for cross-NAT / cross-region bridging. Multi-mesh comes nearly for free.
- **Identity + ALPN** built into the endpoint model — fits the per-node-type discrimination rafka needs.
- **mdns + DNS discovery** out of the box.
- **n0-computer maintains it.** Active, production-deployed by `iroh-blobs` / `iroh-docs` at internet scale.

If iroh fails the chaos battery, the fallback ladder is:
1. **libp2p** (rust-libp2p) — heavier API, more battle-tested. QUIC transport via quinn. Swarm + behaviour composition gives multi-protocol cleanly.
2. **quinn + chitchat (gossip)** — lower-level. Quinn for QUIC, chitchat for SWIM-style gossip membership.
3. **quinn + foca (SWIM)** — alternative to chitchat.

**Custom QUIC mesh is never on this ladder.** That's the principle being established (Golden Principle #13).

## 2. What rafka uses iroh for

- **Endpoint lifecycle** — boot a node, get an `iroh::Endpoint` with a stable `EndpointId`
- **Peer discovery** — via iroh's mdns + DNS lookups; gossip-style membership over the endpoint
- **Bidirectional streams** — for `InternalMeshFrame` request/response
- **ALPN multiplexing** — `rafka-mesh-v1` (intra-mesh ops) + future ALPN strings for new protocols
- **Relay servers** — for cross-NAT / cross-region (Sprint 2)
- **Connection migration** — automatic; rafka writes none of it

## 3. What rafka does NOT write

- ❌ Hand-rolled gossip protocol → use iroh's discovery + extend with own membership table
- ❌ Custom peer-discovery messages → use iroh's discovery framework
- ❌ NAT traversal logic → iroh-relay handles it
- ❌ Connection migration → iroh / quinn handles it
- ❌ Custom relay rendezvous → iroh-relay
- ❌ Custom QUIC accept-loop tuning → iroh manages the endpoint

## 4. Node types

Four node-types from day 1:

| Type | Binary | ALPN | Role |
|---|---|---|---|
| Gateway | `rafka-gateway` | `rafka-mesh-v1` | Customer-facing (when wire protocol returns); mesh-routing brain |
| Broker | `rafka-broker` | `rafka-mesh-v1` | Storage (SingleWal when WAL returns); mesh participant |
| Compute | `rafka-compute` | `rafka-mesh-v1` | Job tailer + RSQL + WASM host (when restored); mesh participant |
| Registry | `rafka-registry` | `rafka-mesh-v1` | Schema registry process (extracted from current gateway-monolithic shape); mesh participant |

Each binary is a process with:
- One iroh `Endpoint`
- One identity (`EndpointId`)
- One `node-type` tag in gossip metadata
- Standard observability hooks (OTLP spans, structured logs)
- NO HTTP, NO REST, NO axum dependency. The single management surface is the `/topology` subscription stream on the mesh itself, consumed by the topology UI process.

## 5. `MeshTransport` reuse from `rafka-mesh-ops`

The existing `rafka-mesh-ops` crate already abstracts the transport via the `MeshTransport` trait (per `_migrationv2/architecture/03-wire-and-mesh.md §5.3`):

```rust
#[async_trait]
pub trait MeshTransport: Send + Sync {
    async fn send(&self, frame: InternalMeshFrame) -> Result<Bytes, MeshError>;
}
```

**New impl:** `IrohMeshTransport` in `crates/rafka-mesh-transport/` implements this trait against iroh's bidirectional-stream API. The custom QUIC impl is deleted in Sprint 1, not preserved as a fallback.

`InternalMeshFrame` shape is unchanged from `03-wire-and-mesh.md §3.1` — same fields, same `for_op` constructor, same `org_id` mandatory. The codec crate is correct; only the transport changes.

## 6. Identity model

- Each node has a permanent `EndpointId` (iroh's Ed25519 keypair) — generated at first boot, persisted in `${RAFKA_DATA_DIR}/node-identity.json`
- Node identity is NOT the same as rafka's `principal_id` (app-layer authz concept; comes back later)
- `EndpointId` is used for:
  - Mesh-layer peer addressing
  - Gossip membership keying
  - Topology UI node identification
  - Audit attribution at the substrate layer

## 7. Boot sequence (per node)

1. Load or mint `EndpointId` from `${RAFKA_DATA_DIR}/node-identity.json`
2. Construct `iroh::Endpoint` bound to `${RAFKA_NODE_BIND_ADDR}` (default `0.0.0.0:0` = ephemeral)
3. Register ALPN `rafka-mesh-v1`
4. Register discovery providers (mdns + dns + manual seed list)
5. Tag gossip metadata: `{node_type: "broker", started_at_unix: 1234567890, version: "0.x.y"}`
6. Start gossip membership task (subscribes to peer arrivals/departures)
7. Start `InternalMeshFrame` accept loop (per-ALPN)
8. Emit `rafka.mesh.node.started{node_id, node_type, addr}` span
9. Mark "ready" in process-local state (the topology UI's `/topology` subscription sees this)

## 8. Configuration

Environment variables ONLY (no config files for substrate; CLAUDE.md KISS):

| Var | Default | Purpose |
|---|---|---|
| `RAFKA_NODE_TYPE` | required | One of `gateway`, `broker`, `compute`, `schema` |
| `RAFKA_NODE_BIND_ADDR` | `0.0.0.0:0` | iroh endpoint bind |
| `RAFKA_DATA_DIR` | `./data/node-${random}` | Identity + persistent state |
| `RAFKA_SEED_NODES` | empty | CSV of `<EndpointId>@<host>:<port>` for bootstrap discovery |
| `RAFKA_RELAY_URLS` | empty | CSV of iroh-relay URLs (Sprint 2+) |
| `RAFKA_GOSSIP_INTERVAL_MS` | `500` | Gossip heartbeat |
| `RAFKA_OTLP_ENDPOINT` | empty | OTLP collector for spans |

## 9. Observability hooks (from day 1)

Every substrate operation emits:

- `rafka.mesh.node.started`
- `rafka.mesh.node.stopped`
- `rafka.mesh.peer.discovered`
- `rafka.mesh.peer.connected`
- `rafka.mesh.peer.disconnected`
- `rafka.mesh.peer.staleness_timeout`
- `rafka.mesh.gossip.heartbeat_sent`
- `rafka.mesh.gossip.heartbeat_received`
- `rafka.mesh.frame.sent` (per `InternalMeshFrame`)
- `rafka.mesh.frame.received`
- `rafka.mesh.frame.decode_failed`
- `rafka.mesh.relay.connect.via-url`
- `rafka.mesh.relay.peer.via-relay-path`

All spans carry `node_id` + `node_type` attributes. The topology UI subscribes to these spans to render real-time state.

## 10. Acceptance criteria (Sprint 0)

1. `cargo run -p rafka-gateway` boots, mints identity, joins mesh, emits `rafka.mesh.node.started`
2. Running 3 nodes (gateway + broker + compute) on `localhost`: every pair discovers each other within 5s
3. Killing one node: surviving nodes emit `rafka.mesh.peer.staleness_timeout` within `4 × RAFKA_GOSSIP_INTERVAL_MS`
4. Restarting the killed node with the SAME identity: surviving nodes re-establish connection within 5s
5. All 14 substrate spans land in `tests/artifacts/mesh-substrate/*.spans.jsonl`
6. Zero `unwrap()` panics on every chaos-inject path during boot
7. Workspace gate: `cargo check --workspace --tests --no-default-features` = 0 errors, 0 new warnings

## 11. Migration from current code

The `rafka-mesh-transport` crate exists today as a thin wrapper around the custom QUIC mesh. The work is:

1. Add an `iroh` dep to `rafka-mesh-transport/Cargo.toml`
2. Create `IrohMeshTransport` struct implementing `MeshTransport` against iroh
3. Switch `GatewayBrokerRpc` (gateway/src/cache/transports.rs) to use `IrohMeshTransport`
4. Delete the custom QUIC mesh implementation (`gateway/src/mesh/quic_mesh.rs` content; only keep what the type system requires)
5. Update binary `main.rs` entry points to call the new iroh-based boot

Sprint 1 deletes the legacy QUIC mesh code path; no fallback maintained.
