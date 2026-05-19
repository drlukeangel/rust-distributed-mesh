# mesh-to-mesh — overview

> **Status:** PLANNED (sprint-13). Not yet implemented.
> **Source:** Cross-mesh peering substrate. Per v1's `i35-cross-mesh-peering` feature; ports forward into v2's iroh substrate.

## What it is

Two (or more) separate rafka mesh clusters that peer via a bridge gateway joining both. The bridge gateway holds iroh connections to all peers in both meshes and forwards control-plane state across the boundary.

Use cases:
- **Geo-distribution.** Region-A mesh + Region-B mesh peer via a bridge gateway with public WAN connectivity. Iroh's DERP relay handles cross-NAT.
- **Test isolation.** Two meshes for chaos experiments — kill all of mesh-A and verify mesh-B operates uninterrupted; bring mesh-A back; verify re-convergence.
- **Tenant isolation.** Premium customers run dedicated meshes; bridge gateway exposes shared cluster-wide metadata.

## How it will work (design)

A bridge gateway is identified by `RAFKA_BRIDGE_MESHES=<mesh_id_a>,<mesh_id_b>` env var. On boot it:

1. Connects to both meshes via separate `IrohMeshTransport` instances (one per mesh's seed list or mdns scope).
2. Maintains `Arc<DashMap<MeshId, PeerRegistry>>` — one peer registry per mesh.
3. Emits `rafka.mesh.cross.peer_connected{mesh_id, peer_id}` spans for each cross-mesh handshake.
4. Forwards subscribed gossip topics across meshes: when mesh-A publishes a TopologyChange, the bridge re-publishes to mesh-B (and vice versa).

Each mesh has its own `mesh_id` (a UUID assigned at cluster creation, propagated via iroh-gossip per D-027). Spans carry `mesh_id` as a top-level attribute everywhere — operators can filter Jaeger to one mesh's traces.

## Locked spans (proposed — not yet shipped)

- `rafka.mesh.cross.peer_connected{node_id, peer_id, peer_mesh_id, direction}`
- `rafka.mesh.cross.peer_disconnected{node_id, peer_id, peer_mesh_id, reason}`
- `rafka.mesh.cross.gossip_forwarded{from_mesh_id, to_mesh_id, topic}`

## Topology UI impact

Per sprint-14, the topology UI gains a per-mesh panel (multi-mesh view per D-018). Bridge gateways render with edges crossing the panel boundary. Each mesh has its own dropdown + waterfall slot.

## Invariants (planned)

1. **Bridge gateway is the only node holding cross-mesh peer registries.** Other gateways stay single-mesh.
2. **Each mesh's gossip topic is independent.** Bridge re-publishes selectively; no automatic full-state mirroring.
3. **mesh_id is a top-level span attribute everywhere.** Operators can filter to one mesh in Jaeger.
4. **Cross-mesh connection is NOT counted in peer_count.** Heartbeat shows `peer_count = same_mesh_peers`; bridge's heartbeat shows two heartbeats (one per mesh, distinct peer_counts).

## Open design questions (resolve at sprint-13 dispatch)

- Bridge gateway = special role (`Role::Bridge`)? Or just a `Role::Gateway` with both meshes' seeds configured?
- Gossip forwarding: opt-in per topic, or all topics by default?
- Failure semantics: bridge gateway dies → both meshes still operate independently, with metadata divergence until bridge recovers. How are operators alerted?

## Cross-references

* Sibling: [`peer-discovery`](../peer-discovery/overview.md) (within-mesh discovery), [`chaos-harness`](../chaos-harness/overview.md) (cross-mesh chaos in sprint-14+).
* v1 reference: `E:/dev/rafka/docs/plans/i35-cross-mesh-peering*` (when accessible).
* Decisions: D-018 (multi-mesh UI), D-027 (iroh-gossip for control plane).
