# PRD — Cross-mesh backbone (mesh-v2)

**Status:** Open — architecture decision, 2026-05-31.
**Supersedes:** PRD `00-topology-cache-and-base-ui-prd.md` §2's "the gateway observes both meshes via
`RAFKA_OBSERVER_MESHES`, no shared global topic." That approach does NOT scale and is replaced here.
**Builds on:** per-mesh gossip (unchanged), the relay (data plane, PRD 01).

---

## 0. Why — the problem with "observe both meshes"

Today the only way a node learns another mesh is to subscribe to that mesh's ENTIRE gossip topic
(`RAFKA_OBSERVER_MESHES`), and only the gateway + admin-ui do it. At scale this breaks:
- **O(meshes²) full cross-subscriptions**, each a firehose of every remote node's per-tick digest.
- With **hundreds of gateways per mesh**, every cross-subscriber holds thousands of remote digests.
- It forces the **admin-ui to be special** (it must subscribe to every mesh to show a global view), and
  it makes the gateway a full member of other meshes' gossip (spurious topology edges).

The fix: separate the **control plane** (who exists, where, how loaded — summarized) from the **data
plane** (the actual cross-mesh writes). Cross-mesh awareness rides a thin **backbone** of per-mesh
*summaries*, not a firehose of per-node digests.

## 1. Three channels (all iroh-gossip / iroh — no new mesh primitives)

| channel | topic / transport | carries | scope |
|---|---|---|---|
| **intra-mesh detail** | per-mesh gossip `blake3(mesh_id)` (unchanged) | full per-node digests (per-node CPU/RAM/frames/location) | stays inside the mesh |
| **cross-mesh control** | **backbone** gossip topic `blake3("rafka.backbone")` (NEW) | per-mesh **summary**: directory + aggregate metrics | all meshes |
| **cross-mesh data** | the **relay** (PRD 01) | actual cross-mesh write frames when no direct path | as needed |

The backbone is **one additional iroh-gossip topic**. Not a DHT, not the reverted cross-mesh-replicated
global cache, no self-organization — hardcoded seeds, same gossip primitive, new topic.

## 2. The backbone summary (what one mesh publishes)

One record per mesh, refreshed each interval:

```
MeshSummary {
  mesh_id:        "mesh1",
  directory:      [ { node_name, node_type, location } ... ],   // low-churn: spawn/kill only
  aggregate:      { node_count, cpu_used, cpu_budget, ram_used, ram_budget, frames_per_sec },
  published_by:   <node_id of the publishing gateway>,
  wall_time_ms,   expires_at_ms,                                  // soft-lease (see §4)
}
```

The **directory** is what a gateway needs to route a cross-mesh write (`name → location`). The
**aggregate** is mesh-level rollup for the global view. The heavy per-node churn (every broker's CPU
every tick) **never leaves its mesh** — only the rollup does.

## 3. Aggregation — the gateway does it

The **gateway** aggregates its own mesh. It is already a member of its mesh's gossip, so its
`live_digests()` already holds every node in the mesh — aggregating is a local sum (no extra traffic):
`node_count`, `Σcpu/ram`, `Σframes_per_sec`, and the `name→{type,location}` directory. It publishes the
`MeshSummary` to the backbone. Brokers/computes/registries aggregate nothing — they stay pure intra-mesh
participants. (Gateway = the mesh's outward-facing role: cross-mesh write routing + backbone publish.)

## 4. Publisher selection — soft lease, NOT an election

Hundreds of gateways per mesh ⇒ exactly one must publish, but **no election protocol, no consensus, no
QUIC handshake** (Golden Principle #1). Leadership is a **soft lease carried on the backbone itself**:

- The publisher stamps each `MeshSummary` with `published_by` + `expires_at_ms` and **renews** it every
  interval.
- Other gateways watch the backbone. If the mesh's claim is **live** (not expired) they stay silent —
  **a lower-id gateway joining does NOT preempt a healthy publisher.** Leadership changes ONLY when the
  current publisher dies (its claim expires — dead-man's switch).
- `min(node_id)` over the live gateways is used **only to break ties for a vacant/expired seat**
  (startup, or after the publisher dies the contenders pick the lowest-id; the rest see the new claim
  and back off).

So node_id is the *vacant-seat tiebreaker*, never the continuous selector (which would flap on every
join). Best-effort + idempotent: a brief double-publish during failover is harmless (consumers key by
`mesh_id`, last-writer-wins).

## 5. Consumers

- **Gateways** route a cross-mesh write by resolving the target's `location` from the **backbone
  directory** — they NO LONGER join the other mesh's full gossip. (Removes the spurious cross-mesh
  membership edges.)
- **admin-ui** consumes the backbone for the **global view** (per-mesh aggregates + directory) and its
  **home mesh's** gossip for full per-node detail. It is a **normal node** (PRD/telemetry §B1) — no
  all-mesh subscription. Full per-node detail of a remote mesh = run an admin-ui in that mesh (UI per
  mesh). The backbone ties the views together.

## 6. What this removes
- `RAFKA_OBSERVER_MESHES = <all other meshes>` as the cross-mesh mechanism (PRD 00 §2). Nodes no longer
  full-subscribe to other meshes. (`RAFKA_OBSERVER_MESHES` may remain only for an explicit operator who
  wants a node to deep-observe a specific mesh — not the default cross-mesh path.)

## 7. Telemetry (CLAUDE.md §10, append-only)
- New spans: `rafka.mesh.backbone.published {mesh_id, node_count, publisher}` (publisher side),
  `rafka.mesh.backbone.received {mesh_id, publisher}` (consumer side). Add to §10 with the emit code.
- The aggregate metrics follow the §10 metric vocab as **per-mesh rollups** (new metric names,
  append-only, e.g. `rafka.mesh.aggregate.node_count{mesh_id}`).

## 8. Boundaries (so this is not the thing we reverted)
One iroh-gossip topic. No DHT, no self-organizing global cache, no consensus/Raft/Paxos, no fencing.
Control-plane *summaries* only; per-node detail never crosses a mesh. Relay stays the data plane.

## 9. Acceptance (sprint-14)
- A cross-mesh write resolves the target's location from the **backbone directory** with the gateway
  **NOT** subscribed to the target mesh's full gossip (verify: the gateway's `topic_membership` does not
  contain the remote mesh).
- Exactly **one** publisher per mesh under steady state (the lease holds); killing the publisher →
  another gateway takes over within ~TTL (failover), no permanent double-publish.
- admin-ui shows per-mesh aggregates + directory sourced from the backbone (not from all-mesh gossip).
- Scale check: with N gateways in a mesh, backbone publish rate stays ~1/mesh (not N/mesh).
- `rafka.mesh.backbone.published`/`received` spans visible in Jaeger; UI + Jaeger screenshots; lead
  verified.
