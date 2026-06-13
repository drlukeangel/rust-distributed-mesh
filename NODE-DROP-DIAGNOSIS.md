# Node-drop on soak — root cause + fix plan (2026-06-13)

## Symptom
Mesh should hold ~19±3 nodes; over the soak it collapses to ~3. Jaeger telemetry
(mesh1.broker `rafka.mesh.heartbeat`): per-node `peer_count` stays **low (0–6, never ~17)**
and individual nodes drop **3→1→0 abruptly and stay at 0 for 8+ min**. Abrupt-flatline-no-recovery
= a **discovery/rejoin** failure, NOT gradual starvation.

## Ruled out
- **Starvation:** the pure-iroh PoC (`E:\dev\iroh-poc`, `presets::Minimal`) scales to 100 nodes with no starvation. Load in the soak is trivial (~4/s).
- **Mailbox:** a real bug in iroh's code, but not the cause here.
- It is **our code**.

## Root cause
The mesh-transport endpoint fix (`e24dc87`, dedicated iroh runtime + `clear_address_lookup`)
**works** — it fixed connect-starvation. The node-drop is a **separate, pre-existing regression**:

The entity-cache **pull-from-peer re-hydration** (original design: `docs/architecture/Entity-Cache.md §4`
in `E:\dev\rafka` / `E:\dev\rafka-v2`) was **dropped when the topology cache was imported from the
topology-spike** (commit `9b4db1b feat(entity-cache): import topology-spike`). It was replaced with
`topology_cache::inject_birth_topology` — a **one-time file push** (`RAFKA_TOPOLOGY_BIRTH_FILE`) at boot.

Consequences — there is **no path to re-acquire the mesh view once lost**:
- `dial_seeds` is a **one-shot** boot task (`break`s after one successful connect; gives up after 10 fails).
- `run_gossip` subscribes with `Vec::new()` (no persistent bootstrap set).
- the tick-loop `join_peers` only joins peers **already in the registry**.
- `inject_birth_topology` only fires once at boot, only if the push-file exists.

So when a node's view collapses mid-soak (chaos suspend/resume, or connection drop → registry empties),
nothing re-bootstraps it → `peer_count` stuck at 0 → peers evict it after `DEFAULT_STALENESS_MS`=30s →
mesh erodes to the never-displaced **seed core (~3)**.

## Original design that was lost (reference impl in `E:\dev\rafka`)
`Entity-Cache.md §4`: **Cold** (first node / no peers → build fresh) / **Warm** (live mesh → pull snapshot
from a `cache_authoritative` peer) / **Hot** (on-disk snapshot + wall-clock validity check → reject stale to
avoid "zombie resurrections").

Wire protocol — `crates/rafka-topology/src/mesh_control.rs`:
- `EntityCacheSnapshotRequest { entity_kind, from_offset, request_id }`
- `EntityCacheSnapshotChunk { entity_kind, request_id, chunk_seq, entries, last_chunk }`
- `EntityCacheSnapshotNotReady { entity_kind, request_id, current_state }` ← donor-not-ready → requester falls back
- readiness-ping-before-request; `stream_snapshot` donor (`rafka-entity-cache/src/factory.rs`); `compute/src/boot_sequencer.rs::hydrate_compute_cache` (wait-for-peer → peer_snapshot, else broker_tail fallback).

## Fix (targeted — new-mesh is gossip-only, so port the principle, not the broker-log tailer)
On the **topology/GossipDigest path** (`live_digests()`), restore warm-pull re-hydration:
1. Snapshot **request/chunk/not-ready** messages (model on `EntityCacheSnapshotRequest`).
2. **Donor:** a peer/seed answers with `snapshot()` of its `live_digests`/topology, or `NotReady`.
3. **Requester + trigger:** on boot AND **on view-collapse** (`peer_count`→0 for N ticks), request from the
   seed; `hydrate_from` (re-acquires peer addresses → can rejoin gossip); fall back to cold/build-fresh if no peer/not-ready.
4. Keep `dial_seeds` re-dialing seeds when the seed connection drops (don't give up permanently).

### Files
- `crates/rafka-node-base/src/lib.rs` — `run_gossip` (view-collapse trigger), `dial_seeds` (persistent re-dial), the new snapshot req/resp handling.
- `crates/rafka-node-base/src/topology_cache.rs` — `inject_birth_topology` → add the pull path.
- new message variant on the gossip control path.

### Validate
**4-node kill-one repro** (advisor-prescribed): spawn 4, converge, kill one, **watch `peer_count` recover**.
Do NOT declare fixed on a compile — runtime is the judge (this session's repeated "compiles ≠ works" trap).

## Branch
Working clone: `E:\dev\rafka-mesh-nodefix` (cloned from `rafka-V2-new-mesh` @ branch `mailbox-fix`,
top commit `e24dc87`). Gemini's working dir `E:\dev\rafka-V2-new-mesh` left untouched.
