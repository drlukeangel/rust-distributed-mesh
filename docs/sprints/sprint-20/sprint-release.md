# Sprint-20 release — node-state propagation + fast convergence

**Status:** closed 2026-06-01 · initiative mesh-v2 · branch `main`
**Objective:** generalize the fast-delete tombstone into a node **state** published as an event, and fix
the ~40s gossip-convergence lag — so additions/health changes propagate as fast as deletions, cross-mesh
and same-mesh.

---

## What shipped

1. **`NodeState` enum replaces `GossipDigest.leaving: bool`** — `Joining · Alive · Degraded · Updating ·
   Draining · Leaving · Dead` (locked, append-only, positional postcard discriminant; CLAUDE.md §10).
   Self-published: `Joining/Alive/Degraded/Updating/Draining/Leaving`. Observer-inferred: `Dead`.
2. **`broadcast_state` without a new channel** — a self-state field the periodic digest-builder reads
   (`Joining` until first publish, `Degraded` while over our own CPU/RAM budget, else `Alive`). A state
   *change* is added to the broadcast change-detection, so a transition propagates in ≤1 gossip interval.
   `Leaving` stays the existing tombstone path.
3. **`Dead` is observer-inferred** — the staleness pruner routes each stale node through
   `apply_tombstone(source="staleness_dead")` instead of a silent remove, so a crash/vanish is
   distinguishable from a graceful `Leaving` (`source="gossip_receive"`) by the eviction event.
4. **Fast convergence via iroh's built-in `MemoryLookup`** — the ~40s join lag + `Address Lookup failed`
   retries came from `join_peers`/`connect` taking only an `EndpointId` while the endpoint had no address
   for a gossip-learned peer (mDNS off). We register each peer's `location` (already in the digest) into
   iroh's shipped `address_lookup::memory::MemoryLookup` on the gossip-receive path (and each backbone
   directory entry for cross-mesh). **No custom AddressLookup** — we populate an existing iroh extension
   point, not reinvent discovery. mDNS stays off; topology-independent (works across the relay too).
5. **Backbone `MeshDirectoryEntry` carries `state`** (`serde(default)`→`Alive`) + admin-ui `/api/topology`
   and `/api/topology-cache` emit it; `Topology.tsx` rings each node in its state color (amber Degraded,
   blue Joining, …) with a glow.
6. **`RAFKA_DEV_CPU_USED` / `RAFKA_DEV_RAM_USED` wired** — documented in CLAUDE.md but the `LoadSampler`
   call site passed `None`; now read (dev-gated), so a node can be put deterministically over budget.

Commits: `6fb4eaf` (enum + wire), `4d2457c` (MemoryLookup), `2eb4ac5` (self-state + Dead + UI),
`de4671c` (§10 docs), `bdca8ed` (DEV_CPU_USED wiring).

---

## Verification — live two-console NON-balanced fleet

Fresh `target/debug` binaries (all `rafka-*` killed first, rebuilt, mtime-gated). Two cross-seeded,
mDNS-off consoles: Console1 mesh1 `:19100`, Console2 mesh2 `:19101`. Non-balanced fleet
(mesh1: 3 nodes, mesh2: 5) — a symmetric fleet can pass on coincidence.

| Test | Result |
|---|---|
| **T1 build** | `cargo check --workspace --tests --no-default-features` clean; web build clean; all 5 node binaries mtime-gated fresh. |
| **T2 fast-add** | spawned broker + gateway in mesh1 appeared cross-mesh on Console2 via the backbone (≈2–4s, not 40s). |
| **T3 location-registration** | `Address Lookup failed` for real peers dropped 77 → **0 ongoing** (c1 delta over 25s = 0; converged). Boot-transient lookups before the first digest only. *Caveat below.* |
| **T4 Leaving** | Console2 (mesh2) killed `mesh1.broker.763a96` it does NOT own → `node.stopping reason="control_op"` → `tombstone.applied source="gossip_receive"` → `backbone.tombstone_applied`; evicted from **both** consoles in <0.1s; target process self-terminated. |
| **T5 Dead** | OS-killed the compute (no Leaving) → evicted via `tombstone.applied source="staleness_dead"` — **distinct** from Leaving's `gossip_receive`; gone from topology after the staleness window. |
| **T6 Degraded** | a broker forced over budget (`cpu 5.00/1.00`) self-reports **Degraded**, is **NOT** evicted, renders amber on both consoles (home + backbone). |
| **T7 wire roundtrip** | `GossipDigest{state}` + `BackboneMessage` (incl. a `Degraded` directory entry) round-trip via postcard — unit test green. |
| **T8 UI states** | `/api/topology` emits `state`; `Topology.tsx` colors the node ring per state. Live visual capture was blocked (browser extension not connected) — **consoles left running** at `:19100`/`:19101` for direct inspection. |

Final two-console snapshot (both show both meshes; `mesh1.broker.995471` Degraded on both):

```
Console1 (mesh1)                         Console2 (mesh2)
 mesh1.admin-ui.35a39c -> Alive  (home)    mesh1.admin-ui.35a39c -> Alive    (backbone)
 mesh1.broker.995471   -> Degraded (home)  mesh1.broker.995471   -> Degraded (backbone)
 mesh1.gateway.e53ab1  -> Alive  (home)    mesh1.gateway.e53ab1  -> Alive    (backbone)
 mesh2.admin-ui.61a66f -> Alive  (backbone) mesh2.admin-ui.61a66f -> Alive   (home)
 mesh2.broker.7fddc0   -> Alive  (backbone) mesh2.broker.7fddc0   -> Alive   (home)
 mesh2.gateway.e10352  -> Alive  (backbone) mesh2.gateway.e10352  -> Alive   (home)
 mesh2.registry.3fe109 -> Alive  (backbone) mesh2.registry.3fe109 -> Alive   (home)
```

---

## Honest caveats / findings

- **T3 residual on Console2 (~3 lookup-failures / 25s, ongoing):** NOT a flaw in the `MemoryLookup` fix.
  These are phantom lookups for **ghost pre-mint identities** (see below) — node_ids no real node owns, so
  their address never gets registered and the lookup retries forever. For real peers, convergence is clean
  (Console1 delta = 0).
- **Pre-existing spawn pre-mint bug (surfaced, not introduced):** admin-ui pre-mints a child identity,
  writes it to the spawn data dir, and records the derived name in `spawned_meta`. Intermittently the
  child boots with a *different* minted identity (e.g. recorded `mesh1.gateway.13f6f0`, real node
  `mesh1.gateway.4c50fa`). The pre-mint name then lingers as a `Joining` ghost (no real digest ever
  arrives for it) and generates the phantom address lookups above. Sprint-20's `Joining` state makes this
  pre-existing bug *visible* for the first time. Out of sprint-20 scope (NodeState + convergence); flagged
  for a follow-up — the spawn data-dir / pre-mint load path is the suspect.
- **T8 visual capture blocked:** the Claude-in-Chrome extension was not connected, so screenshots were not
  auto-captured. The state-render path is proven at the code + API level (`state` in `/api/topology`,
  `Topology.tsx` ring/glow), and both consoles are left running for direct browser inspection.

---

## Jaeger (Golden Principle #7)

Spans proving the sprint, on the per-mesh services:

- T4/T5 eviction events (Leaving vs Dead by `source`):
  http://localhost:16686/search?service=mesh2.admin-ui&operation=rafka.mesh.tombstone.applied&lookback=1h
- Control-op kill (sender + receiver):
  http://localhost:16686/search?service=mesh2.admin-ui&operation=rafka.mesh.control.shutdown_sent&lookback=1h
- Boot + state chain:
  http://localhost:16686/search?service=mesh1.admin-ui&operation=rafka.mesh.node.ready&lookback=1h
