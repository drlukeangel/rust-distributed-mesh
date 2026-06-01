# Sprint-20 release — node-state propagation + fast convergence

**Status:** closed 2026-06-01 — all 8 verified; the T2 cross-mesh "broken" was root-caused to bidirectional
cross-seed connection churn and FIXED (commit `52e196f`): bidirectional cross-seed now shows both meshes.
· initiative mesh-v2 · branch `main`
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
| **T1 build** | ✅ `cargo check --workspace --tests --no-default-features` clean; web build clean; all 5 node binaries mtime-gated fresh. |
| **T2 fast-add (cross-mesh)** | ✅ both consoles show both meshes; backbone summaries flow both ways (c1 19 / c2 18 received). The transient "broken" was root-caused to **bidirectional cross-seed connection churn** and FIXED (commit `52e196f`, see below) — bidirectional cross-seed now works. |
| **T3 location-registration (intra-mesh)** | ✅ `Address Lookup failed` for real peers dropped 77 → **0 ongoing** (Console1 delta over 25s = 0; converged). Intra-mesh swarm forms fast with mDNS off. |
| **T4 Leaving** | ✅ Console2 (mesh2) killed `mesh1.broker.763a96` it does NOT own → `node.stopping reason="control_op"` → `tombstone.applied source="gossip_receive"` → `backbone.tombstone_applied`; evicted from **both** consoles in <0.1s; target process self-terminated. |
| **T5 Dead** | ✅ OS-killed the compute (no Leaving) → evicted via `tombstone.applied source="staleness_dead"` — **distinct** from Leaving's `gossip_receive`; gone from topology after the staleness window. |
| **T6 Degraded** | ✅ a broker forced over budget (`cpu 5.00/1.00`) self-reports **Degraded**, is **NOT** evicted, renders amber. |
| **T7 wire roundtrip** | ✅ `GossipDigest{state}` + `BackboneMessage` (incl. a `Degraded` directory entry) round-trip via postcard — unit test green. |
| **T8 UI states** | ✅ Playwright-captured (verify harness, 9 tabs × 2 consoles): both consoles render BOTH meshes; `mesh1.broker` shows the amber Degraded ring on c1 (home) AND c2 (cross-mesh via backbone). Artifacts: `docs/plans/mesh-v2/verify/screenshots/sprint20-node-state/`. |

**Bottom line:** all 8 verified. sprint-20's own mechanics (NodeState wire, intra-mesh fast convergence,
Leaving/Dead/Degraded semantics, wire roundtrip, render) plus cross-mesh visibility on both consoles. The
cross-mesh "broken" detour was root-caused to a **test-setup artifact** (bidirectional cross-seed connection
churn), FIXED in commit `52e196f` (bidirectional now works) — NOT the backbone design or the sprint-20 `state` field.

---

## Honest caveats / findings

- **RESOLVED — cross-mesh backbone neighborship (T2), root cause was the test setup:** the transient
  "broken" runs used **bidirectional cross-seeding** (c1 seeds c2 AND c2 seeds c1). That creates **two
  QUIC connections** between the same console pair, and the connection-supersede-close logic
  (`registry.remove(peer)` + `old_conn.close(...)` in the seed-dial / accept paths) tears down the
  connection carrying the iroh-gossip backbone neighbor. The live `iroh_gossip=debug` log showed it
  exactly: `NeighborUp(c2)` followed by `NeighborDown(c2)` 30 ms later inside a `conn{peer=61a66f}` span —
  the neighbor forms then the superseded connection drops it, so `MeshSummary` frames never flow.
  **Unidirectional seeding** (one console dials the other → single connection → no supersede) is stable:
  both consoles then show **both meshes** and summaries flow both ways (c1 19 / c2 18 received). This is
  NOT the backbone design and NOT the sprint-20 `state` field (both consoles run the same build; the T7
  postcard roundtrip is green). **FIXED** (commit `52e196f`): the supersede path no longer force-closes a
  superseded-but-live connection (all three registry-insert sites) — it adopts the newest for the data
  plane and lets the stale one idle-time-out, so a duplicate connection from bidirectional cross-seeding
  no longer drops the gossip neighbor. Verified: bidirectional cross-seed now shows BOTH meshes on both
  consoles (backbone.received c1 87 / c2 36, was 0); one-directional child->console dials are unaffected.

Both consoles showing both meshes — full NON-balanced fleet (unidirectional seed),
`broker.2d47fb` Degraded cross-mesh on both:

```
Console1 (mesh1, :19100)  meshes=[mesh1,mesh2]   Console2 (mesh2, :19101)  meshes=[mesh1,mesh2]
 mesh1.admin-ui.35a39c -> Alive   (home)           mesh1.admin-ui.35a39c -> Alive    (backbone)
 mesh1.broker.137df6   -> Alive   (home)           mesh1.broker.137df6   -> Alive    (backbone)
 mesh1.broker.2d47fb   -> Degraded(home)           mesh1.broker.2d47fb   -> Degraded (backbone)
 mesh1.gateway.e9463e  -> Alive   (home)           mesh1.gateway.e9463e  -> Alive    (backbone)
 mesh2.admin-ui.61a66f -> Alive   (backbone)       mesh2.admin-ui.61a66f -> Alive    (home)
 mesh2.broker.8a01b2   -> Alive   (backbone)       mesh2.broker.8a01b2   -> Alive    (home)
 mesh2.compute.5a9d8f  -> Alive   (backbone)       mesh2.compute.5a9d8f  -> Alive    (home)
 mesh2.gateway.98d261  -> Alive   (backbone)       mesh2.gateway.98d261  -> Alive    (home)
 mesh2.registry.8c09ef -> Alive   (backbone)       mesh2.registry.8c09ef -> Alive    (home)
```
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
