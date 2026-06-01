# PRD — Control plane + node-state propagation (mesh-v2)

**Status:** Open — architecture decisions, 2026-06-01 (sprint-19 shipped; sprint-20 in progress).
**Builds on:** the cross-mesh backbone (PRD `03`), per-mesh gossip + the sprint-15 tombstone, the relay (PRD `01`).
**Supersedes:** the admin-ui's "kill = OS `TerminateProcess` on the child it spawned" path, and the binary `GossipDigest.leaving: bool`.

---

## 0. Why

Three gaps surfaced operating the two-console cross-mesh setup:

1. **Killing a node was a central-orchestrator hack.** `admin-ui::kill_one` terminated the OS child it had spawned — so a console could only kill *its own* spawns. You could not kill a node you merely *saw* (another console's, another mesh's). That contradicts the self-aware-fleet model: which OS process owns a node must be irrelevant to who can operate on it.
2. **A mesh was invisible unless it had a gateway.** Only gateways published the mesh's `MeshSummary` to the backbone; the admin-ui was a non-publishing Observer. So a mesh whose only node was its console did not appear to other consoles. A node is a node — its mesh should advertise itself.
3. **Node *appearance* was slow and lopsided.** Removals were instant (the event-driven tombstone), but additions waited on periodic gossip + a fragile, slow swarm-join (≈40s, with `Address Lookup failed` retries because peer addresses weren't resolvable with mDNS off).

This PRD defines the control plane (how any console acts on any node) and node-state propagation (how a node's lifecycle is broadcast), and fixes the convergence latency.

---

## 1. Control-plane kill (sprint-19, shipped)

**A kill is a control op, not OS-process ownership.** Any mesh participant — including a console in another mesh — sends `InternalMeshFrame::Shutdown { reason }` (`op_kind="control"`) to a target it can address. The target handles it, shuts *itself* down gracefully (emits `node.stopping`, self-tombstones), and exits.

- The caller resolves the target's `node_id` + `location` from what it can SEE: its own gossip (`live_digests`), the cross-mesh backbone directory, or its spawn registry. No `Child` handle required.
- The killing console also broadcasts the backbone tombstone (cross-mesh eviction); the target self-tombstones on its own mesh gossip. `RAFKA_FORCE_RELAY`/`TerminateProcess` is no longer the kill mechanism (the OS-child reap remains an optional cleanup only when the console happens to own the process).
- **Verified:** a mesh1 console kills a mesh2 gateway it does not own — the gateway process dies and it evicts from both consoles immediately.

Implementation note (substrate): the sender must hold the QUIC connection open until the target reads the frame (`conn.closed()` with a cap) — dropping it immediately resets the stream before the target's `accept_bi` reads it.

---

## 2. The admin-ui publishes its mesh (sprint-19, shipped)

The admin-ui is a node in its mesh, so it is a **backbone publisher candidate** alongside gateways. The soft lease (`min(node_id)`, no preemption of a live holder) still elects exactly one publisher per mesh; all candidates aggregate the identical `MeshSummary` from `live_digests()`. Result: **every mesh advertises itself cross-mesh, even one whose only node is its console** — both consoles show both meshes regardless of gateways. `Role::Observer` now means only "does not run the data-plane write-sim," not "does not publish."

---

## 3. Node state as a published event (sprint-20)

Generalize the sprint-15 fast-delete tombstone into a node **state** published as an event on every transition. Replace `GossipDigest.leaving: bool` with:

```rust
pub enum NodeState {        // LOCKED, append-only (like op_kind / node_type)
    Joining,    // booting / joining, not yet serving
    Alive,      // up + serving
    Degraded,   // up but unhealthy (over budget, error rate)
    Updating,   // restarting / rolling — transient, do NOT evict
    Draining,   // graceful wind-down — finishing in-flight, no new work
    Leaving,    // graceful departure announced  -> evict (the old tombstone)
    Dead,       // crashed / vanished            -> evict
}
```

- **Self-published** (the node broadcasts its own lifecycle): `Joining, Alive, Degraded, Updating, Draining, Leaving`.
- **Observer-inferred** (assigned by peers when a node vanishes with no `Leaving`): `Dead` — this is the staleness/connection-loss "crash fallback", now a visible state rather than a silent removal.
- **Evict from the directory:** `Leaving` (self, immediate) + `Dead` (detected). `Degraded` / `Updating` / `Draining` stay, flagged and rendered.
- `broadcast_state(node_id, state)` replaces `broadcast_tombstone`; the receive path dispatches on state. **Every transition is an event-driven publish** (boot→`Joining`→`Alive`, →`Degraded`, →`Draining`→`Leaving`) plus an immediate backbone re-publish — additions/health-changes propagate as fast as deletions.
- UI renders state (e.g. green=Alive, amber=Degraded, blue=Updating, distinct Leaving vs Dead).

This is a **wire-format change** (`bool` → enum). Greenfield, so no back-compat is owed — but every node must be the *same fresh build* (see §5).

---

## 4. Fast convergence via `location`-based address registration (sprint-20)

The ~40s join latency + `Address Lookup failed` retries come from nodes not being able to resolve the address of a peer they learn about via gossip (mDNS is off; only seed addresses are known). The `GossipDigest` already carries `location` (the peer's dialable address). **On receiving a peer's digest / backbone directory entry, register its `location` with the iroh endpoint**, so `join_peers` connects directly — no discovery lookup, fast swarm formation.

- **mDNS stays off.** On a shared localhost mDNS discovers every rafka endpoint across meshes *and* unrelated test runs, contaminating the topology. `location`-registration is controlled (in-band, per-mesh) and **topology-independent** — it works same-subnet *and* across the relay, which mDNS (link-local multicast) cannot. A single-subnet production deployment MAY also enable mDNS as a bonus, but it is not required and is not the fix.

---

## 5. Operational invariant — fresh binaries (lesson, 2026-06-01)

Two Windows traps cost a full debugging session and produced false "this sprint is broken" conclusions:
1. A **running `.exe` is file-locked**, so `cargo build` while a node runs leaves a STALE binary (the build still reports success).
2. **The admin-ui spawns child nodes from `target/debug`**, not from where it's run and not from `--release`.

**Invariant:** before any live test or conclusion — kill all `rafka*` processes, rebuild, and gate that **every** binary you'll run/spawn (`target/debug/rafka-*`) has mtime > the changed sources. Never conclude "broken" from a binary you haven't freshness-gated. (Also: a `BackboneMessage`/`GossipDigest` postcard round-trip unit test catches wire-format skew before it reaches the wire.)

---

## 6. Acceptance (sprint-20)

See `docs/sprints/sprint-20/sprint-config.json`. Headline: fast-add appears on the other console in ~2s (not ~40s); 0 `Address Lookup failed` for in-mesh peers; `Leaving` evicts <2s; `Dead` is distinct from `Leaving` in the UI; `Degraded` renders without eviction; both consoles render an identical NON-balanced topology.
