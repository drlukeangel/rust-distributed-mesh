# Node lifecycle & state management

**One sentence:** every node has a `NodeState`, it **publishes that state**, and every observer
(other nodes *and* the admin-ui) **acts on the received state directly** — render it, or evict on a
terminal state. There is one path, and it maps **1:1 to what the UI shows**.

This document describes the model, the states, how transitions happen, where the state is used, and the
telemetry that proves it. It is the canonical description after the *tombstone* subsystem was removed
(2026-06-01) — "tombstone" was a Kafka import; a departure is just the `Leaving`/`Dead` **state**.

---

## The model

```
        a node                         every observer (nodes + admin-ui)
   ┌──────────────┐   GossipDigest    ┌────────────────────────────────┐
   │  NodeState   │ ───{state,…}────▶ │  received state →               │
   │  (publishes) │   (gossip / the   │   • Leaving/Dead → evict        │
   └──────────────┘    backbone)      │   • anything else → upsert+render (1:1 UI) │
                                       └────────────────────────────────┘
```

- A node's state rides its `GossipDigest` (the same heartbeat that carries peers, load, location).
- **On a state change, the digest is published immediately** — it does not wait for the next periodic
  tick. (Non-terminal changes force the next broadcast; a departing node triggers `publish_now` so its
  `Leaving` goes out before it exits.)
- Observers apply the received state **1:1**: a terminal state (`Leaving`/`Dead`) evicts the node from
  `live_digests` (`evict_node`); any other state upserts the digest. The admin-ui reads `live_digests`
  (+ the cross-mesh backbone directory) and renders the state — so **a node's state change is a UI
  update**. This 1:1 mapping is a primary reason the state model exists.
- **No separate "tombstone" mechanism, no third-party broadcast.** A node only ever publishes its *own*
  state. A console killing a node does not hand-edit that node's presence — it asks the node to leave,
  and then acts on the `Leaving` state it observes like everyone else.

---

## The states (`NodeState`, locked + append-only — CLAUDE.md §10)

| State | Set by | Meaning | Effect on observers |
|---|---|---|---|
| `Joining` | self (boot, until first publish) | booting / joining, not yet serving | render (blue) |
| `Alive` | self (auto) | up + serving | render (type color) |
| `Degraded` | self (auto: over its own CPU/RAM budget) | up but unhealthy | render (amber), **not** evicted |
| `Updating` | operator (`SetState`) | rolling / restarting | render (purple), **not** evicted |
| `Draining` | operator (`SetState`) | winding down, finishing in-flight | render (orange), **not** evicted |
| `Leaving` | self (graceful shutdown) | announced departure | **evict** |
| `Dead` | observer-inferred (staleness) | crashed / vanished with no `Leaving` | **evict** |

- **Self-published:** `Joining, Alive, Degraded, Updating, Draining, Leaving`.
- **Observer-inferred:** `Dead` — a crashed node announces nothing, so the staleness pruner assigns it.
- **Evict-from-directory:** `Leaving` (graceful) + `Dead` (crash). The two are distinguished by the
  `evict_node` `source` attribute (`gossip_receive` vs `staleness_dead`) — "left cleanly" vs "crashed."

The enum is **positional** on the wire (postcard discriminant): new variants append at the END, never
reorder/remove/repurpose.

---

## How each transition happens

- **Joining → Alive:** automatic. A node is `Joining` until it publishes its first digest, then `Alive`.
- **Alive ↔ Degraded:** automatic health. The node compares its own `cpu_used/ram_used` to its budget each
  digest; over budget → `Degraded`, back under → `Alive`. (`RAFKA_DEV_CPU_USED`/`_RAM_USED` force it for
  tests.)
- **→ Updating / → Draining:** operator command. Any console sends `InternalMeshFrame::SetState{state}`
  (`POST /api/nodes/{name}/state`, or the Nodes-tab **drain/upd/resume** buttons). The node stores it as a
  self-state override its next digest publishes. `Alive` clears the override (resume).
- **→ Leaving:** graceful shutdown. A control-op kill (`InternalMeshFrame::Shutdown`) makes the target set
  its own `state = Leaving` and `publish_now` (immediate), then exit. Observers evict on the `Leaving`
  digest. **The killer does not broadcast on the target's behalf.**
- **→ Dead:** crash. No announcement; the staleness pruner (`RAFKA_STALENESS_MS`, sweeps every 5s) evicts
  a node whose last digest is older than the window, with `source="staleness_dead"`.

A small **resurrection guard** (`recently_evicted`, `EVICTION_GUARD_MS`) drops a stale in-flight digest
that arrives just after an eviction, so an older `Alive` can't re-add a node that just left. This is
state-receive correctness, not a separate subsystem.

---

## Where the state is used

1. **The admin-ui (the 1:1 surface).** `/api/topology` and `/api/topology-cache` emit each node's `state`;
   `Topology.tsx` rings the node in its state color (amber Degraded, blue Joining, purple Updating, orange
   Draining) with a glow; terminal states are absent (evicted). A node's state change → the next poll
   reflects it. The Nodes tab issues `SetState` (drain/upd/resume) + the kill.
2. **Eviction.** `evict_node` is the single chokepoint — the receive path (terminal `Leaving`/`Dead`) and
   the staleness pruner both funnel through it. It clears `live_digests` + `topic_membership` +
   `last_seen_ms` and records the resurrection guard.
3. **Cross-mesh.** The backbone `MeshSummary` directory carries each node's `state`, so a remote console
   renders it too. A departed node simply drops out of its mesh's next `MeshSummary` — cross-mesh eviction
   needs no separate message.

---

## Telemetry (the proof)

| Span | When | Key attributes |
|---|---|---|
| `rafka.mesh.node.state_changed` | every self-state transition | `node_id`, `node_name`, `from`, `to`, `source="self"` |
| `rafka.mesh.node.evicted` | first eviction of a node (terminal state) | `node_id`, `source` (`gossip_receive` \| `staleness_dead`) |
| `rafka.mesh.control.state_change_sent` / `_received` | a `SetState` op (Updating/Draining/resume) | `node_id`, `peer_id`, `state`, `accepted` |
| `rafka.mesh.control.shutdown_sent` / `_received` | a kill (control op → `Leaving`) | `node_id`, `peer_id`, `reason` |

A node's full lifecycle is therefore a queryable Jaeger trace: `node.state_changed`
`Joining→Alive→Updating→Draining`, then a `node.evicted` (Leaving or Dead). Filter by service
(`<mesh>.<type>`) or by `node_id`.

---

## What was removed (and why)

The pre-generalization "tombstone" path (`broadcast_tombstone`, `apply_tombstone`, the manufactured
`Leaving` digest, the `TOMBSTONE_TX` outbox, and the whole cross-mesh backbone tombstone —
`BackboneMessage::Tombstone`, `BACKBONE_TOMBSTONE_TX`, `broadcast_backbone_tombstone`) was a **parallel
subsystem doing what publishing-state already does.** A departure is a state; observers already act on
received state. Collapsing it leaves one mechanism, named for what it is (`evict_node`,
`node.state_changed`, `node.evicted`), with the fast path preserved (immediate publish on change) and the
correctness preserved (resurrection guard). See the build-log post *"Kill by message, not by ownership"*
for the lineage.
