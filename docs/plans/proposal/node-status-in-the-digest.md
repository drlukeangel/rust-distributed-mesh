# The node's status in its Digest

**Status:** Q387 proposal, updated from `42cd260b8` through `5951a46bc`. This merges the supplied architecture
review and the payload/repair review into this one proposal, including Luke's subsequent
clarifications. Code and architecture pages change together at implementation.

## The design

1. **The final property is `status`.** Remove `node_state`, `NodeState`, `MemberStatus` and the
   duplicate status type/conversions; use the record's one status type. A node publishes
   `Digest { status }` on its own status change and on heartbeat. The envelope supplies its
   exact birth, sequence and time. Local receipt time measures silence, never deletion. Inside
   its mesh a node's status comes from its own Digest, ordered by its sequence, except at its own
   mesh-primary: "since the MP is the one creating/starting the node it gets a op streamed to it
   by the node" (Luke), so the mesh-primary takes the status from the node's reports and decides
   from them, never from gossip (Q197); its Digests give the mesh-primary liveness. Built at
   `25af643f2` (step 2 of #338), after a late, older Digest set a reported Ready back to Pending
   at mesh2's primary and its successor check refused a stop.
2. **Node details travel on the first gossip into the mesh.** That first publication carries
   the node details and pending at accepted join. Peers retain the details; subsequent
   Digests carry only status. Reads supply a joining node's existing view and repair anything
   missed. A new birth has its own first publication. Ready follows successful startup hooks.
3. **The mesh-primary sends the aggregate out and the differential in.** Its own mesh's
   `TopologyFull` goes on the backbone only. Onto its own mesh channel it publishes only
   peer-mesh additions, status changes, removals and applicable lifecycle facts, relative to
   its last publication. An addition carries node details; a later status change carries only
   member identity and status. No own-mesh member projection or peer-mesh full is sent there.

## Minimum payload and held view

| Path | Payload |
|---|---|
| First node gossip → own mesh | Node identity/endpoint details and status |
| Subsequent node gossip → own mesh | `Digest { status }` |
| Mesh-primary → backbone | Each member's identity, endpoints and status; frame-level open lifecycle operations and retained deletions |
| Mesh-primary → own mesh | Peer-mesh differential only; details on addition, identity and status on status change, identity on removal; applicable lifecycle facts |
| Directed read → node | Current view and ordering state needed to initialise or repair it |

An ordinary member holds the facts needed to resolve peers, their statuses, primary facts,
mesh/fabric statuses, applicable lifecycle/deletion facts and ordering state. Node-admins also
need runtime identity and management metadata for the births they may manage, obtained through
records and hydration before becoming ready. A mesh-primary additionally holds its own mesh's
aggregate and one last-published baseline per peer mesh/source publisher. Holding a fact does
not require receiving it on every heartbeat.

## Validated payload reductions

Checked [`MeshDigest` / `MeshNode`](../../../crates/rdm-core/src/digest.rs),
[`WireDigest` / `ProjectionEntry`](../../../crates/rdm-core/src/wire.rs),
[the envelope](../../../crates/rdm-gossip/src/envelope.rs) and
[the forwarder](../../../crates/rdm-gossip/src/snapshot.rs).

| Current content | Merged design |
|---|---|
| Author identity, `digest_seq`, `emitted_at_rafka_ms` repeated in the body | Use envelope `source`, `sequence`, `time`; retain described member identities inside aggregates/deltas |
| `node_state` alongside held `status` | Keep `status`; delete `node_state` and the `NodeState` enum |
| `mesh`, `mesh_id`, `type`, `path`, `name`, `endpoint_id`, `transport_addr`, `supersedes` | First publication and reads; retain locally, omit from subsequent status changes |
| `runtime`, `data_dir`, `admin_api_base` | Record/control metadata; keep available through the required admin/read paths, omit from recurring status gossip |
| `is_primary`, `is_fabric_primary` | Preserve the leadership workflow's role facts; remove repeated copies from recurring Digests |
| `load`, numeric `in_flight`, `gossip` | Remove from the minimum gossip payload; they are telemetry, not node status |
| Frame-level `in_flight`, `deleted` | Keep lifecycle operations and deletion retention; these are not the numeric work counter |

The current codec actually sends the duplicated envelope fields. Its agreement checks must
change with the smaller body. `TopologyDelta.changed` currently serializes an entire
`ProjectionEntry`; additions and status changes need separate payloads so a status change
stops resending node details. `without_load` already removes load, numeric work count and
gossip counters from peer forwarding.

**Gossip counters must never be persisted.** `heard`, `neighbours`, `frames_sent` and
`frames_received` are sampled in memory today. [`node.get`](../../../crates/rdm-admin/src/views.rs)
combines the stored node record with samples from the in-memory digest view.
[`NodeRecord`](../../../crates/rdm-node/src/nodes_storage.rs) has no counter fields. Keep them
out of records, storage writes and durable history.

Eight existing frame kinds remain: `Digest`, `TopologyFull`, `TopologyDelta`, `NodeStopped`,
`NodeDeleting`, `NodeDeleted`, `MeshStatus`, `FabricStatus`. The supplied review's count of
seven omitted `NodeStopped`; this proposal does not remove it.

## Repair and implementation checks

- Preserve accepted member sequence across reads and gossip. An older read cannot replace a
  newer self-authored status. Investigation, stop and deletion evidence stands;
  a late ready heartbeat cannot undo it. Subscribe before reading and install complete reads
  with their source versions atomically; coalesce repair requests.
- A delta with an unheld base triggers GetTopology repair. A missed final delta has no later
  base mismatch to reveal it; retain transport repair and a heal/read repair path.
- Replace every mesh-channel full fallback: first source, changed publisher, role acquisition,
  oversized delta, failed send and replay. Preserve bounded messages and source baselines.
  Removing topology-full replay must preserve mesh/fabric status repair, because those
  statuses are absent from `TopologyFull`.
- Keep node-admin runtime adoption working through record/read hydration when management
  metadata leaves the backbone projection. No completed Build history is needed.
- Preserve lineage validation when `supersedes` leaves aggregate entries. The current
  `LiveNodeResolver::apply` refuses a different incarnation without the held birth's lineage.
  Obtain that evidence from the first publication or a repair read before accepting the
  replacement; do not interpret an omitted field as permission to replace any birth.
- Verify first publication, missed first publication, late join/heal, read/gossip races,
  additions/status changes/removals, source-primary changes, failed/oversized deltas and
  deletion retention. Measure complete encoded frames, including the envelope.

## Remaining decisions

- **`Dead`:** removing `NodeState` is settled. Removing the separate TRUE OFFLINE mark is not;
  preserve its current behavior until that decision is made.
- **Telemetry reads:** the supplied review reports that Rafka samples load locally and does
  not use digest load for placement. RDM's `node.get` does consume digest load/counters today.
  After removal from gossip it must return no remote sample or obtain an explicit in-memory
  sample through a defined read path. Never solve this by persisting counters.
- **Record/role updates:** preserve the existing leadership update/read paths. Whether a live
  birth's `transport_addr` can change is unverified; `Applied::Updated` alone does not prove
  endpoint rebinding. Check the actual transport path before defining any new update trigger.
  This is not a requirement to repeat node details on every gossip.

Apply the wire codec, publishers, readers and architecture edits together in RDM and Rafka.
This reverses R-HB1 and the Pending publication rule in Q97/Q99. Mark the implementation
**durable format moved** and perform the full mesh cutover; no dual-format reader.

## Page text

The thirty architecture edits from `42cd260b8` and edits 31–36 from `5951a46bc`,
updated to the merged design above.
They are applied with the wire and consumer changes, not by this proposal edit.

**Shared wording for edits 6–9 and 13:**
“The mesh-primary publishes its own mesh's aggregate on the backbone only. Each member has
its identity, endpoints and status. Onto its own mesh channel it publishes only the peer-mesh
differential from its last publication: additions with node details and status, subsequent
status changes with member identity and status, removals with member identity, and applicable
lifecycle facts. It never republishes its own mesh's members or a peer mesh's full there.”

The frame's source/version, chunking, open operations and retained deletions remain. Member
identity is required inside an aggregate: the envelope identifies its publisher. Metadata
needed only for runtime management is obtained by node-admins through records and hydration.

### gossip.md

1. **§2 rules, a new line after** "- only receiving peer mesh primaries forward remote mesh topologies
   locally;"
   New: "- a mesh-primary forwards onto its own mesh channel only what it holds of peer meshes, never
   its own mesh's nodes, which publish themselves;"
2. **§3.1, the Digest frame**
   Old: "`Digest { node, incarnation, endpoints with freshness, node_state, runtime, digest_seq, emitted_at_rafka_ms }`"
   New: "`Digest { status }`"
3. **Digest row, author**
   Old: "every node, on its heartbeat timer"
   New: "every node, at once when its status changes and on its heartbeat timer"
4. **Digest row, identity / dedup**
   Old: "same exact birth: higher `digest_seq` is newer, equal or lower is not taken; a new incarnation
   is taken only along its lineage and only for a NodeId not accepted as `NodeDeleted`; its
   `digest_seq` starts again at 1"
   New: "the envelope's `source` is the birth and its `sequence` the birth's `digest_seq`: higher is
   newer, equal or lower is not taken; a new incarnation is taken only along its lineage and only for
   a NodeId not accepted as `NodeDeleted`; `sequence` starts again at 1"
5. **Digest row, use**
   Old: "that node's current identity, endpoints, liveness and runtime fact. `node_state` (Joining /
   Alive / Overloaded / Degraded, node-identity-and-state.md §4) is liveness only: a heartbeat never
   carries or moves a status, and a member's status comes only from its mesh-primary"
   New: "that node's status ([states](../../architecture/lifecycles/states.md)), as the node writes it on its own record
   (Q356), and nothing else; its arrival is its liveness. The first status a Digest carries is pending,
   from the node's accepted join ([node-mesh-join](../../architecture/lifecycles/node-mesh-join.md) step 10); a receiver
   never gives a birth a status it has not published. Inside its mesh, a status the node writes comes
   only from its own Digest; `PendingReconnect`, deleting and deleted are its mesh-primary's, on the
   node's record, and deleting and deleted ride `NodeDeleting` and `NodeDeleted`. Its identity,
   endpoints are sent with its first gossip into the mesh and retained by peers; subsequent
   Digests carry only status. A read repairs missed node details. Runtime management metadata
   is read from the node record"
6. **TopologyFull row, channel**
   Old: "backbone; and the source Mesh's primary's own mesh channel (its own member projection)"
   New: "backbone"
7. **TopologyFull row, use**
   Old: "In `digests`, each member is a projection entry `{ digest, status }`, the status authored by
   the mesh-primary alone. Own-mesh members receive the primary's statuses through the primary's
   versioned member projection."
   New: use the shared wording above for the aggregate's member entries.
   Old: "For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its
   Digest and status from the projection; NodeStopped and NodeDeleting stay immediate events."
   New: "An own-mesh birth supplies its node details on its first gossip and its status on
   subsequent Digests. Reads initialise and repair the held view; NodeStopped and NodeDeleting
   stay immediate events."
8. **TopologyDelta row, author**
   Old: "the local mesh primary, when a source Mesh it holds moved from one version to another; and
   the source Mesh's primary, for its own Mesh, on its own mesh channel"
   New: "the local mesh primary, when a peer Mesh it holds moved from one version to another"
9. **TopologyDelta row, use**
   Old: "In `changed`, each member is a projection entry `{ digest, status }`, the status authored by
   the mesh-primary alone. Own-mesh members receive the primary's statuses through the primary's
   versioned member projection."
   New: use the shared wording above for additions, status changes and removals. A status
   change no longer serializes a complete `ProjectionEntry`.
   Old: "For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its
   Digest and status from the projection; NodeStopped and NodeDeleting stay immediate events."
   New: remove this own-mesh explanation from the peer-only delta row; it is stated in edit 7.
10. **`NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving` row, author**
    Old: "none: connections-deleting and stopping reach peers as the member's status in its
    mesh-primary's projection (the `TopologyFull` and `TopologyDelta` rows above); node.connection.deleted
    is a reply on the call, never gossip"
    New: "none: connections-deleting, connections-deleted and stopping reach the node's own mesh in its
    own Digest, and peer meshes in its mesh-primary's `TopologyFull` and the deltas derived from it;
    node.connection.deleted is a reply on the call, never gossip"
11. **`NodeLeft` row, author**
    Old: "none: a stopping node gossips nothing and its mesh-primary publishes [NodeStopped](../../architecture/gossip.md#node-stopped-lifecycle-overlay)"
    New: "none: a stopping node's last Digest carries stopping, and its mesh-primary publishes [NodeStopped](../../architecture/gossip.md#node-stopped-lifecycle-overlay)"
12. **`NewFabricPrimary` row, author**
    Old: "none: a primary change rides the successor's `Digest` and the mesh-primary's `TopologyFull`
    projection, and every birth's contact addresses (`endpoint_id`, `transport_addr`) ride its `Digest`
    and `TopologyFull` projection"
    New: "none: this frame position remains reserved. Contact fields are supplied with the
    node's first gossip, retained in its projection and recoverable by read. Primary records
    remain governed by the leadership workflow; they are not inferred from `status`."
    Implementation check: preserve delivery of changed primary records when removing their
    repeated copies from Digests.
13. **Reserved lifecycle frame positions**
    Old: "connections-deleting, connections-deleted and stopping reach peers as the member's status in
    its mesh-primary's member projection (the `TopologyFull` and `TopologyDelta` rows of the catalog)"
    New: "connections-deleting, connections-deleted and stopping reach the node's own mesh in
    its Digest; their peer-mesh dissemination follows the shared wording above"
14. **§3.2 cadence**
    Old: "Digest heartbeat         every node; configurable cadence; never slower than 2 s"
    New: "Digest heartbeat         every node; configurable cadence; never slower than 2 s; and at once on the node's status change"
15. **§3.3 table**
    Old: "NODE -> OWN MESH            Digest heartbeat, full local facts, <= 2 s"
    New: "NODE -> OWN MESH            Digest: status, <= 2 s, and at once on its status change"
16. **§3.3, a new paragraph after that table**
    New: "A node's first gossip into its mesh supplies its node details and status; subsequent
    Digests carry status only. At create, join and start, a read supplies the joining node's
    existing view. A missed first publication, unknown birth or unmatched delta base requests
    a top-up. Subscribe before the read and preserve newer facts received during it. The
    mesh-primary does not gossip its own mesh's member projection. Removing topology-full
    neighbour replay does not remove mesh/fabric status replay."

31. **TopologyFull row, use, first sentence**
    Old: "the complete Mesh with loads: what every node-admin holds of every Mesh for routing and control decisions."
    New: "the complete Mesh: what every node-admin holds of every Mesh for routing and control decisions."
32. **§2 overview, the forwarding line**
    Old: "       as deltas backed by periodic full snapshots, loads omitted"
    New: "       as the differential of each peer Mesh from the last delta; no full on the mesh channel"
33. **§3.3, the paragraph before the table**
    Old: "The backbone stays the rich source: every node-admin holds every Mesh complete, loads included,
    because routing and control decisions are an admin's. Inside a Mesh, ordinary nodes need a remote
    Mesh's identity, endpoints with freshness, status, overlays and deletions, and not its loads; one"
    New: "The backbone stays the rich source: every node-admin holds every Mesh complete, because
    routing and control decisions are an admin's. Inside a Mesh, ordinary nodes need a remote Mesh's
    identity, endpoints, status, overlays and deletions; one"
34. **§3.3 table, two rows**
    Old: "MESH PRIMARY -> BACKBONE    TopologyFull, the complete Mesh with loads, versioned, anti-entropy + change"
    New: "MESH PRIMARY -> BACKBONE    TopologyFull, the complete Mesh, versioned, anti-entropy + change"
    Old: "PEER PRIMARY -> OWN MESH    TopologyDelta, base_version -> topology_version, no loads, on change"
    New: "PEER PRIMARY -> OWN MESH    TopologyDelta, the differential from the last delta, base_version -> topology_version, on change"
35. **§3.1 frame examples, the `TopologyFull` example's one `digests` entry**
    Old:
    ```json
    {
      "fabric_id": "fabric1",
      "node": {
        "node_id": "k8pag1qx95gw",
        "name": "mesh1.node.1",
        "endpoint_id": "7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a",
        "transport_addr": "127.0.0.1:54473",
        "incarnation": "0000000000000008",
        "supersedes": null,
        "runtime": null
      },
      "status": "ready",
      "admin_api_base": null,
      "digest_seq": 42,
      "emitted_at_rafka_ms": 1718290000050,
      "data_dir": null,
      "mesh_id": "mesh1",
      "in_flight": null,
      "load": null,
      "gossip": null
    }
    ```
    New:
    ```json
    {
      "node": {
        "node_id": "k8pag1qx95gw",
        "mesh": "mesh1",
        "type": "node",
        "path": "mesh1.node",
        "name": "1",
        "endpoint_id": "7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a",
        "transport_addr": "127.0.0.1:54473",
        "incarnation": "000000000008"
      },
      "status": "ready"
    }
    ```
    The new example uses a canonical 12-character incarnation. The recurring `Digest`
    example becomes `{ "status": "ready" }` under its envelope. Its first publication
    additionally supplies node details, as rule 2 states.

### lifecycles/node-mesh-join.md

17. **Completion row**
    Old: "the first `Digest` heartbeat follows it; the mesh-primary's one Pending publish for this join
    came before it, when the report was accepted (step 10)"
    New: "the node's first gossip carried its node details and pending when it wrote pending
    (step 10); its subsequent heartbeats carry status only"
18. **Step 9**
    Old: "9. `node.mesh.joined`. The node's first `Digest` heartbeat follows."
    New: "9. `node.mesh.joined`. The node's heartbeats follow its pending Digest (step 10)."
19. **Step 10**
    Old: "publishes the node once as Pending when it accepts the node's join report, before it answers
    it, and the node writes pending on its own record (Q356)."
    New: "records the node pending when it accepts the node's join report, before it answers it; the
    node writes pending on its own record (Q356) and publishes its first gossip at once,
    carrying its node details and pending. Subsequent Digests carry status only."
    Old: "and starts no other; the node publishes no Pending of its own."
    New: "and starts no other."
    Old: "and is published ready only once its hook has passed"
    New: "and publishes ready only once its hook has passed"
20. **Sequence diagram, mesh-primary note**
    Old: "10. publishes the node once as Pending when it accepts the node's join report, before it answers it"
    New: "10. records the node pending when it accepts the node's join report, before it answers it"
21. **Checked against**
    Old: "the Pending publish from the mesh-primary, never from the node."
    New: "pending published by the node with its first gossip; the mesh-primary records it."

### node-identity-and-state.md

22. **Scope, line 5–6**
    Old: "the node, mesh and fabric rows and who writes each, the `NodeState` liveness enum, the
    hydration roster"
    New: "the node, mesh and fabric rows and who writes each, the hydration roster"
23. **§1 rule 2**
    Old: "**Aliveness and readiness are separate axes** (§4): readiness is the status a node's row
    holds, liveness the `NodeState` peers observe from its digests (§4)."
    New: "**Liveness and status are separate axes** (§4): the status is on a node's row and in its
    Digest; liveness is its Digests arriving, read on the observer's own clock."
24. **§1 rule 3**
    Old: "Local NodeState::Dead/stale vocabulary"
    New: "Local `Dead`/stale vocabulary"
25. **§1 rule 5**
    Old: "`GossipDigest`, `GossipEnvelope`, `NodeState` and `NodeDigest` are postcard-positional."
    New: "`GossipDigest`, `GossipEnvelope` and `NodeDigest` are postcard-positional."
26. **§4, from "A node has TWO kinds of state" through the Self-publication bullet**
    New: "The node's final property is **`status`**. The `node_state` property and `NodeState`
    type are removed. Its lifecycle status values are starting, pending, ready, connections-deleting,
    connections-deleted, leaving, left, stopping, stopped, deleting and deleted, each set by its
    workflow as [States](../../architecture/lifecycles/states.md#states) and
    [node status](../../architecture/lifecycles/node-status.md#node-status-change) define, and `PendingReconnect`, the
    status of a node under [node investigation](../../architecture/lifecycles/node-investigation.md). The node writes
    its status on its own record (Q356) and publishes it in its Digest at once when it changes and on
    every heartbeat ([gossip](../../architecture/gossip.md#31-frame-catalog)); its mesh-primary writes
    `PendingReconnect`, deleting and deleted. A stopping node's last Digest carries stopping, and its
    mesh-primary publishes `NodeStopped` from the stop stream's `node.stopped` frame."
    (The "Shutdown is signal-driven" bullet stays.)
    Add to §2.5: "Gossip counters (`heard`, `neighbours`, `frames_sent`, `frames_received`) are
    in-memory diagnostics only. Never write them to the node record, storage or durable history.
    A `node.get` response may combine the stored record with an available in-memory sample;
    exposing a counter in that response does not persist it."
27. **§4.1 rows**
    Delete the `Joining`, `Alive`, `Overloaded` and `Degraded` rows; add:
    "| pending | the node, at its accepted join ([node-mesh-join](../../architecture/lifecycles/node-mesh-join.md) step 10) | the traffic/topology filters — routed no traffic |"
    "| ready | the node, at its node.started | the rebind path; every traffic and topology filter |"
    Old: "| `Leaving` | set by the mesh-primary on `node.stopped` (overlay `NodeStopped`) | the routing rule: routed no traffic |"
    New: "| leaving, stopping, stopped | the node, on its own record ([States](../../architecture/lifecycles/states.md#who-sets-each-state)) | the routing rule: routed no traffic |"
    The `Dead` and `PendingReconnect` rows stay.
28. **§4.2**
    Old: "`NodeState` (`rdm-core/src/digest.rs`) and a node's status (its row, §1 rule 2) are separate
    state families that never share an enum or a type; an application's own entity lifecycle is a
    third, the application's."
    New: "A node's status is its one state family; an application's own entity lifecycle is the
    second, the application's."

36. **§2.5 table, the gossip row**
    Old: "| per-heartbeat liveness — `cpu_used`, `ram_used`, reachability | gossip, in memory only | every node, about itself |"
    New: "| per-heartbeat liveness: the node's Digest arriving, carrying its status | gossip, in memory only | every node, about itself |"

### lifecycles/states.md

29. **Line 14**
    Old: "- **Node row silence marks:** a node row may carry the observer-inferred marks
    `PendingReconnect` and `Dead` (TRUE OFFLINE), written by the node's mesh-primary. They are never
    themselves a reason to act ([node recovery](../../architecture/lifecycles/node-recovery.md))."
    New: "- **Node under investigation:** `PendingReconnect` is the status of a node under
    [node investigation](../../architecture/lifecycles/node-investigation.md), written on its record by its mesh-primary
    ([node recovery](../../architecture/lifecycles/node-recovery.md) step 1); the verdict ends it. A node row may also carry the
    observer-inferred mark `Dead` (TRUE OFFLINE), written by the node's mesh-primary, never itself a
    reason to act."

### fabric-mesh-node-lifecycles.md

30. **Index row, Node identity and state**
    Old: "the NodeState liveness enum, "
    New: (removed)
