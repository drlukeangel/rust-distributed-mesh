# The node's status in its Digest

**Status:** proposal, open as Q387. Nothing here is built or applied. The architecture pages change
with the code only after Luke's yes on the exact old and new text in [Page text](#page-text). It
reverses R-HB1 ([node-status-heartbeat-connections](node-status-heartbeat-connections.md), Luke's
direction of 2026-10-11: "the mesh-primary authors and publishes every member status; a heartbeat is
liveness only") and the Pending point of Q97 and Q99 (the mesh-primary publishes a joining node's
Pending once, at the accepted join report).

Luke, 2026-11-06 and 2026-11-07, in his words:

- "the node publishes its own gossip when its state changes to its local mesh and since the MP is
  the one creating/starting the node it gets a op streamed to it by the node."
- "the MP does publish the agregrate but to the backbone and the differential of peer meshses to the
  mesh this shoul be in gossip.md"
- "isn't the author in envelope"
- "it would only reach fr teh MP proejction on create/join/start or direct pull on a sync issue"
- "teh MP should only be sending digest from peer meshes it shouldn't be sending digest goissp from
  teh internal mesh"
- "status is the field node_state is why i told you to mreove"; "nodestate is dead / to be cleaer its
  status"
- "ALSO NODE"; "endpoints should also not be in the digest"; "and digest_seq and emited at time?"
- "pending is the status its been the status / its the status before ready"
- "pending-reconnect is the status of node-under investigation"
- "the different frmo the bcakbone gossip of hte topology peeer mesh is MP just publishes the
  differential from the last status"

## Why

- **One status reaches a node from two sources.** A compute held a ready broker `Pending` for 34 s
  (D68, rdm#337): the broker's own Digest and its mesh-primary's projection both carry the broker's
  status, under two `(source, subject)` pairs ordered by two different keys (`digest_seq`,
  `topology_version`) that cannot be ordered against each other. `e805dad46` made a topology read
  move the status; the two sources remain.
- **One status has two fields.** The code's held Digest has `status: MemberStatus` and
  `node_state: NodeState` side by side (`MeshDigest`). The sender turns the status into `node_state`
  (`NodeState::self_published`: Pending → Joining; Ready, ConnectionsDeleting, Leaving → Alive) and
  sends the Digest with its `status` taken off; the receiver puts `status: MemberStatus::Pending` back
  on every heartbeat and takes the status from the projection (`DigestView::hand_out`,
  `take_projected_status`). `Overloaded` and `Degraded` are never set (`self_published`'s own doc).
  node-identity-and-state.md §4.2 writes the split in as a rule.
- **One status has three types.** `MemberStatus` (Pending, Ready, ConnectionsDeleting, Leaving), and
  two different enums both named `NodeStatus`, one in `rdm_core::model` and one in `rdm_core::views`,
  the second with states.md's whole set.
- **The projection puts the status back beside the Digest.** A projection entry is the Digest without
  its status, then the status the mesh-primary holds (`ProjectionEntry`).
- **The Digest repeats the envelope.** `node` and `incarnation` are the envelope's `source`
  (`rdm://<fabric_id>/<node_id>/<incarnation>`), `digest_seq` is its `sequence` and
  `emitted_at_rafka_ms` its `time` (gossip.md §3.4).
- **The Digest carries the record.** Endpoints, runtime, `admin_api_base` and `data_dir` are the
  node's record (node-identity-and-state.md §2.5: "identity (`id`, `endpoint_id`, `name`, `type`) and
  STATE CHANGES | the node record"); gossip is for "per-heartbeat liveness — `cpu_used`, `ram_used`,
  reachability | gossip, in memory only".
- **The mesh-primary republishes its own mesh on its own mesh channel.** The forwarder builds its own
  mesh's delta as it builds a peer mesh's, and neighbour-up replays its own mesh's full
  (`ForwardPath::replay`), as the TopologyFull and TopologyDelta rows say today.

## The rules it rests on

- **Q356.** A node sets its own status on its own record (states.md, "Who sets each state").
- **gossip.md §3.4.** `source` is the author's exact birth; `sequence` the per-`(source, subject)`
  version, a birth's `digest_seq`; `time` the rafka-time of the fact.
- **gossip.md §3.3.** "The same top-up serves a join, a restart, a detected gap, a source revision
  change and lost local state."
- **node-identity-and-state.md §2.5.** The durable and in-memory split quoted above.
- **node-identity-and-state.md §1 rule 5.** A gossip struct is durable; a new field is a hard
  cutover: "the commit says 'durable format moved' and every live mesh is taken down and deployed
  again, never redeployed in place". This change is one such cutover; no executable reads both
  shapes.
- **Q197.** Nothing waits for gossip.

## The design

- **R-DS1: one status.** `NodeState` is removed. The Digest's field is `status`, with states.md's
  values: starting, pending, ready, connections-deleting, connections-deleted, leaving, left,
  stopping, stopped, deleting, deleted, and `PendingReconnect`, the status of a node under
  [node investigation](../../architecture/lifecycles/node-investigation.md). It has one type, the
  record's; `MemberStatus` and the second `NodeStatus` are removed. Everything that carries a node
  carries its status: its Digest, a projection entry, the start read and the top-up. No shape of a
  node is sent without it.
- **R-DS2: the Digest is `Digest { status }`.** The author is the envelope's `source`, the order
  its `sequence` (the birth's `digest_seq`) and the time its `time`. Identity, endpoints, runtime,
  `admin_api_base` and `data_dir` are on the record and never in a Digest. The Digest carries the
  status and nothing else (Open item 4).
- **R-DS3: published at once on a status change.** A node publishes its Digest when it writes a new
  status on its own record, with the next `sequence`, and on every heartbeat. Its first status in a
  Digest is pending, at its accepted join (node-mesh-join step 10): it is not on its mesh channel
  before that. A receiver never gives a birth a status that birth has not published.
- **R-DS4: inside its mesh, a node's own status comes only from its own Digest.** The statuses its
  mesh-primary writes, `PendingReconnect`, deleting and deleted, are on the node's record; deleting
  and deleted ride `NodeDeleting` and `NodeDeleted` as today.
- **R-DS5: the mesh-primary forwards only peer meshes, and only the differential.** It publishes its
  mesh's `TopologyFull`, the aggregate, on the backbone only. Onto its own mesh channel it publishes,
  for each peer mesh, only the differential from the last status it published of that mesh: the
  nodes whose status changed, were added or were removed since, each a projection entry (R-DS7), no
  loads (`TopologyDelta`). It never publishes its own mesh's nodes there, which publish themselves,
  and never a peer mesh's full: neighbour-up on the mesh channel replays nothing, since a joining node
  takes its view by its read (R-DS6); the backbone's replay of fulls is unchanged.
- **R-DS6: the mesh-primary's view of its own mesh reaches a node only by a read.** At create, join
  and start (the start read) and on a sync issue (the top-up). A Digest from a birth the receiver
  does not hold is a sync issue: the receiver has no endpoints for it until the read.
- **R-DS7: a projection entry is the node's record fields with its status.**
  `ProjectionEntry` becomes the record's identity and endpoint fields (today's `MeshNode`) and the
  node's latest Digest, its status, as the mesh-primary holds it. `MeshDigest` becomes
  the one `Digest`, sent whole; nothing is taken off it on the way out or put back on the way in.

### Open, for Luke

1. **`is_primary` and `is_fabric_primary`.** The code's Digest carries both, the mark the
   mesh-primary sets, and gossip.md's `NewFabricPrimary` row says "a primary change rides the
   successor's `Digest` and the mesh-primary's `TopologyFull` projection". R-DS2 takes them out of
   the Digest; a primary change then reaches the nodes of the mesh only by the record and a read
   (R-DS6). That is election text (no election change without Luke).
2. **An address that moves within one birth.** "endpoints with freshness" today follow a node's
   addresses on every heartbeat. Under R-DS2 a moved address is written on the record and reaches
   the nodes of its mesh only by a read.
3. **`Dead`.** states.md line 14 has `Dead` (TRUE OFFLINE) written on the row by the mesh-primary;
   node-identity-and-state.md §4 has it "observer-inferred ONLY (never sent)". This proposal leaves
   both lines as they are.
4. **The load leaves the Digest.** Today's Digest carries `load` (`cpu_used`, `ram_used`) and the
   gossip stats, and node-identity-and-state.md §2.5 puts "per-heartbeat liveness — `cpu_used`,
   `ram_used`, reachability" in gossip. `TopologyFull` is "the complete Mesh with loads", read for
   routing and control decisions, and `node.get` reads the load. With the Digest carrying only the
   status, where the load goes is open.

## RDM

- `rdm-core`: `NodeState`, `NodeState::self_published`, `MemberStatus` and the second `NodeStatus`
  are removed; `MeshDigest` becomes `Digest { status }`, sent whole (the status-less sent shape
  and its two conversions go); `ProjectionEntry` per R-DS7. The commit says "durable format moved".
- `rdm-gossip`: `Channels` publishes the Digest at once on a status write and on the heartbeat, with
  the envelope's `source`, `sequence` and `time` filled from the birth, its `digest_seq` and
  rafka-time. `DigestView` takes an own-mesh node's status from its Digest only (`hand_out`'s
  override and `take_projected_status` go); a Digest from a birth not held starts the top-up. The
  forwarder takes peer meshes only; `ForwardPath::replay` replays peer meshes only.
- `rdm-admin`: the mesh-primary stops publishing its own mesh's full and deltas on its own mesh
  channel; the join answer no longer publishes the node's Pending (the node does, R-DS3).
- `rdm-node`: a status write on the node's own record publishes its Digest.
- Cells: the 34 s order of rdm#337 (a read and a Digest for one birth in both orders) ends with the
  Digest's status; a mesh-primary's own mesh channel carries no frame about its own mesh's nodes but
  their own Digests; a status change reaches every node of its mesh within one heartbeat with no
  read.
- Removal debt (PRD row): R-HB1's text in node-status-heartbeat-connections.md; `e805dad46`'s read
  status move in `apply_full`, replaced by R-DS4.

## Rafka

- Rafka builds held Digests with `node_state: Default::default()` (`rdm_topology`, a deleting view
  in `rafka-node-base`, `test_view` in `rafka-writer-broker`): those field sets go.
- `rafka-node-base` has its own `NodeState` enum with `is_in_service`, which nothing calls: removed.
- Rafka's `MemberStatus::` matches (about 50, outside tests) move to the one status type.
- Rafka reads a node's `endpoint_id` from the held view by path and name (`node.endpoint_id`): the
  held entry keeps the record's fields (R-DS7), so those reads stay.
- Rafka reads its own primary role from "its own digest ... carries `is_primary`" (`rafka-node-base`):
  follows Open item 1.
- Rafka takes the pin with the "durable format moved" cutover: every live mesh is taken down and
  deployed again.

## Page text

Exact old and new text. Nothing is applied before Luke's yes.

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
   New: "that node's status ([states](lifecycles/states.md)), as the node writes it on its own record
   (Q356), and nothing else; its arrival is its liveness. The first status a Digest carries is pending,
   from the node's accepted join ([node-mesh-join](lifecycles/node-mesh-join.md) step 10); a receiver
   never gives a birth a status it has not published. Inside its mesh, a status the node writes comes
   only from its own Digest; `PendingReconnect`, deleting and deleted are its mesh-primary's, on the
   node's record, and deleting and deleted ride `NodeDeleting` and `NodeDeleted`. Its identity,
   endpoints and runtime are on its record (node-identity-and-state.md §2.5), never in a Digest"
6. **TopologyFull row, channel**
   Old: "backbone; and the source Mesh's primary's own mesh channel (its own member projection)"
   New: "backbone"
7. **TopologyFull row, use**
   Old: "In `digests`, each member is a projection entry `{ digest, status }`, the status authored by
   the mesh-primary alone. Own-mesh members receive the primary's statuses through the primary's
   versioned member projection."
   New: "In `digests`, each node is a projection entry: its record's identity and endpoints, and the
   status of its latest Digest as the mesh-primary holds it."
   Old: "For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its
   Digest and status from the projection; NodeStopped and NodeDeleting stay immediate events."
   New: "For an own-mesh birth, its status comes from its Digest and its identity and
   endpoints from a read (§3.3); NodeStopped and NodeDeleting stay immediate events."
8. **TopologyDelta row, author**
   Old: "the local mesh primary, when a source Mesh it holds moved from one version to another; and
   the source Mesh's primary, for its own Mesh, on its own mesh channel"
   New: "the local mesh primary, when a peer Mesh it holds moved from one version to another"
9. **TopologyDelta row, use**
   Old: "In `changed`, each member is a projection entry `{ digest, status }`, the status authored by
   the mesh-primary alone. Own-mesh members receive the primary's statuses through the primary's
   versioned member projection."
   New: "In `changed`, each node of the peer Mesh whose status changed, or that was added, since the
   forwarder's last delta of that Mesh, a projection entry as in `TopologyFull`: the differential from
   the last status, never the aggregate."
   Old: "For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its
   Digest and status from the projection; NodeStopped and NodeDeleting stay immediate events."
   New: "For an own-mesh birth, its status comes from its Digest and its identity and
   endpoints from a read (§3.3); NodeStopped and NodeDeleting stay immediate events."
10. **`NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving` row, author**
    Old: "none: connections-deleting and stopping reach peers as the member's status in its
    mesh-primary's projection (the `TopologyFull` and `TopologyDelta` rows above); node.connection.deleted
    is a reply on the call, never gossip"
    New: "none: connections-deleting, connections-deleted and stopping reach the node's own mesh in its
    own Digest, and peer meshes in its mesh-primary's `TopologyFull` and the deltas derived from it;
    node.connection.deleted is a reply on the call, never gossip"
11. **`NodeLeft` row, author**
    Old: "none: a stopping node gossips nothing and its mesh-primary publishes [NodeStopped](#node-stopped-lifecycle-overlay)"
    New: "none: a stopping node's last Digest carries stopping, and its mesh-primary publishes [NodeStopped](#node-stopped-lifecycle-overlay)"
12. **`NewFabricPrimary` row, author**
    Old: "none: a primary change rides the successor's `Digest` and the mesh-primary's `TopologyFull`
    projection, and every birth's contact addresses (`endpoint_id`, `transport_addr`) ride its `Digest`
    and `TopologyFull` projection"
    New: (follows Open item 1) "…, and every birth's contact addresses (`endpoint_id`,
    `transport_addr`) are on its record and ride the `TopologyFull` projection"
13. **Reserved lifecycle frame positions**
    Old: "connections-deleting, connections-deleted and stopping reach peers as the member's status in
    its mesh-primary's member projection (the `TopologyFull` and `TopologyDelta` rows of the catalog)"
    New: "connections-deleting, connections-deleted and stopping reach the node's own mesh in its own
    Digest, and peer meshes in its mesh-primary's `TopologyFull` and the deltas derived from it"
14. **§3.2 cadence**
    Old: "Digest heartbeat         every node; configurable cadence; never slower than 2 s"
    New: "Digest heartbeat         every node; configurable cadence; never slower than 2 s; and at once on the node's status change"
15. **§3.3 table**
    Old: "NODE -> OWN MESH            Digest heartbeat, full local facts, <= 2 s"
    New: "NODE -> OWN MESH            Digest: status, <= 2 s, and at once on its status change"
16. **§3.3, a new paragraph after that table**
    New: "An ordinary node holds its own mesh's nodes from their own Digests. It takes its
    mesh-primary's view of them only by a read: at create, join and start, and on a sync issue (the
    top-up below); a Digest from a birth it does not hold is a sync issue. Nothing of its own mesh
    reaches it from its mesh-primary over gossip."

### lifecycles/node-mesh-join.md

17. **Completion row**
    Old: "the first `Digest` heartbeat follows it; the mesh-primary's one Pending publish for this join
    came before it, when the report was accepted (step 10)"
    New: "the node's Digest carrying pending went out when it wrote pending (step 10); its heartbeats
    follow"
18. **Step 9**
    Old: "9. `node.mesh.joined`. The node's first `Digest` heartbeat follows."
    New: "9. `node.mesh.joined`. The node's heartbeats follow its pending Digest (step 10)."
19. **Step 10**
    Old: "publishes the node once as Pending when it accepts the node's join report, before it answers
    it, and the node writes pending on its own record (Q356)."
    New: "records the node pending when it accepts the node's join report, before it answers it; the
    node writes pending on its own record (Q356) and publishes its Digest at once, carrying it."
    Old: "and starts no other; the node publishes no Pending of its own."
    New: "and starts no other."
    Old: "and is published ready only once its hook has passed"
    New: "and publishes ready only once its hook has passed"
20. **Sequence diagram, mesh-primary note**
    Old: "10. publishes the node once as Pending when it accepts the node's join report, before it answers it"
    New: "10. records the node pending when it accepts the node's join report, before it answers it"
21. **Checked against**
    Old: "the Pending publish from the mesh-primary, never from the node."
    New: "pending published by the node, in its Digest; the mesh-primary records it."

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
    New: "A node has one state, its **status**: starting, pending, ready, connections-deleting,
    connections-deleted, leaving, left, stopping, stopped, deleting and deleted, each set by its
    workflow as [States](lifecycles/states.md#states) and
    [node status](lifecycles/node-status.md#node-status-change) define, and `PendingReconnect`, the
    status of a node under [node investigation](lifecycles/node-investigation.md). The node writes
    its status on its own record (Q356) and publishes it in its Digest at once when it changes and on
    every heartbeat ([gossip](gossip.md#31-frame-catalog)); its mesh-primary writes
    `PendingReconnect`, deleting and deleted. A stopping node's last Digest carries stopping, and its
    mesh-primary publishes `NodeStopped` from the stop stream's `node.stopped` frame."
    (The "Shutdown is signal-driven" bullet stays.)
27. **§4.1 rows**
    Delete the `Joining`, `Alive`, `Overloaded` and `Degraded` rows; add:
    "| pending | the node, at its accepted join ([node-mesh-join](lifecycles/node-mesh-join.md) step 10) | the traffic/topology filters — routed no traffic |"
    "| ready | the node, at its node.started | the rebind path; every traffic and topology filter |"
    Old: "| `Leaving` | set by the mesh-primary on `node.stopped` (overlay `NodeStopped`) | the routing rule: routed no traffic |"
    New: "| leaving, stopping, stopped | the node, on its own record ([States](lifecycles/states.md#who-sets-each-state)) | the routing rule: routed no traffic |"
    The `Dead` and `PendingReconnect` rows stay.
28. **§4.2**
    Old: "`NodeState` (`rdm-core/src/digest.rs`) and a node's status (its row, §1 rule 2) are separate
    state families that never share an enum or a type; an application's own entity lifecycle is a
    third, the application's."
    New: "A node's status is its one state family; an application's own entity lifecycle is the
    second, the application's."

### lifecycles/states.md

29. **Line 14**
    Old: "- **Node row silence marks:** a node row may carry the observer-inferred marks
    `PendingReconnect` and `Dead` (TRUE OFFLINE), written by the node's mesh-primary. They are never
    themselves a reason to act ([node recovery](node-recovery.md))."
    New: "- **Node under investigation:** `PendingReconnect` is the status of a node under
    [node investigation](node-investigation.md), written on its record by its mesh-primary
    ([node recovery](node-recovery.md) step 1); the verdict ends it. A node row may also carry the
    observer-inferred mark `Dead` (TRUE OFFLINE), written by the node's mesh-primary, never itself a
    reason to act."

### fabric-mesh-node-lifecycles.md

30. **Index row, Node identity and state**
    Old: "the NodeState liveness enum, "
    New: (removed)
