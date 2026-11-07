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

## The defect

A node's status reached its peers from two sources, its own Digest and its mesh-primary's
projection, under two orders (`digest_seq`, `topology_version`) that cannot be ordered against each
other; a compute held a ready broker `Pending` for 34 s (D68, rdm#337). The Digest also repeated the
envelope (`source`, `sequence`, `time`) and the record (identity, endpoints, runtime, `data_dir`,
`admin_api_base`, the primary revisions): twenty fields, one of them needed.

## The design: three rules

1. **A node's Digest is its status.** `Digest { status }`, states.md's values plus
   `PendingReconnect`, one type. Published at once when the node writes a new status on its own
   record (Q356) and on every heartbeat. The envelope carries the birth (`source`), the order
   (`sequence`, the birth's `digest_seq`) and the time (`time`); nothing of them is repeated inside.
   The first status a Digest carries is pending, at the accepted join (node-mesh-join step 10); a
   receiver never gives a birth a status it has not published. `NodeState`, `MemberStatus` and the
   second `NodeStatus` are removed.
2. **Everything else about a node is on its record and reaches a peer only by a read**: the start
   read, and the top-up when a Digest arrives from a birth the peer does not hold. A projection entry
   is the record's identity and endpoints with the node's latest status as the mesh-primary holds it.
   Inside its mesh a node's own status comes only from its Digest; `PendingReconnect`, deleting and
   deleted are its mesh-primary's, on the record, and deleting and deleted ride `NodeDeleting` and
   `NodeDeleted` as today.
3. **The mesh-primary publishes its mesh's aggregate (`TopologyFull`) on the backbone only, and onto
   its own mesh channel only each peer mesh's differential from the last delta it published of that
   mesh (`TopologyDelta`): the nodes whose status changed, were added or removed, no loads.** Never
   its own mesh's nodes there, which publish themselves, and never a peer mesh's full: neighbour-up
   on the mesh channel replays nothing. A joining node holds the peer meshes its mesh-primary holds
   from its start read (rule 2); a delta whose `base_version` the receiver does not hold triggers the
   GetTopology repair, as today.

`TopologyFull` keeps `in_flight` (the mesh's in-flight operations, derived from Build facts at each
publication) and `deleted` (the retention refusal). Per entry it loses `runtime`, `data_dir`,
`admin_api_base`, `supersedes`, `mesh_id`, the gossip stats, the load and the primary revisions.

### Open, for Luke

1. **`is_primary` and `is_fabric_primary`.** The record's; a primary change reaches a node's peers
   by the read like every other record fact, and the election never read the flag. No election
   behaviour changes; the `NewFabricPrimary` row changes as edit 12 has it. Closable on his word.
2. **An address that moves within one birth.** Not checked, and the code allows it (`live.rs`
   `Applied::Updated`: "the held birth's address or name changed"). If iroh can re-bind a live birth,
   a Digest without the address needs a trigger for the peer to re-read; a dial that fails on the
   held address is the natural one, an event, not a timer. Open until iroh's endpoint API is read.
3. **`Dead`.** The TRUE OFFLINE mark the mesh-primary writes, 79 uses in code and the
   `a_true_offline_member` cells; a second mark beside `PendingReconnect` that "is never itself a
   reason to act". Drop or keep is a recovery change, Luke's.
4. **The load.** Out of the Digest and the `TopologyFull` entry. RDM decides nothing on it; Rafka
   samples its own (`rafka-node-base/src/load.rs`) and reads nothing off the digest's load, verified.
   `node.get` then answers no load unless the application supplies its own. Luke's word, since it is
   a wire change under the same cutover.

## RDM

`rdm-core`: `NodeState`, `MemberStatus` and the second `NodeStatus` removed; `MeshDigest` becomes
`Digest { status }`, sent whole; `ProjectionEntry` per rule 2; "durable format moved".
`rdm-gossip`: publish on a status write and on the heartbeat with the envelope filled from the birth;
`DigestView` takes an own-mesh node's status from its Digest only; a Digest from a birth not held
starts the top-up; the forwarder and `ForwardPath::replay` take peer meshes only. `rdm-admin`: the
mesh-primary publishes nothing of its own mesh on its own channel; the join answer no longer publishes
the node's Pending. `rdm-node`: a status write publishes the Digest. Cells: rdm#337's 34 s order ends
with the Digest's status; the mesh-primary's own channel carries no frame about its own nodes; a
status change reaches every node of its mesh within one heartbeat with no read. Removal debt: R-HB1's
text in node-status-heartbeat-connections.md; `e805dad46`'s read status move.

## Rafka

Takes the pin with the cutover: every live mesh is taken down and deployed again; its ~50
`MemberStatus::` matches move to the one status type; its own `NodeState` enum (unused) goes; its
reads of a held entry's `endpoint_id` stay (rule 2); its own primary-role read follows open item 1.

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
   New: "An own-mesh birth: as `TopologyFull`."
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

31. **TopologyFull row, use, first sentence**
    Old: "the complete Mesh with loads: what every node-admin holds of every Mesh for routing and control decisions."
    New: "the complete Mesh: what every node-admin holds of every Mesh for routing and control decisions."
    (if open item 4 is yes)
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
    (the loads words if open item 4 is yes; the "with freshness" words in any case)
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
        "mesh": "mesh1",
        "type": "node",
        "path": "mesh1.node",
        "name": "1",
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
        "incarnation": "0000000000000008"
      },
      "status": "ready"
    }
    ```
    (the entry's `node` fields are the record's identity and endpoints as the page's example names them today, checked against the page before landing; the example's `Digest` frame becomes `{ "status": "ready" }` under its envelope)

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

36. **§2.5 table, the gossip row** (if open item 4 is yes)
    Old: "| per-heartbeat liveness — `cpu_used`, `ram_used`, reachability | gossip, in memory only | every node, about itself |"
    New: "| per-heartbeat liveness: the node's Digest arriving, carrying its status | gossip, in memory only | every node, about itself |"

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
