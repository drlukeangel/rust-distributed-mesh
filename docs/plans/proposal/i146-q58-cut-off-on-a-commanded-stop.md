## Q58: a mesh-primary whose only other member it stopped by command counts its view cut off and refuses that member's start

### 1. Spec

No spec covers this step. The pages name a "cut-off or healing node-admin" as one that holds no
authority ([fabric-primary.md](../../architecture/fabric-primary.md) line 110,
[build.md](../../architecture/build.md) line 218, [gossip.md](../../architecture/gossip.md) line
948), but no page says when a view is cut off. The rule lives only in code (§4).

The steps around it are specified:

- [node-stop.md](../../architecture/lifecycles/node-stop.md) step 3: the stopped node runs
  node.mesh.leave and publishes nothing after it.
- [node-start.md](../../architecture/lifecycles/node-start.md) steps 2 and 4 and its sequence: the
  mesh-primary answers the start's join, then its `node.topology.get`, rafka-time last.
- [node-topology-get.md](../../architecture/lifecycles/node-topology-get.md): the read is the
  member's own mesh-primary's.

Owner: the mesh-primary. Sequence: node.stop → node.start → join → topology → rafka-time → node.started.

### 2. Issue

`admin_start_declares::a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again`
fails `StartFailed { step: "node.rafka-time", reason: "… refused: the target holds no mesh mesh1" }`.
It has been red on four landing gates: e6.3's `6f873636`, Q56's `6ce360ab` and `187441f1`, and s89's
`7f9b1f1d`. An isolated batch was 30 of 30 green.

### 3. Flame

Testkit span file of gate `7f9b1f1d`, kept at
`kept/gate-gates-e4s8-7f9b1f1d/testkit/a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again/testkit.2531028-4026531836.spans.jsonl`.
The node is mesh1.admin.2 and the root, mesh1's mesh-primary, is mesh1.admin.1. Times are seconds
from the cell's first span.

```text
0.705 rdm.node.mesh.update.via-left            node=mesh1.admin.2           (the commanded stop's leave)
0.795 rdm.node_admin.node.update.via-command-frame node=mesh1.admin.2 frame=command-accepted   (the start)
0.796 rdm.mesh.membership.reject.via-stopped-birth node=mesh1.admin.1 member=mesh1.admin.2 door=join digest_seq=0 held_digest_seq=2 node_state=Joining
                                               trace 4b9d818f99b21c8c511a91b07b749596
0.820 rdm.mesh.entry.update.via-rafka-time-adopted node=mesh1.admin.2 via=join served_by=mesh1.admin.1
0.870 rdm.mesh.topology.serve.via-read         node=mesh1.admin.1 outcome=served snapshots=1
0.878 rdm.mesh.membership.reject.via-stopped-birth node=mesh1.admin.1 member=mesh1.admin.2 door=heartbeat digest_seq=3 held_digest_seq=2
0.882 rdm.mesh.membership.update.via-member-stale node=mesh1.admin.1 member=mesh1.admin.2 silent_ms=0
                                               trace d6db53d0ed11bc79133b7f6104725710
0.882 rdm.mesh.membership.update.via-cut-off   node=mesh1.admin.1 role=start
                                               trace 795ca763f39cc404c85141fa4f53b50a
0.905 rdm.mesh.topology.serve.via-read         node=mesh1.admin.1 outcome=unknown-mesh requested=mesh1
                                               trace ea544d613b2deb17eeef2b38065ff360
0.921 rdm.node_admin.node.update.via-command-frame node=mesh1.admin.2 frame=failed
```

CONTRADICTION: `via-member-stale … silent_ms=0`. The member was not silent. It left the root's
current members at its stop, and the root rejected its start's digests as a stopped birth's.

### 4. Observations (commit `7f9b1f1d`)

- [code] `crates/rdm-transport/src/membership.rs:2879-2880`: `heard_others |= others > 0; let off =
  heard_others && others == 0;`. A view that has heard another member and hears none is cut off,
  whatever ended the hearing.
- [code] `membership.rs:4836`: a digest of a birth the book holds stopped is rejected
  (`via-stopped-birth`). This includes the join and heartbeat digests of a start in progress, until
  the stop ends.
- [code] `crates/rdm-admin/src/admin/boot.rs:2268-2269, 2277`: `mesh_primary = live && holds_mesh_seat`,
  with `live = membership.authorizes()`. A cut-off view drops the mesh-primary flag; the seat record
  is unchanged.
- [code] `membership.rs:3147`: `held_topology` adds the node's own mesh at version 0 only while that
  flag is set.
- [code] `crates/rdm-node/src/topology_read.rs:154`: with neither a held source nor stored nodes, the
  read is answered `unknown_mesh` (the stored map never adds a node's own mesh).
- [flame] The join (0.820) and the topology read (0.870) were served; the rafka-time refresh (0.905),
  after `via-cut-off` (0.882), was refused.

#### Inferred

- [inferred] The red needs the root's 250 ms cut-off watch tick, and a projection round, to land
  between the stop and the rafka-time refresh. The gate's concurrency widens that window; an
  isolated run closes it (30 of 30).

### 5. Delta

No spec covers this step; code has a mesh-primary whose only other member it stopped by command
count its view cut off, drop its primary flag and answer that member's start `unknown-mesh`.

### 6. Arms

Every arm assumes the mesh-primary holds its seat record throughout and the stopped member is the
same birth restarted.

- **A:** a cut-off view is one that stops hearing a member it expects to hear. A member its book holds
  stopped by a completed node.stop, or departed, does not make the view cut off. Changes the cut-off
  rule (`membership.rs` `watch_meshes`); no wire, timer or gossip frame.
- **B:** a mesh-primary answers its own mesh's topology from the seat record it holds, cut off or
  not, so a node it commands can still read topology and rafka-time from it. Changes the topology
  serve's own-mesh source (`held_topology`); the cut-off rule is unchanged.
- **x:** something else.
