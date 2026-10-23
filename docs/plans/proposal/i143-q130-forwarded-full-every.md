## Q130: a forwarded full is sent only when the mesh-primary takes the role, never every `FULL_EVERY` rounds

### 1. Spec
`gossip.md` :155 (frame catalog): "`Members` forwarded full `{ …, snapshot_id, chunk_index, chunk_count }` | the
forwarding primary's own mesh channel | the local mesh primary: every `FULL_EVERY` rounds as passive anti-entropy".
:596-603 §3.3: "A receiver applies a delta only when it holds exactly `base_version` of that source and publisher.
On a mismatch it marks that source's projection desynced, stops applying its deltas, and pulls the current full
fabric topology from its own current mesh-primary [...] installs it atomically, records the publisher and version
baseline, and resumes. Nothing is held for later and nothing is requested over gossip." :610-612: "`FULL_EVERY` is a
bounded, configurable number of aggregate rounds. With the top-up, it bounds only how long a node that noticed
nothing wrong holds a stale projection, so it is passive anti-entropy." Owner: the mesh-primary (forwarder).
Sequence: delta → gap → top-up from the mesh-primary → resume; a stale projection the node did not notice is
bounded by the next forwarded full, every `FULL_EVERY` rounds.

### 2. Issue
A peer mesh's rpc node never retains a departure: `mesh1.rpc.1` held mesh2 at v9 (no departure) for 30 s after the
mesh-primary forwarded v10 (`departed=1`); 1 in 1 land-gate runs at 1918053cb, 0 in 5 runs alone (first sighting);
`node_delete::deleting_a_node_is_a_pre_notice_then_a_proven_departure_every_mesh_hears`.

### 3. Flame
Spans (kept by the gate): `/home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e6s8-1918053cb/artifacts/node-lifecycle/deleting_a_node_is_a_pre_notice_then_a_proven_departure/spans/`
(`rdm-rpc-node.2937826-…` is `mesh1.rpc.1`). Cells export no OTEL.

CONTRADICTION: the top-up ended `still_desynced=""` with `installed: mesh2@9`, while the delta to v10 it had
dropped one millisecond earlier was already published into mesh1.

```
10:18:03.072 rdm.mesh.membership.update.via-gap {"base_version": "8", "frame": "members-delta", "gap": "version-gap", "held_version": "7", "mesh": "mesh2", "node": "mesh1.rpc.1", "repair_requested": "true", "source_publisher": "mesh2.admin.1:b7x93h7pnj32@94kbr583tq3e"}
10:18:03.073 rdm.mesh.membership.update.via-gap {"base_version": "9", "frame": "members-delta", "gap": "already-desynced", "held_version": "-1", "mesh": "mesh2", "node": "mesh1.rpc.1", "repair_requested": "true", "source_publisher": "mesh2.admin.1:b7x93h7pnj32@94kbr583tq3e"}
10:18:03.074 rdm.mesh.membership.update.via-snapshot-installed {"channel": "topology-read", "departed": "0", "in_flight": "2", "members": "2", "mesh": "mesh2", "node": "mesh1.rpc.1", "publisher": "mesh2.admin.1:b7x93h7pnj32@94kbr583tq3e", "snapshot_id": "8", "topology_version": "9"}
10:18:03.075 rdm.mesh.entry.update.via-top-up {"installed": "mesh2@9", "mesh_primary": "mesh1.admin.1", "meshes": "mesh2", "node": "mesh1.rpc.1", "reasons": "mesh2:version-gap", "served_by": "mesh1.admin.1", "still_desynced": ""}
```
At the forwarder (`mesh1.admin.1`, gate log `gates-e6s8-1918053cb.log.d/node_delete.log`):
```
10:18:03.073596Z rdm.mesh.membership.update.via-forwarded-delta{node=mesh1.admin.1 mesh=mesh2 source_publisher=mesh2.admin.1:b7x93h7pnj32@94kbr583tq3e base_version=9 topology_version=10 changed=0 removed=0 in_flight=0 departed=1}
```
After 10:18:03.075, `mesh1.rpc.1` has no span applying a mesh2 delta or a forwarded full; no
`rdm.mesh.membership.update.via-forwarded-full` span appears at any node of the estate after 10:18:03.

### 4. Observations (commit 1918053cb)
- [flame] the delta to v10 reached `mesh1.rpc.1` while desynced and was dropped (`already-desynced`), as §3.3 directs.
- [flame] the top-up served v9: the mesh-primary's read answered from a projection one version behind what its forwarder published in the same millisecond.
- [code] `crates/rdm-transport/src/membership.rs:3733-3749`: the forwarded full is sent when the mesh-primary takes the forwarder role, "the first publication of every source into this Mesh. Nothing sends it again on a timer"; `full_due` (:3352, :3633) is the forwarder's owed-full flag, set on taking the role.
- [code] no `FULL_EVERY` setting exists in `crates/` (grep).
- [flame] no later mesh2 change came from the source, so nothing replaced v9 within the cell's 30 s.
#### Inferred
- [inferred] the race needs a gap whose top-up lands between the forwarder's fold and its publish; under the land gate's concurrency the cell took 39 s against 15 s alone — from the stem times.

### 5. Delta
Spec says the forwarding mesh-primary sends a forwarded full every `FULL_EVERY` rounds; code sends one only when it takes the role.

### 6. Arms
Every arm assumes the §3.3 receive rule stays: a delta on a base the node does not hold is dropped and the node tops up.
- A: the forwarder sends the forwarded full of every source every `FULL_EVERY` aggregate rounds, a setting with a measured default — changes publisher (`membership.rs` round loop sets `full_due` every `FULL_EVERY` rounds) and timer (none new: the aggregate round).
- B: no code: the page drops the periodic full; a node holding a stale projection it did not notice stays stale until the source's next change — changes the page (`gossip.md` :155, :610-612).
- x: the delta itself is the fix — changes publisher.
