# Q56 — the i144 owner's read: how a lost mesh's created admin acquires its mesh's held state

Answers Codex's packet [`i143-q56-held-mesh-state-acquisition.md`](i143-q56-held-mesh-state-acquisition.md),
assigned to i144 by Q54. Read on main `e278636c`.

## 1. Spec

- [mesh-investigation.md, "A node-admin joins the live mesh"](../../architecture/lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh):
  step 2 "The node-admin calls JoinNode on its MAKER, the fabric-primary, then GetTopology on that
  maker. It installs complete topology snapshots and obtains its control/status context." Step 4:
  "Ready waits for the sweep to finish … It then becomes ready-for-traffic and takes the
  mesh-primary seat under the election; as the mesh's successor primary it adopts the mesh's held
  state."
- [fabric-primary.md §7](../../architecture/fabric-primary.md#7-mesh-primary-takeover-is-hydration-not-transition):
  "A successor mesh primary reads/hydrates the Mesh lifecycle state that already exists and adopts
  that exact state locally … It republishes the adopted projection, with no durable Mesh write,
  without manufacturing a lifecycle transition. `Pending` is not promoted by a takeover shortcut."
- [states.md, Who sets each state](../../architecture/lifecycles/states.md#who-sets-each-state): mesh
  `Pending` is set by "the node-admin that launched the mesh's first admin" for a new mesh.

Owner: the joining node-admin acquires; its maker answers. Sequence: JoinNode on the maker (the
answer carries its control and status context) → sweep → Ready → seat → adopt the held state.

## 2. Issue

`container_peer_mesh_lost_recovers_under_same_mesh_id` (release `3cad5423`): the admin the
fabric-primary creates for lost mesh2 completes its sweep and stays Pending past the 300 s bound,
blocked on "its mesh's Pending has not been applied here by the fabric primary". One run of two read:
the same cell's other run (`kept/gate-gates-e7s3-3cad5423release`, build `bid_5g1k-…`) was handed
Pending and passed that gate.

## 3. Flame

From Codex's packet (verified), run `bid_yy8e-xygc-ca06`:

```text
rdm.node_admin.deployment.update.via-step node="mesh2.admin.1" attempt="3" step="ApplyMeshPending" outcome="complete" elapsed_ms="0"
rdm.node_admin.deployment.update.via-step node="mesh2.admin.1" attempt="3" step="WaitForNodeReady" outcome="failed" elapsed_ms="30113"
rdm.node_admin.runtime.reject.via-not-authority-capable node="mesh2.admin.1" detail="… the first admin of mesh2: its mesh's Pending has not been applied here by the fabric primary"
```

The other run, same cell, same step, a hand-off made (`kept/gate-gates-e7s3-3cad5423release/artifacts/i143-2784/…/spans`):

```text
rdm.node_admin.deployment.update.via-step node="mesh2.admin.1" attempt="3" step="ApplyMeshPending" outcome="complete" elapsed_ms="265"
rdm.node_admin.mesh.update.via-pending-handoff node="mesh1.admin.1" target="mesh2.admin.1" state="Pending" outcome="applied"
```

## 4. Observations (main `e278636c`)

- [code] `crates/rdm-admin/src/admin/runner.rs:1006` (`CreateNode`, the run's path: no `node_id` on its
  `via-build` span) supplies the mesh-state hook only when `first_admin_of(node)` holds in the
  fabric-primary's view at create time; `:630` (create over an exited birth) also needs `held_admins` empty.
- [code] `crates/rdm-admin/src/topology.rs:166` `first_admin_of`: no other node-admin of the mesh
  `is_live()` (Pending, ReadyForTraffic, ConnectionsDeleting). `crates/rdm-admin/src/executor.rs:164`
  `held_admins` counts ReadyForTraffic, ConnectionsDeleting, PendingReconnect and Restarting.
- [code] `crates/rdm-admin/src/admin/boot.rs:1634` the created admin's Ready gate needs
  `records.declared.meshes` to hold its mesh, filled only by `commit_mesh_state` (`status_rpc.rs:528`,
  the hand-off's ApplyMeshState); `session.rs:67` `pending_gate` blocks a non-Day-0 admin whose own
  view holds no other live admin of its cohort.
- [code] `runner.rs:491-499`: the hook's own contract is "`Pending` for a new mesh's bootstrap admin
  (e4.s11), the mesh's current state for the admin created for an existing mesh"; both callers
  (`:630`, `:1006`) pass `MeshState::Pending`.
- [code] `session.rs:829` the maker answers JoinNode with `statuses: membership.status_frames()`,
  which is every MeshStatus it holds (`rdm-transport membership.rs:957` `held_frames`);
  `rdm-node workflows/node_mesh_join.rs:105` the joining node learns them. Nothing takes the joining
  admin's own mesh's held state into its applied state.
- [flame] One run: no hand-off (0 ms), gate blocked. Other run: hand-off of Pending, gate passed.

#### Inferred

- [inferred] Whether the hand-off is made depends on whether the fabric-primary's view, at create time,
  still holds the lost mesh's other admin live: two views, two predicates, a race. Where it is made it
  hands Pending to an existing mesh whose held state is past Pending.

## 5. Delta

Spec says the created admin obtains its mesh's held state from its maker's JoinNode answer and adopts
it exactly; code gates its Ready on a Pending applied by a fabric-primary hand-off that is sent only
when the fabric-primary's own view counts no other live admin, and that hands `Pending` to an existing mesh.

## 6. Arms

Every arm keeps Q54 limited to declared-state durability, the same MeshId, and changes no timer, bound
or wire format.

- **A (taken: the existing contracts carry it).** The joining admin takes its own mesh's held state from
  its maker's JoinNode answer (the status context step 2 names, already on the wire) into its applied
  state exactly, no transition and no durable write (§7), where it takes the rest of that answer's
  control context (`take_entry_control`); a word it does not recognise is refused by name. The
  fabric-primary's hook hands an existing mesh its current held state, as the hook's contract says, and
  Pending only to a mesh it holds no state for. Changes: the joining admin's entry take and the hook's
  state; no new message, field or page line.
- **B.** A page line in mesh-investigation step 2 naming the Mesh state as part of the control/status
  context. Not needed: step 2 and §7 already state it.
- **x.** Something else.
