# Topology storage provider: runbook

What the person on call reads, and does, when a storage change does not end the way it should. The
contract is [storage.md §2, Changing it](../../architecture/storage.md#changing-it); the steps and
spans are on the [overview](overview.md); the calls are in the [how-to](how-to.md). Each procedure
names the cell in `crates/rdm-test/tests/fabric/topology_storage_provider.rs` that runs it, unless
another file is named.

## Before you start

- **Ask the fabric-primary.** It accepts the change, runs the copy and the switch, and writes the
  Fabric row. `fabric.get` at any node-admin names it (`fabric_primary`).
- **The change's two ids.** `build_id` is the change's Build (the `build.create` stream's). `change_id`
  is minted at its acceptance; every step's span carries both, and the operation keys carry the
  `change_id` (`storage.switch:<change_id>`, `storage.change:<change_id>`).
- **Searching Jaeger.** Filter on `build_id`; every step of the change is on the Build's trace. A
  provider's secrets never appear in a span.
- **Where the records are.** On the `file` provider a fabric's root is `<RDM_STORE_DIR>/<fabric id>/`:
  `fabric.jsonl`, `meshes.jsonl`, `nodes.jsonl`, `connections.jsonl`, `builds.jsonl`, the `layout`
  marker and `locks/`. Another registered name has the place its registration gives it.
- **No timer ends a block.** A change that does not progress holds every mesh-primary blocked until
  the step it waits on completes. Nothing here is fixed by waiting for a timeout.

## Reading a change's progress

| step | where to read it | done when |
|---|---|---|
| accepted | the `build.create` stream opened; `fabric.get` still names the old provider | the stream carries its Build id |
| 1. Block | `rdm.node_admin.mesh.update.via-block`, one per mesh, `outcome = blocked`, `in_flight` the commands it waited for; receipt `mesh.block:<mesh>` | every mesh's span is recorded |
| 2. Copy | `rdm.node_admin.build.update.via-storage-copy`, `outcome = copied`, `copied` the lines appended, `source_position` | the span ends `copied` |
| 3. Switch | `rdm.node_admin.build.update.via-storage-switch`, `outcome = inserted` (`commit_found = false`: this insert decided it) or `found`; receipt `storage.switch:<change_id>`; the new provider's Fabric row names it, the old one previous | the `commit` record stands on the new provider |
| 4. Unblock | `rdm.node_admin.mesh.update.via-unblock`, one per mesh, `outcome = unblocked`, `provider` where it moved its nodes, `moved` and `recreated` counts; each node's `rdm.node.build.update.via-storage-move`, `outcome = moved`; receipt `mesh.unblock:<mesh>` | every mesh's span is recorded |
| verdict | receipt `storage.change:<change_id>`; the stream's `Complete` | `fabric.get` names the new provider and every node's row names it |

The receipts are read with `build.get` (`BuildCarrier::get(&build_id)` → `BuildReceipts.steps`); a
stream that broke is reattached by its Build id. While the change runs, `fabric.get` names the
fabric's provider (the intent) and each node's row the provider that node writes (the fact): a node
whose row still names the old provider has not moved yet.

Cell: `a_customer_changes_topology_storage_without_restarting_members` (every step's span, one trace).

## A change that stays blocked

Read which step's span is the last one recorded for the `build_id`.

| last recorded | waiting on | what to do |
|---|---|---|
| some meshes' `via-block`, not every mesh's | a mesh-primary whose in-flight commands have not completed, or a mesh-primary that does not reply | read that mesh's in-flight commands; a mesh-primary that does not reply is a lost node, and node investigation re-creates it at its `path.name` as an attempt of the change's own Build ([node investigation](../../architecture/lifecycles/node-investigation.md)). Nothing to do but read |
| every `via-block`, no `via-storage-copy` | the copy cannot open a provider | see [An unavailable provider](#an-unavailable-provider) |
| `via-storage-copy` ending `indeterminate` (`cause` names the store's error), repeated | a provider that does not answer during the copy | see [An unavailable provider](#an-unavailable-provider) |
| `via-storage-switch` ending `no-answer` (`reason`), repeated | the new provider does not answer the insert or the read of the `commit` record | see [An unavailable provider](#an-unavailable-provider); never decide the change from a lost answer |
| some meshes' `via-unblock` | a node that has not moved yet: see [A node that refuses its move](#a-node-that-refuses-its-move) | read that mesh's `via-storage-move` and `via-storage-move-lost` spans |

While blocked, every node keeps serving, `node.get` answers every node ready-for-traffic, and a
caller's command is admitted and issued once its mesh-primary is unblocked. Any Build but
`fabric.delete` is refused `build-in-progress` naming the change's Build.

Cells: `every_serving_node_keeps_its_birth_and_ready_for_traffic_through_a_change`,
`a_build_other_than_fabric_delete_is_refused_during_a_change`.

## An unavailable provider

**At acceptance.** The fabric-primary refuses by name a provider its wiring does not register,
`topology-storage-provider-not-registered`, nothing accepted (cell: `an_unsupported_provider_changes_nothing`).
The check at acceptance is the registration (`crates/rdm-admin/src/control_plane.rs`, the
`TopologyStorageProviderNotRegistered` refusal): a registered provider that does not open is met at
the copy.

**During the change.** A provider that does not answer never decides anything. The copy and the switch
read it again, with backoff, until it answers; a mesh-primary's `mesh.unblock` reads the `commit` record
again until it answers. The mesh-primaries stay blocked meanwhile, and every node keeps serving.

What the person on call does:

1. Read the last span (the table above) and its `cause` or `reason`: it names the provider and the
   store that did not answer.
2. Bring the provider back: its process, its network, its credentials. RDM holds the change at its
   step and carries on once the provider answers; nothing is resubmitted.
3. If the provider cannot come back, the change cannot complete. `fabric.delete` is the one Build
   admitted during a change ([fabric-delete.md](../../architecture/lifecycles/fabric-delete.md)): it
   resolves the change by its `commit` record before its teardown and keeps the records on both
   providers (cell: `a_delete_resolves_the_storage_change_before_teardown`).

No cell makes a provider stop answering mid-change; the procedure is read from
`crates/rdm-admin/src/storage_change.rs` (`decide`, `switch_steps`).

## Restart and adoption during a change

The rule for every start during a change: **the decision is read only from the `commit` record** for
`(build_id, change_id)` on the new provider. Found, the change switched; none on a provider that
answers, it did not; no answer, neither, and the block holds. A launch environment, a view or a lost
answer never decides it.

| who restarts | what happens | cell |
|---|---|---|
| a role node, killed while its mesh-primary is blocked | node recovery re-creates it at its `path.name` over the same data dir once its mesh-primary issues commands again; the new birth is launched on, and writes only, the provider the `commit` record decided | `topology_storage_state_survives_role_node_restart` (both a switched change and one failed before its switch), `a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read` |
| the fabric-primary | a successor fabric-primary resumes the change's Build; its switch step first reads the `commit` record (`via-storage-switch` `outcome = found`, or a copy that appends only the lines the new provider lacks, then the insert) | none |
| nothing alive: the adoption launch ([fabric recovery, Nothing alive](../../architecture/lifecycles/fabric-recovery.md#nothing-alive)) | the adoption launch opens the provider its environment names | none |

**The adoption launch mid-change.** Before starting it, decide which provider holds the fabric's
latest records by the `commit` record alone:

1. Read the `change_id` of the change: the last `via-block` or `via-storage-switch` span of its
   `build_id`, or the change's operation keys in the Build's receipts on the old provider's
   `builds.jsonl`.
2. Look for the record on the new provider. On a file provider:
   `<new provider's root>/locks/commit/<build_id>.<change_id>`.
3. Present: the change switched; start the adoption launch with
   `RDM_TOPOLOGY_STORAGE_PROVIDER=<new provider>`. Absent on a provider that answers: it did not;
   start it with the old provider. The provider does not answer: wait for it; do not guess.
4. The adopted fabric-primary resumes the change's Build, which reads the same record.

## Switched, or failed before the switch

| | switched | failed before the switch |
|---|---|---|
| `commit` record on the new provider | present | absent, read on a provider that answers |
| `via-storage-switch` | `inserted` or `found` | `not-switched` (`commit_found = false`), read before the Build fails |
| the Build | `Complete` | `Failed` at `storage.change:<change_id>`, naming why; its receipts hold the `Failed` of step `Copy` at `storage.switch:<change_id>` |
| `fabric.get` | the new provider, the old one previous | the old provider, previous unchanged |
| each node's row | the new provider | the old provider |
| `via-unblock` `provider` | the new provider | the old provider |
| the new provider | the fabric's records, written from here on | closed; holds what the copy appended, never read by the fabric |
| the old provider | kept as it was at the switch, written no more | the fabric's records, written as before |

A failed change leaves nothing to undo: every node writes the old provider, and a new Build naming the
new provider can be submitted once its cause is fixed.

Cells: `a_customer_changes_topology_storage_without_restarting_members` (switched),
`a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider` (failed before the switch).

## Refused history

The copy appends every line of every store on the old provider to the new one in source order, and the
new provider must hold a prefix of the old one's lines in each store. When it holds a line the old one
does not, or a line out of order, or a claim the old one does not hold with the same content, the copy
is refused by name and nothing switches:

```text
the new topology storage provider `<new>` is not a copy of the old one `<old>` in source order: <why>; nothing is appended and nothing switches
```

It is on the `via-storage-copy` span (`outcome = refused`, `cause`), on the `Failed` receipt of step
`Copy` at `storage.switch:<change_id>`, and in the reason of the Build's `Failed` frame. The change then fails before the switch, as above.

What the person on call does: the new provider's place holds records that are not this fabric's history
(another fabric's, an earlier test's, a line written by hand). RDM deletes nothing. Either clear that
place by the application's own work, or register a fresh place under another name, then submit the
change again. A copy resumed after a lost answer is not a refusal: it appends only what follows the
prefix, never a line twice (`a_copy_resumes_after_a_lost_answer_without_duplicate_records`,
`crates/rdm-node/tests/storage_copy.rs`).

Cell: `a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider` (the new provider holds a
line the old one does not).

## The previous provider

After a switch the previous provider is kept as it was at the switch, named on the Fabric row in
`previous_topology_storage_providers`, written no more and never deleted by RDM. Every store on it reads
whole, and its own Fabric row still names it with no previous provider.

- Reading it: it is the fabric's history up to the switch, readable by anything that reads that backend.
- Returning to it: a Build naming it again is a change, which copies into it only the lines it lacks
  ([how-to §6](how-to.md#6-return-to-a-previous-provider)).
- Removing it: the application's own decision, never RDM's. A later return to a removed provider copies
  every line again.

Cells: `a_previous_provider_remains_unchanged_after_cutover`,
`a_customer_returns_to_a_previous_topology_provider`.

## A node that refuses its move

In `mesh.unblock` a mesh-primary moves each node of its mesh with `StorageStep` (node command op
`0x22`). A node that will not move, because it refuses its move by name (it cannot open the provider,
`via-storage-move` `outcome = refused`, `reason`), refuses it `rejected-stale-change`, or leaves it
unanswered past the staleness floor, is re-created at its `path.name` on the provider the change
decided: node.delete then node.create, steps of the mesh-primary's `mesh.unblock` attempt of the
change's own Build (lead decision D1). The mesh-primary replies `unblocked` once every node of its mesh
moved or was re-created.

What to read:

- `rdm.node_admin.node.update.via-storage-move-lost`, `outcome = recreated`, `reason` naming why the old
  birth would not move, on the same trace as its mesh's `via-unblock`;
- the mesh's `via-unblock`, `recreated` counting the paths re-created;
- `mesh.nodes.get`: the path holds a new node id, ready-for-traffic, writing the new provider; the old
  birth's records stay where they were.

Nothing to do when it completes. A path whose re-created births keep refusing (`via-storage-move-lost`
`outcome = not-recreated`, or a new birth that refuses again) holds its mesh's unblock: no timer ends
it. Read the new births' start refusal: an executable that does not register the provider, or a place
the node cannot reach. Under the container provider, a node whose container does not mount the
provider's place is re-created without end (lead decision D14); the testkit's container cells are
ignored by name until its mount is built (i149 R15).

Cells: `a_node_that_refuses_its_move_is_recreated_on_the_new_provider`,
`a_node_that_never_unblocks_is_recreated_on_the_new_provider` (a node frozen, alive and silent).

## Writes held while blocked, and what a caller does

- **Rows and status declarations** (lead decision D13). Once a mesh-primary has replied `blocked`, its
  row writer holds its writes, owed, and writes them at the unblock on the provider current then; a
  status a node reports while blocked is applied and answered once its row is written after the
  unblock. Nothing of the mesh is written to the old provider after `blocked`, so the copy holds every
  line. Cells: `a_row_change_after_blocked_is_held_and_written_at_the_unblock`
  (`crates/rdm-admin/src/admin.rs`), `a_status_a_node_reports_while_blocked_is_answered_once_written_after_the_unblock`
  (`crates/rdm-admin/src/status_rpc.rs`).
- **A node's connection events** are owed on the node's one writer queue (Q169) and never block an open:
  each lands once. Cell: `a_connection_event_during_a_change_lands_once_after_the_unblock`.
- **A caller's command** is answered only once its line lands. A caller whose call ended without an
  answer reads the row (`node.get`) before sending it again: the command may have landed at the unblock.
  Cell: `a_customer_changes_topology_storage_without_restarting_members` (`node.config.update` during the
  change, answered, its row on the new provider alone).
- **An older change's command** reaching a node after a newer change moved it is refused by name,
  `rejected-stale-change`, naming the newer change; it never moves the node. Cell:
  `an_old_change_cannot_unblock_writes_for_a_new_change`.
