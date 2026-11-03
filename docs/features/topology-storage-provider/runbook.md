# Topology storage provider: runbook

What the person on call reads, and does, when a storage change does not end the way it should. The
contract is [storage.md §2, Changing it](../../architecture/storage.md#changing-it); the steps and
spans are on the [overview](overview.md); the calls are in the [how-to](how-to.md). Each procedure
names the cell in `crates/rdm-test/tests/fabric/topology_storage_provider.rs` that runs it, unless
another file is named.

## Before you start

- **Ask the fabric-primary.** It accepts the change, runs the copy and the switch, writes the Fabric
  row and the change's events. `fabric.get` at any node-admin names it (`fabric_primary`).
- **The change's id.** `change_id` is minted at acceptance and answered to the caller
  (`StorageUpdated { change_id }`). Every step's span carries it, every block, unblock and move command
  carries it, and each of the change's three events names it.
- **Searching Jaeger.** Filter on `change_id`; every step of the change is on its trace. A provider's
  secrets never appear in a span.
- **The change's record is its three events**, in the events store, `type` `rdm.storage.*`:
  `rdm.storage.started` on the old provider before the first command, `rdm.storage.committed` on the
  new provider at the switch, `rdm.storage.ended` (`data.state` `complete` or `failed`, naming why) on
  the provider it decided. They are read with the events store's storage-changes read
  (`EventsStorage::of_storage_changes`). No Build is written for a change and `build.get` knows nothing
  of it.
- **Where the records are.** On the `file` provider a fabric's root is `<RDM_STORE_DIR>/<fabric id>/`:
  the `layout` marker, `fabric.jsonl`, `meshes.jsonl`, `nodes.jsonl`, `connections.jsonl`,
  `builds.jsonl` and `events.jsonl`. Another registered name has the place its registration gives it.
- **A block is complete only when every required node has acknowledged its freeze**, a stopped node
  included (a stopped node still has a Node RPC endpoint). A node that cannot answer, an OS-paused or
  exited process, is resolved through its spent call's `Send` and node investigation/recovery; the mesh
  is not reported blocked while that freeze is unanswered. There is no second timer. Once a block is
  complete, no timer releases it; only `mesh.unblock` does.

## Reading a change's progress

| step | where to read it | done when |
|---|---|---|
| accepted | `rdm.node_admin.storage.update.via-caller`, `outcome = fabric.storage.updated`; `rdm.node_admin.storage.update.via-started`, `outcome = written`; `fabric.get` still names the old provider | the `rdm.storage.started` event is on the old provider |
| 1. Block | `rdm.node_admin.storage.update.via-mesh-command`, `command = BlockMesh`, one per mesh, `outcome = blocked`; on each mesh-primary `rdm.node_admin.mesh.update.via-block`, `outcome = blocked`, `in_flight` the commands it waited for; on each node `rdm.node.record.update.via-storage-block` | every mesh's `via-block` ends `blocked` |
| 2. Copy | `rdm.node_admin.storage.update.via-copy`, `outcome = copied`, `copied` the lines appended, `source_position` | the span ends `copied` |
| 3. Switch | `rdm.node_admin.storage.update.via-switch`, `outcome = inserted` (`commit_found = false`: this append decided it) or `found`; the new provider's Fabric row names it, the old one previous | the `rdm.storage.committed` event is on the new provider |
| 4. Unblock | `via-mesh-command`, `command = UnblockMesh`, one per mesh, `outcome = unblocked`; on each mesh-primary `rdm.node_admin.mesh.update.via-unblock`, `outcome = unblocked`, `provider` where it moved its nodes, `moved` and `recreated` counts; each node's `rdm.node.storage.update.via-move`, `outcome = moved` | every mesh's `via-unblock` ends `unblocked` |
| end | `rdm.node_admin.storage.update.via-ended`, `state = complete`, `outcome = written` | the `rdm.storage.ended` event is on the new provider; `fabric.get` names the new provider and every node's row names it |

While the change runs, `fabric.get` names the fabric's provider (the intent) and each node's row the
provider that node writes (the fact): a node whose row still names the old provider has not moved yet.

Cell: `a_customer_changes_topology_storage_without_restarting_members` (every step's span, one trace).

## A change that stays blocked

Read which step's span is the last one recorded for the `change_id`.

| last recorded | waiting on | what to do |
|---|---|---|
| some meshes' `via-block`, not every mesh's | a mesh-primary whose in-flight commands have not completed, a node whose freeze is unanswered, or a mesh-primary that does not reply (`via-mesh-command` `outcome = no-answer`) | read that mesh's in-flight commands. A node or mesh-primary that does not answer is a lost node: [node investigation](../../architecture/lifecycles/node-investigation.md), and node recovery re-creates it, the primary's own workflow, never part of the change. Nothing to do but read |
| `via-block` ending `not-blocked` (`reason`) | a mesh-primary that refused the block by name | read the `reason`; the change fails before its switch (below) |
| every `via-block`, no `via-copy` | the copy cannot open a provider | see [An unavailable provider](#an-unavailable-provider) |
| `via-copy` ending `indeterminate` (`cause` names the store's error), repeated | a provider that does not answer during the copy | see [An unavailable provider](#an-unavailable-provider) |
| `via-copy` ending `failed` or `refused` (`cause` names the provider and its error), once | a provider that answered the copy with a named error, or holds lines the old one does not | the change fails before its switch: see [An unavailable provider](#an-unavailable-provider) and [Refused history](#refused-history) |
| `via-switch` ending `no-answer` (`reason`), repeated | the new provider does not answer the append or the read of the `rdm.storage.committed` event | see [An unavailable provider](#an-unavailable-provider); never decide the change from a lost answer |
| some meshes' `via-unblock` | a node that has not moved yet | see [A node that refuses its move](#a-node-that-refuses-its-move); read that mesh's `via-move` and `via-storage-move-lost` spans |
| `via-ended` ending `no-answer`, repeated | the decided provider does not answer the `rdm.storage.ended` append | bring the provider back; every mesh is already unblocked |

While blocked, every node keeps serving and `node.get` answers every node ready. A caller's command is
admitted and issued once its mesh-primary is unblocked. Any Build but `fabric.delete`, and any other
storage change, is refused `build-in-progress` by name.

Cells: `every_serving_node_keeps_its_birth_and_ready_through_a_change`,
`a_build_other_than_fabric_delete_is_refused_during_a_change`,
`an_unanswered_storage_block_uses_normal_investigation_before_copy`.

## An unavailable provider

**At acceptance.** The fabric-primary refuses by name a provider its wiring does not register,
`topology-storage-provider-not-registered`, nothing accepted (cell: `an_unsupported_provider_changes_nothing`),
and opens a registered one other than the Fabric row's on its own scope, with the lines the copy
reads. One that does not open, whose lines do not open, or whose lines are refused or not answered is
refused by name, `topology-storage-provider-not-opened`, the detail naming why, nothing accepted, no
event written and no mesh-primary blocked (cells: `a_provider_that_does_not_open_changes_nothing`,
`a_provider_whose_lines_do_not_open_changes_nothing`,
`a_provider_that_refuses_its_list_at_acceptance_changes_nothing`; `TopologyStorageHandle::opens` in
`rdm-admin`).

**During the change.** A provider that does not answer never decides anything. The copy, the switch
and the end read it again, with backoff, until it answers; a mesh-primary's `mesh.unblock` reads the
`rdm.storage.committed` event again until it answers. The mesh-primaries stay blocked meanwhile, and
every node keeps serving. A provider that answers the copy with a named error (it refuses, or holds what
this executable does not recognise) fails the copy by name, once (`via-copy` `outcome = failed`); the
change fails once the fabric-primary has read that no `rdm.storage.committed` event exists for its
`change_id`, and every mesh-primary is unblocked on the old provider (cell:
`a_provider_that_refuses_its_list_fails_the_copy_and_unblocks_on_the_old_provider`).

What the person on call does:

1. Read the last span (the table above) and its `cause` or `reason`: it names the provider and the
   store that did not answer.
2. Bring the provider back: its process, its network, its credentials. RDM holds the change at its
   step and carries on once the provider answers; nothing is resubmitted.
3. If the provider cannot come back, the change cannot complete. `fabric.delete` is the one Build
   admitted during a change ([fabric-delete.md](../../architecture/lifecycles/fabric-delete.md)): it
   reads the change's `rdm.storage.committed` event, sends every mesh-primary `mesh.unblock` onto the
   provider the change decided, ends the change `failed` (`interrupted by fabric.delete`) and keeps the
   records on both providers (cell: `a_delete_resolves_the_storage_change_before_teardown`).

No cell makes a provider stop answering mid-change; the procedure is read from
`rdm_admin::storage_change` (`StorageChanges::decide` and the switch).

## Restart and adoption during a change

The rule for every start during a change: **the decision is read only from the change's
`rdm.storage.committed` event** on the new provider's events store. Found, the change switched; none on
a provider that answers, it did not; no answer, neither, and the block holds. A launch environment, a
view or a lost answer never decides it.

| who starts | what happens | cell |
|---|---|---|
| a role node, killed while its mesh-primary is blocked | node recovery re-creates it at its `path` and `name` over the same data dir once its mesh-primary issues commands again; the new node is launched on, and writes only, the provider the `rdm.storage.committed` event decided | `topology_storage_state_survives_role_node_restart` (both a switched change and one failed before its switch), `a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read` |
| the fabric-primary | its successor reads the change's events: an `rdm.storage.started` with no `rdm.storage.ended` is a change in progress, and it resumes it (`via-started` `outcome = resumed`); its switch first reads the `rdm.storage.committed` event (`via-switch` `outcome = found`, or a copy that appends only the lines the new provider lacks, then the append) | `a_lost_fabric_primarys_successor_resumes_the_change_from_its_events` |
| a node-admin started again on its own data dir with no launcher | it opens the provider its own row in its data dir names, whatever its environment names; the own row records the provider at the node's start and again at each move, before the move answers | `a_restart_without_a_launcher_writes_the_provider_its_own_row_names` |
| nothing alive: the adoption launch ([fabric recovery, Nothing alive](../../architecture/lifecycles/fabric-recovery.md#nothing-alive)) | on an inherited data dir, the adoption launch opens the provider its own row names; on a data dir holding no own row, the one its environment names | none |

**The adoption launch mid-change, on a data dir holding no own row.** Before starting it, decide which
provider holds the fabric's latest records by the `rdm.storage.committed` event alone:

1. Read the `change_id` of the change: the caller's answer, any span of the change, or the
   `rdm.storage.started` event without a matching `rdm.storage.ended` on the old provider's events
   store (on a file provider, a line of `events.jsonl` whose `type` is `rdm.storage.started`).
2. Look for the change's `rdm.storage.committed` event on the new provider's events store. On a file
   provider: a line of `<new provider's root>/events.jsonl` whose `type` is `rdm.storage.committed` and
   whose `data.change_id` is the change's.
3. Present: the change switched; start the adoption launch with
   `RDM_TOPOLOGY_STORAGE_PROVIDER=<new provider>`. Absent on a provider that answers: it did not;
   start it with the old provider. The provider does not answer: wait for it; do not guess.
4. The adopted fabric-primary resumes the change, which reads the same event.

## Switched, or failed before the switch

| | switched | failed before the switch |
|---|---|---|
| `rdm.storage.committed` event on the new provider | present | absent, read on a provider that answers |
| `via-switch` | `inserted` or `found` | `not-switched` (`commit_found = false`), read before the change fails |
| `rdm.storage.ended` event | `state` `complete`, on the new provider | `state` `failed`, naming why, on the old provider |
| `fabric.get` | the new provider, the old one previous | the old provider, previous unchanged |
| each node's row | the new provider | the old provider |
| `via-unblock` `provider` | the new provider | the old provider |
| the new provider | the fabric's records, written from here on | closed; holds what the copy appended, never read by the fabric |
| the old provider | kept as it was at the switch, written no more | the fabric's records, written as before |

A failed change leaves nothing to undo: every node writes the old provider, and a new
`fabric.storage.update` naming the new provider can be sent once its cause is fixed.

Cells: `a_customer_changes_topology_storage_without_restarting_members` (switched),
`a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider` (failed before the switch).

## Refused history

The copy appends every line of every store on the old provider to the new one in source order, and the
new provider must hold a prefix of the old one's lines in each store. When it holds a line the old one
does not, or a line out of order, the copy is refused by name and nothing switches:

```text
the new topology storage provider `<new>` is not a copy of the old one `<old>` in source order: <why>; nothing is appended and nothing switches
```

It is on the `via-copy` span (`outcome = refused`, `cause`) and in the reason of the change's
`rdm.storage.ended` event. The change then fails before the switch, as above.

What the person on call does: the new provider's place holds records that are not this fabric's history
(another fabric's, an earlier test's, a line written by hand). RDM deletes nothing. Either clear that
place by the application's own work, or register a fresh place under another name, then send the
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
- Returning to it: a `fabric.storage.update` naming it again is a change, which copies into it only the
  lines it lacks ([how-to §6](how-to.md#6-return-to-a-previous-provider)).
- Removing it: the application's own decision, never RDM's. A later return to a removed provider copies
  every line again.

Cells: `a_previous_provider_remains_unchanged_after_cutover`,
`a_customer_returns_to_a_previous_topology_provider`.

## A node that refuses its move

In `mesh.unblock` a mesh-primary moves each node of its mesh with `StorageStep` (node command op
`0x22`). A node that will not move, because it refuses its move by name (it cannot open the provider,
`via-move` `outcome = refused`, `reason`), refuses it `rejected-stale-change`, or leaves its call
unanswered with its `Send` spent, is re-created at its `path` and `name` on the provider the change
decided by its mesh-primary's node recovery, never part of the change (lead decision D1). The
mesh-primary replies `unblocked` once every node of its mesh moved or was re-created.

What to read:

- `rdm.node_admin.node.update.via-storage-move-lost`, `outcome = recovery-submitted`, `reason` naming why
  the node would not move, on the same trace as its mesh's `via-unblock`;
- the mesh's `via-unblock`, `recreated` counting the paths re-created;
- `mesh.nodes.get`: the path holds a new node id, ready, writing the new provider; the old node's
  records stay where they were.

Nothing to do when it completes. A path whose re-created nodes keep refusing (a new node that refuses
again) holds its mesh's unblock: no timer ends it. Read the new node's start refusal: an executable
that does not register the provider, or a place the node cannot reach. Under the container provider, a
node whose container does not mount the provider's place is re-created without end (lead decision
D14); the testkit's container cells are ignored by name until its mount is built (i149 R15).

Cells: `a_node_that_refuses_its_move_is_recreated_on_the_new_provider`,
`a_node_that_never_unblocks_is_recreated_on_the_new_provider` (a node frozen, alive and silent).

## Writes held while blocked, and what a caller does

- **Rows and status declarations** (lead decision D13). Once a mesh-primary has replied `blocked`, its
  row writer holds its writes, owed, and writes them at the unblock on the provider current then; a
  status a node reports while blocked is applied and answered once its row is written after the
  unblock. Nothing of the mesh is written to the old provider after `blocked`, so the copy holds every
  line. Cells: `a_row_change_after_blocked_is_held_and_written_at_the_unblock`
  (`rdm-admin`, `admin`), `a_status_a_node_reports_while_blocked_is_answered_once_written_after_the_unblock`
  (`rdm-admin`, `status_rpc`).
- **A node's connection events** are owed on the node's one writer queue (Q169) and never block an open:
  each lands once. Cell: `a_connection_event_during_a_change_lands_once_after_the_unblock`.
- **A caller's command** is answered only once its line lands. A caller whose call ended without an
  answer reads the row (`node.get`) before sending it again: the command may have landed at the unblock.
  Cell: `a_customer_changes_topology_storage_without_restarting_members` (`node.config.update` during the
  change, answered, its row on the new provider alone).
- **An older change's command** reaching a node after a newer change moved it is refused by name,
  `rejected-stale-change`, naming the newer change; it never moves the node. Cell:
  `an_old_change_cannot_unblock_writes_for_a_new_change`.
