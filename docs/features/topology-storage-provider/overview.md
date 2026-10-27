# Topology storage provider: overview

`topology_storage_provider` names the backend RDM's five record stores are written to. An
application starts its fabric on a backend that exists on Day 0 (`file`) and, once its own store
exists, moves RDM's records there with one Build, while every node keeps serving. The contract is
[storage.md §2](../../architecture/storage.md#2-topology_storage_provider-where-rdms-records-go);
this page maps it onto the code and the cells that prove it. The [how-to](how-to.md) shows the calls,
the [runbook](runbook.md) what to read and do when a change does not end the way it should.

Every cell named here is in `crates/rdm-test/tests/fabric/topology_storage_provider.rs` unless another
file is named.

## What it is

| | where | written by | read with |
|---|---|---|---|
| the fabric's provider (the intent) | the Fabric row, `topology_storage_provider`, beside `previous_topology_storage_providers` (each provider the fabric left, in order) | the bootstrap admin on Day 0, the fabric-primary at the switch | `fabric.get` (`FabricView`) |
| a node's provider (the fact) | the node's row, `topology_storage_provider` | RDM, at the node's start and at each move; never a caller | `mesh.nodes.get`, `fabric.nodes.get`, `node.get` (`NodeView`) |

During a change the two differ; that difference is the change's progress. Cell:
`a_customer_reads_each_nodes_topology_provider` (both read on a two-mesh fabric, beside
`storage_persistence`, a separate property of the node: [storage.md §1](../../architecture/storage.md#1-storage_persistence-a-nodes-own-storage)).

The five stores are `fabric.storage`, `mesh.storage`, `nodes.storage`, `connections.storage` and
`builds.storage`. An application's own rows, events and data dirs are not among them.

## Providers and registration

A provider is a backend implementing the five store traits, compiled into an executable and
registered in its wiring under a name. A name is bound to its place at registration: two places of
one backend are two names. A node moves only between providers its own executable registers.

| crate | type | role |
|---|---|---|
| `rdm-admin` | `storage_backend::TopologyStorageProviders` (`file(store_dir)`, `register(name, backend)`) | a node-admin executable's providers, handed to `rdm_admin::entry::run_on` |
| `rdm-admin` | `storage_backend::TopologyStorageHandle` | the admin's one access point: every reader and writer of the admin reaches the stores through it, and a move replaces the provider beneath it (`replace_for`) once every write admitted to the old one is answered there |
| `rdm-node` | `storage_backend::NodeStorageProviders` (`file()`, `register(name, backend)`) | a role node executable's providers; the node opens the one its launch names |
| `rdm-node` | `storage_move` | a node's move: the provider beneath its handle replaced on its mesh-primary's command |
| `rdm-node` | `storage_copy::copy` | the copy, in source order, with every name claim and in-flight row |
| `rdm-admin` | `storage_change::StorageChanges` | the change's steps: `mesh.block`, the copy and the switch, `mesh.unblock`, the verdict |
| `rdm-core` | `launch::ENV_TOPOLOGY_STORAGE_PROVIDER`, `build::StorageChange`, `build::StorageCommit`, `ChangeId` | the launch variable, the change and its `commit` record's identity |
| `rdm-client` | `Change::Reconcile { topology_storage_provider }`, `FabricView`, `NodeView`, `CallEnd` | what a caller sends and reads |

`file` is registered by every RDM binary: one append-only JSONL file per store under the fabric's
root in `RDM_STORE_DIR` (`<store dir>/<fabric id>/`), with its `layout` marker and `locks/`. A name
an executable does not register refuses that executable's start by name (`RDM_TOPOLOGY_STORAGE_PROVIDER
names topology storage provider `<name>`, which this executable does not register (it registers:
...)`).

The testkit registers a second provider, `test-target`: RDM's file backend at its own place beside the
store dir (`rdm_test::test_app::topology_storage_dir`), standing in for an application's backend in
every cell.

## Day 0 and every launch

| when | who sets it | from |
|---|---|---|
| Day 0 | the bootstrap admin, on the Fabric row | `RDM_TOPOLOGY_STORAGE_PROVIDER`, `file` when unset |
| every launch | the launching admin, into the node's environment | the Fabric row's, and while a storage change is in progress the provider its `commit` record decided |
| a start with no launcher (the adoption launch) | the node itself | its own row in its data dir; `RDM_TOPOLOGY_STORAGE_PROVIDER` only when the data dir holds none |
| a change | the application, by `build.create` naming the new value | the Build; a Build that omits it leaves it unchanged (Q166) |
| the end of a change | the fabric-primary, on the Fabric row | the `commit` record |

Cell: `an_omitted_provider_preserves_the_current_provider` (a fabric started on `test-target` writes
nothing to the file provider's store dir, and a later Build that omits the property keeps it).
Cell: `a_restart_without_a_launcher_writes_the_provider_its_own_row_names` (a node-admin started again
on its own data dir with no launcher and no `RDM_TOPOLOGY_STORAGE_PROVIDER` after a change writes the
provider its own row names).

## The request path

A storage change is a `build.create` of `Change::Reconcile` carrying the complete topology with the
same meshes and the new `topology_storage_provider`. A request naming the provider the fabric already
writes is no change. Acceptance:

1. refuses by name a provider the fabric-primary's wiring does not register,
   `topology-storage-provider-not-registered`, or cannot open, `topology-storage-provider-not-opened`,
   nothing accepted (cells: `an_unsupported_provider_changes_nothing`,
   `a_provider_that_does_not_open_changes_nothing`);
2. refuses any other Build while a change is in progress, `build-in-progress` naming the change's
   Build, a change back included; `fabric.delete` alone is admitted (cell:
   `a_build_other_than_fabric_delete_is_refused_during_a_change`);
3. mints the change's `change_id` and records it with the accepted Build before its first command.

The change runs as four steps, each an operation of the change's Build, claimed and dispatched by the
fabric-primary (`build.attempt.run`), fabric-primary → mesh-primary → node, each ending in its receipt:

| step | operation | run by | what it does |
|---|---|---|---|
| 1. Block | `mesh.block:<mesh>` | each mesh-primary | waits until its in-flight commands complete, replies `blocked`, gossips its mesh's topology; issues no new command until unblocked |
| 2. Copy | `storage.switch:<change_id>` | the fabric-primary, once every mesh-primary replied `blocked` | appends every line of every store on the old provider to the new one in source order, every name claim and in-flight row with its exact claimant |
| 3. Switch | `storage.switch:<change_id>` | the fabric-primary | inserts `commit` for `(build_id, change_id)` on the new provider with the insert-and-fail primitive, then appends the Fabric row's line naming the new provider and the old one previous |
| 4. Unblock | `mesh.unblock:<mesh>` | each mesh-primary | moves each node of its mesh onto the provider the `commit` record decided (node command op `0x22`, `StorageStep`), replies `unblocked`, issues commands again |
| verdict | `storage.change:<change_id>` | the fabric-primary | Complete once every mesh-primary replied `unblocked` and the record names the change switched; Failed, naming why, when it is not |

The wire of the move is on [node-rpc-envelope.md, NodeCommand op 0x22](../../architecture/node-rpc-envelope.md#nodecommand-op-0x22-rdm).
Cell: `a_customer_changes_topology_storage_without_restarting_members` (the four steps on one trace,
every line carried in source order, the deleted birth's and the deleted mesh's included).

## The persistence path

- **The `commit` record is the decision.** Whoever resumes a change, a successor fabric-primary or a
  mesh-primary's `mesh.unblock`, learns whether it switched only from that record: found, switched;
  none on a provider that answers, not switched; no answer, neither, and it reads again. On the file
  provider the record is `<new provider's root>/locks/commit/<build_id>.<change_id>`. Cells:
  `a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read`,
  `topology_storage_state_survives_role_node_restart`.
- **The previous provider is kept** as it was at the switch, named on the Fabric row, written no more
  and never deleted by RDM. Its own Fabric row still names it, with no previous provider. Cell:
  `a_previous_provider_remains_unchanged_after_cutover`.
- **A return copies only what is lacking.** A later Build naming a previous provider copies into it
  only the lines it lacks. Cell: `a_customer_returns_to_a_previous_topology_provider`.
- **Held writes (lead decision D13).** Once a mesh-primary has replied `blocked`, nothing of its mesh
  writes the old provider: a row change or a status a node reports while it is blocked is held, owed,
  and written at the unblock on the provider current then. Cells (rdm-admin inline tests):
  `a_row_change_after_blocked_is_held_and_written_at_the_unblock` (`crates/rdm-admin/src/admin.rs`),
  `a_status_a_node_reports_while_blocked_is_answered_once_written_after_the_unblock`
  (`crates/rdm-admin/src/status_rpc.rs`).
- **A caller's command during a change** is admitted and issued once its mesh-primary is unblocked; a
  `node.config.update` sent while blocked is answered, and the row it writes lands on the new provider
  alone (cell: `a_customer_changes_topology_storage_without_restarting_members`).
- **A node's own connection events never wait** (Q169): one written while its mesh-primary is blocked
  lands once on the provider the node writes, and the copy carries it once (cell:
  `a_connection_event_during_a_change_lands_once_after_the_unblock`).

## The failure paths

| what | what RDM does | cell |
|---|---|---|
| a provider no executable registers | refused at acceptance by name, nothing accepted | `an_unsupported_provider_changes_nothing` |
| the new provider holds a line the old one does not | the copy is refused by name, nothing switches; the Build fails once no `commit` is read, and every mesh-primary is sent `mesh.unblock` on the old provider; the Fabric row unchanged | `a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider` |
| a provider that does not answer | the copy and the switch read it again until it answers; the block holds; no timer ends it | none (see the [runbook](runbook.md#an-unavailable-provider)) |
| a node that refuses its move (lead decision D1) | its mesh-primary re-creates it at its `path.name` on the new provider, steps of its `mesh.unblock` attempt of the change's own Build, and replies `unblocked` once every node moved or was re-created | `a_node_that_refuses_its_move_is_recreated_on_the_new_provider` |
| a node that never answers its move | the same: re-created on the new provider once its move is unanswered past the staleness floor | `a_node_that_never_unblocks_is_recreated_on_the_new_provider` |
| a mesh-primary that does not reply `blocked` or `unblocked` | a lost node: [node investigation](../../architecture/lifecycles/node-investigation.md), and on any verdict but `reached` node recovery re-creates it at its `path.name`, an attempt of the change's own Build | none |
| a node killed while its mesh-primary is blocked | re-created at its `path.name` over the same data dir once its mesh-primary issues commands again, writing only the provider the `commit` record decided | `topology_storage_state_survives_role_node_restart` |
| a command of an older change | refused by name, `rejected-stale-change`, naming the change the node acted on | `an_old_change_cannot_unblock_writes_for_a_new_change` |
| `fabric.delete` during a change | admitted; it resolves the change by reading its `commit` record before its teardown; the records on both providers kept | `a_delete_resolves_the_storage_change_before_teardown` |
| a re-created node whose container cannot reach the new provider (lead decision D14) | re-created again: no timer ends a block | the container cells, ignored by name (R15) |

## Surface map

| operation | Node RPC | rdm-client | shared core |
|---|---|---|---|
| change the provider | Build op `0x20`, `build.create` of `Reconcile` | `BuildCarrier::create(Change::Reconcile { .. })`, followed with `follow_with` | the accepted topology and the Build executor |
| read the fabric's provider | op `0x1B`, `fabric.get` | `Fabrics::get` → `FabricView` | the Fabric row |
| read each node's provider | op `0x1B`, `mesh.nodes.get`, `fabric.nodes.get`, `node.get` | `Meshes::nodes`, `Fabrics::nodes`, `Nodes::get` → `NodeView` | the node rows |
| read the change's receipts | Build op `0x20`, `build.get` | `BuildCarrier::get` / `Builds::get` → `BuildReceipts` | the Build journal |
| move one node | node command op `0x22`, `StorageStep` | none: the mesh-primary's alone | `rdm_node::storage_move` |

## Spans

| span | attributes | outcome |
|---|---|---|
| `rdm.node_admin.build.reject.via-topology-storage-provider-not-registered` | `detail` names the provider and the registered ones | the refusal at acceptance |
| `rdm.node_admin.mesh.update.via-block` | `build_id`, `change_id`, `mesh`, `node_id`, `in_flight` | `blocked` |
| `rdm.node_admin.build.update.via-storage-copy` | `build_id`, `change_id`, `source`, `destination`, `source_position`, `copied`, `cause` | `copied`, `refused`, `indeterminate` |
| `rdm.node_admin.build.update.via-storage-switch` | `build_id`, `change_id`, `provider`, `commit_found`, `reason` | `inserted`, `found`, `not-switched`, `no-answer` |
| `rdm.node_admin.mesh.update.via-unblock` | `build_id`, `change_id`, `mesh`, `node_id`, `provider`, `moved`, `recreated` | `unblocked` |
| `rdm.node_admin.node.update.via-storage-move-lost` | `build_id`, `change_id`, `mesh`, `node`, `node_id`, `incarnation_id`, `provider`, `reason`, `error` | `recreated`, `not-recreated` |
| `rdm.node.build.update.via-storage-move` | `build_id`, `change_id`, `node_id`, `provider`, `reason`, `held` | `moved`, `refused`, `rejected-stale-change` |
| `rdm.node_admin.build.update.via-storage-open` | `build_id`, `change_id`, `node_id`, `provider` | `opened`, `refused: <why>` |

Every step is a span of the change's Build trace: the cell
`a_customer_changes_topology_storage_without_restarting_members` asserts the block, the copy, the
switch and the unblock share one trace id.

## Cross-references

- [storage.md §2](../../architecture/storage.md#2-topology_storage_provider-where-rdms-records-go), the contract.
- [build.md §5.3](../../architecture/build.md#53-node-admin-storage-boundaries), the node-admin's storage boundaries.
- [node-rpc-envelope.md, NodeCommand op 0x22](../../architecture/node-rpc-envelope.md#nodecommand-op-0x22-rdm), the move's wire.
- [node investigation](../../architecture/lifecycles/node-investigation.md) and [node recovery](../../architecture/lifecycles/node-recovery.md), a lost node.
- [fabric-delete.md](../../architecture/lifecycles/fabric-delete.md), the one Build admitted during a change.
- [i149 plan](../../plans/i149-topology-storage-provider.md), the requirements and their evidence.
