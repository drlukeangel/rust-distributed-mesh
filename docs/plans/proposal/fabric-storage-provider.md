# The fabric's `storage_provider`: a Fabric property, changed live by a Build

The lead, 2026-10-26, from Luke's direction in conversation (quoted under "Decided"), revised the same
day with Luke, Architect 1's two reviews and the dev's. A proposal: nothing here is built, and no
architecture page changes without Luke's yes on the exact text.

## Why

Q171 (688c32241) removed state-sync, state-commit and the scratchpad. The removed
`fabric-state-sync.md` stated a requirement that went with them: RDM's records end up in the store the
application establishes after its Day 0, which may not exist when the mesh comes up (a store on
Rafka's brokers exists only once the brokers run). The Q171 proposal described state-commit as
re-recording the topology on RDM's own file store root, and did not name that requirement. Luke was
not told; the requirement stands ("that requirement didn't disappear").

Today nothing can meet it:

- Every node opens its storage once, at its start, and holds it until it exits. An admin opens its
  `StorageBackend` at `admin/boot.rs:379` (`storage.open(&scope)`), RDM's file backend from `start` or
  the application's from `start_on`; every other node opens its `NodeStorageBackend`
  (`rdm-node/src/storage_backend.rs:21`) and writes its own connection rows through its
  `ConnectionsWriter` (`node.rs:471`).
- No record names the storage in use. The Fabric record (`fabric_storage.rs:107`) holds `fabric_id`,
  `name`, `events` and `provider`, and that `provider` is the deployment provider
  (`MESH_SPAWN_TYPE`). The file backend's location is `RDM_STORE_DIR`, an env var the launching admin
  writes into each node it launches.

So a running fabric cannot move to another storage, and no node can be asked which storage it writes.

## Decided (Luke, 2026-10-26)

- Records are written to storage, in the storage provider, from Day 0; there is no scratchpad.
  Storage is written to disk as JSONL: the store-root proposal's shape
  ([store-root-one-directory-per-entity.md](store-root-one-directory-per-entity.md), R-SR1 to R-SR5:
  one append-only file per store, every write a whole-record line with its `op`, nothing removed).
  This proposal is written against that shape.
- The storage provider can be changed while the fabric runs, without a node restart.
- The change is per node: every node writes its own records (its connections at least).
- It is made at the fabric level.
- It travels fabric-primary → mesh-primary → node, each level its own step.
- The fabric-primary keeps access to the previous storage providers.
- It is a Build: the Build's topology carries the fabric's `storage_provider` as a new property, the
  meshes unchanged.
- `storage_provider` is a property of the Fabric, written on the Fabric's row, so it can be queried;
  and of the node's row, the provider the node writes.
- The fabric-primary does the copy before any node switches; each node then switches with its own
  tail.
- The node's `storage` property is renamed `storage_persistence`.

## Steps

### The properties

- **R-SP1. `storage_provider` is a property of the Fabric.** One value for the fabric, flat (Q166): the
  name of a storage backend the application's wiring registers. RDM's file backend is registered by
  every binary as `file`. A topology that carries no `storage_provider` reads as `file`. A name is
  bound to its place at registration: `file` is the directory `RDM_STORE_DIR` names, the application's
  backend its own addresses; the node that writes never names a place, it hands the record to the
  trait and the backend behind it knows where (storage-is-persistence-only, Q109). A move between two
  places of one backend is therefore two registered names, or a value that carries the place
  (`file:<dir>`); which, is a line for Luke.
- **R-SP2. The Fabric's row carries it.** The Fabric record gains `storage_provider` (the current one)
  and `previous_storage_providers` (each one the fabric left, in order), written by the fabric-primary,
  and the Fabric row's read (`fabrics.get` under Q167's amendment) answers both. The bootstrap admin
  writes the first value on Day 0: the name of the backend it was started on, `file` unless its
  environment names another. build.md §5.3's sentence that `fabric.storage` holds the Fabric "as an
  immutable identity row, `FabricIdentity { fabric_id, name }`, written once" already differs from the
  code, whose record carries `events` and the deployment `provider`; the page text is proposed under
  "Pages" for Luke's yes, with this property in it.
- **R-SP3. The Build's topology carries it, and the application submits the Build that meets the
  requirement.** `FabricTopology` (`accepted.rs:161`, today `fabric` and `meshes`) gains
  `storage_provider`. A change of storage is a `build.create` of the complete topology with the same
  meshes and the new `storage_provider`. It is admitted and refused like any Build (one in progress at
  a time, build.md:174), its attempts claimed, its receipts written and resumed from like any Build's.
  The requirement of "Why" is met by the application: once its own store exists (its brokers run and
  its Day 0 is done), it submits `build.create` naming its backend; nothing in RDM knows when that
  is. A provider is code, compiled into an executable and registered in its wiring, so a node can
  move only between the providers its own executable registers, and the fabric-primary's wiring is
  not every node's: acceptance refuses by name a provider the fabric-primary's wiring does not
  register or cannot open, nothing moved, and step 1 (R-SP6) is the check on every other node. A
  provider no executable has is an executable replacement, not this Build.
- **R-SP4. `storage_provider` is a property of the node's row too: the provider it writes.** The
  fabric's value is the intent, the node's is the fact, and during a change they differ, which is the
  progress. The node-properties table gains the row:

  | field | set by | changed by | event |
  |---|---|---|---|
  | `storage_provider` (and, during a change, the step reached) | the launching admin, from the Fabric row and the Build in progress, at the birth | the fabric's storage-change Build, step by step: the node writes its own row before it answers the step; never a put | the step's receipt |

  A caller reads it with `nodes.get [storage_provider: file]`, every node still on the old provider
  during a change. The Build journal is itself in the provider, so a starting node cannot learn its
  provider from a Build; it is told, in this order:
  - at launch, by the launching admin, in its environment (`RDM_STORAGE_PROVIDER`, the way
    `RDM_STORE_DIR` and `RDM_ROOTS` are written), the current provider or, during a change, both
    providers and the step the fabric is at, read from the Fabric row and the Build in progress, since
    the Fabric row's value moves only at the end (R-SP9);
  - from its own row in its data dir, beside `node-key` and its held name, which it reads second; a
    start with no launcher, the adoption launch, reads only this.
  A running node never re-reads its environment: a change reaches it as the step's command, it does
  the step's work, writes its own row with the provider or providers it now writes and the step
  completed, and only then answers; the receipt exists only after the row does. A node that crashes
  inside a step comes back one step behind at most, the launcher's value is the fabric's and wins, and
  the node runs the step it missed before it answers its join (R-SP10).
- **R-SP5. Every node's storage can be replaced while it runs.** The handles a node's storage opens
  (an admin's fabric, meshes, nodes, connections and Build journal; every other node's connections)
  are held behind one swappable handle per node, loaded on every call, that a step replaces, so every
  reader and writer of that node moves at once and the writer's cache above it never knows. Nothing
  else in the node holds a storage handle of its own. This is the refactor the proposal costs: every
  site that holds an `Arc<dyn …Storage>` today, in rdm-admin and in rdm-node's `ConnectionsWriter`,
  loads it through the swap. A backend is opened off the boot path by the same registered opener boot
  uses. A node never knows where it writes: every trait method takes the record and nothing else.

### The change

- **R-SP6. Step 1, on every node: open.** Each node opens the new provider beside the old, writes
  nothing to it, and reports. A node whose executable does not register the named provider, or cannot
  open it, fails the step by name, naming the node and the provider; the Build fails before anything
  moved (R-SP13).
- **R-SP7. Step 2, on the fabric-primary: copy, then close claims.** The fabric-primary, the one
  reader of every store, reads every file on the old provider and appends every line to the new one in
  its order, recording per writer the last `op` it copied. Dead births' connection rows, deleted nodes'
  records and dead meshes' records are copied by it with the rest, so every retained record has a
  copier and no key is ownerless. Nodes keep writing the old provider during the copy; nothing is
  written twice. When the copy is done the fabric-primary inserts one row in the old provider,
  `claims-closed`, naming the Build: from that instant every claim is decided in the new provider, a
  claimant that meets the row retries there, and a claim won before the row keeps its lock, which the
  copy carried over. One decider before the row, one after, no window (Architect 1).
- **R-SP8. Step 3, on every node: switch, with its own tail.** Mesh by mesh under the Build's ordinary
  order, the fabric-primary's own records last, each node: stops writing the old provider; appends to
  the new one its own lines written after the `op` step 2 recorded for it, a short tail it alone
  wrote, in its own order, so no copied line ever lands after a newer one; swaps its handle so reads
  and writes go to the new provider; writes its own row; answers. Then it closes the old provider.
  Until a writer has switched, a node that has switched does not see that writer's lines since the
  copy on the new provider: a window the length of this step, holding recent rows of unmoved writers,
  and the only store reads of another writer's rows are a takeover's rebuild and an admin's start.
- **R-SP9. The end.** With the last receipt the fabric-primary writes the Fabric's row: `storage_provider`
  the new one, the old one appended to `previous_storage_providers`. The Build completes.
- **R-SP10. A node born during the change** is launched with both providers and the step the fabric is
  at, and runs that step before it answers its join; a step it missed is run by its mesh-primary's
  attempt as any planned birth's.
- **R-SP11. The fabric-primary keeps access to the previous providers.** Every provider the Fabric's
  row ever named is one the fabric-primary's wiring registers, so it can open it; it writes nothing to
  a previous provider and reads it for nothing in this proposal. Going back to a previous provider is a
  new Build naming it as the new one: step 2 copies from the current provider into it, every line it
  lacks by `op`, so its records stop at nothing. The access is the ability to open, nothing more.
- **R-SP12. The fabric stays ready-for-traffic and no node changes state.** states.md, "Resize and
  executable replacement": "A Build that changes only a node's `config`, `tags`, `storage` or
  `placement` is neither: it applies them as build.md §1 states, and the fabric stays
  ready-for-traffic." A storage change is a property Build, the meshes unchanged, and is the same kind:
  the fabric, every mesh and every node stay ready-for-traffic through all three steps; the node's
  states are its life and its traffic, and a change of where it writes is neither. The progress is the
  step receipts and R-SP4's row, not a state. The page's sentence gains `storage_provider` in its list.
- **R-SP13. Failure.** A failed step fails by name. Before the `claims-closed` row nothing reads or
  decides in the new provider, so a failure in step 1 or during the copy fails the Build as any step
  does: the old provider is whole, the new one is closed and written no more, and the Fabric row is
  unchanged. From the `claims-closed` row on the change completes forward, never reversed: a failed
  step ends its attempt and the fabric-primary opens the next, as it does when an executor is lost
  (build.md §4, "An attempt lost with its executor … leaves the Build in progress"), and the Build
  never reaches Failed past that row. That is one sentence build.md does not have today, since its
  rule is that a failed Build is skipped and never resumed on its own; it goes on the page under
  "Pages". `fabric.delete` during the change is admitted and interrupts the Build as fabric-delete.md
  states for any Build; the records on both providers stay, since RDM deletes nothing on disk.
- **R-SP14. Spans.** Each step spans its node, the provider it opens, switches to or closes, the step,
  the lines copied per writer and the last `op`, and its outcome, under the Build's step spans; the
  names follow the Build's span families and are listed in the landing. The open span of the
  store-root proposal (`rdm.store.open.via-fold`) carries the provider's name.
- **R-SP15. Record formats.** The Fabric record and the journal's topology record each gain a field; a
  record without them reads as `file` and no previous provider. No row, topic address or field moves,
  so no `durable format moved` marker applies (the marker's rule is a move; the dev's correction on
  the store-root proposal).
- **R-SP16. The node's `storage` is renamed `storage_persistence`, and `on_retire` `disposition`**
  (Luke, 2026-10-26). `StorageMeta` is `Ephemeral` or `Persistent { on_retire: Preserve | Release }`:
  the node's data-dir persistence, nothing to do with the fabric's backend, and with a provider on
  the node's row the name `storage` would mean two things. The property is `storage_persistence`,
  values `ephemeral` and `persistent { disposition: preserve | release }`, the page's own word for
  the field ("the declared disposition of the storage once the node is gone") and its own two
  values; `retire` is a banned word and goes with it. The three states stay the three
  (deployment-provider.md: "the three-state `StorageMeta` contract is RDM law"): ephemeral is about
  the node's births, fresh storage for every new node; the disposition is about storage that
  survived them, once the node is gone, preserved or released to the application's own removal,
  since "RDM deletes nothing on disk, ever". Ephemeral is not a disposition: there is no kept storage
  to dispose of. `storage` and `on_retire` are JSON field names on every node record and every
  Build's `NodeSpec`, so this rename is a descriptor field moved: its landing carries `durable format
  moved`, and a fabric born before it is a new fabric; the positional wire carries no names and is
  untouched. Pages: the node-properties table, build.md §1 and §5.3, build-deployment.md §2,
  node-replace.md, node-recovery.md and states.md where they say "storage or placement", the consumer
  guide's examples; Q166's row gains `storage` → `storage_persistence` as one amendment line. Its own
  landing, before or after this proposal's; this proposal's one requirement of it is that the provider
  field is never called `storage`.

## Pages (text for Luke's yes, written with the build)

- build.md §5.3: the Fabric row's sentence (R-SP2), and a section for the storage change (R-SP3 to
  R-SP13); build.md §1 or §4: the sentence that a storage change completes forward from its
  `claims-closed` row (R-SP13).
- fabric-mesh-node-lifecycles.md: the storage change's row.
- node-admin-lifecycle.md §7: the store is the fabric's `storage_provider`, changed by a Build.
- CLAUDE.md env table: `RDM_STORAGE_PROVIDER` (R-SP4).
- node-rpc-envelope.md: the Fabric row's read carries the two fields (R-SP2); the node-properties
  table gains R-SP4's row and renames R-SP16's.
- states.md, "Resize and executable replacement": `storage_provider` in the property list (R-SP12).

## Cells

- **The properties.** `fabrics.get` answers `storage_provider` and `previous_storage_providers`; a
  topology without the field reads as `file`; a Build naming an unregistered provider is refused at
  acceptance by name, nothing moved; `nodes.get [storage_provider: …]` answers each node's, and during
  a change the fabric's and a node's differ; a node's own row carries the provider and the step before
  the step's receipt exists.
- **Step 1.** A node whose executable lacks the provider fails step 1 by name, the Build fails, the old
  provider whole and the Fabric row unchanged.
- **Step 2.** The copy carries every line of every store, dead births' and deleted nodes' included,
  and records the last `op` per writer; competing claimants on opposite sides of the `claims-closed`
  row, one in the old provider just before it and one in the new just after, yield one winner, and a
  claim won before the row keeps its lock after it; a claimant meeting the row retries in the new
  provider.
- **Step 3.** A node's tail, lines written between the copy and its switch, lands once and in order
  after the copied lines; a writer's update overtaking nothing, since nothing is copied after a
  switch; a node that switched reading another writer's rows before that writer switches sees the
  copy and not the tail, and sees the tail once the writer has switched; the fabric-primary switches
  last.
- **Restart and birth.** A node restarted inside each step comes up at the fabric's step from its
  launcher's environment, its own row one step behind, and runs the missed step before its join; a
  node born during the change runs the step it is born at; the adoption launch reads the provider
  from the data-dir row alone.
- **Failure.** A failure before the `claims-closed` row fails the Build with the old provider whole
  and the new one closed; a failure after it opens the next attempt and the change completes, the
  Build never Failed; `fabric.delete` mid-change interrupts the Build and both providers' records
  stay.
- **Go-back.** A Build naming a previous provider copies into it only the lines it lacks by `op`.
- **State.** The fabric, every mesh and every node stay ready-for-traffic through a change; no node
  state transition is published.

## The mechanisms considered before this one

Each was weighed in the same conversation and set aside, for the reason given.

1. **Restore state-sync, state-commit and the scratchpad** (the draft Q182 arm A). Day-0 rows held in
   memory, written into the application's store at state-commit. Set aside: records are written to
   storage from Day 0, never held in a scratchpad (Luke), and it moves storage once, at Day 0, never
   while the fabric runs.
2. **Leave the switch to the application's backend**, its `on_fabric_ready` telling it to move.
   Set aside: `on_fabric_ready` is a notice on the fabric-primary alone, with no step and no check-in,
   while every node writes its own records; nothing would move the other nodes or confirm they moved.
   The application does keep one part: it submits the Build (R-SP3).
3. **Re-birth every node on the new provider**, each copying its own keys at its start. Set aside: a
   restart per node, each mesh dipping to pending while its node is re-born, and the single claim
   decider still needed; Luke: no restart.
4. **A caller op with its own primitive, the `fabric.root.update` shape**: `fabric.storage.update`
   with phases on its own durable row, and `node.storage.update` to each node. Set aside for the Build:
   the Build already carries a change fabric-primary → mesh-primary → node with ordered steps,
   receipts, resume after a lost executor and one change at a time, and an op of its own would build
   those again.
5. **The fabric-primary sending `node.storage.update` to every node through its mesh-primary as a
   forward** (Forward 0x1A, the `mesh.issuer.renew` → `node.issuer.update` shape). Set aside: the
   mesh-primary writes records of its own (its mesh's record and its nodes' rows), so it has a step of
   its own between its nodes' and the fabric's; a forward is for a target its sender cannot reach
   directly, and a mesh-primary reaches its nodes.
6. **The provider set per node in the Build's topology.** Set aside for one Fabric property: per node
   lets a fabric run on several providers indefinitely, and every reader would open each one.
7. **A `migrate` flag on the change.** Set aside: it says how to get there, not where. Step 2 copies
   what the new provider lacks and skips what it holds, which also covers a switch to a provider the
   application filled itself; a switch to an empty provider without a copy cannot be asked for.
8. **One pass, one step per node** (open the new provider, copy, switch reads and writes). Set aside:
   while nodes switch one at a time, a node that has switched writes where a node that has not still
   reads, and two providers decide the same insert-and-fail claim.
9. **Three fabric-wide steps with every node dual-writing** (this proposal's first shape, 1ff723b05:
   write to both and copy, move reads, stop the old writes). Set aside on the two reviews: step 2
   switched the claim decider node by node, so the fabric-primary and a mesh-primary could decide one
   mesh's in-flight row in two providers; a node's copy could land after a newer dual-written record,
   with no version to refuse it; a partial dual write had no defined recovery; records with no live
   writer had no copier; and a failed Build after step 2 contradicted build.md's failed-Build rule.
   The fabric-primary's copy first, one `claims-closed` row, and each node's own tail at its switch
   keep what that shape was for, no restart and no state change, without the dual write.

## What this proposal does not decide

- How the application's backend is built or what it stores (Q65: RDM's boundary contract is the how,
  the application decides who).
- Removing a previous provider's records: RDM writes nothing to it after the switch and deletes
  nothing.
- Retention of a store's lines, on either provider: the store-root proposal's measured trigger.
