# The fabric's `topology_storage_provider`: a Fabric property, changed live by a Build

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
- It is a Build: the Build's topology carries the fabric's `topology_storage_provider` as a new property, the
  meshes unchanged.
- `topology_storage_provider` is a property of the Fabric, written on the Fabric's row, so it can be queried;
  and of the node's row, the provider the node writes.
- The name is `topology_storage_provider`, not `storage_provider` (Luke, 2026-10-26): a node has storage
  of its own, a node-admin's themes and images, a broker's and gateway's WAL, under its
  `storage_persistence`, and the application may have providers for those; this property names only
  where RDM's five record stores go, fabric, meshes, nodes, connections and builds, which is what the
  topology reads read. Not `fabric_storage_provider`: `fabric.storage` is one of the five, the Fabric
  record's own store. RDM's backend keeps its name, `file`.
- Block changes during migration: finish outstanding writes, copy the stable records, switch
  providers, then unblock changes. Reads and serving processes continue.
- The previous provider's data is never deleted by RDM.
- The node's `storage` property is renamed `storage_persistence`.

## Steps

### The properties

- **R-SP1. `topology_storage_provider` is a property of the Fabric.** One value for the fabric, flat (Q166): the
  name of a storage backend the application's wiring registers. RDM's file backend is registered by
  every binary as `file`. A Build's topology that carries no `topology_storage_provider` reads as the
  Fabric row's current value, unchanged, as every property a Build omits does (Q166); the only default
  is Day 0's, the bootstrap admin's environment, `file` when unset. A name is
  bound to its place at registration: `file` is the directory `RDM_STORE_DIR` names, the application's
  backend its own addresses; the node that writes never names a place, it hands the record to the
  trait and the backend behind it knows where (storage-is-persistence-only, Q109). A move between two
  places of one backend is therefore two registered names, or a value that carries the place
  (`file:<dir>`); which, is a line for Luke.
- **R-SP2. The Fabric's row carries it.** The Fabric record gains `topology_storage_provider` (the current one)
  and `previous_topology_storage_providers` (each one the fabric left, in order), written by the fabric-primary,
  and the Fabric row's read (`fabrics.get` under Q167's amendment) answers both. The bootstrap admin
  writes the first value on Day 0: the name of the backend it was started on, `file` unless its
  environment names another. build.md §5.3's sentence that `fabric.storage` holds the Fabric "as an
  immutable identity row, `FabricIdentity { fabric_id, name }`, written once" already differs from the
  code, whose record carries `events` and the deployment `provider`; the page text is proposed under
  "Pages" for Luke's yes, with this property in it.
- **R-SP3. The Build's topology carries it, and the application submits the Build that meets the
  requirement.** `FabricTopology` (`accepted.rs:161`, today `fabric` and `meshes`) gains
  `topology_storage_provider`. A change of storage is a `build.create` of the complete topology with the same
  meshes and the new `topology_storage_provider`. It is admitted and refused like any Build (one in progress at
  a time, build.md:174), its attempts claimed, its receipts written and resumed from like any Build's.
  The requirement of "Why" is met by the application: once its own store exists (its brokers run and
  its Day 0 is done), it submits `build.create` naming its backend; nothing in RDM knows when that
  is. A provider is code, compiled into an executable and registered in its wiring, so a node can
  move only between the providers its own executable registers, and the fabric-primary's wiring is
  not every node's: acceptance refuses by name a provider the fabric-primary's wiring does not
  register or cannot open, nothing moved, and step 1 (R-SP6) is the check on every other node. A
  provider no executable has is an executable replacement, not this Build.
- **R-SP4. `topology_storage_provider` is a property of the node's row too: the provider it writes.** The
  fabric's value is the intent, the node's is the fact, and during a change they differ, which is the
  progress. The node-properties table gains the row:

  | field | set by | changed by | event |
  |---|---|---|---|
  | `topology_storage_provider` (and, during a change, the step reached) | the launching admin, from the Fabric row and the Build in progress, at the birth | the fabric's storage-change Build, step by step: the node writes its own row before it answers the step; never a put | the step's receipt |

  A caller reads it with `nodes.get [topology_storage_provider: file]`, every node still on the old provider
  during a change. The Build journal is itself in the provider, so a starting node cannot learn its
  provider from a Build; it is told, in this order:
  - at launch, by the launching admin, in its environment (`RDM_TOPOLOGY_STORAGE_PROVIDER`, the way
    `RDM_STORE_DIR` and `RDM_ROOTS` are written), the current provider or, during a change, both
    providers and the step the fabric is at, read from the Fabric row and the Build in progress, since
    the Fabric row's value moves only at the end (R-SP9);
  - from its own row in its data dir, beside `node-key` and its held name, which it reads second; a
    start with no launcher, the adoption launch, reads only this.
  A running node never re-reads its environment: a change reaches it as the step's command, it does
  the step's work, writes its own row with the provider or providers it now writes and the step
  completed, and only then answers; the receipt exists only after the row does. A node that crashes
  inside a step comes back one step behind at most, the node reconciles its launcher's value with its durable migration run before joining. Blocked changes cannot be overridden by launch environment; the node resolves that run's decision first
  and runs any missed step before its join (R-SP10).
- **R-SP5. Every node's storage can be replaced while it runs.** The handles a node's storage opens
  (an admin's fabric, meshes, nodes, connections and Build journal; every other node's connections)
  are held behind one swappable handle per node, loaded on every call, that a step replaces, so every
  reader and writer of that node moves at once and the writer's cache above it never knows. Nothing
  else in the node holds a storage handle of its own. This is the refactor the proposal costs: every
  site that holds an `Arc<dyn …Storage>` today, in rdm-admin and in rdm-node's `ConnectionsWriter`,
  loads it through the swap. A backend is opened off the boot path by the same registered opener boot
  uses. A node never knows where it writes: every trait method takes the record and nothing else.

### The change

The final contract is [Storage §2](../../architecture/storage.md#changing-it). The sequence is
**block changes, finish outstanding writes, copy, switch providers, unblock changes**. There is
no background-copy stage and no separate node lifecycle state. The expected short duration is
measured, never used as a safety timeout.

- **R-SP6. Check and block.** Check that each executable opens the destination, block new record
  changes and claim operations, and finish every already-issued write and claim on the old provider.
  Commands travel fabric-primary → mesh-primary → node. Every live writer confirms before copying
  starts; a missing writer must be found `exited`, never inferred unable to write from silence.
- **R-SP7. Copy the stable source.** The fabric-primary copies every store's complete history in
  source order, including dead writers' records, then transfers all name claims and in-flight
  attempts with their exact holders. Verify durable copies and claims. Only append after a matching
  destination prefix; refuse holes and conflicting history. Retry from verified copy progress.
- **R-SP8. Switch.** Insert the run's exclusive commit decision, update the Fabric row on the new
  provider and switch each node's storage handle. Record the provider before each confirmation.
  Changes remain blocked until all participating nodes have switched or their exact old writer is
  found unable to write. Reads continue against the selected provider throughout.
- **R-SP9. Unblock and complete.** Waiting changes write to the new provider, with their answers
  following durability. Complete the Build after the nodes confirm. Observed connection events
  stay owed on the node's writer queue (Q169) and land after the unblock; an open never waits.
- **R-SP10. Restart and new births.** Persist the migration identity, blocked changes and both
  provider names before confirming. Restart resolves that same migration before ordinary writes;
  an older launch environment cannot unblock changes. A new birth joins the current migration
  before issuing writes and is included in its writer confirmations.
- **R-SP11. Keep the previous provider.** It remains unchanged after commit, named in the Fabric
  row, and available to the fabric-primary. RDM deletes none of its records. Going back is another
  Build using this same blocking procedure.
- **R-SP12. Serving state does not change.** Nodes, meshes and the fabric stay ready-for-traffic.
  Progress is the Build's receipts and provider properties. This blocks storage changes, not nodes.
- **R-SP13. Resolve failure by the commit record.** One insert-and-fail `commit` record for
  `(build_id, change_id)` on the new provider is the switch. Found, the change is switched and the
  Fabric row's line is written if missing; none on a provider that answers, not switched; a provider
  that does not answer is neither, and the block holds. A timeout never means not switched. No Build
  but fabric.delete is admitted during a change. A step failing before the switch fails the Build
  once no `commit` is read, and every node unblocks on the old provider. A node that will not block
  or unblock is investigated and re-created on the provider of its step (Q182). Delayed messages of
  an older change_id are refused by name. fabric.delete reads the record before it writes.
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
  R-SP13).
- fabric-mesh-node-lifecycles.md: the storage change's row.
- node-admin-lifecycle.md §7: the store is the fabric's `topology_storage_provider`, changed by a Build.
- CLAUDE.md env table: `RDM_TOPOLOGY_STORAGE_PROVIDER` (R-SP4).
- node-rpc-envelope.md: the Fabric row's read carries the two fields (R-SP2); the node-properties
  table gains R-SP4's row and renames R-SP16's.
- states.md, "Resize and executable replacement": `topology_storage_provider` in the property list (R-SP12).

## Cells

- **Properties and opening:** public Fabric/node reads carry the selected provider; omission retains
  the current provider. Unsupported registrations fail by name before records move.
- **Blocking:** a change submitted during migration waits. An outstanding write or claim prevents
  copying until it has finished durably. A delayed writer confirmation holds the copy. Reads and
  serving states continue; measure the migration's blocked duration in the smoke test.
- **Copy:** preserve every record and current value in source order, including dead writers and
  deleted entities. Transfer held names and attempts unchanged. A destination with operations 1 and
  3 but missing 2 is refused without making 2 current. Lost copy answers do not duplicate records.
- **Switch and unblock:** waiting changes remain blocked until participating nodes switch; they
  then use the new provider and answer only after durability. No ordinary write resumes on the old
  provider. All serving identities and states remain unchanged.
- **Switch:** lose the commit answer; the record read decides. Kill the fabric-primary between the
  commit and the Fabric-row write; its successor finishes switching. A provider that does not answer
  keeps changes blocked. Fail a step before the switch; every node unblocks on the old provider.
- **Restart:** restart after changes are blocked with an older launch environment; resolve the same
  decision before writing. An observed connection event while blocked lands once after the unblock. Include
  new births in the writer confirmations before allowing their writes.
- **Admission and lost nodes:** a Build other than fabric.delete is refused during a change. Old
  commands cannot switch or unblock a later change. A node that never acknowledges is re-created on
  the provider of its step. Delete reads the commit record before teardown.
- **History and return:** the old provider is unchanged after commit; a later go-back Build uses the
  same procedure. Full public reads, including config filters, preserve their results.

## The mechanisms considered before this one

Each was weighed in the same conversation and set aside, for the reason given.

1. **Restore state-sync, state-commit and the scratchpad** (a withdrawn draft arm). Day-0 rows held in
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
   The current contract blocks changes before copying; it retains live serving processes without
   dual writes or a catch-up stage.
10. **A switch without blocking changes** (this proposal's second shape, 4011b5c58: each node switches as
    soon as its tail is copied, claims cut over by a `claims-closed` row, the change completing
    forward past it). Set aside on Luke's heads-up: from the first switch on the two providers
    diverge, a moved writer's lines exist on the new provider alone, and a change stuck after the row
    can neither finish nor go back without losing them, so both providers' data would have to go. The current contract blocks changes before copying and switches only after the stable copy is
    verified; it does not use this background-copy design.

## What this proposal does not decide

- How the application's backend is built or what it stores (Q65: RDM's boundary contract is the how,
  the application decides who).
- Retention of a store's lines, on either provider: the store-root proposal's measured trigger.

The earlier copy-while-writing design with final tails is also superseded. The complete migration
blocks changes from before the copy until the switch is confirmed; it needs no catch-up phase.
