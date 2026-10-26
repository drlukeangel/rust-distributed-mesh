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
- The fabric-primary does the copy before any node switches; the fabric-primary then copies the final tails in source order, under a freeze of writes, so that at every instant one provider holds the whole truth and the
  change is reversible until one commit point ("if we don't block it we will have to delete both
  storage providers' data").
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
  inside a step comes back one step behind at most, the node reconciles its launcher's value with its durable migration run before joining. A durable
  freeze cannot be overridden by launch environment; the node resolves that run's decision first
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

Each migration run has a durable `change_id`, recorded with its Build before any migration command.
Commands, freeze acknowledgements, copy progress and the decision carry that identity. Recovery of
an unfinished run reuses it, including when its decision is uncertain. After a confirmed abort and
completion of the rollback, resubmitting the same Build starts a new run with a new `change_id`.
Nodes retain the resolved run identity and refuse delayed commands from an older run by name;
accepting a new run does not erase that protection. A committed run only resumes release.

- **R-SP6. Step 1, on every node: open.** Each node opens the new provider beside the old, writes
  nothing to it, and reports. A node whose executable does not register the named provider, or cannot
  open it, fails the step by name, naming the node and the provider; the Build fails before anything
  moved (R-SP13).
- **R-SP7. Step 2, on the fabric-primary: copy, then close claims.** The fabric-primary, the one
  reader of every store, reads every file on the old provider and appends every line to the new one in
  its order, recording per writer the last `op` it copied. Dead births' connection rows, deleted nodes'
  records and dead meshes' records are copied by it with the rest, so every retained record has a
  copier and no key is ownerless. The copy is in source order per file, and its progress is a
  verified position per source file: the offset through which every line is durable and verified
  on the new provider in source order per key, across writer changes too. A lost copy receipt is
  reconciled against the already-landed prefix before continuing, never by blindly appending again. Nodes keep writing the old provider during the copy; nothing is
  written twice, and nothing is blocked: the copy takes the time it takes, under no freeze. Claims
  stay decided in the old provider throughout the copy.
- **R-SP8. Step 3: freeze, tails, commit, release.** The switch is blocked, as a freeze and never a
  refusal, so that one provider holds the whole truth at every instant (Luke: unblocked, a failed
  change leaves data on both sides and nothing to go back to).
  - **Freeze.** Each node closes admission to ordinary appends and claim operations for the
    `change_id`, drains already-issued appends and claim operations to a resolved durable outcome,
    and durably records the frozen state and both providers in its data dir before acknowledging.
    An indeterminate operation prevents acknowledgement. Restart reads this state before opening
    ordinary writes and resolves the same run's decision; stale launch environment cannot unfreeze
    it. Migration control records use the explicit migration path while ordinary writes stay blocked.
    Each node holds every
    write it would make from then on as owed. Nothing is refused, and nothing is acknowledged before
    it is durable: a caller's command that would write, and a node's declaration its mesh-primary
    would record, are answered only when their write has landed, under the one delivery rule, so the
    caller and the declarer retry until then and a frozen writer that dies has acknowledged nothing it
    did not write (Q54's rule, an Applied declaration durable before its answer, holds through the
    freeze). A fact a node observes rather than is asked for, a connection event, cannot be retried
    by anyone, so during the freeze the node appends it to a pending log in its own data dir, beside
    `node-key`, fsynced, and replays it at the release or at its restart; the log is empty outside a
    freeze. No claim is decided during the freeze: a claimant waits, as its write does (Architect 1).
  - **Tails.** Each frozen node records its final source positions and its own row, then acknowledges.
    The fabric-primary continues the ordered copy from its verified positions to the final positions,
    including departed writers' records. Nodes do not independently append unordered tails. Every
    writer must have acknowledged or be proven unable to write by node investigation's `exited`
    verdict; silence and a copied tail prove neither. For each key, the destination history must be
    a prefix of the source history before missing records are appended. Existing operations are
    verified by identity and content, never just a highest identity or set membership. A hole or
    reordered history is refused by name before cutover: an older missing record is never appended
    behind a newer record of that key. The pass verifies the completed order and current value of
    every key. It then transfers every held name claim and in-flight row naming the exact same
    holder, and reads both claim sets back equal. Only then may it decide.
  - **Commit.** The change's decision is one durable, mutually exclusive record on the new provider:
    `locks/change/<build id>/<change id>`, the store's insert-and-fail primitive, the one every provider
    implements for the in-flight row, holding `commit` or `abort` and the decider's birth. When the
    completeness pass holds, the fabric-primary inserts `commit`; only after that insert succeeded
    does it append the Fabric row's line on the new provider, `topology_storage_provider` the new
    one, the old appended to `previous_topology_storage_providers`, the line's `op` its own and its
    record naming the Build. The insert is the commit point and the claim cutover; the row is its
    consequence. Before the insert nothing but copies, tails and claims has reached the new provider,
    so a failure reverts (R-SP13); after it every writer's lines and every claim are on the new
    provider and only the release remains. The decision is known only by reading it: a successor, or
    the fabric-primary itself after a lost reply, reads `locks/change/<build id>/<change id>`. `commit` found is
    committed, and the successor appends the Fabric row's line if it is not there yet. Nothing found
    is not yet decided, and reverting is itself a decision: the successor inserts `abort`, and only
    an insert that succeeded permits the revert, because a commit request still queued at the
    provider, delivered after a recovery read answered absent, then meets `abort` and fails. A
    provider not answering is neither, so the freeze holds until it answers, and if it never does the
    change is a person's to end, by `fabric.delete` or a new fabric, never by a guess. A timeout never
    means "not committed", and an absent read never permits a revert on its own (Architect 1).
  - **Release.** Each node swaps its handle to the new provider, appends its owed writes there in its
    own order, answers its owed commands, and closes the old provider. Claims are decided in the new
    provider from here. A node that fails to release keeps its writes owed and retries; it is on the
    new provider and its row says so.
  The freeze lasts the tails and the commit, seconds; reads never block. The fabric-primary completes all final tails before the decision and releases first, so the
  Fabric row's writer is on the new provider before anyone reads it there.
- **R-SP9. The end.** With the last release receipt the Build completes. The Fabric row was written at
  the commit; no further write marks the end.
- **R-SP10. A node born during the change** is launched with both providers and the step the fabric is
  at, and runs that step before it answers its join; a step it missed is run by its mesh-primary's
  attempt as any planned birth's.
- **R-SP11. The previous provider is kept, frozen at the commit, and never deleted by RDM.** After the
  commit the old provider is a complete copy of the fabric as of the freeze, to the last tail, written
  no more and named on the Fabric row in `previous_topology_storage_providers` ("RDM deletes nothing on disk,
  ever", deployment-provider.md; never delete, only append). Removing it is the application's own
  work, as a node's storage with the `release` disposition is, or nobody's; RDM gives it the fact,
  the row and the Build's completion receipt, and does nothing further. Every provider the Fabric's
  row ever named is one the fabric-primary's wiring registers, so it can open it; the access is the
  ability to open, nothing more. Going back to a previous provider is a new Build naming it as the
  new one: step 2 copies from the current provider into it only the lines it lacks by `op`, which,
  frozen complete at its commit, are few.
- **R-SP12. The fabric stays ready-for-traffic and no node changes state.** states.md, "Resize and
  executable replacement": "A Build that changes only a node's `config`, `tags`, `storage` or
  `placement` is neither: it applies them as build.md §1 states, and the fabric stays
  ready-for-traffic." A storage change is a property Build, the meshes unchanged, and is the same kind:
  the fabric, every mesh and every node stay ready-for-traffic through all three steps; the node's
  states are its life and its traffic, and a change of where it writes is neither. The progress is the
  step receipts and R-SP4's row, not a state. The page's sentence gains `topology_storage_provider` in its list.
- **R-SP13. Failure.** A failed step fails by name and fails the Build as any step does; there is no
  forward-only exception. Before the decision nothing but copies, tails and claims has reached the
  new provider, so a failure in step 1, during the copy, or during the freeze and tails reverts, once
  `abort` is inserted in `locks/change/<build id>/<change id>` (R-SP8): the freeze lifts on the old provider,
  owed writes land there, the new provider is closed and written no more, the Fabric row is
  unchanged, nothing lost. A revert without the `abort` insert is not a revert. After the commit point every node has its tail on
  the new provider and the Fabric row names it; what remains is each node's release, which is an owed
  step a node retries until it lands, so a failure there is a node still releasing, never a Build to
  reverse: the Build stays in progress until the last release, as any Build whose attempt is lost
  with its executor does (build.md §4), and its resubmission reattaches. `fabric.delete` during the
  change is admitted and interrupts the Build as fabric-delete.md states for any Build; the records on
  both providers stay, since RDM deletes nothing on disk.
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

- **The properties.** `fabrics.get` answers `topology_storage_provider` and `previous_topology_storage_providers`; a
  topology without the field reads as `file`; a Build naming an unregistered provider is refused at
  acceptance by name, nothing moved; `nodes.get [topology_storage_provider: …]` answers each node's, and during
  a change the fabric's and a node's differ; a node's own row carries the provider and the step before
  the step's receipt exists.
- **Step 1.** A node whose executable lacks the provider fails step 1 by name, the Build fails, the old
  provider whole and the Fabric row unchanged.
- **Step 2.** The copy carries every line of every store, dead births' and deleted nodes' included,
  and records verified source positions; nodes write the old provider throughout and nothing is
  blocked; a claim during the copy is decided in the old provider.
- **Step 3.** A frozen node's connection fact, its mesh-primary's row write and a caller's config
  update are owed, not refused, and land on the new provider at the release in the writer's order; a
  node's tail lands once and in order after the copied lines; no write reaches the new provider
  between the last tail and the commit; a claimant during the freeze waits and is decided in the new
  provider after the release; the final tails preserve source order per key and the fabric-primary releases first; a node reading
  another writer's rows on the new provider after the commit sees every line; a node whose freeze
  command is delayed appends to the old provider after the completeness pass began, and the commit
  waits on its acknowledgement, so its line is in its tail; a writer silent with no `exited` verdict
  holds the commit with no timer; a writer with the verdict and lines behind is copied by the pass.
- **Completeness.** A destination holding operations 1 and 3 without 2 refuses the migration,
  leaves 3 current and does not append 2 behind it. An interrupted ordered copy resumes at its
  verified prefix without a duplicate; ownership passing from writer A to writer B preserves the
  source order and B's final value even when B reports its freeze before A.
- **Freeze durability.** An already-issued append delayed past the freeze command prevents the
  acknowledgement until it completes durably and is included in the final positions. Restart after
  acknowledgement retains the freeze even with an old launch environment; no ordinary append or
  claim runs before resolving the same change's decision.
- **Run identity.** Abort one run, finish rollback, and resubmit the same Build: a new change id can
  commit. Recovery of an uncertain decision keeps its original change id. Delayed freeze, release
  and abort commands from the resolved run cannot affect the new run.
- **The decision.** The commit request delayed at the provider, a recovery read answering absent,
  the successor inserting `abort`, then the original commit delivered: it fails on `abort` and the
  revert stands; the reverse order, `commit` inserted first and the successor's `abort` failing, the
  successor releases instead; the commit inserted and the Fabric row's line not yet written when the
  fabric-primary dies: the successor finds `commit` and writes the line.
- **Restart and birth.** A node restarted inside each step reconciles the launcher's environment
  with its durable run identity and freeze, resolves any pending decision, and runs the missed step
  before its join; a
  node born during the change runs the step it is born at; the adoption launch reads the provider
  from the data-dir row alone.
- **Failure.** A failure before the commit, in step 1, the copy, the freeze or the tails, fails the
  Build, lifts the freeze on the old provider with the owed writes landing there, closes the new
  provider and leaves the Fabric row unchanged, every line still there; a node failing its release
  after the commit retries it with its writes still owed and the Build still in progress; the commit
  written and its reply lost, then the fabric-primary dead: the successor finds the commit line and
  releases, never reverts; the new provider not answering at the commit: the freeze holds, nothing
  reverts, until it answers; `fabric.delete` mid-change interrupts the Build and both providers'
  records stay.
- **Writers that die.** A node that appends after the copy and dies before its tail: the
  completeness pass copies its remaining lines and the commit waits on it; a held name and an active
  attempt cross the cutover with the same holders, read back equal on both providers before the
  commit, and the name is not free on the new provider; a frozen node dying after observing a
  connection event finds the event in its pending log at restart and replays it; a frozen
  mesh-primary dying with a declaration unanswered: the declarer retries and the row is written once,
  on whichever provider the fabric is then on; a caller's config update during the freeze is
  answered once, after its line landed; the new provider not answering at the decision with no
  `abort` insertable: the freeze holds and nothing reverts.
- **The previous provider.** After the commit it holds every line to the last tail, nothing more is
  written to it, RDM removes nothing of it, and a go-back Build copies into it only the lines it
  lacks.
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
   The fabric-primary's copy first and each node's own tail at its switch keep what that shape was
   for, no restart and no state change, without the dual write.
10. **A switch without a freeze** (this proposal's second shape, 4011b5c58: each node switches as
    soon as its tail is copied, claims cut over by a `claims-closed` row, the change completing
    forward past it). Set aside on Luke's heads-up: from the first switch on the two providers
    diverge, a moved writer's lines exist on the new provider alone, and a change stuck after the row
    can neither finish nor go back without losing them, so both providers' data would have to go. The
    freeze makes one provider whole at every instant and the Fabric row the one commit point, and
    removes the row, the forward-only rule and the reads window at the cost of seconds of owed writes.

## What this proposal does not decide

- How the application's backend is built or what it stores (Q65: RDM's boundary contract is the how,
  the application decides who).
- Retention of a store's lines, on either provider: the store-root proposal's measured trigger.
