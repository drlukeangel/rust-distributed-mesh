# The fabric's `storage_provider`: a Fabric property, changed live by a Build

The lead, 2026-10-26, from Luke's direction in conversation (quoted under "Decided"). A proposal:
nothing here is built, and no architecture page changes without Luke's yes on the exact text.

## Why

Q171 (688c32241) removed state-sync, state-commit and the scratchpad. The removed
`fabric-state-sync.md` stated a requirement that went with them: RDM's records end up in the store the
application establishes after its Day 0, which may not exist when the mesh comes up (a store on
Rafka's brokers exists only once the brokers run). The Q171 proposal described state-commit as
re-recording the topology on RDM's own file store root, and did not name that requirement. Luke was
not told; the requirement stands ("that requirement didn't disappear").

Today nothing can meet it:

- Every node opens its storage once, at its start, and holds it until it exits. An admin opens its
  `StorageBackend` at `boot.rs:379-380` (`storage.open(&scope)`), RDM's file backend from `start` or
  the application's from `start_on`; every other node opens its `NodeStorageBackend`
  (`rdm-node/src/storage_backend.rs:21`, `node.rs:377/385`) and writes its own connection rows through
  its `ConnectionsWriter` (`node.rs:471`).
- No record names the storage in use. The Fabric record (`fabric_storage.rs:107`) holds `fabric_id`,
  `name`, `events` and `provider`, and that `provider` is the deployment provider
  (`MESH_SPAWN_TYPE`). The file backend's location is `RDM_STORE_DIR`, an env var the launching admin
  writes into each node it launches (`pipeline.rs:882`, `1741`).

So a running fabric cannot move to another storage, and no node can be asked which storage it writes.

## Decided (Luke, 2026-10-26)

- Records are written to storage, in the storage provider, from Day 0; there is no scratchpad.
- The storage provider can be changed while the fabric runs.
- The change is per node: every node writes its own records (its connections at least).
- It is made at the fabric level.
- It travels fabric-primary → mesh-primary → node, each level its own step (the shape "1",
  agreed over "mp.forward(node.storage.update)").
- The fabric-primary keeps access to the previous storage providers.
- It is a Build: the Build's topology carries the fabric's `storage_provider` as a new property, the
  meshes unchanged.
- `storage_provider` is a property of the Fabric, written on the Fabric's row, so it can be queried.

## Steps

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
  and `fabric.get` answers both. The bootstrap admin writes the first value on Day 0: the name of the
  backend it was started on. build.md §5.3's sentence that `fabric.storage` holds the Fabric "as an
  immutable identity row, `FabricIdentity { fabric_id, name }`, written once" already differs from the
  code, whose record carries `events` and the deployment `provider`; the page text is proposed under
  "Pages" for Luke's yes, with this property in it.
- **R-SP3. The Build's topology carries it.** `FabricTopology` (`accepted.rs:161`, today `fabric` and
  `meshes`) gains `storage_provider`. A change of storage is a `build.create` of the complete topology
  with the same meshes and the new `storage_provider`. It is admitted and refused like any Build (one
  in progress at a time, Q170), its attempts claimed, its receipts written and resumed from like any
  Build's. A Build that names a provider the fabric-primary's wiring does not register, or one the
  fabric-primary cannot open, is refused by name at acceptance, nothing moved. A provider is code,
  compiled into an executable and registered in its wiring, so a node can move only between the
  providers its own executable registers, and the fabric-primary's wiring is not every node's. Step 1
  is the check on every other node: a node whose executable does not register the named provider
  fails step 1 by name, naming the node and the provider, and the Build fails before step 2 with the
  old provider whole (R-SP12). A provider no executable has is an executable replacement, not this
  Build.
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
    the Fabric row's value moves only at the end of step 3 (R-SP8);
  - from its own row in its data dir, beside `node-key` and its held name, which it reads second; a
    start with no launcher, the adoption launch, reads only this.
  A running node never re-reads its environment: a change reaches it as the step's command, it does
  the step's work, writes its own row with the provider or providers it now writes and the step
  completed, and only then answers; the receipt exists only after the row does. A node that crashes
  inside a step comes back one step behind at most, the launcher's value is the fabric's and wins, and
  the node runs the step it missed before it answers its join (R-SP9).
- **R-SP5. Every node's storage can be replaced while it runs.** The handles a node's storage opens
  (an admin's fabric, meshes, nodes, connections and Build journal; every other node's connections)
  are held behind one handle per node that a step replaces, so every reader and writer of that node
  moves at once. Nothing else in the node holds a storage handle of its own.
- **R-SP6. Step 1, on every node: write to both, copy.** Each node opens the new provider, writes every
  write to both providers from then on, and copies its own records into the new one, whole records
  (one writer per key: the fabric-primary the Fabric record and the Build journal, each mesh-primary
  its mesh's record, in-flight row and its nodes' rows, each node its own connections), skipping a
  record the new provider already holds at the same version. The old provider still serves every read
  and decides every insert-and-fail claim. The step's receipt is the node's.
- **R-SP7. Step 2, on every node: reads and claims move.** Once the fabric-primary holds every node's
  step-1 receipt, each node reads from the new provider and claims are decided there. Every node still
  writes to both, so a node whose step 2 comes later still reads complete records from the old one.
- **R-SP8. Step 3, on every node: the old provider stops being written.** Once every node's step-2
  receipt is held, each node stops writing the old provider and closes it. With the last receipt the
  fabric-primary writes the Fabric's row: `storage_provider` the new one, the old one appended to
  `previous_storage_providers`. The Build completes.
- **R-SP9. A node born during the change** is launched with both providers and the step the fabric is
  at, and runs that step before it answers its join; a step it missed is run by its mesh-primary's
  attempt as any planned birth's.
- **R-SP10. The order of the three steps** is the Build's ordinary order: the fabric-primary's own
  records first, then each mesh by its mesh-primary's attempt (`build.attempt.run`), the mesh-primary's
  records before its nodes'. A step is fabric-wide: step 2 starts on no node until step 1 is received
  from every node, and step 3 likewise.
- **R-SP11. The fabric-primary keeps read-only access to the previous providers.** It opens a previous
  provider by its name from the Fabric's row when it reads it, and writes nothing to it. Any admin that
  becomes fabric-primary does the same, so every admin's wiring registers every provider the Fabric's
  row names. Going back to a previous provider is a new Build naming it; its records stop at the switch,
  so step 1 copies what it lacks.
- **R-SP12. A failed step** fails by name and fails the Build as any step does. Before step 2 nothing
  reads the new provider, so a failed or abandoned change leaves the old provider whole; the new one is
  closed and written no more. A failure from step 2 on is resumed, never reversed: every node still
  writes to both until step 3.
- **R-SP13. Spans.** Each step spans its node, the provider it opens or closes, the step, the records
  copied and skipped, and its outcome, under the Build's step spans; the names follow the build's
  span families and are listed in the landing.
- **R-SP14. Durable format.** The Fabric record and the journal's topology record each gain a field;
  a record without them reads as `file` and no previous provider. The landing carries
  `durable format moved`.

- **R-SP15. The fabric stays ready-for-traffic and no node changes state.** states.md, "Resize and
  executable replacement": "A Build that changes only a node's `config`, `tags`, `storage` or
  `placement` is neither: it applies them as build.md §1 states, and the fabric stays
  ready-for-traffic." A storage change is a property Build, the meshes unchanged, and is the same kind:
  the fabric, every mesh and every node stay ready-for-traffic through all three steps; the node's
  states are its life and its traffic, and a change of where it writes is neither. The progress is the
  step receipts and R-SP4's row, not a state. The page's sentence gains `storage_provider` in its list.
- **R-SP16. The node's `storage` is renamed `storage_persistence`, and `on_retire` `reclaim`** (Luke,
  2026-10-26). `StorageMeta` is `Ephemeral` or `Persistent { on_retire }`: the node's data-dir
  persistence, nothing to do with the fabric's backend, and with a provider on the node's row the
  name `storage` would mean two things. The property is `storage_persistence`, values `ephemeral` and
  `persistent { reclaim: retain | delete }`, the Kubernetes pair by another spelling
  (`storageClassName`, `persistentVolumeReclaimPolicy: Retain | Delete`); `retire` is a banned word
  and goes with it. `storage` and `on_retire` are JSON field names on every node record and every
  Build's `NodeSpec`, so this is a descriptor field moved: its landing carries `durable format moved`,
  and a fabric born before it is a new fabric; the positional wire carries no names and is untouched.
  Pages: the node-properties table, build.md §1 and §5.3, build-deployment.md §2, node-replace.md,
  node-recovery.md and states.md where they say "storage or placement", the consumer guide's
  examples; Q166's row gains `storage` → `storage_persistence` as one amendment line. Its own landing,
  before or after this proposal's; this proposal's one requirement of it is that the provider field is
  never called `storage`.

## Pages (text for Luke's yes, written with the build)

- build.md §5.3: the Fabric row's sentence (R-SP2), and a section for the storage change (R-SP3 to
  R-SP12).
- fabric-mesh-node-lifecycles.md: the storage change's row.
- node-admin-lifecycle.md §7: the store is the fabric's `storage_provider`, changed by a Build.
- CLAUDE.md env table: `RDM_STORAGE_PROVIDER` (R-SP4).
- node-rpc-envelope.md: the Fabric row's read (`fabrics.get` under Q167's amendment) carries the two
  fields (R-SP2); the node-properties table gains R-SP4's row and renames R-SP16's.
- states.md, "Resize and executable replacement": `storage_provider` in the property list (R-SP15).

## The mechanisms considered before this one

Each was weighed in the same conversation and set aside, for the reason given.

1. **Restore state-sync, state-commit and the scratchpad** (the draft Q182 arm A). Day-0 rows held in
   memory, written into the application's store at state-commit. Set aside: records are written to
   storage from Day 0, never held in a scratchpad (Luke), and it moves storage once, at Day 0, never
   while the fabric runs.
2. **Leave the switch to the application's backend**, its `on_fabric_ready` telling it to move.
   Set aside: `on_fabric_ready` is a notice on the fabric-primary alone, with no step and no check-in,
   while every node writes its own records; nothing would move the other nodes or confirm they moved.
3. **Restart every node on the new provider.** Set aside: not live.
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
7. **A `migrate` flag on the change.** Set aside: it says how to get there, not where. Step 1 copies
   what the new provider lacks and skips what it holds, which also covers a switch to a provider the
   application filled itself; a switch to an empty provider without a copy cannot be asked for.
8. **One pass, one step per node** (open the new provider, copy, switch reads and writes). Set aside:
   while nodes switch one at a time, a node that has switched writes where a node that has not still
   reads, and two providers decide the same insert-and-fail claim. The three fabric-wide steps are
   the established online-migration order (write to both, copy, move reads, stop the old writes).

## What this proposal does not decide

- How the application's backend is built or what it stores (Q65: RDM's boundary contract is the how,
  the application decides who).
- Removing a previous provider's records: RDM writes nothing to it after step 3 and deletes nothing.
