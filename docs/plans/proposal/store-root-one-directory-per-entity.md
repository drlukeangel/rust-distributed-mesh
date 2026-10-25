# Proposal: the store root, one file per store (R-SR1, R-SR2)

**Status:** proposal, 2026-10-25. Asks Luke's yes on the layout of RDM's file backend's fabric store
root and on the amendment of R14 items 1, 3, 5 and 7 and R-ML2 item 4 that it needs. Storage layout
and one write rule: no read, no wire frame, and one trait change (`NodesStorage` loses `remove_node`,
R-SR2). Rafka binds the same traits to its own topics and is untouched. It proves nothing about
`connections.get`: a store read's contract is the reads proposal's (Q167, R-RD7).

Luke, 2026-10-25: "never delete, only append"; "I would have assumed we're just using a JSONL file";
"one file, one row in that JSONL file; we don't need to create a million files."

## 1. The question

The storage boundaries are five and flat (`storage_backend.rs`: `fabric.storage`, `mesh.storage`,
`nodes.storage`, `connections.storage`, `builds.storage`). The disk is not: eleven top-level entries,
one file per record, directories per writer. What is the correct layout?

## 2. Today

The layout the code writes, read from `storage.rs`, `fabric_storage.rs`, `build_state.rs`,
`mesh_claims.rs` and `rdm-node/src/storage.rs` (the `store_root.rs` header and R14 item 1 describe an
older one):

```text
<store_dir>/fabrics/<name>                      the fabrics' name index (R-FN1)
<store_dir>/<fabric id>/
  fabric/record                                 the Fabric record (fabric-primary)
  fabric/builds/<build id>/<fact>               the fabric-primary's Build records, one file per fact (R-BI6)
  seat-moves/<T>--<node id>--<incarnation>      the one fabric-wide sequence of primary moves (Q35, Q109)
  mesh-records/<mesh id>.json                   each Mesh record, keyed by id (storage.rs:286)
  mesh-names/<name>.json                        each mesh's name claim (storage.rs:311)
  meshes/<mesh name>/claims/in-flight.json      the mesh's one attempt in flight (mesh_claims.rs)
  meshes/<mesh name>/builds/<build id>/<fact>   the mesh-primary's drift-attempt Build records (R-BI6)
  nodes/<mesh name>/records/<node id>           node records, with paths/ (the path.name index),
  nodes/<mesh name>/{paths,names,tags/<label>}/   names/ (the claims) and tags/ beside them
  runtimes/<…>                                  the runtimes an admin started (ROOT_RUNTIMES, storage.rs:1142)
  connections/<source path.name>/<key>          each node's latest connection fact per key
  connections/<source path.name>/history.jsonl  each node's raw log, one line per fact
  connections-by-destination/<node id>/         the by-destination index
  status/<mesh>/                                dead: no writer, no reader; StoreRoot::status has only its own test
```

Every write is a whole record to its own file by temp, fsync and rename (`record_store.rs`), so a
record's earlier writes are gone; a never-run node's record is removed (`node_rows.rs`
`release_record`); the writer map is drawn as directories; `status/` is dead; `seat-moves/` and the
refused `seat-transfers/` are banned words; mesh and node directories are keyed by the mesh's name by
ruling (R14 item 1, R-ML2 items 4 and 5), to keep a name claim path-unique across a mesh's
re-creation. One file is already the shape asked for: the connections raw log, `history.jsonl`, one
line per fact, one appender.

## 3. The rules

- **R-SR1, one file per store.** The fabric's store root holds a layout marker, one JSONL file per
  storage boundary, and the locks: `layout`, `fabric.jsonl`, `meshes.jsonl`, `nodes.jsonl`,
  `connections.jsonl`, `builds.jsonl`, `locks/`. A fact lives in the file of the store whose object
  it describes. Indexes are not on disk: they are the fold. The fabrics' name index stays at
  `<store_dir>/fabrics/<name>` above the roots as R-FN1 ruled it: it answers which root a name is
  before any root is opened.
- **R-SR2, every write appends the whole record; nothing is ever removed.** A line is one record
  write: `{"format": "<record kind>/<n>", "key": "<the record's key>", … the whole record}`. The
  latest complete line for a key is the entity's current state; every earlier line remains. A
  delete appends the record marked Deleted (a node) or Dead (a mesh); a create withdrawn before the
  node ran appends the record marked Withdrawn, over which a name claim wins as over a Deleted one,
  so the one place a record disappears today goes. Retention of a file's lines is nobody's until
  Luke rules it; this proposal introduces no compaction and assumes none.
- **R-SR3, one append protocol for every file.** A line is `\n` + one JSON object + `\n`: JSONL,
  readable by ordinary tooling. The object carries `format`, `key`, the whole record, and last a
  `crc32` of the object's bytes before that field. A writer appends one line in one `write` on a
  descriptor opened `O_APPEND`, then `fsync`s, and reports success only after the fsync. Both
  providers put the root on one host kernel (the process provider's directory, the container
  provider's bind mount), where a single `write` on `O_APPEND` to a regular file lands whole and in
  order; R14 item 5's filesystem contract is restated for it. A reader folds a file line by line: a
  blank line is skipped; a line that does not parse as JSON is a torn fragment, skipped and counted
  in the open's span with its offset; a line that parses and whose `crc32` does not match is
  corruption, and the open is refused by name naming the file and the offset, never an older line
  shown as current. Because every line opens with its own newline, a fragment another writer appends
  after is a line of its own and swallows nothing. A short write (fewer bytes written than the line)
  is indeterminate, as a cut call is (Node RPC §34): the writer's object may already be complete but
  for its newline, and any later opening newline completes it. Before its next append the writer
  re-reads the tail from its consumed offset and finds one of three things: its object whole and
  terminated (another writer's newline did it), so it fsyncs and proceeds, appending nothing; its
  object whole without a newline at the end of the file, so it appends one newline alone, fsyncs and
  proceeds, and if another writer's line landed in between the lone newline is a blank line, skipped;
  or its object absent or cut short, so it appends the whole line again. The writer knows its own
  write by its bytes, which end in its `crc32`. Under either schedule, the other writer first or the
  retry first, the record is folded once (Architect 1, 2026-10-25). Corruption is refused rather
  than skipped because a skipped whole record exposes the line before it as current; a fragment is
  skipped because it never was a record.
- **R-SR4, a reader of rows it does not write reads the tail first.** A writer's own objects live in
  its cache (storage-whole-records.md). Rows another writer appends (another mesh's node records,
  every node's connection facts, the Build facts a successor folds) are read by consuming the
  file's complete lines from the reader's consumed offset before the answer that needs them. The
  consumed offset is the fold's own: it advances only over bytes the fold has read whole, never to
  an append position, and an incomplete tail stays unconsumed. A writer's own append does not move
  it, so after A consumes to 100, B appends and A appends, A's next fold starts at 100 and reads B's
  line and then its own (Architect 1, 2026-10-25). The shared file's view is not the writer's cache.
- **R-SR5, one winner stays one winner.** The conditional operations keep their guarantees outside
  the logs, under `locks/`, because an append cannot refuse. A lock is a create-exclusive file whose
  content names its claimant: the birth (node id, incarnation) and, for an attempt, the attempt
  `(build_id, mesh, attempt)`. The attempt in flight on a mesh name is `locks/in-flight/<mesh name>`,
  taken and released as `mesh_claims.rs` does today. A name claim is `locks/name/<name>`, and the
  lock is the claim, as today's exclusive insert is (`claim_mesh_name`, `storage.rs:356`; the file
  backend's `claim_node_name`, `:1303`; R14 item 3 describes the node name as a conditional write and
  is corrected to the code): there is no claim line, so there is no window between a lock and a
  line. The name is released at Deleted or Dead by the name's holder, and the next claimant inserts
  the lock again over a Deleted or Withdrawn latest line, which `claim_against` decides before the
  take. Recovery across a crash: a lock whose claimant's latest record line is Deleted, Dead or
  Withdrawn is a release not finished, and the next claimant finishes it, releasing and taking in
  one step, which is idempotent and replays safely; a lock whose claimant birth is proven exited
  with no record line of that birth under the name is a claim not finished, and the executor
  resuming that attempt adopts it (the lock names the attempt), while any other claimant waits for
  the attempt's verdict, which releases it. A lock naming a live claimant, or a claimant whose latest
  line is anything but Deleted, Dead or Withdrawn, is never removed by another (Architect 1,
  2026-10-25). The writer map says who may claim a name; the lock is what makes two processes that
  both believe they may, a recovery executor and an adoption launch, yield one winner.

## 4. The layout

```text
<store_dir>/fabrics/<name>                       → fabric id (R-FN1, unchanged)
<store_dir>/<fabric id>/
  layout                                         r-sr1
  fabric.jsonl                                   the Fabric record's writes; every primary move (one line at its T)
  meshes.jsonl                                   every Mesh record's writes
  nodes.jsonl                                    every node record's writes; every runtime row; every tag change
  connections.jsonl                              every connection fact of every source (the raw log; the latest per key is the current view)
  builds.jsonl                                   every Build fact: accepted, opened, claimed, step and attempt receipts, forgotten
  locks/in-flight/<mesh name>                    the one attempt in flight on that mesh name, create-exclusive
  locks/name/<name>                              the claim of a mesh name or a node path.name, create-exclusive, naming its claimant
```

Writers, file by file; one writer per key, keys never shared:

| File | Record kinds and keys | Writers |
|---|---|---|
| `fabric.jsonl` | `fabric-record` at the fabric; `primary-move` at `T` | the fabric-primary; a primary move's own writer at the next `T`, having folded every earlier move (Q109) |
| `meshes.jsonl` | `mesh-record` at the mesh id | that mesh's primary; the fabric-primary for a mesh with none |
| `nodes.jsonl` | `node-record` at the node id; `node-runtime` at the node id; `node-tag` at (label, node id) | the mesh-primary of the node's mesh; the fabric-primary for a mesh with none; under build.md §5.3's exception (Q122) the fabric recovery workflow's executor for the rows its operations require, and the adoption launch for its own row; the launching admin for the runtime row |
| `connections.jsonl` | `node-connection` at (source, destination, alpn, kind) | the source node |
| `builds.jsonl` | the Build facts at their keys: `<build_id>`, `<build_id>-<attempt>`, `<build_id>-<mesh>-<attempt>`, a claim, a receipt per step | the fabric-primary for a topology attempt; the mesh-primary for its drift attempt, the mesh in the key (R-BI7) |
| `locks/in-flight/<mesh name>` | the mesh name | its claimant, create-exclusive, released at the attempt's verdict while it still names that attempt |
| `locks/name/<name>` | the mesh name or the path.name | its claimant, create-exclusive, the lock being the claim; released at Deleted or Dead by the name's holder, or by the next claimant when the holder's latest line is Deleted, Dead or Withdrawn |

**Path uniqueness holds without the mesh directory.** The claim on `mesh2.admin.1` is the lock
`locks/name/mesh2.admin.1`, fabric-wide and independent of any mesh id, so a mesh created again under
the same name, a new mesh with a new id (R-ML2 item 3), contends on the same lock and takes it only
over a Deleted or Withdrawn latest line, exactly as today's exclusive insert. Two `mesh2.admin.1`
cannot exist because the claim is one file by name; where any record sits does not enter.

**Connections stay keyed by path.name.** A fact's ends are path.names (`ConnectionEnd.name`;
node-identity-and-state.md §2.2) and its key is `(source, destination, alpn, kind)` (connections.md
§3). The raw log and the latest-per-key view are the same file: the fold keeps the latest line per
key as the compacted current view connections.md §3 describes, and the by-destination index is the
same fold keyed the other way. A re-born node at the same path.name appends the same keys. The
`remove_connection` of the latest-per-kind row becomes an appended line, as every write is.

**The runtime row moves to the node's store**, `node-runtime` in `nodes.jsonl` (a fact about that
node), read by the adoption launch where it reads `runtimes/` today (node-admin-lifecycle.md:57).

**A new mesh-primary's rebuild** (R14 item 7) folds `meshes.jsonl` and `nodes.jsonl` and keeps the
records whose `mesh` is its own, one pass each. The pass is the same fold every reader does at open.

**The Build journal is one file.** R-BI6 made the journal's location the adapter's and provider's
choice; this is the file adapter's choice. The fold over a Build's facts is a filter on one file
instead of `fabric/builds/<id>/` plus every `meshes/<mesh>/builds/<id>/`.

**`primary-move` replaces `seat-move`.** The word is banned; a new layout is the moment to drop it.
The row format `seat-move/1` becomes `primary-move/1`, with the byte fixtures the format carries.

**Before the first state-commit nothing changes.** Rows stay in the scratchpad until the commit-state
action publishes them (fabric-state-sync.md), and the scratchpad publishes as lines appended to §4's
files. The three stores that write before state-commit append to the new files: the Build facts to
`builds.jsonl`, the attempt claim to `locks/in-flight/`, the declaration to `nodes.jsonl`.

**What open costs.** A fold reads each file once. At the record's size and count (142 nodes in the
largest estate in the record) and the write rate the cells show, a long run's `nodes.jsonl` is
thousands of lines and a few megabytes. A file whose size shows in telemetry is the first fact of a
retention question, which is Luke's and not assumed here.

## 5. Delta

### 5.1 Code (RDM's file backend only; no wire or Rafka change; one trait method goes)

- `rdm-node/src/record_store.rs`: `FileRecords` (one file per record, temp, fsync, rename) gives way
  to `StoreLog`: `append(record)` by R-SR3, `fold()` and `fold_from(offset)` by R-SR3 and R-SR4,
  `lock_take(kind, name, claimant)` and `lock_release(kind, name, claimant)` by R-SR5, over
  `locks/in-flight/` and `locks/name/`. The hard-link insert is what the locks are; it leaves the
  records. Temp file names carry the writer's incarnation no longer, there being none.
- `rdm-node/src/store_root.rs`: the header states §4; `nodes(mesh)`, `meshes(mesh)`, `fabric_builds()`,
  `mesh_builds(mesh)`, `connections(path.name)`, `connections_to(node id)` and `status(mesh)` go;
  `fabric()`, `meshes()`, `nodes()`, `connections()`, `builds()` (the five files), `lock(name)` and
  `layout()` come. The root's creator writes `layout` once; `open` refuses a root without it or with
  another value by name, naming the path and the value found. The three `refuse_retired` checks in
  `fabric_storage.rs` are subsumed and go.
- `rdm-admin/src/storage.rs`: `FileNodesStorage` and `FileMeshStorage` over `nodes.jsonl` and
  `meshes.jsonl`, the path.name and tags indexes as the fold; `NodesStorage` loses `remove_node`:
  `release_record` appends the record Withdrawn and releases the name, `claim_against` yields the
  name over Withdrawn as over Deleted; `take_in_flight`, `release_in_flight` and `in_flight` keep
  their name-keyed signatures over `locks/in-flight/`; `ROOT_RUNTIMES` goes, the runtime row is a
  `node-runtime` line; the header states R-SR2 to R-SR5.
- `rdm-admin/src/fabric_storage.rs`: the Fabric record and the primary moves in `fabric.jsonl`;
  `SEAT_MOVES_DIR`, `REFUSED_TRANSFERS_DIR` and the retired-directory constants go; `seat-move/1` →
  `primary-move/1`.
- `rdm-admin/src/build_state.rs`: `FileJournal` over `builds.jsonl`, one line per fact;
  `refused_journal_files` and the revision walk read the one file.
- `rdm-node/src/storage.rs`: `FileConnectionsStorage` over `connections.jsonl`; `history()` is the
  file, `connections()` and `connections_to()` the fold; the per-source `history.jsonl` and the
  by-destination directory go.
- `rdm-admin/src/status_rpc.rs:2361`: the comment names the node record, not `status/<mesh>/`.
- **An old root is refused by its marker, no migration.** A root without `layout`, or with a value
  other than `r-sr1`, is refused by name at open. A fabric born before the landing is a new fabric;
  the gate's and the demo's estates are born fresh on every run. The next layout change bumps the
  value.

### 5.2 Pages (old → new)

build.md line 434, the layout sentence: `In RDM's file adapter they open under
<store_dir>/fabrics/<name> and one store root per fabric, <store_dir>/<fabric_id>/{fabric,
meshes/<mesh>, nodes/<mesh>/, connections/<node>/}, where <mesh> is the mesh's logical name. One
writer per item: the fabric-primary writes fabric/ (the fabric's identity, immutable) and
fabric/builds/, every Build record, … each mesh-primary its meshes/<mesh> and nodes/<mesh>/ (where
is_primary and is_fabric_primary record the election's result); each node its own
connections/<node>/.` → `In RDM's file adapter they open under <store_dir>/fabrics/<name> and one
store root per fabric, <store_dir>/<fabric_id>/{layout, fabric.jsonl, meshes.jsonl, nodes.jsonl,
connections.jsonl, builds.jsonl, locks/}: one append-only JSONL file per storage boundary, every
write one line carrying the whole record with its format and key, the latest complete line for a key
the current state, nothing ever removed, indexes folded in memory, the attempt in flight and every
name claim a create-exclusive lock under locks/, and a layout marker a root is refused without (R-SR1 to R-SR5). One writer per
key: the fabric-primary writes the Fabric record and every topology attempt's Build facts, … each
mesh-primary its mesh's record, its mesh's node records and tag rows, and the name locks of both
(where is_primary and is_fabric_primary record the election's result) and its drift attempts' Build
facts, the mesh in the key; the launching admin a birth's runtime row; each node its own connection
facts.` The Q122 exception and the sentence from `The Build journal holds one record per fact` on
are unchanged, "record" there now a line.

node-admin-lifecycle.md line 57: `laid out <store_dir>/<fabric_id>/{fabric, meshes/<mesh>,
nodes/<mesh>/, connections/<node>/, runtimes/}` → `laid out <store_dir>/<fabric_id>/{layout,
fabric.jsonl, meshes.jsonl, nodes.jsonl, connections.jsonl, builds.jsonl, locks/} (R-SR1; a birth's
runtime row is a node-runtime line in nodes.jsonl)`.

node-primary-update.md line 52: `(its own connections/<node>/ rows are its own, R14)` → `(its own
connection facts in connections.jsonl are its own, R14)`.

connections.md §3, where it names `history.jsonl` and the latest-per-kind rows as separate things,
gains one sentence: `In RDM's file backend the raw log and the current view are one file,
connections.jsonl, the view the fold's latest line per key.`

### 5.3 Rulings

- R14 item 1 gains: `**Amended (Luke, 2026-10-25; [one file per store](../proposal/store-root-one-directory-per-entity.md)):**
  the layout is {layout, fabric.jsonl, meshes.jsonl, nodes.jsonl, connections.jsonl, builds.jsonl,
  locks/}, one append-only JSONL file per storage boundary, every write the whole record as one line
  with its format and key, nothing removed, indexes folded, a layout marker; status/ is gone; path
  uniqueness lives in the name locks under locks/name/, as today's exclusive inserts, fabric-wide.` Item 3:
  corrected to the code: `The name claim is the sole writer's conditional write, not a
  create-exclusive insert.` → `The name claim is a create-exclusive insert under locks/name/, as the
  code has made it for both names (storage.rs:356, :1303), the lock being the claim; the attempt in
  flight is the other, under locks/in-flight/.` Its `claim_against` sentence stands: the decision over
  a Deleted or Withdrawn line is the claimant's, before the take. Item 5: `relies on hard-link insertion, atomic rename,
  file and directory fsync, and coherent reads and listings` → `relies on one-line O_APPEND writes
  landing whole and in order, file fsync, create-exclusive insertion under locks/, and coherent reads
  on one backing namespace (R-SR3)`. Item 7: `reads nodes/<mesh>/ and meshes/<mesh>` → `folds
  meshes.jsonl and nodes.jsonl and keeps its mesh's records`.
- R-ML2 item 4 gains: `Amended 2026-10-25 (R-SR1): the recreated mesh shares the name locks under
  locks/name/ and the in-flight lock under its name, not a directory.` Item 5's reason stands: no
  MeshId is in a name claim's key.
- R-BI6 is unchanged: the journal's location was the adapter's choice, and `builds.jsonl` is it.
- R-FN1 is unchanged.

### 5.4 Cells

- `StoreLog`, one cell set: two writers appending concurrently to one file, every line found whole
  and in order; a torn tail followed by another writer's line, the fragment skipped and counted and
  the line read; the short-write boundary, a record written whole but for its final newline and
  reported short, under both schedules: another writer's newline completes it before the re-read,
  and the re-read comes first and appends one newline alone; in each the record is folded once and
  success is reported after the fsync; a line that parses with a wrong `crc32` refusing the open by
  name with the file and the offset; a blank line skipped; several
  writes of one key, every line present after reopen and the latest read as current; the A–B–A
  interleaving, A consumed to an offset, B appends, A appends, A's next fold reads B's line (R-SR4);
  the in-flight lock taken once across two claimants and released only by the attempt that holds
  it; two processes claiming one name, one lock taken, the other refused `NameHeld` and launching
  nothing; the two crash boundaries of a name (R-SR5): a lock taken by a birth that died before any
  record line, adopted by the executor resuming its attempt and refused to any other claimant until
  the verdict; and a Deleted, Dead or Withdrawn line appended by a holder that died before the
  release, released and taken by the next claimant in one step and replayable; a lock naming a live
  claimant never removed by another.
- The path-asserting tests move to §4: `store_root.rs` (9 assertions), `build_state.rs` (11),
  `rdm-admin/storage.rs` (6), `store_root_boot.rs` (4), `rdm-node/storage.rs` (2),
  `elections/fabric_takeover.rs` (2), `estate.rs`, `node/node_stop.rs`, `elections/node_primary_update.rs`,
  `fabric/named_listeners.rs`, `mesh/fabric_primary_handover.rs` (1 each); `mesh_claims.rs:114`
  (`locks/in-flight/mesh2`); `fabric_storage.rs`'s older-root move-rows test reads `primary-move/1`
  lines in `fabric.jsonl`; `demo/tests/connections.rs:86`, which asserts `fabric/record.json`, asserts
  the Fabric record's line in `fabric.jsonl`.
- New: a root without `layout`, and one with another value, is refused by name at open, naming the
  path and the value; a populated root in the new layout reopens; a create withdrawn before its node
  ran leaves its record Withdrawn and a later claim of the name wins over it, and no test of the nodes
  store removes a record (R-SR2); a recreated mesh's `mesh2.admin.1` claim wins over the Deleted line
  and its records carry the new MeshId; a delayed release of an attempt on the old mesh against the
  re-created mesh's name finds the lock naming another attempt and releases nothing; a re-born node
  at the same path.name appends the same connection keys and the fold's current view shows the new
  birth's; the adoption launch reads the runtime line; a new mesh-primary's rebuild folds its mesh's
  records; a Build with a topology attempt and a drift attempt folds from `builds.jsonl`; rows written
  before the first state-commit publish from the scratchpad as lines.

## 6. Arms

- **A (recommended): R-SR1 to R-SR5**, the layout of §4, the delta of §5, R14 items 1, 3, 5 and 7
  and R-ML2 item 4 amended.
- **B: today's layout**, one file per record and directories per writer, with `status/` deleted,
  `connections-by-destination/` moved under `connections/`, the layout marker added and R-SR2's
  Withdrawn record: nothing removed, but every write still replaces its file and the writer map
  stays on disk.
- **X: something else.**
