# Proposal: the store root, one directory per entity (R-SR1, R-SR2)

**Status:** proposal, 2026-10-25. Asks Luke's yes on one rule for RDM's file backend's layout of the
fabric's shared store root, and on the amendment of R14 item 1 and R-ML2 item 4 that it needs.
Storage layout and one write rule: no read, no wire frame, and one trait change (`NodesStorage`
loses `remove_node`, R-SR2). Rafka binds the same traits to its own topics and is untouched. It proves nothing about `connections.get`: a store read's contract,
which facts are retained against live and how complete the by-destination index is at the instant
of a read, is the reads proposal's (Q167, R-RD7), not a consequence of where a directory sits.

## 1. The question

Luke, 2026-10-25, on the reads proposal: the stores are one per entity, fabric, mesh, nodes,
connections, builds; why is anything nested? The storage boundaries are five and flat
(`storage_backend.rs`: `fabric.storage`, `mesh.storage`, `nodes.storage`, `connections.storage`,
`builds.storage`). The disk is not. "Let's not keep the mess because it already exists. What is the
correct way?"

## 2. Today

The layout the code writes, read from `storage.rs`, `fabric_storage.rs`, `build_state.rs`,
`mesh_claims.rs` and `rdm-node/src/storage.rs` (the `store_root.rs` header and R14 item 1 describe an
older one):

```text
<store_dir>/fabrics/<name>                      the fabrics' name index (R-FN1)
<store_dir>/<fabric id>/
  fabric/record                                 the Fabric record (fabric-primary)
  fabric/builds/<build id>/                     the fabric-primary's Build records (R-BI6)
  seat-moves/<T>--<node id>--<incarnation>      the one fabric-wide sequence of primary moves (Q35, Q109)
  mesh-records/<mesh id>.json                   each Mesh record, keyed by id already (storage.rs:286)
  mesh-names/<name>.json                        each mesh's name claim (storage.rs:311)
  meshes/<mesh name>/claims/in-flight.json      the mesh's one attempt in flight (mesh_claims.rs)
  meshes/<mesh name>/builds/<build id>/         the mesh-primary's drift-attempt Build records (R-BI6)
  nodes/<mesh name>/records/<node id>           node records, with paths/ (the path.name index),
  nodes/<mesh name>/{paths,names,tags/<label>}/   names/ (the claims) and tags/ beside them
  runtimes/<…>                                  the runtimes an admin started (ROOT_RUNTIMES, storage.rs:1142)
  connections/<source path.name>/               each node's own connection rows and raw log
  connections-by-destination/<node id>/         the by-destination index
  status/<mesh>/                                dead: no writer, no reader; StoreRoot::status has only its own test
```

Eleven top-level entries for five stores. What the picture shows:

- **Two conventions for one thing.** Mesh records are keyed by id in a flat `mesh-records/`; node
  records are keyed by id under a directory per mesh name. Mesh name claims are flat in
  `mesh-names/`; node name claims are under the mesh. The Build journal is one entity in two places.
- **The writer map is drawn as directories.** `fabric/`, `meshes/<mesh>/`, `nodes/<mesh>/`,
  `connections/<node>/`: each a writer's, and "no two writers write one key" (build.md:434).
- **`status/<mesh>/` is dead** and still listed in R14 item 1, the header and `status_rpc.rs:2361`.
- **Two names on the list are banned words.** `seat-moves/` and the refused `seat-transfers/`.
- **Mesh directories are keyed by the mesh's name by ruling.** R14 item 1 ("the mesh's logical name,
  the path segment, never the MeshId"), R-ML2 item 4 ("the recreated mesh shares the directory") and
  item 5 ("a MeshId in the key would let two `mesh2.admin.1` exist at one path"). The property those
  lines protect is the path uniqueness of a name claim across a mesh's re-creation.

## 3. The rule

- **R-SR1, one directory per entity.** Under the fabric's store root, one directory per storage
  boundary, named as the boundary is: `fabric/`, `meshes/`, `nodes/`, `connections/`, `builds/`.
  A record sits at its entity's id. An index sits under its entity's directory, named
  `by-<property>`, and a fact whose key is a property rather than the id (a name claim, the attempt
  in flight on a mesh name) sits under that property's index entry. A key has exactly one writer, so
  the writer map is carried in the key and no directory is a writer's. A fact lives in the store of
  the object it describes. The root carries a layout marker, `<root>/layout`, and a root without it
  or with another value is refused by name. The fabrics' name index stays at `<store_dir>/fabrics/<name>`
  above the roots as R-FN1 ruled it: it answers which root a name is before any root is opened.
- **R-SR2, every write is the whole record and no record is ever removed.** The latest write of a
  key is the entity's current state (storage-whole-records.md: one cached object, one writer, one
  whole write; Rafka stores each object as a topic of whole records). A delete appends the record
  marked Deleted (a node) or Dead (a mesh); a create withdrawn before the node ran appends the record
  marked Withdrawn, and a name claim wins over a Withdrawn row as over a Deleted one, so the only
  place a record disappears today (`node_rows.rs` `release_record`, `remove_node`) goes. Locks keep
  their take and release (the in-flight row); indexes are derived and follow their record (Luke,
  Architect 1, 2026-10-25). **Whether the file backend keeps every earlier write of a record is
  §6's second question.** Today it writes the latest by temp, fsync and rename, and the history a
  reader uses is elsewhere: the Build journal, the primary-moves sequence, the connections raw log
  and the node record's own per-birth facts. Nothing reads an earlier version of a record. Rafka's
  topic is append-only by nature; what it retains is Rafka's.

## 4. The layout

```text
<store_dir>/fabrics/<name>                                 → fabric id (R-FN1, unchanged)
<store_dir>/<fabric id>/
  layout                                                   `r-sr1`
  fabric/record                                            the Fabric record
  fabric/primary-moves/<T>--<node id>--<incarnation>       the one fabric-wide sequence of primary moves (Q35, Q109)
  meshes/<mesh id>/record                                  the Mesh record
  meshes/by-name/<name>/claim                              the mesh's name claim → mesh id
  meshes/by-name/<name>/in-flight                          the one attempt in flight on that mesh name
  nodes/<node id>/record                                   the node record (its row and its births' facts)
  nodes/<node id>/runtime                                  the runtime the launching admin started for it
  nodes/by-name/<path.name>                                the node's name claim → node id
  nodes/by-tag/<label>/<node id>                           the tags index (R-TG1)
  connections/<source path.name>/<destination path.name>-<kind>   the latest fact per (source, destination, kind)
  connections/<source path.name>/history.jsonl             the source's raw log
  connections/by-destination/<destination path.name>/<source path.name>-<kind>
  builds/<build id>/<fact key>                             every fact of the Build: the fabric-primary's
                                                           attempts and each mesh's drift attempts
```

One writer per key, directory by directory:

| Directory | Key | Writer |
|---|---|---|
| `layout` | the root | whoever creates the root directory, once; never rewritten |
| `fabric/record` | the fabric | the fabric-primary |
| `fabric/primary-moves/<T>…` | `T` | the move's own writer at the next `T`, having folded every earlier move (Q109) |
| `meshes/<mesh id>/` | the mesh id | that mesh's primary; the fabric-primary for a mesh with none |
| `meshes/by-name/<name>/claim` | the name | the claiming mesh's primary, a conditional write over a Dead row (R14 item 3, R-ML2 item 3) |
| `meshes/by-name/<name>/in-flight` | the mesh name | its claimant, insert-and-fail (build.md:405, `mesh_claims.rs`): one attempt in flight per mesh name. The key is the name because the row exists before the mesh does (a mesh.create's topology attempt inserts it) and across a re-creation, and `take_in_flight`, `release_in_flight` and `in_flight` take the name (Architect 1, 2026-10-25) |
| `nodes/<node id>/record` | the node id | the mesh-primary of the node's mesh; the fabric-primary for a mesh with none; under build.md §5.3's exception (Q122) the fabric recovery workflow's executor, for the rows that attempt's operations require, and the adoption launch for its own row |
| `nodes/<node id>/runtime` | the node id | the admin that launched the birth |
| `nodes/by-name/<path.name>` | the path.name | the claiming mesh-primary, over a Deleted row only |
| `nodes/by-tag/<label>/<node id>` | (label, node id) | the node's mesh-primary |
| `connections/<source path.name>/` | (source, destination, kind) | the source node |
| `connections/by-destination/<destination path.name>/…` | (destination, source, kind) | the source node, beside its own row |
| `builds/<build id>/` | `<build_id>-<attempt>` or `<build_id>-<mesh>-<attempt>`, a claim, a receipt per step | the fabric-primary for a topology attempt, the mesh-primary for its drift attempt: the mesh is in the key (R-BI7), so the two never share one |

**Path uniqueness holds without the mesh directory.** The claim on `mesh2.admin.1` is the key
`nodes/by-name/mesh2.admin.1`, fabric-wide and independent of any mesh id, so a mesh created again
under the same name, a new mesh with a new id (R-ML2 item 3), contends on the same key and wins only
over the Deleted row, exactly as today. Two `mesh2.admin.1` cannot exist because the claim is by
name; where the record sits does not enter. The same holds for `meshes/by-name/<name>/claim` over a
Dead row.

**Connections stay keyed by path.name, on purpose.** A connection fact's ends are path.names
(`ConnectionEnd.name`; node-identity-and-state.md §2.2, a reference stores the `path.name`), and its
key is `(source, destination, alpn, kind)` (connections.md §3). A re-born node at the same
`path.name` writes the same keys and replaces the dead birth's latest rows, as today; nothing is
left behind per birth and no retention rule is added. `node.name.update` cannot move the directory
because a node is renamed only with no connection held (R-CR1). The by-destination index moves
under `connections/` and takes the destination's `path.name`, the same end the fact names, where
today it takes the node id.

**The runtime rows move to the node.** `runtimes/` holds the runtime a launching admin started for a
birth, a fact about that node (R-SR1: a fact lives in the store of the object it describes), so it
sits at `nodes/<node id>/runtime`, read by the adoption launch and by node investigation where they
read `runtimes/` today (node-admin-lifecycle.md:57).

**A new mesh-primary's rebuild** (R14 item 7, "reads `nodes/<mesh>/` and `meshes/<mesh>`") reads
`meshes/<mesh id>/record` and the node records whose `mesh` field is its own, one pass over `nodes/`.
At the record's size and count (142 nodes in the largest estate in the record) the pass is one
directory listing and one read per node, which the rebuild does today under its mesh directory. A
`nodes/by-mesh/<mesh id>/` index is the arm not recommended: a second key to keep right for a read
that happens at a primary change.

**The Build journal is one directory per Build.** R-BI6 made the journal's location the adapter's
and provider's choice; this is the file adapter's choice. The fold over a Build's facts reads one
directory instead of `fabric/builds/<id>/` plus every `meshes/<mesh>/builds/<id>/`.

**`primary-moves/` replaces `seat-moves/`.** The word is banned; a new layout is the moment to drop
it. The rows' format, `seat-move/1`, is a wire-level name and is renamed with the directory in the
same landing, `primary-move/1`, with the byte fixtures the format carries.

**Before the first state-commit nothing changes.** Rows stay in the scratchpad until the commit-state
action publishes them (fabric-state-sync.md), and the scratchpad publishes to the keys of §4. The
three stores that write to the root before state-commit write to the new keys: the Build facts to
`builds/<build id>/`, the attempt claims to `meshes/by-name/<name>/in-flight`, the declaration to the
node record.

## 5. Delta

### 5.1 Code (RDM's file backend only; no trait, wire or Rafka change)

- `rdm-node/src/store_root.rs`: the header states §4; `nodes(mesh)`, `meshes(mesh)`, `fabric_builds()`,
  `mesh_builds(mesh)`, `connections_to(node id)` and `status(mesh)` go; `node(id)`, `node_names()`,
  `node_tags()`, `mesh(id)`, `mesh_names()`, `build(id)`, `connections(path.name)`,
  `connections_to(path.name)` and `layout()` come. The root's creator writes `layout` once; `open`
  refuses a root without it or with another value by name, naming the path and the value found. The
  three `refuse_retired` checks in `fabric_storage.rs` are subsumed and go.
- `rdm-admin/src/storage.rs`: `FileNodesStorage` records at `nodes/<id>/record`, the runtime row at
  `nodes/<id>/runtime` (`ROOT_RUNTIMES` goes), the name claim at `nodes/by-name/`, the tags index at
  `nodes/by-tag/`; `FileMeshStorage` records at `meshes/<id>/record`, the claim and the in-flight row
  at `meshes/by-name/<name>/` (`mesh-records/`, `mesh-names/` and `meshes/<name>/claims/` go); the
  `MeshStorage` and `NodesStorage` traits unchanged, their name-keyed `take_in_flight`,
  `release_in_flight`, `in_flight` and `claim_against` included, except `remove_node`, which goes
  (R-SR2): `release_record` writes the record Withdrawn and releases the name, `claim_against` yields
  the name over a Withdrawn row as over a Deleted one, and the header states R-SR2; `nodes_answer`
  and the rebuild read by the record's `mesh` field.
- `rdm-admin/src/fabric_storage.rs`: `SEAT_MOVES_DIR` → `fabric/primary-moves`, the row format
  `primary-move/1`; `REFUSED_TRANSFERS_DIR` and the retired-directory constants go with their checks.
- `rdm-admin/src/build_state.rs`: `FileJournal` at `builds/<build id>/`, one directory per Build;
  `refused_journal_files` and the revision walk read `builds/` only.
- `rdm-node/src/storage.rs`: `FileConnectionsStorage` as today under `connections/<source path.name>/`;
  the by-destination rows under `connections/by-destination/<destination path.name>/`.
- `rdm-admin/src/status_rpc.rs:2361`: the comment names the node record, not `status/<mesh>/`.
- **An old root is refused by its marker, no migration.** A root without `layout`, or with a value
  other than `r-sr1`, is refused by name at open, naming the path and the value found. No entry's
  shape is inspected: a mesh name that happens to look like an id passes no heuristic because none
  is run. A fabric born before the landing is a new fabric. The gate's and the demo's estates are
  born fresh on every run. The next layout change bumps the value.

### 5.2 Pages (old → new)

build.md line 434, the layout sentence: `In RDM's file adapter they open under
<store_dir>/fabrics/<name> and one store root per fabric, <store_dir>/<fabric_id>/{fabric,
meshes/<mesh>, nodes/<mesh>/, connections/<node>/}, where <mesh> is the mesh's logical name. One
writer per item: the fabric-primary writes fabric/ (the fabric's identity, immutable) and
fabric/builds/, every Build record, … each mesh-primary its meshes/<mesh> and nodes/<mesh>/ (where
is_primary and is_fabric_primary record the election's result); each node its own
connections/<node>/.` → `In RDM's file adapter they open under <store_dir>/fabrics/<name> and one
store root per fabric, <store_dir>/<fabric_id>/{layout, fabric/, meshes/, nodes/, connections/,
builds/}: one directory per storage boundary, a record at its entity's id, an index under its entity
named by-<property> (meshes/by-name, nodes/by-name, nodes/by-tag, connections/by-destination), a fact
keyed by a property under that property's index entry, one writer per key, and a layout marker a
root is refused without (R-SR1). The fabric-primary writes fabric/ (the fabric's identity, immutable)
and every topology attempt's Build records under builds/<build_id>/, … each mesh-primary its mesh's
record at meshes/<mesh_id>/ and its name claim at meshes/by-name/<name>/claim, beside which the one
attempt in flight on that name sits as meshes/by-name/<name>/in-flight, inserted by its claimant; its
mesh's node records at nodes/<node_id>/ (where is_primary and is_fabric_primary record the election's
result) and its drift attempts' Build records under the same builds/<build_id>/, the mesh in the key;
the launching admin a birth's runtime row at nodes/<node_id>/runtime; each node its own
connections/<path.name>/.` The Q122 exception and the sentence from `The Build journal holds one
record per fact` on are unchanged.

node-admin-lifecycle.md line 57: `laid out <store_dir>/<fabric_id>/{fabric, meshes/<mesh>,
nodes/<mesh>/, connections/<node>/, runtimes/}` → `laid out <store_dir>/<fabric_id>/{layout, fabric/,
meshes/, nodes/, connections/, builds/} (R-SR1; a birth's runtime row at nodes/<node_id>/runtime)`.

node-primary-update.md line 52: `(its own connections/<node>/ rows are its own, R14)` →
`(its own connections/<path.name>/ rows are its own, R14)`.

### 5.3 Rulings

- R14 item 1 gains: `**Amended (Luke, 2026-10-25; [one directory per entity](../proposal/store-root-one-directory-per-entity.md)):**
  the layout is {layout, fabric/, meshes/, nodes/, connections/, builds/}, one directory per storage
  boundary, a record at its entity's id, indexes by-<property> under the entity, a fact keyed by a
  property under that property's index entry, one writer per key, a layout marker; status/ is gone;
  the mesh directory is the MeshId, and path uniqueness lives in the name claims
  nodes/by-name/<path.name> and meshes/by-name/<name>/claim, fabric-wide.` Item 7: `reads nodes/<mesh>/
  and meshes/<mesh>` → `reads meshes/<mesh_id>/ and the node records whose mesh is its own`.
- R-ML2 item 4 gains: `Amended 2026-10-25 (R-SR1): the recreated mesh shares the name claims
  nodes/by-name/<path.name> and meshes/by-name/<name>/claim and the in-flight row under the name, not
  a directory; its records sit at its own MeshId.` Item 5's reason stands: no MeshId is in a name
  claim's key.
- R-BI6 is unchanged: the journal's location was the adapter's choice, and `builds/<build_id>/` is it.
- R-FN1 is unchanged.

### 5.4 Cells

- The path-asserting tests move to §4's paths: `store_root.rs` (9 assertions), `build_state.rs`
  (11), `rdm-admin/storage.rs` (6), `store_root_boot.rs` (4), `rdm-node/storage.rs` (2),
  `elections/fabric_takeover.rs` (2), `estate.rs`, `node/node_stop.rs`, `elections/node_primary_update.rs`,
  `fabric/named_listeners.rs`, `mesh/fabric_primary_handover.rs` (1 each); `mesh_claims.rs:114`
  (`meshes/by-name/mesh2/in-flight`); `fabric_storage.rs`'s older-root move-rows test reads
  `fabric/primary-moves/`; `demo/tests/connections.rs:86` asserts `fabric/record.json`, unchanged.
- New: a root without `layout`, and one with another value, is refused by name at open, naming the
  path and the value; a populated root in the new layout, every index directory present, reopens;
  a create withdrawn before its node ran leaves its record Withdrawn and a later claim of the name
  wins over it (R-SR2); no test of the nodes store removes a record;
  a recreated mesh's `mesh2.admin.1` claim wins over the Deleted row at `nodes/by-name/` and its
  records sit at the new MeshId; a delayed release of an attempt on the old mesh against the
  re-created mesh's name finds the in-flight row naming another attempt and removes nothing (the
  release takes the row only while it still names its attempt, `mesh_claims.rs`); a re-born node at
  the same `path.name` replaces the dead birth's latest connection rows by key and the
  by-destination rows with them; the adoption launch reads the runtime row at `nodes/<id>/runtime`;
  a new mesh-primary's rebuild reads its mesh's records by the `mesh` field; a Build with a topology
  attempt and a drift attempt folds from one `builds/<id>/` directory; rows written before the first
  state-commit publish from the scratchpad to §4's keys.

## 6. Arms

Two questions, the second only if the first is A.

**Question 2 (R-SR2's file backend).** Does the file backend keep every earlier write of a record?

- **2a (recommended): the latest write only**, as today. The contract promises the latest whole
  record and no removal, which every backend meets; a record's history is the facts it carries and
  the journals that exist for history. Keeping versions adds a per-record sequence, a reader's fold
  to the latest, and growth with no retention rule, for a reader that does not exist (less
  machinery, prove it with telemetry).
- **2b (Architect 1): every write appends**, `nodes/<id>/record/<seq>`, earlier entries remain, the
  latest supplies the current state, an index may name it but never replaces the history; plus a
  cell that writes several versions, reopens the store, finds every version and reads the latest as
  current. The cost is 2a's list; the gain is a disk history of records nobody reads today.

**Question 1 (the layout).**

- **A (recommended): R-SR1 and R-SR2**, the layout of §4, the delta of §5, R14 item 1 and R-ML2 item 4 amended.
- **B: today's layout**, with `status/` deleted, `connections-by-destination/` moved under
  `connections/` and the layout marker added: the writer map stays on disk, two conventions stay,
  Builds stay under `fabric/` and `meshes/`.
- **X: something else.**
