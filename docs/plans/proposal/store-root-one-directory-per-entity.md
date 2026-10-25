# Proposal: the store root, one directory per entity (R-SR1)

**Status:** proposal, 2026-10-25. Asks Luke's yes on one rule for RDM's file backend's layout of the
fabric's shared store root, and on the amendment of R14 item 1 and R-ML2 item 4 that it needs.
Storage layout only: no read, no wire frame and no trait changes. Rafka binds the same traits to its
own topics and is untouched.

## 1. The question

Luke, 2026-10-25, on the reads proposal: the stores are one per entity, fabric, mesh, nodes,
connections, builds; why is anything nested? The storage boundaries are five and flat
(`storage_backend.rs`: `fabric.storage`, `mesh.storage`, `nodes.storage`, `connections.storage`,
`builds.storage`). The disk is not. "Let's not keep the mess because it already exists. What is the
correct way?"

## 2. Today

`store_root.rs` lines 4 to 11 state the layout under `<store_dir>/<fabric_id>/`:

> `fabric/` (the fabric-primary's identity, seats, mesh deletes and `fabric/builds/` Build journal),
> `meshes/<mesh>` (each mesh's primary's Mesh row), `meshes/<mesh>/claims/` (the mesh's one attempt
> in flight …), `meshes/<mesh>/builds/` (the mesh's primary's Build receipts; R-BI6), `nodes/<mesh>/`
> (each mesh's primary's members' Node rows and contacts), `connections/<node>/` (each node its own)
> and `status/<mesh>/` (each mesh's primary's status rows). Beside the per-fabric roots,
> `<store_dir>/fabrics/<name>` is the fabrics' name index (R-FN1).

Three things that text does not say:

- **`status/<mesh>/` is dead.** `StoreRoot::status` has no caller but its own test; nothing writes or
  reads under it. Its rows were folded into the node record (`storage.rs` header: "declared states,
  operation effects, silence, data-dir facts, cert dates"). R14 item 1 and the header still list it,
  and `status_rpc.rs:2361` still says the Pending of another mesh's birth is put there.
- **There is a sixth top-level directory**, `connections-by-destination/<node id>/`, the by-destination
  index (`StoreRoot::connections_to`), beside the five the header names.
- **Two directories are keyed by names that can change.** `connections/<path.name>/` is keyed by a
  name `node.name.update` replaces, safe today only because R-CR1 lets a node be renamed only with no
  connection held. `meshes/<mesh>/` and `nodes/<mesh>/` are keyed by the mesh's name, by ruling
  (R14 item 1: "the mesh's logical name, the path segment, never the MeshId"; R-ML2 item 4: "the
  recreated mesh shares the directory"; item 5: "a MeshId in the key would let two `mesh2.admin.1`
  exist at one path"). The property those rulings protect is path uniqueness of a name claim across
  a mesh's re-creation, not the directory.

The directories are the writer map drawn on disk: the fabric-primary writes `fabric/` and
`fabric/builds/`, each mesh-primary its `meshes/<mesh>`, `nodes/<mesh>/` and `meshes/<mesh>/builds/`,
each node its `connections/<node>/`, and "no two writers write one key" (build.md:434). The Build
journal splits one entity across two of them, `fabric/builds/` for the fabric-primary's attempts and
`meshes/<mesh>/builds/` for a mesh-primary's drift attempts, so the Builds, which Q167 puts outside
the fabric, sit on disk under it.

## 3. The rule

- **R-SR1, one directory per entity.** Under the fabric's store root, one directory per storage
  boundary, named as the boundary is: `fabric/`, `meshes/`, `nodes/`, `connections/`, `builds/`.
  A record sits at its entity's id. An index sits under its entity's directory, named
  `by-<property>`. A key has exactly one writer, so the writer map is carried in the key and no
  directory is a writer's. A fact lives in the store of the object it describes. The fabrics' name
  index stays at `<store_dir>/fabrics/<name>` above the roots as R-FN1 ruled it: it answers which
  root a name is before any root is opened.

## 4. The layout

```text
<store_dir>/fabrics/<name>                                 → fabric id (R-FN1, unchanged)
<store_dir>/<fabric id>/
  fabric/record                                            the Fabric record
  fabric/seat-moves/<T>--<node id>--<incarnation>          the one fabric-wide sequence of primary moves (Q35, Q109)
  meshes/<mesh id>/record                                  the Mesh record
  meshes/<mesh id>/in-flight                               the mesh's one attempt in flight (its key is the mesh)
  meshes/by-name/<name>                                    the mesh's name claim → mesh id
  nodes/<node id>/record                                   the node record (its row and its births' facts)
  nodes/by-name/<path.name>                                the node's name claim → node id
  nodes/by-tag/<label>/<node id>                           the tags index (R-TG1)
  connections/<source id>/<destination id>-<kind>          the latest fact per (source, destination, kind)
  connections/<source id>/history.jsonl                    the source's raw log
  connections/by-destination/<destination id>/<source id>-<kind>
  builds/<build id>/<fact key>                             every fact of the Build, both the fabric-primary's
                                                           attempts and each mesh's drift attempts
```

One writer per key, directory by directory:

| Directory | Key | Writer |
|---|---|---|
| `fabric/` | the fabric; `T` of a primary move | the fabric-primary; the primary move's writer at the next `T` |
| `meshes/<mesh id>/` | the mesh id | that mesh's primary (the fabric-primary for a mesh with none) |
| `meshes/<mesh id>/in-flight` | the mesh | its claimant, insert-and-fail (build.md:405): one attempt in flight per mesh, so the row's key is the mesh and it stays with the mesh, not the Build |
| `meshes/by-name/<name>` | the name | the claiming mesh's primary, a conditional write over a Dead row (R14 item 3, R-ML2 item 3) |
| `nodes/<node id>/` | the node id | the mesh-primary of the node's mesh |
| `nodes/by-name/<path.name>` | the path.name | the claiming mesh-primary, over a Deleted row only |
| `nodes/by-tag/<label>/<node id>` | (label, node id) | the node's mesh-primary |
| `connections/<source id>/` | (source, destination, kind) | the source node |
| `connections/by-destination/<destination id>/<source id>-<kind>` | (destination, source, kind) | the source node, beside its own row |
| `builds/<build id>/` | `<build_id>-<attempt>` or `<build_id>-<mesh>-<attempt>`, a claim, a receipt per step | the fabric-primary for a topology attempt, the mesh-primary for its drift attempt: the mesh is in the key (R-BI7), so the two never share one |

**Path uniqueness holds without the mesh directory.** The claim on `mesh2.admin.1` is the key
`nodes/by-name/mesh2.admin.1`, fabric-wide and independent of any mesh id, so a mesh created again
under the same name, a new mesh with a new id (R-ML2 item 3), contends on the same key and wins only
over the Deleted row, exactly as today. Two `mesh2.admin.1` cannot exist because the claim is by
name; where the record sits does not enter. The same holds for `meshes/by-name/<name>` over a Dead
row.

**A new mesh-primary's rebuild** (R14 item 7, "reads `nodes/<mesh>/` and `meshes/<mesh>`") reads
`meshes/<mesh id>/record` and the node records whose `mesh` field is its own, one pass over
`nodes/`. At the record's size and the record's count (about 142 nodes in the largest estate in the
record) the pass is one directory listing and one read per node, which the rebuild does today under
its mesh directory. A `nodes/by-mesh/<mesh id>/` index is the arm not recommended: a second key to
keep right for a read that happens at a primary change.

**The Build journal is one directory per Build.** R-BI6 made the journal's location the adapter's
and provider's choice; this is the file adapter's choice. The fold over a Build's facts reads one
directory instead of `fabric/builds/<id>/` plus every `meshes/<mesh>/builds/<id>/`.

## 5. Delta

### 5.1 Code (RDM's file backend only; no trait, wire or Rafka change)

- `rdm-node/src/store_root.rs`: the header states §4; `nodes(mesh)` → `node(id)`, `node_names()`,
  `node_tags()`; `meshes(mesh)` → `mesh(id)`, `mesh_names()`; `fabric_builds()` and `mesh_builds(mesh)`
  → `build(id)`; `connections(path.name)` → `connections(node id)`; `connections_to(id)` under
  `connections/by-destination/`; `status(mesh)` deleted.
- `rdm-admin/src/storage.rs`: `FileNodesStorage` records at `nodes/<id>/`, the name claim at
  `nodes/by-name/`, the tags index at `nodes/by-tag/`; `FileMeshStorage` at `meshes/<id>/`, the claim
  at `meshes/by-name/`, `in-flight` beside the record; `nodes_answer` and the rebuild read by the
  record's `mesh` field.
- `rdm-admin/src/build_state.rs`: `FileJournal` at `builds/<build id>/`, one directory per Build;
  `refused_journal_files` and the revision walk read `builds/` only.
- `rdm-node/src/storage.rs`: `FileConnectionsStorage` keyed by node id; the by-destination index rows
  under `connections/by-destination/`.
- `rdm-admin/src/status_rpc.rs:2361`: the comment names the node record, not `status/<mesh>/`.
- **An old root is refused by name, no migration.** At open, a root holding `status/`,
  `connections-by-destination/`, `fabric/builds/`, a `meshes/` or `nodes/` entry that is not a
  canonical Crockford60 id, or a `connections/` entry that is not one, is refused naming the path
  and the layout rule, in the shape `fabric_storage.rs` uses for `fabric/pointers/`, `fabric/seats/`
  and `fabric/handovers/` today. A fabric born before the landing is a new fabric. The gate's and the
  demo's estates are born fresh on every run.

### 5.2 Pages (old → new)

build.md line 434, the layout sentence: `In RDM's file adapter they open under
<store_dir>/fabrics/<name> and one store root per fabric, <store_dir>/<fabric_id>/{fabric,
meshes/<mesh>, nodes/<mesh>/, connections/<node>/}, where <mesh> is the mesh's logical name. One
writer per item: the fabric-primary writes fabric/ (the fabric's identity, immutable) and
fabric/builds/, every Build record, … each mesh-primary its meshes/<mesh> and nodes/<mesh>/ (where
is_primary and is_fabric_primary record the election's result); each node its own
connections/<node>/.` → `In RDM's file adapter they open under <store_dir>/fabrics/<name> and one
store root per fabric, <store_dir>/<fabric_id>/{fabric/, meshes/, nodes/, connections/, builds/}: one
directory per storage boundary, a record at its entity's id, an index under its entity named
by-<property> (meshes/by-name, nodes/by-name, nodes/by-tag, connections/by-destination), and one
writer per key (R-SR1). The fabric-primary writes fabric/ (the fabric's identity, immutable) and
every topology attempt's Build records under builds/<build_id>/, … each mesh-primary its mesh's
record and in-flight row at meshes/<mesh_id>/, its mesh's node records at nodes/<node_id>/ (where
is_primary and is_fabric_primary record the election's result) and its drift attempts' Build
records under the same builds/<build_id>/, the mesh in the key; each node its own
connections/<node_id>/.` The Q122 exception and the sentence from `The Build journal holds one
record per fact` on are unchanged.

node-primary-update.md line 52: `(its own connections/<node>/ rows are its own, R14)` →
`(its own connections/<node_id>/ rows are its own, R14)`.

### 5.3 Rulings

- R14 item 1 gains: `**Amended (Luke, 2026-10-25; [one directory per entity](../proposal/store-root-one-directory-per-entity.md)):**
  the layout is {fabric/, meshes/, nodes/, connections/, builds/}, one directory per storage
  boundary, a record at its entity's id, indexes by-<property> under the entity, one writer per key;
  status/ is gone; the mesh directory is the MeshId, and path uniqueness lives in the name claims
  nodes/by-name/<path.name> and meshes/by-name/<name>, fabric-wide.` Item 7: `reads nodes/<mesh>/
  and meshes/<mesh>` → `reads meshes/<mesh_id>/ and the node records whose mesh is its own`.
- R-ML2 item 4 gains: `Amended 2026-10-25 (R-SR1): the recreated mesh shares the name claims
  nodes/by-name/<path.name>, not a directory; its records sit at its own MeshId.` Item 5's reason
  stands: no MeshId is in a name claim's key.
- R-BI6 is unchanged: the journal's location was the adapter's choice, and `builds/<build_id>/` is it.
- R-FN1 is unchanged.

### 5.4 Cells

- The path-asserting tests move to §4's paths: `store_root.rs` (9 assertions), `build_state.rs`
  (11), `rdm-admin/storage.rs` (6), `store_root_boot.rs` (4), `rdm-node/storage.rs` (2),
  `elections/fabric_takeover.rs` (2), `estate.rs`, `node/node_stop.rs`, `elections/node_primary_update.rs`,
  `fabric/named_listeners.rs`, `mesh/fabric_primary_handover.rs` (1 each); `mesh_claims.rs:114`
  (`meshes/<mesh_id>/in-flight`); `demo/tests/connections.rs:86` asserts `fabric/record.json`,
  unchanged.
- New: a root holding each old directory in turn is refused by name at open, naming the path; a
  recreated mesh's `mesh2.admin.1` claim wins over the Deleted row at `nodes/by-name/` and its
  records sit at the new MeshId; a new mesh-primary's rebuild reads its mesh's records by the `mesh`
  field; a Build with a topology attempt and a drift attempt folds from one `builds/<id>/`
  directory.

## 6. Arms

- **A (recommended): R-SR1**, the layout of §4, the delta of §5, R14 item 1 and R-ML2 item 4 amended.
- **B: today's layout**, with `status/` deleted and `connections-by-destination/` moved under
  `connections/`: the writer map stays on disk, Builds stay under `fabric/` and `meshes/`.
- **X: something else.**
