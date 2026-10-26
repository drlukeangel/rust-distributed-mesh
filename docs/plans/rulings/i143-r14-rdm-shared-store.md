# R14 — RDM's stores live in one shared root per fabric (ruling on `docs/plans/proposal/i143-rdm-shared-store.md`)

Ruled 2026-10-11 (architect 2, with architect 1's review folded in). This page supersedes the
proposal where they differ. Code paths are RDM i143-land1 at 144a45bd; re-read at the tip.

## Problem

In Rafka, RDM's five storage interfaces (fabric, mesh, nodes, connections, status) are backed by
Rafka's system topics through Rafka's adapter (R-SC3): one nodes topic, so every writer and
reader sees the same rows. RDM standalone has no equivalent. Every node-admin opens its own file
stores under its own data dir (`rafka-node-admin-core/src/admin.rs:2821-2828`, `:3373`), and the
testkit rpc node opens its connections store in its data dir too
(`rafka-node-rpc-testkit/src/node.rs:333`). When a mesh's primary seat moves, the successor
rebuilds from its own files and the predecessor's rows (`.old`, Deleted, config) are not there.
That is a defect in the standalone store's location, not in the writer ladder.

## Docs of record

- Writer ladder: `docs/architecture/fabric-node-lifecycle.md` §4.3 (`:540-545`, `:636-640`) and
  `lifecycles/fabric-state-commit.md` step 3: the fabric-primary writes the Fabric row once; each
  mesh-primary writes its Mesh row and its members' Node rows, no other writer; each node writes
  its own connections rows.
- R13 (`i143-rulings-2026-10-11-dev.md`): the path.name index is mesh-scoped by construction, so
  writers never share a key; rebuild after a seat change reads the mesh's own rows.
- R-SC3: Rafka's scratchpad adapter until state-commit, then the topics; every node-admin keeps a
  transient on-disk copy that only a person-started recovery reads.
- R-EL1a: a seat moves by one committed seat row; the holder's execution authority ends at that
  row's epoch.

## Ruling

1. **One store root per fabric.** Node-admin and node config gain `store_dir`: one location every
   node of the fabric reaches on the same backing namespace. Layout
   `<store_dir>/<fabric_id>/{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}`,
   where `<mesh>` is the mesh's logical name, the path segment, never the MeshId: a mesh created
   again under the same name shares the directory and claims over the Deleted rows (R-MN1).
   The file adapters open there. The data dir keeps only the node's own runtime material
   (endpoint key, identity, runtime record, app data) and the R-SC3 transient copy.
   **Amended (Luke, 2026-10-25; [one file per store](../proposal/store-root-one-directory-per-entity.md)):**
   the layout is {layout, fabric.jsonl, meshes.jsonl, nodes.jsonl, connections.jsonl, builds.jsonl,
   locks/}, one append-only JSONL file per storage boundary, every write the whole record as one line
   with its format and key, nothing removed, indexes folded, a layout marker; status/ is gone; path
   uniqueness lives in the name locks under locks/name/, as today's exclusive inserts, fabric-wide.
2. **Writers are exactly the ladder.** Fabric-primary: `fabric/`. Each mesh-primary:
   `meshes/<its mesh>` and `nodes/<its mesh>/`. Each node, ordinary nodes included:
   `connections/<itself>/`. Keys never overlap across writers, so no cross-process lock or lease
   exists anywhere.
3. **The name claim is a create-exclusive insert under locks/name/, as the code has made it for both names (storage.rs:356, :1303), the lock being the claim; the attempt in flight is the other, under locks/in-flight/.**
   `claim_node_row` (`storage.rs:339`) reads the row at the name key, decides in `claim_against`
   (`:216`: same node → held by it; a live row of another node → `NameHeld { by }`; absent or
   Deleted → written) and writes the whole row from the record the mesh-primary holds, under its
   in-process `row_writes` lock. With one writer per key, serialized in-process, this is not the
   two-writer read-modify-write the canon bans; and a Deleted row must stay claimable so a node
   re-created at that path.name seeds its config from it. The proposal's item 3 ("the adapter's
   create-exclusive insert") is corrected to this; the code does not change here. The record
   store's hard-link insert (`record_store.rs:167`) stays what it is used for today.
4. **Outstanding writes across a seat transfer are drained by ordering inside the holder.** The
   record store's write guard today lives in the async caller (`record_store.rs:120`): a caller
   cancelled while awaiting its `spawn_blocking` job drops the guard while the disk write goes on.
   Fix at the root: the guard is taken owned and moved into the blocking closure, so it is held
   until the disk operation finishes and no cancellation can release it early. With that, the
   SeatTransfer step (R-EL1a) in the holder P is, in order: set the write cut-off; take each of
   its stores' write locks once, which now waits for every issued disk job; write the seat row;
   answer SeatsCommitted. A write queued on the lock before the cut-off re-checks the cut-off
   after it acquires the lock and is refused by name, so the single-writer boundary covers
   outstanding and queued writes, not only new calls. A crashed P has no outstanding write; a P
   whose loss is proven has an exited process. Nothing cross-process is added.
5. **The filesystem contract is stated and enforced.** The adapter relies on serialized O_APPEND
   writes on the local filesystem, checked write results and file fsync, create-exclusive insertion
   under locks/, and coherent reads on one backing namespace (R-SR3). The same path string proves nothing; the provider supplies the same
   namespace with those guarantees to every node of the fabric. The directory fsync result,
   ignored today at `record_store.rs:162` and `:176`, is propagated: a write whose directory
   fsync fails is a failed write, named. The same holds for deletion: `FileRecords::remove`
   (`record_store.rs:147`) removes the file without syncing its directory, so a release or delete
   that must survive a crash does not; it syncs the directory and propagates the failure. Temp
   file names carry the writer's incarnation (`.<key>.<incarnation>.json.tmp`).
6. **Providers.** Process provider first: one directory per estate under the estate root, handed
   to every node of that estate. Any other provider qualifies by a functional cell that proves
   the four guarantees on its actual backing store, cross-host included; without that proof it
   is refused by name. RDM interprets nothing about how the location is backed.
7. **Rebuild on a seat change** folds meshes.jsonl and nodes.jsonl and keeps its mesh's records from the shared root
   and takes live digests as current. Rows supply what gossip does not retain (`.old`, Deleted,
   config); liveness comes from the aggregate and 0x1E (R-T6, R-T7). A row never overrides a live
   digest's status.
8. **Failure is named, with no fallback.** A missing or unreadable `store_dir` fails the node's
   boot by name, naming the path and the cause. Nothing falls back to the data dir.
9. **Before state-commit nothing changes:** rows stay in the scratchpad (`fabric-state-sync.md`),
   except a declared state acknowledged as Applied (Q54), the Build facts and attempt claims (Q61) and a
   launching admin's runtime row (the launcher's durable map, written once; R-SR1, 2026-10-25), which are
   written to the shared root when they are committed. At commit-state each writer writes
   what it holds to the shared root; after it, writes go there directly.

## Answers to the proposal's open questions

1. **Build journal: per admin.** It is the fabric-primary's claim log, replicated by the Build
   topic and 0x1F FetchBuildFacts. A copy in the shared root would be a second truth.
2. **Connections rows: each node writes its own** into the shared root and therefore receives
   `store_dir` like its data dir. Writing through the mesh-primary would move the writer boundary.
3. **The shared store is authoritative.** R-SC3's transient copy stays, read only by a
   person-started recovery, never a fallback.
4. **Process provider first; others by proof** (item 6).

## What it does not change

The Rafka adapter and its topics; the writer ladder; R13's validation order and the
insert-and-fail semantics of a claim as seen by callers (`NameHeld { by }`); wire formats, gossip,
the Build carrier. No durable row format moves: only where the files live and the temp-file name.

## Code changes

1. `AdminConfig` and the node launch gain `store_dir` (env `RDM_STORE_DIR`), required, refused by
   name when missing. `admin.rs:2821-2828`, `:3373` open under
   `<store_dir>/<fabric_id>/...`; `node.rs:333` likewise for connections.
2. `record_store.rs`: the write guard is owned and moved into the blocking closure (held across
   the disk job); directory fsync errors propagated in `write_file`, `insert_file` and `remove`;
   incarnation-tagged temp names.
3. SeatTransfer (R-EL1a) step in the holder: cut-off, drain by taking each lock once, seat row,
   reply; queued writes re-check the cut-off after acquiring the lock (item 4).
4. Process provider: estate-level `store_dir` handed to every launch of the estate.
5. The rebuild path reads the mesh's rows from the shared root (item 7).

## Acceptance

- A mesh seat change: the successor rebuilds and holds the predecessor's rows (Node rows,
  `.old`, Deleted, config), shown on a live mesh with spans.
- A `.old` delete after a mesh-primary change finds the stopped node's row.
- Two mesh-primaries write their own meshes' rows at once on one shared root without touching
  each other's keys.
- An old primary's write delayed across handover lands before the seat row, never after, including
  a caller cancelled during a delayed write.
- A write queued before the cut-off and granted the lock after it is refused by name.
- An ordinary node's connections rows land under `connections/<node>/` in the shared root and the
  successor mesh-primary reads them there.
- Two independent adapter instances claiming one name in one mesh cannot occur by the ladder;
  the cell that proves it is the handover one above, not a two-process race.
- The Deleted row's config seeds the replacement before its row is overwritten at that key (the
  claim writes the new row at the same name key; no separate tombstone is kept).
- A directory fsync failure fails the write, the insert or the delete by name.
- A missing `store_dir` is refused by name at boot.

## Story

Filed under i143.e12 as its own story (shared store root), after R-EL1a's seat row lands, since
item 4 is a step of that transfer.
