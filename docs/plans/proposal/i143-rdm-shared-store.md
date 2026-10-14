# Proposal: RDM's stores live in one shared location every node of a fabric reaches

## Problem
In Rafka, RDM's five storage interfaces (fabric, mesh, nodes, connections, status) are backed by Rafka's system topics through Rafka's adapter (R-SC3). There is one nodes topic, so every node-admin reads and writes the same rows.

RDM standalone has no equivalent. Each node-admin opens its own file stores inside its own data dir:
- `rafka-node-admin-core/src/admin.rs:2821-2827`: `FileFabricStorage`, `FileMeshStorage`, `FileNodesStorage`, `FileConnectionsStorage` and the Build journal, each `::open(&cfg.data_dir)`.
- The testkit rpc node opens connections in its own data dir too (`rafka-node-rpc-testkit/src/node.rs:333`).
- A maker hands a new birth only the runtime rows it holds (`deployment/pipeline.rs:1220`).

The result: when a mesh's primary seat moves, the new mesh-primary rebuilds from its own local files, and the rows its predecessor wrote are not there. Those include the `.old` row, Deleted rows and config. RDM's tests therefore cannot exercise the behaviour Rafka has, and seats keep hitting this. One example: an `.old` delete after a mesh-primary change could not find the parked node's row.

## Docs of record
- **Row writer ladder**, `node-identity-and-state.md` §4.3 (:540-543, :636-640):
  - fabric row: the fabric-primary, written once at state-commit;
  - mesh row and its members' Node rows: that mesh's primary, no other writer;
  - connections rows: each node its own.
- **R13**, `docs/plans/rulings/i143-rulings-2026-10-11-dev.md`: "Rebuild after a seat change reads the mesh's own rows". The path.name index is mesh-scoped, and writers never share a key.
- **R-SC3**: Rafka's scratchpad adapter until state-commit, then the topics; every node-admin keeps a transient on-disk copy that only a person-started recovery reads.

## Proposal
1. **One store root per fabric.** Every node of the fabric (node-admins and ordinary nodes) is given a `store_dir`, one location every node reaches at the same path.
   - Layout: `<store_dir>/<fabric_id>/{fabric, meshes, nodes/<mesh>, connections/<node>, status/<mesh>}`.
   - The file adapters open there instead of `cfg.data_dir`. The data dir keeps only the node's own runtime material (endpoint key, identity, runtime record, app data).
2. **Writers stay exactly the ladder.**
   - The fabric-primary writes `fabric/`.
   - Each mesh-primary writes `meshes/<its mesh>` and `nodes/<its mesh>/`.
   - Each node, ordinary nodes included, writes its own row to the shared `connections/<itself>`.
   - No two writers share a key, so no locks or leases are needed across processes.
3. **Write mechanics stay as today.** One file per key, written as a temp file then renamed, fsync'd, on the blocking pool (`record_store.rs:117-160`). The insert-and-fail name claim (R13) is the adapter's create-exclusive insert (`insert_file`, :131-133), which is atomic on one filesystem. Readers point-read by key. Nothing reads, edits and writes back a row, so no read-modify-write.
4. **The provider supplies the location.** `store_dir` is part of the deployment contract, the same way the data dir is. Each provider gives every node of a fabric the same location. RDM interprets nothing about how that location is backed.
   - Process provider: one directory per estate under the estate root, passed to every node of that estate. Tests get one place per fabric, as Rafka has one topic.
   - Any other provider supplies its own equivalent; a container provider, for example, would mount one shared volume at `store_dir`. That is that provider's implementation; RDM's code stays the same whichever provider runs.
5. **Rebuild on seat change** reads `nodes/<mesh>/` and `meshes/<mesh>` from the shared root. It is the same read the new primary does today, just in a different place.
6. **Failure is named, with no fallback.** A missing or unreadable `store_dir` fails the node-admin's boot by name, naming the path and the cause. No silent fallback to the data dir.

## What it does not change
- The Rafka path: Rafka's adapter keeps using the topics; this is only RDM's own adapter.
- The writer ladder, R13, wire formats, gossip and the Build carrier.
- No durable row format moves. Only where the files live changes.

## Acceptance
- A mesh seat change: the new mesh-primary rebuilds and holds the predecessor's rows (Node rows, `.old`, Deleted, config), shown on a live mesh with spans.
- A `.old` delete after a mesh-primary change finds the parked node's row in the shared store.
- Two mesh-primaries write their own meshes' rows, and ordinary nodes their own connections rows, at the same time without touching each other's keys (functional cell over one shared directory).
- A missing `store_dir` is refused by name at boot.

## Open questions for the architects
1. **Build journal:** stay per-admin (it already replicates over the Build topic and 0x1F FetchBuildFacts), or move to `<store_dir>/<fabric_id>/builds/`?
2. **R-SC3's per-admin transient copy:** in RDM standalone, does the shared store replace it, or does each admin still keep its own copy for person-started recovery?
3. **What a provider must guarantee:** the file adapter needs atomic rename and create-exclusive on `store_dir`. Is that the stated provider contract, and is a provider whose location spans hosts in scope?
