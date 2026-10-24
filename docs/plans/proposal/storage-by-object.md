## Storage by object: every row on the object it describes, mapped to rafka's ops topics

Proposal for i143. The RDM side is i143's: `status.storage` goes and every row moves to the store of the
object it describes, with the keys, writers, durability and receipt rules below. The rafka side is i141's:
the mapping of each RDM store onto rafka's ops topics and the retention change (Q140). Each line below says
whether Luke has ruled on it.

### Problem
`status.storage` holds rows of every kind: mesh status, fabric events, a birth's declared state, an
operation effect, a silence, a lost or unreadable data dir, member certs and their receipts, mesh issuers
and their receipts. It is not an object (rafka's `ops.admin-rows`, its mirror, is dead). A dead
runtime's end is held nowhere (the routing job's two exited containers at dc3661eb4).

### Rule (Luke, 2026-10-23)
One store per object. A row lives on the object it describes, written by that object's writer (the
fabric-primary for the fabric, Builds and its own mesh; each mesh-primary for its own mesh and its
nodes). A store keeps no row about another object. Every RDM store maps onto a topic rafka already has.
`status.storage` goes.

### The map

| Object | RDM store | rafka topic | Rows on it |
|---|---|---|---|
| Fabric | `fabric.storage` | `<system_org>.ops.fabrics` | the Fabric record (id, name) and its name's claim; the primary records; the fabric's status and its fabric events; the fabric root's issue date and the fabric CA's issue date and expiry (the root has no TTL: it changes only on `fabric.root.update`; the root and the CA stay local to each node-admin, fabric-certs.md §Custody; no receipts); the fabric's state, shutdown among its states (no topic of its own) |
| Mesh | `mesh.storage` | `<system_org>.ops.meshes` | the mesh row (MeshId, name, dead); the mesh's status; its in-flight row |
| Node | `nodes.storage` | `<system_org>.ops.nodes` | the node at its path.name (config, tags); each birth: its runtime (runtime fact, data dir), its declared state, the operation effects applied to it, its member cert's expiry, its silence (`PendingReconnect`, `Dead`, none once heard), a lost or unreadable data dir; a node-admin's contacts (the births it heard), on its own node (no topic of its own); a node-admin birth's signing cert issue date and expiry. Only the dates are stored; every cert stays local to its node, like the root, and the mesh has no issuer (Luke, 2026-10-23) |
| Node connections | `connections.storage` | `<system_org>.ops.nodes.connections` | the connections log (unchanged) |
| Build | `builds.storage` | `<system_org>.ops.builds` | the Build facts and each attempt's context; the attempt's receipts name the births it made, and that is the only sense in which a birth is the Build's state: never a second copy of a node row (one writer per item) |

Ruled by Luke, 2026-10-23: the rule; `ops.admin-rows` dead; shutdown is a state of the fabric; the births are
the Build's state; the contacts are on the node; the mesh has no issuer, a node-admin's signing cert dates
are on its node row; only cert dates are stored; Q141 A (below). Q139, Q140 and Q141 are recorded in `docs/plans/rulings/INDEX.md` (2026-10-23).

### Keys, writers, fold

Replaced by [storage-whole-records.md](storage-whole-records.md)'s "The rule", "The objects" and
"Durability" (Luke, 2026-10-23), copied here; the map's rafka topics above stand.

#### The rule (Luke, 2026-10-23)

RDM holds every object it writes, whole, in its cache. A write is:

1. take the object from the cache;
2. apply the change to it (a state, an effect, a mark, dates);
3. write the whole object to storage;
4. once storage answers, replace the cached object with the updated one.

No write reads storage first: no read-modify-write and no version check. Storage stores and returns
whole objects and decides nothing (Q109). Storage is read only to fill a cache, and the cache is filled
whenever write ownership of an object is acquired, before any mutation of it is accepted: at boot; when an
admin becomes its mesh's primary and folds its mesh's records; when the fabric-primary becomes a mesh's
writer because its last node-admin is gone, loading that mesh's records then and never from its boot-time
copy; when a node-admin becomes fabric-primary, loading the Fabric record.

One writer per object does not serialize two tasks inside that writer: a declaration handler and a
renewal task can both take the cached record, and the second write would erase the first. Every mutation
of an object holds that object's own lock from the cache read through the mutation, the store's
acknowledgment and the cache replacement; two updates of one object never overlap.

Because the cache is the copy a write starts from, each object has exactly one writer, the writer map's:
the fabric-primary writes the fabric record and its own mesh's records; each mesh-primary writes its own
mesh's record and its nodes' records; the fabric-primary writes a mesh's records only while that mesh
has no primary (mesh-create, node-mesh-join, mesh-left with no survivor). Fabric recovery's executor
writes only the records its own attempt's operations require (Q122), as today.

#### The objects

| Object | Key | What it holds | Writer | Cache that holds it |
|---|---|---|---|---|
| Fabric record | `fabric_id` | identity (id, name), immutable inside a record written on every fabric event; the fabric events applied (`ready-for-traffic`, `shutdown-initiated:<by>`) | the fabric-primary | the fabric-primary's applied set (`Declared.fabric` today) |
| Mesh record | mesh id | id, name, dead; the states entered (Pending, ReadyForTraffic, Leaving, Dead) | the mesh's primary; the fabric-primary while the mesh has none | the writer's applied set (`Declared.meshes` today) |
| Node record | minted node id | today's node row (path.name, status, config, tags, allocated, executable, renamed_from, app types, inherited_by) and, per incarnation: the declared states, the operation effects, the silence mark, `data-dir-lost`, `data-dir-unreadable`, the member cert dates, a node-admin's signing cert dates; `predecessor`, the exact birth this node replaced, written at its create from the recovery attempt that named it (`inherited_by` points the other way and is not followed backwards). The record keeps the facts of its current incarnation and of the one a restart replaced (`RDM_SUPERSEDES`), nothing older; the predecessor is another node with its own record: the Build journal keeps the history, and a record rewritten on every silence mark stays small | the node's mesh-primary; the fabric-primary while the mesh has none | the writer's node rows (`node_rows::WrittenRows` today), into which `Declared.nodes`, `Declared.operations` and the silence memo fold |

Outside the records, unchanged: the fabric name's claim, each mesh's in-flight row and each node's name
claim (`NameHeld { by: node.id }`, node-name-update.md) are claims, an insert that fails when the key is
held, raced by design and never updated; the node's name claim is the path.name-to-node-id lookup a rename
moves and recovery uses to find the record at a path.name, so a rename is a claim move plus one record
write; the Build journal; a node's connections log; an admin's own row, contacts and launch handles in its
data dir.

The tags index is RDM's to keep, not the store's: today `put_node_row` reads the previous row inside the store
to move the tag entries (storage.rs:1457-1462), a read before a write and logic in the provider. RDM
computes the index moves from its cached record before the write and the store writes the index entries
blindly. A record write that succeeds while an index write fails keeps the pending index moves on the
writer until the index acknowledges them; the retry never recomputes an empty difference from the
already-updated cache.

A record's facts stay independent inside it, keyed by their full identity: one declared entry per state
(a late Pending never removes ReadyForTraffic; the reader takes the greatest), one effect per
(build_id, attempt, operation). A start beginning a birth's next cycle clears that birth's declared states
on the cached record and keeps its effects; the record is then written whole.

#### Durability

A Node record carries the declared states, and an Applied declaration is durable before the reply (Q54),
so the whole Node record is written straight to the store, never held in the scratchpad, before
state-commit too. The Fabric and Mesh records keep the scratchpad until state-commit, as today. A fabric
that dies before its first state-commit is abandoned (Q74), so a node record on its root changes nothing.

### Outside the map, by design

A node-admin's own `self` row and the launch handles of the runtimes it launched live in its own data
dir, beside `node-key`, `ca-secret` and `fabric-ca`: a restarting admin reads them before it opens any
store, to learn which fabric and mesh it belongs to, and cannot read a topic whose fabric it does not yet
know. They are a bootstrap seed, not a record of the fabric; what the fabric needs to know of a birth's
runtime is on the node in `nodes.storage`. Every record RDM keeps of the fabric is in one of the five
stores above, and each store is one rafka topic. `ops.nodes.offline` is folded into `ops.nodes` (Luke,
2026-10-23): a node's offline episodes are facts on the node, like its silence; `topology-nodes` is the
nodes projection, not a store.

### What moves, row by row (`crates/rdm-admin/src/status_storage.rs`)

| Row today | Goes to | Writer |
|---|---|---|
| `MeshStatusRow` | the mesh's status, `mesh.storage` / `ops.meshes` | the mesh-primary of that mesh; the fabric-primary while the mesh has no primary (Keys, writers, fold) |
| `FabricEventRow` | the fabric's events, `fabric.storage` / `ops.fabrics` | the fabric-primary |
| `NodeDeclaredRow` | the birth's declared state, `nodes.storage` / `ops.nodes` | the node's mesh-primary; the fabric-primary while the mesh has no primary (the joining birth's Pending, `put_node_declared_of` today) |
| `NodeOperationRow` | the birth's operation effects, `nodes.storage` / `ops.nodes` | the node's mesh-primary |
| `NodeSilenceRow` | the birth's silence, `nodes.storage` / `ops.nodes` | the node's mesh-primary (its tickle loop, as today: `status_storage.rs:136`) |
| `NodeDataDirLostRow`, `NodeDataDirUnreadableRow` | the birth, `nodes.storage` / `ops.nodes` | the node's mesh-primary |
| `NodeCertRow` | the birth's member cert expiry (`issued_at`, `expires_at`, `renew_at`), `ops.nodes` | the issuer, on `CertRenewed` (Q141) |
| `NodeCertReceiptRow` | gone (Q141) | |
| `MeshIssuerRow` | the node-admin birth's signing cert issue date and expiry, `ops.nodes` | written on that admin's `IssuerUpdated` (Q141) |
| `MeshIssuerReceiptRow` | gone (Q141) | |
| a dead runtime's end (the problem's two exited containers) | not in this proposal: Q111 removed the exit rows and no page names such a row; it needs one page sentence on node-delete first, and no field nothing reads | |

`StatusStorage` and `MemoryStatusStorage` go; their methods move to the object stores' interfaces
(`fabric_storage`, `storage::{MeshStorage, NodesStorage}`). Callers today: `admin/{boot,mesh_delete,records,recovery,runner}.rs`,
`admin.rs`, `cert_renewal.rs`, `control_plane.rs`, `fabric_state.rs`, `join.rs`, `mesh_delete.rs`,
`node_properties.rs`, `offline.rs`, `status_declare.rs`, `status_rpc.rs`, `storage_backend.rs`, `views.rs`,
`rdm-test/src/rig.rs` and 16 test files. No migration: this moves durable formats, and the canon's marker
(`durable format moved`) routes such a landing to a rebirth, never a restage, so no old `status.storage`
record is ever read, and no issue is ever confused with a receipt by a copy.

### What goes (i143)

- `status.storage` as an interface and a store: `StatusStorage`, `FileStatusStorage`, `MemoryStatusStorage`,
  the `status/<mesh>/` directories on the store root (`meshes`, `fabric`, `declared`, `operations`,
  `silence`, `data-dir-lost`, `data-dir-unreadable`, `data-dir-lost-at`, `data-dir-unreadable-at`,
  `node-certs`, `node-cert-receipts`, `issuers`, `issuer-receipts`) and their formats; the "other
  meshes" rows it opened on first use; its `StorageBoundaries.status` slot. The five storage interfaces
  become four: fabric, mesh, nodes, connections, beside the Build journal.
- The cert and issuer receipt rows and their formats (Q141): the written expiry is the receipt.
- The idea that a row about one object can live in a store named for something else: a new row goes in
  the store of the object it describes, never the nearest one.

### Page lines, old to new, landing with the code

- `build.md:56`, `:102`, `:413`: "the five storage interfaces, fabric, mesh, nodes, connections and status"
  becomes "the four storage interfaces, fabric, mesh, nodes and connections"; `:413`'s layout
  `{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}` loses `status/<mesh>/`, and
  its writer sentence loses "and `status/<mesh>/`".
- `node-admin-lifecycle.md:57`: the same layout loses `status/<mesh>/`.
- `lifecycles/states.md:51`: "is written to `status/<mesh>/declared` on the shared store root before the
  reply" becomes "is written to the birth's `declared/<state>` row in `nodes/<mesh>/` on the shared store
  root before the reply".
- `node-identity-and-state.md:656`: "is its status rows: the `PendingReconnect` and" becomes "is rows on
  the node: the `PendingReconnect` and".
- `fabric-certs.md:104`: "a status-row kind beside" becomes "a row on the birth in `nodes.storage` beside".
- `lifecycles/node-cert-renew.md` and `lifecycles/mesh-issuer-renew.md`: the Q141 lines below;
  `node-issuer-update.md` (step 1, Steps): "mesh issuer" / "issuer row" to the node-admin's signing cert
  and its dates on its node row.

### Cells, RED first

- one per key family: two facts of one birth under different keys both survive a compaction and a fold
  (`declared/<state>` keeps `ReadyForTraffic` beside a late `Pending`; two attempts' operation effects
  both stand; lost and unreadable data-dir facts both stand);
- one per writer: the mesh-primary writes its nodes' facts, the fabric-primary writes them while the mesh
  has no primary and stops when one exists, the issuer writes cert dates for a member and for a
  mesh-primary's own cert, and no named writer is refused;
- the durable declared write: `Applied` answered, the writer killed before state-commit, the fact read
  back by its successor;
- the renewal paths: answered, unanswered, and answered but lost before the write, each ending with the
  row's dates as the proposal states;
- both storage backends.

### Q141 A: the date on the node row is the receipt (Luke, 2026-10-23)
A renewal writes the cert's dates once, when the node answers. `CertRenewed` and `IssuerUpdated` carry the
birth and the expiry of the cert installed, nothing else (`rdm-core/src/status.rs`), so the issuer matches
the answer to the cert it issued for that birth with that expiry and writes that cert's `issued_at`,
`expires_at` and `renew_at`, which it has from the issue; an answer naming an expiry the issuer did not
issue writes nothing. No wire change. An unanswered renewal writes nothing: the stored expiry stays as it was, the row's `renew_at`, already
passed, stays overdue, and the next tick renews again. An issuer that answered the node but was lost
before it wrote is the same case: nothing was written, so the next tick renews again, and the node
installs the newer cert. A successor primary does the same from the one stored row. Page text, approved, to land with the code:

`lifecycles/node-cert-renew.md`, Row:
- old: "The issuer puts the birth's cert row, keyed by the birth and `issued_at`, before it sends (`issued_at`, `expires_at`, `renew_at`), and puts the receipt row under the same key on the receipt; the newest issue and whether it has a receipt are the birth's renewal state"
- new: "On `CertRenewed` the issuer writes the birth's cert expiry on its node row (`issued_at`, `expires_at`, `renew_at`): the written expiry is the receipt. A renewal not answered writes nothing, and the row's `renew_at`, already passed, renews it on the next tick"

Step 2: old "…on rafka-time, and writes the row." new "…on rafka-time."

Step 5: old "The issuer records the receipt on the row. The old cert expires on its own." new "The issuer writes the expiry the node answered on its node row. The old cert expires on its own."

`lifecycles/mesh-issuer-renew.md`, Receipt:
- old: "The issuer row carries each admin's `IssuerUpdated`; the workflow is complete when every admin in the view has one"
- new: "On each admin's `IssuerUpdated` its node row takes its signing cert issue date and expiry; the workflow is complete when every admin in the view holds the new dates"

Step 4: old "The row records the last receipt. The old issuer expires on its own." new "Each admin's node row holds the dates it answered. The old issuer expires on its own."

### Before any row moves
- **Retention first (Q140).** `ops.nodes` and `ops.builds` are delete, 7 days, and not compacted (rafka-v2
  `gateway/tests/functional/store_plane_policy_at_birth.rs:120-128`). `ops.nodes` is an event topic behind
  rafka-v2's entity store, which keeps the latest record per node, so compaction is not the requirement
  there; the requirement is a cell showing a node unchanged for longer than the retention reads back whole
  after a restart. `ops.builds` must give a Build record the same survival, by compaction or the same kind
  of store, shown the same way. Both cells are i141's and come before any row moves.

### Open
- `ops.nodes.offline` is folded into `ops.nodes` (Luke, 2026-10-23): rafka's `NodeOfflineRecord` (one observer's
  offline episode of one node, with a broker's refill; `rafka-entity-topology/src/offline.rs:147`) becomes a
  fact on the node, keyed under it like every other, written by the observer that owns the episode. No RDM
  row maps to it, so Q139 stays withdrawn; the fold is i141's.
- A storage-provider page with this map (i141, with the rafka mapping).
