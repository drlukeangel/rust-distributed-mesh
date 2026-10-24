## Storage as whole records: one cached object, one writer, one whole write

Proposal for i143, for Luke's yes before anything is built. It replaces the per-fact storage interface
landed at `749c796f5` (storage-by-object.md). **Built (2026-10-23, lead), in the landing that carries
this line:** the code, the page lines below, and the cells below (the carrier write of another admin's
signing cert dates through `rdm_transport::CarriedReplies`, span `rdm.node_admin.issuer.update.via-carried`).

### What is wrong at 749c796f5

RDM hands the provider single facts: `put_node_cert` carries only a birth's cert dates,
`put_node_declared` one state, `put_mesh_status` one state, `put_fabric_event` one event. rafka stores
each object as a topic of whole records (`rafka-entity-topology`: `Node` keyed by its minted id, `Mesh`
by its id, `Fabric` by `fabric_id`). A provider handed a single fact must read the record, merge the fact
and write it back: logic in the provider, which rafka does not take (rafka builds only the providers;
RDM does the logic). storage-by-object.md:36-48 ("not one compacted record per object. Every fact is its
own row ... compacted by key") is superseded by this, and by Q140 as corrected (no compaction).

### The rule (Luke, 2026-10-23)

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

### The objects

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

### Who writes what changes

- **Mesh ReadyForTraffic.** Today the fabric-primary writes it on the mesh-primary's report. Under one
  writer per object the mesh-primary writes it on its own mesh's record when the fabric-primary answers
  its `DeclareMeshState` `Applied`: the fabric-primary decides, the writer records. This changes what
  `Applied` guarantees, and the pages say so (Q144): the fabric-primary's `Applied` confirms its decision;
  the mesh's ReadyForTraffic is durable when the mesh-primary's record write succeeds; the declaration stays
  owed until that write succeeds, under the one delivery rule; a successor mesh-primary that loads Pending
  with every planned birth ready declares again, and a repeated declaration receives `Applied` again even
  though the first was accepted. Mesh Pending is written
  by the mesh's first admin on its `ApplyMeshState` (today's receiver), Leaving and Dead by the
  mesh-primary, Dead at mesh-left by the fabric-primary when no primary survived.
- **Fabric events.** Written by the fabric-primary on the fabric record; a mesh-primary applying
  `ApplyFabricEvent` writes nothing (as at 749c796f5).
- **Member cert dates.** A member's: its mesh-primary, the issuer, on `CertRenewed`. A mesh-primary's
  own (issued by the fabric-primary, fabric-certs.md:79): the mesh-primary, on its own record, when it
  installs the cert; the fabric-primary schedules its renewal from the dates it issued and the expiry the
  reply confirmed, kept in its own cache, the same rule as the signing certs below.
- **Signing cert dates.** A node-admin's: its mesh-primary, on that admin's record: its own at its
  install; another admin's when that admin's `IssuerUpdated` returns through it as the carrier. The
  fabric-primary schedules renewals from the dates it issued and the expiry the reply confirmed, kept in
  its own cache, never by reading storage; a successor fabric-primary reads the records when it acquires
  ownership. It writes none of another mesh's.

### Durability

A Node record carries the declared states, and an Applied declaration is durable before the reply (Q54),
so the whole Node record is written straight to the store, never held in the scratchpad, before
state-commit too. The Fabric and Mesh records keep the scratchpad until state-commit, as today. A fabric
that dies before its first state-commit is abandoned (Q74), so a node record on its root changes nothing.

### The interface

| Store | Calls |
|---|---|
| `fabric.storage` | `fabric() -> Option<FabricRecord>`; `put_fabric(&FabricRecord)`; the name claim as today |
| `mesh.storage` | `mesh(mesh_id) -> Option<MeshRecord>`; `mesh_named(name) -> Option<MeshRecord>` (the name claim's lookup); `meshes() -> Vec<MeshRecord>`; `put_mesh(&MeshRecord)`; `claim_mesh_name(name, mesh_id)` / `release_mesh_name(name)` (new: today the record stored at its name is the claim, `claim_mesh`, storage.rs:136; rafka keys Mesh by id, so the record moves to its id and the name becomes a claim like the node's); the in-flight claim as today |
| `nodes.storage` | `node(node_id) -> Option<NodeRecord>`; `node_at(path.name) -> Option<NodeRecord>` (the path index); `nodes(mesh) -> Vec<NodeRecord>`; `put_node(&NodeRecord)`; `tagged(label)` (the tags index, written from the moves RDM computes); `claim_node_name(path.name, node_id)` / `release_node_name(path.name)` (new: today the row stored at the path.name is the claim, `claim_node_row`, storage.rs:927; the claim is the path.name-to-id lookup); `remove_node(node_id)` releases nothing: a deleted record stands and is read by id, and a name is released only by `release_node_name`; the admin's own row, its runtimes and its contacts, as today |

Every per-fact call of 749c796f5 goes: `put_node_declared`, `remove_node_declared`, `node_declared`,
`put_node_operation`, `node_operations`, `remove_node_operation`, `put_node_silence`, `node_silences`,
the four `data_dir_*` reads and puts and their `-at` indexes, `put_node_cert`, `node_certs`,
`put_signing_cert`, `signing_certs`, `put_mesh_status`, `mesh_status_held`, `mesh_statuses`,
`put_fabric_event`, `fabric_events`; and the row types `NodeDeclaredRow`, `NodeOperationRow`,
`NodeSilenceRow`, `NodeDataDirLostRow`, `NodeDataDirUnreadableRow`, `NodeCertRow`, `SigningCertRow`,
`MeshStatusRow`, `FabricEventRow` as stored rows. The file backend stores a node record at its node id
(`nodes/<mesh>/<node id>`) with the path.name index beside it, so a deleted predecessor's record stands
and is read by id.

### Open in this proposal

- None. A birth recovered over a fresh data dir reads its predecessor's `data-dir-lost` through the
  `predecessor` reference on its own record (The objects), written at its create; `inherited_by` points
  from predecessor to successor and is not followed backwards.

### Page lines, old to new (for Luke's yes)

`lifecycles/states.md:51`
- old: "Before state-commit, RDM holds its entity rows in the scratchpad; state-commit publishes the required held rows to the configured stores. A node declared-state fact acknowledged as Applied is written to the birth's `declared/<state>` row in `nodes/<mesh>/` on the shared store root before the reply, including before state-commit; a successor acquires that fact without creating a new lifecycle transition."
- new: "Before state-commit, RDM holds the Fabric and Mesh records in the scratchpad; state-commit publishes them to the configured stores. A Node record is written straight to the store, before state-commit too, because a declared state acknowledged as Applied is on it and is durable before the reply; a successor acquires that fact without creating a new lifecycle transition."

`lifecycles/states.md:35`
- old: "| mesh ReadyForTraffic | the fabric-primary | its mesh-primary's report that every planned birth is ready |"
- new: "| mesh ReadyForTraffic | the fabric-primary decides and answers `Applied`; the mesh-primary records it on its mesh's record, and the state is durable at that write; the declaration stays owed until then and is answered `Applied` again if repeated (Q144) | its mesh-primary's report that every planned birth is ready |"

`lifecycles/states.md:82-84`
- old: "The reason is that before state-commit the Fabric row, each mesh's Mesh row and its members' Node rows live only in their writers' scratchpads, so a primary change or a row written in that window is lost with its writer or cannot be read by its successor."
- new: "The reason is that before state-commit the Fabric record and each mesh's Mesh record live only in their writers' scratchpads, so a primary change or a record written in that window is lost with its writer or cannot be read by its successor."

`node-identity-and-state.md:656-660`
- old: "**What a mesh-primary records of a silent member** is rows on the node: the `PendingReconnect` and `Dead` marks ([States](lifecycles/states.md#states), node row silence marks), the `data-dir-lost` and `data-dir-unreadable` facts ([node recovery](lifecycles/node-recovery.md), [deployment provider](deployment-provider.md) §2.5), each a typed keyed row written and removed by the mesh-primary"
- new: "**What a mesh-primary records of a silent member** is on the node's record: the `PendingReconnect` and `Dead` marks ([States](lifecycles/states.md#states), node row silence marks), the `data-dir-lost` and `data-dir-unreadable` facts ([node recovery](lifecycles/node-recovery.md), [deployment provider](deployment-provider.md) §2.5), each set and cleared by the mesh-primary on the record it holds and written whole"

`fabric-certs.md` §Tracking (103-108)
- old: "The issuer writes the birth's cert row when the birth holds the cert, at its join's admission and on each renewal's `CertRenewed`: `NodeCertRow { node_id, name, incarnation, issued_at, expires_at, issuer_expires_at, renew_at }`, a row on the birth in `nodes.storage` beside `data-dir-lost` and `data-dir-unreadable`, keyed by the birth (format `node-cert/2`); the written expiry is the receipt, and the row is the birth's renewal state. `node.get` surfaces it as `cert { issued_at, expires_at, issuer_expires_at }`. A successor mesh-primary reads the rows and continues the schedule from them;"
- new: "The node's mesh-primary writes the birth's cert dates (`issued_at`, `expires_at`, `issuer_expires_at`, `renew_at`) on the node's record when the birth holds the cert: a member's at its join's admission and on its renewal's `CertRenewed`, the mesh-primary's own when it installs the cert the fabric-primary issued. They sit beside `data-dir-lost` and `data-dir-unreadable`; the written expiry is the receipt, and the dates are the birth's renewal state. `node.get` surfaces them as `cert { issued_at, expires_at, issuer_expires_at }`. A successor mesh-primary reads its nodes' records and continues the schedule from them;"

`lifecycles/node-cert-renew.md`
- Row, old: "On `CertRenewed` the issuer writes the birth's cert expiry on its node row (`issued_at`, `expires_at`, `renew_at`): the written expiry is the receipt."
- Row, new: "On `CertRenewed` the node's mesh-primary writes the birth's cert dates (`issued_at`, `expires_at`, `renew_at`) on the node's record and writes the record whole: the written expiry is the receipt. A mesh-primary's own cert, issued by the fabric-primary, it writes on its own record when it installs it."
- step 5, old: "5. The issuer writes the expiry the node answered on its node row."
- step 5, new: "5. The node's mesh-primary writes the expiry the node answered on the node's record."

`lifecycles/mesh-issuer-renew.md`
- Receipt, old: "On each admin's `IssuerUpdated` its node row takes its signing cert issue date and expiry;"
- Receipt, new: "On each admin's `IssuerUpdated` its mesh-primary writes its signing cert issue date and expiry on the admin's record (its own at its install);"
- step 1, old: "The fabric-primary's tick reads the signing cert dates on each mesh's node-admin rows;"
- step 1, new: "The fabric-primary's tick reads the signing cert dates it holds in its cache, the dates it issued and each admin's `IssuerUpdated` confirmed;"
- step 4, old: "4. Each admin's node row holds the dates it answered."
- step 4, new: "4. Each admin's record holds the dates it answered, written by its mesh-primary."

`lifecycles/node-issuer-update.md`
- step 4, old: "4. The fabric-primary writes the signing cert's issue date and expiry on that admin's node row."
- step 4, new: "4. The admin's mesh-primary writes the signing cert's issue date and expiry on the admin's record: its own at its install, another admin's when its `IssuerUpdated` returns through the mesh-primary as the carrier."

`lifecycles/node-name-update.md`, Claim and move
- old: "The move is two writes: the row is put at the new key carrying `renamed_from: <old path.name>`, then the old key is released."
- new: "The move is the name claim at the new path.name (`NameHeld { by: node.id }` names the node it is for), then one write of the node's record carrying `renamed_from: <old path.name>`, then the old claim's release. The step is resumed from its receipts like every Build step: a retry, or a successor writer resuming it, finds the new name claimed by this node and continues from the record write; a claim held by this node whose record does not carry the name is that unfinished step, never `NameHeld` against itself. A name claim is abandoned only by the step's named failure, which releases it."

`lifecycles/node-name-update.md`, the failure line
- old (line 83): "**The put at the new key fails**: a named storage error, nothing written, the old name stands."
- new: "**The record write after the claim fails**: a named storage error; the claim stands, named by this node, and the record stands under the old name; the resumed step writes the record, or the step's named failure releases the claim."
- old (line 84): "**The put succeeded and the release of the old key fails or is interrupted**: the node holds both"
- new: "**The record is written and the release of the old claim fails or is interrupted**: the node holds both"

`node-identity-and-state.md` §2.8, "It is written at Day 0 and at state-commit"
- old: "Nothing writes the row again until [fabric.delete](lifecycles/fabric-delete.md)."
- new: "The identity is never written again; the record is rewritten whole with each fabric event, by the fabric-primary."

`storage-by-object.md` "Keys, writers, fold" (36-48): replaced by this proposal's "The rule", "The
objects" and "Durability" sections; its map's rafka topics stand.

### Cells, RED first

- a write takes the cached object and writes it whole: a store wrapper records every put, and each put
  carries the whole record (every fact the cache held), never one fact; no read precedes a put;
- two facts of one birth both stand on the record across a reload (a late Pending beside
  ReadyForTraffic; two attempts' effects; lost and unreadable), on the memory and file backends;
- the writers: the mesh-primary writes its nodes' and its mesh's records; the fabric-primary writes a
  mesh's records only while it has no primary; a mesh-primary writes its own cert and signing cert dates;
- two different updates to one node overlapped in time (a declaration and a renewal), both present on the
  record afterwards; sequential updates do not show this;
- a rename's writer killed after the new name claim and before the record write: the resumed step finds
  its own claim and completes the rename; a second rename to that name by another node is `NameHeld`;
- a fabric-primary that becomes a mesh's writer after its last node-admin is gone writes `Dead` from the
  records it loaded then, not from its boot-time copy;
- Q54: Applied answered, the writer killed before state-commit, the declared state read back from the
  Node record by its successor;
- a renewal answered, unanswered, and answered but lost before the write;
- mesh ReadyForTraffic recorded by the mesh-primary on the fabric-primary's `Applied`; the mesh-primary
  killed after receiving `Applied` and before its write, the successor declares again, receives `Applied`
  again and records ReadyForTraffic (Q144);
- a record write that succeeds while a tag-index write fails: the retry writes the pending index moves,
  and the index matches the record afterwards.
