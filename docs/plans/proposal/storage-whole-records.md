## Storage as whole records: one cached object, one writer, one whole write

Proposal for i143, for Luke's yes before anything is built. It replaces the per-fact storage interface
landed at `749c796f5` (storage-by-object.md). Nothing below is built.

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
whole objects and decides nothing (Q109). Storage is read only to fill a cache: at boot, and when an
admin becomes its mesh's primary and folds its mesh's records.

Because the cache is the copy a write starts from, each object has exactly one writer, the writer map's:
the fabric-primary writes the fabric record and its own mesh's records; each mesh-primary writes its own
mesh's record and its nodes' records; the fabric-primary writes a mesh's records only while that mesh
has no primary (mesh-create, node-mesh-join, mesh-left with no survivor). Fabric recovery's executor
writes only the records its own attempt's operations require (Q122), as today.

### The objects

| Object | Key | What it holds | Writer | Cache that holds it |
|---|---|---|---|---|
| Fabric record | `fabric_id` | identity (id, name); the fabric events applied (`ready-for-traffic`, `shutdown-initiated:<by>`) | the fabric-primary | the fabric-primary's applied set (`Declared.fabric` today) |
| Mesh record | mesh id | id, name, dead; the states entered (Pending, ReadyForTraffic, Leaving, Dead) | the mesh's primary; the fabric-primary while the mesh has none | the writer's applied set (`Declared.meshes` today) |
| Node record | minted node id | today's node row (path.name, status, config, tags, allocated, executable, renamed_from, app types, inherited_by) and, per incarnation: the declared states, the operation effects, the silence mark, `data-dir-lost`, `data-dir-unreadable`, the member cert dates, a node-admin's signing cert dates | the node's mesh-primary; the fabric-primary while the mesh has none | the writer's node rows (`node_rows::WrittenRows` today), into which `Declared.nodes`, `Declared.operations` and the silence memo fold |

Outside the records, unchanged: the fabric name's claim and each mesh's in-flight row are claims, an
insert that fails when the key is held, raced by design and never updated; the Build journal; a node's
connections log; an admin's own row, contacts and launch handles in its data dir.

A record's facts stay independent inside it, keyed by their full identity: one declared entry per state
(a late Pending never removes ReadyForTraffic; the reader takes the greatest), one effect per
(build_id, attempt, operation). A start beginning a birth's next cycle clears that birth's declared states
on the cached record and keeps its effects; the record is then written whole.

### Who writes what changes

- **Mesh ReadyForTraffic.** Today the fabric-primary writes it on the mesh-primary's report. Under one
  writer per object the mesh-primary writes it on its own mesh's record when the fabric-primary answers
  its `DeclareMeshState` `Applied`: the fabric-primary decides, the writer records. Mesh Pending is written
  by the mesh's first admin on its `ApplyMeshState` (today's receiver), Leaving and Dead by the
  mesh-primary, Dead at mesh-left by the fabric-primary when no primary survived.
- **Fabric events.** Written by the fabric-primary on the fabric record; a mesh-primary applying
  `ApplyFabricEvent` writes nothing (as at 749c796f5).
- **Member cert dates.** A member's: its mesh-primary, the issuer, on `CertRenewed`. A mesh-primary's
  own (issued by the fabric-primary): the mesh-primary, on its own record, when it installs the cert.
- **Signing cert dates.** A node-admin's: its mesh-primary, on that admin's record: its own at its
  install; another admin's when that admin's `IssuerUpdated` returns through it as the carrier. The
  fabric-primary reads the records to schedule renewals and writes none of another mesh's.

### Durability

A Node record carries the declared states, and an Applied declaration is durable before the reply (Q54),
so the whole Node record is written straight to the store, never held in the scratchpad, before
state-commit too. The Fabric and Mesh records keep the scratchpad until state-commit, as today. A fabric
that dies before its first state-commit is abandoned (Q74), so a node record on its root changes nothing.

### The interface

| Store | Calls |
|---|---|
| `fabric.storage` | `fabric() -> Option<FabricRecord>`; `put_fabric(&FabricRecord)`; the name claim as today |
| `mesh.storage` | `mesh(name) -> Option<MeshRecord>`; `meshes() -> Vec<MeshRecord>`; `put_mesh(&MeshRecord)`; the in-flight claim as today |
| `nodes.storage` | `node(node_id) -> Option<NodeRecord>`; `node_at(path.name) -> Option<NodeRecord>` (the path index); `nodes(mesh) -> Vec<NodeRecord>`; `put_node(&NodeRecord)`; `tagged(label)` (the tags index); `remove_node(node_id)` (a name's release); the data-dir rows as today |

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

- **A predecessor's facts by path.name.** A birth recovered over a fresh data dir answers its
  predecessor's `data-dir-lost` today through a path.name index of the fact. With the fact on the
  predecessor's record, the new record names the record it replaced (the existing `inherited_by` link,
  read the other way); the read follows it. To be confirmed against node-recovery.md before building.

### Page lines, old to new (for Luke's yes)

`lifecycles/states.md:51`
- old: "Before state-commit, RDM holds its entity rows in the scratchpad; state-commit publishes the required held rows to the configured stores. A node declared-state fact acknowledged as Applied is written to the birth's `declared/<state>` row in `nodes/<mesh>/` on the shared store root before the reply, including before state-commit; a successor acquires that fact without creating a new lifecycle transition."
- new: "Before state-commit, RDM holds the Fabric and Mesh records in the scratchpad; state-commit publishes them to the configured stores. A Node record is written straight to the store, before state-commit too, because a declared state acknowledged as Applied is on it and is durable before the reply; a successor acquires that fact without creating a new lifecycle transition."

`lifecycles/states.md:35`
- old: "| mesh ReadyForTraffic | the fabric-primary | its mesh-primary's report that every planned birth is ready |"
- new: "| mesh ReadyForTraffic | the fabric-primary decides, the mesh-primary records it on its mesh's record | its mesh-primary's report that every planned birth is ready, answered `Applied` |"

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
- step 1, new: "The fabric-primary's tick reads the signing cert dates on each mesh's node-admin records;"
- step 4, old: "4. Each admin's node row holds the dates it answered."
- step 4, new: "4. Each admin's record holds the dates it answered, written by its mesh-primary."

`lifecycles/node-issuer-update.md`
- step 4, old: "4. The fabric-primary writes the signing cert's issue date and expiry on that admin's node row."
- step 4, new: "4. The admin's mesh-primary writes the signing cert's issue date and expiry on the admin's record: its own at its install, another admin's when its `IssuerUpdated` returns through the mesh-primary as the carrier."

`storage-by-object.md` "Keys, writers, fold" (36-48): replaced by this proposal's "The rule", "The
objects" and "Durability" sections; its map's rafka topics stand.

### Cells, RED first

- a write takes the cached object and writes it whole: a store wrapper records every put, and each put
  carries the whole record (every fact the cache held), never one fact; no read precedes a put;
- two facts of one birth both stand on the record across a reload (a late Pending beside
  ReadyForTraffic; two attempts' effects; lost and unreadable), on the memory and file backends;
- the writers: the mesh-primary writes its nodes' and its mesh's records; the fabric-primary writes a
  mesh's records only while it has no primary; a mesh-primary writes its own cert and signing cert dates;
- Q54: Applied answered, the writer killed before state-commit, the declared state read back from the
  Node record by its successor;
- a renewal answered, unanswered, and answered but lost before the write;
- mesh ReadyForTraffic recorded by the mesh-primary on the fabric-primary's `Applied`.
