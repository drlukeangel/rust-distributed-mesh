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

"A row lives on the object" is not one compacted record per object. Every fact is its own row, keyed under
the object it describes, with the complete identity today's status rows carry, so that no later or
delayed write can erase an independent fact: a node's facts under its path.name and, where the fact is a
birth's, its exact birth `(node_id, incarnation)`, then the fact's full name:
`declared/<state>` (one row per state, as `NodeDeclaredRow::key` is today, so a delayed `Pending` never
replaces `ReadyForTraffic`; the fold reads them in lifecycle order), `operation/<build_id>/<attempt>/<operation>`
(one row per effect, so a later attempt never erases an earlier attempt's), `silence`, `cert`,
`signing-cert` (a node-admin's), `data-dir-lost`, `data-dir-unreadable`, `runtime`; the mesh's and the
fabric's likewise. Each key has one writer and no write replaces another
writer's row (Q109: the storage decides nothing, the writer decides before it writes, every reader folds).
A reader folds the rows of one object into its view; a cycle reset deletes the birth's declared facts and
keeps its operation effects, as today. A tombstone removes one key, never an object's other facts. The
object's topics are compacted by key (Q140, below), so the newest row of each key is what survives.

**Durability before `Applied`.** A birth's `declared` facts are written straight to the store, never
through the bootstrap scratchpad, so an `Applied` declaration survives a crash before state-commit
(Q54; today `status_storage.rs` opens `declared` to the disk while every other row waits in the
scratchpad). The move keeps that boundary: the node store writes `declared` durably and answers `Applied`
only after, whichever writer holds the pen, the acting fabric-primary included. No declaration is
acknowledged from a buffer.

**The writer of a mesh's and its nodes' facts** is that mesh's mesh-primary; while the mesh has no
primary it is the fabric-primary ([node-mesh-join.md](../../architecture/lifecycles/node-mesh-join.md) names it the
accepting primary then; [mesh-create.md](../../architecture/lifecycles/mesh-create.md) has it set the first node-admin's
ReadyForTraffic; [mesh.delete](../../architecture/lifecycles/mesh-delete.md) has it act for the last admin), and
ownership passes to the mesh-primary when there is one. This is the one acting case; fabric recovery's
executor is not it (Q122: it takes no role; it writes only its own attempt's operations' rows).

**The writer, per fact kind.** `declared`, `operation`, `silence`, `data-dir-lost`, `data-dir-unreadable`
and `runtime`: the node's mesh-primary (the fabric-primary while the mesh has no primary). `cert` and
`signing-cert`: the issuer, which is not always the mesh-primary ([fabric-certs.md](../../architecture/fabric-certs.md)):
a member's cert is issued and its dates written by its mesh-primary; a mesh-primary's own cert and a
node-admin's signing cert are issued and their dates written by the fabric-primary, while that mesh's
primary exists (`cert_renewal.rs::renewed_by` today). One writer per key still holds: each key has
exactly one, named here; an implementation refuses nothing a named writer writes.

### Outside the map, by design

A node-admin's own `self` row and the launch handles of the runtimes it launched live in its own data
dir, beside `node-key`, `ca-secret` and `fabric-ca`: a restarting admin reads them before it opens any
store, to learn which fabric and mesh it belongs to, and cannot read a topic whose fabric it does not yet
know. They are a bootstrap seed, not a record of the fabric; what the fabric needs to know of a birth's
runtime is on the node in `nodes.storage`. Every record RDM keeps of the fabric is in one of the five
stores above, and each store is one rafka topic. `ops.nodes.offline` is rafka's own and `topology-nodes`
is the nodes projection, neither a store.

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
  `gateway/tests/functional/store_plane_policy_at_birth.rs:120-128` asserts exactly that, and that only
  fabrics and meshes override). A node row or Build record moved there today expires a week after its last
  write. Both planes become compacted by key, in rafka-v2, with a cell that proves it, before any row moves.

### Open
- `ops.nodes.offline` is rafka's own (`NodeOfflineRecord`: one observer's offline episode of one node, with a
  broker's refill; `rafka-entity-topology/src/offline.rs:147`). No RDM row maps to it, so Q139 is withdrawn.
- A storage-provider page with this map (i141, with the rafka mapping).
