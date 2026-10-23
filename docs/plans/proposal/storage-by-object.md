## Storage by object: every row on the object it describes, mapped to rafka's ops topics

Proposal for i143; built by i141 (the storage work), not by i143. Each line below says whether Luke has
ruled on it.

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
| Fabric | `fabric.storage` | `<system_org>.ops.fabrics` | the Fabric record (id, name) and its name's claim; the primary records; the fabric's status and its fabric events; the fabric root, CA and their receipts; the fabric's state, shutdown among its states (no topic of its own) |
| Mesh | `mesh.storage` | `<system_org>.ops.meshes` | the mesh row (MeshId, name, dead); the mesh's status; its in-flight row |
| Node | `nodes.storage` | `<system_org>.ops.nodes` | the node at its path.name (config, tags); each birth: its runtime (runtime fact, data dir), its declared state, the operation effects applied to it, its member cert's expiry, a lost or unreadable data dir, and its runtime's end (exit code, where its logs were kept); a node-admin's contacts (the births it heard), on its own node (no topic of its own); a node-admin birth's signing cert issue date and expiry. Only the dates are stored; every cert stays local to its node, like the root, and the mesh has no issuer (Luke, 2026-10-23) |
| Node offline | `nodes.storage` | `<system_org>.ops.nodes.offline` | a node's silence: each observer's offline verdict about it (Q139 open: fold into `ops.nodes`) |
| Node connections | `connections.storage` | `<system_org>.ops.nodes.connections` | the connections log (unchanged) |
| Build | `builds.storage` | `<system_org>.ops.builds` | the Build facts and each attempt's context; the births a Build made, as the Build's state (no topic of its own) |

Ruled by Luke, 2026-10-23: the rule; `ops.admin-rows` dead; shutdown is a state of the fabric; the births are
the Build's state; the contacts are on the node; the mesh has no issuer, a node-admin's signing cert dates
are on its node row; only cert dates are stored; Q141 A (below).

### What moves, row by row (`crates/rdm-admin/src/status_storage.rs`)

| Row today | Goes to | Writer |
|---|---|---|
| `MeshStatusRow` | the mesh's status, `mesh.storage` / `ops.meshes` | the mesh-primary of that mesh; the fabric-primary for a mesh it acts for |
| `FabricEventRow` | the fabric's events, `fabric.storage` / `ops.fabrics` | the fabric-primary |
| `NodeDeclaredRow` | the birth's declared state, `nodes.storage` / `ops.nodes` | the node's mesh-primary |
| `NodeOperationRow` | the birth's operation effects, `nodes.storage` / `ops.nodes` | the node's mesh-primary |
| `NodeSilenceRow` | the node's offline verdicts, `ops.nodes.offline` (or `ops.nodes`, Q139) | each observer, as today |
| `NodeDataDirLostRow`, `NodeDataDirUnreadableRow` | the birth, `nodes.storage` / `ops.nodes` | the node's mesh-primary |
| `NodeCertRow` | the birth's member cert expiry (`issued_at`, `expires_at`, `renew_at`), `ops.nodes` | the issuer, on `CertRenewed` (Q141) |
| `NodeCertReceiptRow` | gone (Q141) | |
| `MeshIssuerRow` | the node-admin birth's signing cert issue date and expiry, `ops.nodes` | written on that admin's `IssuerUpdated` (Q141) |
| `MeshIssuerReceiptRow` | gone (Q141) | |
| new: the runtime's end | the birth, `ops.nodes`: exit code, where its logs were kept | the delete; then the provider removes the exited runtime |

`StatusStorage` and `MemoryStatusStorage` go; their methods move to the object stores' interfaces
(`fabric_storage`, `storage::{MeshStorage, NodesStorage}`). Callers today: `admin/{boot,mesh_delete,records,recovery,runner}.rs`,
`admin.rs`, `cert_renewal.rs`, `control_plane.rs`, `fabric_state.rs`, `join.rs`, `mesh_delete.rs`,
`node_properties.rs`, `offline.rs`, `status_declare.rs`, `status_rpc.rs`, `storage_backend.rs`, `views.rs`,
`rdm-test/src/rig.rs` and 17 test files. Old `status.storage` records are read once on upgrade and written
to their object (durable format moved).

### Q141 A: the date on the node row is the receipt (Luke, 2026-10-23)
A renewal writes the cert's dates once, when the node answers, from what it answered. An unanswered renewal
writes nothing; the row's `renew_at`, already passed, renews it on the next tick. A successor primary folds
the one row. Page text, approved, to land with the code:

`lifecycles/node-cert-renew.md`, Row:
- old: "The issuer puts the birth's cert row, keyed by the birth and `issued_at`, before it sends (`issued_at`, `expires_at`, `renew_at`), and puts the receipt row under the same key on the receipt; the newest issue and whether it has a receipt are the birth's renewal state"
- new: "On `CertRenewed` the issuer writes the birth's cert expiry on its node row (`issued_at`, `expires_at`, `renew_at`): the written expiry is the receipt. A renewal not answered writes nothing, and the row's `renew_at`, already passed, renews it on the next tick"

Step 2: old "…on rafka-time, and writes the row." new "…on rafka-time."

Step 5: old "The issuer records the receipt on the row. The old cert expires on its own." new "The issuer writes the expiry the node answered on its node row. The old cert expires on its own."

`lifecycles/mesh-issuer-renew.md`, Receipt:
- old: "The issuer row carries each admin's `IssuerUpdated`; the workflow is complete when every admin in the view has one"
- new: "On each admin's `IssuerUpdated` its node row takes its signing cert issue date and expiry; the workflow is complete when every admin in the view holds the new dates"

Step 4: old "The row records the last receipt. The old issuer expires on its own." new "Each admin's node row holds the dates it answered. The old issuer expires on its own."

### Open
- Q139: `ops.nodes.offline` folded into `ops.nodes`.
- Q140: `ops.nodes` and `ops.builds` compacted like `ops.fabrics` and `ops.meshes`. Today they are delete,
  7 days (rafka-v2 `gateway/tests/functional/store_plane_policy_at_birth.rs:126`), so every node row and
  Build record expires 7 days after its last write.
- Page text, for Luke's yes before the build: a storage-provider page with this map; fabric-certs.md:104
  ("a status-row kind") to "a row on the birth in `nodes.storage`"; the "mesh issuer" / "issuer row"
  wording in `mesh-issuer-renew.md` and `node-issuer-update.md` (step 1, Steps) to the node-admin's signing
  cert and its dates on its node row.
