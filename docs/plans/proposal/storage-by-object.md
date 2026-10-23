## Storage by object: every row on the object it describes, mapped to rafka's ops topics

### Problem
`status.storage` holds rows of every kind: mesh status, fabric events, a birth's declared state, an
operation effect, a silence, a lost or unreadable data dir, member certs and their receipts, mesh issuers
and their receipts. It is not an object (rafka's `ops.admin-rows`, its mirror, is dead). A dead
runtime's end is held nowhere (the routing job's two exited containers at dc3661eb4).

### Rule
One store per object. A row lives on the object it describes, written by that object's writer (the
fabric-primary for the fabric, Builds and its own mesh; each mesh-primary for its own mesh and its
nodes). A store keeps no row about another object. `status.storage` goes.

### The map

| Object | RDM store | rafka topic | Rows on it |
|---|---|---|---|
| Fabric | `fabric.storage` | `<system_org>.ops.fabrics` | the Fabric record (id, name) and its name's claim; the primary records; the fabric's status and its fabric events; the fabric root, CA and their receipts; the fabric's state, shutdown among its states (no topic of its own) |
| Mesh | `mesh.storage` | `<system_org>.ops.meshes` | the mesh row (MeshId, name, dead); the mesh's status; its in-flight row; its issuer and the issuer's receipts |
| Node | `nodes.storage` | `<system_org>.ops.nodes` | the node at its path.name (config, tags); each birth: its runtime (runtime fact, data dir), its declared state, the operation effects applied to it, its member cert and the cert's receipts, a lost or unreadable data dir, and its runtime's end (exit code, where its logs were kept) |
| Node offline | `nodes.storage` | `<system_org>.ops.nodes.offline` | a node's silence: each observer's offline verdict about it |
| Node connections | `connections.storage` | `<system_org>.ops.nodes.connections` | the connections log (unchanged) |
| Build | `builds.storage` | `<system_org>.ops.builds`, `<system_org>.ops.builds.births` | the Build facts and each attempt's context; the births index (unchanged) |

### What moves
- `status.storage` rows: a Mesh status to `ops.meshes`; a fabric event to `ops.fabrics`; a birth's declared
  state, operation effect, lost or unreadable data dir and member cert (with receipts) to `ops.nodes`; a
  silence to `ops.nodes.offline`; a mesh issuer (with receipts) to `ops.meshes`.
- New: a runtime's end on its birth in `ops.nodes`, written by the delete; the provider then removes the
  exited runtime.
- `status.storage` is removed. Old records are read once on upgrade and written to
  their object (durable format moved).

### Open
- `ops.nodes.contacts` (a node-admin's restart hints: the births it heard). Keep it as the node object's
  contact list in `ops.nodes`, or keep the topic.
- Pages: a storage-provider page with this map; fabric-certs.md:104 ("a status-row kind") changes to "a row
  on the birth in `nodes.storage`".
