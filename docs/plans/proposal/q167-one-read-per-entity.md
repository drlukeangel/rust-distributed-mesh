# Proposal: one read per entity (Q167)

**Status:** proposal, 2026-10-25. Asks Luke's yes on an amendment of the Q167 row (rulings INDEX): the
singular reads go, one plural read per entity stays. Reads only; every change (`node.stop`,
`node.config.update`, `mesh.delete`, `build.create`) acts on one and is untouched.

## 1. The question

Why does a read of one thing differ from a read of the list filtered to one? Q165 gave every entity
a singular read and a nested plural (`node.get { id }`, `mesh.nodes.get`, `fabric.nodes.get`); Q167
kept the pair and un-nested the plural (`node.get { id }` / `nodes.get { mesh?, tags? }`). Each pair is
one wire request with two names, two client methods, two spans, two page rows and two answers for
"not there" (`NOT_FOUND` against an empty list). Luke, 2026-10-25: why `fabric.get` and not
`fabrics.get { id? }`?

The singular adds two things only: the `NOT_FOUND` code and a non-list return type. Under one read
per entity "not there" is the empty list, and a caller that asked for one id reads the length. That
is the shape of AWS `Describe*` (ids or filters in, a list out) and of `kubectl get <kind> -l`.

## 2. Today, measured three ways

| | Pages (Q165, open) | Ruling (Q167 row) | Code (Q165) |
|---|---|---|---|
| fabric | `fabric.get`, `fabric.meshes.get`, `fabric.nodes.get { tags? }` | `fabric.get { id }` / `fabrics.get` | `GetFabric` → `Fabric`; `GetMeshes { None }`; `GetNodes { Fabric \| Tags }` |
| mesh | `mesh.get { name }`, `mesh.nodes.get { tags? }` | `mesh.get { name }` / `meshes.get` | `GetMeshes { Some }` → `Meshes` or `UnknownMesh`; `GetNodes { Mesh \| MeshTags }` |
| node | `node.get { id }`, `NOT_FOUND` when absent | `node.get { id }` / `nodes.get { mesh?, tags? }` | `GetNodes { Id }` → `Nodes`, empty read as `NOT_FOUND` by the client |
| connections | `node.connections.get` | (not named) | `GetConnections { node_id, incarnation }` → `Connections`, served at the exact birth |
| build | `build.get { id }`: the Build and its receipts, for reattachment | `build.get { id }` (topology, receipts) / `builds.get` (ids, current marked) | 0x20 `Get { build_id }`: `Steps` chunks, then `Got` (phase, attempt, executor, reason, last_failure) |

The Status reply bound is 64 KiB: an answer past it is `ReadOverBound`, read as `RESOURCE_EXHAUSTED`
(`status.rs` `within_reply_bound`; one mesh of 150 nodes fits, 400 do not). The Build op is the
fabric-primary's alone, not forwardable, and already streams in chunks.

Not in the table: `FetchCurrentBuild` (op `0x1F`). It is a node-admin's own catch-up of the Build
topic's facts at its start (node-start.md step 4, Q98, Q100), served by any node-admin, the
fabric-primary first and then every other live admin, and carries no executable, tags or config
because the Build topic does not. It is not a caller read and this proposal does not touch it.

## 3. The rule

- **R-RD1, one read per entity.** Five entities, one store each, one read each: `fabrics.get`,
  `meshes.get`, `nodes.get`, `connections.get`, `builds.get`. Every read takes optional filters,
  combined with AND, and answers a list in a stable order. No read is nested under another entity.
- **R-RD2, one answer for "not there".** Nothing matching, including a `mesh` filter naming a mesh
  the admin does not hold, is the empty list (the `unknown-mesh` refusal goes). A filter that cannot
  be meant (a non-canonical id, a label outside the tag grammar, more than 32 labels) is
  `INVALID_ARGUMENT`, naming the field. An answer past the reply bound is `ReadOverBound`,
  `RESOURCE_EXHAUSTED`, as today; the caller narrows the filter. No paging is added to a Status read.
- **R-RD3, a property's read is its row's field.** `node.tags.get`, `node.config.get`,
  `node.cert.get`, `mesh.config.get` and `fabric.config.get` name fields of the entity's row (no wire
  request of their own exists) and leave the pages as calls; their updates stay.
- **R-RD4, a Build is read whole.** `builds.get` answers each Build with its header (phase, attempt,
  executor, reason, last failure), its topology (every node's `NodeSpec`: executable, storage,
  placement, tags, config) and its receipts, as `build.get` does today, so a read, edit and resubmit
  never clears a field and a failed Build's evidence stays readable. The current Build is selected
  once per read (Q98's rule) and marked.
- **R-RD5, the row keeps its content.** `FabricView` carries its meshes and `MeshView` names its
  nodes as today: a row's content is not a second read, and the view types do not change (the
  durable format).

## 4. The reads

| Read | Filters (optional, AND) | Answers, in order | Served by |
|---|---|---|---|
| `fabrics.get` | `ids` | the fabric the admin's view holds: id, name, state, provider, fabric-primary, meshes | a node-admin |
| `meshes.get` | `names` (a name or minted id each) | meshes, by name | a node-admin |
| `nodes.get` | `ids`, `mesh` (name or id), `tags` (all or any of the labels) | node rows by `path.name`, each with status, ports, config, tags and executable; no filter: every node of the fabric, its topology | a node-admin |
| `connections.get` | `node` (required: id and incarnation), `ids` | the exact birth's own connections from its connections log, as today | the exact birth |
| `builds.get` | `ids`, `current` | Builds newest first, each whole (R-RD4); the current one marked | the fabric-primary |

`node` is required on `connections.get` because a connection is read at the live birth that holds
it, not from a store row. "One node" is `nodes.get { ids: [id] }`; "the current Build" is
`builds.get { current: true }`.

## 5. Delta

### 5.1 Wire (the commit carries the durable-format marker)

- **Status 0x1B.** One request variant appended, `Get { filter: ReadFilter }`, with
  `ReadFilter::Fabrics { ids } | Meshes { names } | Nodes { ids, mesh, tags } | Connections { node_id, incarnation, ids }`;
  one reply variant appended, `Fabrics { fabrics: Vec<FabricView> }`. `REQUEST_VARIANTS` 32 → 33,
  `REPLY_VARIANTS` 55 → 56. `GetNodes`, `GetMeshes`, `GetFabric` and `GetConnections` keep their
  positions and decode into the same filter path, so one handler serves both; rdm-client no longer
  sends them. `Nodes`, `Meshes`, `Connections`, `ReadOverBound` and the malformed replies are reused;
  `UnknownMesh` is retained unsent.
- **Build 0x20.** One request variant appended, `GetBuilds { ids, current }`; three reply variants
  appended: `Current { build_id: Option<String> }` first on the stream, the Build selected once for the
  read; per Build newest first `Topology { build_id, chunk_index, chunk_count, nodes: Vec<NodeSpec> }`
  chunks (a 256 KiB request can carry a topology no 64 KiB reply frame holds), then the existing
  `Steps` chunks and the existing `Got` header; `End { builds }` last. `REQUEST_VARIANTS` 4 → 5,
  `REPLY_VARIANTS` 26 → 29. `Get { build_id }` keeps its position and decodes as
  `GetBuilds { ids: [build_id] }`. A stream that ends before `End` is an incomplete read, never a
  list (Node RPC §34, §43).

### 5.2 Client and spans

- rdm-client: `Fabrics::get(&FabricFilter) -> Vec<FabricView>`, `Meshes::get(&MeshFilter) -> Vec<MeshView>`,
  `Nodes::get(&NodeFilter) -> Vec<NodeView>`, `Connections::get(&ConnectionFilter)`,
  `Builds::get(&BuildFilter) -> Vec<Build>` (header, `nodes`, `steps`, `current`). Gone:
  `Nodes::get(&NodeId)`, `Meshes::get(&str)`, `Meshes::nodes`, `Fabrics::get()`, `Fabrics::meshes`,
  `Fabrics::nodes`, `Builds::get(&BuildId)` and `BuildReceipts`. No rafka-v2 code calls them; the
  callers are RDM's tests and the demo.
- Spans: `rdm.node_admin.fabrics.get.via-call`, `…meshes.get…`, `…nodes.get…`,
  `…connections.get…`, `…builds.get.via-call`; `NodeSelect::read()` and `BuildOp::Get`'s name follow.
  `docs/reference/rdm/wire.md` and `spans.md` are docgen output, regenerated.

### 5.3 Pages (old → new)

node-rpc-envelope.md:

- line 215, op table 0x20: `build.create, build.attempt.run, build.get, build.delete;` →
  `build.create, build.attempt.run, builds.get, build.delete;`.
- lines 246 to 249, the typed objects list: `node.get`, `node.connections.get/delete`,
  `node.tags.get/create/delete`, `node.config.get/update`, `mesh.get`, `mesh.nodes.get`,
  `mesh.config.get/update`, `fabric.get`, `fabric.meshes.get`, `fabric.nodes.get`,
  `fabric.config.get/update` → `node.connections.delete`, `node.tags.create/delete`,
  `node.config.update`, `mesh.config.update`, `fabric.config.update`, and after `fabric.delete`:
  `, and the five reads: fabrics.get, meshes.get, nodes.get, connections.get, builds.get (one read
  per entity, optional filters combined with AND, a list in a stable order, empty when nothing
  matches)`.
- line 269: `build.get is a read for reattachment, never a resume.` → `builds.get { ids } is a read
  for reattachment, never a resume.`
- line 315, the `node.get` row → `| nodes.get | [Status 0x1B](#status-op-0x1b-rdm) Get { filter:
  Nodes { ids?, mesh?, tags? } }, answered Nodes: every node row the answering admin holds that
  matches every filter given, by path.name; no filter is every node of the fabric, its topology; mesh
  by name or minted id; tags all of the given labels or any of them, read through the node store's
  tags index; a node's tags, config and cert are fields of its row | the rows, NodeView; a stopped
  node answers stopped; no event | OK, the empty list when nothing matches; a filter that cannot be
  meant INVALID_ARGUMENT; read bound RESOURCE_EXHAUSTED |`.
- line 324, `node.cert.get`: `A field of node.get:` → `A field of a node's row (nodes.get):`.
- line 326, the mesh row's read half: `mesh.get { name } answers the one mesh with that name or
  minted id, … the filter read through the node store's tags index (Q165);` → `meshes.get { names? }
  answers every mesh the answering admin holds, by name, [Status 0x1B](#status-op-0x1b-rdm) Get {
  filter: Meshes }, the empty list when none matches; a mesh's nodes are nodes.get { mesh };`. The
  row's heading loses `mesh.get` and `mesh.nodes.get`, `mesh.config.get/update` becomes
  `mesh.config.update`, and `mesh.tags.get` goes from its list.
- line 328, the fabric row's read half: `fabric.get answers the fabric: … the filter read through
  the node store's tags index (Q165);` → `fabrics.get { ids? } answers the fabric the answering
  admin holds: its id, name, state, provider, fabric-primary and meshes, [Status 0x1B](#status-op-0x1b-rdm)
  Get { filter: Fabrics }, answered Fabrics; its nodes are nodes.get;`. The heading loses
  `fabric.get`, `fabric.meshes.get`, `fabric.nodes.get`, and `fabric.config.get/update` becomes
  `fabric.config.update`.
- lines 360 and 361: `node.get answers it whole` → `nodes.get answers it whole`;
  `node.get answers held whole` → `nodes.get answers held whole`.
- line 366: `node.get answers the whole node.` → `nodes.get answers the whole node.`
- line 1300, the Build calls table: `| build.get | caller → fabric-primary | build_id | read
  accepted Build and receipts for reattachment after a cut; never progress polling |` → `| builds.get
  | caller → fabric-primary | ids?, current? | a stream: Current, then per Build newest first Topology
  chunks, Steps chunks and Got, then End; each Build whole, its topology and receipts; the current
  Build selected once per read; for reattachment after a cut, never progress polling |`.
- line 1316: `build.get for the receipts` → `builds.get { ids: [build_id] } for the receipts`.
- The 0x20 reply enum listing gains the three appended variants and the counts of §5.1.

build.md: line 172 `audit/history for build.get` → `audit/history for builds.get`; line 202
`build.attempt.run, build.get and build.delete` → `build.attempt.run, builds.get and build.delete`;
line 405 `build.get is a read of the evidence, not a reattachment` → `builds.get is a read of the
evidence, not a reattachment`.

build-deployment.md §5, the paragraph → `The node's row is the source. nodes.get returns every node
row that matches its filters, each with its executable beside its status, config and tags
(NodeView.executable): one node by ids, a mesh's nodes by mesh, the fabric's by no filter. The digest,
the Members projection and node.topology.get carry no executable.`

consumer-guide.md: line 31 → `The read calls follow one rule: one read per entity, optional filters
combined with AND, a list in a stable order, empty when nothing matches.`; the table at lines 33 to
39 becomes §4's table; line 69 `node.tags.get, or the tags field of its node row` → `the tags field
of its node row (nodes.get)`; line 70 `fabric.nodes.get { tags } or mesh.nodes.get { tags } (Q165)`
→ `nodes.get { tags }, with mesh for one mesh's`.

fabric-mesh-node-lifecycles.md: line 100 `| [node.get](…) | Reading a node |` → `| [nodes.get](…) |
Reading nodes |`; line 123 (0x1F) unchanged.

Name pass, meaning unchanged (`node.get` → `nodes.get`, `fabric.get` → `fabrics.get`, `mesh.get` →
`meshes.get`, `build.get` → `builds.get`): connections.md (2), fabric-certs.md (4),
node-identity-and-state.md (4), gossip.md (2), node-rpc.md (1), lifecycles/node-recovery.md (2),
lifecycles/fabric-ca-renew.md (2), lifecycles/node-status.md (1), lifecycles/states.md (1),
lifecycles/node-connections-delete.md (2), eng/provider-runbook.md (5), eng/node-storage-runbook.md (1),
eng/fabric-recovery-runbook.md (1).

### 5.4 Rulings

The Q167 row gains: `**Amended (Luke, 2026-10-25; [one read per entity](../proposal/q167-one-read-per-entity.md)):**
one plural read per entity with optional filters, AND-combined, a list in a stable order, the empty
list when nothing matches; the singular reads go; builds.get answers each Build whole, header,
topology and receipts, the current one selected once per read and marked; connections.get { node, ids? }
is the fifth; a property's read is its row's field.` The Q165 row's state column: `superseded by
Q167 as amended`.

### 5.5 Cells

- One cell per Status entity: the AND filter, the empty list for no match (a mesh filter naming an
  unknown mesh included), `INVALID_ARGUMENT` for a filter that cannot be meant, the existing
  over-bound cell renamed.
- `builds.get` over a history of three Builds, one Failed part-way: order, the current marked, every
  header, topology and receipt as `build.get` reads them today; the current Build moved mid-stream
  names one Build (the 0x1F cell at `build_facts_read.rs:599` is the model); a stream cut between
  chunks is an incomplete read.
- The retained variants decode into the new path (`status.rs` wire tests, `build_wire.rs`).

## 6. Arms

- **A (recommended): one read per entity**, §3 to §5, Q167 amended as §5.4.
- **B: Q167 as recorded**: a singular and a plural read per entity, `NOT_FOUND` on the singular.
- **X: something else.**
