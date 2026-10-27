# Proposal: one read per entity (Q167)

**Status:** proposal, 2026-10-25. Asks Luke's yes on an amendment of the Q167 row (rulings INDEX): the
singular reads go, one plural read per entity stays, and a read's filter is a property match. Reads
only; every change (`node.stop`, `node.config.update`, `mesh.delete`, `build.create`) acts on one and
is untouched.

## 1. The question

Why does a read of one thing differ from a read of the list filtered to one? Q165 gave every entity
a singular read and a nested plural (`node.get { id }`, `mesh.nodes.get`, `fabric.nodes.get`); Q167
kept the pair and un-nested the plural (`node.get { id }` / `nodes.get { mesh?, tags? }`). Each pair is
one wire request with two names, two client methods, two spans, two page rows and two answers for
"not there" (`NOT_FOUND` against an empty list). Luke, 2026-10-25: why `fabric.get` and not
`fabrics.get { id? }`? And why name filters one at a time (`kinds`, `mesh_primary`) when every one of
them is a property of the row?

The singular adds two things only: the `NOT_FOUND` code and a non-list return type. Under one read
per entity "not there" is the empty list, and a caller that asked for one id reads the length. That
is the shape of AWS `Describe*` (ids or filters in, a list out) and of `kubectl get <kind> -l`. A
named filter adds a wire field per property; a property match adds none, ever.

### Against Kubernetes and AWS

Kubernetes keeps a get by name (`GET nodes/{name}`, 404 when absent) beside the list
(`GET nodes`, a `NodeList`), for three reasons RDM does not have: the list carries its own metadata
(the collection's `resourceVersion`, a `continue` token) to start a watch and to page; the get is one
point read of one key where a list is a range scan with the selector applied after; and `get` and
`list` are separate permission verbs, only `get` restrictable to named resources. RDM has no watch,
no paging, reads from the admin's in-memory view where an `id` match is a point lookup, and has no
permission model. What is left is the error code, and the empty list says it.

The filter is AWS `Describe*`'s `Filters` (a list of `{ Name, Values }`, AND across, any-of within,
`tag:<key>` into the tag map) and Kubernetes' field and label selectors (`key=value`, `key in (a,b)`,
comma for AND), one syntax.

| Kubernetes / AWS | This proposal |
|---|---|
| plural resource names, `kubectl get nodes` | `nodes.get`, `meshes.get`, `builds.get` |
| label selector `-l`; AWS `tag:<key>` | `tags` pair, any-of; repeated for all-of |
| field selector `metadata.name=x`; AWS `Filters` | `(property, values)` pairs, any property of the row |
| namespace as a scope flag, `--all-namespaces` | `mesh` pair; no pair is the whole fabric |
| subresource `nodes/{name}/proxy`, the parent required | `connections.get { node }`, the node the read's address |
| no read of one field; the object is read | R-RD3 |
| `get` by name, 404 | omitted (above) |
| `spec` and `status` on one object | desired in the current Build's `NodeSpec`, observed in the node row: the Build is the only topology writer |
| `limit` and `continue` on a list | R-RD6: not until telemetry shows the bound hit |
| `!=`, ranges, substrings | not until a caller needs one |
| `rollout history`, Events as their own resource | the Build history with each Build's receipts inside it |

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
  `meshes.get`, `nodes.get`, `connections.get`, `builds.get`. Every read takes one filter shape and
  answers a list in a stable order. No read is nested under another entity.
- **R-RD2, a filter is a property match.** A filter is a list of `(property, values)` pairs. Pairs
  are AND-combined; a repeated property is AND (all of these tags); values within a pair are any-of.
  The property is a public field of the entity's row by the name its page gives it (the
  node-properties table for a node; `MeshView`, `FabricView`, `ConnectionView` and the Build header
  for the others), with one dot into the one map a row has: `config.<key>`, the key being everything
  after the first dot, so a key holding a dot is addressed whole. A set or list field matches when it
  contains the value. Values are strings on the wire; a boolean is `true` or `false`, an enum is its
  page name (`ready`, `gateway`, `admin`), a number its decimal digits, an id or name its canonical
  text; a string field matches exactly, and `""` matches an empty string, never an absent one. An
  empty filter list is no filter; an empty values list is refused. Validation is against the entity's
  schema, never a row: an unknown property, a value the field cannot take, an empty values list or a
  pair on a field that is not public is `INVALID_ARGUMENT` naming the pair; a property the schema has
  and one row lacks (a config entry) simply does not match that row. Public field names and these
  comparison rules are the contract; a field that exists only to serialize is not a property.
  Whether a property is matched by a scan of the view or through an index (the tags index today) is
  the implementation's, invisible in the answer.
- **R-RD3, one answer for "not there".** Nothing matching, including a `mesh` pair naming a mesh the
  admin does not hold, is the empty list (the `unknown-mesh` refusal goes). An answer past the reply
  bound is `ReadOverBound`, `RESOURCE_EXHAUSTED`, as today; the caller narrows. An AND of pairs
  makes an empty answer say less (`[id: x, mesh: m]` empty is "x absent" or "x not in m"); a caller
  that must tell them apart drops the mesh pair and reads the row's mesh (a consumer-guide line, §5.3).
- **R-RD4, a property's read is its row's field.** `node.tags.get`, `node.config.get`,
  `node.cert.get`, `mesh.config.get` and `fabric.config.get` name fields of the entity's row and have
  no wire request of their own; the names go from the typed-objects list and the row headings
  (§5.3), the updates stay.
- **R-RD5, a Build is read whole.** `builds.get` answers each Build with its header (phase, attempt,
  executor, reason, last failure), its topology (every node's `NodeSpec`: executable, storage,
  placement, tags, config) and its receipts, as `build.get` does today, so a read, edit and resubmit
  never clears a field and a failed Build's evidence stays readable. The current Build is selected
  once per read (Q98's rule) and marked. The Build read is the fabric-primary's store-backed stream,
  not a scan of a view; its filter matches the Build header's properties (`id`, `current`, `phase`,
  `attempt`, `executor`).
- **R-RD6, the row keeps its content.** `FabricView` carries its meshes and `MeshView` names its
  nodes as today, because a row's content is not a second read. No store persists a view, so the
  durable-format rule has no say here (dev review, 2026-10-25).
- **R-RD7, the read's address is not a filter.** `connections.get` is served by the exact birth
  whose connections log holds the connections (connections.md, "the data plane is the connections
  log"), so the node, id and incarnation, is the read's address, required and outside the filter
  list, as the fabric-primary is `builds.get`'s address. The filter then matches the connection
  rows (`id`, `destination`, `kind`, `state`) (Architect 1, 2026-10-25).
- **R-RD8, no paging until telemetry shows the bound hit.** The 64 KiB reply bound holds about 150
  node rows at the test fixture's size and about 12 at the maximal row (2 KiB config, 32 labels of
  64 bytes, listeners, executable path and sha, cert); the largest estates in the record are 141 and
  142 nodes. Since Q166 a node's config is set at its birth, so maximal rows are the common case and
  one mesh of them passes the bound well before 150 nodes. A `nodes.get` past the bound is refused
  `ReadOverBound` by name and the caller narrows: by mesh, which is how a fabric is organized, and
  when a mesh alone is past the bound, by `id` in batches, which always gets under it. Builds stream
  in chunks and never meet the bound. The trigger for paging is a `ReadOverBound` span on a real
  fabric that no pair brings under the bound; the shape when it comes is Kubernetes' `limit` and
  `continue`, one appended request variant carrying them and one appended reply carrying the token,
  the mechanism Q98 used. Nothing is reserved for it now.

## 4. The reads

| Read | Address | Properties a pair may name | Answers, in order | Served by |
|---|---|---|---|---|
| `fabrics.get` | — | `id`, `name`, `status`, `provider`, `fabric_primary` | the fabric the admin's view holds: id, name, state, provider, fabric-primary, meshes | a node-admin |
| `meshes.get` | — | `id`, `name`, `status`, `primary_admin` | meshes, by name | a node-admin |
| `nodes.get` | — | every field of the node-properties table: `id`, `name`, `mesh`, `kind`, `status`, `tags`, `config.<key>`, `types`, `is_primary`, `is_fabric_primary`, `executable`, `stopped`, … | node rows by `path.name`, each with status, ports, config, tags and executable; no pair: every node of the fabric, its topology | a node-admin |
| `connections.get` | `node` (id, incarnation), required | `id`, `destination`, `kind`, `state` | the exact birth's own connections from its connections log, as today | the exact birth |
| `builds.get` | — | `id`, `current`, `phase`, `attempt`, `executor` | Builds newest first, each whole (R-RD5); the current one marked | the fabric-primary |

Written as the pair list the wire carries:

```text
nodes.get  [["kind", ["gateway", "broker"]]]
nodes.get  [["mesh", ["mesh1"]], ["kind", ["gateway"]]]
nodes.get  [["kind", ["admin"]], ["is_primary", ["true"]]]        every mesh-primary
nodes.get  [["is_fabric_primary", ["true"]]]
nodes.get  [["tags", ["red"]], ["tags", ["eu"]]]                  all of; one pair is any of
nodes.get  [["config.region", ["eu-west"]], ["status", ["ready"]]]
nodes.get  [["id", ["<node id>"]]]                                 "one node"
nodes.get  []                                                      the fabric's topology
meshes.get [["name", ["mesh1"]]]
builds.get [["current", ["true"]]]                                 "the current Build"
connections.get { node: (<id>, <incarnation>) } [["state", ["connected"]]]
```

## 5. Delta

### 5.1 Wire (appended variants only; no row or field moves, so no `durable format moved` marker)

- **The filter type**, in rdm-core beside the Status request: `Filter { pairs: Vec<(String, Vec<String>)> }`
  with the R-RD2 rules, and one matcher per entity that names its public properties and converts each
  field to its canonical text once; the same matcher serves every op that reads that entity.
- **Status 0x1B.** One request variant appended, `Get { entity: Entity, node: Option<(NodeId, IncarnationId)>, filter: Filter }`,
  `Entity::Fabrics | Meshes | Nodes | Connections`, `node` required for `Connections` and refused
  otherwise; one reply variant appended, `Fabrics { fabrics: Vec<FabricView> }`. `REQUEST_VARIANTS`
  32 → 33, `REPLY_VARIANTS` 55 → 56. `GetNodes`, `GetMeshes`, `GetFabric` and `GetConnections` keep
  their positions and decode into the same pairs (`NodeSelect::Id(x)` is `[id: x]`,
  `Mesh(m)` is `[mesh: m]`, `Tags(All[a, b])` is `[tags: a][tags: b]`, `Tags(Any[a, b])` is
  `[tags: a, b]`), so one handler serves both; rdm-client no longer sends them. `Nodes`, `Meshes`,
  `Connections`, `ReadOverBound` and the malformed replies are reused; `UnknownMesh` is retained
  unsent.
- **Build 0x20.** One request variant appended, `GetBuilds { filter: Filter }`; three reply variants
  appended, per Build newest first: `Topology { build_id, chunk_index, chunk_count, nodes: Vec<NodeSpec> }`
  chunks (a 256 KiB request can carry a topology no 64 KiB reply frame holds), the existing `Steps`
  chunks, then the header `Build { build_id, phase, attempt, executor, reason, last_failure, current,
  steps, chunks }`, `Got`'s fields with the `current` mark, a data frame that closes one Build; then
  `End { builds }`, the read's one terminal. `Got` is a terminal frame today (`build.rs`
  `frame_kind`) and cannot close one Build of several, so it is not on this stream (Architect 1,
  2026-10-25). The current Build is selected once per read (Q98) and is the one header marked
  `current`; the filter is applied to each Build's header before it is streamed, so `[current: true]`
  streams one Build and `[id: x]` the one named. `REQUEST_VARIANTS` 4 → 5, `REPLY_VARIANTS` 26 → 29.
  `Get { build_id }` keeps its position and its answer (`Steps`, then `Got`) over the same read
  implementation; rdm-client no longer sends it. A stream that ends before `End` is an incomplete
  read, never a list (Node RPC §34, §43).

### 5.2 Client and spans

- rdm-client: one `Filter` builder (`Filter::new().eq("kind", ["gateway"]).eq("mesh", ["mesh1"])`),
  and `Fabrics::get(&Filter) -> Vec<FabricView>`, `Meshes::get(&Filter) -> Vec<MeshView>`,
  `Nodes::get(&Filter) -> Vec<NodeView>`, `Connections::get(&ExactBirth, &Filter)`,
  `Builds::get(&Filter) -> Vec<Build>` (header, `nodes`, `steps`, `current`). Gone:
  `Nodes::get(&NodeId)`, `Meshes::get(&str)`, `Meshes::nodes`, `Fabrics::get()`, `Fabrics::meshes`,
  `Fabrics::nodes`, `Builds::get(&BuildId)`, `BuildReceipts`, `NodeSelect` and `TagFilter` as client
  types. No rafka-v2 code calls them; the callers are RDM's tests and the demo.
- Spans: `rdm.node_admin.fabrics.get.via-call`, `…meshes.get…`, `…nodes.get…`,
  `…connections.get…`, `…builds.get.via-call`, each recording the pairs' property names;
  `NodeSelect::read()` and `BuildOp::Get`'s name follow. `docs/reference/rdm/wire.md` and `spans.md`
  are docgen output, regenerated.

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
  per entity; the filter a list of (property, values) pairs, AND across pairs, any-of within, a
  repeated property AND, the property a public field of the row by its page name with one dot into
  config; a list in a stable order, empty when nothing matches; an unknown property, a value the
  field cannot take or an empty values list INVALID_ARGUMENT)`.
- line 269: `build.get is a read for reattachment, never a resume.` → `builds.get [id: build_id] is a
  read for reattachment, never a resume.`
- line 315, the `node.get` row → `| nodes.get | [Status 0x1B](#status-op-0x1b-rdm) Get { entity:
  Nodes, filter }, answered Nodes: every node row the answering admin holds that matches every pair,
  by path.name; no pair is every node of the fabric, its topology; a pair names any field of the
  node-properties table by its name, config entries as config.<key>; tags matches a node carrying
  the value, repeated for all of; a node's tags, config and cert are fields of its row | the rows,
  NodeView; a stopped node answers stopped; no event | OK, the empty list when nothing matches; an
  unknown property, a value the field cannot take or an empty values list INVALID_ARGUMENT naming
  the pair; read bound RESOURCE_EXHAUSTED |`.
- line 318, the connections row: the heading `node.connections.get/[delete](…)` →
  `connections.get { node } / [node.connections.delete](…)`; `Held-connection read;` →
  `connections.get { node } answers the exact birth's own connections from its connections log, the
  node (id, incarnation) the read's address and the filter over id, destination, kind and state,
  [Status 0x1B](#status-op-0x1b-rdm) Get { entity: Connections, node, filter }, answered Connections;`;
  `as node.connections.get returns them` → `as connections.get returns them`.
- line 319, the tags row: the heading `node.tags.get/create/delete` → `node.tags.create/delete`;
  `` `node.tags.get` answers the node's labels; `` → `a node's labels are the tags field of its row
  (nodes.get);`.
- line 324, `node.cert.get`: `A field of node.get:` → `A field of a node's row (nodes.get):`.
- line 326, the mesh row's read half: `mesh.get { name } answers the one mesh with that name or
  minted id, … the filter read through the node store's tags index (Q165);` → `meshes.get answers
  every mesh the answering admin holds that matches every pair (id, name, status, primary_admin), by
  name, [Status 0x1B](#status-op-0x1b-rdm) Get { entity: Meshes, filter }, the empty list when none
  matches; a mesh's nodes are nodes.get [mesh: name];`. The row's heading loses `mesh.get` and
  `mesh.nodes.get`, `mesh.config.get/update` becomes `mesh.config.update`, and `mesh.tags.get` goes
  from its list.
- line 328, the fabric row's read half: `fabric.get answers the fabric: … the filter read through
  the node store's tags index (Q165);` → `fabrics.get answers the fabric the answering admin holds
  when it matches every pair (id, name, status, provider, fabric_primary): its id, name, state,
  provider, fabric-primary and meshes, [Status 0x1B](#status-op-0x1b-rdm) Get { entity: Fabrics,
  filter }, answered Fabrics; its nodes are nodes.get;`. The heading loses `fabric.get`,
  `fabric.meshes.get`, `fabric.nodes.get`, and `fabric.config.get/update` becomes
  `fabric.config.update`.
- the node-properties table (lines 340 to 366) gains one sentence before it: `Every field named
  here is a property a nodes.get pair may match, by this name; config entries as config.<key>.`
- lines 360 and 361: `node.get answers it whole` → `nodes.get answers it whole`;
  `node.get answers held whole` → `nodes.get answers held whole`.
- line 366: `node.get answers the whole node.` → `nodes.get answers the whole node.`
- line 1300, the Build calls table: `| build.get | caller → fabric-primary | build_id | read
  accepted Build and receipts for reattachment after a cut; never progress polling |` → `| builds.get
  | caller → fabric-primary | filter over id, current, phase, attempt, executor | a stream: per Build
  newest first Topology chunks, Steps chunks and its Build header, then End; each Build whole, its
  topology and receipts; the current Build selected once per read and its header marked current; for
  reattachment after a cut, never progress polling |`.
- line 1316: `build.get for the receipts` → `builds.get [id: build_id] for the receipts`.
- The 0x20 reply enum listing gains the three appended variants and the counts of §5.1.

build.md: line 172 `audit/history for build.get` → `audit/history for builds.get`; line 202
`build.attempt.run, build.get and build.delete` → `build.attempt.run, builds.get and build.delete`;
line 405 `build.get is a read of the evidence, not a reattachment` → `builds.get is a read of the
evidence, not a reattachment`.

build-deployment.md §5, the paragraph → `The node's row is the source. nodes.get returns every node
row that matches its pairs, each with its executable beside its status, config and tags
(NodeView.executable): one node by id, a mesh's nodes by mesh, the fabric's by no pair. The digest,
the MeshTopology projection and node.topology.get carry no executable.`

consumer-guide.md: line 31 → `The read calls follow one rule: one read per entity; the filter is a
list of (property, values) pairs, AND across pairs, any-of within, a repeated property AND; a
property is any field of the row by its page name, config entries as config.<key>; a list in a
stable order, empty when nothing matches.`; the table at lines 33 to 39 becomes §4's table and the
pair-list examples, followed by two lines: `A read past the 64 KiB reply bound is refused
ReadOverBound (RESOURCE_EXHAUSTED); read by mesh, and a mesh past the bound by id in batches.` and
`Pairs combine with AND, so [id: x][mesh: m] answers empty when x is absent or when x is in another
mesh; to tell them apart, read [id: x] and look at the row's mesh.`; line 69 `node.tags.get, or
the tags field of its node row` → `the tags field of its node row (nodes.get)`; line 70
`fabric.nodes.get { tags } or mesh.nodes.get { tags } (Q165): the nodes carrying all of the given
labels, or any of them; read through the node store's tags index, one point read per label` →
`nodes.get [tags: a, b] for any of the labels, [tags: a][tags: b] for all of them, with [mesh: m]
for one mesh's`.

fabric-mesh-node-lifecycles.md: line 100 `| [node.get](…) | Reading a node |` → `| [nodes.get](…) |
Reading nodes |`; line 101 `| [node.connections.get](…) | Reading a node's held connections |` →
`| [connections.get](…) | Reading a node's held connections |`; line 123 (0x1F) unchanged.

Name pass, meaning unchanged (`node.get` → `nodes.get`, `fabric.get` → `fabrics.get`, `mesh.get` →
`meshes.get`, `build.get` → `builds.get`, `node.connections.get` → `connections.get { node }`):
connections.md (lines 56 and 603), node-connections-delete.md (lines 19 and 28), fabric-certs.md (4),
node-identity-and-state.md (4), gossip.md (2), node-rpc.md (1), lifecycles/node-recovery.md (2),
lifecycles/fabric-ca-renew.md (2), lifecycles/node-status.md (1), lifecycles/states.md (1),
lifecycles/node-connections-delete.md (2), eng/provider-runbook.md (5), eng/node-storage-runbook.md (1),
eng/fabric-recovery-runbook.md (1).

### 5.4 Rulings

The Q167 row gains: `**Amended (Luke, 2026-10-25; [one read per entity](../proposal/q167-one-read-per-entity.md)):**
one plural read per entity; the filter a list of (property, values) pairs, AND across pairs, any-of
within, a repeated property AND, the property any public field of the row by its page name, config
entries as config.<key>, validated against the entity's schema; a list in a stable order, the empty
list when nothing matches; the singular reads go; builds.get answers each Build whole, header,
topology and receipts, the current one selected once per read and marked; connections.get { node }
is the fifth, the node its address; a property's read is its row's field.` The Q165 row's state
column: `superseded by Q167 as amended`.

### 5.5 Cells

- The matcher, one cell per entity: AND across pairs, any-of within, a repeated property AND; a
  `config.<key>` pair with a dotted key; a property the schema has and one row lacks matching the
  others; the empty list for no match (a mesh pair naming an unknown mesh included);
  `INVALID_ARGUMENT` naming the pair for an unknown property, a value the field cannot take (a
  boolean spelt otherwise, a non-canonical id) and an empty values list; `""` matching an empty
  config value and not an absent key; the existing over-bound cell renamed.
- `connections.get` without its node is refused by name; with it, the filter over the birth's
  connection rows.
- `builds.get` over a history of three Builds through the real carrier, one Failed part-way: all
  three arrive in order, only `End` completes the read, the current one's header is marked, every
  header, topology and receipt as `build.get` reads them today; `[current: true]` streams one;
  the current Build moved mid-stream names one Build (the 0x1F cell at `build_facts_read.rs:599` is
  the model); a stream cut between chunks is an incomplete read.
- The retained `Get { build_id }` still ends on `Got` (the existing `build.get` cells, kept).
- The retained variants decode into the same pairs (`status.rs` wire tests, `build_wire.rs`).

## 6. Arms

- **A (recommended): one read per entity, the filter a property match**, §3 to §5, Q167 amended
  as §5.4.
- **B: one read per entity with named filters** (`ids`, `mesh`, `tags`, `kinds`, …), a wire field per
  property.
- **C: Q167 as recorded**: a singular and a plural read per entity, `NOT_FOUND` on the singular.
- **X: something else.**
