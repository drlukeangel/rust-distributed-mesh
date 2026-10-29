# Node identity and state — the architecture

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns node identity (the minted id, `endpoint_id`, `path.name`, incarnation and the
display prefixes), the node, mesh and fabric rows and who writes each, the `NodeState` liveness
enum, the hydration roster, local eviction and the stopped-node collector. Lifecycle states and
check-ins are [States](lifecycles/states.md#states); each workflow is its page in the
[lifecycle index](fabric-mesh-node-lifecycles.md); gossip is [gossip.md](gossip.md); the primaries are
[elections](fabric-node-lifecycle-elections.md).

---

## 1. The model

Every node, whatever its kind, is an `rdm-node` process; a node-admin is `rdm-admin` on it. The
node runtime owns identity, the iroh mesh transport, gossip, the topology projection and the
lifecycle state machine; the application layers its own boot on top through the
[hooks](fabric-node-hooks.md) and `Wiring`.
The lifecycle architecture rests on five design rules:

1. **One status writer per node.** Its mesh-primary sets a node's status from the node's reports
   on its workflow's reply stream ([node status](lifecycles/node-status.md)); absence, a failed
   dial or RPC, a transport drop or an unheard peer mesh never proves an exact runtime gone
   ([States](lifecycles/states.md#routing)).
2. **Aliveness and readiness are separate axes** (§4): readiness is
   the status a node's row holds, liveness the `NodeState` peers observe from its digests (§4).
3. **Observation and exact departure are different facts.** Local NodeState::Dead/stale vocabulary may
   describe an observer's routing view, but destructive recovery and Node RPC Gone require canonical
   exact-runtime deletion/supersession evidence. A stopped status is a lifecycle state, not proof that the
   runtime has disappeared.
4. **Events, not timers.** Every transition is event-observable (span-emitting). The time-based
   mechanisms are the heartbeat and anti-entropy cadence
   ([gossip.md §3.2](gossip.md#32-cadence-timers-only-for-heartbeat-and-anti-entropy)), the
   staleness floor (an inference bound, §6) and the stopped-node collector's ttl (§6.9); none is
   a coordination timer.
5. **A gossip struct is DURABLE, and adding a field MOVES the format.** `GossipDigest`,
   `GossipEnvelope`, `NodeState` and `NodeDigest` are postcard-positional. Postcard does not
   honour `#[serde(default)]` for a missing trailing field: an old-shaped frame EOF-errors and
   is dropped whole ([gossip.md §3.1](gossip.md#31-frame-catalog)). So a new field is a **hard cutover** — the commit says "durable format moved" and
   every live mesh is taken down and deployed again, never redeployed in place, before anything
   attaches.

## 2. A node is an ENTITY, and it has three ids

**A node is one entity.** Every node, whatever its kind, is one record shape, the node row
(`NodeRecord`, `rdm-admin/src/storage.rs`: `node_id`, `fabric_id`, `name`, `mesh_id`, the
incarnation, the status and the config), living in the ONE nodes store of the fabric
(`NodesStorage` on the fabric's shared store root; Rafka binds it to its `<system-org>.ops.nodes`),
one writer per mesh, never a separate per-mesh store (§2.5). Its id is minted by the one minter and
its fields are the node's durable facts. **The
gossip digest is a PROJECTION of that record**, not a second source for it — which is why a key
rotation lands in the record first and gossips afterwards, and never the reverse.

**A node has one identity on the fabric, a name that says where it sits, and one ID — its
minted id. THE FABRIC IS ALL MESHES**, so the endpoint identity is globally unique — never
mesh-unique. A mesh is the named group of nodes a node belongs to; `mesh_id` is the mesh's own id
and is never a node's. The id IS the minted id (the short Crockford id), one thing with one name;
nothing is ever derived from the `endpoint_id`. Only the id is shown on public interfaces (REST, wire,
logs), and it does not change on restart; anything that refers to a node durably stores its `path.name` (§2.2).
The three words are `endpoint_id`, `id`, and the prefixed dashed RENDER of that same `id`.

| id | what it is | uniqueness | who sees it |
|---|---|---|---|
| **`endpoint_id`** | the node's iroh key (ed25519 public key, hex) — its identity on the FABRIC, the thing a dial resolves to. A FIELD of the node record | globally unique across every mesh | **THE TRANSPORT ONLY.** Never on a public interface — not a REST body, not a wire response, not an audit row, not a log line or span attribute NAMING a node (§9 rule 4). The one exception is a span whose SUBJECT is the routing key itself — a dial's target, the handshake, the identity load/mint before an id exists to log — never a narrative "which node did this" field. Exposing the routing id outside those is a security breach |
| **`path.name`** | path and name are the node's two fields, joined only as the path.name index key; both are free text the caller chooses at node.create (`<mesh>.<kind>.<name>` by convention, a name, never an ordinal); the node's type is its own field, `node_type` (§2), never read from the path | unique per (path, node), by the ordinary name claim | what topic placement and `replica.targets` store (§2.2); a Build step key names its target by it (`node.create:<path.name>`) |
| **`id` — the node's minted id** | the node row's own id — minted once, 60 bits, 12 Crockford characters, bare internally | globally unique, by the same property every minted id in the platform has | **the PUBLIC, STORED and AUDIT identity.** It names one node instance: what the audit log names a node by (§2.4) and what REST and the wire render for a node. Its external form renders the type prefix from `node_type`; a client only ever names a BROKER, so **`brk_<crockford>` is the only form a client sees** |

- **One minter, no second form.** The id is `mint_id()` like every other entity's — no short form,
  no per-mesh structured mint, no separate allocator. Random collision is n²/2⁶¹ and negligible
  precisely because nothing derives it from an input.
- **The id is MINTED, not derived.** Nothing computes it from the `endpoint_id`: a derived id would
  change when the identity changes, and a node's public name must not move because its key did.
- **`endpoint_id` is ROUTING-ONLY.** Gossip digests, the topology projection, the mesh handshake and
  the dial itself key on it — nothing stored on behalf of an application does. A stored reference
  to a node (Rafka's replica sets and placement among them) carries the node's `path.name`, and a
  dial resolves `path.name` → `endpoint_id` at DIAL time (§2.2), never from a stored copy. A stored
  64-hex `endpoint_id` is refused by name.
- **The id names one node INSTANCE and is never reissued.** A stop or a start keeps the id,
  incarnation, endpoint and port; a new bin is a new node with a fresh id (the replace steps), and so
  is a node that replaces another or is added (§2.3).
- **A node PROCESS has an incarnation.** The `Node` row carries `incarnation_id`, an opaque id minted by the
  one minter at node.create, naming the exact runtime that serves the node's id and `path.name`. It stays the
  same through stop and start (the process never exits) and ends at delete; recover and replace create
  a new node with its own. The process reads it once at boot (`RDM_INCARNATION_ID`, written by node-admin at launch). A reader
  compares incarnations for EQUALITY only: it is not a counter, and it is not the writer incarnation that refuses stale writes
  a writer's rows. `nodes.get` returns it per node.
- **A node has a `node_type`.** `node_type` is a field of the node record, a string the caller sets in
  node.create's spec. An omitted or empty value is stored as `node`. Its grammar is the tag-label grammar,
  1-64 characters of `a-z 0-9 - _ . ~` with no leading `.`; a value outside it, or with leading or trailing
  whitespace, is refused by name at node.create. It is immutable: node.update refuses a change by name.
- **A node's `path.name` is what placement references.** Nodes use the same
  `path.name` mechanism as every entity: the path and the name are free text set at node.create;
  nothing is derived from either. A single node is created, for example, as `mesh1.broker.north`. A node advertises its `path.name`, its `endpoint_id` and its minted
  id; the last two change, the `path.name` does not. An application that stores a node reference
  stores the `path.name`, so a new `endpoint_id` or minted id has no effect on it.
  A re-attach or a growth changes nothing: the nodes are created at the same `path.name`s, and the
  routes to the new nodes materialize with no update to any topic. There are two renames, both
  node.name.update under its [rename precondition](lifecycles/node-name-update.md), always of a live node whose
  connections are deleted and which has left its mesh: a replace's predecessor to `<name>.old`, and a rollback's
  back to `<name>` ([node replace](lifecycles/node-replace.md)). A stopped node is never renamed. Nodes are system
  entities, never renamed by hand.
- **A path is a path.** A path is dotted segments, minted once and immutable, with no structural meaning. The segments in use are a convention (`mesh1.broker`, `fabric1.meshes`). The link from a node to its mesh is the `MeshId` on the node row, and where a row is stored says nothing about the entity's path.
- **The external form appears only on public interfaces** (an API answer, a log line, a span) and
  is stripped once, at ingress; the application's own boundaries (Rafka: REST and its audit log) are
  the application's.

### 2.0 One id space, four HUMAN-READABLE prefixes — so the DECODER enforces the type, not the renderer

The prefix renders from `node_type` over ONE 12-character id space, human-readable by node type: `brk_` = broker, `adm_` = node admin,
`gtw_` = gateway, `cmp_` = compute. A Build id is the bare 12-character Crockford60 on the wire, in every store row and in every page example, and renders `bid_xxxx-xxxx-xxxx` (`BuildId::log()`) in log lines and span fields only; nothing parses or stores that form.

### 2.2 The node `path.name` is what a reference STORES — the `endpoint_id` is resolved at dial time

An application names nodes in its own configuration, so a stored reference to
a node is its `path.name` (`mesh1.broker.1`), not its minted id or its iroh key.

- **Every dial resolves `path.name` → the node → its `endpoint_id` from the `topology-nodes`
  projection** (§2.6), which every node already holds in memory when it routes. The
  projection carries a `path.name` index, so a node whose `endpoint_id` moved (§2.3), or a `path.name` a
  new node now holds (§2.3), is reached without a single config changing.
- **The node owns its name**: it is the owner of the name it publishes, and what it publishes is the row's name. A
  member never writes its row merely because it is the subject (§2.5).
- **A 64-hex `endpoint_id` or a minted node id in a stored reference is a defect** and is refused
  by name at the reader.
- **The stored `path.name` does not change on a RESTART, a RE-ATTACH, a GROWTH or a REPLACE** (§2.3): a re-attached or grown node is created at its `path.name`; a replaced predecessor, live and
  out of its mesh, is renamed `<path.name>.old` and stopped, and its successor is created at the `path.name`: a re-birth. No stored
  reference to it is ever rewritten.

### 2.3 A new node is a new id — a REPLACE takes the old node's `path.name`, an ADD takes its own

A node's id follows the INSTANCE, so every replacement is traceable, and every stored reference
follows the `path.name` (§2.2), so nothing stored changes when a node is replaced.

| path | node ids |
|---|---|
| first create | fresh |
| golden attach, restage | fresh for a node that is replaced; the same for a node that is not |
| golden attach or restage + expansion | as above, plus a fresh id for each new node |

| a new node that… | its id | its `path.name` | the node it relieves | placement |
|---|---|---|---|---|
| REPLACES a node in the topology | fresh | created at `mesh1.broker.1` once the predecessor released it | out of its mesh, renamed `mesh1.broker.1.old` then stopped, or deleted | stays as it is |
| is ADDED | fresh | its own next one (`mesh1.broker.4`) | none | an application refers to it only by an explicit change of its own configuration |

- **A replace is [node replace](lifecycles/node-replace.md).** Insert-and-fail on each of its
  renames means two nodes never hold one `path.name`.
- **Node certs:** [fabric-certs.md](fabric-certs.md).
- **node.delete releases the node's identity and name; its row stays.** The node's mesh-primary
  writes the row with status deleted, keeping the record and its config; the deleted row releases
  the `path.name`, so a node created there afterwards claims it with a fresh id and seeds its
  config from it. The workflow is [node delete](lifecycles/node-delete.md).

### Day zero

node.create mints a node's id and hands it to the process at launch, so a node knows its own id
before it reads anything from a store. Its mesh-primary writes its row to the store at once
([States](lifecycles/states.md#states)). **Nothing on the boot path reads the store:**
a node that cannot reach a broker still has its identity, its name and its id, because all three
came down with it.

No node.create launches the bootstrap admin: the bootstrap's first step mints its node id, before
anything else of its boot, its telemetry included, so its telemetry's resource carries it, and its
Day-0 claim keeps it on its own row. A node-admin started with no launcher over an inherited data dir, and an
adoption launch, are new nodes: the first step mints a new id, never the inherited row's (Q208).

### 2.5 ONE system org, one projection, one writer per mesh

The fabric has one nodes store on its shared store root (R14), held by every mesh, so a mesh whose
peer goes unheard keeps serving with a complete record of the fabric as it last knew it. It is the
fabric's record of every node in every mesh, and each mesh-primary writes its own mesh's rows into
it. How an application replicates its own copy of that record to every mesh is the application's.

**This is what the one-writer-per-mesh rule buys.** Because a mesh-primary only ever authors its
OWN mesh's rows, two meshes that could not hear each other cannot have written conflicting versions
of the same node — they wrote disjoint sets of rows. Rejoining is therefore a UNION of each mesh's
rows, not a conflict to adjudicate: no last-writer-wins, no vector clocks, no merge policy to get
wrong. Dividing the writers by mesh is the property that lets each mesh keep working while a peer
mesh goes unheard.

The one thing a union does not settle by itself is a node whose STATE changed on the far side while
the two were apart — a member that died, was deleted, or moved. That is a membership event in its
own mesh's rows, authored by that mesh's own primary (§4), so it arrives as that mesh's own
history rather than as a competing claim about a node this side owns.

**Each mesh's PRIMARY node-admin is the only writer for its OWN mesh's nodes, and writes reach it
only over node-RPC.** N meshes means N writers, one per mesh, never more — the writers are
divided by mesh, so two of them can never contend for the same node. A multi-mesh fabric has
two primaries, neither owning the other's members.

<a id="the-projection-differs-from-every-other-projection-in-exactly-two-ways"></a>
#### The projection is filled from two gossip channels

The topology projection is filled from two gossip channels, the mesh-local channel and the
backbone ([gossip.md §3](gossip.md#3-hierarchical-topology)), where every other
projection has one source. **Neither channel writes**: gossip updates the projection and never authors the record
([gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)), and
a mesh's own nodes arrive only through node.create calls for that mesh.

#### CRUD is the ORDINARY entity-framework path

1. A node.create reaches the node's mesh-primary as a Build step the fabric-primary dispatches; a
   node.update goes caller → the node's mesh-primary.
2. The mesh-primary claims the name where it changes and writes the row from the record it holds,
   a blind put.
3. **Only once that write is acked** does it update the topology projection and tell the node (row
   first, mesh second).

Ack before insert, exactly as every other projection in the system. A virtual topic per status
is how a reader takes a subset (`pending`, `ready`).

A write to a broker does not move the projection. It changes on membership, reachability and role
events, and at no other time.

#### A node-admin is the CARETAKER OF ITS MESH — that is all it needs to know

It does not know whether its mesh is the first or the 101st, and it does not care. There is no
"am I first" question anywhere in its boot, because nothing it does depends on the answer.

Its day zero does NOT read `ops.nodes`. At Day 0 the bootstrap admin, as mesh1's primary, claims its
own name and writes its own row through the create path, the first write of its create. That write is a row write only: it is never permission to originate a rename of its own row. Every later
node-admin's row is written by its mesh's primary. From there the topology grows by two inputs: the
backbone mesh topology of each peer mesh, and node.create calls for its own mesh.

Mesh-local gossip is not a third input: it carries liveness for members this admin already knows,
which are there because one of those two put them there.

#### What is durable, and what is never durable

| | where | written by |
|---|---|---|
| identity (`id`, `endpoint_id`, `name`, `node_type`) and STATUS CHANGES | the durable record on `ops.nodes` | that mesh's primary node-admin, alone |
| per-heartbeat liveness — `cpu_used`, `ram_used`, reachability | gossip, in memory only | every node, about itself |

In normal operation nothing reads `ops.nodes` back — the mesh-primary holds the record. The store
is read only to rebuild, after a primary change or a recovery, before anything is written, each
mesh-primary from its own mesh's rows.

A status change — starting → pending → ready → connections-deleting → connections-deleted → stopping → stopped → deleting → deleted — happens on a REAL membership,
reachability or role event, the same boundary §4 and the owner principle draw, and is worth a
durable write. Per-heartbeat metrics are not and never become one: **gossip converges with no
broker alive at all**, and a durable heartbeat would destroy that property. Status is its own
field precisely so the durable half stays distinct from the noisy half.

#### What a peer mesh's primary takes from the fabric-primary, and what it keeps

A mesh-primary takes rafka-time and the fabric-level projections from the fabric-primary when it
enters the fabric ([node.mesh.join](lifecycles/node-mesh-join.md): the join answer's rafka-time, the
entry answer's projections) and each time the fabric-primary changes.

### 2.6 The projection is `topology-nodes`, and it is fed like any other projection

`ops.nodes` is the BACKING. `topology-nodes` is its PROJECTION, and the projection is what every
reader actually reads — one flat list, one row per thing, typed `node`, `mesh` or `fabric` ([States](lifecycles/states.md#states)):

```
[ { "type": "fabric", "name": "<fabric name>", "path": "", "id": "9ab1234cd567", "status": "ready" },
  { "type": "mesh", "name": "mesh1", "path": "", "id": "cd123456789a", "channel": "<channel derived from the mesh id>",
    "status": "ready" },
  { "type": "broker", "name": "1", "path": "mesh1.broker", "id": "56789ab1234c", "status": "ready" },
  { "type": "node-admin", "name": "1", "path": "mesh1.admin", "id": "5cd16789ab23",
    "status": "ready", "is_primary": true, "is_fabric_primary": true } ]
```

Node rows come from the nodes store (this projection); a mesh's status is its mesh-primary's `MeshStatus` frame
(§2.7); the fabric row is the fabric's identity, carried by gossip (§2.8).
`nodes.get` returns the node rows.

An application's reset of its own copies of these records (Rafka's WAL reset) is the application's
operation, outside the RDM lifecycle and its name rules.

**The primary node-admin is the one writer, because it does all of its mesh's node provisioning.**
It writes the nodes store when a node workflow step or a node.update reaches it, and when a status change happens
INORGANICALLY — a node lost, a role moved, anything the mesh observes rather than was asked for.
Those are the two events that author a row; nothing else does.

**There is no special case, Day 0 included.** Every writer writes its rows to the store from Day 0
(Q171); a node still carries its identity down with it at launch (§ Day zero) rather than reading it
from the store. Every update to the projection follows the ordinary
path: the write goes to the nodes store, is durable before its caller is answered, and the projection
updates from the record; no bespoke node path.

**In steady state gossip carries most of the traffic, and that is not a second source.** Nodes are
not launched often, so in a running fabric the predominant update is a digest arriving, and the
digest is the record's projection onto the wire ([gossip.md §3.1](gossip.md#31-frame-catalog)) — it carries the record's facts, it does not
author them. That is the whole content of the source→projection inversion: a key rotation lands in
the record and gossips afterwards, never the reverse.

### 2.7 A mesh is an entity, and node-admin owns it

**A mesh is an entity**: one row per mesh in the fabric's mesh store (`MeshStorage` on the shared
store root; Rafka binds it to its `<system_org>.ops.meshes`).

- **Its id is minted** (bare Crockford internally, displayed `msh_xxxx-xxxx-xxxx`), and
  `Node.mesh_id` holds that minted id. A mesh id is what an address names wherever a mesh must be
  named: a store's audit topic is `<store>.adt_<mesh id>` in every fabric, one mesh or ten.
- **Its name is `mesh<#>`, its position in the shape**: node-admin's own mesh
  takes `RDM_MESH` (`mesh1` when unset), and a mesh a Build creates takes the Build's `mesh_name`. The name is never derived from the mesh's
  channel. It is the first segment of every member's path.name (`mesh1.broker.1`, §2.2). The mesh's
  own path is opaque (§2).
- **The mesh's primary node-admin claims the mesh's name and writes its row at creation.** The
  mesh's first node-admin is its primary (the only candidate of its election),
  and the mesh-primary keeps that mesh's row in the mesh store: it claims the mesh's name at the key
  `(path, name)`, unique within its path, the path opaque (a deployment may name it after the
  fabric, as it names a node's after its mesh; that is its choice, not the key's shape), writes
  the row to the store when the mesh is created and writes every status from then on. Held means held by a different MeshId: `NameHeld` names
  that holder. The same MeshId re-claiming its own name, on a retry or from a replaced primary, is
  idempotent and accepted. The fabric-primary does the same for the fabric's row on its own
  `(path, name)`, at Day 0, idempotent for the same FabricId. At Day 0 the fabric-primary is its mesh's mesh-primary too and creates the fabric row and its own mesh's row, the latter as that mesh's primary; every other mesh's row is created by that mesh's primary. It writes its own members' `Node` rows; no other
  node-admin writes them (a secondary writes none, a WAL reset's rewrite included).
- **A mesh outage moves no row.** The fabric-primary observes it
  ([mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation)) and never writes the
  peer mesh's row; an unheard mesh is not dead.
- **It carries tags** as its own property (mesh.tags.get / create / delete, the same shape as a node's tags), so an application selector resolves a mesh by tag before it
  submits mesh.delete or any other Build intent.

- **Its lifecycle states and who declares each are defined only in [States](lifecycles/states.md#states).** Each status has a virtual topic, so "which meshes are ready" is one filtered read.

The row is `MeshRecord` (`rdm-admin/src/storage.rs`: `mesh_id`, `name`, `dead`); `Node.mesh_id`
holds the minted `MeshId`. The mesh's channel is derived from its MeshId and its status is its
mesh-primary's `MeshStatus`. Who is primary in the mesh is the [election](fabric-node-lifecycle-elections.md)'s
result; the mesh-primary records it as `is_primary` on each node's row and `is_fabric_primary` on its
own, at the primary's `revision` ([node.primary.update](lifecycles/node-primary-update.md)); the write
records the role and does not grant it.

- **Who mints it.** The Build that creates a mesh's first node-admin mints the mesh id, never
  reissued; the fabric-primary drives that node.create and decides from the accepted topology whether
  the mesh exists. The mesh-primary, elected like every primary, claims the mesh's name and writes its
  row. A node-admin that joins a mesh adopts its id. Assignment and recovery are owned by the
  [Build carrier](node-rpc-envelope.md#build-op-0x20-rdm).
- **The row's author is the current mesh-primary, as the election names it.**
  [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md) is the only specification of how that primary is elected. A node-admin that is not the mesh-primary may adopt the row but does not author it. When the mesh-primary changes, row ownership follows it; this document does not define a second overlap, comparator, or successor rule.
- **The row changes writer with the mesh-primary.** The mesh-primary changes by its own hand-over
  ([node.primary.update](lifecycles/node-primary-update.md)) or, unheard past the staleness floor, by
  election; either way the successor is the mesh's writer from the moment it is mesh-primary and records
  its own row. Every row of the mesh, the predecessor's included, is the successor's to write from then and
  nothing is rewritten; the successor writes from the rows its caches hold, hydrated at its create, start
  and join, and reads no storage (Q227). A node-admin that is not the mesh-primary writes no row.
- **The write.** The mesh-primary writes the `Mesh` row and every member's `Node` row through the
  mesh rows writer, and every node writes its connections rows, each create and update at once,
  from Day 0 (Q171; [States](lifecycles/states.md#states)). A write the
  store refuses or fails is owed: the writer retries the row's current state under the one backoff
  rule ([Node RPC §43](node-rpc.md#43-owed-messages-delivery-is-the-runtimes)) until the store
  acknowledges it, on its own clock, never waiting for a birth, a digest or a primary change, and marks
  the row written only on that acknowledgement, while it is the writer. The
  row's write is the claim then the record (`rdm-admin/src/admin/rows_writer.rs`,
  `rdm.node_admin.mesh.create.via-mesh-row`: `row-written`, `row-held-by-mesh` for the same MeshId,
  `name-held` naming the holder). A mesh whose name another mesh's row already holds stays held,
  naming the holder; it is never renamed, and the id never moves.
- **Every node holds its mesh's status** from the mesh-primary's `MeshStatus`
  ([gossip.md §3.1](gossip.md#31-frame-catalog)).
- **A member that exits outside a workflow** is its mesh-primary's
  [node recovery](lifecycles/node-recovery.md).

### 2.8 A fabric is an entity, and the fabric-primary owns it

**A fabric is an entity**: one row per fabric in the fabric store (`FabricStorage` on the shared
store root; Rafka binds it to its `<system_org>.ops.fabrics`). A fabric exists from the first node: one
node in one mesh is a fabric of one mesh.

- **Its row** is `FabricIdentity` (`rdm-admin/src/fabric_storage.rs`: `fabric_id`, `name`), immutable; its status is the
  fabric-primary's `FabricStatus` frame. Which node-admin is fabric-primary is the election's result, recorded as
  `is_fabric_primary` on that node-admin's own row, never on this one. It carries no pointer to a Build: the current Build is read from the Builds newest first ([build.md §4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)).
- **Its id is minted** (bare Crockford internally, displayed `fab_xxxx-xxxx-xxxx`). Its path is `""`, the root every
  mesh and node sits under, so its path.name is its name. A row that names the fabric it belongs to carries that id in
  a field named `fabric`; `endpoint_id` is a node's routing key (§2) and never names a fabric.
- **The first node-admin creates it.** The node-admin that is both mesh-primary and fabric-primary, `is_primary` and `is_fabric_primary` (the
  first node of the first mesh), mints the fabric and its mesh together and holds both in memory; the sequence is
  [fabric.create](lifecycles/fabric-create.md).
- **It is written at Day 0.** The fabric-primary claims the fabric's name at its `(path, name)`, the
  path opaque, idempotent for the same FabricId and `NameHeld` naming a different one, and writes the
  fabric's row to the fabric store at Day 0. See [States](lifecycles/states.md#states). The identity
  is never written again; the record is rewritten whole with each fabric event, by the fabric-primary.
- **Every node holds it through gossip.** The fabric row lives in `topology-nodes` (§2.6), filled from gossip only;
  no node hydrates it from the broker. node-admin's spawn records (`spawned_meta`) carry the fabric each spawned node
  belongs to, as they carry its mesh.
- **Its status** is the fabric's, set by the fabric-primary as [States](lifecycles/states.md#states) defines.
- **`fabrics.get` answers from memory.** Any node-admin answers it from the fabric row and mesh rows it holds in
  `topology-nodes`, plus the fabric-primary's observed-down overlay (§2.7), with no store read: the fabric's id, its status, every mesh with its status, the meshes below
  ready, and the fabric-primary node-admin.

<a id="3-the-three-gossip-planes-and-two-non-wire-adjuncts"></a>
## 3. The three gossip planes (and a non-wire adjunct)

Lifecycle information moves on separate iroh-gossip planes that never share a topic. The planes,
their subscription, the JoinNode (op `0x1D`), topology and every gossiped kind are
[`gossip.md`](gossip.md).

<a id="31-primary-mesh-gossip--gossipenvelope"></a>
### 3.1 Primary mesh gossip

One topic per mesh; its frames, their authors, dedup and retention are
[gossip.md §3.1](gossip.md#31-frame-catalog).

### 3.2 The backbone — cross-mesh, node-admin-only

Who joins the backbone and who publishes on it are
[gossip.md §3](gossip.md#3-hierarchical-topology); a cross-mesh deletion is
[gossip.md §6.1](gossip.md#61-deletions-an-event-for-promptness-meshtopology-for-retention).

### 3.3 Non-wire adjuncts

- **node-admin's in-process event ring is internal-only** — a buffer for instant timeline
  rendering ahead of Jaeger ingestion latency, merged client-side with Jaeger-derived events,
  never broadcast.

### 3.4 Projection gossip, and the pull at entry

The projected kinds' topics, the projection frame and the pull a node makes at entry are [gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth).

## 4. The state machine

A node has TWO kinds of state, and they answer different questions:

- its **status** — the readiness on its durable row: starting, pending, ready,
  connections-deleting, connections-deleted, stopping, stopped, deleting and deleted, each set by its workflow as
  [States](lifecycles/states.md#states) and [node status](lifecycles/node-status.md#node-status-change)
  define;
- its **`NodeState`** — the liveness it self-publishes on its gossip digest (below).

A status change is written as a blind put of the row the mesh-primary holds, and only the mesh's
primary writes it: a row of a mesh this node-admin does not own (`MeshWriteGate::NotOwned`) is never
put.

`NodeState` (`rdm-core/src/digest.rs`), postcard-positional:

```
Joining → Alive ⇄ Overloaded          self-published (auto, every tick)
Alive ⇄ Degraded                      self-published (auto, every tick)
Alive → PendingReconnect              observer-inferred on staleness, or on a command never sent for a reachability reason (soft, reversible)
*     → Leaving                       set by the mesh-primary on node.stopped; published as NodeStopped
*     → Dead                          observer-inferred ONLY (never sent)
```

A node's `NodeState` is self-published from what the node measures, inferred by an observer from
absence, or set from its mesh-primary's lifecycle overlay (`NodeStopped`).

- **Self-publication** recomputes state each gossip tick: `Joining` until the node has published
  once and is self-ready; `Overloaded` when over its CPU or RAM budget; `Degraded` when it reaches a
  broker only through a peer or node-admin (§8); else `Alive`. The node never publishes its own
  departure: a stopping node gossips nothing, and its mesh-primary publishes `NodeStopped` from the
  stop stream's `node.stopped` frame.
- **Shutdown is signal-driven.** SIGINT and SIGTERM run the node's own stop steps
  ([node stop](lifecycles/node-stop.md), the signal stop). No file poll, no timer.

### 4.1 State effects

| state | produced by | read by |
|---|---|---|
| `Joining` | self-published until published-once ∧ self-ready | the traffic/topology filters — routed no traffic |
| `Alive` | self-published, default | the rebind path; every traffic and topology filter |
| `Overloaded` | self-published, over CPU or RAM budget | `nodes.get`; the span reason |
| `Degraded` | self-published while the node reaches a peer only through a forward (§8) | routing's peer choice, which prefers a source that is not `Degraded`; the span reason naming each such broker |
| `Leaving` | set by the mesh-primary on `node.stopped` (overlay `NodeStopped`) | the routing rule: routed no traffic |
| `Dead` | never sent; receiver-side vocabulary | the traffic/topology filters |
| `PendingReconnect` | the staleness sweep; the mesh-primary, at once, for a member a command was never sent to for a reachability reason ([node recovery](lifecycles/node-recovery.md) step 1) | the mesh-primary's recovery tick ([node recovery](lifecycles/node-recovery.md)) |

Every state in that table has a reader. That is the bar a state has to clear to exist.

### 4.2 Two state models, and they do not meet

`NodeState` (`rdm-core/src/digest.rs`) and a node's status (its row, §1 rule 2) are separate state
families that never share an enum or a type; an application's own entity lifecycle is a third, the application's.

### 4.3 A returning node's catch-up is the application's, never a node status

A node has no catch-up status. What an application copies to a returning node of its own kind runs after the node is ready, with the application's own routing keeping clients off it, and
grants no readiness of its own (Q69). Its admission is the [Join schema](node-rpc-envelope.md#join-op-0x1d-rdm).

- **The mesh-primary is the one writer of its mesh's node rows**, its own
  included. A create's name claim and row are written by it when the Build reaches it, before
  anything is launched; an update goes caller → the node's mesh-primary (Forward `0x1A` when not
  directly reachable). The fabric-primary writes a row only as acting primary of a mesh with no
  primary. No other process writes a `Node` row.

### 4.4 Fabric, mesh and node readiness

Fabric and mesh states, the checklists and traffic opening are
[States](lifecycles/states.md#states); a node's own readiness is §5.

## 5. Readiness — when a node takes traffic

Readiness is the end of [node start](lifecycles/node-start.md), reported by the node's
`node.started` and set by its mesh-primary as Ready on that report.

1. **The gates are the start's.** The join (its cert and rafka-time, the mesh channel joined), the
   application's [`pre_node_ready` hook](fabric-node-hooks.md) returning Ok, complete topology
   installed, the required Build floor held, rafka-time refreshed last. Nothing else makes a node
   ready; a partial snapshot or a Blocked hook makes none.
2. **The application's one contribution is the hook.** Whatever an application must hold before it
   can serve
   it does inside `pre_node_ready`, which it may answer Ok, Blocked with a retry event, or Failed by
   name ([Outcomes and the gate](fabric-node-hooks.md#outcomes-and-the-gate)). A node with nothing to
   hold is trivially ready.
3. **The node reports `node.started` once every gate holds** — not on listener-bind, not on a
   timeout; its mesh-primary sets Ready on that report.
4. **Failure surfaces; nothing times out.** A failed hook fails the start by name
   (`Failed { step, reason }`); a blocked one answers `Blocked` until its retry event. The node stays
   pending. There is no timeout fallback: a flip that can fire with nothing held means nothing.
5. **No surface is readier than another.** A node is ready or it is not; an application that
   served on one surface before the report would be a defect.

**There is exactly ONE readiness mechanism**: the start's gates and the `node.started` report; a
second path to Ready is a defect.

- **Internal node work begins when that node itself is ready**; node, mesh and fabric
  readiness are separate scopes ([States](lifecycles/states.md#states)), and what a start completes
  is [node start](lifecycles/node-start.md). An application's internal workers wait on their own node's
  readiness and nothing else; once open it stays open, and a later status change stops no work
  already started.
  - **node-admin does not admit an incomplete birth.** Missing prerequisite topology or
    unreadable storage prevents completion with the schema's typed readiness refusal. A fabric
    of one mesh does not wait for a peer mesh that does not exist. Admission, not a socket or
    an unanswered stream, establishes completion.

### 5.x Readiness has separate scopes

Node, mesh and fabric readiness are separate scopes, defined in
[States](lifecycles/states.md#states); none substitutes for another.

## 6. Departure

Departure removes a node from the ROUTING view. It does not release anything the node owns: a
node keeps its identity, its replica-set membership and its data across every class below. Only
node.delete releases its identity and name; its storage stays with the spec (§7.3).

All removal from the view is the topology view's (`rdm-transport/src/topology.rs`): a member
unheard past the staleness floor is held, not current (`rdm.mesh.topology.update.via-member-stale`);
a stopped birth leaves the view at `NodeStopped` (`rdm.mesh.topology.remove.via-node-stopped`); a
deleted birth leaves at `NodeDeleted` and its id is held deleted for the retention
([gossip.md §6.1](gossip.md#61-deletions-an-event-for-promptness-meshtopology-for-retention)). A member
whose silence a lifecycle event the view applied explains is expected silence, neither stale nor
counted ([gossip.md §6](gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours), Q58).

| Class | Trigger | Latency |
|---|---|---|
| **Stopped** | the mesh-primary's `NodeStopped` overlay, from node.stop's `node.stopped` | on receipt |
| **Stale** (observer) | unheard past the staleness floor (local receive-time, clock-skew-safe): held, not current | the staleness floor |
| **Deleted** | the executor's `NodeDeleting`, then `NodeDeleted` after exact provider proof | on receipt |
| **Re-born** | the same `path.name` under a new node.id ([node recovery](lifecycles/node-recovery.md)): the predecessor is deleted by the recovery Build, so the projection holds one entry per `path.name` | on receipt |

The "Stale" row above is **local routing/view eviction only**. They do not publish a deletion, do not make ExactNode resolve Gone, and do not license destructive Build recovery. A retained Gone deletion is produced only by canonical exact deletion proof, published as `NodeDeleted` by the Mesh executor that holds the operation and retained in `MeshTopology.deleted`, as defined by [gossip.md](gossip.md) §3.1 and §6.1.

A removal by [node.delete](lifecycles/node-delete.md), [mesh.delete](lifecycles/mesh-delete.md)
or [fabric.delete](lifecycles/fabric-delete.md) is its workflow's.

**A node's topology row — any type — is never RELEASED by a liveness gap or a same-identity
restart.** It goes `PendingReconnect` and stays in the projection's kind index,
because that index is what every reader resolves a node from: a row removed on a heartbeat gap
leaves a stored reference with no resolvable node. Only node.delete releases it, and then the row
stays with status deleted while the data dir stays.

### 6.9 The stopped-node garbage collector

<a id="stopped-node-garbage-collector"></a>
A stopped node is a live process with its connections cut ([node stop](lifecycles/node-stop.md)): present in every
topology, marked stopped, holding its name, its data and its endpoint, until its own start or
delete. Left alone it is garbage, and the fabric does not keep garbage forever. The garbage
collector is the one clock in the lifecycle, and it decides nothing about liveness: it only
selects candidates and submits ordinary Builds.

| Field | Contract |
|---|---|
| Setting | `stopped_node_ttl` (`RDM_STOPPED_NODE_TTL`), read by the fabric-primary only. Blank means forever: no sweep, a stopped node stays until its own start or delete. RDM's shipped config sets 30 minutes; Rafka's deployment sets 1 hour |
| Who | The fabric-primary, the sole Build driver. On a fabric-primary handover the successor sweeps from the same durable receipts |
| Cadence | A sweep every ttl. A node is eligible after one ttl and is normally collected between one and two ttl, plus Build contention |
| Age | The current stopped period's own stop receipt in the Build log, never an earlier stop before a later start. A birth whose receipt cannot be read is skipped by name, never treated as expired |
| Selection | Every node stopped longer than the ttl, except a birth an open Build still references (a replace mid-transfer keeps its rollback point) |
| Action | One ordinary node.delete Build per selected node, one at a time: one topology Build runs at a time, so the other candidates wait for the next sweep; a `BuildInProgress` refusal loses nothing, the candidate is seen again. The sweep itself deletes nothing |
| Recheck | The stopped and open-Build checks run again at the delete Build's admission, serialized with it: a node started or referenced by a new Build after the scan is refused there, never deleted from a stale decision |
| Effect | The delete's own: row deleted, name released, NodeDeleted in the mesh topology, deleted retention, Gone |
| Wire | Nothing. No frame gains a field; the age lives in the receipt |
| Span | `rdm.node_admin.node.delete.via-stopped-ttl` with node, stopped_at, ttl, build_id |

## 7. node-admin — who drives a node's lifecycle

node-admin is a full mesh participant (`Role::Admin`) and a backbone subscriber. Its launch and
start modes are [node-admin-lifecycle.md](node-admin-lifecycle.md), whether it is a primary is
[elections](fabric-node-lifecycle-elections.md), the frames it publishes are
[gossip.md §3.1](gossip.md#31-frame-catalog), and the node workflows it executes as a Build step are
the [typed node objects](node-rpc-envelope.md#typed-object-contract).

**Attribution rule:** `rdm.mesh.*` and `rdm.node.*` spans are the node runtime's, emitted inside
whichever process runs it, node-admin's own included. `rdm.node_admin.*` spans are node-admin's
orchestration layer.

### 7.1 One node-admin; meshes are channels

There is one node-admin: the `rdm-node-admin` binary, joined to its mesh as `Role::Admin`.
There is no supervisor and no observer role. A mesh is an iroh gossip channel on one network, so any
node can reach any node. The fabric-primary accepts and drives every Build and dispatches each run
to `executor_for`'s executor, in its own mesh or another
([Build carrier](node-rpc-envelope.md#build-op-0x20-rdm),
[build.md](build.md)). Every node is an independent process — nothing
binds it to the admin's life — so a fleet keeps running through its admin's replacement, as it
would in its own container. A test suite tears a mesh down by the tag it created the mesh with
([mesh delete](lifecycles/mesh-delete.md)).

<a id="72-node-admins-own-lifecycle-birth-scale-replace-reap"></a>
### 7.2 node-admin's own lifecycle: create, scale, replace, delete

A node-admin is created, stopped, started, replaced and deleted like any node
([node create](lifecycles/node-create.md), [node stop](lifecycles/node-stop.md), [node start](lifecycles/node-start.md),
[node replace](lifecycles/node-replace.md), [node delete](lifecycles/node-delete.md)); its launch,
start modes and replacement rules are [node-admin-lifecycle.md](node-admin-lifecycle.md), and its
the fabric-primary moves as [elections §4.4](fabric-node-lifecycle-elections.md#44-explicit-committed-fabric-handover)
defines.

- **The shape is the accepted Build's.** A mesh may run N node-admins for HA; scaling up never
  implies a paired deletion.
- **Every mesh always has a LISTENING node-admin**: ready and holding a live backbone
  mesh topology it RECEIVED from every other mesh. No delete, stop or
  replace leaves a mesh without one; the separate node.delete of a mesh's last node-admin after
  mesh.deleted is the one exception ([mesh delete](lifecycles/mesh-delete.md)).
- **A node-admin executable replacement replaces a non-primary admin first**, so the mesh keeps another listening
  admin before the current primary goes; the next primary is whatever the election resolves.
- **A created node-admin adopts, it does not mint.** Only the bootstrap admin mints the fabric and
  its mesh; every later node-admin takes its mesh's held state from its maker's entry answer (Q56)
  and pulls the current projections on entry. A second admin that mints a divergent fabric is a defect.
- **A node-admin never writes its own row as the subject**; its mesh-primary writes every row of the
  mesh, its own included (§2.7).

<a id="73-release--the-one-act-that-lets-go-of-a-stateful-nodes-data"></a>
### 7.3 Delete releases identity; storage stays

**Observed death means UNAVAILABLE, not gone** (§6): a broker or a compute that stops being
reachable serves again on return under its own identity. Only
[node.delete](lifecycles/node-delete.md) releases a node's identity and name, never its storage.
The offline verdict writes no application work of its own; what an application does on a node's
release is the application's.
A silent member is its mesh-primary's
[node recovery](lifecycles/node-recovery.md); a peer mesh is the fabric-primary's
[mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation).

**What a mesh-primary records of a silent member** is on the node's record: the `PendingReconnect`
and `Dead` marks ([States](lifecycles/states.md#states), node row silence marks), the
`data-dir-lost` and `data-dir-unreadable` facts ([node recovery](lifecycles/node-recovery.md),
[deployment provider](deployment-provider.md) §2.5), each set and cleared by the mesh-primary on the
record it holds and written whole and revalidated by a successor's own
inspection. An application's own record of an outage and the work it owes on a return (Rafka's
`NodeOfflineRecord` and its refill job) is the application's. A deleted node's identity never takes
its name back: a digest from a deleted incarnation is refused by name as deleted and by the
peer-location view ([gossip.md §3.1](gossip.md#31-frame-catalog)); its data dir stays.

**A replace across a STORAGE-FORMAT move is not a replace.** Preserved data under
new-format binaries refuses at the record header's checksum on the first read — the correct
failure, and the reason a durable format move takes the mesh down and deploys it again rather
than replacing members in place.

### 7.4 The fabric-primary writes topology

The fabric-primary and each mesh-primary are [elections](fabric-node-lifecycle-elections.md); the
elected fabric-primary alone accepts a Fabric topology change
([build.md](build.md)).

## 8. Connections are a kept log, and routing reads them

Node-to-node connections, their durable log, indexes and proxy routes are
[connections.md](connections.md). Which node serves an application operation and how the caller
reaches it is the application's routing;
a member that is actually down is its mesh-primary's [node recovery](lifecycles/node-recovery.md).

**The calling node stays in service, and it is `Degraded` while a peer is reached only through a
forward**. A forward marks the peer, and the node publishes `Degraded` with a reason naming each
such peer; the peer's next answered direct dial clears it, and the node returns to `Alive` when none
is left. A `Degraded` node stays `ready`; the CPU/RAM over-budget state is `Overloaded`.

## 9. The event taxonomy

Design rule: **every lifecycle transition is observable as a span**. Span names, the verb
whitelist and the `via-`/`from-` reasons are the [runtime spans](node-rpc-envelope.md#runtime-spans);
the generated reference of every span name and its source line is `docs/reference/rdm/spans.md`.

**The node runtime** — `rdm.mesh.*` and `rdm.node.*`: the topology view, the topology projection,
connections and the node's own lifecycle steps, each named on its lifecycle page.

**node-admin** — component `node_admin`, entity `node`: every primitive and workflow step emits
`rdm.node_admin.node.<verb>.via-…`; each [lifecycle page](fabric-mesh-node-lifecycles.md) lists its
step spans.

Four rules the taxonomy enforces:

1. **The component is a node kind or a shared crate.** There
   is no `ui` component: a span named for the surface that renders it is named for its reader
   instead of its emitter.
2. **The verb is on the whitelist, the reason carries the discriminator, and every span name is a
   literal** ([runtime spans](node-rpc-envelope.md#runtime-spans)).
3. **A span that names an ATTEMPT must not read as a success.** An outcome belongs in an
   `outcome` attribute or in the reason, never in a name that is silent about which way it went.
4. **A span, a log line or an audit row that names a node names it by its ID and its `name`,
   never by its `endpoint_id`.** Logs are a public interface (§2); the
   routing key stays inside the transport. **The id attribute is the SAME prefixed, dashed
   external form an API answer renders** (`brk_abcd-1234-5678`, §2.0), never the bare minted id and
   never a hand-assembled prefix: logs use the same renderer as the API. The `endpoint_id`
   stays on a span ONLY where the span's subject IS the routing key itself (a dial's target, the
   handshake, the identity load/mint before an id exists to log).

## Cross-references

- **Gossip — delivery, topics, topology, the JoinNode (op `0x1D`), every gossiped kind:** [`gossip.md`](gossip.md)
