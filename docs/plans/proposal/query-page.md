# Proposal: docs/architecture/query.md (Q167, Q232, Q233, Q234)

**Status:** proposal, 2026-10-27, for the architects' review and Luke's yes. Below the line is the page as it would land with #3234's code, then the open question Q233; after it, the consumer guide's Reading state section that points to it.

---

# Query: reading the fabric by property

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node properties](node-rpc-envelope.md#node-properties) · [Gossip](gossip.md)

This page owns how anything reads the fabric: the filter, its matching rules, and the two places a
query is answered (Q167, Q232, Q234). A query is answered from a primary's Atlas, or by the node
itself from the topology it holds. Both take the same filter.

## 1. External: a query to the primaries

A caller outside the node sends the read to the fabric-primary. Only a primary keeps an atlas
(Q224): a mesh-primary's holds its own mesh and nothing else, the fabric-primary's holds every mesh,
and a node-admin that is not a primary keeps none. The fabric-primary answers from its atlas; a
mesh-primary answers a read of its own mesh from its atlas and carries any other read up to the
fabric-primary.

A node reads another node's row from the nodes storage itself (Q237, Luke): a point read by `id` or
by `name`, the store opened on the node's first read and never at its start, join or create, where
the hydration sync from its mesh-primary gives it everything it needs (Q227, Q234). The row is the
record: the mesh-primary writes it and its atlas in one step, so the store is never behind an atlas,
and the read sees a node's own update as soon as its mesh-primary has written it. A node's filter
beyond `id` or `name`, "every broker in eu-1", is the primaries' query above, sent to the
mesh-primary its topology view names and carried up for another mesh: a scan of the store is never a
node's. The store's reader must not meet a line still being written (#3222 lands first).

One read per entity, each answering a list in a stable order: `fabrics.get`, `meshes.get`,
`nodes.get`, `connections.get`, `builds.get`. A filter is a list of `[property, [values…]]` pairs.

```text
nodes.get  [["kind", ["gateway", "broker"]]]                       any of the values in one pair
nodes.get  [["mesh", ["mesh1"]], ["kind", ["gateway"]]]            pairs are AND
nodes.get  [["kind", ["admin"]], ["is_primary", ["true"]]]         every mesh-primary
nodes.get  [["is_fabric_primary", ["true"]]]
nodes.get  [["tags", ["red"]], ["tags", ["eu"]]]                   a repeated property is AND (all of)
nodes.get  [["config.region", ["eu-west"]], ["status", ["ready"]]]
nodes.get  [["id", ["<node id>"]]]                                 one node
nodes.get  []                                                      no filter: the fabric's topology
meshes.get [["name", ["mesh1"]]]
builds.get [["current", ["true"]]]                                 the current Build
connections.get { node: (<id>, <incarnation>) } [["state", ["connected"]]]
```

From a node:

```text
node.node_row(id | name)                                           one row, from the nodes storage (Q237)
node.nodes_get([["mesh", [my mesh]], ["kind", ["broker"]]])        my mesh's brokers, from the mesh-primary's atlas
node.nodes_get([["config.zone", ["eu-1"]]])                        every node in eu-1, carried up to the fabric-primary
```

The matching rules:

- **Property:** any public field of the row by its page name ([Node properties](node-rpc-envelope.md#node-properties)
  for a node; `MeshView`, `FabricView`, `ConnectionView` and the Build header for the others).
  `config.<key>` reads into the application's config map; the key is everything after the first dot.
- **Values:** strings. A boolean is `true` or `false`, an enum its page name (`ready`, `gateway`), a
  number its digits, an id or a name its canonical text.
- **Match:** a set field (`tags`) matches when it contains the value; a string field matches exactly;
  `""` matches an empty string, never an absent one. A row without a property the schema has (a
  config key) does not match.
- **Refused:** an unknown property, a value the field cannot take, an empty values list or a field that
  is not public is `INVALID_ARGUMENT`, naming the pair.
- **Nothing matching** is `[]`, a mesh the fabric-primary does not hold included. An answer past the
  reply bound is `ReadOverBound` (`RESOURCE_EXHAUSTED`); the caller narrows, by mesh, then by `id`.
- **`connections.get`:** the node (id and incarnation) is the read's address, outside the filter; the
  filter matches that node's connection rows (`id`, `destination`, `kind`, `state`).

## 2. Internal: the node itself

A node holds its own topology: the latest gossiped digest of every node it hears, the primaries and
the mesh and fabric statuses ([gossip.md](gossip.md)). Its hydration sync, `node.topology.get` from its own
mesh-primary at join, start and create ([node.topology.get](lifecycles/node-topology-get.md)), carries
its own whole row from the mesh-primary's Atlas with the topology, held on its own entry and kept current by `ApplyNodeConfig`, the tag commands and `ApplyNodeName` (Q234). It
queries that view with the same filter and the same matching rules, locally, with no call:

```text
node.topology_view.nodes_get([["kind", ["broker"]], ["status", ["ready"]]])   the ready brokers I hear
node.topology_view.nodes_get([["mesh", [my mesh]]])                           my mesh as I hold it
node.topology_view.nodes_get([["id", [me]]])                                  my own whole row, config and tags included
```

- **Properties:** for every other node, only what gossip carries: `id`, `name`, `kind`, `mesh`,
  `status`, the endpoint and the primaries. Gossip carries what routing needs and nothing else
  (Q203). For the node's own entry, every property of its row, `config.<key>` and `tags` included.
- **Scope, decided before matching:** a local query is self-scoped only when its filter holds an `id`
  pair whose one value is the node's own id. A self-scoped query may name every property of the row.
  Any other local query may name only what gossip carries: a row-only property (`tags`,
  `config.<key>`, `executable`, …) in it is `INVALID_ARGUMENT` by name, naming `nodes.get`, whatever
  rows would have matched. The local view never answers a row-only property as no match.
- **Freshness:** best effort, as of the last digest heard; a node silent past the staleness floor
  (`RDM_STALENESS_MS`) leaves the view. The record is the external query (§1): the mesh-primary's
  atlas holds a node's map from its `Applied`; the fabric-primary's holds it once the mesh-primary's
  push is acknowledged, bounded by the owed delivery, and may lag by that. Every install is ordered by
  the row's revision (the config flow, P1).

## Open question for the architects: Q233, what a query answers as a node's status

### 1. Spec

- states.md "Who sets each state" (lines 23–35) lists as node states: starting, pending, ReadyForTraffic
  (line 27; a node's state is `ready`, Luke 2026-10-27; ready-for-traffic is the mesh's and the
  fabric's), connections-deleting, connections-deleted, leaving, left, stopping, stopped, deleting, deleted.
  Each is set by the node's mesh-primary from a frame.
- node-stop.md:22: "stopping from the stop's admission, then connections-deleting,
  connections-deleted, leaving and left from its primitives' frames, then stopped with a live
  process".
- node-stop.md:31–32: "only this admission makes the mesh-primary record stopping. A never-sent
  command writes no state."
- Luke, 2026-10-27: an external query is answered from the Atlas with the node's current state, never
  its in-flight state. A `node.stop` sent to `mesh2.rpc.1` leaves `mesh2.rpc.1.get` at `ready` until
  `mesh2.rpc.1` answers that it stopped.

### 2. Issue

Most of states.md's node states are an operation's intermediate responses, not the node's condition:
- starting, stopping and deleting come from the operation's admission;
- connections-deleting, connections-deleted, leaving and left come from the operation's primitives.

A query that answers them as status reports an operation's progress as the node's state.

### 3. Flame

None: a contract question.

### 4. Observations

- [code] The Atlas holds the topology and the in-flight operations of the meshes its node-admin is
  primary for (Q224, `rdm-admin/src/atlas.rs`). Node reads answer each node from the Atlas
  (02db3214f).
- [code] The node itself blocks new application work while its connections are being deleted, and
  refuses by name (states.md:28).
- [inferred] Whether today's Atlas read keeps the in-flight overlay out of the status it answers has
  not been checked.

### 5. Delta

The spec makes every operation response a node state, while an external query must answer the
node's current state only.

### 6. Arms

Every arm assumes a status changes only on the node's own report, and that a command the node has
not answered changes nothing.

- **A: the intermediate responses are not shown.** A node's status is one of pending, ready, stopped
  and deleted. A stop leaves the node `ready` until its stopped frame. Progress is read on the
  operation: its own stream, and the Build's operation receipts through `builds.get`. Changes:
  states.md's table splits into node statuses and operation progress; the Status rows of node-stop,
  node-start, node-delete, node-mesh-leave and node-connections-delete; CLAUDE.md's vocabulary line
  "the state is `connections-deleting`".
- **B: a status and a sub-status.** Status is pending, ready, stopped or deleted. A second field
  carries the operation's last reported step (stopping, connections-deleting, connections-deleted,
  leaving, left, deleting, starting) while the operation runs, and is empty otherwise. Changes: a
  field on the node row, the wire, every writer of a step, the pages' Status rows.
- **X: something else.**

dev recommendation: A. The progress is already on the operation's stream and the Build's receipts.
Routing does not need it in status, because the node refuses new work by name while its connections
are being deleted. A sub-status is a second field that every writer must keep in step with the stream.

**Ruled by Luke (2026-10-27, INDEX):** the node's acknowledgement of a command sets the "ing" state, its
completion the "ed"; each is the row's one status, never a sub-state; a query answers the state the node last
reported, and a command not acknowledged moves the row nowhere. states.md states it (landed).


---

## The consumer guide (docs/eng/consumer-guide.md), Reading state, replaced by

## Reading state

Every read is a query by property, answered in one of two places
([query.md](../architecture/query.md)):

- **The primaries** answer `fabrics.get`, `meshes.get`, `nodes.get`, `connections.get` and
  `builds.get` from their atlases (the mesh-primary for its own mesh, the fabric-primary for the rest): any property of the row, `config.<key>` and `tags` included. From
  inside a node it is `node.nodes_get(filter)`, sent to its mesh-primary and carried up for another
  mesh ([query.md §1](../architecture/query.md#1-external-a-query-to-the-primaries)).
- **The nodes storage** answers a node's read of one other node's row, by id or by name:
  `node.node_row(id | name)`, the record itself, opened on the first read and never at start
  ([query.md §1](../architecture/query.md#1-external-a-query-to-the-primaries), Q237).
- **The node itself** answers `node.topology_view.nodes_get(filter)` from the topology it holds,
  locally, with no call: what gossip carries for every node, and its own whole row, synced from its
  mesh-primary's Atlas at join, start and create
  ([query.md §2](../architecture/query.md#2-internal-the-node-itself)).

The node row's fields: [node-rpc-envelope.md, Node properties](../architecture/node-rpc-envelope.md#node-properties).

---

## Open question for the architects: Q237, where a node reads another node's row at runtime

### 1. Spec

- Q227 (Luke): no node reads anything from the store on create, start or rejoin. Every node pulls
  from its mesh-primary or the fabric-primary.
- Q203 (Luke): "never pull it from the nodes storage": a node read never reads the node store.
- Q224: only a primary keeps an Atlas. A node such as `gw1` keeps none: it holds its gossiped
  topology and, under Q234, its own row.
- The draft above, §1: a node's read of rows goes to its mesh-primary's Atlas, and is carried to the
  fabric-primary for another mesh.

### 2. Issue

`gw1` needs `mesh1.broker.1`'s config at runtime. Asking a primary costs a hop. Asking `broker1`
itself fails when `broker1` cannot be reached. Luke, 2026-10-27: the nodes storage is the source of
truth, so reading it is better, and this use case is worth it. Q227 rules only start, join and create.

### 3. Flame

None: a contract question.

### 4. Observations

- [code] The topology storage is a pluggable backend (`RDM_TOPOLOGY_STORAGE_PROVIDER`; the file
  store, Rafka's topics). A node reading it needs that backend's client and access.
- [code] The file backend has a point read by `path.name` (`node_at`, used by `rdm-admin/src/join.rs:353`)
  and the tags index. Any other property filter is a scan of the store.
- [code] A store read racing a writer meets a line still being written. That is #3222: `read_whole`'s
  refusal after 40 ms. Its fix is built on `i143-3222-store-reader`, which is not landed.
- [code] The mesh-primary writes the row and its Atlas in the same step, so the store is never
  fresher than the mesh-primary's Atlas, and is current for a node that cannot be reached.

### 5. Delta

Q203 keeps every node read off the nodes storage. The use case wants a node to read another node's
row from the store at runtime, where Q227 says nothing.

### 6. Arms

Every arm assumes no node reads the store at create, start or rejoin (Q227).

- **A: a node reads another node's row from the nodes storage at runtime.** It opens the store
  lazily on its first read, never at its start. Point reads are by `id` or `path.name`; a filter on
  any other property is a scan. Q203 narrows to the primaries' answers. #3222's fix lands first.
  Changes: a store handle on every node, the query page's §1 for a node, the store's read path.
- **B: a node asks its mesh-primary** (the draft as written). The mesh-primary answers from its Atlas
  for its own mesh and carries the read up for the rest. Changes: nothing beyond the draft.
- **C: a node asks the other node itself, then its mesh-primary when that node cannot be reached.**
  Changes: a node-served row read, and two remote paths.
- **X: something else.**

dev recommendation: B. It keeps the store as persistence only, and keeps every node off a pluggable
backend's client. It also keeps the file store's whole-file reads and #3222's race away from every
gateway. A is Luke's lean and works with #3222's fix landed. A filter beyond `id` and `path.name` is a
scan on the file backend.

**Ruled A (Luke, 2026-10-27, INDEX):** a node reads another node's row from the nodes storage directly at
runtime, a point read by id or name, the store opened on its first read and never at its start. Q203 and Q227
rule out the start, join and create reads only: the hydration sync from the mesh-primary makes them needless.
#3222's reader lands first. §1's "a node asking for rows it does not hold sends it to ... its mesh-primary" is
rewritten to this before the landing.
