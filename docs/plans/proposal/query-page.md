# Proposal: docs/architecture/query.md (Q167, Q232, Q233, Q234)

**Status:** proposal, 2026-10-27, for the architects' review and Luke's yes. Below the line is the page as it would land with #3234's code, then the open question Q233; after it, the consumer guide's Reading state section that points to it.

---

# Query: reading the fabric by property

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node properties](node-rpc-envelope.md#node-properties) · [Gossip](gossip.md)

This page owns how anything reads the fabric: the filter, its matching rules, and the two places a
query is answered (Q167, Q232, Q234). A query is answered from a primary's Atlas, or by the node
itself from the topology it holds. Both take the same filter.

## 1. External: a query to the primaries

A caller outside the node sends the read to the fabric-primary. A node asking for rows it does not
hold sends it to the node-admin it joined through, its mesh-primary, as it sends its own
`node.config.update`; the node never names the fabric-primary. Only a primary keeps an Atlas
(Q224): a mesh-primary's holds its own mesh and nothing else, the fabric-primary's holds every mesh,
and a node-admin that is not a primary keeps none. The mesh-primary answers a read of its own mesh
from its Atlas and carries any other read up to the fabric-primary. No read touches the store (Q227).

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

From a node, the same read:

```text
node.nodes_get([["id", [me]]])                                     my whole row
node.nodes_get([["mesh", [my mesh]], ["kind", ["broker"]]])        my mesh's brokers
node.nodes_get([["config.zone", ["eu-1"]]])                        every node in eu-1
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
- **Refused:** a property only rows carry (`tags`, `config.<key>`, `executable`, …), asked of any
  node but the node itself, is `INVALID_ARGUMENT` by name, naming `nodes.get`: the local view never
  answers it as no match.
- **Freshness:** best effort, as of the last digest heard; a node silent past the staleness floor
  (`RDM_STALENESS_MS`) leaves the view. The truth is the external query (§1); a node routes off this
  view and reads rows when it needs the record.

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


---

## The consumer guide (docs/eng/consumer-guide.md), Reading state, replaced by

## Reading state

Every read is a query by property, answered in one of two places
([query.md](../architecture/query.md)):

- **The primaries** answer `fabrics.get`, `meshes.get`, `nodes.get`, `connections.get` and
  `builds.get` from their Atlases (the mesh-primary for its own mesh, the fabric-primary for the rest): any property of the row, `config.<key>` and `tags` included. From
  inside a node it is `node.nodes_get(filter)`, carried up by the node-admin the node joined through
  ([query.md §1](../architecture/query.md#1-external-a-query-to-the-primaries)).
- **The node itself** answers `node.topology_view.nodes_get(filter)` from the topology it holds,
  locally, with no call: what gossip carries for every node, and its own whole row, synced from its
  mesh-primary's Atlas at join, start and create
  ([query.md §2](../architecture/query.md#2-internal-the-node-itself)).

The node row's fields: [node-rpc-envelope.md, Node properties](../architecture/node-rpc-envelope.md#node-properties).
