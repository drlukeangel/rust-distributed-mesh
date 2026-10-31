# Query: reading the fabric by property

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node properties](node-rpc-envelope.md#node-properties) · [Gossip](gossip.md)

This page owns how anything reads the fabric: the filter, its matching rules, and the two places a
query is answered (Q167, Q232). A query is answered from a primary's Atlas, or by the node
itself from the topology it holds. Both take the same filter and match it with the same rules.

## 1. External: a query to the fabric-primary

A caller outside the node sends `fabric.get`, `mesh.get` and `node.get` to the fabric-primary,
which answers from its atlas: every
mesh's node records, held in memory, filled from the node records once at its start or hand-over and
written by each op after that (Q203, Q204, Q224). Only a primary keeps an atlas (Q224): a mesh-primary's
holds its own mesh and nothing else, the fabric-primary's holds every mesh, and a node-admin that is
not a primary keeps none. A node that is not a primary, an rpc node or a node-admin that is neither primary, carries a
read it receives to its own mesh-primary and never to the fabric-primary (Q243). A mesh-primary answers a read of its own mesh from its atlas and carries any
other read up to the fabric-primary. A read makes no store read: the filter is matched
against the atlas, and a nodes store that fails every read leaves the answer unchanged.

A node reads another node's row from the nodes storage itself (Q237, Luke): a point read by `id` or
by `name`, the store opened on the node's first read and never at its start, join or create, where
the hydration sync from its mesh-primary gives it everything it needs (Q227, Q234). The row is the
record: the mesh-primary writes it and its atlas in one step, so the store is never behind an atlas,
and the read sees a node's own update as soon as its mesh-primary has written it. A scan of the store
is never a node's. The store's reader must not meet a line still being written (#3222).

One read per entity, each answering a list in a stable order: `fabric.get`, `mesh.get`,
`node.get`, `node.connection.get`. A filter is a list of `[property, [values…]]` pairs.

```text
node.get  [["kind", ["gateway", "broker"]]]                       any of the values in one pair
node.get  [["mesh", ["mesh1"]], ["kind", ["gateway"]]]            pairs are AND
node.get  [["kind", ["admin"]], ["is_primary", ["true"]]]         every mesh-primary
node.get  [["is_fabric_primary", ["true"]]]
node.get  [["tags", ["red"]], ["tags", ["eu"]]]                   a repeated property is AND (all of)
node.get  [["config.region", ["eu-west"]], ["status", ["ready"]]]
node.get  [["id", ["<node id>"]]]                                 one node
node.get  []                                                      no filter: the fabric's topology
node.get  [["!status", ["dead", "deleted"]]]                      every current node: a negated pair
mesh.get [["name", ["mesh1"]]]
node.connection.get { node: (<id>, <incarnation>) } [["state", ["connected"]]]
```

From a node:

```text
node.node_row(id | name)                                           one row, from the nodes storage (Q237)
```

The matching rules:

- **Property:** any public field of the row by its page name ([Node properties](node-rpc-envelope.md#node-properties)
  for a node; `MeshView`, `FabricView` and `ConnectionView` for the others).
  `config.<key>` reads into the application's config map; the key is everything after the first dot.
- **Values:** strings. A boolean is `true` or `false`, an enum its page name (`ready`,
  `gateway`), a number its digits, an id or a name its canonical text.
- **Match:** a set field (`tags`) matches when it contains the value; a string field matches exactly;
  `""` matches an empty string, never an absent one. A row without a property the schema has (a
  config key) does not match.
- **Negate:** a property written `!<property>` negates its pair (Q306): the row matches when the
  property is none of the values, a set field when it contains none of them, and a row without the
  property matches. It ANDs with the other pairs and is refused as its positive pair is.
- **Refused:** an unknown property, a value the field cannot take, an empty values list or a field that
  is not public is `INVALID_ARGUMENT`, naming the pair.
- **Nothing matching** is `[]`, a mesh the fabric-primary does not hold included. Every matching row is
  answered, streamed in frames on [op 0x23](node-rpc-envelope.md#read-op-0x23-rdm) (Q285); a row that fits no
  frame alone is `ReadOverBound` (`RESOURCE_EXHAUSTED`) naming it, and the read fails.
- **Status:** a node's `status` is its row's as written: the state the row last took from the node's
  frame or the mesh-primary's receipt ([states.md](lifecycles/states.md#who-sets-each-state)). A
  command the node has not acknowledged moves it nowhere: a stop sent and not yet acknowledged reads
  `ready`. The fabric-primary holds its own mesh's rows as its writer writes them; another
  mesh's operation states (stopping through deleted) reach its atlas with the operation, its receipt or
  the fill at its attempt's completion, and until each mesh-primary feeds the fabric-primary's atlas
  another mesh's node answers the topology's status for its create-time progress (starting, pending,
  ready). Silence (`pending-reconnect`, `dead`) is observer-inferred
  ([node identity and state](node-identity-and-state.md)), answered while the row holds the node live.
- **`node.connection.get`:** answered by the exact birth from its own connections log, not from the atlas:
  the node (id and incarnation) is the read's address, outside the filter; the filter matches that
  node's connection rows (`id`, `destination`, `kind`, `state`).

## 2. Internal: the node itself

A node holds its own topology: the latest gossiped digest of every node it hears, the primaries and
the mesh and fabric statuses ([gossip.md](gossip.md)). It queries that view with the same filter and
the same matching rules, locally, with no call:

```text
topology_view.nodes_get([["kind", ["broker"]], ["status", ["ready"]]])   the ready brokers I hear
topology_view.nodes_get([["mesh", [my mesh]]])                                       my mesh as I hold it
```

- **Properties:** only what gossip carries: `id`, `incarnation`, `name`, `path`, `node_type`,
  `kind`, `mesh`, `status`, the endpoint (`endpoint`, `transport_addr`) and the primaries
  (`is_primary`, `is_fabric_primary`). Gossip carries what routing needs and nothing else (Q203).
- **Refused:** a row-only property (`tags`, `config.<key>`, `executable`, …) is `INVALID_ARGUMENT` by
  name, naming `node.get`, whatever rows would have matched; the local view never answers a row-only
  property as no match. A pair the node schema refuses is refused as in §1.
- **Freshness:** best effort, as of the last digest heard; a node silent past the staleness floor
  (`RDM_STALENESS_MS`) leaves the view. The record is the external query (§1).
