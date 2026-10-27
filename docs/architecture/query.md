# Query: reading the fabric by property

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node properties](node-rpc-envelope.md#node-properties) · [Gossip](gossip.md)

This page owns how anything reads the fabric: the filter, its matching rules, and the two places a
query is answered (Q167, Q232). A query is answered from the fabric-primary's Atlas, or by the node
itself from the topology it holds. Both take the same filter and match it with the same rules.

## 1. External: a query to the fabric-primary

A caller outside the node sends `fabrics.get`, `meshes.get` and `nodes.get` to the fabric-primary,
which answers from its atlas: every
mesh's node records, held in memory, filled from the node records once at its start or hand-over and
written by each op after that (Q203, Q204, Q224). A read makes no store read: the filter is matched
against the atlas, and a nodes store that fails every read leaves the answer unchanged.

One read per entity, each answering a list in a stable order: `fabrics.get`, `meshes.get`,
`nodes.get`, `connections.get`. A filter is a list of `[property, [values…]]` pairs.

```text
nodes.get  [["kind", ["gateway", "broker"]]]                       any of the values in one pair
nodes.get  [["mesh", ["mesh1"]], ["kind", ["gateway"]]]            pairs are AND
nodes.get  [["kind", ["admin"]], ["is_primary", ["true"]]]         every mesh-primary
nodes.get  [["is_fabric_primary", ["true"]]]
nodes.get  [["tags", ["red"]], ["tags", ["eu"]]]                   a repeated property is AND (all of)
nodes.get  [["config.region", ["eu-west"]], ["status", ["ready-for-traffic"]]]
nodes.get  [["id", ["<node id>"]]]                                 one node
nodes.get  []                                                      no filter: the fabric's topology
meshes.get [["name", ["mesh1"]]]
connections.get { node: (<id>, <incarnation>) } [["state", ["connected"]]]
```

The matching rules:

- **Property:** any public field of the row by its page name ([Node properties](node-rpc-envelope.md#node-properties)
  for a node; `MeshView`, `FabricView` and `ConnectionView` for the others).
  `config.<key>` reads into the application's config map; the key is everything after the first dot.
- **Values:** strings. A boolean is `true` or `false`, an enum its page name (`ready-for-traffic`,
  `gateway`), a number its digits, an id or a name its canonical text.
- **Match:** a set field (`tags`) matches when it contains the value; a string field matches exactly;
  `""` matches an empty string, never an absent one. A row without a property the schema has (a
  config key) does not match.
- **Refused:** an unknown property, a value the field cannot take, an empty values list or a field that
  is not public is `INVALID_ARGUMENT`, naming the pair.
- **Nothing matching** is `[]`, a mesh the fabric-primary does not hold included. An answer past the
  reply bound is `ReadOverBound` (`RESOURCE_EXHAUSTED`); the caller narrows, by mesh, then by `id`.
- **Status:** a node's `status` is its row's as written: the state the row last took from the node's
  frame or the mesh-primary's receipt ([states.md](lifecycles/states.md#who-sets-each-state)). A
  command the node has not acknowledged moves it nowhere: a stop sent and not yet acknowledged reads
  `ready-for-traffic`.
- **`connections.get`:** answered by the exact birth from its own connections log, not from the atlas:
  the node (id and incarnation) is the read's address, outside the filter; the filter matches that
  node's connection rows (`id`, `destination`, `kind`, `state`).

## 2. Internal: the node itself

A node holds its own topology: the latest gossiped digest of every node it hears, the primaries and
the mesh and fabric statuses ([gossip.md](gossip.md)). It queries that view with the same filter and
the same matching rules, locally, with no call:

```text
topology_view.nodes_get([["kind", ["broker"]], ["status", ["ready-for-traffic"]]])   the ready brokers I hear
topology_view.nodes_get([["mesh", [my mesh]]])                                       my mesh as I hold it
```

- **Properties:** only what gossip carries: `id`, `incarnation`, `name`, `path`, `node_type`,
  `kind`, `mesh`, `status`, the endpoint (`endpoint`, `transport_addr`) and the primaries
  (`is_primary`, `is_fabric_primary`). Gossip carries what routing needs and nothing else (Q203).
- **Refused:** a row-only property (`tags`, `config.<key>`, `executable`, …) is `INVALID_ARGUMENT` by
  name, naming `nodes.get`, whatever rows would have matched; the local view never answers a row-only
  property as no match. A pair the node schema refuses is refused as in §1.
- **Freshness:** best effort, as of the last digest heard; a node silent past the staleness floor
  (`RDM_STALENESS_MS`) leaves the view. The record is the external query (§1).
