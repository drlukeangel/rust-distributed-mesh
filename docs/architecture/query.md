# Query: reading the fabric by property

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node properties](node-rpc-envelope.md#node-properties) · [Gossip](gossip.md)

This page owns the shared filter and its matching rules. External node reads use the shared
records; internal topology queries use the local gossip view. These surfaces have different
freshness and available properties.

## 1. External: a query to the fabric-primary

The existing command route remains: a non-primary forwards a read to its mesh-primary, which
serves its own scope or forwards to the fabric-primary. The fabric-primary's own Build, accepted
topology and in-flight facts remain its control state. Node rows and property maps are not copied
into an atlas to serve `node.get` or acceptance.

A current-pair lookup resolves the path and name to a node ID from the node records. The reader reads the node's record
at that node ID: one record per node, append-only, its latest line current, the node's own properties
and the status and role flags its mesh-primary writes in the same record (Q323). A node that has not
written yet has no properties yet. A raw last-written-path index supplies no fallback. Record read
errors remain named errors, not empty results or fabricated lifecycle status.

`node.get` materializes records and applies its complete filter in stable pair/ID order. Historical
records, including Deleted nodes, remain queryable by their own IDs and matching properties; a
history query does not redirect to the current pair's claimant. A direct own-property read may
preserve retained state during startup. Ordinary startup receives its lifecycle/topology context
through the existing startup operations.

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
node.node_row(id | { path, name })                                one resolved node, from the shared records (Q237)
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
- **Status:** the node record's latest status, declarations and committed operation
  effects. Sending an unacknowledged command moves nothing. Silence is the existing observer-inferred
  fact, never evidence of exit. Gossip does not make a record current or complete an operation.
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
