# Consumer guide: an application on RDM

One index for an application built on RDM. Each line answers one "how do I…"
and links the page that owns it. The pages are the contract; this guide only points at them. Read
the linked page before asking: every answer here was already on one.

## Starting and wiring

| How do I… | Where |
|---|---|
| compose the node-admin and my node kinds (my seams, hooks, app protocols, storage adapters) | [build.md, Node-admin composition and customer seams](../architecture/build.md#node-admin-composition-and-customer-seams) |
| start a fabric (Day 0) and give it its root | [fabric-certs.md, How RDM is instantiated](../architecture/fabric-certs.md#how-rdm-is-instantiated); `RDM_CA_SECRET` in [Configuration](../reference/rdm/config.md) |
| know every environment variable a node reads | [Configuration](../reference/rdm/config.md) (no other configuration mechanism exists) |
| hand a node a secret | `RDM_DECLARED_SECRETS` in [build-deployment.md §3](../architecture/build-deployment.md) and [Configuration](../reference/rdm/config.md) (Q150): declared by name with the kinds it is set on; never in a row, a span or a record |
| pass a variable through to every node (`RUST_LOG`, `RUST_BACKTRACE`, …) | RDM's fixed pass-through list (`crates/rdm-admin/src/admin/records.rs` `PASSTHROUGH`); under the process provider the host environment also reaches the child, minus `RDM_*`, the trace context and declared names |
| bind my listeners (REST, Kafka, …) | `RDM_LISTENERS` in [Configuration](../reference/rdm/config.md): one per listener my wiring declares on the kind, port 0, the bound port reported at the join and published on the node row |
| run on processes or containers | `MESH_SPAWN_TYPE`; [deployment-provider.md §2.6–2.7](../architecture/deployment-provider.md#26-the-process-provider) |

## Changing the topology

| How do I… | Where |
|---|---|
| create, resize or reshape meshes and nodes | the Build: [build.md §1–2](../architecture/build.md#1-build-is-the-only-topology-mutation-engine), the wire on [node-rpc-envelope.md, Build op 0x20](../architecture/node-rpc-envelope.md#build-op-0x20-rdm) |
| name the executable each node runs, and replace it | [build-deployment.md](../architecture/build-deployment.md) |
| move RDM's records to my own backend, read where they go, return to a previous one | [topology storage provider how-to](../features/topology-storage-provider/how-to.md); the contract on [storage.md §2](../architecture/storage.md#2-topology_storage_provider-where-rdms-records-go) |
| know when the fabric is usable | [states.md](../architecture/lifecycles/states.md): pending → ready; a Build that changes the fabric is Complete only after its own ready (Q154) |
| stop, start, replace or delete one node | the lifecycle pages under [lifecycles/](../architecture/fabric-mesh-node-lifecycles.md) (`node-stop.md`, `node-start.md`, `node-replace.md`, `node-delete.md`) |
| delete the whole fabric | [fabric-delete.md](../architecture/lifecycles/fabric-delete.md): wait for the fabric.delete stream's `Complete` (fabric.deleted), then `node.delete` of the fabric-primary to its `node.deleted`; the process then exits by itself. Kill nothing before that |

## Reading by property

Every read is a query by property, answered in one of two places ([query.md](../architecture/query.md)):

- **The fabric-primary** answers `fabric.get`, `mesh.get` and `node.get` from its atlas, with no
  store read: any property of the row, `config.<key>` and `tags` included; `node.connection.get` is the
  exact birth's own ([query.md §1](../architecture/query.md#1-external-a-query-to-the-fabric-primary)).
- **The node itself** answers `topology_view.nodes_get(filter)` from the topology it holds, locally,
  with no call: what gossip carries for every node it hears; a row-only property is refused naming
  `node.get` ([query.md §2](../architecture/query.md#2-internal-the-node-itself)).

The read calls follow one rule (Q167): one read per entity; the filter is a list of (property,
values) pairs, AND across pairs, any-of within, a repeated property AND; a property is any field of
the row by its page name, config entries as `config.<key>`; a list in a stable order, empty when
nothing matches.

| Read | Address | Properties a pair may name | Answers, in order | Served by |
|---|---|---|---|---|
| `fabric.get` | — | `id`, `name`, `status`, `provider`, `fabric_primary`, `admin_api_base`, `build_id`, `meshes`, `topology_storage_provider` | the fabric the admin's view holds: id, name, state, provider, fabric-primary, meshes | the fabric-primary |
| `mesh.get` | — | `id`, `name`, `status`, `primary_admin`, `admin_api_base`, `nodes` | meshes, by name | the fabric-primary |
| `node.get` | — | every field of the node-properties table: `id`, `name`, `mesh`, `kind`, `status`, `tags`, `config.<key>`, `types`, `is_primary`, `is_fabric_primary`, `executable`, `stopped`, … | node rows by `path.name`, each with status, ports, config, tags and executable; no pair: every node of the fabric, its topology | the fabric-primary, from its atlas |
| `node.connection.get` | `node` (id, incarnation), required | `id`, `source`, `destination`, `kind`, `state`, `carrier`, `reason`, `logged_at_ms` | the exact birth's own connections from its connections log | the exact birth |

Written as the pair list the wire carries:

```text
node.get  [["config.zone", ["eu-1"]]]                           a config entry, by its key
node.get  [["tags", ["gold"]], ["tags", ["ssd"]]]               all of; one pair is any of
node.get  [["tags", ["gold", "silver"]]]                         any of
node.get  [["mesh", ["mesh1"]], ["kind", ["gateway"]]]           AND across pairs
node.get  [["kind", ["admin"]], ["is_primary", ["true"]]]        every mesh-primary
node.get  [["id", ["<node id>"]]]                                 "one node"
node.get  [["path", ["mesh1.broker"]], ["name", ["1"]]]         one node by its path.name (mesh1.broker.1): `path` is
                                                                   the part before the last dot, `name` the part after it
node.get  []                                                      the fabric's topology
mesh.get [["name", ["mesh1"]]]
node.connection.get { node: (<id>, <incarnation>) } [["state", ["connected"]]]
```

The same pair list, side by side, from both places:

```text
node.get                   [["kind", ["broker"]], ["status", ["ready"]]]   the fabric-primary's atlas: the rows
topology_view.nodes_get     [["kind", ["broker"]], ["status", ["ready"]]]   my own topology: the digests I hear
topology_view.nodes_get     [["config.zone", ["eu-1"]]]                                  refused: config is the row's, read it with node.get
```

A pair naming a property the row has not (`zone` without `config.`), a value the field cannot take
(`is_primary: yes`) or no value is refused `INVALID_ARGUMENT` naming the pair; a mesh the admin does
not hold is the empty list.

A read past the 64 KiB reply bound is refused ReadOverBound (RESOURCE_EXHAUSTED); read by mesh, and a mesh past the bound by id in batches.

Pairs combine with AND, so [id: x][mesh: m] answers empty when x is absent or when x is in another mesh; to tell them apart, read [id: x] and look at the row's mesh.

The node row's fields: [node-rpc-envelope.md, Node properties](../architecture/node-rpc-envelope.md#node-properties).
`node.topology.get` is different: one node's held gossip view, for repair, not for callers.

## Where my data goes

| What | Where |
|---|---|
| small per-node settings | the node's `config` map: mine, RDM never reads it; replaced whole by `node.config.update`, a node's own (`config_update`) going up through its mesh-primary and a caller's down from the fabric-primary; a Build's `config` supersedes the node's current map, whoever set it, so read the node's current config (`node.get`) before composing a Build: every write is a PUT (Q236); at most 32 keys and 2048 bytes ([node-config-update.md](../architecture/lifecycles/node-config-update.md), [gossip.md §3.1](../architecture/gossip.md#31-frame-catalog)) |
| labels to select nodes by | `tags`: see [Tags](#tags) below |
| a node's durable data | under its `RDM_DATA_DIR`; carried across stop and start, recovery and replacement by its path's storage policy ([node-storage-runbook.md](node-storage-runbook.md)) |
| my rows and events | my own storage, and my events on gossip ([gossip.md §8](../architecture/gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)); RDM orders nothing of mine |
| RDM's own records on my storage | the storage adapters ([build.md §5](../architecture/build.md#5-buildstateadapter)): persistence only, RDM writes whole records |
| which backend RDM's records go to (`topology_storage_provider`), and registering mine | [topology storage provider overview](../features/topology-storage-provider/overview.md) |

## Tags

A tag is a label on a node (and on a mesh: `mesh.tag.*`, the same shape). It selects nodes; it
holds no value (values go in `config`). It is an annotation with no execution semantics: RDM
decides nothing on a tag, and the provider reads none
([deployment-provider.md §4](../architecture/deployment-provider.md#4-storage-policy-and-the-data-dir)).
The contract is the `node.tag.get/create/delete` row on [node-rpc-envelope.md](../architecture/node-rpc-envelope.md#typed-objects-mapped-to-wire-and-execution-owners)
and the `tags` row of [Node properties](../architecture/node-rpc-envelope.md#node-properties).

| How do I… | What happens |
|---|---|
| tag a node when it is created | put the labels on its path in the Build; a change of a path's tags alone is no resize, and the fabric stays ready |
| add labels | `node.tag.create { label \| [labels] }`: one label or a list; a blind put per label, so a label the node already carries is reported `ALREADY_EXISTS` for that label and nothing is duplicated |
| remove labels | `node.tag.delete { All \| [labels] }`: every label or the named ones; a label the node does not carry is reported `NOT_FOUND` for that label |
| read a node's labels | the `tags` field of its node row (`node.get`) |
| select nodes by label | `node.get [tags: a, b]` for any of the labels, `[tags: a][tags: b]` for all of them, with `[mesh: m]` for one mesh's |
| keep labels across a re-birth | nothing to do: tags are inherited with the config at the same path.name on recovery and on replacement |

A label is 1–64 characters of `a-z 0-9 - _ . ~`, with no leading `.`, and a node carries at most 32.
A label outside that grammar, or one past the 32, is refused by name (`INVALID_ARGUMENT`) and
nothing is written. Tags are not config: `node.config.update` never touches them, and they do not
count against the config cap.

## My work inside a lifecycle

| Hook | Where |
|---|---|
| before a node is ready (`pre_node_ready`; `inherited` says whether the data dir was carried) | [fabric-node-hooks.md, pre_node_ready](../architecture/fabric-node-hooks.md#pre_node_ready) |
| after it is ready | [on_node_ready](../architecture/fabric-node-hooks.md#on_node_ready) |
| traffic opening (and my Day-0 work, which is my own) | [on_fabric_ready](../architecture/fabric-node-hooks.md#on_fabric_ready) |
| the clock | [rafka-time](../architecture/fabric-node-hooks.md#rafka-time) |

## Talking between nodes

| How do I… | Where |
|---|---|
| call a node | Node RPC: [node-rpc-envelope.md](../architecture/node-rpc-envelope.md); outcomes and codes on [node-rpc.md](../architecture/node-rpc.md) |
| add my own Node RPC ops (declare, reserve in the op ledger, serve, call) | [consumer-guide-ops.md](consumer-guide-ops.md) |
| open a stream on my own app protocol | the node's connections handle for my ALPN; routes, reconnect and proxies on [connections.md](../architecture/connections.md); a node's member cert admits it ([fabric-certs.md](../architecture/fabric-certs.md)) |
| publish and receive my events | gossip's envelope ([gossip.md §3.4](../architecture/gossip.md#34-the-envelope)) |

## When something breaks

| What | Where |
|---|---|
| following a call, a Build or a birth across processes | [telemetry.md](../architecture/telemetry.md) |
| no node-admin is alive: bring the fabric-primary back | [fabric-recovery-runbook.md](fabric-recovery-runbook.md) |
| a node's data dir is gone or broken | [provider-runbook.md](provider-runbook.md) |
| a stateful node's data across stop and start, recovery, replacement and delete | [node-storage-runbook.md](node-storage-runbook.md) |
| a storage change that stays blocked, fails before its switch, or meets a restart | [topology storage provider runbook](../features/topology-storage-provider/runbook.md) |
| a refusal | its code and named detail: [node-rpc-envelope.md, Runtime spans](../architecture/node-rpc-envelope.md#runtime-spans); every refusal names its reason |
