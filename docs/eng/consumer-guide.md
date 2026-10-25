# Consumer guide: an application on RDM

One index for an application built on RDM (Rafka is the first). Each line answers one "how do I…"
and links the page that owns it. The pages are the contract; this guide only points at them. Read
the linked page before asking: every answer here was already on one.

## Starting and wiring

| How do I… | Where |
|---|---|
| compose the node-admin and my node kinds (my seams, hooks, app protocols, storage adapters) | [build.md, Node-admin composition and customer seams](../architecture/build.md#node-admin-composition-and-customer-seams) |
| start a fabric (Day 0) and give it its root | [fabric-certs.md, How RDM is instantiated](../architecture/fabric-certs.md#how-rdm-is-instantiated); `RDM_CA_SECRET` in the env table (`CLAUDE.md`) |
| know every environment variable a node reads | the env table in `CLAUDE.md` (no other configuration mechanism exists) |
| hand a node a secret | `RDM_DECLARED_SECRETS` in the env table (Q150): declared by name with the kinds it is set on; never in a row, a span or a record |
| pass a variable through to every node (`RUST_LOG`, `RUST_BACKTRACE`, …) | RDM's fixed pass-through list (`crates/rdm-admin/src/admin/records.rs` `PASSTHROUGH`); under the process provider the host environment also reaches the child, minus `RDM_*`, the trace context and declared names |
| bind my listeners (REST, Kafka, …) | `RDM_LISTENERS` in the env table: one per listener my wiring declares on the kind, port 0, the bound port reported at the join and published on the node row |
| run on processes or containers | `MESH_SPAWN_TYPE`; [deployment-provider.md §2.6–2.7](../architecture/deployment-provider.md#26-the-process-provider) |

## Changing the topology

| How do I… | Where |
|---|---|
| create, resize or reshape meshes and nodes | the Build: [build.md §1–2](../architecture/build.md#1-build-is-the-only-topology-mutation-engine), the wire on [node-rpc-envelope.md, Build op 0x20](../architecture/node-rpc-envelope.md#build-op-0x20-rdm) |
| name the executable each node runs, and replace it | [build-deployment.md](../architecture/build-deployment.md) |
| know when the fabric is usable | [states.md](../architecture/lifecycles/states.md): pending → state-sync → state-commit → ready-for-traffic; a Build that changes the fabric is Complete only after its own ready-for-traffic (Q154/Q158) |
| stop, start, restart, delete one node | the lifecycle pages under [lifecycles/](../architecture/fabric-mesh-node-lifecycles.md) (`node-stop.md`, `node-start.md`, `node-restart.md`, `node-delete.md`) |
| delete the whole fabric | [fabric-delete.md](../architecture/lifecycles/fabric-delete.md): wait for the fabric.delete stream's `Complete` (fabric.deleted), then `node.delete` of the fabric-primary to its `node.deleted`; the process then exits by itself. Kill nothing before that |

## Reading state

The read calls follow one rule (Q165): `<entity>.get` returns one, `<parent>.<children>.get` returns the list.

| Call | Returns |
|---|---|
| `fabric.get` | the fabric: id, name, state, provider, fabric-primary |
| `fabric.meshes.get` | every mesh |
| `fabric.nodes.get { tags? }` | every node row of the fabric: the fabric topology, with each node's ports (`transport_addr`, `listeners`), status, config, tags and executable |
| `mesh.get { name }` / `mesh.nodes.get { tags? }` | one mesh / one mesh's node rows |
| `node.get { id }` | one node, `NOT_FOUND` when absent |

The node row's fields: [node-rpc-envelope.md, Node properties](../architecture/node-rpc-envelope.md#node-properties).
`node.topology.get` is different: one node's held gossip view, for repair, not for callers.

## Where my data goes

| What | Where |
|---|---|
| small per-node settings | the node's `config` map: mine, RDM never reads it; replaced whole by `node.config.update`; at most 32 keys and 2048 bytes ([node-config-update.md](../architecture/lifecycles/node-config-update.md), [gossip.md §3.1](../architecture/gossip.md#31-frame-catalog)) |
| mesh and fabric attributes | `mesh.config.*`, `fabric.config.*` ([node-rpc-envelope.md](../architecture/node-rpc-envelope.md#typed-objects-mapped-to-wire-and-execution-owners)) |
| labels to select nodes by | `tags`: `node.tags.create` / `node.tags.delete` |
| a node's durable data | under its `RDM_DATA_DIR`; carried across restart and re-birth by its path's storage policy ([node-storage-runbook.md](node-storage-runbook.md)) |
| my rows and events | my own storage, and my events on gossip ([gossip.md §8](../architecture/gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)); RDM orders nothing of mine |
| RDM's own records on my storage | the storage adapters ([build.md §5](../architecture/build.md#5-buildstateadapter)): persistence only, RDM writes whole records |

## My work inside a lifecycle

| Hook | Where |
|---|---|
| before a node is ready (`before_ready`; `inherited` says whether the data dir was carried) | [fabric-node-hooks.md, before_ready](../architecture/fabric-node-hooks.md#before_ready) |
| after it is ready | [after_ready](../architecture/fabric-node-hooks.md#after_ready) |
| the fabric's state-sync (my Day-0 work) | [sync_state](../architecture/fabric-node-hooks.md#sync_state); [fabric-state-sync.md](../architecture/lifecycles/fabric-state-sync.md) |
| traffic opening | [traffic_opened](../architecture/fabric-node-hooks.md#traffic_opened) |
| the clock | [rafka-time](../architecture/fabric-node-hooks.md#rafka-time) |

## Talking between nodes

| How do I… | Where |
|---|---|
| call a node | Node RPC: [node-rpc-envelope.md](../architecture/node-rpc-envelope.md); outcomes and codes on [node-rpc.md](../architecture/node-rpc.md) |
| open a stream on my own app protocol | the node's connections handle for my ALPN; routes, reconnect and proxies on [connections.md](../architecture/connections.md); a node's member cert admits it ([fabric-certs.md](../architecture/fabric-certs.md)) |
| publish and receive my events | gossip's envelope ([gossip.md §3.4](../architecture/gossip.md#34-the-envelope)) |

## When something breaks

| What | Where |
|---|---|
| following a call, a Build or a birth across processes | [telemetry.md](../architecture/telemetry.md) |
| no node-admin is alive: bring the fabric-primary back | [fabric-recovery-runbook.md](fabric-recovery-runbook.md) |
| a node's data dir is gone or broken | [provider-runbook.md](provider-runbook.md) |
| a stateful node's data across restart, re-birth, replacement, delete | [node-storage-runbook.md](node-storage-runbook.md) |
| a refusal | its code and named detail: [node-rpc-envelope.md, Runtime spans](../architecture/node-rpc-envelope.md#runtime-spans); every refusal names its reason |
