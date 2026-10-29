# Fabric, Mesh and Node Lifecycles

This page is the map of the fabric, mesh and node architecture: one line per page saying what it
owns, the workflow tables, the wire-op → workflow table and the terminology firewall. A concept is
stated on exactly one page, its owner; every other page links it. Source paths under
`rust-distributed-mesh/` name the pinned RDM repository.

## Pages

| page | owns |
|---|---|
| this page | the page map, the workflow tables, the wire-op → workflow table, the terminology firewall |
| [States](lifecycles/states.md) | fabric, mesh and node lifecycle states, who sets each, setting a state and its checklist, the one routing rule, stopped nodes, resize and executable replacement |
| [Gossip](gossip.md) | the channels, the frame catalog (each frame's fields, author and channels), cadence and timers, peers' receive rules, external topology, deleted retention, held-member repair, frame examples |
| [Node RPC envelope](node-rpc-envelope.md) | the op ledger, target and context, every op's request/reply schema and positional order, reserved stream codes, the typed object → wire mapping, the Build carrier `0x20` schema, RPC runtime span names, caller execution and certainty |
| [Query](query.md) | how anything reads the fabric: the filter and its matching rules, the reads the fabric-primary answers from its atlas, and a node's local query of the topology it holds |
| [Node RPC runtime](node-rpc.md) | how one invocation executes: resolver, pool, dispatch after the target, admission and memory, supervision, HandlerScope/ReplySink, the RpcOutcome algebra, budgets, connections-deleting runtime behaviour, retry guidance |
| workflow pages under `lifecycles/` (including [fabric.create](lifecycles/fabric-create.md) and [node.name.update](lifecycles/node-name-update.md)) | one workflow each: its sequence, actors, completion proof, failure, diagram and observability table (tables below) |
| [Fabric and node hooks](fabric-node-hooks.md) | where application work enters a lifecycle (`pre_node_ready`, `on_node_ready`, `on_fabric_ready`), NodeReadyCtx, outcomes, the rafka-time handle |
| [Fabric certs](fabric-certs.md) | the cert chain, issuance, renewal and verification at a connection; the cert workflows table |
| [Elections](fabric-node-lifecycle-elections.md) | the leadership election: a heard primary stays, unheard past the staleness floor it is superseded; the Mesh row and the fabric row as its inputs |
| [Node-admin lifecycle](node-admin-lifecycle.md) | node-admin launch inputs and start modes, who starts a node-admin of an existing mesh, the Rafka recovery copy |
| [Build](build.md) | the Build engine: accepted topology, the Build/attempt/claim model, BuildStateAdapter, storage interfaces, crash safety, lifecycle transitions, the RDM/Rafka boundary and import rule |
| [Build deployment](build-deployment.md) | what a Build deploys: the executable each node runs, named per node at create, recorded on the row, re-hashed at the launch, replaced only by re-birth, recovered from the row, read from the rows |
| [Deployment provider](deployment-provider.md) | the deployment provider and its environment policy, the DeploymentPipeline, the RuntimeFact contract, current runtime metadata and the exact runtime handle |
| [Fabric primary](fabric-primary.md) | the fabric-primary: the mesh roster from the accepted Build, takeover as hydration, the complete-view gate, fabric identity hydration, the event-distinction table |
| [Node identity and state](node-identity-and-state.md) | node identity (ids, endpoint_id, path.name, incarnation, prefixes), node/mesh/fabric rows and their writers, the NodeState liveness enum, the hydration roster, local eviction, the stopped-node collector |
| [Connections](connections.md) | connections, routes, reach and reroute |
| [Storage](storage.md) | the two things called storage: `storage_persistence`, a node's own data dir across its births and once it is gone, and `topology_storage_provider`, the backend RDM's five record stores are written to and how a fabric changes it live |
| [Telemetry](telemetry.md) | span export, and how one W3C trace crosses processes: Node RPC calls and workflows, gossip frames, a launch, data-plane connections, the fabric's change; where a trace starts; what is joined by identity |

## Three mechanisms

```text
TOPOLOGY GOSSIP
    says what the Fabric looks like now
    topology channels, mesh topologies, lifecycle events

BUILD / ATTEMPT / CLAIM / STEP
    assigns durable work and records its outcome
    survives a primary change; a successor continues the same Build
    exactly one executor holds an attempt at a time

NODE RPC
    an exact-target command from a Mesh executor to one node, with a typed reply
    immediate; never a second assignment protocol
```

None replaces another. A Build assigns; Node RPC executes against one node; gossip tells every
node what changed and when. The fabric-primary drives every Build through the
[Build carrier](node-rpc-envelope.md#build-op-0x20-rdm). Node RPC carries both assignment and
progress; a won claim supplies durable ownership. Gossip carries the recovery evidence, never the
progress wait.

What an operation is admitted against, gate by gate and the source each reads, is
[Build §4.4](build.md#44-the-gates-before-an-operation).

## Fabric workflows

| Workflow | Read when |
|---|---|
| [fabric.create](lifecycles/fabric-create.md) | Creating a fabric: minting it, its first mesh's first node-admin as fabric-primary, the fabric's states to ready |
| [node.primary.update](lifecycles/node-primary-update.md) | Moving the primary of the nodes of one type in one mesh to its successor, first in every stop, delete, replace or `node.connection.delete { scope: All }` of a primary |
| [node.fabric-primary.update](lifecycles/node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) | Moving the fabric-primary, the first step of a mesh.delete of its mesh |
| [fabric.ca.renew](lifecycles/fabric-ca-renew.md) | RDM's CA rotated internally on policy under the same anchor: every issuer re-minted, no node's trust set touched |
| [fabric.root.update](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op) | The trust anchor rotated on a caller's new root, through an overlap in which every node trusts both, then the CA re-minted under it and the old anchor retired |
| [Recovery](lifecycles/fabric-recovery.md) | Launching a node-admin again after the loss of every node-admin; the loss of the fabric-primary while a node-admin lives: the fabric recovery workflow, one mechanism from its own mesh or the backbone, nobody taking the role |
| [fabric.delete](lifecycles/fabric-delete.md) | Deleting the fabric, mesh by mesh, the fabric-primary's own mesh last |

## Mesh workflows

| Workflow | Read when |
|---|---|
| [mesh.issuer.renew](lifecycles/mesh-issuer-renew.md) | The fabric-primary re-minting a mesh's issuing cert at two thirds of its TTL and carrying it to that mesh's node-admins |
| [mesh.create](lifecycles/mesh-create.md) | A mesh's first node-admin: the mesh-absent arm of node.create, the MeshId minted at acceptance, the channel, the first mesh-primary, the mesh's first states |
| [Investigation](lifecycles/mesh-investigation.md) | Probing an unheard peer mesh and deciding whether to run node.create of a node-admin into it; the node-admin's join |
| [mesh.delete](lifecycles/mesh-delete.md) | Deleting a mesh and its members |

## Node workflows

Callers use the [typed node and Build objects](node-rpc-envelope.md#typed-object-contract).

| Workflow / call | Read when |
|---|---|
| [node.create](lifecycles/node-create.md) | Bringing a new node into a mesh, through its first start |
| [node.start](lifecycles/node-start.md) | Taking a node from a join to ready, first start or after a stop |
| [node.mesh.join](lifecycles/node-mesh-join.md) (primitive) | A node's report to its primary and its channel join, `joining` then `node.mesh.joined`; run inside start, standalone after a tickle that reached a node off its channel, and by a node-admin entering a mesh |
| [node.mesh.leave](lifecycles/node-mesh-leave.md) (primitive) | A node leaving its channels after its connections are deleted, `leaving` then `node.mesh.left`; the step stop and delete end with |
| [node.tickle](lifecycles/node-tickle.md) (primitive) | The reach ladder on an exact birth: direct, then both via-peer kicks at once; `reached` or `unreached`, never dead; run by node investigation and the primary-update successor ladder |
| [node.runtime.inspect](lifecycles/node-runtime-inspect.md) (primitive) | The provider's answer about the exact runtime handle; node investigation's first step after the index read, run by it, delete, mesh.delete and fabric.delete |
| [node investigation](lifecycles/node-investigation.md) (primitive) | The one verdict on one exact birth: the by-destination connections index read, node.runtime.inspect, node.tickle direct and via those peers; reached is commanded back through node.mesh.join, any other verdict is re-created at the same path.name with a new node.id |
| [node.stop](lifecycles/node-stop.md) | Taking a node out of traffic while its process stays alive |
| [node.delete](lifecycles/node-delete.md) | Removing a node and releasing its identity |
| [node.connection.delete](lifecycles/node-connections-delete.md) | Closing a node's connections while its process keeps running: all of them gracefully, or exactly the named ones |
| [node recovery (RDM)](lifecycles/node-recovery.md) | Replacing a member whose exit its mesh-primary proved |
| [Rafka replace](lifecycles/node-replace.md) | Replacing a node under Rafka's steps: the predecessor renamed `<name>.old` live and stopped, the successor re-born at `<name>` |
| [node status (internal)](lifecycles/node-status.md) | The node's status report and its mesh-primary's apply; not a caller op |
| [node.topology.get](lifecycles/node-topology-get.md) | Reading a node's held topology; a [running node's reconnect](lifecycles/node-topology-get.md#running-node-reconnect) |
| [node.name.update](lifecycles/node-name-update.md) | Renaming a node |
| node.tag.get/create/delete | A node's tag labels ([typed-object table](node-rpc-envelope.md#typed-object-contract)) |
| [node.get](node-rpc-envelope.md#typed-object-contract) | Reading nodes |
| [node.connection.get](node-rpc-envelope.md#typed-object-contract) | Reading a node's held connections |
| [node.config.get/update](lifecycles/node-config-update.md) | Reading or putting a node's application config |
| [node.cert.renew](lifecycles/node-cert-renew.md) (primitive) | The mesh-primary, as issuer, installing a fresh member cert on a running node before the old one expires; presented on new connections, terminates nothing |
| [node.issuer.update](lifecycles/node-issuer-update.md) (primitive) | The fabric-primary installing a mesh's re-minted issuing material on a running node-admin |
| [node.roots.update](lifecycles/fabric-ca-renew.md#noderootsupdate-the-primitive) (primitive) | The fabric-primary installing the fabric's root set on one node; one or two roots through a rotation |
| [node.connection.create](lifecycles/node-connections-admit.md) (primitive) | The acceptor's verification of a dialer's member cert on an inbound connection of a registered ALPN; admitted or refused, a connection fact |
| [build.create/get/delete](node-rpc-envelope.md#build-op-0x20-rdm) | Submitting a Build, reattaching to it, or deleting its history |

## Wire-op-to-workflow index

Op definitions live in the envelope. The typed node, mesh, fabric and build calls map onto
these wire ops, with Build on 0x20; [adapter questions](node-rpc-envelope.md#open-typed-object-questions)
are explicit.

| Op | Contract | Workflows |
|---|---|---|
| 0x01 Ping | [Ping](node-rpc-envelope.md#ping-op-0x01-core) | [Mesh investigation](lifecycles/mesh-investigation.md), [a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh), [fabric recovery](lifecycles/fabric-recovery.md) |
| 0x1A Forward | [Forward](node-rpc-envelope.md#forward-op-0x1a-core) | [Mesh investigation](lifecycles/mesh-investigation.md) |
| 0x1B Status | [Status](node-rpc-envelope.md#status-op-0x1b-rdm) | [node.cert.renew](lifecycles/node-cert-renew.md), [node.issuer.update](lifecycles/node-issuer-update.md), [node.roots.update](lifecycles/fabric-ca-renew.md#noderootsupdate-the-primitive), [Node status](lifecycles/node-status.md), [replace](lifecycles/node-replace.md), [delete](lifecycles/node-delete.md), [ready](lifecycles/states.md#the-fabrics-ready), [mesh.delete](lifecycles/mesh-delete.md), [start](lifecycles/node-start.md), [create](lifecycles/node-create.md), [node recovery](lifecycles/node-recovery.md), [stop](lifecycles/node-stop.md), [node.connection.delete](lifecycles/node-connections-delete.md) |
| 0x1C BuildClaim | [BuildClaim](node-rpc-envelope.md#buildclaim-op-0x1c-rdm) | [Build carrier](node-rpc-envelope.md#build-op-0x20-rdm) claim-and-dispatch, [a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh), [fabric recovery](lifecycles/fabric-recovery.md) |
| 0x1D JoinNode (node.mesh.join's report) | [Join](node-rpc-envelope.md#join-op-0x1d-rdm) | [node.mesh.join](lifecycles/node-mesh-join.md), [create](lifecycles/node-create.md), [replace](lifecycles/node-replace.md), [node recovery](lifecycles/node-recovery.md), [a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) |
| 0x1E GetTopology (node.topology.get) | [Topology](node-rpc-envelope.md#topology-op-0x1e-rdm) | [Create](lifecycles/node-create.md), [replace](lifecycles/node-replace.md), [a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh), [fabric recovery](lifecycles/fabric-recovery.md), [node.topology.get and running reconnect](lifecycles/node-topology-get.md), [node recovery](lifecycles/node-recovery.md) |
| 0x1F FetchBuildFacts, FetchCurrentBuild, FetchBuildSection | [Build facts](node-rpc-envelope.md#build-facts-op-0x1f-rdm) | [node.start](lifecycles/node-start.md#end-to-end-sequence) step 4: FetchBuildSection of the node's own mesh, of the Build a blocker names or, when the missing pointer names none, of the responder's current Build; a primary taking its role ([states](lifecycles/states.md)) |
| 0x20 build | [Build carrier](node-rpc-envelope.md#build-op-0x20-rdm) | [Node create](lifecycles/node-create.md), [stop](lifecycles/node-stop.md), [start](lifecycles/node-start.md), [delete](lifecycles/node-delete.md); fabric-primary claim-and-dispatch of topology operations; mesh-primary claim-and-run of drift attempts; reattachment |
| 0x21 PrimaryUpdate | [PrimaryUpdate](node-rpc-envelope.md#primaryupdate-op-0x21-rdm) | [node.primary.update and node.fabric-primary.update](lifecycles/node-primary-update.md), before a primary's [node.connection.delete](lifecycles/node-connections-delete.md), [stop](lifecycles/node-stop.md), replace or delete |
| 0x22 NodeCommand | [NodeCommand](node-rpc-envelope.md#nodecommand-op-0x22-rdm) | [node.connection.delete](lifecycles/node-connections-delete.md), [stop](lifecycles/node-stop.md), [start](lifecycles/node-start.md): the mesh-primary to the exact node |

## Terminology firewall

Five concepts are distinct and never mixed.

**Build operation.** `(build_id, attempt, operation)` identifies a lifecycle operation in
reconciliation. Operation names render mechanically from the typed object:

```text
node.create:<path.name>
node.stop:<path.name>
node.start:<path.name>
node.delete:<path.name>
node.connection.delete:<path.name>
```

A typed call has one canonical name, noun.verb, and a command completes as noun.verb in the past tense: node.create completes as node.created, node.connection.delete as node.connection.deleted. The Rust request and reply variants (`NodeRequest::Create`, `NodeReply::Created`), the Build step key (`noun.verb:<path.name>`, an opaque string, see [the Build schema](node-rpc-envelope.md#build-schema)) and the span (`rdm.<component>.<noun>.<verb>.via-...`) are rendered from it mechanically and are never named by hand.

A new node is node.created. Birth and re-birth are the application's names for its own workflows, never RDM operations. Rafka names its own recover/replace steps without creating an RDM opcode.

**Node RPC op code.** A row of the [op ledger](node-rpc-envelope.md#the-op-ledger). A code is
not a Build operation identity: only `build_id + attempt + operation` names a Build operation.
A stop step sends StopNode for [node.stop](lifecycles/node-stop.md#command-and-completion-messages) on op
`0x22` (NodeCommand).

**Topology read.** [node.topology.get](node-rpc-envelope.md#topology-op-0x1e-rdm) reads a node's
held topology. It is not gossip, not a join and not a Build operation.

**Lifecycle gossip event.** `NodeStopped`, `NodeDeleting`, `NodeDeleted`:
dissemination facts around an operation ([frame catalog](gossip.md#31-frame-catalog)). They are
not the Build operation and not `0x1B` NodeState variants.

**Node self-published status.** A topology digest reports the admitted instance's status, for
example `Pending` or `Ready`. It is a topology fact, not a caller progress event.
