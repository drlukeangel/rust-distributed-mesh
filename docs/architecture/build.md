# Build: the topology engine, leadership, lifecycle transitions and state adapters

This page owns the Build engine (the accepted topology, the Build record written once, its events, `BuildStateAdapter` and `EventsStorage`), the node-admin storage interfaces and crash safety, and the RDM/Rafka boundary with its import rule. The deployment provider and the RuntimeFact contract are [deployment-provider.md](deployment-provider.md).

It defines the generic control-plane contract built and tested in `rust-distributed-mesh` (RDM), an independently owned and versioned reusable distributed-system substrate that Rafka imports at an exact revision. Rafka is its first customer: RDM owns how distributed execution works, and Rafka owns what its product asks nodes to do.

The ownership test is behavioral: a mechanism that makes sense for a non-Rafka distributed mesh belongs in RDM. Behavior that depends on Rafka roles, entities, authorization, storage policy or other product concepts belongs in Rafka.

The Fabric → Mesh → Node hierarchy is [fabric-primary.md §1](fabric-primary.md#1-the-product-hierarchy-is-fabric---mesh---node). Exact node lifecycle operation/RPC/gossip choreography is owned by [fabric-mesh-node-lifecycles.md](fabric-mesh-node-lifecycles.md).

The control plane has deliberately separate mechanisms:

```text
fabric-primary + the current Build
    the one topology writer, and the Build the Builds read newest first name current (§4)

Topology + RuntimeFacts + current runtime metadata
    current exact births/runtimes that actually exist

Build / Reconciler
    realizes the accepted topology Build

builds.storage (BuildStateAdapter)
    each Build, written once

events.storage (EventsStorage)
    what happened while executing each Build, and each node's investigations

The leadership election
    deterministic node-type primary -> mesh-primary -> fabric-primary projection

DeploymentPipeline
    how one runtime is created, adopted, observed, or retired

LifecycleTransitionPipeline
    what must complete before Node/Mesh/Fabric changes lifecycle state
```

No one mechanism impersonates another. Build storage is execution evidence: it is neither a second topology writer nor the current-runtime database. The current Build is read from `builds.storage` newest first (§4); `builds.storage` also records what happened while executing it.

RDM `rdm-admin` is the single implementation of Build reconciliation, runtime adoption and leadership projection; Rafka imports it, binds its adapters and policy into it, and never reimplements leadership, shape reconciliation, executor selection, runtime adoption, drift detection or recovery.

### Node-admin composition and customer seams

```text
Rafka node-admin process
    +-- Rafka integration: inputs, policy, bindings and product hooks
    `-- imported RDM node-admin-core: generic control algorithms
```

RDM node-admin-core MUST expose one documented embedding/composition boundary through which
a customer supplies its approved bindings. That boundary MUST NOT require copying or
reimplementing generic algorithms. This requirement does not prescribe a Rust constructor;
a reference launcher choosing file stores/provider configuration is not the only supported
customer composition model.

Allowed customer seams are:

- implementations of the four storage interfaces, fabric, mesh, nodes and connections, on the fabric's shared store root, and the Build and events stores (`builds.storage`, `BuildStateAdapter`; `events.storage`, `EventsStorage`; on the fabric's shared store root per R-BI6);
- product Build-input/shape normalization and each node's explicit desired properties;
- deployment-provider bindings, resolved launch/listener configuration, and mapping of customer
  runtime-management metadata into the imported current-runtime projection;
- current topology/runtime fact feeds and projection/public-API mappings;
- product authorization and the hooks of [Fabric and node hooks](fabric-node-hooks.md).

RDM retains Build compilation/reconciliation, the leadership election,
executor selection, generic lifecycle eligibility/commit/receipts, readiness roots, recovery,
exact-runtime adoption/a sign the node is dead, provider-domain execution and connection mechanics.
Customer seams supply policy or facts; they MUST NOT substitute a second implementation of
those algorithms. Hooks are registered before the registry is built, idempotent and bounded, and report
failure through the lifecycle receipt path. A hook requesting exact synchronous execution
of gateway-owned application behavior uses a typed Rafka Op on the exact gateway rather than
importing the gateway implementation. Durable asynchronous job/event contracts and explicitly
authorized read projections are valid under the same owner boundary.

RDM's only surface is Node RPC: the `node`, `mesh`, `fabric` and `build` op families and the typed objects over them ([Node RPC envelope](node-rpc-envelope.md#typed-object-contract)). No RDM crate serves HTTP, depends on an HTTP server or defines a route. An application that wants HTTP routes builds them in its own crate as thin callers over the same typed objects.

Shared typed contracts, genuinely role-neutral domain models and RDM substrate packages are allowed across roles. A crate that holds role-owned executable handlers, stores or boot registrations does not become a shared model by being named `core` or `shared`. Same-role implementation composition is allowed.

### Role-neutral execution and policy

Who executes follows topology and lifecycle ownership, not a node's type.
The owning Mesh executor realizes ordinary members and sibling node-admins; an exact Pending
bootstrap admin or a node-admin joining a live mesh executes only its own Mesh under the fabric-primary. Fabric-wide and
outside-Mesh retirement rules remain generic control decisions. A general rule equivalent to
`if type == "node" { mesh executor } else { fabric executor }` is forbidden. Provider-domain
locality may change execution location, never topology or the election.

RDM may carry an opaque or typed node class required by generic algorithms, but MUST NOT
require a closed Rafka Broker/Gateway/Compute taxonomy as its generic abstraction. Rafka
owns its role vocabulary and ingress policy. Generic `StoragePersistence` validity and pipeline
interpretation remain RDM-owned; `default_for(RafkaRole)` and product storage-default choices
belong downstream. This is an embedding requirement; it is not a new wire schema.

### Packages

Rafka imports these RDM packages at one exact revision:

| package | what it holds |
|---|---|
| `rdm-core` | the Node RPC protocol contract: framing, outcomes, reserved codes and protocol traits, with no transport dependency |
| `rdm-core` | the generic Mesh entity: exact-node identity, process incarnation, topology, endpoint freshness, RuntimeFact and connection projections |
| `rdm-transport` | the gossip substrate on Iroh: the topology digest view, versioned cross-mesh topology snapshots and deltas, and the gossip frame codec |
| `rdm-transport` | the Node RPC runtime over Iroh: exact-target client and server, pool, commit cut and admission |
| `rdm-admin` | generic Fabric/Mesh/Node control: Build/Reconciler, the leadership election, the four storage interfaces, fabric, mesh, nodes and connections, on the fabric's shared store root, and the Build and events stores (`builds.storage`, `BuildStateAdapter`; `events.storage`, `EventsStorage`; on the fabric's shared store root per R-BI6) with memory and durable-file implementations, and the lifecycle, deployment, runtime-adoption, provider-domain and recovery contracts |
| `rdm-client` | the typed client and DTOs for the node-admin control API |
| `rdm-test-scenario` | provider-neutral scenario, evidence and replay contracts |
| `rdm-test` | the generic RPC test node, test protocols and probes |

the chaos kit (`rdm-test`'s `faults` modules), the fault backends with typed outcomes, is an RDM test package used by
`rdm-test-scenario`. RDM packages import no Rafka domain crate.

---

## 1. Build is the only topology mutation engine

Every external requested change to the topology that results in the addition or a removal of a node is its own Build, submitted through build.create with its own build_id. node.stop, node.start and node.connection.delete change no shape: each is a node command the primary above the node runs, recorded on the node record, never a Build or part of one. A repair (delete, then create at the same path and name) is not external: it is the primary's own workflow ([node recovery](lifecycles/node-recovery.md)), recorded on the node record, never a Build or part of one. Work on one node is serialized at its mesh-primary.

The request is a batch (Q280; [batch call](node-rpc-envelope.md#caller-execution-and-certainty)), one of three changes, each with a result per node:

| Change | Names | Refused by name |
|---|---|---|
| `Create { meshes, nodes }` | each mesh by its Meshes schema, each node by its Nodes schema ([schemas](node-rpc-envelope.md#build-schema)), whole; a node whose mesh is not yet live creates that mesh with its first node-admin | a new mesh with no node-admin among its nodes; a node naming a mesh neither in the request nor live; a path and name already held; an undeclared type; a node with no executable, its own or its type's |
| `Delete { meshes, node_ids }` | each mesh by name, each node by its node.id, never by its path and name | a node.id already deleted, in its own result |
| `DeleteFabric` | the fabric | (§1, the one exception below) |

The caller states only what it adds or removes, never a node it does not touch: an existing node's `config` and `tags` change through `node.config.update` and `node.tag.*` at any time, the last write winning, and no Build writes them back. A Build states each node it creates whole (Q236, amended by Q342).

~~~text
current Build = B (completed)

submitted change
    -> applied once to the fabric-primary's records
    -> the complete topology at acceptance, each created node's type defaults filled in
    -> persist Build B' (the change as it came in, that topology, its source and time; written once; current once durable)
    -> dispatch each mesh's section to its mesh-primary
~~~

The Build record is never updated as the system runs: the topology of the moment is the records ([node identity](node-identity-and-state.md)), and the Build's progress is its events (§5). Counts are the product's door's vocabulary, expanded into named nodes before the typed call; RDM mints a path and name only for a create it originates itself.

Exactly one current fabric-primary accepts topology-changing requests. A non-primary refuses by name. A second topology-changing request while the current Build is actively reconciling is refused with a named build-in-progress refusal naming the current Build; RDM does not implement Build supersession/cancellation. The one exception is fabric.delete, which may interrupt the Build in progress because its target is nothing. Its acceptance record is the durable intent: once written, the interrupted Build opens no further operation and is never resumed, and a successor fabric-primary reads the delete as the current Build and continues the teardown. Operations already admitted at an executor finish or fail within their existing bounds and their launch evidence is retained; a fabric state the fabric-primary is setting ends at the acceptance, its hook's future dropped. When every step of the interrupted Build in flight has ended, that Build ends `incomplete`, each section it did not finish naming the delete, and the teardown's steps run mesh by mesh. An operation that fails to end within its bound fails the delete by name, the intent retained; nothing reconciles the interrupted Build again. No other Build ends another.

A node command and a repair carry the exact birth they act on (node id and incarnation), which refuses a stale birth by name. They never change the Build.

Every run of a Build reads its topology and the records and computes the remaining work. A step whose `rdm.build.step` event ended `complete` is not run again; a Build never resumes from a mutable instruction pointer. A section that ends short, its mesh-primary lost among the causes, runs no more in that run, and the Build ends `incomplete` naming it (Q346); its resubmission dispatches the section to the mesh's primary of the moment, which reads the Build's events and runs the steps its mesh owes (elections §4.1).

Forbidden:

- application entry points directly spawning or killing nodes outside Build execution;
- a separate Rafka shape/resize/recovery engine beside imported Build;
- any node-admin other than the current fabric-primary accepting a topology change;
- storing only counts and letting each executor independently choose which node to add or remove;
- a Build for node.stop, node.start, node.connection.delete, node recovery or a primary change, or any of them run as part of a Build;
- a Build that states a node it does not add or remove;
- updating a Build record after it is written;
- accepting a second topology mutation while the current Build is actively reconciling;
- blindly replaying operations already satisfied in current state.

Rafka shape vocabulary may remain product-facing input. It is expanded into the named nodes of one `Create` or `Delete` before the typed call.

---

## 2. Public Build calls

RDM creates, drives, reads and deletes Builds only through the
[Build carrier](node-rpc-envelope.md#build-op-0x20-rdm), which defines build.create,
build.attempt.run, build.get and build.delete, their direction, streams and refusals.
node.create and node.delete are submitted through build.create, as §1's `Create` and
`Delete`; node.stop, node.start and node.connection.delete are node commands, never Build calls
(§1). Application bootstrap and fault injection are application work, not
Build calls.

---

## 3. Product identity and transport identity are different

The node, mesh and fabric ids and the node's transport `endpoint_id` are defined in
[node-identity-and-state.md §2](node-identity-and-state.md#2-a-node-is-an-entity-and-it-has-three-ids),
[§2.7](node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it) and
[§2.8](node-identity-and-state.md#28-a-fabric-is-an-entity-and-the-fabric-primary-owns-it); leadership
ordering over them is [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md).
BuildId, DeploymentId and IncarnationId are minted like every other id, bare 12-character Crockford60
(`rdm-core/src/ids.rs`); there is one id type. Transition ids, provider handles and
control-domain values are not ids: they are the provider's or the pipeline's own strings.

---

## 4. The accepted topology is the fabric-primary's current Build

There is exactly one Fabric topology writer: the current fabric-primary.

```text
many node-admins
    -> the leadership election
    -> current fabric-primary
    -> the ONLY topology writer
```

```text
non-fabric-primary topology mutation -> REFUSE
fabric-primary topology mutation     -> ACCEPT -> complete-topology Build B18, durable in builds.storage -> B18 is the current Build
```

The writer is the current fabric-primary from [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). A stale projected boolean never creates a second writer.

"Desired topology" means the topology the current fabric-primary accepted, which the current Build is intended to realize. It is not a separate object beside the Build:

- the current Build is found by reading the Builds last in, first out: the first one that is in progress or completed, by its events (§5). In progress, it is the Build being driven, and it is resumed; completed, it is the known state. An incomplete or failed Build is skipped: it is never current, nothing resumes it on its own, and a caller resubmits it by its build_id, whose run skips every step whose event ended `complete`;
- every node-admin reads the current Build from `builds.storage`; the Fabric row carries no pointer to it;
- a leadership change does not change it;
- later runtime loss is reconciled against it.

Persisted copies are not writers: several node-admins may each hold the same Fabric and Build facts for recovery, and only the current fabric-primary, as the election names it, may accept the next topology mutation.

A request adds or removes the nodes it names; the fabric-primary composes the complete topology from its records at acceptance and writes it into the Build once. node.stop, node.start, node.connection.delete and node recovery are never part of a Build (§1).

### 4.1 Build identity is active reconciliation state

The fabric-primary is the one driver of a Build; an attached caller does not own its execution.

While a Build is in progress, `builds.storage` carries enough information for any eligible successor to continue the same work:

```text
build_id (the current Build)
the change as it came in
the complete topology at acceptance, each node's Nodes schema with its executable
```

and the events store carries what happened since: each mesh's dispatch and each step's outcome (§5).

A primary or executor change during that active Build keeps the same `build_id`. A primary change is not a topology mutation and never mints a Build. Each mesh's section is one call from the fabric-primary, addressed to the mesh's primary under the call's one retry mechanism (Q344); a mesh's new mesh-primary runs the section from the events, and a step whose `rdm.build.step` event ended `complete` is not run again. A step that ends `failed { reason }`, or a section call that ends short of its end, ends its mesh's section, never the Build; the other sections run on, and nothing is reverted (Q346). The Build ends `complete` when every section finished, `incomplete` when it ran and not every section finished (what ran stands), and `failed` only when it could not start. The first step is the fabric-primary's [`on_build_started`](fabric-node-hooks.md#on_build_started) notice, sent from the write of `rdm.build.started`; it gates nothing. The last is [`on_build_ended`](fabric-node-hooks.md#on_build_ended), sent from the write of `rdm.build.ended` (`complete`, `incomplete { sections }` or `failed { reason }`); it gates nothing.

```text
Build B123 in progress (the current Build)
fabric-primary disappears
the election names the successor fabric-primary
        |
        v
same B123
load B123 from builds.storage
recompute accepted - observed
        |
        v
continue remaining work
```

A node lost after B123 completed is a repair, not a Build: the node's mesh-primary investigates it and re-creates it (a lost mesh-primary: the successor its election names; the fabric-primary only when no successor takes the role) at the same path and name ([node recovery](lifecycles/node-recovery.md)), recorded on the node record. B123 stays completed and is not written.

Each topology change is its own Build with its own build_id (§1). A Build is the request, the topology accepted from it, and its events.

### 4.2 Hydration of the accepted topology

A joining or reconnecting node-admin reads the current Build (§4):

```text
node-admin start
    -> fabric.storage / builds.storage (durable local state)

first connection / JoinNode (op `0x1D`)
    -> Fabric identity + the Build to hold
```

Join admission and its answer are defined by [JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm). An older completed Build is history, never the topology; the current Build is.

### 4.3 The accepted topology changes first; the observed topology follows execution

The accepted topology is the Build's; the observed topology follows the executor's steps and the provider's inspect answer. The moment-by-moment table of what every node observes while a node is removed, and which status and overlay stop application routing, is [States: Routing](lifecycles/states.md#routing); the removal's steps are [node.delete](lifecycles/node-delete.md). The fabric-primary's dispatch of each mesh's section to its mesh-primary and a successor's resumption are the [Build carrier](node-rpc-envelope.md#calls-and-direction)'s.

---

### 4.4 The gates before an operation

Every Build passes the fabric-primary's gates in this order before anything runs. Each gate reads
one source. A gate that fails refuses by name and nothing is written. A node command's checks are its own page's
([node.stop](lifecycles/node-stop.md), [node.start](lifecycles/node-start.md),
[node.connection.delete](lifecycles/node-connections-delete.md)).

| Gate | Reads | Refusal |
|---|---|---|
| The receiver is the fabric-primary | its own view | `not-fabric-primary`, naming the fabric-primary |
| The current Build is settled | the Build store and its events (R-BI3) | `build-in-progress`, naming the Build; fabric.delete excepted (§1) |
| A created node's path and name are free, its mesh live or created in the request with a node-admin, its type declared | the fabric-primary's records | `name-held`, `unknown-mesh`, `unknown-type`, by name |
| A deleted node.id stands | the fabric-primary's records by node.id | `unknown-node` |
| The node's mesh is heard | the fabric-primary's view | refused by name, naming the mesh and how long it has gone unheard (R-D1) |
| A deleted primary has a successor | the fabric-primary's view | refused by name, naming the primary |

The operation carries the exact birth the gates pinned (node id and incarnation). The mesh-primary
that runs it verifies that birth again against its own view at execution, so a birth replaced
between acceptance and execution is refused there, by name. A pinned birth its view does not hold yet is
incomplete evidence, not a replacement: the step ends incomplete and resumes on that same pinned birth
when its view moves ([incomplete steps](node-rpc-envelope.md#caller-execution-and-certainty), Q253), never
on a newer birth at its name.

---

## 5. BuildStateAdapter

Build mechanics must not depend on one storage technology.

```rust
#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    /// Write the Build record, once.
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
    /// The Build's view: its record and its events.
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError>;
    /// The current Build, read newest first by its events (§4).
    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError>;
    /// The current Build's view, when one is.
    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError>;
    /// The newest acceptance's time: the acceptance-order index's high-water mark.
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError>;
    /// A finished Build leaves history (build.delete).
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
    /// The Builds held, newest accepted first: the acceptance-order index.
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError>;
    /// The events store the Build's events are in.
    fn events(&self) -> Arc<dyn EventsStorage>;
    /// The path and name this admin holds, when the adapter knows it.
    fn held_name(&self) -> Option<PathName>;
    /// Write a step's `rdm.build.step` event, as the run's writer; only a run's step log writes one.
    async fn write_step(&self, step: &StepWrite) -> Result<(), BuildStateError>;
    /// The step in flight is blocked for `reason`: live progress, never durable.
    async fn note_blocked(&self, build_id: &BuildId, operation: &OperationId, step: &str, reason: &str);
}

#[async_trait]
pub trait EventsStorage: Send + Sync {
    /// Append `event`.
    async fn append(&self, event: &Event) -> Result<(), StorageError>;
    /// The event `id`, when the store holds it.
    async fn event(&self, id: &EventId) -> Result<Option<Event>, StorageError>;
    /// The events of one Build (by `data.build_id`), in event.id order.
    async fn of_build(&self, build_id: &BuildId) -> Result<Vec<Event>, StorageError>;
    /// The events of one node (by `subject`), in event.id order.
    async fn of_node(&self, node_id: &NodeId) -> Result<Vec<Event>, StorageError>;
    /// The storage changes' events (`type` `rdm.storage.*`), in event.id order.
    async fn of_storage_changes(&self) -> Result<Vec<Event>, StorageError>;
}
```

A Build is one record, `BuildAccepted`, written once. What happened while executing it is the events store's: one append-only record per event, a CloudEvents 1.0 envelope ([gossip.md §3.4](gossip.md#34-the-envelope)'s attributes, `id` a time-ordered UUIDv7, `datacontenttype` `application/json`), never gossiped:

| `type` | Writer | `subject` | `data` |
|---|---|---|---|
| `rdm.build.started` | the fabric-primary | none | `build_id` |
| `rdm.build.dispatched` | the fabric-primary | none | `build_id`, `mesh` |
| `rdm.build.step` | the mesh's mesh-primary | the node acted on | `build_id`, `mesh`, `step`, `op`, `state` (`complete` or `failed { reason, code }`) |
| `rdm.build.ended` | the fabric-primary | none | `build_id`, `state` (`complete`; `incomplete { sections }`, each unfinished section's `mesh`, `op`, `step`, `reason` and `code`; or `failed { reason, code }`, why it could not start) |
| `rdm.node.investigated` | the primary above the node | node.id | `incarnation`, `path`, `name`, `verdict`, `next` |
| `rdm.storage.started`, `rdm.storage.committed`, `rdm.storage.ended` | the fabric-primary | none | `change_id`, `topology_storage_provider`, and for the ending its `state` |
| `rdm.mesh.pending`, `rdm.mesh.ready`, `rdm.mesh.deleting`, `rdm.mesh.deleted` | the mesh's mesh-primary (the fabric-primary while the mesh has none) | none | `mesh_id`, `mesh` |
| `rdm.fabric.pending`, `rdm.fabric.ready` | the fabric-primary | none | `fabric_id` |

The store answers four reads: by event.id, the events of one Build (by `data.build_id`, in event.id order), the events of one node (by `subject`, in event.id order) and the storage changes' events (`type` `rdm.storage.*`, in event.id order). The current Build is read newest first (§4, `current_build()` over the acceptance-order index, with `newest_accepted_at_ms()` as the acceptance high-water mark). Neither is a second topology writer or the current-runtime store; completed Builds and their events remain history for API views, diagnostics, audit and chaos evidence.

### 5.1 RDM adapters

RDM shows the contract without inventing a broker-backed database:

- `MemoryBuildStateAdapter` for unit/functional evidence;
- `FileJournal`, the Build journal on the fabric's shared store root (`builds.jsonl`, §5.3), beside
  `FileEventsStorage` (`events.jsonl`);
- `FabricBuildStateAdapter`, a node-admin's Build state: that journal, read and written behind the
  admin's primary records.

Single-writer safety comes from one writer per record: the fabric-primary writes each Build record, once, and the Build-level events; a mesh-primary writes the `rdm.build.step` events of its own mesh's steps and its own mesh's rows; each event is its own record. With that come idempotent operation keys and stable logical topology identities. When an unheard peer mesh is heard again, the election converges and the current fabric-primary reconciles reality rather than trusting a Build's history.

### 5.2 Rafka adapter

Rafka maps the same semantic contract onto its durable application/control infrastructure.

Rafka implements the Build and events stores on its entity framework (Q341): the Build an entity written once at its build_id; each event an entity at its event.id with two declared index slots, (build.id, event.id) and (node.id, event.id), read in key order. Nothing is read-modify-written and no record is updated.

A live run is reattached, never re-run: the caller resubmits the same build_id through build.create and reads the Build's events ([Build carrier](node-rpc-envelope.md#build-op-0x20-rdm)); build.get is a read of the record and its events. The fabric-primary is the one writer of Builds.

Rafka persists each accepted Build (its complete topology), written once, behind `builds.storage`; the Fabric row carries no pointer to a Build. A `system.ops`-backed implementation is one option; the reconciler knows the storage interface, never a specific topic.

The current Build cannot be deleted (build.delete refuses by name); an older Build is deletable history once a newer Build is current.

A Build fact a successor needs (the accepted Build, a step's event, the ending) is not committed until it is durable in storage that survives the loss of the current fabric-primary. Storage local to the active fabric-primary alone is insufficient for failover. The Build facts are the Build journal on the fabric's shared store root (§5.3), each record at its own key, written by the fabric-primary, so a successor reads them there as it becomes fabric-primary. An executor, a starting node-admin and a node-admin taking a primary role read the named Build's committed records there too, fresh: the read answers the records it read or an explicit error, never a cached projection, and an unreadable store is not an empty Build (Q292). The fabric-primary stays the one writer of Builds, and a read grants no write. No command carries a copy of the Build: `build.attempt.run` names the Build and the mesh. The accepted Build is recorded whole whatever its size: the store has no frame.

Acceptance is crash-safe because a Build is accepted by one durable write of the Build itself:

```text
fabric-primary prepares B19
    -> persist complete B19 through builds.storage, written once
    -> B19 is durable beyond this fabric-primary
    -> NOW B19 is accepted: the newest Build, in progress, so the current Build

crash before B19 is durable
    -> B19 does not exist
    -> the previous current Build remains the known state

crash after B19 is durable
    -> B19 is in progress
    -> the successor reads it as the current Build and resumes it
```

No Build is current before it is recoverably durable.

### 5.3 Node-admin storage boundaries

Control-plane state has six boundaries on the fabric's shared store root: fabric, meshes, nodes,
connections, Builds and events. On the file backend these live under
`<store_dir>/<fabric_id>/{layout, fabric.jsonl, meshes.jsonl, nodes.jsonl, connections.jsonl,
builds.jsonl, events.jsonl}`. Copy every boundary, current values and history, in source
order.

The fabric-primary writes the Fabric record, each Build once, and the Build-level events. Each
mesh-primary writes the step events of its mesh (the fabric-primary those of the steps it runs in process), its Mesh record and the status and role flags in its members' node records (Q323).
Each node writes its own config, tags, bound addresses and declarations through its property
writer, and its connection facts through the connection writer. The launching admin records the
birth's runtime and immutable launch snapshot. A node has one record, keyed by its node.id,
append-only, its latest line current; it has no revision or tenure counter (Q323).

While the fabric-primary is unavailable, the fabric-recovery executor writes only the node-record
status its operations require for a mesh with no active mesh-primary, and no Build record. That
exception grants no primary role (Q122).

The Build store retains each Build at its build_id; the events store each event at its event.id.
The generic code uses these interfaces; the application supplies the backend.

`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once. It carries no pointer to a Build (RDM's `FabricRecord` in `rdm-admin`).

Restarting every node-admin at once must not erase the facts needed to reconstruct the Fabric and continue an active Build; gossip or another live admin is never the only persistence.

```text
node-admin starts
    -> open the fabric's store root: its Build and events stores
    -> reconstruct local control-plane state
    -> the leadership election: mesh primaries and current fabric-primary
new fabric-primary
    -> reads the Builds newest first -> the current Build -> its events -> continues its remaining work while it is in progress
```

---

## 6. The leadership election is library-owned

All normative election semantics live in [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md).

This document does not restate eligibility, ordering, hierarchy, preemption, or shutdown behavior. It only consumes the election's result.

Rafka supplies lifecycle/topology facts and maps the resolved leadership view into product-facing state. Rafka does not keep an active local comparator, lowest-name rule, incumbent ladder, or second fabric-primary algorithm.

---

## 9. LifecycleTransitionPipeline

Lifecycle state must be more than a status field somebody happens to write after side work. Node, Mesh, and Fabric transitions are explicit state-machine operations with registered hooks.

```rust
pub enum LifecycleScope {
    Node,
    Mesh,
    Fabric,
}

pub struct TransitionKey {
    pub scope: LifecycleScope,
    pub from: LifecycleState,
    pub to: LifecycleState,
}

pub struct Transition<'a> {
    pub transition_id: String,
    pub target: String,
    pub key: TransitionKey,
    pub shape: &'a ShapeFacts,
}
```

`LifecycleTransitionPipeline` runs a `Transition` through its built hooks, its eligibility check and its commit; a failed blocking hook prevents the commit (RDM's `LifecycleTransitionPipeline` in `rdm-admin`). `transition_id` is internal to one transition execution and keys its once-only hook receipts; the Status calls do not carry it, and their natural idempotency keys are defined in [the envelope](node-rpc-envelope.md#status-op-0x1b-rdm).

Application work enters a lifecycle only through [Fabric and node hooks](fabric-node-hooks.md). What an Applied reply means beside gossip is [gossip.md §10](gossip.md#10-status-dissemination-versus-status-certainty); what shows a runtime dead is [deployment-provider.md §2.4](deployment-provider.md#24-exact-provider-locator-and-control-domain).

---

## 10. Readiness gates are shape-aware through the accepted Build

SN/MN/MM are minimum test rungs, not caps. Lifecycle eligibility should usually derive from the accepted Build rather than hard-coded `if shape == MM` branches.

The Fabric leaves `pending` when every Mesh the accepted Build names is ready (every planned member ready), and only then ([States](lifecycles/states.md#states), Q174). A Build that put the fabric back to pending completes when its ready state is set, not with its last step ([States](lifecycles/states.md#resize-and-a-new-executable)).

For a node-admin's `Pending -> Ready` transition, required generic hydration includes:

```text
the current Build and its accepted topology
current topology required by its role
RuntimeFacts + required runtime metadata for held births it may need to manage
provider-control-domain reachability or valid domain-local execution path
```

This prevents a newcomer from becoming Ready before it can actually act as a primary. These are lifecycle readiness prerequisites; leadership semantics are defined only in [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md).

### 10.1 Ready root / deadlock guard

An ordinary joining or restarted admin must hydrate current control state from an existing Fabric control projection before it may be Ready. Missing hydration is not permission to invent topology or self-Ready.

Only two lifecycle roots establish Ready without a live Ready primary:

```text
Day 0
    no prior fabric-primary exists
    the first node-admin is the first fabric-primary and accepts the first Build
    establish first Ready through the Day-0 lifecycle path (the sequence is lifecycles/fabric-create.md)

single-admin recovery root
    only when canonical lifecycle evidence says no live Ready admin exists
    use the defined single-admin recovery-root Ready self-apply
```

Those roots establish lifecycle eligibility only. They do not make primaries; the primaries come from the election in [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). A node-admin joining a live mesh, where a live fabric primary exists, does not use the recovery-root shortcut.

For SN/MN, the accepted Build names one mesh. For MM it names two. The same gate therefore naturally waits for the correct estate.

---

## 11. Rafka's Day-0 work is Rafka's

RDM carries no system-org bootstrap logic and no state for it (Q171). Rafka's Day-0 work (its system org and its system topics) is its own; it opens client traffic on the [`on_fabric_ready`](fabric-node-hooks.md#on_fabric_ready) notice, which gates nothing in RDM.

---

## 12. Mesh creation, a node-admin joining a live mesh, and a new node are different Build semantics

All three must be shown.

### 12.1 Create a new mesh

```text
fabric-primary accepts Build B19:
    {mesh1} -> {mesh1, mesh2}
    B19 is the current Build
```

The fabric-primary decides that mesh2 exists and what it contains; mesh2's node-admin creates mesh2's nodes. mesh2 comes into being with the `node.create` of its first node-admin ([mesh-absent arm](lifecycles/node-create.md#mesh-absent)); that is what `mesh.create` names: there is no separate op.

### 12.2 Add a node-admin to a live mesh

```text
the accepted Build still names mesh1 + mesh2
mesh1's node-admins are all lost, its members live (mesh1 holds a node, so it is alive)
    -> node.create of a node-admin into mesh1 under the same logical FabricId + MeshId
```

[Node recovery](lifecycles/node-recovery.md) owns the sequence (Q335): the fabric-primary's node investigation of the lost mesh-primary ends in a verdict, and the recovery re-creates one node-admin under the mesh's MeshId, the fabric-primary's own workflow, never a Build ([a node-admin created into a live mesh](lifecycles/node-create.md#a-node-admin-created-into-a-live-mesh)). The fabric-primary is not the local Build executor: the joining node-admin is the local executor for its own mesh, and while it is `Pending` it executes only work the fabric-primary dispatches for that mesh, never another mesh's or fabric-wide work ([Role-neutral execution](#role-neutral-execution-and-policy)). A surviving runtime in another provider control domain is handled by [deployment-provider.md §2.4](deployment-provider.md#24-exact-provider-locator-and-control-domain).

### 12.3 Delete a mesh and create another

```text
fabric-primary accepts Build B20:
    {mesh1, mesh2} -> {mesh2, mesh3}
    B20 is the current Build
```

mesh1 is deleted by `mesh.delete` and mesh3 comes into being with the `node.create` of its first node-admin. mesh3 always has a new MeshId; a MeshId is never reissued, even when the new mesh takes mesh1's name after mesh1's row is deleted. A dead mesh is never restarted: mesh3 is the mesh-absent arm of `node.create` under a new MeshId, and it does not preserve mesh1's logical identity merely to reuse its name.

### 12.4 Remove one node

```text
fabric-primary accepts Build B21:
    mesh2 {mesh2.node.north, mesh2.node.south} -> mesh2 {mesh2.node.north}
    B21 is the current Build
```

The removal is `Delete { node_ids: [mesh2.node.south's node.id] }`, the [node.delete](lifecycles/node-delete.md)
mesh2's primary executes on the fabric-primary's dispatch; the fabric-primary never commands
mesh2.node.south and never publishes its events. A delete of mesh2's mesh-primary: it first runs
[node.primary.update](lifecycles/node-primary-update.md), commanded by the fabric-primary; its successor,
now mesh2's primary, executes the delete.

### 12.5 A node on a new executable

~~~text
fabric-primary accepts Build B22:
    Create { nodes: [mesh2.node.north on executable B] }
    B22 is the current Build
~~~

A node's `executable` is the executable used to launch the node; nothing changes it after. A new
executable is a new node: a Build that adds it ([node.create](lifecycles/node-create.md), a new
node.id), at a path and name no live node holds ([build-deployment.md §3](build-deployment.md)). To keep
the old node's data, the old node is stopped ([node.stop](lifecycles/node-stop.md)) and its name changed
with [node.name.update](lifecycles/node-name-update.md) first; without it, a Delete of the old node
releases its path and name. These are primitives, each its own call. Going back is a Build of the
former topology and configuration; nothing is ever reverted (Q345). Nothing else in the fabric
changes.

---

## 13. Held-member repair when a peer is heard again

Held-member repair is a generic topology mechanism owned by [gossip.md §6](gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours), with its heal path in [§12](gossip.md#12-an-unheard-peer-mesh). Repair dials a held member with a known transport identity and address; the accepted topology alone is not dial material. Rafka adds no second repair loop beside it (§15).

---

## 14. Control endpoint ownership and a primary change

Callers reach the current fabric-primary over Node RPC at the endpoint its digest carries. An application contact such as `admin_api_base` is application config on the node record.

A fabric-primary lost mid-Build continues the current Build (§4.1); a node lost after a Build completed is a repair, never part of the Build (§1).

Leadership may recover before every application hook is complete. A resolved primary and Fabric Ready are distinct facts; election semantics are owned only by [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md).

---

## 15. Import/binding rule

The target dependency is deliberately one-way:

```text
RDM rdm-admin
    Build/Reconciler
    RuntimeFact + current runtime metadata + provider-control-domain adoption contract
    the leadership election
    executor rules
    lifecycle/deployment contracts
        |
        v
Rafka adapters
    shape mapping into Builds
    the current-Build read + the single-writer (is_fabric_primary) Build call
    production backends for imported fabric/mesh/nodes/connections/builds storage interfaces
    DeploymentProvider implementation / domain routing
    current runtime metadata mapping
    lifecycle hooks/status persistence
    auth/cert/Rafka-time policy
    topology/public-view mapping
        |
        v
Rafka protocol/domain consumers
```

An import boundary is eligible when it shows:

1. logical FabricId and transport EndpointId are distinct everywhere in target APIs;
2. exactly one fabric-primary topology writer and one current Build exist, and no node command or repair runs as part of a Build; no node-admin other than the fabric-primary accepts a topology change, and no desired topology stands beside the accepted Build, including imported generic desired-state/revision machinery promoted into Rafka's topology;
3. one Build/Reconciler implementation exists; no active Rafka-local `nodes_to_reach_shape`/shape reconciliation competes with it;
4. one election implementation exists; no Rafka-local comparator or primary ladder, and no product protocol recomputing a mesh/fabric primary, stands in for it;
5. one current RuntimeFact + runtime-metadata publication/adoption contract exists; no Rafka-local runtime adoption depends on launcher-private completed Build receipts, and no launcher-private `data_dir`/runtime-management metadata keeps a successor from managing a current birth;
6. provider/bootstrap produces the locator and the node publishes the one normalized RuntimeFact;
7. one provider-control-domain execution contract exists, and host locality never alters the election;
8. one exact-runtime provider/death-evidence contract exists, including Day-0 adoption;
9. one Ready-root rule exists: ordinary admins hydrate, and only the Day-0 or single-admin recovery root establishes Ready without a live Ready primary (§10.1);
10. one held-member repair mechanism exists (§13);
11. one Direct/Proxy route-truth source exists ([connections.md](connections.md));
12. one Node RPC certainty implementation exists;
13. no Rafka-local executor rule overrides the imported recovery primary/executor semantics, and Rafka's adaptation adds policy, storage and product semantics without a second generic control brain.

An unmet item is a defect. If two layers can independently answer "who is primary?", "what should exist?", "how does a new admin learn the accepted topology?", "which exact runtime is this current birth?", "what current runtime metadata is required to manage it?", "which provider domain owns it?", "who executes this recovery?", "may this admin self-Ready?" or "is this runtime dead?", the ownership cut is incomplete.

A compatibility wrapper is allowed only when it delegates to the imported mechanism and has no independent state/decision algorithm.

---

## 16. Test ownership

Framework mechanisms are tested once by the Mesh product feature that owns them; a downstream application feature consumes those guarantees and tests only the application semantics it adds.

RDM's role executables, the demo's `rshape-broker`, `rshape-gateway`, `rshape-compute` and `rshape-node-admin` (`demo/`), are role processes on `rdm-node` that carry no Rafka product behaviour; they are not Rafka's product roles. A generic test runs node-admins, the RPC test nodes of `rdm-test` and these role processes, and asserts no Rafka customer or product state; that evidence stays in Rafka.

The RDM-generic tests of this page's mechanisms:

- an all-node-admin restart kills every node-admin at once and restarts them from durable storage: Fabric/Mesh/Node/Connection state is recovered, the Builds read newest first still name the active Build current, its evidence is recovered, exactly one fabric-primary continues it without repeating completed work, and no new Build or competing topology appears (§5.3);
- a fabric-primary death mid-Build: the successor recovers and continues the same Build (§4.1);
- process/container evidence demonstrates same-domain successor control and refusal of cross-domain PID/container-ID misuse ([deployment-provider.md §2.4](deployment-provider.md#24-exact-provider-locator-and-control-domain)); a multi-host test demonstrates remotely controllable locators or domain-local exact-runtime execution without changing the election ([deployment-provider.md §1](deployment-provider.md#1-deployment-provider-is-environment-policy));
- elastic shape/grow/shrink tests assert accepted per-kind counts or settled shape rather than requiring surviving names to be contiguous; identity/leadership/restart tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
- leadership test uses [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md) as its only expected-result specification;
- private maps are not E2E truth.

Topology, hydration, late-admin adoption and repair tests are [gossip.md §14](gossip.md#14-required-tests).
