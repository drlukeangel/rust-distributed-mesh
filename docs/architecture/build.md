# Build: the topology engine, leadership, lifecycle transitions and state adapters

This page owns the Build engine (the accepted topology, the Build/attempt/claim model and `BuildStateAdapter`), the node-admin storage interfaces and crash safety, and the RDM/Rafka boundary with its import rule. The deployment provider and the RuntimeFact contract are [deployment-provider.md](deployment-provider.md).

It defines the generic control-plane contract proven in `rust-distributed-mesh` (RDM), an independently owned and versioned reusable distributed-system substrate that Rafka imports at an exact revision. Rafka is its first customer: RDM owns how distributed execution works, and Rafka owns what its product asks nodes to do.

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
    Build intent/attempt/receipt/completion evidence and history

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

- implementations of the four storage interfaces, fabric, mesh, nodes and connections, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6);
- product Build-input/shape normalization and each node's explicit desired properties;
- deployment-provider bindings, resolved launch/listener configuration, and mapping of customer
  runtime-management metadata into the imported current-runtime projection;
- current topology/runtime fact feeds and projection/public-API mappings;
- product authorization and the hooks of [Fabric and node hooks](fabric-node-hooks.md).

RDM retains Build compilation/reconciliation and attempts, the leadership election,
executor selection, generic lifecycle eligibility/commit/receipts, readiness roots, recovery,
drift, exact-runtime adoption/death proof, provider-domain execution and connection mechanics.
Customer seams supply policy or facts; they MUST NOT substitute a second implementation of
those algorithms. Hooks are registered before the registry is built, idempotent and bounded, and report
failure through the lifecycle receipt path. A hook requesting exact synchronous execution
of gateway-owned application behavior uses a typed Rafka Op on the exact gateway rather than
importing the gateway implementation. Durable asynchronous job/event contracts and explicitly
authorized read projections are valid under the same owner boundary.

RDM's only surface is Node RPC: the `node`, `mesh`, `fabric` and `build` op families and the typed objects over them ([Node RPC envelope](node-rpc-envelope.md#typed-object-contract)). No RDM crate serves HTTP, depends on an HTTP server or defines a route. An application that wants HTTP routes builds them in its own crate as thin callers over the same typed objects.

Shared typed contracts, genuinely role-neutral domain models and RDM substrate packages are allowed across roles. A crate that holds role-owned executable handlers, stores or boot registrations does not become a shared model by being named `core` or `shared`. Same-role implementation composition is allowed.

### Role-neutral execution and policy

Who executes follows topology and lifecycle ownership, not application NodeKind.
The owning Mesh executor realizes ordinary members and sibling node-admins; an exact Pending
bootstrap admin or a node-admin joining a live mesh executes only its own Mesh under the fabric-primary. Fabric-wide and
outside-Mesh retirement rules remain generic control decisions. A general rule equivalent to
`if kind == RpcNode { mesh executor } else { fabric executor }` is forbidden. Provider-domain
locality may change execution location, never topology or the election.

RDM may carry an opaque or typed node class required by generic algorithms, but MUST NOT
require a closed Rafka Broker/Gateway/Compute taxonomy as its generic abstraction. Rafka
owns its role vocabulary and ingress policy. Generic `StorageMeta` validity and pipeline
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
| `rdm-admin` | generic Fabric/Mesh/Node control: Build/Reconciler and drift trigger, the leadership election, the four storage interfaces, fabric, mesh, nodes and connections, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6) with memory and durable-file implementations, and the lifecycle, deployment, runtime-adoption, provider-domain and recovery contracts |
| `rdm-client` | the typed client and DTOs for the node-admin control API |
| `rdm-test-scenario` | provider-neutral scenario, evidence and replay contracts |
| `rdm-test` | the generic RPC proof node, proof protocols and probes |

the chaos kit (`rdm-test`'s `faults` modules), the fault backends with typed outcomes, is an RDM proof package used by
`rdm-test-scenario`. RDM packages import no Rafka domain crate.

---

## 1. Build is the only topology mutation engine

A submitted BuildIntent/change request is **input to compilation**, not the topology itself.

The current fabric-primary applies that change once to the complete topology of the current Build (§4) and produces the complete explicit next-topology map.

~~~text
current Build = B (completed)

submitted change
    -> apply once to B.topology
    -> exact complete next topology
    -> persist new Build B' (written once; current once durable)
    -> reconcile B'.topology
~~~

The accepted topology names the exact path.names that should exist, grouped by mesh. Counts may remain product-facing resize input, but the compiler resolves a count change to exact paths before the Build is persisted.

Each node's create spec carries the executable the node runs ([build-deployment.md](build-deployment.md)); a request creates a node by naming its `path.name` and its properties, the compiler holds the executable on the path's entry before the Build is persisted, and a created path with none is refused by name. Counts are the product's door's vocabulary, expanded into named paths before the typed call; RDM mints a `path.name` only for a create it originates itself.

**The create spec is the node's properties, flat (Q166).** A node entry names its `path.name` and the
node's own properties under the [node properties](node-rpc-envelope.md#node-properties) names:
`executable`, `storage`, `placement`, `tags` and `config`. There is no nested configuration type;
`config` is the application's map and nothing else (RDM never reads it).

**The Build applies the diff.** The reconciler compares each node's entry with what the node's row
records and applies every property that differs, each by its own operation. It decides nothing else:

| What differs | Applied as |
|---|---|
| a path added / removed | `node.create` / `node.delete`: a resize |
| `executable` | the node's replacement: a new birth at the same `path.name` ([build-deployment.md §3](build-deployment.md#3-changing-a-nodes-executable)) |
| `config` | in place, as a caller's `node.config.update` applies a map: the row first, then the node. The Build's map supersedes the node's current one, whoever set it: every write is a PUT, and the submitter reads the node's current config (`nodes.get`) before it composes (Q236) |
| `tags` | in place: the node's tag rows |
| `storage`, `placement` | onto the row, effective at the node's next birth; with an executable change they ride that re-birth (Q157) |

Only a resize or a replacement puts the fabric back to pending ([states.md](lifecycles/states.md#resize-and-executable-replacement)); a change of `config`, `tags`, `storage` or `placement` alone leaves it ready. A node's `config` and `tags` change at any time, between Builds and while one runs, through `node.config.update` and `node.tags.*`; the last write wins: a property a Build applied to a node, at its create's claim or by its own operation, is not applied again by a later attempt of that Build.

~~~text
B.topology:
  mesh1:
    mesh1.admin.alpha
    mesh1.admin.beta
    mesh1.rpc.north
    mesh1.rpc.south
    mesh1.rpc.east

submit: remove mesh1.rpc.south

B'.topology:
  mesh1:
    mesh1.admin.alpha
    mesh1.admin.beta
    mesh1.rpc.north
    mesh1.rpc.east
~~~

The reconciler never later decides which node "count 2" meant. It diffs the stored exact topology map against current realized/observed state.

The submitted change may remain on B' as audit/history for builds.get, but it is never replayed as topology and is never read to decide reconciliation after B' exists.

Exactly one current fabric-primary accepts topology-changing requests. A non-primary refuses by name. A second topology-changing request while the current Build is actively reconciling is refused with a named build-in-progress refusal naming the current Build; RDM does not implement Build supersession/cancellation. The one exception is fabric.delete, which may interrupt the Build in progress because its target is nothing. Its acceptance record is the durable intent: once written, the interrupted Build opens no further operation and is never resumed, and a successor fabric-primary reads the delete as the current Build and continues the teardown. Operations already admitted at an executor finish or fail within their existing bounds and their launch evidence is retained; a fabric state the fabric-primary is setting ends at the acceptance, its hook's future dropped. When every open attempt of the interrupted Build has ended, its run receives `Failed { step, reason }` naming the delete, and the teardown's steps open mesh by mesh under the one-attempt-per-mesh rule. An operation that fails to end within its bound fails the delete's attempt by name, the intent retained; nothing reconciles the interrupted Build again. No other Build ends another. One attempt is in flight per mesh, not per fabric: a topology Build's operation on mesh X and mesh X's drift attempt never both open; the loser is refused build-in-progress naming the open attempt (R-BI7).

Every change to the topology is its own Build, submitted through build.create with its own build_id: node.create, node.delete, Rafka's replace steps (one Build of node primitives, [node replace](lifecycles/node-replace.md)), mesh.delete and fabric.delete. node.stop, node.start, node recovery, a primary change and runtime drift leave the topology unchanged: each is an attempt of the current Build, never a new Build. One node.create is recovery, not a change to the shape: the node-admin mesh investigation creates into a live mesh that lost its node-admins runs on the same accepted Build, as its next attempt, under the mesh's own MeshId ([mesh-investigation.md](lifecycles/mesh-investigation.md) steps 5-6, [fabric-primary.md](fabric-primary.md#13-node-admin-storage-restart-and-recovery)).

Workflow input is execution state, not topology: node.stop and node.start on an exact birth (node id and incarnation) and node recovery (`node.delete` then `node.create` at the same path.name, keyed on the exited birth) carry the exact birth they act on, which fences them against a stale birth. They never change the accepted topology map.

Every execution attempt re-reads the accepted topology and current realized/observed state and computes remaining work. Completed step receipts prevent repeating already-satisfied work; a Build never resumes from a mutable instruction pointer. A step receipt is `(build_id, attempt, operation, step)` with its output; the exact birth it acted on is the attempt's fenced action. Each workflow's `AttemptOpened` starts a run; a resume after the executor is lost (R-BI4) continues that run; a receipt is reused only within its run. A later attempt of the same run replays a held Complete receipt for the same operation and step. A receipt of an earlier run at the same path.name satisfies nothing for a later run: a second recovery at the same name opens its own run and runs its own steps.

Forbidden:

- application entry points directly spawning or killing nodes outside Build execution;
- a separate Rafka shape/resize/recovery engine beside imported Build;
- any node-admin other than the current fabric-primary accepting a topology change;
- treating BuildIntent/change-request data as the accepted topology;
- storing only counts and letting each executor independently choose which logical path to add/remove;
- minting a new Build for node.stop, node.start, node recovery, a primary change, or runtime drift against unchanged topology;
- accepting a second topology mutation while the current Build is actively reconciling;
- replaying the submitted change to reconstruct topology;
- blindly replaying operations already satisfied in current state.

Rafka shape vocabulary may remain product-facing input. It compiles once into the exact complete topology map stored in the accepted Build.

---

## 2. Public Build calls

RDM creates, drives, reads and deletes Builds only through the
[Build carrier](node-rpc-envelope.md#build-op-0x20-rdm), which defines build.create,
build.attempt.run, builds.get and build.delete, their direction, streams and refusals.
node.create, node.stop, node.start and node.delete are submitted through
build.create; §1 decides whether an input compiles a new Build or opens an attempt of
the current one. Application bootstrap and fault injection are application work, not
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

- the current Build is found by reading the Builds last in, first out: the first one that is in progress or completed. In progress, it is the Build being driven, and it is resumed; completed, it is the known state, and it stays completed when a later attempt of its unchanged topology fails. A failed Build is skipped: it is never current, nothing resumes it on its own, and a caller resubmits it;
- every node-admin reads the current Build from `builds.storage`; the Fabric row carries no pointer to it;
- a leadership change does not change it;
- later runtime loss is reconciled against it.

Persisted copies are not writers: several node-admins may each hold the same Fabric and Build facts for recovery, and only the current fabric-primary, as the election names it, may accept the next topology mutation.

A topology-changing request compiles the current accepted explicit path map plus the requested change into a new complete Build. Create/remove/add/resize select exact logical paths during compilation. node.stop, node.start and node recovery are attempts of the current Build and keep that exact topology map unchanged.

### 4.1 Build identity is active reconciliation state

The fabric-primary is the one driver of a Build; an attached caller does not own its execution.

While a Build is in progress, `builds.storage` carries enough information for any eligible successor to continue the same work:

```text
build_id (the current Build)
complete explicit topology map, each node's create spec with its executable
submitted change history (optional; never the plan)
current executor/attempt evidence
completed step receipts and outputs
```

A primary or executor change during that active Build keeps the same `build_id`. A primary change is not a topology mutation and never mints a Build. An attempt lost with its executor (no terminal receipt, the executor unheard) leaves the Build in progress (`Running`), and a successor or drift pass claims the next attempt (`attempt + 1`); only an attempt that ends with a named terminal failure (`Failed { step, reason }`) makes the Build failed. An opening's first step is the fabric-primary's [`on_build_started`](fabric-node-hooks.md#on_build_started) notice, sent after the claim of the opening's first attempt is recorded; it writes no receipt and gates nothing. The ending's last step is the fabric-primary's [`on_build_ended`](fabric-node-hooks.md#on_build_ended) notice to the application, sent after the terminal (`Complete` or `Failed { step, reason }`) is recorded; it writes no receipt and gates nothing.

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

Later drift after B123 has already converged is an execution problem, not a topology change:

```text
Build B123 completed (the current Build)

later exact runtime death of a node
    -> node recovery: node.delete of the lost node, node.create at the same path.name
    -> accepted topology unchanged
    -> the mesh-primary of the member's mesh opens the next attempt of B123 on its mesh's rows, claims and runs it itself (R-BI7); the Build stays completed, and the fabric-primary reads the attempt's verdict from the Build journal
    -> perform only unsatisfied work
    -> append receipts to B123 in builds.storage
    -> B123 converged again
```

[Node recovery](lifecycles/node-recovery.md) owns who detects the loss, proves the exit and submits the recovery.

Each topology change is its own Build with its own build_id (§1). Proven runtime drift against an unchanged accepted topology opens a new attempt of the current Build; it never mints a new Build. A Build is the durable accepted topology plus the history of attempts to realize it.

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

The accepted topology is the Build's; the observed topology follows the executor's steps and the provider's proof. The moment-by-moment table of what every node observes while a node is removed, and which status and overlay stop application routing, is [States: Routing](lifecycles/states.md#routing); the removal's steps are [node.delete](lifecycles/node-delete.md). The claim (the fabric-primary's for a topology attempt, the mesh-primary's for a drift attempt in its mesh), the fabric-primary's dispatch of build.attempt.run to the executor `executor_for` names, the hand-off between executors and a successor's resumption are the [Build carrier](node-rpc-envelope.md#calls-and-direction)'s.

---

### 4.4 The gates before an operation

Every operation on the topology, a Build or a standalone node.stop, node.start,
node.connections.delete or node.delete, passes the fabric-primary's gates in this order before
anything runs. Each gate reads one source. A gate that fails refuses by name and nothing is written.

| Gate | Reads | Refusal |
|---|---|---|
| The receiver is the fabric-primary | its own view | `not-fabric-primary`, naming the fabric-primary |
| The current Build is settled | the Build store (R-BI3) | `build-in-progress`, naming the Build; fabric.delete excepted (§1) |
| The node's mesh holds no attempt of its own | the Build store (R-BI7) | `build-in-progress`, naming the mesh |
| A node stands at the path | the fabric-primary's view by name; a `.old` path by node id through the row index | `unknown-node` |
| The node's state admits the action: a start needs a stopped birth, a stop or a connections delete a live one, a delete a stopped one, one whose exit is proven, or a live one, which its own sequence stops first ([node.delete](lifecycles/node-delete.md) Live target) | the fabric-primary's view, which holds a peer mesh's births as that mesh's primary publishes them on the backbone ([gossip §3](gossip.md#3-hierarchical-topology)) | `node-not-live` |
| The node's mesh is heard | the fabric-primary's view | refused by name, naming the mesh and how long it has gone unheard (R-D1) |
| A primary's stop, connections delete or delete has a successor | the fabric-primary's view | refused by name, naming the primary |
| The attempt is claimed | the Build store | `attempt-taken` |

The operation carries the exact birth the gates pinned (node id and incarnation). The mesh-primary
that runs it verifies that birth again against its own view at execution, so a birth replaced
between acceptance and execution is refused there, by name.

---

## 5. BuildStateAdapter

Build mechanics must not depend on one storage technology.

```rust
#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError>;
    async fn publish_fabric(&self, record: &FabricIdentity) -> Result<(), BuildStateError>;
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError>;
    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError>;
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError>;
    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError>;
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError>;
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError>;
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError>;
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError>;
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError>;
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
    async fn note_blocked(&self, build_id: &BuildId, mesh: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str);
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError>;
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError>;
}
```

The exported semantic vocabulary (`BuildFact` variants) is append/fold friendly:

```text
BuildAccepted
AttemptOpened
BuildAttemptClaim
BuildStepReceipt
BuildAttemptReceipt
```

These facts describe reconciliation execution. The current Build is read from them newest first (§4, `current_build()` over the acceptance-order index, with `newest_accepted_at_ms()` as the acceptance high-water mark of every state and `recovery_of` answering the point read by exited birth); they also record what happened while executing it. They are not a second topology writer and not the current-runtime store. Completed Build records remain valid history for recovery, API views, diagnostics, audit and chaos evidence.

### 5.1 RDM adapters

RDM proves the contract without inventing a broker-backed database:

- `MemoryBuildStateAdapter` for unit/functional proof;
- fabric-projected **active** Build facts for the fabric recovery workflow's executor;
- local-file journal for same-node-admin restart, replay, and evidence artifacts.

Single-writer safety comes from one writer per record: the fabric-primary writes every Build record (acceptance, attempts, claims, step receipts, results) through the configured Build storage; a mesh-primary runs its mesh's attempt and reports each receipt up the attempt's `build.attempt.run` stream, and writes only its own mesh's rows. With that come idempotent operation keys and stable logical topology identities. When an unheard peer mesh is heard again, the election converges and the current fabric-primary reconciles reality rather than trusting attempt history.

### 5.2 Rafka adapter

Rafka maps the same semantic contract onto its durable application/control infrastructure.

Rafka must not persist one mutable Build row and increment `attempt` by read-modify-write. Instead:

```text
BuildAccepted
    insert-and-fail on build_id

AttemptOpened(build_id, mesh, attempt)
    insert-and-fail on (build_id, mesh, attempt)

BuildAttemptClaim(build_id, mesh, attempt)
    insert-and-fail claim on (build_id, mesh, attempt)

BuildStepReceipt(...)
    append

BuildAttemptReceipt(...)
    append

current attempt
    folded maximum/winning claim

current Build
    current_build() over the acceptance-order index

recovery index
    recovery_of(path, node_id, incarnation)
```

The claim is one record on the fabric's shared store root and names its claimant: the insert-and-fail is on the store, never on an admin's own log, and the mesh's in-flight row taken by another claimant for the same `(build_id, mesh, attempt)` is held, never won, so a second claimant of one attempt loses and runs nothing; an admin's log holds the record the store decided (Q142). The next attempt is claimed after the running attempt's terminal receipt (Complete, Failed or HandedOff), or, when the executor is lost with the attempt unfinished, after the provider's inspection of the executor's exact runtime answers `exited` or `os-stopped` ([node investigation](lifecycles/node-investigation.md)): `running`, `unproven` and silence never let another claimant take it, and an os-stopped executor that resumes after the next attempt is claimed is a deleted birth whose writes every reader refuses ([node recovery](lifecycles/node-recovery.md)). A live run is reattached, never re-claimed: the caller resubmits the same build_id through build.create and the executor's build.attempt.run replays the held receipts ([Build carrier](node-rpc-envelope.md#build-op-0x20-rdm)); builds.get is a read of the evidence, not a reattachment. Claims are the claimant's (the fabric-primary's for a topology attempt, the mesh's mesh-primary's for a drift attempt inside its mesh) and are insert-and-fail on `(build_id, mesh, attempt)`, which prevents two claims of one attempt; a call carrying a claim that is not the current folded attempt of the receiver's projection is refused `StaleClaim` naming the held and carried attempts, and `adopt_claim` moves an admin's view only to a claim its claimant decided Won, so a delayed dispatch or receipt of an earlier attempt never reopens a terminally closed run. The attempt counter is never read-modify-written.

Rafka persists each accepted Build (its complete topology), written once, with its execution evidence behind `builds.storage`; the Fabric row carries no pointer to a Build. A `system.ops`-backed implementation is one option; the reconciler knows the storage interface, never a specific topic.

The current Build cannot be deleted (build.delete refuses by name); an older Build is deletable history once a newer Build is current.

A recovery-critical Build fact (the accepted Build, an attempt claim, a step receipt, a completion) is not committed until it is durable in storage that survives the loss of the current fabric-primary. Storage local to the active fabric-primary alone is insufficient for failover. The Build facts are the Build journal on the fabric's shared store root (§5.3), each record at its own key, written by the fabric-primary, so a successor reads them there as it becomes fabric-primary; it is the only admin that reads them (Q227), and every other admin holds what reached it through FetchBuildFacts (`0x1F`) and `build.attempt.run`.

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

Control-plane state lives behind the four storage interfaces, fabric, mesh, nodes and connections, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6). In RDM's file adapter they open under `<store_dir>/fabrics/<name>` and one store root per fabric, `<store_dir>/<fabric_id>/{layout, fabric.jsonl, meshes.jsonl, nodes.jsonl, connections.jsonl, builds.jsonl, locks/}`: one append-only JSONL file per storage boundary, every write one line carrying the whole record with its format and key, the latest complete line for a key the current state, nothing ever removed, indexes folded in memory, the attempt in flight and every name claim a create-exclusive lock under `locks/`, and a layout marker a root is refused without (R-SR1 to R-SR5). One writer per key: the fabric-primary writes the Fabric record and every topology attempt's Build facts, with one exception: while the fabric-primary is unavailable, the [fabric recovery](lifecycles/fabric-recovery.md) workflow's executor writes that attempt's Build records and, while the target mesh has no active mesh-primary, the mesh and node records that attempt's operations require (the lost node-admin's Deleted row, the re-created node's claim and row), nothing else; this does not make it primary, and ordinary mesh writing passes to the restored mesh-primary when it assumes the role (Q122); each mesh-primary its mesh's record, its mesh's node records and tag rows, and the name locks of both (where `is_primary` and `is_fabric_primary` record the election's result); the fabric-primary also writes each mesh's drift attempts' Build facts, the mesh in the key (Q118); the launching admin a birth's runtime row; each node its own connection facts. The Build journal holds one record per fact at the fact's own key: one row at `build_id`, one at `(build_id, mesh, attempt)`, one claim at `(build_id, mesh, attempt)`, one receipt per step of an attempt. No record is written twice and no two writers write one key. The generic code knows the interfaces, never a specific database; Rafka binds its system topics to them.

`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once; the fabric's name is claimed at `<store_dir>/fabrics/<name>` above the per-fabric roots, and a deleted fabric releases it. It carries no pointer to a Build (RDM's `FabricRecord` in `rdm-admin`).

Restarting every node-admin at once must not erase the facts needed to reconstruct the Fabric and continue an active Build; gossip or another live admin is never the only persistence.

```text
node-admin starts
    -> open the fabric's store root and this admin's Build journal
    -> reconstruct local control-plane state
    -> the leadership election: mesh primaries and current fabric-primary
new fabric-primary
    -> reads the Builds newest first -> the current Build -> continues its remaining work while it is in progress
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

Application work enters a lifecycle only through [Fabric and node hooks](fabric-node-hooks.md). What an Applied reply means beside gossip is [gossip.md §10](gossip.md#10-status-dissemination-versus-status-certainty); what proves a runtime dead is [deployment-provider.md §2.4](deployment-provider.md#24-exact-provider-locator-and-control-domain).

---

## 10. Readiness gates are shape-aware through the accepted Build

SN/MN/MM are minimum proof rungs, not caps. Lifecycle eligibility should usually derive from the accepted Build rather than hard-coded `if shape == MM` branches.

The Fabric leaves `pending` when every Mesh the accepted Build names is ready (every planned member ready), and only then ([States](lifecycles/states.md#states), Q174). A Build that put the fabric back to pending completes when its ready state is set, not with its last step ([States](lifecycles/states.md#resize-and-executable-replacement)).

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
    only when canonical lifecycle proof says no live Ready admin exists
    use the defined single-admin recovery-root Ready self-apply
```

Those roots establish lifecycle eligibility only. They do not make primaries; the primaries come from the election in [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). A node-admin joining a live mesh, where a live fabric primary exists, does not use the recovery-root shortcut.

For SN/MN, the accepted Build names one mesh. For MM it names two. The same gate therefore naturally waits for the correct estate.

---

## 11. Rafka's Day-0 work is Rafka's

RDM carries no system-org bootstrap logic and no state for it (Q171). Rafka's Day-0 work (its system org and its system topics) is its own; it opens client traffic on the [`on_fabric_ready`](fabric-node-hooks.md#on_fabric_ready) notice, which gates nothing in RDM.

---

## 12. Mesh creation, a node-admin joining a live mesh, and replacement are different Build semantics

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

[Mesh investigation](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh) owns the sequence: the fabric-primary's carrier-edge-lost verdict creates one node-admin under the mesh's MeshId and runs its work as attempts of the same Build (§4.1). The fabric-primary is not the local Build executor: the joining node-admin is the local executor for its own mesh, and while it is `Pending` it executes only work the fabric-primary dispatches for that mesh, never another mesh's or fabric-wide work ([Role-neutral execution](#role-neutral-execution-and-policy)). A surviving runtime in another provider control domain is handled by [deployment-provider.md §2.4](deployment-provider.md#24-exact-provider-locator-and-control-domain).

### 12.3 Replace a mesh intentionally

```text
fabric-primary accepts Build B20:
    {mesh1, mesh2} -> {mesh2, mesh3}
    B20 is the current Build
```

mesh1 is deleted by `mesh.delete` and mesh3 comes into being with the `node.create` of its first node-admin. mesh3 always has a new MeshId; a MeshId is never reissued, even when the new mesh takes mesh1's name after mesh1's row is deleted. A dead mesh is never restarted: mesh3 is the mesh-absent arm of `node.create` under a new MeshId, and it does not preserve mesh1's logical identity merely to reuse its name.

### 12.4 Remove one node

```text
fabric-primary accepts Build B21:
    mesh2 {mesh2.rpc.north, mesh2.rpc.south} -> mesh2 {mesh2.rpc.north}
    B21 is the current Build
```

The removal is the [node.delete](lifecycles/node-delete.md) of mesh2.rpc.south, which mesh2's primary
executes under the fabric-primary's claim; the fabric-primary never commands mesh2.rpc.south and never
publishes its events. A delete of mesh2's mesh-primary: it first runs
[node.primary.update](lifecycles/node-primary-update.md), commanded by the fabric-primary; its successor,
now mesh2's primary, executes the delete.

### 12.5 Replace a node's executable

~~~text
fabric-primary accepts Build B22:
    mesh2.rpc.north: executable A -> executable B
    B22 is the current Build
~~~

The topology names the same paths. mesh2.rpc.north's entry names executable B while its row records A,
so the reconciler replaces it: [node.delete](lifecycles/node-delete.md) then [node.create](lifecycles/node-create.md)
at the path, a new node.id, under mesh2's primary and the guards of [build-deployment.md §3](build-deployment.md).
A primary of any type moves first. Nothing else in the fabric changes.

---

## 13. Held-member repair when a peer is heard again

Held-member repair is a generic topology mechanism owned by [gossip.md §6](gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours), with its heal path in [§12](gossip.md#12-an-unheard-peer-mesh). Repair dials a held member with a known transport identity and address; the accepted topology alone is not dial material. Rafka adds no second repair loop beside it (§15).

---

## 14. Control endpoint ownership and a primary change

Callers reach the current fabric-primary over Node RPC at the endpoint its digest carries. An application contact such as `admin_api_base` is application config on the node record.

A fabric-primary lost mid-Build and runtime drift after a Build completed both continue the current Build (§4.1).

Leadership may recover before every application hook is complete. A resolved primary and Fabric Ready are distinct facts; election semantics are owned only by [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md).

---

## 15. Import/binding rule

The target dependency is deliberately one-way:

```text
RDM rdm-admin
    Build/Reconciler + drift trigger
    RuntimeFact + current runtime metadata + provider-control-domain adoption contract
    the leadership election
    executor/attempt rules
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

An import boundary is eligible when it proves:

1. logical FabricId and transport EndpointId are distinct everywhere in target APIs;
2. exactly one fabric-primary topology writer and one current Build exist, with one drift mechanism that opens attempts of the current Build, driven by the mesh-primary of the drifting mesh; no node-admin other than the fabric-primary accepts a topology change, and no desired topology stands beside the accepted Build, including imported generic desired-state/revision machinery promoted into Rafka's topology;
3. one Build/Reconciler implementation exists; no active Rafka-local `nodes_to_reach_shape`/shape reconciliation competes with it;
4. one election implementation exists; no Rafka-local comparator or primary ladder, and no product protocol recomputing a mesh/fabric primary, stands in for it;
5. one current RuntimeFact + runtime-metadata publication/adoption contract exists; no Rafka-local runtime adoption depends on launcher-private completed Build receipts, and no launcher-private `data_dir`/runtime-management metadata keeps a successor from managing a current birth;
6. provider/bootstrap produces the locator and the node publishes the one normalized RuntimeFact;
7. one provider-control-domain execution contract exists, and host locality never alters the election;
8. one exact-runtime provider/death-proof contract exists, including Day-0 adoption;
9. one Ready-root rule exists: ordinary admins hydrate, and only the Day-0 or single-admin recovery root establishes Ready without a live Ready primary (§10.1);
10. one held-member repair mechanism exists (§13);
11. one Direct/Proxy route-truth source exists ([connections.md](connections.md));
12. one Node RPC certainty implementation exists;
13. no Rafka-local executor rule overrides the imported recovery primary/executor semantics, and Rafka's adaptation adds policy, storage and product semantics without a second generic control brain.

An unmet item is a defect. If two layers can independently answer "who is primary?", "what should exist?", "how does a new admin learn the accepted topology?", "which exact runtime is this current birth?", "what current runtime metadata is required to manage it?", "which provider domain owns it?", "who executes this recovery?", "may this admin self-Ready?" or "is this runtime dead?", the ownership cut is incomplete.

A compatibility wrapper is allowed only when it delegates to the imported mechanism and has no independent state/decision algorithm.

---

## 16. Test ownership

Framework mechanisms are proven once by the Mesh product feature that owns them; a downstream application feature consumes those guarantees and tests only the application semantics it adds.

RDM's role executables, the demo's `rshape-broker`, `rshape-gateway`, `rshape-compute` and `rshape-node-admin` (`demo/`), are role processes on `rdm-node` that carry no Rafka product behaviour; they are not Rafka's product roles. A generic proof runs node-admins, the RPC proof nodes of `rdm-test` and these role processes, and asserts no Rafka customer or product state; that proof stays in Rafka.

The RDM-generic proofs of this page's mechanisms:

- an all-node-admin restart kills every node-admin at once and restarts them from durable storage: Fabric/Mesh/Node/Connection state is recovered, the Builds read newest first still name the active Build current, its evidence is recovered, exactly one fabric-primary continues it without repeating completed work, and no new Build or competing topology appears (§5.3);
- a fabric-primary death mid-Build: the successor recovers and continues the same Build (§4.1);
- process/container proof demonstrates same-domain successor control and refusal of cross-domain PID/container-ID misuse ([deployment-provider.md §2.4](deployment-provider.md#24-exact-provider-locator-and-control-domain)); a multi-host proof demonstrates remotely controllable locators or domain-local exact-runtime execution without changing the election ([deployment-provider.md §1](deployment-provider.md#1-deployment-provider-is-environment-policy));
- elastic shape/grow/shrink tests assert accepted per-kind counts or settled shape rather than requiring surviving names to be contiguous; identity/leadership/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
- leadership proof uses [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md) as its only expected-result specification;
- private maps are not E2E truth.

Topology, hydration, late-admin adoption and repair proofs are [gossip.md §14](gossip.md#14-required-proofs).
