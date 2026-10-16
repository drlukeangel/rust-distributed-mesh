# Deployment provider, RuntimeFact and exact runtime proof

This page owns the deployment provider: the contract every provider satisfies, the two providers RDM ships (a process provider over pids and a container provider over a Docker daemon), the RuntimeFact each birth publishes, the exact runtime proof, the runtime state the provider reports, and the storage rule for a birth's data dir. The Build engine that drives the provider, the storage interfaces and crash safety are [build.md](build.md). The sequences that use the provider are the workflow pages in the [lifecycle index](fabric-mesh-node-lifecycles.md); the one primitive that reads the provider is [node.runtime.inspect](lifecycles/node-runtime-inspect.md).

It is part of the generic control-plane contract proven in `rust-distributed-mesh` (RDM) and imported by Rafka ([build.md](build.md)).

**The one sentence.** The provider launched the runtime and is the only thing that can see it. It reports what it sees of the exact runtime it launched or adopted: running, suspended by the operating system, exited with its code, or unknown. It never infers; the mesh never infers what the provider can read. Commands to a node travel on Node RPC; the provider is observational except for its one explicit act, terminating the exact runtime it is asked to.

## 1. Deployment provider is environment policy

The first node-admin bootstrap chooses the deployment mechanism with `MESH_SPAWN_TYPE=process|container`.

It is parsed once into:

```rust
pub enum ProviderKind {
    Process,
    Container,
}
```

Unknown values are a bootstrap refusal. They never silently fall back to process.

The established provider becomes fabric policy and is inherited by later node-admins and nodes. A Build request does not choose its provider. A joining admin whose local bootstrap value conflicts with established fabric policy refuses deployment authority by name.

A runtime locator is meaningful only inside the **provider control domain** that owns it. The provider policy makes both the exact runtime locator and its control domain available as current control facts.

The two providers are local:

```text
process provider
    control domain = one host / pid namespace

container provider
    control domain = one reachable Docker daemon
```

A provider whose runtime handle only works inside the original launcher's private memory is incompatible with preemptible leadership. A pid or a container id is never assumed meaningful across hosts.

A multi-host provider must take one of two shapes; both are constraints on any provider, not a promise that one is built:

```text
A. remotely controllable provider domain
   any eligible authority can adopt and control the published locator

B. domain-local execution
   the current authority stays the control-plane authority
   the exact runtime operation is routed to an executor that owns the published control domain
```

What is built of this today is the refusal: a locator from another control domain is refused by name (`foreign-control-domain`), never acted on as if it were local. Authority and deployment locality are separate. Host or provider locality may constrain readiness or execution routing, never leadership ordering.

---

## 2. The provider contract

Node-admin remains the brain. A provider is only the runtime hand.

```rust
#[async_trait]
pub trait DeploymentProvider: Send + Sync {
    fn kind(&self) -> ProviderKind;
    fn control_domain(&self) -> String;
    /// Launch `spec`; the launch record is durable in the data dir before the executable runs.
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError>;
    /// What became of this exact launch in this domain: Running(handle) | Exited | NeverLaunched.
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError>;
    /// The state of the exact runtime `handle`: Running | OsStopped | Exited { code } | Unknown.
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus;
    /// Stage one of the stop ladder: ask the runtime to stop (SIGTERM / the daemon's stop).
    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError>;
    /// The one explicit act: end the exact runtime `handle` as `mode` says.
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError>;
    /// Every runtime this provider started that it has not seen exit.
    fn launched(&self) -> Vec<DeploymentHandle>;
    /// The host address a runtime binds on, port 0; the node reports its port at its join.
    async fn bind_ip(&self, node: &PathName) -> Result<IpAddr, DeployError>;
    /// The runtime's last words, for a failure named after an early stop.
    async fn failure_detail(&self, handle: &DeploymentHandle, data_dir: &Path) -> String;
}

/// `fact` as a handle `provider` may act on, or the named reason it may not
/// (invalid-fact, other-provider, foreign-control-domain).
pub fn adopt(provider: &dyn DeploymentProvider, fact: &RuntimeFact) -> Result<DeploymentHandle, AdoptRefusal>;
```

Exact Rust naming may differ. The semantic cuts may not:

- **Spawn records before it runs.** The launch record (`deployment.json` in the data dir) is durable before the executable runs, so a runtime that runs always has its record and an executor lost at any point of the spawn leaves `find` a record to read. A record that cannot be written stops the launch before the executable runs.
- **Find is for the launcher's successor.** It tells a running launch, one that ran and exited, and one never launched apart, from the record and the domain, so a re-run after a crash mid-`DeployRuntime` adopts the runtime instead of starting a second one.
- **Adopt is for any authority.** A successor reconstructs a control handle from a birth's published RuntimeFact alone, with no launcher and no completed Build receipts. The Day-0 admin a person or the test harness started enters the same tracking by adopting its own current runtime before Ready.
- **Inspect reads, terminate acts, nothing else commands.** The provider sends no signal to a runtime but the stop ladder's and the terminate's. It never sends `SIGCONT`, never unpauses a container, never authors a node's status, never speaks Node RPC.

### 2.1 RuntimeFact is compact exact current-runtime identity

Every current managed birth publishes one compact typed RuntimeFact as part of its membership digest and projection.

```text
RuntimeFact {
    deployment_id
    provider            process | container
    control_domain      opaque, equality only
    locator             Process { pid, start } | Container { id }
}
```

The fact is:

- bound to the exact logical NodeId and current IncarnationId it rides with;
- immutable for that incarnation;
- sufficient to determine where and how that exact runtime can be adopted, inspected and terminated;
- a locator and identity fact, not a credential or a control capability;
- current observed topology state, not Build history.

`control_domain` is opaque equality-only deployment metadata. It is not a product identity and not a leadership key.

Changing the locator or control domain for the same incarnation is a named inconsistency and refusal. A restart keeps the NodeId, the IncarnationId and the RuntimeFact: the process never exits. A new node (create, recovery, Rafka's replace) has a new NodeId, IncarnationId and RuntimeFact.

A provider's local handle cache is never the source of truth merely because that admin launched the runtime.

### 2.2 Provider produces or discovers the locator; the node publishes the fact

Do not equate provider-specific locator acquisition with membership publication.

```text
provider / runtime bootstrap seam
    -> produces or discovers the exact provider locator and its control domain

node / runtime birth
    -> publishes ONE normalized RuntimeFact
       through its own current membership digest and projection
```

There is no competing launcher-owned RuntimeFact stream.

```text
process
    the runtime self-discovers its pid and the kernel's start time of that pid

container
    the provider learns the immutable container id after `docker run`
    and makes it available to the runtime before Ready through the launch record

externally started Day-0 admin
    adopts its own current runtime and obtains the same normalized identity before Ready
```

The final locator cannot be supplied in the launch environment: a pid does not exist until after the spawn.

### 2.3 Runtime identity and operational metadata are separate

A successor may need current operational metadata that is not identity.

```text
RuntimeFact
    exact runtime identity

current runtime metadata of the birth
    the operational context needed to manage it: data_dir, the resolved executable, the launch's environment
```

Required runtime-management metadata converges to every authority-capable admin as current birth projection state. It never stays launcher-private.

`data_dir` is not part of RuntimeFact equality. RuntimeFact stays compact; management metadata rides the node's record and config ([node.config.update](lifecycles/node-config-update.md)).

### 2.4 Exact provider locator and control domain

The provider identity is non-reusable for the exact concrete runtime:

```text
process provider:
    control_domain + pid + the kernel's start time of the pid
    a pid alone is insufficient: a recycled pid never matches

container provider:
    control_domain + immutable container id
    a deterministic container name alone is insufficient: a reused name never impersonates

other provider:
    control_domain + an equivalent non-reusable runtime identity
```

A cross-domain locator cannot be acted on as though it were local: it is refused by name, or routed to an executor that owns the domain. A domain mismatch is never a silent fallback.

`ResolvedNodeLaunch` carries the already-resolved logical identity, FabricId and MeshId, executable, arguments, environment, data directory, assigned endpoints, resource limits and deployment id. The provider does not decide topology, elections, restart versus replacement, advertised ports, desired counts, readiness or logical identity.

### 2.5 Runtime state: what the provider reports

```rust
pub enum DeploymentStatus {
    /// The exact runtime exists and at least one of its tasks is executing or runnable.
    Running,
    /// The exact runtime exists and the operating system holds every task of it suspended
    /// (a process in state `T`/`t`; a paused container). Observed suspension: it is NOT the
    /// lifecycle status `stopped` and proves no completed `node.stop`.
    OsStopped,
    /// The exact runtime exited; `code` when its record proves one.
    Exited { code: Option<i32> },
    /// The exact runtime's state cannot be told: an unreadable, partial or inconclusive inspection.
    Unknown,
}
```

The four states are provider-independent: every caller (the lifecycle pages, node recovery, node.runtime.inspect) interprets the common result and never `/proc` or Docker details.

What each state is, and what the mesh does with it:

| state | meaning | proof value | the caller's act |
|---|---|---|---|
| `Running` | the exact runtime is alive and executing | the runtime can still accept work, however silent the node | nothing is recovered or deleted; a silent `Running` birth follows [node recovery](lifecycles/node-recovery.md) steps 2-4 (tickle, TRUE OFFLINE, `held-running`, the R-FM1 hold and terminate) |
| `OsStopped` | the operating system holds every task suspended | the runtime is neither hung nor gone; it answers nothing but a continue from whoever suspended it | the mesh-primary records the durable status-row fact `os-stopped { since }` (surfaced on `node.get`, never a gossip `MemberStatus`), opens no R-FM1 hold, runs no `TerminateRuntime` for this observation, sends no signal, and re-inspects every recovery tick |
| `Exited { code }` | the exact runtime ended | the only proof that the concrete runtime can no longer accept work | the caller writes the birth's exit-proof row durably before anything else, then its page continues (recovery, `node.deleted`) |
| `Unknown` | the inspection could not tell | neither life nor exit | recorded as uncertainty; no hold, no terminate, no exit proof is inferred; the next tick asks again |

`OsStopped` changes state only through the operating system. While the fact is held, the recovery tick keeps inspecting the exact runtime: `Running` again clears the fact and RPC recovery proceeds through the ordinary ladder (a reached tickle; the pending lifecycle step resumes from its receipts, never a `node.start` selected because the provider reports `Running`); `Exited` is the ordinary exit proof; `Unknown` records the uncertainty. A new authority revalidates the stored fact with its own inspection: the fact is an observation, not permanent runtime truth. An explicit `node.delete` or an explicitly commanded terminate stays available through its defined exact-runtime path.

What stays inference: a runtime the operating system shows `Running` that answers nothing (a deadlock). No provider can see it; RPC silence and the R-FM1 hold are the only signal.

### 2.6 The process provider

The control domain is one host and pid namespace. The handle is `{ pid, start }`, where `start` is the kernel's start time of the pid, read at launch; a recycled pid never matches (`is(pid, start)`).

- **Spawn.** The launch record is written to `<data_dir>/deployment.json` before the executable runs. The launcher's own `RDM_*`, `TRACEPARENT` and `TRACESTATE` never reach the child; the child sees exactly the launch contract its spec names.
- **Inspect reads the live thread group, not the leader.** A process is gone only once every task of its thread group has exited: during `exit_group` the leader can read as a zombie while sibling threads still hold the group's sockets. The provider reads `/proc/<pid>/task/*/stat` for every task, and the same for tracked children: all live tasks in `T` or `t` is `OsStopped`; any live task in another state is `Running`; no live task, or `/proc/<pid>` gone with the start identity matched, is `Exited`; task files present but unreadable or partial is `Unknown`, never exit proof. The same reads serve the launcher's children and adopted runtimes alike.
- **Exit code.** For a child the provider has the code from its wait. For an adopted process it does not: the process runtime records a deliberate exit in `<data_dir>/exit.json`, keyed to its deployment and incarnation, and only that exact record turns an unknown code into a proven one.
- **Signal stop and terminate.** `signal_stop` is `SIGTERM`. `terminate(Graceful { grace })` is `SIGTERM` then `SIGKILL` after `grace`; `terminate(Immediate)` is `SIGKILL`. Nothing else is ever sent.

### 2.7 The container provider

The control domain is one Docker daemon. The handle is `{ container id, init pid }`; the immutable id is the identity, the container's init pid a convenience for the netns reads.

- **Network.** Each fabric gets a bridge network `rafka-<fabric>` carved from `RDM_CONTAINER_SUBNET_POOL` (a `/24` another network uses is skipped); node-admin assigns every container's address (`--ip`) so a node's bind address is known before it runs.
- **Spawn and find.** The launch is recorded in the data dir, the container named deterministically for `find`, and the immutable id learned after `docker run`. `find` reads the daemon's state and the record: running, exited, or never created.
- **Inspect** maps the daemon's state: `running`, `restarting` and `created` are `Running`; `paused` is `OsStopped` (the daemon freezes the container's cgroup: this reports container suspension, not a stopped process inside a running container); `exited` and `dead` are `Exited { code }` with Docker's own code; a daemon that cannot answer is `Unknown`. An unreachable daemon refuses the provider by name at bootstrap.
- **Signal stop and terminate** are the daemon's stop and kill of the exact id.

### 2.8 Exit proof

Once a handle is adopted or spawned, the provider's `Exited` for that exact handle is the proof that the concrete runtime can no longer accept work, and the only one. A RuntimeFact's presence proves neither running nor gone. An RPC failure, a failed dial, a closed endpoint, a timeout, an `unreached` tickle and the observer-inferred `Dead` prove nothing ([node.tickle](lifecycles/node-tickle.md)). The exit-proof row is written durably before any step rests on it; a later attempt reads the row and asks the provider again only for a birth with no row.

## 3. The pipeline's steps

The provider is driven by the DeploymentPipeline, whose steps are the Build's receipts (build.md §4.1). The canonical managed creation:

```text
AllocateIdentity
ClaimName                 the mesh-primary's insert-and-fail on path.name; NameHeld launches nothing
AllocateEndpoints
PrepareStorage            a fresh data_dir, or an inherited one inspected (gone / broken, §4)
PrepareNetwork
DeployRuntime             spawn, or find and adopt a launch a lost executor left running
RegisterExactRuntimeHandle
ResolveProviderControlDomain
MakeRuntimeFactAvailableToBirth
WaitForBind
PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata
WaitForMeshJoin
WaitForNodeReady
Complete
```

The externally started Day-0 adoption: `AdoptCurrentRuntime`, `RegisterExactRuntimeHandle`, `ResolveProviderControlDomain`, `PublishRuntimeFactAndCurrentRuntimeMetadata`, then the normal bootstrap.

The successor's management cut: from a current membership birth's RuntimeFact and runtime metadata, resolve the control domain, adopt the published runtime (or route to an executor of its domain), and inspect or terminate as the Build requires.

A `node.delete` runs the whole [node.stop](lifecycles/node-stop.md) (its first step [node.connections.delete](lifecycles/node-connections-delete.md) with `{ scope: All }`), then `TerminateRuntime` on the exact handle, the provider's exit proof, then identity release with storage retained. The R-FM1 recovery of a hung runtime begins with the same `TerminateRuntime { node_id, incarnation, handle }` step after its hold, and only then. Externally visible steps reconcile forward; published topology is never blindly reversed.

## 4. Storage policy and the data dir

A path's config is typed: `PathConfig { storage: StorageMeta, placement: PlacementMeta }` ([node.config.update](lifecycles/node-config-update.md)). `Tag` is an extrinsic annotation with no execution semantics. The pipeline interprets the config and resolves concrete storage actions against exact locators; the provider interprets neither.

Persistence belongs to the logical `path.name`, not its incarnation: Ephemeral storage is fresh for a new node; Persistent storage survives stop, start and restart, and a new node created over an inherited data dir keeps it. Delete releases no storage; `Preserve` issues no release action.

`PrepareStorage` for a recovery (R-FM2) tells **gone** from **broken** by the error, never by a guess:

- **Gone.** The inherited directory itself is absent (`NotFound` on the directory). When it stays absent for `RDM_DATA_DIR_GONE_FLOORS` staleness floors (default 2, to ride out a remount), the mesh-primary records `data-dir-lost { path, since }` on the exited birth and the same recovery creates the node at the same `path.name` over a fresh `data_dir` beside the old path with a newly minted endpoint key (the old key is never re-minted: the key belongs to the data dir), and `hydrate_before_ready` runs with `inherited = false` (`RDM_INHERITED_DATA_DIR=0`; the application resyncs).
- **Broken.** The directory exists but `node-key` is missing, unreadable or corrupt. `PrepareStorage` fails by name without minting a key, records `data-dir-unreadable { path, cause }` on the exited birth, and the mesh-primary holds the recovery (not due while the fact stands) until the key reads again, then proceeds with `inherited = true`.

RDM deletes nothing on disk, ever. Rafka's per-kind defaults (broker and node-admin Persistent, gateway and compute Ephemeral) belong to Rafka's ingress adapter; the three-state `StorageMeta` contract is RDM law. Normalization happens before Build acceptance; an accepted Build carries explicit per-path config, and neither pipeline nor provider applies hidden defaults.

## 5. What the provider never does

- Never authors a node's status, publishes gossip, or speaks Node RPC.
- Never infers: `Unknown` is an answer, silence is not.
- Never sends a signal but the stop ladder's and the terminate's; never continues a suspended runtime.
- Never acts on a locator from another control domain.
- Never decides topology, identity, readiness, restart versus replacement, or storage.

## 6. Evidence

| span | emitted by | fields |
|---|---|---|
| `rdm.node_admin.deployment.update.via-step` | the pipeline, per step | build_id, attempt, node, step (`DeployRuntime`, `PrepareStorage`, `TerminateRuntime`, …), outcome |
| `rdm.node_admin.node.resolve.via-provider-inspection` | the caller of node.runtime.inspect | node, node_id, incarnation_id, inspector, handle, `verdict` (`running` / `os-stopped` / `exited` / `unavailable`), exit_code, exit_proof (`put` / `held` / none), reason |
| `rdm.node_admin.runtime.reject.via-<reason>` | adopt | `invalid-fact`, `other-provider`, `foreign-control-domain` |
| `rdm.node_admin.node.update.via-data-dir-lost`, `via-data-dir-unreadable` | PrepareStorage | node, node_id, incarnation_id, path, cause |

Environment: `MESH_SPAWN_TYPE`, `RDM_CONTAINER_SUBNET_POOL`, `RDM_REQUIRE_CONTAINER`, `RDM_HUNG_HOLD_FLOORS`, `RDM_DATA_DIR_GONE_FLOORS`, `RDM_INHERITED_DATA_DIR` (the table in `CLAUDE.md`).
