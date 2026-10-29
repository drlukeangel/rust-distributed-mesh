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

The established provider becomes fabric policy and is inherited by later node-admins and nodes. A Build request does not choose its provider. A joining admin whose local bootstrap value conflicts with established fabric policy refuses to deploy, by name.

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
   any primary can adopt and control the published locator

B. domain-local execution
   the current primary stays the control-plane primary
   the exact runtime operation is routed to an executor that owns the published control domain
```

What is built of this today is the refusal: a locator from another control domain is refused by name (`foreign-control-domain`), never acted on as if it were local. Being primary and deployment locality are separate. Host or provider locality may constrain readiness or execution routing, never leadership ordering.

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
    /// What became of this exact launch in this domain: Running(handle) | Exited(handle, when its record names one) | NeverLaunched.
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
- **Adopt is for any primary.** A successor reconstructs a control handle from a birth's published RuntimeFact alone, with no launcher and no completed Build receipts. The Day-0 admin a person or the test harness started enters the same tracking by adopting its own current runtime before Ready.
- **Inspect reads, terminate acts, nothing else commands.** The provider sends no signal to a runtime but the stop ladder's and the terminate's. It never sends `SIGCONT`, never unpauses a container, never authors a node's status, never speaks Node RPC.

### 2.1 RuntimeFact is compact exact current-runtime identity

Every current managed birth publishes one compact typed RuntimeFact as part of its topology digest and projection.

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

Changing the locator or control domain for the same incarnation is a named inconsistency and refusal. A stop and a start keep the NodeId, the IncarnationId and the RuntimeFact: the process never exits. A new node (create, recovery, Rafka's replace) has a new NodeId, IncarnationId and RuntimeFact.

A provider's local handle cache is never the source of truth merely because that admin launched the runtime.

### 2.2 Provider produces or discovers the locator; the node publishes the fact

Do not equate provider-specific locator acquisition with topology publication.

```text
provider / runtime bootstrap seam
    -> produces or discovers the exact provider locator and its control domain

node / runtime birth
    -> publishes ONE normalized RuntimeFact
       through its own current topology digest and projection
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
    the operational context needed to manage it: data_dir, the resolved executable, the launch's environment with its declared secrets excepted (a declared secret is written to the admins that may launch its kinds and to those processes, never to a record or a projection)
```

Required runtime-management metadata converges to every node-admin as current birth projection state. It never stays launcher-private.

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
    /// The exact runtime exists, is nonterminal and is not observed OS-suspended. It proves
    /// neither responsiveness nor readiness: a sleeping task, a runtime still starting and a
    /// deadlocked one all read `Running`.
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
| `Running` | the exact runtime exists, is nonterminal and is not observed OS-suspended | the runtime is not gone; this proves neither that it answers nor that it is ready, however silent or loud the node | [node investigation](lifecycles/node-investigation.md)'s `running` verdict: a silent `Running` birth is re-created, `TerminateRuntime` first ([node recovery](lifecycles/node-recovery.md)) |
| `OsStopped` | the operating system holds every task suspended | the exact runtime exists and is observed suspended: this proves neither exit nor a completed `node.stop`, and says nothing about whether the node would answer once the suspension ends | node investigation's `os-stopped` verdict: re-created over a newly allocated data dir; an explicit node.delete skips the stop and terminates |
| `Exited { code }` | the exact runtime ended | the concrete runtime can no longer accept work | node investigation's `exited` verdict: re-created over the inherited data dir, `TerminateRuntime` first, which removes what the runtime left (Q145); a delete proceeds to `node.deleted` |
| `Unknown` | the inspection could not tell | neither life nor exit | node investigation's `unproven` verdict: re-created over a newly allocated data dir |

`OsStopped` changes state only through the operating system. It is an answer of node.runtime.inspect and a verdict of node investigation like the others; nothing holds on it and nothing waits for it to change. An explicit `node.delete` of an OS-stopped birth stays available: it skips the Node RPC stop by name (a suspended process answers no call), runs `TerminateRuntime` on the exact handle as `terminate(Immediate)` (no graceful wait; nothing of the runtime is resumed) and continues ([node.delete](lifecycles/node-delete.md)).

What stays inference: a runtime the operating system shows `Running` that answers nothing (a deadlock). No provider can see it; RPC silence and the R-FM1 hold are the only signal.

### 2.6 The process provider

The control domain is one host and pid namespace. The handle is `{ pid, start }`, where `start` is the kernel's start time of the pid, read at launch; a recycled pid never matches (`is(pid, start)`).

- **Spawn.** The launch record is written to `<data_dir>/deployment.json` before the executable runs. The launcher's own `RDM_*`, `TRACEPARENT` and `TRACESTATE` never reach the child; the child sees exactly the launch contract its spec names.
- **Inspect reads the live thread group, not the leader.** A process is gone only once every task of its thread group has exited: during `exit_group` the leader can read as a zombie while sibling threads still hold the group's sockets. The provider reads `/proc/<pid>/task/*/stat` for every task, and the same for tracked children: all live tasks in `T` or `t` is `OsStopped`; any live task in another state is `Running`; no live task, or `/proc/<pid>` gone with the start identity matched, is `Exited`; task files present but unreadable or partial is `Unknown`. The same reads serve the launcher's children and adopted runtimes alike.
- **Exit code.** For a child the provider has the code from its wait. For an adopted process it does not: the process runtime records a deliberate exit in `<data_dir>/exit.json`, keyed to its deployment and incarnation, and only that exact record supplies an unknown code.
- **Signal stop and terminate.** `signal_stop` is `SIGTERM`. `terminate(Graceful { grace })` is `SIGTERM` then `SIGKILL` after `grace`; `terminate(Immediate)` is `SIGKILL`. Nothing else is ever sent.

### 2.7 The container provider

The control domain is one Docker daemon. The handle is `{ container id, init pid }`; the immutable id is the identity, the container's init pid a convenience for the netns reads.

- **Network.** Each fabric gets a bridge network `rafka-<fabric>` carved from `RDM_CONTAINER_SUBNET_POOL` (a `/24` another network uses is skipped); node-admin assigns every container's address (`--ip`) so a node's bind address is known before it runs.
- **Executables.** A containerized node-admin mounts the executable directories its launch named (`RDM_EXECUTABLE_DIRS`), read-only at their host paths; an executable outside them is refused by name at acceptance ([build-deployment.md §2](build-deployment.md)).
- **Spawn and find.** The launch is recorded in the data dir, the container named deterministically for `find`, and the immutable id learned after `docker run`. `find` reads the daemon's state and the record: running, exited, or never created.
- **Inspect** maps the daemon's state conservatively: `running`, and also `restarting` and `created` (a container not yet executing or between executions: nonterminal, not suspended, nothing more claimed), are `Running`; `paused` is `OsStopped` (the daemon freezes the container's cgroup: this reports container suspension, not a stopped process inside a running container); `exited` and `dead` are `Exited { code }` with Docker's own code; a daemon that cannot answer is `Unknown`. An unreachable daemon refuses the provider by name at bootstrap.
- **Signal stop and terminate.** `signal_stop` is the daemon's kill of the exact id with SIGTERM, refused by name on a paused container, so this path cannot thaw it to deliver SIGTERM. `terminate(Graceful { grace })` is the daemon's stop, its grace the stop's timeout. `terminate(Immediate)` is the daemon's kill with SIGKILL, never a stop: a stop delivers SIGTERM and thaws a paused container, which then runs; a paused container killed with SIGKILL ends without running again (measured 2026-10-16 on Docker 29.6.2, trace 1921e2bc0ef83a739e75e4e7a14cb127: a paused container killed with SIGKILL ended in 159 ms with no application progress observed; docker stop -t 0 let it run).

### 2.8 Exited

Once a handle is adopted or spawned, the provider's `Exited` for that exact handle says the concrete runtime can no longer accept work. A RuntimeFact's presence says neither running nor gone. An RPC failure, a failed dial, a closed endpoint, a timeout, an `unreached` tickle and the observer-inferred `Dead` say nothing by themselves ([node.tickle](lifecycles/node-tickle.md)): [node investigation](lifecycles/node-investigation.md)'s verdict is the one decision.

## 3. The pipeline's steps

The provider is driven by the DeploymentPipeline, whose steps are the Build's receipts (build.md §4.1). The canonical managed creation:

```text
AllocateIdentity
ClaimName                 the mesh-primary reads the row at path.name and claims it; NameHeld launches nothing
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

A launch that runs a bound executable re-hashes it before `DeployRuntime` and launches it only when the hash matches its binding; the hash reads the whole file, so it runs on the blocking pool, off the runtime's workers ([Node RPC §21](node-rpc.md#21-iroh-owns-liveness)).

Which executable a node runs, and how it is named, recorded, replaced and read, is [build-deployment.md](build-deployment.md)'s.

The externally started Day-0 adoption: `AdoptCurrentRuntime`, `RegisterExactRuntimeHandle`, `ResolveProviderControlDomain`, `PublishRuntimeFactAndCurrentRuntimeMetadata`, then the normal bootstrap.

The successor's management cut: from a current topology birth's RuntimeFact and runtime metadata, resolve the control domain, adopt the published runtime (or route to an executor of its domain), and inspect or terminate as the Build requires.

A `node.delete` of a live target runs the whole [node.stop](lifecycles/node-stop.md) (its first step [node.connection.delete](lifecycles/node-connections-delete.md) with `{ scope: All }`), then `TerminateRuntime` on the exact handle, then identity release with storage retained. A `node.delete` of a target whose provider state is `OsStopped` skips the stop by name and begins at `TerminateRuntime`: the stop cannot execute, and the delete was explicitly commanded. The recovery of a birth begins with the same `TerminateRuntime { node_id, incarnation, handle }` step whatever its verdict but `os-stopped`; of an exited runtime it removes what the runtime left, and a runtime already removed terminates as done (Q145). Externally visible steps reconcile forward; published topology is never blindly reversed.

## 4. Storage policy and the data dir

A node's storage and placement are typed properties of its create spec (`storage: StorageMeta`, `placement: PlacementMeta`; [build.md §1](build.md#1-build-is-the-only-topology-mutation-engine)); `config` is the application's map, which RDM never reads. `Tag` is an extrinsic annotation with no execution semantics. The pipeline interprets the storage and placement and resolves concrete storage actions against exact locators; the provider interprets neither.

Persistence belongs to the logical `path.name`, not its incarnation: Ephemeral storage is fresh for a new node; Persistent storage survives stop and start, and a new node created over an inherited data dir keeps it. Delete releases no storage; `Preserve` issues no release action.

`PrepareStorage` for a recovery (R-FM2) tells **gone** from **broken** by the error, never by a guess:

- **Gone.** The inherited directory itself is absent (`NotFound` on the directory). When it stays absent for `RDM_DATA_DIR_GONE_FLOORS` staleness floors (default 2, to ride out a remount), the mesh-primary records `data-dir-lost { path, since }` on the exited birth and the same recovery creates the node at the same `path.name` over a fresh `data_dir` beside the old path with a newly minted endpoint key (the old key is never re-minted: the key belongs to the data dir), and `pre_node_ready` runs with `inherited = false` (`RDM_INHERITED_DATA_DIR=0`; the application resyncs).
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
| `rdm.node_admin.node.resolve.via-provider-inspection` | the caller of node.runtime.inspect | node, node_id, incarnation_id, inspector, handle, `verdict` (`running` / `os-stopped` / `exited` / `unavailable`), exit_code, reason |
| `rdm.node_admin.runtime.reject.via-<reason>` | adopt | `invalid-fact`, `other-provider`, `foreign-control-domain` |
| `rdm.node_admin.node.update.via-data-dir-lost`, `via-data-dir-unreadable` | PrepareStorage | node, node_id, incarnation_id, path, cause |

Environment: `MESH_SPAWN_TYPE`, `RDM_CONTAINER_SUBNET_POOL`, `RDM_REQUIRE_CONTAINER`, `RDM_DATA_DIR_GONE_FLOORS`, `RDM_INHERITED_DATA_DIR` (the table in `CLAUDE.md`).
