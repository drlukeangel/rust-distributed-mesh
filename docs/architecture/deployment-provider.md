# Deployment provider, RuntimeFact and exact runtime proof

This page owns the deployment provider and its environment policy, the DeploymentPipeline, the RuntimeFact contract, current runtime metadata and the exact runtime proof. The Build engine that drives the provider, `BuildStateAdapter`, the node-admin storage interfaces and crash safety, lifecycle transitions and the RDM/Rafka boundary with its import rule are [build.md](build.md). The sequences that use the provider are the workflow pages in the [lifecycle index](fabric-mesh-node-lifecycles.md).

It is part of the generic control-plane contract proven in `rust-distributed-mesh` (RDM) and imported by Rafka ([build.md](build.md)).

## 1. Deployment provider is environment policy

The first node-admin bootstrap chooses the deployment mechanism with `MESH_SPAWN_TYPE=process|container`.

It is parsed once into:

```rust
pub enum DeploymentProviderKind {
    Process,
    Container,
}
```

Unknown values are a bootstrap refusal. They never silently fall back to process.

The established provider becomes fabric policy and is inherited by later node-admins and nodes. A Build request does not choose its provider. A joining admin whose local bootstrap value conflicts with established fabric policy refuses deployment authority by name.

A runtime locator is meaningful only inside the **provider control domain** that owns it. The provider policy must therefore make both the exact runtime locator and its control domain available as current control facts.

The process and container providers are local:

```text
process provider
    control domain = one host/process namespace

container provider
    control domain = one reachable Docker daemon / host runtime
```

A provider whose runtime handle only works inside the original launcher's private memory is incompatible with preemptible leadership. A PID or container ID is never assumed globally meaningful across hosts.

A multi-host provider has two valid shapes:

```text
A. remotely controllable provider domain
   any eligible authority can adopt/control the published locator

B. domain-local execution
   current authority remains control-plane authority
   exact runtime operation is routed/delegated to an executor/agent
   that owns the published provider control domain
```

Authority and deployment locality are separate. Host/provider locality may constrain readiness or execution routing, never leadership ordering.

---

## 2. DeploymentPipeline, RuntimeFact, current runtime metadata, and exact runtime proof

Node-admin remains the brain. A provider is only the runtime hand.

```rust
#[async_trait]
pub trait DeploymentProvider: Send + Sync {
    async fn spawn(&self, spec: ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError>;
    async fn adopt_current_runtime(&self, spec: CurrentRuntimeSpec) -> Result<DeploymentHandle, DeployError>;
    async fn adopt_published_runtime(&self, fact: RuntimeFact) -> Result<DeploymentHandle, DeployError>;
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError>;
    async fn wait(&self, handle: &DeploymentHandle) -> Result<ExitStatus, DeployError>;
    async fn inspect(&self, handle: &DeploymentHandle) -> Result<DeploymentStatus, DeployError>;
}
```

Exact Rust naming may differ. The semantic cuts may not.

`adopt_current_runtime` exists so the Day-0 admin started by a person or the test harness enters the same runtime tracking as Build-launched nodes. `adopt_published_runtime` means a successor can reconstruct a control handle from a current membership RuntimeFact without needing the launcher or completed Build receipts. If the locator belongs to another control domain, the library routes the exact-runtime action to a valid domain-local executor instead of pretending the locator is local.

### 2.1 RuntimeFact is compact exact current-runtime identity

Every current managed birth publishes one compact typed RuntimeFact as part of its membership birth/projection.

```text
RuntimeFact {
    node_id
    incarnation_id
    deployment_id
    provider
    provider_control_domain
    locator
}
```

The fact is:

- bound to exact logical NodeId + current IncarnationId;
- immutable for that incarnation;
- sufficient to determine where and how that exact runtime can be adopted/inspected/terminated;
- a locator/identity fact, not a credential or secret control capability;
- current observed topology state, not Build history.

`provider_control_domain` is opaque equality-only deployment metadata. It is not a product identity and is not an leadership key.

Changing the locator or provider control domain for the same incarnation is a named inconsistency/refusal. Restart keeps the NodeId, the IncarnationId and the RuntimeFact: the process never exits. A new node (create, recover, Rafka's replace) has a new NodeId, IncarnationId and RuntimeFact.

The local `handles` map may cache adopted/spawned handles. It is never source of truth merely because that admin launched the runtime.

### 2.2 Provider produces/discovers the locator; the node publishes the fact

Do not equate provider-specific locator acquisition with membership publication.

```text
provider / runtime bootstrap seam
    -> produces or discovers exact provider locator + provider control domain

node / runtime birth
    -> publishes ONE normalized RuntimeFact
       through its own current membership digest/projection
```

There is no competing launcher-owned authoritative RuntimeFact stream.

Valid provider-specific acquisition includes:

```text
process
    runtime self-discovers pid + process-start token

container
    provider learns immutable container id after docker run
    provider-written bootstrap record/channel makes it available to the runtime before Ready

externally started Day-0 admin
    adopt_current_runtime / self-discovery obtains the same normalized identity before Ready
```

The final runtime locator generally cannot be supplied in the original launch env. For example, a process PID does not exist until after spawn.

### 2.3 Runtime identity and operational metadata are separate

A successor may need current operational metadata that is not identity.

```text
RuntimeFact
    exact runtime identity

current Node runtime metadata
    operational context needed to manage the birth
    e.g. data_dir, resolved storage/executable context where required
```

Required runtime-management metadata must converge to every authority-capable admin as current Node/birth projection state. It must not remain launcher-private.

`data_dir` is not automatically part of RuntimeFact equality/fencing. Do not inflate RuntimeFact with arbitrary operational blobs merely because membership carries it. RuntimeFact stays compact; larger provider-independent management metadata is projected beside it.

### 2.4 Exact provider locator and control domain

The provider/runtime identity must be non-reusable for the exact concrete runtime:

```text
process provider:
    provider_control_domain + pid + process-start identity/token
    PID alone is insufficient

container provider:
    provider_control_domain + immutable container id
    deterministic container name alone is insufficient

other provider:
    provider_control_domain + equivalent non-reusable runtime identity
```

A cross-domain locator cannot be acted on as though it were local. The provider must either expose a remotely usable control path for that domain or the control library must route the action to a valid domain-local executor. A domain mismatch is a named adoption/execution refusal, not death evidence.

Once adopted/spawned, provider `Exited`/terminal status for that exact handle is valid evidence that the concrete runtime can no longer accept work. RuntimeFact presence by itself does not prove Running or Dead. RPC failure, failed dial, gossip silence, topology absence, or inability of one admin to access a provider domain is not equivalent death proof.

`ResolvedNodeLaunch` contains the already-resolved logical identity, FabricId/MeshId, executable, args, env, data directory, assigned endpoints, resource limits, and deployment id. `CurrentRuntimeSpec` supplies the corresponding already-known logical identity/provider context for external adoption.

The provider does not decide topology, elections, restart vs replacement, advertised ports, desired counts, readiness, or logical identity.

Canonical managed creation pipeline:

```text
AllocateIdentity
ClaimName                 the mesh-primary's insert-and-fail on path.name; NameHeld launches nothing
AllocateEndpoints
PrepareStorage
PrepareNetwork
DeployRuntime
RegisterExactRuntimeHandle
ResolveProviderControlDomain
MakeRuntimeFactAvailableToBirth
WaitForBind
PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata
ConfigureDNS              optional
ConfigureLoadBalancer     optional
ConfigureFirewall         optional
WaitForMeshJoin
WaitForNodeReady
Complete
```

Canonical externally-started Day-0 adoption cut:

```text
process/container already running
    -> AdoptCurrentRuntime
    -> RegisterExactRuntimeHandle
    -> ResolveProviderControlDomain
    -> PublishRuntimeFact + current runtime metadata
    -> continue normal bootstrap/lifecycle
```

Canonical successor management cut:

```text
current membership birth
    -> RuntimeFact + current runtime metadata
    -> resolve provider_control_domain
    -> provider adopts published runtime
       OR route to a valid domain-local executor
    -> local DeploymentHandle/control object
    -> inspect / terminate / restart as Build requires
```

Retirement is [node.delete](lifecycles/node-delete.md): the whole node.stop, the provider's exact exit proof, then identity release with storage retained. Optional external steps (load balancer, DNS, firewall) run before the stop and reconcile forward.

Externally visible steps reconcile forward. Do not blindly reverse published topology/DNS/LB state.

### Resource vocabulary and storage policy

`Tag` is an extrinsic descriptive annotation with zero execution semantics. `NodeMeta` holds
intrinsic desired properties, including typed `StorageMeta` and `PlacementMeta`; `RuntimeMeta`
holds operational information for an exact current birth. `RuntimeFact` identifies that birth's
concrete provider runtime and locator. An `Overlay` is an ephemeral lifecycle/routing projection.

Persistence belongs to logical `path.name`, not its incarnation: Ephemeral storage is fresh
for a new node; Persistent storage survives stop, start and restart, and a new node created
over an inherited data dir keeps it. Delete releases no storage. When `PrepareStorage` runs
for a recovery (`R-FM2`), it distinguishes an absent inherited `data_dir` (`NotFound` on the
directory itself: **gone**) from an existing directory whose `node-key` is missing,
unreadable, or corrupt (**broken**). When the directory is absent past
`RDM_DATA_DIR_GONE_FLOORS` staleness floors (default 2), the mesh-primary records
`data-dir-lost { path, since }` on the exited birth and `PrepareStorage` allocates a fresh
`data_dir` beside the old path and mints a new endpoint key (never re-minting the old key),
running `hydrate_before_ready` with `inherited = false`. When the directory exists on disk
with a missing or unreadable `node-key`, `PrepareStorage` fails by name without minting a
key, records `data-dir-unreadable { path, cause }` on the exited birth, and the mesh-primary
holds recovery without retrying until `<data_dir>/node-key` is repaired (`inherited = true`).
The selected Rafka versioned ingress defaults are broker and node-admin Persistent/Preserve,
and gateway and compute Ephemeral. These choices and `default_for(RafkaRole)` belong to
Rafka's ingress adapter, not generic RDM role policy. The three-state `StorageMeta` contract
remains generic RDM law. Explicit per-path NodeMeta precedes the customer default.
Normalization occurs only before Build acceptance. Accepted Builds carry resolved, explicit
per-path NodeMeta; neither pipeline nor provider applies hidden defaults.

DeploymentPipeline interprets desired properties and resolves concrete storage actions against
exact locators. DeploymentProvider interprets neither desired NodeMeta nor descriptive Tags;
Preserve issues no release action.

A `node.delete` of a target believed live runs the whole [node.stop](lifecycles/node-stop.md), whose first step is [node.connections.delete](lifecycles/node-connections-delete.md) with `{ scope: All }`. The stop's receipt is written before provider termination and exact inspection. A dead or wedged node cannot block an authorized delete forever, and silence on a desired running node never authorizes replacement. When a TRUE OFFLINE member or mesh-primary is inspected as `Running` throughout the `RDM_HUNG_HOLD_FLOORS` hold (`held-running { since, verdicts }`, `R-FM1`), the recovery Build's `node.delete` runs `TerminateRuntime { node_id, incarnation, handle }` on the exact runtime handle recorded at launch (`terminate(handle, Graceful)` then `Forced` after the stop grace), records `exit_proof` durably and clears `held-running` before completing `NodeDeleted` and `node.create`. A refused or unproven terminate ends the attempt `Failed { step: TerminateRuntime, reason }` by name.
