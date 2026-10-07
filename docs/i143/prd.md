# i143 - Build, prove, and export the generic Mesh product on RDM

**Initiative:** `i143`  
**Repository under build:** `drlukeangel/rust-distributed-mesh`  
**Status:** implementation-grade, final architecture reconciliation 2026-10-05  
**Milestone:** 59 - i143 Node RPC + generic Mesh product on RDM  
**Target architecture (rafka-v2):** `docs/architecture/node-rpc-rdm-ownership.md`, `docs/architecture/connections.md`, `docs/architecture/mesh-control-plane.md`, `docs/architecture/node-lifecycle-elections.md`, `docs/architecture/gossip.md`  
**Parity ledger:** [`transport-parity.md`](transport-parity.md)  
**Downstream import (rafka-v2):** `docs/plans/i141-node-rpc-substrate-prd.md`  
**Downstream migration (rafka-v2):** `docs/plans/i142-node-rpc-migrations-prd.md`

> This is the single binding i143 PRD. Former amendment files are folded here. The parity ledger remains separate because it records evidence, not alternate architecture.

---

## 0. Purpose

RDM is the implementation and proving ground for the generic Mesh product that Rafka imports. i143 must deliver one reusable generic control/runtime substrate, not a parallel Rafka-shaped implementation.

The exported boundary includes:

```text
logical Fabric / Mesh / Node identities
Iroh transport integration
hierarchical membership/topology
current RuntimeFact + required current runtime metadata per birth
current Fabric record + complete accepted Build
Direct / Proxy connections
Node RPC contract/runtime/pool
Build / Reconciler
BuildStateAdapter contract
LeadershipResolver
DeploymentProvider / DeploymentPipeline
LifecycleTransitionPipeline
cross-mesh recovery
scenario/evidence/chaos support
```

No generic package imports a Rafka domain crate.

---

## 1. Locked architecture decisions

1. Build is the only generic topology mutation engine.
2. `rafka-node-admin-core` is the **single generic implementation owner** of Build reconciliation, leadership projection, executor/attempt rules, runtime adoption, generic Fabric/Mesh/Node lifecycle and recovery mechanics.
3. i141 binds Rafka policy and production storage/provider backends into that library. i142 consumes the resulting views/mechanisms and must not reimplement them.
4. There is one topology authority and one accepted topology:

```text
current fabric-primary = who may accept a topology change
Fabric.build_id -> complete Build = what topology the Fabric has accepted
```

5. RDM has no independent desired-topology authority beside `Fabric.build_id -> Build`. Any internal shape cache is disposable derived state and has no revision, lineage, fork-winner or writer semantics.
6. Keep three distinct fact classes:

```text
Fabric.build_id -> complete Build = accepted topology
membership + RuntimeFacts + current runtime metadata = exact current births/runtimes
builds.storage facts = execution evidence for attempts of that Build
```

7. A new Build id is minted only when the current fabric-primary accepts a topology change.
8. Restart, same-path replacement, authority movement and proven runtime drift that do not change accepted topology open a new attempt of the Build `Fabric.build_id` already names.
9. A second topology-changing request while the current Build is actively reconciling is refused by name with `409 build-in-progress`, naming that Build. RDM does not implement Build supersession/cancellation.
10. Build acceptance is crash-safe: persist the complete Build through `builds.storage` first, then persist `Fabric.build_id` through `fabric.storage`. The pointer never names an unrecoverable Build.
11. Joining/restarting authority-bearing admins hydrate the current Fabric record plus the complete Build `Fabric.build_id` names before Ready. Older Build history is never replayed to reconstruct topology.
12. RDM owns the generic `fabric.storage`, `mesh.storage`, `nodes.storage`, `connections.storage` and `builds.storage` interfaces plus memory and durable file implementations used by restart/chaos proofs. `builds.storage` adopts the existing BuildStateAdapter memory/FileJournal implementation rather than rebuilding it.
13. Deployment provider is Fabric policy selected at first bootstrap; ordinary Build requests never choose it.
14. Every current managed runtime publishes a compact typed RuntimeFact bound to exact NodeId + IncarnationId + DeploymentId + provider + provider control domain + exact provider locator. A future eligible admin can adopt/manage the runtime without launcher-private Build receipts.
15. Provider/runtime bootstrap **produces or discovers** the exact provider locator. The node/runtime birth publishes the one normalized RuntimeFact through its own membership projection. There is no competing launcher-owned authoritative RuntimeFact stream.
16. Current operational metadata required by a successor, for example `data_dir`, is current Node/runtime projection state. It must converge to future authorities but is not part of RuntimeFact identity merely because it is needed for management.
17. The externally started Day-0 admin adopts/registers its own exact provider runtime and publishes the same RuntimeFact/current runtime metadata before authority-bearing Ready.
18. Process exact identity is provider-control-domain + PID + process-start token or stronger; PID alone is insufficient. Container identity is provider-control-domain + immutable container ID; deterministic container name alone is insufficient.
19. A provider locator is valid only inside its provider control domain. Current process/container proof is same-host/same-provider-domain. Future multi-host control requires either remotely controllable locators or domain-local exact-runtime execution. Provider locality never changes election priority.
20. Provider exact-handle terminal status can prove that exact runtime exited. RuntimeFact presence, provider-domain mismatch, RPC failure, failed dial, gossip silence, DNS disappearance, or partition cannot.
21. `LifecycleTransitionPipeline` owns Node/Mesh/Fabric transition policy and blocking hooks. `LifecycleTransition.transition_id` identifies one transition execution and keys hook receipts; Node/Mesh/Fabric status-control RPCs carry no transition id and are idempotent on their natural keys.
22. Normal joining/restarted admins do not self-Ready around missing control hydration. Day 0 and the explicitly fenced single-admin recovery-root are the only lifecycle roots that may establish Ready without an already-live Ready authority; election still runs afterward.
23. SN/MN/MM are minimum proof rungs, not caps.
24. Mesh creation, recovery and intentional replacement are separate Build contracts.
25. Fabric/Mesh views expose current owning node-admin control endpoints; tests never depend on hidden maps.
26. Node RPC certainty commits only after complete request send + request-direction finish. Incomplete send -> 499/NotSent.
27. Direct/Proxy connection truth is imported from the i66.e3 parity source; RDM does not create a second carrier-evidence or reconnect authority.
28. Domain routing selects WHICH final node. Connections selects HOW to reach it. Node RPC executes it.
29. Gossip/iroh-gossip owns current membership/status/runtime dissemination and repair. Explicit status/control Node RPC directly notifies the semantic authority. Neither plane proves, approves, attests or confirms the other.
30. Partition repair is **held-member stale-coverage repair**, not `neighbor_count == 0` as the only trigger.
31. Iroh owns authenticated transport identity and QUIC. Logical product identity is separate.
32. Logical `NodeId`, `MeshId`, `FabricId` are bare 12-character lowercase Crockford Base32 values carrying 60 random bits.
33. Only `NodeId` is an election ordering key. `MeshId` and `FabricId` are equality-only identities.
34. Iroh `EndpointId` is transport identity. It must never be represented by logical `FabricId`.
35. Canonical leadership is ReadyForTraffic candidates -> lowest complete NodeId. Ready is the only election eligibility bit.
36. For node-admins, Ready means authority-capable hydration is complete: current Fabric record + accepted Build, RuntimeFacts + required runtime metadata for held births it may have to manage, and provider-control-domain reachability or a valid domain-local execution path. These facts are readiness prerequisites, not election scoring.
37. Product chaos remains downstream i87; i143 proves generic deterministic + real process/container fault behavior.
38. Transitional Rafka legacy tags exist only as downstream i141/i142 adapters. Generic RDM has no Rafka legacy handlers.

---

## 2. Substrate ownership

```text
Iroh
    EndpointId authentication
    QUIC/path mechanics
    ALPN/Router multiplexing
    configured address lookup

iroh-gossip
    HyParView topic membership
    Plumtree eager/lazy dissemination and repair

RDM
    logical Fabric/Mesh/Node model
    current RuntimeFact + runtime-metadata projection
    current Fabric record + accepted Build
    topology and endpoint projection
    runtime incarnation + endpoint freshness
    Direct/Proxy effective route model
    Node RPC + semantic pool
    Build/Reconciler + drift trigger
    LeadershipResolver
    lifecycle/status certainty
    deployment/runtime adoption + provider-domain routing + proof
    recovery policy and proof
```

One Iroh Endpoint per process is preferred, multiplexing Node RPC + gossip. Exceptions require a named Iroh limitation and proof.

Canonical address authority is explicit product topology/EndpointAddr plus explicit gossip seeds. Optional mDNS is discovery only. No hidden N0 DNS/Pkarr authority is allowed in the canonical path.

The Node RPC semantic pool key includes peer EndpointId, runtime incarnation, endpoint slot, and slot freshness. EndpointId is not FabricId.

---

## 3. Gap/build map

| gap | requirement | owner |
|---|---|---|
| R1 | Node RPC contract/runtime | e5/e6 |
| R2 | Mesh identity/topology/reachability/freshness | e4 |
| R3 | Direct/Proxy connections parity | e0/e4/e6 |
| R4 | fabric-primary-only complete-Build topology mutation + Fabric.build_id | e1/e3 |
| R5 | builds.storage (existing BuildStateAdapter) + fabric/mesh/nodes/connections.storage | e1/e3 |
| R6 | DeploymentProvider/Pipeline + exact runtime proof | e2 |
| R7 | LifecycleTransitionPipeline | e3/e4/e6 |
| R8 | canonical product IDs + transport identity separation | e4/e10 |
| R9 | hierarchical membership/backbone/forwarding | e4 |
| R10 | held-member partition repair | e4 |
| R11 | canonical deterministic leadership | e4 |
| R12 | definitive Node/Mesh/Fabric status certainty | e4/e6 |
| R13 | create/recover/replace, authority != executor | e4 |
| R14 | semantic target -> route -> Node RPC composition | e6 |
| R15 | iroh-gossip nonblocking send + immutable pin | e4 |
| R16 | one-process Endpoint/address boundary | e4/e6/e10 |
| R17 | stateful proof/evidence/scenario package | e7/e8 |
| R18 | load/soak reconciliation | e9 |
| R19 | machine-readable import/export gate, including no downstream duplicate control algorithms | e10 |
| R20 | current RuntimeFact publication + runtime-metadata convergence + successor adoption + provider control-domain semantics independent of Build history | e4/e2/e10 |
| R21 | Fabric.build_id + accepted-Build bootstrap/catch-up + same-Build proven-drift attempts | e1/e4/e10 |

---

## 4. Package architecture

```text
crates/rafka-node-rpc-contract/
crates/rafka-mesh-entity/
crates/rafka-mesh-transport/
crates/rafka-node-rpc/
crates/rafka-node-admin-core/
crates/rafka-node-admin-client/
crates/rafka-test-scenario/
crates/rafka-node-rpc-testkit/
crates/rafka-chaos/
```

`rafka-node-admin-core` exports the generic control brain. The concrete public API names may vary, but the semantic surface must let i141 bind:

```text
Fabric record read/write + Fabric.build_id hydration
fabric/mesh/nodes/connections/builds storage interfaces
complete Build acceptance + reconciliation + same-Build drift attempts
LeadershipView / resolver
Build attempt/executor rules
DeploymentProvider including current/published-runtime adoption + provider-domain execution
current RuntimeFact + runtime-metadata projections
Lifecycle transition hooks/status certainty
public Fabric/Mesh/Node projections
```

A downstream wrapper may add policy/evidence but may not reimplement the generic algorithm.

---

## 5. Accepted topology, Build, deployment, exact runtime proof, lifecycle

### 5.1 Accepted topology

The complete Build named by `Fabric.build_id` is the Fabric's accepted topology. There is no second desired-topology record, revision, lineage or fork winner.

```text
current fabric-primary accepts topology change
  -> compile current complete Build topology + requested mutation
  -> persist complete new Build B through builds.storage
  -> persist Fabric.build_id = B through fabric.storage
  -> B is accepted
```

Only the current fabric-primary may accept a topology-changing request. A non-primary refuses by name with the current fabric-primary. Persisted copies are not writers.

Joining/restarting admins hydrate:

```text
FabricRecord { id, name, build_id }
  -> complete Build named by build_id
  -> current membership/runtime facts
  -> authority-bearing Ready
```

Older Build history is not replayed to reconstruct topology.

Topology-changing convenience routes such as create/remove Mesh, add/remove node and shape changes compile the accepted Build plus the requested mutation into a new complete Build. Restart/same-path replacement does not change topology and therefore does not mint a Build.

While the current Build is actively reconciling, another topology-changing request is refused:

```text
409 build-in-progress
current_build_id = B
```

RDM does not implement Build supersession/cancellation.

Storage ownership is generic RDM infrastructure:

```text
fabric.storage
mesh.storage
nodes.storage
connections.storage
builds.storage
```

RDM supplies interfaces plus memory and durable file implementations for restart/chaos proof. `builds.storage` is the existing BuildStateAdapter contract and its memory/FileJournal implementations adopted under the storage name. Rafka later binds production backends.

`fabric.storage` holds at least:

```text
FabricRecord
  id
  name
  build_id

FabricShutdown?
```

Every admin persists the current Fabric record it learns. Replication preserves state across authority loss; it does not grant write authority.

### 5.2 Build-id and attempt semantics

```text
accepted topology change
  -> new complete Build B
  -> persist B
  -> Fabric.build_id = B

active Build B + authority/executor movement
  -> continue B
  -> new attempt only when required

completed Build B + later proven runtime drift
  -> new attempt of B
  -> same Fabric.build_id
  -> no new Build
```

A new Build id means the accepted topology changed. Runtime drift, restart and same-path replacement do not mint topology.

Attempts and receipts append to the Build `Fabric.build_id` names. Evidence for drift carries `build_id=B`, `attempt=N`, `reason=proven-drift`; there is no `reconcile_build_id`.

`DELETE /api/builds?id=B` refuses while `Fabric.build_id == B`, even when the latest attempt is complete.

### 5.3 RuntimeFact / provider / current runtime metadata

Conceptually:

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

RuntimeFact is compact current membership/topology **identity** state. It is immutable for one incarnation and sufficient to identify where/how an eligible successor can adopt/control that exact runtime. It contains no provider secret/control credential.

Do not conflate locator acquisition with publication:

```text
provider / runtime bootstrap seam
  -> produces or discovers exact provider locator + provider_control_domain

node / runtime birth
  -> publishes ONE normalized RuntimeFact through its own membership projection
```

There is no launcher-owned competing authoritative RuntimeFact stream.

Examples:

```text
process
  runtime may self-discover PID + process-start token

container
  provider learns immutable container ID after docker run
  provider bootstrap record/channel makes it available to child before Ready

Day 0
  externally started admin adopts/self-discovers exact runtime before Ready
```

Operational metadata required by a successor is current Node/runtime projection state, not necessarily identity:

```text
RuntimeFact
  = exact runtime identity/fence

current runtime metadata
  = operational context needed to manage/restart the birth
  = e.g. data_dir / resolved storage or executable context where required
```

Required runtime metadata must converge to successor admins and cannot remain launcher-private. `data_dir` does not join RuntimeFact equality/fencing merely because it is needed by restart.

Exact locator semantics:

```text
process:
  provider_control_domain + PID + process-start identity/token

container:
  provider_control_domain + immutable container ID

future provider:
  provider_control_domain + equivalent non-reusable locator
```

Current RDM process/container proof is local to one provider control domain:

```text
process provider   -> one host/process namespace
container provider -> one reachable Docker daemon / host runtime
```

A PID/container ID from another provider control domain must be refused, not acted on. A future multi-host provider must either expose a remotely usable locator/control path or route/delegate exact-runtime actions to a valid executor/agent in that RuntimeFact's provider domain. Election authority remains NodeId-derived; provider locality changes readiness/execution routing only.

A managed process cannot receive the final PID in launch env because the PID does not exist until after spawn. The provider/pipeline owns the bootstrap/adoption seam that obtains/persists the final exact locator + provider domain and makes the matching fact available to the runtime before Ready.

Managed creation includes:

```text
DeployRuntime
RegisterExactRuntimeHandle
ResolveProviderControlDomain
MakeRuntimeFactAvailableToBirth
PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata
```

Day 0 performs:

```text
AdoptCurrentRuntime
RegisterExactRuntimeHandle
ResolveProviderControlDomain
PublishRuntimeFactAndCurrentRuntimeMetadata
```

A successor performs:

```text
current membership RuntimeFact + current runtime metadata
  -> resolve provider_control_domain
  -> provider adopt published runtime
     OR route to a valid domain-local executor
  -> inspect / drain / terminate / restart
```

The local launcher `handles` map may cache handles but is not authority.

A changed locator or provider control domain for the same NodeId+IncarnationId is a named inconsistency/refusal. Inability of one admin to control a nonlocal provider domain is not proof that the runtime is Dead.

### 5.4 Lifecycle and Ready root guard

Lifecycle transitions are explicit operations with blocking hooks. `LifecycleTransition.transition_id` identifies one lifecycle transition execution and keys once-only hook receipts. Node/Mesh/Fabric status-control RPCs do not carry that transition id; they are naturally idempotent on Node `(NodeId, IncarnationId, status)`, Mesh `(MeshId, status)`, and Fabric `(FabricId, event)`. Required semantic cuts include pre-eligibility, post-eligibility/pre-commit, after-transition, before-drain, after-drain.

Node-admin Ready is authority-capable readiness. It may not commit until the current Fabric record + complete accepted Build, RuntimeFacts + required current runtime metadata for held births it may need to manage, and provider-control-domain reachability or a valid domain-local execution path are hydrated.

An ordinary joining/restarted admin may not self-Ready because hydration is unavailable. Only:

```text
Day 0
  no prior Fabric control authority exists
  bootstrap current control state and first Ready through Day-0 lifecycle

single-admin recovery root
  fenced rule proves no live Ready admin exists
  recovery-root self-apply grants Ready only
```

may establish Ready without an existing Ready authority. Neither root assigns leadership; normal NodeId election runs after Ready. Cross-mesh recovery with a live fabric primary does not use the recovery-root shortcut.

### 5.5 Fabric shutdown

Fabric shutdown is Fabric control state, not topology. It mints no Build and leaves `Fabric.build_id` unchanged.

```text
/api/shutdown at non-primary
  -> RejectedNotAuthority(current fabric-primary)

/api/shutdown at fabric-primary
  -> 202
  -> persist FabricShutdown through fabric.storage
  -> disseminate on Fabric control channel

every admin that receives/hydrates it
  -> persist own copy
  -> freeze Build reconciliation, drift recovery and rebirth
  -> gossip MemberStatus::Draining

all current live admin births Draining
  -> freeze barrier satisfied

each mesh-primary
  -> drain ordinary members
  -> drain non-primary admins
  -> exact RuntimeFact for launched or adopted runtime

fabric-primary
  -> drains its own Mesh the same way
  -> waits until each Mesh has no live runtime except its mesh-primary
  -> stops the other mesh-primaries through exact RuntimeFacts
  -> stops itself last
```

`Leaving` alone is not a drained-runtime proof. The runtime must be gone/terminal in the current view before its mesh-primary is stopped.

A runtime still live past the bound, or in a provider control domain its stopper cannot control, is named as shutdown incomplete and is never inferred stopped. `/api/fabric` exposes shutdown phase `frozen|draining|spine` and the incomplete list while the fabric-primary remains alive.

Every admin persists the active FabricShutdown, so it survives fabric-primary loss and an all-admin restart on the same data dirs. An admin that starts/joins while shutdown is active comes up frozen. FabricShutdown is never deleted while its Fabric lives.

An OS signal/local admin stop remains local and never starts Fabric-wide shutdown.

---

## 6. Hierarchical topology, repair, IDs, elections, recovery

### 6.1 Topology

```text
ordinary member
    -> own mesh membership channel

mesh primary
    -> own mesh aggregate on node-admin backbone

receiving peer mesh primary
    -> forward remote aggregate onto own mesh channel
```

Ordinary members never join the backbone. Only node-admins do. Publisher responsibility follows derived leadership. Current membership convergence carries the current RuntimeFact + required current runtime metadata for each birth.

### 6.2 Held-member partition repair

```text
held current member
+ known EndpointId/address
+ stale/missing coverage for repair window
    -> bounded join/refeed attempt
```

Accepted topology alone is not dial material. Failed dial is not death proof. Retries are bounded/backed off per peer/window. Exact-runtime/lifecycle retirement removes the peer from repair targets. Zero-neighbour repair may remain fallback. Relearned membership restores RuntimeFact/current runtime metadata.

### 6.3 Product IDs

```text
NodeId
MeshId
FabricId
format = 12 lowercase Crockford chars
alphabet = 0123456789abcdefghjkmnpqrstvwxyz
payload = 60 random bits
```

One shared primitive backs typed wrappers. BuildId, IncarnationId, DeploymentId, transition IDs, provider handles/control-domain values, EndpointId, and freshness tokens are not converted merely because they contain `Id` or identify something.

Current RDM transport identity formerly named `FabricId` must be renamed/retyped, preferably to Iroh EndpointId directly.

### 6.4 Election canon

```text
eligible = ReadyForTraffic candidates
winner   = lowest complete canonical NodeId
```

Not election inputs:

```text
ready_since_ms
ordinal
mesh name
MeshId
FabricId
creation time
incarnation
deployment id
RuntimeFact/provider locator/provider_control_domain
current runtime metadata
Fabric.build_id
EndpointId
endpoint freshness
incumbency
```

Ownership:
1. node-admin owns each `(mesh, kind)` election;
2. `(mesh,node_admin)` winner is the mesh primary, same seat;
3. only mesh primaries own fabric-primary election;
4. fabric candidates are mesh-primary nodes and use the same comparator.

Day 0 naturally produces node-admin primary == mesh primary == fabric primary.

Restart preserves NodeId. Permanent logical replacement mints a fresh NodeId. A lower eligible NodeId may preempt immediately; no incumbent guarantee exists.

`is_primary`/`is_fabric_primary` are derived projections. Gossiping a claimed winner does not make it authoritative.

### 6.5 Election evidence

Required spans include:

```text
rafka.mesh.election.resolve.via-recompute
  election_level="node_type"
  mesh, kind, candidate_count, eligible_count
  winner_node_id, winner_path, previous_node_id
  election_key="node_id_crockford"

rafka.mesh.election.resolve.via-mesh-primary
  election_level="mesh_primary"
  mesh, winner_node_id, winner_path, previous_node_id
  source_kind="node_admin"

rafka.mesh.election.resolve.via-fabric-recompute
  election_level="fabric_primary"
  fabric, fabric_id, candidate_mesh_primaries
  winner_node_id, winner_path, winner_mesh, previous_node_id
  election_key="node_id_crockford"
```

The mesh-primary span is a role projection, not a second election.

### 6.6 New mesh / recovery ordering

If affected mesh held fabric authority, surviving mesh primaries elect a current fabric primary **first**.

```text
fabric authority exists
  -> fence current old admin births with direct answer + exact runtime/lifecycle facts
  -> birth ONE bootstrap/recovery admin with exact accepted FabricId + MeshId
  -> provider/bootstrap obtains locator/domain; admin publishes RuntimeFact + runtime metadata
  -> normal authenticated first connection
  -> join/adopt exact mesh + backbone
  -> hydrate Fabric.build_id + accepted Build + current RuntimeFacts/runtime metadata + provider-domain capability
  -> downstream trust/Rafka-time hooks where applicable
  -> fabric-primary exact-target ApplyMeshState(Pending)
  <- Applied | AlreadyApplied
  -> ONLY THEN run/hand Build/ReconcileMesh
  -> bootstrap admin locally reconciles own mesh
       preserve valid held live births using RuntimeFacts/runtime metadata
       operate nonlocal runtimes only through valid provider-domain control/executor paths
       retire only canonically proven dead/superseded births
       create missing ordinary members
       create missing sibling node-admins
  -> ReadyForTraffic
  -> normal node-admin election chooses actual mesh primary
  -> elected mesh primary owns normal Mesh lifecycle/status
  -> fabric election recomputes
```

**Authority != executor:** fabric primary owns cross-mesh recovery authority and accepted topology intent. The target bootstrap/recovery admin is the local Build executor. While Pending it may claim/execute Build work only when every affected path belongs to its own mesh. It cannot perform another mesh's or fabric-wide work.

Provider locality is another execution boundary, not an authority boundary. If a RuntimeFact belongs to another provider control domain, the current authority/local Build executor must use the provider's remote-control path or delegate that exact runtime action to a valid domain-local executor. It must not infer Dead because a PID/container ID is nonlocal.

Recovery preserves MeshId/FabricId. True replacement is a separate contract and may mint a new MeshId.

Failed RPC/dial/silence/provider-domain mismatch never authorizes tombstoning. Destructive replacement requires canonical exact-runtime/lifecycle evidence.

---

## 7. Definitive state over short Node RPC

Gossip remains the dissemination/topology plane. Short exact-target Node RPC adds certainty only for explicit Node/Mesh/Fabric lifecycle/control events.

Canonical authority chain:

```text
ordinary node        -> elected mesh primary       DeclareNodeState
node-admin            -> fabric primary             DeclareNodeState
elected mesh primary -> fabric primary             DeclareMeshState
fabric primary        -> bootstrap node-admin       ApplyMeshState(Pending)
fabric primary        -> elected mesh primary       ApplyFabricEvent
```

Replies include:

```text
Applied
AlreadyApplied
RejectedStaleBirth
RejectedNotAuthority
RejectedInvalidTransition
NotSent
Indeterminate
```

Pending-before-Build is hard: only `Applied|AlreadyApplied` permits reconciliation to start. A retry resends the same naturally-idempotent operation with exact target/birth fencing. Status/control natural keys are Node `(NodeId, IncarnationId, status)`, Mesh `(MeshId, status)`, and Fabric `(FabricId, event)`; these calls carry no transition id.

After durable cutover, `Applied` means the authoritative write ACKed.

Single-admin restart may use the defined recovery-root Ready self-apply only if no live Ready admin exists. It grants Ready only; election still runs. Ordinary joining admins never use that as a fallback for failed hydration.

---

## 8. Semantic target -> connections -> Node RPC

```text
domain selector chooses ExactNode(target)
  -> connections effective_route(target)
      Direct | ViaPeer(exact carrier) | NoActiveRoute
  -> Node RPC executes exactly target
```

Connections never reselects a semantic destination. Node RPC never owns Proxy lifecycle. A carrier performs exactly one direct inner invocation to the original target.

---

## 9. Node RPC runtime/pool/certainty

Generic proof ops may remain simple stateful test operations. Production contract requirements are:
- stable protocol catalog and typed unknown variant handling;
- exact final target preservation;
- complete-send commit cut;
- incomplete send -> 499/NotSent;
- post-commit reply ambiguity -> Indeterminate;
- no automatic replay of mutating Indeterminate;
- semantic pool keyed by process identity/incarnation and exact endpoint-slot freshness;
- stale slot cancellation does not evict valid sibling slots.

---

## 10. Proof shapes and mandatory scenarios

Minimum generic rungs:

```text
SN = 1 node-admin + 1 ordinary RPC node
MN = 2 node-admins + 3 ordinary RPC nodes
MM = two MN meshes under one fabric
```

Mandatory proof includes:
- elastic grow/shrink;
- shape/resize assertions use accepted-Build per-cohort counts/settled shape rather than assuming surviving ordinals remain contiguous;
- identity/election/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
- no election test remembers an incumbent; expected winner is calculated from the current public eligible candidates;
- node restart and replacement;
- three-mesh election with names/ordinals/MeshIds deliberately shuffled;
- fabric-primary loss with proven runtime death;
- secondary admin-cohort loss with ordinary survivors;
- partial member survival;
- all-member loss under same MeshId;
- three-mesh cascading authority loss;
- partition/heal where both halves retain neighbours and held-member repair rejoins them;
- Pending bootstrap executor creates missing sibling admins only in its own mesh;
- Day-0 admin exact runtime adoption/death proof;
- Build-launched process/container produces/discovers exact locator and the runtime publishes the normalized RuntimeFact;
- non-launcher admin learns `data_dir`/required current runtime metadata without asking the original launcher;
- lower-NodeId admin born **after an older Build completed** hydrates `Fabric.build_id` + the accepted Build + RuntimeFacts/runtime metadata + provider-domain capability, becomes Ready, wins and manages older live runtimes without completed Build receipts;
- the parked s14 RED (`no Build recorded the birth … it cannot be adopted`, trace `eb8f18449f2d1479`) is reproduced before #2850/#2851 and GREEN after them;
- older Build history is forgotten, the Build `Fabric.build_id` names remains recoverable, and later exact runtime death opens a new attempt of that same Build;
- PID reuse/container-name reuse cannot impersonate a previous exact runtime;
- process successor in the same host control domain can manage a non-launcher exact process runtime, while a PID from another control domain is refused;
- container successor sharing the same Docker control domain can manage a non-launcher immutable container ID, while a locator from another domain is refused;
- any multi-host proof demonstrates remotely controllable locators or domain-local exact-runtime execution without changing NodeId election ordering;
- ordinary joining admin cannot self-Ready around missing control hydration; only Day-0/fenced single-admin recovery-root semantics are allowed.

Blackbox tests calculate expected election winners independently from public NodeIds/status, then compare public views + OTLP.

---

## 11. Gossip substrate gates

### 11.1 Nonblocking iroh-gossip send

The actor must never await capacity on one peer's bounded queue. Active/full peer enqueue drops that enqueue and continues; ordinary Plumtree repair owns convergence. Closed queue preserves cleanup.

The reciprocal queue-saturation regression is export evidence. No RDM resend/throttle workaround substitutes for the library fix.

### 11.2 Address/repair boundary

Canonical gossip bootstrap and rejoin use explicit EndpointId/address knowledge from product topology/seeds. Hidden N0 lookup is not repair authority.

Application repair uses held-member stale coverage as described in §6.2. Do not add another peer scheduler on top of iroh-gossip.

RuntimeFact is compact current-birth identity state and rides ordinary membership convergence. Required runtime-management metadata may ride beside it in the current birth/Node projection. Do not add a completed Build-receipt archive to the Build topic; the 4032-byte gossip frame budget remains a hard boundary.

---

## 12. Chaos and observability

Layers:

```text
model/action + shrink
explicit failpoints
deterministic network/time where practical
real process/container fault backend
seeded soak
```

Wedge cuts include request pre-send/post-apply, Build/deployment stall, Fabric.build_id/accepted-Build persistence and catch-up, build-in-progress refusal, RuntimeFact production/publication/adoption/provider-domain stall, current runtime-metadata hydration stall, lifecycle hook stall, Pending-before-Build stall, election authority movement, stale-slot race, partition/heal, exact-runtime exit, FabricShutdown freeze/drain/restart, and recovery authority/executor handoff.

Every E2E produces blackbox state plus OTLP artifacts. Fault injection itself is never the success criterion.

### 12.1 Seeded soak (export bar)

One deterministic endurance gate, not a second scenario suite:

```text
shape:     multi-Mesh estate; enough admins for real mesh-primary and fabric-primary movement;
           ordinary rpc nodes in every Mesh
duration:  30 minutes
seed:      fixed, printed in the evidence, rerunnable exactly
faults, repeatedly:
    node-admin restart          ordinary node restart
    same-path replacement       exact runtime kill
    mesh-primary loss           fabric-primary loss
    temporary partition + heal  endpoint slot/freshness supersession
```

It passes on outcomes, never on faults detected:

- no hang or deadlock;
- no two current births for one path;
- no two accepted topology writers;
- no duplicate drift attempt opened for one proven loss;
- no Build minted for unchanged-topology drift;
- no stale-slot application dispatch;
- no permanent split after a partition heals;
- no loss of `Fabric.build_id` or the accepted Build it names;
- no unexplained orphan attempt or claim;
- the final accepted topology converges, and every surviving expected path is reachable and current.

A named refusal the architecture calls for is a pass. An unexplained timeout, silent drop, duplicate authority action or non-convergence fails the soak.

### 12.2 Container proof (export bar)

The named container cells, not the whole scenario gate rerun under `MESH_SPAWN_TYPE=container`:

- exact container identity is the provider control domain plus the immutable container ID;
- a successor in the same provider control domain adopts, inspects and controls the exact container;
- an authority in another provider control domain never treats the locator as local: it refuses by name or delegates to a domain-local executor;
- a real container kill: exact provider inspection proves it terminal, that is the death evidence, and recovery proceeds. This cell runs through the Build and runtime-adoption path, not the provider alone.

---

## 13. Story ordering and late gates

Canonical control order:

```text
e4.s12 #2811  iroh-gossip nonblocking send/pin
  -> e4.s13 #2816 explicit Iroh address boundary
  -> e4.s9  #2801 hierarchical topology + held-member repair reproof
  -> e4.s15 #2842 canonical product IDs / transport-id split
  -> e4.s16 #2850 current RuntimeFact/runtime-metadata publication + successor adoption + provider-domain contract
  -> storage #48 fabric.storage + existing builds.storage adoption
  -> storage #43/#44/#45 mesh/nodes/connections.storage
  -> topology #47 Fabric.build_id accepted topology + same-Build drift
  -> e4.s14 #2840 deterministic leadership + authority-capable Ready proof
  -> e6.s8 #2815 one process Endpoint + request slot/freshness framing
  -> e6.s4 generic Forward
  -> e6.s5 connections integration
  -> e6.s6 #2802 semantic-target routing composition
  -> e6.s7 #2804 + e4.s11 #2805 definitive lifecycle/status certainty
  -> e6.s9 #42 caller identity + W3C context
  -> e4.s10 #2803 cross-mesh recovery
  -> e4.s8 replacement revalidation
```

`#2850`, #48, #43/#44/#45 and #47 are hard prerequisites for s14 GREEN. Fabric.build_id/accepted-Build hydration, RuntimeFact/runtime metadata, and provider domain are not election keys; they are prerequisites for a node-admin to truthfully commit the one eligibility state, `ReadyForTraffic`. #2851's independent desired-revision model is superseded by #47.

The first s14 branch is parked at `i143-e4-s14` commit `243ff4a`; rebase it after #2850, the storage stories, and #47, then redo its `docs/i143/design.md` reconciliation against the then-current design-of-record rather than restoring older wording.

The Node RPC order above is intentional: e6.s8 changes endpoint ownership and request framing, so Forward/routing/control RPC must build on it rather than be retrofitted later.

### 13.1 e11 — the integration proof-of-concept, before i141

The export gate proved the substrate on RDM's own proof nodes. Before i141 imports it into Rafka, this
epic proves the import pattern itself, in RDM, with RDM's own role binaries: the packaging, a
`rafka-node-base` that imports the packages the way Rafka's node-base will, the role binaries
(`broker`, `gateway`, `compute`, `registry`) built on that base, and one small test per Node RPC
feature Rafka extends. Each story is its test and its implementation together; a feature without a
cell is not integrated.

```text
e11.s1  packaging: the import manifest is the eight packages at one rev; `rafka-node-base` depends on
        them by workspace path exactly as rafka-v2 depends on them by git+rev; the dep-rules audit
        holds the import boundary (no RDM package imports node-base)
e11.s2  node-base imports: one Iroh Endpoint per process carrying the mesh, gossip and Node RPC ALPNs;
        one sealed effective catalog (core ping + the product's transitional adapters); one
        NodeRpcServer accepting on the Node RPC ALPN; one LiveNodeResolver fed by membership; one
        NodeRpcClient
        cells: a role process serves core ping on its one endpoint; a tag the catalog does not hold
        is 421; a product adapter is catalogued and unserved on the Node RPC ALPN
e11.s3  role binaries on the base: broker/gateway/compute/registry build on node-base, born by
        node-admin as NodeKind::{Broker,Gateway,Compute} path.names (`mesh1.broker.1`), publishing
        RuntimeFact and the kind's status like any rpc node
        cell: a Build of {node_admin:1, broker:1, gateway:1, compute:1} converges; each birth's ready
        span names its kind; a second Build removes the compute and the path is retired
e11.s4  leadership, lite: every role cohort elects its lowest ReadyForTraffic NodeId through the
        imported LeadershipResolver; a product reads the mesh primary from the public view and never
        computes it
        cell: kill the broker primary; the next-lowest broker is primary in every view; no
        product-side comparator exists (grep gate)
e11.s5  exact target + fence: a gateway calls a broker by ExactNode and by CurrentPath; after the
        broker restarts, the old incarnation's pooled connection is evicted and a call to it is
        RejectedStale (425), never dispatched
        cell: `node_rpc__restart_fence` on the role shape
e11.s6  certainty: a request cut before FIN is NotSent (499) and never applied; a reply lost after
        commit is Indeterminate; a handler fault is 423; the reply budget alone is not death proof
        cell: the four outcomes on a gateway->broker call, each asserted on its span reason
e11.s7  routing/proxy, lite: a gateway that cannot reach a broker directly invokes it ViaPeer through
        another gateway; the carrier makes exactly one inner invocation and never changes the final
        target; NoActiveRoute starts no leg
        cell: Direct, ViaPeer and NoActiveRoute, each on the role shape, with the carrier's
        `rafka.node_rpc.request.serve.via-forward` span
e11.s8  product family: a product-owned unary family (`broker_data`, forwardable) composed beside the
        core, served only by the broker; a gateway's call to a compute for it is 421
        cell: the family round-trips to a broker and is unserved at a compute
e11.s9  observability pass-through: caller_system and W3C traceparent/tracestate/baggage ride the
        request header; a malformed value is dropped (`via-context-dropped`) and the outcome is
        unchanged; the broker's serve span is a child of the gateway's call span in Jaeger
        cell: one call, one trace, two services
e11.s10 status/lifecycle on a role: a broker declares ReadyForTraffic to its mesh primary over the
        status op; a wedged broker is tickled (ping, then the status kick) and marked
        pending-reconnect then dead; drift rebirths it at the same path
        cell: `mesh_runtime__role_wedge`
```

Shape for every cell: one mesh, one node-admin, and the roles the cell names; the process provider.
Nothing of Rafka's domain (orgs, topics, writers) enters: the families are proof families. The epic is
done when the gate runs every cell green and the import manifest is unchanged by it.

---

## 14. Export/import gate

i141 pins RDM once: the exact 40-hex merged `main` commit on which the complete export gate below is green. There is no partial pin and no sequence of pins.

`eligible=true` requires one exact RDM SHA with proof that:

### Identity
- NodeId/MeshId/FabricId are canonical Crockford60 typed logical IDs;
- EndpointId is distinct transport identity;
- no canonical 32-char hex logical product IDs remain;
- recovery preserves MeshId/FabricId; restart preserves NodeId; replacement mints new logical identity where required.

### Accepted topology / Build
- the current fabric-primary is the only topology writer; non-primary topology mutation is refused by name;
- `Fabric.build_id` names one complete accepted Build and no independent desired-topology authority exists;
- complete Build persistence precedes the `Fabric.build_id` write;
- every admin can hydrate the Fabric record + complete Build before authority-bearing Ready;
- older Build history is not replayed to reconstruct topology;
- topology-changing convenience APIs compile the current complete topology plus their mutation into a new complete Build;
- a second topology-changing request while the current Build is reconciling is `409 build-in-progress`;
- authority movement, restart, same-path replacement and proven drift with unchanged topology use another attempt of the same Build;
- `DELETE /api/builds?id=B` refuses while `Fabric.build_id == B`;
- `GET /api/fabric` exposes `build_id`, not a second stored desired-topology object;
- all-admin restart restores the current Fabric record, accepted Build and active FabricShutdown from RDM storage interfaces.
 
### Runtime/death/provider domain
- every current birth carries a compact typed RuntimeFact bound to exact NodeId + IncarnationId + DeploymentId + provider + provider control domain + exact locator;
- provider/bootstrap produces/discovers the locator and the node publishes exactly one normalized current RuntimeFact;
- no competing launcher-owned RuntimeFact authority exists;
- required current runtime-management metadata such as `data_dir` converges to non-launcher authorities without becoming RuntimeFact identity by default;
- late successor can adopt/manage current runtime without completed Build receipts;
- Day-0 externally started admin registers/publishes an exact provider runtime/current metadata;
- process identity resists PID reuse; container identity uses immutable container ID;
- no PID/container ID is assumed globally meaningful across provider domains;
- same-incarnation locator or provider-domain change is refused;
- current process/container proof demonstrates same-domain successor control and cross-domain refusal;
- any multi-host eligibility proves remotely controllable locators or domain-local exact-runtime execution;
- provider terminal state for the adopted exact runtime is available to lifecycle/recovery;
- failed RPC/dial/silence/provider-domain mismatch is never exported as death proof.

### Leadership
- only ReadyForTraffic candidates are eligible;
- node-admin Ready includes current Fabric.build_id + accepted Build + RuntimeFact/runtime-metadata + provider-domain authority-capable hydration;
- normal joining admins cannot self-Ready around missing hydration; only Day-0/fenced single-admin recovery root may establish Ready without existing Ready authority;
- complete NodeId is the only election key;
- provider control domain/locality/runtime metadata is not an election key;
- node-admin cohort winner == mesh primary;
- only mesh primaries own fabric election;
- Day 0 + three-mesh + preemption/replacement + late-admin takeover proofs are green;
- s14's non-launcher-adoption RED is GREEN through #2850/#2851;
- no stale amendment/parallel election canon exists.

### Repair/recovery
- held-member repair heals a partition while each side retains neighbours;
- failed rejoin is bounded and never marks Dead;
- if fabric authority is lost, fabric election completes before recovery starts;
- recovery proves exact identity -> RuntimeFact/runtime-metadata/provider-domain hydration -> Fabric.build_id + accepted-Build hydration -> Pending Applied -> reconcile -> own-mesh local execution -> Ready -> election;
- Pending executor cannot claim another mesh/fabric-wide work;
- exact-runtime work in another provider domain uses valid remote/domain-local execution, never local PID/container guessing;
- fabric authority and local recovery executor are distinct;
- the accepted Build remains recoverable independently of older Build history.

### Downstream binding
The i141 import manifest proves Rafka binds to the exported generic mechanisms rather than recreating them:

```text
one imported Fabric.build_id/accepted-Build bootstrap-catch-up + same-Build drift mechanism
one imported Build/Reconciler
one imported LeadershipResolver
one imported RuntimeFact + current runtime-metadata adoption/provider-domain contract
one imported recovery/executor rule
one imported exact-runtime provider contract
one imported held-member repair mechanism
```

After i141 cutover, any active Rafka-local generic comparator, incumbent ladder, lowest-name fabric selection, shape/drift/recovery algorithm, launcher-receipt/runtime-metadata authority, host-local provider guess, or conflicting executor rule is a blocking import defect. Compatibility wrappers are allowed only as stateless delegates.

### Transport/Node RPC
- one canonical Iroh Endpoint per process or named/proved exception;
- no second generic internal mTLS stack;
- no hidden N0 DNS/Pkarr product address authority;
- Direct/Proxy connections parity and target-preserving Node RPC composition are green;
- complete-send certainty, 499, pool supersession, and carried execution are green.

The final import also reconciles downstream `node-lifecycle.md`, feature docs, tests, runbooks, and RDM `docs/i143/design.md` so target vocabulary no longer uses legacy transport `fabric_id`, ready_since/lowest-name/incumbent election scoring, launcher-private completed Build receipts/runtime metadata as runtime authority, cross-host PID/container assumptions, zero-neighbour-only repair, ordinary self-Ready fallback, or silence/RPC failure as recovery death proof.

---

## 15. Verification artifacts

Each E2E/hostile run emits:

```text
scenario manifest + seed
FabricRecord snapshots including build_id
accepted Build requests/folded attempt views
Fabric/Mesh/Node snapshots
membership RuntimeFact snapshots
current runtime-metadata snapshot/digest where required
leadership candidate/result evidence
runtime adopt/inspect/exit evidence
provider-domain resolution/execution evidence
lifecycle/Pending receipts
RPC ledger
partition/repair ledger
fault ledger
OTLP JSONL
trace URLs
final reconciliation report
```

Identity proof records `node_id`, `mesh_id`, `fabric_id`, `id_format="crockford60"`, and separate EndpointId evidence.

Recovery proof records at least:

```text
fabric_id
mesh_id
mesh_name
build_id
attempt
recovery_admin_node_id
provider
provider_control_domain_fingerprint
runtime_locator_kind
runtime_locator_fingerprint
runtime_metadata_digest where applicable
adopter_node_id
execution_node_id
desired_shape
live_before
preserved_count
tombstoned_count
created_count
live_after
```

---

## 16. Guardrails

- no second topology mutation/reconciliation engine;
- no downstream topology/drift engine beside imported RDM `Fabric.build_id -> Build` control state;
- no downstream election comparator beside imported LeadershipResolver;
- no launcher-private handle map, runtime metadata, or completed Build receipts as current-runtime authority;
- no competing launcher-owned RuntimeFact publication stream;
- no completed Build history replay as the accepted-topology database;
- no independent desired-topology revision/lineage/fork authority beside `Fabric.build_id -> Build`;
- no ordinary self-Ready fallback when current control state cannot be hydrated;
- no character-sum/hash election score;
- no ready_since/ordinal/mesh-name/MeshId/FabricId/incumbent election priority;
- no RuntimeFact/provider locator/provider control domain/current runtime metadata/Build id as election priority;
- no logical FabricId used as EndpointId or transport key;
- no broad conversion of BuildId/IncarnationId/DeploymentId/provider-domain/freshness/transition IDs to Crockford merely for uniformity;
- no recovery MeshId/FabricId remint;
- no PID-only process death identity;
- no container-name-only exact identity;
- no assumption that PID/container ID is globally meaningful across provider domains;
- no `data_dir` treated as exact runtime identity without a provider-specific reason;
- no Build before Pending Applied;
- no bootstrap executor treated as elected primary;
- no Pending executor outside its own mesh;
- no failed RPC/dial/silence/provider-domain mismatch interpreted as Dead;
- no zero-neighbour-only repair requirement;
- no application resend protocol beside iroh-gossip;
- no completed-runtime receipt archive on Build gossip;
- no exact surviving-ordinal/name assumption in elastic shape tests where cohort count is the semantic contract;
- no semantic target selection inside connections/Node RPC;
- no second internal mTLS stack;
- no hidden N0 address authority;
- no private/internal map as E2E truth.

---

## 17. Definition of done

- exact eligible RDM revision exported;
- Fabric.build_id/accepted-Build hydration + same-Build drift, Build/Reconciler and LeadershipResolver live once in `rafka-node-admin-core`;
- RuntimeFact publication/adoption + runtime-metadata convergence + provider-control-domain handling is part of current membership/runtime state;
- provider/bootstrap locator production and node-owned RuntimeFact publication are distinct and proven;
- i141 has clear adapter APIs and no need to copy algorithms;
- NodeId/MeshId/FabricId and EndpointId meanings are unambiguous;
- Day-0 runtime adoption/control-root bootstrap and exact death proof are green;
- ordinary joining admin cannot self-Ready around missing control hydration;
- late non-launcher authority can manage current runtimes and required runtime metadata after completed Build history is gone;
- current process/container proof demonstrates same-provider-domain successor control and rejects cross-domain PID/container misuse;
- multi-host eligibility, when claimed, proves remote provider control or domain-local exact-runtime execution without changing election ordering;
- the Build `Fabric.build_id` names remains recoverable after older Build history is forgotten and drives later recovery;
- hierarchical membership and held-member partition repair are green;
- canonical NodeId leadership is green at node-type, mesh-primary, and fabric-primary levels;
- s14's parked non-launcher-adoption RED is GREEN after #2850/#2851;
- Pending/status certainty is green;
- create/recover/replace and authority/executor separation are green;
- active Build id survives authority movement; later independent drift gets a new Build id;
- Direct/Proxy target-preserving Node RPC is green;
- process/container scenarios, deterministic faults, real chaos, and soak are green;
- parity ledger has zero blocking gaps;
- final manifest proves downstream import uses the library mechanisms rather than a parallel Rafka control brain.