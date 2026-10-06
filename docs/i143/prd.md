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
current DesiredTopologyProjection
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
2. `rafka-node-admin-core` is the **single generic implementation owner** of DesiredTopologyProjection/drift, Build reconciliation, leadership projection, executor/attempt rules, runtime adoption, generic Fabric/Mesh/Node lifecycle and recovery mechanics.
3. i141 binds Rafka adapters/policy into that library. i142 consumes the resulting views/mechanisms and must not reimplement them.
4. Keep three distinct control facts:

```text
DesiredTopologyProjection = what should exist
membership + RuntimeFacts + current runtime metadata = what exact current births/runtimes exist
BuildStateAdapter facts   = how one reconciliation is executing
```

5. Completed Build history is neither the desired-state database nor the current-runtime database.
6. Build identity belongs to active reconciliation state. Authority movement during an active Build keeps the same `build_id` and desired revision. Later proven drift after a prior Build completed creates a **new** reconciliation Build against the unchanged desired revision.
7. Desired topology is bounded Fabric control state and survives Build completion/history forget. Conflicting desired updates are revision-fenced/refused, never silent last-write-wins.
8. A joining/reconnecting authority-bearing admin hydrates current DesiredTopologyProjection through Fabric control-state bootstrap/catch-up, not by replaying active/completed Build history.
9. Deployment provider is Fabric policy selected at first bootstrap; ordinary Build requests never choose it.
10. Every current managed runtime publishes a compact typed RuntimeFact bound to exact NodeId + IncarnationId + DeploymentId + provider + provider control domain + exact provider locator. A future eligible admin can adopt/manage the runtime without launcher-private Build receipts.
11. Provider/runtime bootstrap **produces or discovers** the exact provider locator. The node/runtime birth publishes the one normalized RuntimeFact through its own membership projection. There is no competing launcher-owned authoritative RuntimeFact stream.
12. Current operational metadata required by a successor, for example `data_dir`, is current Node/runtime projection state. It must converge to future authorities but is not part of RuntimeFact identity merely because it is needed for management.
13. The externally started Day-0 admin adopts/registers its own exact provider runtime and publishes the same RuntimeFact/current runtime metadata before authority-bearing Ready.
14. Process exact identity is provider-control-domain + PID + process-start token or stronger; PID alone is insufficient. Container identity is provider-control-domain + immutable container ID; deterministic container name alone is insufficient.
15. A provider locator is valid only inside its provider control domain. Current process/container proof is same-host/same-provider-domain. Future multi-host control requires either remotely controllable locators or domain-local exact-runtime execution. Provider locality never changes election priority.
16. Provider exact-handle terminal status can prove that exact runtime exited. RuntimeFact presence, provider-domain mismatch, RPC failure, failed dial, gossip silence, DNS disappearance, or partition cannot.
17. `LifecycleTransitionPipeline` owns Node/Mesh/Fabric transition policy and blocking hooks.
18. Normal joining/restarted admins do not self-Ready around missing control hydration. Day 0 and the explicitly fenced single-admin recovery-root are the only lifecycle roots that may establish Ready without an already-live Ready authority; election still runs afterward.
19. SN/MN/MM are minimum proof rungs, not caps.
20. Mesh creation, recovery, and intentional replacement are separate Build contracts.
21. Fabric/Mesh views expose current owning node-admin control endpoints; tests never depend on hidden maps.
22. Node RPC certainty commits only after complete request send + request-direction finish. Incomplete send -> 499/NotSent.
23. Direct/Proxy connection truth is imported from the i66.e3 parity source; RDM does not create a second carrier-evidence or reconnect authority.
24. Domain routing selects WHICH final node. Connections selects HOW to reach it. Node RPC executes it.
25. Gossip/iroh-gossip owns membership/dissemination/repair. RDM does not add an application resend protocol.
26. Partition repair is **held-member stale-coverage repair**, not `neighbor_count == 0` as the only trigger.
27. Iroh owns authenticated transport identity and QUIC. Logical product identity is separate.
28. Logical `NodeId`, `MeshId`, `FabricId` are bare 12-character lowercase Crockford Base32 values carrying 60 random bits.
29. Only `NodeId` is an election ordering key. `MeshId` and `FabricId` are equality-only identities.
30. Iroh `EndpointId` is transport identity. It must never be represented by logical `FabricId`.
31. Canonical leadership is ReadyForTraffic candidates -> lowest complete NodeId. Ready is the only election eligibility bit.
32. For node-admins, Ready means authority-capable hydration is complete: current topology, current DesiredTopologyProjection, RuntimeFacts + required runtime metadata for held births it may have to manage, and provider-control-domain reachability or a valid domain-local execution path. These facts are readiness prerequisites, not election scoring.
33. Product chaos remains downstream i87; i143 proves generic deterministic + real process/container fault behavior.
34. Transitional Rafka legacy tags exist only as downstream i141/i142 adapters. Generic RDM has no Rafka legacy handlers.

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
    current DesiredTopologyProjection
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
| R4 | Build-only topology mutation | e1/e3 |
| R5 | BuildStateAdapter | e1 |
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
| R21 | current DesiredTopologyProjection + bootstrap/catch-up hydration + post-completion drift reconciliation | e1/e4/e10 |

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
DesiredTopologyProjection read/update/revision fence + bootstrap/catch-up
Build/Reconcile desired state + automatic proven-drift reconciliation
LeadershipView / resolver
Build attempt/executor rules
DeploymentProvider including current/published-runtime adoption + provider-domain execution
current RuntimeFact + runtime-metadata projections
Lifecycle transition hooks/status certainty
public Fabric/Mesh/Node projections
```

A downstream wrapper may add policy/evidence but may not reimplement the generic algorithm.

---

## 5. Desired state, Build, deployment, exact runtime proof, lifecycle

### 5.1 Current desired topology

Current desired topology is bounded Fabric control state. Every authority-capable node-admin can hydrate it. `DELETE /api/builds` removes Build-history views only; it does not erase desired topology.

Topology-changing requests update desired state under an expected/current desired revision or equivalent insert-and-fail contract. Concurrent stale writers cannot silently overwrite one another.

Hydration paths are explicit:

```text
first connection / entry pull
  -> current DesiredTopologyProjection + desired_revision

admin-to-admin control catch-up / reconnect
  -> latest current DesiredTopologyProjection + desired_revision

Build-topic catch-up
  -> active Build facts only
```

The exact control message may reuse an existing entry/control snapshot or a bounded dedicated current-state record. Completed Build receipts never reconstruct the desired estate.

### 5.2 Build-id semantics

```text
active Build B + authority/executor changes
  -> same build_id B
  -> same desired revision
  -> replan desired - observed

completed Build A + later proven runtime drift
  -> new reconciliation Build B
  -> current unchanged desired revision
```

Unreachability alone is not proven drift requiring replacement.

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

Node/Mesh/Fabric transitions are explicit operations with stable transition IDs, idempotent receipts, and blocking hooks. Required semantic cuts include pre-eligibility, post-eligibility/pre-commit, after-transition, before-drain, after-drain.

Node-admin Ready is authority-capable readiness. It may not commit until the current desired revision, RuntimeFacts + required current runtime metadata for held births it may need to manage, and provider-control-domain reachability or a valid domain-local execution path are hydrated.

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

Desired intent alone is not dial material. Failed dial is not death proof. Retries are bounded/backed off per peer/window. Exact-runtime/lifecycle retirement removes the peer from repair targets. Zero-neighbour repair may remain fallback. Relearned membership restores RuntimeFact/current runtime metadata.

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
DesiredTopology revision
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
  -> birth ONE bootstrap/recovery admin with exact desired FabricId + MeshId
  -> provider/bootstrap obtains locator/domain; admin publishes RuntimeFact + runtime metadata
  -> normal authenticated first connection
  -> join/adopt exact mesh + backbone
  -> hydrate topology + current desired state + current RuntimeFacts/runtime metadata + provider-domain capability
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

**Authority != executor:** fabric primary owns cross-mesh recovery authority and desired intent. The target bootstrap/recovery admin is the local Build executor. While Pending it may claim/execute Build work only when every affected path belongs to its own mesh. It cannot perform another mesh's or fabric-wide work.

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

Pending-before-Build is hard: only `Applied|AlreadyApplied` permits reconciliation to start. A retry resends the same operation, idempotent on its natural key, with exact target/birth fencing.

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
- shape/resize assertions use desired per-cohort counts/settled shape rather than assuming surviving ordinals remain contiguous;
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
- lower-NodeId admin born **after an older Build completed** hydrates desired state + RuntimeFacts/runtime metadata + provider-domain capability, becomes Ready, wins and manages older live runtimes without completed Build receipts;
- the parked s14 RED (`no Build recorded the birth … it cannot be adopted`, trace `eb8f18449f2d1479`) is reproduced before #2850/#2851 and GREEN after them;
- completed Build history is forgotten, current desired state remains, later exact runtime death creates a new reconciliation Build against the same desired revision;
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

The exact fork/revision and reciprocal queue-saturation regression are export evidence. No RDM resend/throttle workaround substitutes for the library fix.

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

Wedge cuts include request pre-send/post-apply, Build/deployment stall, desired-state bootstrap/catch-up/update conflict, RuntimeFact production/publication/adoption/provider-domain stall, current runtime-metadata hydration stall, lifecycle hook stall, Pending-before-Build stall, election authority movement, stale-slot race, partition/heal, exact-runtime exit, and recovery authority/executor handoff.

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
  -> e1.s7  #2851 current DesiredTopologyProjection + bootstrap/catch-up + drift reconciliation
  -> e4.s14 #2840 deterministic leadership + authority-capable Ready proof
  -> e6.s7 #2804 + e4.s11 #2805 definitive lifecycle/status certainty
  -> e4.s10 #2803 cross-mesh recovery
  -> e4.s8 replacement revalidation
```

`#2850` and `#2851` may be implemented in parallel where code dependencies allow, but **both are hard prerequisites for s14 GREEN**. Desired revision, RuntimeFact/runtime metadata, and provider domain are not election keys; they are prerequisites for a node-admin to truthfully commit the one eligibility state, `ReadyForTraffic`.

The first s14 branch is parked at `i143-e4-s14` commit `243ff4a`; rebase it after #2850/#2851 and redo its `docs/i143/design.md` reconciliation against the then-current design-of-record rather than restoring older wording.

Other late export gates include #2815 one-process Endpoint and #2802 semantic routing composition.

---

## 14. Export/import gate

i141 pins RDM once: the exact 40-hex merged `main` commit on which the complete export gate below is green. There is no partial pin and no sequence of pins.

`eligible=true` requires one exact RDM SHA with proof that:

### Identity
- NodeId/MeshId/FabricId are canonical Crockford60 typed logical IDs;
- EndpointId is distinct transport identity;
- no canonical 32-char hex logical product IDs remain;
- recovery preserves MeshId/FabricId; restart preserves NodeId; replacement mints new logical identity where required.

### Desired topology / Build
- one bounded current DesiredTopologyProjection exists and is hydrated by new admins through first-entry/control-state catch-up independently of active Build facts;
- reconnect catch-up repairs a missed desired revision;
- Build history forget does not erase desired topology;
- conflicting desired updates are revision-fenced;
- active Build authority movement preserves build_id;
- post-completion proven drift creates a new reconciliation Build against the unchanged desired revision;
- no reachability failure alone creates replacement drift;
- ordinary joining admin cannot self-Ready around missing desired-state hydration.

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
- node-admin Ready includes current desired + RuntimeFact/runtime-metadata + provider-domain authority-capable hydration;
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
- recovery proves exact identity -> RuntimeFact/runtime-metadata/provider-domain hydration -> desired-state hydration -> Pending Applied -> reconcile -> own-mesh local execution -> Ready -> election;
- Pending executor cannot claim another mesh/fabric-wide work;
- exact-runtime work in another provider domain uses valid remote/domain-local execution, never local PID/container guessing;
- fabric authority and local recovery executor are distinct;
- desired shape survives completed Build history.

### Downstream binding
The i141 import manifest proves Rafka binds to the exported generic mechanisms rather than recreating them:

```text
one imported DesiredTopologyProjection/drift/bootstrap-catch-up mechanism
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
DesiredTopologyProjection snapshots/revisions
Build requests/folded views
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
desired_revision
source_build_id
reconcile_build_id
recovery_admin_node_id
provider
provider_control_domain_fingerprint
runtime_locator_kind
runtime_locator_fingerprint
runtime_metadata_digest where applicable
adopter_node_id
execution_node_id
pending_transition_id
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
- no downstream desired-state/drift engine beside imported RDM control state;
- no downstream election comparator beside imported LeadershipResolver;
- no launcher-private handle map, runtime metadata, or completed Build receipts as current-runtime authority;
- no competing launcher-owned RuntimeFact publication stream;
- no completed Build history as desired-state database;
- no active-Build facts as a substitute for current DesiredTopologyProjection hydration;
- no ordinary self-Ready fallback when current control state cannot be hydrated;
- no character-sum/hash election score;
- no ready_since/ordinal/mesh-name/MeshId/FabricId/incumbent election priority;
- no RuntimeFact/provider locator/provider control domain/current runtime metadata/desired revision as election priority;
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
- DesiredTopologyProjection/drift/bootstrap-catch-up, Build/Reconciler and LeadershipResolver live once in `rafka-node-admin-core`;
- RuntimeFact publication/adoption + runtime-metadata convergence + provider-control-domain handling is part of current membership/runtime state;
- provider/bootstrap locator production and node-owned RuntimeFact publication are distinct and proven;
- i141 has clear adapter APIs and no need to copy algorithms;
- NodeId/MeshId/FabricId and EndpointId meanings are unambiguous;
- Day-0 runtime adoption/control-root bootstrap and exact death proof are green;
- ordinary joining admin cannot self-Ready around missing control hydration;
- late non-launcher authority can manage current runtimes and required runtime metadata after completed Build history is gone;
- current process/container proof demonstrates same-provider-domain successor control and rejects cross-domain PID/container misuse;
- multi-host eligibility, when claimed, proves remote provider control or domain-local exact-runtime execution without changing election ordering;
- current desired topology survives Build completion/history forget and drives later recovery;
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