# i143 - Build, prove, and export the generic Mesh product on RDM

**Initiative:** `i143`  
**Repository under build:** `drlukeangel/rust-distributed-mesh`  
**Status:** implementation-grade, final architecture reconciliation 2026-10-05  
**Milestone:** 59 - i143 Node RPC + generic Mesh product on RDM  
**Target architecture (rafka-v2):** `docs/architecture/connections.md`, `docs/architecture/build.md`, `docs/architecture/deployment-provider.md`, `docs/architecture/fabric-node-lifecycle-elections.md`, `docs/architecture/gossip.md`  
**Parity ledger:** [`transport-parity.md`](transport-parity.md)  
**Downstream import (rafka-v2):** `docs/plans/i141-node-rpc-substrate-prd.md`  
**Downstream migration (rafka-v2):** `docs/plans/i142-node-rpc-migrations-prd.md`

> This is the single binding i143 PRD. Former amendment files are folded here. The parity ledger remains separate because it records evidence, not alternate architecture.

---

<!-- section:0-purpose -->
## 0. Purpose

RDM is the implementation and proving ground for the generic Mesh product that Rafka imports. i143 must deliver one reusable generic control/runtime substrate, not a parallel Rafka-shaped implementation.

The exported boundary includes:

```text
logical Fabric / Mesh / Node identities
Iroh transport integration
hierarchical membership/topology
current RuntimeFact + required current runtime metadata per birth
current Fabric record + the current Build (read last in, first out)
Direct / Proxy connections
Node RPC contract/runtime/pool
Build / Reconciler
BuildStateAdapter contract
LeadershipResolver
DeploymentProvider / DeploymentPipeline
LifecycleTransitionPipeline
mesh investigation (a node-admin into a live mesh)
scenario/evidence/chaos support
```

No generic package imports a Rafka domain crate.

---

<!-- /section:0-purpose -->
<!-- section:1-locked-architecture-decisions -->
## 1. Locked architecture decisions

1. Build is the only generic topology mutation engine.
2. `rafka-node-admin-core` is the **single generic implementation owner** of Build reconciliation, leadership projection, executor/attempt rules, runtime adoption, generic Fabric/Mesh/Node lifecycle and recovery mechanics.
3. i141 binds Rafka policy and production storage/provider backends into that library. i142 consumes the resulting views/mechanisms and must not reimplement them.
4. There is one topology authority and one accepted topology:

```text
current fabric-primary = who may accept a topology change
the current Build      = the newest Build in builds.storage, read last in, first out, that is in progress or completed
```

5. RDM has no independent desired-topology authority beside the current Build. A Build is its own entity, written once; the Fabric row carries no pointer to the newest Build (at most the Build that created it, set once). Any internal shape cache is disposable derived state and has no revision, lineage, fork-winner or writer semantics.
6. Keep three distinct fact classes:

```text
the current Build (LIFO read) = accepted topology
membership + RuntimeFacts + current runtime metadata = exact current births/runtimes
builds.storage = each Build entity and the execution evidence of its attempts
```

7. Every change to the system's shape is its own Build with its own id: node.create, node.delete, the replace recipe, mesh.delete and fabric.delete. The current fabric-primary accepts it.
8. node.stop, node.start, node.restart, node recovery, authority movement and proven runtime drift leave the topology unchanged and open the next attempt of the current Build. An attempt lost with its executor leaves the Build in progress, and the next attempt resumes from its receipts; only a named terminal failure makes a Build failed. Reading last in, first out, the current Build is the first that is in progress or completed; a Build that failed before ever completing is skipped and only a caller's resubmit opens it again, and a Build that completed once stays current as completed when a later attempt of it fails (R-BI3 refinement, Luke 2026-10-12).
9. A second topology-changing request while the current Build is actively reconciling is refused by name with `409 build-in-progress`, naming that Build. RDM does not implement Build supersession/cancellation. Attempts are scoped per mesh (R-BI7, Luke 2026-10-12): the mesh-primary claims and runs the drift attempts of the current Build inside its own mesh on its own mesh's rows, without the fabric-primary; the fabric-primary claims and dispatches the attempts of a topology Build's operations; one attempt is in flight per mesh, and the refusal names the holder.
10. Build acceptance is crash-safe: the complete Build is durable in `builds.storage`, in its acceptance order, before any reader can read it as current. No second write publishes it.
11. Joining/restarting authority-bearing admins read the current Build before Ready. Older Build history is never replayed to reconstruct topology.
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
24. Mesh creation is a Build. There is no mesh recovery: a mesh is alive while it holds a node, and a mesh with no node-admin gets one through mesh investigation, on the same Build and its own MeshId. Replace is Rafka's recipe of node primitives, submitted as one Build.
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
36. For node-admins, Ready means authority-capable hydration is complete: the current Build, RuntimeFacts + required runtime metadata for held births it may have to manage, and provider-control-domain reachability or a valid domain-local execution path. These facts are readiness prerequisites, not election scoring.
37. Product chaos remains downstream i87; i143 proves generic deterministic + real process/container fault behavior.
38. Transitional Rafka legacy tags exist only as downstream i141/i142 adapters. Generic RDM has no Rafka legacy handlers.

---

<!-- /section:1-locked-architecture-decisions -->
<!-- section:2-substrate-ownership -->
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

<!-- /section:2-substrate-ownership -->
<!-- section:3-gap-build-map -->
## 3. Gap/build map

| gap | requirement | owner |
|---|---|---|
| R1 | Node RPC contract/runtime | e5/e6 |
| R2 | Mesh identity/topology/reachability/freshness | e4 |
| R3 | Direct/Proxy connections parity | e0/e4/e6 |
| R4 | fabric-primary-only complete-Build topology mutation; each Build its own entity, the current one read last in, first out | e1/e3 |
| R5 | builds.storage (existing BuildStateAdapter) + fabric/mesh/nodes/connections.storage | e1/e3 |
| R6 | DeploymentProvider/Pipeline + exact runtime proof | e2 |
| R7 | LifecycleTransitionPipeline | e3/e4/e6 |
| R8 | canonical product IDs + transport identity separation | e4/e10 |
| R9 | hierarchical membership/backbone/forwarding | e4 |
| R10 | held-member partition repair | e4 |
| R11 | canonical deterministic leadership | e4 |
| R12 | definitive Node/Mesh/Fabric status certainty | e4/e6 |
| R13 | create, node recovery, the replace recipe; authority != executor | e4 |
| R14 | semantic target -> route -> Node RPC composition | e6 |
| R15 | iroh-gossip nonblocking send + immutable pin | e4 |
| R16 | one-process Endpoint/address boundary | e4/e6/e10 |
| R17 | stateful proof/evidence/scenario package | e7/e8 |
| R18 | load/soak reconciliation | e9 |
| R19 | machine-readable import/export gate, including no downstream duplicate control algorithms | e10 |
| R20 | current RuntimeFact publication + runtime-metadata convergence + successor adoption + provider control-domain semantics independent of Build history | e4/e2/e10 |
| R21 | current-Build read (LIFO) at bootstrap/catch-up + same-Build attempts for drift, restart and recovery | e1/e4/e10 |

---

<!-- /section:3-gap-build-map -->
<!-- section:4-package-architecture -->
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
Fabric record read/write + the current-Build read (last in, first out)
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

<!-- /section:4-package-architecture -->
<!-- section:5-accepted-topology-build-deployment-exact-runtime-proof-lifecycle -->
## 5. Accepted topology, Build, deployment, exact runtime proof, lifecycle

<!-- section:5-1-accepted-topology -->
### 5.1 Accepted topology

The current Build is the Fabric's accepted topology. Each Build is its own entity in `builds.storage`, written once in acceptance order.
A reader reads the Builds last in, first out, and the current Build is the first that is in progress or completed. There is no second
desired-topology record, revision, lineage, fork winner or pointer.

```text
current fabric-primary accepts a change to the shape
  -> compile the current Build's complete topology + the requested change
  -> persist the complete new Build B through builds.storage, in acceptance order
  -> B is accepted, and is the current Build while it is in progress or once it completes
```

Only the current fabric-primary may accept a topology-changing request. A non-primary refuses by name, naming the current
fabric-primary. Persisted copies are not writers.

Joining/restarting admins hydrate:

```text
Fabric record { id, name, path, state, status }
  -> the current Build (LIFO read)
  -> current membership/runtime facts
  -> authority-bearing Ready
```

Older Build history is not replayed to reconstruct topology.

Every change to the shape is its own Build: node.create, node.delete, the replace recipe (node.create of `<name>.new`, stop, update,
start, delete of `<name>.old`), mesh.delete and fabric.delete. node.stop, node.start, node.restart, node recovery, authority movement and
proven runtime drift change no shape and open the next attempt of the current Build.

While the current Build is in progress, another topology-changing request is refused by name: build.create answers
`BuildInProgress { current_build_id }`. RDM does not implement Build supersession/cancellation.

Storage ownership is generic RDM infrastructure:

```text
fabric.storage
mesh.storage
nodes.storage
connections.storage
builds.storage
```

RDM supplies the interfaces plus memory and durable file implementations for restart/chaos proof. Every interface is awaited
(`async_trait`, as `builds.storage` always was): a production backend reads and writes a durable medium, so nothing in node-admin holds
a lock or a sync callback across a storage call; the entry answer, which carries the Fabric record, is awaited too. `builds.storage` is
the existing BuildStateAdapter contract and its memory/FileJournal implementations adopted under the storage name, plus the acceptance
order the LIFO read uses. Rafka later binds production backends.

`fabric.storage` holds the Fabric record: id, name, path, state, status, and at most the Build that created the fabric (set once). It
holds no pointer to the newest Build.

Every admin persists the current Fabric record it learns. Replication preserves state across authority loss; it does not grant write
authority.

<!-- /section:5-1-accepted-topology -->
<!-- section:5-2-build-id-and-attempt-semantics -->
### 5.2 Build-id and attempt semantics

```text
a change to the shape
  -> new complete Build B, its own id
  -> persist B in acceptance order

Build B in progress + authority/executor movement, or an attempt lost with its executor
  -> B stays in progress
  -> the next attempt resumes from B's receipts; completed steps are replayed, never re-run

Build B completed + later proven runtime drift, restart or node recovery
  -> next attempt of B
  -> no new Build

Build B ends with a named terminal failure (Failed { step, reason })
  -> B is failed; the LIFO read skips it
  -> only a caller's resubmit opens its next attempt
```

A new Build id means the shape changed. Runtime drift, restart and node recovery mint no Build.

Attempts and receipts append to their Build. Evidence for drift carries `build_id=B`, `attempt=N`, `reason=proven-drift`; there is no
`reconcile_build_id`.

`build.delete` refuses by name the current Build (the first in progress or completed in the LIFO read). Older Builds become deletable
history once a newer Build is current.

<!-- /section:5-2-build-id-and-attempt-semantics -->
<!-- section:5-3-runtimefact-provider-current-runtime-metadata -->
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

<!-- /section:5-3-runtimefact-provider-current-runtime-metadata -->
<!-- section:5-4-lifecycle-and-ready-root-guard -->
### 5.4 Lifecycle and Ready root guard

Lifecycle transitions are explicit operations with blocking hooks. `LifecycleTransition.transition_id` identifies one lifecycle transition execution and keys once-only hook receipts. Node/Mesh/Fabric status-control RPCs do not carry that transition id; they are naturally idempotent on Node `(NodeId, IncarnationId, status)`, Mesh `(MeshId, status)`, and Fabric `(FabricId, event)`. Required semantic cuts include pre-eligibility, post-eligibility/pre-commit, after-transition, before-drain, after-drain.

Node-admin Ready is authority-capable readiness. It may not commit until the current Fabric record + the current Build (LIFO read), RuntimeFacts + required current runtime metadata for held births it may need to manage, and provider-control-domain reachability or a valid domain-local execution path are hydrated.

An ordinary joining/restarted admin may not self-Ready because hydration is unavailable. Only:

```text
Day 0
  no prior Fabric control authority exists
  bootstrap current control state and first Ready through Day-0 lifecycle

single-admin recovery root
  fenced rule proves no live Ready admin exists
  recovery-root self-apply grants Ready only
```

may establish Ready without an existing Ready authority. Neither root assigns leadership; the seat rule runs after Ready. A node-admin created into a live mesh by mesh investigation (lifecycles/mesh-investigation.md) does not use the recovery-root shortcut. The recovery root is fabric recovery (lifecycles/fabric-recovery.md), including the case where nothing is alive, which reuses the fabric id and MeshIds.

<!-- /section:5-4-lifecycle-and-ready-root-guard -->
<!-- section:5-5-fabric-shutdown -->
### 5.5 Fabric delete

`fabric.delete` is a workflow the fabric-primary runs as one Build (lifecycles/fabric-delete.md). It removes every mesh, leaf-inward,
the fabric-primary's own mesh last:

```text
fabric.delete
  > mesh.delete          each mesh, the fabric-primary's own mesh last; on its own mesh the seat stays put (no other
                         mesh is left to take it), so that mesh.delete skips its node.fabric-primary.update step
                         a mesh-primary that does not answer: reached through mesh investigation, whose outcome
                         during fabric.delete is the delete (a node-admin that answers runs mesh.delete; else the
                         fabric-primary tickles each remaining node and runs node.delete for each one it reaches)
  > node.delete          each other mesh's mesh-primary, after its mesh.deleted; the fabric-primary executes it and
                         proves its exit
    < fabric.deleted     reported by the fabric-primary, still alive
then a separate call from outside the fabric:
node.delete              the fabric-primary; node.deleted is its last reply and the caller takes its word
```

Every runtime the workflow deletes has exact provider exit proof, except the fabric-primary's own, which nobody is left to prove. A
seat moves only by its holder's yield, a committed transfer or a proven loss, so fabric.delete moves no seat. A local OS signal or a
node stop is node lifecycle and starts no fabric.delete.

---

<!-- /section:5-5-fabric-shutdown -->
<!-- /section:5-accepted-topology-build-deployment-exact-runtime-proof-lifecycle -->
<!-- section:6-hierarchical-topology-repair-ids-elections-recovery -->
## 6. Hierarchical topology, repair, IDs, elections, recovery

<!-- section:6-1-topology -->
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

<!-- /section:6-1-topology -->
<!-- section:6-2-held-member-partition-repair -->
### 6.2 Held-member partition repair

```text
held current member
+ known EndpointId/address
+ stale/missing coverage for repair window
    -> bounded join/refeed attempt
```

Accepted topology alone is not dial material. Failed dial is not death proof. Retries are bounded/backed off per peer/window. Exact-runtime/lifecycle retirement removes the peer from repair targets. Zero-neighbour repair may remain fallback. Relearned membership restores RuntimeFact/current runtime metadata.

<!-- /section:6-2-held-member-partition-repair -->
<!-- section:6-3-product-ids -->
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

<!-- /section:6-3-product-ids -->
<!-- section:6-4-election-canon -->
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
the current Build
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

A living holder keeps its seat; a lower-NodeId candidate that later becomes Ready never displaces it. Silence, an observer-inferred Dead or an unheard route is not proof. A holder that yields, or whose exact birth is proven gone, hands the seat to the lowest ReadyForTraffic NodeId of its own mesh. Restart preserves NodeId; replacement mints a new NodeId (fabric-node-lifecycle-elections.md §5).

`is_primary`/`is_fabric_primary` are derived projections. Gossiping a claimed winner does not make it authoritative.

<!-- /section:6-4-election-canon -->
<!-- section:6-5-election-evidence -->
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

<!-- /section:6-5-election-evidence -->
<!-- section:6-6-new-mesh-recovery-ordering -->
### 6.6 A mesh that loses its node-admins

There is no mesh recovery. A mesh is alive while it holds a node; a crash is not leaving. Only its mesh-primary leaves a mesh, always
last, at the end of mesh.delete; the mesh is then dead and never restarted (R-ML2). A dead mesh's name used again is mesh.create of a
new mesh with a new MeshId.

A peer mesh the fabric-primary stops hearing is mesh investigation (lifecycles/mesh-investigation.md), owned by the fabric-primary and
timed on elapsed time since the last direct backbone receipt:
- 20 s: probe 1, the exact mesh-primary first, then the other node-admins, each direct then via a peer;
- 30 s: the silent mark;
- 40 s: probe 2;
- an admin answer or a backbone receipt stops it;
- 60 s, the decision step, in two cases only:
  - members live: a carrier-edge-lost verdict;
  - fully crashed: the provider's exact exit proof of the mesh's node-admins, never silence.
  Either case runs node.create of a node-admin into the mesh, in process, on the same accepted Build and the mesh's own MeshId.
  Otherwise it holds.
- When a mesh is given a new node-admin, both the mesh and the fabric carry degraded until the new mesh-primary's report clears them
  (R-DG1).
- The created node-admin joins as the mesh's successor primary, adopts the mesh's held state, and re-gossips once established.
  Nobody publishes a mesh on its behalf (R-RX4). It brings back unreached members through node recovery.

**Authority != executor:** the fabric-primary owns the investigation and the accepted topology. The created node-admin is the local
executor of its own mesh's work.

Provider locality is another execution boundary, not an authority boundary. If a RuntimeFact belongs to another provider control
domain, the current authority/local Build executor must use the provider's remote-control path or delegate that exact runtime action to
a valid domain-local executor. It must not infer Dead because a PID/container ID is nonlocal.

Failed RPC/dial/silence/provider-domain mismatch never authorizes a delete. A delete requires exact provider exit proof.

---

<!-- /section:6-6-new-mesh-recovery-ordering -->
<!-- /section:6-hierarchical-topology-repair-ids-elections-recovery -->
<!-- section:7-definitive-state-over-short-node-rpc -->
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

<!-- /section:7-definitive-state-over-short-node-rpc -->
<!-- section:8-semantic-target-connections-node-rpc -->
## 8. Semantic target -> connections -> Node RPC

```text
domain selector chooses ExactNode(target)
  -> connections effective_route(target)
      Direct | ViaPeer(exact carrier) | NoActiveRoute
  -> Node RPC executes exactly target
```

Connections never reselects a semantic destination. Node RPC never owns Proxy lifecycle. A carrier performs exactly one direct inner invocation to the original target.

---

<!-- /section:8-semantic-target-connections-node-rpc -->
<!-- section:9-node-rpc-runtime-pool-certainty -->
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

<!-- /section:9-node-rpc-runtime-pool-certainty -->
<!-- section:10-proof-shapes-and-mandatory-scenarios -->
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
- an admin born **after an older Build completed** reads the current Build (LIFO) + RuntimeFacts/runtime metadata + provider-domain capability, becomes Ready, and, once it holds a seat, manages older live runtimes without completed Build receipts; a lower NodeId never displaces a living seat holder;
- older Build history is forgotten, the current Build (LIFO read) remains readable, and later exact runtime death opens a new attempt of that same Build;
- PID reuse/container-name reuse cannot impersonate a previous exact runtime;
- process successor in the same host control domain can manage a non-launcher exact process runtime, while a PID from another control domain is refused;
- container successor sharing the same Docker control domain can manage a non-launcher immutable container ID, while a locator from another domain is refused;
- any multi-host proof demonstrates remotely controllable locators or domain-local exact-runtime execution without changing NodeId election ordering;
- ordinary joining admin cannot self-Ready around missing control hydration; only Day-0/fenced single-admin recovery-root semantics are allowed.

Blackbox tests calculate expected election winners independently from public NodeIds/status, then compare public views + OTLP.

---

<!-- /section:10-proof-shapes-and-mandatory-scenarios -->
<!-- section:11-gossip-substrate-gates -->
## 11. Gossip substrate gates

<!-- section:11-1-nonblocking-iroh-gossip-send -->
### 11.1 Nonblocking iroh-gossip send

The actor must never await capacity on one peer's bounded queue. Active/full peer enqueue drops that enqueue and continues; ordinary Plumtree repair owns convergence. Closed queue preserves cleanup.

The reciprocal queue-saturation regression is export evidence. No RDM resend/throttle workaround substitutes for the library fix.

<!-- /section:11-1-nonblocking-iroh-gossip-send -->
<!-- section:11-2-address-repair-boundary -->
### 11.2 Address/repair boundary

Canonical gossip bootstrap and rejoin use explicit EndpointId/address knowledge from product topology/seeds. Hidden N0 lookup is not repair authority.

Application repair uses held-member stale coverage as described in §6.2. Do not add another peer scheduler on top of iroh-gossip.

RuntimeFact is compact current-birth identity state and rides ordinary membership convergence. Required runtime-management metadata may ride beside it in the current birth/Node projection. Do not add a completed Build-receipt archive to the Build topic; the 4032-byte gossip frame budget remains a hard boundary.

---

<!-- /section:11-2-address-repair-boundary -->
<!-- /section:11-gossip-substrate-gates -->
<!-- section:12-chaos-and-observability -->
## 12. Chaos and observability

Layers:

```text
model/action + shrink
explicit failpoints
deterministic network/time where practical
real process/container fault backend
seeded soak
```

Wedge cuts include request pre-send/post-apply, Build/deployment stall, Build acceptance-order persistence and the current-Build read, build-in-progress refusal, RuntimeFact production/publication/adoption/provider-domain stall, current runtime-metadata hydration stall, lifecycle hook stall, Pending-before-Build stall, election authority movement, stale-slot race, partition/heal, exact-runtime exit, fabric.delete (including a mesh whose primary does not answer), and recovery authority/executor handoff.

Every E2E produces blackbox state plus OTLP artifacts. Fault injection itself is never the success criterion.

<!-- section:12-1-seeded-soak -->
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
- no loss of the current Build;
- no unexplained orphan attempt or claim;
- the final accepted topology converges, and every surviving expected path is reachable and current.

A named refusal the architecture calls for is a pass. An unexplained timeout, silent drop, duplicate authority action or non-convergence fails the soak.

<!-- /section:12-1-seeded-soak -->
<!-- section:12-2-container-proof -->
### 12.2 Container proof (export bar)

The named container cells, not the whole scenario gate rerun under `MESH_SPAWN_TYPE=container`:

- exact container identity is the provider control domain plus the immutable container ID;
- a successor in the same provider control domain adopts, inspects and controls the exact container;
- an authority in another provider control domain never treats the locator as local: it refuses by name or delegates to a domain-local executor;
- a real container kill: exact provider inspection proves it terminal, that is the death evidence, and recovery proceeds. This cell runs through the Build and runtime-adoption path, not the provider alone.

---

<!-- /section:12-2-container-proof -->
<!-- /section:12-chaos-and-observability -->
<!-- section:13-story-ordering-and-late-gates -->
## 13. Story ordering and late gates

Remaining work and its order: the open builder blocks below (each names what it waits on) and §13.3's rollout steps.

<!-- section:13-1-e11-the-integration-proof-of-concept-before-i141 -->
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
e11.s4  leadership, lite: every role cohort's seat is the one election (`rafka-node-admin-core`
        `election.rs`, lowest ReadyForTraffic NodeId per (mesh, kind) cohort); a product reads a
        cohort's primary from node-admin's public view (`rafka-node-admin-client` `nodes()`,
        `is_primary`) and never computes it
        cell: kill the broker primary; the next-lowest broker is announced and is the view's primary;
        node-base holds no comparator (a cell reads its own source)
e11.s5  exact target + fence: a gateway calls a broker by ExactNode and by CurrentPath through its
        one client; after the broker restarts (same NodeId, new incarnation), the gateway evicts
        the old incarnation's pooled connection and the next call reaches the new birth; a dial to
        a birth that moves before it is pooled is RejectedStale (425), never dispatched
        cell: `node_rpc__restart_fence` on the role shape (the gateway carries the probe's calls);
        the in-flight arm is `rafka-node-rpc/tests/pool.rs`
e11.s6  certainty: a request cut before FIN is NotSent (499) and never applied; a reply lost after
        commit is Indeterminate; a handler fault is 423; the reply budget alone is not death proof
        cell: the four outcomes on a gateway->broker call over the gateway's own client, the
        broker staged by the proof family's keys (`proof:hang`, `proof:fault`), each asserted on
        its typed reason (`rafka-node-base/tests/role_process.rs`)
e11.s7  routing/proxy, lite: a gateway that cannot reach a broker directly invokes it ViaPeer through
        another gateway; the carrier makes exactly one inner invocation and never changes the final
        target; NoActiveRoute starts no leg
        cell: Direct, ViaPeer and NoActiveRoute, each on the role shape, with the carrier's
        `rafka.node_rpc.request.serve.via-forward` span
e11.s8  product family: a product-owned unary family (`broker_data`, forwardable) composed beside the
        core, ledgered under the product, served only by the broker and carried by every other role;
        a call to a compute for it is 421
        cell: the family round-trips to a broker and is unserved at a compute
e11.s9  observability pass-through: caller_system and W3C traceparent/tracestate/baggage ride the
        request header; a malformed value is dropped (`via-context-dropped`) and the outcome is
        unchanged; a carrier hands the original context through verbatim, so the broker's serve
        span and the gateway's carried invocation both descend from the caller's span in Jaeger
        cell: one call, one trace, three services (`node_rpc__role_context`); the malformed arm on
        the gateway's own client (`rafka-node-base/tests/role_process.rs`)
e11.s10 status/lifecycle on a role: a broker declares ReadyForTraffic to its mesh primary over the
        status op; a wedged broker is marked pending-reconnect at the floor, tickled (one connect,
        direct then via a peer) and marked dead; its runtime still runs, so it is held, never
        replaced; thawed, the same birth is back ready
        cell: `mesh_runtime__role_wedge`
e11.s11 what the import found: a product's durable backend is async, so the four storage
        interfaces (fabric/mesh/nodes/connections) are awaited like builds.storage, and the entry
        answer (which carries the Fabric record) is awaited; RDM's memory and file backends are
        unchanged in behavior
        cells: the existing storage, shutdown and restart cells on the awaited interfaces; the
        gate unchanged
```

Shape for every cell: one mesh, one node-admin, and the roles the cell names; the process provider.
Nothing of Rafka's domain (orgs, topics, writers) enters: the families are proof families. The epic is
done when the gate runs every cell green and the import manifest is unchanged by it.


<!-- /section:13-1-e11-the-integration-proof-of-concept-before-i141 -->
<!-- section:13-2-e12-conformance-to-the-approved-lifecycle-spec -->
### 13.2 e12 — conformance to the approved lifecycle spec (epic #2991)

Each story is its red cell, its fix and its Jaeger flame from a live mesh, in that order. The design
of record is the document each row names; a story with no document is not seated.
Every story's acceptance includes: its red cell first; the named document's sequence holding in a deterministic
cell with its trace; the affected stems green once at gate cadence; mesh-audit and the gate build set green; a
gossip or mesh change carries a Jaeger flame from a live mesh; it lands only through the land script.

```text
e12.s34 #3060  ops-naming step 1: stop parks, start rejoins, restart keeps the incarnation,
               NodeStopped from the mesh primary, delete with exit proof; R-FW1 ordered forward
               design: §13.3 step 1 (R-ON1, R-ON7); lifecycles/node-stop.md,
               node-start.md, node-restart.md, node-delete.md; gossip.md
e12.s35 #3061  Build path: Blocked framed only for a named Ready blocker; one topology Build in
               flight, refused BuildInProgress{current_build_id}, planned from the accepted topology
               design: node-rpc-envelope.md#build-op-0x20-rdm (R-ON8, one Build in flight); lifecycles/node-create.md
e12.s41 #3068  open: the builder block `e12.s41` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s41)
e12.s42 #3069  open: the builder block `e12.s42` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s42)
e12.s30 #3043  R-ON8 landing two: typed seat-move frame ends a Build stream when the
               fabric-primary seat moves mid-Build; acceptance test 5 (old FP dispatch Fenced)
               design: node-rpc-envelope.md#build-op-0x20-rdm (R-ON8); lifecycles/node-primary-update.md
               (op 0x21)
e12.s38 #3065  mesh leave 1/3: the fabric-primary seat is handed over (0x21) before a leave of
               its mesh; NewFabricPrimary on the backbone
               design: lifecycles/node-primary-update.md; gossip.md new-fabric-primary; R-ST2
e12.s40 #3067  mesh leave 3/3: peer meshes on mesh-leaving / mesh-left: routing hold (statuses
               unchanged), replay to joiners, Leaving/Dead external projection
               design: gossip.md "Mesh-admin hears mesh-leaving / mesh-left"; R-ST5; R-MD1
               (mesh-primary commits Dead at mesh.deleted before its handoff hook)
               (#2992 is split into s38-s40)
e12.s44 #3072  stopped-node garbage collector (R-GC1): stopped_node_ttl (RDM_STOPPED_NODE_TTL, blank = forever,
               RDM 30 min); the fabric-primary sweeps every ttl and submits one ordinary node.delete Build per
               node stopped longer than the ttl (age from the current stopped period's own stop receipt; births an
               open Build references skipped); the checks run again at the delete's admission
               design: node-identity-and-state.md §6.9; rulings 2026-10-11 dev page §R-GC1
e12.s45 #3073  one routing rule: application routing selects a birth only while it and its mesh are
               ready-for-traffic; anything else is a routing stop on receipt, never a barrier; a later
               ready-for-traffic restores selection; only application routing reads lifecycle; a leaving
               mesh is reported leaving by its own mesh-primary (R-MS1)
               design: lifecycles/states.md "Routing"; states.md:12 mesh states
e12.s46 #3074  open: the builder block `e12.s46` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s46)
e12.s47 #3075  open: the builder block `e12.s47` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s47)
e12.s50 #3079  generated docs site: mdBook + rafka-docgen (RDM tool, syn) writing wire / frames / statuses /
               spans / config reference pages into rafka-v2; generated SUMMARY from the lifecycle index; a gate
               regenerates and fails on a dirty diff; workflow step lists after the workflows-call-primitives move
               design: docs/plans/proposal/rdm-code-conformance.md §0.1; Luke 2026-10-11 "let's try mdbook"
e12.s51 #3081  delete timeout-strike pool eviction (pool.rs strikes, client.rs timed_out arm, the via-timeout-strikes
               span); the two pool tests flip to the ruled negative control
               design: node-rpc.md §21; node-rpc session rulings 2026-10-03 §4
e12.s52 #3082  R-HB1: the mesh-primary authors and publishes every member status through its own-mesh projection;
               a heartbeat is liveness only and is refused by name after node.stopped (beta set; gossip — flame at
               landing). Code owed: RDM `stopped_refuses` loses its heartbeat rejoin; `WireDigest` field 2
               `status: MemberStatus` becomes the liveness `node_state` (durable format moved, mesh-audit reseal)
               design: docs/architecture/gossip.md §3.1; lifecycles/states.md "Who sets each state";
               docs/plans/proposal/node-status-heartbeat-connections.md R-HB1
e12.s53 #3083  open: the builder block `e12.s53` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s53)
e12.s55 #3085  open: the builder block `e12.s55` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s55)
e12.s56 #3087  open: the builder block `e12.s56` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s56)
e12.s57 #3088  open: the builder block `e12.s57` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s57)
e12.s58 #3089  open: the builder block `e12.s58` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s58)
e12.s59 #3090  open: the builder block `e12.s59` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s59)
e12.s60 #3091  open: the builder block `e12.s60` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s60)
e12.s61 #3092  node.delete keeps storage
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s63 #3094  remove sender-identity gates inside the mesh; direction refusals stay
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s64 #3095  the RDM standalone store is written at state-commit, not from boot
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s65 #3096  one node.start workflow (workflows call primitives)
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s66 #3097  peer-mesh probe order: exact mesh-primary first, then the other admins, direct then via a peer
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s68 #3099  named typed refusals for an epoch overflow and a stale mesh-delete round
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s69 #3100  open: the builder block `e12.s69` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s69)
e12.s72 #3103  open: the builder block `e12.s72` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s72)
e12.s75 #3106  open: the builder block `e12.s75` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s75)
e12.s76 #3107  open: the builder block `e12.s76` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s76)
e12.s79 #3120  open: the builder block `e12.s79` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s79)
e12.s80 #3121  open: the builder block `e12.s80` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s80)
e12.s81 #3122  open: the builder block `e12.s81` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s81)
e12.s82 #3123  open: the builder block `e12.s82` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s82)
e12.s83 (issue to generate)  open: the builder block `e12.s83` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s83)
e12.s49 #3078  mesh.delete ends at mesh.deleted: the fabric-primary's leave completes at the
               validated handoff; the final admin stays alive publishing MeshStatus Dead; its
               end is a separate node.delete (fabric-primary executor, exit proof, row Deleted,
               NodeDeleted departed); the step-5 shutdown of the final primary and the
               FinalExited resume arm are deleted; mesh-left frame unchanged until rollout 3/4
               design: ops-naming-proposal.md MESH/delete; R-NS1 items 1-3; R-MD1; R11
               after: s40 #3067 and s41 #3068 land (they hold the Dead writes)
e12.s23 #3016  open: the builder block `e12.s23` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s23)
```

<!-- /section:13-2-e12-conformance-to-the-approved-lifecycle-spec -->
<!-- section:13-3-ops-naming-rollout-and-acceptance -->
### 13.3 ops-naming rollout and acceptance

The contract is in the architecture docs (start at
[the lifecycle index](../architecture/fabric-mesh-node-lifecycles.md)); this block owns the order
the pieces land in and the tests that prove them. Four moves, each its own landing, none bundled
into another. Step 1 lands before the i143 soak (R-ON7 moved the `NodeStopped` variant from step 2
to step 1); steps 0, 2 and 3 land after the i141 pin, behind the typed objects, so they change no
Rafka caller.

```text
ops-naming rollout (order and cost)
step 0  REST out of RDM: code placement only (open: e12.s60)
        rafka-node-admin-core/src/http.rs, the axum/tower dependencies of that crate and their users
        (admin.rs, tests/control_routes.rs, tests/client_contract.rs, tests/drift_convergence.rs,
        tests/provider_policy.rs, rafka-node-rpc-testkit/src/admin_faults.rs) move to the demo crate
        as thin callers over the typed objects. admin_api_base, the REST base URL NewFabricPrimary
        carries in its contact addresses, becomes the demo's field.
        done when: grep -rn 'axum\|"/api/' crates/rafka-node-admin* returns nothing; that grep
        is then the CI guard (iroh exports a Router, so Router is not grepped). Until it lands, a PR that adds a NEW route to an RDM crate is refused.
step 1  typed objects, the node lifecycle and Build 0x20 (landed: e12.s34)
        additive build family, fabric-primary sole driver; 0x1B unchanged; node workflows are
        submitted through build.create and mapped onto the existing node ops. Lands together: stop stops and start rejoins; restart keeps the
        incarnation and the mesh transport's restart completion moves to the same incarnation;
        delete proves the exit; node.deleted fences the old incarnation in the peer-location book;
        the mesh-primary publishes NodeStopped from the stop call's stopped frame.
        NodeStopped is a postcard enum variant: this landing carries `durable format moved`
        (a stop that stops has no other publisher for its departure, so it cannot wait for step 2).
        `permanent` removal is a code step of this landing: ?permanent=true changes four things today,
        all deleted: release of the data dir and node-identity.json on exit
        (node-admin/src/lib.rs should_release_on_exit), PermanentRemoval broker departure rows
        (node-admin/src/main.rs), the spawn-registry reap (main.rs), the permanent flag on the
        gossiped pending-delete (main.rs). Delete releases identity, never storage.
        SpawnRequest is the create spec that node.create carries through build.create; it is not a
        chaos mechanism.
step 2  Build step keys: node.create:<path.name> replaces create-node:<path.name> (open: e12.s72, with step 4)
        keys are stored in accepted Builds and receipts: `durable format moved`. They are built and
        parsed in rust-distributed-mesh, which is checked first for anything that splits a key on "-".
step 3  split the remaining 0x1B calls into the node, mesh and fabric op families (open: e12.s53)
        a wire contract change hidden behind the typed objects: only the objects change
after 0  config in the digest: its own landing
        admin_api_base leaves the typed MeshDigest field (rafka-mesh-entity/src/digest.rs) and
        NewFabricPrimary's contact addresses and becomes a config key the demo owns; `extra` is
        renamed `config` (a name only; postcard is positional); the cap is enforced. Removing two
        fields moves positional rows: `durable format moved`. The architecture pages that still name
        admin_api_base as a typed field (gossip.md, node-rpc-envelope.md) are tidied after this
        landing.
step 4  gossip frames NodeDeleting, NodeDeleted and NodeRestarting follow the same naming;
        MeshLeaving / MeshLeave / MeshLeft are deleted (there is no mesh-leaving); only with Luke's
        go-ahead (a gossip change needs it) (open: e12.s72). NodeStopped landed with step 1 (R-ON1).
deferred node.storage is not built. If a consumer needs it, a write carries a declaration (kind, ttl,
        backing store), never projection rows, because a projection fills only through the read path.
```

```text
ops-naming acceptance (each test ships its Jaeger trace)
A1  a failure names its step: node.start on a node whose join fails -> stream carries Started, then
    Failed { step: "node.join", reason }; neither node.joined nor node.started is ever emitted; the
    node's status never reaches ready-for-traffic.
A2  a broken stream is not a failure: cut the caller's stream mid-workflow -> NotSent or
    Indeterminate, never an inferred step failure; for a Build-backed call build.get returns the
    receipts of exactly the steps that completed and resubmitting by build_id re-runs none of them.
A3  stop stops, start rejoins: node.stop then node.start -> the same node.id, incarnation and endpoint
    before and after; node.get answers Current { parked } in between; the mesh-primary's start
    reaches the stopped node over node-RPC at the same endpoint (a fresh dial, since
    connections.delete cut the old ones); peers' books show the same birth throughout.

Build carrier acceptance (R-ON8; each case ships its Jaeger flame from a live mesh)
B1  node.restart over 0x20 is one call with Started, all step frames and Complete; no build.get
    polling and no Build-topic wait before the first frame.
B2  fabric-primary death after claim and before dispatch: the successor's build.attempt.run starts
    the claimed work; resubmission by build_id streams through Complete, no completed step runs twice.
B3  executor loss ends the inner stream Indeterminate; a successor attempt is claimed only after
    departure proof; the caller receives the successor's frames.
B4  cutting the outer stream leaves the Build running; resubmitting build_id after completion returns
    AlreadyApplied with the terminal frame.
B5  a mid-Build seat handover ends the outer stream with the typed new-seat frame, not Indeterminate;
    old-seat dispatch is refused after the fence and its span records Fenced.
B6  build.attempt.run sent to a recipient the claim does not name, or with a stale folded claim,
    receives a typed named refusal.

Open
    mesh and fabric stop and start do not exist; add them only if something needs to park a whole mesh.
```

---

<!-- /section:13-3-ops-naming-rollout-and-acceptance -->
<!-- /section:13-story-ordering-and-late-gates -->
<!-- section:14-export-import-gate -->
## 14. Export/import gate

i141 pins RDM once: the exact 40-hex merged `main` commit on which the complete export gate below is green. There is no partial pin and no sequence of pins.

`eligible=true` requires one exact RDM SHA with proof that:

<!-- section:identity -->
### Identity
- NodeId/MeshId/FabricId are canonical Crockford60 typed logical IDs;
- EndpointId is distinct transport identity;
- no canonical 32-char hex logical product IDs remain;
- a live mesh keeps its MeshId, and a mesh created again under a dead mesh's name gets a new MeshId; restart preserves NodeId and incarnation; node recovery and replacement mint a new NodeId; nothing alive comes back under the same fabric id and MeshIds.

<!-- /section:identity -->
<!-- section:accepted-topology-build -->
### Accepted topology / Build
- the current fabric-primary is the only topology writer; non-primary topology mutation is refused by name;
- each Build is its own entity, written once in acceptance order; the current Build is the first in progress or completed, read last in, first out; no independent desired-topology authority and no pointer to the newest Build exists;
- a Build is durable before any reader can read it as current;
- every admin reads the Fabric record + the current Build before authority-bearing Ready;
- older Build history is not replayed to reconstruct topology;
- every change to the shape (node.create, node.delete, the replace recipe, mesh.delete, fabric.delete) is its own Build compiled from the current topology plus its change;
- a second topology-changing request while the current Build is in progress is refused `BuildInProgress { current_build_id }`;
- authority movement, restart, node recovery and proven drift use another attempt of the current Build; an attempt lost with its executor leaves the Build in progress; only a named terminal failure fails it, and a failed Build is skipped;
- nothing that moves a seat or writes caller state runs before the initial fabric is ready-for-traffic (`fabric-not-ready`);
- `build.delete` refuses the current Build by name;
- `fabric.get` answers the current Build, not a second stored desired-topology object;
- all-admin restart restores the current Fabric record and the current Build from RDM storage interfaces.

<!-- /section:accepted-topology-build -->
<!-- section:runtime-death-provider-domain -->
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

<!-- /section:runtime-death-provider-domain -->
<!-- section:leadership -->
### Leadership
- only ReadyForTraffic candidates are eligible;
- node-admin Ready includes the current Build + RuntimeFact/runtime-metadata + provider-domain authority-capable hydration;
- normal joining admins cannot self-Ready around missing hydration; only Day-0/fenced single-admin recovery root may establish Ready without existing Ready authority;
- complete NodeId is the only election key;
- provider control domain/locality/runtime metadata is not an election key;
- node-admin cohort winner == mesh primary;
- only mesh primaries own fabric election;
- a living seat holder keeps its seat (a lower NodeId never displaces it); a seat moves only by yield, committed transfer or proven loss;
- Day 0 + three-mesh + replacement + late-admin takeover proofs are green;
- no stale amendment/parallel election canon exists.

<!-- /section:leadership -->
<!-- section:repair-recovery -->
### Repair/recovery
- held-member repair heals a partition while each side retains neighbours;
- failed rejoin is bounded and never marks Dead;
- there is no mesh recovery: an unheard mesh is mesh investigation; a node-admin is created into a live mesh only on a carrier-edge-lost verdict or, fully crashed, on exact provider exit proof; both set degraded on the mesh and the fabric until the new mesh-primary reports;
- nobody publishes a mesh on its behalf; the new mesh-primary re-gossips once established;
- a node recovery proves the exact exit, then deletes and creates at the same path.name with a new node.id over the inherited data dir;
- exact-runtime work in another provider domain uses valid remote/domain-local execution, never local PID/container guessing;
- fabric authority and the local executor are distinct;
- nothing inside the mesh checks who sent a frame or call; an out-of-order gossip frame is ignored once, in the receive path;
- the current Build remains readable independently of older Build history.

<!-- /section:repair-recovery -->
<!-- section:downstream-binding -->
### Downstream binding
The i141 import manifest proves Rafka binds to the exported generic mechanisms rather than recreating them:

```text
one imported current-Build read (LIFO) at bootstrap/catch-up + same-Build drift mechanism
one imported Build/Reconciler
one imported LeadershipResolver
one imported RuntimeFact + current runtime-metadata adoption/provider-domain contract
one imported recovery/executor rule
one imported exact-runtime provider contract
one imported held-member repair mechanism
```

After i141 cutover, any active Rafka-local generic comparator, incumbent ladder, lowest-name fabric selection, shape/drift/recovery algorithm, launcher-receipt/runtime-metadata authority, host-local provider guess, or conflicting executor rule is a blocking import defect. Compatibility wrappers are allowed only as stateless delegates.

<!-- /section:downstream-binding -->
<!-- section:transport-node-rpc -->
### Transport/Node RPC
- one canonical Iroh Endpoint per process or named/proved exception;
- no second generic internal mTLS stack;
- no hidden N0 DNS/Pkarr product address authority;
- Direct/Proxy connections parity and target-preserving Node RPC composition are green;
- complete-send certainty, 499, pool supersession, and carried execution are green.

The final import also reconciles downstream `node-identity-and-state.md`, feature docs, tests, runbooks, and RDM `docs/i143/design.md` so target vocabulary no longer uses legacy transport `fabric_id`, ready_since/lowest-name/incumbent election scoring, launcher-private completed Build receipts/runtime metadata as runtime authority, cross-host PID/container assumptions, zero-neighbour-only repair, ordinary self-Ready fallback, or silence/RPC failure as recovery death proof.

---

<!-- /section:transport-node-rpc -->
<!-- /section:14-export-import-gate -->
<!-- section:15-verification-artifacts -->
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

<!-- /section:15-verification-artifacts -->
<!-- section:16-guardrails -->
## 16. Guardrails

- no second topology mutation/reconciliation engine;
- no downstream topology/drift engine beside the imported RDM current-Build control state;
- no downstream election comparator beside imported LeadershipResolver;
- no launcher-private handle map, runtime metadata, or completed Build receipts as current-runtime authority;
- no competing launcher-owned RuntimeFact publication stream;
- no completed Build history replay as the accepted-topology database;
- no independent desired-topology revision/lineage/fork authority or pointer beside the current Build;
- no ordinary self-Ready fallback when current control state cannot be hydrated;
- no character-sum/hash election score;
- no ready_since/ordinal/mesh-name/MeshId/FabricId/incumbent election priority;
- no RuntimeFact/provider locator/provider control domain/current runtime metadata/Build id as election priority;
- no logical FabricId used as EndpointId or transport key;
- one id type: BuildId, DeploymentId and IncarnationId are minted Crockford60 like NodeId/MeshId/FabricId, never hex (R-BI5);
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

<!-- /section:16-guardrails -->
<!-- section:17-definition-of-done -->
## 17. Definition of done

- exact eligible RDM revision exported;
- the current-Build read + same-Build drift, Build/Reconciler and LeadershipResolver live once in `rafka-node-admin-core`;
- RuntimeFact publication/adoption + runtime-metadata convergence + provider-control-domain handling is part of current membership/runtime state;
- provider/bootstrap locator production and node-owned RuntimeFact publication are distinct and proven;
- i141 has clear adapter APIs and no need to copy algorithms;
- NodeId/MeshId/FabricId and EndpointId meanings are unambiguous;
- Day-0 runtime adoption/control-root bootstrap and exact death proof are green;
- ordinary joining admin cannot self-Ready around missing control hydration;
- late non-launcher authority can manage current runtimes and required runtime metadata after completed Build history is gone;
- current process/container proof demonstrates same-provider-domain successor control and rejects cross-domain PID/container misuse;
- multi-host eligibility, when claimed, proves remote provider control or domain-local exact-runtime execution without changing election ordering;
- the current Build (LIFO read) remains readable after older Build history is forgotten and drives later recovery;
- hierarchical membership and held-member partition repair are green;
- canonical NodeId leadership is green at node-type, mesh-primary, and fabric-primary levels;
- Pending/status certainty is green;
- node.create, node recovery, the replace recipe and authority/executor separation are green;
- nothing inside the mesh checks who sent a frame or call, and an out-of-order gossip frame is ignored once, in the receive path;
- fabric.delete leaves no node, mesh or fabric row; a mesh with no answering primary is reached by investigation and deleted;
- every open builder block below has landed through the land script, with its flame where it is a gossip or mesh change;
- an in-progress Build survives authority movement and executor loss; drift, restart and node recovery are attempts of the current Build, and every change to the shape is its own Build;
- Direct/Proxy target-preserving Node RPC is green;
- process/container scenarios, deterministic faults, real chaos, and soak are green;
- parity ledger has zero blocking gaps;
- final manifest proves downstream import uses the library mechanisms rather than a parallel Rafka control brain.

<!-- /section:17-definition-of-done -->
<!-- section:the-plan-of-record -->
## The plan of record

This PRD and the architecture docs are the source of truth for i143 (Luke 2026-10-12: "the prd is the source of truth plus the
architecture"). A seat's brief is `python3 scripts/prd.py brief docs/plans/i143-prd.md <story>`, taken at launch from main. A seat
reads any other block with `python3 scripts/prd.py get docs/plans/i143-prd.md <id>`, and never the whole PRD. GitHub issues are
generated once from a block, for tracking only, and are never read back as the plan. The seat rules (the lead's scratchpad
seat-rules.md) apply to every seat. Each story's architecture names its lifecycle page. A doc line, ruling wording or code that
disagrees with that page is a defect to report.

<!-- /section:the-plan-of-record -->
<!-- section:story-bar -->
## Story bar

Every open story below meets this bar:
- its red cell comes first;
- the named document's sequence holds in a deterministic cell with its trace;
- the affected stems are green once at gate cadence;
- mesh-audit and the gate build set are green;
- nothing inside the mesh checks who sent a frame or call (R-RX1), and ordering lives only in the gossip receive path (R-RX3);
- a gossip or mesh change carries its Jaeger flame from a live mesh;
- it lands only through the land script.

<!-- /section:story-bar -->
<!-- epic:e12.s41 -->
## e12.s41 — node row: Deleted kept at node.delete with record and config; config seeded from the previous row; MetaFieldIsRdmOwned / NameHeld refusals; the node record's identity fields: path, name, node_type

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3068 (generated once; not read back) · **Architecture:** node-rpc-envelope.md#meta-and-config; lifecycles/node-delete.md step 3; node-identity-and-state.md §2 (the node entity) and :260 (identity fields); rafka record.rs:21 (Node.node_type); rulings R-MD1, R12/R13

### State

landed 1395a916 (s41), be6b5cf0 (departed-by-id), c7642f18 (config on the record, absorbed s36). The departed fence it built is an R-RX1 sender gate, removed by the s53 RX unit. R-NP1 Phase 0 LANDED e5b127d7 (2026-10-13, gate 140 s, its build 70.8 s wall): node.tags.create / node.tags.delete are their own 0x1B commands (CreateTags 26, DeleteTags 27, TagsCreated 45, TagsDeleted 46; durable format moved; mesh-audit 28/47), served by the mesh-primary through its held record (node_properties.rs), the label grammar is the envelope's (a-z 0-9 - _ . ~, no leading '.'), and node.config.update and the tags commands are refused fabric-not-ready until the initial fabric first reached ready-for-traffic (traces 742cce081fb7fd063c80a0fb68c286c8, 0226c97808812cf57436b34203faa6c2); part 5 built Phase 1 on i143-s41-name-update (RED d045661b `durable format moved`: UpdateName 20 carries the name only, NameUpdated, RejectedAdmittingConnections, RejectedName 47, BusyHoldingConnections 48, NodeRow.renamed_from; GREEN 053dd0f3: node_name_update.rs, the §2.1 validator order, the pending release finished by the next writer); Phase 1 LANDED 4ec1b96c (2026-10-13, gate 140 s, its build 72.6 s wall) with its remainder (the config and tags writers finish a pending release, RED 06cf0e2f GREEN 59fed18d; one grammar function for tag labels and node names) and the step io split (each pipeline step's span carries io_calls, io_queued_ms, io_ms: RED d4f49a9f GREEN 344c1112); its first gate's red, node_objects::a_create_whose_join_fails_names_the_join_step_and_emits_nothing_after_it, was PrepareStorage taking 4670 ms (busy 0) for one load_or_mint_key before the 5 s stream bound: not the park gate (park_gate.rs:126-144 waits only while parked), the root not reproduced in one diagnostic run; the bound stays and the next firing's step span names queue vs io; the rename admission block LANDED 1f8d6c14 (2026-10-13, gate 104 s, its build 36.5 s wall): a node's data-plane client refuses a dial after `All` (`at=dial`, pool.rs:295) and closes a connection that arrives after it (`at=pool`, pool.rs:406), each on `rdm.node_rpc.connection.reject.via-admission-block`, until the name apply resolves or the node starts; the control pool and gossip are untouched (RED 7daae8ba, GREEN 3e448570); and All reads the open connections after its wait, so a connection admitted work opened during the wait is closed (node-connections-delete.md step 4); reported, no work: no RDM process logs accepted connections (the inbound observer, server.rs:295, has a test caller only) and the admin process has no data-plane client; Phase 2 LANDED 97a59269 (recovery is node.delete then node.create; tags seeded with config at the recovery claim, indexed under the new id) and only a recovery's claim takes the Deleted row's tags, a plain create takes its create spec's LANDED 590a025e (2026-10-13; RED 1f5017fb). R-OW1 items 1-2 LANDED 95e10533 / 7c3b753c (2026-10-13, gate 83 s, build 67.3 s wall; RED f2dc3321): the mesh-primary writes its own node row on arrival (node_rows.rs:595, the `n.node_id != *me` filter gone) and the Day-0 admin claims its own path.name as the first write of its birth before Ready (admin.rs `claim_day0_name_in_boot`; cells `store_root_boot::the_day0_admin_claims_its_own_path_name_as_the_first_write_of_its_birth`, `node_rows::the_mesh_primary_writes_its_own_node_row_on_arrival`; trace 9507a0275e4661e978ea396aab8b661f). D2 replace inheritance LANDED e939a8de (RED 302726b1): `node-replace.md` inheritance covers both predecessor branches (`.old` row or `Deleted` row), rollback restores the predecessor's tag-index entries under its original id, and a repeated cutover preserves the successor's already-inherited values (cell `node_rows::repeated_replace_cutover_preserves_already_inherited_values_and_rollback_restores_predecessor_tag_index`). Cohort seats and `Seated` (Q2/Q2a) LANDED 933b0c61 / e7ffb680 / 908d01e8 (2026-10-14, gate 110 s, build 13.5 s wall; RED ad6af7ad): every `node_type` cohort keeps its own seat record, `node.primary.update` moves any cohort's primary (`fabric_handover::a_mesh_primary_moves_its_rpc_cohorts_primary_by_commanding_the_holder_with_its_committed_row` trace f5e608798264b071a22bcb4a3b8451a4, `::the_fabric_primary_commands_an_rpc_cohorts_primary_update_only_through_its_mesh_primary` trace 818f93d5bf219fe6f0248d682ce69b2d, `::an_rpc_cohort_with_no_other_node_of_its_type_refuses_node_primary_update_by_name` trace 263e0b0c741b92157bb04aa1148c7b0f), `Seated` is ordered by revision per seat key in `receive_order.rs` with out-of-order records ignored on `rdm.mesh.membership.reject.via-earlier-version` (`membership::tests::a_late_older_seated_record_is_ignored_by_the_receive_path_and_cannot_restore_an_exited_holder` trace 977340378600fe731b8c9bb8f623b74f span 6e218b197f6b3e28). Free-text `path`/`name` and immutable `node_type` LANDED e634b766 (2026-10-14, gate 149 s, `TIMINGS wall=71.2s units=455 max_concurrency=11 unit_sum=191s`; RED b0f888b0): `PathName` holds a free-text `name`, `NodeRow`/`NodeRecord`/`NodeView` carry `path`, `name`, and `node_type` as separate fields, `node.update` of `node_type` and invalid `node_type` values are refused by name (cells `node_rows::a_node_created_with_a_free_text_name_round_trips_name_path_and_node_type_through_create_get_stop_start_recovery_and_replace` trace 18ff5a0dfeba5c86f1e11a1565512626, `node_rows::node_update_of_node_type_and_invalid_node_type_are_refused_by_name` trace 2e6082eb62990e91f4621a360d23ec60). The proposal's §5 doc edits 1-7, 9 and 10 landed 5ed5413028 (node-name-update.md, node-config-update.md, the envelope's node properties and 0x1B frames)

### Remaining
- [x] node.update goes; every property has its own command (proposal `docs/plans/proposal/node-properties-own-commands.md`, Luke's yes 2026-10-12, §9 = A: any name within the grammar, unique within its path): `node.name.update` with its validator (exact birth, writer, `RejectedAdmittingConnections`, `RejectedName`, the adapter claim), 0x1B `UpdateName`/`NameUpdated`, `RejectedMetaFieldIsRdmOwned` and the client `NodeMeta`/`check_update`/`meta_of` deleted, `rafka_mesh_entity::meta::NodeMeta` → `PathConfig`, replace's two renames on the new object, recovery's step 6 deleted; the §5 doc edits in the same landing; `durable format moved`; sequenced behind s53 (shared files: node_update.rs, rejoin_name, the 0x1B replies); the rafka-v2 pin bump and harness `NameUpdate` agreed with the dev lead before either lands

- [x] a path is free text, and a node is created as <mesh>.<kind>.<name> (a name, not an ordinal); path and name are TWO fields, joined only as the path.name index key, never a field ("path.name is not a field, it's the path and the name field"); the node's type is its own field node_type (RDM `kind` takes Rafka's name), set by the caller at node.create, immutable, refused by node.update, copied by replace and recovery. RDM's PathName { mesh, kind: NodeKind, ordinal: u32, old, new } (rafka-mesh-entity/src/path.rs:71) is replaced. Large: PathName is used across RDM; the store keys that read it are durable (durable format moved) (landed e634b766, RED b0f888b0, gate 149 s)
- [x] every node_type cohort keeps a seat record like the node-admin cohort does, so node.primary.update moves any type's primary (R-MT1: "same for NA as it is for RPC node"). Part 13 on `i143-s41-cohort-seats` @6612fd57 (stacked on 2839ff30) and Part 14 @79e57295 landed 933b0c61 (2026-10-14, gate 110 s): every cohort's seat record held and read by the election (03bbff62, durable format moved; election.rs:138 `resolve_with`, admin.rs:892 `incumbency_of`, topology_read.rs:219), the mesh-primary gossips `Seated` for each cohort seat the election fills (6612fd57, admin.rs:934), and `node.primary.update` moves any cohort's primary per C1 (traces f5e608798264b071a22bcb4a3b8451a4, 818f93d5bf219fe6f0248d682ce69b2d, 263e0b0c741b92157bb04aa1148c7b0f, 2a742304f6581cb9dec07ef8774cb181, 6c9a48837d90fa8c3a27d0f0acb5e827)
- [x] R-OW1 items 1-2 (rulings 2026-10-11-dev :413): landed 95e10533 / 7c3b753c (RED f2dc3321, gate 83 s): the mesh-primary's own birth arrives and its row is written like any member's (node_rows.rs:595, the `n.node_id != *me` filter gone); the Day-0 admin's first write is the insert-and-fail claim of its own path.name before Ready (admin.rs `claim_day0_name_in_boot`, one `own_claim` path shared with the recovery claim); trace 9507a0275e4661e978ea396aab8b661f (claim +1.4 ms, ready +37 ms)
- [x] spec gap for Luke (resolved in node-config-update.md step 3, 2026-10-13): node-config-update.md step 3 completes a stopped node after the durable row write with nothing sent

- [x] D2 (docs updated 2026-10-13 on Luke's decision; code landed e939a8de, RED 302726b1): node-replace.md inheritance covers both predecessor branches (retained `<name>.old` row, or the Deleted row at `<name>`); the destination claim is the name move's first write; rollback restores the predecessor's tag-index entries under its original id, and a repeated cutover keeps the successor's already-inherited values (never overwrites a later update); node-config-update.md step 3 (a stopped node is sent nothing, the command completes after the durable row write)
- [x] Seated (Q2/Q2a) (docs updated 2026-10-13 on Luke's decision; code landed 933b0c61 / e7ffb680, RED ad6af7ad, and 3eaae3e6): gossip.md frame catalog row and order-key list; code: `Seated`'s order key is the holder record's revision per seat key (`(mesh, node_type)` / `fabric`) in receive_order.rs, ignored in the one receive path with the ignore span (`rdm.mesh.membership.reject.via-earlier-version`, trace http://127.0.0.1:16686/trace/977340378600fe731b8c9bb8f623b74f span 6e218b197f6b3e28: `rdm.mesh.membership.reject.via-earlier-version { node = "mesh1.rpc.1", channel = "mesh:mesh1", frame = "seated", source = "mesh-primary mesh1", held = "revision=2", offered = "revision=1" }`); membership.rs's `refused-not-newer` handler check is gone; the seat record's epoch is named revision (3eaae3e6); SeatedTogether is retired with TakeSeats; cells: records for different cohorts never suppress each other; a late older record cannot restore a holder whose process is gone; join and catch-up carry every cohort's holder, not only the admin's. Lesson for the seat (Luke): a gossip frame's reach is asked first and landed second
### Acceptance criteria

<!-- crit:e12.s41.1 -->
- [x] a node created with a free-text name round-trips name, path and node_type through create, get, stop/start, recovery and replace
<!-- /crit:e12.s41.1 -->
<!-- crit:e12.s41.2 -->
- [x] node.update of node_type is refused by name
<!-- /crit:e12.s41.2 -->
<!-- crit:e12.s41.3 -->
- [x] mesh-audit resealed
<!-- /crit:e12.s41.3 -->
<!-- crit:e12.s41.4 -->
- [x] the full fast gate green
<!-- /crit:e12.s41.4 -->
<!-- crit:e12.s41.5 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s41.5 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s41 -->
<!-- epic:e12.s42 -->
## e12.s42 — replace recipe per node-replace.md (R12 + R13) and the paths it exposed: the row-name view, the new seat resuming an unheard Build, the lost-executor create identity, the router on the exact birth, the id index at publication, the heir watch, the Build-log snapshot, the provider find states, the launch record before exec

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3069 (generated once; not read back) · **Architecture:** lifecycles/node-replace.md; node-start.md step 2; node-recovery.md (Detection); node-create.md step 1; node-rpc-envelope.md (TakeSeats, Build frames, fabric status rounds); rulings R12, R13, R-UC1, R-RX1

### State

landed b0b7f31d, eb785cd6 (parts 2-6) and 0cd2c3fa (parts 7-8: a superseded round's owed completion is withdrawn; begin() fails a step by name when a Build cannot be read; a hook Blocked with no reason fails the node by name; the scenario's hydrate knob is staged and renamed, never rewritten in place); landed 4704c6db (A2, R-UC1a and R-RX5): a start joins back over the endpoint of the call that commanded it (`NodeTarget::Endpoint`, no node id), the receiver-side 425 target check retired, the subject-itself refusals removed, a second start redirects the one in flight, the leave hands back over its call (live flame 5e9ca6c06ca10ee3c706753fadd25363); landed f0523ca8 (2026-10-14, gate 148 s, `TIMINGS wall=69.6s units=455 max_concurrency=11 unit_sum=188s`; RED b8a7fdd8): `Pool::purge_stale` adopts the join's endpoint-keyed pooled connection under the resolved birth and evicts it when superseded (`pool.rs:206-256`), `Pool::get_or_dial` reports the `Direct` connection fact once the commander's birth is resolved (`pool.rs:324-346`; cell `one_endpoint::a_join_to_an_endpoint_no_record_names_is_adopted_by_the_birth_and_purged_when_superseded` trace 25d1d6220a9f6166c8e439ae61fc5f14), and `rejoin_commander` 5/5 (traces ec45e5feba7f4a6f9d50f80940c3aa5c, 290521ccf3af059611eb3f2e6f6845d1) plus `mesh_delete_source` 3/3 (trace 282995d4256c9ba8a3e0b558bf787d46) are green at gate cadence

### Remaining
- [x] (done 2026-10-12, with the Fence→Target rename) docs (Luke's yes to the exact text): node-rpc-envelope.md §0's 425 row and its sentence "the socket a request arrives on names nothing: the fence does", the reserved-code 425 entry, node-rpc.md :480/:595 (RejectedStale as a receiver answer)
- [x] the join's pooled connection is adopted under the resolved birth and purged on supersession (`pool.rs:206-256`), and a join to a commander no record names at join time reports its `Direct` connection fact once the birth is resolved (`pool.rs:324-346`; landed f0523ca8, RED b8a7fdd8, cell `one_endpoint::a_join_to_an_endpoint_no_record_names_is_adopted_by_the_birth_and_purged_when_superseded` trace 25d1d6220a9f6166c8e439ae61fc5f14; live traces ec45e5feba7f4a6f9d50f80940c3aa5c, 290521ccf3af059611eb3f2e6f6845d1, 282995d4256c9ba8a3e0b558bf787d46)

### Acceptance criteria

<!-- crit:e12.s42.4 -->
- [x] a stopped node commanded by an admin born after its stop starts: rejoin_commander.rs (red cell on branch i143-s42-ruc1-red @ ac0d8493) is green
<!-- /crit:e12.s42.4 -->
<!-- crit:e12.s42.5 -->
- [x] mesh_delete_source.rs (~105) hands back to the caller's endpoint; node_stop, node_start_sequence and mesh_leave_resume green at gate cadence
<!-- /crit:e12.s42.5 -->
<!-- crit:e12.s42.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s42.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s42 -->
<!-- epic:e12.s46 -->
## e12.s46 — R-QH1: the QUIC join timeout — root cause found (host packet filter), and chaos faults that cannot outlive their owner

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3074 (generated once; not read back) · **Architecture:** rulings 2026-10-11 dev page §R-QH1; the packet scratchpad e12/r-qh1-packet.md (evidence chain)

### State

diagnostics landed 65122557. Root cause: leaked RAFKA-INB-692042-0 (lo udp dport 56958 drop) and RAFKA-PART-* chains left by killed chaos-kit processes (FirewallInbound / Partition cleanup was Drop-only). Kit fix landed 33ee1f85 / a17f7448 (RED 5efc16c4; merged onto main via 7b65e7d3 / 08948db5, 2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`): each fault is an nftables table held by a `nft -i` helper's stdin pipe so the kernel drops it when the owner dies; `/usr/sbin/nft` added to sudoers, leaked `RAFKA-*` chains removed, 127.0.0.1:56958 probe receives, and `chaos_kit__restored_faults` 6/6 (including `firewall_inbound_ends_with_the_process_that_installed_it`) is green on the host

### Remaining

- [x] remove the four leaked chains; add /usr/sbin/nft to sudoers. Then: re-probe 127.0.0.1:56958, run firewall_inbound_ends_with_the_process_that_installed_it on the host, land i143-qh1-kit (landed 33ee1f85 / a17f7448 via 08948db5, gate 152 s)

### Acceptance criteria

<!-- crit:e12.s46.1 -->
- [x] the 56958 probe receives
<!-- /crit:e12.s46.1 -->
<!-- crit:e12.s46.2 -->
- [x] the SIGKILL cell green on the host
<!-- /crit:e12.s46.2 -->
<!-- crit:e12.s46.3 -->
- [x] no RAFKA-* chain survives a killed cell
<!-- /crit:e12.s46.3 -->
<!-- crit:e12.s46.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s46.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s46 -->
<!-- epic:e12.s47 -->
## e12.s47 — a node names itself by the name it holds, everywhere: its connections end (R-CN1) and every span, executor and control-plane identity

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3075 (generated once; not read back) · **Architecture:** node-identity-and-state.md §8; lifecycles/node-update.md (rename); node-start.md step 2; rulings R12/R13, R-CR1

### State

parts 1-3 landed 0d772ec3; R-CN1 (Q6) LANDED 878d4473 / d4ef297c (RED 0a795fbd) and §9 due-time reconnect (Q7) LANDED b00db084 (2026-10-14, gate 102 s, build 67.2 s wall; RED 2d746961): an inbound accept writes no `Direct` entry and wakes the source's own dial whose `Direct Connected` retires the `Proxy` (live cell `i143_acceptance_2938::source_accept_wakes_its_own_dial_whose_direct_retires_proxy_before_cutback` traces `http://127.0.0.1:16686/trace/a49ad091afd37c5ec1507a860294e548` and `http://127.0.0.1:16686/trace/0c65e852045816acf1deb69d2bb6612a`; deterministic cells `connections_writer::an_accept_from_the_proxy_destination_writes_nothing_and_wakes_the_source_dial_whose_connected_retires_the_proxy` trace 470af165290965e49c2e7652bc9f2a73 / e8a218e9d062a2fe863e8df63960f0c3, `::a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row` trace 9b630285ae2cfc8b6681308c4e1b471c / 2d2220e13a684bc1c5dffc8c77f87391, `::repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff` trace 6dc00ceef79f4f7e41a5cd8cda700957 / 6e0bfbea7f1216da42e2b773103d9db0, `::an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled` trace 521a1da17272e96ae13280984c240317, and `::a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts` traces `http://127.0.0.1:16686/trace/9df1542e9a12dfc0cad3f459be21d74d`, `http://127.0.0.1:16686/trace/c2b0687c4d1dd0df7f249b25661c93e2`, `http://127.0.0.1:16686/trace/8db8fe4733dedc40902767162d4311e0`; verbatim span line: `rdm.node.connections.record.write.done.ok.via-recorded trace_id=8db8fe4733dedc40902767162d4311e0 span_id=f68452a9f06c598e rdm.node=mesh1.rpc.1 rdm.conn.id=mesh1.rpc.1->mesh1.rpc.2:direct:4 rdm.conn.peer=mesh1.rpc.2 rdm.conn.kind=direct rdm.conn.state=connected rdm.conn.attempt=4`). The boot-name sweep LANDED 6d264101 (gate 152 s) and bcfa253f (2026-10-14, RED 8aadd33c): control-plane, Build, row, handover, status, and admin spans read `held_name()` instead of the boot `PathName`, and `RunDoor`/`BuildExecutor` admit a renamed admin under its held name (cells `held_name_spans::`, `rejoin_name::a_node_renamed_while_stopped_starts_under_its_rows_name_and_config`)

### Remaining

- [x] the boot-name sweep — about 92 span sites and about 60 control-plane sites use the boot name; read the held name (`node_self.rs:312` `held_name()` on the node, `membership.rs:2996` `held_name(birth)` for another birth). `self.me` / `me: PathName` sites on RDM main 01a65889, by file: admin.rs 63, fabric_handover.rs 33, status_rpc.rs 30, build_drive.rs 29, admin/leave.rs 24, node_rows.rs 19, status_declare.rs 14, build_op.rs 14, round.rs 12, build_run.rs 10, hydrate.rs 9, join.rs 8, http.rs 8, build_claim.rs 6, storage.rs 5 (rafka-node-admin-core/src). Also BuildExecutor.executor, RunDoor (NotExecutor for a renamed admin) and about 30 `me` fields; assert the renamed-while-stopped cell's CONTRACT (no span names the boot name after the join) (landed 6d264101 and bcfa253f, RED 8aadd33c)

- [x] R-CN1 (Q6) (docs updated 2026-10-13 on Luke's decision; code landed 878d4473 / d4ef297c, RED 0a795fbd): connections.md §10 list and §13 G; the four cells, named or added in this unit: CONN-G (`i143_acceptance_2938::source_accept_wakes_its_own_dial_whose_direct_retires_proxy_before_cutback`); an accept followed by a failed source dial with Proxy still usable (`connections_writer::a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row`); repeated accepts coalesced into one pending attempt with backoff respected (`connections_writer::repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff`); an accept from a stale destination birth producing no attempt (`connections_writer::an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled`)
- [x] §9 due-time reconnect (Q7) (docs updated 2026-10-13 on Luke's decision; code landed b00db084, RED 2d746961, gate 102 s): connections.md §9.4 states one reconciler, two arming paths, one pending item; build: the reconciler attempts every due `reconnect_plan` item, one pending attempt per (destination, incarnation) shared with R-CN1's accept trigger (an accept coalesces into a pending item, never a second attempt, never skipping the backoff); a dial change, lands with its flame (`connections_writer::a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts`, traces `http://127.0.0.1:16686/trace/9df1542e9a12dfc0cad3f459be21d74d`, `http://127.0.0.1:16686/trace/c2b0687c4d1dd0df7f249b25661c93e2`, `http://127.0.0.1:16686/trace/8db8fe4733dedc40902767162d4311e0`, span `rdm.node.connections.record.write.done.ok.via-recorded trace_id=8db8fe4733dedc40902767162d4311e0 span_id=f68452a9f06c598e rdm.node=mesh1.rpc.1 rdm.conn.id=mesh1.rpc.1->mesh1.rpc.2:direct:4 rdm.conn.peer=mesh1.rpc.2 rdm.conn.kind=direct rdm.conn.state=connected rdm.conn.attempt=4`: Failed row, due time, attempt, then Direct Connected or the next Failed row); Luke's condition: a cell that needs Proxy to stay put seeds the Failed row with a due time beyond its window, no test-only knob
### Acceptance criteria

<!-- crit:e12.s47.1 -->
- [x] a renamed admin runs a claimed attempt under its new name (red today: NotExecutor)
<!-- /crit:e12.s47.1 -->
<!-- crit:e12.s47.2 -->
- [x] no span after the join names the boot name
<!-- /crit:e12.s47.2 -->
<!-- crit:e12.s47.3 -->
- [x] node_stop, node_replace, sticky_seat green. Mechanical: a Sonnet seat after s42/s56 land
<!-- /crit:e12.s47.3 -->
<!-- crit:e12.s47.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s47.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s47 -->
<!-- epic:e12.s53 -->
## e12.s53 — the 0x1B / status / connections cut, and no receiver authorization inside the mesh

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3083 (generated once; not read back) · **Architecture:** lifecycles/node-connections-delete.md; node-rpc.md §14; gossip.md §3.1 (receive path, order keys); lifecycles/states.md (stopped and deleted); docs/plans/proposal/node-status-heartbeat-connections.md §B; rulings R-CR4, R-CR5, R-ST1, R-DK1, R-RX1, R-RX2, R-RX3, R-RX4, R-TG2 (connections shape); the audit scratchpad e12/rx-authz-audit.md

### State

absorbed s54 (R-ST1 names), s77 (0x1B split), s48 (natural-key admission), s67 (stop/start replies only). RX removal landed 01a65889: the R-RX3 receive-path check (receive_order.rs) with the A1 sender gates removed, the A3 handover sender checks removed, 6 of 8 PeerUnresolved -> SubjectUnknown, the acting-primary publisher and `publishers_own` removed (R-RX4; A'4 has no case left), and a seat taker adopts the statuses its fold acquired into its book before its first projection (fabric-authority.md §7; s60's beee5a42, which the R-RX4 cut exposed: 2803 8/8, trace 5d2760030779eaaf011d78cb34b22662). The coordinated cut landed 0dd4ed15 (2026-10-13, gate 103 s, its build 20.1 s wall incremental; with the harness: the record_store cell exact, workspace crates incremental): node.connections.delete { All | [ids] } closes connections-log entries only (R-CR5), the control client and pool unlogged and a data-plane client inside the node logging its own (R-CR6), stop = All then its own transport cut, DrainNode/NodeDrained refused by name, ConnectionsReceipt (durable format moved), the deleted mesh's NodeStopped aggregate published from the stop's own status event (mesh-delete.md sequence; deleted_mesh_aggregate.rs), a start reissued by a later attempt of its Build settles AlreadyApplied and takes no node.joined, AwaitNodeLeft takes the StopNode receipt's Stopped as the left reply (a successor executor reusing a lost one's stop), the state-commit cell holds an originated data-plane connection (R-CR6). R-NS2 (Luke 2026-10-13, B: a stop does not wait for gossip delivery; final announcements are best-effort; committed facts recover through the read, the replay or the surviving publisher, the executing authority publishing as author) LANDED 34056198: when a primary holder's stop completes, its mesh-primary (the new holder, the seat row's next writer) says "primary updated" once as author before NodeStopped (pipeline.rs stop_completed, membership.rs announce_update_of_stopped_holder); the four cells drop the final announcements on purpose through a test-only hook armed by the fault door and assert convergence through the authority paths (node_stop :536, sticky_seat's hand-over, admin_restart, fabric_primary_handover); their gate skips came out; no wait, bound or flush on the stop path; gossip.md's sentence reads as best-effort (rafka-v2). Q12 = A (Members publisher reads its stamp inside the version lock, one critical section with `topology_version` and `snapshot_id`), R-RX3 (one held version per source across both receivers and the topology read), and the same-source handler ordering checks folded into `receive_order.rs` LANDED 4d6d0497 / 00c9f9e9 (2026-10-13, gate 77 s, build 13.7 s wall; RED c53a73eb; cell `membership::tests::interleaved_backbone_publishes_and_neighbour_up_replay_stamp_and_forward_in_version_order` trace `http://127.0.0.1:16686/trace/5716e3d0c404b73782c246a0b2ed2d4d` spans `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=b91eb571037da31e rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=1 rdm.target_version=2` and `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=da0a78724a2e9f44 rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=2 rdm.target_version=3`, `topology_read::a_topology_read_install_advances_the_held_version_so_an_older_gossip_frame_is_ignored`, and `rejoin_commander` 5/5 green with `NeighborUp` replay queued onto `ForwardPath::replay` under `forwarder.lock()`). R-CN1 (Q6) LANDED 878d4473 / d4ef297c (RED 0a795fbd; live `CONN-G` dial flame a49ad091afd37c5ec1507a860294e548 -> 0c65e852045816acf1deb69d2bb6612a) and Q7 due-time reconnect LANDED b00db084 (2026-10-14, gate 102 s, build 67.2 s wall; RED 2d746961; flame `http://127.0.0.1:16686/trace/8db8fe4733dedc40902767162d4311e0`). A2 (R-UC1a) landed 4704c6db / f0523ca8. R-BC1 (Luke 2026-10-14): 0x1C keeps `NotClaimant` as its one seat refusal; the page's `RejectedNotAuthority` line struck (docs only; the code never had it, landed 00d097cd). R-RX6 (Luke 2026-10-14): the four A' attribution items stay as landed (docs only, landed f1109974, gate 44 s). `MeshLeft`'s refusal deleted with `e12.s72` (`b81f66a0`). Left: none

### Remaining

- [x] R-BC1 (Luke 2026-10-14, option 1): `NotClaimant` stays the one seat refusal on 0x1C; the envelope's `RejectedNotAuthority { reason }` line (added in 8fc5be4c without a ruling) struck at :860 and :872; build_claim.rs unchanged
- [x] A2 waits on R-UC1a (landed 4704c6db / f0523ca8)
- [x] R-RX6 (Luke 2026-10-14, "i agree"; landed f1109974, gate 44 s): the four A' attribution items stay as landed (docs only): round `heard_from` keyed on the sender (`status_rpc.rs:582`), `ReadyForTraffic` round report attribution under `Reporter::Birth(incarnation)` or `Reporter::Endpoint(peer)` (`status_rpc.rs:671`), the probe wake `wake.addressed_by(sender, peer)` (`status_rpc.rs:1028`), and rafka-time lineage in `from_mesh_primary_digest` / `from_fabric_primary_members` admitting a stamp only when the digest or aggregate's own payload birth is the seat book's holder and `ReadyForTraffic` (`rafka-mesh-transport/src/discipline.rs:42-78`)
- [x] the same-source handler ordering checks fold into receive_order.rs (landed 4d6d0497 / 00c9f9e9)
- [x] the coordinated cut (landed 0dd4ed15): graceful All on the connections log, no new outbound connection through the name apply, DrainNode/NodeDrained retired, ConnectionsReceipt, node.connections.delete { All | connection | [connections] }, status names (Leaving -> stopping/stopped, departed -> deleted)
  - parts A and B on i143-s53-cut @ f96ebdb7 (lane e6s7), not landed: the control client and pool write nothing to the log (admin.rs:5107 `None`), the originate door's data-plane client (testkit node.rs:379) writes the Direct entries, `All` closes log entries only and cuts no pool and no gossip channel, stop = `All` then its own transport cut, DrainNode/NodeDrained refused by name, ConnectionsReceipt on ConnectionsDeleted and Stopped (`durable format moved`, 3d318e1e), rejoin_name runs `All` then ApplyNodeName. R-CR6 = A (Luke 2026-10-12): a data-plane connection may ride Node RPC framing when a second client inside the node originates and logs it; connections.md's control-plane sentence names "the node's control-plane Node RPC pool"
  - before it lands: node_lifecycle__node_replace deleting the stopped predecessor answers 422 node-not-live on the branch and passes on main f2d33a9a (1 run, 34 s); the cut span under `All` becomes the connection's own delete span, the pool's via-hard-cut only in the stop's transport cut; via-observed carries the entry id and peer; `park` at node_self.rs:695 and the replace cell's name lose the retired word; ProcessNodeRpc::new loses its unused observer parameter; rshape_burn_in.rs:3415 stops reading removed spans; the gate build rebuilt
  - not s53's: probe_root_span's 1-in-40 dial timeout is R-QH1 (e12.s46): the failed probe's local port was 127.0.0.1:56958, the leaked `lo udp dport 56958 drop` chain; the four leaked RAFKA-* iptables chains (RAFKA-PART-690706-0, RAFKA-INB-692042-0, RAFKA-PART-692070-0, RAFKA-PART-691826-2; installers gone) were removed 2026-10-13 (Luke) and the dropped ports deliver again, so probe_root_span runs in the gate; e12.s46's kit (each fault an nftables table its installer owns) keeps a fault from leaking again
- [x] mesh-delete.md step 7 LANDED b488b828 (the fabric-primary publishes a deleted mesh's aggregate from its last admin's stop: NodeStopped then NodeDeleted; never for a live mesh; the view keeps a deleted mesh deleted; flame e3fb77ec7d572bd2ea2d7257b43760ef vs main 8ffa0cdd41888f6cd5054095b8a2133d); the seat move during the retention LANDED 783143d3 (2026-10-13, gate 114 s, its build 31.6 s wall): the per-process marks are gone, any admin holding the seat reads each deleted mesh's aggregate from its view and book (57dfb1c1), and a mesh the view names no more keeps its departures listed until the deleted retention ends (handover stem's mesh2.admin.1 kept publishing mesh1 after the view dropped it)
- [x] MeshLeft's refusal of the deleted mesh's republishes (reject.via-mesh-left) goes with e12.s72's MeshLeft deletion (landed b81f66a0, gate 71 s)

- [x] Q12 = A (Luke 2026-10-13; gossip.md order-key list updated; landed 4d6d0497, RED c53a73eb, gate 77 s): the Members publisher reads its stamp inside the version lock, one critical section with `topology_version` and `snapshot_id` (membership.rs `publish`: `sent` is read under `next_seq`). Two publishes, the round loop (`publish_every`) and the admin's event-driven calls (admin.rs:3169, :4877, …), interleave and with the stamp under the lock both versions are stamped and installed in order (cell `membership::tests::interleaved_backbone_publishes_and_neighbour_up_replay_stamp_and_forward_in_version_order` trace `http://127.0.0.1:16686/trace/5716e3d0c404b73782c246a0b2ed2d4d` spans `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=b91eb571037da31e rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=1 rdm.target_version=2` and `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=da0a78724a2e9f44 rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=2 rdm.target_version=3`; live `rejoin_commander` 5/5 green)

- [x] R-RX3 gossip.md:201 (docs updated 2026-10-13 on Luke's decision; code landed 4d6d0497 / 00c9f9e9): one held version per source whichever channel or read it arrives by; the topology-read install advances the held version, so a frame older than a read-installed snapshot is ignored with the ignore span (`topology_read::a_topology_read_install_advances_the_held_version_so_an_older_gossip_frame_is_ignored`)
### Acceptance criteria

<!-- crit:e12.s53.1 -->
- [x] no receiver span refuses by sender
<!-- /crit:e12.s53.1 -->
<!-- crit:e12.s53.2 -->
- [x] out-of-order frames ignored only in receive_order.rs
<!-- /crit:e12.s53.2 -->
<!-- crit:e12.s53.3 -->
- [x] the fast gate green
<!-- /crit:e12.s53.3 -->
<!-- crit:e12.s53.4 -->
- [x] a live flame of a seat move with no refused frame
<!-- /crit:e12.s53.4 -->
<!-- crit:e12.s53.5 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s53.5 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s53 -->
<!-- epic:e12.s55 -->
## e12.s55 — parked post-beta: R-HB3 (digest_seq as evidence on four 0x1B replies)

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** none (the #3085 reference never resolved; parked post-beta, this row is the record) · **Architecture:** docs/plans/proposal/node-status-heartbeat-connections.md §B

### State

parked post-beta (Luke 2026-10-11); not seated. (The due-time reconnect reconciler Q7 and R-CN1 Q6 from the same connections proposal landed on main in b00db084 / 878d4473 / d4ef297c, 2026-10-14, gate 102 s, build 67.2 s wall, RED 2d746961 / 0a795fbd; traces 9df1542e9a12dfc0cad3f459be21d74d, a49ad091afd37c5ec1507a860294e548, 0c65e852045816acf1deb69d2bb6612a; tracked under e12.s47 and e12.s53.)

### Remaining

- [ ] none named

### Acceptance criteria

<!-- crit:e12.s55.1 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s55.1 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s55 -->
<!-- epic:e12.s56 -->
## e12.s56 — replace is Rafka's recipe (RDM's replace path deleted); the investigation arms that create a node-admin

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3087 (generated once; not read back) · **Architecture:** lifecycles/node-replace.md; build.md (BuildChange::Recipe); lifecycles/mesh-investigation.md steps 5-7 (members-live and fully-crashed arms); rulings R-BI1, R-DG1, R-ML3

### State

landed 08948db5 / 7cf280d9 / 49bf704b / 9bd1a1d8 / 2f7f85b4 / 7b65e7d3 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`; merging c2352312 parts 1-7 and parts 8-10 2db14fa1 / f843f14a / d67e2fb1 / de4bb246): recipe carrier, RDM's replace path deleted, the verdict's one-step Create recipe, the fully-crashed arm with its proof read in the drive covering every node of the mesh (`investigate::tests::an_unheard_mesh_with_exited_admins_and_a_running_member_holds_then_opens_create_when_all_exit`, `rdm.node_admin.mesh.update.via-crash-proof`), R-DG1 degraded set on both the mesh and the fabric when the fabric-primary gives a mesh a new node-admin on either arm and cleared by the new mesh-primary's report (`investigate::tests::both_investigation_arms_mark_mesh_and_fabric_degraded_until_the_new_primary_reports`), R-BI1 each topology operation (`node.create`, `node.delete`, the replace recipe, `mesh.delete`, `fabric.delete`) mints its own Build, `first_admin` rule, build-drive turn held across `via-drive` span close (7cf280d9), and `node_replace` 15/15, `sticky_seat` 5/5, `fabric_primary`, `mesh_recover`, `2803_detect` green at gate cadence

### Remaining

- [x] the crash proof covers every node of the mesh, not only its node-admins (mesh-investigation.md step 5, "Fully crashed": "Every node of mesh-x, its node-admins included, has crashed ... The fabric-primary acts on the provider's exact exit proof"). On c2352312 `crash_proven` (build_drive.rs ~721) and `crashed_peer_mesh` (admin.rs ~2371) read only `m.cohort(NodeKind::NodeAdmin)`, so a mesh whose node-admins exited while a member still runs gets a node-admin created in process; that member's mesh is the members-live arm (carrier-edge-lost only, else hold). Each node the accepted topology names is proven Exited by its exact birth, or NeverLaunched by the provider find when it has no birth; any other answer holds the mesh, named on `rdm.node_admin.mesh.update.via-crash-proof` (landed f843f14a / 08948db5, RED d67e2fb1, cell `investigate::tests::an_unheard_mesh_with_exited_admins_and_a_running_member_holds_then_opens_create_when_all_exit`)
- [x] R-DG1: degraded on BOTH the mesh and the fabric when the fabric-primary gives a mesh a new node-admin, on either arm, cleared by the new mesh-primary's report (landed 2db14fa1 / 08948db5, RED de4bb246, cell `investigate::tests::both_investigation_arms_mark_mesh_and_fabric_degraded_until_the_new_primary_reports`)
- [x] R-BI1: node.create, node.delete, the replace recipe, mesh.delete and fabric.delete each mint their own Build (build_op.rs:112-129, http.rs open_with; landed c2352312 / 08948db5)

### Acceptance criteria

<!-- crit:e12.s56.1 -->
- [x] node_replace 15, sticky_seat 5, fabric_primary, mesh_recover, 2803_detect green at gate cadence
<!-- /crit:e12.s56.1 -->
<!-- crit:e12.s56.2 -->
- [x] degraded set and cleared on both arms in a deterministic cell
<!-- /crit:e12.s56.2 -->
<!-- crit:e12.s56.4 -->
- [x] a deterministic cell: an unheard peer mesh whose node-admins all exited while one member still runs gets no node-admin created in process and holds, its via-crash-proof span naming the running member; with that member also exited, the create opens
<!-- /crit:e12.s56.4 -->
<!-- crit:e12.s56.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s56.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s56 -->
<!-- epic:e12.s57 -->
## e12.s57 — node recovery = node.delete + node.create + node.update copying caller-owned meta

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3088 (generated once; not read back) · **Architecture:** lifecycles/node-recovery.md; lifecycles/node-update.md; fabric-recovery.md (a relaunched node-admin is a new node.id)

### State

absorbed s33 (node recovery landed 4a48eaae) and s62 (restart one op landed 8ceb6b3d). Reader conformance (`wire.rs` refuses unknown output fields by name; RED 6c58a80a) landed 3be2092c. Relaunched node-admin recovery identity, one start path (`relaunch_exit_proof` / `RECOVERY_PULL_WITHIN` wait deleted), Q4/Q4a "Nothing alive" provider-confirmed exit proof, and the four `store_root_boot` replacement cells (`a_relaunched_admin_mints_a_new_node_id_and_recovers_its_inherited_birth`, `a_relaunched_admin_with_every_node_proven_exited_is_fabric_primary_from_start`, `a_relaunched_admin_on_a_root_holding_ready_for_traffic_knows_at_boot_the_fabric_first_reached_it`, `a_relaunched_admin_on_a_root_in_state_commit_does_not_know_it_reached_ready_for_traffic`) LANDED 95e10533 / 75219e6b (2026-10-13, gate 83 s, build 67.3 s wall; RED d6be024b; trace 893deceb77302425afbee48d97dcb764); `node_replace` and `mesh_recover` green at gate cadence (152cb4fd, f0523ca8 gate 148 s). Left: the two `i143_acceptance_2803` `fabric_recovery` cells (Category B in `FP_KILL_SKIP_ARGS`, pending Luke's call)

### Remaining

- [x] superseded 2026-10-12: `otel_debug` is call context, not a node property (Luke); recovery's step 6 goes with node.update (proposal node-properties-own-commands.md §8); nothing is carried between node.delete and node.create but the config seeded at the claim. Was: (superseded by the R-BI8 row once Luke says yes: step 6 goes) node[2].update copies the exited node's caller-owned meta as a blind put from the predecessor's Deleted row; the step's receipt carries no output
- [x] the admin boot restart mode still reuses the old node id and sets supersedes (admin.rs ~3740-3775); a relaunched node-admin is "a new node over its inherited data dir: new node.id and incarnation, same path.name and endpoint key, new port" (node-admin-lifecycle.md :75, :110), with no supersedes (node-recovery.md "Lineage"). Built on `i143-recovery-identity` @e581cda1 and LANDED 95e10533 / 75219e6b (2026-10-13, gate 83 s, build 67.3 s wall; RED d6be024b): the start on an inherited root mints a new node.id (admin.rs:5111), `rdm.node_admin.node.create.via-inherited-root`, the inherited birth written Deleted after its provider exit proof then the path.name claimed as a recovery seeded from it (node_rows.rs:737 `take_inherited_name`); trace 893deceb77302425afbee48d97dcb764; store_root_boot 6/6, node_restart + node_stop 10/10
- [x] one start for every node (Luke 2026-10-12: "fp mp node they all start the same way, fp mp have a couple of extra steps"): the boot-time exit-proof check of an old birth on the same data dir (`runtime_proof`, `relaunch_exit_proof`) is a second start path and code for "the fabric-primary died"; it goes. The old birth that does not answer is node recovery's (fabric-recovery.md: "node recovery for non-answers; exact exit proof required"; landed 95e10533 / 75219e6b, gate 83 s)
- [ ] i143_acceptance_2803 `fabric_recovery_restarts_one_admin_and_restores_every_missing_admin` (:248) and `fabric_recovery_with_no_running_node_adopts_the_holders_own_clock` (:380) kill the fabric-primary with no hand-off (banned): rewrite each to run node.fabric-primary.update first (lifecycles/fabric-primary-update.md) and take the former fabric-primary's admin down only after the confirmed transfer (Luke 2026-10-12); until then neither cell is run
- [x] superseded 2026-10-12: `otel_debug` is call context, not a node property (Luke); recovery's step 6 goes with node.update (proposal node-properties-own-commands.md §8); nothing is carried between node.delete and node.create but the config seeded at the claim. Was: R-BI8 (awaiting Luke's yes; a1 and the orchestrator recommend B, proposal `build-enitty-updates-plus-otherstuff.md` Part D): the claim at step 4 seeds config AND the caller-owned meta group from the Deleted row in one row replacement; recovery's step 6 goes; `NodeRow` grows the caller-owned meta group (`durable format moved`, unrecognised row refused by name); `meta_of`'s hardcoded `otel_debug: false` (node_update.rs:18) goes; the create carries the inherited level into the launch before FirstStart; replace inherits the same way; cells: interruption before and after the claim, executor replacement, repeated claim, each asserting the row value and otel_debug at FirstStart
- [x] reader conformance, separate from R-BI8: wire.rs ~511 `typed()` drops an output's unknown fields instead of refusing by name; the decoder refuses unknown fields naming the field; a Build-topic decoder change, so it lands with its flame (landed 3be2092c, RED 6c58a80a)

- [x] Q4/Q4a (docs updated 2026-10-13 on Luke's decision; code landed 95e10533 / 75219e6b, gate 83 s): origin-ordering.md and fabric-recovery.md "Nothing alive": fabric recovery is nothing alive, every node the store names confirmed gone by the provider (the old process on the inherited dir included), never by a wait; the launched admin is the only node, the fabric-primary, and its clock is rafka-time from the start; if any node answers, it takes rafka-time from the existing fabric-primary. Code: the own-clock branch keys on that provider confirmation (no new check); the root's RECOVERY_PULL_WITHIN wait goes; the inherited birth is written Deleted only after its provider exit proof (node recovery's node.delete)
### Acceptance criteria

<!-- crit:e12.s57.1 -->
- [x] (superseded 2026-10-12 with R-BI8 / step 6 removal: recovery seeds config at the claim) a recovered node carries its predecessor's caller-owned meta (node.update:<path.name> receipt, blind put) and config seeded from the Deleted row
<!-- /crit:e12.s57.1 -->
<!-- crit:e12.s57.2 -->
- [x] a relaunched node-admin has a new node.id and incarnation, the same path.name and endpoint key, and no supersedes
<!-- /crit:e12.s57.2 -->
<!-- crit:e12.s57.3 -->
- [x] node_replace and mesh_recover green
<!-- /crit:e12.s57.3 -->
<!-- crit:e12.s57.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s57.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s57 -->
<!-- epic:e12.s58 -->
## e12.s58 — mesh and fabric name claims; the Build is its own entity and the current Build is read LIFO

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3089 (generated once; not read back) · **Architecture:** node-identity-and-state.md §2.7-§2.8; build.md; rulings R-MN1, R-BI1, R-BI3 and the "Build is its own entity" direction

### State

The fabric_states__rounds red (no climb of the second Build) LANDED 6007ca6f (2026-10-13, gate 71 s): the fabric-primary put the fabric back to pending one status-loop turn after accepting a resize, so `fabric.get` answered ready-for-traffic under a Build never climbed; the reset now runs at the acceptance before the door answers (control_plane.rs:435 `resize` hook, fabric_rounds.rs:448 `accepted`, states.md "A resize or roll puts the fabric back to pending"); RED ace5e565; GREEN trace f1ab087f3bc4a2634feb8042c1199ac8 (reset .7337 s, door .7355 s); 8/8. landed 671d84fd, 500f610a, 231ec2a2, 9a414010, 95f01ece (MeshId at acceptance, create-mesh receipt, typed build_op cells, Leaving kept in the view, a delete's NodeDeleted step never skipped); landed a8b5f7eb (parts 6-8): R-BI3 (the current Build read last in, first out; no Fabric pointer rows; failed skipped; a once-completed Build stays current), R-BI4 (a lost participant hands the attempt back, the Build stays in progress), fabric.get answers current_build(), the 2780/2782 accept cuts on the one durable write, the telemetry flush counting a span only once it is queued, the leave wait ending on the final primary's exit proof (stems 141 s -> ~25 s), a deleting mesh gets a created node-admin only when no admin survives by exit proof, R-WF1 (WireFabric.build_id removed; 64 -> 34 bytes, traces d8ba2a35a7d3010704eccf449ebb5a9c / 5d8f74ea452eac1b1d1667a775b61285). Landed e8cd3ae1 (2026-10-13, gate 82 s) after s53: R-BI5 Crockford ids (BuildId, DeploymentId, IncarnationId; durable format moved), the probe refuses a view entry it cannot read naming node, field, value and parse error. `mesh.delete`'s lost report (`DeleteMesh`/`MeshDeleted` replay on re-admission) LANDED 9ec1b13b / 26227bcb (2026-10-13, gate 102 s, build 67.6 s wall; RED 07d0968e, GREEN 85fff0c9; cell `mesh_delete::tests::a_lost_mesh_deleted_report_is_replayed_to_the_next_attempt_and_resolves_it_exactly_once`). R-BI6 (`fabric/builds/` on the shared store root, `meshes/<mesh>/builds/` for a mesh lane's receipts, per-admin journal and `store_root.rs` exclusion deleted) LANDED db60161f / 275f9937 / e245ba90 (2026-10-13, gate 77 s, build 21.8 s wall; RED b97092ef; cell `store_root_boot::a_relaunched_admin_reads_its_build_from_the_shared_store_root_with_no_local_builds_dir`), and the single seat-source fix (`holds_fabric` and `holds_mesh` reading only `membership.book.seats`, removing `!launched` and `seats.has_publisher()`, with Nothing-Alive recovery loading once before locking `journal` to `membership.book.seats`) LANDED c01cb3ef (2026-10-14, gate 149 s, `TIMINGS wall=74.6s units=455 max_concurrency=10 unit_sum=208s`). R-BI5a (`BuildId::log()` -> `BuildIdLog` rendering `bid_xxxx-xxxx-xxxx` in `build_id` span fields while `Display`, durable row keys, stored `BuildFact` rows and `0x20` wire strings stay bare 12-character Crockford60; `BuildId::parse("bid_...")` refused by name; `node-identity-and-state.md` §2.0 updated on Luke's yes) LANDED 55131ee4 (2026-10-14, gate 46 s; RED 8f0e64d0; cell `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans`; live Jaeger trace `a592b745aaea8819ba14a5777842e24b`). Left: none

### Remaining

- [x] R-BI6 = x (Luke 2026-10-12; landed db60161f / 275f9937 / e245ba90, gate 77 s, RED b97092ef, and single seat-source fix c01cb3ef, gate 149 s): the Build journal's location is the adapter's and provider's choice; RDM's file adapter keeps it on the fabric's shared store root (`fabric/builds/` under the writer ladder, the fabric-primary its writer, and `meshes/<mesh>/builds/` for a mesh lane's receipts) so a successor reads it there; one fact gets one source (`membership.book.seats`); the per-admin journal and `store_root.rs`'s exclusion of it go; build.md:371 states the choice and promises nothing beyond the adapter's storage (proposal A6)
- [x] (on main at 87df0a35: `mint_crockford60`, rafka-mesh-entity ids.rs:50/:116; `bld-` only in the cells that refuse it, ids.rs:260, client lib.rs:204, tests/ids_crockford.rs:33) R-BI5 (Luke 2026-10-12: "build-id is a crockford id, we only have one id type, never a hex"): `BuildId::mint` (build.rs:26-32, `bld-<hex>`), `DeploymentId::mint` (model.rs:12-18) and the `opaque_id!` minters (rafka-mesh-entity ids.rs:178-204, IncarnationId and a minted EndpointId) mint Crockford60 through `mint_crockford60` and parse with its refusal by name; iroh's own EndpointId untouched; the `rafka-node-admin-client` `BuildId` (lib.rs:39) parses the same; a stored `bld-` id is refused by name, so the landing is `durable format moved`; build.md:196-198 replaced on Luke's yes (2026-10-14) with the proposal's text (one id type, bare 12-character Crockford60; transition ids, provider handles and control-domain values are not ids)
- [x] mesh.delete's lost report (Luke 2026-10-13, replacing "HANDOFF_WAIT goes"; landed 9ec1b13b / 26227bcb, gate 102 s, RED 07d0968e, GREEN 85fff0c9): the bounded wait for mesh.deleted stays (MESH_DELETED_WAIT; on expiry the attempt ends incomplete, HandedOff, receipts standing); re-admitting DeleteMesh replays the recorded MeshDeleted report naming the attempt its manifest was recorded under, with no member workflow run again; LeaveMesh -> DeleteMesh, MeshLeave -> MeshDeleted with the module, types, handlers, spans, stems and doc lines (durable format moved; cell `mesh_delete::tests::a_lost_mesh_deleted_report_is_replayed_to_the_next_attempt_and_resolves_it_exactly_once`). The MeshLeaving/MeshLeave/MeshLeft gossip frames, their hooks and spans are untouched (e12.s72)
- [x] R-BI6 mesh-lane receipts answered and landed in db60161f / 275f9937: RDM's mesh lanes (R-BI7) write their Build journal under `meshes/<mesh>/builds/` on the shared store root under the mesh-primary writer check while the fabric lane writes under `fabric/builds/`
- [x] the build.md / pointer doc sweep (A2): `grep -rn "Fabric.build_id\|FabricPointer" docs/architecture` returns only the no-pointer sentences; build.md:196-198 replaced and the six `bld-` example ids (node-rpc-envelope.md:364, mesh-delete.md:114/:138, node-mesh-leave.md:59, node-mesh-join.md:94/:155) made canonical 12-character Crockford60 on Luke's yes (2026-10-14)
- [x] R-BI5a (Luke 2026-10-14, "i agree"; landed 55131ee4, gate 46 s, RED 8f0e64d0; cell `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans`; live Jaeger trace `a592b745aaea8819ba14a5777842e24b`, span `c696c67b348c3a5e`): `BuildId::log(&self) -> BuildIdLog<'_>` (`rafka-mesh-entity/src/ids.rs:193-212`) renders `bid_xxxx-xxxx-xxxx` across `build_id = %<expr>.log()` span fields where `<expr>` is a `BuildId` (`rafka-node-admin-client`, `rafka-node-admin-core`), while `Display` (`ids.rs:151`), durable row keys (`node.primary.update:{build_id}:{run}:{op_key}`), stored `BuildFact` rows, `0x20` wire strings and page examples stay the bare 12 characters, and `BuildId::parse("bid_...")` refuses by name (`WrongWidth` / `NonCanonical`). Evidence-reader decisions: crate integration tests (`rafka-node-admin-core/tests/{build_facts_read,build_takeover,drift_convergence,node_objects,receipt_refusal,recovery_admission,store_root_boot}.rs`, `rafka-node-rpc-testkit/tests/{common/mod,node_delete_pipeline,recover_pipeline}.rs`) match `build_id.log().to_string()`; scenario JSONL readers (`Estate::spans` in `rafka-test-scenario/src/estate.rs:794-799` and `spans_of` in `rafka-test-scenario/tests/i143_acceptance_2777.rs:53-58`) normalize `bid_xxxx-xxxx-xxxx` on `fields.build_id` to bare 12-char for scenario REST/RPC string comparisons while on-disk `.spans.jsonl` files keep `bid_xxxx-xxxx-xxxx`; `/home/admin/rafka-v2/scripts` has no span `build_id` readers; `/home/admin/rafka-v2` has one `build_id = %` hit at `crates/rafka-entity-topology/src/builds.rs:346` (`build_id: &str` on `rafka.node_admin.build.resolve.via-build-fact-index`, untouched); `docs/architecture/node-identity-and-state.md:107-108` (§2.0) updated on Luke's yes

### Acceptance criteria

- [x] build_op, fabric_primary_handover, sticky_seat, fabric_states__rounds, accepted_topology green
- [x] a successor reads the in-progress Build and resumes it
- [x] a failed newest Build is skipped
- [x] The story bar holds (section story-bar).

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s58 -->
<!-- epic:e12.s59 -->
## e12.s59 — fabric.delete replaces FabricShutdown

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3090 (generated once; not read back) · **Architecture:** lifecycles/fabric-delete.md; lifecycles/mesh-investigation.md "During fabric.delete"; rulings R-FS1

### State

landed 45405284 (2026-10-14, gate 148 s, `TIMINGS wall=70.1s units=455 max_concurrency=11 unit_sum=188s`; RED 568a28eb): `FabricShutdown` deleted (`shutdown.rs`, the `Draining` freeze, `hold_seats`, `shutdown.json`, the `shutdown` field in the Build-topic message and the join answer); `fabric.delete` implemented per `lifecycles/fabric-delete.md` (`BuildChange::DeleteFabric`, `fabric_delete.rs`); every mesh other than the fabric-primary's is deleted by `mesh.delete` or reached through the one mesh investigation (`R-FS1`: answering admin runs `mesh.delete`; members live -> fabric-primary creates one node-admin that runs `mesh.delete`; every node proven exited -> rows written deleted on proof; unproven -> refused naming the unproven primary on `rdm.node_admin.mesh.reject.via-unproven-primary`); the fabric-primary's own mesh deletes its non-fabric-primary nodes first, writes the mesh and fabric rows `Deleted` (`rdm.node_admin.fabric.delete.via-build`), and only then runs the fabric-primary's own `node.delete` as the final step (cells `fabric_delete::fabric_delete_leaves_no_node_mesh_or_fabric_row_and_fabric_primary_awaits_its_own_node_delete` trace ac5d0d0751fac815568a3bd72aa7666d and `fabric_delete::unanswered_mesh_during_fabric_delete_creates_one_admin_when_members_live_or_deletes_rows_on_crash_proof_and_refuses_unproven` trace c97dc9dce2ed878fca1c24632fa26d45; `mesh-audit` resealed)

### Remaining

- [x] delete FabricShutdown (shutdown.rs, the Draining freeze, hold_seats, shutdown.json, the `shutdown` field in the Build-topic message and the join answer); fabric.delete per its page; an unanswered mesh is reached by the one mesh investigation (R-FS1 revised, docs landed): a node-admin that answers runs mesh.delete; members live -> the fabric-primary creates a node-admin that runs it; every node proven exited -> rows written deleted on proof; held -> named in the failure; the fabric-primary never runs node.delete on a mesh's nodes directly. mesh.delete of the fabric-primary's mesh calls `node.fabric-primary.update` with that mesh excluded (landed 45405284, RED 568a28eb, gate 148 s; traces ac5d0d0751fac815568a3bd72aa7666d and c97dc9dce2ed878fca1c24632fa26d45)

### Acceptance criteria

<!-- crit:e12.s59.1 -->
- [x] fabric.delete of a 2-mesh fabric leaves no node, mesh or fabric row
<!-- /crit:e12.s59.1 -->
<!-- crit:e12.s59.2 -->
- [x] the unanswered-mesh arm deletes by node.delete
<!-- /crit:e12.s59.2 -->
<!-- crit:e12.s59.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s59.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s59 -->
<!-- epic:e12.s60 -->
## e12.s60 — REST out of RDM (step 0); entity tags as their own property

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3091 (generated once; not read back) · **Architecture:** docs/plans/proposal/ops-naming-proposal.md "RDM has no REST" and step 0; node-rpc-envelope.md typed-object contract (node.get, node.tags.*); rulings R-TG2, R-TG2a, R-TG3, R-OW1

### State

landed 207172f5, 98b5fcb5 (node.get/mesh.get/fabric.get/connections.get over Status; tags field; admin_api_base no longer gates Ready). The 2803 recovery fix (567f3234, beee5a42: DigestBook::adopt_status at seat take) landed with the RX removal 01a65889. Landed f5ca6ab4 (parts 2b1-2b3): typed Estate, 24 acceptance files on the typed objects (2780/2782 over the shared 0x1F read, tests/build_log.rs), FabricView.build_id answered from the fabric record, six stale cells, build.create's RemoveNode of a deleted mesh's last admin; landed f2d33a9a (part 3): 12 more scenario files off REST and polling (accepted_topology, canonical_ids, node_stop, mesh_create, node_delete, proof_store, await_attempt, node_drain, restart_fence, admin_restart, shape_reconcile, role_wedge), the hydration wedge family's cell restored on a topology-read cut (2782, trace c07791bd34ff3a0a41f094c6bc7e61fc), build.delete of a history Build streams Started then Deleted (was a 424 reset), 2775 asserts a restart keeps its incarnation and awaits the fabric's first ready-for-traffic. Part 4 landed 47a9a6dc (2026-10-13, gate 84 s): accepted_topology awaits the late admin's completion fact, admin_restart anchors on the frame's version, proof_store submits the replace after the recovery attempt completed. Parts 6-9 (layout step 3 part A) landed 4c6be135 (2026-10-13, gate 153 s): canary, runner, soak, mesh_membership__backbone, mesh_leave_resume, admin-core fabric_not_ready/provider_policy, estate start/stop over fabric.get, rshape_burn_in off REST ("/api/ 364 → 238), a restart keeps its birth in rshape (C11, C18 rebuilt on node recovery), a non-fabric-primary admin's Node RPC Build refusal spanned; the estate's handover landed 71af3368; the estate's stop landed c7368e46 (2026-10-13, gate 114 s, its build 52.4 s wall): SIGTERM to every process, runtime and container of the estate, then a named kill past the bound, in place of the fabric-primary's POST /api/shutdown loop and the drop's raw POST; part 11 LANDED 6fbfe353 (2026-10-13, gate 84 s, its build 17.6 s wall): `Estate::enter(&PathName)` sets the entry by name, and poc_chaos_kit, late_authority, successor_adoption, mesh_recover, container_kill, fabric_primary, sticky_seat and fabric_primary_handover run on the typed objects (their `"/api/` sites 54 -> 0); parts 12+13 LANDED 9634d9c2 (2026-10-13, gate 140 s, its build 73.0 s wall): a cell follows the Build it started and no submit is refused and resubmitted (submit_change, Estate::submit; the estate's birth follows its Day-0 Build), node_replace 24 -> 0 `"/api/`, build_op to settles_to, the estate's REST methods and `http` field deleted (the scenario crate's `"/api/` is 0; `axum|"/api/` over crates 208 -> 122), node.get answers a node's config (NodeView.config, durable format moved; the config cell's two assertions back verbatim, RED f2d426f4, GREEN 1fcc2d9f); part 14 LANDED d2bbb305 (2026-10-13, gate 137 s, its build 70.5 s wall): the fault door is the testkit op 0x75 `fault-door` (arm, release, state; CutSpec crosses the wire through a serde mirror, RED 7eeaf1d0 GREEN 9776c45f), served through `Wiring.serve_app`, the 12 `Door::open` sites call the admin by path.name, the testkit's axum router and the scenario crate's reqwest deleted (`axum|"/api/` over crates 122 -> 115); part 15 LANDED 6e53b469 (2026-10-13, gate 146 s, its build 84.5 s wall): a member whose delete finished under the lost primary is published NodeDeleted by the amended primary from its finished delete's receipt (RED d347e271, GREEN 0466b5bc; live trace 9aee82adc25dc8156afe9344eafba656); the REST cut LANDED e2549838 / ac630d82 / bb265526 (2026-10-13, layout step 3 done for the crates: `grep -rn 'axum\|"/api/' crates` returns nothing): http.rs is control_plane.rs, the Build door's control plane only, its 14 routes, router, handlers and HTTP rendering of refusals deleted; the admin binds no control listener and its application contact `admin_api_base` is `rdm://<node id>`; NodeAdminClient and reqwest leave admin-client, reqwest leaves the testkit, axum/tower/http-body-util leave admin-core; control_routes.rs and client_contract.rs deleted with the routes; the probe enters at an admin's birth over Node RPC (ac630d82); `Estate::admin` retyped to the entry admin's `PathName` LANDED 748ef8f0 / 895c956d (2026-10-13, gate 101 s, build 17.3 s wall; `d1c9abe8`, `a6a7f1bc`)

### Remaining

- [x] 2782's pointer cells (`accept:build-durable-before-pointer`, `pointer:fabric-record-write`, FabricPointerCatchUp) cut the Fabric pointer, which R-BI3 deletes: they move to the Build's one durable accept write in the same landing as the R-BI3 code (e12.s58; landed a8b5f7eb)
- [x] the other scenario families onto the typed objects (landed f5ca6ab4, f2d33a9a, 47a9a6dc, 4c6be135, 6fbfe353, 9634d9c2, d2bbb305, e2549838, 748ef8f0)
- [x] part 4 (dba547a4, 14 files off REST) gate red, tree recorded: all three reds are main's cells, none in the branch's files, each green on the 24 gates before. Fixed on the branch in part 5 (landed 47a9a6dc):
  - fabric_build__accepted_topology.rs:110 reads the late admin's Build once, 50 ms after the create stream's terminal (FP 65704, read 65754), and finds attempt 1 running; the completion facts reach mesh1.admin.3 by 65866 (14 facts against 7). Build facts are broadcast once on append and absorbed by every admin, and progress waits on no gossip (fabric_builds.rs:5-15, :516-519), so the read awaits the fact with build_log::build_fact as step 1 does. Telemetry gap: absorbing a Build fact emits no span, so the arrival is not in the trace
  - node_lifecycle__admin_restart.rs:123 anchors "after the publish" on rdm.node_admin.node.update.via-node-restarting, which opens 34 ms after the publish it names: the step's receipt puts the hold into the book (membership via-node-restarting +0), the aggregate bumps to topology_version 8 (+15.6 ms) and forwards, and the frame carries that version (gossip.md:163), published at +34 ms (admin.rs:3468, pipeline.rs:1851-1852). The cell's bumped/installed/forwarded checks pass only on later unrelated spans; they anchor on the frame's topology_version (trace 7e135378efebf79f2685e3995e22e15a)
- [x] delete REST from node-admin-core, Peers::nodes, the demo connections view (landed e2549838 / ac630d82 / bb265526 / 748ef8f0)
- [x] R-TG2/R-TG3: <entity>.tags.get / create { label | [labels] } / delete { All | [labels] }, one keyed row per label (no duplicates; creating an existing label is ignored and named), grammar 1-64 of a-z 0-9 - _ . ~, no leading '.', max 32; UpdateNode drops its tags field; FabricView.build_id becomes the R-BI3 current Build (landed 207172f5, 98b5fcb5, a8b5f7eb)
- [x] rshape_burn_in.rs still asserts a new incarnation after a restart (node-restart.md:13: same node.id and incarnation): `Until::Reborn` (:1542, :1767) never holds after a restart; sites :1127/:1155, :1871/:1885, :1920, :2322, :2523, :2866, :2946, :3922, :4637 (release-only); a supersession cell needs a node recovery, not a restart (landed 4c6be135)

### Acceptance criteria

<!-- crit:e12.s60.1 -->
- [x] `grep -rn 'Router|axum|"/api/' crates/rafka-node-admin*` returns nothing
<!-- /crit:e12.s60.1 -->
<!-- crit:e12.s60.2 -->
- [x] every migrated file green at gate cadence
<!-- /crit:e12.s60.2 -->
<!-- crit:e12.s60.3 -->
- [x] tags cells for create/get/delete/filter and the grammar refusals
<!-- /crit:e12.s60.3 -->
<!-- crit:e12.s60.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s60.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s60 -->
<!-- epic:e12.s69 -->
## e12.s69 — Rafka side: the entry pull moves off the rafka-mesh-ops projection frames; no cert check between mesh nodes (R-SV1)

**Builder:** Rafka engineer (rafka-v2; coordinate the landing with the dev lead) · **Tracking issue:** #3100 (generated once; not read back) · **Architecture:** gossip.md §8; fabric-node-hooks.md; fabric-certs.md#inside-the-mesh; internal-mesh-auth.md; rulings R-SV1

### State

not started; absorbed s70

### Remaining

- [ ] rafka-mesh-ops tags 16/18/19 (ControlFrame::ProjectionPull / ProjectionSnapshotPart / ProjectionRefused; rafka-node-base serve_projections, read_projection_answer) move to node.topology.get; delete dialer_send_svid / acceptor_verify_svid and the admission verdict, connect_verified becomes a plain connect. Rafka repo: coordinate the landing with the dev lead

### Acceptance criteria

<!-- crit:e12.s69.1 -->
- [ ] a Rafka node enters through node.topology.get
<!-- /crit:e12.s69.1 -->
<!-- crit:e12.s69.2 -->
- [ ] no SVID frame on rafka-mesh-v1
<!-- /crit:e12.s69.2 -->
<!-- crit:e12.s69.3 -->
- [ ] the Rafka gate green
<!-- /crit:e12.s69.3 -->
<!-- crit:e12.s69.4 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s69.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s69 -->
<!-- epic:e12.s72 -->
## e12.s72 — the gossip cut: Build step keys renamed to the op names; the gossip frames MeshLeaving / MeshLeave / MeshLeft deleted (a mesh is deleted, never left; node.mesh.leave's 0x1B `MeshLeft` reply is e12.s75's); MeshStatus strings; NodeLeft refused by name

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3103 (generated once; not read back) · **Architecture:** docs/plans/proposal/rdm-code-conformance.md §1 (step-key and frame rows); gossip.md §3.1; lifecycles/mesh-delete.md; rulings R-CR4, R-ML1

### State

LANDED b81f66a0 (2026-10-14, gate 71 s, `TIMINGS wall=29.5s units=455 max_concurrency=11 unit_sum=94s`): Build step keys renamed to the op names (`node.create:`, `node.connections.delete:`, `node.delete:`, `mesh.delete:`, `fabric.delete:`, `node.stop:`, `node.restart:`); gossip frames `Frame::MeshLeaving`, `Frame::MeshLeave`, and `Frame::MeshLeft` deleted along with `publish_mesh_leaving`/`publish_mesh_left` and `reject.via-mesh-left`; a deleted mesh's last admin ends its publishing with its final aggregate (`status = Leaving`, `in_flight: [node.delete:<last_admin>]`) and the fabric-primary's `stamp_deleted_mesh` / acting deleted-mesh aggregate path is removed (`deleted_mesh_aggregate.rs`); `NodeLeft` refused by name; `AdminHandle::leave` self-publication and 250ms linger removed; `Forwarder::source` advances version on empty delta when `version > basis.version + 1` so multi-version jumps after `in_flight` removal forward the post-delete member projection (`node_lifecycle__admin_restart`, `mesh_shapes__live_resize`)

### Remaining

- [x] the fabric-primary deletes a mesh by command; the mesh-primary does the mesh (mesh-delete.md :19, :24, steps 2 and 5-7): the fabric-primary sends `DeleteMesh` and accepts `MeshDeleted`, and publishes nothing for the mesh; `Frame::MeshLeaving`, `Frame::MeshLeave`, and `Frame::MeshLeft` are deleted (landed b81f66a0, gate 71 s)
- [x] a mesh's last admin ends its publishing with its final aggregate (Luke 2026-10-14; gossip.md "Mesh delete", mesh-delete.md step 7, R-NS1): when the fabric-primary's node.delete reaches it, its aggregate carries the mesh deleted and itself deleting, its last gossip; the fabric-primary publishes no NodeStopped, NodeDeleted or aggregate for that admin or its mesh (`stamp_deleted_mesh` removed), and receivers hold the mesh for the deleted retention from that aggregate (landed b81f66a0, gate 71 s; `deleted_mesh_aggregate.rs`)
- [x] all of the above as one landing (step keys ride lifecycle frames, so every receiver moves together; durable format moved; landed b81f66a0, gate 71 s)

- [x] evidence for Luke (2026-10-13, from the s60 estate landing's gate red at 030e7466, cell `mesh_lifecycle__fabric_primary_handover::deleting_the_fabric_primarys_mesh_hands_the_seat_over_first_then_deletes_it` :448; artifacts scratchpad/e13/s60estate-red/): resolved by deleting `Frame::MeshLeft` (`refuse_left_mesh`) and `stamp_deleted_mesh` in b81f66a0

### Acceptance criteria

<!-- crit:e12.s72.1 -->
- [x] every step key reads its op name
<!-- /crit:e12.s72.1 -->
<!-- crit:e12.s72.2 -->
- [x] no Mesh*Leav* frame exists
<!-- /crit:e12.s72.2 -->
<!-- crit:e12.s72.3 -->
- [x] the fast gate green
<!-- /crit:e12.s72.3 -->
<!-- crit:e12.s72.4 -->
- [x] a live flame
<!-- /crit:e12.s72.4 -->
<!-- crit:e12.s72.5 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s72.5 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s72 -->
<!-- epic:e12.s75 -->
## e12.s75 — workflows call primitives: one workflow module per lifecycle page, primitives below; the code-only renames; the comment-citation sweep

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3106 (generated once; not read back) · **Architecture:** docs/plans/proposal/rdm-code-conformance.md §0.1, §1 (code-only rows), §4; fabric-mesh-node-lifecycles.md (the index)

### State

absorbed s71 and s74. The first Node RPC frame is the target: landed 87df0a35 (2026-10-13, gate 86 s, build 11.3 s): `Fence { target_node_id, op }` → `Target { node_id, op }`, `RequestHeader::target`, `MAX_TARGET_BYTES`/`peek_target`, the framing doc's `target_len`/`target`, four test fns, the stem `node_rpc__restart_fence` → `node_rpc__restart_target`; postcard positional, no wire move; `grep -rnwi fence` in rafka-node-rpc-contract + rafka-node-rpc 100 → 0 (seven substring hits left, all `BuildReply::Fenced`, the seat epoch). The land script refuses a diff that adds a retired word in an identifier or file name, or an id-named test file (layout §4 name check). `SeatHolder.epoch` → `revision` (`SeatEpoch` → `SeatRevision`, `RejectedEpochOverflow` → `RejectedRevisionOverflow`) landed e7ffb680 / 933b0c61 (RED ad6af7ad). `UpdateNode`/`NodeUpdated` → `UpdateName`/`NameUpdated` landed e634b766 (RED b0f888b0). The code-only renames (`Forwarded`, `ConnectionsDeleting`, `SeatYielded`, `recovery_round`) LANDED 3eaae3e6 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`). `node.mesh.join` (`JoinMesh` / `MeshJoined` on `0x1B`), `node.mesh.leave` (`LeaveMesh` / `MeshLeft` and `BusyHoldingConnections` on `0x1B`), their `rdm.node.mesh.*` spans, the reserved status/frame renames (`Reserved6`, `Reserved7`, `Reserved2`, `NodeConnectionsDeleting`, `NodeConnectionsDeleted`), and regenerated `docs/reference/rdm/*.md` LANDED e34468a0 (2026-10-14, gate 68 s, `units=455`; RED 9f2f9b55; cell `connections_all::node_mesh_leave_and_node_mesh_join_standalone_and_inside_stop` traces 686542851fe55fd7a6974693a1bbd7ce, d995f8f05f1b7d536ed20fb9787771ec, bc5f29b816021eecb9b07343f7e2a2d5, 88b538196f2a62b70768c2b8b2cf42fb and `node_start_sequence::every_start_path_runs_the_same_start_steps_in_the_same_order` trace 51dd1e00f0b75c2b7f972d7c4c1ef34b). `AdminHandle::leave` gossip self-publication & linger removal LANDED b81f66a0 (2026-10-14, gate 71 s). Left: `claim_revision` (coordinated with rafka-v2), and `src/workflows/<op>` module layout

### Remaining
- [x] the 0x1B node-update frames follow the node-properties proposal: `UpdateNode`/`NodeUpdated` → `UpdateName`/`NameUpdated` (landed e634b766, RED b0f888b0, with the s41 row above, listed here so the rename sweep does not touch them twice)
- [x] the seat record's counter is a **revision**, never an epoch (Luke 2026-10-12: an epoch is timekeeping; this is the count of seat moves): `rafka-mesh-entity/src/seat.rs:42` `SeatHolder.epoch` → `revision`, `SeatEpoch` → `SeatRevision`, `expected_epoch`, `RejectedEpochOverflow` → `RejectedRevisionOverflow`, the `SeatRow` key, spans and comments; the gossip source "epoch" (a new source primary's first full) is the same number and takes the same name; the serde field name changes in the JSON seat rows, so `durable format moved`. Docs done 2026-10-12 (envelope 0x21, elections §3/§4.4, gossip, fabric-primary-update, the node.primary.update proposal); the Kafka-domain producer/claim epochs in jobs, transactions and offsets are not this and keep their word (landed e7ffb680 / 933b0c61, RED ad6af7ad)
- [x] the first Node RPC frame is the **target**, never a fence (Luke 2026-10-12: a fence is an epoch comparison, this frame is the call's address): `framing.rs:26` `Fence { target_node_id, op }` → `Target { node_id, op }`, `RequestHeader::fence` → `::target`, `RpcOutcome::RejectedStale(s).target_node_id()`, the `BadTarget` arm's wording, every `fence` in comments/spans of rafka-node-rpc-contract and rafka-node-rpc; postcard is positional so the wire does not move; the word fence stays only for the seat epoch (`Fenced { node, by }`, the seat row). Docs done 2026-10-12 (envelope §0, "Target and context", node-rpc.md)

- [x] the Forward op's success reply is `Forwarded { inner }`, never Relayed (relay is banned; node-rpc-envelope.md Forward schema): `rafka-node-rpc-contract/src/forward.rs:55,147,197`, `outcome.rs:414`, `rafka-node-rpc/src/forward.rs:147`; variant position unchanged, no format move (landed 3eaae3e6, gate 152 s)
- [x] the runtime refusal while a node deletes its connections is `ConnectionsDeleting { reason }`, never Draining (connections are deleted, never drained; node-rpc.md §35, envelope readiness gate): every `Draining` variant and `draining` refusal string in the contract and runtime (224 sites at 152cb4fd), the lifecycle vocabulary table, the spans that name it; position unchanged, no format move (landed 3eaae3e6, gate 152 s)
- [x] a Build dispatch refused because the fabric-primary yielded its seat answers `SeatYielded { node, by }`, never Fenced (envelope Build op): `Fenced` in the Build reply enum and its 17 sites; position unchanged (landed 3eaae3e6, gate 152 s)
- [ ] a connection's reconnect series is a **recovery round**, never an epoch (connections.md §9.2, the Direct Failed row): the `epoch` field and prose in the connections code (landed in RDM 3eaae3e6; rafka-v2 `rafka-reach-reroute` and `rafka-node-base` coordinated with the rafka-v2 lead); the durable-job claim counter is `claim_revision` (node-rpc.md durable-job mutation; rafka-v2 jobs code, 8 files, coordinated with the rafka-v2 lead)
- [x] landed 3fbba798 (2026-10-13, gate green): the comment-citation sweep after the docs move (RDM df378f7a): every `//!`/`///` that cites a page by its rafka-v2 path (`rafka-v2 docs/architecture/...`, `docs/plans/rulings/...`, `docs/plans/proposal/...`; 20 sites) cites the in-repo path; `rafka-mesh-ops/src/framer.rs:16` cites the deleted `docs/architecture/mesh.md` and cites the envelope page instead
- [x] done 2026-10-13 (RDM rulings landing; rafka-v2 5845639d09): the envelope page's rafka-owned op families (0x10 to 0x19) are rafka-v2 `docs/architecture/node-rpc-rafka-ops.md`, text unchanged; the core, rdm and testkit families stay in `docs/architecture/node-rpc-envelope.md`
- [x] node.mesh.leave is its own primitive (node-mesh-leave.md, R-MJ1). Nodes join and leave a mesh; a mesh is deleted, never left (the fabric-primary's mesh.delete is forwarded to the mesh-primary, which runs it). node.stop and node.delete both end their live part with the leave, after `node.connections.delete { All }`, and a caller can command it on its own: `LeaveMesh { node_id, incarnation, build_id, attempt, operation }` on 0x1B, answered `MeshLeft { node_id, incarnation, left }` (`own`, and `backbone` for a node-admin) or `BusyHoldingConnections { connections }`; an appended variant pair, `durable format moved`; spans `rdm.node.mesh.update.via-leaving`, `via-left` (mesh, mesh_id, which), `rdm.node.mesh.reject.via-busy-holding-connections` (0x1B `LeaveMesh`/`MeshLeft`/`BusyHoldingConnections`, `node.stop` sharing `leave_mesh`, and `rdm.node.mesh.*` spans LANDED e34468a0, RED 9f2f9b55, gate 68 s; traces d995f8f05f1b7d536ed20fb9787771ec and bc5f29b816021eecb9b07343f7e2a2d5; `AdminHandle::leave` self-publication and linger removal LANDED b81f66a0, gate 71 s). This 0x1B `MeshLeft` reply is the node's; it is not the gossip `Frame::MeshLeft` e12.s72 deletes
- [x] node.mesh.join's standalone command (node-mesh-join.md, R-MJ1): `JoinMesh { node_id, incarnation, build_id, attempt, operation }` on 0x1B, answered `MeshJoined { node_id, incarnation, joined }` (`WhichMesh`: `own`, and `backbone` for a node-admin), commanded by the node's mesh-primary after a tickle that reached the exact birth, the fabric-primary for a mesh-primary. The envelope's Status enums append `JoinMesh` then `LeaveMesh`, and `MeshJoined` then `MeshLeft`; `durable format moved` (landed e34468a0, RED 9f2f9b55, gate 68 s; traces 686542851fe55fd7a6974693a1bbd7ce, 88b538196f2a62b70768c2b8b2cf42fb, 51dd1e00f0b75c2b7f972d7c4c1ef34b)
- [x] the reserved positions carry no retired word (envelope Status enums; gossip.md "Reserved lifecycle frame positions"); positional, so no format move: `StatusRequest::DrainNode` → `Reserved6`, `NodeDrained` → `Reserved7`, `StatusReply::NodeDrainingApplied` → `Reserved2` (rafka-node-rpc-contract/src/status.rs), `Frame::NodeDraining` → `NodeConnectionsDeleting` and `Frame::NodeDrained` → `NodeConnectionsDeleted` (rafka-mesh-transport/src/membership.rs:501, :529), with their labels `node-draining` / `node-drained` (:2140-2142) (landed e34468a0, RED 9f2f9b55, gate 68 s)
- [ ] src/workflows/<op> per lifecycle page; primitives (node.connections.delete, the transport cut, topology reads, ...); renames (mesh_leave -> mesh_delete, retire -> delete, park -> stop, crate::http -> crate::control, departed -> deleted; a mesh is never reborn: the span `rdm.node_admin.mesh.create.via-rebirth` (investigate.rs ~583) is the node.create of a node-admin into the live mesh (mesh-investigation.md step 6), and the "reborn mesh" wording goes from investigate.rs, admin.rs, round.rs, build_drive.rs and their cells (2803_detect 80, rshape_burn_in 35, drift_convergence 23, build_drive 19 hits on main 01a65889); a node's re-birth stays); ~120 comment citations to current doc paths. One mechanical landing after the cuts

### Acceptance criteria

<!-- crit:e12.s75.1 -->
- [ ] one module per op page
<!-- /crit:e12.s75.1 -->
<!-- crit:e12.s75.2 -->
- [ ] no comment cites a removed page
<!-- /crit:e12.s75.2 -->
<!-- crit:e12.s75.3 -->
- [ ] the fast gate green
<!-- /crit:e12.s75.3 -->
<!-- crit:e12.s75.4 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s75.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s75 -->
<!-- epic:e12.s76 -->
## e12.s76 — docs only: the architecture docs carry every ruling

**Builder:** docs seat (rafka-v2 docs only) · **Tracking issue:** #3107 (generated once; not read back) · **Architecture:** the rulings pages (i143-rulings-2026-10-11-dev.md, i143-rulings-2026-10-12-rx-authz.md)

### State

landed this session: R-ML3 (fabric-recovery, mesh-investigation), R-PB1/R-PC1 (fabric-not-ready), stopped/deleted, R-TG2/R-TG3 tags, BuildChange::Recipe, HandedOff.to, the R-HB1 gossip rows, the R-RX docs; absorbed s8 and s32

### Remaining

- [x] build.md for R-BI1 (each topology change is its own Build) and R-BI3 (no Fabric pointer, LIFO read); mesh-investigation step 6 and :162 for R-DG1 (degraded on mesh and fabric, both arms); gossip.md ~:118 and ~:154 for R-RX4; the envelope 0x1C RejectedNotAuthority line (Luke's yes); the lifecycle index node.tags row; the consolidation's page renames; regenerate docs/reference/rdm after each RDM landing
- [x] fabric-node-lifecycle-elections.md §5 ("Election movement never changes Fabric.build_id") and every architecture line naming Fabric.build_id as a pointer: the current Build is read LIFO (R-BI3, R-BI4); build.md for R-BI4 (an attempt lost with its executor leaves the Build in progress)
- [x] (applied 2026-10-13 on Luke's direction, merged with that day's decisions Q2-Q10) the approved node.primary.update proposal's doc edits: a docs seat's attempt (97ebf365b1, 9e0e3bf757, 441f463f2a, 3d98dfd16c) was reverted (5f8db8a2d7..550c07cbe4) — it went beyond the listed edits and was relayed unread. Redo only on Luke's direction, each edit read by the lead before it is reported
- [x] (done 2026-10-12, with the Fence→Target rename) R-UC1a's doc lines (envelope §0's 425 row, the reserved-code 425, node-rpc.md :480/:595) and the R-BI3 pointer sweep (build proposal A2) on Luke's yes

### Acceptance criteria

- [ ] no architecture line contradicts a ruling
- [ ] check-doc-symbols clean
- [ ] The story bar holds (section story-bar).

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s76 -->
<!-- epic:e12.s78 -->
## e12.s78 — R-BI7: the mesh-primary self-heals its own mesh; attempts scoped per mesh

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3124 · **Architecture:** proposal `docs/plans/proposal/build-enitty-updates-plus-otherstuff.md` Part C; rulings R-BI7; node-recovery.md; node-rpc-envelope.md 0x1C

### State

landed 6d885a2d (2026-10-13, gate 140 s, its build 75.4 s wall), 47ae04f1 (gate 95 s, build 23.3 s wall), ed74c2ba (gate 136 s, build 64.1 s wall), 7f07059e (gate 147 s, build 73.8 s wall), and 5d53ecef: per-mesh drift lanes (`BuildProjection::in_lane`, `LaneBuilds`, `AttemptRuns` keyed `(build, lane, attempt)`, `RunDoor::attempt_run_in`), the mesh-primary's submit running recovery locally with no call to the fabric-primary (`RecoverySubmitter::submit_in` -> `run_recovery`; live acceptance 1 trace 0737e8226e22178ed1694061993b0d0f vs contrast 46ef274b500baa22b542e5b91c5772f7), a departed claimant taken over on its exact departure proof (trace a80d0b85869ad60ec6563f8c360ef615; case b open unclaimed drift attempt claimable by the successor mesh-primary), a proven exit's row recorded once on the tick that proved it (`record_verdict_once`), a member's run starting at its latest claimed open and the owed submit claiming the open attempt (7f07059e, RED 1510cab3), and item B disproved with a green regression cell (5d53ecef: a delete whose executor exits between its `NodeDeleted` receipt and the publish reuses the receipt on take-over and ends Complete). The 2938 `CONN-G` fork was ruled `R-CN1` and landed in 878d4473 / d4ef297c / b00db084 (tracked under e12.s47 and e12.s53)

### Remaining

- [x] claim key `(build_id, mesh, attempt)`; the mesh-primary claims drift attempts on its own mesh's rows (`meshes/<mesh>/` on the shared store root); the fabric-primary claims topology operations; `ClaimDoor`, `claim_attempt` / `open_attempt`, `store_root.rs`
- [x] the recovery `submit` (admin.rs ~1236) opens the attempt locally when this admin is the member's mesh-primary; no build.create to the fabric-primary
- [x] one attempt in flight per mesh: `current_settled` per mesh; `BuildInProgress { current_build_id, mesh, held_by }`; `NotClaimant { mesh, fabric_primary, mesh_primary }`; durable format moved
- [x] the Part C doc table (envelope 0x1C and refusals, node-recovery.md, build.md, fabric-mesh-node-lifecycles.md:108, fabric-authority.md, R-ON8 cross-reference) in the same landing
- [x] flame: a member exits while the fabric-primary is stopped; the mesh-primary's own claim and completed recovery, no fabric-primary call; the same scenario on the previous sha for contrast

### Acceptance criteria

<!-- crit:e12.s78.1 -->
- [x] a member exit with the fabric-primary stopped is recovered by its mesh-primary (cell + flame)
<!-- /crit:e12.s78.1 -->
<!-- crit:e12.s78.2 -->
- [x] a recovery in mesh 2 runs while a node.create runs in mesh 1
<!-- /crit:e12.s78.2 -->
<!-- crit:e12.s78.3 -->
- [x] a topology operation on mesh X and mesh X's drift attempt never both open; the loser is refused naming the holder
<!-- /crit:e12.s78.3 -->
<!-- crit:e12.s78.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s78.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; the landing carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s78 -->
<!-- epic:e12.s79 -->
## e12.s79 — layout step 2: the built-in set is the node-admin and one node executable; consumer-fixture, test-roles and scratch-json-dumper deleted

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3120 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §2 and §4 step 2

### State

landed 764238d0 (2026-10-13, gate 108 s; units 484 → 472): the built-in set is the node-admin and one node executable, `rafka-node` (rafka-node-base `src/bin/rafka_node.rs`, node_type from the launch); the demo's rshape-* binaries bound explicitly; 2776 and the acceptance gate on the binding; rafka-consumer-fixture, test-roles and scratch-json-dumper deleted. `rpc_node` keeps the testkit's rafka-rpc-node: a proof binary (§2 "Proof applications never link into a production crate"; step 5 makes it rdm-test's). NodeKind's Broker/Gateway/Compute variants went with e12.s41's node_type item (e634b766)

### Remaining

- [x] the process provider's built-in set (admin.rs:3622-3631 at f2d33a9a; estate.rs:309-316; faults.rs:114) is the node-admin and one node executable taking `node_type` from the launch; no product role named in RDM
- [x] the demo's `rshape-*` binaries bound explicitly through `bindings.resolve(kind)` (admin.rs:3617)
- [x] `i143_acceptance_2776.rs` and `scripts/i143-acceptance-gate.sh:94` repoint to the binding
- [x] `rafka-consumer-fixture`, `test-roles/*`, `scratch-json-dumper` deleted in the same landing

### Acceptance criteria

<!-- crit:e12.s79.1 -->
- [x] a mesh born with no explicit binding launches every non-admin node from the one node executable, node_type from the launch (cell + launch span)
<!-- /crit:e12.s79.1 -->
<!-- crit:e12.s79.2 -->
- [x] no product role or deleted package is named in crates, scripts, tools or the workspace manifest, except the demo's explicit bindings
<!-- /crit:e12.s79.2 -->
<!-- crit:e12.s79.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s79.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; it lands only through the land script with its timings row; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s79 -->
<!-- epic:e12.s80 -->
## e12.s80 — build: the OTLP exporter on http-proto; tonic's transport and axum 0.7 leave the graph

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3121 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §5, §6 "What is obvious enough to do now"

### State

landed 8cb99b64 (2026-10-13, gate 157 s; units 495 → 483): the exporter on http-proto with the hyper client (axum 0.7, axum-core 0.4, matchit 0.7, tower 0.4, hyper-timeout and five more leave; 379 → 369 packages, 50 → 37 duplicate roots); the gate on the collector's OTLP/HTTP port 5318; found on the path and fixed: every RDM span since 13068501 reached the collector with no service.name (CountingBatchProcessor::set_resource forwarded nothing)

### Remaining

- [x] `rafka-mesh-telemetry`'s exporter from `grpc-tonic` to `http-proto`: the feature and the exporter builder
- [x] the endpoint the tests and the gate set (`OTEL_EXPORTER_OTLP_ENDPOINT`) on the collector's http port

### Acceptance criteria

<!-- crit:e12.s80.1 -->
- [x] `tonic` stays only as `opentelemetry-proto`'s codegen (features `codegen`, `prost`; no `transport`, hyper, h2 or axum): `http-proto` requires `gen-tonic-messages` (opentelemetry-proto-0.27.0/Cargo.toml:114-117); `cargo tree -i axum@0.7`, `-i axum-core@0.4`, `-i matchit@0.7`, `-i tower@0.4` match no package
<!-- /crit:e12.s80.1 -->
<!-- crit:e12.s80.2 -->
- [x] span files still land and spans reach the collector endpoint the tests set
<!-- /crit:e12.s80.2 -->
<!-- crit:e12.s80.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s80.3 -->

### Done when

- [x] Every acceptance criterion holds; it lands only through the land script with its timings row against the previous one; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s80 -->
<!-- epic:e12.s81 -->
## e12.s81 — build: the dev-dependency loops end; cross-stack cells move to the scenario crate

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3122 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §5, §6 "What is obvious enough to do now"

### State

landed 830846f4 (2026-10-13, gate 77 s): eight cross-stack cells moved to the scenario crate under mechanism names (mesh_membership__digest_sequence, node_rpc__apply_cuts, node_rpc__scheduler_replay, node_rpc__shared_dial), three id-named files gone from the lower crates; rafka-mesh-entity and rafka-node-rpc dev-depend on no workspace member; the gate runs rafka-mesh-entity's tests and the four stems

### Remaining

- [x] the cells in `rafka-mesh-entity/tests` and `rafka-node-rpc/tests` that need transport, rpc, scenario or admin-core move to `rafka-test-scenario/tests`, bodies unchanged
- [x] the dev-dependencies that formed the loops are deleted

### Acceptance criteria

<!-- crit:e12.s81.1 -->
- [x] `rafka-node-rpc`'s and `rafka-mesh-entity`'s dev-dependencies name no crate above them
<!-- /crit:e12.s81.1 -->
<!-- crit:e12.s81.2 -->
- [x] discovery counted per crate before and after; every moved cell named in the commit
<!-- /crit:e12.s81.2 -->
<!-- crit:e12.s81.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s81.3 -->

### Done when

- [x] Every acceptance criterion holds; it lands only through the land script with its timings row against the previous one; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s81 -->
<!-- epic:e12.s82 -->
## e12.s82 — one test binary is one process: env set once in its ctor, no global subscriber

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3123 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §3 ("One binary is one process") and §6 item 7

### State

part B (rafka-node-rpc-testkit) landed 38366fbb (2026-10-13, gate 76 s): 14 env sites to the ctor, the two cells needing different values each in their own process with its own env; part A landed e41a9e2d (2026-10-13, gate 83 s): mesh-transport's 18 env sites to its ctor, mesh-telemetry's runtime_watchdog on a thread-scoped subscriber (the watchdog enters the dispatcher in force at its spawn), s80's two telemetry cells; parts D and E landed b3b7e20a (2026-10-13, gate 89 s): EvidenceCapture (thread-scoped, its own file, OTLP export, flush and shutdown in its Drop; 14 transport own-process forks gone); admin-core's five and the chaos kit's five global-init callers moved; the cadence reader refuses a malformed value by name; the test-only process_holds and its racing cell deleted and part C landed 992e30bc (2026-10-13, gate 80 s): the connections-delete deadline is launch config read once, a malformed value refused by name, reaching every launched node (the provider had stripped it: launched nodes ran 5000 ms whatever the commander waited); admin-core's test env in its ctor; the testkit ctor no longer overrides a parent's env (RED/GREEN); role_build waits for each node's own digest before reading its load; part F landed 2737d1bb (2026-10-13): admin-core `tests/fabric_handover.rs`'s two set_global_default to a thread-scoped capture; its 11 own-process forks gone; part G landed e401ebc0 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`): `chaos_kit__restored_faults` passes `RDM_EVIDENCE_DIR` via `delegated_with_env` instead of calling `set_var` in the test body; part H landed d6b1b746 (2026-10-14, gate 77 s, `units=455`): replaced the five `set_var("RDM_EVIDENCE_DIR")` calls in `drift_convergence.rs` with `EvidenceCapture::start`. R-SF1 (Luke 2026-10-14, option A, landed f1109974, gate 44 s): `cadence::staleness_floor()` (`rafka-mesh-entity/src/cadence.rs:56`) stays process-wide; the four transport cells keep their own subprocess (`channel_stopped.rs:69`, `gossip_stats.rs:77`, `peer_location_book.rs:148`, `heartbeat_spans.rs:63`). Left: none

### Remaining

- [x] part A (landed e41a9e2d)
- [x] parts D and E (landed b3b7e20a); testkit transport_diag's two cells keep the product init on purpose (each proves the init's own environment-driven diagnostic layer, in its own process): the telemetry init's global try_init and propagator in a test process (rafka-mesh-telemetry lib.rs:146-148, :470-476), eighteen transport cells; mesh_membership__digest_sequence.rs:116's set_var
- [x] R-SF1 (Luke 2026-10-14, "i agree", option A, docs only; landed f1109974, gate 44 s): four transport cells need different staleness floors (60000 ms: `channel_stopped.rs:69`, `gossip_stats.rs:77`, `peer_location_book.rs:148`; 2000 ms: `heartbeat_spans.rs:63`), read once per process (`rafka-mesh-entity/src/cadence.rs:56`); each keeps running in its own subprocess with `RDM_STALENESS_MS` set through `own_process::delegated_with_env`; no floor argument on `Membership::join`
- [x] part C (landed 992e30bc): `connections_delete_deadline_from_env` read on every call (node_self.rs:62-63, :427; node_commands.rs:158,164; status_rpc.rs:769) becomes a value NodeSelf holds from the launch; admin-core's 17 test env sites
- [x] part F landed 2737d1bb (2026-10-13): admin-core `tests/fabric_handover.rs`'s two set_global_default to a thread-scoped capture; its 11 own-process forks gone
- [x] part G landed e401ebc0 (2026-10-14, gate 152 s) and part H landed d6b1b746 (2026-10-14, gate 77 s): `chaos_kit__restored_faults` passes `RDM_EVIDENCE_DIR` via `delegated_with_env` and `drift_convergence.rs` uses `EvidenceCapture::start` (`git grep 'set_var(\|remove_var(\|set_global_default' -- crates/*/tests` names 0 hits)

### Acceptance criteria

<!-- crit:e12.s82.1 -->
- [x] `git grep 'set_var(\|remove_var(\|set_global_default' -- crates/*/tests` names only ctors (and the two intentional `transport_diag` global-init proof cells)
<!-- /crit:e12.s82.1 -->
<!-- crit:e12.s82.2 -->
- [x] each binary green as listed and with `--test-threads 1`; `--list` counts unchanged
<!-- /crit:e12.s82.2 -->
<!-- crit:e12.s82.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s82.3 -->

### Done when

- [x] Every acceptance criterion holds; each part lands only through the land script with its timings row; this block's State names the landing shas.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s82 -->
<!-- epic:e12.s83 -->
## e12.s83 — node.create's name claim never returns: the PrepareStorage step idles for the whole deadline with no claim span

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `docs/architecture/lifecycles/node-create.md` step 1 (the name claim is the first durable write), `docs/architecture/build.md` §4 (a step's receipt is its commit), `crates/rafka-node-admin-core/src/deployment/pipeline.rs:1471-1495`, `crates/rafka-node-admin-core/src/node_rows.rs:277` (`NodeRows::claim`)

### State

landed d471da63 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`; RED 1ab151af): `NodeRows::claim` opens `rdm.node_admin.node.create.via-node-row` before its first await (`writer_check` -> `put_seeded`) and records `stage`, `outcome`, and `reason` on the span at every exit, while `PrepareStorage` records `stage` across `claim`, `prepare_storage`, and `save_manifest` so any await held in `PrepareStorage` is named in the trace from the artifact alone (cell `node_objects::a_create_traces_every_prepare_storage_await_and_opens_the_claim_span_before_its_first_await`; `node_objects` 8/8 green at gate cadence with its 5 s bound unchanged)

### Remaining

- [x] read why: the first await inside the step (`rows.claim` → `writer_check` → `put_seeded`, or whatever precedes the `via-node-row` span) with a span or event on every await it holds, run once at gate cadence, the trace naming the await that does not return (landed d471da63, RED 1ab151af)
- [x] fix at the source; delete nothing that was coping; the cell stays as written (5 s) (landed d471da63)
- [x] the span that was missing is emitted before the first await, so a repeat is attributable from the artifact alone (`NodeRows::claim` opens `rdm.node_admin.node.create.via-node-row` before `writer_check`; landed d471da63, cell `node_objects::a_create_traces_every_prepare_storage_await_and_opens_the_claim_span_before_its_first_await`)

### Acceptance criteria

<!-- crit:e12.s83.1 -->
- [x] the trace of a create names every await of PrepareStorage, and the claim span opens before the claim's first await
<!-- /crit:e12.s83.1 -->
<!-- crit:e12.s83.2 -->
- [x] node-admin-core tests/node_objects.rs green at gate cadence, no bound changed
<!-- /crit:e12.s83.2 -->
<!-- crit:e12.s83.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s83.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s83 -->
<!-- epic:e12.s23 -->
## e12.s23 — seats and the Build-door admission: a seat moves only by explicit yield or proven loss (R-EL1 = Y + D, R-MT1 node.primary.update), and nothing that moves a seat or writes caller state runs before the initial fabric is ready-for-traffic

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3016 (generated once; not read back) · **Architecture:** fabric-node-lifecycle-elections.md §5-§6; lifecycles/states.md "The initial fabric before ready-for-traffic" (#fabric-not-ready); lifecycles/fabric-primary-update.md (precondition); node-rpc-envelope.md Build refusals (#fabric-not-ready-refusal); rulings R-EL1, R-EL1a, R-PB1, R-PC1

### State

R-D1 drive landed bd201c0c; R14 shared store landed fbcb087b (absorbed s43); landed a35ad7f6 (parts 1-2, R-PB1/R-PC1): one admission check `http.rs::admit_once_ready`, `Rejected { reason: "fabric-not-ready" }` on the Build carrier, `StatusReply::RejectedFabricNotReady` for node.update (durable format moved), the `FirstReady` latch on the ready-for-traffic fabric-event row (the fabric-primary now puts its own mesh's row), the typed live cell (7 refusal traces, e.g. 38fb1708b064d59ec282dc4f965b6486), the estate's REST polling wait deleted. R-MT1 landed 6aae90ee (2026-10-13, gate 77 s): node.primary.update and node.fabric-primary.update (the holder writes the one committed row, the successor installs from it, the holder announces "primary updated"; the 0x21 Take/Taken frames, the SeatTransfer step and the fabric/seats rows gone), R-PB1 decided on the fabric-primary's ready-for-traffic row put before its latch (C'), the door's acceptance its last write, the holder-lost case (the successor of a proven-exited holder and a restarted holder say "updated"), a proven-exited member is not another node of its mesh, the fabric seat moves alone before a mesh retire of its own mesh. Red 2 and the drive's park-gate turn landed 7b9ff487 (gate 104 s); Red 1 (the fabric-primary's own seat update runs under its claimed attempt and ends `HandedOff` after step 7's "primary updated", with the successor reattaching at step 8) landed 3bb4b346 (gate 147 s), and the executor's own run gated landed b3cfde0d. R-SB1 (Q3: seat-holder stop/restart refusals, `fabric.delete` and final-admin exceptions, and the four `store_root_boot` replacement cells) LANDED 95e10533 / 75219e6b (2026-10-13, gate 83 s, build 67.3 s wall; RED d6be024b; trace 893deceb77302425afbee48d97dcb764). R-PU2 (Q8-Q10: `the new holder's process is gone, provider-confirmed, after the committed row`, run component in `node.primary.update:<build_id>:<run>:<op>`, 0x21 `Failed { step, reason }`, `via-seated-again` writing P's record before announcing after `via-successor-ladder` ends exit-proven; `mesh-audit` resealed) LANDED 19f268be (2026-10-13, gate 107 s, build 77.1 s wall; RED 8e77f712; traces e5dc9045a468c89e4055913d7f46cefb and 0eb2c0ebcd667d476c5b55753c5d87e2). The 13 Category A hand-off scenario cells (`mesh_elections__sticky_seat`, `mesh_elections__fabric_primary`, `mesh_elections__late_authority`, `mesh_runtime__successor_adoption`, `mesh_lifecycle__mesh_recover`, `mesh_membership__backbone`, `fabric_build__build_op` x3, `mesh_runtime__container_kill` x2, `drift_convergence` x2) were rewritten to transfer `fabric-primary` before taking the former holder down and unskipped in the gate LANDED dc980004 (2026-10-14, gate 151 s, `TIMINGS wall=74.7s units=455 max_concurrency=11 unit_sum=215s`; traces 0893da23d65473468a7342f8bc36eb7c / 30318e3cb83fa8a98823f60d15ed80d0, deb531511853a6e96cb77df3f263c358 / ff2785acd660abc9cc406b207d2c09e2, 0c9d702b0726dd0a00c1943b03cffa9c / 569f12075e78b27cbf29d3a8430df88c, bc9c3ff62530a00571d0938c97a7f202 / 558adaaabe9dc35e78a8f985ac31bc84, 29f67173633cc218f1467ebc8e52349e, b17ff10db9820837661e9b7730e3f9dd / d6601deacd9c6f9b3cfef39d543bcafe, 755c304cbbeba96fd6ab12b166a5e7ab / 3450b8a41ed9f126a47ced8d6806d115). Day-0 seat installation at `ReadyForTraffic` (before `until_ready` returns), `StopNode` projecting the stopped state and waiting for the mesh-primary's `NodeStopped` row before returning `Complete`, and restoring `sticky_seat`'s exact `attempt == transfer_attempt` assertion and `stop_and_rename` / `until_ready` cells without extra waits LANDED c01cb3ef (2026-10-14, gate 149 s, `TIMINGS wall=74.6s units=455 max_concurrency=10 unit_sum=208s`). The remaining 6 cells in `FP_KILL_SKIP_ARGS` (`mesh_elections__fabric_primary::losing_the_fabric_primary_mesh_moves_control`, `fabric_build__build_op::a_successor_fabric_primary_*` x3, and `i143_acceptance_2803` `fabric_recovery_*` x2) plus built-in `node.primary.update` / `node.fabric-primary.update` C2 step-1/step-2 and dual-seat resolution in `serve_update_primary` and `NodeRpc` / `Nodes` cleared `FP_KILL_SKIP_ARGS=""`. R-G3 (Luke 2026-10-14, arm A; rulings page i143-rulings-2026-10-14-g3-claim-ids.md): peers resolve a holder's contacts from the digest; `Frame::NewFabricPrimary` is a reserved position (14), its dead sender, receiver and `SeatBook` contacts side-table deleted, gossip.md's row, example and section gone; no wire change. The three lines that named the announcement (build.md:554, node-rpc-envelope.md:1418-1419/:1423) now name the digest alone (Luke's yes 2026-10-14). Left: nothing

### Remaining

- [x] R-MT1, design of record the approved proposal `docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md` (Luke yes 2026-10-12): `node.primary.update` and `node.fabric-primary.update` (which replaces `fabric.primary.update`, its 0x21 TakeFabricPrimary/FabricPrimaryTaken and its page). The cohort's row writer (the holder, for the node-admin cohort) pre-checks a successor, publishes "primary updating", cuts off and drains, writes the one committed row (`meshes/<holder mesh>/seat-transfers/<operation>`, one or both seats; the `fabric/seats` rows go), then calls PrimaryUpdate on the successor, which installs from the row and answers `node.primary.updated`; "primary updated" follows, the committed revision their order key in the gossip receive path (SeatedTogether retires: a gossip change with its flame). No successor-side refusal but conflicting payload and fabric-not-ready (no WrongBirth). After the row the successor is the holder whatever the call returns; discovery is not installation. Refusals with no successor use Q7's exact texts (landed 6aae90ee, 3bb4b346, 19f268be; non-admin cohorts landed e7ffb680 / 933b0c61 in e12.s41)
- [x] R-G3 (Luke 2026-10-14, arm A): "updated" carries no contact address; peers resolve a holder's `endpoint_id` and `transport_addr` from its `Digest` and `Members` projection through the live resolver (node_rpc.rs `feed_once`, live.rs). `Frame::NewFabricPrimary` keeps position 14 as a reserved position, never published, refused by name on receive; `announce_new_fabric_primary`, `note_new_fabric_primary`, `FabricContacts`, `attach_fabric_contacts` and `fabric_contacts` deleted; gossip.md loses the catalog row, the decoded example and the "New-fabric-primary and contact addresses" section, and names position 14 in its reserved-positions row and section; frames.md / spans.md regenerated. Flame (Jaeger 192.168.68.99:16687): 0e8028bd75dde6f1fa7a9805b1fa08b7, eaa335e5935a3af0beeb581f5f85076e, 9171f3aca1237368a47452e4a68d98f9 (same mesh); 01d16476139da2d8c64a88852eae5c8a, 33cf0bfa84389031b4123798469eb8f7, 7f2c45aaff44122ff1aa07187f107808 (cross mesh); none carries a `via-new-fabric-primary` span
- [x] build.md:554 and node-rpc-envelope.md:1418-1419, :1423 say the contacts come from the exact birth's digest alone (Luke's yes on the exact text 2026-10-14); `git grep new-fabric-primary-and-contact-addresses -- docs/architecture` names 0 hits
- [x] the gate skips the ten 0x21 TakeFabricPrimary/FabricPrimaryTaken cells that send a Take directly and then read the incumbent: node.fabric-primary.update replaced 0x21 Take/Taken, deleted the ten 0x21 cells, and removed their gate skips (landed 6aae90ee)
- [x] the cells that kill the fabric-primary's admin with no planned hand-off (Luke's ban; audit 2026-10-12): 13 Category A cells (`mesh_elections__sticky_seat::a_dead_fabric_primary_hands_the_seat...`, `mesh_elections__fabric_primary::losing_the_fabric_primary_mesh_moves_control` and `::three_mesh_primaries_keep_the_day0_holder...`, `mesh_elections__late_authority::a_lower_node_id_admin_born_after_a_build...`, `mesh_runtime__successor_adoption::a_successor_manages_births_it_never_launched`, `mesh_lifecycle__mesh_recover::a_lost_mesh_recovers_as_itself...`, `mesh_membership__backbone::membership_rides_mesh_channels_and_the_admin_backbone`, `fabric_build__build_op::a_successor_fabric_primary_starts...`, `::a_successor_fabric_primary_reads_the_create_mesh_receipt...`, `::a_successor_fabric_primary_settles_a_create_mesh...`, the two `mesh_runtime__container_kill` sigkill cells, and `drift_convergence::a_durable_runtime_row_of_a_departed_birth...` + `::a_restarted_mesh_primary_recovers...`) were rewritten to run `node.fabric-primary.update` first and unskipped in the gate (landed dc980004, gate 151 s); `store_root_boot`'s four restarted-admin cells were replaced and unskipped in 95e10533 (gate 83 s); `poc_chaos_kit::poc_rp1_kill_every_admin...`, `estate_teardown::a_dropped_estate...`, `drift_convergence::a_lost_fabric_primary_is_recovered...`, and `build_takeover::a_successor_admin_completes_the_same_build...` landed 533cd70c; the remaining 6 cells in `FP_KILL_SKIP_ARGS` (`losing_the_fabric_primary_mesh_moves_control`, `fabric_build__build_op::a_successor_fabric_primary_*` x3, and `i143_acceptance_2803` `fabric_recovery_*` x2) plus built-in `node.primary.update` and `node.fabric-primary.update` C2 step-1/step-2 and dual-seat resolution in `serve_update_primary` and `NodeRpc` / `Nodes` cleared `FP_KILL_SKIP_ARGS=""`
- [x] the post-landing reds (part 11 found both roots). Red 2, an own-mesh member's status held twice in the digest book, fixed on i143-s23-after-land (RED e1eb014e, GREEN bf44123e, a4402742; build_op lost-executor flame 29b68b2dadc1698aab747c3cde0ebf0a, no Pending revert after node.started); its landing gate (342c0434) went red on park_gate_quiesce, and so did s60 part 11's (09348873, scenario-only) on main c7368e46: main's Day-0 Build drive runs outside the gate across the stop's park and transport cut (41 ms and 499 ms past the cut; the second's claim broadcast refused after the cut), the cell's Must-NOT; red 2 and the drive's root fix LANDED 7b9ff487 (2026-10-13, gate 104 s, its build 43.0 s wall): each pass of a Build drive is a turn of its admin's park gate (build_drive.rs:770 `turns.turn("build-drive")`), so a stop's quiesce waits for the pass in progress and a stopped admin begins none (RED cc3b9f58); park_gate_quiesce runs in the gate again. Red 1, sticky_seat :332: the successor's drive dispatches the holder's stop on its own view before node.primary.updated is acknowledged, so the holder's "primary updated" goes out on parked channels; part 13 built the successor announcing before its dispatch (RED d75fb818, GREEN b1db8031, not landed); the lead held it: the design's step 8 ("Only then does the commanding Build continue", after step 7's "primary updated" by the holder, R-MT1 "NA1 gossips") and the build rule ("A live run is reattached, never re-claimed"; build.md:365) put the root at the Build continuing before the holder's step 7 (N claimed attempt 3 at +92.9 ms while P's step-6 call was in flight), and the successor announces only as the next writer after the holder's departure proof; part 14 (lane storage) fixes it at the claim or the handoff, and gates the executor's own run (build_run.rs:414, the same class as the drive). The gate skips a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts until part 13 lands (red on gates 6bc92cf8 and 0021b424) Red 1 LANDED 3bb4b346 (2026-10-13, gate 147 s, its build 74.4 s wall) at its source: the fabric-primary's own seat update runs under the attempt it claims for itself and ends HandedOff after its step 7 "primary updated"; the successor reattaches and claims the next attempt only then (step 8), and announces "updated" only after the holder's proven exit; the holder's own attempt releases its mesh's in-flight row at its verdict; with it, part 16's holder retry through node-recovery's tickle ladder on the successor's exact birth (the seat vacates only on the provider's exit proof) and a refusal before the row failing the Build by name. sticky_seat's hand-over cell and node_stop's mesh-primary stop cell are tied to R-NS2 (a "primary updated" sent 38.7 ms before the holder's transport cut is lost on the backbone at the cut). The executor's own run gated LANDED b3cfde0d (a run served for a call takes its turn of the park gate; a drive pass's own dispatch runs under the pass's turn, never waiting). R-PU2 (A) and R-SB1 (x) ruled 2026-10-13, their units seated (lanes e1s8, storage); fork: after the successor's proven exit the ordinary rule can seat the stopping holder again under the same operation key (A: the step fails by name, a resubmit gets a new key; recommended).
- [x] resolved by R-RX3 (one held version per source across both receivers and the topology read; landed 4d6d0497 / 00c9f9e9, RED c53a73eb, in e12.s53): a non-primary admin keeps one held Members version per source across backbone_rx, mesh_rx, and the topology read so an older frame on the other channel cannot revert a newer read-installed snapshot

- [x] node.primary.update docs applied (docs updated 2026-10-13 on Luke's decision; code landed 6aae90ee, 3bb4b346, 19f268be, 95e10533): lifecycles/node-primary-update.md (fabric-primary-update.md deleted), the envelope's 0x21 PrimaryUpdate block, the lifecycle index, node-stop/node-connections-delete/states/mesh-delete/elections/build.md/node-identity lines
- [x] R-PU2 (Q8-Q10) (docs updated 2026-10-13 on Luke's decision; code landed 19f268be, RED 8e77f712, gate 107 s; traces e5dc9045a468c89e4055913d7f46cefb and 0eb2c0ebcd667d476c5b55753c5d87e2): the failure reason reads `the new holder's process is gone, provider-confirmed, after the committed row`; no "exited" state and no invented names (SuccessorExited etc. go from i143-rpu2: `SeatMoveEnd::SuccessorExited`, `Ended`, span texts "exit-proven"/"successor-exited"); the row key's second component is the run (`node.primary.update:<build_id>:<run>:<op>`, the first attempt of the series a submission opened), named run in code, docs and spans; the commanded arm: 0x21 gains `Failed { step, reason }` (the Build carrier's terminal), `Incomplete` is never used for an ended step, the commander fails the step on the variant and claims the attempt at the claim door before commanding (wire change: mesh-audit reseal, durable format moved, flame of the commanded arm ending Failed); checks on the landing's spans: `via-seated-again` writes P's record before it announces, for each seat given back and no other; no new "epoch" span text; the install only after `via-successor-ladder` ends exit-proven
- [x] R-SB1 (Q3) (docs updated 2026-10-13 on Luke's decision; code landed 95e10533 / 75219e6b, RED d6be024b, gate 83 s; trace 893deceb77302425afbee48d97dcb764): node-stop.md seat-holder row and node-restart.md row (refusal texts; never yields a seat without a successor; the two exceptions: fabric.delete and the final-admin deletion after mesh.delete's confirmed fabric handover — verified Q7 refusal does not fire on those paths); fabric-recovery.md "Nothing alive"; the four retired store_root_boot names left the gate's skip list in the commit that registered their replacement cells
### Acceptance criteria

<!-- crit:e12.s23.1 -->
- [x] a red cell for the fabric-not-ready check first
<!-- /crit:e12.s23.1 -->
<!-- crit:e12.s23.2 -->
- [x] a seat move and a mesh.delete during the initial state-sync are refused by name with no attempt claimed
<!-- /crit:e12.s23.2 -->
<!-- crit:e12.s23.3 -->
- [x] sticky_seat, build_op, fabric_primary_handover green at gate cadence
<!-- /crit:e12.s23.3 -->
<!-- crit:e12.s23.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s23.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s23 -->

<!-- epic:e10.s2 -->
## e10.s2 — export: one RDM SHA on which the whole export gate is green, the import manifest, and the final parity scan

**Builder:** the lead (gate runs) with an RDM engineer for fixes · **Tracking issue:** #2790 (generated once; not read back) · **Architecture:** §14 Export/import gate; §15 Verification artifacts; §17 Definition of done; transport-parity.md

### State

The parity scan has run before (a ledger row per boundary commit, no MIRROR pending). The open e12 builder blocks land first.

### Remaining

- [ ] Every open e12 builder block has landed.
- [ ] One RDM main SHA on which the fast gate and the release suite are green (§14 every subsection), with no red anywhere left without an owning block.
- [ ] The import manifest names that SHA; the parity ledger has zero blocking gaps; i141 pins exactly that SHA, once.

### Acceptance criteria

<!-- crit:e10.s2.1 -->
- [ ] `eligible=true` for one exact 40-hex RDM main SHA, every §14 checklist item proven with its artifact (§15)
<!-- /crit:e10.s2.1 -->
<!-- crit:e10.s2.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e10.s2.2 -->

### Done when

- [ ] The export gate's artifacts are posted against the SHA, the manifest is committed, and the §17 list holds.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 14-export-import-gate, 15-verification-artifacts, 17-definition-of-done]
<!-- /epic:e10.s2 -->

<!-- epic:e11.s12 -->
## e11.s12 — the seeded soaks and the container proof, after the whole suite is green on one SHA

**Builder:** the lead (long runs are the lead's) · **Tracking issue:** #2949 (generated once; not read back) · **Architecture:** §12 Chaos and observability; §12.1 Seeded soak; §12.2 Container proof

### State

Not started. No soak runs until the fast gate and the release suite are green on one SHA (Luke 2026-10-09). Chaos runs last.

### Remaining

- [ ] The release suite (fast + release stems) green on one RDM main SHA.
- [ ] The §12.1 seeded soak profiles and the §12.2 container proof on that SHA, each with its receipts.

### Acceptance criteria

<!-- crit:e11.s12.1 -->
- [ ] Every §12.1 soak invariant holds on the SHA; the container proof runs the real provider; every soak red is explained from its own evidence with an owning block
<!-- /crit:e11.s12.1 -->
<!-- crit:e11.s12.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e11.s12.2 -->

### Done when

- [ ] The soak and container receipts are posted against the SHA that e10.s2 exports.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 12-chaos-and-observability, 12-1-seeded-soak, 12-2-container-proof]
<!-- /epic:e11.s12 -->
