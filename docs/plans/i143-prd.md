# i143 - Build, prove, and export the generic Mesh product on RDM

## § Close-out plan (2026-10-17, the lead; Luke: "take stock of what's left in 143 and break it apart")

i143 pivoted mid-flight into standardising the Node RPC and gossip formats (i144 e2b/e5/e6, i146, i147). What is
left of i143 itself, by block, and who finishes it; the sequential tail (one green SHA → soaks and container
proof → export) is last and the lead's.

| block | left | disposition | owner | parallel with |
|---|---|---|---|---|
| e12.s90 | 13 criteria | **closed as superseded** by i144 (e1 the four states, e2 the fact, e3 resumption and delete, e4 canaries all landed); nothing builds here | lead (record) | — |
| e12.s57 | the two fabric_recovery cells that kill the fabric-primary with no hand-off | **absorbed by i144 e6.2**: under Q24 a fabric-primary lost with its mesh is the takeover, and `fabric_takeover::…` is that cell; the two acceptance cells are restated to the takeover path in e6.2's landing | i144 dev (opus) | Codex's s75/s88 |
| e12.s23 | the sticky_seat.rs:551 read (a planned hand-over moves both seats in one row before the new one acts) | **absorbed by i144 e6.1**: `seat-moves/<T>` is that one row; the cell is e6.1's | i144 dev | — |
| e12.s75 | `NodeRpcServer::drain()` and its 9 callers (RDM); the `epoch` field in rafka-v2's reach-reroute and node-base | RDM rename: Codex, one landing after Q47; the rafka-v2 `epoch` rename **moved to rafka-v2 i142** | Codex | everything |
| e12.s88 | R14 item 6: the container provider's store-root qualification cell, the bind mount and `RDM_STORE_DIR` in the container env, the three container cells unskipped in the gate, green in release | independent of every other open block; needs the docker host | Codex, after s75 | the dev's e6 and i146 e3 |
| e12.s89 | the five `rshape-*-process` jobs green in one release run; each red read where it lives | most of its reds were the defects i144/i146/i147 fixed (C17 → i144 e2 landed; stopping on admission → D2 landed; identity after restart → Q47; authority hand-offs → e6; control loss hearing two births → i147). **One release run after Q47 and e6 land**, then the remaining reds split: authority and lifecycle jobs to the i144 dev, routing and partition jobs to Codex; the `node-not-live` page question is answered (node-restart.md's row, Q2) | lead runs; dev + Codex read | — |
| e12.s69 | Rafka side: the entry pull off the rafka-mesh-ops projection frames onto node.topology.get; the belt and accept loop move under i145 (post-chaos) | **moved to rafka-v2 i142** ("Moved from RDM i143", 2026-10-17): the entry pull onto node.topology.get; the belt and accept loop stay under i145 | Codex, after s88 | the dev's e6 and i146 e3 |
| e12.s55 | R-HB3 | stays parked post-beta | — | — |
| e10.s2 | one green SHA, the import manifest, the parity scan, the export gate | **sequential tail**, after every row above | lead | — |
| e11.s12 | the seeded soaks and the container proof on that SHA | after e10.s2's SHA; the container proof needs s88 | lead, with the dev | — |

Order: (1) Q47 lands (Codex); e6.1 → e6.2 → e6 item 3 → i146 e3 owners (dev); s75 → s88 → s69 and the rafka-v2 `epoch` rename (Codex), all in parallel with the dev. (2) One release run at the resulting SHA (lead); s89's remaining
reds read and fixed, split as above. (3) The fast gate and the release suite green on one SHA → e10.s2 → e11.s12.
i145 lands after that (post-chaos). Every landing still goes through the land script, RED first, with its flame.

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
e12.s84 (issue to generate)  R-CY1: the canary red read and fixed (Root 1: cohort seat holder restart runs `node.primary.update` under `restart.attempt` ending `handed-off` and executes the restart steps under `reconcile.attempt = restart.attempt + 1`; RED 368d110a, landed 69752953, gate 39 s; trace `ef78ccae756293f65263737ffab36200`)
e12.s85 (issue to generate)  layout pre-step: `fence.rs` -> `seat_revision.rs`, `departed_status.rs` -> `deleted_status.rs`, and the 32 `i143_acceptance_*.rs` files renamed into their crates' test binaries with 100% discovery parity and 0 stem violations
e12.s86 (issue to generate)  R-FM1: hung runtime hold (`RDM_HUNG_HOLD_FLOORS`, default 10), `TerminateRuntime` first step of recovery Build, and `held-running` store fact (`NodeView`/`FabricView`, gossip wire untouched; RED 505d99eb, landed 549493fe, gate 59 s; traces `07f3b640645fb9621219d50fa3135604`, `6f2cee1c90c07a9a7ce03607ded20cdb`, `88f6435305841044a5468305d6c1f66e`, `ed394bca8f94b70380f4cf4ff249b2fa`)
e12.s87 (issue to generate)  R-FM2: gone vs broken inherited `data_dir` (`RDM_DATA_DIR_GONE_FLOORS`, default 2; `data-dir-lost` and `data-dir-unreadable` store facts + `node.get` fields, gossip wire untouched; RED 5068c55d; traces `3afc5ad0a17c48afa65aebd97fa550b4`, `b969d8fa8549ea89be5403ec5c232a8b`, `1d467f78873ef57f1d928f6e156e24d2`, `d7f84fcf6d32c35ceb719b6db6263a9c`, `3398e9d6a778824a3336aa83c29442ac`, `af93811d9ec896682b5c0ec249c0a3bd`, `2302862bb508b7b0584913877b8b491d`)
e12.s90 (issue to generate)  open: the builder block `e12.s90` below (R-PV1: the provider reports an OS-stopped runtime; os-stopped is a fact, never a hold or a terminate)
e12.s89 (issue to generate)  the R-shape release cells: nine red cells across the authority, lifecycle, routing and partition chaos-process jobs, each read from its own trace and fixed where it lives
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
- [x] R-BI5a (Luke 2026-10-14, "i agree"; landed 55131ee4, gate 46 s, RED 8f0e64d0; cell `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans`; live Jaeger trace `a592b745aaea8819ba14a5777842e24b`, span `c696c67b348c3a5e`): `BuildId::log(&self) -> BuildIdLog<'_>` (`rafka-mesh-entity/src/ids.rs:193-212`) renders `bid_xxxx-xxxx-xxxx` across `build_id = %<expr>.log()` span fields where `<expr>` is a `BuildId` (`rafka-node-admin-client`, `rafka-node-admin-core`), while `Display` (`ids.rs:151`), durable row keys (`node.primary.update:{build_id}:{run}:{op_key}`), stored `BuildFact` rows, `0x20` wire strings and page examples stay the bare 12 characters, and `BuildId::parse("bid_...")` refuses by name (`WrongWidth` / `NonCanonical`). Evidence-reader decisions: crate integration tests (`rafka-node-admin-core/tests/{build_facts_read,build_takeover,drift_convergence,node_objects,receipt_refusal,recovery_admission,store_root_boot}.rs`, `rafka-node-rpc-testkit/tests/{common/mod,node_delete_pipeline,recover_pipeline}.rs`) match `build_id.log().to_string()`; scenario JSONL readers (`Estate::spans` in `rafka-test-scenario/src/estate.rs:794-799` and `spans_of` in `rafka-test-scenario/tests/i143_acceptance_2777.rs:53-58`) normalize `bid_xxxx-xxxx-xxxx` on `fields.build_id` to bare 12-char for scenario REST/RPC string comparisons while on-disk `.spans.jsonl` files keep `bid_xxxx-xxxx-xxxx`; `/home/admin/rafka-v2/scripts` has no span `build_id` readers; `/home/admin/rafka-v2` has one `build_id = %` hit at `crates/rafka-entity-topology/src/builds.rs:346` (`build_id: &str` on `rafka.node_admin.build.resolve.via-build-fact-index`, untouched); `docs/architecture/node-identity-and-state.md:107-108` (§2.0) updated on Luke's yes (2026-10-15, to the exact sentence)
- [x] the gate of docs-only tree b3022dce went red on `node_lifecycle__node_restart::rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state` (canary.rs:157: no `NodeRestarting` step span under the restart call's attempt; the detail log shows that attempt ending `handed-off`), the one red of that stem in 645 gate runs on this box; the tree changed no code, so the red is main's (R-LD1) and the docs landed as a fast-forward. The read is its own RED-first landing with a flame, two candidate roots: the cell assumes the restart runs under `restart.attempt` while a cohort seat holder's restart runs `node.primary.update` under its own attempt first and the restart under the next; or the restart reply should name the attempt that executed it (a product question). Never a cell edit inside another landing. R-CY1 (Luke 2026-10-15): the read is block e12.s84 (Root 1 confirmed; RED 368d110a; trace `ef78ccae756293f65263737ffab36200`)

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

**Builder:** Rafka engineer (rafka-v2; coordinate the landing with the dev lead) · **Tracking issue:** #3100 (generated once; not read back) · **Architecture:** gossip.md §8; fabric-node-hooks.md; fabric-certs.md#on-a-connection; internal-mesh-auth.md; rulings R-SV1

### State

moved 2026-10-17 to rafka-v2 `docs/plans/i142-node-rpc-migrations-prd.md` ("Moved from RDM i143"); the belt and accept loop are i145's; this block builds nothing. Was: not started; absorbed s70

### Remaining

- [ ] rafka-mesh-ops tags 16/18/19 (ControlFrame::ProjectionPull / ProjectionSnapshotPart / ProjectionRefused; rafka-node-base serve_projections, read_projection_answer) move to node.topology.get. The SVID belt, Rafka's accept loop and Rafka's node-admin minting do not get deleted here: they move to RDM under the cert epic (`docs/plans/i145-cert-ownership-prd.md`, from `docs/plans/proposal/cert-verification-ownership.md`; Luke 2026-10-16 superseded R-SV1's sentence). Rafka repo: coordinate the landing with the dev lead

### Acceptance criteria

<!-- crit:e12.s69.1 -->
- [ ] a Rafka node enters through node.topology.get
<!-- /crit:e12.s69.1 -->
<!-- crit:e12.s69.2 -->
- [ ] no `rafka.mesh.auth.*` span on rafka-mesh-v1; the admission fact and span are RDM's (`node.connections.admit`)
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

absorbed s71 and s74. The first Node RPC frame is the target: landed 87df0a35 (2026-10-13, gate 86 s, build 11.3 s): `Fence { target_node_id, op }` → `Target { node_id, op }`, `RequestHeader::target`, `MAX_TARGET_BYTES`/`peek_target`, the framing doc's `target_len`/`target`, four test fns, the stem `node_rpc__restart_fence` → `node_rpc__restart_target`; postcard positional, no wire move; `grep -rnwi fence` in rafka-node-rpc-contract + rafka-node-rpc 100 → 0 (seven substring hits left, all `BuildReply::Fenced`, the seat epoch). The land script refuses a diff that adds a retired word in an identifier or file name, or an id-named test file (layout §4 name check). `SeatHolder.epoch` → `revision` (`SeatEpoch` → `SeatRevision`, `RejectedEpochOverflow` → `RejectedRevisionOverflow`) landed e7ffb680 / 933b0c61 (RED ad6af7ad). `UpdateNode`/`NodeUpdated` → `UpdateName`/`NameUpdated` landed e634b766 (RED b0f888b0). The code-only renames (`Forwarded`, `ConnectionsDeleting`, `SeatYielded`, `recovery_round`) LANDED 3eaae3e6 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`). `node.mesh.join` (`JoinMesh` / `MeshJoined` on `0x1B`), `node.mesh.leave` (`LeaveMesh` / `MeshLeft` and `BusyHoldingConnections` on `0x1B`), their `rdm.node.mesh.*` spans, the reserved status/frame renames (`Reserved6`, `Reserved7`, `Reserved2`, `NodeConnectionsDeleting`, `NodeConnectionsDeleted`), and regenerated `docs/reference/rdm/*.md` LANDED e34468a0 (2026-10-14, gate 68 s, `units=455`; RED 9f2f9b55; cell `connections_all::node_mesh_leave_and_node_mesh_join_standalone_and_inside_stop` traces 686542851fe55fd7a6974693a1bbd7ce, d995f8f05f1b7d536ed20fb9787771ec, bc5f29b816021eecb9b07343f7e2a2d5, 88b538196f2a62b70768c2b8b2cf42fb and `node_start_sequence::every_start_path_runs_the_same_start_steps_in_the_same_order` trace 51dd1e00f0b75c2b7f972d7c4c1ef34b). `AdminHandle::leave` gossip self-publication & linger removal LANDED b81f66a0 (2026-10-14, gate 71 s). `crates/rdm-admin/src/admin.rs` split into `admin/{records,projection,rows_writer,recovery,reconcile,session,runner,boot,mesh_delete}.rs`, `workflows/*.rs` and `primitives/mod.rs` with conformance §1 code-only renames (`delete_mesh`, `lead_delete`, `admin_for_deleting_mesh`, `create_deleting_mesh_admin`, `revise_predecessor`, `revise_exited`, `delete_stopped`, `start_stopped`, `stopped_birth*`, `delete_node`, `clear_unwatched`, `DeleteRequest`, `DeleteStep`) LANDED 18cbfc5f (2026-10-15, gate 66 s). Left: `claim_revision` (coordinated with rafka-v2)


RDM admission API rename: `NodeRpcServer::begin_connections_delete()` and all ten current callers replace the retired server method. The switch, refusal and resume semantics and every wire byte stand. Compile RED `cb877dac` (`/home/admin/rust-distributed-mesh-deploy/s75-server-rename-red.log`); the existing connections-deleting refusal, Forward, stop/start and handover cells exercise the renamed calls. The Forward cell is `a_nested_forward_and_a_carrier_deleting_connections_are_still_refused_by_name`. The rafka-v2 recovery-round/claim-revision work stays with its lead. Landing gate follows.

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
- [x] src/workflows/<op> per lifecycle page; primitives (node.connections.delete, the transport cut, topology reads, ...); renames (mesh_leave -> mesh_delete, retire -> delete, park -> stop, crate::http -> crate::control, departed -> deleted; a mesh is never reborn: the span `rdm.node_admin.mesh.create.via-rebirth` (investigate.rs ~583) is the node.create of a node-admin into the live mesh (mesh-investigation.md step 6), and the "reborn mesh" wording goes from investigate.rs, admin.rs, round.rs, build_drive.rs and their cells (2803_detect 80, rshape_burn_in 35, drift_convergence 23, build_drive 19 hits on main 01a65889); a node's re-birth stays); ~120 comment citations to current doc paths. One mechanical landing after the cuts (landed 18cbfc5f, gate 66 s)

### Acceptance criteria

<!-- crit:e12.s75.1 -->
- [x] one module per op page
<!-- /crit:e12.s75.1 -->
<!-- crit:e12.s75.2 -->
- [x] no comment cites a removed page
<!-- /crit:e12.s75.2 -->
<!-- crit:e12.s75.3 -->
- [x] the fast gate green
<!-- /crit:e12.s75.3 -->
<!-- crit:e12.s75.4 -->
- [x] The story bar holds (section story-bar).
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

done (R-BI9 landed on `docs/architecture/build.md`: A7, A8, A9, A10, A11, A13 applied; A6 at :371 and A14 already on main; A12 packages table deferred to the crate layout's packages-table landing after the `rdm-` rename); earlier this session: R-ML3 (fabric-recovery, mesh-investigation), R-PB1/R-PC1 (fabric-not-ready), stopped/deleted, R-TG2/R-TG3 tags, BuildChange::Recipe, HandedOff.to, the R-HB1 gossip rows, the R-RX docs; absorbed s8 and s32. Nothing left in this block (A12 tracked under the crate layout step 9).

### Remaining

- [x] build.md for R-BI1 (each topology change is its own Build) and R-BI3 (no Fabric pointer, LIFO read); mesh-investigation step 6 and :162 for R-DG1 (degraded on mesh and fabric, both arms); gossip.md ~:118 and ~:154 for R-RX4; the envelope 0x1C RejectedNotAuthority line (Luke's yes); the lifecycle index node.tags row; the consolidation's page renames; regenerate docs/reference/rdm after each RDM landing
- [x] fabric-node-lifecycle-elections.md §5 ("Election movement never changes Fabric.build_id") and every architecture line naming Fabric.build_id as a pointer: the current Build is read LIFO (R-BI3, R-BI4); build.md for R-BI4 (an attempt lost with its executor leaves the Build in progress)
- [x] (applied 2026-10-13 on Luke's direction, merged with that day's decisions Q2-Q10) the approved node.primary.update proposal's doc edits: a docs seat's attempt (97ebf365b1, 9e0e3bf757, 441f463f2a, 3d98dfd16c) was reverted (5f8db8a2d7..550c07cbe4) — it went beyond the listed edits and was relayed unread. Redo only on Luke's direction, each edit read by the lead before it is reported
- [x] (done 2026-10-12, with the Fence→Target rename) R-UC1a's doc lines (envelope §0's 425 row, the reserved-code 425, node-rpc.md :480/:595) and the R-BI3 pointer sweep (build proposal A2) on Luke's yes

### Acceptance criteria

- [x] R-BI9 (Luke 2026-10-15): build.md A7, A8, A9, A10, A11 and A13 land as the build proposal writes them, one docs landing; the dev re-verifies each line against main (A6 at :371 and A14 are already on the page) and records the exact diff here; A12 (packages table) deferred to the crate layout's packages-table landing after the `rdm-` rename:
  ```diff
  --- a/docs/architecture/build.md
  +++ b/docs/architecture/build.md
  @@ -3 +3 @@
  -This page owns the Build engine (the accepted topology, the Build/attempt/claim model and `BuildStateAdapter`), the node-admin storage interfaces and crash safety, and the RDM/Rafka boundary with its import rule. The deployment provider and the RuntimeFact contract are [deployment-provider.md](deployment-provider.md); this page's sections skip §7 and §8 so that §9 to §16 keep their numbers.
  +This page owns the Build engine (the accepted topology, the Build/attempt/claim model and `BuildStateAdapter`), the node-admin storage interfaces and crash safety, and the RDM/Rafka boundary with its import rule. The deployment provider and the RuntimeFact contract are [deployment-provider.md](deployment-provider.md).
  @@ -56 +56 @@ Allowed customer seams are:
  -- implementations of the imported Fabric/Mesh/Nodes/Connections/Build storage interfaces;
  +- implementations of the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6);
  @@ -99 +99 @@ Rafka imports these RDM packages at one exact revision:
  -| `rafka-mesh-entity` | the generic Mesh entity: exact-node identity, process incarnation, membership, endpoint-slot freshness, RuntimeFact and connection projections |
  +| `rafka-mesh-entity` | the generic Mesh entity: exact-node identity, process incarnation, membership, endpoint freshness, RuntimeFact and connection projections |
  @@ -102 +102 @@ Rafka imports these RDM packages at one exact revision:
  -| `rafka-node-admin-core` | generic Fabric/Mesh/Node control: Build/Reconciler and drift trigger, LeadershipResolver, the fabric/mesh/nodes/connections/builds storage interfaces with memory and durable-file implementations, and the lifecycle, deployment, runtime-adoption, provider-domain and recovery contracts |
  +| `rafka-node-admin-core` | generic Fabric/Mesh/Node control: Build/Reconciler and drift trigger, LeadershipResolver, the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6) with memory and durable-file implementations, and the lifecycle, deployment, runtime-adoption, provider-domain and recovery contracts |
  @@ -128 +128 @@ submitted change
  -The accepted topology names the exact logical slots/path.names that should exist, grouped by Mesh. Counts may remain product-facing resize input, but the compiler resolves a count change to exact paths before the Build is persisted.
  +The accepted topology names the exact path.names that should exist, grouped by mesh. Counts may remain product-facing resize input, but the compiler resolves a count change to exact paths before the Build is persisted.
  @@ -133,5 +133,5 @@ B.topology:
  -    admin.1
  -    admin.2
  -    rpc.1
  -    rpc.2
  -    rpc.3
  +    mesh1.admin.alpha
  +    mesh1.admin.beta
  +    mesh1.rpc.north
  +    mesh1.rpc.south
  +    mesh1.rpc.east
  @@ -139 +139 @@ B.topology:
  -submit: remove mesh1.rpc.2
  +submit: remove mesh1.rpc.south
  @@ -143,4 +143,4 @@ B'.topology:
  -    admin.1
  -    admin.2
  -    rpc.1
  -    rpc.3
  +    mesh1.admin.alpha
  +    mesh1.admin.beta
  +    mesh1.rpc.north
  +    mesh1.rpc.east
  @@ -309 +309,3 @@ pub trait BuildStateAdapter: Send + Sync {
  -    async fn publish_intent(&self, intent: &BuildIntentFact) -> Result<(), BuildStateError>;
  +    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
  +    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError>;
  +    async fn publish_fabric(&self, record: &FabricIdentity) -> Result<(), BuildStateError>;
  @@ -310,0 +313,3 @@ pub trait BuildStateAdapter: Send + Sync {
  +    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError>;
  +    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError>;
  +    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError>;
  @@ -312,0 +318 @@ pub trait BuildStateAdapter: Send + Sync {
  +    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError>;
  @@ -314,0 +321,6 @@ pub trait BuildStateAdapter: Send + Sync {
  +    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
  +    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
  +    async fn note_blocked(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str);
  +    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError>;
  +    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
  +    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError>;
  @@ -318 +330 @@ pub trait BuildStateAdapter: Send + Sync {
  -The exported semantic vocabulary is append/fold friendly:
  +The exported semantic vocabulary (`BuildFact` variants) is append/fold friendly:
  @@ -321 +333,2 @@ The exported semantic vocabulary is append/fold friendly:
  -BuildIntentFact
  +BuildAccepted
  +AttemptOpened
  @@ -325 +337,0 @@ BuildAttemptReceipt
  -BuildCompletionFact
  @@ -328 +340 @@ BuildCompletionFact
  -These facts describe reconciliation execution. The current Build is read from them newest first (§4); they also record what happened while executing it. They are not a second topology authority and not the current-runtime store. Completed Build records remain valid history for recovery, API views, diagnostics, audit and chaos evidence.
  +These facts describe reconciliation execution. The current Build is read from them newest first (§4, `current_build()` over the acceptance-order index, with `newest_accepted_at_ms()` as the acceptance high-water mark of every state and `recovery_of` answering the point read by exited birth); they also record what happened while executing it. They are not a second topology authority and not the current-runtime store. Completed Build records remain valid history for recovery, API views, diagnostics, audit and chaos evidence.
  @@ -336 +348 @@ RDM proves the contract without inventing a broker-backed database:
  -- optional local-file journal for same-node-admin restart, replay, and evidence artifacts.
  +- local-file journal for same-node-admin restart, replay, and evidence artifacts.
  @@ -347,2 +359,2 @@ Rafka must not persist one mutable Build row and increment `attempt` by read-mod
  -BuildIntentFact
  -    blind put once
  +BuildAccepted
  +    insert-and-fail on build_id
  @@ -350,2 +362,5 @@ BuildIntentFact
  -BuildAttemptClaim(build_id, attempt)
  -    insert-and-fail claim
  +AttemptOpened(build_id, mesh, attempt)
  +    insert-and-fail on (build_id, mesh, attempt)
  +
  +BuildAttemptClaim(build_id, mesh, attempt)
  +    insert-and-fail claim on (build_id, mesh, attempt)
  @@ -360,0 +376,6 @@ current attempt
  +
  +current Build
  +    current_build() over the acceptance-order index
  +
  +recovery index
  +    recovery_of(path, node_id, incarnation)
  @@ -392 +413 @@ No Build is current before it is recoverably durable.
  -Control-plane state lives behind RDM's five storage interfaces: fabric, mesh, nodes, connections and status. In RDM's file adapter they open under one store root per fabric, `<store_dir>/<fabric_id>/{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}`, where `<mesh>` is the mesh's logical name. Writers are the ladder: the fabric-primary writes `fabric/`, each mesh-primary its `meshes/<mesh>` and `nodes/<mesh>/`, each node its own `connections/<node>/`. Each node-admin also keeps its own Build journal (`builds.storage`). The generic code knows the interfaces, never a specific database; Rafka binds its system topics to them.
  +Control-plane state lives behind the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6). In RDM's file adapter they open under `<store_dir>/fabrics/<name>` and one store root per fabric, `<store_dir>/<fabric_id>/{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}`, where `<mesh>` is the mesh's logical name. Writers are the ladder: the fabric-primary writes `fabric/` (and `fabric/builds/`), each mesh-primary its `meshes/<mesh>` (including `meshes/<mesh>/claims/` and `meshes/<mesh>/builds/`), `nodes/<mesh>/` and `status/<mesh>/`, each node its own `connections/<node>/`. The generic code knows the interfaces, never a specific database; Rafka binds its system topics to them.
  @@ -394 +415 @@ Control-plane state lives behind RDM's five storage interfaces: fabric, mesh, no
  -`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once. It carries no pointer to a Build (RDM's `FabricRecord` in `rafka-node-admin-core`).
  +`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once; the fabric's name is claimed insert-and-fail at `<store_dir>/fabrics/<name>` above the per-fabric roots, and a deleted fabric releases it. It carries no pointer to a Build (RDM's `FabricRecord` in `rafka-node-admin-core`).
  @@ -508 +529 @@ fabric-primary accepts Build B19:
  -The fabric-primary decides that mesh2 exists and what it contains; mesh2's node-admin creates mesh2's nodes. mesh2 comes into being with the `node.create` of its first node-admin, whose sequence is [node.create's mesh-absent arm](lifecycles/node-create.md#mesh-absent).
  +The fabric-primary decides that mesh2 exists and what it contains; mesh2's node-admin creates mesh2's nodes. mesh2 comes into being with the `node.create` of its first node-admin ([mesh-absent arm](lifecycles/node-create.md#mesh-absent)); that is what `mesh.create` names: there is no separate op.
  @@ -528 +549 @@ fabric-primary accepts Build B20:
  -mesh1 is deleted by `mesh.delete` and mesh3 comes into being with the `node.create` of its first node-admin. mesh3 always has a new MeshId; a MeshId is never reissued, even when the new mesh takes mesh1's name after mesh1's row is deleted. A dead mesh is never restarted: mesh3 is mesh.create of a new mesh, and it does not preserve mesh1's logical identity merely to reuse its slot/name.
  +mesh1 is deleted by `mesh.delete` and mesh3 comes into being with the `node.create` of its first node-admin. mesh3 always has a new MeshId; a MeshId is never reissued, even when the new mesh takes mesh1's name after mesh1's row is deleted. A dead mesh is never restarted: mesh3 is the mesh-absent arm of `node.create` under a new MeshId, and it does not preserve mesh1's logical identity merely to reuse its name.
  @@ -534 +555 @@ fabric-primary accepts Build B21:
  -    mesh2 {rpc.1, rpc.2} -> mesh2 {rpc.1}
  +    mesh2 {mesh2.rpc.north, mesh2.rpc.south} -> mesh2 {mesh2.rpc.north}
  @@ -538,2 +559,2 @@ fabric-primary accepts Build B21:
  -The removal is the [node.delete](lifecycles/node-delete.md) of mesh2.rpc.2, which mesh2's primary
  -executes under the fabric-primary's claim; the fabric-primary never commands mesh2.rpc.2 and never
  +The removal is the [node.delete](lifecycles/node-delete.md) of mesh2.rpc.south, which mesh2's primary
  +executes under the fabric-primary's claim; the fabric-primary never commands mesh2.rpc.south and never
  @@ -622 +643 @@ The RDM-generic proofs of this page's mechanisms:
  -- elastic shape/grow/shrink tests assert accepted per-cohort counts or settled shape rather than requiring surviving ordinals to be contiguous; identity/leadership/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
  +- elastic shape/grow/shrink tests assert accepted per-kind counts or settled shape rather than requiring surviving names to be contiguous; identity/leadership/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
  ```
- [x] no architecture line contradicts a ruling
- [x] check-doc-symbols clean
- [x] The story bar holds (section story-bar).

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

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

<!-- epic:e12.s84 -->
## e12.s84 — R-CY1: the canary red is read; the restart of a cohort seat holder runs under the attempt after its hand-off

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-CY1; `lifecycles/node-primary-update.md` (the holder's own seat update runs under its attempt and ends HandedOff); `lifecycles/node-restart.md`; R-SB1

### State

done (closed 2026-10-15; RED 368d110a; landed 69752953, gate 39 s; Root 1 confirmed: `Estate::complete(&mut stream)` in `crates/rafka-test-scenario/src/typed.rs:626-633` returns `stream.accepted()`, which carries the `Started { build_id, attempt }` frame sent when the `node.restart` workflow stream is first admitted (`attempt == 2`). When the restarted `rpc` node holds the `rpc_node-primary` cohort seat, `build_drive.rs::seats_first` (`:1240-1299`) claims `restart.attempt` (`2`), dispatches `move_seats` (`rdm.node_admin.build.update.via-dispatch` -> `rdm.node_admin.seat.update.via-primary-update`), and completes `restart.attempt` with `BuildReply::HandedOff` (`outcome = "handed-off"`). The Build drive then claims `attempt == 3` (`restart.attempt + 1`) and executes `rdm.node_admin.build.update.via-reconcile` (`operations = "node.restart:<node>"`) and all five restart deployment steps (`NodeRestarting`, `StopNode`, `AwaitNodeLeft`, `StartNode`, `WaitForNodeReady`) under that reconcile's attempt. `canary.rs` now selects the `rpc` cohort primary seat holder deterministically (`is_primary == true`, lowest `node_id`), asserts the `restart.attempt` hand-off (`rdm.node_admin.build.update.via-dispatch` with `outcome == "handed-off"` and descending `rdm.node_admin.seat.update.via-primary-update` with `outcome == "updated"`), reads the executing attempt from `reconcile["attributes"]["attempt"]` (`restart.attempt + 1`), and asserts the five restart step spans under that attempt. Live Jaeger trace `ef78ccae756293f65263737ffab36200` on `build_id = 7wsh5fxxa8vt` (`bid_7wsh-5fxx-a8vt`): `rdm.node_admin.build.update.via-node-rpc` `b42fd02886983c02` (`attempt = "2"`), `rdm.node_admin.build.update.via-dispatch` `70e0f9c8179495bf` (`attempt = "2"`, `outcome = "handed-off"`), `rdm.node_admin.seat.update.via-primary-update` `50599e1d8b2c5042` (`attempt = "2"`, `outcome = "updated"`, `holder = "mesh1.rpc.1"`, `seat = "rpc_node-primary"`), `rdm.node_admin.build.update.via-reconcile` `7fb375cd7794c724` (`attempt = "3"`, `outcome = "converged"`, `operations = "node.restart:mesh1.rpc.1"`), `rdm.node_admin.node.update.via-build` `e5927972c77819a0` (`attempt = "3"`), `rdm.node_admin.node.update.via-step` `6541c02bddeb6259` (`attempt = "3"`, `step = "StartNode"`, `outcome = "complete"`), `rdm.node.lifecycle.start.via-rpc` `3042f51a7e6df98b` (`attempt = "3"`, `outcome = "started"`))

### Remaining

- [x] RED: a cell that gives the restarted rpc node the cohort primary seat deterministically (lowest node id, or the seat taken before the restart) and runs the canary body; it must fail at canary.rs:157 as the two gate reds did (RED 368d110a: `canary.rs` deterministically picks the `rpc` cohort primary holder `mesh1.rpc.1` and panics at `canary.rs:157:32` because `attempt == "2"` ends `handed-off`)
- [x] flame from a live mesh: the restart call's attempt running `node.primary.update` and ending `handed-off`, the restart's steps under the next attempt (Jaeger trace `ef78ccae756293f65263737ffab36200`: `attempt = "2"` runs `rdm.node_admin.build.update.via-dispatch` `70e0f9c8179495bf` -> `rdm.node_admin.seat.update.via-primary-update` `50599e1d8b2c5042` ending `handed-off`; `attempt = "3"` runs `rdm.node_admin.build.update.via-reconcile` `7fb375cd7794c724` -> `rdm.node_admin.node.update.via-build` `e5927972c77819a0` and the five restart steps)
- [x] name the root: (1) the cell's assumption (the restart runs under `restart.attempt`) is wrong and the cell reads the attempt from the reconcile that executed the restart; or (2) the restart reply should name the executing attempt, which is a ruling for Luke before code moves (write the packet, stop) — Root (1) confirmed: `stream.accepted()` returns the attempt opened when the stream was admitted (`restart.attempt`), which dispatches `node.primary.update` and ends `handed-off`, while the reconcile and its five restart steps execute under `reconcile.attempt = restart.attempt + 1`
- [x] the fix lands alone with that sentence in this State; the gate's red story row under e12.s58/s76 points here (landed 69752953, gate 39 s)

### Acceptance criteria

<!-- crit:e12.s84.1 -->
- [x] `node_lifecycle__node_restart` green at gate cadence with the seat-holder case covered deterministically, its trace cited here (`ef78ccae756293f65263737ffab36200`)
<!-- /crit:e12.s84.1 -->
<!-- crit:e12.s84.2 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s84.2 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s84 -->

<!-- epic:e12.s85 -->
## e12.s85 — layout pre-step: the two retired-stem files and the 32 id-named test files, before any crate directory moves

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §3, §4 (name check), the lead's sequencing note on the rulings page (2026-10-15)

### State

done (closed 2026-10-15; pre-step `crates/rafka-node-admin-core/src/fence.rs` -> `seat_revision.rs`, `crates/rafka-mesh-transport/tests/departed_status.rs` -> `deleted_status.rs`, and all 32 `i143_acceptance_*.rs` test files renamed into their crates' `tests/main.rs` binaries by mechanism (`members_snapshot`, `status_reinforcement`, `refeed_scheduler` in `rafka-mesh-transport`; `status_rpc`, `executor_redecide`, `connections_reconnect`, `gossip_frames_wire` in `rafka-node-admin-core`; `proof_store` in `rafka-node-rpc-testkit`; and 24 `<family>__<case>` modules in `rafka-test-scenario`); extraction map refreshed at `f5446971`, test discovery ratchet added to `rdm-gate-at.sh`, and ungated main reds fixed including the `rafka-mesh-telemetry` OTLP HTTP exporter URI `/v1/traces` path pin LANDED `f7f41b29` [gate 62 s]; Step 4a `rdm-core` fold LANDED `a005cda8` [gate 63 s]; Step 4b `rdm-client` fold LANDED `cd9e18b3` [gate 66 s]; Step 5 `rdm-node` extraction LANDED `4d9a794c` [gate 60 s]; Step 6a `rdm-transport` fold LANDED `93747854` [gate 61 s]; Step 6b `rdm-admin` fold LANDED `6a8ba1c1` [gate 67 s]; Step 6c `rdm-test` fold LANDED `15d8a582` [gate 61 s]; Step 7 `admin.rs` split into `admin/{records,projection,rows_writer,recovery,reconcile,session,runner,boot,mesh_delete}.rs`, `workflows/*.rs` and `primitives/mod.rs` with conformance §1 code-only renames LANDED `18cbfc5f` [gate 66 s]; Step 8 test family layout across `crates/rdm-test/tests/{build,chaos,elections,fabric,gossip,mesh,node,rpc,runtime}/` and `crates/rdm-admin/tests/` LANDED `a982a84c` [gate 83 s, after `bce35b63` hit the gate's stem filter where `routing::` matched `role_routing::`, fixed at `rdm-gate-at.sh:239`]; Step 9 `rdm-mesh-audit` and `rdm-docgen` tool package prefix renames and regenerated `docs/reference/rdm/*.md` LANDED `1bcc2200` [gate 69 s]; `accepted_topology` 170 s timeout at `a269d7e5` (red climb trace `http://192.168.68.99:16687/trace/6d1b67fa8fd0191d4a75f4c7e5b4c8bc`: `RoundBook::take_due` discarded `route_lost` while `commit-state` was `Command::Out` when `mesh1.admin.2` crossed the 3000 ms staleness floor at `17.268 s` before `ReplyLost` at `17.327 s`, and `RoundBook::undelivered` did not wake the round driver when `addressed` was `false`) plus `boot.rs:1616-1645` Ready status race fixed LANDED `689a94d6` [gate 59 s, live fix flame `http://192.168.68.99:16687/trace/a7ce40d701966474a9fbd0105ebdcf3b` (`event=reachable-again` on `rdm.node_admin.fabric.update.via-round-command`), climb flame `http://192.168.68.99:16687/trace/f3921d858ea5e6b5fe4096402c8b752c`, cell trace `http://192.168.68.99:16687/trace/5b5cbe684e21ea4a0c8f90525eb35aa6`]; `node_stop` red at `a269d7e5` (ClickHouse traces `http://192.168.68.99:16687/trace/edccc8a30252ff9245b6b54339212f3a` [client `nodes.stop` + `via-build-stream`] and `http://192.168.68.99:16687/trace/10b22166562bc5e985c4de7038b81b41` [`NA2` `mesh2.admin.2` `via-reconcile` -> `via-step` `StopNode` -> `PrimaryUpdated`]) measured: after `NA2` (`mesh2.admin.2`) completed `P`'s stop at `15441.3 ms` and published `PrimaryUpdated` at `15441.4 ms`, 1-hop `mesh2.rpc.1` heard `via-primary-updated` at `+0.6 ms` (`15442.0 ms`, trace `http://192.168.68.99:16687/trace/bfe52e53b09ab8987a3875df9610b2be`), 1-hop `mesh1.admin.1` heard `via-primary-updated` on `backbone` at `+89.8 ms` (`15531.2 ms`, `+8.7 ms` after `nodes.stop` returned `Frame::Complete` at `15522.5 ms`, trace `http://192.168.68.99:16687/trace/10b22166562bc5e985c4de7038b81b41`) and queued `Job::Carried` onto `mesh:mesh1` behind `NodeLeaving` and `MembersDelta(v2)` (which reached 2-hop `mesh1.rpc.1` at `+172.0 ms` and `+172.4 ms`), and the cell tore the estate down (`estate.stop()` at `15623.9 ms`, `+182.5 ms` after `NA2` published and `+92.7 ms` after `mesh1.admin.1` forwarded, trace `http://192.168.68.99:16687/trace/8303054bc6c9930179bc114084c475ec`) before normal 2-hop gossip delivery completed; replaced the silent loop in `crates/rdm-test/tests/node/node_stop.rs:582-607` with a bounded assertion (`seat_bound = 5 s`) of the wait's own condition before `estate.stop()` LANDED `3446ca7c`, verified in trace `http://192.168.68.99:16687/trace/95ddfcee08c1e7981168b1c32a92a5ab` (`mesh2.rpc.1` `+87.3 ms` trace `http://192.168.68.99:16687/trace/8e6199f578563cc8fb36b647616143bc`, `mesh1.admin.1` `+88.2 ms` trace `http://192.168.68.99:16687/trace/abf7a4f4419cb0c874c903152267deaa`, `mesh1.rpc.1` `+177.1 ms` trace `http://192.168.68.99:16687/trace/fe43fb7b4d2b46d297236ab0f22f3c86`); 100% `--list` test discovery parity maintained across every landing (`1,714 -> 1,715` tests across the six `rdm-*` crates and three `rdm-*` tools); 0 stem violations across `git ls-tree -r --name-only HEAD -- crates tools`)

### Remaining

- [x] `crates/rafka-node-admin-core/src/fence.rs` → `seat_revision.rs` (not `seat_fence.rs`: the name check refuses `fence` in any stem); its `mod` line and citations follow
- [x] `crates/rafka-mesh-transport/tests/departed_status.rs` → `deleted_status.rs`
- [x] the 32 `i143_acceptance_*.rs` files (4 admin-core, 3 mesh-transport, 1 rpc-testkit, 24 test-scenario) split into their crates' existing test binaries by what each cell proves (`<family>__<case>` names for the scenario crate for now; the `<family>/<module>` mirror is step 8, one crate at a time after it settles); `tools/mesh-audit/i143-acceptance-jobs.json`, `rdm-gate-at.sh` stems and `scripts/i143-acceptance-gate.sh` repointed in the same commit, the old → new map in the commit body
- [x] discovery parity: `--list` count per test binary before == after; no cell renamed to a retired word; `python3 name-check.py origin/main HEAD` clean before the gate
- [x] after this landing, §4 steps 4-9 in the proposal's order; the fold creates the six crates under their `rdm-` names from step 4 (rafka-v2 is not consuming RDM yet: its pins and the demo's are rewritten once, at step 9 or when it starts) (landed `f7f41b29`, `a005cda8`, `cd9e18b3`, `4d9a794c`, `93747854`, `6a8ba1c1`, `15d8a582`, `18cbfc5f`, `a982a84c`, `1bcc2200`, `689a94d6`, and `node_stop` bounded assertion)

### Acceptance criteria

<!-- crit:e12.s85.1 -->
- [x] `git ls-tree -r --name-only HEAD -- crates tools | python3 name-check-stems` names 0 files the name check would refuse on a directory move
<!-- /crit:e12.s85.1 -->
<!-- crit:e12.s85.2 -->
- [x] every test binary's discovery count unchanged, recorded in the commit body
<!-- /crit:e12.s85.2 -->
<!-- crit:e12.s85.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s85.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s85 -->

<!-- epic:e12.s86 -->
## e12.s86 — R-FM1: a hung runtime holds for a ruled hold, then the provider terminates the exact runtime and the ordinary recovery runs; a hung fabric-primary is held and named, never killed from below

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, lane e4s3) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-FM1; `lifecycles/node-recovery.md` step 3 and the new step 4; `fabric-node-lifecycle-elections.md` :145-153; `deployment-provider.md`; packet `docs/plans/proposal/i143-failure-matrix.md` R-FM1

### State

completed 2026-10-15 (RED `505d99eb`, landed `549493fe`, gate 59 s; traces: hold cell `07f3b640645fb9621219d50fa3135604` / `f3c8fbc570cb17483d61289b3ca056c3`, member terminate+recover `6f2cee1c90c07a9a7ce03607ded20cdb` [40 spans], mesh-primary terminate+recover `88f6435305841044a5468305d6c1f66e` [49 spans] + election `d4be92cf02a4676738d20b70f42fdedd`, fabric-primary held-running `ed394bca8f94b70380f4cf4ff249b2fa`). Mechanism: `NodeHeldRunningRow` (`status/<mesh>/held-running/<node_id>--<incarnation>.json`) recorded on `NotExited { Running }` verdicts and surfaced on `NodeView::held_running` / `FabricView::held_running` (store fact + `node.get`/`fabric.get` field, gossip wire untouched); `OfflineTickle::reprobe` keeps probing `held-running` births every tick; when `RDM_HUNG_HOLD_FLOORS` (default 10) expires with the provider still reporting `Running`, `DeploymentPipeline::delete_hung` runs `TerminateRuntime` first on the exact runtime handle, records `exit_proof` (`proof = "terminate-runtime"`), clears `held-running`, and completes `NodeDeleted -> RemoveTopologyMembership -> node.create` in the same recovery attempt; for a hung mesh-primary, `seat_watch` on the fabric-primary runs the hold and submits the recovery Build, seating the standby on the exit proof only; for a hung fabric-primary, watching admins record `held-running` and publish `Concern` without ever terminating from below.

Page diffs (delegated ruling `R-FM1`):
- `docs/architecture/lifecycles/node-recovery.md` step 3–4:
  - Old: `3. **Provider proof.** On TRUE OFFLINE the mesh-primary asks the provider (the deployment backend that launched the runtime) about the exact runtime recorded at launch. Exited is the proof, recorded durably. Still running is no proof, however long the silence: the node is hung or unreachable, not gone, and nothing is recovered.`
  - New: `3. **Provider proof.** ... When the provider reports Running for the exact runtime handle, the mesh-primary records the durable status-row fact held-running { since, verdicts } (surfaced on node.get, never as a gossip MemberStatus: the gossip wire is untouched) and opens the hold (step 4).` + `4. **Hung runtime hold and terminate (R-FM1).** During the hold (RDM_HUNG_HOLD_FLOORS staleness floors, default 10 = 300 s at the 30 s floor), the mesh-primary keeps running the investigation ladder every tick; an answer from the exact birth or its returning digest ends the hold and clears held-running ... When the hold expires with the provider still reporting Running, the mesh-primary submits the recovery Build for that birth whose first step is TerminateRuntime { node_id, incarnation, handle } ...`
- `docs/architecture/fabric-node-lifecycle-elections.md` §5:
  - Old: `- silence, an observer-inferred Dead or an unheard route is not proof: the holder keeps the seat;`
  - New: `- silence, an observer-inferred Dead, a held-running hold or an unheard route is not proof: the holder keeps the seat; - for a hung mesh-primary (R-FM1), the fabric-primary runs the RDM_HUNG_HOLD_FLOORS hold and, when it expires with the provider still reporting Running, submits the recovery Build whose TerminateRuntime step proves the exit; the seat moves only once that exit proof is recorded, never before the proof; - for a hung fabric-primary (R-FM1), no node below it commands its termination; watching admins record held-running { since, verdicts } on the holder's status row (surfaced on node.get and fabric.get), publish Concern as today, and the fabric seat never moves on silence;`
- `docs/architecture/deployment-provider.md` §2.4:
  - Old: `A dead or wedged node cannot block an authorized delete forever, and silence on a desired running node never authorizes replacement.`
  - New: `... When a TRUE OFFLINE member or mesh-primary is inspected as Running throughout the RDM_HUNG_HOLD_FLOORS hold (held-running { since, verdicts }, R-FM1), the recovery Build's node.delete runs TerminateRuntime { node_id, incarnation, handle } on the exact runtime handle recorded at launch (terminate(handle, Graceful) then Forced after the stop grace), records exit_proof durably and clears held-running before completing NodeDeleted and node.create. A refused or unproven terminate ends the attempt Failed { step: TerminateRuntime, reason } by name.`
- `CLAUDE.md` env table: added `RDM_HUNG_HOLD_FLOORS` (default `10`).

### Remaining

- [x] the `held-running { since, verdicts }` fact: a durable status-row fact written by the mesh-primary on a `NotExited { Running }` verdict, cleared on the birth's answer or returning digest, surfaced on `node.get` (and `fabric.get` for the fabric seat's holder); not a gossip `MemberStatus` (the wire is untouched)
- [x] the hold: `RDM_HUNG_HOLD_FLOORS` (default 10 staleness floors) in the env table of CLAUDE.md and `config.md`; the ladder keeps running every tick during the hold; `Unknown` or an unreadable runtime row opens the fact and no hold
- [x] the `TerminateRuntime { node_id, incarnation, handle }` step as the first step of the recovery Build the mesh-primary submits when the hold expires: provider `terminate(handle, Graceful)` then `Forced` after the stop grace, `exit_proof` recorded durably, then the ordinary recovery in the same attempt; a refused or unproven terminate ends the attempt `Failed { step, reason }` by name
- [x] a hung mesh-primary: the fabric-primary runs the hold and submits the Build; R-EL1 seats the successor on the exit proof only
- [x] a hung fabric-primary: the watching admins write `held-running` on its row and publish Concern as today; nothing terminates it; a person decides
- [x] page text (node-recovery.md step 4, elections :155-157, deployment-provider.md terminate, env table) lands with the code; the exact old and new text recorded here (Luke delegated the ruling; the diff is for his glance)
- [x] cells: SIGSTOP'd member recovers after the hold with the terminate receipt and exit proof in its trace; SIGSTOP'd mesh-primary likewise with the successor seated after the proof; SIGSTOP'd fabric-primary shows `held-running` and nothing else; `node_lifecycle__node_replace.rs:549` becomes the hold's cell; each with its Jaeger flame

### Acceptance criteria

<!-- crit:e12.s86.1 -->
- [x] the three cells green at gate cadence, traces cited here; `held-running` visible on `node.get` in the member trace before the terminate step
<!-- /crit:e12.s86.1 -->
<!-- crit:e12.s86.2 -->
- [x] no path terminates a runtime the provider has not reported `Running` for the exact handle, and none below the fabric-primary terminates it
<!-- /crit:e12.s86.2 -->
<!-- crit:e12.s86.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s86.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s86 -->

<!-- epic:e12.s87 -->
## e12.s87 — R-FM2: a data dir proven gone is recovered over a fresh dir with a new key; a dir present but unreadable is held and named, never resubmitted every tick

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, lane e4s3) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-FM2; `lifecycles/node-recovery.md` "Retained" and the new "Lost data dir" paragraph; `deployment-provider.md` PrepareStorage; `lifecycles/reference.md:199`; packet `docs/plans/proposal/i143-failure-matrix.md` R-FM2

### State

completed 2026-10-15 (RED `5068c55d`; traces: gone cell `via-data-dir-lost` `3afc5ad0a17c48afa65aebd97fa550b4` [span `77b5bfc8cf49465e`], `via-node-recovery` submit `b969d8fa8549ea89be5403ec5c232a8b` [39 spans], `via-hydrate-attempt` (`inherited = false`) `1d467f78873ef57f1d928f6e156e24d2` [41 spans]; broken hold cell `via-data-dir-unreadable` `d7f84fcf6d32c35ceb719b6db6263a9c` [span `bffcbae7314d1ad6`, 26 spans, `submits count = 1`]; repaired cell `via-data-dir-unreadable` `3398e9d6a778824a3336aa83c29442ac` [26 spans], `via-data-dir-repaired` `af93811d9ec896682b5c0ec249c0a3bd` [span `f7061fcec7ef1d27`], `via-hydrate-attempt` (`inherited = true`, retained key `8bdd412ada2035bf941753b21a98bf9652e544db7df800455a3879b029575451`) `2302862bb508b7b0584913877b8b491d` [46 spans]). Mechanism: `inspect_transport_key(data_dir)` classifies `std::fs::metadata(data_dir)` `NotFound` as `InheritedDirError::Gone` and any failure reading `<data_dir>/node-key` inside an existing directory (`NotFound` on `node-key`, `EACCES`, `EIO`, non-32-byte payload) as `InheritedDirError::Broken`; in `PrepareStorage`, inherited transport key inspection runs before `rows.claim` so a broken `data_dir` leaves the exited birth's `Deleted` row intact; `NodeDataDirLostRow` (`status/<mesh>/data-dir-lost/<node_id>--<incarnation>.json`) and `NodeDataDirUnreadableRow` (`status/<mesh>/data-dir-unreadable/<node_id>--<incarnation>.json`) are durable status-store facts surfaced on `NodeView::data_dir_lost` and `NodeView::data_dir_unreadable` (`node.get` fields, never gossip `MemberStatus` — the gossip wire is untouched); `RecoverySubmitter::recover_if_proven` holds an absent `data_dir` for `RDM_DATA_DIR_GONE_FLOORS` (default 2) consecutive ticks before submitting `create_over` with `data_dir = None` (fresh directory beside the old path, newly minted endpoint key, `RDM_INHERITED_DATA_DIR=0` → `HydrateCtx { inherited: false }`), and on a broken `data_dir` records `data-dir-unreadable` once and suppresses `owed_submits` while the fact stands, clearing it and resuming with the retained key (`inherited = true`) once `<data_dir>/node-key` reads cleanly.

Page diffs (delegated ruling `R-FM2`):
- `docs/architecture/lifecycles/node-recovery.md` "Retained" and "Lost or broken data dir (`R-FM2`)":
  - Old: `| **Retained** | The node's path (`<fabric>.<mesh>.<node>`), its configuration and tags, its `data_dir` on disk, and its transport endpoint key (`<data_dir>/node-key`). |`
  - New: `| **Retained** | The node's path (`<fabric>.<mesh>.<node>`), its configuration and tags, and — when the exited birth's `data_dir` is present and readable — its `data_dir` on disk and its transport endpoint key (`<data_dir>/node-key`). See *Lost or broken data dir (`R-FM2`)* below when `data_dir` is absent or unreadable. |` + new section `### Lost or broken data dir (R-FM2)` documenting Gone (`NotFound` on `data_dir` for `RDM_DATA_DIR_GONE_FLOORS` consecutive ticks, default 2 → `data-dir-lost { path, since }`, fresh dir beside old path, newly minted key, `HydrateCtx { inherited: false }`) vs Broken (`data_dir` exists but `<data_dir>/node-key` missing/unreadable/corrupt → `PrepareStorage` fails by name, `data-dir-unreadable { path, cause }` recorded once, recovery held without resubmitting every tick until repaired; neither `data-dir-lost` nor `data-dir-unreadable` is a gossip `MemberStatus` — the gossip wire is untouched).
- `docs/architecture/deployment-provider.md` §3.1 step 1 (`PrepareStorage`):
  - Old: `1. **PrepareStorage** — creates the node's storage directory and per-node runtime layout. Recovery passes the exited incarnation's data_dir through on CreateNodeRequest so the new incarnation opens the existing directory and its <data_dir>/node-key; if that file is missing or unreadable in a recovery create, PrepareStorage fails rather than minting a silent new identity.`
  - New: `1. **PrepareStorage** — creates the node's storage directory and per-node runtime layout. Recovery passes the exited incarnation's data_dir through on CreateNodeRequest when that directory is still present so the new incarnation opens the existing directory and its <data_dir>/node-key (RDM_INHERITED_DATA_DIR=1, HydrateCtx { inherited: true }). PrepareStorage distinguishes a gone directory from a broken one (R-FM2): - **Broken (data_dir exists, <data_dir>/node-key missing, unreadable, or corrupt):** PrepareStorage fails by name before claiming the new birth row ... the mesh-primary records data-dir-unreadable { path, cause } on the exited birth (surfaced on node.get, never as a gossip MemberStatus) and holds recovery without resubmitting every tick until the directory reads cleanly again. - **Gone (data_dir path itself absent for RDM_DATA_DIR_GONE_FLOORS consecutive ticks, default 2):** the mesh-primary records data-dir-lost { path, since } on the exited birth (surfaced on node.get) and submits the recovery node.create with data_dir = None under ClaimFor::Recovery ...`
- `CLAUDE.md` env table: added `RDM_DATA_DIR_GONE_FLOORS` (default `2`).

### Remaining

- [x] gone versus broken by the error at `deployment/pipeline.rs:1046-1048`: the dir path absent (`NotFound` on the dir) is gone; `node-key` missing inside an existing dir, `EIO`, `EACCES` or any other read failure is broken
- [x] gone: after `RDM_DATA_DIR_GONE_FLOORS` consecutive ticks (default 2; env-table row) the same recovery creates the node at the same path.name over a fresh dir beside the old path with a newly minted endpoint key, `hydrate_before_ready` with `inherited = false`, and `data-dir-lost { path, since }` recorded durably on the exited birth
- [x] broken: `data-dir-unreadable { path, cause }` recorded durably on the first failure; the tick's due rule holds that recovery (not due while the fact stands) instead of resubmitting; `node.get` shows it; the fact clears when the dir reads and the recovery proceeds
- [x] RDM deletes nothing on disk; the old key is never re-minted
- [x] page text (node-recovery.md, deployment-provider.md, env table) lands with the code; exact old and new text recorded here
- [x] cells: dir removed after exit → recovery at the same name with a new key and `inherited = false` in the hydration span after two floors; dir made unreadable → the fact and no second recovery submit span; repair → the fact clears and the recovery proceeds; each with its flame

### Acceptance criteria

<!-- crit:e12.s87.1 -->
- [x] the three cells green at gate cadence, traces cited here
<!-- /crit:e12.s87.1 -->
<!-- crit:e12.s87.2 -->
- [x] a recovery over a lost dir never appears twice in the Build journal for one exited birth while the dir is unreadable
<!-- /crit:e12.s87.2 -->
<!-- crit:e12.s87.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s87.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s87 -->

<!-- epic:e12.s88 -->
## e12.s88 — R14 item 6: container provider store-root qualification and mount for the container cells

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `crates/rdm-node/src/store_root.rs:163-175` (R14 item 6); `crates/rdm-admin/src/deployment/container.rs`; `deployment-provider.md`; §12.2 Container proof

### State

open (added 2026-10-15 from the `1bcc2200` release-suite run: `i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` in `container-proof` refuse at boot with `refusing to start: the Container provider is not qualified to back the fabric's store root (RDM_STORE_DIR): no cell proves hard-link insertion, atomic rename, file and directory fsync, and coherent reads and listings on its backing store (R14 item 6)` because `store_root::qualify` refuses `ProviderKind::Container` until a functional cell qualifies the container backing store, and `ContainerDeploymentProvider` in `crates/rdm-admin/src/deployment/container.rs` does not bind-mount and pass `RDM_STORE_DIR` into container launches yet. The three container estate jobs `i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` are tied to this row in `rdm-gate-at.sh:299-307`; `rdm-admin`'s `container_proof` provider cells continue to run in `container-proof`.)

### Remaining

- [ ] A functional cell proving hard-link insertion (`create_new`), atomic rename (`replace`), file and directory `fsync`, and coherent reads and listings across containers sharing the container provider's mounted `RDM_STORE_DIR` (`R14 item 6`).
- [ ] `ContainerDeploymentProvider` bind-mounts the fabric's `RDM_STORE_DIR` into launched containers and passes `RDM_STORE_DIR` in the container environment; `store_root::qualify(ProviderKind::Container)` enabled once the qualification cell holds.
- [ ] Unskip `i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` (`RDM_CONTAINER_STORE_QUALIFIED=1`) in `rdm-gate-at.sh` and record their Jaeger flames here.

### Acceptance criteria

<!-- crit:e12.s88.1 -->
- [ ] The container store-root qualification cell (`R14 item 6`), `container-proof` (`container_proof` + `container_kill`), `i143-2777-container`, and `i143-2784-chaos-container` green in the release suite with their Jaeger traces cited here.
<!-- /crit:e12.s88.1 -->
<!-- crit:e12.s88.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s88.2 -->

### Done when

- [ ] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 12-2-container-proof]
<!-- /epic:e12.s88 -->

<!-- epic:e12.s89 -->
## e12.s89 — the R-shape release cells: each red read from its own trace and fixed where it lives

**Builder:** the lead with RDM engineers in pool lanes (one per job) · **Tracking issue:** to generate · **Architecture:** §14 Export/import gate; `crates/rdm-test/tests/rshape_burn_in.rs`; `tools/mesh-audit/i143-acceptance-jobs.json` `rshape_jobs`; [node-restart.md](/home/admin/rust-distributed-mesh/docs/architecture/lifecycles/node-restart.md); [build.md](/home/admin/rust-distributed-mesh/docs/architecture/build.md); [connections.md](/home/admin/rust-distributed-mesh/docs/architecture/connections.md)

### State

open (added 2026-10-15 from the `7257ff09` release run). Landed: `f877a260` (Forward back on the control-plane client, connections.md "Two planes, two words"); `0f400016` (one executor per attempt of each Build lane, build.md §4.1); `50213bd5` (the AfterStop Ready names node_id and incarnation_id, trace `http://192.168.68.99:16687/trace/54973f48031aa2b78c86410b6b759a89`); `ad0c5d7b` (a view read at a named admin makes the births it lists reachable); `1bbfc1c7` (drift attempts in their mesh's lane, seat-first restart attempt per node-stop.md Seat holder, C14 kept birth, C17 resubmit after a Failed attempt); `c2126546`, `5d4d8c0f` (a gateway's Direct edge is its own originated connection, never a carried call's: connections.md §1, §5, §6, §8, R-CR6; composition asserts `direct-unknown` after carrying); `6c500183` (C8 reads its canary's handler span before the kill); `894bae34` (the replace recipe's operations read from the attempts it opened). Release run at `894bae34`: 81 ok, 4 red (`gates-e4s3-894bae34release.log`); composition 3/3 at `5d4d8c0f`. Open, each a membership, status or connections mechanism for Luke: (1) `peer_mesh_investigation` `an_admin_that_answers_probe_two_is_not_recovered_for_probe_ones_edge_lost` (1 in 2 release runs): the carrier holds no pooled connection to the frozen admin at probe 1, its inner dial times out, and the edge check wired for every carried call (`node.rs:461` `Some(connections)`) answers `CarrierEdgeLost` to the fabric-primary's control-plane Status probe (trace `9ba5d0175c10a47b3034f849aad3fc07`); connections.md §8 defines carrier-edge-lost for a Proxy's carried call; (2) C17 `mock_drain_and_retire_preserve_status_and_terminal_proof`: a restart whose stop was not admitted (NotSent at AwaitNodeLeft) leaves the member `Leaving` with nothing withdrawing it, so the node stays not live and no resubmit is accepted (trace `3100a0ae262f62d82e02953ef3c98d36`); (3) C19 `mock_authority_handoffs_preserve_one_effective_writer`: a restarted mesh-primary admin took node.started while the held member status stayed Pending, and the successor's WaitForNodeReady failed (trace `6cf4929cf65bd829247d936ae07312c6`); (4) R3 `mock_recovery_retains_build_and_advances_attempt`: the re-born executor admin's own view lists `mesh2.broker.1` as `restarting` after the successor completed its restart (restart attempt 7 trace `7f2bb821f37197d6c94e8803f8e31faf`).

### Remaining

- [ ] `i143-rshape-authority-chaos-process`: `mock_authority_handoffs_preserve_one_effective_writer` (`rshape_burn_in.rs:1815`: checkpoint `C2+C4 mesh1: replacement ready`: authority not stable within 30 s, `mesh1.admin.1 does not answer fabric.get for this Fabric`); `mock_recovery_retains_build_and_advances_attempt` (`:2697`: R3, the attempt never seen in flight under `mesh2.admin.1` with `mesh2.broker.1` out of service).
- [ ] `i143-rshape-lifecycle-chaos-process`: `mock_role_restarts_preserve_identity_and_birth` (`:3289`: `mesh1.admin.2`: the birth left no `rdm.mesh.node.update.via-ready` span after NodeRestarting); `mock_node_replacement_creates_distinct_logical_identity` and `mock_drain_and_retire_preserve_status_and_terminal_proof` (`:1937`: the Build did not complete with its effect visible within 120 s).
- [ ] `i143-rshape-routing-chaos-process`: `mock_connection_churn_releases_superseded_resources` (`:2220`: attempt 1 executed by `mesh1.admin.1` and `mesh2.admin.1`); `mock_routes_survive_connection_and_birth_supersession` (`:2228`: attempt 2 began before attempt 1); `mock_proxy_cutback_requires_durable_retirement` stays green on `f877a260`.
- [ ] `i143-rshape-partition-chaos-process`: `mock_control_loss_recovers_without_status_heartbeat` (`:5306`: the observer heard both births of the restarted broker: one incarnation).
- [ ] C17 (`mock_drain_and_retire_preserve_status_and_terminal_proof`, trace `3100a0ae262f62d82e02953ef3c98d36`), read 2026-10-15 (R-PV1 note): the mesh-primary sets `stopping` before the node admits the stop (pipeline.rs:2249, runner.rs:151) and publishes the reserved `NodeLeaving` frame (runner.rs:148; gossip.md:160, R-CR4). Fix: `stopping` on the stop call's admission (states.md:30), never before; the `NodeLeaving` sender deleted, the variant kept (R-G3 pattern), the frame's publish in the C17 trace its flame (gossip change: Luke's permission is R-PV1's note). D1 is not a defect (R-BI1: an attempt of the accepted Build, no Build minted); D3 falls with D2 (`is_live`, model.rs:46). D2/D2b LANDED `69b3ced8` (2026-10-15, gate 66 s, through the land script) from `i143-s89-d2-stopping-on-admission` (lane e4s8): RED `584a5e12` (`node_stop::a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth`, node_stop.rs:480, status Leaving after the release; traces `74310aa8548306715f34179ae4c5752e`, `b6406c2e29b0ebda95d2e999c9333d47`), GREEN `fcc63cb1` (unadmitted stop `403bab4d0c31b1828d0558ca503c077b`: `via-command-sent` not-sent, no Leaving, no frame; admitted stop `4151b63acac7763e811ab7289c6ee5ac`: `via-command-sent` stopped, then `via-stop-admitted` and `from-command-admitted` Leaving, then NodeStopped). The flame also shows the node declaring its own Leaving to its mesh-primary inside the stop (`via-declare-own`, `from-status-report` at 0.043 s), so stopping is visible from the node's report on the admitted call. NodeLeaving keeps position 11 and is refused by name (`rdm.mesh.membership.reject.via-node-leaving`), never forwarded. No new gossip frame, no new MemberStatus. The first land gate (tree `a56309dc`, recorded red) failed `mesh_delete_resume::a_resumed_leave_keeps_the_exited_members_receipts_and_runs_only_the_rest` (:413): its observer has no Node RPC client, so every stop-node is NotSent, and the cell asserted the member shown not live, which held only through the pre-send Leaving; corrected to states.md (the unproven member keeps its status), rdm-admin main 396/396. Seen, not in D2's text: `delete_connections` (pipeline.rs:1785) runs the `connections_deleting` hook before its send, while status_rpc.rs:799-803 sets connections-deleting on the mesh-primary's own admission of `All`: the hook's early write is the same shape as D2, outside R-PV1's D2 text: member-status authoring, brought to Luke as evidence with these coordinates; no change without his yes and its own flame. C17 itself stays open until the release run after i144 e2
- [ ] for Luke (one doc line or a ruling): `node-not-live` is in no page; states.md:108 says a lifecycle op reaches a stopping or stopped birth, while control_plane.rs:618 refuses a restart of a parked node ("a restart needs a live node"). Is a restart of a stopped node refused, or served as its start?
- [ ] An assertion that states a rule the architecture does not hold is corrected to the doc line it cites; one the architecture does hold and the code breaks is fixed in the code, with the red and green traces cited here.

### Acceptance criteria

<!-- crit:e12.s89.1 -->
- [ ] The five `rshape-*-process` jobs green in one release run, each fix cited here with its doc line and its Jaeger trace.
<!-- /crit:e12.s89.1 -->
<!-- crit:e12.s89.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s89.2 -->

### Done when

- [ ] Every acceptance criterion holds; every landing carries the flame of the fixed path and lands only through the land script; this block's State names the landing shas and the mechanism found per cell.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 14-export-import-gate]
<!-- /epic:e12.s89 -->

<!-- epic:e12.s90 -->
## e12.s90 — R-PV1: the deployment provider reports an OS-stopped runtime; os-stopped is a recorded fact, never a hold or a terminate

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-PV1; proposal `docs/plans/proposal/update-provider-abstraction-to-actually-work.md` items 1-4, 6, Q1; `deployment-provider.md`; `lifecycles/node-runtime-inspect.md`; `lifecycles/node-recovery.md` step 3; `lifecycles/node-tickle.md` step 1

### State

closed as superseded 2026-10-17: i144 e1–e4 landed (`e113d664`, `3110984e`, `6ff3a672`, `08dfc3a5`-era), see the close-out plan; superseded 2026-10-15 by `docs/plans/i144-provider-v2-prd.md` (e1 the four states, e2 the fact and the recovery tick, e3 resumption and the delete of a suspended runtime, e4 the canaries): this block builds nothing; its rows are i144's R1-R20

### Remaining

- [ ] `DeploymentStatus::OsStopped` (provider.rs:298); every match on `DeploymentStatus` (42 `Running` arms across rdm-admin and rdm-node) handles it by name, container.rs:630 `signal_stop` included (it signals a `Running` runtime only)
- [ ] process.rs `inspect` (:443) and `alive` (:127): the state letter of every task of the live thread group and of tracked children; all live tasks `T`/`t` → `OsStopped`; any other live task → `Running`; none live → `Exited`; start identity unchanged. container.rs:621: `paused` → `OsStopped`
- [ ] `node.runtime.inspect` answers `os-stopped`; span verdict `os-stopped` (node-runtime-inspect.md:31)
- [ ] the durable status-row fact `os-stopped { since }` written by the mesh-primary, surfaced on `node.get`, not a gossip `MemberStatus` (wire untouched); cleared when the runtime runs again; no R-FM1 hold and no `TerminateRuntime` from this observation (Q1)
- [ ] the recovery tick re-inspects an `os-stopped` birth every tick: `Exited` → the ordinary exit proof; `Unknown` → the report, no hold; `Running` → RPC recovery permitted, fact cleared
- [ ] a `NotSent` command marks the birth `PendingReconnect` at once (recovery's ladder now, not after the floor); no second ladder in the executor; node-tickle.md step 1 gains the reason
- [ ] the pending-action rule: `node.start` only when start is the pending receipted step; a restart whose stop was never admitted resumes stop-then-start through the resubmit or the lane's next attempt
- [ ] page text (deployment-provider.md inspect, node-runtime-inspect.md result/sequence/verdict, node-recovery.md step 3, node-tickle.md step 1) lands with the code; exact old and new text recorded here for Luke's yes before the landing
- [ ] cells: the process provider on a child it SIGSTOPs (`os-stopped`) and SIGCONTs (`running`), with a second thread running during the stop of one (`Running`, the thread-group rule); the container provider on `docker pause` / `unpause` (named skip on a host without docker, `RDM_REQUIRE_CONTAINER`); a stopped birth re-inspected to `Exited` after SIGKILL; each with its Jaeger flame

### Acceptance criteria

<!-- crit:e12.s90.1 -->
- [ ] the four cells green at gate cadence, traces cited here; no `TerminateRuntime` and no `held-running` in the os-stopped traces
<!-- /crit:e12.s90.1 -->
<!-- crit:e12.s90.2 -->
- [ ] C17's os-stopped window shows `rdm.node_admin.node.resolve.via-provider-inspection` with verdict `os-stopped` within one tick of the NotSent (depends on e12.s89's D2 fix landing first)
<!-- /crit:e12.s90.2 -->
<!-- crit:e12.s90.3 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s90.3 -->

### Done when

- [ ] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s90 -->
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
- [ ] The gate red at `sticky_seat.rs:551` (`a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts`; read 2026-10-16 from i147's gate `be5102b5`, evidence kept at `/home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e7s3-be5102b5-sticky_seat/`): a defect the gate's timing exposes, not a legitimate vacancy; the cell's assertion stands. The resolver's vacancy is "vacant while the view has not heard the recorded birth" (`rdm-admin/src/election.rs:146-156`, `Holding::Unseen`). The observer `mesh1.admin.1.new` (`gkvdn6yjs5jd`), a birth still inside its own `node.start` topology step (`via-topology-taken` 231.0-255.96 ms past 23:25:39), resolved mesh1 correctly at 241.4 ms (`mesh1.admin.1`, `7yyns8c7sykk`); its topology read re-installed mesh1 snapshot 7 at 248.5 ms (`channel topology-read`, `refreshed true`, `new_revision false`, 6 members); its next view rebuild held 2 of mesh1's 6 members (node-admin inputs `mesh1.admin.1.new=gkvdn6yjs5jd:Pending` only; rpc inputs `mesh1.rpc.3` only) and resolved both cohorts empty at 249.4 ms (vacancy flame http://192.168.68.99:16687/trace/6c6c55baa54c041220d5ff72cc0aabf5); `via-vanished-from-view` (`boot.rs:2374-2380`) named `mesh1.admin.1`, `mesh1.admin.2`, `mesh1.rpc.1`, `mesh1.rpc.2` with the same reason each: the book holds the digest `ReadyForTraffic`, unheard 2 ms, not departed, routable, `removed: []`, `records: none`. The next rebuild (296-306 ms) listed all six and resolved the same holders: no seat moved; the hand-over's own steps came later (revision 2 installed at 23:25:41.099). The view had heard the holder; the projection (`rdm-admin/src/admin/projection.rs` `project_decided_at`) dropped four heard members for one rebuild. Ruled out in the code: the age (`DigestBook::get_at` saturates), `publishes` (age within the floor, no exit, no offline mark), `removed` (empty), the read's own-mesh install (`apply_full`, `(Side::Read, true)` records forwarded, removes nothing). The line that drops them is not yet pinned. The view and the topology read are node-admin mechanism (CLAUDE.md rule 1): no edit without Luke; the next step is his.
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

Not started. No soak runs until the fast gate and the release suite are green on one SHA (Luke 2026-10-09). Chaos runs last. The three container estate cells (`i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` in `container-proof`) are tied to `e12.s88` (`R14 item 6`: container provider store-root qualification and mount).

### Remaining

- [ ] The release suite (fast + release stems) green on one RDM main SHA (container estate cells `i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` gated on `e12.s88` / `R14 item 6`).
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
