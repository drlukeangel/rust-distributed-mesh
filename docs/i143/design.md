# i143 — the generic Mesh product in RDM: working design

Plan: rafka-v2 `docs/plans/i143-node-rpc-pos-on-RDM.md`. Architecture of record lives in rafka-v2 `docs/architecture/{node-rpc-rdm-ownership,mesh-control-plane,node-lifecycle-elections,gossip,node-rpc,connections}.md`.

This document states the concrete RDM contracts the stories build to: binaries, control state, membership/runtime facts, the control API, views, probes, evidence and span names. **Where this document differs from the rafka-v2 PRD/architecture, the rafka-v2 design-of-record wins and this file must be corrected in the same story.**

## 1. Packages and binaries

| package | owns | binary |
|---|---|---|
| `rafka-node-rpc-contract` | framing, `RpcOutcome`, reply classes, reserved codes, catalog/seal, certainty rules; no Iroh | — |
| `rafka-mesh-entity` | Mesh EF: logical IDs, runtime incarnation, RuntimeFact + current runtime metadata, endpoint slots/freshness, membership, imported connections model | — |
| `rafka-mesh-transport` | Iroh endpoint/gossip integration, explicit addresses, membership channels, held-member repair | — |
| `rafka-node-rpc` | server/client runtime, commit cut, admission, slot-aware pool, streaming, one-hop carried execution | — |
| `rafka-node-admin-core` | Fabric record/accepted-Build hydration, fabric/mesh/nodes/connections/builds storage, Build/Reconciler, deployment providers/pipelines, lifecycle transitions, leadership, recovery, HTTP | `rafka-node-admin` |
| `rafka-node-admin-client` | typed DTOs + HTTP client for control API | — |
| `rafka-node-rpc-testkit` | Echo + stateful proof protocol/store, probes | `rafka-rpc-node`, `rafka-rpc-probe` |
| `rafka-test-scenario` | scenario runner, evidence/replay manifests, e2e canaries | `rafka-scenario` |
| `rafka-chaos` | generator/shrinker, failpoints, simulated network/time, real fault backends | — |

Proof shapes use exactly two node kinds: `node_admin` and `rpc_node`. Legacy role binaries are not proof-shape nodes.

## 2. Names and identities

| fact | form | survives restart | survives replacement |
|---|---|---|---|
| `path.name` | `<mesh>.<kind>.<ordinal>` | yes | yes, points at replacement |
| `node_id` | logical NodeId, canonical Crockford60 | yes | no |
| `mesh_id` | logical MeshId, canonical Crockford60 | yes | recovery keeps it; intentional new mesh gets new id |
| `fabric_id` | logical FabricId, canonical Crockford60 | yes | kept for Fabric lifetime |
| `transport_id` | Iroh EndpointId | yes for same logical restart policy | no |
| `incarnation_id` | one process birth, opaque | no | no |
| endpoint slot | `{slot, addr, freshness}` | per slot policy | no |
| `runtime_fact` | exact provider runtime identity for this NodeId + IncarnationId | no | no |
| current runtime metadata | operational context such as `data_dir` required to manage the birth | per operation/provider policy | no |

Product identities are 12 lowercase Crockford Base32 chars carrying 60 random bits using:

```text
0123456789abcdefghjkmnpqrstvwxyz
```

Only `node_id` has ordering semantics, and only for elections. `mesh_id` and `fabric_id` are equality-only. Product IDs carry no age/time/ordinal semantics.

Freshness, incarnation, deployment, transition, Build, provider-control-domain and provider-runtime identities are not converted to Crockford merely for uniformity.

An `rpc_node` has two proof slots:
- `rpc-0`, fresh: restart gets new port/token;
- `rpc-1`, stable: restart keeps port/token, replacement gets new values.

### 2.0 Address authority

Every canonical Iroh endpoint uses explicit product topology/EndpointAddr plus explicit seeds. No N0 DNS/Pkarr product authority exists. Optional mDNS, where retained, is discovery only and never liveness/death truth.

### 2.1 RuntimeFact, runtime metadata, and provider control domain

Every current managed birth publishes one typed compact runtime fact with semantics equivalent to:

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

The fact is immutable for one incarnation and sufficient to identify **where and how** an eligible node-admin can adopt/inspect/terminate that exact runtime.

In RDM it is `rafka_mesh_entity::RuntimeFact`, carried as `MeshNode::runtime`: the birth's NodeId and IncarnationId are the `MeshNode`'s own, so the fact cannot be detached from its birth. Encoded, it is at most `MAX_FACT_BYTES` (384); a digest carrying the largest one still packs within one backbone frame.

| provider | `provider_control_domain` | `locator` |
|---|---|---|
| process | `process:<boot id>:<pid namespace inode>` | `pid` + `start` (field 22 of `/proc/<pid>/stat`) |
| container | `container:<Docker daemon id>` | the 64-hex immutable container id `docker run -d` answers |

A locator means something only in its control domain. `deployment::provider::adopt` turns a fact into a handle only when its provider and domain are the adopting provider's; otherwise it refuses by name (`other-provider`, `foreign-control-domain`, `invalid-fact`) and nothing is signalled. The process provider never signals a pid whose start token differs (a recycled pid is "exited" for the old birth). Membership refuses a digest of the held incarnation that names another locator or domain (`MembershipRefusal::RuntimeChanged`, `rafka.mesh.runtime.reject.via-locator-changed`); a restart is a new incarnation with a new fact.

The digest also carries the birth's `data_dir`: current operational metadata a successor needs to manage the birth, outside the fact's identity, so every admin's view advertises it.

Provider locator requirements:

```text
process:
    provider_control_domain + pid + process-start identity/token
    PID alone is insufficient

container:
    provider_control_domain + immutable container id
    deterministic container name alone is insufficient

future provider:
    provider_control_domain + equivalent non-reusable locator
```

`provider_control_domain` is opaque equality-only execution metadata. It is not a product identity, EndpointId, or election key.

Current proof domains are intentionally local:

```text
process provider
    one host/process namespace

container provider
    one reachable Docker daemon / host runtime
```

No implementation may assume PID/container ID is globally meaningful. Future multi-host execution must either provide remotely controllable locators or route/delegate exact-runtime work to an executor/agent in the RuntimeFact's provider control domain. Leadership remains NodeId-derived.

RuntimeFact is not a credential, transport identity, election key, or proof of Running/Dead. Provider inspection of the adopted exact locator supplies runtime status.

#### Producer vs publisher

Provider-specific locator acquisition and membership publication are distinct:

```text
provider / runtime bootstrap seam
    -> produces or discovers exact locator + provider_control_domain

node / runtime birth
    -> publishes ONE normalized RuntimeFact through its own membership digest/projection
```

There is no launcher-owned competing authoritative RuntimeFact stream.

Examples:
- process may self-discover PID + process-start token;
- container provider learns immutable container ID after `docker run` and writes/exposes it through the bootstrap record/channel before the child may become Ready;
- Day-0/operator-started admin uses `adopt_current_runtime`/self-discovery to obtain the same normalized identity before Ready.

The final locator cannot generally be required in the original launch env because post-spawn identity such as PID or immutable container ID does not exist before spawn.

#### Runtime identity vs current operational metadata

RuntimeFact remains compact exact identity/fencing state. Operational context required to manage/restart the birth is current Node/runtime projection state:

```text
RuntimeFact
    exact runtime identity / fence

current runtime metadata
    operational context needed by a successor
    e.g. data_dir, resolved storage/executable context where required
```

Required current runtime metadata must converge to every authority-capable admin. It may not remain launcher-private. `data_dir` is not part of RuntimeFact equality/fencing merely because restart needs it.

The local admin `handles` and runtime-metadata maps are caches only. They are not authority because leadership can move to an admin that did not launch the runtime.

A changed locator or provider control domain for the same incarnation is a named inconsistency/refusal, never a silent update.

In RDM:

The seam is a record in the birth's data dir, `<data_dir>/runtime.json`. The provider clears any record an earlier birth left and realises the runtime; the pipeline then commits each prerequisite as its own step, receipt and span, over what `DeployRuntime` obtained (no further provider call):

```text
DeployRuntime                          provider launches (or finds) the runtime
RegisterExactRuntimeHandle             the handle names it exactly (pid + start token / immutable id)
ResolveProviderControlDomain           it lives in this provider's control domain
MakeRuntimeFactAvailableToBirth        the normalized fact is written to runtime.json and read back
WaitForBind
PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata
                                       the node record and data dir are published; the fact
                                       rides the birth's own digest
WaitForMeshJoin                        the birth's own digest carries exactly that fact and data dir
WaitForNodeReady                       begins only when the Build state holds a Complete receipt
                                       for each of the four steps above (READY_PREREQUISITES)
```

The runtime waits for the record, refuses one that does not describe itself (a process: its own pid, start token and domain; a container: an id its hostname begins), and publishes it with its birth. The provider produces the locator; the runtime publishes the fact; no launcher gossips a competing one. Receipts carry fingerprints, never a raw pid, container id or domain.

Day-0/operator-started admin uses `adopt_current_runtime` or equivalent, registers the same exact runtime identity and publishes RuntimeFact before authority-bearing Ready. Here: `CurrentRuntimeAdoption` runs `AdoptCurrentRuntime` (`RuntimeFact::of_this_process` under a minted deployment id), `RegisterExactRuntimeHandle`, `ResolveProviderControlDomain` and `PublishRuntimeFactAndCurrentRuntimeMetadata` (the fact and data dir into its own digest), each under a `via-step` span of a `via-pipeline` span with `pipeline = adopt-current`, and keeps their receipts in `<data_dir>/runtime-adoption.json`. Its Ready gate holds it Pending until that file holds a Complete receipt for each.

A successor adopts a birth it did not launch from that birth's current digest (`AdminRunner::handle_for`); the `handles` map caches the result. No completed Build's `AllocateIdentity`/`DeployRuntime` receipt is read, and the Build topic still hands a new neighbour active Builds only.

Authority-capable Ready, runtime part: a node-admin publishes `Pending` until every live birth it holds (but itself) publishes a fact its provider adopts in its own control domain (`authority_blockers`); only then does it commit `ReadyForTraffic` and become eligible for any seat. While blocked it reports `rafka.node_admin.runtime.reject.via-not-authority-capable` naming each blocking birth. The desired-topology part of this gate is #2851's.

### 2.2 Accepted topology, Build state, and storage

Three current fact classes remain separate:

```text
Fabric.build_id -> complete Build
    = what Fabric/Mesh shape has been accepted

membership + RuntimeFacts + current runtime metadata
    = what exact current births/runtimes exist and how they can be managed

Build facts
    = how attempts of the accepted Build are executing
```

There is no independent `DesiredTopologyProjection`, desired revision, lineage, fork winner or `source_build_id` authority.

Only the current fabric-primary accepts topology changes. A non-primary topology route refuses by name with the current fabric-primary.

Crash-safe acceptance is:

```text
compile complete Build B from current accepted topology + requested mutation
  -> persist complete B through builds.storage
  -> persist Fabric.build_id = B through fabric.storage
  -> B is accepted
```

The pointer is never written first.

RDM storage boundaries are:

```text
fabric.storage
mesh.storage
nodes.storage
connections.storage
builds.storage
```

RDM owns each interface plus memory and durable file implementations used by restart/chaos proof. `builds.storage` is the existing `BuildStateAdapter` contract with its memory/FileJournal implementations adopted under that name.

The minimum Fabric record is:

```text
FabricRecord
  id
  name
  build_id

FabricShutdown?
```

Every admin persists the current Fabric record it learns. Those copies are durable state, not writer authority.

Hydration is direct:

```text
first connection / entry pull
    -> current FabricRecord
    -> complete Build named by Fabric.build_id

admin reconnect / control catch-up
    -> latest FabricRecord + named Build

Build-topic catch-up
    -> active Build execution facts only
```

Older Build history is never replayed to reconstruct current topology.

Topology-changing routes compile the current Build's complete topology plus their mutation into a new complete Build. `RestartNode` and same-path replacement do not change topology and therefore do not mint a Build.

A second topology-changing request while the current Build is actively reconciling refuses:

```text
409 build-in-progress
current_build_id = B
```

There is no Build supersession/cancellation state machine.

Build-id semantics:

```text
accepted topology change
    -> new complete Build B
    -> Fabric.build_id = B

active Build B + authority/executor movement
    -> same Build B
    -> next attempt as required

completed Build B + later proven runtime drift
    -> next attempt of B
    -> same Fabric.build_id
    -> reason = proven-drift
```

Attempts/receipts append to B. There is no `reconcile_build_id`.

`DELETE /api/builds?id=B` refuses while `Fabric.build_id == B`.

Drift checks compare the complete accepted Build with current births. A birth that the view no longer hears remains held unless the provider inspected its exact published runtime and found it terminal. Proven shortfall opens another attempt of the same Build. A fabric shutdown freezes reconciliation before any runtime stop.

### 2.3 Canonical elections and authority-capable Ready### 2.3 Canonical elections and authority-capable Ready

A cohort is one kind's members in one mesh.

```text
eligible = ReadyForTraffic candidates
winner   = lowest complete canonical NodeId
```

Do **not** use:

```text
ready_since_ms
ordinal/path
mesh name
MeshId/FabricId
creation time
incarnation/deployment
RuntimeFact/provider locator/provider_control_domain
current runtime metadata
Fabric.build_id
EndpointId/freshness
incumbency
```

Ownership:

```text
node-admin observers own (mesh, kind) cohort resolution
(mesh, node_admin) winner == mesh primary
only mesh primaries own fabric-primary resolution
fabric candidates = current eligible mesh-primary nodes
same lowest-NodeId comparator
```

Day 0 naturally yields:

```text
node-admin cohort primary == mesh primary == fabric primary
```

No special primary assignment exists.

Restart preserves NodeId, so a restarted lower NodeId may retake a seat once Ready. Permanent logical replacement mints a new NodeId and may or may not win.

A node-admin may not commit `ReadyForTraffic` until it has hydrated:

```text
current membership/topology
current FabricRecord + complete accepted Build
current RuntimeFacts + required current runtime metadata for held births it may have to manage
provider-control-domain reachability or a valid domain-local execution route
```

This hydration is a readiness prerequisite, not an election score.

#### Ready root / deadlock guard

An ordinary joining/restarted admin does not self-Ready merely because current-control hydration is unavailable.

Only two lifecycle roots may establish Ready without an already-live Ready authority:

```text
Day 0
    no prior Fabric authority exists
    bootstrap first current control projection
    commit first Ready through the Day-0 lifecycle path

single-admin recovery root
    only when canonical lifecycle proof says no live Ready admin exists
    use the defined fenced recovery-root Ready self-apply
```

These paths grant lifecycle eligibility only. Normal lowest-NodeId election assigns leadership afterward. Cross-mesh recovery with a live fabric primary does not use the self-root shortcut.

Election evidence:

```text
rafka.mesh.election.resolve.via-recompute
rafka.mesh.election.resolve.via-mesh-primary
rafka.mesh.election.resolve.via-fabric-recompute
```

The parked first s14 branch (`i143-e4-s14`, commit `243ff4a`) produced the canonical RED after a lower-NodeId authority won and could not adopt an older live birth. The GREEN s14 rebase must prove that case succeeds through current RuntimeFact/runtime-metadata plus `Fabric.build_id` + accepted-Build hydration. #47 supersedes #2851's independent desired-revision model.

### 2.4 Build execution and recovery

Build authority and local executor are distinct.

Normal mesh creation/recovery:

```text
fabric authority owns desired update/recovery authority
  -> birth ONE bootstrap/recovery node-admin with exact FabricId + MeshId
  -> provider/bootstrap obtains exact runtime locator + control domain
  -> node publishes RuntimeFact + required current runtime metadata
  -> hydrate current FabricRecord + accepted Build + RuntimeFacts/runtime metadata + provider-domain capability
  -> ApplyMeshState(Pending)
  <- Applied | AlreadyApplied
  -> run/hand the current accepted Build for the target Mesh
  -> Pending admin executes only its own mesh
       preserve held current births
       adopt/manage them through RuntimeFacts/runtime metadata
       route nonlocal exact-runtime work through valid provider-domain execution
       retire only canonically proven dead/superseded births
       create missing ordinary members
       create missing sibling admins
  -> Ready
  -> canonical mesh election
  -> canonical fabric election recompute
```

A Pending bootstrap admin does not become elected primary by executing Build.

If failed mesh held fabric authority, surviving mesh primaries elect current fabric primary **first**.

If an active Build already exists, authority movement keeps the same Build id. If the current Build is complete and later exact-runtime drift appears without a topology change, the fabric-primary opens the next attempt of that same Build.

Runtime death proof:

```text
provider inspect(adopted exact runtime) -> terminal/Exited
```

Not death proof:

```text
RPC failure
NoActiveRoute
gossip silence
failed held-member rejoin
provider-domain mismatch / local inability to control a foreign locator
partition
```

Partition healing uses held current member + known EndpointId/address + stale coverage -> bounded `join_peers`/refeed. Zero neighbours remains fallback only. Relearned current birth restores RuntimeFact + required runtime metadata.

### 2.5 Fabric shutdown

Fabric shutdown is Fabric control state, never a Build or topology change. It leaves `Fabric.build_id` untouched.

```text
/api/shutdown at non-primary
    -> RejectedNotAuthority(current fabric-primary)

/api/shutdown at fabric-primary
    -> 202
    -> persist FabricShutdown through fabric.storage
    -> disseminate it on the Fabric control channel

every admin that receives/hydrates FabricShutdown
    -> persist own copy
    -> stop Build reconciliation, drift recovery and rebirth
    -> gossip Draining

all current live admin births Draining
    -> freeze barrier satisfied

each mesh-primary
    -> stop ordinary members through exact RuntimeFacts
    -> stop non-primary admins through exact RuntimeFacts

fabric-primary
    -> drains its own Mesh by the same rule
    -> waits until every Mesh has no live runtime except its mesh-primary
    -> stops other mesh-primaries through exact RuntimeFacts
    -> stops itself last
```

A runtime merely in `Leaving` is still live for the drain barrier. The runtime must be gone/terminal before its mesh-primary is stopped.

An uncontrollable or still-live runtime is named in the shutdown incomplete list and in `rafka.node_admin.fabric.update.via-shutdown-incomplete`; shutdown is not reported complete. `GET /api/fabric` exposes `shutdown.phase = frozen|draining|spine` and exact incomplete runtime + reason while the primary remains alive.

Every admin persists active FabricShutdown, so a restart of every admin on the same data dirs comes back frozen. FabricShutdown is never deleted while its Fabric lives. Local OS/admin stop remains local.

## 3. Process contract

Core bootstrap environment remains provider/input policy, not a runtime-history channel:

| var | read by | meaning |
|---|---|---|
| `MESH_SPAWN_TYPE` | first node-admin | `process` or `container`; establishes Fabric provider policy |
| `RAFKA_FABRIC` | node-admin bootstrap | Fabric display/name input |
| `RAFKA_FABRIC_ID` | bootstrap/joining nodes | logical FabricId |
| `RAFKA_MESH` | node-admin | mesh name |
| `RAFKA_DATA_DIR` | every binary | current runtime/control/evidence data dir; must project to successor admins where management requires it |
| `RAFKA_NODE_ADMIN_API_BIND` | node-admin | HTTP bind |
| `RAFKA_BIN_DIR` | node-admin | binary directory |
| `RAFKA_EVIDENCE_DIR` | every binary | JSONL span output root |
| `TRACEPARENT` | spawned binary | W3C parent of boot span |

Do not add "final DeploymentHandle" as a required launch env value. Provider/bootstrap may use a post-spawn record/channel to expose the exact locator to the runtime before Ready.

A serving node-admin prints one `RAFKA_NODE_ADMIN_API_BASE=<url>` line and writes the same value in its data dir.

## 4. Control API

Every topology mutation returns `202 {"build_id":"..."}` and delegates to desired-state + Build control.

```text
POST   /api/build
GET    /api/builds?id=<build_id>
DELETE /api/builds?id=<build_id>   # history admin; refuses the Build Fabric.build_id names
POST   /api/nodes/spawn
DELETE /api/nodes/<path.name>
POST   /api/nodes/<path.name>/restart
GET    /api/nodes
GET    /api/meshes/<id|name>
GET    /api/fabric
POST   /api/meshes
DELETE /api/meshes/<id|name>
POST   /api/shutdown               # runtime administration, not Build
```

Public views expose logical IDs, current status, owning admin endpoint, incarnation/endpoints and enough runtime-control evidence for blackbox proof without exposing secrets.

Conceptual `NodeView` includes:

```json
{
  "name": "mesh1.rpc.2",
  "kind": "rpc_node",
  "mesh": "mesh1",
  "node_id": "<crockford60>",
  "transport_id": "<iroh-endpoint-id>",
  "incarnation_id": "...",
  "deployment_id": "...",
  "provider": "process",
  "provider_control_domain_fingerprint": "...",
  "runtime_locator_kind": "process",
  "runtime_locator_fingerprint": "...",
  "data_dir": "...",
  "status": "pending|ready-for-traffic|draining|leaving|dead",
  "is_primary": false,
  "is_fabric_primary": false,
  "admin_api_base": null,
  "endpoints": [{"slot":"rpc-0","addr":"127.0.0.1:41001","freshness":"..."}]
}
```

Raw sensitive provider credentials are never exposed. Runtime locator/control-domain values may be represented by safe fingerprints in public/evidence views if policy forbids raw disclosure. Operational metadata such as `data_dir` is not a runtime-identity key.

Fabric/Mesh views carry logical IDs, the current `build_id` where appropriate, shutdown progress where active, and the live owning admin endpoint. `/api/fabric` does not store or expose a second desired-topology record.

## 5. Probe

```text
rafka-rpc-probe --admin <api_base> <op> --target exact:<node_id>|path:<path.name> --key <u64>
               [--value <s>] [--expected <s>] [--pin <slot>=<freshness>] [--cut-before-finish]
op = put | get | delete | cas | echo
```

Output remains one JSON line with `Reply|NotSent|Unserved|Indeterminate`, executing node/mesh, incarnation, slot/freshness and domain result.

Pinned superseded slot is pre-commit `NotSent`; `--cut-before-finish` proves the 499 pre-commit cut.

## 6. Evidence

E2E evidence must be sufficient to prove the late-authority and hydration cases without private maps.

Artifacts include at least:

```text
manifest.json
FabricRecord before/after including build_id
accepted-Build bootstrap/reconnect hydration ledger
Build request/status/facts for active Build only
Node/Mesh/Fabric public snapshots
membership RuntimeFact snapshot/fingerprints
current runtime-metadata snapshot/digest
runtime locator production/publication/adopt ledger
provider-domain resolution/execution ledger
runtime inspect/exit ledger
Ready/root lifecycle receipt ledger
rpc-ledger.jsonl
partition/repair ledger
spans/*.jsonl
trace-url.txt
```

Causality is asserted by `parent_span_id`, not timestamp enclosure.

Mandatory late-authority proof:

```text
Build A converges
A history is complete/forgotten
birth lower-NodeId admin
admin hydrates Fabric.build_id + accepted Build + current RuntimeFacts/runtime metadata + provider-domain capability
admin Ready -> wins
admin adopts/controls a runtime launched before it existed
later proven drift -> next attempt of the same Build B
```

Mandatory Ready-root proof:

```text
ordinary joining admin lacks current desired/runtime hydration
    -> remains non-Ready
restore catch-up
    -> hydrates -> Ready -> election

Day 0 / fenced single-admin recovery root
    -> may establish Ready without prior Ready authority
    -> election still determines seat
```

Elastic shape evidence asserts settled per-cohort counts where surviving ordinals may legally have gaps. Identity-specific tests still assert exact path/NodeId/IncarnationId semantics.

## 7. Span names

Five segments, `rafka.<component>.<entity>.<action>.<reason>`.

Existing canonical spans remain, including:

| span | purpose |
|---|---|
| `rafka.node_admin.build.create.via-rest` | Build accepted |
| `rafka.node_admin.build.update.via-reconcile` | desired - observed reconciliation |
| `rafka.node_admin.deployment.update.via-pipeline` | provider pipeline |
| `rafka.node_admin.deployment.update.via-step` | pipeline step |
| `rafka.mesh.node.create.via-deployment` | process boot |
| `rafka.mesh.election.resolve.via-recompute` | node-type election |
| `rafka.mesh.election.resolve.via-mesh-primary` | node-admin winner -> mesh-primary projection |
| `rafka.mesh.election.resolve.via-fabric-recompute` | fabric election |
| `rafka.node_rpc.request.serve.via-direct` | direct invocation |
| `rafka.node_rpc.request.reject.via-unserved-tag` / `via-malformed` / `via-frame-not-sent` | refusals |
| `rafka.node_rpc.connection.evict.via-slot-superseded` / `via-incarnation-superseded` / `via-timeout-strikes` | pool eviction |

New implementation should add evidence spans for:

```text
FabricRecord/build_id accept/first-hydration/reconnect-hydration
current runtime-metadata hydration
provider-control-domain resolution/delegated execution
Ready root/refusal where applicable
proven-drift reconciliation trigger
held-member stale-coverage rejoin
```

RuntimeFact publish/learn/adopt/refuse:

| span | purpose |
|---|---|
| `rafka.mesh.node.update.via-ready` | publish: the birth's runtime fact (`deployment_id`, `provider`, `provider_control_domain_fingerprint`, `runtime_locator_kind`, `runtime_locator_fingerprint`, `source = self-published-membership`); a node-admin's also `runtime_facts_held` |
| `rafka.node_admin.runtime.update.via-adopt` | a successor adopted a runtime it did not launch, from membership |
| `rafka.node_admin.runtime.reject.via-unpublished` / `via-other-provider` / `via-foreign-control-domain` / `via-invalid-fact` | a fact not adopted, by name |
| `rafka.node_admin.runtime.reject.via-not-authority-capable` | an admin holds Pending: a held birth's runtime cannot be managed from here |
| `rafka.mesh.runtime.reject.via-locator-changed` | membership refused a birth offering another runtime |

Fingerprints are the first 12 hex of blake3; pids, container ids and domains are not emitted raw.

Exact names follow repository span grammar, and raw provider credentials must never enter telemetry.
