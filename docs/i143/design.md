# i143 — the generic Mesh product in RDM: working design

Plan: rafka-v2 `docs/plans/i143-node-rpc-pos-on-RDM.md`. Architecture of record lives in rafka-v2 `docs/architecture/{node-rpc-rdm-ownership,mesh-control-plane,node-lifecycle-elections,gossip,node-rpc,connections}.md`.

This document states the concrete RDM contracts the stories build to: binaries, control state, membership/runtime facts, the control API, views, probes, evidence and span names. **Where this document differs from the rafka-v2 PRD/architecture, the rafka-v2 design-of-record wins and this file must be corrected in the same story.**

## 1. Packages and binaries

| package | owns | binary |
|---|---|---|
| `rafka-node-rpc-contract` | framing, `RpcOutcome`, reply classes, reserved codes, catalog/seal, certainty rules; no Iroh | — |
| `rafka-mesh-entity` | Mesh EF: logical IDs, runtime incarnation, RuntimeFact, endpoint slots/freshness, membership, imported connections model | — |
| `rafka-mesh-transport` | Iroh endpoint/gossip integration, explicit addresses, membership channels, held-member repair | — |
| `rafka-node-rpc` | server/client runtime, commit cut, admission, slot-aware pool, streaming, one-hop carried execution | — |
| `rafka-node-admin-core` | DesiredTopologyProjection, Build/Reconciler, BuildStateAdapter, deployment providers/pipelines, lifecycle transitions, leadership, recovery, HTTP | `rafka-node-admin` |
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

Product identities are 12 lowercase Crockford Base32 chars carrying 60 random bits using:

```text
0123456789abcdefghjkmnpqrstvwxyz
```

Only `node_id` has ordering semantics, and only for elections. `mesh_id` and `fabric_id` are equality-only. Product IDs carry no age/time/ordinal semantics.

Freshness, incarnation, deployment, transition, Build and provider-runtime identities are not converted to Crockford merely for uniformity.

An `rpc_node` has two proof slots:
- `rpc-0`, fresh: restart gets new port/token;
- `rpc-1`, stable: restart keeps port/token, replacement gets new values.

### 2.0 Address authority

Every canonical Iroh endpoint uses explicit product topology/EndpointAddr plus explicit seeds. No N0 DNS/Pkarr product authority exists. Optional mDNS, where retained, is discovery only and never liveness/death truth.

### 2.1 RuntimeFact

Every current birth publishes one typed compact runtime fact with semantics equivalent to:

```text
RuntimeFact {
    node_id
    incarnation_id
    deployment_id
    provider
    locator
}
```

The fact is immutable for one incarnation and sufficient for any eligible node-admin under the established Fabric provider policy to adopt/inspect/terminate that exact runtime.

Provider locator requirements:

```text
process:
    pid + process-start identity/token
    PID alone is insufficient

container:
    immutable container id
    deterministic container name alone is insufficient

future provider:
    equivalent non-reusable locator
```

RuntimeFact is not a credential, transport identity, election key, or proof of Running/Dead. Provider inspection of the adopted exact locator supplies runtime status.

The local admin `handles` map is a cache only. It is not authority because leadership can move to an admin that did not launch the runtime.

A managed process cannot receive its final PID in the original launch env because the PID exists only after spawn. The deployment provider/pipeline therefore owns the bootstrap/adoption seam that obtains/persists the final locator and makes the matching RuntimeFact available to the runtime before Ready.

Day-0/operator-started admin uses `adopt_current_runtime` or equivalent, registers the same exact runtime identity and publishes RuntimeFact before authority-bearing Ready.

### 2.2 Desired topology and Build state

Keep three separate facts:

```text
DesiredTopologyProjection
    = what Fabric/Mesh shape should exist

membership + RuntimeFacts
    = what exact current births/runtimes exist

Build facts
    = how one reconciliation attempt is executing
```

The current desired projection is bounded Fabric control state with a `desired_revision` (or equivalent fencing token). It survives Build completion and Build-history forget.

Topology-changing control requests update desired state under expected/current revision or equivalent insert-and-fail semantics. Silent conflicting last-write-wins is forbidden.

Build-id rules:

```text
active Build B + authority/executor movement
    -> continue B
    -> same desired revision

completed Build A + later proven runtime drift
    -> new Build B
    -> unchanged current desired revision
```

A failed RPC/dial/partition is not proven drift requiring replacement.

### 2.3 Canonical elections

A cohort is one kind's members in one mesh.

```text
eligible = committed ReadyForTraffic candidates
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
RuntimeFact/provider locator
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
current DesiredTopologyProjection
current RuntimeFacts for held births it may have to manage
required lifecycle/status provenance
```

This hydration is a readiness prerequisite, not an election score.

Election evidence:

```text
rafka.mesh.election.resolve.via-recompute
rafka.mesh.election.resolve.via-mesh-primary
rafka.mesh.election.resolve.via-fabric-recompute
```

### 2.4 Build execution and recovery

Build authority and local executor are distinct.

Normal mesh creation/recovery:

```text
fabric authority owns desired update/recovery authority
  -> birth ONE bootstrap/recovery node-admin with exact FabricId + MeshId
  -> register/publish RuntimeFact
  -> hydrate current topology + desired state + RuntimeFacts
  -> ApplyMeshState(Pending)
  <- Applied | AlreadyApplied
  -> run/hand ReconcileMesh for current desired revision
  -> Pending admin executes only its own mesh
       preserve held current births
       adopt them through RuntimeFacts
       retire only canonically proven dead/superseded births
       create missing ordinary members
       create missing sibling admins
  -> Ready
  -> canonical mesh election
  -> canonical fabric election recompute
```

A Pending bootstrap admin does not become elected primary by executing Build.

If failed mesh held fabric authority, surviving mesh primaries elect current fabric primary **first**.

If an active Build already exists, authority movement keeps the same Build id. If prior Build is complete/forgotten and later exact-runtime drift appears, DesiredTopologyProjection drives a new reconciliation Build.

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
partition
```

Partition healing uses held current member + known EndpointId/address + stale coverage -> bounded `join_peers`/refeed. Zero neighbours remains fallback only.

## 3. Process contract

Core bootstrap environment remains provider/input policy, not a runtime-history channel:

| var | read by | meaning |
|---|---|---|
| `MESH_SPAWN_TYPE` | first node-admin | `process` or `container`; establishes Fabric provider policy |
| `RAFKA_FABRIC` | node-admin bootstrap | Fabric display/name input |
| `RAFKA_FABRIC_ID` | bootstrap/joining nodes | logical FabricId |
| `RAFKA_MESH` | node-admin | mesh name |
| `RAFKA_DATA_DIR` | every binary | identity/control/evidence data dir |
| `RAFKA_NODE_ADMIN_API_BIND` | node-admin | HTTP bind |
| `RAFKA_BIN_DIR` | node-admin | binary directory |
| `RAFKA_EVIDENCE_DIR` | every binary | JSONL span output root |
| `TRACEPARENT` | spawned binary | W3C parent of boot span |

Do not add "final DeploymentHandle" as a required launch env value. The provider obtains the exact post-spawn runtime locator through its own bootstrap/adoption seam.

A serving node-admin prints one `RAFKA_NODE_ADMIN_API_BASE=<url>` line and writes the same value in its data dir.

## 4. Control API

Every topology mutation returns `202 {"build_id":"..."}` and delegates to desired-state + Build control.

```text
POST   /api/build
GET    /api/builds?id=<build_id>
DELETE /api/builds?id=<build_id>   # history only; does not erase desired state
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
  "runtime_locator_kind": "process",
  "status": "pending|ready-for-traffic|draining|leaving|dead",
  "is_primary": false,
  "is_fabric_primary": false,
  "admin_api_base": null,
  "endpoints": [{"slot":"rpc-0","addr":"127.0.0.1:41001","freshness":"..."}]
}
```

Raw sensitive provider credentials are never exposed. Runtime locator may be represented by a safe fingerprint in public/evidence views if policy forbids raw locator disclosure.

Fabric/Mesh views carry logical IDs, current desired revision/state summary where appropriate, and live owning admin endpoint.

## 5. Probe

```text
rafka-rpc-probe --admin <api_base> <op> --target exact:<node_id>|path:<path.name> --key <u64>
               [--value <s>] [--expected <s>] [--pin <slot>=<freshness>] [--cut-before-finish]
op = put | get | delete | cas | echo
```

Output remains one JSON line with `Reply|NotSent|Unserved|Indeterminate`, executing node/mesh, incarnation, slot/freshness and domain result.

Pinned superseded slot is pre-commit `NotSent`; `--cut-before-finish` proves the 499 pre-commit cut.

## 6. Evidence

E2E evidence must be sufficient to prove the new late-authority cases without private maps.

Artifacts include at least:

```text
manifest.json
DesiredTopologyProjection before/after
desired revision
Build request/status/facts for active Build only
Node/Mesh/Fabric public snapshots
membership RuntimeFact snapshot/fingerprints
runtime adopt/inspect/exit ledger
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
admin hydrates desired state + current RuntimeFacts
admin Ready -> wins
admin adopts/controls a runtime launched before it existed
later proven drift -> new Build B against same desired revision
```

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
DesiredTopologyProjection update/conflict/hydration
RuntimeFact publish/learn/adopt/refuse
proven-drift reconciliation trigger
held-member stale-coverage rejoin
```

Exact names follow repository span grammar, and raw provider credentials must never enter telemetry.
