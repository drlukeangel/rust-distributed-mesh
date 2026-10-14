# i143 - RDM generic Mesh product handoff

Node RPC codes and schemas are defined in the [envelope ledger](../architecture/node-rpc-envelope.md#the-op-ledger).

**Initiative:** `i143`  
**Repository under build:** `drlukeangel/rust-distributed-mesh`  
**Status:** downstream handoff index; not an independent implementation PRD  
**Milestone:** 59 - i143 Node RPC + generic Mesh product on RDM  
**Canonical implementation PRD:** [`i143-prd.md`](i143-prd.md)
**Canonical implementation design:** [`i143-design.md`](i143-design.md)
**Parity ledger:** `docs/plans/i143-transport-parity.md`  
**Downstream import:** `docs/plans/i141-node-rpc-substrate-prd.md`  
**Downstream migration:** `docs/plans/i142-node-rpc-migrations-prd.md`

This file deliberately does **not** duplicate the i143 implementation architecture. RDM main
`d73d021` places its initiative record in Rafka's `docs/plans/`; the linked PRD and design are
the implementation record. This file records the contract before i141 may import an RDM
revision. The [2026-10-07 convergence audit](i141-i143-convergence-audit.md) records later
lock stories and distinguishes the shipped pin from the pending wire/control contract.

---

## 1. Export contract

### Topology authority

```text
current fabric-primary
    = the only Fabric topology writer

Fabric.build_id -> complete Build
    = the one accepted Fabric topology
```

There is no independent `DesiredTopologyProjection`, desired revision, lineage, fork winner or `source_build_id` topology authority.

A topology change mints a new complete Build. Restart, same-path replacement, authority movement and proven runtime drift with unchanged topology use another attempt of the Build `Fabric.build_id` already names.

A second topology-changing request while the current Build is actively reconciling is refused by name with `409 build-in-progress`.

Crash-safe acceptance is:

```text
persist complete Build B through builds.storage
    -> persist Fabric.build_id = B through fabric.storage
    -> B is accepted
```

The pointer never names a Build that is not recoverable.

### Storage

RDM owns these generic interfaces plus memory and durable file implementations used by restart/chaos proof:

```text
fabric.storage
mesh.storage
nodes.storage
connections.storage
builds.storage
```

`builds.storage` adopts the existing RDM BuildStateAdapter memory/FileJournal implementation. i141 binds Rafka production backends to these interfaces; it does not invent the storage boundaries.

The RDM Fabric record contains `id`, `name`, and `build_id`. Joining/restarting admins hydrate that Fabric record plus the complete Build it names before authority-bearing Ready. Older Build history is not replayed to reconstruct topology.

### Authority and execution

The fabric-primary owns **what** topology is accepted. Runtime realization follows the owning Mesh:

```text
healthy Mesh
    -> mesh-primary executes ordinary-member and sibling-node-admin work

new/recovering Mesh before election
    -> exact Pending bootstrap/recovery admin executes only its own Mesh

whole-Mesh retirement
    -> explicit outside-Mesh executor rule
```

Provider-domain execution may delegate an exact runtime action to a valid domain-local executor. That changes execution location, never topology or election authority.

### Lifecycle/status communication

Gossip disseminates current membership/status/runtime facts. Short exact-target Node RPC directly notifies the semantic authority for explicit Node/Mesh/Fabric lifecycle/control operations.

Neither plane proves, approves, attests or confirms the other.

Status/control operations carry no transition id and are naturally idempotent on:

```text
Node   -> (NodeId, IncarnationId, status)
Mesh   -> (MeshId, status)
Fabric -> (FabricId, event)
```

`LifecycleTransition.transition_id` remains exclusively the identity of one lifecycle-transition execution and its hook receipts.

Election eligibility is current `ReadyForTraffic`; the winner is the lowest complete canonical NodeId. There is no committed/confirmed/authority-Applied Ready state or lifecycle/status provenance layer.

### Fabric shutdown

`FabricShutdown` is Fabric control state in `fabric.storage`, not a Build. It leaves `Fabric.build_id` unchanged.

Only the current fabric-primary accepts `/api/shutdown`. Every admin that receives/hydrates the shutdown persists its own copy, freezes Build reconciliation/drift/rebirth, and gossips `Draining`.

No runtime stop begins until every current live admin birth is Draining. Each mesh-primary then drains its own ordinary members and non-primary admins through exact RuntimeFacts, including adopted runtimes. The fabric-primary drains its own Mesh the same way, waits for actual runtime disappearance, then stops the remaining mesh-primaries and itself last.

A runtime that cannot be stopped is named as incomplete and is never inferred stopped. An all-admin restart on the same data dirs reloads FabricShutdown and comes up frozen.

### Node RPC

One canonical Iroh Endpoint belongs to a process, with protocol separation through Router/ALPN.
[node-rpc-envelope.md](../architecture/node-rpc-envelope.md) owns framing: the fence carries
target NodeId and Op; incarnation and endpoint freshness remain local pool/resolver knowledge.
A target NodeId mismatch is the reserved pre-dispatch `425 STALE_TARGET` refusal.

Generic Forward is core op `0x1A`, one-hop only, and preserves the already-selected final target.

Caller identity and W3C trace context live in the generic Node RPC request header. Malformed/oversized observability context is dropped with diagnostics and never changes RPC semantics. RDM baggage-to-span allowlist is exactly `test_case`, `scenario`, `operation`.

---

## 2. Required correction stories before export

The canonical RDM plan must include these before restart/chaos/export claims depend on them:

- rust-distributed-mesh#48 — `fabric.storage`, including `Fabric.build_id` and `FabricShutdown`;
- rust-distributed-mesh#47 — remove the independent desired-revision model; make `Fabric.build_id -> Build` the accepted topology; same-Build drift attempts;
- rust-distributed-mesh#43 — `mesh.storage`;
- rust-distributed-mesh#44 — `nodes.storage`;
- rust-distributed-mesh#45 — `connections.storage`;
- existing BuildStateAdapter adopted as `builds.storage`, not rebuilt;
- e6.s8 one-process Iroh Endpoint + request slot/freshness framing before Forward/routing/control RPC;
- e6.s9 caller identity + W3C context after framing/Forward;
- FabricShutdown persistence/drain/restart proof before generic chaos claims shutdown survival.

The old desired-topology scenario from i143.e1.s7/#40 is not an export gate under its old assertions. It must return as an accepted-Build/Fabric-pointer scenario proving the corrected model.

---

## 3. i141 import gate

i141 pins one exact RDM SHA only after the canonical RDM gates prove:

- one fabric-primary topology writer;
- `Fabric.build_id -> complete Build` as the sole accepted topology;
- same-Build/new-attempt drift;
- all five generic storage interfaces with durable restart proof;
- RuntimeFact/runtime-metadata successor adoption;
- provider-domain exact-runtime execution semantics;
- ReadyForTraffic + NodeId-only leadership;
- create/recover/replace with authority != executor;
- FabricShutdown freeze/drain/all-admin-restart behavior;
- one-process Iroh Endpoint, stale-slot fencing, Direct/ViaPeer target preservation and generic Forward;
- status/control natural-key idempotency with no status-RPC transition id;
- caller/W3C observability propagation that cannot change protocol outcome;
- process/container deterministic faults, real fault tests and soak;
- no duplicate generic control, topology, election, reachability, storage or recovery authority downstream.

If this file conflicts with RDM `docs/i143/prd.md`, the RDM PRD must be corrected first and this handoff updated to match. Do not grow this file back into a second implementation PRD.
