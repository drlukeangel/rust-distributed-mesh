# Fabric primary

This page owns the fabric-primary's rules: the Fabric's mesh roster comes from the accepted Build, a primary takeover is hydration and not a transition, the fabric-primary's complete-view gate, fabric identity hydration, and the table that tells takeover, restart, reset, join, removal, a node-admin joining a live mesh and Day 0 apart.

Companion owners:
- [`node-identity-and-state.md`](node-identity-and-state.md) for node identity and the node, mesh and fabric rows and their writers;
- [`fabric-node-lifecycle-elections.md`](fabric-node-lifecycle-elections.md) for mesh-primary and fabric-primary election semantics;
- [`build.md`](build.md) for the Build engine, Build storage and crash safety, lifecycle transition machinery and the RDM/Rafka boundary;
- [`deployment-provider.md`](deployment-provider.md) for runtime adoption, provider control and the RuntimeFact;
- [States](lifecycles/states.md#states) for fabric, mesh and node states and setting them;
- [`gossip.md`](gossip.md) for dissemination and repair.

The central rules are:

> **Fabric is the network root. A Fabric contains Meshes; a Mesh contains Nodes.**
>
> **A primary change selects who may act. It does not itself change Fabric topology or Fabric/Mesh lifecycle state.**

Exactly one fabric-primary changes Fabric topology, and each Mesh's mesh-primary node-admin realizes its own Mesh ([build.md §4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)).

---

## 1. The product hierarchy is Fabric -> Mesh -> Node

A Rafka deployment/control estate has one logical **Fabric** network root. A Mesh is not another independent network that later gets associated with a Fabric. It is a logical subdivision inside the Fabric. A Node belongs to one Mesh and therefore belongs to the Fabric transitively.

```text
Fabric F
  |
  +-- Mesh M1
  |     |
  |     +-- Node N1
  |     +-- Node N2
  |
  +-- Mesh M2
        |
        +-- Node N3
```

The logical identity chain is:

```text
Fabric.id
    -> Mesh.fabric

Mesh.id
    -> Node.mesh_id
```

A node's logical id and its transport `endpoint_id` are different identities, and `endpoint_id` never names a Fabric ([node-identity-and-state.md §2](node-identity-and-state.md#2-a-node-is-an-entity-and-it-has-three-ids), [§2.8](node-identity-and-state.md#28-a-fabric-is-an-entity-and-the-fabric-primary-owns-it)).

### 1.1 Identity creation order

Day 0 establishes the Fabric's identity before its Meshes', and a Mesh's before its Nodes':

```text
mint/hold Fabric F
    -> mint/hold Mesh M with Mesh.fabric = F
        -> mint/hold Nodes with Node.mesh_id = M
```

The one node id minted before its Mesh's is the bootstrap admin's own: the bootstrap's first step mints it, after the Fabric's id and before anything else of its boot, so its telemetry carries it from its first span (Q208).

Identity creation order and durable publication order are not the same thing. The Fabric, Mesh, and Node identities may be minted/held before the Fabric durable write plane is available. Their durable rows are published only when lifecycle permits it (§3).

This ordering removes circular identity discovery: a Mesh never has to discover its Fabric after mint, and a Node never has to discover its logical Fabric directly. Its Fabric follows from `Node.mesh_id -> Mesh.fabric`.

Commands follow the same hierarchy, downward. The fabric-primary commands mesh-primaries. A mesh-primary commands its own mesh's nodes, writes their rows and announces their status: it is the owner of its nodes, always. A command the fabric-primary originates for a mesh's node reaches it as a forwarded command through its mesh-primary, never directly. No node commands its own primary, and nothing in a mesh commands the mesh-primary but the fabric-primary. A mesh-primary the fabric-primary cannot reach is [mesh investigation](lifecycles/mesh-investigation.md)'s case, which gives the mesh a new mesh-primary; the fabric-primary never acts on a mesh's nodes in its place.

---

## 2. Four planes must stay separate

Fabric/Mesh control has four distinct planes.

```text
ACCEPTED TOPOLOGY
    the current Build (the Builds read newest first) -> the accepted topology
    Fabric -> Mesh -> exact logical node path.names

DURABLE LIFECYCLE TRUTH
    system.ops.meshes
    system.ops.fabrics

CURRENT PRIMARIES
    mesh primary
    fabric primary

DISSEMINATED PROJECTION
    mesh-primary MeshStatus
    fabric-primary FabricStatus
```

A bug that substitutes one plane for another is an architecture bug, even if the values happen to agree in one run.

### 2.1 Accepted topology

The Fabric's topology is the current Build's: the complete explicit topology the current fabric-primary accepted, compiled once from a submitted change and never replayed ([build.md §1](build.md#1-build-is-the-only-topology-mutation-engine), [§4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)). A mesh comes into being through the `node.create` of its first node-admin ([node.create](lifecycles/node-create.md#mesh-absent)). This plane answers which meshes the Fabric contains (§5).

### 2.2 Durable lifecycle truth

The `Mesh` row on `<system_org>.ops.meshes` and the `Fabric` row on `<system_org>.ops.fabrics` are the durable lifecycle records of entities that exist; their fields, writers and write times are [node-identity-and-state.md §2.7](node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it) and [§2.8](node-identity-and-state.md#28-a-fabric-is-an-entity-and-the-fabric-primary-owns-it). The rows are not the Fabric's mesh directory (§5), and gossip projects them without becoming a second source of truth ([gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)).

### 2.3 Current primaries

Election semantics live only in [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). This document consumes the elected mesh-primary and fabric-primary and does not restate eligibility, ordering, or failover rules.

A primary change changes the actor that performs future lifecycle work. It does not imply a Mesh or Fabric state transition and does not change the accepted topology.

#### 2.3.1 The topology writer

Only the current fabric-primary, as the election names it, changes Fabric topology; a non-primary refuses ([build.md §4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)).

#### 2.3.2 Mesh execution boundary

```text
fabric-primary
    -> decides/accepts that Mesh2 should exist
    -> decides/accepts what topology Mesh2 should have
    -> calls Mesh2's mesh-primary node-admin to perform Mesh2's mutations

Mesh2 mesh-primary node-admin
    -> creates, manages and deletes Mesh2's nodes
    -> realizes Mesh2's part of the accepted Build
```

The fabric-primary owns WHAT topology exists; each Mesh's mesh-primary node-admin owns HOW its Mesh realizes it. The fabric-primary may bootstrap a new Mesh's first node-admin, but it never becomes the direct launcher of every node in every Mesh. Which executor runs each operation, and how the fabric-primary calls it, is the [Build carrier](node-rpc-envelope.md#calls-and-direction)'s.

### 2.4 Disseminated projection

The mesh-primary's `MeshStatus` and the fabric-primary's `FabricStatus`, their authors, channels and replay are defined in the [frame catalog](gossip.md#31-frame-catalog); the states they carry are [States](lifecycles/states.md#states). A missing projection is unknown, never an implied lower or higher state (§8.2).

---

## 3. The state families

Fabric, mesh and node states, transition events and who declares each are defined only
in [States](lifecycles/states.md#states). This document composes those rules with topology, durable publication and
accepted Build execution; it defines no additional state family.

## 4. Fabric creation (`fabric.create`)

The creation sequence (mint the fabric, create each mesh's first node-admin, the fabric's states, `fabric.created`) is [fabric.create](lifecycles/fabric-create.md). The identity order it follows is §1.1.

---

## 5. The Fabric's mesh roster comes from the accepted Build

The answer to **which meshes the Fabric is supposed to contain now** comes from the current Build's accepted topology, not by reconstructing membership from `system.ops.meshes`.

This distinction matters during topology change:

```text
Create Mesh M3
    the fabric-primary accepts Build B19 with M3 as a mesh of F
    B19 is the current Build
    BEFORE M3 necessarily has a durable Mesh row

Remove Mesh M2
    the fabric-primary accepts Build B20 without M2
    B20 is the current Build
    mesh.delete may still be deleting M2's members
```

Therefore:

- the accepted Build owns **the Fabric's mesh membership**;
- `system.ops.meshes` owns the **durable lifecycle record of each Mesh entity**;
- the topology projection carries **current observed Mesh status**;
- the Build reconciles the Fabric's meshes against observed/runtime state.

Do not scan `system.ops.meshes` to discover the Fabric roster. Do not add a Mesh-by-Fabric durable secondary index merely to reconstruct membership. `Mesh.fabric` is the durable foreign key to its Fabric and a consistency fact on the Mesh record; it is not a replacement for the accepted Fabric shape.

Do not infer the roster from whichever Mesh-primary statuses happen to be held in memory either. Observation is not membership.

---

## 6. Mesh status dissemination

The durable Mesh row is lifecycle truth. Its current mesh-primary republishes the status as `MeshStatus` ([frame catalog](gossip.md#31-frame-catalog)), and every node holds it. The projection never replaces the row; peers, connection-row gates and the fabric-primary's complete view (§8) read it. A successor mesh-primary republishes the status it adopts (§7).

---

## 7. Mesh-primary takeover is hydration, not transition

A successor mesh primary reads/hydrates the Mesh lifecycle state that already exists and adopts that exact state locally.

The successor adopts the held or durable Mesh state under [States](lifecycles/states.md#states); it does not demote,
promote or rewrite that state solely because the primary changed. It republishes the adopted
projection, with no durable Mesh write, without manufacturing a lifecycle transition.

`Pending` is not promoted by a takeover shortcut. The ordinary lifecycle transition machinery owns advancement out of `Pending`.

A takeover should not emit a lifecycle status-change event merely because the primary changed. A takeover-adoption event may name the durable/adopted status and the projection publication, but it is not a state transition.

This boundary is intentional:

```text
the primary changed
    -> hydrate/adopt existing lifecycle fact

lifecycle fact changed
    -> explicit lifecycle transition path
```

---

## 8. Fabric-primary takeover and complete-view recomputation

A newly elected fabric-primary must already be able to act immediately; readiness prerequisites are lifecycle policy, and the election lives only in [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). It hydrates the fabric's current state's check-ins and completion evidence and the **complete** Mesh view. Mesh observations alone cannot declare fabric readiness ([States](lifecycles/states.md#states)).

A successor reads the current Build from `builds.storage` ([build.md §4.2](build.md#42-hydration-of-the-accepted-topology)). Therefore it knows which meshes are required independently of which Mesh-status projections have arrived at that instant.

### 8.1 Expected Mesh set

The expected set comes from the accepted topology Build:

```text
expected = required meshes of the current Build
```

The expected set does **not** come from:

- `held_primary_mesh_statuses()`;
- currently visible node-admin digests;
- the set of Mesh rows that happen to have been durably published already;
- a scan or secondary-index reconstruction of `system.ops.meshes`.

Those are lifecycle/observation artifacts, not the accepted topology.

### 8.2 Status sources

For each required Mesh:

- a Mesh the successor itself currently owns uses its own local/held Mesh state;
- a peer Mesh uses the status published by that Mesh's current primary into the topology projection.

A required Mesh with no current primary/status projection is unknown. A newly required Mesh that has not yet published its lifecycle state is likewise not silently omitted from the fold.

### 8.3 Complete-view gate

The fabric primary computes:

```text
expected = required meshes of the accepted Build
observed = current own/projected status by Mesh
missing  = expected - observed
```

If `missing` is non-empty:

```text
publish no Fabric status
emit the held-recompute span, naming the missing Meshes
retry on subsequent status passes
```

No conservative or optimistic Fabric status is manufactured from a partial set. In particular:

```text
expected = {mesh1, mesh2}
observed = {mesh1 = Ready}

WRONG: derive([Ready]) -> Fabric Ready
RIGHT: mesh2 is unknown -> HOLD
```

The successor's one-shot recompute completion latch is set only after a complete-view hydration establishes the current Fabric result from its completed check-in evidence. A held partial attempt does not consume the one shot.

### 8.4 Fabric lifecycle evidence

A successor re-sends the current state's command and rebuilds its checklist from fresh check-ins under
[the check-in checklist](lifecycles/states.md#the-check-in-checklist). A complete Mesh view is control
evidence, not readiness.

---

## 9. Fabric identity hydration

Fabric identity and Fabric status have different succession rules.

The first Day-0 admin mints the Fabric identity **before** minting its Meshes. Each Mesh is minted with that `FabricId`; each Node is then minted with its `MeshId`, the bootstrap admin's own node id excepted (§1.1).

Ordinary node-admins hydrate/hold the immutable Fabric identity and the current Build before they are Ready. Fabric identity may be disseminated before the durable Fabric row exists; otherwise creation would deadlock on a row that cannot be written until the Fabric reaches its durable publication phase.

On fabric-primary succession:

```text
held Fabric identity
    + the current Build's meshes (from builds.storage)
    + complete current Mesh-status view
    + current-state completion evidence
    -> publish the held fabric lifecycle fact as current fabric-primary
```

Gossiped Fabric status is never promoted into the fabric-primary's own status.

The durable `system.ops.fabrics` row remains the Fabric lifecycle record once the durable write plane exists. A hydrating admin may validate immutable identity against it when available, but ordinary Ready must not depend on a row that does not yet exist during Fabric creation.

---

## 10. Joining or removing a Mesh

A Mesh topology change is accepted by the fabric-primary as a Build, not a primary change's side effect.

### 10.1 Joining Mesh

```text
fabric-primary accepts Build B21 naming mesh M
    B21 is the current Build
```

M comes into being with the `node.create` of its first node-admin ([node.create's mesh-absent arm](lifecycles/node-create.md#mesh-absent)). The joining Mesh counts toward Fabric completeness from the moment the accepted Build names it, before its Mesh row exists. A mesh join is a resize: the fabric returns to pending and changes to ready again ([States](lifecycles/states.md#states)), and what a serving mesh does meanwhile is [States: Resize and executable replacement](lifecycles/states.md#resize-and-executable-replacement).

### 10.2 Removing Mesh

A Mesh is removed by [mesh.delete](lifecycles/mesh-delete.md), which owns its steps, rows and identity release. The accepted Build that drops the Mesh removes it from the roster at acceptance, so a Mesh row still being deleted never keeps it there (§5).

---

## 11. Reset, replacement, node-admin join, and takeover are different events

Do not collapse these operations into one generic "re-entry" rule.

| event | accepted topology changes? | primary changed? | lifecycle may change? | owner |
|---|---:|---:|---:|---|
| mesh-primary takeover | no | yes | **not by takeover itself** | the election + hydration |
| fabric-primary takeover | no (the same active Build continues) | yes | hydrate current-state evidence and the accepted Build's complete Mesh set | the election + current-state completion evidence |
| node stop and start | no | maybe later | node lifecycle only unless another explicit event follows | Build + node lifecycle |
| WAL reset | no | not inherently | yes, only through its explicit reset/re-entry contract | reset lifecycle path |
| Mesh join | yes | yes/a new primary may appear | yes | Build + Mesh/Fabric lifecycle transition |
| Mesh remove | yes | yes as estate changes | yes | Build + Mesh/Fabric lifecycle transition |
| node-admin joins a live mesh whose node-admins are all lost | no | yes (it becomes the mesh's successor primary) | **not by the join itself** (§7) | Build (the [mesh investigation](lifecycles/mesh-investigation.md) connection-lost verdict runs node.create) + the election + hydration |
| Mesh replacement (mesh.delete, then a new mesh) | yes | yes | yes | Build + Mesh/Fabric lifecycle transition |
| Day 0 | establishes the first accepted Build | the initial primaries | yes | lifecycle root + the election |

A reset-specific clear/rewrite/re-entry path must remain explicit. It must not be inferred from a primary change, and takeover code must not be reused as a hidden reset transition.

### 11.1 fabric.delete

`fabric.delete` is a workflow the fabric-primary runs as one Build that deletes every mesh, its own last; [fabric.delete](lifecycles/fabric-delete.md) owns its steps and their failure.

---

## 12. Node RPC and LifecycleTransitionPipeline

Node RPC does not own a second Fabric/Mesh state machine or a second topology writer: it gives exact-target transport and certainty for one requested transition. The accepted Build owns the Fabric's mesh set (§5), [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md) owns who is primary, [build.md §9](build.md#9-lifecycletransitionpipeline) owns the transition pipeline, and [gossip.md](gossip.md) disseminates the resulting projection. The Status calls, their direction, replies and natural idempotency keys are defined only in [the envelope](node-rpc-envelope.md#status-op-0x1b-rdm). A reply never decides when a Mesh or Fabric may change state or which meshes the Fabric contains.

---

## 13. Node-admin storage, restart, and recovery

The node-admin storage interfaces, `fabric.storage` and `builds.storage`, Build retention and crash-safe acceptance are [build.md §5.2](build.md#52-rafka-adapter) and [§5.3](build.md#53-node-admin-storage-boundaries); persisted copies are recovery copies, never topology writers ([§4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)). A fabric-primary change mid-Build continues the same Build. Each topology change is its own Build with its own `build_id`; a workflow that keeps the accepted topology, the node.create of a node-admin into a live mesh and fabric recovery each run as the next attempt of the current Build ([§1](build.md#1-build-is-the-only-topology-mutation-engine), [§4.1](build.md#41-build-identity-is-active-reconciliation-state)). Runtime loss after a Build completed is [node recovery](lifecycles/node-recovery.md), which the lost node's mesh-primary starts. Inside Rafka, the [storage and recovery policy](node-admin-lifecycle.md#7-rafka-storage-and-recovery) owns the on-disk last-ready snapshot.

### 13.1 Recovery's inputs

A recovery's inputs follow the [storage and recovery owner](node-admin-lifecycle.md#7-rafka-storage-and-recovery). A live fabric-primary takeover uses held current records and the same accepted Build, and re-sends the current state's command under [the check-in checklist](lifecycles/states.md#the-check-in-checklist); no new Build or competing topology is minted because the primary changed.

---

## 14. Required invariants

Each rule of this page is stated once, in its section: the hierarchy (§1), the planes (§2), the mesh roster (§5), takeover as hydration (§7), the complete-view gate (§8), fabric identity hydration (§9) and the event distinctions (§11). The Build-engine invariants (one topology writer, a complete accepted topology, Build facts durable before acceptance, persisted copies that are not writers) are [build.md §1, §4 and §5](build.md#1-build-is-the-only-topology-mutation-engine).

---

## 15. Reference diagrams

The hierarchy is drawn in §1, the four planes in §2, mesh-primary takeover in §7 and the fabric-primary's complete-view gate in §8.3.
