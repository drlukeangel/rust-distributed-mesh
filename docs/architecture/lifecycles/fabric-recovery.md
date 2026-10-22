# Fabric recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric recovery restores the fabric from the fabric-primary's durable topology after every
node-admin of every mesh is lost, whether or not any node is still alive; while any node-admin lives,
the fabric-primary moves and is brought back without a person
([the loss of the fabric-primary](#the-loss-of-the-fabric-primary-while-a-node-admin-lives),
[elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)). It starts when a person or a script
launches one node-admin on the fabric-primary's
data dir ([who starts recovery](../node-admin-lifecycle.md#44-fabric-primary-recovery-and-who-starts-it));
durable topology selects recovery, never Day 0. No surviving maker exists, so there are no
maker JoinNode/GetTopology calls. The launched node-admin is a new node over its inherited
data dir: a new node.id and incarnation, the same path.name and endpoint key, and a new port.
The sequence below reads topology from a node that is alive; when no node is,
[Nothing alive](#nothing-alive) is the case.

## The loss of the fabric-primary while a node-admin lives

One sequence, whatever stopped the fabric-primary and whichever node-admin is fabric-primary until it is
back. Only two things vary: when the row is written, and which node-admin the one leadership election
names ([elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)): the lowest
ReadyForTraffic node-admin of the fabric-primary's own mesh while it has one, else the lowest
ReadyForTraffic mesh-primary on the backbone. The mesh-primary and the fabric-primary move together: the
fabric-primary is the mesh-primary of one mesh, an explicit property of that node-admin
(`is_fabric_primary`, [node.primary.update](node-primary-update.md)).
They are separated in one case only: no node-admin of that mesh is available to the backbone. Then the
mesh-primary of another mesh is fabric-primary temporarily, stays mesh-primary of its own mesh, and
re-creates the lost mesh's node-admins; the fabric-primary returns to that mesh with the node at the lost
primary's path.name as its mesh-primary (step 4).

1. **Unheard past the staleness floor.** Every admin marks the fabric-primary `PendingReconnect` at the
   floor ([node recovery](node-recovery.md) step 1). Silence is the trigger; no provider verdict stands
   between the mark and the row.
2. **The successor writes the one row.** It names itself by the election on its own view and runs the
   writer's steps of [node.fabric-primary.update](node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate):
   the call, the successor's digest carrying the change, the record at the next revision. Any other admin that notices
   tells the successor; the successor acts only when its own view agrees; repeats answer `AlreadyApplied`.
   - *Its own mesh*: the successor is mesh-primary and fabric-primary at the first tick after the mark,
     once the record has named the unheard primary for one staleness floor in its own book: a record
     learned after a heal names a primary whose silence in this admin's book began before the record
     did. It records both on its own row as the mesh's writer. It needs no probe
     to know it is alive, and it publishes the mesh's aggregate as its primary within one backbone interval.
   - *Outside its mesh*: a mesh-primary hears nothing of that mesh once its aggregate stops, so it runs
     [mesh investigation](mesh-investigation.md)'s ladder on it: the fabric-primary first, then its other
     node-admins. An answer from any of them ends the investigation and the fabric-primary stays in that
     mesh. No answer from any of them by the decision step is the backbone rule, and the lowest
     ReadyForTraffic mesh-primary is fabric-primary and records it on its own row.
3. **The successor is fabric-primary above the lost one**, and the standing playbook runs downward from
   it: [node investigation](node-investigation.md) decides (Q111). Reached, by the investigation's answer
   from the exact birth and nothing else, the lost birth is commanded back through [node.mesh.join](node-mesh-join.md) and runs its start sequence,
   hook, cert and rafka-time (Q87). Any other verdict, node recovery re-creates it at the same path.name
   with a new node.id: over the inherited data dir with the same endpoint key when the investigation's
   provider step found the old runtime exited, over a newly allocated data dir, `inherited = false`,
   otherwise (`R-FM2`). An old birth that comes back after that is refused at admission
   ([node.mesh.join](node-mesh-join.md), `superseded-birth`) and its runtime is terminated (Q110).
   Nothing holds, and nothing waits for proof.
4. **The fabric-primary goes home.** Once the node at the lost fabric-primary's path.name, returned or
   re-created, has been ReadyForTraffic for one staleness floor, it is handed back, and the mesh-primary
   with it. In its own mesh, the successor runs [node.primary.update](node-primary-update.md) to that
   node, which becomes mesh-primary and fabric-primary and records both on its own row. From the
   backbone, the re-created node-admin first becomes mesh-primary of its mesh by the election (the
   recorded primary is a birth that is gone) and records it; then the temporary fabric-primary's
   hand-back, `node.fabric-primary.update` to it, makes it fabric-primary and each records its own row.
   The hand-back runs as an attempt of the current accepted Build, keyed `handback:<path.name>@<T>`, and
   does not declare the returning node lost. The fabric-primary returns only that way. The fabric's shared
   store root stays where it was. The fabric resolves to a fabric-primary at the original path.name over
   the original data dir and endpoint key when the investigation's provider step found the old runtime
   exited, and over a newly allocated data dir and endpoint key otherwise (R-FM2).

| from the fabric-primary's last frame | default (floor 30 s) | gate (floor 3 s) |
|---|---|---|
| mark `PendingReconnect`, every admin | 30 s | 3 s |
| its own mesh: the successor's row, at the first tick after the mark | 30 to 32 s | 3 to 3.5 s |
| its own mesh: the mesh's aggregate published again | by 34 s | by 4 s |
| outside its mesh: probe 1, probe 2, decision ([mesh investigation](mesh-investigation.md)) | 20 s, 40 s, 60 s | 2 s, 4 s, 6 s |
| outside its mesh: the row, when nobody in that mesh answered either probe | 60 s | 6 s |
| the hand-back, after the node at its path.name is ReadyForTraffic | one floor | one floor |

1. Load the durable topology and the accepted Build map; bind port 0 and retain the OS-assigned
   endpoint. The first GetTopology targets a node from that durable map: OWN MESH first,
   install only complete source snapshots. Stabilize the own mesh before investigating peers.
2. Obtain current own-mesh topology, join the mesh channel from its member addresses, recover
   control and become ready-for-traffic under the ordinary election.
3. Run the single bounded own-mesh Ping sweep
   ([a node-admin joins the live mesh](mesh-investigation.md#a-node-admin-joins-the-live-mesh) steps 4–5); a
   non-answering member goes to [node recovery](node-recovery.md).
4. Restore the own mesh's secondary admin as executor under that SAME accepted Build, with the
   claim the recovered fabric-primary's. Reach the other meshes through
   [mesh investigation](mesh-investigation.md): its decision step runs node.create of
   a node-admin into each mesh that has none, under that mesh's own MeshId. Each such
   mesh-primary restores its own admin capacity and sweeps only its own mesh.

```mermaid
sequenceDiagram
    participant H as Person
    participant FP as Recovered fabric-primary
    participant L as Node in durable own-mesh map
    participant O as Node in another mesh
    participant P as Provider
    H->>FP: start on FP data dir; mesh, seed, both primary flags
    FP->>FP: durable topology exists; recovery mode; bind port 0
    FP->>L: FIRST topology read: 0x1E GetTopology
    L-->>FP: streamed current topology
    FP->>FP: complete snapshots; join own mesh; ready-for-traffic / election
    FP->>L: one bounded own-mesh 0x01 Ping
    FP->>P: node investigation for non-answers
    FP->>FP: same Build claim; execute only when won
    FP->>P: restore own secondary admin
    Note over FP: own mesh stabilized before peer investigation
    FP->>O: probe via live connection into peer mesh; Ping / Forward ladder
    opt ladder proves no admin
        FP->>P: node.create of the peer mesh's first node-admin with its MeshId
    end
```

## Fabric recovery telemetry chain

| Actor | Channel/op | Payload / proof | Span |
|---|---|---|---|
| Recovered fabric-primary | GetTopology to durable-map node | [Topology schema](../node-rpc-envelope.md#topology-op-0x1e-rdm); own mesh first, no maker calls | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.mesh.topology.serve.via-read`, `rdm.mesh.topology.update.via-read-install` |
| Recovered own-mesh primary | Ping / node recovery | same own-mesh sweep and node investigation as a node-admin joining a live mesh | `rdm.node_rpc.request.update.via-call`, `rdm.node_admin.deployment.update.via-step` |
| Recovered fabric-primary | accepted Build / claim | same Build, each mesh's own MeshId, claims as fabric-primary | `rdm.node_admin.build.update.via-claim-request`, `rdm.node_admin.build.update.via-claim-decision` |

## Command, completion and gossip messages

Recovery composes deployment, topology reads, Ping, Build claims and readiness; it has no
recovery opcode. A provider creates the process.

```text
recovered fabric-primary → durable-map node of its own mesh   GetTopology        ← Started, Snapshot chunks, End
recovered fabric-primary → each own-mesh member               Ping, once         ← Pong | no answer: node recovery
recovered fabric-primary                                      ClaimAttempt for the attempt restoring its own mesh's secondary admin ← Won
```

The bodies are the envelope's [Topology](../node-rpc-envelope.md#topology-op-0x1e-rdm),
[Ping](../node-rpc-envelope.md#ping-op-0x01-core) and [BuildClaim](../node-rpc-envelope.md#buildclaim-op-0x1c-rdm)
execution. A Ping non-answer decides nothing: [node investigation](node-recovery.md) decides, and no RPC is
sent to a birth it has re-created. Restored members report status
under [node status](node-status.md#node-status-declaration) and publish their own Digest under the
[frame catalog](../gossip.md#31-frame-catalog).

A member deleted while the accepted mesh is restored is node.delete's NodeDeleted, published only
after its exact provider receipt. It is not a mesh-death event: the mesh keeps its MeshId,
and topology omission is never death.

### Node-side and receiving-side trace evidence

Calls and service emit the [runtime spans](../node-rpc-envelope.md#runtime-spans); evidence
correlates admission, completion and the subject's action to the exact birth and operation.

## Nothing alive

Nothing is alive when no mesh or fabric has a channel with an active listener. Nothing inside
the fabric brings it back: a person or a script launches a node-admin.

- Nothing ran mesh.delete or fabric.delete, so the fabric and its meshes were never deleted.
  The launched node-admin reuses the fabric id and each MeshId it reads from the store, and
  mints none.
- A channel with no active listener ends by definition. A mesh's channel is derived from its
  MeshId ([a mesh is an entity](../node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it)),
  and it is created again under that id by its first listener. It is never resumed.
- Every node the store names is re-created through node recovery, never waited for; an old process on the
  inherited data dir that comes back is refused at admission and terminated. When the recovery root contains no other node from which to obtain
  rafka-time, the launched node-admin adopts its own clock as rafka-time immediately; it does not enter a
  peer-adoption wait with an empty candidate set. It is the only node, the fabric-primary, and its own clock
  is rafka-time from the moment it starts. If any node answers, it is not this case: the launched admin
  joins the live fabric and takes rafka-time from the fabric-primary that exists.
- No node is alive to read topology from or to ping, so the topology read, the own-mesh sweep
  and carrier-edge-lost of the sequence above never arise. Every node comes back through
  [node recovery](node-recovery.md): node.delete of the exited instance, then node.create at the
  same path.name, with a new node.id over the inherited data dir. The members of any mesh with
  no mesh-primary run in process at the fabric-primary
  ([Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm)), so each other mesh gets its
  node-admin there, on the same accepted Build and that mesh's own MeshId.
- The fabric id re-claims the fabric's name
  ([a fabric is an entity](../node-identity-and-state.md#28-a-fabric-is-an-entity-and-the-fabric-primary-owns-it))
  and each MeshId re-claims its mesh's name
  ([a mesh is an entity](../node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it)).
  A same-id re-claim is idempotent, so there is no NameHeld and no mass delete. A node's name is released by node
  recovery's node.delete and taken by its node.create, which carries a new node.id. A takeover
  ([elections §5](../fabric-node-lifecycle-elections.md#5-day-0-preemption-restart-and-replacement)) moves the
  mesh-primary with the fabric-primary. The lost birth's name is taken by the node re-created at it; a lost birth
  that comes back is refused at admission and its runtime terminated (Q110).
