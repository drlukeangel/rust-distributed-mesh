# Fabric recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric recovery restores the fabric from the fabric-primary's durable topology after every
node-admin of every mesh is lost, whether or not any node is still alive; while any node-admin lives,
the seats move and the fabric-primary is brought back without a person
([the loss of the fabric-primary](#the-loss-of-the-fabric-primary-while-a-node-admin-lives),
[elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)). It starts when a person or a script
launches one node-admin on the fabric-primary's
data dir ([who starts recovery](../node-admin-lifecycle.md#44-fabric-primary-relaunch-and-who-starts-recovery));
durable topology selects recovery, never Day 0. No surviving maker exists, so there are no
maker JoinNode/GetTopology calls. The relaunched node-admin is a new node over its inherited
data dir: a new node.id and incarnation, the same path.name and endpoint key, and a new port.
The sequence below reads topology from a node that is alive; when no node is,
[Nothing alive](#nothing-alive) is the case.

## The loss of the fabric-primary while a node-admin lives

One sequence, whatever stopped the fabric-primary and whoever holds its seats until it is back. Only
two things vary: when the row is written, and which node-admin the one leadership calculation names
([elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)): the lowest ReadyForTraffic
node-admin of the fabric-primary's own mesh while it has one, else the lowest ReadyForTraffic mesh-primary
on the backbone.

1. **Unheard past the staleness floor.** Every admin marks the fabric-primary `PendingReconnect` at the
   floor ([node recovery](node-recovery.md) step 1). Silence is the trigger; no provider verdict and no hung
   hold stands between the mark and the row. An admin whose own view is cut off or healing acts on nothing
   ([gossip §6](../gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours)).
2. **The successor writes the one row.** It names itself by the calculation on its own view and runs the
   writer's steps of [node.fabric-primary.update](node-primary-update.md#the-fabric-seat-nodefabric-primaryupdate):
   "primary updating", the seat row at the next revision behind the store's authority fence, "primary
   updated". The row moves every seat the fabric-primary held, the mesh seat with the fabric seat (Q51), to
   one holder: a fabric-primary is a mesh-primary. Any other admin that notices tells the successor; the
   successor acts only when its own view agrees; repeats answer `AlreadyApplied`; the fence lets one row
   commit.
   - *Its own mesh*: the standby's seat watch wakes at the first tick after the mark and writes. It needs
     no probe to know it is alive, and it publishes the mesh's aggregate as its primary within one
     backbone interval.
   - *Outside its mesh*: a mesh-primary hears nothing of that mesh once its aggregate stops, so it runs
     [mesh investigation](mesh-investigation.md)'s ladder on it: the fabric-primary first, then its other
     node-admins. An answer from any of them ends the investigation and the seats stay in that mesh. No
     answer from any of them by the decision step is the backbone rule, and the lowest ReadyForTraffic
     mesh-primary writes the row.
3. **The successor is now the authority above the lost fabric-primary**, and the standing playbook runs
   downward from it: heard again by its digest or its tickle, the lost birth is commanded back through
   [node.mesh.join](node-mesh-join.md) and runs its start sequence, hook, cert and rafka-time (Q87); the
   provider reporting it running, it is `held-running` and the hung hold toward its terminate runs
   (`R-FM1`); proven exited, [node recovery](node-recovery.md) re-creates it at the same path.name over the
   inherited data dir with the same endpoint key and a new node.id; nobody able to inspect it, the row's
   `takeover { revision }` proof releases its name and it is re-created over a new data dir, `inherited = false`
   (`R-FM2`).
4. **The seats go home.** Once the node at the lost fabric-primary's path.name, returned or re-created,
   has been ReadyForTraffic for one staleness floor, the holder hands every seat it took back to it by the
   committed handover of [elections §4.4](../fabric-node-lifecycle-elections.md#44-explicit-committed-fabric-handover),
   naming it as the successor, in one row. The seats return only that way. The holder held the seats,
   never the storage: the fabric's shared store root and the fabric-primary's data dir stay where they
   were, and the fabric resolves to a fabric-primary at the original path.name with the original endpoint
   key.

| from the fabric-primary's last frame | default (floor 30 s) | gate (floor 3 s) |
|---|---|---|
| mark `PendingReconnect`, every admin | 30 s | 3 s |
| its own mesh: the standby's row, at the first tick after the mark | 30 to 32 s | 3 to 3.5 s |
| its own mesh: the mesh's aggregate published again | by 34 s | by 4 s |
| outside its mesh: probe 1, probe 2, decision ([mesh investigation](mesh-investigation.md)) | 20 s, 40 s, 60 s | 2 s, 4 s, 6 s |
| outside its mesh: the row, when nobody in that mesh answered either probe | 60 s | 6 s |
| the hand-back, after the node at its path.name is ReadyForTraffic | one floor | one floor |
| its terminate when the provider keeps reporting it running | ten floors | ten floors |

1. Load the durable topology and the accepted Build map; bind port 0 and retain the OS-assigned
   endpoint. The first GetTopology targets a node from that durable map: OWN MESH first,
   install only complete source snapshots. Stabilize the own mesh before investigating peers.
2. Obtain current own-mesh topology, join the mesh channel from its member addresses, recover
   control and become ready-for-traffic under the ordinary seat rules.
3. Run the single bounded own-mesh Ping sweep
   ([a node-admin joins the live mesh](mesh-investigation.md#a-node-admin-joins-the-live-mesh) steps 4–5); a
   non-answering member goes to [node recovery](node-recovery.md).
4. Restore the own mesh's secondary admin as executor under that SAME accepted Build, with the
   claim bound to the recovered fabric-primary's seat. Reach the other meshes through
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
    FP->>FP: complete snapshots; join own mesh; ready-for-traffic / seat rules
    FP->>L: one bounded own-mesh 0x01 Ping
    FP->>P: node recovery for non-answers; exact exit proof required
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
| Recovered own-mesh primary | Ping / node recovery | same own-mesh sweep and exact-runtime exit proof as a node-admin joining a live mesh | `rdm.node_rpc.request.update.via-call`, `rdm.node_admin.deployment.update.via-step` |
| Recovered fabric-primary | accepted Build / claim | same Build, each mesh's own MeshId, claims under its seat | `rdm.node_admin.build.update.via-claim-request`, `rdm.node_admin.build.update.via-claim-decision` |

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
schemas. Only a Won claim permits
execution. A Ping non-answer does not prove exit; node recovery's provider check supplies exact
exit proof, and no RPC is sent to a birth already proven terminal. Restored members report status
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
- Every node the store names, the old process on the inherited data dir included, is confirmed gone by
  the provider, never by a wait. When the recovery root contains no other node from which to obtain
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
  recovery's node.delete and taken by its node.create, which carries a new node.id. A committed takeover
  ([elections §5](../fabric-node-lifecycle-elections.md#5-day-0-preemption-restart-and-replacement)) moves every seat
  the lost fabric-primary held in its one row. Only when no surviving admin can inspect the lost birth does the row
  release its name: its row is written Deleted with exit proof `takeover { revision }`, and the mesh's re-created
  node-admin takes the name. A lost birth an admin can inspect keeps its name until the provider proves its exit.
