# Fabric recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric recovery restores the fabric from the fabric-primary's durable topology after every
node-admin is lost, whether or not any node is still alive. It starts when a person or a script
launches one node-admin on the fabric-primary's
data dir ([who starts recovery](../node-admin-lifecycle.md#44-fabric-primary-relaunch-and-who-starts-recovery));
durable topology selects recovery, never Day 0. No surviving maker exists, so there are no
maker JoinNode/GetTopology calls. The relaunched node-admin is a new node over its inherited
data dir: a new node.id and incarnation, the same path.name and endpoint key, and a new port.
The sequence below reads topology from a node that is alive; when no node is,
[Nothing alive](#nothing-alive) is the case.

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
  recovery's node.delete and taken by its node.create, which carries a new node.id.
