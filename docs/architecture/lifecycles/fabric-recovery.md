# Fabric recovery

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Fabric recovery restores the fabric from the fabric-primary's durable topology after every
node-admin of every mesh is lost, whether or not any node is still alive; while any node-admin lives,
the fabric-primary moves and is brought back without a person
([the loss of the fabric-primary](#the-loss-of-the-fabric-primary-while-a-node-admin-lives),
[elections §4.3](../fabric-node-lifecycle-elections.md#43-fabric-primary)). It starts when a person or a script
launches one node-admin at the fabric-primary's path.name: the adoption launch ([Nothing alive](#nothing-alive),
Q156), or a launch on the fabric-primary's data dir
([who starts recovery](../node-admin-lifecycle.md#44-fabric-primary-recovery-and-who-starts-it)); the
adoption launch's `RDM_NODE_NAME` or the durable topology selects recovery, never Day 0. Before its first
mutation the launch on the fabric-primary's data dir asks, as the adoption launch does, whether any node-admin
answers: a launch that finds a living admin is refused NameHeld, never an ordinary admin at the primary's name,
and that admin's fabric recovery re-creates the birth (Q156, Q199). No surviving maker
exists, so there are no maker JoinNode/GetTopology calls. The launched node-admin is a new node at the same
path.name: a new node.id and incarnation and a new port, with the same endpoint key over the inherited data
dir and a new one over a new data dir. The person's steps are in the
[fabric recovery runbook](../../eng/fabric-recovery-runbook.md).
The sequence below reads topology from a node that is alive; when no node is,
[Nothing alive](#nothing-alive) is the case.

## The loss of the fabric-primary while a node-admin lives

Fabric recovery is one workflow, an attempt of the current Build, and one mechanism wherever it starts
(Q122). At the staleness floor each node-admin of the fabric-primary's mesh asks: am I a node-admin; am I
the lowest Ready one; was the former mesh-primary also the fabric-primary? If not, it is
mesh-primary now ([elections §4.1](../fabric-node-lifecycle-elections.md#41-the-nodes-of-one-type-in-one-mesh)).
If so, it takes no primary role: it runs fabric recovery and broadcasts it on the backbone, the fabric
`degraded` with the workflow named (R-DG1). When the mesh has no node-admin left, the lowest
Ready mesh-primary on the backbone, from [mesh investigation](mesh-investigation.md)'s decision
step, does exactly the same. The workflow re-creates the node-admin at the fabric-primary's path.name
([node recovery](node-recovery.md), over the inherited data dir when the investigation's provider step
found the old runtime exited, otherwise over a newly allocated one, R-FM2). When that node is
Ready the workflow restores it as mesh-primary and fabric-primary (step 4) and completes. Nothing is
taken over and nothing is handed back; the fabric-primary's path.name never changed, so a replacement
executor starting the workflow over knows it. The attempt names what it recovers, as every recovery
attempt does, and its executor writes this attempt's Build records while the fabric-primary is
unavailable, and, while the target mesh has no active mesh-primary, the mesh and node records its own operations
require, nothing else; this makes it no primary, and ordinary mesh writing passes to the restored
mesh-primary when it assumes the role ([build.md §5.3](../build.md#53-node-admin-storage-boundaries)). If the executor is lost, the next one in
the same order starts the workflow again, resuming the attempt from its recorded progress.

1. **Unheard past the staleness floor.** Every admin marks the fabric-primary `PendingReconnect` at the
   floor ([node recovery](node-recovery.md) step 1). Silence is the trigger; no provider verdict stands
   between the mark and the row.
2. **Fabric recovery starts.** The executor names itself by the four questions on its own view, opens
   the attempt and broadcasts it. Any other admin that notices tells it; it acts only when its own view
   agrees; repeats answer `AlreadyApplied`.
   - *Its own mesh*: the lowest Ready node-admin starts fabric recovery at the first tick after
     the mark, once the record has named the unheard primary for one staleness floor in its own view: a
     record learned after a heal names a primary whose silence in this admin's view began before the
     record did. It takes no primary role. It needs no probe to know it is alive.
   - *Outside its mesh*: a mesh-primary hears nothing of that mesh once its mesh topology stops, so it runs
     [mesh investigation](mesh-investigation.md)'s ladder on it: the fabric-primary first, then its other
     node-admins. An answer from any of them ends the investigation and the fabric-primary stays in that
     mesh. No answer from any of them by the decision step starts fabric recovery at the lowest
     Ready mesh-primary, exactly as in the own-mesh arm.
3. **The executor runs the standing playbook on the lost one**: [node investigation](node-investigation.md) decides (Q111). Reached, by the investigation's answer
   from the exact birth and nothing else, the lost birth is commanded back through [node.mesh.join](node-mesh-join.md) and runs its start sequence,
   hook, cert and rafka-time (Q87). Any other verdict, node recovery re-creates it at the same path.name
   with a new node.id: over the inherited data dir with the same endpoint key when the investigation's
   provider step found the old runtime exited, over a newly allocated data dir, `inherited = false`,
   otherwise (`R-FM2`). An old birth that comes back after that is a deleted birth: every view refuses its
   Digest by name, and its mesh-primary decides it by its path.name and its atlas
   ([node investigation](node-investigation.md), Q207): its re-created birth is there, so it is reported
   and the fabric-primary terminates its runtime (Q110).
   Nothing holds, and nothing waits for proof. From the backbone the re-create terminates nothing in the
   mesh. On `exited`, `os-stopped` or `unproven`, its node.delete of the exact lost birth writes that
   birth's row deleted with no `TerminateRuntime` and releases the name, then node.create at the
   path.name; a delete that finds another birth at the path writes nothing; a lost birth that comes back
   is a deleted birth (Q142, Q148). On `running`, the investigation's report stands and the executor
   writes nothing: no row, no name release, no mark against the birth in its own view, no create. A
   runtime the executor can inspect running but cannot reach is unreachable from the executor, not lost; a
   running fabric-primary its own mesh cannot reach is the own-mesh arm's (Q153).
4. **The fabric-primary is back.** When the node at the fabric-primary's path.name, returned or
   re-created, is Ready, the workflow restores it as mesh-primary and fabric-primary: the node
   installs both roles, acknowledges directly, and records them on its own row as the mesh's writer; its
   normal Digest carries the change. The write records the roles; it does not grant them. The workflow
   completes on the acknowledgement. The fabric's shared store root stays where it was. The fabric
   resolves to a fabric-primary at the original path.name over the original data dir and endpoint key
   when the investigation's provider step found the old runtime exited, and over a newly allocated data
   dir and endpoint key otherwise (R-FM2). Under an in-force fabric.delete (build.md §1), the workflow
   re-creates and restores nothing: the delete is the current Build and names no node, so the executor
   holds the fabric-primary's roles to the end and continues the delete, whose mesh.delete of the lost
   birth's mesh deletes its row with the rest; no hand-back follows (Q155). The executor takes the fabric-primary role and the lost mesh's mesh-primary role by one winning claim and one recorded transition at the next revision, keeping its own mesh's; a returning fabric-primary's writes carry its superseded revision and every reader ignores them, its Build calls are refused StaleClaim, and it stops on its first read after the heal (Q109); a runtime the executor cannot inspect or terminate fails the delete's step by name, no receipt manufactured, the intent retained; its own mesh is deleted last, it reports fabric.deleted alive, and an outside caller deletes it (Q160).

| from the fabric-primary's last frame | default (floor 30 s) | gate (floor 3 s) |
|---|---|---|
| mark `PendingReconnect`, every admin | 30 s | 3 s |
| its own mesh: fabric recovery starts, at the first tick after the mark | 30 to 32 s | 3 to 3.5 s |
| its own mesh: the mesh's topology published again | by 34 s | by 4 s |
| outside its mesh: probe 1, probe 2, decision ([mesh investigation](mesh-investigation.md)) | 20 s, 40 s, 60 s | 2 s, 4 s, 6 s |
| outside its mesh: the row, when nobody in that mesh answered either probe | 60 s | 6 s |
| the workflow completes, when the node at the fabric-primary's path.name is Ready | — | — |

1. Load the durable topology and the accepted Build map; bind port 0 and retain the OS-assigned
   endpoint. The first GetTopology targets a node from that durable map: OWN MESH first,
   install only complete source snapshots. Stabilize the own mesh before investigating peers.
2. Obtain current own-mesh topology, join the mesh channel from its member addresses, recover
   control and become ready under the ordinary election.
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
    FP->>FP: complete snapshots; join own mesh; ready / election
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
| Recovered fabric-primary | accepted Build / claim | same Build, each mesh's own MeshId, claims as fabric-primary | `rdm.node_rpc.request.update.via-call` on the claimant, `rdm.node_admin.build.update.via-claim-decision` on the fabric-primary |

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

No node-admin is alive, the fabric's other nodes alive or not. Nothing inside the fabric brings an admin back: a person or a script launches a node-admin at the lost fabric-primary's path.name, with no launcher, given the fabric id, its mesh and the store root, which must hold that fabric or the start is refused by name. It adopts what the store holds and mints no fabric or mesh id; it is a new birth with its own node.id. It is given the fabric root and CA, from its environment or an inherited data dir, and the declared values (Q150), and refuses to start without them. Before any recovery mutation other than the claim itself, it claims the fabric-recovery attempt of the current Build for that mesh; a second launch's claim loses and it exits. Concurrent adoption launches resolve to the same recovery attempt for the exact lost birth, read from the Build journal on the store; while that attempt is active, no other launch opens a competing attempt for that birth. It installs the recorded primaries before electing (Q149) and runs node investigation on the lost birth: reached, or any node-admin answering before its first mutation, it is refused NameHeld and exits, closing the attempt through the failure path. Otherwise the claimed attempt releases the name, re-creates the birth as itself, adopts rafka-time from live members, and restores it as fabric-primary by step 4 once Ready; with nothing alive it is the only node, below (Q156).

- A store that names no node but the launching admin's own lost birth holds no fabric to recover: the launch deletes that storage and starts as Day 0, a new fabric-primary.
- Nothing ran mesh.delete or fabric.delete, so the fabric and its meshes were never deleted.
  The launched node-admin reuses the fabric id and each MeshId it reads from the store, and
  mints none.
- A fabric.delete in force when no node-admin was alive is the current Build (build.md §1): the adoption launch is created at a new path.name of the node-admin type in an existing, not-yet-deleted mesh, never at the lost fabric-primary's, which the delete removes; this is the one create a delete admits, and ordinary recovery creates none. It claims the delete's next attempt, takes by that claim and one recorded transition the fabric-primary role, the lost mesh's mesh-primary role and its own mesh's, continues the teardown, reports fabric.deleted while alive, and is then deleted by an outside caller (fabric-delete.md). If no not-yet-deleted mesh remains, the remaining work is the dead fabric-primary's own node.delete from outside, and nothing is created. It restarts nothing of the interrupted topology and revives nothing whose deletion receipt is durable; a claim and a provider's exit evidence release no mesh row by themselves. While a node-admin lives, the adoption is refused by name and that admin's executor continues (Q159).
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
  and connection-lost of the sequence above never arise. Every node comes back through
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
  recovery's node.delete and taken by its node.create, which carries a new node.id. The lost birth's name is taken by the node re-created at it; a lost birth
  that comes back is refused at admission and its runtime terminated (Q110).
