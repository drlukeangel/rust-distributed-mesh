# Mesh investigation

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

Mesh investigation is the fabric-primary's probe of an unheard peer mesh and its decision
whether to run node.create of a node-admin into it. A mesh is alive while it holds a node, and
a crash is not leaving: a mesh whose node-admins are all lost but whose members live is alive.
Only its mesh-primary leaves a mesh, always last, at the end of [mesh.delete](mesh-delete.md); the
mesh is then dead and never restarted. Remote liveness comes only from direct backbone receipt and a node's
own call and connection facts, never forwarded topology
([Gossip §3.3](../gossip.md#33-cross-mesh-topology-inside-a-mesh-deltas-full-snapshots-and-the-top-up),
[held-member repair](../gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours)).
When the unheard mesh's mesh-primary is the fabric-primary, every mesh-primary runs this ladder on it, and its decision step
starts [fabric recovery](fabric-recovery.md#the-loss-of-the-fabric-primary-while-a-node-admin-lives) at the
lowest Ready mesh-primary, which takes no primary role (Q122).

One investigation per peer mesh, owned by the fabric-primary. Let t be the elapsed monotonic time
since the fabric-primary's last valid direct backbone receipt from mesh-x. Only that receipt starts
or resets t; forwarded snapshots and forwarded topology never reset or extend it, and the backbone
interval or a delayed tick never moves a threshold: the thresholds below are elapsed time, not
counts of intervals. This investigation does not author mesh-x's MeshStatus. Probe traffic carries no
status change. Node recovery of a mesh's own members stays its mesh-primary's
([Node recovery](node-recovery.md)). Over a mesh whose members live, the fabric-primary runs no
per-node recovery ladder over its node-admins, and the per-admin observations it records are
diagnostic spans that open no node.create.

1. At 20 s, probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other
   node-admins, with [ProbeNodeState](../node-rpc-envelope.md#status-op-0x1b-rdm). Each target
   follows the established direct-then-via-peer policy: direct first; when that fails,
   [Ping](../node-rpc-envelope.md#ping-op-0x01-core) a few mesh-x members in parallel (active
   connections in the connections table first, otherwise members dialled from held topology) and,
   on a pong, use that member as the carrier for [Forward](../node-rpc-envelope.md#forward-op-0x1a-core).
   A successful contact ends the failed-reachability investigation.
2. At 30 s, mark mesh-x silent using the shared staleness mark. Send nothing at this mark.
   The mark is neither a MeshStatus change nor death proof.
3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
   each direct then via a peer), preferring a different carrier member. Record the latest completed probe's outcome; an older
   `connection-lost` cannot override a newer unreachable outcome.
4. If the node-admin answers either probe (Current or Blocked, under the
   [Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm)), stop the investigation.
   Direct backbone receipt from mesh-x at any point cancels the investigation and resets the
   unheard interval.
5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
   finish first; no silence-only path marks the mesh deleted. A node-admin is created into mesh-x in
   one of two cases only:
   - **Members live.** The latest probe answers `connection-lost`: a mesh-x member reports that
     it cannot reach its own node-admin. Record the verdict before opening the node.create attempt.
   - **No member answers.** Every node of mesh-x, its node-admins included, is unheard while the
     fabric is alive, so connection-lost cannot arise. The fabric-primary runs
     [node investigation](node-investigation.md) on mesh-x's node-admins, and the verdict decides
     (Q111): reached, that node-admin is commanded back through node.mesh.join; any other verdict,
     the fabric-primary creates mesh-x's node-admin in process (members of any mesh with no
     mesh-primary run in process at the fabric-primary, per the
     [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm)), on the same accepted Build and
     mesh-x's own MeshId, with the node.create step 6 describes, over the inherited data dir when
     the verdict is `exited` and over a newly allocated one otherwise (R-FM2). When mesh-x's
     mesh-primary is the fabric-primary, this arm is [fabric recovery](fabric-recovery.md), run by
     the lowest Ready mesh-primary on the backbone, which takes no primary role (Q122). A crash does not end a mesh, so this is never a new mesh. A mesh-x node that
     comes back after it was re-created is refused at admission and decided by its path.name and the
     atlas ([node investigation](node-investigation.md), Q207): it is deleted and its runtime terminated (Q110).
6. A connection-lost verdict runs node.create of a node-admin into mesh-x, on the SAME accepted Build
   and mesh-x's own MeshId: a node-admin joining a live mesh. The node.create carries mesh-x's
   MeshId, a seed and the mesh-primary launch mode, and the node-admin never mints a MeshId. It joins
   through its maker and reads topology as [a node-admin joins the live mesh](#a-node-admin-joins-the-live-mesh)
   states. At that same moment, both the mesh (`MeshStatus` degraded) and the fabric (`FabricStatus` degraded)
   are marked degraded (R-DG1), on either arm: the 60 s connection-lost verdict and the no-member-answers arm on
   its verdict, sent under [gossip §3.2](../gossip.md#32-cadence-timers-only-for-heartbeat-and-anti-entropy).
   Probing itself sends no FabricStatus or MeshStatus.
7. The joined node-admin, as mesh-x's successor primary, adopts the mesh's held state
   ([fabric primary §7](../fabric-primary.md#7-mesh-primary-takeover-is-hydration-not-transition)) and
   republishes its held MeshStatus, preserving changed_at, only after the
   fresh planned-birth checklist in [event-driven declarations](node-status.md#node-status-declaration).
   Only on that report, the fabric-primary clears the degraded health observation on both the mesh and the fabric.
   The fabric-primary never authors mesh-x's MeshStatus outside the investigation arms (R-DG1). Fabric readiness is
   [States](states.md#states)'s. Routing uses admitted births, lifecycle and connection
   facts; it never keys off FabricStatus; a mesh's status reaches routing only through the
   [one routing rule](states.md#routing-stop).

These probe/verdict spans record investigation evidence. RPC success and the named carried
outcome come from the [envelope schemas](../node-rpc-envelope.md#schemas); they are not inferred
from a forwarded topology frame. A node.create of a node-admin is not a terminal receipt for
an unreachable node-admin; what became of that node-admin is node investigation's verdict.

| t | Actor | Op/frame | Outcome | Span |
|---|---|---|---|---|
| unheard interval | Fabric-primary | direct backbone receipt absent | track mesh and last-heard time | `rdm.node_admin.mesh.update.via-unheard` (`mesh`, `at`) |
| 20 s | Fabric-primary → mesh-x's exact mesh-primary, then its other node-admins | ProbeNodeState direct; else Ping a member and, on pong, Forward carrying ProbeNodeState | probe 1: admin answered (ends the investigation), connection-lost or unreachable | `rdm.node_admin.mesh.update.via-probe` (`mesh`, `probe=1`, `member`, `carrier`, `outcome`); RPC call/serve chain below |
| 30 s | Fabric-primary | no frame | shared mesh silent mark only | `rdm.node_admin.mesh.update.via-unheard` (`mesh`, `at`) |
| 40 s | Fabric-primary → mesh-x's exact mesh-primary, then its other node-admins | as probe 1 | probe 2, prefer a different carrier member | `rdm.node_admin.mesh.update.via-probe` (`mesh`, `probe=2`, `member`, `carrier`, `outcome`); RPC call/serve chain below |
| any time | Fabric-primary / answering admin | admin answer or direct backbone receipt | stop / cancel | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome`, `at`) |
| 60 s | Fabric-primary | the decision step opens; outstanding probes finish first | connection-lost: node.create of a node-admin; no member answers: node investigation of mesh-x's node-admins, then node.mesh.join or node.create in process on its verdict (step 5) | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome=connection-lost or unreachable`, `at`) |
| 60 s, connection-lost verdict | Fabric-primary / provider | same-Build node.create of a node-admin into mesh-x; MeshStatus and FabricStatus degraded (both arms, R-DG1) | mesh-x's MeshId; health marker | `rdm.node_admin.mesh.create.via-recovery` (`mesh`, `at`) |
| new primary ready | Mesh-x primary; fabric-primary | MeshStatus ready; FabricStatus ready | fabric ready requires [States](states.md#states) completion | readiness evidence and status dissemination below |

The RPC chain uses `rdm.node_rpc.request.update.via-call` and
`rdm.node_rpc.request.serve.via-direct` for Ping and outer Forward. The carrier's inner
ProbeNodeState call uses `rdm.node_rpc.request.update.via-call`; the target emits
`rdm.node_rpc.request.serve.via-carried-inner` when reached. A connection failure cannot
produce an inner server span. Trace ancestry follows the envelope's carried-call context.

```mermaid
sequenceDiagram
    participant FP as Fabric-primary
    participant C as Mesh-x members / carriers
    participant A as Mesh-x node-admin (unreachable)
    participant P as Provider
    Note over FP: t since last direct backbone receipt; receipt cancels at any point
    FP->>C: t=20 s, probe 1: 0x01 Ping (few in parallel)
    C-->>FP: pong
    FP->>C: 0x1A Forward carrying 0x1B ProbeNodeState to admin
    C->>A: direct carried ProbeNodeState
    C-->>FP: connection-lost
    Note over FP: t=30 s: shared silent mark; nothing sent
    FP->>C: t=40 s, probe 2: Ping; prefer different member
    C-->>FP: pong
    FP->>C: Forward carrying ProbeNodeState
    C->>A: direct carried ProbeNodeState
    C-->>FP: connection-lost
    alt latest probe is connection-lost and no backbone receipt
        FP->>FP: t=60 s: connection-lost verdict; mesh-x's MeshId, same Build
        FP->>FP: author FabricStatus degraded
        FP->>P: node.create of a node-admin into mesh-x (a node-admin joining a live mesh)
    else no member answers
        FP->>A: node investigation of mesh-x's node-admins (node.runtime.inspect, then the tickle)
        alt reached
            FP->>A: node.mesh.join (commanded back)
        else any other verdict
            FP->>P: node.create of a node-admin in process, same Build, mesh-x's MeshId
        end
    end
```

```mermaid
sequenceDiagram
    participant FP as Fabric-primary
    participant C as Mesh-x carrier
    participant A as Mesh-x node-admin
    FP->>C: probe 1 or 2: Ping
    C-->>FP: pong
    FP->>C: Forward carrying ProbeNodeState
    C->>A: direct carried ProbeNodeState
    A->>A: reassert presence
    A-->>C: typed Status answer
    C-->>FP: carried answer
    FP->>FP: stop investigation; no node.create or degraded status
```

## Command, completion and gossip messages

A probe of one node-admin uses the
[Ping](../node-rpc-envelope.md#ping-op-0x01-core), [Forward](../node-rpc-envelope.md#forward-op-0x1a-core)
and [Status](../node-rpc-envelope.md#status-op-0x1b-rdm) schemas:

```text
fabric-primary → mesh-x admin            ProbeNodeState (direct)       ← Current | Blocked
else:
fabric-primary → mesh-x member           Ping                          ← Pong
fabric-primary → answering carrier       Forward { target: admin, inner: ProbeNodeState }
  carrier → admin                        ProbeNodeState (one direct inner call)
  ← carried StatusReply | ConnectionLost | another named Forward refusal
```

A pong proves the candidate answered, not that its node-admin is reachable. No reply,
connection-lost and a Current answer are distinct evidence.

No status or lifecycle gossip is sent at the 20 s/40 s probes or the 30 s silent mark.
At the 60 s connection-lost verdict, or on the no-member-answers arm's verdict (both arms, R-DG1),
both the mesh (`MeshStatus` degraded) and the fabric (`FabricStatus` degraded) are marked degraded, and travel as the
[frame catalog](../gossip.md#31-frame-catalog) states. The joined node-admin, as mesh-primary, later
authors its MeshStatus readiness after its fresh planned-birth checklist, which clears the degraded marker on both the
mesh and the fabric. Neither a probe nor degraded means the mesh is deleted.

Trace propagation and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).

## A node-admin joins the live mesh

The node-admin that step 5 or step 6 creates joins mesh-x under mesh-x's own MeshId and never mints another.
The mesh is still in the accepted topology, so the attempt belongs to the SAME accepted Build. (A
lost member of a mesh with a live mesh-primary is [node recovery](node-recovery.md), driven by that
mesh-primary.) Op schemas are the envelope's: [JoinNode](../node-rpc-envelope.md#join-op-0x1d-rdm),
[GetTopology](../node-rpc-envelope.md#topology-op-0x1e-rdm), [Ping](../node-rpc-envelope.md#ping-op-0x01-core),
the [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm) and [Status](../node-rpc-envelope.md#status-op-0x1b-rdm).

1. The node.create of step 6 opens an attempt of the SAME accepted Build and deploys the mesh's
   first node-admin (its MeshId, seed and mesh-primary launch mode). It records the deployed birth;
   the node-admin binds port 0 and the OS assigns the port.
2. The node-admin calls JoinNode on its MAKER, the fabric-primary, then GetTopology on that
   maker. It installs complete topology snapshots and obtains its control/status context.
3. It calls GetTopology on a LOCAL node of its own mesh to obtain current topology; members
   can exit while the mesh has no node-admin. It installs complete snapshots and joins its mesh
   channel using member addresses (RDM's `get_topology` in `rdm-admin`'s reenter module).
   A mesh with no live node has no local node to read, so there is no local topology read, and no
   member address to join from: its mesh channel is created again under the mesh's MeshId by its first
   listener, the created node-admin.
4. Before it is ready, it sends one bounded Ping, once, to every node IN ITS OWN MESH
   (a 2 s deadline each, at most 8 at once). A reply identifies a live exact birth; no reply
   is "not yet reached", never death. Ready waits for the sweep to finish (RDM's `start_booted` in
   `rdm-admin`'s admin module and `enter_existing_mesh` in its reenter module).
   It then becomes ready and is mesh-primary under the election; as the
   mesh's successor primary it adopts the mesh's held state
   ([fabric primary §7](../fabric-primary.md#7-mesh-primary-takeover-is-hydration-not-transition)).
   A mesh with no live node has no member to ping, so there is no sweep: the node-admin brings
   every member back through [node recovery](node-recovery.md), under the same MeshId.
5. Every node the sweep did not reach goes to [node recovery](node-recovery.md): the new
   mesh-primary's [node investigation](node-investigation.md), then recovery on its verdict.
   A missing Ping reply is the trigger, never the verdict.
6. The fabric-primary claims the creation of the missing secondary node-admin on its own log
   and calls the new mesh-primary with `build.attempt.run`; the executor adopts the won claim and
   runs it as an attempt of that same Build. The secondary node-admin calls its maker, obtains
   topology, joins the channel and becomes ready.

```mermaid
sequenceDiagram
    participant FP as Fabric-primary / maker
    participant A as Joining node-admin
    participant L as Local mesh node
    participant D as Non-answering member
    participant P as Provider
    FP->>FP: same Build attempt; node.create of a node-admin into the mesh's MeshId
    FP->>P: deploy first node-admin; no chosen port
    P->>A: start with the mesh's MeshId and deployed identity
    A->>A: bind port 0; OS assigns port
    A->>FP: 0x1D JoinNode
    FP->>FP: verify report; install address; discard old-address work
    FP-->>A: join result (envelope schema)
    A->>FP: 0x1E GetTopology
    FP-->>A: streamed maker topology
    A->>L: 0x1E GetTopology
    L-->>A: streamed current local topology
    A->>A: install complete snapshots; join mesh channel; ready / mesh-primary
    A->>L: one bounded 0x01 Ping
    L-->>A: reply from live exact birth
    A->>D: one bounded 0x01 Ping
    Note over A,D: no answer hands D to node recovery; it is not death proof
    A->>D: node investigation: tickle
    A->>P: node.runtime.inspect
    P-->>A: verdict
    alt reached
        A->>D: node.mesh.join
    else any other verdict
        A->>FP: one recovery attempt: node.delete D, node.create at the same path.name
    end
    FP->>FP: claim secondary node-admin attempt of same Build
    FP->>A: build.attempt.run (Won claim)
    A->>P: node.create of the secondary node-admin
```

### Telemetry chain

| Actor | Channel/op | Payload / proof | Span |
|---|---|---|---|
| Fabric-primary | accepted Build / node.create | same Build attempt; the mesh's MeshId | `rdm.node_admin.node.update.via-build`, `rdm.node_admin.deployment.update.via-pipeline` |
| Joining node-admin → maker | JoinNode | [Join schema](../node-rpc-envelope.md#join-op-0x1d-rdm); exact deployed birth admitted | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.node_admin.node.update.via-join` |
| Joining node-admin → maker / local node | GetTopology | [Topology schema](../node-rpc-envelope.md#topology-op-0x1e-rdm); complete meshes installed | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.mesh.topology.serve.via-read`, `rdm.mesh.topology.update.via-read-install` |
| Mesh-primary → own members | Ping once | [Ping schema](../node-rpc-envelope.md#ping-op-0x01-core); exact-birth reply or non-answer | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct` |
| Mesh-primary / provider | node recovery | node investigation, one recovery attempt on its verdict | `rdm.node_admin.deployment.update.via-pipeline`, `rdm.node_admin.deployment.update.via-step` |
| Fabric-primary → new mesh-primary | build.attempt.run | [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm); the fabric-primary claims on its own log, the executor adopts the won claim | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct` (op 0x20) |

Call/serve ancestry follows the [runtime spans](../node-rpc-envelope.md#runtime-spans).

### Command, completion and gossip messages

Joining composes deployment, topology reads, Ping, Build claims and readiness; it has no opcode of
its own. A provider creates the process.

```text
fabric-primary                              same Build attempt: node.create of a node-admin, the mesh's MeshId
joining node-admin → maker (fabric-primary) JoinNode            ← Joined | JoinMismatch | DeploymentAbandoned
joining node-admin → maker                  GetTopology         ← Started, Snapshot chunks, End
joining node-admin → local own-mesh node    GetTopology         ← Started, Snapshot chunks, End
joining node-admin → each own-mesh member   Ping, once          ← Pong | no answer: node recovery
fabric-primary → new mesh-primary           build.attempt.run   (Won claim) create the secondary node-admin
```

Each body is its envelope schema, linked above. A JoinMismatch or DeploymentAbandoned grants
no admission. A Ping non-answer hands the member to [node recovery](node-recovery.md), whose
investigation decides, and no RPC is sent to a birth it has re-created. The joined node-admin reports
its status under
[node status](node-status.md#node-status-declaration). Each node created while the node-admin
joins publishes its own Digest under the [frame catalog](../gossip.md#31-frame-catalog).

A member deleted while the node-admin joins is node recovery's Build node.delete: its NodeDeleted
follows the step's receipt. It is not a mesh-death event; the mesh keeps its MeshId, and
topology omission is never death.


## During fabric.delete

While [fabric.delete](fabric-delete.md) runs, a mesh whose mesh-primary does not answer its
mesh.delete is reached through this investigation. The same probes reach the unheard mesh: its
exact mesh-primary first, then its other node-admins, each direct then via a peer. During
fabric.delete the outcome is a delete, never a node.create of a node-admin:

- a node-admin that answers runs that mesh's [mesh.delete](mesh-delete.md);
- if no node-admin answers, the fabric-primary tickles each remaining node of the mesh directly,
  and on any successful connection runs [node.delete](node-delete.md) for that node, as executor.

Nothing of the fabric's topology is left behind. In a live fabric a mesh without a reachable
node-admin gets one through the steps above; this section applies only while fabric.delete runs.
