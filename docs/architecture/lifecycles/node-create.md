# Node create

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.create(spec)` creates a logical node and composes the whole
[node.start](node-start.md) workflow with `start_kind = FirstStart`.
RDM's create owns identity, launch and admission; Rafka supplies its application hook.

## Operation contract

| Field | Contract |
|---|---|
| Caller | Accepted control/Build caller through the typed object |
| Executor | Accepted node-admin executor; mesh-primary for an ordinary node |
| Identity | New node.id and incarnation; immutable path and claimed name |
| Endpoint key | Mint into a new data dir, or load the key from an inherited data dir |
| Port | Bind port 0; advertise the actual OS-assigned transport_addr |
| Spec | The create spec: the node's properties, flat (Q166): its executable (path, sha256, image), storage, placement, tags and config, and whether the data dir is inherited; the one description of a node to create, never a fault-injection mechanism. The executable is the create's input and nothing else's: recorded on the row at the claim, re-hashed at the launch, never changed for the birth ([build-deployment.md](../build-deployment.md)) |
| Progress | Every event of the complete `node.start`, then `node.created` |
| Hooks | The composed [node.start](node-start.md)'s: [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready), blocking, with `start_kind = FirstStart`, and [`on_node_ready`](../fabric-node-hooks.md#on_node_ready), a notice after `node.started` |
| Completion | The composed [node.start](node-start.md) ends with `node.started`; `node.created` follows it |
| Wire | [Typed-object mapping](../node-rpc-envelope.md#typed-object-contract); no new op number in this workflow |

<a id="mesh-absent"></a>
## A node-admin whose mesh does not exist yet

There is no mesh create call: a mesh comes into being with its first node-admin. node.create names
its mesh, and the fabric-primary, which drives every node.create and holds the complete accepted
topology, decides from that topology: mesh absent is [mesh.create](mesh-create.md), this arm's own
page; mesh present, the node joins like any other node. The decision is taken from the accepted
topology, never by looking whether a channel exists and then creating it.

<a id="ordinary-process-birth"></a>

## Bound endpoint and join admission

1. Record the exact deployment, prepare its identity/data dir and launch its executable.
   The executor never picks, reserves or predicts a transport port. Before this step the
   node's [row writer](../node-identity-and-state.md#25-one-system-org-one-projection-one-writer-per-mesh)
   claims the separate path/name pair for the allocated node ID. The lifecycle claim retains the
   exact entry-owned launch snapshot; the node writes
   its own properties before readiness. A replacement uses its entry, true recovery the
   predecessor's acknowledged properties. No raw last-written-path lookup supplies a birth;
   `NameHeld { by: node.id }` refuses a held name and nothing is launched.
2. The process loads `endpoint_id` and binds port 0. In the exact-address configuration,
   endpoint startup neither awaits nor launches host-interface enumeration. The
   advertised ip:port is the actual bound address, not a guessed port or outgoing socket.
3. Once identity, data dir, launch and bind complete, compose
   [node.start](node-start.md#end-to-end-sequence) with FirstStart; `node.created` follows
   the whole start (step 6). The process's first outbound Node RPC is the report of [node.mesh.join](node-mesh-join.md): a node-admin
   joins its maker; an ordinary node joins its mesh-primary.
4. The accepting primary validates the reported digest against the deployment, the name
   against the create spec at a first start, and refuses a mismatch by name, installing
   nothing and releasing no wait ([Join contract](../node-rpc-envelope.md#join-op-0x1d-rdm)).
5. On acceptance it installs the actual address and completes WaitForBind / WaitForMeshJoin
   from the validated report, never from gossip. A JoinNode after deployment abandonment is
   refused by name.
6. The rest of the composed [start](node-start.md#end-to-end-sequence) runs from the join
   answer through `node.started`. Create then ends with `node.created`.

A fabric-primary recovering from its durable map follows the separate
[Fabric recovery](fabric-recovery.md) workflow. It has no maker call. Ordinary
reconnect and topology gap repair reuse the endpoint and call `node.topology.get`;
they do not create a logical node.

## Join completion and failure evidence

Only an accepted full report completes the deployment's bind/join waits. Provider
launch, a listening socket, a digest, a timeout or an unanswered call is insufficient.
Join acceptance does not complete start or bypass the hook. A typed refusal names
its step; an uncertain answer is not Joined. Runtime exit requires independent exact
provider proof, even for a deployment that never joined.

## Command, completion and gossip messages

The caller invokes `node.create(spec)` through the
[typed contract](../node-rpc-envelope.md#typed-object-contract). The progress view is:

```text
Started
[node.start(FirstStart): node.joined, node.started]
node.created
```

The bracketed sequence is composed from [Node start](node-start.md); it is not another
wire enum. A topology Digest cannot complete the caller's stream or the deployment wait
([caller execution](../node-rpc-envelope.md#caller-execution-and-certainty)).

## Numbered end-to-end sequence

Follow the [create steps](#bound-endpoint-and-join-admission), then the complete
[start sequence](node-start.md#end-to-end-sequence). `node.created` proves creation;
it does not claim the node is ready.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as Fabric-primary
    participant M as Mesh-primary (executor)
    C->>F: build.create(node.create spec)
    F->>M: build.attempt.run
    Note over M: claim name, mint, launch; compose whole node.start with FirstStart (pre_node_ready, then on_node_ready)
    M-->>F: start events, node.started, node.created
    F-->>C: step frames, Complete
```

## A node-admin created into a live mesh

A node-admin that [node recovery](node-recovery.md) re-creates into a mesh that lost its node-admins (Q335: the fabric-primary's node investigation of the lost mesh-primary, then recovery at its path.name) joins that mesh under the mesh's own MeshId and never mints another.
The mesh is still in the accepted topology, so the attempt belongs to the SAME accepted Build. (A
lost member of a mesh with a live mesh-primary is the same [node recovery](node-recovery.md), driven by that
mesh-primary.) Op schemas are the envelope's: [JoinNode](../node-rpc-envelope.md#join-op-0x1d-rdm),
[GetTopology](../node-rpc-envelope.md#topology-op-0x1e-rdm), [Ping](../node-rpc-envelope.md#ping-op-0x01-core),
the [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm) and [Status](../node-rpc-envelope.md#status-op-0x1b-rdm).

1. The recovery's node.create opens an attempt of the SAME accepted Build and deploys the mesh's
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

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
