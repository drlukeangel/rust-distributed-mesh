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
| Identity | New node.id and incarnation; immutable path and a name free in it, or held by a node that is not a node-admin and replaced ([build-deployment.md §3](../build-deployment.md)), checked at the Build's acceptance |
| Endpoint key | Minted and written into the data dir, a new one or an inherited one; never loaded from an inherited data dir (Q358) |
| Port | Bind port 0; advertise the actual OS-assigned transport_addr |
| Spec | The create spec: the node's properties, flat (Q166): its type, executable (path, sha256, image), storage_persistence, tags and config, and whether the data dir is inherited; the one description of a node to create, never a fault-injection mechanism. The executable is the create's input and nothing else's: recorded on the row at the create, re-hashed at the launch, never changed for the birth ([build-deployment.md](../build-deployment.md)) |
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
   mesh-primary checks the path and name against its view and the creates it has in flight. The
   launch carries the exact entry-owned launch snapshot; the node writes its own record at its
   start, before readiness. A new node uses its entry, true recovery the
   predecessor's acknowledged properties. No raw last-written-path lookup supplies a birth;
   `NameHeld { by: node.id }` refuses a held name and nothing is launched.
2. The process loads `endpoint_id` and binds port 0. In the exact-address configuration,
   endpoint startup neither awaits nor launches host-interface enumeration. The
   advertised ip:port is the actual bound address, not a guessed port or outgoing socket.
3. Once identity, data dir, launch and bind complete, compose
   [node.start](node-start.md#end-to-end-sequence) with FirstStart; `node.created` follows
   the whole start (step 6). The process's first outbound Node RPC is the report of [node.mesh.join](node-mesh-join.md): a node-admin
   joins its maker; an ordinary node joins its mesh-primary.
4. The node-admin it reports to validates the reported digest against the deployment, the name
   against the create spec at a first start, and refuses a mismatch by name, installing
   nothing and releasing no wait ([Join contract](../node-rpc-envelope.md#join-op-0x1d-rdm)).
5. On acceptance it installs the actual address and completes WaitForBind / WaitForMeshJoin
   from the validated report, never from gossip. A JoinNode after deployment abandonment is
   refused by name.
6. The rest of the composed [start](node-start.md#end-to-end-sequence) runs from the join
   answer through `node.started`. The first node of its type in its mesh is commanded `node.primary.update` by the mesh-primary (Q407); the create completes on `node.primary.updated`. Create then ends with `node.created`.

A fabric-primary recovering from its durable map follows the separate
[Fabric recovery](fabric-recovery.md) workflow. It has no maker call. Ordinary
reconnect and topology gap repair reuse the endpoint and call `node.topology.get`;
they do not create a logical node.

## Join completion and failure evidence

Only an accepted full report completes the deployment's bind/join waits. Provider
launch, a listening socket, a digest, a timeout or an unanswered call is insufficient.
Join acceptance does not complete start or bypass the hook. A typed refusal names
its step; an uncertain answer is not Joined. Runtime exit requires independent exact
the provider's inspect answer, even for a deployment that never joined.

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
[start sequence](node-start.md#end-to-end-sequence). `node.created` shows creation;
it does not claim the node is ready.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as Fabric-primary
    participant M as Mesh-primary (executor)
    C->>F: build.create(node.create spec)
    F->>M: build.section.run
    Note over M: mint, launch; compose whole node.start with FirstStart (pre_node_ready, then on_node_ready); writes each step's rdm.build.step
    Note over M: the launched node writes starting, pending, ready on its own record
    M-->>F: start events, node.started, node.created
    F-->>C: step frames, Complete
```

## A node-admin created into a live mesh

A node-admin that [node recovery](node-recovery.md) re-creates into a mesh that lost its node-admins (Q335: the fabric-primary's node investigation of the lost mesh-primary, then recovery at its path and name) joins that mesh under the mesh's own MeshId and never mints another.
It is the fabric-primary's own workflow, never a Build or part of one (Q342). (A
lost member of a mesh with a live mesh-primary is the same [node recovery](node-recovery.md), driven by that
mesh-primary.) Op schemas are the envelope's: [JoinNode](../node-rpc-envelope.md#join-op-0x1d-rdm),
[GetTopology](../node-rpc-envelope.md#topology-op-0x1e-rdm), [Ping](../node-rpc-envelope.md#ping-op-0x01-core),
the [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm) and [Status](../node-rpc-envelope.md#status-op-0x1b-rdm).

1. The recovery's node.create deploys the mesh's
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
6. Each other node-admin the mesh lost is a member like any other: the new mesh-primary recovers it
   through [node recovery](node-recovery.md) (step 5). It calls its maker, obtains topology, joins
   the channel and becomes ready.

```mermaid
sequenceDiagram
    participant FP as Fabric-primary / maker
    participant A as Joining node-admin
    participant L as Local mesh node
    participant D as Non-answering member
    participant P as Provider
    FP->>FP: node recovery: node.create of a node-admin into the mesh's MeshId
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
    Note over A,D: no answer hands D to node recovery; it is not a sign the node is dead
    A->>D: node investigation: tickle
    A->>P: node.runtime.inspect
    P-->>A: verdict
    alt reached
        A->>D: node.mesh.join
    else any other verdict
        A->>A: node recovery: node.delete D, node.create with D's path and name
    end
    A->>P: node recovery of each other lost node-admin
```

### Telemetry chain

| Actor | Channel/op | Payload / evidence | Span |
|---|---|---|---|
| Fabric-primary | node recovery / node.create | its own workflow; the mesh's MeshId | `rdm.node_admin.deployment.update.via-pipeline` |
| Joining node-admin → maker | JoinNode | [Join schema](../node-rpc-envelope.md#join-op-0x1d-rdm); exact deployed birth admitted | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.node_admin.node.update.via-join` |
| Joining node-admin → maker / local node | GetTopology | [Topology schema](../node-rpc-envelope.md#topology-op-0x1e-rdm); complete meshes installed | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.mesh.topology.serve.via-read`, `rdm.mesh.topology.update.via-read-install` |
| Mesh-primary → own members | Ping once | [Ping schema](../node-rpc-envelope.md#ping-op-0x01-core); exact-birth reply or non-answer | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct` |
| Mesh-primary / provider | node recovery | node investigation, one recovery on its verdict | `rdm.node_admin.deployment.update.via-pipeline`, `rdm.node_admin.deployment.update.via-step` |

Call/serve ancestry follows the [runtime spans](../node-rpc-envelope.md#runtime-spans).

### Command, completion and gossip messages

Joining composes deployment, topology reads, Ping and readiness; it has no opcode of
its own. A provider creates the process.

```text
fabric-primary                              node recovery: node.create of a node-admin, the mesh's MeshId
joining node-admin → maker (fabric-primary) JoinNode            ← Joined | JoinMismatch | DeploymentAbandoned
joining node-admin → maker                  GetTopology         ← Started, Snapshot chunks, End
joining node-admin → local own-mesh node    GetTopology         ← Started, Snapshot chunks, End
joining node-admin → each own-mesh member   Ping, once          ← Pong | no answer: node recovery
new mesh-primary                            node recovery of each other lost node-admin
```

Each body is its envelope schema, linked above. A JoinMismatch or DeploymentAbandoned grants
no admission. A Ping non-answer hands the member to [node recovery](node-recovery.md), whose
investigation decides, and no RPC is sent to a birth it has re-created. The joined node-admin reports
its status under
[node status](node-status.md#node-status-declaration). Each node created while the node-admin
joins publishes its own Digest under the [frame catalog](../gossip.md#31-frame-catalog).

A member deleted while the node-admin joins is node recovery's node.delete: its NodeDeleted
follows the step, recorded on the node record. It is not a mesh-death event; the mesh keeps its MeshId, and
topology omission is never death.

## States written

The launched node writes each of its states on its own record (Q356), a `node-record` line in `nodes.jsonl`, whole. The executor writes each step of the Build as an `rdm.build.step` line in `events.jsonl`.

| written | by | when |
|---|---|---|
| `starting` | the node | its launch: no process can acknowledge a create |
| `pending` | the node | its join report is accepted |
| `ready` | the node | `node.started`, after every start gate |
| `rdm.build.step`, `state` `complete` | the executor | the step completes |

As stored:

`starting`:

```json
{"len":1452,"specversion":"1.0","id":"01a1ae8e-ef6d-7fe3-a822-15a3b3638e5d","source":"rdm://x8f5kgwpm730/0y38y18jejj4/4camrb86y7tw","type":"node-record","subject":"0y38y18jejj4","time":"2026-11-06T01:52:45.420Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"0y38y18jejj4","path":"mesh1.node-admin","name":"1","type":"node-admin","incarnation":"4camrb86y7tw","state":"starting","endpoint_id":"ba493a5956dd6a7a414e4d689477c2d36e8c1801e2e8faaea20761a83bbe1a96","transport_addr":"127.0.0.1:40037","listeners":[],"runtime":{"deployment_id":"z3d3edy88m74","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1610241,"start":65856803}}},"is_primary":null,"is_fabric_primary":null,"cert":{"issued_at":1793929965415,"expires_at":1794016365415,"issuer_expires_at":1796521965415,"renew_at":1793988420127},"signing_cert":{"issued_at":1793929965415,"expires_at":1796521965415,"renew_at":1795706339479},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1610161/i143-creating_a_mesh_hands_its_members_to_its_own_primary-1610233-0/mesh1.node-admin.1"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/rdm-node-admin","sha256":"8cb2122a53f91157b0e89b0cca40fbcb00822a487e44b9a71dc53cc14834ce5f","image":null},"config":{},"tags":[]},"crc32":"ff901e2e"}
```

`pending`:

```json
{"len":1451,"specversion":"1.0","id":"01a1ae8e-ef6d-7fe3-a822-15a3b3638e5e","source":"rdm://x8f5kgwpm730/0y38y18jejj4/4camrb86y7tw","type":"node-record","subject":"0y38y18jejj4","time":"2026-11-06T01:52:45.421Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"0y38y18jejj4","path":"mesh1.node-admin","name":"1","type":"node-admin","incarnation":"4camrb86y7tw","state":"pending","endpoint_id":"ba493a5956dd6a7a414e4d689477c2d36e8c1801e2e8faaea20761a83bbe1a96","transport_addr":"127.0.0.1:40037","listeners":[],"runtime":{"deployment_id":"z3d3edy88m74","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1610241,"start":65856803}}},"is_primary":null,"is_fabric_primary":null,"cert":{"issued_at":1793929965415,"expires_at":1794016365415,"issuer_expires_at":1796521965415,"renew_at":1793988420127},"signing_cert":{"issued_at":1793929965415,"expires_at":1796521965415,"renew_at":1795706339479},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1610161/i143-creating_a_mesh_hands_its_members_to_its_own_primary-1610233-0/mesh1.node-admin.1"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/rdm-node-admin","sha256":"8cb2122a53f91157b0e89b0cca40fbcb00822a487e44b9a71dc53cc14834ce5f","image":null},"config":{},"tags":[]},"crc32":"6f6e2727"}
```

`ready`:

```json
{"len":1449,"specversion":"1.0","id":"01a1ae8e-efd5-7029-9720-82d979110be9","source":"rdm://x8f5kgwpm730/0y38y18jejj4/4camrb86y7tw","type":"node-record","subject":"0y38y18jejj4","time":"2026-11-06T01:52:45.524Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"0y38y18jejj4","path":"mesh1.node-admin","name":"1","type":"node-admin","incarnation":"4camrb86y7tw","state":"ready","endpoint_id":"ba493a5956dd6a7a414e4d689477c2d36e8c1801e2e8faaea20761a83bbe1a96","transport_addr":"127.0.0.1:40037","listeners":[],"runtime":{"deployment_id":"z3d3edy88m74","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1610241,"start":65856803}}},"is_primary":null,"is_fabric_primary":null,"cert":{"issued_at":1793929965415,"expires_at":1794016365415,"issuer_expires_at":1796521965415,"renew_at":1793988420127},"signing_cert":{"issued_at":1793929965415,"expires_at":1796521965415,"renew_at":1795706339479},"storage":{"persistence":{"persistence":"persistent","disposition":"preserve"},"data_dir":"/dev/shm/rdm-lane-5/cells-1610161/i143-creating_a_mesh_hands_its_members_to_its_own_primary-1610233-0/mesh1.node-admin.1"},"executable":{"launch_id":"node-admin","executable":"/home/admin/rdm-lanes/5/target/debug/rdm-node-admin","sha256":"8cb2122a53f91157b0e89b0cca40fbcb00822a487e44b9a71dc53cc14834ce5f","image":null},"config":{},"tags":[]},"crc32":"647b2bfd"}
```

`rdm.build.step`:

```json
{"len":492,"specversion":"1.0","id":"01a1ae8e-f225-7dad-a6eb-39824fb0d315","source":"rdm://x8f5kgwpm730/0y38y18jejj4/4camrb86y7tw","type":"rdm.build.step","subject":"q30r7znj1ff8","time":"2026-11-06T01:52:46.116Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-1132564795dfcfc3536eb95fa63c2266-b4b1d3efcb89589c-01","data":{"build_id":"yvbpmp15hydb","mesh":"mesh1","op":"node.create:q30r7znj1ff8","state":"complete","step":"AllocateIdentity"},"crc32":"1f9f3c62"}
```

## Failure

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
