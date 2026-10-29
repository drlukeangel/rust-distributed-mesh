# Node start

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.start` rejoins a stopped or just-created process. It creates no identity or
runtime. Create composes it with FirstStart; a start of a stopped node runs it with AfterStop.

## Operation contract

| Field | Contract |
|---|---|
| Identity | Same node.id, incarnation, path/name, endpoint key, bound port and bin |
| Entry | A just-created process or a live process in stopped |
| Admission | The fabric-primary admits a start from its view, where a stopped node of a peer mesh is the mesh-primary's NodeStopped ([Build §4.4](../build.md#44-the-gates-before-an-operation)); the mesh-primary verifies the exact birth (node id and incarnation) when it runs the start |
| Stopped reachability | Fresh Node RPC dial to its still-bound endpoint ([stopped nodes](states.md#stopped-nodes)) |
| Context | `start_kind = FirstStart` or `AfterStop`; the spec says whether the data dir is inherited |
| Hooks | [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready), blocking, with joined channel, cert and current rafka-time; topology follows the hook. [`on_node_ready`](../fabric-node-hooks.md#on_node_ready), a notice, once after `node.started` |
| Completion | Hook Ok, complete topology, current time, required Build floor, `node.started`, and the mesh-primary's Ready on it |

## End-to-end sequence

1. A stopped process remains the same exact
   runtime and keeps its endpoint/port; it does not start a new process or load a new executable.
2. Run [node.mesh.join](node-mesh-join.md): `JoinNode` on the accepting primary, the answer's name, cert
   and rafka-time installed, the mesh channel joined (and the backbone, a node-admin), `node.mesh.joined`. FirstStart makes it the process's first
   outbound call; AfterStop rejoins the same `(node.id, incarnation)` over the endpoint and path of
   the call that commanded it, and learns who answered from the answer. Emit
   `NodeJoined` (Data) only after the accepted join and channel join; the mesh-primary records
   pending from that frame. node.start clears NodeStopped at NodeJoined, or, for an executor whose
   replayed start of a birth its view holds stopped is answered `AlreadyApplied`, at that answer: the
   join it did not see; it clears the mark in its overlay and publishes the clear as the mesh's
   primary (Q249). No state is written
   before `CommandAccepted`; the mesh-primary records starting from that frame, the node's
   acknowledgement (a create's starting from the launch), and a query answers it ([states](states.md)).
   The join and the start stream are two calls with no order between them: a starting frame that
   reaches the mesh-primary after the same start's join has set pending, or its completion has set
   ready, moves the row nowhere, as a late frame does on the stop ([node.stop](node-stop.md)); a
   later start's acknowledgement is its own. The progression in [states](states.md) is an order,
   not a promise that every intermediate state is durably observed.
3. Run [pre_node_ready](../fabric-node-hooks.md#pre_node_ready) with the
   start kind and inherited-data-dir indication. The app subscribes its kind topics
   through the joined channel and reads no topology snapshot as a prerequisite.
   Ready waits for the hook; its Ok, Blocked and Failed outcomes, the Blocked probe
   answer and cancellation follow [Outcomes and the gate](../fabric-node-hooks.md#outcomes-and-the-gate).
4. Obtain complete topology through [node.topology.get](node-topology-get.md). Install
   only complete source snapshots; partial snapshots never complete start. The read is the
   node's hydration sync (Q234): with the topology it carries the node's own whole row, the
   recorded primaries the mesh-primary holds (a lost birth's marked gone) and each mesh's id,
   and a node-admin started by a launch reads none of them from the store (Q227). Fetch Build
   facts only when the node is below the primary's floor, after the hook and before
   node.started: the node-admin Ready check reports a missing pointer, an unreadable Build
   or holdings behind the required floor, and the node reads the facts with
   [FetchBuildSection](../node-rpc-envelope.md#build-facts-op-0x1f-rdm) of its own mesh: of the responder's
   current Build when the missing pointer names none, else of the Build named, each served
   only by a node-admin, under that schema's resolution order and backoff. The
   facts are absorbed through the Build log; the step completes when the Ready check holds
   the required facts, never on a received frame alone. A node-admin started by a launch, and
   any node-admin started after a stop: its Build-floor check reads the facts its Build log
   holds, never the store (Q227). Refresh rafka-time **last**, so it
   is current when the node re-enters.
5. Emit `NodeStarted` (Terminal): its hook passed, with nothing pending. Neither a partial snapshot nor a
   Blocked hook permits it. That frame is the node's report: the mesh-primary sets
   Ready on that exact birth through the [node status](node-status.md) apply,
   not a caller op. The node also declares Ready to its mesh-primary as an owed message, as it declares
   Stopped (Q36, Q87); frame and declaration apply once (Q49). A typed
   refusal of that apply completes no readiness.
6. Start ends with `node.started`. The node then calls
   [on_node_ready](../fabric-node-hooks.md#on_node_ready) once, with the same `NodeReadyCtx`, on a
   task of its own: a notice that gates nothing (`rdm.mesh.node.update.via-on-node-ready`). Create
   ends with `node.created` after the start it contains.

## Command, completion and gossip messages

The mesh-primary → exact-node hop uses NodeCommand 0x22. The caller's hop and Build carrier
are unchanged:

```text
StartNode { node_id, incarnation, build_id, attempt, operation }
  ← CommandAccepted (Started; admission, no lifecycle state)
  ← NodeJoined (Data; mesh-primary records pending)
  [pre_node_ready]
  [node.topology.get, missing Build floor, rafka-time last]
  ← NodeStarted (Terminal; mesh-primary records Ready)
  [on_node_ready, a notice]
  [outer Build Complete only after the Ready record]
```

If join fails after start:

```text
CommandAccepted
Failed { step: "node.join", reason: "…", code } (Terminal)
```

No node.joined, node.started or downstream success follows that failure
([caller execution](../node-rpc-envelope.md#caller-execution-and-certainty)). The process's
`digest_seq` continues across stop/start and no new incarnation is minted; digest ordering
is the [frame catalog](../gossip.md#31-frame-catalog)'s.

## Sequence diagram

```mermaid
sequenceDiagram
    participant M as Mesh-primary
    participant N as Node
    M->>N: node.start
    N->>M: join
    M-->>N: cert and rafka-time
    N-->>M: node.joined
    Note over N: pre_node_ready
    N->>M: node.topology.get
    M-->>N: topology, required Build facts, rafka-time last
    N-->>M: node.started
    Note over N: on_node_ready (notice)
    Note over M: sets Ready
    N-->>M: Complete
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
