# Node start

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.start` rejoins a stopped or just-created process. It creates no identity or
runtime. Create composes it with FirstStart; restart composes it with AfterStop.

## Operation contract

| Field | Contract |
|---|---|
| Identity | Same node.id, incarnation, path/name, endpoint key, bound port and bin |
| Entry | A just-created process or a live process in stopped |
| Stopped reachability | Fresh Node RPC dial to its still-bound endpoint ([stopped nodes](states.md#stopped-nodes)) |
| Context | `start_kind = FirstStart` or `AfterStop`; the spec says whether the data dir is inherited |
| Hook | `hydrate_before_ready`, with joined channel, cert and current rafka-time; topology follows the hook |
| Completion | Hook Ok, complete topology, current time, required Build floor, `node.started`, and the mesh-primary's ReadyForTraffic on it |

## End-to-end sequence

1. A stopped process remains the same exact
   runtime and keeps its endpoint/port; it does not relaunch or load a new executable.
2. Run [node.mesh.join](node-mesh-join.md): `JoinNode` on the accepting authority, the answer's name, cert
   and rafka-time installed, the mesh channel joined (and the backbone, a node-admin), `node.mesh.joined`. FirstStart makes it the process's first
   outbound call; AfterStop rejoins the same `(node.id, incarnation)` over the endpoint and path of
   the call that commanded it, and learns who answered from the answer. A node renamed while
   stopped starts with its old name in its digest and is admitted under the row's. Emit
   `NodeJoined` (Data) only after the accepted join and channel join; the mesh-primary records
   pending from that frame. node.start clears NodeStopped at NodeJoined. No state is written
   before `CommandAccepted`; starting remains the executing authority's overlay.
3. Run [hydrate_before_ready](../fabric-node-hooks.md#hydrate_before_ready) with the
   start kind and inherited-data-dir indication. The app subscribes its kind topics
   through the joined channel and reads no topology snapshot as a prerequisite.
   Ready waits for the hook; its Ok, Blocked and Failed outcomes, the Blocked probe
   answer and cancellation follow [Outcomes and the gate](../fabric-node-hooks.md#outcomes-and-the-gate).
4. Obtain complete topology through [node.topology.get](node-topology-get.md). Install
   only complete source snapshots; partial snapshots never complete start. Fetch Build
   facts only when the node is below the authority's floor, after the hook and before
   node.started: the node-admin Ready check reports a missing pointer, an unreadable Build
   or holdings behind the required floor, and the node reads the facts with
   [FetchBuildFacts](../node-rpc-envelope.md#build-facts-op-0x1f-rdm), served only by a
   node-admin, under that schema's resolution order and backoff. The
   facts are absorbed through the Build log; the step completes when the Ready check holds
   the required facts, never on a received frame alone. Refresh rafka-time **last**, so it
   is current when the node re-enters.
5. Emit `NodeStarted` (Terminal): hydrated, with nothing pending. Neither a partial snapshot nor a
   Blocked hook permits it. That frame is the node's report: the mesh-primary sets
   ReadyForTraffic on that exact birth through the [node status](node-status.md) apply,
   not a caller op. The node sends no separate status report after node.started. A typed
   refusal of that apply completes no readiness.
6. Start ends with `node.started`. Create ends with `node.created` after the
   start it contains.

## Command, completion and gossip messages

The mesh-primary → exact-node hop uses NodeCommand 0x22. The caller's hop and Build carrier
are unchanged:

```text
StartNode { node_id, incarnation, build_id, attempt, operation }
  ← CommandAccepted (Started; admission, no lifecycle state)
  ← NodeJoined (Data; mesh-primary records pending)
  [hydrate_before_ready]
  [node.topology.get, missing Build floor, rafka-time last]
  ← NodeStarted (Terminal; mesh-primary records ReadyForTraffic)
  [outer Build Complete only after the ReadyForTraffic record]
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
    Note over N: hydrate_before_ready
    N->>M: node.topology.get
    M-->>N: topology, required Build facts, rafka-time last
    N-->>M: node.started
    Note over M: sets ReadyForTraffic
    N-->>M: Complete
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
