# Q47 — exact page replacements for Luke

Approved implementation: Q47 A. These page lines await Luke’s yes.

## docs/architecture/lifecycles/node-stop.md

Old:
```text
node.stop
  ← Started
  ← node.connections.deleted ConnectionsReceipt [Established { in_flight, closed } | Deadline { last_in_flight, closed, held }]
  ← node.stopped
```

New:
```text
node.stop (mesh-primary → exact node, NodeCommand 0x22)
  ← CommandAccepted (Started; admission, no lifecycle state)
  ← StopConnectionsDeleted { receipt: ConnectionsReceipt } (Data)
  ← NodeStopped (Terminal)
  or, after acceptance: Failed { step, reason, code } (Terminal)
```

## docs/architecture/lifecycles/node-stop.md

Old:
```text
1. Call node.stop for the accepted exact node, preserving its operation trace and
   Build/attempt identity where Build-backed.
```

New:
```text
1. The mesh-primary sends NodeCommand `StopNode { node_id, incarnation, build_id, attempt,
   operation }` to the exact birth. The caller's hop and Build carrier are unchanged. The node
   validates the command and sends `CommandAccepted`; only this admission makes the mesh-primary
   record stopping. A never-sent command writes no state.
```

## docs/architecture/lifecycles/node-stop.md

Old:
```text
   and the closed ids. The stop's visible status is stopping; the child call keeps its events,
```

New:
```text
   and the closed ids, reported as `StopConnectionsDeleted { receipt }` (Data) on the stop stream.
   The stop's visible status is stopping; the child call keeps its events,
```

## docs/architecture/lifecycles/node-stop.md

Old:
```text
4. Emit node.stopped on that same reply stream, and declare `Stopped` to the mesh-primary naming
```

New:
```text
4. Emit `NodeStopped` (Terminal) on that same reply stream, and declare `Stopped` to the mesh-primary naming
```

## docs/architecture/lifecycles/node-start.md

Old:
```text
The following is a decoded caller progress view, not a new wire enum:

```text
node.start
  ← Started
  ← node.joined
  [hydrate_before_ready]
  [node.topology.get, missing Build floor, rafka-time last]
  ← node.started
  [mesh-primary sets ReadyForTraffic]
  ← Complete
```
```

New:
```text
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
```

## docs/architecture/lifecycles/node-start.md

Old:
```text
Started
Failed { step: "node.join", reason: "…" }
```

New:
```text
CommandAccepted
Failed { step: "node.join", reason: "…", code } (Terminal)
```

## docs/architecture/lifecycles/node-start.md

Old:
```text
   `node.joined` only after the accepted join and channel join. node.start clears NodeStopped at
   node.joined.
```

New:
```text
   `NodeJoined` (Data) only after the accepted join and channel join; the mesh-primary records
   pending from that frame. node.start clears NodeStopped at NodeJoined. No state is written
   before `CommandAccepted`; starting remains the executing authority's overlay.
```

## docs/architecture/lifecycles/node-start.md

Old:
```text
5. Emit `node.started`: hydrated, with nothing pending.
```

New:
```text
5. Emit `NodeStarted` (Terminal): hydrated, with nothing pending.
```

## docs/architecture/lifecycles/states.md

Old:
```text
| node stopping | the node's mesh-primary | the stop call's admission (node.stop) |
```

New:
```text
| node stopping | the node's mesh-primary | the node's `CommandAccepted` admission frame on the `StopNode` command stream (0x22); no state before that frame |
```

## docs/architecture/node-rpc-envelope.md

Old:
```text
| [node.start](lifecycles/node-start.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Join 0x1D](#join-op-0x1d-rdm), [topology 0x1E](#topology-op-0x1e-rdm), [Build facts 0x1F](#build-facts-op-0x1f-rdm), [Status 0x1B](#status-op-0x1b-rdm) | node.joined, node.started; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
```

New:
```text
| [node.start](lifecycles/node-start.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Join 0x1D](#join-op-0x1d-rdm), [topology 0x1E](#topology-op-0x1e-rdm), [Build facts 0x1F](#build-facts-op-0x1f-rdm), [NodeCommand 0x22 StartNode](#nodecommand-op-0x22-rdm), exact-node hop only; CommandAccepted → NodeJoined (Data) → NodeStarted (Terminal) | node.joined, node.started; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
```

## docs/architecture/node-rpc-envelope.md

Old:
```text
| [node.stop](lifecycles/node-stop.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Status 0x1B StopNode](#node-stop-command-pair); stopped rides the StopNode reply; the stop's first step is `node.connections.delete { scope: All }` | node.connections.deleted, node.stopped; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
```

New:
```text
| [node.stop](lifecycles/node-stop.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [NodeCommand 0x22 StopNode](#nodecommand-op-0x22-rdm), exact-node hop only; CommandAccepted → StopConnectionsDeleted (Data) → NodeStopped (Terminal); the stop's first step is `node.connections.delete { scope: All }` | node.connections.deleted, node.stopped; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
```

## docs/architecture/node-rpc-envelope.md

Old:
```text
    StopNode { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
```

New:
```text
    StopNode { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 8, reserved: refused by name, never executed
```

## docs/architecture/node-rpc-envelope.md

Old:
```text
    StartNode { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
```

New:
```text
    StartNode { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 16, reserved: refused by name, never executed
```

## docs/architecture/node-rpc-envelope.md

Old:
```text
The caller contract is [node.stop / node.delete](#typed-object-contract); the sequences are
[node.connections.delete](lifecycles/node-connections-delete.md), [node.stop](lifecycles/node-stop.md) and
[node.delete](lifecycles/node-delete.md). The Status bodies StopNode and NodeLeft are
[StatusRequest](#status-op-0x1b-rdm) positions 8 and 9.

The stop's graceful closure is `node.connections.delete { scope: All }`; its receipt is a
`ConnectionsReceipt`, `Established { in_flight, closed }` or `Deadline { last_in_flight, closed, held }`.
Stopped rides the StopNode reply as `Stopped { receipt }`, and the receipt keeps the deadline evidence
when the stop hard-cut at the deadline. Reserved6 and Reserved7 keep their positions 6 and 7 and are
never sent; a receiver refuses them by name. Reserved2 keeps StatusReply position 2 and is
never sent. NodeLeft keeps its position and is never sent; a receiver refuses it by name.
Enum discriminants are never reordered or reused ([Section 2](#section-2-the-body)).

```

New:
```text
The caller contract is [node.stop / node.delete](#typed-object-contract). The mesh-primary's
exact-node stop and start hops use [NodeCommand 0x22](#nodecommand-op-0x22-rdm); caller routing and
the Build carrier are unchanged. StatusRequest positions 8 `StopNode` and 16 `StartNode` are reserved:
a receiver refuses each by name and executes nothing. Reply positions 17 `Stopped`, 18 `Started`
and 19 `StartFailed` stand. Reserved6, Reserved7 and NodeLeft retain positions 6, 7 and 9 and are
refused by name; Reserved2 retains StatusReply position 2. No position is reordered or reused.

Stop's whole `node.connections.delete { scope: All }` reports its `ConnectionsReceipt` in
`StopConnectionsDeleted` (Data), preserving `Established` or `Deadline` evidence; `NodeStopped`
ends the stream after the transport cut. The operation-named owed Stopped declaration remains
(Q36); frame and declaration apply once through the operation's held row (Q49).

```

## docs/architecture/node-rpc-envelope.md

Old:
```text
RDM owns this family. It carries the mesh-primary-to-node hop of
[node.connections.delete](lifecycles/node-connections-delete.md), which owns sequencing and the
failure contract. Direction: a node's mesh-primary calls the exact birth. Every call is
server-streaming and not forwardable; the server keeps serving it while the node is
connections-deleting. Field order and enum positions are on the generated
[wire page](/home/admin/rust-distributed-mesh/docs/reference/rdm/wire.md). Births are exact
identities, never names.

```text
mesh-primary → node   DeleteConnections { node_id, incarnation, scope: All | Connections([id, …]) }
node → mesh-primary   CommandAccepted { node_id, incarnation }            (Started)
node → mesh-primary   ConnectionsDeleting { node_id, incarnation }        (Data; All only)
node → mesh-primary   ConnectionsDeleted { closed, not_open, held, receipt }   (Terminal)
```

A refusal before acceptance is one frame, one of the reply's refusal variants on the wire page.
`CommandAccepted` carries no lifecycle state and is not node.start's
`Started`. Only `ConnectionsDeleting` makes the mesh-primary record connections-deleting. A stream
that breaks after `CommandAccepted` is incomplete.

```

New:
```text
RDM owns this family. It carries the mesh-primary → exact-node hop of
[node.connections.delete](lifecycles/node-connections-delete.md), [node.stop](lifecycles/node-stop.md)
and [node.start](lifecycles/node-start.md). The caller's hop and Build carrier are unchanged.
Every call is server-streaming, never forwarded, and served while connections-deleting.
Births are exact identities, never names. Every earlier byte and enum position stands; requests
`StopNode` and `StartNode` append at 1 and 2. Replies append after the existing positions 0–10.

```text
DeleteConnections { node_id, incarnation, scope }
  ← CommandAccepted { node_id, incarnation } (Started)
  ← ConnectionsDeleting { node_id, incarnation } (Data; All only)
  ← ConnectionsDeleted { closed, not_open, held, receipt } (Terminal)
StopNode { node_id, incarnation, build_id, attempt, operation }
  ← CommandAccepted { node_id, incarnation } (Started)
  ← StopConnectionsDeleted { receipt } (Data; appended reply 11)
  ← NodeStopped (Terminal; appended reply 12)
StartNode { node_id, incarnation, build_id, attempt, operation }
  ← CommandAccepted { node_id, incarnation } (Started)
  ← NodeJoined (Data; appended reply 13)
  ← NodeStarted (Terminal; appended reply 14)
after StopNode or StartNode acceptance:
  ← Failed { step, reason, code } (Terminal; appended reply 15)
```

A refusal before acceptance is one refusal frame. `CommandAccepted` is admission and carries no
lifecycle state: on a stop it makes the mesh-primary record stopping. `ConnectionsDeleting`
records connections-deleting for connections.delete; `NodeStopped` records stopped, `NodeJoined`
records pending, and `NodeStarted` records ReadyForTraffic after every start gate and before the
outer Build's Complete. Starting stays the executing authority's overlay. Nothing is written
before CommandAccepted or for NotSent. There is no separate status declaration for an open start;
the Q36 owed report remains when no stream is open. A stream lost after acceptance is incomplete.
The owner records each frame before acknowledging recovery; repeated frames cause no second record.

```
