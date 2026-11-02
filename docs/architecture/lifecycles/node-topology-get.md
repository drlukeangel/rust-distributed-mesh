# Node topology get

<a id="node-topology-top-up"></a>

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Op contracts](../node-rpc-envelope.md#the-op-ledger)

An already-held member requests [node.topology.get](../node-rpc-envelope.md#topology-op-0x1e-rdm)
from its own current mesh-primary when a differential cannot be applied to the state it holds.
For this repair it requests the affected mesh with `since: None`: no publisher or version is
sent. It installs a complete snapshot from the reply, then resumes applicable differentials. The answer also carries the node's own
node record at its node.id (Q323), read from
the shared backend. This startup context is not a second property owner; the node's writer
preserves retained properties and application recognition (Q234).
JoinNode is only a join and is
never a topology re-read. Gossip gap detection schedules the read; it does not create a
Build or change the target's meta.

## Typed read contract

`node.topology.get` maps to GetTopology on op 0x1E. It emits no lifecycle event and keeps
the admitted endpoint and port.

Startup is separate from gap repair: after the hook and required Build-facts check it makes
one final topology request with no conditional source version. That reply supplies both complete
topology and current Rafka-time before the node reports started. It does not fetch topology
before the Build check or call this endpoint a second time solely to refresh time.

The typed API has an optional conditional `since` field: the server answers `Unchanged` only
when the supplied projection publisher and version both match. Neither the gap-repair request
nor the final startup fetch supplies it. Its existence does not require a preliminary fetch.

## Command, completion and gossip messages

This is a read/connection repair, not a stop, restart or new admission. It has no
command/completion gossip hook, and the same running endpoint and birth are reused.

```text
node.topology.get { mesh: affected mesh, since: None }     exact member → own mesh-primary
  ← Started
  ← Snapshot chunks (the requested mesh); other reply variants follow the typed contract
  ← Primaries (each recorded primary the mesh-primary holds, gone or not)
  ← RafkaTime
  ← OwnRow (the reader's own whole row)
  ← End
install complete source snapshots, then resume deltas
```

The request, reply variants, install rules and spans are the
[Topology schema](../node-rpc-envelope.md#topology-op-0x1e-rdm). The node's own Digest and
its mesh-primary's mesh topologies follow the [frame catalog](../gossip.md#31-frame-catalog) and
the [cross-mesh projection](../gossip.md#33-cross-mesh-topology-inside-a-mesh-deltas-full-snapshots-and-the-top-up).
Read completion does not prove a runtime exited.

## Message sequence diagram

```mermaid
sequenceDiagram
    participant N as Node
    participant M as Mesh-primary
    N->>M: node.topology.get
    M-->>N: Started
    M-->>N: complete Snapshot chunks for the affected mesh
    M-->>N: End
    Note over N: Install complete source snapshots, then resume deltas
```

## Running node reconnect

A running node that loses its connections for a while reconnects as the same exact birth:
same node.id, incarnation, endpoint key and path.name. Only its reachability changes.
Reconnect is not an op. It opens no Build and emits no join receipt.

1. The node reuses its bound endpoint, its OS-assigned port and its held topology; no port
   is picked or rebound. It sends no JoinNode: JoinNode is [node.start](node-start.md)'s
   join step, never a topology re-read. A restarted node also keeps its endpoint and port;
   a recovered node (a new node over the inherited data dir) binds port 0 through
   [node.create](node-create.md).
2. When source versions reveal a delta gap, the node calls node.topology.get on its own
   exact current mesh-primary, installs the complete snapshot, then applies further deltas
   against that source version. A fresher digest alone does not fill a gap.
3. The node publishes its current digest. Transient connection markers clear only on
   connection or readiness proof for the same exact birth.

A topology refusal or an incomplete stream installs no partial snapshot. Transport
uncertainty is neither a successful read nor proof of death. Reconnect needs no restart
and changes no identity.

```mermaid
sequenceDiagram
    participant N as Running node
    participant M as Mesh-primary
    opt topology gap
        N->>M: node.topology.get
        M-->>N: complete topology stream
    end
    Note over N: Same endpoint and port, no JoinNode
```

| Actor | Channel/op | Payload / proof | Span |
|---|---|---|---|
| Held member → own mesh-primary | node.topology.get on delta gap | [Topology schema](../node-rpc-envelope.md#topology-op-0x1e-rdm) | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.mesh.topology.serve.via-read` |
| Held member | complete topology install | complete source snapshot; no partial install | `rdm.mesh.topology.update.via-read-install` |
