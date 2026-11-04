# Node name update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.name.update` renames a node. It is a single call, not a lifecycle operation: it moves no status
and is never a Build or part of one. It needs no connections: the node holds none, and a live node
rejoins its mesh under the new name. To keep a node aside, [node.stop](node-stop.md) follows as its
own primitive. A node's other properties have their own commands ([node properties](../node-rpc-envelope.md#node-properties));
[node.config.update](node-config-update.md) replaces its application config.

## Operation contract

| Field | Contract |
|---|---|
| Caller / executor | Caller to the node's mesh-primary, the node's mesh-primary (Forward `0x1A` when not directly reachable). Before a mesh has a primary, the fabric-primary acts as its writer |
| Entry | The exact `(node.id, incarnation)` and unchanged registered path and new name as separate fields. A different path is refused; mesh, type and tags do not change |
| Precondition | The node holds no connection: it is stopped, or this mesh-primary holds the birth's `node.connection.deleted` of an `All` ([node.connection.delete](node-connections-delete.md)) that left zero connections held. A live node holding a connection is refused. No new connection is admitted from the row write until the node rejoins its mesh, retries after an interruption included |
| Validation | The exact birth matches the held record, else `RejectedStaleIncarnation { held }`; the receiver is the node's writer, else `ReceiverNotPrimary { needed }` (R-RX5); the precondition holds, else `RejectedAdmittingConnections { status, held_connections }`; the path/name pair is valid and the name has no dot, else `RejectedName { reason }`. The supplied path must equal the registered immutable path; a different path is refused by name |
| Check and move | The mesh-primary checks the destination pair against the node records; held by another node it refuses `NameHeld { by: node.id }` and nothing is written. Then the node writes its own record at its node.id with the destination name (Q356): the name move is that one write |
| Order | Check first, the node's write second, then the live node's rejoin |
| Rejoin | A live node, once it holds the new name, is commanded [node.mesh.join](node-mesh-join.md) by its mesh-primary and rejoins its mesh under the new name. A stopped node rejoins at its [node.start](node-start.md) |
| Completion | [By case](#completion-by-case) |
| Idempotency | Row-held and node-applied are two facts. A repeat naming the name the row already holds resends `ApplyNodeName`, and for a live node its rejoin, and completes as the first command does. A repeat with a different name is a new rename |
| Wire | `UpdateName { node_id, incarnation, name: { path, name } }` and `ApplyNodeName { node_id, incarnation, name: { path, name } }` on [`0x1B`](../node-rpc-envelope.md#status-op-0x1b-rdm), then `JoinMesh` on `0x1B` for a live node; replies `NameUpdated`, `NameHeld`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`, `BusyHoldingConnections { connections }`, `NameNotApplied` |

## Numbered end-to-end sequence

1. The caller sends `node.name.update` to the node's mesh-primary; an admin that is not the fabric-primary carries it to the fabric-primary (Forward `0x1A` when not directly reachable) and carries the answer back. The fabric-primary sends the command to the node's mesh-primary (Forward `0x1A` when not directly reachable) and returns its completion without installing a copied node row.
2. The mesh-primary validates from its view and the node's record: exact birth, that it is the node's mesh-primary, the precondition from the node's `state` and its `node.connection.deleted` receipt, the name grammar.
3. The mesh-primary checks the destination pair against the node records; a pair held by another node refuses `NameHeld { by }` with nothing written.
4. The node is sent `ApplyNodeName`. It writes its own record at its node.id with the new name (Q356), the one durable name move, and only once that write is acked does it hold the new name and publish it.
5. It answers `Applied`, `AlreadyApplied` for a name it already holds, or `BusyHoldingConnections { connections }` naming each connection it still holds, with nothing written; a write the store refuses is answered `NodeRecordRefused`. `Busy` is the interrupted case under [Failure and receipts](#failure-and-receipts).
6. A live node rejoins its mesh under the new name: the mesh-primary commands its
   [node.mesh.join](node-mesh-join.md), the node reports under the new name and joins its mesh again,
   and answers `MeshJoined` once it is back on its mesh. A stopped node rejoins at its start.
7. `node.name.updated` is emitted [by case](#completion-by-case).

The node owns its name: it is the owner of the name it publishes, and what it publishes is the
row's name. A node never renames itself.

## Completion by case

- **The live node:** `node.name.updated` after `Applied` or `AlreadyApplied` and its `MeshJoined`:
  the node is back on its mesh, publishing the new name (step 6).
- **The stopped node:** `node.name.updated` after `Applied` or `AlreadyApplied`; its start rejoins it
  under the name.
- **Row written, apply or rejoin unresolved** (no answer, `Busy`, or a join that did not answer
  `MeshJoined`): the command ends incomplete, `NameNotApplied`, naming the row's new name and how the
  apply or the rejoin ended. The name has moved durably and the node will publish it at its next
  apply or join; the caller resubmits, the resubmit resends `ApplyNodeName` and the rejoin, and
  completes on the node's answers, never from the row. A live node is never reported renamed on the
  row alone.

## Command and completion messages

```text
node.name.update
  → validate → check the new pair against the node records
  → ApplyNodeName (the node, holding no connection, writes its record with the new name, then publishes it)
  → node.mesh.join (a live node: it rejoins its mesh under the new name)
  ← node.name.updated
```

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant M as Mesh-primary
    participant N as Node
    C->>M: node.name.update (exact birth, name)
    Note over M: validate, check the pair against the node records
    M->>N: ApplyNodeName (no connection held)
    Note over N: write its record with the new name, then publish
    N-->>M: Applied (name held and published)
    M->>N: JoinMesh (a live node)
    N->>M: JoinNode under the new name
    M-->>N: Joined
    N-->>M: MeshJoined (back on its mesh)
    M-->>C: node.name.updated
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

- **Refused before the write** (stale birth, wrong receiver, admitting connections, bad name, `NameHeld`):
  nothing written, nothing changed.
- **The record write fails**: a named storage error; the record stands under the old name, and a retry
  writes it.
- **Apply or rejoin fails or is interrupted after the row**: the incomplete case [above](#completion-by-case).
  The admission block stays in force on the node until it rejoins its mesh, so no connection is
  opened under the old name while the row carries the new one; a retry of the same command resends
  `ApplyNodeName` and the rejoin and completes on the node's answers.
- **Connection opened between the precondition check and the apply**: impossible by the admission
  block; a node that reports `Busy` is a defect in that block, named by the connections it lists.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans). The mesh-primary's
`rdm.node_admin.name.update.via-writer` span records `connections` (`deleted` or `stopped` when
admitted), `apply` and `answered`, and `rejoin` (`mesh-joined`, `at-start`, or how the join ended);
the rejoin is its `rdm.node_admin.node.update.via-mesh-join-commanded` span, `cause = node.name.update`,
in the same trace.
