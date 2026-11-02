# Node name update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.name.update` renames a node. It is a single call, not a lifecycle operation: it moves no status
and is not a Build step by itself, though the [replace](node-replace.md) steps call it as a
step. A node's other properties have their own commands ([node properties](../node-rpc-envelope.md#node-properties));
[node.config.update](node-config-update.md) replaces its application config.

## Operation contract

| Field | Contract |
|---|---|
| Caller / executor | Caller to the node's mesh-primary, the only writer of its mesh's rows (Forward `0x1A` when not directly reachable). Before a mesh has a primary, the fabric-primary acts as its writer. The replace steps call it as a step; a caller may call it alone |
| Entry | The exact `(node.id, incarnation)` and unchanged registered path and new name as separate fields. A different path is refused; mesh, kind and tags do not change |
| Precondition | The node is live and admits no connection: this mesh-primary holds the birth's `node.connection.deleted` of an `All`, then its `node.mesh.left` ([node.mesh.leave](node-mesh-leave.md)), and it holds zero connections. A stopped node is never renamed; a ready node, or one still holding a connection, is refused. No new connection is admitted from the row write through `ApplyNodeName`, retries after an interruption included |
| Validation | The exact birth matches the held record, else `RejectedStaleIncarnation { held }`; the receiver is the node's writer, else `ReceiverNotPrimary { needed }` (R-RX5); the precondition holds, else `RejectedAdmittingConnections { status, held_connections }`; the path/name pair is valid and the name has no dot, else `RejectedName { reason }`. The supplied path must equal the registered immutable path; a different path is refused by name. Replacement renames the predecessor to `<name>-old` at that same path; rollback restores its name |
| Check and move | The mesh-primary checks the destination pair against the records it holds; held by another node it refuses `NameHeld { by: node.id }` and nothing is written. Then one write of the node's record at its node.id (Q323) carrying the destination pair and `renamed_from: <old pair>`: the name move is that one write. A retry, or a successor writer resuming the command, reads the record: one that already carries the destination pair is the move done, and it continues to the apply |
| Order | Row first, node second |
| Completion | [By case](#completion-by-case) |
| Idempotency | Row-held and node-applied are two facts. A repeat naming the name the row already holds finishes any pending release and then, for a live node, resends `ApplyNodeName` and completes on `Applied` or `AlreadyApplied`. A repeat with a different name is a new rename |
| Wire | `UpdateName { node_id, incarnation, name: { path, name } }` and `ApplyNodeName { node_id, incarnation, name: { path, name } }` on [`0x1B`](../node-rpc-envelope.md#status-op-0x1b-rdm); replies `NameUpdated`, `NameHeld`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`, `BusyHoldingConnections { connections }` |

## Numbered end-to-end sequence

1. The caller sends `node.name.update` to the node's mesh-primary; an admin that is not the fabric-primary carries it to the fabric-primary (Forward `0x1A` when not directly reachable) and carries the answer back. The fabric-primary sends the command to the node's mesh-primary (Forward `0x1A` when not directly reachable) and returns its completion without installing a copied node row.
2. The mesh-primary validates from the record it holds: exact birth, its own writer role, the
   precondition from the node's status and its `node.connection.deleted` and `node.mesh.left` receipts, the name grammar.
3. The mesh-primary checks the destination pair against its records; a pair held by another node
   refuses `NameHeld { by }` with nothing written.
4. The mesh-primary writes the row with the new name and `renamed_from`. This one write is the
   durable name move.
5. The left node is sent `ApplyNodeName`. It holds the new name and, off its mesh, publishes nothing
   ([node.mesh.leave](node-mesh-leave.md)): its stop or its next join carries the name. It answers
   `Applied`, `AlreadyApplied` for a name it already holds, or
   `BusyHoldingConnections { connections }` naming each connection it still holds; `Busy` after the row is the
   interrupted case under [Failure and receipts](#failure-and-receipts).
6. `node.name.updated` is emitted [by case](#completion-by-case).

The node owns its name: it is the owner of the name it publishes, and what it publishes is the
row's name. A node never renames itself.

## Completion by case

- **The left node:** `node.name.updated` after `Applied` or `AlreadyApplied` (step 5).
- **Row written, apply unresolved** (no answer, or `Busy`): the command ends incomplete, naming the
  row's new name and the apply's state. The name has moved durably and the node will publish it at
  its next apply or join; the caller resubmits, the resubmit resends `ApplyNodeName`, and completes on
  `Applied` or `AlreadyApplied` from the node, never from the row. A live node is never reported
  renamed on the row alone.

## Command and completion messages

```text
node.name.update
  → validate → check the new pair against the records → put the node's record at its node.id with the new pair (renamed_from)
  → ApplyNodeName (the live node that has left its mesh)
  ← node.name.updated
```

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant M as Mesh-primary
    participant N as Node
    C->>M: node.name.update (exact birth, name)
    Note over M: validate from held record, check the pair, put row (renamed_from)
    M->>N: ApplyNodeName (live, left its mesh, no connection held)
    N-->>M: Applied (name held; nothing published off its mesh)
    M-->>C: node.name.updated
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

- **Refused before the write** (stale birth, wrong receiver, admitting connections, bad name, `NameHeld`):
  nothing written, nothing changed.
- **The record write fails**: a named storage error; the record stands under the old name, and a retry
  writes it.
- **Apply fails or is interrupted after the row**: the incomplete case [above](#completion-by-case).
  The admission block stays in force on the node until the apply resolves, so no connection is
  opened under the old name while the row carries the new one; a retry of the same command resends
  `ApplyNodeName` and completes on the node's answer.
- **Connection opened between the precondition check and the apply**: impossible by the admission
  block; a node that reports `Busy` is a defect in that block, named by the connections it lists.

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
