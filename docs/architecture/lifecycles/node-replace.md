# Node replace

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

Replace is Rafka's steps, submitted as one Build. RDM executes its primitives
and records their receipts; it has no replace opcode or knowledge of Rafka data.
A Build provides durable resumable execution, not automatic rollback.

## Operation contract

| Field | Contract |
|---|---|
| Steps owner | Rafka |
| Precondition | The Day-0 Build is Complete. Before then it is the Build in progress, the steps' Build is refused by name, `build-in-progress` (Q170), and nothing runs |
| Writer | The node's [row writer](../node-identity-and-state.md#25-one-system-org-one-projection-one-writer-per-mesh) writes every row of these steps |
| Successor | Created at `<name>` once the predecessor has released it: a re-birth, with a new node.id, incarnation and runtime; new port; key from its specified data dir; its `Create` step names its executable ([build-deployment.md](../build-deployment.md)) |
| Predecessor | Its connections deleted, it leaves its mesh, then it is renamed `<name>.old` and stopped, or deleted |
| Immutable exe | New bin/budgets require this new node, never a start of the stopped one |
| Inherited | Config and tags are inherited from the retained `<name>.old` row, or from the Deleted predecessor row at `<name>` when the predecessor was deleted. The successor's tag-index entries use its own id. Retained predecessor labels are removed from the active index, not erased from its row. A repeated cutover keeps the successor's already-inherited values and never overwrites a later update. Nothing is copied later ([node properties](../node-rpc-envelope.md#node-properties)) |
| Data transfer | Rafka's own step; preserve its declared move/copy and commit boundary |
| Name ownership | [The node owns its name](../node-identity-and-state.md#22-the-node-pathname-is-what-placement-stores--the-endpoint_id-is-resolved-at-dial-time) |
| Name change | [node.name.update](node-name-update.md): claim, row first, node second, only under its rename precondition |
| Rollback | Explicit Rafka steps while the predecessor is stopped as `<name>.old` and its data remains available |

## Numbered end-to-end sequence

A node is renamed only while it is live: its connections deleted and its mesh left first, so nothing
is connected to it and nothing routes to it; it is stopped after. A stopped node is never renamed.

1. node[1]: [node.connection.delete](node-connections-delete.md) `{ scope: All }`, then
   [node.mesh.leave](node-mesh-leave.md).
2. [node.name.update](node-name-update.md) of node[1] to `<name>.old`; node.name.updated is the
   step's receipt. Then node[1].stop runs the complete [stop](node-stop.md) and leaves it stopped as
   `<name>.old`, with its incarnation and data retained. If no rollback is wanted, node[1].delete
   releases the identity instead of the rename and the stop.
3. [node.create](node-create.md) of node[2] at `<name>`, free only because the row above moved the
   key: a re-birth with a new node.id, incarnation and the specified bin. The Build fixes the bin on its
   Create step, so every admin that runs or resumes the step launches that one, and the row
   records it beside `config`. Its row inherits config and
   tags from the predecessor row (the retained `<name>.old` row, or the Deleted row at `<name>`),
   whose labels leave the active tags index. Its ready is the cutover: `path.name` now
   resolves to the successor. A claim that finds `<name>` held is refused `NameHeld { by: node[1] }`
   and the Build stops at this step by name.
4. Rafka transfers its data as its own step. RDM interprets no application data.
   The steps declare when a move ends rollback, or when a copy is reclaimed on commit.

## Rollback

While node[1] is stopped as `<name>.old` and its data remains available:

1. node[2]: node.connection.delete `{ scope: All }`, node.mesh.leave, then node[2].delete releases
   `<name>` after its node.delete receipt. Any wanted successor data is Rafka's to copy out first.
2. node[1].start rejoins the same stopped process with AfterStop under `<name>.old`: same
   incarnation, endpoint key, port and bin. Complete its full hook and readiness gates.
3. node[1]: node.connection.delete `{ scope: All }`, node.mesh.leave, then
   [node.name.update](node-name-update.md) to `<name>` (its tag-index entries restored under its
   own id), then node[1].stop and node[1].start under `<name>`: its start's
   [node.mesh.join](node-mesh-join.md) rejoins it, and its ready ends the rollback.

No general rollback is inferred from Build durability. A missing or failed receipt
leaves the Build at the named step; resume or run the explicit compensation steps.

## Command, completion and gossip messages

```text
forward:
  node.connection.delete(node[1], All) → node.connection.deleted
  node.mesh.leave(node[1]) → node.mesh.left
  node.name.update(node[1], name.old) → row written, applied → node.name.updated
  node[1].stop → every stop event
    or node[1].delete → node.deleted
  node.create(name) → every FirstStart event, config and tags inherited, node.created
  Rafka data-transfer step
rollback:
  node.connection.delete(node[2], All), node.mesh.leave(node[2]), node[2].delete → node.deleted
  node[1].start → every AfterStop event, joined as name.old
  node.connection.delete(node[1], All), node.mesh.leave(node[1])
  node.name.update(node[1], name.old → name) → node.name.updated
  node[1].stop → every stop event
  node[1].start → every AfterStop event, joined as name
```

Stop publishes its mesh-primary NodeStopped overlay; delete publishes NodeDeleting and
NodeDeleted. The successor's digest under `<name>` is the only
new birth peers hear, and it is dissemination: the row is the fact, the apply's
reply is the receipt ([node.name.update](node-name-update.md)). The steps' caller acts on streamed events and receipts, never on
peer gossip.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant R as Rafka steps
    participant F as Fabric-primary
    participant M as Mesh-primary (executor, writes rows)
    participant N1 as node[1]
    participant N2 as node[2]
    R->>F: build.create(replace steps)
    F->>M: build.attempt.run
    M->>N1: node.connection.delete All, then node.mesh.leave
    M-->>F: node.connection.deleted, node.mesh.left
    M->>N1: node.name.update to name.old (row first, then applied)
    M-->>F: node.name.updated
    M->>N1: node[1].stop
    M-->>F: stop events
    Note over M: node.create(name): a re-birth, inheriting config and tags from the predecessor row
    M-->>F: FirstStart events; ready is the cutover
    Note over R: Rafka data-transfer step and commit boundary
    F-->>R: step frames, Complete
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
