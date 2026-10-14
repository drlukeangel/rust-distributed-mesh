# Node replace

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

Replace is Rafka's recipe, submitted as one Build. RDM executes its primitives
and records their receipts; it has no replace opcode or knowledge of Rafka data.
A Build provides durable resumable execution, not automatic rollback.

## Operation contract

| Field | Contract |
|---|---|
| Recipe owner | Rafka |
| Precondition | The fabric has first reached ready-for-traffic. Before then the recipe's Build is refused by name, `fabric-not-ready`, and nothing runs ([States](states.md#fabric-not-ready)) |
| Writer | The node's [row writer](../node-identity-and-state.md#25-one-system-org-one-projection-one-writer-per-mesh) writes every row of this recipe |
| Successor | Born first as `<name>.new`: new node.id, incarnation and runtime; new port; key from its specified data dir; Ready, and nothing routes to it; it is stopped before it takes the name and started under it |
| Predecessor | Stopped, then renamed to `<name>.old`, or deleted |
| Immutable exe | New bin/budgets require this new node, never a restart of the stopped one |
| Inherited | Config and tags are inherited from the retained `<name>.old` row, or from the Deleted predecessor row at `<name>` when the predecessor was deleted. The successor's tag-index entries use its own id. Retained predecessor labels are removed from the active index, not erased from its row. A repeated cutover keeps the successor's already-inherited values and never overwrites a later update. Nothing is copied later ([node properties](../node-rpc-envelope.md#node-properties)) |
| Data transfer | Rafka's own step; preserve its declared move/copy and commit boundary |
| Name ownership | [The node owns its name](../node-identity-and-state.md#22-the-node-pathname-is-what-placement-stores--the-endpoint_id-is-resolved-at-dial-time) |
| Name change | [node.name.update](node-name-update.md): claim, row first, node second, only under its rename precondition |
| Rollback | Explicit Rafka steps while the predecessor is stopped and its data remains available |

## Numbered end-to-end sequence

1. [node.create](node-create.md) creates the successor as `<name>.new` with a new
   node.id, incarnation and the specified bin. The full FirstStart workflow runs to
   Ready. Nothing routes to `<name>.new`, so it carries no traffic.
2. node[1].stop runs the complete [stop](node-stop.md) and leaves the predecessor
   stopped, with its incarnation and data retained. Stop alone never frees the name.
3. [node.name.update](node-name-update.md) of node[1] to `<name>.old` on the stopped predecessor;
   node.name.updated is the step's receipt. If the predecessor need not
   remain for data/rollback, node[1].delete releases the identity instead.
4. node[2].stop; then [node.name.update](node-name-update.md) of node[2] to `<name>` on the stopped
   successor, which is sent nothing and reads the name at its join; `<name>` is free only because
   the row above moved the key. The claim of `<name>` is the name move's first write, and the
   successor's row there inherits config and tags from the predecessor row (the retained `<name>.old`
   row, or the Deleted row at `<name>`), whose labels leave the active tags index; then node[2].start
   under `<name>`. The cutover is its ready-for-traffic:
   `path.name` now resolves to the successor. A claim that finds `<name>` held is refused `NameHeld { by: node[1] }` and
   the Build stops at this step by name.
5. Rafka transfers its data as its own recipe step. RDM interprets no application data.
   The recipe declares when a move ends rollback, or when a copy is reclaimed on commit.

## Rollback

While node[1] is stopped and its data remains available:

1. node[2].stop, so the successor has no traffic. Then node.name.update renames node[2] from `<name>`
   back to `<name>.new`, or node[2].delete releases the name after exact runtime exit
   proof. Any wanted successor data is Rafka's to copy out first.
2. [node.name.update](node-name-update.md) renames node[1] from `<name>.old` back to `<name>` on the
   stopped node; the predecessor's tag-index entries are restored under its original id.
3. node[1].start rejoins the same stopped process with AfterStop: same incarnation,
   endpoint key, port and bin; its join answer hands it `<name>`. Complete its full hook
   and readiness gates.

No general rollback is inferred from Build durability. A missing or failed receipt
leaves the recipe at the named step; resume or run the explicit compensation steps.

## Command, completion and gossip messages

```text
forward:
  node.create(<name>.new) → every FirstStart event, node.created
  node[1].stop → every stop event
  node.name.update(node[1], name.old) → row written → node.name.updated
    or node[1].delete → node.stopped, node.deleted
  node[2].stop → every stop event
  node.name.update(node[2], name) → row written, config and tags inherited → node.name.updated
  node[2].start → every AfterStop event, joined as name
  Rafka data-transfer step
rollback:
  node[2].stop → every stop event
  node.name.update(node[2], name → name.new) → node.name.updated, or node[2].delete → node.stopped, node.deleted
  node.name.update(node[1], name.old → name) → node.name.updated
  node[1].start → every AfterStop event
```

Stop publishes its mesh-primary NodeStopped overlay; delete publishes NodeDeleting and
NodeDeleted. The successor's digest under `<name>` is the only
new thing peers hear, and it is dissemination: the row is the authority, the apply's
reply is the receipt ([node.name.update](node-name-update.md)). The recipe's caller acts on streamed events and receipts, never on
peer gossip.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant R as Rafka recipe
    participant F as Fabric-primary
    participant M as Mesh-primary (executor, writes rows)
    participant N1 as node[1]
    participant N2 as node[2]
    R->>F: build.create(replace recipe)
    F->>M: build.attempt.run
    Note over M: node.create(<name>.new)
    M-->>F: start events, then node.created
    M->>N1: node[1].stop
    M-->>F: stop events
    Note over M: node[1] to name.old (node.name.update); N1 stopped, sent nothing
    M-->>F: node.name.updated
    M->>N2: node[2].stop
    M-->>F: stop events
    Note over M: node[2] to name (node.name.update), inherit config and tags from the predecessor row; N2 stopped, sent nothing
    M-->>F: node.name.updated
    M->>N2: node[2].start (joins as name)
    M-->>F: AfterStop events; ready-for-traffic is the cutover
    Note over R: Rafka data-transfer step and commit boundary
    F-->>R: step frames, Complete
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
