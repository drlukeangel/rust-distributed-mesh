# Proposal: the node investigation page (Q107 item 1, Q110, Q111; order item (a))

For Luke's yes on the exact text. Every sentence below is Luke's from the RULING FOR DEV §2 (Q110
`88f4ecd1`, Q111 `dbc35dd0`, `b92d1c18`) and Q107 (2), or a page's own words; nothing else is new.
On a yes, the page lands with the code that runs it (the primitive replaces the tickle ladders in
`offline.rs` and the recovery workflow, and the inspect copies in `seat_watch`, the runner, recovery
and `seat_revision`), and the (c) page lines that point to it follow in their own text for his yes.

## New page: `docs/architecture/lifecycles/node-investigation.md`

```markdown
# node investigation

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [node.tickle](node-tickle.md) · [node.runtime.inspect](node-runtime-inspect.md) · [Connections](../connections.md#31-declared-indexes)

A primitive. Node investigation is the one function every caller uses on one exact birth, ending in
one verdict: `reached`, `running`, `os-stopped`, `exited` or `unproven`. Its steps, in order:

1. Read the by-destination connections index ([connections.md §3.1](../connections.md#31-declared-indexes)).
   It names the peers to try; it never proves the node is reachable.
2. [node.tickle](node-tickle.md), direct.
3. The same tickle via those peers.
4. [node.runtime.inspect](node-runtime-inspect.md).

Only an answer from the exact birth counts as `reached`.

The verdict is the one decision. Reached: the node is commanded back through
[node.mesh.join](node-mesh-join.md). Any other verdict: it is re-created at the same path.name with a
new node.id. A wrong verdict is fixed in the investigation.

Nothing waits on a proof of termination: not node recovery, a seat move, a name's release, the hung
hold, node.delete or mesh.delete.

Data dir: if the provider step finds the runtime exited, the re-created node takes the inherited data
dir; on any other verdict, a newly allocated one (R-FM2).

A node that comes back after it was re-created is refused at
[node.connections.admit](node-connections-admit.md) (`superseded-birth`). The node that refused it
reports it to the fabric-primary, which terminates its runtime.

## Operation contract

| Field | Contract |
|---|---|
| Caller | The authority above the node: its mesh-primary for a member, the fabric-primary for a mesh-primary or another mesh's node-admin |
| Target | One node, by `(node.id, incarnation)` |
| Steps | The by-destination connections index read, node.tickle direct, the same tickle via those peers, node.runtime.inspect |
| Result | `reached`, `running`, `os-stopped`, `exited` or `unproven` |
| What follows | Reached: node.mesh.join. Any other verdict: re-created at the same path.name with a new node.id, over the inherited data dir when the verdict is `exited`, over a newly allocated one otherwise |
| Spans | [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own |
```

## Index line: `docs/architecture/fabric-mesh-node-lifecycles.md`

Added to the primitives' table, beside node.tickle and node.runtime.inspect:

```markdown
| [node investigation](lifecycles/node-investigation.md) (primitive) | The one verdict on one exact birth: the by-destination connections index read, node.tickle direct and via those peers, node.runtime.inspect; reached is commanded back through node.mesh.join, any other verdict is re-created at the same path.name with a new node.id |
```

## Not in this text

- The words `running` and `os-stopped` keep their node.runtime.inspect meaning; `unproven` is the
  inspect's `unavailable { reason }`, the word the elections page already uses for it.
- Every page line that requires proof of termination (128 lines, the removal inventory `cbe7d824`
  §B) is (c)'s, in its own text for Luke's yes once this page stands.
