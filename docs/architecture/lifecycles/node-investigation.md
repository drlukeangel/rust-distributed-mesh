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

A node that does not answer a command is silent, whatever it gossips (Q182): an owed command it
leaves unanswered past the staleness floor starts node investigation on its birth as silence does,
and a tickle answered by a birth that has left a command unanswered past the floor is not `reached`.

The verdict is the one decision (Q111). Reached: the node is commanded back through
[node.mesh.join](node-mesh-join.md). Any other verdict: it is re-created at the same path.name with a
new node.id (Q110). Fabric recovery's backbone arm excepts `running`
([fabric recovery](fabric-recovery.md) step 3). A wrong verdict is fixed in the investigation.

Nothing waits on a proof of termination: not node recovery, a primary change, a name's release,
node.delete or mesh.delete.

Data dir: if the provider step finds the runtime exited, the re-created node takes the inherited data
dir; on any other verdict, a newly allocated one (R-FM2).

A caller birth a primary does not hold (its Digest refused by name under [gossip.md §3.1 receiver
rules](../gossip.md#31-frame-catalog), its status declaration refused `SubjectUnknown`, or its join report refused `JoinMismatch` because the row at its path.name holds another node) is decided by
its path.name and the primary's atlas, one decision for the fabric-primary and every mesh-primary
(Q207): another birth at that path.name now, or an op executing that re-creates it there, deletes the
caller; nothing there, its record live and the topology needing the path re-admits it through
[node.mesh.join](node-mesh-join.md); nothing there and the topology not needing it deletes it. A
caller whose record is deleted is deleted, its runtime terminated, never re-admitted; a path still
needed is recovered by re-create (Q217). A
node-admin's atlas is its topology and its in-flight commands over the meshes it is primary for, its own
mesh as a mesh-primary and every mesh as the fabric-primary, each filled once from the node records as
it takes that primary and narrowed off as it hands it away. A node-admin that is not the fabric-primary
verifies its decision at the root with `fabrics.atlas.get` before it acts, and the root's answer wins, except that a caller whose record it holds deleted stays deleted (Q217). A
deleted caller is reported to the fabric-primary (`rdm.node_admin.node.update.via-returned-reported`),
which terminates its runtime under `node.terminate:<path>@<incarnation>`; each decision is one
`rdm.node_admin.node.update.via-refused-caller` span. A node that comes back after it was re-created is
such a caller: its re-created birth is at its path.name, and it is deleted. A second birth another
executor made at the path.name across a cut is made a deleted birth by its mesh-primary, which writes its
row deleted ([node.delete](node-delete.md), releasing the name) and reports it the same way (Q142).

## Operation contract

| Field | Contract |
|---|---|
| Caller | The primary above the node: its mesh-primary for a member of any type, the fabric-primary for a mesh-primary or another mesh's node-admin |
| Target | One node, by `(node.id, incarnation)` |
| Steps | The by-destination connections index read, node.tickle direct, the same tickle via those peers, node.runtime.inspect |
| Result | `reached`, `running`, `os-stopped`, `exited` or `unproven` |
| What follows | Reached: node.mesh.join. Any other verdict: re-created at the same path.name with a new node.id, over the inherited data dir when the verdict is `exited`, over a newly allocated one otherwise |
| Spans | [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own |
