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

A node that comes back after it was re-created is a deleted birth, and the view is its door: every
node refuses its Digest by name ([gossip.md §3.1 receiver rules](../gossip.md#31-frame-catalog)), its
mesh-primary reports it to the fabric-primary (`rdm.node_admin.node.update.via-returned-reported`), and
the fabric-primary terminates its runtime under `node.terminate:<path>@<incarnation>`. No admit reason
exists for it, and none is added. A second birth another executor made at the path.name across a cut is
made a deleted birth by its mesh-primary, which writes its row deleted ([node.delete](node-delete.md),
releasing the name) and reports it the same way (Q142).

## Operation contract

| Field | Contract |
|---|---|
| Caller | The primary above the node: its mesh-primary for a member of any type, the fabric-primary for a mesh-primary or another mesh's node-admin |
| Target | One node, by `(node.id, incarnation)` |
| Steps | The by-destination connections index read, node.tickle direct, the same tickle via those peers, node.runtime.inspect |
| Result | `reached`, `running`, `os-stopped`, `exited` or `unproven` |
| What follows | Reached: node.mesh.join. Any other verdict: re-created at the same path.name with a new node.id, over the inherited data dir when the verdict is `exited`, over a newly allocated one otherwise |
| Spans | [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own |
