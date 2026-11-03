# node investigation

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [node.tickle](node-tickle.md) · [node.runtime.inspect](node-runtime-inspect.md) · [Connections](../connections.md#31-declared-indexes)

A primitive. Node investigation is the one function every caller uses on one exact birth, ending in
one verdict: `reached`, `running`, `os-stopped`, `exited` or `unproven`. Its steps, in order:

1. Read the by-destination connections index ([connections.md §3.1](../connections.md#31-declared-indexes)).
   It names the peers to try; it never shows the node is reachable.
2. [node.runtime.inspect](node-runtime-inspect.md). `exited` or `os-stopped` is the verdict; no tickle is sent.
3. [node.tickle](node-tickle.md), direct.
4. The same tickle via those peers. Unreached, the verdict is the inspect's answer: `running`, or `unproven` for `unavailable`.

Only an answer from the exact birth counts as `reached`.

A lost mesh-primary is investigated by the successor its election names, as any member of its
mesh ([node recovery](node-recovery.md) step 2; Q335). When the mesh's aggregate stays unheard past the staleness floor (no successor took the role), the
fabric-primary investigates it the same way, and the re-created node-admin is that mesh's primary by
the same election.

A node that does not answer a call is silent, whatever it gossips (Q182): a call whose attempts end
unanswered marks its birth at the primary above it (the caller itself when it is that primary; any
other caller reports the birth to it with Status `ReportCallFailed`), and the primary investigates
that birth as it investigates silence ([node recovery](node-recovery.md) step 2).

The verdict is the one decision (Q111). The primary that investigated writes it as one `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, verdict and what follows ([build.md §5](../build.md#5-buildstateadapter)); an investigation is never an entity of its own. Reached: the node is commanded back through
[node.mesh.join](node-mesh-join.md). Any other verdict: it is re-created at the same path and name with a
new node.id (Q110). Fabric recovery's backbone arm excepts `running`
([fabric recovery](fabric-recovery.md) step 3). A wrong verdict is fixed in the investigation.

Nothing waits on the provider's `Exited`: not node recovery, a primary change, a name's release,
node.delete or mesh.delete.

Data dir: if the provider step finds the runtime exited, the re-created node takes the inherited data
dir; on any other verdict, a newly allocated one (R-FM2).

A caller birth a primary does not hold (its Digest refused by name under [gossip.md §3.1 receiver
rules](../gossip.md#31-frame-catalog), its status declaration refused `SubjectUnknown`, or its join report refused `JoinMismatch` because the row at its path and name holds another node) is decided by
its path and name and the primary's atlas, one decision for the fabric-primary and every mesh-primary
(Q207): another birth at that path and name now, or an op executing that re-creates it there, deletes the
caller; nothing there, its record live and the topology needing the path re-admits it through
[node.mesh.join](node-mesh-join.md); nothing there and the topology not needing it deletes it. A
caller whose record is deleted is deleted, its runtime terminated, never re-admitted; a path still
needed is recovered by re-create (Q217). A
node-admin's atlas is its topology and its in-flight commands over the meshes it is primary for, its own
mesh as a mesh-primary and every mesh as the fabric-primary, taken as it takes that primary and narrowed off as it hands it away; the caller's record and the record at its path and name are read through the current-pair lookup ([query.md §1](../query.md#1-external-a-query-to-the-fabric-primary)). A node-admin that is not the fabric-primary
verifies its decision at the root with `fabric.atlas.get` before it acts, and the root's answer wins, except that a caller whose record it holds deleted stays deleted (Q217). A
deleted caller's runtime is terminated by the primary whose atlas decided it, its own workflow, under `node.terminate:<path>@<incarnation>` (`rdm.node_admin.node.update.via-returned-reported`); each decision is one
`rdm.node_admin.node.update.via-refused-caller` span. A node that comes back once its verdict is not `reached`, during its replacement's birth or after it, is
such a caller: its recovery or its re-created birth holds its path and name, and it is deleted. A second birth another
executor made at the path and name across a cut is made a deleted birth by its mesh-primary, which writes its
row deleted ([node.delete](node-delete.md), releasing the name) and reports it the same way (Q142).

## Operation contract

| Field | Contract |
|---|---|
| Caller | The primary above the node: its mesh-primary for a member of any type, the fabric-primary for a mesh-primary or another mesh's node-admin |
| Target | One node, by `(node.id, incarnation)` |
| Steps | The by-destination connections index read, node.runtime.inspect, node.tickle direct, the same tickle via those peers |
| Result | `reached`, `running`, `os-stopped`, `exited` or `unproven` |
| What follows | Reached: node.mesh.join. Any other verdict: re-created at the same path and name with a new node.id, over the inherited data dir when the verdict is `exited`, over a newly allocated one otherwise |
| Spans | [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own |
