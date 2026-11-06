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
mesh ([node recovery](node-recovery.md) step 2; Q335). When the mesh's aggregate stays unheard for twice the staleness floor (60 s at the default floor; no successor took the role, Q349), the
fabric-primary investigates it the same way, and the re-created node-admin is that mesh's primary by
the same election. A lost fabric-primary is investigated from outside its mesh, by a mesh-primary, only once it has been unheard for twice the staleness floor ([fabric recovery](fabric-recovery.md) step 2, Q349).

A node that does not answer a call is silent, whatever it gossips (Q182): a call whose attempts end
unanswered marks its birth at the primary above it (the caller itself when it is that primary; any
other caller reports the birth to it with Status `ReportCallFailed`), and the primary investigates
that birth as it investigates silence ([node recovery](node-recovery.md) step 2).

The verdict is the one decision (Q111). The primary that investigated writes it as one `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, verdict and what follows ([build.md §5](../build.md#5-buildstateadapter)); an investigation is never an entity of its own. Reached: the node is commanded back through
[node.mesh.join](node-mesh-join.md). Any other verdict: it is re-created at the same path and name with a new node.id (Q110). `running` from an investigator that hears no other node (every other birth it holds is unheard past the floor, so it is the one cut off) is unreachable from that investigator, not lost: nothing is written, and the next investigation decides again (Q153). Fabric recovery's backbone arm excepts `running` ([fabric recovery](fabric-recovery.md) step 3). A wrong verdict is fixed in the investigation.

Nothing waits on the provider's `Exited`: not node recovery, a primary change, a name's release,
node.delete or mesh.delete.

Data dir: if the provider step finds the runtime exited, the re-created node takes its predecessor's data dir when its `storage_persistence` is `persistent` and the directory exists; otherwise, and on any other verdict, a newly allocated one (Q359). Its `endpoint_id` and `transport_addr` are always new (Q358).

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
`rdm.node_admin.node.update.via-refused-caller` span. A node that comes back once its verdict is not `reached`, during its successor's creation or after it, is
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
| What follows | Reached: node.mesh.join. Any other verdict (except `running` from an investigator that hears no other node, Q153): re-created at the same path and name with a new node.id, over the predecessor's data dir when the verdict is `exited`, its storage `persistent` and the directory there, over a newly allocated one otherwise |
| Spans | [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own |

## States written

This operation writes no state. The primary that investigated writes its verdict as one `rdm.node.investigated` line in `events.jsonl`, `subject` the node.id.

| written | by | when |
|---|---|---|
| `rdm.node.investigated` | the primary that investigated | the verdict is decided, except `running` from an investigator that hears no other node, which writes nothing |

As stored:

`running`:

```json
{"len":479,"specversion":"1.0","id":"01a1ae8d-f7ff-78e6-ae7a-39a947f345d3","source":"rdm://1x5fjsz93tvg/qn3q564n8162/e17kbs6jpr37","type":"rdm.node.investigated","subject":"2f24nqm969pk","time":"2026-11-06T01:51:42.078Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-f6d90ec5f0388ba0a08e7bd700fdcfbc-2af7920987bef87b-01","data":{"incarnation":"sj4z3ysdhycf","name":"2","next":"recreate","path":"mesh2.node","verdict":"running"},"crc32":"92311d5d"}
```

`exited`:

```json
{"len":478,"specversion":"1.0","id":"01a1ae8e-8303-7b68-a51e-7c18a3cda9d3","source":"rdm://r15rc3tfkg4r/hreemf4h5nvz/81sve0k0m8tj","type":"rdm.node.investigated","subject":"sxq64k0v26ft","time":"2026-11-06T01:52:17.663Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-c8bb95b7d2b4fe23b91c4599eb7e3aef-2966089bcad66d63-01","data":{"incarnation":"tejwha92fdsb","name":"3","next":"recreate","path":"mesh1.node","verdict":"exited"},"crc32":"8be0c776"}
```

`os-stopped`:

```json
{"len":482,"specversion":"1.0","id":"01a1ae8d-e0cc-7129-9be0-4c3bdb204341","source":"rdm://3fe2gyr0vby2/3mfm6ref1hfp/2237s0fb95jf","type":"rdm.node.investigated","subject":"9mn1y1x703ft","time":"2026-11-06T01:51:36.138Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-c493098f704d67cd64d1d14876e49e4a-abb790c9e3f739f1-01","data":{"incarnation":"psc6we56fj63","name":"1","next":"recreate","path":"mesh2.node","verdict":"os-stopped"},"crc32":"b88c01db"}
```
