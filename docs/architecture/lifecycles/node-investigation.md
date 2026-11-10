# node investigation

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [node.tickle](node-tickle.md) · [node.runtime.inspect](node-runtime-inspect.md) · [Connections](../connections.md#31-declared-indexes)

A primitive. Node investigation is the one function every caller uses on one exact birth, ending in one result for its client (Q420–Q423): `ready` (the birth answered: it was ready the whole time), `rejoined` (the investigation brought it back: commanded back through node.mesh.join, or started) or `re-created` (it could not be brought back: written `dead`, and [node recovery](node-recovery.md) deletes it and creates it again at its path and name with a new node.id). node.runtime.inspect's answers (`running`, `os-stopped`, `exited`, `unavailable`) are steps inside it, never a result. Its steps, in order:

1. Read the by-destination connections index ([connections.md §3.1](../connections.md#31-declared-indexes)).
   It names the peers to try; it never shows the node is reachable.
2. [node.runtime.inspect](node-runtime-inspect.md). On `exited` or `os-stopped` no tickle is sent: an `exited` birth is written `dead`; an `os-stopped` one is commanded to start (step 5).
3. [node.tickle](node-tickle.md), direct.
4. The same tickle via those peers. Unreached, a `running` birth is commanded back through [node.mesh.join](node-mesh-join.md) (step 5); an inspect that answers `unavailable` is asked again under the investigation's retries, and a birth still not proven at their end is written `dead`.
5. The command back or the start, each under its `Send`: answered, the investigation ends `rejoined`; unanswered at the end of its attempts, the birth is written `dead`. A birth written `dead` ends it `re-created`.

Only an answer from the exact birth counts: to a tickle, the investigation ends `ready`; to the command back or the start, `rejoined`.

A lost mesh-primary is investigated by the successor its election names, as any member of its
mesh ([node recovery](node-recovery.md) step 2; Q335). When the mesh's aggregate stays unheard for twice the staleness floor (60 s at the default floor; no successor took the role, Q349), the
fabric-primary investigates it the same way, and the re-created node-admin is that mesh's primary by
the same election. A lost fabric-primary is investigated from outside its mesh, by a mesh-primary, only once it has been unheard for twice the staleness floor ([fabric recovery](fabric-recovery.md) step 2, Q349).

A node that does not answer a call is silent, whatever it gossips (Q182): a call whose attempts end
unanswered starts an ordered investigation of its birth at the primary above it (the caller itself when it is that primary; any
other caller reports the birth to it with Status `ReportCallFailed`), which sets no `PendingReconnect` ([node recovery](node-recovery.md) step 3, Q393).

The result is the one decision (Q111). The primary that investigated writes it as one `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, result and the inspect's answer that led to it ([build.md §5](../build.md#5-buildstateadapter)); an investigation is never an entity of its own. `ready`: a held birth off its mesh is commanded back through [node.mesh.join](node-mesh-join.md) for its view (Q87), and is still `ready`. `re-created`: [node recovery](node-recovery.md) deletes the birth and creates it again at the same path and name with a new node.id (Q110). An investigator that hears no other node (every other birth it holds is unheard past the floor, so it is the one cut off) ends with no result: nothing is written, and the next investigation decides (Q153). Fabric recovery's backbone arm excepts that case ([fabric recovery](fabric-recovery.md) step 3). A wrong result is fixed in the investigation.

Nothing waits on the provider's `Exited`: not node recovery, a primary change, a name's release,
node.delete or mesh.delete.

Data dir: if the provider step finds the runtime exited, the re-created node takes its predecessor's data dir when its `storage_persistence` is `persistent` and the directory exists; otherwise, and on any other answer, a newly allocated one (Q359). Its `endpoint_id` and `transport_addr` are always new (Q358).

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
`rdm.node_admin.node.update.via-refused-caller` span. A node that comes back once its investigation ends `re-created`, during its successor's creation or after it, is
such a caller: its recovery or its re-created birth holds its path and name, and it is deleted. A second birth another
executor made at the path and name across a cut is made a deleted birth by its mesh-primary, which writes its
row deleted ([node.delete](node-delete.md), releasing the name) and reports it the same way (Q142).

## Operation contract

| Field | Contract |
|---|---|
| Caller | The primary above the node: its mesh-primary for a member of any type, the fabric-primary for a mesh-primary or another mesh's node-admin |
| Target | One node, by `(node.id, incarnation)` |
| Steps | The by-destination connections index read, node.runtime.inspect, node.tickle direct, the same tickle via those peers |
| Result | `ready`, `rejoined` or `re-created` (Q420–Q423); none from an investigator that hears no other node (Q153), nor from an ordered investigation whose inspect's answer matches the birth's status in the topology (Q393) |
| What follows | `ready` and `rejoined`: nothing more from RDM; `on_node_investigated` tells the application. `re-created`: [node recovery](node-recovery.md), over the predecessor's data dir when the inspect answered `exited`, its storage `persistent` and the directory there, over a newly allocated one otherwise |
| Spans | [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own |

## States written

Its mesh-primary writes the birth `PendingReconnect` as a self-heal starts and `dead` when the investigation concludes in failure (Q393); the primary that investigated writes its result as one `rdm.node.investigated` line in `events.jsonl`, `subject` the node.id.

| written | by | when |
|---|---|---|
| `rdm.node.investigated` | the primary that investigated | the result is decided; an investigator that hears no other node writes nothing |

As stored:

`running`:

```json
{"len":526,"specversion":"1.0","id":"01a1c3e9-675b-7f68-81b2-4bf565c508e0","source":"rdm://kzjdxwa0vbjs/jgs4k71hwvd4/yp0v648xykfd","type":"rdm.node.investigated","subject":"q68kytkd7a7f","time":"2026-11-10T05:23:35.898Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-df52d4ddf1396483eb70fcd0f3fbb22d-9ed27f4424127abf-01","data":{"incarnation":"z7mwtv40gzcq","inspected":"running","name":"2","path":"mesh1.node","reason":"self-heal","result":"re-created","trigger":"silence"},"crc32":"4115a3cb"}
```

`exited`:

```json
{"len":525,"specversion":"1.0","id":"01a1c3e8-1242-7d80-885b-f796beca20ef","source":"rdm://0p6sym3kkarb/0bdgxxw4pkmc/2qn20r32106k","type":"rdm.node.investigated","subject":"v7ckgsc6jq6f","time":"2026-11-10T05:22:08.578Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-d53a79d0750b874258309319dd62eec8-8df032936be6d722-01","data":{"incarnation":"54jsaesdk413","inspected":"exited","name":"3","path":"mesh1.node","reason":"self-heal","result":"re-created","trigger":"silence"},"crc32":"224ef4db"}
```

`os-stopped`:

```json
{"len":529,"specversion":"1.0","id":"01a1c3e8-5cef-77b8-af09-027597610b4a","source":"rdm://c9bz9hqzxqne/570f9zbf00fq/qxq5e2qy1kmv","type":"rdm.node.investigated","subject":"wbs3hteya5vt","time":"2026-11-10T05:22:27.694Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-012ddc20349404568f7b41a446a7a0c6-e0b7ffb6d44cfd32-01","data":{"incarnation":"w5dfgy987p3k","inspected":"os-stopped","name":"2","path":"mesh1.node","reason":"self-heal","result":"re-created","trigger":"silence"},"crc32":"32c7032f"}
```

`reason` is `self-heal` or `ordered`; `trigger` is `silence` for a self-heal, else what started the ordered investigation: `connect-refused`, `connect-timed-out`, `connect-failed`, `endpoint-unresolved`, `attempts-spent` or `report-call-failed` (Q393). `inspected` is node.runtime.inspect's answer that led to the result: `running`, `os-stopped`, `exited` or `unavailable`; a `ready` event carries none, the tickle's answer having led to it.
