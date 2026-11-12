# Node records: diagnosis

| Symptom | What to read |
|---|---|
| Wrong properties after a Build | The node's record, and the Build's step events (`rdm.build.step`): a Build writes only the nodes it creates. |
| A recovered node shows its predecessor's ports | The node records the `(path, name)` resolves to, and the recovery: a recovered node takes its predecessor's config and tags, never its bound addresses. |
| A stopped node's status reads live | The node's latest lifecycle line. A primary that lost its election and resumes can write one line after its successor; the record stands corrected at its successor's next write (Q323; lead-decisions-2026-11-02 D7). |
| `node.get` lists a node with properties unavailable | The node has written no property record yet, or its backend keeps its stores in another process. |
| A config update ends `ConfigNotApplied` | The node did not answer. Nothing is recorded, and a later update applies as usual (Q310). |
| Properties revert after a restart | The node's retained property record against its launch environment. |
| A `node.get` or `fabric.get` is slow at the answering admin | The read's span, `rdm.node_admin.node.serve.via-get` or `rdm.node_admin.fabric.serve.via-get`: `io_calls` counts its store calls (a read naming nodes by id reads each record at its id; by `(path, name)`, the current-pair lookup and each record; any other read, each mesh's records as one call; never one call per node), `io_queued_ms` and `io_ms` what they took. |
| A Build acceptance is slow at the fabric-primary | The acceptance's span, `rdm.node_admin.build.create.via-node-rpc`: `io_calls` counts its store calls (each mesh's node records as one call, the deleted or replaced node's pair, the rows of the held paths a Create names (one path's record at its node id, more as each mesh's records in one call), and its builds, events and fabric stores' reads; never one call per node or per path), `io_queued_ms` and `io_ms` what they took. |
| A Build's completion is slow at the fabric-primary | The completion's span, `rdm.node_admin.fabric.update.via-atlas-receipt` with `io_calls`: `io_calls` counts its store calls (the Build's read, and the record of each completed node.stop, node.start or node.delete step whose birth the Build pinned none of, at its node id, the reads made together; none for a create or a pinned delete), `io_queued_ms` and `io_ms` what they took. |

The spans to read:
- `rdm.node.record.update.via-properties`: the node's own write.
- `rdm.node_admin.node.update.via-lifecycle-record`: the mesh-primary's write.
- `rdm.node_admin.node.resolve.via-lifecycle-record`: a read.

Evidence records node ids, operations, backends and outcomes, never config values or secrets. A
call whose `Send` is spent is an unknown outcome, not proof that a write did not happen.
