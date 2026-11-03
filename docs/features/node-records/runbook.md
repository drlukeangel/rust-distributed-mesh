# Node records: diagnosis

| Symptom | What to read |
|---|---|
| Wrong properties after a Build | The node's record, and the Build's step events (`rdm.build.step`): a Build writes only the nodes it creates. |
| A recovered node shows its predecessor's ports | The node records the `(path, name)` resolves to, and the recovery: a recovered node takes its predecessor's config and tags, never its bound addresses. |
| A stopped node's status reads live | The node's latest lifecycle line. A primary that lost its election and resumes can write one line after its successor; the record stands corrected at its successor's next write (Q323; lead-decisions-2026-11-02 D7). |
| `node.get` lists a node with properties unavailable | The node has written no property record yet, or its backend keeps its stores in another process. |
| A config update ends `ConfigNotApplied` | The node did not answer. Nothing is recorded, and a later update applies as usual (Q310). |
| Properties revert after a restart | The node's retained property record against its launch environment. |

The spans to read:
- `rdm.node.record.update.via-properties`: the node's own write.
- `rdm.node_admin.node.update.via-lifecycle-record`: the mesh-primary's write.
- `rdm.node_admin.node.resolve.via-lifecycle-record`: a read.

Evidence records node ids, operations, backends and outcomes, never config values or secrets. A
call whose `Send` is spent is an unknown outcome, not proof that a write did not happen.
