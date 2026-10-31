# Node records

**Integration work in progress.** This describes the approved i150 target, not a completed release.
Acceptance and remaining work are tracked in [i150](../../plans/i150-node-writes-lifecyle-updates.md).
The [design](../../plans/proposal/node-record.md) records the decisions; approved architecture
changes are recorded in the [documentation review](../../plans/proposal/node-record-documentation.md).

A node writes its own config, tags, bound addresses and declarations. A primary writes lifecycle
facts under the subject node ID and its own writer birth. A late predecessor's write therefore
cannot replace the current primary's lifecycle record. Current path/name selection uses the
existing claim operation; path and name are separate values and carry no implicit node kind.
A node is bound to its immutable path. Its name can change; replacement renames the predecessor
to `<name>-old` at the same path and creates the successor under the original name.
Existing election revisions remain; property and lifecycle records have no new revision counter.

Builds apply properties to new and existing nodes. Config, tags and semantic application identity
persist together. A repeated attempt recognizes that effect and preserves subsequent node or
caller updates; a later Build can override them. Recovery inherits acknowledged properties and
application identities; replacement takes its entry's initial properties.

The sixth storage boundary, `NodeRecordsStorage`, stores both property and lifecycle records.
The file backend uses `node-records.jsonl`. Moving providers must preserve both record kinds,
applied identities, immutable initial snapshots and identified deferred obligations.

See the [how-to](how-to.md) for integration and the [runbook](runbook.md) for diagnosis.
