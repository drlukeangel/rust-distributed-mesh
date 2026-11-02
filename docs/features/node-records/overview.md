# Node records

Every node has one record, keyed by its node id. It is append-only, and its latest line is current
([query.md §1](../../architecture/query.md), Q323). The work record is
[i150](../../plans/i150-node-writes-lifecyle-updates.md), and the design is the
[node-record proposal](../../plans/proposal/node-record.md).

**Who writes what.** A node writes its own properties: config, tags, bound addresses and
declarations. Its mesh-primary writes its status and its role flags (`is_primary`,
`is_fabric_primary`). No writer is part of the record's key, and the record carries no writer field
and no revision. A node that has not written yet has no properties yet.

**How a node is found.** A path and a name are two separate values, and neither encodes the node's
kind. A `(path, name)` resolves through its name claim to a node id, and the reader reads that node's
record. A read by node id reads that node's record directly, whether or not the view still holds the
node, so a deleted node answers Deleted. The path is fixed for a node's life, and only its name
changes. A replacement renames the predecessor to `<name>-old` at the same path and creates the
successor under the original name ([node-replace.md](../../architecture/lifecycles/node-replace.md)).

**Builds.** A Build applies its entry's config and tags to new and existing nodes. A later attempt of the same Build repeats
nothing, so an update the node or a caller makes afterwards stands until the next Build. A node with
no record yet has no properties, so its entry is applied. A recovery takes its predecessor's
acknowledged properties. A replacement takes its entry's.

**Primaries.** Leadership is as main has it (Q322). The successor installs the election's result that
the call carries, and its mesh-primary records the role flags. There is no takeover step.

**Storage.** `NodeRecordsStorage` keeps the property record and the lifecycle record of every node.
The file backend writes them to `node-records.jsonl`, and a provider move copies both.

See the [how-to](how-to.md) for integration and the [runbook](runbook.md) for diagnosis.
