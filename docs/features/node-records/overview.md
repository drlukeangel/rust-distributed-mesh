# Node records

Every node has one record, keyed by its node id. It is append-only, and its latest line is current
([query.md §1](../../architecture/query.md), Q323). The work record is
[i150](../../plans/i150-node-writes-lifecyle-updates.md), and the design is the
[node-record proposal](../../plans/proposal/node-record.md).

**Who writes what.** A node writes every field of its own record itself (Q356): its identity, state,
addresses, runtime, role flags, cert dates, storage, executable, config and tags. Its mesh-primary
writes a node's record only for a node that is lost, killed or deleted. No writer is part of the
record's key; the line's `source` names its writer, and the record has no revision. A node that has not written yet has no properties yet.

**How a node is found.** A path and a name are two separate values, and neither encodes the node's
type. A `(path, name)` lookup resolves the pair to a node id from the node records, and the reader
reads that node's record. A read by node id reads that node's record directly, whether or not the
view still holds the node, so a deleted node answers Deleted. The path is fixed for a node's life,
and only its name changes, by [node.name.update](../../architecture/lifecycles/node-name-update.md); a
new executable is a new node, a Build that deletes the old one and creates the new
([build-deployment.md](../../architecture/build-deployment.md), Q345).

**Builds.** A Build states each node it creates whole, its config and tags included, and writes no
property of a node it does not create: an existing node's config and tags change only through
`node.config.update` and `node.tag.*`, the last write winning
([build.md](../../architecture/build.md), Q236, Q342). A recovered node takes its deleted
predecessor's config and tags ([node recovery](../../architecture/lifecycles/node-recovery.md)).

**Primaries.** Each node records its own role flags of the election's result. There is no
takeover step.

**Storage.** `NodesStorage` keeps every node's record.
The file backend writes it to `nodes.jsonl`, and a storage change copies it.

See the [how-to](how-to.md) for integration and the [runbook](runbook.md) for diagnosis.
