# Node config update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.config.update` replaces a node's application config, the whole map: a PUT, never a patch. It is
a single call, not a Build. Config is a property bag the application stores on the node; RDM never
reads it ([node properties](../node-rpc-envelope.md#node-properties)). A node's first config is its create
spec's `config` in the Build ([build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine)), written to its row at the claim and
carried in its hydration sync ([node.topology.get](node-topology-get.md)); a later Build's different
`config` is applied as a caller's update is, and supersedes the node's current map, whoever set it.

The update runs one of two ways. A caller's goes down from the fabric-primary. A node's own goes up
from the node, so its mesh-primary and the fabric-primary hold the map the node will be hydrated with
and a Build is diffed against; every install, at the node and in each atlas, is ordered by the row's
revision.

## Numbered end-to-end sequence

### A caller's update

1. The caller sends `node.config.update` to the fabric-primary, which sends the command to the
   node's mesh-primary (Forward `0x1A` when not directly reachable).
2. The mesh-primary writes the record, stepping its revision, and its atlas.
3. A live node is sent `ApplyNodeConfig` with the map and its revision; it installs the map over an
   older revision. The command completes after `Applied` or `AlreadyApplied`. A stopped node is sent
   nothing: the command completes after the durable row write, and its next start receives the config
   in its hydration sync.
4. The completion carries the row as written; the fabric-primary writes it into its atlas over an
   older revision. Emit `node.config.updated`.

### A node's own update

1. The node holds the map and sends it to the mesh-primary its topology view names, owed until
   answered and retargeted on a primary change; in process when that mesh-primary is this process.
2. The mesh-primary writes the record, stepping its revision, and its atlas, and answers the node
   `Applied` or refuses it by name; either answer carries the row as written, which the node installs
   over an older revision.
3. The mesh-primary owes the row to the fabric-primary, keyed by the node and its revision, until
   acknowledged; the fabric-primary writes it into its atlas over an older revision. A successor
   mesh-primary owes it again from the row. A mesh-primary that is the fabric-primary writes both at
   step 2.
4. Emit `node.config.updated`.

Row first, node second. It is admitted at any time (Q166), and never refused because a Build is running: every change to a node record goes through its mesh-primary's one held
record, one at a time, so any interleaving with a Build is a serial order of the two. Two topology
changes never run together; a second is refused `BuildInProgress`
([Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm)).

## Command and completion messages

```text
node.config.update (a caller's, at the fabric-primary)
  → write record → ApplyNodeConfig (a live node only)
  ← node.config.updated
node.config.update (a node's own, at its mesh-primary)
  → write record
  ← node.config.updated
  → NodeConfigUpdated (to the fabric-primary, owed until acknowledged)
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
