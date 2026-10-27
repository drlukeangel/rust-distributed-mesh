# Node config update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.config.update` replaces a node's application config, the whole map. It is a single call, not a
Build. Config is a property bag the application stores on the node; RDM never reads it
([node properties](../node-rpc-envelope.md#node-properties)). A node's first config is its create spec's
`config` in the Build ([build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine)), written to its row at the claim and
carried in its first join; a later Build's different `config` is applied as this call applies a map.

## Numbered end-to-end sequence

1. The caller sends `node.config.update` to the fabric-primary. A node sends its own through the node-admin it joined through, and a node-admin's own goes in process to that admin; an admin that is not the fabric-primary carries it to the fabric-primary (Forward `0x1A` when not directly reachable) and carries the answer back. The fabric-primary writes the map to the node in its atlas and sends the command to the node's mesh-primary (Forward `0x1A` when not directly reachable).
2. The mesh-primary writes the record.
3. A live node is sent `ApplyNodeConfig`; it applies the map. The command
   completes after `Applied` or `AlreadyApplied`. A stopped node is sent nothing: the command completes after
   the durable row write, and its next start receives the config in the join answer.
4. Emit `node.config.updated`.

Row first, node second. It is admitted at any time (Q166), and never refused because a Build is running: every change to a node record goes through its mesh-primary's one held
record, one at a time, so any interleaving with a Build is a serial order of the two. Two topology
changes never run together; a second is refused `BuildInProgress`
([Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm)).

## Command and completion messages

```text
node.config.update
  → write record → ApplyNodeConfig (a live node only)
  ← node.config.updated
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
