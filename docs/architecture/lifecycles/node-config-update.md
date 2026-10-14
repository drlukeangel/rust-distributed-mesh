# Node config update

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.config.update` replaces a node's application config, the whole map. It is a single call, not a
Build. Config is a property bag the application stores on the node; RDM never reads it
([node properties](../node-rpc-envelope.md#node-properties)).

## Numbered end-to-end sequence

1. The caller sends `node.config.update` to the node's mesh-primary (Forward `0x1A` when not
   directly reachable).
2. The mesh-primary validates the map against the [cap](../gossip.md#31-frame-catalog) and refuses
   an over-cap map by name, then writes the record.
3. A live node is sent `ApplyNodeConfig`; it applies the map and carries it in its next digest. The command
   completes after `Applied` or `AlreadyApplied`. A stopped node is sent nothing: the command completes after
   the durable row write, and its next start receives the config in the join answer.
4. Emit `node.config.updated`.

Row first, node second. It is refused `fabric-not-ready` until the initial fabric has first reached
ready-for-traffic ([States](states.md#fabric-not-ready)), with nothing written. It is never refused
because a Build is running: every change to a node record goes through its mesh-primary's one held
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
