# Node delete

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.delete` composes the whole [node.stop](node-stop.md), then proves exact runtime
exit and releases identity. It releases node.id and name, **not storage**. The data
dir and node-identity.json / endpoint key stay with that data dir. Application storage
removal is separate work.

## Operation contract

| Field | Contract |
|---|---|
| Caller / executor | Typed caller / accepted node-admin executor |
| Live target | Complete node.stop first; the stopped process remains reachable |
| Crashed target | Stop steps are already done; no call is required from an exited process |
| OS-suspended target | The provider reports the exact runtime `os-stopped` ([deployment-provider §2.5](../deployment-provider.md#25-runtime-state-what-the-provider-reports)): the stop is skipped by name (a suspended process answers no call) and the delete begins at `TerminateRuntime` on the exact handle, as `terminate(Immediate)`: a suspended runtime runs no handler, so there is no graceful wait, and nothing of it is resumed; the delete completes only on the provider's exit proof. Explicit delete only, never recovery, which respects suspension |
| Runtime action | Request termination and inspect the exact accepted runtime |
| Terminal event | Executor emits node.deleted only from exact runtime Exited proof, after releasing node.id and name |
| Storage | Retain data dir and endpoint key; a later spec can inherit them |
| Peer block | node.deleted blocks old `(node.id, incarnation)` in the peer-location book |
| Forbidden proof | Gossip, silence, Ping failure and timeout never prove runtime exit |

## Numbered end-to-end sequence

1. For a live process compose the whole [node.stop](node-stop.md#numbered-end-to-end-sequence).
   For a crashed process those stop steps are already done; do not fabricate node replies or
   receipts.
2. Stop and inspect the exact runtime through provider control. Match its immutable
   runtime identity; a recycled pid or reusable container name cannot satisfy proof.
3. The node's [row writer](../node-identity-and-state.md#25-one-system-org-one-projection-one-writer-per-mesh)
   writes the node's row with status deleted; the record and its config stay. Release node.id
   and the name claim. Retain the data dir, node-identity.json and endpoint key.
4. The surviving executor emits node.deleted on the caller stream from the Exited
   evidence and records the complete delete receipt. The exited node cannot publish
   its own terminal proof or serve that stream.
5. Block the old `(node.id, incarnation)` in the peer-location book. Peers refuse a late
   digest of the deleted birth under the [frame catalog's receiver rules](../gossip.md#31-frame-catalog),
   even when a successor uses the same endpoint key.

## Command, completion and gossip messages

```text
node.delete
  ← every node.stop event, if the process is live
  ← node.deleted [executor: exact provider Exited proof; node.id and name released]
```

The executor, not the exited node, publishes the last event.
[NodeDeleting / NodeDeleted](../gossip.md#31-frame-catalog) are peer dissemination;
node.deleted is the caller event. The caller waits on the stream and durable receipts.

## Exact runtime exit evidence

Stop's node.stopped is pre-exit (stopped) and insufficient. Retain the exact accepted RuntimeFact
and require the provider's matching Exited observation before node.deleted or identity
release. An unreachable provider leaves delete incomplete. A crashed node also needs
that proof; lack of a reply never licenses deletion. The runtime-exit responsibility
is owned here and is composed by [node recovery](node-recovery.md), the mesh-primary's
drift reconciliation.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as Fabric-primary
    participant M as Executor
    C->>F: build.create(node.delete)
    F->>M: build.attempt.run
    M-->>F: node.stop events for a live process
    Note over M: exact provider Exited proof; row deleted
    M-->>F: node.deleted
    F-->>C: step frames, Complete
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
