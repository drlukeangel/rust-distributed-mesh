# Node create

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Typed calls and wire mapping](../node-rpc-envelope.md#typed-object-contract)

`node.create(spec)` creates a logical node and composes the whole
[node.start](node-start.md) workflow with `start_kind = FirstStart`.
RDM's create owns identity, launch and admission; Rafka supplies its application hook.

## Operation contract

| Field | Contract |
|---|---|
| Caller | Accepted control/Build caller through the typed object |
| Executor | Accepted node-admin executor; mesh-primary for an ordinary node |
| Identity | New node.id and incarnation; immutable path and claimed name |
| Endpoint key | Mint into a new data dir, or load the key from an inherited data dir |
| Port | Bind port 0; advertise the actual OS-assigned transport_addr |
| Spec | The create spec: the node's properties, flat (Q166): its executable (path, sha256, image), storage, placement, tags and config, and whether the data dir is inherited; the one description of a node to create, never a fault-injection mechanism. The executable is the create's input and nothing else's: recorded on the row at the claim, re-hashed at the launch, never changed for the birth ([build-deployment.md](../build-deployment.md)) |
| Progress | Every event of the complete `node.start`, then `node.created` |
| Hooks | The composed [node.start](node-start.md)'s: [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready), blocking, with `start_kind = FirstStart`, and [`on_node_ready`](../fabric-node-hooks.md#on_node_ready), a notice after `node.started` |
| Completion | The composed [node.start](node-start.md) ends with `node.started`; `node.created` follows it |
| Wire | [Typed-object mapping](../node-rpc-envelope.md#typed-object-contract); no new op number in this workflow |

<a id="mesh-absent"></a>
## A node-admin whose mesh does not exist yet

There is no mesh create call: a mesh comes into being with its first node-admin. node.create names
its mesh, and the fabric-primary, which drives every node.create and holds the complete accepted
topology, decides from that topology: mesh absent is [mesh.create](mesh-create.md), this arm's own
page; mesh present, the node joins like any other node. The decision is taken from the accepted
topology, never by looking whether a channel exists and then creating it.

<a id="ordinary-process-birth"></a>

## Bound endpoint and join admission

1. Record the exact deployment, prepare its identity/data dir and launch its executable.
   The executor never picks, reserves or predicts a transport port. Before this step the
   node's [row writer](../node-identity-and-state.md#25-one-system-org-one-projection-one-writer-per-mesh)
   claims the name, reading the row at it;
   `NameHeld { by: node.id }` refuses a held name and nothing is launched.
2. The process loads `endpoint_id` and binds port 0. In the exact-address configuration,
   endpoint startup neither awaits nor launches host-interface enumeration. The
   advertised ip:port is the actual bound address, not a guessed port or outgoing socket.
3. Once identity, data dir, launch and bind complete, compose
   [node.start](node-start.md#end-to-end-sequence) with FirstStart; `node.created` follows
   the whole start (step 6). The process's first outbound Node RPC is the report of [node.mesh.join](node-mesh-join.md): a node-admin
   joins its maker; an ordinary node joins its mesh-primary.
4. The accepting primary validates the reported digest against the deployment, the name
   against the create spec at a first start, and refuses a mismatch by name, installing
   nothing and releasing no wait ([Join contract](../node-rpc-envelope.md#join-op-0x1d-rdm)).
5. On acceptance it installs the actual address and completes WaitForBind / WaitForMeshJoin
   from the validated report, never from gossip. A JoinNode after deployment abandonment is
   refused by name.
6. The rest of the composed [start](node-start.md#end-to-end-sequence) runs from the join
   answer through `node.started`. Create then ends with `node.created`.

A fabric-primary recovering from its durable map follows the separate
[Fabric recovery](fabric-recovery.md) workflow. It has no maker call. Ordinary
reconnect and topology gap repair reuse the endpoint and call `node.topology.get`;
they do not create a logical node.

## Join completion and failure evidence

Only an accepted full report completes the deployment's bind/join waits. Provider
launch, a listening socket, a digest, a timeout or an unanswered call is insufficient.
Join acceptance does not complete start or bypass the hook. A typed refusal names
its step; an uncertain answer is not Joined. Runtime exit requires independent exact
provider proof, even for a deployment that never joined.

## Command, completion and gossip messages

The caller invokes `node.create(spec)` through the
[typed contract](../node-rpc-envelope.md#typed-object-contract). The progress view is:

```text
Started
[node.start(FirstStart): node.joined, node.started]
node.created
```

The bracketed sequence is composed from [Node start](node-start.md); it is not another
wire enum. A topology Digest cannot complete the caller's stream or the deployment wait
([caller execution](../node-rpc-envelope.md#caller-execution-and-certainty)).

## Numbered end-to-end sequence

Follow the [create steps](#bound-endpoint-and-join-admission), then the complete
[start sequence](node-start.md#end-to-end-sequence). `node.created` proves creation;
it does not claim the node is ready.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as Fabric-primary
    participant M as Mesh-primary (executor)
    C->>F: build.create(node.create spec)
    F->>M: build.attempt.run
    Note over M: claim name, mint, launch; compose whole node.start with FirstStart (pre_node_ready, then on_node_ready)
    M-->>F: start events, node.started, node.created
    F-->>C: step frames, Complete
```

## Failure and receipts

Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

Trace propagation, step spans and correlation follow the
[runtime spans](../node-rpc-envelope.md#runtime-spans).
