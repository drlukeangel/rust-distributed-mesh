# node.mesh.leave

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Status op](../node-rpc-envelope.md#status-op-0x1b-rdm) · [Gossip](../gossip.md)

A primitive (R-MJ1). `node.mesh.leave` takes a node off its mesh and, for a node-admin, off the backbone
too. It is the step [node.stop](node-stop.md) and [node.delete](node-delete.md)
end their live part with, after the node's connections are deleted and never before (R-CR5): the
data plane first, then the control plane. It is never a command of its own: node.stop already deletes
connections and leaves the mesh (Q345). Progress is `leaving`, completion `node.mesh.left`. The
node keeps its endpoint bound, its identity, name, port and data dir; a left node is reachable by
a fresh Node RPC dial for its rename, start or delete.

## Operation contract

| Field | Contract |
|---|---|
| Commander | The workflow that stops or deletes the node: its mesh-primary for a member, the fabric-primary for a mesh-primary (a mesh's last admin included, mesh-delete.md step 7). Commands go down |
| Executor | The node itself |
| Entry | The exact `(node.id, incarnation)`, connections-deleted: the stop's `node.connection.delete { All }` completed with its receipt (`Established`, or `Deadline` with the held ids recorded) |
| Leaves | Its gossip neighbours on its mesh (and the backbone, a node-admin) and its Node RPC pool; the control plane, which are not connections (R-CR5) |
| Keeps | Its endpoint bound, node.id, incarnation, path and name, port, bin, data dir |
| Progress | `leaving` from the step until the mesh is left (and the backbone, a node-admin): its `Leaving` frame on the stop's stream, from which the mesh-primary writes leaving on the node's row; the node publishes nothing in that state and nothing after it (R-ON1) |
| Completion | `node.mesh.left`, the `StopMeshLeft` frame on the stop's stream, from which the mesh-primary writes left on the node's row; the stop's `Stopped { receipt }` or the delete's next step follows it |
| Gossip | None from the node. Its mesh-primary publishes NodeStopped from the stop's matching stopped frame, or NodeDeleted after node.delete's step event; a mesh's last admin after mesh.deleted has its final mesh topology instead ([mesh.delete](mesh-delete.md) step 7), and the fabric-primary publishes nothing for it. The reserved `NodeLeaving` / `NodeLeft` frame positions stay reserved and unpublished |
| Wire | Its acknowledgement and completion are Data frames on the stop's stream, `Leaving` then `StopMeshLeft { left }` ([node.stop](node-stop.md) step 3). NodeCommand 0x22 request position 4 and reply positions 21 and 22, and Status 0x1B request position 29 and reply position 50, are reserved and refused by name |
| Spans | `rdm.node.mesh.update.via-leaving` at the command, `via-left` once per mesh left, each naming mesh, MeshId and which (`own` or `backbone`) |

## Numbered end-to-end sequence

1. The stop has completed `node.connection.delete { All }` on the node and holds its receipt: step 2
   of the stop; the leave is its step 3.
2. `leaving`: the node reports `Leaving` on the stop's stream, and the mesh-primary writes leaving
   from it.
3. The node leaves its mesh: its gossip neighbours on `f(FabricId, MeshId)`. A node-admin then
   leaves the backbone. Each one left emits `via-left`.
4. The node cuts its Node RPC pool. Its endpoint stays bound, and the commanding call's stream and
   the connection to its mesh-primary its owed declarations ride stay open.
5. `node.mesh.left`: `StopMeshLeft { left }`, and the mesh-primary writes left from it. The node
   publishes nothing. `Stopped { receipt }` follows on the stop's stream and the mesh-primary
   publishes NodeStopped from it; inside a delete, the provider's
   [runtime inspect](node-runtime-inspect.md) follows. A node already left since its last start
   answers `Leaving` and `StopMeshLeft` at once and leaves nothing again.

## Command, completion and gossip messages

The leave is a step of the stop's own stream, the progress view
[node-stop.md](node-stop.md#command-and-completion-messages) shows: `Leaving` (Data), then
`StopMeshLeft { left }` (Data), the frame before `node.stopped`:

```json
{ "Leaving": { "node_id": "k8pag1qx95gw", "incarnation": "0000000000000008" } }
{ "StopMeshLeft": { "left": ["own"] } }
```

A node-admin answers `"left": ["own", "backbone"]`. The gossip after it is the mesh-primary's, not
the node's: the [NodeStopped example](../gossip.md#examples) for a stop, or
[node.delete](node-delete.md)'s NodeDeleting and NodeDeleted for a delete.

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant M as Mesh-primary (commander)
    participant N as Node
    Note over M,N: 1. the stop's node.connection.delete { All } complete, receipt held
    N-->>M: 2. Leaving on the stop's stream
    Note over M: writes leaving
    Note over N: 3. leave the mesh f(FabricId, MeshId), via-left, a node-admin also the backbone
    Note over N: 4. cut the Node RPC pool, endpoint stays bound
    N-->>M: 5. StopMeshLeft { left } (node.mesh.left)
    Note over M: writes left; Stopped { receipt } follows and M publishes NodeStopped
```

Checked against the numbered sequence: the connections are deleted before the leave; the mesh (and
the backbone) is left before the pool; the node publishes nothing at any step.

## Failure and receipts

A mesh that cannot be left is reported by name with which one, the primitive stays `leaving`, and the workflow's rerun resumes it; the node never
re-publishes meanwhile. Failures and stream certainty follow the
[caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

| Step | Actor | Span | Fields |
|---|---|---|---|
| Step | node | `rdm.node.mesh.update.via-leaving` | mesh, mesh_id, operation |
| Mesh left | node | `rdm.node.mesh.update.via-left`, once per mesh the node was on: its own mesh, and the backbone for a node-admin | mesh, mesh_id, which (`own` or `backbone`) |
| Completion | node, commander | the `StopMeshLeft` frame on the stop's stream | operation |

Trace propagation follows the [runtime spans](../node-rpc-envelope.md#runtime-spans).
