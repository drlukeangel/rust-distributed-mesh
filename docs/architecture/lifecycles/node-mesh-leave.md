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
| Progress | `leaving` from the step until the mesh is left (and the backbone, a node-admin): its `Leaving` frame on the stop's stream, and the node writes leaving on its own record (Q356) and publishes it in its Digest at once, its last Digest before it leaves the mesh; it publishes nothing after it (R-ON1, Q415) |
| Completion | `node.mesh.left`, the `StopMeshLeft` frame on the stop's stream, and the node writes left on its own record (Q356); the stop's `Stopped { receipt }` or the delete's next step follows it |
| Gossip | From the node, its last Digest, carrying leaving (Q415). Its mesh-primary publishes NodeStopped from the stop's matching stopped frame, or NodeDeleted after node.delete's step event; a mesh's last admin after mesh.deleted has its final mesh topology instead ([mesh.delete](mesh-delete.md) step 7), and the fabric-primary publishes nothing for it. The reserved `NodeLeaving` / `NodeLeft` frame positions stay reserved and unpublished |
| Wire | Its acknowledgement and completion are Data frames on the stop's stream, `Leaving` then `StopMeshLeft { left }` ([node.stop](node-stop.md) step 3). NodeCommand 0x22 request position 4 and reply positions 21 and 22, and Status 0x1B request position 29 and reply position 50, are reserved and refused by name |
| Spans | `rdm.node.mesh.update.via-leaving` at the command, `via-left` once per mesh left, each naming mesh, MeshId and which (`own` or `backbone`) |

## Numbered end-to-end sequence

1. The stop has completed `node.connection.delete { All }` on the node and holds its receipt: step 2
   of the stop; the leave is its step 3.
2. `leaving`: the node reports `Leaving` on the stop's stream, and writes leaving on its own record.
3. The node leaves its mesh: its gossip neighbours on `f(FabricId, MeshId)`. A node-admin then
   leaves the backbone. Each one left emits `via-left`.
4. The node cuts its Node RPC pool. Its endpoint stays bound, and the commanding call's stream and
   the connection to its mesh-primary its owed declarations ride stay open.
5. `node.mesh.left`: `StopMeshLeft { left }`, and the node writes left on its own record. The node
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
    Note over N: writes leaving
    Note over N: 3. leave the mesh f(FabricId, MeshId), via-left, a node-admin also the backbone
    Note over N: 4. cut the Node RPC pool, endpoint stays bound
    N-->>M: 5. StopMeshLeft { left } (node.mesh.left)
    Note over N: writes left
    Note over M: Stopped { receipt } follows and M publishes NodeStopped
```

Checked against the numbered sequence: the connections are deleted before the leave; the mesh (and
the backbone) is left before the pool; the node publishes nothing at any step.

## States written

The node writes each state on its own record (Q356), a `node-record` line in `nodes.jsonl`, whole.

| written | by | when |
|---|---|---|
| `leaving` | the node | it reports `Leaving` on the stop's stream |
| `left` | the node | it reports `StopMeshLeft` |

As stored:

`leaving`:

```json
{"len":987,"specversion":"1.0","id":"01a1ae8c-01f5-741b-a731-3ed2f92b322f","source":"rdm://tngwfrd0p9vv/9rgap8z13kty/pze496vbjh1n","type":"node-record","subject":"9rgap8z13kty","time":"2026-11-06T01:49:33.554Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"9rgap8z13kty","path":"mesh1.node","name":"1","type":"node","incarnation":"pze496vbjh1n","state":"leaving","endpoint_id":"b8c03c61bb182d7b41402d182888a1eba21895f3ee2ea3bd0a0e5bd207845eee","transport_addr":"127.0.0.1:45458","listeners":[],"runtime":{"deployment_id":"k2ryj3ypad1n","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1506148,"start":65837629}}},"is_primary":null,"is_fabric_primary":null,"cert":null,"signing_cert":null,"storage":{"persistence":{"persistence":"ephemeral"},"data_dir":"/dev/shm/rdm-lane-5/cells-1506098/stop-start-ymgd6qjpp569"},"executable":null,"config":{},"tags":[]},"crc32":"1e4882ac"}
```

`left`:

```json
{"len":984,"specversion":"1.0","id":"01a1ae8c-01f5-7a4e-9a7b-0703df51279a","source":"rdm://tngwfrd0p9vv/9rgap8z13kty/pze496vbjh1n","type":"node-record","subject":"9rgap8z13kty","time":"2026-11-06T01:49:33.555Z","datacontenttype":"application/json","dataschema":"node-record/2","data":{"node_id":"9rgap8z13kty","path":"mesh1.node","name":"1","type":"node","incarnation":"pze496vbjh1n","state":"left","endpoint_id":"b8c03c61bb182d7b41402d182888a1eba21895f3ee2ea3bd0a0e5bd207845eee","transport_addr":"127.0.0.1:45458","listeners":[],"runtime":{"deployment_id":"k2ryj3ypad1n","provider":"process","control_domain":"process:e152f7e9-9c84-4260-8679-e39839d273ff:4026531836","locator":{"process":{"pid":1506148,"start":65837629}}},"is_primary":null,"is_fabric_primary":null,"cert":null,"signing_cert":null,"storage":{"persistence":{"persistence":"ephemeral"},"data_dir":"/dev/shm/rdm-lane-5/cells-1506098/stop-start-ymgd6qjpp569"},"executable":null,"config":{},"tags":[]},"crc32":"8784d094"}
```

## Failure

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
