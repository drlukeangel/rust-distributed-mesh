# Mesh create

[Lifecycle index](../fabric-mesh-node-lifecycles.md) · [States](states.md) · [Mesh entity](../node-identity-and-state.md#27-a-mesh-is-an-entity-and-node-admin-owns-it) · [Build](../build.md)

There is no mesh.create call. A mesh comes into being with its first node-admin: `mesh.create` is
the name of the mesh-absent arm of [node.create](node-create.md), run as a step of the Build
whose accepted topology names the new mesh. The fabric-primary, which drives every node.create
and holds the complete accepted topology, decides from that topology alone whether the node's
mesh exists; it never looks for a channel and creates one on not finding it. A dead mesh's name
used again is this arm under a new MeshId (R-ML2, R-MN1): a MeshId is never reissued.

## Operation contract

| Field | Contract |
|---|---|
| Caller / driver | The topology Build that adds the mesh ([build.md §12.1](../build.md#121-create-a-new-mesh)); the fabric-primary accepts it and drives it through the [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm) |
| Precondition | The Day-0 Build is Complete; before its ready it is the Build in progress, and the Build is refused `build-in-progress` (Q170) |
| MeshId | Minted once, when the mesh enters the accepted topology, and carried by every Build after it; never reissued |
| Name | `mesh<#>` by default, the Build's `mesh_name`; the first node-admin, as the mesh's primary, claims it at `(path, name)`, unique within its path; held by a different MeshId answers `NameHeld` |
| Executor / maker | The fabric-primary: it is the maker of the mesh's first node-admin and writes the mesh's rows until the mesh has a primary (R-OW1) |
| Channel | `f(FabricId, MeshId)`, created by its first listener, the node-admin |
| Members | The first node-admin only. The mesh's other nodes are created afterwards by that node-admin as mesh-primary, under the same accepted Build's attempts |
| Mesh states | `pending`, set by the node-admin that launched the mesh's first admin, one `ApplyMeshState(Pending)` after the launched birth answers Ping; `ready`, set by the fabric-primary on its mesh-primary's report that every planned birth is ready ([States](states.md)) |
| Rows | The mesh row, written by the mesh-primary at creation; the node row, written by the fabric-primary until the mesh has a primary, then by the mesh-primary |
| Completion | The node-admin's `node.created`; the mesh is in the topology from acceptance and in traffic from `ready` |
| Not this | A node-admin created into a mesh that already exists and lost its node-admins is [mesh investigation](mesh-investigation.md#a-node-admin-joins-the-live-mesh): same MeshId, same Build, no mesh create |

## Numbered end-to-end sequence

1. A topology Build is accepted whose complete topology names a mesh the current topology does
   not have (`{mesh1} → {mesh1, mesh2}`). Acceptance mints `mesh2`'s MeshId; every later Build
   carries it. The Build is in progress until its attempts realize the topology.
2. The fabric-primary claims the attempt and executes `node.create` of `mesh2`'s first node-admin
   itself: the mesh has no primary, so the fabric-primary is maker and executor. It records the
   deployment, prepares the data dir and seed, and launches the executable in the Create start
   mode ([node-admin lifecycle §3](../node-admin-lifecycle.md#3-start-modes)).
   `mesh.issuer.create`: at the mesh's acceptance the fabric-primary mints `mesh2`'s issuing cert, signed
   by the root, and writes it into the first admin's launch environment (`RDM_ISSUING_KEY`,
   `RDM_ISSUING_CERT`; [fabric certs](../fabric-certs.md#the-cert-workflows)).
3. The node-admin binds port 0 and runs [node.mesh.join](node-mesh-join.md): its report goes to its maker, the fabric-primary.
   The answer hands it the fabric identity, rafka-time, its cert, its name and config.
4. The node-admin creates `mesh2`'s channel, `f(FabricId, MeshId)`, as its first listener, and
   joins the node-admin backbone.
5. The fabric-primary sends one bounded Ping to the launched birth and, on the answer, applies
   the mesh's first state, `ApplyMeshState(Pending)`, within 20 s of the launch
   ([States](states.md), the mesh Pending row).
6. The rest of the node-admin's [start](node-start.md#end-to-end-sequence) runs: the
   [`pre_node_ready`](../fabric-node-hooks.md#pre_node_ready) hook, the topology read on its maker,
   the Build floor, rafka-time last, `node.started`, then its
   [`on_node_ready`](../fabric-node-hooks.md#on_node_ready) notice. The fabric-primary, as writer of a mesh with no primary, sets the node-admin's
   Ready.
7. The [election](../fabric-node-lifecycle-elections.md#42-mesh-primary) names the node-admin
   `mesh2`'s mesh-primary: the only candidate. As mesh-primary it claims the mesh's name,
   writes the mesh row and records `is_primary` on its own row, which its `Digest` carries, publishes its mesh's status
   `pending` as `MeshStatus`, and becomes the writer of `mesh2`'s rows. `node.created` ends the
   step.
8. The mesh-primary creates `mesh2`'s other nodes as attempts of the same Build, at once: a
   create that fails stops none of the others (Q245). When every
   planned birth is ready it declares `Ready` to the fabric-primary (`DeclareMeshState`, owed until
   answered), which decides it and answers `Applied`; answered, the mesh-primary records it on the
   mesh's record (Q144), publishes
   `MeshStatus` ready, and the mesh is in traffic. Once that state is newly on the mesh's
   record the mesh-primary calls [on_mesh_ready](../fabric-node-hooks.md#on_mesh_ready) once, a
   notice that gates nothing (`rdm.node_admin.mesh.update.via-on-mesh-ready`).

The decision in step 1 is taken from the accepted topology, never from the presence of a channel.
A mesh that exists in the topology and has no reachable node-admin is not this arm.

## Command, completion and gossip messages

The Build's step is a `node.create` with the node-admin's spec naming `mesh2`; the Build carrier's
frames are [node.create](node-create.md)'s, with `node.created` as its last event. The one call
this arm adds is the mesh's first state. A decoded request from the fabric-primary to the new
node-admin, op `0x1B` (27):

```json
{
  "target": { "node_id": "adm2b7d4f9h1", "op": 27 },
  "context": {
    "caller_system": "rdm",
    "traceparent": "00-c4e2a9b1d6f30857a1b2c3d4e5f60718-3333333333333333-01",
    "tracestate": null,
    "baggage": "operation=node.create%3Amesh2.admin.1"
  },
  "body": {
    "ApplyMeshState": {
      "mesh_id": "msh9r3t6v2x8",
      "mesh_name": "mesh2",
      "state": "Pending"
    }
  }
}
```

The unary reply, the body alone:

```json
{ "Applied": null }
```

A repeat of the same apply answers `AlreadyApplied`; an apply that names a mesh the receiver does
not hold answers `RejectedStaleMesh { held }`; a state the mesh cannot move to answers
`RejectedInvalidMeshTransition { current }`. At step 8 the mesh-primary declares
`Ready` upward instead (`DeclareMeshState`), and the fabric-primary's `Applied` answers it (Q144).

## Gossip messages

Three frames, all authored by the new mesh-primary once it is mesh-primary; nothing is published
for `mesh2` before that, and the fabric-primary never publishes on its behalf (R-RX4).

The primary the election named, on `mesh2`'s channel and the backbone
([catalog](../gossip.md#31-frame-catalog), the node-admin's own `Digest`, `is_primary` at revision 1):

```json
{
  "frame": "digest",
  "mesh": "mesh2",
  "node_id": "adm2b7d4f9h1",
  "incarnation": "0000000000000001",
  "digest_seq": 1,
  "is_primary": true,
  "revision": 1
}json
{
  "frame": "mesh-status",
  "mesh": "mesh2",
  "status": "pending",
  "publisher": "mesh2.admin.1",
  "changed_at_rafka_ms": 1718290010000
}
```

And the mesh's first backbone `MeshTopology`, one member, itself; the shape is the
[catalog's example](../gossip.md#examples) with `mesh2`, `publisher` the node-admin's exact
birth, `topology_version` 1 and that one digest in `digests`. Peer mesh primaries install it into
their held external topology and forward it on their own channels under the rules in
[gossip.md §3.3](../gossip.md#33-cross-mesh-topology-inside-a-mesh-deltas-full-snapshots-and-the-top-up).

## Mermaid sequence diagram

```mermaid
sequenceDiagram
    participant C as Caller
    participant F as Fabric-primary
    participant A as mesh2's first node-admin
    participant G as mesh2 channel / backbone
    participant R as Peer mesh-primaries
    C->>F: build.create (topology adds mesh2)
    Note over F: 1. accepted, MeshId minted
    Note over F: 2. claim attempt, node.create of mesh2.admin.1, fabric-primary as maker
    A->>F: 3. JoinNode on the maker
    F-->>A: Joined (fabric, rafka-time, cert, name, config)
    A->>G: 4. create mesh2's channel, join the backbone
    F->>A: 5. Ping
    A-->>F: reply
    F->>A: 5. ApplyMeshState(Pending)
    A-->>F: Applied
    Note over A: 6. pre_node_ready, topology read, node.started, on_node_ready; fabric-primary sets Ready
    Note over A: 7. election names it mesh-primary, claims mesh name, writes mesh row
    A->>G: 7. Digest (is_primary), MeshStatus pending, first mesh topology
    G-->>R: install mesh2 into external topology
    F-->>C: node.created
    Note over A: 8. creates mesh2's other nodes under the same Build
    A->>F: 8. every planned birth ready
    F->>A: ApplyMeshState(Ready)
    A->>G: MeshStatus ready
    Note over A: on_mesh_ready (notice)
```

Checked against the numbered sequence: acceptance mints the id before anything is launched; the
join precedes the channel; Pending follows the Ping; the primary, the name claim and the first frames
come after Ready; nothing about `mesh2` is on gossip before step 7.

## Failure and receipts

A refused name claim (`NameHeld` by another MeshId) fails the create step by name and nothing is
launched; a join refusal or a hook failure fails it as [node.create](node-create.md) states, and
the next attempt of the same Build resumes from the receipts (R-BI4). A mesh whose first
node-admin exits after creation is not re-created: it is a live mesh that lost its node-admins
and [mesh investigation](mesh-investigation.md) gives it one under the same MeshId. Failures and
stream certainty follow the [caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty).

## Observability contract

| Step | Actor | Span / evidence | Correlation |
|---|---|---|---|
| Acceptance | fabric-primary | `rdm.node_admin.build.create.via-accepted`; the MeshId in the accepted topology | Build id |
| Create | fabric-primary | [node.create](node-create.md)'s spans as maker and executor | Build, attempt, `node.create:mesh2.admin.1` |
| Join | node-admin, fabric-primary | [node.mesh.join](node-mesh-join.md#observability-contract) | same operation |
| Mesh Pending | fabric-primary | the Ping, then `ApplyMeshState(Pending)`'s call span | mesh id, mesh name |
| Seat and name | node-admin | `rdm.mesh.election.resolve.via-recompute`; the name claim; the mesh row write | mesh id, revision |
| Mesh Ready | node-admin, fabric-primary | `DeclareMeshState(Ready)` answered `Applied`; `MeshStatus` publish | mesh id |
