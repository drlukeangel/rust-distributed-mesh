# Gossip and hierarchical topology architecture

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md#states) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns how facts travel over gossip: the channels, the frame catalog (each frame's fields, author and channels), cadence and timers, peers' receive rules, external topology, deleted retention, held-member repair and frame examples.

Fabric, mesh and node lifecycle states and who sets them are defined only in [States](lifecycles/states.md#states).
Join admission is defined by [JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm).

Gossip is the dissemination/topology plane. It is not a second control RPC system and it decides nothing about recovery.

Companion owners:
- [`build.md`](build.md) for the topology writer (one fabric-primary, the current Build), Build/recovery and the RDM/Rafka ownership boundary, and [`deployment-provider.md`](deployment-provider.md) for runtime adoption;
- the [lifecycle index](fabric-mesh-node-lifecycles.md) for the workflows whose events ride these channels;
- [`fabric-node-lifecycle-elections.md`](fabric-node-lifecycle-elections.md) for the single normative leadership/election specification;
- [`connections.md`](connections.md) for Direct/Proxy route truth.

---

## 1. Identity vocabulary

Logical product identities:

```text
NodeId
MeshId
FabricId
```

are canonical Crockford60 values.

Iroh `EndpointId` is transport identity; `endpoint_id` keys transport peers. `FabricId` identifies the logical Fabric.

A current birth also publishes a compact typed `RuntimeFact` bound to its NodeId + IncarnationId. RuntimeFact contains the exact provider runtime identity/control-domain locator needed by a future node-admin to adopt/manage that concrete runtime. It is not leadership policy, transport identity, or a credential.

The acquisition of the provider locator and publication of the fact are deliberately separate:

```text
provider / runtime bootstrap seam
    -> produces or discovers exact provider locator + provider control domain

node / runtime birth
    -> publishes ONE normalized RuntimeFact
       through its own current topology digest/projection
```

There is no second launcher-owned RuntimeFact stream. For example, a process may self-discover PID + process-start token, while a container provider learns the immutable container ID after `docker run` and makes it available to the child before Ready.

Operational metadata required by successor management, such as `data_dir`, is current Node/runtime projection state, not part of exact RuntimeFact identity merely because it must converge to a future primary.

---

## 2. Ownership

```text
Iroh
    authenticated EndpointId + QUIC paths

iroh-gossip
    HyParView channel membership
    Plumtree eager/lazy dissemination and repair
    neighbour state and peer queues

RDM topology layer
    channel derivation
    known/held peer directory
    current birth facts, including RuntimeFact + required runtime metadata
    hierarchical mesh/backbone publisher policy
    bounded held-member rejoin policy

Fabric control state
    the current Build and its accepted topology
    active Build facts

Build/lifecycle/leadership
    accepted topology (fabric-primary only)
    exact runtime adoption/death proof
    destructive reconciliation
    primary projection
```

Gossip silence, neighbour loss, failed dial, or missing push never proves a node Dead by itself.

Completed Build receipt history is not required to recover current runtime identity and is not replayed merely so a future primary can manage a birth.

---

## 3. Hierarchical topology

Each Mesh has its own topology channel derived from MeshId.

```text
ordinary member
    -> own mesh topology channel only

node-admin
    -> own mesh topology channel
    -> inter-mesh node-admin backbone

mesh primary
    -> publishes own mesh topology on backbone

receiving peer mesh primary
    -> forwards remote topology onto own mesh channel
       as deltas backed by periodic full snapshots, loads omitted

mesh executor (the primary holding a claimed attempt)
    -> publishes lifecycle events on its own mesh channel; the backbone carries them
       in its mesh topology (in_flight, deleted)

receiving peer mesh primary
    -> forwards remote lifecycle events onto own mesh channel
```

Rules:
- ordinary nodes never join the backbone;
- only node-admins join the backbone;
- the current mesh-primary, as the election names it, publishes its own mesh's topology on the backbone; a mesh with no primary goes unheard: nobody publishes a live mesh on its behalf, and each mesh's primary is the sole publisher of its mesh's MeshTopology projection; [mesh investigation](lifecycles/mesh-investigation.md) gives it a node-admin, and that new mesh-primary publishes once it is established (R-RX4);
- only receiving peer mesh primaries forward remote mesh topologies locally;
- lifecycle events are published by the executor on its own mesh channel and reach the backbone only inside MeshTopology.in_flight and MeshTopology.deleted;
- ordinary nodes converge to all-mesh topology without a flat fabric-wide topology channel;
- publisher responsibility follows imported leadership changes;
- who is primary is the election's result and the Mesh row's fact, not trusted because a publisher claimed it;
- the mesh topology/current birth projection preserves the current RuntimeFact and required current runtime metadata needed by its primary to manage that birth.

Use **forward** for this topology action. The transport's relay servers are disabled on every endpoint (`relay_mode Disabled`) and the word names nothing here.

### 3.1 Frame catalog

Nothing in the receive path checks who sent a frame: a frame is taken or ignored by what it carries, never by its sender.

A mesh-channel delta, a snapshot install and a topology read install move the held source
version and the digest view under one hold of the receiver. A reader of that version never finds
an older digest view.

Two channels carry the topology: a Mesh's topology channel (one per MeshId) and the node-admin
backbone (one per FabricId). No other channel exists.

Four orderings, four jobs, never substituted for one another:

```text
digest_seq            orders one birth's own heartbeats          (sender-local, per incarnation)
topology_version      orders a Mesh's material topology states   (source mesh-primary)
Rafka-time            says when a fact or publication happened    (never liveness or Gone; the first key of a MeshTopology publication's order)
local Instant         silence and elapsed time                    (receiver-local, never a remote clock)
```

| frame | channel | author | forwarded by | identity / dedup | retained | use |
|---|---|---|---|---|---|---|
| `Digest { node, incarnation, endpoints with freshness, node_state, runtime, digest_seq, emitted_at_rafka_ms }` | own mesh channel | every node, on its heartbeat timer | nobody | same exact birth: higher `digest_seq` is newer, equal or lower is not taken; a new incarnation is taken only along its lineage and only for a NodeId not accepted as `NodeDeleted`; its `digest_seq` starts again at 1 | until silent for the silence window, or until `NodeDeleted` | that node's current identity, endpoints, liveness and runtime fact. `node_state` (Joining / Alive / Overloaded / Degraded, node-identity-and-state.md §4) is liveness only: a heartbeat never carries or moves a status, and a member's status comes only from its mesh-primary |
| `MeshTopology { mesh, publisher (exact identity), forwarded_by, topology_version, published_at_rafka_ms, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted }` | backbone; and the source Mesh's primary's own mesh channel (its own member projection) | the source Mesh's current primary; after mesh.deleted its last node-admin, still its primary, until it is stopped or deleted; nobody else, ever: nobody publishes a live mesh on its behalf, and each mesh's primary is the sole publisher of its mesh's `MeshTopology` projection (the fabric-primary never publishes a mesh as its acting primary, R-RX4) | nobody as is; a receiving primary derives the two frames below from it | `(mesh, publisher, topology_version, snapshot_id)`; a receiving admin installs the snapshot atomically once it holds every chunk, and only then advances the held source version or derives a delta; the version bumps on a material change of the normalized projection below and not on an unchanged republish; the publisher's exact identity is the version's revision | `digests`: as held; `in_flight`: while the operation is claimed and has no completed receipt (derived from Build facts at each publication, never stored), and NodeStopped until the next lifecycle op on that exact birth; `deleted`: local age since accepted, up to the deleted retention | the complete Mesh with loads: what every node-admin holds of every Mesh for routing and control decisions. In `digests`, each member is a projection entry `{ digest, status }`, the status authored by the mesh-primary alone. Own-mesh members receive the primary's statuses through the primary's versioned member projection. One mesh has one publisher on the backbone, its mesh-primary, and none while it has no primary (R-RX4), so a receiver takes its MeshTopology and MeshTopologyDelta as they arrive. A frame received out of order, with an order key earlier than the newest the receiver holds for that mesh, is ignored in the gossip receive path for every frame kind (the receiver rules below). Receivers install the complete snapshot atomically; they do not queue unmatched status events. For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its Digest and status from the projection; NodeStopped and NodeDeleting stay immediate events. |
| `MeshTopologyDelta { mesh, source_publisher, base_version, topology_version, published_at_rafka_ms, changed, removed, in_flight, deleted }` | the forwarding primary's own mesh channel | the local mesh primary, when a source Mesh it holds moved from one version to another; and the source Mesh's primary, for its own Mesh, on its own mesh channel | nobody | `(mesh, source_publisher, base_version, topology_version)`; applied only when the receiver holds exactly `base_version` of that source and publisher | not retained as a frame | transforms the held remote projection from `base_version` into `topology_version`: births added or changed, identities removed, overlay and deleted changes, loads omitted. In `changed`, each member is a projection entry `{ digest, status }`, the status authored by the mesh-primary alone. Own-mesh members receive the primary's statuses through the primary's versioned member projection. A frame received out of order, with an order key earlier than the newest the receiver holds for that mesh, is ignored in the gossip receive path for every frame kind (the receiver rules below). A delta whose base version the receiver does not hold triggers GetTopology repair. Receivers install the complete snapshot atomically; they do not queue unmatched status events. For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its Digest and status from the projection; NodeStopped and NodeDeleting stay immediate events. |
| [`NodeStopped { op: LifecycleOp, forwarded_by, publisher, topology_version }`](#node-stopped-lifecycle-overlay) | owning mesh channel; backbone MeshTopology.in_flight; peer mesh channels | mesh-primary from matching stop-stream stopped; none for a mesh's last admin after mesh.deleted ([Mesh delete](#mesh-delete)) | peer mesh primaries preserve op | `(kind, build_id, attempt, operation, node_id, incarnation)` | the lifecycle in-flight overlay | subject is stopped; not runtime-exit proof. `publisher` is the exact publishing birth and `topology_version` the version of the subject's mesh at which the subject changed |
| [`NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving`](#reserved-lifecycle-frame-positions) `{ op: LifecycleOp, forwarded_by }` | reserved positions; never published | none: connections-deleting and stopping reach peers as the member's status in its mesh-primary's projection (the `MeshTopology` and `MeshTopologyDelta` rows above); node.connections.deleted is a reply on the call, never gossip | — | — | — | — |
| `NodeLeft { op: LifecycleOp, forwarded_by }` | reserved position; never published | none: a stopping node gossips nothing and its mesh-primary publishes [NodeStopped](#node-stopped-lifecycle-overlay) | — | — | — | — |
| [`NewFabricPrimary { primary, endpoint_id, transport_addr, admin_api_base }`](#reserved-lifecycle-frame-positions) (position 14) | reserved position; never published | none: a primary change rides the successor's `Digest` and the mesh-primary's `MeshTopology` projection, and every birth's contact addresses (`endpoint_id`, `transport_addr`) ride its `Digest` and `MeshTopology` projection | — | — | — | — |
| `MeshStatus { mesh, status, publisher, changed_at_rafka_ms }` | backbone and the mesh primary's own mesh channel; then each peer mesh channel | the mesh primary only, when its Mesh's status changes | peer mesh primaries, unchanged | `(mesh, publisher, changed_at_rafka_ms)` | the held status, until the next change; replayed by its publisher on a join, heal, refeed or top-up | the Mesh's status projection |
| `FabricStatus { fabric, status, publisher, changed_at_rafka_ms }` | backbone and the fabric primary's own mesh channel; then each peer mesh channel | the fabric primary only, when the Fabric's status changes | peer mesh primaries, unchanged | `(publisher, changed_at_rafka_ms)` | the held status, until the next change; replayed by its publisher on a join, heal, refeed or top-up | the Fabric's status projection |
| [`NodeRestarting { op: LifecycleOp, forwarded_by, publisher, topology_version }`](#reserved-lifecycle-frame-positions) (position 7) | reserved position (Q183); never published | none: node.restart is removed; callers run node.stop then node.start, and the stop's stopped is published as [NodeStopped](#node-stopped-lifecycle-overlay) | — | — | — | — |
| `NodeDeleting { op: LifecycleOp, forwarded_by, publisher, topology_version }` | executor's own mesh channel; backbone MeshTopology.in_flight | the mesh executor, once its own journal holds the adopted won claim for the attempt that carries the operation; seeing the Build, a hand-off receipt naming the operation or being the mesh-primary does not start it | peer mesh primaries, onto their mesh channels, unchanged | `(kind, build_id, attempt, operation)`; applied once however many copies arrive | not retained as a frame; the fact rides `MeshTopology.in_flight` until the operation completes | application routing stops selecting the node; the node stays `Found`. `publisher` is the exact publishing birth and `topology_version` the version of the subject's mesh at which the subject changed |
| `NodeDeleted { op: LifecycleOp, forwarded_by }` | executor's own mesh channel; backbone MeshTopology.deleted | the mesh executor, after exact provider proof that the runtime is terminal; none for a mesh's last admin after mesh.deleted, whose final mesh topology carries it deleting ([Mesh delete](#mesh-delete)) | same | `(kind, build_id, attempt, operation)` | the deletion rides `MeshTopology.deleted` for the deleted retention | the birth is removed, its exact identity is held deleted, the resolver answers `Gone`, the current topology no longer lists it |

`operation` is the lifecycle operation's idempotency key (`node.delete:mesh2.rpc.2`), not a
deployment pipeline step: one node.delete runs many steps and publishes one `NodeDeleting` and one
`NodeDeleted`. `(build_id, attempt, operation)` is the only operation identity and is carried on
the operation's Step receipt; no other operation id is minted. Every removal has a Build, and an
operation without that identity is not a lifecycle op.

The material topology projection, which `topology_version` orders and the forwarder's delta is
computed on, is the same normalized view on every node:

```text
DOES bump topology_version
    a birth added, its incarnation or lineage changed, or removed
    an endpoint slot added, removed, or its freshness moved
    a status that routing reads
    RuntimeFact or required current runtime metadata
    an in_flight overlay added or cleared
    a deleted identity added or expired

DOES NOT bump topology_version
    digest_seq alone
    emitted_at_rafka_ms alone
    loads alone
    an unchanged anti-entropy republish
```

A heartbeat is not a topology change; a delta stream that bumped on every heartbeat would be the
re-broadcast it exists to avoid.

Receiver rules:

- a `Digest`, a mesh topology entry, a delta entry or a snapshot entry for a deleted exact birth is refused by name, and so is any later incarnation of a NodeId accepted as `NodeDeleted`, for the deleted retention; the deleted record keeps `(NodeId, IncarnationId)` for exact proof, the refusal keys on the NodeId. A restart never emits `NodeDeleted`, so a new incarnation along the lineage of a node that was not deleted is taken;
- a lifecycle event whose key the receiver already applied is a no-op;
- the `NodeDeleting` overlay, like every lifecycle state and overlay, is read only by application routing, under the [routing rule](lifecycles/states.md#routing-stop); the election and the Node RPC client never read lifecycle;
- a receiving primary forwards a status frame and never restates it as its own;
- a frame received out of order is ignored by one check in the gossip receive path, for every frame kind: a frame whose order key is earlier than the newest the receiver holds from that source is ignored before any handler reads it, whichever channel or read it arrives by. The order keys are the ones on the frames:
  - `MeshTopology` and `MeshTopologyDelta`: `published_at_rafka_ms`, then `topology_version`, per mesh; the publisher reads
    `published_at_rafka_ms` in the same critical section that assigns `topology_version` and `snapshot_id`, so a
    version's stamp is never earlier than the one before it;
  - `Digest`: `digest_seq`, per birth;
  - lifecycle facts (`NodeStopped`, `NodeDeleting`): `topology_version`, per publisher and subject mesh;
- no Rafka-time field decides liveness or `Gone`: silence is local receipt age.

<a id="gossip-correlation"></a>
Correlation: a gossip frame carries no Node RPC target or CallContext. Its envelope carries the
publishing span's trace context (§3.4), so a receive span is that span's child; the publishing span
need not be the operation's ([Telemetry](telemetry.md)). Correlate a frame with its operation by `build_id`, `attempt`, `operation`, `node_id` and
`incarnation`, and a mesh topology by its `publisher`, `topology_version` and `snapshot_id`. A gossip
receiver's TraceId or ParentSpanId need not match the operation's trace.

Clock: every timestamp above is Rafka-time. The mesh substrate takes its clock through the process
composition and invents no wall clock of its own; the product supplies the adopted Rafka-time source.

Size:

- every frame fits one gossip message; a backbone mesh topology and a forwarded full are each chunked under one `snapshot_id`, and every list they carry (`digests`, `in_flight`, `deleted`) is inside that sizing; neither snapshot is installed until every chunk is held;
- there is no ACK, no per-peer resend, no deletion queue and no gossip-level replay: a lost push is repaired by iroh-gossip; a node that missed an event learns the state from the next delta, full or top-up (§3.3).

#### Reserved lifecycle frame positions

The eleven reserved type names in the receive table retain their historical page positions; their
old numeric wire discriminants are not reused. None is published. Each type is refused before data
decode as via-reserved; an unknown type is refused distinctly. node.connection.delete and node.stop are calls, not gossip: connections-deleting,
connections-deleted and stopping reach peers as the member's status in its mesh-primary's member
projection (the `MeshTopology` and `MeshTopologyDelta` rows of the catalog), and node.connections.deleted is a
reply on the call, never gossip. The typed node.stop's stopped is a frame on its call stream, not a
NodeLeft call or node-authored gossip; its mesh-primary publication is
[NodeStopped](#node-stopped-lifecycle-overlay).

```rust
NodeConnectionsDeleting { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NodeLeaving             { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NodeLeft                { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NodeConnectionsDeleted  { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NewFabricPrimary        { primary: Primary, endpoint_id: String, transport_addr: SocketAddr, admin_api_base: Option<String> }, // reserved (R-G3): never published
NodeRestarting          { op: LifecycleOp, forwarded_by: Option<String>, publisher: PublisherId, topology_version: u64 }, // reserved (Q183): never published
Reserved15              { hook: StateHook, forwarded_by: Option<String> }, // reserved (Q171): never published
Reserved16              { hook: StateHook, forwarded_by: Option<String> }, // reserved (Q171): never published
Reserved17              { hook: StateHook, forwarded_by: Option<String> }, // reserved (Q174): never published
Reserved18              { hook: StateHook, forwarded_by: Option<String> }, // reserved (Q174): never published

LifecycleOp {
    build_id: String,
    attempt: u32,
    operation: String,
    node_id: NodeId,
    incarnation: IncarnationId,
    name: PathName,
    event_at_rafka_ms: u64,
}
```

`LifecycleOp` is the body every lifecycle event frame below carries. RDM keeps the five reserved variants and `NodeRestarting` (Q183) in the topology `Frame` enum with byte fixtures. `NewFabricPrimary` (position 14) is likewise a reserved position, never published: a primary change rides the successor's `Digest` and the mesh-primary's `MeshTopology` projection, and every birth's contact addresses (`endpoint_id`, `transport_addr`) ride its `Digest` and `MeshTopology` projection.

#### Node-stopped lifecycle overlay

Sequence: [node.stop](lifecycles/node-stop.md). This section owns the frame and its overlay only:
the mesh-primary publishes NodeStopped from the stop's matching stopped frame, carrying the accepted
operation and exact subject in the lifecycle in-flight overlay.

```rust
NodeStopped { op: LifecycleOp, forwarded_by: Option<String>, publisher: PublisherId, topology_version: u64 },
```

The owning mesh-primary disseminates the overlay on its own mesh channel and in
backbone MeshTopology.in_flight. Peer mesh primaries carry it onto their own mesh channels
under the mesh topology/forwarding rules, preserving operation and subject.
The decoded message is the [NodeStopped example](#examples).
What a stopped node is, and why its endpoint stays reachable, is [States](lifecycles/states.md#stopped-nodes).
The announcement is not node.deleted or provider proof and refreshes no liveness.

node.restart is removed (Q183): callers run node.stop then node.start, and the stop's stopped is
published as NodeStopped. An exit and a new birth under a new node.id are the [recover steps](lifecycles/node-recovery.md), not a restart. No other gossip frame is renamed here.
NodeStopped is a postcard enum variant with byte fixtures; it rides the same in-flight mechanism as
NodeDeleting. NodeStopped stays in the in-flight overlay until the next
lifecycle op on the same `(node.id, incarnation)`. node.start clears it at
node.joined; node.delete replaces it with NodeDeleted's deleted retention.
No timer expires it.

<a id="primary-update-announcements"></a>

#### A primary change on gossip

[node.primary.update](lifecycles/node-primary-update.md) has no frame of its own. The successor's next
`Digest` carries `is_primary` (and `is_fabric_primary`) at the next revision, its normal announcement;
the mesh-primary records it on the node's row and its next `MeshTopology` projection carries it, its normal
publication. The election's result is the fact and the row records it; a node that hears a digest
installs a member fact, never a primary by advertisement. A primary the staleness-floor rule elects is
announced the same way. Best-effort dissemination: a stop does not wait for it (R-NS2).

#### Mesh delete

Sequence: [mesh.delete](lifecycles/mesh-delete.md). On gossip the workflow publishes nothing of its
own beyond the frames in the catalog:

- at `mesh.deleting` the mesh-primary publishes its mesh's status, deleting, as its `MeshStatus`
  frame; peers learn it only from the mesh-primary;
- each member's stop and delete publish exactly what [node.stop](lifecycles/node-stop.md) and
  [node.delete](lifecycles/node-delete.md) publish;
- after `mesh.deleted` the mesh's last node-admin, still its primary, keeps publishing its
  `MeshTopology` on the backbone, itself the only member and the mesh deleted;
- the separate node.delete of that admin, the fabric-primary as executor, ends its publishing:
  when the delete reaches it, its final mesh topology, the mesh deleted and itself deleting, is its
  last gossip, the node.delete's NodeDeleting in mesh topology form. It is best-effort dissemination,
  and the stop does not wait for its delivery (R-NS2). The fabric-primary stops it, proves its exit
  and writes its row deleted, and publishes nothing for the mesh or that admin: no `NodeStopped`, no
  `NodeDeleted`, never the mesh's topology (R-RX4);
- receivers hold the mesh for the deleted retention from that final mesh topology, and after the
  retention nothing refers to the mesh.

#### External topology when a mesh is deleted

Each receiving mesh-admin installs a peer mesh's topology from the backbone into its held
external topology, and its mesh-primary forwards it on its own mesh channel; ordinary nodes
install that projection. During mesh.delete the mesh topology carries the mesh's status deleting and
each member's own state as its node.delete moves it; under the
[routing rule](lifecycles/states.md#routing-stop) none of them is selected for new
application traffic. A member's NodeDeleted moves it to
`deleted`, where it resolves Gone for the deleted retention. After mesh.deleted receivers install
the last admin's mesh topologies as any other; from its final mesh topology, the mesh deleted and its last
admin deleting, they hold the mesh for the deleted retention and then nothing. A delta still requires its exact held base version;
a chunked full installs only when every chunk is held.

#### Examples

Topology frames are postcard `Frame` / `WireDigest` encodings. The JSON below is a readable
decoded view, not a gossip wire codec; the fields are the catalog's above.

A lifecycle event frame carries one `LifecycleOp`. `NodeDeleting` from the executor of a claimed
node.delete:

```json
{
  "frame": "node-deleting",
  "op": {
    "build_id": "bld_1a2b3c4d",
    "attempt": 1,
    "operation": "node.delete:mesh1.rpc.1",
    "node_id": "k8pag1qx95gw",
    "incarnation": "0000000000000008",
    "name": "mesh1.rpc.1",
    "event_at_rafka_ms": 1718290000050
  },
  "forwarded_by": null,
  "publisher": {
    "node": "mesh1.admin.1",
    "incarnation": "0000000000000008"
  },
  "topology_version": 7
}
```

`NodeDeleted` (`node-deleted`, same `op`, a later `event_at_rafka_ms`) carries `op` and
`forwarded_by` only. A peer
primary that forwards one sets `forwarded_by` to itself and changes nothing in `op`.

`NodeStopped` (`node-stopped`) from the mesh-primary of a node that has stopped, published from
that node's matching stopped frame:

```json
{
  "frame": "node-stopped",
  "op": {
    "build_id": "bld_1a2b3c4d",
    "attempt": 1,
    "operation": "node.stop:mesh1.rpc.1",
    "node_id": "k8pag1qx95gw",
    "incarnation": "0000000000000008",
    "name": "mesh1.rpc.1",
    "event_at_rafka_ms": 1718290001050
  },
  "forwarded_by": null,
  "publisher": {
    "node": "mesh1.admin.1",
    "incarnation": "0000000000000008"
  },
  "topology_version": 7
}
```

Publisher and receiver spans: `NodeDeleted` emits
`rdm.node_admin.node.delete.via-node-deleted` at the executor and
`rdm.mesh.topology.remove.via-node-deleted` at each receiver.

A backbone `MeshTopology` from the source mesh-primary, one chunk, one birth, one open
removal in `in_flight`:

```json
{
  "frame": "mesh-topology",
  "mesh": "mesh1",
  "publisher": {
    "node": "mesh1.admin.1",
    "incarnation": "0000000000000008"
  },
  "forwarded_by": null,
  "topology_version": 7,
  "published_at_rafka_ms": 1718290000050,
  "snapshot_id": 42,
  "chunk_index": 0,
  "chunk_count": 1,
  "digests": [
    {
      "fabric_id": "fabric1",
      "node": {
        "node_id": "k8pag1qx95gw",
        "name": "mesh1.rpc.1",
        "endpoint_id": "7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a",
        "transport_addr": "127.0.0.1:54473",
        "incarnation": "0000000000000008",
        "supersedes": null,
        "runtime": null
      },
      "status": "ready",
      "admin_api_base": null,
      "digest_seq": 42,
      "emitted_at_rafka_ms": 1718290000050,
      "data_dir": null,
      "mesh_id": "mesh1",
      "in_flight": null,
      "load": null,
      "gossip": null
    }
  ],
  "in_flight": [
    {
      "build_id": "bld_1a2b3c4d",
      "attempt": 1,
      "operation": "node.delete:mesh1.rpc.1",
      "node_id": "k8pag1qx95gw",
      "incarnation": "0000000000000008",
      "name": "mesh1.rpc.1",
      "event_at_rafka_ms": 1718290000050
    }
  ],
  "deleted": []
}
```

The same birth's own `Digest` heartbeat on its mesh channel is
`{ "frame": "digest", "digest": { … } }`, where the `digest` object is the
[`MeshDigest`](node-rpc-envelope.md#join-op-0x1d-rdm) shown as the one entry of `digests` above.

#### Receive rules: stale, duplicate and gap, per frame

No stale frame may undo a newer fact, and no required fact may be lost because another frame arrived
first ([i147](../plans/i147-gossip-conformance-prd.md)). Three outcomes, decided using the frame's
declared identity and ordering fields, never the transport sender's identity: *stale* (older than what
the receiver holds about the same subject: refused by name), *duplicate* (the same frame again, by its
identity, with identical content: nothing happens), *gap* (ahead of what the receiver holds in a way it
cannot apply: it requests repair, §3.3 and §6, and logs a gap, never "stale"). A frame equal in revision
to the held one but different in content is an inconsistency, refused by name, for every state
projection, primary announcement and snapshot publication. What a receiver may do with any frame: install a projection, record a fact, or carry the frame onward. It decides nothing on a frame (§10: gossip disseminates; the directed call is the certainty). Four categories:

| category | stale | duplicate | gap |
|---|---|---|---|
| replacement state | a lower revision of the same source projection than the one held, including one installed by a topology read | the held revision with identical content | — |
| snapshot or delta | a publication older than the installed one; a delta older than the installed version | a chunk of a publication already installed, or an already-applied delta; distinct chunks of an unfinished publication stay admissible | a delta whose base is ahead of the installed version: repair, never applied, never stale |
| operation fact | — ; an unrelated newer frame does not supersede it, and its effect is reconciled against the same operation's completion and the exact birth's later lifecycle evidence: a completed operation is never reopened | the same operation identity already applied | — |
| reserved | — | — | — ; refused by name, nothing applied |

P(n) is the 20-digit zero-padded decimal representation of the existing u64 version. Context is CloudEvents §3.4; sequencetype is unset. Operation facts omit sequence. Identity admission is in the envelope; carried projection and primary data retain their existing validity rules. A receiver decides nothing on a frame.

| frame / type | category | source / subject | sequence / data validity | id / identity |
|---|---|---|---|---|
| `Digest` (`rdm.mesh.digest`) | replacement state | the author birth / that exact birth | `P(digest_seq)`, higher is newer | birth and digest sequence |
| `MeshTopology` (`rdm.mesh.members`, source publication and forwarded full) | snapshot | the original author birth / `(mesh, source_publisher)` | `P(topology_version)` against the installed floor, including a topology read; equal material version admits a new publication's hearing and distinct unfinished chunks | source projection, topology version, publication owner's exact birth and counter, chunk index; a resend keeps id |
| `MeshTopologyDelta` (`rdm.mesh.members.delta`) | delta | the original author birth / `(mesh, source_publisher)` | `P(topology_version)`; `base_version` in data equals installed to apply, ahead requests repair as via-gap, lower version is stale | source projection, base version and topology version |
| `NodeDeleting`, `NodeStopped` (`rdm.node.deleting`, `rdm.node.stopped`) | operation fact | the author birth / `(build_id, attempt, operation, node_id, incarnation)` | absent; the operation cannot reopen after completion or the exact birth's later lifecycle. The carried `topology_version` is data, checked on the projection's own subject by its existing per-publisher floor rule, even for a repeated fact | kind and the complete operation identity; a repeat runs no effect twice, including when the carried version is refused |
| `NodeDeleted` (`rdm.node.deleted`) | operation fact, terminal | the author birth / `(build_id, attempt, operation, node_id, incarnation)` | absent; stands after unrelated traffic | kind and the complete operation identity |
| `MeshStatus`, `FabricStatus` (`rdm.mesh.status`, `rdm.fabric.status`) | replacement state | the author birth / stable mesh or fabric status scope | `P(the primary's revision) + "." + P(changed_at_rafka_ms)`, lexical order; the publisher takes its revision from the Mesh row. A predecessor's later timestamp loses to the successor's higher revision; within one primary its change time orders changes | subject and composite sequence; equal sequence is duplicate with identical content, inconsistent with different content |
| `Concern` (`rdm.mesh.concern`, position 10) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |
| `NodeConnectionsDeleting`, `NodeLeaving`, `NodeLeft`, `NodeConnectionsDeleted`, `NewFabricPrimary` (`rdm.node.connections.deleting`, `rdm.node.leaving`, `rdm.node.left`, `rdm.node.connections.deleted`, `rdm.fabric.primary.new`) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |
| `Reserved15`, `Reserved16` (`rdm.fabric.state.committing`, `rdm.fabric.state.committed`, positions 16 and 17, Q171) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |
| `Reserved17`, `Reserved18` (`rdm.fabric.traffic.opening`, `rdm.fabric.traffic.opened`, positions 18 and 19, Q174) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |
| `NodeRestarting` (`rdm.node.restarting`, position 7, Q183) | reserved | not read to act | absent; type refused before decoding data | via-reserved, nothing applied |

A publisher change: a delayed predecessor cannot replace the successor's status: the author's
the primary's revision is the first component of status sequence, before change time. The other
source-projection floors remain as specified above. A recovery
through a topology read installs the held version under the receiver's one hold; stale gossip after
it is refused by these rules and never rolls it back. Spans: `rdm.mesh.topology.reject.via-stale`
(frame, subject, held key, frame key), `rdm.mesh.topology.update.via-duplicate` (frame, identity,
`ignored`), `rdm.mesh.topology.update.via-gap` (frame, subject, installed, frame key,
`repair-requested`), `rdm.mesh.topology.reject.via-inconsistent` (frame, subject, revision),
`rdm.mesh.topology.reject.via-reserved` (position).

### 3.2 Cadence: timers only for heartbeat and anti-entropy

```text
PERPETUAL TIMERS
    Digest heartbeat         every node; configurable cadence; never slower than 2 s
    MeshTopology anti-entropy     the mesh primary on the backbone; a few seconds; also sent
                             at once on a material change

EVENT / BURST DRIVEN
    MeshTopologyDelta             on a remote Mesh version change
    NodeDeleting, NodeDeleted, and the later lifecycle events
    MeshStatus, FabricStatus
    mesh state change        the mesh-primary, as MeshStatus
    top-up on join / restart / desync / source revision change
```

A status frame carries a bounded reinforcement: the change is sent at once, then once per second,
five sends in all, then nothing. All five sends carry the same `changed_at_rafka_ms`: they are
reinforcement of one status fact, not five status transitions. That is a short internal timer around
one change, not a heartbeat; no change means no status traffic.

The backbone cadence is `RDM_BACKBONE_INTERVAL_MS`, default 5000 ms.

The own-mesh silence window is derived from the heartbeat cadence and is longer than one tick.
Peer-mesh silence uses direct backbone receipt at node-admins, never forwarded topology.
The fabric-primary marks an unheard peer mesh silent at 30 s under the
[peer-mesh investigation timeline](lifecycles/mesh-investigation.md#mesh-investigation).

### 3.3 Cross-Mesh topology inside a Mesh: deltas, full snapshots and the top-up

The backbone stays the rich source: every node-admin holds every Mesh complete, loads included,
because routing and control decisions are an admin's. Inside a Mesh, ordinary nodes need a remote
Mesh's identity, endpoints with freshness, status, overlays and deletions, and not its loads; one
node changing in a remote Mesh of a hundred must not make every other Mesh re-broadcast all
hundred.

```text
NODE -> OWN MESH            Digest heartbeat, full local facts, <= 2 s
MESH PRIMARY -> BACKBONE    MeshTopology, the complete Mesh with loads, versioned, anti-entropy + change
PEER PRIMARY -> OWN MESH    MeshTopologyDelta, base_version -> topology_version, no loads, on change
ORDINARY NODE -> OWN PRIMARY   top-up: pull the current full fabric topology it holds
```

**Forwarded topology carries topology, never liveness.** An ordinary node retains
peer-mesh members as topology and never ages them to silent for lack of forwarded frames.
Remote liveness is judged only where it is heard: node-admins use direct backbone receipt,
and any node uses its own call results and connection facts, including `carrier-edge-lost`.
A forwarded frame refreshes nobody's liveness, node-admins included. Source versions order
topology; they are not liveness receipts. See the [peer-mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation).

A delta is an optimization. It is never the fact and never the only recovery.

**Versions belong to the source Mesh.** `topology_version` is the source primary's; the source
publisher's exact identity is its revision. A new source primary begins a new revision, and a receiver
installs a full of that revision before applying any of its deltas.

**The forwarder diffs the fulls it holds.** A local primary that held source version 41 and now
holds 43 emits one delta `base_version = 41, topology_version = 43`: the complete difference between
the two projections it holds, not a replay of intermediate events. `base_version` is the version
the forwarder last published into its Mesh, so the forwarder's own missed backbone frames never
break the chain inside the Mesh. A forwarder that becomes the primary publishes a full first; it needs no
stream of its own.

**A version mismatch desynchronizes, and the node tops up.** A receiver applies a delta only when
it holds exactly `base_version` of that source and publisher. On a mismatch it marks that source's
projection desynced, stops applying its deltas, and pulls the current full fabric topology from its
own current mesh-primary with [GetTopology](node-rpc-envelope.md#topology-op-0x1e-rdm), installs it atomically, records
the publisher and version baseline, and resumes. Nothing is held for later and nothing is requested
over gossip. The same top-up serves a join, a restart, a detected gap, a source revision change and
lost local state. The ordinary node never contacts the remote Mesh; its primary already holds the
whole fabric from the backbone.

**A chunked full applies atomically.** A `MeshTopology` full on the backbone larger than one message travels
as chunks under one `snapshot_id` with `chunk_index` and `chunk_count`. The receiver replaces the remote
projection only when it holds every chunk of that snapshot; a snapshot missing a chunk is never
applied and the held projection stays.

**Nothing is pushed periodically to repair a stale copy; a node that notices a gap pulls.** An
ordinary node pulls the current fabric topology from its own mesh-primary, as above. A mesh-primary
holds every source Mesh from the backbone's complete `MeshTopology` snapshots, which carry no base: a lost
one is replaced by the source's next publication, and the forwarder's next delta into its own Mesh runs
from the version it last forwarded. A node that installs a pulled copy older than a delta it dropped while the pull
was in flight pulls again (Q130). A node that noticed nothing wrong keeps what it holds until the
source's next change shows it a gap; no forwarded full and no cadence exists for that.

**Loads are omitted from the ordinary-node cross-Mesh projection, not from the protocol.** They
remain on the backbone. If ordinary nodes are ever made responsible for load-aware remote target
selection, the forwarded frames carry loads again; nothing here makes their absence a law.

**Events keep the fast path, and recover two different ways.** `NodeDeleting` and `NodeDeleted`
are immediate events on the executor's own mesh channel and in the next mesh topology;
`MeshStatus` and `FabricStatus` are immediate on every channel. None waits for a delta.
A node that missed a lifecycle event recovers it from `MeshTopology.in_flight` or `MeshTopology.deleted`,
through the next delta, full or top-up. A node that missed a status frame recovers it only from a
replay of the held status by whoever holds it, on a join, heal, refeed or top-up; a status never
rides inside `MeshTopology`.

The forwarder's own Mesh is a source Mesh too: its members hear every birth directly, so the own-Mesh
delta carries overlays and deletions, and a top-up or full is what a member that missed an event or
was re-fed to the backbone resynchronizes from.

---

### 3.4 The envelope

Every frame on either channel is a [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
event with the [Sequence extension](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/extensions/sequence.md)
(Luke, 2026-10-16: industry standards, CloudEvents). No RDM-named attribute is on the wire; the
catalog's per-frame fields (§3.1) are the values these attributes carry.

| attribute | carries |
|---|---|
| `specversion` | `1.0` |
| `id` | the delivery identity: one per frame, the same on a resend |
| `source` | the author's exact birth, `rdm://<fabric_id>/<node_id>/<incarnation>`; a new incarnation is a new source |
| `type` | the frame kind (`rdm.mesh.digest`, `rdm.mesh.members`, `rdm.node.deleted`, …); a reserved kind is a `type` the receiver refuses by name |
| `subject` | the key the fact is about: a birth, a `(mesh, publisher)` projection, the nodes of one type in one mesh, a fabric |
| `sequence` | the per-`(source, subject)` version: a birth's `digest_seq`, a projection's `topology_version`, a primary's `revision` |
| `time` | rafka-time of the fact; never an ordering |
| `data` | the body, the frame's own fields |
| `datacontenttype` | `application/octet-stream`, always: `data` is postcard bytes; any other value is refused by name |
| `dataschema` | absent on an `rdm.*` frame; on an app event, the app's own schema name for `data`, carried opaque both ways (an executable replacement's slot); RDM reads it for nothing |
| `traceparent`, `tracestate` | the [Distributed Tracing extension](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/extensions/distributed-tracing.md): the author's W3C trace context, so the receive spans parent to the author's trace. Observability only: never dispatch, routing or execution; a malformed value is dropped by name and the frame continues, the same rule as the Node RPC context section (Luke, 2026-10-17) |

The receive rules (§3.1) in these terms: for one `(source, subject)`, a higher `sequence` installs,
an equal one with identical content is a duplicate, a lower one is stale, a gap is repair; a new
incarnation in `source` supersedes the earlier incarnation's facts for that subject. An operation
fact is a subject whose latest value is its completion, so a late or repeated one changes nothing.
Gossip stays best-effort (§7): no acknowledgement and no delivery identity beyond `id`'s duplicate
suppression.

A declared app type (§8, `rafka.entity.<kind>`) rides this same envelope on its own channel (§4).
Its receiver is the app: RDM authors `id`, `source`, `time` and the trace context, carries
`subject`, `sequence` and `data` opaque, and decides nothing on the frame. The receive rules above
apply to the `rdm.*` types only. An event whose complete encoded frame, metadata included, exceeds
`MAX_MESSAGE_BYTES` goes as `rdm.gossip.chunk` frames, each with its own `id` (`<event id>/<i>`), `subject` the event's `id`
and `data { i, n, bytes }`, `n` at most 16; the receiver assembles by `(source, event id)`, takes
a duplicate chunk as nothing and a reordered one in its place, decodes the whole event once under
its own `id`, and evicts an assembly still incomplete after `RDM_STALENESS_MS` by name
(`rdm.mesh.topology.reject.via-chunk-incomplete`). Each channel's receive queue is bounded (1024
events): a full queue drops the newest event by name (`rdm.mesh.channel.reject.via-queue-full`, with
the count) and never holds the carrier, so a slow app stalls nothing but itself. Every event RDM loses
locally on a channel, a drop or an assembly evicted incomplete, advances that channel's loss
generation (`Channel::lost()`), which the app reads to re-pull; a loss that cannot name a subject is
still counted.

Until the envelope lands ([i147 e3](../plans/i147-gossip-conformance-prd.md)) the frames carry the
same values under the catalog's field names; the receive rules are the same either way.

## 4. Channel identity

A channel is one gossip subscription: RDM's word for what iroh-gossip calls a topic (`TopicId`), kept
apart from Rafka's topics. Channel derivation uses logical Fabric/Mesh identity, not transport identity.

Conceptually:

```text
mesh topology channel = f(logical FabricId, MeshId)
backbone channel        = f(logical FabricId)
app channel             = f(logical FabricId, declared type)
```

EndpointId/address data is used to join/dial peers; it is not the logical Fabric identity or channel identity.

---

## 5. Join and first hydration

Sequence: [node.mesh.join](lifecycles/node-mesh-join.md), then [node.start](lifecycles/node-start.md). This
section owns only what gossip is to a joiner:

- the join is a call, [JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm), and gossip plays no part in
  it; the primary learns the birth from the accepted report, never from a frame;
- after the accepted join the birth joins its mesh channel, `f(FabricId, MeshId)`, and the
  node-admin backbone when it is a node-admin; a mesh with no live listener has its channel created
  again by its first listener under the mesh's own MeshId;
- topology is not in the join answer and not learned from gossip first: the joiner reads complete
  source snapshots, the recorded primaries and its own row with [node.topology.get](lifecycles/node-topology-get.md), and later deltas, fulls
  and top-ups (§3.3) keep them current;
- the accepted topology is the current Build, read last in, first out from `builds.storage`; it is
  never reconstructed from Build gossip. Gossip disseminates; it is not the persistence of a Build;
- a node-admin is not Ready because it can observe peers. Ready is readiness to act as a primary, and
  an ordinary joining admin cannot self-Ready around missing hydration; only the Day-0 root and
  fabric recovery's root establish Ready without a live Ready primary
  ([fabric recovery](lifecycles/fabric-recovery.md)).

---

## 6. Repair: held-member stale coverage, not zero neighbours

Two groups of node-admins that cannot hear each other can each keep local neighbours forever. Therefore `neighbor_count == 0` is insufficient as the canonical repair criterion.

A forwarded frame refreshes no node's held-member coverage. A node-admin's coverage of
peer-mesh admins comes only from what it hears directly on the backbone. A peer mesh the
primary cannot hear goes stale in every node-admin of the mesh, and each one re-feeds it.
The primary's forwarded copy cannot keep another admin's coverage fresh; only that admin's
own backbone receipt can do so. Ordinary nodes keep peer-mesh topology without a
forwarded-frame silence timer; their own calls supply their connection facts.

Normal repair policy:

```text
member is still held/current
+ EndpointId/address are known
+ this member/remote coverage has gone stale for the repair window
    -> bounded join/refeed attempt to that held member
```

Required behavior:
- one failed rejoin per peer/window or equivalent bounded backoff;
- failed dial does not mark Dead;
- retries continue only while the member remains held/current;
- relearning the current birth also restores its RuntimeFact + required current runtime metadata through ordinary topology convergence;
- when canonical exact-runtime/lifecycle evidence retires/tombstones the birth, it stops being a refeed target;
- zero-neighbour refeed may remain an edge fallback;
- a channel reopened after the birth left it, and the zero-neighbour refeed, dial the held members
  of the mesh with the peers the channel was seeded or repaired through (Q88); a dial is never a
  neighbour: the neighbour is iroh-gossip's to form;
- reconnecting transport plus held-member repair must merge the views of node-admins that could not hear each other even when each still has neighbours.

Evidence names at least:

```text
channel
peer EndpointId
logical peer NodeId/path when known
reason = stale-held-member | no-neighbours-fallback
silent_for_ms / coverage_age_ms
join result
```

The exact span name follows repository observability canon.

**Expected silence is never stale.** A member whose node.stop this view admitted or applied (stopping;
stopped, `NodeStopped` until node.joined) or whose deletion
it applied (`NodeDeleted`, for the deleted retention) left the view's current members through an event the
view applied, and is not counted as silent. A node-admin acts on its own view: what it hears is its view,
and a question its view cannot answer is answered by its direct operations
([node investigation](lifecycles/node-investigation.md)), never by a rule about its own silence. A
mesh-primary whose mesh has no other live member stays mesh-primary, publishes as its primary and serves
its mesh's topology and rafka-time, and a stopped member's start reads them from it
([node.start](lifecycles/node-start.md) step 4).

### 6.1 Deletions: an event for promptness, `MeshTopology` for retention

A known exact deletion is topology state and a lifecycle event, never a second gossip protocol.

The mesh executor that holds the node.delete publishes `NodeDeleted` (§3.1) on its own mesh
channel, and carries it in `MeshTopology.deleted` on the backbone, the moment it holds exact provider
proof that the runtime is terminal; peer primaries forward it. The operation's Step receipt is
written after `NodeDeleted`. The same deletion is then retained in the source Mesh's `MeshTopology.deleted`
for the deleted retention, so a node that missed the event, joined later or was re-fed after going unheard
learns it from the next mesh topology, delta, full or top-up.

A deletion is recorded only from exact provider proof:
- the controlling executor has exact proof that an intentionally removed runtime is terminal.

These are **not** deletion proof:
- the node's own reply;
- a stopped status by itself;
- silence/staleness;
- failed RPC or failed dial;
- an unheard peer mesh;
- holding the Build claim.

Retention is bounded current topology state, not permanent history: at least the topology
repair horizon, expired by local age since the receiver accepted it; `event_at_rafka_ms` is evidence
and never a remote expiry decision. After retention a formerly `Gone` identity resolves `Unknown`.
`Gone` means this process currently retains the deletion, not that the Fabric
keeps an eternal NodeId graveyard.

A NodeId accepted as `NodeDeleted` stays deleted for the retention: a later digest of the deleted
birth, and a later incarnation wearing that NodeId, are refused. The record keeps the
incarnation for exact proof; the refusal keys on the NodeId. A restart never emits `NodeDeleted`, so a
restarted node that was not deleted is taken along its lineage.

Every running process feeds current births, overlays and deletions from the topology view into its
process-scoped LiveNodeResolver. [node-rpc.md](node-rpc.md) owns the resolver contract and
Found/Gone/Unknown semantics; [node.delete](lifecycles/node-delete.md) owns the executor's workflow,
and the emission points of `NodeDeleting` and `NodeDeleted` are the §3.1 catalog rows and this section.

---

## 7. Dissemination reliability boundary

Ordinary lost pushes are repaired by iroh-gossip Plumtree mechanisms. RDM does not add:
- application ACK/resend protocol for topology facts;
- its own eager/lazy tree;
- a per-peer gossip scheduler competing with iroh-gossip.

The pinned iroh-gossip nonblocking-send fix must ensure one full peer queue cannot block the actor. A dropped full-queue enqueue is recovered through ordinary lazy-push/GRAFT behavior.

Lifecycle events and mesh topologies divide reliability between them:

```text
NodeDeleting / NodeDeleted events
    = prompt notification on the executor's own mesh channel

MeshTopology.in_flight
    = the same open lifecycle overlays, retained while the operation is open

MeshTopology.deleted
    = the same deletions, retained for the deleted retention
```

A connected node reacts to the event at once. A node that missed it, joined later or was re-fed after going unheard learns the same state from the next mesh topology, delta or full it hears. `MeshStatus` and `FabricStatus` are not in the mesh topology: a node that missed one gets the held status replayed on a join, heal, refeed or top-up. No event is retried by the application and no second delivery mechanism exists.

The same boundary holds for cross-Mesh topology inside a Mesh (§3.3): a delta that cannot be applied is dropped and the receiver tops up from its own mesh-primary; nothing asks for a replay over gossip.

RuntimeFact is compact bounded current-birth identity state. Do not replace it with replay of every completed `AllocateIdentity`/`DeployRuntime` Build receipt and do not inflate it with arbitrary operational metadata merely because the topology view carries it. Required current runtime metadata may be projected beside the fact in the current Node/birth view. Build gossip already has a bounded frame budget and completed Build history grows without bound.

---

## 8. Entity projection gossip remains projection, not source of truth

Rafka entity projection gossip may carry durable-row copies into hot in-memory projections. The durable application row remains the source of truth for application entities. Generic RDM topology facts remain the control projection used by Build/leadership.

Projection kind enrolment remains explicit. Provisioning/writing an entity kind does not automatically enroll the caller as a projection consumer.

Message ordering/version rules remain owned by the entity/projection framework, not by the leadership or recovery layer.

**Only projected kinds gossip, and each goes only to the nodes that project it.** A kind is a
declared type, `rafka.entity.<kind>`: the role declares its types once at boot, beside its
`pre_node_ready` hook, and its birth carries them in its [JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm)
(`types`: at most 256 types and 8 KiB encoded, refused by name as `TypesOverCap`; a different
declaration is a restart). No type list rides a digest: 158 kinds today are 4.4 KB of names, more
than a gossip message. Two sets, both answered by `node.get`: `types`, what the birth declared, immutable
for its life (a node-admin's through `Wiring`: its embedded app's own kinds); and `held`, the types
the node has joined, equal to `types` for an ordinary node and, for a node-admin, its own `types`,
its members' declarations and what it learned from other node-admins, growing as members join and
never shrinking within a birth. A node-admin joins its own `types` before its `pre_node_ready` hook, as
every node does. Its mesh's node-admin records the declaration beside the row (a node-admin that
writes no rows yet, the Day-0 admin before it is Ready, holds the declaration and writes it with its
first write after it becomes the primary; the join is answered on the held declaration),
joins every type it does not already hold before it answers the join, and writes its own row's
`held_hash` (blake3 of the sorted `held`), the one hash that rides a digest. Another node-admin
that sees a new `held_hash` on the backbone reads that admin's `node.get` and holds what it
learns, so every projected kind reaches every node-admin (§10: gossip says changed, the call is
the certainty). A
node joins the channel of every type it declared through `TopologyView::join_channel`
(`rdm_transport::gossip`); the channel id is `f(fabric_id, type)` (§4), so two fabrics on one
network never share a kind; its first neighbour is its own mesh's node-admin, already joined. A write is one
`Event { subject, sequence, data }` published once on its kind's channel. So a compute node never
receives a gateway-only kind (`principal`, `service_account`, `path_config` and the rest).

**The event rides the envelope (§3.4) and RDM decides nothing on it.** `subject` is the row key,
`<org>/<kind>/<key>`; `sequence` is the row's own version as the application defines it; `data` is the row, `offset`, `order_key`
and `value` (`None` is a tombstone), opaque to RDM. RDM authors `id`, `source` and `time`, takes
`traceparent` and `tracestate` from the span the publisher is in (the write is the author; RDM
starts no trace of its own and no byte of `data` enters a span), chunks `data` over
`MAX_MESSAGE_BYTES` under one `id`, and hands the receiver the whole event with a span parented
to the author's. Stale, duplicate and gap for a row are the framework's comparator over
`sequence` and `order_key`, across authors, never RDM's. The receive seam holds the row under
the subject's org and refuses an empty org by name. An undeclared type is refused by name
on publish and dropped by name on receive.

**node-admin holds every channel, as the app.** node-admin holds its own declared types and the union
of its members' (`held`), so every projected kind reaches it; it is a Rafka binary embedding RDM, and the rows
it keeps are the app's: the entity framework runs in node-admin as a `Channel` subscriber and applies
the kind's one comparator, the same one its typed stores apply, read from the event's header
`(offset, order_key, version, tombstone)` without decoding the value. RDM holds none of them and
decides nothing on them.

**Entry ends with a pull.** When a gateway or compute node has registered its stores and its gossip loop is
running, it pulls its kinds' current rows from node-admin with Rafka's projection op, served through node-admin's
`serve_app` (`node.topology.get` is topology repair and carries no app row); node-admin answers from its
stores and its raw projection under the one comparator, and the node installs the answer only once it is complete. Live frames
and pulled rows share one apply rule, so nothing gossiped during the pull is lost. Only then does the node mark
itself ready (span `rafka.mesh.entry.update.via-projection-pulled`, with `entity_rows` and `raw_applied`).

A pull that cannot be answered holds the node below `ready`. node-admin refuses a pull it cannot serve (it is not the primary that serves the snapshot, the snapshot will not encode, or it cannot serve the system org's own record) by name (`rafka.mesh.entry.reject.via-projection-refused`). The node names each failed attempt, refused, unreachable or cut short, on `rafka.mesh.entry.reject.via-projection-pull-failed` with its reason, backs off, re-resolves the current primary node-admin from the topology it holds on every attempt, and pulls again from that primary until an answer is applied; only then does it mark itself ready. A `not-born` answer before day 0 is a valid answer and does not hold the node. Every store reads through to the broker on a runtime miss; read-through never stands in for a refused pull. The durable topics remain the completeness backstop: the pull carries the hot working set node-admin holds, not a copy of every broker's whole set.

---

## 9. JoinNode (op `0x1D`) / bootstrap hydration

Sequence: [node.mesh.join](lifecycles/node-mesh-join.md) and [node.start](lifecycles/node-start.md) steps 3 to
5. A joining or restarted node hydrates what its role needs before it is Ready: for a node-admin,
the fabric and mesh identities, its held node-admin peers with their endpoint and address data,
current birth facts with RuntimeFacts and runtime metadata for the births it may manage, the
current Build (LIFO read) and the Build floor the join answer names, and its provider domain's
reachability. It joins its gossip channels before or while it reads, so concurrent frames converge
through the ordinary ordering (§3.1). A late node-admin needs no completed Build history to manage
current runtimes or to know the current accepted topology.

---

## 10. Status dissemination versus status certainty

[States](lifecycles/states.md#states) owns the lifecycle states, who sets them, setting a state and
their checklist; the directed calls are the certainty.

Applied means the target accepted/applied the operation. Gossip independently disseminates the resulting current MemberStatus/state. Neither plane proves, approves, attests, or confirms the other.

RPC failure is not converted into a negative gossip lifecycle state.

The workflows whose events ride these channels are listed in the [lifecycle index](fabric-mesh-node-lifecycles.md#node-workflows).

---

## 11. Leadership relationship

Gossip carries the current candidate facts consumed by leadership. It does not define election semantics.

The **single normative election specification** is [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). This document deliberately does not repeat eligibility, ordering, hierarchy, preemption, or shutdown rules.

Primary bits learned over gossip are projections/hints. Consumers that need the current primary use the election's result rather than rebuilding an election from gossip locally.

---

## 12. An unheard peer mesh

While a peer mesh goes unheard, the elected primary may temporarily differ because current topology views differ. The semantics of that condition are owned by [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md), not redefined here.

Safety comes from:
- exact birth/runtime fencing;
- definitive lifecycle/control receipts;
- idempotent Build operations;
- one fabric-primary topology writer;
- direct-answer fencing before destructive replacement;
- failed reachability is not death.

Heal path: [mesh investigation](lifecycles/mesh-investigation.md) owns it. On the gossip side,
transport reachable again means held-member repair (§6) rejoins the stale peers, their current
birth, RuntimeFact and runtime-metadata facts merge under the catalog's ordering, and the
deterministic leadership converges on the merged facts; a mesh with no primary gets its
node-admin through the investigation and that new mesh-primary publishes once it is established
(R-RX4).

There is no leadership-specific repair protocol.

---

## 13. Build/recovery relationship

Gossip does not decide to recreate a missing node.

Build-channel facts remain broadcast for durability and recovery. The fabric-primary
is the sole Build driver; it claims and calls the executor over the
[Build carrier](node-rpc-envelope.md#build-op-0x20-rdm). Caller progress waits on
no gossip receipt.

For a mesh whose node-admins are unheard the sequence is [mesh investigation](lifecycles/mesh-investigation.md)
for a mesh with no node-admin and [node recovery](lifecycles/node-recovery.md) for a member whose
mesh-primary is live; who claims and executes is [build.md](build.md). Gossip's part is fixed:
canonical runtime and lifecycle evidence, never silence, establishes which births are gone; the
current Build, read from `builds.storage`, establishes what should exist; a node-admin created into
an existing mesh keeps that mesh's MeshId.

Held live ordinary members remain reconciliation inputs and are preserved through their current topology + RuntimeFacts/runtime metadata.

A deletion is published by the executor that holds the operation, after its node.delete step's receipt. Holding a claim does not delete; neither does the node's own `stopped`, a failed RPC, silence or an unheard peer mesh. A lost member is recovered by its mesh-primary ([node recovery](lifecycles/node-recovery.md)): node investigation, then on any verdict but reached one recovery attempt that deletes the unheard node (NodeDeleted from that step) and creates its successor at the same path.name.

---

## 14. Required proofs

At minimum:
1. ordinary nodes never join backbone yet converge all meshes;
2. only mesh primary publishes own mesh topology;
3. receiving peer primary forwards remote mesh topology locally;
4. publisher movement follows canonical leadership;
5. lost pushes recover through ordinary iroh-gossip repair;
6. node-admins that cannot hear each other, each with neighbours, are re-fed through held-member stale-coverage rejoin;
7. failed rejoin never marks Dead;
8. a held member node investigation re-created is removed from repair targets;
9. a restarted/joining node-admin cannot become Ready before required topology + the current Build + RuntimeFact/runtime-metadata + provider-domain hydration;
10. normal joining admin cannot self-Ready around missing hydration; only Day-0/single-admin recovery-root behavior is permitted;
11. late lower-NodeId admin can learn/adopt current runtimes it did not launch without completed Build receipts;
12. provider/bootstrap locator production results in exactly one normalized node-published RuntimeFact, not a competing launcher-owned fact;
13. the current Build comes from durable storage or the JoinNode (op `0x1D`), never from gossip alone;
14. older Build history is not replayed to recover RuntimeFacts or the accepted topology;
15. status certainty remains short RPC plus subsequent gossip dissemination, not gossip guesswork;
16. the members of the executor's own Mesh learn a deletion from the event and from the next mesh topology, without joining the backbone;
17. a late digest of a deleted exact birth is refused, and a later incarnation of a NodeId accepted as `NodeDeleted` is refused for the deleted retention; a restarted node that was not deleted is taken along its lineage;
18. a node that joins or heals after a lifecycle event learns the open overlay from `MeshTopology.in_flight` and the deletion from `MeshTopology.deleted`;
19. a node under `NodeDeleting` is `Found` and is not selected by application routing;
20. the same event received on the own mesh channel and as a forwarded copy is applied once;
21. a mesh topology carrying overlays and deletions still packs within the gossip message bound;
22. a quiet fabric carries only the heartbeat, the backbone mesh topology and the periodic full: no status, lifecycle or delta frame travels without a change;
23. a status change is reinforced five times and then stops; a late joiner learns the status from a join-triggered re-send, not from a heartbeat;
24. one node changing in a remote Mesh produces a delta naming that node, not a re-broadcast of the Mesh;
25. a delta whose `base_version` the receiver does not hold desynchronizes that source's projection, and the node tops up from its own mesh-primary; nothing is requested over gossip;
26. a forwarder that becomes the primary publishes a full before any delta; a new source primary is a new revision and its first delta is never applied before its full;
27. a full missing a chunk is never applied, on the backbone or inside a Mesh; the held projection and the held source version survive until a complete snapshot or a top-up, and no delta is derived from an incomplete one;
28. a stale copy is repaired by the holder's own pull on a noticed gap, never by a periodic push; a mesh-primary's sources are the backbone's complete MeshTopology snapshots, replaced by the source's next publication;
29. the forwarded cross-Mesh projection carries no loads while the backbone mesh topology does;
30. two heartbeats of one birth are ordered by `digest_seq` alone: a stalled or repeated Rafka-time never makes a newer heartbeat look older;
31. a forwarder that missed an intermediate source version emits one delta from the version it last published to the one it holds;
32. a source Mesh whose nodes only heartbeat does not bump `topology_version`, and no delta travels;
33. `NodeDeleting` is never emitted before the executor's own journal holds its adopted claim for the attempt; a multi-mesh Build's second Mesh emits its `NodeDeleting` only after the fabric-primary claims the next attempt and dispatches build.attempt.run and that Mesh's executor adopts the won claim, keyed on that attempt, never on Build acceptance;
34. `NodeDeleted` is never emitted without node.delete's step receipt; silence, a failed RPC and an unheard peer never produce it; after it, `ExactNode(old)` is `Gone`, the path has no node until a new birth, and the current topology does not list the node;
35. unplanned loss produces one `NodeDeleted` for the old identity, after proof, from the recovery Build's node.delete.
