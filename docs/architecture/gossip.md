# Gossip and hierarchical membership architecture

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [States](lifecycles/states.md#states) · [Op contracts](node-rpc-envelope.md#the-op-ledger)

This page owns how facts travel over gossip: the channels, the frame catalog (each frame's fields, author and channels), cadence and timers, peers' receive rules, external topology, deleted retention, held-member repair and frame examples.

Fabric, mesh and node lifecycle states and who sets them are defined only in [States](lifecycles/states.md#states).
Join admission is defined by [JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm).

Gossip is the dissemination/topology plane. It is not a second control RPC system and it is not the authority for destructive recovery decisions.

Companion owners:
- [`build.md`](build.md) for topology authority (one fabric-primary, `Fabric.build_id`), Build/recovery and the RDM/Rafka ownership boundary, and [`deployment-provider.md`](deployment-provider.md) for runtime adoption;
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
       through its own current membership digest/projection
```

There is no second launcher-owned authoritative RuntimeFact stream. For example, a process may self-discover PID + process-start token, while a container provider learns the immutable container ID after `docker run` and makes it available to the child before Ready.

Operational metadata required by successor management, such as `data_dir`, is current Node/runtime projection state, not part of exact RuntimeFact identity merely because it must converge to future authorities.

---

## 2. Ownership

```text
Iroh
    authenticated EndpointId + QUIC paths

iroh-gossip
    HyParView topic membership
    Plumtree eager/lazy dissemination and repair
    neighbour state and peer queues

RDM membership/topology layer
    topic derivation
    known/held peer directory
    current birth facts, including RuntimeFact + required runtime metadata
    hierarchical mesh/backbone publisher policy
    bounded held-member rejoin policy

Fabric control state
    Fabric.build_id + the accepted Build
    active Build facts

Build/lifecycle/leadership
    accepted topology (fabric-primary only)
    exact runtime adoption/death proof
    destructive reconciliation
    primary projection
```

Gossip silence, neighbour loss, failed dial, or missing push never proves a node Dead by itself.

Completed Build receipt history is not required to recover current runtime identity and is not replayed merely so a future authority can manage a birth.

---

## 3. Hierarchical membership topology

Each Mesh has its own membership channel derived from MeshId.

```text
ordinary member
    -> own mesh membership channel only

node-admin
    -> own mesh membership channel
    -> inter-mesh node-admin backbone

mesh primary
    -> publishes own mesh aggregate on backbone

receiving peer mesh primary
    -> forwards remote topology onto own mesh channel
       as deltas backed by periodic full snapshots, loads omitted

mesh executor (the primary holding a claimed attempt)
    -> publishes lifecycle events on its own mesh channel; the backbone carries them
       in its Members aggregate (in_flight, deleted)

receiving peer mesh primary
    -> forwards remote lifecycle events onto own mesh channel
```

Rules:
- ordinary nodes never join the backbone;
- only node-admins join the backbone;
- the current mesh primary from the canonical LeadershipView publishes its own mesh's aggregate on the backbone; a mesh with no primary goes unheard: nobody publishes it on its behalf, [mesh investigation](lifecycles/mesh-investigation.md) gives it a node-admin, and that new mesh-primary publishes once it is established (R-RX4);
- only receiving peer mesh primaries forward remote aggregates locally;
- lifecycle events are published by the executor on its own mesh channel and reach the backbone only inside Members.in_flight and Members.deleted;
- ordinary nodes converge to all-mesh topology without a flat fabric-wide membership topic;
- publisher responsibility follows imported leadership changes;
- leadership projections are derived by the canonical LeadershipView, not trusted because a publisher claimed them;
- the aggregate/current birth projection preserves the current RuntimeFact and required current runtime metadata needed by an authority-bearing admin to manage that birth.

Use **forward** for this topology action. The transport's relay servers are disabled on every endpoint (`relay_mode Disabled`) and the word names nothing here.

### 3.1 Frame catalog

Nothing in the receive path checks who sent a frame: a frame is taken or ignored by what it carries, never by its sender.

A mesh-channel delta, a snapshot install and a topology read install move the held source
version and the book under one hold of the receiver. A reader of that version never finds
an older book.

Two channels carry membership: a Mesh's membership channel (one per MeshId) and the node-admin
backbone (one per FabricId). The Build topic is a third, separate channel for Build facts
(`Accepted`, `Opened`, `Claim`, `Step`, `Attempt`, `Forget`); it is not membership and is not in
this catalog. No other topic exists.

Four orderings, four jobs, never substituted for one another:

```text
digest_seq            orders one birth's own heartbeats          (sender-local, per incarnation)
topology_version      orders a Mesh's material topology states   (source mesh-primary)
Rafka-time            says when a fact or publication happened    (never liveness or Gone; the first key of a Members publication's order)
local Instant         silence and elapsed time                    (receiver-local, never a remote clock)
```

| frame | channel | author | forwarded by | identity / dedup | retained | use |
|---|---|---|---|---|---|---|
| [`NewFabricPrimary { holder, endpoint_id, transport_addr, admin_api_base }`](#new-fabric-primary-and-contact-addresses), frame `new-fabric-primary` | backbone | committed fabric-primary | no new authority by forwarding | exact holder birth/revision; contacts belong to that holder | held fabric seat record and matching contact information | discover the committed fabric-primary and its Node RPC endpoint and independent application contact |
| `Digest { node, incarnation, endpoints with freshness, node_state, runtime, digest_seq, emitted_at_rafka_ms }` | own mesh channel | every node, on its heartbeat timer | nobody | same exact birth: higher `digest_seq` is newer, equal or lower is not taken; a new incarnation is taken only along its lineage and only for a NodeId not accepted as `NodeDeleted`; its `digest_seq` starts again at 1 | until silent for the silence window, or until `NodeDeleted` | that node's current identity, endpoints, liveness and runtime fact. `node_state` (Joining / Alive / Overloaded / Degraded, node-identity-and-state.md §4) is liveness only: a heartbeat never carries or moves a status, and a member's status comes only from its mesh-primary |
| `Members { mesh, publisher (exact identity), forwarded_by, topology_version, published_at_rafka_ms, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted }` | backbone; and the source Mesh's primary's own mesh channel (its own member projection) | the source Mesh's current primary; after mesh.deleted its last node-admin, still its primary, until it is stopped or deleted; nobody else, ever: the fabric-primary never publishes a mesh as its acting primary (R-RX4) | nobody as is; a receiving primary derives the two frames below from it | `(mesh, publisher, topology_version, snapshot_id)`; a receiving admin installs the snapshot atomically once it holds every chunk, and only then advances the held source version or derives a delta; the version bumps on a material change of the normalized projection below and not on an unchanged republish; the publisher's exact identity is the version's revision | `digests`: as held; `in_flight`: while the operation is claimed and has no completed receipt (derived from Build facts each round, never stored), and NodeStopped until the next lifecycle op on that exact birth; `deleted`: local age since accepted, up to the deleted retention | the complete Mesh with loads: what every node-admin holds of every Mesh for routing and control decisions. In `digests`, each member is a projection entry `{ digest, status }`, the status authored by the mesh-primary alone. Own-mesh members receive authoritative statuses through the primary's versioned member projection. One mesh has one publisher on the backbone, its mesh-primary, and none while it has no primary (R-RX4), so a receiver takes its Members and MembersDelta as they arrive. A frame received out of order, with an order key earlier than the newest the receiver holds for that mesh, is ignored in the gossip receive path for every frame kind (the receiver rules below). Receivers install the complete snapshot atomically; they do not queue unmatched status events. For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its Digest and status from the projection; NodeStopped, NodeDeleting and NodeRestarting stay immediate events. |
| `MembersDelta { mesh, source_publisher, base_version, topology_version, published_at_rafka_ms, changed, removed, in_flight, deleted }` | the forwarding primary's own mesh channel | the local mesh primary, when a source Mesh it holds moved from one version to another; and the source Mesh's primary, for its own Mesh, on its own mesh channel | nobody | `(mesh, source_publisher, base_version, topology_version)`; applied only when the receiver holds exactly `base_version` of that source and publisher | not retained as a frame | transforms the held remote projection from `base_version` into `topology_version`: births added or changed, identities removed, overlay and deleted changes, loads omitted. In `changed`, each member is a projection entry `{ digest, status }`, the status authored by the mesh-primary alone. Own-mesh members receive authoritative statuses through the primary's versioned member projection. A frame received out of order, with an order key earlier than the newest the receiver holds for that mesh, is ignored in the gossip receive path for every frame kind (the receiver rules below). A delta whose base version the receiver does not hold triggers GetTopology repair. Receivers install the complete snapshot atomically; they do not queue unmatched status events. For an own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its Digest and status from the projection; NodeStopped, NodeDeleting and NodeRestarting stay immediate events. |
| `Members` forwarded full `{ …, snapshot_id, chunk_index, chunk_count }` | the forwarding primary's own mesh channel | the local mesh primary: every `FULL_EVERY` rounds as passive anti-entropy | nobody | `(mesh, source_publisher, topology_version, snapshot_id)`; applied atomically once every chunk is held | the held remote projection, until the next full or top-up | the complete remote Mesh without loads |
| [`NodeStopped { op: LifecycleOp, forwarded_by, publisher, topology_version }`](#node-stopped-lifecycle-overlay) | owning mesh channel; backbone Members.in_flight; peer mesh channels | mesh-primary from matching stop-stream stopped; none for a mesh's last admin after mesh.deleted ([Mesh delete](#mesh-delete)) | peer mesh primaries preserve op | `(kind, build_id, attempt, operation, node_id, incarnation)` | the lifecycle in-flight overlay | subject is stopped; not runtime-exit proof. `publisher` is the exact publishing birth and `topology_version` the version of the subject's mesh at which the subject changed |
| [`primary updating`, `primary updated`](#primary-update-announcements) | mesh channel; backbone | the mesh-primary (the holder itself for the `node-admin` cohort), the sole author of member status: "primary updating" before the seat row, "primary updated" after `node.primary.updated`; a node of another cohort never publishes its own seat | peer mesh primaries preserve operation and subject | `(operation, cohort, committed revision)` | announcements only | name the operation, the cohort, both exact births and the revisions; the committed seat row is the authority; a late "updating" never undoes an "updated" |
| `Seated { seat, holder }` | the holder's mesh channel; backbone | the mesh-primary of the seat's mesh when the ordinary rule fills a seat there with no node.primary.update (its own mesh-primary seat and each other cohort's primary seat, a holder seated again after a failed update included); the fabric-primary for the fabric seat | peer mesh primaries preserve seat, holder and revision and never author a result | the holder record's revision, per seat key (`(mesh, node_type)` for a cohort, `fabric` for the fabric seat); an older record is ignored in the receive path | until the next record of that seat | the ordinary election's holder record with its exact birth ([elections §3](fabric-node-lifecycle-elections.md#3-election-key)), an election input; never sufficient to override a committed seat row |
| [`NodeConnectionsDeleting`, `NodeConnectionsDeleted`, `NodeLeaving`](#reserved-lifecycle-frame-positions) `{ op: LifecycleOp, forwarded_by }` | reserved positions; never published | none: connections-deleting and stopping reach peers as the member's status in its mesh-primary's projection (the `Members` and `MembersDelta` rows above); node.connections.deleted is a reply on the call, never gossip | — | — | — | — |
| `NodeLeft { op: LifecycleOp, forwarded_by }` | reserved position; never published | none: a stopping node gossips nothing and its mesh-primary publishes [NodeStopped](#node-stopped-lifecycle-overlay) | — | — | — | — |
| `MeshStatus { mesh, status, publisher, changed_at_rafka_ms }` | backbone and the mesh primary's own mesh channel; then each peer mesh channel | the mesh primary only, when its Mesh's status changes | peer mesh primaries, unchanged | `(mesh, publisher, changed_at_rafka_ms)` | the held status, until the next change; replayed by its holder on a join, heal, refeed or top-up | the Mesh's status projection |
| `FabricStatus { fabric, status, publisher, changed_at_rafka_ms }` | backbone and the fabric primary's own mesh channel; then each peer mesh channel | the fabric primary only, when the Fabric's status changes | peer mesh primaries, unchanged | `(publisher, changed_at_rafka_ms)` | the held status, until the next change; replayed by its holder on a join, heal, refeed or top-up | the Fabric's status projection |
| `NodeRestarting { op: LifecycleOp, forwarded_by, publisher, topology_version }` | executor's own mesh channel; backbone Members.in_flight | the mesh-primary of the restarting node | peer mesh-primary derives external projection from aggregate | `(kind, build_id, attempt, operation)` | while restart operation is open | keep same logical node known; no Gone from expected silence. `publisher` is the exact publishing birth and `topology_version` the version of the subject's mesh at which the subject changed |
| `NodeDeleting { op: LifecycleOp, forwarded_by, publisher, topology_version }` | executor's own mesh channel; backbone Members.in_flight | the mesh executor, once its own journal holds the adopted won claim for the attempt that carries the operation; seeing the Build, a hand-off receipt naming the operation or holding the primary seat does not start it | peer mesh primaries, onto their mesh channels, unchanged | `(kind, build_id, attempt, operation)`; applied once however many copies arrive | not retained as a frame; the fact rides `Members.in_flight` until the operation completes | application routing stops selecting the node; the node stays `Found`. `publisher` is the exact publishing birth and `topology_version` the version of the subject's mesh at which the subject changed |
| `NodeDeleted { op: LifecycleOp, forwarded_by }` | executor's own mesh channel; backbone Members.deleted | the mesh executor, after exact provider proof that the runtime is terminal; none for a mesh's last admin after mesh.deleted, whose final aggregate carries it deleting ([Mesh delete](#mesh-delete)) | same | `(kind, build_id, attempt, operation)` | the deletion rides `Members.deleted` for the deleted retention | the birth is removed, its exact identity is held deleted, the resolver answers `Gone`, the current topology no longer lists it |

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

- a `Digest`, an aggregate entry, a delta entry or a snapshot entry for a deleted exact birth is refused by name, and so is any later incarnation of a NodeId accepted as `NodeDeleted`, for the deleted retention; the deleted record keeps `(NodeId, IncarnationId)` for exact proof, the refusal keys on the NodeId. A restart never emits `NodeDeleted`, so a new incarnation along the lineage of a node that was not deleted is taken;
- a lifecycle event whose key the receiver already applied is a no-op;
- the `NodeDeleting` overlay, like every lifecycle state and overlay, is read only by application routing, under the [routing rule](lifecycles/states.md#routing-stop); the resolver and the Node RPC client never read lifecycle;
- a receiving primary forwards a status frame and never restates it as its own;
- a frame received out of order is ignored by one check in the gossip receive path, for every frame kind: a frame whose order key is earlier than the newest the receiver holds from that source is ignored before any handler reads it, whichever channel or read it arrives by. The order keys are the ones on the frames:
  - `Members` and `MembersDelta`: `published_at_rafka_ms`, then `topology_version`, per mesh; the publisher reads
    `published_at_rafka_ms` in the same critical section that assigns `topology_version` and `snapshot_id`, so a
    version's stamp is never earlier than the one before it;
  - `Digest`: `digest_seq`, per birth;
  - lifecycle facts (`NodeStopped`, `NodeDeleting`, `NodeRestarting`): `topology_version`, per publisher and subject mesh;
  - `Seated`: the holder record's revision, per seat key (`(mesh, node_type)` for a cohort, `fabric` for the fabric seat);
  - `primary updating` and `primary updated`: the committed revision, per seat key; at one revision "updated" is after "updating".
- no Rafka-time field decides liveness or `Gone`: silence is local receipt age.

<a id="gossip-correlation"></a>
Correlation: a gossip frame carries no Node RPC target or CallContext and has no W3C parent edge.
Correlate a frame with its operation by `build_id`, `attempt`, `operation`, `node_id` and
`incarnation`, and an aggregate by its `publisher`, `topology_version` and `snapshot_id`. A gossip
receiver's TraceId or ParentSpanId need not match the operation's trace.

Clock: every timestamp above is Rafka-time. The mesh substrate takes its clock through the process
composition and invents no wall clock of its own; the product supplies the adopted Rafka-time source.

Size:

- every frame fits one gossip message; a backbone aggregate and a forwarded full are each chunked under one `snapshot_id`, and every list they carry (`digests`, `in_flight`, `deleted`) is inside that sizing; neither snapshot is installed until every chunk is held;
- a node's `config` map rides every heartbeat, so it is capped at 32 keys and 2048 bytes (`MAX_CONFIG_KEYS`, `MAX_CONFIG_BYTES`, `rust-distributed-mesh/crates/rafka-mesh-entity/src/digest.rs`) and refused by name, with the measured size and the bound, as `ConfigOverCap`. The two numbers are set, not computed from the 4032-byte gossip message: config takes about half of it and the digest's other fields the rest. A change to them is a change to that budget in the code, never a runtime percentage. [Meta and config](node-rpc-envelope.md#node-properties) owns the config rules;
- there is no ACK, no per-peer resend, no deletion queue and no gossip-level replay: a lost push is repaired by iroh-gossip; a node that missed an event learns the state from the next delta, full or top-up (§3.3).

#### Reserved lifecycle frame positions

The frame enum keeps the positions of NodeConnectionsDeleting, NodeLeaving, NodeLeft and NodeConnectionsDeleted. None of
the four is published. node.connections.delete and node.stop are calls, not gossip: connections-deleting,
connections-deleted and stopping reach peers as the member's status in its mesh-primary's member
projection (the `Members` and `MembersDelta` rows of the catalog), and node.connections.deleted is a
reply on the call, never gossip. The typed node.stop's stopped is a frame on its call stream, not a
NodeLeft call or node-authored gossip; its mesh-primary publication is
[NodeStopped](#node-stopped-lifecycle-overlay).

```rust
NodeConnectionsDeleting { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NodeLeaving             { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NodeLeft                { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published
NodeConnectionsDeleted  { op: LifecycleOp, forwarded_by: Option<String> }, // reserved: never published

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

`LifecycleOp` is the body every lifecycle event frame below carries. RDM keeps the four reserved variants in the membership `Frame` enum with byte fixtures.

#### Node-stopped lifecycle overlay

Sequence: [node.stop](lifecycles/node-stop.md). This section owns the frame and its overlay only:
the mesh-primary publishes NodeStopped from the stop's matching stopped frame, carrying the accepted
operation and exact subject in the lifecycle in-flight overlay.

```rust
NodeStopped { op: LifecycleOp, forwarded_by: Option<String>, publisher: PublisherId, topology_version: u64 },
```

The owning mesh-primary disseminates the overlay on its own mesh channel and in
backbone Members.in_flight. Peer mesh primaries carry it onto their own mesh channels
under the aggregate/forwarding rules, preserving operation and subject.
The decoded message is the [NodeStopped example](#examples).
What a stopped node is, and why its endpoint stays reachable, is [States](lifecycles/states.md#stopped-nodes).
The announcement is not node.deleted or provider proof and refreshes no liveness.

Inside node.restart, the stop's stopped is published as NodeRestarting, not NodeStopped;
peers hold the instance and expect it back. NodeRestarting stays until node.started of the same `(node.id, incarnation)` arrives; a digest with a different incarnation does not complete it. An exit and relaunch under a new node.id is the [recover recipe](lifecycles/node-recovery.md), not a restart. No other gossip frame is renamed here.
NodeStopped is a postcard enum variant with byte fixtures; it rides the same in-flight mechanism as NodeRestarting
and NodeDeleting. NodeStopped stays in the in-flight overlay until the next
lifecycle op on the same `(node.id, incarnation)`. node.start clears it at
node.joined; node.delete replaces it with NodeDeleted's deleted retention.
No timer expires it.

<a id="primary-update-announcements"></a>

#### Primary update announcements

[node.primary.update](lifecycles/node-primary-update.md) has two announcements on the mesh channel and the
backbone: "primary updating" before the writer writes the seat row, and "primary updated" after the
successor answers `node.primary.updated`. Each names the operation, the cohort, both exact births and the
revisions. The announcer is the mesh-primary (the holder itself for the `node-admin` cohort), the sole
author of member status; a node of another cohort never publishes its own seat.

Peer mesh primaries carry them under the forwarding rules, preserving operation and subject. They are
announcements, never authority: the committed seat row is the authority, and a node that hears an
announcement installs nothing until the row names the revision. A late "updating" never undoes an
"updated": the committed revision, per cohort, is their order key in the one gossip receive path
(R-RX3). They are best-effort dissemination, and a stop does not wait for their delivery (R-NS2).

#### Fabric round gossip hooks

The fabric state-commit and traffic-opening pairs run these hooks in the same accepted
command/completion paths as their [Status calls](node-rpc-envelope.md#fabric-round-command-pairs):

| directed call | hook frame | author |
|---|---|---|
| commit-state | StateCommitting | commanding authority |
| state-committed | StateCommitted | subject sending its completed check-in |
| open-traffic | TrafficOpening | commanding authority |
| traffic-opened | TrafficOpened | subject sending its completed check-in |

Each frame carries these fields:

```rust
fabric_id: FabricId,
node_id: NodeId,
incarnation: IncarnationId,
build_id: String,
attempt: u32,
operation: String,
publisher: String,
event_at_rafka_ms: u64,
forwarded_by: Option<String>,
```

The subject is the exact commanded/reporting birth. Publisher is the author at that
hop. Node-admin authors publish on their own mesh channel and backbone; ordinary
node authors publish on their own mesh channel and their mesh-admin carries the same
record onto the backbone. Peer mesh primaries forward without changing authored
fields. Identity is `(kind, fabric_id, build_id, attempt, operation, node_id, incarnation)`.
Neither timestamp nor forwarded receipt supplies liveness or operation authority.

What a check-in is and when a primary may report upward is the
[round checklist](lifecycles/states.md#the-round-checklist). Gossip receipt cannot replace
the authenticated directed completion. Lost events are repaired through the held
round/accepted Build evidence; snapshot installation preserves the accepted round facts and
never infers completion from a count of gossip frames. MeshStatus/FabricStatus frames
disseminate committed lifecycle/health projections with their original author and timestamp.

RDM implements these frame variants in the membership `Frame` enum with byte fixtures.

#### Mesh delete

Sequence: [mesh.delete](lifecycles/mesh-delete.md). On gossip the workflow publishes nothing of its
own beyond the frames in the catalog:

- at `mesh.deleting` the mesh-primary publishes its mesh's status, deleting, as its `MeshStatus`
  frame; peers learn it only from the mesh-primary;
- each member's stop and delete publish exactly what [node.stop](lifecycles/node-stop.md) and
  [node.delete](lifecycles/node-delete.md) publish;
- after `mesh.deleted` the mesh's last node-admin, still its primary, keeps publishing its
  `Members` aggregate on the backbone, itself the only member and the mesh deleted;
- the separate node.delete of that admin, the fabric-primary as executor, ends its publishing:
  when the delete reaches it, its final aggregate, the mesh deleted and itself deleting, is its
  last gossip, the node.delete's NodeDeleting in aggregate form. It is best-effort dissemination,
  and the stop does not wait for its delivery (R-NS2). The fabric-primary stops it, proves its exit
  and writes its row deleted, and publishes nothing for the mesh or that admin: no `NodeStopped`, no
  `NodeDeleted`, never the mesh's aggregate (R-RX4);
- receivers hold the mesh for the deleted retention from that final aggregate, and after the
  retention nothing refers to the mesh.

#### External topology when a mesh is deleted

Each receiving mesh-admin installs a peer mesh's aggregate from the backbone into its held
external topology, and its mesh-primary forwards it on its own mesh channel; ordinary nodes
install that projection. During mesh.delete the aggregate carries the mesh's status deleting and
each member's own state as its node.delete moves it; under the
[routing rule](lifecycles/states.md#routing-stop) none of them is selected for new
application traffic. A member's NodeDeleted moves it to
`deleted`, where it resolves Gone for the deleted retention. After mesh.deleted receivers install
the last admin's aggregates as any other; from its final aggregate, the mesh deleted and its last
admin deleting, they hold the mesh for the deleted retention and then nothing. A delta still requires its exact held base version;
a chunked full installs only when every chunk is held.

#### Examples

Membership frames are postcard `Frame` / `WireDigest` encodings. The JSON below is a readable
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

`NodeRestarting` (`node-restarting`, operation `node.restart:mesh1.rpc.1`) has the same body.
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

Publisher and receiver spans: `NodeRestarting` emits
`rdm.node_admin.node.update.via-node-restarting` at the publishing mesh-primary and
`rdm.mesh.membership.update.via-node-restarting` at each receiver; `NodeDeleted` emits
`rdm.node_admin.node.delete.via-node-deleted` at the executor and
`rdm.mesh.membership.remove.via-node-deleted` at each receiver.

A backbone `Members` aggregate from the source mesh-primary, one chunk, one birth, one open
restart in `in_flight`:

```json
{
  "frame": "members",
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
      "status": "ready-for-traffic",
      "admin_api_base": null,
      "digest_seq": 42,
      "emitted_at_rafka_ms": 1718290000050,
      "data_dir": null,
      "mesh_id": "mesh1",
      "in_flight": null,
      "config": {},
      "load": null,
      "gossip": null
    }
  ],
  "in_flight": [
    {
      "build_id": "bld_1a2b3c4d",
      "attempt": 1,
      "operation": "node.restart:mesh1.rpc.1",
      "node_id": "k8pag1qx95gw",
      "incarnation": "0000000000000008",
      "name": "mesh1.rpc.1",
      "event_at_rafka_ms": 1718290000050
    }
  ],
  "deleted": []
}
```

The backbone `new-fabric-primary` frame published by a successor fabric-primary after the seat
transfer commits:

```json
{
  "frame": "new-fabric-primary",
  "holder": {
    "mesh": "mesh2",
    "node_id": "n3w5b7r9t2qx",
    "incarnation": "0000000000000001",
    "revision": 8
  },
  "endpoint_id": "5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c5c",
  "transport_addr": "127.0.0.1:54474",
  "admin_api_base": "http://127.0.0.1:41778"
}
```

The same birth's own `Digest` heartbeat on its mesh channel is
`{ "frame": "digest", "digest": { … } }`, where the `digest` object is the
[`MeshDigest`](node-rpc-envelope.md#join-op-0x1d-rdm) shown as the one entry of `digests` above.

### 3.2 Cadence: timers only for heartbeat and anti-entropy

```text
PERPETUAL TIMERS
    Digest heartbeat         every node; configurable cadence; never slower than 2 s
    Members anti-entropy     the mesh primary on the backbone; a few seconds; also sent
                             at once on a material change
    forwarded full           the forwarding primary inside its Mesh; every FULL_EVERY rounds

EVENT / BURST DRIVEN
    MembersDelta             on a remote Mesh version change
    NodeDeleting, NodeDeleted, and the later lifecycle events
    MeshStatus, FabricStatus
    mesh state change        the mesh-primary, as MeshStatus
    top-up on join / restart / desync / source revision change
```

A status frame carries a bounded reinforcement: the change is sent at once, then once per second,
five sends in all, then nothing. All five sends carry the same `changed_at_rafka_ms`: they are
reinforcement of one status fact, not five status transitions. That is a short internal timer around
one change, not a heartbeat; no change means no status traffic.

Rafka's backbone cadence is `RAFKA_BACKBONE_INTERVAL_MS`, default 2000 ms.

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
MESH PRIMARY -> BACKBONE    Members, the complete Mesh with loads, versioned, anti-entropy + change
PEER PRIMARY -> OWN MESH    MembersDelta, base_version -> topology_version, no loads, on change
                            forwarded full, no loads, every FULL_EVERY rounds (passive anti-entropy)
ORDINARY NODE -> OWN PRIMARY   top-up: pull the current full fabric topology it holds
```

**Forwarded topology carries topology, never liveness.** An ordinary node retains
peer-mesh members as topology and never ages them to silent for lack of forwarded frames.
Remote liveness is judged only where it is heard: node-admins use direct backbone receipt,
and any node uses its own call results and connection facts, including `carrier-edge-lost`.
A forwarded frame refreshes nobody's liveness, node-admins included. Source versions order
topology; they are not liveness receipts. See the [peer-mesh investigation](lifecycles/mesh-investigation.md#mesh-investigation).

A delta is an optimization. It is never authority and never the only recovery.

**Versions belong to the source Mesh.** `topology_version` is the source primary's; the source
publisher's exact identity is its revision. A new source primary begins a new revision, and a receiver
installs a full of that revision before applying any of its deltas.

**The forwarder diffs the fulls it holds.** A local primary that held source version 41 and now
holds 43 emits one delta `base_version = 41, topology_version = 43`: the complete difference between
the two projections it holds, not a replay of intermediate events. `base_version` is the version
the forwarder last published into its Mesh, so the forwarder's own missed backbone frames never
break the chain inside the Mesh. A forwarder that takes the seat publishes a full first; it needs no
stream of its own.

**A version mismatch desynchronizes, and the node tops up.** A receiver applies a delta only when
it holds exactly `base_version` of that source and publisher. On a mismatch it marks that source's
projection desynced, stops applying its deltas, and pulls the current full fabric topology from its
own current mesh-primary with [GetTopology](node-rpc-envelope.md#topology-op-0x1e-rdm), installs it atomically, records
the publisher and version baseline, and resumes. Nothing is held for later and nothing is requested
over gossip. The same top-up serves a join, a restart, a detected gap, a source revision change and
lost local state. The ordinary node never contacts the remote Mesh; its primary already holds the
whole fabric from the backbone.

**A chunked full applies atomically.** A forwarded full larger than one message travels as chunks
under one `snapshot_id` with `chunk_index` and `chunk_count`. The receiver replaces the remote
projection only when it holds every chunk of that snapshot; a snapshot missing a chunk is never
applied and the held projection stays.

**`FULL_EVERY` is a bounded, configurable number of aggregate rounds.** With the top-up, it bounds
only how long a node that noticed nothing wrong holds a stale projection, so it is passive
anti-entropy. Its default is chosen from measured stripped-full sizes under the scale and chaos
proofs.

**Loads are omitted from the ordinary-node cross-Mesh projection, not from the protocol.** They
remain on the backbone. If ordinary nodes are ever made responsible for load-aware remote target
selection, the forwarded frames carry loads again; nothing here makes their absence a law.

**Events keep the express lane, and recover two different ways.** `NodeDeleting` and `NodeDeleted`
are immediate events on the executor's own mesh channel and in the next Members aggregate;
`MeshStatus` and `FabricStatus` are immediate on every channel. None waits for a delta.
A node that missed a lifecycle event recovers it from `Members.in_flight` or `Members.deleted`,
through the next delta, full or top-up. A node that missed a status frame recovers it only from a
replay of the held status by whoever holds it, on a join, heal, refeed or top-up; a status never
rides inside `Members`.

The forwarder's own Mesh is a source Mesh too: its members hear every birth directly, so the own-Mesh
delta carries overlays and deletions, and a top-up or full is what a member that missed an event or
was re-fed to the backbone resynchronizes from.

---

## New-fabric-primary and contact addresses

The `new-fabric-primary` gossip frame announces the committed fabric-primary and
its contact addresses on the **backbone channel**. The
[decoded example](#examples)
shows a complete message.

```rust
NewFabricPrimary {
    holder: SeatHolder, // mesh, node_id, incarnation, revision
    endpoint_id: String,
    transport_addr: SocketAddr,
    admin_api_base: Option<String>,
}
```

The announcer supplies the exact holder birth's endpoint key, actual bound QUIC UDP
address and application-supplied contact URL including its actual bound port.
The contacts are independent; neither port is inferred from the other. These contact fields
are outside SeatHolder: changing an address does not change a seat's identity or revision.
An embedding application supplies admin_api_base when it exposes a contact surface. RDM serves Node RPC.

A receiver applies the seat's revision/birth acceptance rules and associates the contacts
only with that exact holder. A delayed announcement must not overwrite newer contact
information learned from that birth's digest. Contact changes continue through the
birth's ordered digest updates. An announcement grants no authority independently
of committed leadership, completes no handover and proves no runtime exit. Its
contact fields do not refresh liveness.

RDM implements the contact-bearing frame with byte fixtures. The internal SeatHolder identifies the holder; the frame carries its
contacts explicitly. Acceptance
must prove both contact addresses belong to the committed holder, preserve digest
ordering for subsequent contact changes and reject stale-holder announcements.


## 4. Topic identity

Topic derivation uses logical Fabric/Mesh identity, not transport identity.

Conceptually:

```text
mesh membership topic = f(logical FabricId, MeshId)
backbone topic        = f(logical FabricId)
```

EndpointId/address data is used to join/dial peers; it is not the logical Fabric identity or topic identity.

---

## 5. Join and first hydration

Sequence: [node.mesh.join](lifecycles/node-mesh-join.md), then [node.start](lifecycles/node-start.md). This
section owns only what gossip is to a joiner:

- the join is a call, [JoinNode](node-rpc-envelope.md#join-op-0x1d-rdm), and gossip plays no part in
  it; the authority learns the birth from the accepted report, never from a frame;
- after the accepted join the birth joins its mesh channel, `f(FabricId, MeshId)`, and the
  node-admin backbone when it is a node-admin; a mesh with no live listener has its channel created
  again by its first listener under the mesh's own MeshId;
- topology is not in the join answer and not learned from gossip first: the joiner reads complete
  source snapshots with [node.topology.get](lifecycles/node-topology-get.md), and later deltas, fulls
  and top-ups (§3.3) keep them current;
- the accepted topology is the current Build, read last in, first out from `builds.storage`; it is
  never reconstructed from Build gossip. Gossip disseminates; it is not the persistence of a Build;
- a node-admin is not Ready because it can observe peers. Ready is authority-capable readiness, and
  an ordinary joining admin cannot self-Ready around missing hydration; only the Day-0 root and
  fabric recovery's root establish Ready without a live Ready authority
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
- retries continue only while the member remains held/current and unproven dead;
- relearning the current birth also restores its RuntimeFact + required current runtime metadata through ordinary membership convergence;
- when canonical exact-runtime/lifecycle evidence retires/tombstones the birth, it stops being a refeed target;
- zero-neighbour refeed may remain an edge fallback;
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

### 6.1 Deletions: an event for promptness, `Members` for retention

A known exact deletion is membership state and a lifecycle event, never a second gossip protocol.

The mesh executor that holds the node.delete publishes `NodeDeleted` (§3.1) on its own mesh
channel, and carries it in `Members.deleted` on the backbone, the moment it holds exact provider
proof that the runtime is terminal; peer primaries forward it. The operation's Step receipt is
written after `NodeDeleted`. The same deletion is then retained in the source Mesh's `Members.deleted`
for the deleted retention, so a node that missed the event, joined later or was re-fed after going unheard
learns it from the next aggregate, delta, full or top-up.

A deletion is recorded only from exact provider proof:
- the controlling executor has exact proof that an intentionally removed runtime is terminal.

These are **not** deletion proof:
- the node's own reply;
- a stopped status by itself;
- silence/staleness;
- failed RPC or failed dial;
- an unheard peer mesh;
- holding the Build claim.

Retention is bounded current membership state, not permanent history: at least the membership
repair horizon, expired by local age since the receiver accepted it; `event_at_rafka_ms` is evidence
and never a remote expiry decision. After retention a formerly `Gone` identity resolves `Unknown`.
`Gone` means this process currently retains authoritative deletion knowledge, not that the Fabric
keeps an eternal NodeId graveyard.

A NodeId accepted as `NodeDeleted` stays deleted for the retention: a later digest of the deleted
birth, and a later incarnation wearing that NodeId, are refused. The record keeps the
incarnation for exact proof; the refusal keys on the NodeId. A restart never emits `NodeDeleted`, so a
restarted node that was not deleted is taken along its lineage.

Every running process feeds current births, overlays and deletions from membership into its
process-scoped LiveNodeResolver. [node-rpc.md](node-rpc.md) owns the resolver contract and
Found/Gone/Unknown semantics; [node.delete](lifecycles/node-delete.md) owns the executor's workflow,
and the emission points of `NodeDeleting` and `NodeDeleted` are the §3.1 catalog rows and this section.

---

## 7. Dissemination reliability boundary

Ordinary lost pushes are repaired by iroh-gossip Plumtree mechanisms. RDM does not add:
- application ACK/resend protocol for membership facts;
- its own eager/lazy tree;
- a per-peer gossip scheduler competing with iroh-gossip.

The pinned iroh-gossip nonblocking-send fix must ensure one full peer queue cannot block the actor. A dropped full-queue enqueue is recovered through ordinary lazy-push/GRAFT behavior.

Lifecycle events and aggregates divide reliability between them:

```text
NodeDeleting / NodeDeleted events
    = prompt notification on the executor's own mesh channel

Members.in_flight
    = the same open lifecycle overlays, retained while the operation is open

Members.deleted
    = the same proven deletions, retained for the deleted retention
```

A connected node reacts to the event at once. A node that missed it, joined later or was re-fed after going unheard learns the same state from the next aggregate, delta or full it hears. `MeshStatus` and `FabricStatus` are not in the aggregate: a node that missed one gets the held status replayed on a join, heal, refeed or top-up. No event is retried by the application and no second delivery mechanism exists.

The same boundary holds for cross-Mesh topology inside a Mesh (§3.3): a delta that cannot be applied is dropped and the receiver tops up from its own mesh-primary; nothing asks for a replay over gossip.

RuntimeFact is compact bounded current-birth identity state. Do not replace it with replay of every completed `AllocateIdentity`/`DeployRuntime` Build receipt and do not inflate it with arbitrary operational metadata merely because membership carries it. Required current runtime metadata may be projected beside the fact in the current Node/birth view. Build gossip already has a bounded frame budget and completed Build history grows without bound.

---

## 8. Entity projection gossip remains projection, not source of truth

Rafka entity projection gossip may carry durable-row copies into hot in-memory projections. The durable application row remains the source of truth for application entities. Generic RDM membership/topology facts remain the control projection used by Build/leadership.

Projection kind enrolment remains explicit. Provisioning/writing an entity kind does not automatically enroll the caller as a projection consumer.

Message ordering/version rules remain owned by the entity/projection framework, not by the leadership or recovery layer.

**Only projected kinds gossip, and each goes only to the nodes that project it.** A node's
kind set is the set of entity kinds whose stores registered a projection in that process
(`crates/rafka-node-base/src/kind_scope.rs::own_kind_set`). It rides the node's own gossip
digest (`crates/rafka-node-base/src/lib.rs::GossipDigest`, `kind_set`). Each kind rides its own
iroh-gossip topic, and a node joins the topic of every kind it enrolled
(`crates/rafka-node-base/src/kind_topics.rs`); a write is
broadcast once on its kind's topic (`crates/rafka-node-base/src/entity_gossip.rs`
`fn broadcast_on_kind_topic`). So a compute node never receives a gateway-only kind
(`principal`, `service_account`, `path_config` and the rest).

**The projection frame names its org.** Every sender builds
`MeshControlMessage::EntityProjectionUpdateV2` (`crates/rafka-topology/src/mesh_control.rs`),
which carries `entity_kind`, `org_id`, `key`, `offset`, `version` and `value` (`None` is a
tombstone). The receive seam holds the row under the frame's org and refuses an empty org by
name (`crates/rafka-entity-framework/src/gossip.rs::entity_projection_gossip_in_org`,
`rafka.entity_store.projection.reject.via-org-unstamped`). `version` is the row's own ordering
(`crates/rafka-entity-framework/src/traits.rs::Entity::gossip_version`). It is `0` for a kind
ordered by offset, and compiled ACL cells carry their generation stamp.

**node-admin holds every channel raw.** node-admin publishes the union of its peers' kind sets
as its own, so every projected kind reaches it. It keeps each kind's current rows undecoded in
a `RawProjectionStore` (`crates/rafka-entity-framework/src/raw_projection.rs`): one row per
`(org, key)`, where a higher `version` wins, then a higher `offset`. Tombstones and
clear-projection apply to it like any other row.

**Entry ends with a pull.** When a gateway or compute node has registered its stores and its gossip loop is
running, it reads its kind set from node-admin with [node.topology.get](lifecycles/node-topology-get.md).
node-admin answers with the current rows of exactly those kinds: a kind node-admin holds as a store is served
from that store, every other kind raw, and the node installs the answer only once it is complete. Live frames
and pulled rows share one apply rule, so nothing gossiped during the pull is lost. Only then does the node mark
itself ready (span `rafka.mesh.entry.update.via-projection-pulled`, with `entity_rows` and `raw_applied`).

A pull that cannot be answered holds the node below `ready-for-traffic`. node-admin refuses a pull it cannot serve (it is not the snapshot authority, the snapshot will not encode, or it cannot serve the system org's own record) by name (`rafka.mesh.entry.reject.via-projection-refused`). The node names each failed attempt, refused, unreachable or cut short, on `rafka.mesh.entry.reject.via-projection-pull-failed` with its reason, backs off, re-resolves the current primary node-admin from the topology it holds on every attempt, and pulls again from that authority until an answer is applied; only then does it mark itself ready. A `not-born` answer before day 0 is a valid answer and does not hold the node. Every store reads through to the broker on a runtime miss; read-through never stands in for a refused pull. The durable topics remain the completeness backstop: the pull carries the hot working set node-admin holds, not a copy of every broker's whole set.

---

## 9. JoinNode (op `0x1D`) / bootstrap hydration

Sequence: [node.mesh.join](lifecycles/node-mesh-join.md) and [node.start](lifecycles/node-start.md) steps 3 to
5. A joining or restarted node hydrates what its role needs before it is Ready: for a node-admin,
the fabric and mesh identities, its held node-admin peers with their endpoint and address data,
current birth facts with RuntimeFacts and runtime metadata for the births it may manage, the
current Build (LIFO read) and the Build floor the join answer names, and its provider domain's
reachability. It joins its gossip topics before or while it reads, so concurrent frames converge
through the ordinary ordering (§3.1). A late node-admin needs no completed Build history to manage
current runtimes or to know the current accepted topology.

---

## 10. Status dissemination versus status certainty

[States](lifecycles/states.md#states) owns the lifecycle states, who sets them, the rounds and
their checklist; the directed calls are the certainty.

Applied means the target accepted/applied the operation. Gossip independently disseminates the resulting current MemberStatus/state. Neither plane proves, approves, attests, or confirms the other.

RPC failure is not converted into a negative gossip lifecycle state.

The workflows whose events ride these channels are listed in the [lifecycle index](fabric-mesh-node-lifecycles.md#node-workflows).

---

## 11. Leadership relationship

Gossip carries the current candidate facts consumed by leadership. It does not define election semantics.

The **single normative election specification** is [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md). This document deliberately does not repeat eligibility, ordering, hierarchy, preemption, or shutdown-seat rules.

Primary bits learned over gossip are projections/hints. Consumers that need current authority use the imported LeadershipView rather than rebuilding an election from gossip locally.

---

## 12. An unheard peer mesh

While a peer mesh goes unheard, LeadershipView may temporarily differ because current membership views differ. The semantics of that condition are owned by [fabric-node-lifecycle-elections.md](fabric-node-lifecycle-elections.md), not redefined here.

Safety comes from:
- exact birth/runtime fencing;
- definitive lifecycle/control receipts;
- idempotent Build operations;
- one fabric-primary topology writer (a cut-off view holds no fabric-primary authority);
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

Build-topic facts remain broadcast for durability and recovery. The fabric-primary
is the sole Build driver; it claims and calls the executor over the
[Build carrier](node-rpc-envelope.md#build-op-0x20-rdm). Caller progress waits on
no gossip receipt.

For lost admin authority the sequence is [mesh investigation](lifecycles/mesh-investigation.md)
for a mesh with no node-admin and [node recovery](lifecycles/node-recovery.md) for a member whose
mesh-primary is live; who claims and executes is [build.md](build.md). Gossip's part is fixed:
canonical runtime and lifecycle evidence, never silence, establishes which births are gone; the
current Build, read from `builds.storage`, establishes what should exist; a node-admin created into
an existing mesh keeps that mesh's MeshId.

Held live ordinary members remain reconciliation inputs and are preserved through their current membership + RuntimeFacts/runtime metadata.

A deletion is published only from proof. `NodeDeleted` for an exact birth is emitted by the executor that holds the operation, after the provider's exact inspection says the runtime is terminal. Holding a claim does not prove death; neither does the node's own `stopped`, a failed RPC, silence or an unheard peer mesh. A lost member is recovered by its mesh-primary ([node recovery](lifecycles/node-recovery.md)): tickle, exact provider exit proof, then one recovery Build that deletes the exited node (NodeDeleted from that proof) and creates its successor at the same path.name.

---

## 14. Required proofs

At minimum:
1. ordinary nodes never join backbone yet converge all meshes;
2. only mesh primary publishes own aggregate;
3. receiving peer primary forwards remote aggregate locally;
4. publisher movement follows canonical leadership;
5. lost pushes recover through ordinary iroh-gossip repair;
6. node-admins that cannot hear each other, each with neighbours, are re-fed through held-member stale-coverage rejoin;
7. failed rejoin never marks Dead;
8. proven-dead held member is removed from repair targets;
9. restarted/joining authority node cannot become Ready before required topology + `Fabric.build_id`/Build + RuntimeFact/runtime-metadata + provider-domain hydration;
10. normal joining admin cannot self-Ready around missing hydration; only Day-0/single-admin recovery-root behavior is permitted;
11. late lower-NodeId admin can learn/adopt current runtimes it did not launch without completed Build receipts;
12. provider/bootstrap locator production results in exactly one normalized node-published RuntimeFact, not a competing launcher-owned fact;
13. `Fabric.build_id` and its Build come from durable storage or the JoinNode (op `0x1D`), never from gossip alone;
14. older Build history is not replayed to recover RuntimeFacts or the accepted topology;
15. status certainty remains short RPC plus subsequent gossip dissemination, not gossip guesswork;
16. the members of the executor's own Mesh learn a deletion from the event and from the next aggregate, without joining the backbone;
17. a late digest of a deleted exact birth is refused, and a later incarnation of a NodeId accepted as `NodeDeleted` is refused for the deleted retention; a restarted node that was not deleted is taken along its lineage;
18. a node that joins or heals after a lifecycle event learns the open overlay from `Members.in_flight` and the deletion from `Members.deleted`;
19. a node under `NodeDeleting` is `Found` and is not selected by application routing;
20. the same event received on the own mesh channel and as a forwarded copy is applied once;
21. an aggregate carrying overlays and deletions still packs within the gossip message bound;
22. a quiet fabric carries only the heartbeat, the backbone aggregate and the periodic full: no status, lifecycle or delta frame travels without a change;
23. a status change is reinforced five times and then stops; a late joiner learns the status from a join-triggered re-send, not from a heartbeat;
24. one node changing in a remote Mesh produces a delta naming that node, not a re-broadcast of the Mesh;
25. a delta whose `base_version` the receiver does not hold desynchronizes that source's projection, and the node tops up from its own mesh-primary; nothing is requested over gossip;
26. a forwarder that takes the seat publishes a full before any delta; a new source primary is a new revision and its first delta is never applied before its full;
27. a full missing a chunk is never applied, on the backbone or inside a Mesh; the held projection and the held source version survive until a complete snapshot or a top-up, and no delta is derived from an incomplete one;
28. a node that noticed nothing wrong holds stale remote topology for at most `FULL_EVERY` rounds;
29. the forwarded cross-Mesh projection carries no loads while the backbone aggregate does;
30. two heartbeats of one birth are ordered by `digest_seq` alone: a stalled or repeated Rafka-time never makes a newer heartbeat look older;
31. a forwarder that missed an intermediate source version emits one delta from the version it last published to the one it holds;
32. a source Mesh whose nodes only heartbeat does not bump `topology_version`, and no delta travels;
33. `NodeDeleting` is never emitted before the executor's own journal holds its adopted claim for the attempt; a multi-mesh Build's second Mesh emits its `NodeDeleting` only after the fabric-primary claims the next attempt and dispatches build.attempt.run and that Mesh's executor adopts the won claim, keyed on that attempt, never on Build acceptance;
34. `NodeDeleted` is never emitted without exact provider proof; silence, a failed RPC and an unheard peer never produce it; after it, `ExactNode(old)` is `Gone`, the path has no holder until a new birth, and the current topology does not list the node;
35. unplanned loss produces one `NodeDeleted` for the old identity, after proof, from the recovery Build's node.delete.
