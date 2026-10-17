# Q41 — e5.1 outcome code page replacements for Luke

Status: proposed, not applied. Q40 B’s separate Forward reseal is unchanged. The code view adds no reply fields or wire bytes. The current catalog has ten protocols: the plan’s nine plus NodeCommand.

The existing not_open union is stated explicitly below: it provides no already-closed discriminator, so it maps to NOT_FOUND for a missing current open entry. No decoder derives a distinction from text or local history.

## node-rpc.md §25

Old:

````text
## 25. Shared reply classification, protocol-owned bytes

There is no universal success/refusal wire envelope.

Before dispatch, Node RPC has transport outcomes such as `Unserved`.

After protocol dispatch, every reply byte belongs to that protocol's codec.

Shared semantics are represented by `ReplyKind`:

```rust
pub enum ReplyKind {
    Success,
    PeerUnresolved,
    NotReady,
    Busy,
    ConnectionsDeleting,
    Malformed(MalformedKind),
    Unauthorized,
    ProtocolRefusal,
    Unclassified,
}
```

`Malformed` carries the envelope's [`MalformedKind`](node-rpc-envelope.md#runtime-spans).

The protocol supplies constructors for shared states and a classifier for decoded replies.

Node RPC uses the classification for telemetry/outcome metadata only. It never infers meaning from reply bytes or strings and never automatically retries because of a class.

Required codec tests prove every shared constructor survives:

```text
constructor -> encode -> decode -> classify
```

with the same class.

---


````

New:

````text
## 25. Shared reply classification, protocol-owned bytes

There is no universal success/refusal wire envelope. Before dispatch, Node RPC has transport outcomes such as `Unserved`; after dispatch, every reply byte belongs to that protocol's codec.

The shared classification is `rdm_core::outcome::Code`: exactly the 17 [standard gRPC codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html), numbers 0–16 and their standard names. RDM adds no code. `ReplyKind` is a view holding a `Code`. `Replied<R>` keeps it beside `R`, the protocol's original typed result or refusal, exposed by `detail()` and `value()`. The named RDM outcome and its fields live in that typed detail. The classifier is total: every decoded reply variant maps to exactly one code.

| number | code | RDM meaning |
|---:|---|---|
| 0 | `OK` | Applied, a read result, a completion; a served set edit's call code |
| 1 | `CANCELLED` | the owner cancelled the invocation (`CallerGone`, the delivery end) |
| 2 | `UNKNOWN` | a lost reply or a reset after the commit cut, except the known handler-fault reset |
| 3 | `INVALID_ARGUMENT` | malformed arguments or an unknown variant |
| 4 | `DEADLINE_EXCEEDED` | any lapse of the owner's bound; the typed outcome retains NotSent before commit and Indeterminate after commit |
| 5 | `NOT_FOUND` | SubjectUnknown, an unknown subject or id; per element, not-carried or not-open |
| 6 | `ALREADY_EXISTS` | AlreadyApplied or an existing claimed name; per element, already-carried or an explicitly declared already-closed result |
| 7 | `PERMISSION_DENIED` | reserved; never constructed by a mesh handler |
| 8 | `RESOURCE_EXHAUSTED` | TooLarge or a bounded read/admission result past its size limit |
| 9 | `FAILED_PRECONDITION` | an invalid transition, not-the-writer, NotForwardable, or a named refusal of state |
| 10 | `ABORTED` | RejectedStale or a concurrent claim/revision conflict |
| 11 | `OUT_OF_RANGE` | reserved; no current use |
| 12 | `UNIMPLEMENTED` | RequestNotHandled or an unserved op |
| 13 | `INTERNAL` | INTERNAL_RPC_FAILURE or a runtime/framing contract violation |
| 14 | `UNAVAILABLE` | NotReady, Busy, ConnectionsDeleting; NotSent except an expired owner bound or NotForwardable; a receiving birth proven exited or removed, detail receiver-gone |
| 15 | `DATA_LOSS` | Corrupt |
| 16 | `UNAUTHENTICATED` | reserved; never constructed by a mesh handler |

A per-element set answer keeps its existing lists and derives a code per element; a served set call is `OK`. A requested tag in already_carried is `ALREADY_EXISTS`; one in not_carried is `NOT_FOUND`; the other requested labels were applied (`OK`). A named connection in closed is `OK`. The current not_open list names an id with no current open entry and deliberately includes both unknown and previously closed ids: its code is `NOT_FOUND`, and the decoder invents no distinction absent from that list. An `All` closure's Deadline receipt records an owner-bound lapse and is `DEADLINE_EXCEEDED`.

The code describes the result; the typed certainty still decides whether the request reached dispatch. Cancellation adds no RpcOutcome variant or reply bytes. Forward's typed causes preserve that distinction across the carrier (Q40). Code is derived locally from the typed reply on both ends and is not added to these wire frames.

Node RPC does not infer code from explanation text and does not retry a new execution because of a code. The existing delivery obligation remains §43. Each protocol's codec cell enumerates every reply position, checks its code before and after encode/decode, and retains its byte goldens. The landing name check refuses added runtime construction of PERMISSION_DENIED or UNAUTHENTICATED, including indirect numeric construction; exemptions are codec definitions, classifiers, fixtures and assertions, by code kind and never by source directory.

---


````

## node-rpc.md §40

Old:

````text
## 40. Error truth table

| fact | local outcome | handler ran? | automatic Node RPC retry? |
|---|---|---:|---:|
| resolver `Unknown/Gone` | `NotSent` | no | no |
| caller knows pinned slot/freshness is superseded | `RejectedStale` | no | no |
| dial/open/send failure before complete-send + finish | `NotSent` | no | no |
| unserved op reset | `Unserved` | no | no |
| `PeerUnresolved` typed reply | `Reply` | no domain handler | no |
| `NotReady` typed reply | `Reply` | no domain handler when guard/refusal path | no |
| `Busy` typed reply | `Reply` | no | no |
| `ConnectionsDeleting` typed reply | `Reply` | no | no |
| `TooLarge` typed reply | `Reply` | no | no |
| `UnknownVariant` typed reply | `Reply` | no | no |
| `Corrupt` typed reply | `Reply` | no | no |
| `Unauthorized` typed reply (codec position only; a mesh receiver never sends it) | `Reply` | no | no |
| domain refusal | `Reply` | yes or no per protocol semantics | no |
| valid reply races with request write error | `Reply` wins | as reply says | no |
| committed stream dies before valid reply | `Indeterminate` | unknown | no |
| handler panic / `HandlerFault` | `Indeterminate` | yes/possibly partial | no |
| streaming EOF after Started before Terminal | `Indeterminate` | yes/unknown completion | no |

Node RPC communicates facts. State machines decide the next action.

---


````

New:

````text
## 40. Error truth table

| fact | local outcome | code | handler ran? | automatic Node RPC retry? |
|---|---|---|---:|---:|
| resolver `Unknown/Gone` | `NotSent` | `UNAVAILABLE` | no | no |
| caller knows pinned slot/freshness is superseded | `RejectedStale` | `ABORTED` | no | no |
| dial/open/send failure before complete-send + finish | `NotSent` | `UNAVAILABLE` | no | no |
| owner's bound lapses before commit, including a permit wait | `NotSent` | `DEADLINE_EXCEEDED` | no | no |
| owner's bound lapses after commit | `Indeterminate` | `DEADLINE_EXCEEDED` | unknown | no |
| receiving birth proven exited or removed | `NotSent` before commit; `Indeterminate` after commit, detail receiver-gone | `UNAVAILABLE` | no before commit; unknown after | no |
| owner cancels the invocation | `CallerGone` delivery end | `CANCELLED` | unknown after commit | no |
| unserved op reset | `Unserved` | `UNIMPLEMENTED` | no | no |
| `PeerUnresolved` typed reply | `Reply` | `NOT_FOUND` | no domain handler | no |
| `NotReady` typed reply | `Reply` | `UNAVAILABLE` | no domain handler when guard/refusal path | no |
| `Busy` typed reply | `Reply` | `UNAVAILABLE` | no | no |
| `ConnectionsDeleting` typed reply | `Reply` | `UNAVAILABLE` | no | no |
| `TooLarge` typed reply | `Reply` | `RESOURCE_EXHAUSTED` | no | no |
| `UnknownVariant` typed reply | `Reply` | `INVALID_ARGUMENT` | no | no |
| `Corrupt` typed reply | `Reply` | `DATA_LOSS` | no | no |
| `Unauthorized` typed reply (codec position only; a mesh receiver never sends it) | `Reply` | `PERMISSION_DENIED` (reserved) | no | no |
| a named domain refusal | `Reply` | the exact typed result's code in §25 | yes or no per protocol semantics | no |
| valid reply races with request write error | `Reply` wins | the typed reply's code | as reply says | no |
| committed stream dies before valid reply, or a non-fault reset after commit | `Indeterminate` | `UNKNOWN` | unknown | no |
| handler panic / `HandlerFault` | `Indeterminate` | `INTERNAL` | yes/possibly partial | no |
| corrupt reply bytes | `Indeterminate` | `DATA_LOSS` | unknown | no |
| unknown reply variant | `Indeterminate` | `INVALID_ARGUMENT` | unknown | no |
| streaming EOF after Started before Terminal | `Indeterminate` | `INTERNAL` for the ordering violation | yes/unknown completion | no |

Node RPC communicates facts. State machines decide the next action. This table's retry column does not cancel §43's delivery of the same owed invocation; a standard code authorizes no new execution.

---


````

## node-rpc.md §41 — remove the deleted catch-all enum label from the adjacent descriptive table

Old:

````text
| `ProtocolRefusal` | protocol-defined | protocol-defined |
````

New:

````text
| named typed refusal (detail) | protocol-defined | protocol-defined |
````

## node-rpc-envelope.md — typed-object table

Old:

````text
| Typed caller | Existing wire / execution component | Caller result / events |
|---|---|---|
| [node.create](lifecycles/node-create.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; first join on [JoinNode 0x1D](#join-op-0x1d-rdm); a held name answers NameHeld { by }; the create of a mesh's first node-admin is the mesh-absent arm, [mesh.create](lifecycles/mesh-create.md), with no call of its own | every FirstStart event ending node.started, then node.created |
| [node.start](lifecycles/node-start.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Join 0x1D](#join-op-0x1d-rdm), [topology 0x1E](#topology-op-0x1e-rdm), [Build facts 0x1F](#build-facts-op-0x1f-rdm), [Status 0x1B](#status-op-0x1b-rdm) | node.joined, node.started; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic |
| [node.stop](lifecycles/node-stop.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Status 0x1B StopNode](#node-stop-command-pair); stopped rides the StopNode reply; the stop's first step is `node.connections.delete { scope: All }` | node.connections.deleted, node.stopped; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic |
| [node.restart](lifecycles/node-restart.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; the stop's and the start's ops; no restart opcode | every stop/start event, node.restarted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic |
| [node.delete](lifecycles/node-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; the stop's ops, then provider inspection, which is not a Node RPC | every stop event, then node.deleted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic |
| [node.mesh.join](lifecycles/node-mesh-join.md) | The report is [JoinNode 0x1D](#join-op-0x1d-rdm); standalone, [Status 0x1B](#status-op-0x1b-rdm) `JoinMesh`, answered `MeshJoined { joined }`; inside node.start a step of the start's stream | node.mesh.joined; a refused report fails the primitive by the report's refusal |
| [node.mesh.leave](lifecycles/node-mesh-leave.md) | Standalone, [Status 0x1B](#status-op-0x1b-rdm) `LeaveMesh`, answered `MeshLeft { left }`; node.stop and node.delete end their live part with it, and inside a stop it is the stop's own step and this frame is not sent | node.mesh.left; `BusyHoldingConnections { connections }` while the node holds a connection |
| [node.tickle](lifecycles/node-tickle.md) | [Status 0x1B ProbeNodeState](#status-op-0x1b-rdm) direct, and the same probe through up to two live peers of the node's mesh on [Forward 0x1A](#forward-op-0x1a-core) | `reached { answer }` or `unreached`; no event, and unreached is never death |
| [node.runtime.inspect](lifecycles/node-runtime-inspect.md) | The deployment provider's inspect of the runtime handle recorded at launch; not a Node RPC | `running`, `exit-proven { code }` (recorded as the birth's exit proof) or `unavailable { reason }`; no event |
| node.get | [Status 0x1B ProbeNodeState](#status-op-0x1b-rdm) for one exact subject; node.get answers one node by id, the nodes of a mesh, or the nodes that carry given tag labels, all of the given labels or any of them; a node's tags are its own property (node.tags.get/create/delete), and the tag filter names a label, matches every node that carries it, and is read through the node store's tags index | Current; stopped answers CurrentParked; no event |
| [node.topology.get](lifecycles/node-topology-get.md) | [GetTopology 0x1E](#topology-op-0x1e-rdm); complete source snapshot read | complete read result; no event |
| [node.name.update](lifecycles/node-name-update.md) | Rename, to the node's mesh-primary; frames `UpdateName` and `ApplyNodeName` on [0x1B](#status-op-0x1b-rdm); the node admits no connection | node.name.updated; `NameHeld { by }`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`; `fabric-not-ready` |
| node.connections.get/[delete](lifecycles/node-connections-delete.md) | Held-connection read; `node.connections.delete { scope: All }` or `{ scope: Connections([connection id, …]) }`. `All` is graceful closure of every open connection of the node, each open Direct or Proxy entry of its connections log (gossip neighbours and the Node RPC pool are not connections), with status connections-deleting then connections-deleted and the [order and deadline](lifecycles/node-connections-delete.md#numbered-end-to-end-sequence) of that page; `Connections` closes exactly the named connections, by the minted id of their open entry as node.connections.get returns them; an empty list is refused by name. The endpoint and an active stop stream stay open; each drop is written under the node's current name; the caller's carrier to the node's mesh-primary is [Status 0x1B](#status-op-0x1b-rdm): StatusRequest position 21 `DeleteConnections`, answered by StatusReply position 31 `ConnectionsDeleted`, or by position 32 `RejectedConnectionScope` for a scope that names nothing the node can cut; the mesh-primary carries it to the exact node on NodeCommand 0x22 | read result / node.connections.deleted, naming every id it closed and every id it did not know or found already closed; for `All`, carrying a `ConnectionsReceipt`: `Established { in_flight, closed }` or `Deadline { last_in_flight, closed, held }` |
| node.tags.get/create/delete | A node's tags are its own property, not config. `node.tags.get` answers the node's labels; `node.tags.create { label \| [labels] }` adds one label or a list; `node.tags.delete { All \| [labels] }` removes every label or the named ones, the same scope shape as `node.connections.delete`. One item or a list, uniform: a single item is the same as a list of one. Each label is its own keyed row (label, node id) in the node store's tags index. Create is a blind put of that key, so creating a label the node already carries is ignored and writes no duplicate; delete tombstones the named keys. A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading '.', and a node carries at most 32; a label outside that grammar, or one past the 32, is refused by name and nothing is written; frames `CreateTags { node_id, incarnation, labels }` and `DeleteTags { node_id, incarnation, scope: All | Labels }` on 0x1B, replies `TagsCreated { node_id, incarnation, already_carried }` and `TagsDeleted { node_id, incarnation, not_carried }` | read result / create: the reply names each label the node already carried, AlreadyApplied for those |
| node.config.get/update | Application config read / full-map put; the [config update sequence](lifecycles/node-config-update.md); dedicated request mapping is open | read result / node.config.updated |
| [node.cert.renew](lifecycles/node-cert-renew.md) | The issuing authority to the exact birth: [Status 0x1B](#status-op-0x1b-rdm) `RenewCert { node_id, incarnation, cert }`, answered `CertRenewed`; the node installs the cert and presents it on every connection it opens after; nothing is terminated | node.cert.renewed; a named refusal for another birth or an undecodable cert |
| [node.issuer.update](lifecycles/node-issuer-update.md) | The fabric-primary to one node-admin, exact birth: [Status 0x1B](#status-op-0x1b-rdm) `UpdateIssuer { node_id, incarnation, key, cert }`, answered `IssuerUpdated`; refused by name for another mesh or another birth | node.issuer.updated |
| [node.roots.update](lifecycles/fabric-ca-renew.md#noderootsupdate-the-primitive) | The fabric-primary to one node, exact birth: [Status 0x1B](#status-op-0x1b-rdm) `UpdateRoots { node_id, incarnation, roots, phase }`, answered `RootsUpdated`; a set that drops a root the node still admits a chain under is refused `root-still-chained` | node.roots.updated |
| node.cert.get | A field of node.get: `NodeView.cert { issued_at, expires_at, issuer_expires_at }` on the `Nodes` reply, from the birth's cert row ([fabric-certs.md](fabric-certs.md#tracking)) | read result; no event |
| [node.connections.admit](lifecycles/node-connections-admit.md) | The acceptor's verification of the dialer's member cert on the first bi-stream of an inbound connection of a registered product ALPN; RDM framing, not a Node RPC op | `admitted` or `refused { reason }`, a connection fact ([connections.md](connections.md#12-connection-state)) |
| mesh.get, mesh.update, mesh.nodes.get, mesh.config.get/update | mesh.get answers one mesh, or every mesh of a fabric; mesh.update puts the full mesh meta as a blind put and never changes status; mesh.nodes.get answers the mesh's members; mesh.config.get/update hold the application's attributes on the mesh row, as node.config does on a node; a mesh's tags are its own property, mesh.tags.get / mesh.tags.create { label | [labels] } / mesh.tags.delete { All | [labels] }, the same shape and label grammar as node.tags; dedicated request mapping is open | read result / mesh.updated / mesh.config.updated |
| [fabric.root.update](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op) | A single call to the fabric-primary with a new root secret, the trust anchor; starts the root rotation's phases; refused `rotation-in-progress` or `root-already-trusted` by name; `fabric.get` carries `roots { phase, expires_at }` and `ca { expires_at }` | fabric.root.updated |
| fabric.get, fabric.update, fabric.meshes.get, fabric.nodes.get, fabric.config.get/update | fabric.get answers from memory; fabric.update puts the full fabric meta as a blind put and never changes status; fabric.meshes.get lists the meshes; fabric.nodes.get answers the fabric topology; fabric.config.get/update hold the application's attributes on the fabric row; dedicated request mapping is open | read result / fabric.updated / fabric.config.updated |
| [mesh.delete](lifecycles/mesh-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [0x21](#primaryupdate-op-0x21-rdm) (node.fabric-primary.update) when the mesh holds the fabric-primary seat; [Status 0x1B DeleteMesh / MeshDeleted](#mesh-delete-command-pair); node.delete's ops per member | mesh.deleting, mesh.deleted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic |
| [fabric.create](lifecycles/fabric-create.md) | The first node-admin, started with no mesh and no durable topology, is the executor and the fabric-primary; the accepted topology rides [build.create 0x20](#build-op-0x20-rdm) | fabric.created when the fabric is ready-for-traffic |
| [fabric.delete](lifecycles/fabric-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow, one Build the fabric-primary drives: every mesh removed leaf-inward, its own mesh last | fabric.deleted from the fabric-primary, still alive; its own node.delete is a separate call from outside the fabric |
| [node.primary.update](lifecycles/node-primary-update.md) | [0x21 PrimaryUpdate / PrimaryUpdated](#primaryupdate-op-0x21-rdm), the holder to its successor's exact birth, carrying the committed seat row | node.primary.updated { revisions }; RejectedConflictingPayload; `fabric-not-ready`; on the commanded arm, `Failed { step, reason }` when the update ends failed |
| [node.fabric-primary.update](lifecycles/node-primary-update.md#fabric-seat) | the same call, the fabric seat as the `fabric` entry of `seats` | node.fabric-primary.updated; the fabric arm's no-successor refusal |
| build.create/get/delete | [Build 0x20](#build-op-0x20-rdm); the fabric-primary accepts every Build and drives topology Builds; a mesh-primary drives drift attempts of the current Build inside its mesh. [ClaimAttempt 0x1C](#buildclaim-op-0x1c-rdm) is the attempt claim; [FetchBuildFacts 0x1F](#build-facts-op-0x1f-rdm) is the facts read | Started, step frames, Complete / Failed; reattachment reads; deletion result |
| build.attempt.run | [Build 0x20](#build-op-0x20-rdm); fabric-primary calls the named executor with the won claim | receipt replay, current Blocked, then live frames |
````

New:

````text
Every row also retains §25 and §40's delivery code when it does not produce a typed result: an owner-bound lapse is DEADLINE_EXCEEDED (certainty stays NotSent or Indeterminate), receiver-gone is UNAVAILABLE, and a lost reply is UNKNOWN. A row can span several calls; each reply frame has its own code and typed detail. A provider inspection or a connection admission remains its own typed result, with no new Node RPC wire code.

| Typed caller | Existing wire / execution component | Caller result / events | code |
|---|---|---|---|
| [node.create](lifecycles/node-create.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; first join on [JoinNode 0x1D](#join-op-0x1d-rdm); a held name answers NameHeld { by }; the create of a mesh's first node-admin is the mesh-absent arm, [mesh.create](lifecycles/mesh-create.md), with no call of its own | every FirstStart event ending node.started, then node.created | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.start](lifecycles/node-start.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Join 0x1D](#join-op-0x1d-rdm), [topology 0x1E](#topology-op-0x1e-rdm), [Build facts 0x1F](#build-facts-op-0x1f-rdm), [Status 0x1B](#status-op-0x1b-rdm) | node.joined, node.started; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.stop](lifecycles/node-stop.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Status 0x1B StopNode](#node-stop-command-pair); stopped rides the StopNode reply; the stop's first step is `node.connections.delete { scope: All }` | node.connections.deleted, node.stopped; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.restart](lifecycles/node-restart.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; the stop's and the start's ops; no restart opcode | every stop/start event, node.restarted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.delete](lifecycles/node-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; the stop's ops, then provider inspection, which is not a Node RPC | every stop event, then node.deleted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.mesh.join](lifecycles/node-mesh-join.md) | The report is [JoinNode 0x1D](#join-op-0x1d-rdm); standalone, [Status 0x1B](#status-op-0x1b-rdm) `JoinMesh`, answered `MeshJoined { joined }`; inside node.start a step of the start's stream | node.mesh.joined; a refused report fails the primitive by the report's refusal | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.mesh.leave](lifecycles/node-mesh-leave.md) | Standalone, [Status 0x1B](#status-op-0x1b-rdm) `LeaveMesh`, answered `MeshLeft { left }`; node.stop and node.delete end their live part with it, and inside a stop it is the stop's own step and this frame is not sent | node.mesh.left; `BusyHoldingConnections { connections }` while the node holds a connection | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.tickle](lifecycles/node-tickle.md) | [Status 0x1B ProbeNodeState](#status-op-0x1b-rdm) direct, and the same probe through up to two live peers of the node's mesh on [Forward 0x1A](#forward-op-0x1a-core) | `reached { answer }` or `unreached`; no event, and unreached is never death | `OK` for a served probe; unreached retains its typed delivery code, never exit proof |
| [node.runtime.inspect](lifecycles/node-runtime-inspect.md) | The deployment provider's inspect of the runtime handle recorded at launch; not a Node RPC | `running`, `exit-proven { code }` (recorded as the birth's exit proof) or `unavailable { reason }`; no event | — (typed provider result; no Node RPC) |
| node.get | [Status 0x1B ProbeNodeState](#status-op-0x1b-rdm) for one exact subject; node.get answers one node by id, the nodes of a mesh, or the nodes that carry given tag labels, all of the given labels or any of them; a node's tags are its own property (node.tags.get/create/delete), and the tag filter names a label, matches every node that carries it, and is read through the node store's tags index | Current; stopped answers CurrentParked; no event | `OK`; unknown subject `NOT_FOUND`, read bound `RESOURCE_EXHAUSTED` |
| [node.topology.get](lifecycles/node-topology-get.md) | [GetTopology 0x1E](#topology-op-0x1e-rdm); complete source snapshot read | complete read result; no event | `OK`; unknown subject `NOT_FOUND`, read bound `RESOURCE_EXHAUSTED` |
| [node.name.update](lifecycles/node-name-update.md) | Rename, to the node's mesh-primary; frames `UpdateName` and `ApplyNodeName` on [0x1B](#status-op-0x1b-rdm); the node admits no connection | node.name.updated; `NameHeld { by }`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`; `fabric-not-ready` | `OK`; NameHeld `ALREADY_EXISTS`, stale `ABORTED`, invalid name `INVALID_ARGUMENT`, other state refusal `FAILED_PRECONDITION` |
| node.connections.get/[delete](lifecycles/node-connections-delete.md) | Held-connection read; `node.connections.delete { scope: All }` or `{ scope: Connections([connection id, …]) }`. `All` is graceful closure of every open connection of the node, each open Direct or Proxy entry of its connections log (gossip neighbours and the Node RPC pool are not connections), with status connections-deleting then connections-deleted and the [order and deadline](lifecycles/node-connections-delete.md#numbered-end-to-end-sequence) of that page; `Connections` closes exactly the named connections, by the minted id of their open entry as node.connections.get returns them; an empty list is refused by name. The endpoint and an active stop stream stay open; each drop is written under the node's current name; the caller's carrier to the node's mesh-primary is [Status 0x1B](#status-op-0x1b-rdm): StatusRequest position 21 `DeleteConnections`, answered by StatusReply position 31 `ConnectionsDeleted`, or by position 32 `RejectedConnectionScope` for a scope that names nothing the node can cut; the mesh-primary carries it to the exact node on NodeCommand 0x22 | read result / node.connections.deleted, naming every id it closed and every id it did not know or found already closed; for `All`, carrying a `ConnectionsReceipt`: `Established { in_flight, closed }` or `Deadline { last_in_flight, closed, held }` | call `OK` when served; closed `OK`, not_open `NOT_FOUND`; `All` Deadline `DEADLINE_EXCEEDED` |
| node.tags.get/create/delete | A node's tags are its own property, not config. `node.tags.get` answers the node's labels; `node.tags.create { label \\| [labels] }` adds one label or a list; `node.tags.delete { All \\| [labels] }` removes every label or the named ones, the same scope shape as `node.connections.delete`. One item or a list, uniform: a single item is the same as a list of one. Each label is its own keyed row (label, node id) in the node store's tags index. Create is a blind put of that key, so creating a label the node already carries is ignored and writes no duplicate; delete tombstones the named keys. A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading '.', and a node carries at most 32; a label outside that grammar, or one past the 32, is refused by name and nothing is written; frames `CreateTags { node_id, incarnation, labels }` and `DeleteTags { node_id, incarnation, scope: All \| Labels }` on 0x1B, replies `TagsCreated { node_id, incarnation, already_carried }` and `TagsDeleted { node_id, incarnation, not_carried }` | read result / create: the reply names each label the node already carried, AlreadyApplied for those | call `OK` when served; already_carried `ALREADY_EXISTS`, not_carried `NOT_FOUND`, applied label `OK`; invalid label `INVALID_ARGUMENT` |
| node.config.get/update | Application config read / full-map put; the [config update sequence](lifecycles/node-config-update.md); dedicated request mapping is open | read result / node.config.updated | `OK`; AlreadyApplied `ALREADY_EXISTS`, malformed config `INVALID_ARGUMENT`, state/row refusal `FAILED_PRECONDITION` |
| [node.cert.renew](lifecycles/node-cert-renew.md) | The issuing authority to the exact birth: [Status 0x1B](#status-op-0x1b-rdm) `RenewCert { node_id, incarnation, cert }`, answered `CertRenewed`; the node installs the cert and presents it on every connection it opens after; nothing is terminated | node.cert.renewed; a named refusal for another birth or an undecodable cert | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.issuer.update](lifecycles/node-issuer-update.md) | The fabric-primary to one node-admin, exact birth: [Status 0x1B](#status-op-0x1b-rdm) `UpdateIssuer { node_id, incarnation, key, cert }`, answered `IssuerUpdated`; refused by name for another mesh or another birth | node.issuer.updated | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.roots.update](lifecycles/fabric-ca-renew.md#noderootsupdate-the-primitive) | The fabric-primary to one node, exact birth: [Status 0x1B](#status-op-0x1b-rdm) `UpdateRoots { node_id, incarnation, roots, phase }`, answered `RootsUpdated`; a set that drops a root the node still admits a chain under is refused `root-still-chained` | node.roots.updated | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| node.cert.get | A field of node.get: `NodeView.cert { issued_at, expires_at, issuer_expires_at }` on the `Nodes` reply, from the birth's cert row ([fabric-certs.md](fabric-certs.md#tracking)) | read result; no event | `OK`; unknown subject `NOT_FOUND`, read bound `RESOURCE_EXHAUSTED` |
| [node.connections.admit](lifecycles/node-connections-admit.md) | The acceptor's verification of the dialer's member cert on the first bi-stream of an inbound connection of a registered product ALPN; RDM framing, not a Node RPC op | `admitted` or `refused { reason }`, a connection fact ([connections.md](connections.md#12-connection-state)) | — (typed connection fact; no Node RPC) |
| mesh.get, mesh.update, mesh.nodes.get, mesh.config.get/update | mesh.get answers one mesh, or every mesh of a fabric; mesh.update puts the full mesh meta as a blind put and never changes status; mesh.nodes.get answers the mesh's members; mesh.config.get/update hold the application's attributes on the mesh row, as node.config does on a node; a mesh's tags are its own property, mesh.tags.get / mesh.tags.create { label \| [labels] } / mesh.tags.delete { All \| [labels] }, the same shape and label grammar as node.tags; dedicated request mapping is open | read result / mesh.updated / mesh.config.updated | `OK`; AlreadyApplied `ALREADY_EXISTS`, malformed config `INVALID_ARGUMENT`, state/row refusal `FAILED_PRECONDITION` |
| [fabric.root.update](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op) | A single call to the fabric-primary with a new root secret, the trust anchor; starts the root rotation's phases; refused `rotation-in-progress` or `root-already-trusted` by name; `fabric.get` carries `roots { phase, expires_at }` and `ca { expires_at }` | fabric.root.updated | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| fabric.get, fabric.update, fabric.meshes.get, fabric.nodes.get, fabric.config.get/update | fabric.get answers from memory; fabric.update puts the full fabric meta as a blind put and never changes status; fabric.meshes.get lists the meshes; fabric.nodes.get answers the fabric topology; fabric.config.get/update hold the application's attributes on the fabric row; dedicated request mapping is open | read result / fabric.updated / fabric.config.updated | `OK`; AlreadyApplied `ALREADY_EXISTS`, malformed config `INVALID_ARGUMENT`, state/row refusal `FAILED_PRECONDITION` |
| [mesh.delete](lifecycles/mesh-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [0x21](#primaryupdate-op-0x21-rdm) (node.fabric-primary.update) when the mesh holds the fabric-primary seat; [Status 0x1B DeleteMesh / MeshDeleted](#mesh-delete-command-pair); node.delete's ops per member | mesh.deleting, mesh.deleted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [fabric.create](lifecycles/fabric-create.md) | The first node-admin, started with no mesh and no durable topology, is the executor and the fabric-primary; the accepted topology rides [build.create 0x20](#build-op-0x20-rdm) | fabric.created when the fabric is ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [fabric.delete](lifecycles/fabric-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow, one Build the fabric-primary drives: every mesh removed leaf-inward, its own mesh last | fabric.deleted from the fabric-primary, still alive; its own node.delete is a separate call from outside the fabric | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.primary.update](lifecycles/node-primary-update.md) | [0x21 PrimaryUpdate / PrimaryUpdated](#primaryupdate-op-0x21-rdm), the holder to its successor's exact birth, carrying the committed seat row | node.primary.updated { revisions }; RejectedConflictingPayload; `fabric-not-ready`; on the commanded arm, `Failed { step, reason }` when the update ends failed | `OK`; stale/conflict `ABORTED`, absent record `NOT_FOUND`, other state refusal `FAILED_PRECONDITION` |
| [node.fabric-primary.update](lifecycles/node-primary-update.md#fabric-seat) | the same call, the fabric seat as the `fabric` entry of `seats` | node.fabric-primary.updated; the fabric arm's no-successor refusal | `OK`; stale/conflict `ABORTED`, absent record `NOT_FOUND`, other state refusal `FAILED_PRECONDITION` |
| build.create/get/delete | [Build 0x20](#build-op-0x20-rdm); the fabric-primary accepts every Build and drives topology Builds; a mesh-primary drives drift attempts of the current Build inside its mesh. [ClaimAttempt 0x1C](#buildclaim-op-0x1c-rdm) is the attempt claim; [FetchBuildFacts 0x1F](#build-facts-op-0x1f-rdm) is the facts read | Started, step frames, Complete / Failed; reattachment reads; deletion result | `OK` for read/result/completion; AlreadyApplied `ALREADY_EXISTS`, unknown Build `NOT_FOUND`, stale/conflict `ABORTED`, writer/state refusal `FAILED_PRECONDITION`, BuildInProgress `UNAVAILABLE` |
| build.attempt.run | [Build 0x20](#build-op-0x20-rdm); fabric-primary calls the named executor with the won claim | receipt replay, current Blocked, then live frames | `OK` for read/result/completion; AlreadyApplied `ALREADY_EXISTS`, unknown Build `NOT_FOUND`, stale/conflict `ABORTED`, writer/state refusal `FAILED_PRECONDITION`, BuildInProgress `UNAVAILABLE` |
````

