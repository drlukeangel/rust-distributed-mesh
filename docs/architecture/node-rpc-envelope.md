# Node RPC envelope: the target, the context, the typed body

[Lifecycle index](fabric-mesh-node-lifecycles.md) · [Node RPC runtime](node-rpc.md)

This page owns the Node RPC wire: the op ledger, the target, the context, every op's request/reply
schema and positional order, the reserved stream codes, the typed object → wire mapping, the
Build carrier `0x20` schema, the RPC runtime span names, and caller execution and certainty. It is
the sole op registry and schema owner; other documents link to the ledger or a schema here
and describe usage and policy.
Transport connections are keyed by `endpoint_id`; `fabric_id` identifies the logical Fabric.

An invocation begins on the QUIC bi-stream that carries its request frame. Every frame, in either
direction, begins with its transport header
([Transport frames](#transport-frames-delivery-below-the-protocol)), then the protocol's sections.
Every section, the header included, is length-prefixed with an unsigned LEB128 varint (at most 10
bytes) and encoded with postcard. A stream's FIN ends that stream, never an invocation: an
invocation ends with its terminal response frame, delivered by `invocation_id` on any stream from
the responding birth.

```text
request frame:  header_len | Request  | target_len | target | context_len | context | request_len | body
response frame: header_len | Response | reply_len | reply      (a unary reply is one, at seq 0)
ack frame:      header_len | Ack
```

## Transport frames: delivery below the protocol

Every frame on a Node RPC stream, in either direction, begins with a transport header the runtime
consumes before any protocol bytes are parsed. Three shapes, tagged by kind, each field held once:

```text
Request  { sender: Birth, delivery_seq: u64, forget_below: u64 }                             then sections 0-2
Response { sender: Birth, delivery_seq: u64, forget_below: u64, invocation_id: DeliveryId, seq: u32 } then the protocol's reply bytes
Ack      { sender: Birth, delivery_seq: u64 }                                                nothing follows
```

`Birth` is the sending node's exact `(node.id, incarnation)`; a `DeliveryId` is `(sender, delivery_seq)`.

A frame's delivery identity is `(sender, delivery_seq)`, minted by its sender, the same on every resend
([node-rpc.md §43](node-rpc.md#43-owed-messages-delivery-is-the-runtimes)): `delivery_seq` is a
per-receiver sequence the sending birth never reuses. Every frame carries the sender's
`forget_below`, the lowest `delivery_seq` it still holds unacked toward that receiver and a promise
never to resend anything below it; the receiver forgets duplicate state only below that watermark or
on the sender birth's re-creation or accepted removal, never by time elapsed. Until then a resend keeps
its identity and cannot dispatch again. A carried call keeps its origin's identity: the duplicate
window is per origin birth, never per carrier. An Owed request's carried attempt is a Forward of its own
toward the carrier, with a new identity each attempt, and names the request's identity in `owed`, which
the inner call keeps; a carrier that answered one attempt carries the next. A request's
`invocation_id` is its own delivery identity, derived and never stored a second time; every response
frame carries that `invocation_id` as its correlation and its `seq` as its position in the
invocation's response (`0` for a unary reply; `Started` is `0`, each `Data` the next, `Terminal`
the last). A missing or malformed header resets `424 PROTOCOL_VIOLATION`, like any section.

`Ack` is the runtime's control frame: it names the frame it acknowledges and nothing else; it travels on
the acknowledged stream's reverse direction or on an Ack stream of the current connection, and
correlates by delivery identity, independent of any stream, is never
classified by a protocol and is never itself owed. The responder acks request frames; the caller
acks response frames. Any response frame for an invocation acks the request implicitly, valid only
when the response was produced after the request's recoverable-acceptance boundary; the explicit
`Ack` is for acceptance that precedes the first response frame. A named refusal is a response frame
and so acks the request by itself; no acceptance write is invented for it.

Responses are routed by `invocation_id`, not by stream. The caller keeps its open invocations keyed
by `invocation_id`; a response frame arriving on any stream from that birth is delivered to its
invocation and ordered by `seq`. After a reconnect the responder resends its unacked response frames,
same ids and `seq`, on a new stream. The caller acknowledges a response frame only once its owner can
recover the accepted progress or result; a frame held only in a reorder buffer is not acknowledged.
Reordering is bounded in positions and in bytes (the op's reply ceiling per frame); two different frames
at the same `(invocation_id, seq)` are a `424 PROTOCOL_VIOLATION` whatever their delivery identities; `seq`
never wraps; a `Terminal` completes nothing while an earlier position is missing. A response is
acked and dropped only when the owner has completed, explicitly cancelled or superseded the
invocation, or the exact receiving birth has ended; absence from an in-memory invocation map alone
establishes none of that.

Response and `Ack` frames reach the delivery demultiplexer before handler dispatch and before lifecycle
admission: an endpoint that admits no new command still receives an owed result.

A duplicate request is suppressed. Before recoverable acceptance it is pending and unacked; after
it, the responder acks it and closes that stream. That `Ack` then EOF is delivery only: the caller's
invocation stays open and the original's response frames reach it by `invocation_id`. It is never a
result and never a failed command. The protocol-visible grammar
([node-rpc.md §11](node-rpc.md#11-unary-and-streaming-response-order)) is the sequence of an
invocation's response frames in `seq` order after this layer; it is unchanged by it.

## Section 0: the target

```text
Target { node_id: String, op: u8 }              at most 256 bytes encoded
```

The receiver reads the target first and alone, and decides on it before the context, the length
or the body exist to it:

| check | answer |
|---|---|
| `op` is not an op this process serves | the stream is reset `421 UNSERVED_OP`; nothing is dispatched |
| the target does not decode or is over its bound | the stream is reset `424 PROTOCOL_VIOLATION` |

One process has one endpoint, so the socket a request arrives on names nothing: the target names the node the call is for, and no receiver refuses a call for the node id it names (R-UC1a).
The target says nothing about the process birth. Which birth a caller is talking to is the
resolver's and the pool's knowledge (the incarnation, held locally, never on the wire); a dial to
a superseded birth is invalidated before dispatch. A restarted node keeps its id; a replacement
is a new id.

## Section 1: the context

```text
CallContext {
    caller_system: Option<String>,   at most 32 bytes
    traceparent:   Option<String>,   W3C
    tracestate:    Option<String>,   at most 512 bytes
    baggage:       Option<String>,   at most 8 KiB; valid value propagated whole
}                                    at most 12 KiB encoded
```

The declared context length is checked against the 12 KiB bound before the context is
allocated or read, as the target's and the body's lengths are against theirs; a context over the
bound resets `424 PROTOCOL_VIOLATION`. The context is observability only. A part over its own
bound or malformed is dropped by name
(`caller_system`, `traceparent`, `tracestate`, `baggage`; an invalid `traceparent` takes
`tracestate` with it) and the call continues. Nothing in it affects dispatch, authorization,
routing or execution, and no delivery identity is in it: that is the transport header's
([Transport frames](#transport-frames-delivery-below-the-protocol)). A context that does not decode
at all resets `424 PROTOCOL_VIOLATION`.

Valid baggage propagates whole; only `test_case`, `scenario` and `operation` are projected
to generic span attributes. The allowlist does not remove other keys from the carried value.

A `caller_system` matches `[a-z0-9-]+`, and a part that is dropped is never truncated
(RDM's `CallContext::sanitized` in `rdm-core`).

## Section 2: the body

The body is the protocol's own typed request, a postcard-encoded enum. Each protocol declares:

```text
NodeProtocol {
    OP: u8                        the op the target carries
    NAME
    MAX_REQUEST_FRAME_BYTES       the request ceiling, checked on the declared length
    MAX_REPLY_FRAME_BYTES
    FORWARDABLE: bool             carried execution through a peer, opt-in
    REQUEST_VARIANTS, REPLY_VARIANTS
    type Request, type Reply      postcard enums
}
```

A served op whose declared length is over its ceiling is refused typed `Malformed(TooLarge)`
before the body is allocated or read. A body that does not decode as the protocol's request
enum is refused typed `Malformed`. Under postcard a field is never appended to an existing
enum variant: new data is a new variant, and a build that does not know a variant refuses it by
name.

Evolution rules: a wire-incompatible change to an established family receives a new op. Within a
family, operation enums are append-only, and enum discriminants are never reordered or reused.

Replies are the protocol's own typed reply enum. Every protocol supplies typed refusals the
runtime can answer on its behalf: `busy`, `connections-deleting`, `malformed`, `not_ready`,
`peer_unresolved`, `unauthorized`. There is no universal success or error envelope around a
body; a refusal is a variant of the protocol's reply.

A connections-deleting node refuses new non-status calls with the protocol's `connections-deleting` refusal before
the body is read. The status family's connections-deleting-admission invariant is defined below.

## Reserved stream codes

| code | name | meaning |
|---|---|---|
| 421 | `UNSERVED_OP` | the target's op is not served here |
| 422 | `REQUEST_STOP` | the receiver asks the sender to stop a stream |
| 423 | `INTERNAL_RPC_FAILURE` | the handler faulted after dispatch |
| 424 | `PROTOCOL_VIOLATION` | a section does not decode or is over its bound |
| 499 | `FRAME_NOT_SENT` | the caller reset an unfinished request; no partial frame is dispatched |

The reset codes `413`, `501` and `503` are reserved forever for the compatibility adapters and
are never reassigned.

## The op ledger

The ledger is the registry of op codes: one row per op, naming its family, its owner and
whether it is live or retired. An op is a family: its request enum's variants are the calls. A
retired op is reserved forever and never reissued.

The ledger is consulted in exactly one place: the build, once per process at boot. The core rows
are built into the contract; a product adds its rows through the server builder. The build takes
the ledger and the entries the process serves and makes the built catalog, refusing the
process on any of:

- an op of zero;
- a duplicate ledger row;
- a row in the testkit range `0x70` to `0x7F` whose owner is not testkit, or a testkit owner
  outside that range;
- a served entry with no ledger row;
- a served entry whose row is retired;
- an owner that differs between the row and the entry;
- a core entry marked transitional;
- a forwardable server-streaming entry;
- a zero request or reply ceiling.

After the build the ledger is not read again. The target consults the built catalog's served
entries: the op is looked up for its request ceiling, and an op with no entry is `421`. The
ledger decides what a process may serve, the catalog decides what it does serve, and the wire
sees only the catalog.

| op | family | owner | ledger state | serving | calls |
|---|---|---|---|---|---|
| `0x01` | ping | core | live | canonical | `Ping` |
| `0x11` | echo | core | retired | | |
| `0x1A` | forward | core | live | canonical | `Forward` |
| `0x1B` | status | rdm | live | canonical | the request and reply variants, in positional order, are on the generated [wire page](/home/admin/rust-distributed-mesh/docs/reference/rdm/wire.md); `Reserved6`, `Reserved7` and `NodeLeft` are reserved positions, never sent; until the initial fabric has first reached ready-for-traffic, every node property command (`UpdateName`, `CreateTags`, `DeleteTags`, `UpdateNodeConfig`) is refused `fabric-not-ready`, and `DeleteMesh` and the node workflow calls are sent only for a Build admitted past that refusal ([States](lifecycles/states.md#fabric-not-ready)) |
| `0x1C` | build-claim | rdm | live | canonical | `ClaimAttempt` |
| `0x1D` | join | rdm | live | canonical | `JoinNode` |
| `0x1E` | topology | rdm | live | canonical | `GetTopology` |
| `0x1F` | build-facts | rdm | live | canonical | `FetchBuildFacts`, `FetchCurrentBuild` |
| `0x20` | build | rdm | live | canonical | build.create, build.attempt.run, build.get, build.delete; build.create refuses `fabric-not-ready` until the initial fabric has first reached ready-for-traffic ([Build refusals](#fabric-not-ready-refusal)) |
| `0x21` | primary-update | rdm | live | canonical | `PrimaryUpdate`, `PrimaryUpdated`, `AlreadyApplied`; refused `fabric-not-ready` until the initial fabric has first reached ready-for-traffic |
| `0x22` | node-command | rdm | live | canonical | `DeleteConnections`, `StopNode`, `StartNode`, the mesh-primary to the exact node; server-streaming, never forwarded; the request and reply variants are on the generated [wire page](/home/admin/rust-distributed-mesh/docs/reference/rdm/wire.md) |
| `0x10` | legacy-control | rafka | live | transitional | `ControlFrame` |
| `0x12` | data-frame | rafka | live | transitional | `DataFrame` broker operations |
| `0x13` | snapshot | rafka | live | transitional | mesh digest snapshot |
| `0x14` | control | rafka | live | transitional | `MeshControlMessage` |
| `0x15` | credential-resolve | rafka | live | transitional | credential resolution |
| `0x16` | layout-reprovision | rafka | retired | | |
| `0x17` | forward-write | rafka | live | transitional | write through a peer |
| `0x18` | forward-read | rafka | live | transitional | read through a peer |
| `0x19` | peer-tickle | rafka | live | transitional | tickle exact path |
| `0x70` | proof-store | testkit | live | canonical | `Get`, `Put`, `Delete`, `CompareAndSwap` |
| `0x71` | resolve-probe | testkit | live | canonical | `Resolve` |
| `0x72` | declare-probe | testkit | live | canonical | `Declare` |
| `0x73` | originate | testkit | live | canonical | `Call`, `ArmFault`, `ReleaseFault`, `Snapshot`, `RecordProxy`, `Dial`, `StopTransport` |
| `0x74` | before-ready-pull | testkit | live | canonical | `Pull` |
| `0x75` | fault-door | testkit | live | canonical | `Arm`, `Release`, `State` |

The ledger state says whether an op may be served at all: `live` or `retired`. The serving
mode is the catalog entry's, not the ledger's: `canonical`, or `transitional` naming the
canonical family that replaces it. A rafka adapter is live in the ledger and transitional in
this process's catalog.

Owners: `core` is the generic substrate; `rdm` is the mesh product; `rafka` is Rafka's own
adapters, which are transitional entries; `testkit` holds `0x70` to
`0x7F` for test-only oracles that never ship in a product binary.

## Typed object contract

Callers use typed objects: `node.create`, `node.start`, `node.stop`, `node.restart`,
`node.delete`, `node.get`, `node.name.update`,
`node.topology.get`, `node.connections.get/delete`, `node.tags.get/create/delete`, `node.config.get/update`,
`mesh.get`, `mesh.update`, `mesh.nodes.get`, `mesh.config.get/update`, `mesh.delete`,
`fabric.get`, `fabric.update`, `fabric.meshes.get`, `fabric.nodes.get`,
`fabric.config.get/update`, `node.primary.update`, `node.fabric-primary.update`, `fabric.create`, `fabric.delete` and
`build.create/get/delete`. The objects build the requests. There is one implementation of each
typed object: the Build runner, tests and the application all call it, and no surface builds a
request by hand. This section owns the caller vocabulary, the lifecycle pages own the workflow
shapes, and the ledger and schemas here own wire op numbers and codecs.
The node, mesh and fabric families have no op number; their typed objects map onto the
existing ops below. The build family is op 0x20.

### Caller execution and certainty

A changing call returns a streamed execution view: Started, each completed step's
noun/past-tense event, then its terminal result. Reads return their typed result and
publish no lifecycle event. A failure is `Failed { step, reason }`: no downstream
success event follows it.

Transport uncertainty is NotSent before the request commit cut and Indeterminate
after it; both are statements about execution certainty, and under either the runtime keeps
delivering the same invocation ([node-rpc.md §43](node-rpc.md#43-owed-messages-delivery-is-the-runtimes)),
which authorises no new execution. A broken stream proves neither step failure nor completion. The Build's
only durable step outcomes are Complete and Failed { step, reason }. build.get is a read for reattachment, never a resume. The caller resubmits the
same build_id to the fabric-primary under the [Build carrier](#build-op-0x20-rdm). Reissuing a naturally
idempotent uncertain step can settle it with AlreadyApplied. A Build adds durability, not
visibility: without one, the caller still sees what completed, where it stopped and why on the
reply stream. Gossip is for peers, never the caller's completion interface. Trace propagation across the calls of one
operation is [Runtime spans](#runtime-spans).

Call patterns:

- **Single call:** one command to one node, with one reply.
- **Parallel calls:** a node-admin sends the same command to several nodes, each with its own
  target, caller span, propagated trace context and result, under the workflow's trace
  ([Runtime spans](#runtime-spans)); unrelated parallel calls may use separate traces.
- **Batch call:** one request lists several targets. Its protocol defines a result per target;
  grouping calls does not define a batch protocol.
- **Workflow:** one call that RDM runs as several steps in one Build, each step a frame on that
  call's reply stream, such as [node.delete](lifecycles/node-delete.md). A step that answers
  Indeterminate or a refusal leaves the workflow incomplete. A later attempt resumes from the
  recorded receipts, never from step 1, and validates those receipts against the accepted
  operation and exact births before it reuses them.

The target is `{ node_id, op }` ([Section 0](#section-0-the-target)); `(node.id, incarnation)`
is the exact-birth identity used by resolution, lifecycle checks and terminal proof.
Stop/start/restart retain the same instance. A stopped endpoint answers anyone who reaches it:
the mesh adds no generic caller check or authorship validation. connections-deleting and stopped
are defined in [States](lifecycles/states.md#stopped-nodes); the graceful closure behind
connections-deleting is [node.connections.delete](lifecycles/node-connections-delete.md), whose
receipt is a `ConnectionsReceipt`, `Established { in_flight, closed }` or `Deadline { last_in_flight, closed, held }`.

### Typed objects mapped to wire and execution owners

These are adapter mappings, not new enum definitions.

Every row also retains §25 and §40's delivery code when it does not produce a typed result: an owner-bound lapse is DEADLINE_EXCEEDED (certainty stays NotSent or Indeterminate), receiver-gone is UNAVAILABLE, and a lost reply is UNKNOWN. A row can span several calls; each reply frame has its own code and typed detail. A provider inspection or a connection admission remains its own typed result, with no new Node RPC wire code.

| Typed caller | Existing wire / execution component | Caller result / events | code |
|---|---|---|---|
| [node.create](lifecycles/node-create.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; first join on [JoinNode 0x1D](#join-op-0x1d-rdm); a held name answers NameHeld { by }; the create of a mesh's first node-admin is the mesh-absent arm, [mesh.create](lifecycles/mesh-create.md), with no call of its own | every FirstStart event ending node.started, then node.created | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.start](lifecycles/node-start.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [Join 0x1D](#join-op-0x1d-rdm), [topology 0x1E](#topology-op-0x1e-rdm), [Build facts 0x1F](#build-facts-op-0x1f-rdm), [NodeCommand 0x22 StartNode](#nodecommand-op-0x22-rdm), exact-node hop only; CommandAccepted → NodeJoined (Data) → NodeStarted (Terminal) | node.joined, node.started; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.stop](lifecycles/node-stop.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [NodeCommand 0x22 StopNode](#nodecommand-op-0x22-rdm), exact-node hop only; CommandAccepted → StopConnectionsDeleted (Data) → NodeStopped (Terminal); the stop's first step is `node.connections.delete { scope: All }` | node.connections.deleted, node.stopped; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.restart](lifecycles/node-restart.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; the stop's and the start's ops; no restart opcode | every stop/start event, node.restarted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.delete](lifecycles/node-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; the stop's ops, then provider inspection, which is not a Node RPC | every stop event, then node.deleted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.mesh.join](lifecycles/node-mesh-join.md) | The report is [JoinNode 0x1D](#join-op-0x1d-rdm); standalone, [Status 0x1B](#status-op-0x1b-rdm) `JoinMesh`, answered `MeshJoined { joined }`; inside node.start a step of the start's stream | node.mesh.joined; a refused report fails the primitive by the report's refusal | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.mesh.leave](lifecycles/node-mesh-leave.md) | Standalone, [Status 0x1B](#status-op-0x1b-rdm) `LeaveMesh`, answered `MeshLeft { left }`; node.stop and node.delete end their live part with it, and inside a stop it is the stop's own step and this frame is not sent | node.mesh.left; `BusyHoldingConnections { connections }` while the node holds a connection | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.tickle](lifecycles/node-tickle.md) | [Status 0x1B ProbeNodeState](#status-op-0x1b-rdm) direct, and the same probe through up to two live peers of the node's mesh on [Forward 0x1A](#forward-op-0x1a-core) | `reached { answer }` or `unreached`; no event, and unreached is never death | `OK` for a served probe; unreached retains its typed delivery code |
| [node.runtime.inspect](lifecycles/node-runtime-inspect.md) | The deployment provider's inspect of the runtime handle recorded at launch; not a Node RPC | `running`, `os-stopped`, `exited { code }` or `unavailable { reason }`, node investigation's provider verdict; no event | — (typed provider result; no Node RPC) |
| node.get | [Status 0x1B ProbeNodeState](#status-op-0x1b-rdm) for one exact subject; node.get answers one node by id, the nodes of a mesh, or the nodes that carry given tag labels, all of the given labels or any of them; a node's tags are its own property (node.tags.get/create/delete), and the tag filter names a label, matches every node that carries it, and is read through the node store's tags index | Current; stopped answers CurrentParked; no event | `OK`; unknown subject `NOT_FOUND`, read bound `RESOURCE_EXHAUSTED` |
| [node.topology.get](lifecycles/node-topology-get.md) | [GetTopology 0x1E](#topology-op-0x1e-rdm); complete source snapshot read | complete read result; no event | `OK`; unknown subject `NOT_FOUND`, read bound `RESOURCE_EXHAUSTED` |
| [node.name.update](lifecycles/node-name-update.md) | Rename, to the node's mesh-primary; frames `UpdateName` and `ApplyNodeName` on [0x1B](#status-op-0x1b-rdm); the node admits no connection | node.name.updated; `NameHeld { by }`, `RejectedAdmittingConnections`, `RejectedName`, `RejectedStaleIncarnation`; `fabric-not-ready` | `OK`; NameHeld `ALREADY_EXISTS`, stale `ABORTED`, invalid name `INVALID_ARGUMENT`, other state refusal `FAILED_PRECONDITION` |
| node.connections.get/[delete](lifecycles/node-connections-delete.md) | Held-connection read; `node.connections.delete { scope: All }` or `{ scope: Connections([connection id, …]) }`. `All` is graceful closure of every open connection of the node, each open Direct or Proxy entry of its connections log (gossip neighbours and the Node RPC pool are not connections), with status connections-deleting then connections-deleted and the [order and deadline](lifecycles/node-connections-delete.md#numbered-end-to-end-sequence) of that page; `Connections` closes exactly the named connections, by the minted id of their open entry as node.connections.get returns them; an empty list is refused by name. The endpoint and an active stop stream stay open; each drop is written under the node's current name; the caller's carrier to the node's mesh-primary is [Status 0x1B](#status-op-0x1b-rdm): StatusRequest position 21 `DeleteConnections`, answered by StatusReply position 31 `ConnectionsDeleted`, or by position 32 `RejectedConnectionScope` for a scope that names nothing the node can cut; the mesh-primary carries it to the exact node on NodeCommand 0x22 | read result / node.connections.deleted, naming every id it closed and every id it did not know or found already closed; for `All`, carrying a `ConnectionsReceipt`: `Established { in_flight, closed }` or `Deadline { last_in_flight, closed, held }` | call `OK` when served; closed `OK`, not_open `NOT_FOUND`; `All` Deadline `DEADLINE_EXCEEDED` |
| node.tags.get/create/delete | A node's tags are its own property, not config. `node.tags.get` answers the node's labels; `node.tags.create { label \\| [labels] }` adds one label or a list; `node.tags.delete { All \\| [labels] }` removes every label or the named ones, the same scope shape as `node.connections.delete`. One item or a list, uniform: a single item is the same as a list of one. Each label is its own keyed row (label, node id) in the node store's tags index. Create is a blind put of that key, so creating a label the node already carries is ignored and writes no duplicate; delete tombstones the named keys. A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading '.', and a node carries at most 32; a label outside that grammar, or one past the 32, is refused by name and nothing is written; frames `CreateTags { node_id, incarnation, labels }` and `DeleteTags { node_id, incarnation, scope: All \| Labels }` on 0x1B, replies `TagsCreated { node_id, incarnation, already_carried }` and `TagsDeleted { node_id, incarnation, not_carried }` | read result / create: the reply names each label the node already carried, AlreadyApplied for those | call `OK` when served; already_carried `ALREADY_EXISTS`, not_carried `NOT_FOUND`, applied label `OK`; invalid label `INVALID_ARGUMENT` |
| node.config.get/update | Application config read / full-map put; the [config update sequence](lifecycles/node-config-update.md); dedicated request mapping is open | read result / node.config.updated | `OK`; AlreadyApplied `ALREADY_EXISTS`, malformed config `INVALID_ARGUMENT`, state/row refusal `FAILED_PRECONDITION` |
| [node.cert.renew](lifecycles/node-cert-renew.md) | The mesh-primary, as issuer, to the exact birth: [Status 0x1B](#status-op-0x1b-rdm) `RenewCert { node_id, incarnation, cert }`, answered `CertRenewed`; the node installs the cert and presents it on every connection it opens after; nothing is terminated | node.cert.renewed; a named refusal for another birth or an undecodable cert | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.issuer.update](lifecycles/node-issuer-update.md) | The fabric-primary to one node-admin, exact birth, the mesh-primary directly and any other admin as a forwarded call through the mesh-primary ([Forward 0x1A](#forward-op-0x1a-core)): [Status 0x1B](#status-op-0x1b-rdm) `UpdateIssuer { node_id, incarnation, key, cert }`, answered `IssuerUpdated`; refused by name for another mesh or another birth | node.issuer.updated | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.roots.update](lifecycles/fabric-ca-renew.md#noderootsupdate-the-primitive) | The fabric-primary to one node, exact birth: [Status 0x1B](#status-op-0x1b-rdm) `UpdateRoots { node_id, incarnation, roots, phase }`, answered `RootsUpdated`; a set that drops a root the node still admits a chain under is refused `root-still-chained` | node.roots.updated | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| node.cert.get | A field of node.get: `NodeView.cert { issued_at, expires_at, issuer_expires_at }` on the `Nodes` reply, from the birth's cert row ([fabric-certs.md](fabric-certs.md#tracking)) | read result; no event | `OK`; unknown subject `NOT_FOUND`, read bound `RESOURCE_EXHAUSTED` |
| [node.connections.admit](lifecycles/node-connections-admit.md) | The acceptor's verification of the dialer's member cert on the first bi-stream of an inbound connection of a registered product ALPN; RDM framing, not a Node RPC op | `admitted` or `refused { reason }`, a connection fact ([connections.md](connections.md#12-connection-state)) | — (typed connection fact; no Node RPC) |
| mesh.get, mesh.update, mesh.nodes.get, mesh.config.get/update | mesh.get answers one mesh, or every mesh of a fabric; mesh.update puts the full mesh meta as a blind put and never changes status; mesh.nodes.get answers the mesh's members; mesh.config.get/update hold the application's attributes on the mesh row, as node.config does on a node; a mesh's tags are its own property, mesh.tags.get / mesh.tags.create { label \| [labels] } / mesh.tags.delete { All \| [labels] }, the same shape and label grammar as node.tags; dedicated request mapping is open | read result / mesh.updated / mesh.config.updated | `OK`; AlreadyApplied `ALREADY_EXISTS`, malformed config `INVALID_ARGUMENT`, state/row refusal `FAILED_PRECONDITION` |
| [fabric.root.update](lifecycles/fabric-ca-renew.md#fabricrootupdate-the-caller-op) | A single call to the fabric-primary with a new root secret, the trust anchor; starts the root rotation's phases; refused `rotation-in-progress` or `root-already-trusted` by name; `fabric.get` carries `roots { phase, expires_at }` and `ca { expires_at }` | fabric.root.updated | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| fabric.get, fabric.update, fabric.meshes.get, fabric.nodes.get, fabric.config.get/update | fabric.get answers from memory: the fabric row, the mesh whose mesh-primary is the fabric-primary and that primary's revision; fabric.update puts the full fabric meta as a blind put and never changes status; fabric.meshes.get lists the meshes; fabric.nodes.get answers the fabric topology; fabric.config.get/update hold the application's attributes on the fabric row; dedicated request mapping is open | read result / fabric.updated / fabric.config.updated | `OK`; AlreadyApplied `ALREADY_EXISTS`, malformed config `INVALID_ARGUMENT`, state/row refusal `FAILED_PRECONDITION` |
| [mesh.delete](lifecycles/mesh-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow; [0x21](#primaryupdate-op-0x21-rdm) (node.fabric-primary.update) when the mesh's mesh-primary is the fabric-primary; [Status 0x1B DeleteMesh / MeshDeleted](#mesh-delete-command-pair); node.delete's ops per member | mesh.deleting, mesh.deleted; `fabric-not-ready` refusal until the initial fabric has first reached ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [fabric.create](lifecycles/fabric-create.md) | The first node-admin, started with no mesh and no durable topology, is the executor and the fabric-primary; the accepted topology rides [build.create 0x20](#build-op-0x20-rdm) | fabric.created when the fabric is ready-for-traffic | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [fabric.delete](lifecycles/fabric-delete.md) | [build.create 0x20](#build-op-0x20-rdm) carries the workflow, one Build the fabric-primary drives: every mesh removed leaf-inward, its own mesh last | fabric.deleted from the fabric-primary, still alive; its own node.delete is a separate call from outside the fabric | `OK` for served result/completion; `FAILED_PRECONDITION` for named state refusal |
| [node.primary.update](lifecycles/node-primary-update.md) | [0x21 PrimaryUpdate / PrimaryUpdated](#primaryupdate-op-0x21-rdm), the primary to its successor's exact birth | node.primary.updated { revisions }; RejectedConflictingPayload; `fabric-not-ready`; on the commanded arm, `Failed { step, reason }` when the update ends failed | `OK`; stale/conflict `ABORTED`, absent record `NOT_FOUND`, other state refusal `FAILED_PRECONDITION` |
| [node.fabric-primary.update](lifecycles/node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) | the same call, the fabric-primary as the `fabric` entry of `primaries` | node.fabric-primary.updated; the fabric arm's no-successor refusal | `OK`; stale/conflict `ABORTED`, absent record `NOT_FOUND`, other state refusal `FAILED_PRECONDITION` |
| build.create/get/delete | [Build 0x20](#build-op-0x20-rdm); the fabric-primary accepts every Build and drives topology Builds; a mesh-primary drives drift attempts of the current Build inside its mesh. [ClaimAttempt 0x1C](#buildclaim-op-0x1c-rdm) is the attempt claim; [FetchBuildFacts 0x1F](#build-facts-op-0x1f-rdm) is the facts read | Started, step frames, Complete / Failed; reattachment reads; deletion result | `OK` for read/result/completion; AlreadyApplied `ALREADY_EXISTS`, unknown Build `NOT_FOUND`, stale/conflict `ABORTED`, writer/state refusal `FAILED_PRECONDITION`, BuildInProgress `UNAVAILABLE` |
| build.attempt.run | [Build 0x20](#build-op-0x20-rdm); fabric-primary calls the named executor with the won claim | receipt replay, current Blocked, then live frames | `OK` for read/result/completion; AlreadyApplied `ALREADY_EXISTS`, unknown Build `NOT_FOUND`, stale/conflict `ABORTED`, writer/state refusal `FAILED_PRECONDITION`, BuildInProgress `UNAVAILABLE` |

Codec evidence: `crates/rdm-core/src/status.rs::StatusRequest`
(DeclareNodeState, ApplyNodeState, ProbeNodeState; StopNode and StartNode are reserved there),
`crates/rdm-core/src/node_command.rs::NodeCommandRequest` (DeleteConnections, StopNode, StartNode),
`crates/rdm-core/src/join.rs::JoinRequest`,
`crates/rdm-core/src/topology.rs::TopologyRequest`,
`crates/rdm-core/src/build_facts.rs::BuildFactsRequest` and
`crates/rdm-core/src/build_claim.rs::BuildClaimRequest`. The
request target is `crates/rdm-core/src/framing.rs::Target`. The
request-commit certainty boundary is
`crates/rdm-core/src/outcome.rs::RpcOutcome`.

### Node properties

Each command goes to the node's mesh-primary as the row writer (Forward `0x1A` when not directly reachable), is refused `fabric-not-ready` until the initial fabric has first reached ready-for-traffic, and runs through that writer's defined storage operation.

| property | set by | changed by | precondition | completion |
|---|---|---|---|---|
| `node.id`, `incarnation`, `endpoint`, `transport_addr`, the runtime handle | RDM at birth | nothing | — | — |
| `status` | RDM | [node status](lifecycles/node-status.md) only | — | that status's own completion |
| `path`, `mesh`, `node_type` | the create spec | nothing: immutable for the node's life. `node.name.update` carries the name only, so the path cannot move by construction | — | — |
| `name` | the create spec | **`node.name.update { node_id, incarnation, name }`** ([node.name.update](lifecycles/node-name-update.md)) | the node admits no connection: stopped, or connections-deleted after an `All` with zero held; never ready-for-traffic (R-CR1) | `node.name.updated` |
| `tags` | the create spec (may be empty); inherited with the config on recover (from the Deleted row at the claim) and on replace (from the retained predecessor row at the cutover rename) | `node.tags.create { label \| [labels] }` adds; `node.tags.delete { All \| [labels] }` removes. Additive and removal commands on keyed label rows, not a replacement of a tags property | none | `node.tags.created` / `node.tags.deleted` |
| `config` | the create spec | `node.config.update { map }`, the whole map replaced ([node.config.update](lifecycles/node-config-update.md)) | none | `node.config.updated` |
| `types` | the birth's [JoinNode](#join-op-0x1d-rdm): what it declared; empty for a node-admin | nothing: a different declaration is a restart. `node.get` answers it whole | — | — |
| `held`, `held_hash` | RDM: the types the node has joined ([gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)); equal to `types` for an ordinary node; for a node-admin the union of its members' declarations and what it read from other node-admins. `held_hash` is blake3 of the sorted set and rides the row's digest (32 bytes; no list rides gossip) | RDM, as members join; grows, never shrinks within a birth; never a put. `node.get` answers `held` whole | — | — |
| `is_primary` | the election's result, recorded by the mesh-primary | `node.primary.update`; never a put | node.primary.update's | `node.primary.updated` |
| `is_fabric_primary` | the election's result, recorded by the node-admin on its own row | `node.fabric-primary.update` | node.fabric-primary.update's | `node.fabric-primary.updated` |
| connections | — (data plane, never a property) | `node.connections.delete { All \| [ids] }` | — | `node.connections.deleted` |

`node.get` answers the whole node. Nothing above is PATCH: a rename replaces the name, a tag command adds or removes whole labels on their own rows, a config update replaces the whole map, and each runs once through the one writer.

Status moves only through [node status](lifecycles/node-status.md), never a put. A new exe requires a new node.

node.config is application attributes, a property bag; a node's tags are its own property, not config. Config is distinct from join's cert/time
and the topology read. Config is a map of name to value; a body that names a key twice keeps the
last value, `admin_api_base` included. It is a field of the node record, and the node's digest
carries the live copy (the `config` map). RDM never reads it; it is capped in keys and bytes and
refused by name past either ([cap and budget](gossip.md#31-frame-catalog)). The
[config update sequence](lifecycles/node-config-update.md) is a single call, not a Build. A
node created at a path.name seeds its config from the previous row at that path.name; a rejoining
node gets it back in its join answer.

### Open typed object questions

- Mesh and fabric workflows compose node.stop and node.delete as [mesh.delete](lifecycles/mesh-delete.md) and [fabric.delete](lifecycles/fabric-delete.md) state; neither has an op number of its own.

Server-streaming entries are not Forward-eligible under the existing catalog rule.
The objects hide the eventual family split; wire positions and field order need
coordinated fixtures when that split lands.

## Node RPC readiness gate

A stopped node's control service stays reachable through its bound endpoint
([States](lifecycles/states.md#stopped-nodes)); that reachability is separate from this door.

The router refuses every op, Ping included, with typed NotReady until the Node RPC door
is ready. A node-admin waits until its primary is filled and its view holds it;
an rpc node waits until it joins its mesh. The rejection span is
`rdm.node_rpc.request.reject.via-not-ready`. This door is the generic serving gate, not a
fabric traffic-opening declaration. Source:
`crates/rdm-transport/src/server.rs::with_ready_gate`.

NotReady, on every op and from every handler, means one thing: the door or the handler cannot
serve this call yet, and the reason says which. It is never a lifecycle fact about the subject.
A caller that receives NotReady learns nothing about the subject's hook, frames no Build
`Blocked`, and waits or retries. The subject's readiness blocker is a served answer, Status
[`Blocked`](#status-op-0x1b-rdm): a probe that reached the exact birth and found its
`before_ready` hook Blocked. The two must never share a variant.

# Schemas

Postcard is positional: a type's field order and an enum's variant order are the wire format.
These definitions are the schemas; a change to any of them moves the format.

## Target and context

```rust
pub struct Target {
    pub node_id: String,
    pub op: u8,
}

pub struct CallContext {
    pub caller_system: Option<String>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    pub baggage: Option<String>,
}
```

A decoded request, shown as a readable JSON view of the three postcard sections (the wire is
postcard, not JSON): a StopNode on NodeCommand op `0x22` (34).

```json
{
  "target": { "node_id": "k8pag1qx95gw", "op": 34 },
  "context": {
    "caller_system": "rdm",
    "traceparent": "00-f16a8b75a1b2c3d4e5f60718293a4b5c-1111111111111111-01",
    "tracestate": null,
    "baggage": "operation=node.stop%3Amesh1.rpc.1"
  },
  "body": {
    "StopNode": {
      "node_id": "k8pag1qx95gw",
      "incarnation": "0000000000000008",
      "build_id": "3f9a1c7e5b2d",
      "attempt": 1,
      "operation": "node.stop:mesh1.rpc.1"
    }
  }
}
```

A unary reply is the body alone, with no second target or context.

## Runtime spans

`MalformedKind` is shared by the protocol refusal variants:

```rust
pub enum MalformedKind { TooLarge, UnknownVariant, Corrupt, TypesOverCap }
```

RDM-owned Node RPC instrumentation uses the `rdm` owner root under the repository grammar
`rdm.node_rpc.<entity>.<action>.via-<reason>`. Rafka-owned handlers and integration bindings keep
`rafka`; third-party spans keep their native namespace. Resource `service.name` names the host
process, while `caller_system` identifies invocation origin and is observability only. See
[observability conventions](/home/admin/rafka-v2/docs/eng/observability-conventions.md). Runtime acceptance cites the
names below.

Every canonical call emits `rdm.node_rpc.request.update.via-call` on the caller and
`rdm.node_rpc.request.serve.via-direct` on the server. The caller records `target`, `op`,
`outcome`, `reason`, `reused`, `elapsed_ms`, `code`, `detail`; the server records `protocol`,
`op`, `peer`, `caller_system`, `context_dropped`, `test_case`, `scenario`, `operation`,
`code`, `detail`. `code` is the standard gRPC name from §25. `detail` names the typed result
or refusal, or the typed local delivery reason; explanation text stays in `reason` and never
selects a code. Both attributes come from the typed outcome. An owner's bound lapsing is
`DEADLINE_EXCEEDED`, including before commit; resolver Unknown is `NOT_FOUND`, while a re-created
receiver-gone is `UNAVAILABLE`; a lost connection after commit is `UNKNOWN`, while HandlerFault
and a real PROTOCOL_VIOLATION are `INTERNAL`.
With propagated context, the server shares the caller's TraceId and its ParentSpanId is the
call span's SpanId. Handler spans belong to the serving process. The invocation's own end — a stream's
`rdm.node_rpc.stream.serve.via-terminal` or refusal, a unary fault or panic — is a child of its serve span.
Stream result frames and the invocation's end record their own `code` and `detail`; workflow step
spans retain the step's code and named detail, and a failed Build receipt carries that code beside
its step and explanation. An application's compatibility adapters emit their own diagnostics, stated
on its pages; they do not acquire canonical spans merely by being reserved in the ledger.

Each call carries the operation trace; the child calls of one workflow share it, each with its
own caller span. Correlate events and receipts by `build_id`, `attempt`, `operation`, `step`,
`node_id` and `incarnation`. Each completed workflow step has its event, a span under
`rdm.node_admin.node.<verb>.via-…` and, for Build-backed work, its own receipt. Call and serve
evidence includes the target node's action, not only the node-admin's. Gossip carries no
CallContext: a gossip frame is correlated by identity ([gossip correlation](gossip.md#gossip-correlation)),
never by a W3C parent edge.

The Node RPC runtime (`rdm-transport`) emits:

```text
rdm.node_rpc.request.serve.via-direct
rdm.node_rpc.request.serve.via-carried-inner
rdm.node_rpc.request.reject.via-unserved-op
rdm.node_rpc.request.reject.via-busy
rdm.node_rpc.request.reject.via-not-ready
rdm.node_rpc.request.reject.via-malformed
rdm.node_rpc.request.reject.via-protocol-violation
rdm.node_rpc.request.reject.via-frame-not-sent
rdm.node_rpc.request.reject.via-not-forwardable
rdm.node_rpc.request.reject.via-carrier-edge-lost
rdm.node_rpc.request.reject.via-forward-budget-spent
rdm.node_rpc.request.update.via-call
rdm.node_rpc.request.update.via-context-dropped
rdm.node_rpc.handler.reject.via-internal-rpc-failure
rdm.node_rpc.connection.reject.via-stale-target
rdm.node_rpc.connection.evict.via-incarnation-superseded
rdm.node_rpc.connection.evict.via-timeout-strikes
rdm.node_rpc.route.resolve.via-connections
rdm.node_rpc.stream.serve.via-direct
rdm.node_rpc.stream.serve.via-terminal
rdm.node_rpc.stream.reject.via-protocol-violation
```

The live resolver's membership updates (`rdm-admin`, `rdm-test`):

```text
rdm.node_rpc.node.update.via-membership
rdm.node_rpc.node.remove.via-membership
rdm.node_rpc.node.reject.via-membership
```

The testkit oracles and the proof product's own families (`rdm-test`,
`rdm-node`) are RDM-owned test and proof instrumentation, never product acceptance
evidence:

```text
rdm.node_rpc.proof_store.serve.via-request
rdm.node_rpc.proof_store.resolve.via-probe
rdm.node_rpc.resolve_probe.serve.via-request
rdm.node_rpc.declare_probe.serve.via-request
rdm.node_rpc.status.update.via-declare-own
rdm.node_rpc.broker_data.serve.via-append
```

Per-call pre-body info spans are forbidden. Counters carry storm-rate information; caller spans
carry individual refusal evidence. Protocol and domain spans nest below the invocation span.

The attributes the runtime records, where known:

```text
protocol
op
target
node_id
receiver_node_id
peer
caller
caller_system
context_dropped
decided_by
mismatch
route
carrier
outcome
reason
fault
current
strikes
bytes
frames
caller_cancelled
cancellation_reason
test_case
scenario
operation
```

## Ping, op `0x01`, core

Contract source: RDM `crates/rdm-core/src/ping.rs::PingRequest`.

Direction: peer. Any mesh member calls an exact member; that member serves it. Request ceiling 64 KiB; reply ceiling 64 KiB + 64 bytes. Not forwardable. Typed refusals are the six refusal variants below. Caller/server emit the runtime spans above with this op.

```rust
pub enum PingRequest {
    Ping { payload: Vec<u8> },
}

pub enum PingReply {
    Pong { payload: Vec<u8> },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}
```

## Forward, op `0x1A`, core

Contract source: `crates/rdm-core/src/forward.rs::ForwardRequest`;
reply variants and count: `crates/rdm-core/src/forward.rs::ForwardReply`
and `crates/rdm-core/src/forward.rs::Forward`.
Forward is a core op served by every node, node-admins included. Direction: peer. The source
calls an exact carrier; the carrier makes one direct inner call and never chooses a different
target or another carrier. Not itself forwardable. Forward is a control-plane call and keeps nothing; a
data-plane path through a carrier is a `Proxy` connection
([connections.md §1.1](connections.md#11-connection-kind)). Carried ceiling 1 MiB; request/reply ceilings
are that plus 256 bytes. The received CallContext is copied verbatim into the inner call.

```rust
pub enum ForwardRequest {
    Forward {
        target: NodeId,
        inner_op: u8,
        inner: Vec<u8>,
        remaining_ms: u64,
        owed: Option<OwedDelivery>,
    },
}

pub struct OwedDelivery {
    delivery_seq: u64,
    forget_below: u64,
}

pub enum ForwardReply {
    Forwarded { inner: Vec<u8> },
    InnerNotSent { reason: String, cause: NotSentCause },
    InnerUnserved { op: u8 },
    InnerRejectedStale { target_node_id: NodeId },
    InnerIndeterminate { reason: String, cause: IndeterminateCause },
    NotForwardable { op: u8 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    CarrierEdgeLost { reason: String },
    NoInnerBudget { remaining_ms: u64, reserve_ms: u64 },
}

pub enum IndeterminateCause {
    ReceiverGone,
    ReplyDeadline,
    ReplyLost,
    Reset(u64),
    UnsupportedReplyVariant,
    CorruptReply,
    ProtocolViolation,
}

pub enum NotSentCause {
    Resolve(ResolveFailure),
    Connection,
    StreamBudget,
    Deadline,
    FrameNotSent,
    Carried,
    CarrierEdgeLost,
    CarrierNoBudget {
        remaining_ms: u64,
        reserve_ms: u64,
    },
    NotForwardable {
        op: u8,
    },
    NoActiveRoute,
    AdmissionBlocked,
}

pub enum ResolveFailure { Unknown, Gone, Unavailable }
```

The `cause` field is appended after `reason` in each of these two variants. It is filled from the inner call’s typed outcome, never from its explanation. The origin rebuilds the typed `NotSentReason` or `IndeterminateReason` from `cause` and keeps `reason` as explanation. A receiving birth that was re-created is `UNAVAILABLE` with detail `receiver-gone`; an owner’s bound lapsing is `DEADLINE_EXCEEDED`, before or after commit; a lost reply or non-fault reset after commit is `UNKNOWN`. `INTERNAL_RPC_FAILURE` is `INTERNAL`. The carrier’s own `CarrierEdgeLost` and failed inner dial are `UNAVAILABLE`; `NotForwardable` is `FAILED_PRECONDITION`. These codes describe the typed result; they change neither dispatch certainty nor delivery obligations. This is the Q40 Forward-only wire change; the 14 reply positions stand, and no legacy decoder is kept.

The fourth, last request field `remaining_ms` is a postcard varint of whole milliseconds.
The source measures it immediately before writing the frame, after carrier resolution and
dial: an overall budget uses deadline minus now; a split budget uses its reply budget.
Forwarding never resets a caller's deadline to the default.
The carrier bounds its inner call by remaining_ms minus FORWARD_REPLY_RESERVE (100 ms).
The reserve covers transport and decode only; it is not connection-fact bookkeeping time.
See `crates/rdm-transport/src/client.rs::open` and
`crates/rdm-transport/src/forward.rs::carry_once`.

Reply position 13 is NoInnerBudget; REPLY_VARIANTS is 14. If remaining minus reserve is
<= 0, the carrier emits `rdm.node_rpc.request.reject.via-forward-budget-spent`, makes no
inner call and returns NoInnerBudget. The source maps it to NotSent(CarrierNoBudget).
The carrier returns CarrierEdgeLost directly from its failed dial; its connection-fact
write happens after the reply and is never on the reply path. Outer and inner calls emit
`rdm.node_rpc.request.update.via-call`; outer serving emits
`rdm.node_rpc.request.serve.via-direct`, and carried inner serving emits
`rdm.node_rpc.request.serve.via-carried-inner`.

## Status, op `0x1B`, rdm

Contract source: RDM `crates/rdm-core/src/status.rs::StatusRequest`.

Direction: upward for declarations; downward for applies and probes. An ordinary node declares to its mesh-primary node-admin. Every node's status, node-admins included, is set by its mesh's primary on the node's report, and the mesh-primary sets its own ([node.status](lifecycles/node-status.md)); a mesh-primary declares its mesh state to the fabric-primary. The primary applies/probes the exact node birth; the fabric-primary applies mesh state at its bootstrap admin and fabric events at its mesh-primary.

The request and reply ceilings and the request and reply variants, in their positional order, are on the generated wire page ([wire.md](/home/admin/rust-distributed-mesh/docs/reference/rdm/wire.md)). Forwardable. Nested enum order is part of the wire schema. Typed stale, not-the-primary (`RejectedNotPrimary`), not-handled (`RequestNotHandled`) and invalid-transition refusals are separate from runtime refusals. Caller/server emit the runtime spans above; status handling emits `rdm.node_admin.status.update.via-declaration` or `rdm.node_admin.status.update.via-probe` for the corresponding effects. Stop command/completion evidence follows the separate pair below.

Fabric and mesh lifecycle semantics are owned by [States](lifecycles/states.md#states).
The [node stop pair](#node-stop-command-pair), the
[fabric status rounds](#fabric-status-rounds) and the
[mesh delete pair](#mesh-delete-command-pair) specify the lifecycle calls.
The lifecycle op pairs in States are carried by 0x1B as separate down (command) and up (completion) variants; enum definitions live only here. Dead is never sent.

Status remains admissible while connections-deleting. The generic guard MUST NOT reject it before variant-specific direction, authorization, exact-birth and transition checks. `node.connections.delete { scope: All }` is the sole path into connections-deleting. Lifecycle callers MUST NOT use ApplyNodeState(ConnectionsDeleting); ApplyNodeState carries the permitted transitions other than connections-deleting. ProbeNodeState answers Current, or Blocked { node_id, incarnation, reason, retry_on } while the subject's [before_ready](fabric-node-hooks.md#outcomes-and-the-gate) hook is Blocked: the probe served the exact birth and found an active readiness blocker, answered before any republish so a polled blocked birth republishes nothing. Its `reason` is the hook's own text and `retry_on` the event the hook runs again on. Running and Passed hooks answer Current. NotReady is the [readiness gate's](#node-rpc-readiness-gate) answer and never a hook blocker; the executor's WaitForNodeReady frames `Blocked { WaitForNodeReady, reason }` only from Status Blocked. Declarations and mesh/fabric applies answer Applied or AlreadyApplied. ApplyNodeName, sent down by the node's mesh-primary after it wrote the renamed row ([node.name.update](lifecycles/node-name-update.md)), answers Applied once the node holds and publishes the name, AlreadyApplied for the name it already holds, and `BusyHoldingConnections { connections }` naming each connection it still holds. ConnectionsDeleting is a refusal, never the success of node.connections.delete. Idempotency is the natural key `(node_id, incarnation, state)`, `(mesh_id, state)` or `(fabric_id, event)`. Backward transitions are refused by name.

Nothing inside the mesh checks who sent a frame or call. `RejectedNotPrimary` says the receiver is not the primary for the call: `ReceiverNotPrimary { needed }` when the receiver is not the primary the declaration needs, and `SubjectUnknown` when the receiver does not hold the subject at all. `RequestNotHandled { request }` means the receiver's Status door has no handler for the named request. It is decided by dispatch alone, without checking node kind, primary or sender, and performs no operation. The receiver emits `rdm.node.status.reject.via-request-not-handled` with `request` and `node`. A handler that exists may still return its defined result, including a genuine writer redirect. `SenderNotSubject` and `Unauthorized` keep their codec positions and a mesh receiver never sends them; a caller-side `PeerUnresolved` likewise.

```rust
pub enum NodeState {
    Pending,
    ReadyForTraffic,
    ConnectionsDeleting,
    Stopped,
}

// Positional postcard discriminants: Pending=0, ReadyForTraffic=1, Deleting=2, Deleted=3.
pub enum MeshState {
    Pending,
    ReadyForTraffic,
    Deleting,
    Deleted,
}

pub enum FabricEvent {
    ReadyForTraffic,
    ShutdownInitiated { initiated_by: String },
}

// The operation that produced a declared state: a stop's Stopped, a start's Pending and
// ReadyForTraffic; None for a state the node measured itself.
pub struct DeclaredOperation { build_id: String, attempt: u32, operation: String }

pub enum StatusRequest {
    DeclareNodeState { node_id: NodeId, incarnation: IncarnationId, state: NodeState, operation: Option<DeclaredOperation> },
    DeclareMeshState { mesh_id: MeshId, state: MeshState },
    ApplyNodeState { node_id: NodeId, incarnation: IncarnationId, state: NodeState },
    ProbeNodeState { node_id: NodeId, incarnation: IncarnationId },
    ApplyMeshState { mesh_id: MeshId, mesh_name: String, state: MeshState },
    ApplyFabricEvent { fabric_id: FabricId, event: FabricEvent },
    Reserved6 { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 6, reserved: never sent
    Reserved7 { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 7, reserved: never sent
    StopNode { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 8, reserved: refused by name, never executed
    NodeLeft { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 9, reserved: never sent
    DeleteMesh { mesh_id: MeshId, build_id: String, attempt: u32, operation: String },
    MeshDeleted {
        mesh_id: MeshId,
        build_id: String,
        attempt: u32,
        operation: String,
        final_node_id: NodeId,
        final_incarnation: IncarnationId,
        final_runtime: RuntimeFact,
        receipt_manifest: String,
    },
    CommitState { fabric_id: FabricId, node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
    StateCommitted { fabric_id: FabricId, node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
    OpenTraffic { fabric_id: FabricId, node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
    TrafficOpened { fabric_id: FabricId, node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
    StartNode { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String }, // position 16, reserved: refused by name, never executed
    UpdateNodeConfig { node_id: NodeId, incarnation: IncarnationId, config: BTreeMap<String, String> },
    ApplyNodeConfig { node_id: NodeId, incarnation: IncarnationId, config: BTreeMap<String, String> },
    ApplyNodeName { node_id: NodeId, incarnation: IncarnationId, name: String },
    UpdateName { node_id: NodeId, incarnation: IncarnationId, name: String },
    DeleteConnections { node_id: NodeId, incarnation: IncarnationId, scope: ConnectionScope },
    GetNodes { select: NodeSelect },
    GetMeshes { mesh: Option<String> },
    GetFabric,
    GetConnections { node_id: NodeId, incarnation: IncarnationId },
    CreateTags { node_id: NodeId, incarnation: IncarnationId, labels: BTreeSet<String> },
    DeleteTags { node_id: NodeId, incarnation: IncarnationId, scope: TagScope },
    JoinMesh { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
    LeaveMesh { node_id: NodeId, incarnation: IncarnationId, build_id: String, attempt: u32, operation: String },
}

pub enum TagScope {
    All,
    Labels(Vec<String>),
}

// node.connections.delete's scope (lifecycles/node-connections-delete.md): All, or the exact
// connections named by their minted ids, at least one.
pub enum ConnectionScope {
    All,
    Connections(Vec<ConnectionId>),
}

// Which mesh a node.mesh.join or node.mesh.leave reached: its own mesh, and the node-admin
// backbone for a node-admin.
pub enum WhichMesh {
    Own,
    Backbone,
}

// NodeSelect (Id, Mesh or Tags), TagFilter, NodeView, MeshView, FabricView, ConnectionsView, NodeStatus and
// MeshDeleteRound are on the generated wire page with their fields.

pub enum NotPrimary {
    ReceiverNotPrimary { needed: String },
    SenderNotSubject { sender: String },
    SubjectUnknown,
}

pub enum ConnectionsReceipt {
    Established { in_flight: u64, closed: Vec<ConnectionId> },
    Deadline { last_in_flight: u64, closed: Vec<ConnectionId>, held: Vec<ConnectionId> },
}

pub struct ConfigOverCap {
    pub keys: u32,
    pub bytes: u32,
    pub max_keys: u32,
    pub max_bytes: u32,
}

pub enum StatusReply {
    Applied,
    AlreadyApplied,
    Reserved2 { in_flight: u64 }, // position 2, reserved: never sent
    Current { node_id: NodeId, incarnation: IncarnationId, state: NodeState },
    RejectedStaleIncarnation { held: IncarnationId },
    RejectedStaleMesh { held: MeshId },
    RejectedStaleFabric { held: FabricId },
    RejectedNotPrimary { why: NotPrimary },
    RejectedInvalidNodeTransition { current: NodeState },
    RejectedInvalidMeshTransition { current: MeshState },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    RejectedUnmatchedCompletion { field: String, expected: String, reported: String },
    Stopped { receipt: ConnectionsReceipt },
    Started,
    StartFailed { step: String, reason: String },
    CurrentParked { node_id: NodeId, incarnation: IncarnationId },
    ConfigUpdated { node_id: NodeId, incarnation: IncarnationId },
    RejectedConfig { over: ConfigOverCap },
    ConfigRowRefused { node: String, reason: String },
    ConfigNotApplied { node: String, answered: String, reason: String },
    Blocked { node_id: NodeId, incarnation: IncarnationId, reason: String, retry_on: RetryOn },
    NameUpdated { node_id: NodeId, incarnation: IncarnationId },
    RejectedAdmittingConnections { status: NodeStatus, held_connections: Vec<ConnectionId> },
    NameHeld { name: String, by: NodeId },
    NodeRowRefused { node: String, reason: String },
    NameNotApplied { node: String, answered: String, reason: String },
    ConnectionsDeleted { closed: Vec<ConnectionId>, not_open: Vec<ConnectionId>, held: Vec<ConnectionId>, receipt: Option<ConnectionsReceipt> },
    RejectedConnectionScope { reason: String },
    Nodes { nodes: Vec<NodeView> },
    Meshes { meshes: Vec<MeshView> },
    Fabric { fabric: FabricView },
    Connections { connections: ConnectionsView },
    UnknownMesh { mesh: String },
    ReadOverBound { read: String, bytes: u64, max: u64 },
    RejectedStaleRound { held: MeshDeleteRound, offered: MeshDeleteRound },
    RejectedConflictingRound { held: MeshDeleteRound, offered: MeshDeleteRound },
    RejectedIncompleteManifest { missing: u32, without_exit: Vec<String>, reason: String },
    RejectedSourceFailed { source: String, op: String, cause: String },
    RejectedTags { reason: String },
    RejectedFabricNotReady { workflow: String, status: String },
    TagsCreated { node_id: NodeId, incarnation: IncarnationId, already_carried: BTreeSet<String> },
    TagsDeleted { node_id: NodeId, incarnation: IncarnationId, not_carried: BTreeSet<String> },
    RejectedName { reason: String },
    BusyHoldingConnections { connections: Vec<ConnectionId> },
    MeshJoined { node_id: NodeId, incarnation: IncarnationId, joined: Vec<WhichMesh> },
    MeshLeft { node_id: NodeId, incarnation: IncarnationId, left: Vec<WhichMesh> },
    RequestNotHandled { request: String },
}

// The event a Blocked before_ready hook runs again on (fabric-node-hooks.md,
// "Outcomes and the gate"). Positional order is part of the wire schema.
pub enum RetryOn {
    PrimaryChanged,
    TargetReachable { node_id: NodeId },
    PrimaryReady,
    PrimaryAddressed,
}
```

`ConfigOverCap` is `crates/rdm-core/src/digest.rs::ConfigOverCap`.

<a id="fabric-round-command-pairs"></a>

### Fabric status rounds

The fabric statuses state-commit and ready-for-traffic are carried down in rounds on op
0x1B: CommitState / OpenTraffic carry the status down, StateCommitted / TrafficOpened are
the check-ins. The four bodies are [StatusRequest](#status-op-0x1b-rdm) positions 12 to 15.
Who commands whom, when a check-in is owed and when a primary may report upward are the
[round checklist](lifecycles/states.md#the-round-checklist). The payload's node_id/incarnation
names the commanded or reporting subject; the target names the receiver of that particular
directed call.

CommitState/StateCommitted use operation `commit-state:<fabric_id>`;
OpenTraffic/TrafficOpened use `open-traffic:<fabric_id>`. The round is scoped to the
accepted Build, attempt and planned exact births. A successful admission reply is Applied or
AlreadyApplied; completion is a separate upward call with its own target and trace context,
also acknowledged Applied or AlreadyApplied. Status refusals are typed; an unmatched check-in is
`RejectedUnmatchedCompletion` naming the field, the value held and the value reported. No missing reply is a completed check-in.

These calls are forwardable and served while connections-deleting under Status's rules. Carried service
emits `rdm.node_rpc.request.serve.via-carried-inner`. Upward completion preserves the round's
trace id, with a new caller span. Subject action/checklist evidence is required in addition to
generic RPC success.

Command and completion hooks use the [round gossip frames](gossip.md#fabric-round-gossip-hooks).
Who declares the fabric state is [States](lifecycles/states.md#who-sets-each-state); Rafka
receives one outer traffic-opened integration notice after the completed rounds.

### Application state-sync handoff

SyncState and StateSynced name the external RDM/Rafka integration messages, with the
round fields `fabric_id: FabricId`, `build_id: String`, `attempt: u32`,
`operation: String` (`state-sync:<fabric_id>`) and observability `context: CallContext`.
SyncState hands state-sync to the application; StateSynced reports its requested work
complete to the fabric-primary. It triggers state-commit, not ready-for-traffic.
The integration transport has no Node RPC opcode or target defined here; an application
message must not be presented as an implemented Node RPC variant. RDM owns no day-0,
seed or application writer logic. Integration message examples live in
[Fabric state-sync](lifecycles/fabric-state-sync.md).

### Node stop command pair

The caller contract is [node.stop / node.delete](#typed-object-contract). The mesh-primary's
exact-node stop and start hops use [NodeCommand 0x22](#nodecommand-op-0x22-rdm); caller routing and
the Build carrier are unchanged. StatusRequest positions 8 `StopNode` and 16 `StartNode` are reserved:
a receiver refuses each by name and executes nothing. Reply positions 17 `Stopped`, 18 `Started`
and 19 `StartFailed` stand. Reserved6, Reserved7 and NodeLeft retain positions 6, 7 and 9 and are
refused by name; Reserved2 retains StatusReply position 2. No position is reordered or reused.

Stop's whole `node.connections.delete { scope: All }` reports its `ConnectionsReceipt` in
`StopConnectionsDeleted` (Data), preserving `Established` or `Deadline` evidence; `NodeStopped`
ends the stream after the transport cut. The operation-named owed Stopped declaration remains
(Q36); frame and declaration apply once through the operation's held row (Q49).

### Mesh delete command pair

The caller contract is [mesh.delete](#typed-object-contract); the sequence is
[mesh.delete](lifecycles/mesh-delete.md). The command and the mesh-primary's report are the Status
bodies DeleteMesh and MeshDeleted on op `0x1B`,
[StatusRequest](#status-op-0x1b-rdm) positions 10 and 11.

DeleteMesh is downward, called by the fabric-primary and served by the mesh's primary; its
admission starts mesh.deleting. It is sent only for an admitted mesh.delete Build: until the
initial fabric has first reached ready-for-traffic that Build is refused `fabric-not-ready` at
build.create ([Build refusals](#fabric-not-ready-refusal)) and no DeleteMesh is sent.
MeshDeleted is upward, called by that still-running mesh-primary and served by
the fabric-primary: it carries mesh.deleted, with `receipt_manifest` referencing every other
member's exact Exited receipt and `final_*` naming the mesh-primary's own running birth. Both
use the standard target, CallContext and authenticated PeerContext.

The receipt_manifest is a bounded reference (at most 128 bytes) to immutable terminal evidence
in the accepted Build's durable operation records, scoped to
`(mesh_id, build_id, attempt, operation)`. It is not arbitrary caller-supplied storage to
trust. The fabric-primary must read and validate those records before it accepts the handoff;
a reference alone is not terminal proof. No storage API name is fixed here. RuntimeFact uses
RDM's exact deployment/provider/control-domain/locator identity, defined in
`crates/rdm-core/src/runtime.rs::RuntimeFact`.

The referenced manifest records the accepted roster of exact births, the final primary
that remains running, and a provider terminal receipt for every other roster entry.
Each receipt binds node_id, incarnation, immutable RuntimeFact and the provider's exact
Exited inspection result. A recycled pid or reusable container name cannot satisfy it.
The manifest must cover the accepted roster exactly, plus every roster amendment the
fabric-primary recorded ([Mesh delete, resume after the primary's loss](lifecycles/mesh-delete.md#resume-after-the-primarys-loss)):
no missing birth, extra birth or substitution. The fabric-primary validates the final
primary against the birth it admitted, or the birth its last `RosterAmended` receipt names
as that primary's replacement, and against its provider domain, not merely its path or
current primary's name. A manifest that names the admitted primary after an amendment, or omits
the amendment, is refused by name.

The workflow identity is `(mesh_id, build_id, attempt, operation)`; the operation is
`mesh.delete:<mesh_id>`. DeleteMesh answers Applied or AlreadyApplied on admission; that reply is
not mesh.deleted. The fabric-primary answers MeshDeleted Applied or AlreadyApplied after handoff
validation. A stale/conflicting round or incomplete manifest is refused by name with a typed Status
refusal naming the mismatch; not-primary, stale-mesh, malformed and other Status refusals remain typed. Both
calls are forwardable under Status rules. A lost handoff or missing exact receipt leaves the
workflow incomplete, never successful.

The mesh-primary commits the mesh row deleted before it calls MeshDeleted. A deleted mesh is not proof of the
mesh-primary's exit; a separate node.delete, with the fabric-primary as executor, proves it.

## BuildClaim, op `0x1C`, rdm

Contract source: RDM `crates/rdm-core/src/build_claim.rs::BuildClaimRequest`.

Direction: upward, to the fabric-primary, which writes every Build record: a mesh-primary claims its mesh's drift attempt through it, and the fabric-primary claims a topology Build's attempt itself. The claim is one record at `(build_id, mesh, attempt)` in the Build journal, written once by the fabric-primary; the fabric-primary hands a topology attempt's Won claim to the executor `executor_for` names through [build.attempt.run](#build-op-0x20-rdm), and a mesh-primary runs its drift attempt itself, reporting its receipts up the attempt's stream. Request ceiling 512 bytes; reply ceiling 9216 bytes. Not forwardable. Lost, NotOpen, NotClaimant and BuildInProgress are typed refusals; runtime refusals are enumerated below. The returned CallContext is attempt observability context, not a Build fact. Caller/server emit the runtime spans above with op=0x1C; the claim decision emits `rdm.node_admin.build.update.via-claim-decision`.

```rust
pub enum BuildClaimRequest {
    ClaimAttempt { build_id: String, attempt: u32, executor_node_id: NodeId, executor_incarnation: IncarnationId, mesh: Option<String> },
}

pub enum BuildClaimReply {
    Won { context: CallContext },
    Lost { claimant: String },
    NotOpen { next: Option<u32> },
    NotClaimant { mesh: Option<String>, fabric_primary: Option<String>, mesh_primary: Option<String> },
    BuildInProgress { current_build_id: String, mesh: String, held_by: Option<String> },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}
```

## Join, op `0x1D`, rdm

Direction: upward. An ordinary node calls its mesh-primary node-admin; a node-admin birth
calls its maker, the node-admin that deploys it. JoinNode is the managed birth's FIRST call after it
binds port 0 and the OS assigns its port. The deploying primary serves it. Not forwardable: the report is attributed directly to the
joining birth.

```rust
pub enum JoinRequest {
    JoinNode { digest: MeshDigest, types: Vec<String> },
}
```

`types` is the birth's declared app types ([gossip.md §8](gossip.md#8-entity-projection-gossip-remains-projection-not-source-of-truth)),
at most 256 and 8 KiB encoded; over the bound the join is `Malformed { kind: TypesOverCap }` and
the birth is never a member. The serving node-admin records the declaration beside the row, joins
every type it does not already hold before it answers, and writes its own row's `held_hash`.

The reply alternatives and their complete named fields are:

```text
JoinReply =
    Joined { answer: Vec<u8> }
  | JoinMismatch { field: String, deployed: String, reported: String }
  | NotPrimary { primary: Option<String> }
  | PeerUnresolved { reason: String }
  | NotReady { reason: String }
  | Busy { reason: String }
  | ConnectionsDeleting { reason: String }
  | Malformed { kind: MalformedKind }
  | Unauthorized { reason: String }
  | DeploymentAbandoned { build_id: String, attempt: u32, node_id: NodeId, incarnation: IncarnationId }
  | CertRefused { name: String, detail: String }
```

Joined.answer is postcard-encoded WireJoinAnswer from RDM node-admin-core wire.rs.
Its decoded JoinAnswer contains served_by, control and statuses. Control contains
provider, optional Fabric and shutdown facts, optional Build floor, rafka-time, the
member cert, and, for a birth the primary already holds, the row's name and config; statuses
retain their original authors and change instants. The request uses WireDigest, the
shared positional digest shape. Request ceiling is 64 KiB; reply ceiling is 4 MiB.
There are eleven replies in the shown order. DeploymentAbandoned identifies an exact
birth whose deployment ended before its join arrived; this is not NotPrimary.
JoinMismatch names the differing field and its deployed and reported values;
NotPrimary names the required primary. These are typed replies, not transport faults.
JoinNode is only a join. It does not re-read topology for an already-held member.
After acceptance and its `before_ready` hook, the caller reads topology with [GetTopology](#topology-op-0x1e-rdm), in the order [node.start](lifecycles/node-start.md) sets.
A fabric-primary recovered with durable topology follows [fabric recovery](lifecycles/fabric-recovery.md#fabric-recovery) without maker calls; its first topology read targets a node from that map.

MeshDigest is the full gossip Digest, not an address-only report. Its MeshNode carries the
real bound `transport_addr`, `endpoint_id`, exact `node_id` and `incarnation` together with
its path and runtime fact. Its complete fields are the shared membership types below.


```rust
pub struct MeshNode {
    pub node_id: NodeId,
    pub name: PathName,
    pub endpoint_id: EndpointId,
    pub transport_addr: std::net::SocketAddr,
    pub incarnation: IncarnationId,
    pub supersedes: Option<IncarnationId>,
    pub runtime: Option<RuntimeFact>,
}

pub enum MemberStatus {
    Pending,
    ReadyForTraffic,
    ConnectionsDeleting,
    Stopped,
}

pub struct NodeLoad {
    pub cpu_used_millicores: u32,
    pub cpu_budget_millicores: u32,
    pub ram_used_bytes: u64,
    pub ram_budget_bytes: u64,
}

pub struct GossipStats {
    pub heard: u32,
    pub neighbours: u32,
    pub frames_sent: u64,
    pub frames_received: u64,
}

pub struct MeshDigest {
    pub fabric_id: FabricId,
    pub node: MeshNode,
    pub status: MemberStatus,
    pub admin_api_base: Option<String>,
    pub digest_seq: u64,
    pub emitted_at_rafka_ms: u64,
    pub data_dir: Option<String>,
    pub mesh_id: Option<MeshId>,
    pub in_flight: Option<u64>,
    pub config: BTreeMap<String, String>,
    pub load: Option<NodeLoad>,
    pub gossip: Option<GossipStats>,
}
```

The admin verifies node_id, incarnation, deployment id / RuntimeFact, path and mesh against
what it deployed. A mismatch is refused by field name. For a birth the primary already
holds, the name must be the row's name or the name it last accepted for that exact birth, else
JoinMismatch names the reported, the row's and the last accepted value; the accepted digest is
installed under the row's name. A first start's name is checked against its create spec. On acceptance it installs the reported
address for that exact key, drops pooled connections and pending work aimed at that key's old
address, and completes the birth's WaitForBind / WaitForMeshJoin from the report. The peer's
source socket is not the advertised address. The node serves calls on its bound endpoint
while JoinNode is outstanding, subject to the [Node RPC readiness gate](#node-rpc-readiness-gate). Joined supplies control and statuses; GetTopology supplies
the topology used to join the mesh channel.

Caller: `rdm.node_rpc.request.update.via-call` with op=0x1D. Server:
`rdm.node_rpc.request.serve.via-direct`, followed by `rdm.node_admin.node.update.via-join`.
A mismatch emits `rdm.node_admin.node.reject.via-join-mismatch`. The join call and serve
have the runtime causal edge; handler evidence identifies the exact joining birth.

## Topology, op `0x1E`, rdm

Contract source: `crates/rdm-core/src/topology.rs::StoredNode`,
`crates/rdm-core/src/topology.rs::TopologyRequest` (request),
`crates/rdm-core/src/topology.rs::TopologyReply` (reply), and
`crates/rdm-core/src/topology.rs::Topology` (reply count).
Direction: peer read, any node → any node. Every node serves it. Read-only: the target
changes no state because of the read. Not forwardable: this is a server-streaming entry.
The request ceiling is 4 KiB and reply ceiling is 8 KiB. Meshes are named by String on the
wire; an optional stored MeshId is a separate identity. PublisherId is the publisher's
exact birth and scopes the source-version revision as in [gossip](gossip.md#31-frame-catalog).

```rust
pub struct StoredNode {
    pub node_id: NodeId,
    pub name: String,
    pub endpoint_id: EndpointId,
    pub incarnation: IncarnationId,
    pub transport_addr: std::net::SocketAddr,
}

pub struct SourceVersion {
    pub publisher: PublisherId,
    pub topology_version: u64,
}

pub enum TopologyRequest {
    GetTopology { mesh: Option<String>, since: Option<SourceVersion> },
}

pub enum TopologyReply {
    Snapshot {
        mesh: String,
        publisher: PublisherId,
        topology_version: u64,
        snapshot_id: u64,
        chunk_index: u32,
        chunk_count: u32,
        digests: Vec<WireDigest>,
        in_flight: Vec<LifecycleOp>,
        departed: Vec<LifecycleOp>,
    },
    Unchanged { mesh: String, publisher: PublisherId, topology_version: u64 },
    End { meshes: u32 },
    NotReady { reason: String },
    UnknownMesh { mesh: String },
    PeerUnresolved { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    Started,
    Stored { mesh: String, mesh_id: Option<MeshId>, nodes: Vec<StoredNode> },
    Primaries { kind: PrimaryKind, primary: Primary, gone: bool },
    RafkaTime { ms: u64, kind: Option<PrimaryKind> },
}

// A primary as the topology answers it (the election's result the mesh-primary
// recorded as is_primary / is_fabric_primary), distinct from the per-type entry of op 0x21.
pub enum PrimaryKind {
    MeshPrimary,
    FabricPrimary,
}

pub struct Primary {
    pub mesh: String,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
    pub revision: u64,
}
```

`mesh=None` asks for every held or eligible stored mesh; Some(m) asks for one mesh.
A matching held source version answers Unchanged. Accepted reads stream Started, then
Snapshot, Unchanged or Stored frames, then End. Snapshot uses gossip Members' shape and
chunking; a mesh is installed atomically only after every chunk of the same snapshot_id,
publisher and topology_version is present. End.meshes counts each answered mesh once.
A mesh with no published snapshot is absent unless an eligible stored map supplies it.
NotReady means the RPC door is unopened, or reading the stored map failed, with the cause
named. A mesh neither held nor stored answers UnknownMesh.

Stored is reply position 11; REPLY_VARIANTS is 14. Stored is data-class
and carries up to 24 nodes per frame; End.meshes counts a stored mesh once across frames.
It serves only a mesh the maker holds no snapshot of, never the maker's own mesh. A held
snapshot wins. The reader never installs Stored as topology: it uses the map only to reach
a live node of its own mesh. A read from a node of another mesh installs topology only for
other meshes and holds none of the reader's own mesh as heard.
Serving: `crates/rdm-node/src/topology_read.rs::StoredMap` and
`crates/rdm-node/src/topology_read.rs::TopologyDoor`.

Caller span: `rdm.node_rpc.request.update.via-call`, op=0x1E. Server:
`rdm.node_rpc.request.serve.via-direct`, child `rdm.mesh.topology.serve.via-read`, with
`meshes`, `snapshots`, `unchanged`, `stored`, `bytes`. Caller installation emits
`rdm.mesh.topology.update.via-read-install` per mesh, with publisher, topology_version
and target node. Complete state installation is proof in addition to trace ancestry.

Uses: [node-admin create](lifecycles/node-create.md#ordinary-process-birth),
[delta-gap top-up](lifecycles/node-topology-get.md#node-topology-top-up),
[a node-admin joining a live mesh](lifecycles/mesh-investigation.md#a-node-admin-joins-the-live-mesh)
and [fabric recovery](lifecycles/fabric-recovery.md#fabric-recovery).

## Build-facts, op `0x1F`, rdm

Contract source: `crates/rdm-core/src/build_facts.rs::BuildFactsRequest`
and `crates/rdm-core/src/build_facts.rs::BuildFactsReply`;
ledger: `crates/rdm-core/src/catalog.rs::core_ledger`.
Direction: peer read between node-admins. RDM owns this server stream; it is not
forwardable. A non-node-admin answers 421 UNSERVED_OP. Request ceiling 1024 bytes;
reply ceiling 8 KiB. There is no cursor. REQUEST_VARIANTS is 2; REPLY_VARIANTS is 11. Positions are fixed in the shown order (FetchBuildFacts=0, FetchCurrentBuild=1; Facts=0 through NoCurrentBuild=10), the two last appended, none reordered.

```rust
pub enum BuildFactsRequest {
    FetchBuildFacts { build_id: String },
    FetchCurrentBuild,
}

pub enum BuildFactsReply {
    Facts { build_id: String, chunk_index: u32, chunk_count: u32, facts: Vec<u8> },
    End { build_id: String, facts: u32, chunks: u32, complete: bool },
    NotReady { reason: String },
    UnknownBuild { build_id: String },
    PeerUnresolved { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    Started,
    NoCurrentBuild { reason: String },
}
```

The stream is Started, Facts*, End. Each Facts chunk is one Build-channel frame, at most
4032 bytes. The caller accepts it only when End arrives, every chunk index in 0..chunks
is present exactly once, and the fact count equals End.facts. complete=true only when
the responder holds the Build fully. End.complete must be true for the Build to count as fetched. Missing, duplicated or incomplete evidence never
counts. Facts are absorbed through the existing Build log.

The [node start workflow](lifecycles/node-start.md) drives the read.
The Ready check drives reads through typed blockers: no-pointer-wanted, no-pointer,
unreadable-build, behind-floor. no-pointer, which names no Build, reads `FetchCurrentBuild`: the responder selects its current accepted Build once for the request and streams that Build's facts, every chunk and End naming its id, or answers `NoCurrentBuild` and the blocker stays; every other blocker reads `FetchBuildFacts` of the Build it names. No blocker is read by a second JoinNode. It asks the fabric-primary first, then other node-admins,
then the launcher. Read-only recovery is authorized and uses bounded backoff, 250 ms
doubling to 4 s. Source: `crates/rdm-admin/src/fetch_build.rs::NodeAdminNotReady`,
`crates/rdm-admin/src/fetch_build.rs::BACKOFF_BASE` and
`crates/rdm-admin/src/fetch_build.rs::BACKOFF_CAP`.
Responder spans: `rdm.node_admin.build.serve.via-fetch-facts`,
`rdm.node_admin.build.reject.via-unservable-fact`. Requester spans:
`rdm.node_admin.build.update.via-fetch-facts`, `rdm.node_admin.build.reject.via-fetch-incomplete`,
`rdm.node_admin.build.reject.via-fetch-failed`.
Generic RPC call/serve spans carry op=0x1F. Typed refusals are the reply variants above.

## Build, op `0x20`, rdm

Contract source: `crates/rdm-core/src/build.rs::BuildRequest`.

The `build` family is RDM-owned, live and canonical. RDM accepts, drives, reads and
deletes Builds only through Node RPC. An older node answers `UNSERVED_OP` by name for
`0x20`. Request ceiling 256 KiB; reply ceiling 64 KiB. REQUEST_VARIANTS is 4;
REPLY_VARIANTS is 25. The [schema](#build-schema) gives both enums.

### Calls and direction

| Call | Direction / caller → server | Request content | Reply / stream |
|---|---|---|---|
| build.create | upward, caller → fabric-primary | submitted Build intent or node.create/stop/start/restart/delete workflow; a preset is a Build input; existing build_id for resubmission | Started { build_id }, step frames, Complete or Failed { step, reason }; a fabric-primary change ends with FabricPrimaryMoved, the typed frame naming the new one |
| build.attempt.run | downward, fabric-primary → executor named by executor_for | (build_id, attempt) and the Won claim | replay held Complete receipts, current Blocked if still blocked, then live step frames; a run that stops at another executor's operation ends HandedOff { to }; executor loss ends Indeterminate |
| build.get | caller → fabric-primary | build_id | read accepted Build and receipts for reattachment after a cut; never progress polling |
| build.delete | caller → fabric-primary | build_id | named deletion result or refusal; the current Build ([build.md §4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build)) cannot be deleted |

The fabric-primary accepts **every** Build and drives every topology Build: it claims each topology attempt through its claim door, then calls the executor named by
`executor_for`. A drift attempt of the current Build inside a mesh (a node recovery) is run by that mesh's primary, which claims it through the fabric-primary ([BuildClaim](#buildclaim-op-0x1c-rdm)) and reports its receipts up the attempt's stream; the fabric-primary writes every Build record. The executor validates and adopts the won claim through
`adopt_claim`; it does not claim or pick up work autonomously from a projection, and it persists nothing of the Build.
A call covers one contiguous run of operations with the same executor, in plan
order. Mesh creation and deletion, plus members of any mesh with no mesh-primary,
run in process at the fabric-primary and feed the same outer stream
(`crates/rdm-admin/src/executor.rs::executor_for`). Hand-off is the fabric-primary's next claim and call.

### Frames, durable receipts and reattachment

Every progress frame (Step, Blocked) carries the receipt key
`(build_id, attempt, operation, step)` and receivers drop duplicates by that key. A caller
watching a Build whose connection closes (`Budget::Stream`'s one end) reattaches by `build_id`:
`build.get` for the receipts, then the outer stream again, resuming from the receipts; the break is
never a defect and never a new execution (Node RPC §34, §43).

| Frame / outcome | Meaning |
|---|---|
| Started { build_id, attempt, disposition } | accepted Build identity on the outer stream |
| Step, the durable step-outcome frame | on the inner attempt stream, the executor reporting the step's result upstream to the fabric-primary; on the outer stream, the fabric-primary publishing it downstream only after it has recorded the receipt |
| Complete | terminal success; completed step receipts are read and replayed, never executed again |
| Failed { step, reason } | terminal named failure; nothing follows it on that stream |
| Blocked { step, reason } | nonterminal hook blocker, emitted on a new blocker or reason change, never on a timer; replay the current blocker on reattach if still blocked. Its only source is the subject's [Status `Blocked`](#status-op-0x1b-rdm) reply to the executor's ProbeNodeState |
| AlreadyApplied with terminal frame | resubmission of a completed Build |
| HandedOff { to } | terminal of build.attempt.run: the run ended at an operation another executor runs; `to` names the fabric-primary of the view the run ends with, and that fabric-primary's next claim picks the executor of the remaining operations |
| FabricPrimaryMoved { build_id, fabric_primary }, the typed frame naming the new fabric-primary | deliberate outer-stream end naming the new fabric-primary; caller resubmits the same build_id there |
| NotSent / Indeterminate | transport cut outcome under the request commit cut; executor loss ends the inner call Indeterminate |

Blocked is live progress, **not a durable step outcome**. Durable step outcomes
remain Complete and Failed. A stream cut cancels neither the fabric-primary's
Build drive nor the executor's inner run. Only a fabric-primary change or a failed step ends an
attempt. Progress waits on no gossip; Build-channel facts remain durability and
recovery evidence through the existing publication path.

Resubmission by build_id never creates a second Build. The fabric-primary answers:
completed → AlreadyApplied with terminal frame; running with a live executor → that
executor's stream through build.attempt.run reattachment; failed, or node investigation's
verdict on the executor anything but reached → the next attempt. A successor uses the Build projection as its recovery
source. If the fabric-primary dies after claiming but before dispatch, its
successor dispatches the still-current claimed work; a live executor is reattached,
not replaced merely because the fabric-primary changed. It plans from accepted intent and observed topology and never reruns a
caller-visible step with a Complete receipt. Node investigation's verdict on the executor, never
silence alone and never a proof of its exit, opens the next attempt for its successor.

### Refusals, fencing and bounds

build.attempt.run refuses by name when the recipient is not the executor named in
the claim (NotExecutor { named, recipient }) or the claim is not the current folded
attempt (StaleClaim { held_attempt, held_executor, carried_attempt }). A replaced mesh-primary
gets a fresh claim. The canonical Node RPC target remains node id + op;
claim validation is the Build-specific execution check.

<a id="fabric-not-ready-refusal"></a>
build.create refuses by name until the initial fabric has first reached ready-for-traffic: the
existing `Rejected { reason, detail }` reply with reason `fabric-not-ready`, whose `detail` names
the refused workflow and the fabric's current status. It refuses a Build that would move a
fabric-primary or mesh-primary, mesh.delete, and every node.stop, node.start, node.restart,
node.delete and replace recipe a caller submits. The initial accepted Build, which creates the
fabric's nodes and brings the fabric to ready-for-traffic, runs as usual, and once the fabric is
ready-for-traffic these are admitted as usual. A refused Build claims no attempt and sends no call.
[States](lifecycles/states.md#fabric-not-ready) gives the reason. `NotReady` is not this refusal:
it stays the serving door's answer.

Every dispatch passes the fabric-primary check and the claim decision. A
node-admin that is fabric-primary no more cannot dispatch and refuses NotFabricPrimary { node, by }.
A fabric-primary change ends the outer
stream with FabricPrimaryMoved, **not Indeterminate**. An accidental transport cut remains
NotSent or Indeterminate under the existing request commit cut.

There is **no overall deadline** on the outer or inner stream. Stream bounds
are idle = staleness floor and keepalive = gossip / 2. The family is not
forwardable: every call is a stream.

Caller and server emit the [runtime spans](#runtime-spans) with op=0x20; child calls continue
the outer trace under the same rule. A dispatch refused after the fabric-primary check records NotFabricPrimary in
its span.

Mechanism:
`crates/rdm-admin/src/build_claim.rs::ClaimDoor` (claim door),
`crates/rdm-admin/src/executor.rs::executor_for` (executor_for),
`crates/rdm-admin/src/build_state.rs::BuildStateAdapter` (adopt_claim),
`crates/rdm-admin/src/build_state.rs::BuildAttemptClaim` (claim row),
`crates/rdm-admin/src/build_state.rs::BuildStepReceipt` (step receipt) and
`crates/rdm-admin/src/build_state.rs::StepOutcome` (Complete/Failed outcomes).

build.attempt.run carries the stored BuildAttemptClaim { build_id, attempt,
executor } plus the Won reply's context; its whole body is `AttemptRun` below.

### Build schema

```rust
pub struct MeshCounts {
    pub name: String,
    pub node_admin: u32,
    pub rpc_node: u32,
    pub broker: u32,
    pub gateway: u32,
    pub compute: u32,
}

pub enum BuildChange {
    ReconcileFabric { fabric: String, meshes: Vec<MeshCounts> },
    ReconcileMesh { mesh: MeshCounts },
    AddNode { mesh: String, node_kind: NodeKind },
    RemoveNode { node: PathName },
    CreateMesh { mesh: MeshCounts },
    RemoveMesh { mesh: String },
    Restart { node: PathName },
    // RecipeStep = Create | Stop | Update | Start | Delete: one node primitive per step.
    // A recipe is one Build of node primitives (lifecycles/node-replace.md).
    Recipe { steps: Vec<RecipeStep> },
    ConnectionsDelete { node: PathName },
    Stop { node: PathName },
    Start { node: PathName },
    Recover { node: PathName, node_id: NodeId, incarnation: IncarnationId },
}

pub enum BuildSubmit {
    Change(BuildChange),
    Resubmit { build_id: String, from_attempt: u32 },
}

pub enum BuildRequest {
    Create { submit: BuildSubmit },
    AttemptRun {
        build_id: String,
        attempt: u32,
        executor: String,
        context: CallContext,
        intent: Vec<Vec<u8>>,
        operations: Vec<Vec<u8>>,
        hand_off_to: Option<String>,
    },
    Get { build_id: String },
    Delete { build_id: String },
}

pub enum Disposition {
    Created,
    Attached,
    NextAttempt,
    AlreadyApplied,
    Started,
    Reattached,
}

pub enum StepResult {
    Complete,
    Failed { reason: String, code: Code },
}

pub struct StepReceipt {
    pub attempt: u32,
    /// A Build step key: an opaque string, op then colon then target, as in
    /// `node.create:mesh1.rpc.1`. The dots before the colon do not collide with the dotted
    /// path.name after it.
    pub operation: String,
    pub step: String,
    pub result: StepResult,
}

pub enum BuildPhase {
    Pending,
    Running,
    Complete,
    Failed,
}

pub enum BuildReply {
    Started { build_id: String, attempt: u32, disposition: Disposition },
    Step { build_id: String, attempt: u32, operation: String, step: String },
    Blocked { build_id: String, attempt: u32, operation: String, step: String, reason: String },
    Steps { build_id: String, chunk_index: u32, chunk_count: u32, steps: Vec<StepReceipt> },
    Complete { build_id: String, attempt: u32 },
    Failed { build_id: String, attempt: u32, operation: String, step: String, reason: String, code: Code },
    HandedOff { build_id: String, attempt: u32, to: String },
    Got {
        build_id: String,
        phase: BuildPhase,
        attempt: u32,
        executor: Option<String>,
        reason: String,
        last_failure: Option<String>,
        steps: u32,
        chunks: u32,
    },
    Deleted { build_id: String },
    NotFabricPrimary { fabric_primary: Option<String> },
    NotExecutor { named: String, recipient: String },
    StaleClaim { held_attempt: u32, held_executor: Option<String>, carried_attempt: u32 },
    UnknownBuild { build_id: String },
    Rejected { reason: String, detail: String },
    BuildInProgress { current_build_id: String, mesh: Option<String>, held_by: Option<String> },
    AttemptTaken { detail: String },
    NotFabricPrimary { node: String, by: String },
    CannotDelete { build_id: String, reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    FabricPrimaryMoved { build_id: String, fabric_primary: Option<String> },
}
```

A stream is one refusal, or Started, then Step, Blocked or Steps frames, then one
terminal: Complete, Failed or FabricPrimaryMoved for Create; Complete, Failed, HandedOff or
FabricPrimaryMoved for AttemptRun; Got for Get; Deleted for Delete.

<a id="primaryupdate-op-0x21-rdm"></a>

## PrimaryUpdate, op `0x21`, rdm

RDM owns this family. It carries [node.primary.update and node.fabric-primary.update](lifecycles/node-primary-update.md),
which owns sequencing and the failure contract. Direction: the primary calls its successor's exact birth;
only node-admins and the nodes of a type the mesh-primary commands serve it. Every call is unary and not forwardable. Requests use the canonical target and
CallContext; replies use the canonical unary reply frame. Field order and enum positions are the order
shown. Births are exact identities, never names.

```text
primary → successor   PrimaryUpdate { primaries: [(node_type, primary: { node_id, incarnation }, successor: { node_id, incarnation }, revision)], build_id, run, operation }
successor             becomes primary: installs the election's result in its topology view and announces it in its digest; the mesh-primary records it afterwards
successor → primary   PrimaryUpdated { revisions }   (on that call's reply stream)
```

`primaries` carries the election's result the successor installs, one entry or two: the fabric-primary
is a second entry, node type `fabric`. For the `node-admin` type the successor records its own row on
this call, after it answered; for every other type the mesh-primary records the successor's row from
the successor's digest, the way it records every member fact it hears. The receiver refuses only:

- RejectedConflictingPayload: the same operation with different content; the row decides;
- `fabric-not-ready`: before the initial fabric first reached ready-for-traffic.

There is no RejectedWrongBirth (R-UC1a, R-RX5: a receiver never refuses a call for the node id it
names), no RejectedStaleRevision and no RejectedNotEligible on the receiver: eligibility is the writer's
pre-check before the row.

`operation` identifies one update
by (build_id, run): `node.primary.update:<build_id>:<run>:<op>`. The run is the first attempt of the series a
submission opened: a resume continues it and replays that run's row; a resubmit after Failed opens a new run,
so a new transfer.

The commanded arm rides the same op: the fabric-primary commands a mesh-primary's update with
`UpdatePrimary { node_type, build_id, run, operation }` on the primary, and the mesh-primary commands another type's
primary the same way. The primary runs the sequence and answers `PrimaryUpdated { revisions }` when it completes,
or `Failed { step, reason, code }`, the Build carrier's terminal (R-BI4), when it ends failed; `Incomplete` is never
used for a step that ended. The commander fails the step on that variant, retaining the code and never selecting it from the reason's text, and
claims the attempt at the claim door before it commands.

There is no take-over op. A fabric-primary unheard past the staleness floor is [fabric recovery](lifecycles/fabric-recovery.md)'s: the node-admin at its path.name is re-created and is mesh-primary and fabric-primary again when ReadyForTraffic; nobody takes the role meanwhile (Q122).

Lost replies: the successor keeps serving PrimaryUpdate and Status after the row. An identical retry
answers the same PrimaryUpdated before any revision check. A different payload for the same operation is
RejectedConflictingPayload. The primary never treats NotSent or Indeterminate as an answer: it retries the
identical call, and the election resolves uncertainty.

The primary's full digest
carries `admin_api_base`, the application contact URL including its bound port. Consumers
resolve the successor's application contact from that exact birth's contact record;
The primary's digest carries its contact addresses.
The two contact fields are explicit: `transport_addr` is the bound QUIC UDP ip:port;
`admin_api_base` is an application-supplied contact URL with its bound port.
RDM serves Node RPC; the application contact is independent of that endpoint; consumers cannot derive either port from the other. Contact changes
are learned from the exact birth's digest.

Spans: the caller emits `rdm.node_rpc.request.update.via-call` with op=0x21; the successor's install and
every refusal carry the operation, build_id, attempt, both exact births and each entry's revision. A change
to these frames is `durable format moved`, with byte fixtures.

## NodeCommand, op `0x22`, rdm

RDM owns this family. It carries the mesh-primary → exact-node hop of
[node.connections.delete](lifecycles/node-connections-delete.md), [node.stop](lifecycles/node-stop.md)
and [node.start](lifecycles/node-start.md). The caller's hop and Build carrier are unchanged.
Every call is server-streaming, never forwarded, and served while connections-deleting.
Births are exact identities, never names. Every earlier byte and enum position stands; requests
`StopNode` and `StartNode` append at 1 and 2. Replies append after the existing positions 0–10.

```text
DeleteConnections { node_id, incarnation, scope }
  ← CommandAccepted { node_id, incarnation } (Started)
  ← ConnectionsDeleting { node_id, incarnation } (Data; All only)
  ← ConnectionsDeleted { closed, not_open, held, receipt } (Terminal)
StopNode { node_id, incarnation, build_id, attempt, operation }
  ← CommandAccepted { node_id, incarnation } (Started)
  ← StopConnectionsDeleted { receipt } (Data; appended reply 11)
  ← NodeStopped (Terminal; appended reply 12)
StartNode { node_id, incarnation, build_id, attempt, operation }
  ← CommandAccepted { node_id, incarnation } (Started)
  ← NodeJoined (Data; appended reply 13)
  ← NodeStarted (Terminal; appended reply 14)
after StopNode or StartNode acceptance:
  ← Failed { step, reason, code } (Terminal; appended reply 15)
before lifecycle acceptance:
  ← RejectedInvalidNodeTransition { current } (Refusal; appended reply 16, FAILED_PRECONDITION)
```

A refusal before acceptance is one refusal frame. `CommandAccepted` is admission and carries no
lifecycle state: on a stop it makes the mesh-primary record stopping. `ConnectionsDeleting`
records connections-deleting for connections.delete; `NodeStopped` records stopped, `NodeJoined`
records pending, and `NodeStarted` records ReadyForTraffic after every start gate and before the
outer Build's Complete. Starting stays the executor's overlay. Nothing is written
before CommandAccepted or for NotSent. There is no separate status declaration for an open start;
the Q36 owed report remains when no stream is open. A stream lost after acceptance is incomplete.
The owner records each frame before acknowledging recovery; repeated frames cause no second record.

## Transitional codec conventions

A transitional catalog entry uses its compatibility codec; being live in the ledger does not
turn a struct, raw snapshot or fire-and-forget message into a postcard request/reply enum.
The exact bodies and typed replies of Rafka's adapters are rafka-v2's
[`node-rpc-rafka-ops.md`](/home/admin/rafka-v2/docs/architecture/node-rpc-rafka-ops.md).
A compatibility rail without a typed reply enum has none. Numeric domain fields named fabric_id do not key transport;
transport uses endpoint_id.

## The rafka op families, `0x10` to `0x19`

rafka's own families on this envelope (legacy-control `0x10`, data-frame `0x12`, snapshot `0x13`, control `0x14`,
credential-resolve `0x15`, forward-write `0x17`, forward-read `0x18`, peer-tickle `0x19`) are rafka's product
ops: their schemas, directions and reply enums are in rafka-v2
[`docs/architecture/node-rpc-rafka-ops.md`](/home/admin/rafka-v2/docs/architecture/node-rpc-rafka-ops.md).
They ride the same target, context and body, the same ledger and the same runtime rules as every op here;
nothing in this repository serves them.

## Proof-store, op `0x70`, testkit

Contract source: RDM `crates/rdm-test/src/proof_store.rs::ProofRequest`.

Direction: peer. Test clients call a testkit node that serves its durable proof store. Forwardable. Mismatch, TooLarge and StoreFailed are typed refusals; runtime refusals are in ProofReply. Caller/server emit the canonical runtime spans above. These families are served only by testkit binaries, never product binaries.

```rust
pub enum ProofRequest {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    CompareAndSwap { key: Vec<u8>, expected: Option<Vec<u8>>, new: Option<Vec<u8>> },
}

pub enum ProofOp {
    Get,
    Put,
    Delete,
    CompareAndSwap,
}

pub struct Provenance {
    pub node_id: String,
    pub node: String,
    pub mesh: String,
    pub incarnation_id: String,
    pub op: ProofOp,
}

pub enum ProofReply {
    Value { at: Provenance, value: Vec<u8> },
    Absent { at: Provenance },
    Stored { at: Provenance },
    Deleted { at: Provenance },
    Swapped { at: Provenance },
    Mismatch { at: Provenance, current: Option<Vec<u8>> },
    TooLarge { at: Provenance, field: String, limit: u32, got: u32 },
    StoreFailed { at: Provenance, reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}
```

## Resolve-probe, op `0x71`, testkit

Contract source: RDM `crates/rdm-test/src/resolve_probe.rs::ResolveRequest`.

Direction: peer. Test clients call the testkit node whose held resolver they inspect. Forwardable. BadTarget is a typed refusal; Gone, Unknown and Unavailable are resolver results. Server additionally emits `rdm.node_rpc.resolve_probe.serve.via-request`. Caller/server emit the canonical runtime spans above. These families are served only by testkit binaries, never product binaries.

```rust
pub enum ProbeTarget {
    Exact(String),
    Path(String),
}

pub enum ResolveRequest {
    Resolve { target: ProbeTarget },
}

pub struct AnsweredBy {
    pub node_id: String,
    pub node: String,
    pub incarnation_id: String,
}

pub enum ResolveReply {
    Found { by: AnsweredBy, node_id: String, name: String, incarnation_id: String },
    Gone { by: AnsweredBy },
    Unknown { by: AnsweredBy },
    Unavailable { by: AnsweredBy },
    BadTarget { by: AnsweredBy, reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}
```

## Declare-probe, op `0x72`, testkit

Contract source: RDM `crates/rdm-test/src/declare_probe.rs::DeclareRequest`.

Direction: peer. Test clients call a testkit node and ask it to declare upward to the selected primary. Not forwardable. BadTarget is a typed refusal; Answered carries the actual status outcome/reply. Server additionally emits `rdm.node_rpc.declare_probe.serve.via-request`. Caller/server emit the canonical runtime spans above. These families are served only by testkit binaries, never product binaries.

```rust
pub enum DeclareTarget {
    Exact(String),
    Path(String),
}

pub enum DeclareRequest {
    Declare { to: DeclareTarget, state: NodeState, node_id: Option<String>, incarnation: Option<String> },
}

pub enum DeclareReply {
    Answered { outcome: String, reply: Option<StatusReply>, reason: Option<String> },
    BadTarget { reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}
```

## Originate, op `0x73`, testkit

Contract source: RDM `crates/rdm-test/src/originate.rs::OriginateRequest`.

Direction: peer. Test clients call a testkit node to originate calls or control its test fault/evidence seams. Not forwardable. BadDestination and the runtime refusal variants are typed refusals. ProofOp here is local to the originate family, distinct from the proof-store operation label. Caller/server emit the canonical runtime spans above. These families are served only by testkit binaries, never product binaries.

```rust
pub enum ProofOp {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

pub enum OriginateRequest {
    Call { destination: String, op: ProofOp },
    ArmFault { refuse_index: u32, refuse_history: u32 },
    ReleaseFault,
    Snapshot,
    RecordProxy { destination: String, carrier: String, failed_attempts: u32 },
    Dial { destination: String },
    StopTransport { reason: String },
}

pub struct AnsweredBy {
    pub node_id: String,
    pub node: String,
    pub incarnation_id: String,
}

pub enum OriginateReply {
    Called {
        by: AnsweredBy,
        destination_node_id: String,
        route: String,
        carrier: Option<String>,
        retired: Option<String>,
        outcome: String,
        reply: Option<ProofReply>,
        pooled: Vec<String>,
    },
    FaultArmed { by: AnsweredBy, refuse_index: u32, refuse_history: u32 },
    FaultReleased { by: AnsweredBy, refused: u32 },
    Snapshot { by: AnsweredBy, own_active_proxies: Vec<NodeConnection>, own_latest_directs: Vec<NodeConnection>, active_len: usize, owed: Vec<NodeConnection>, fault_refused: u32 },
    BadDestination { by: AnsweredBy, reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    ProxyRecorded { by: AnsweredBy, destination_node_id: String, destination_incarnation: String, carrier_node_id: String, carrier_incarnation: String, failed_attempts: u32 },
    Dialed { by: AnsweredBy, destination_node_id: String, outcome: String },
    TransportStopMarked { by: AnsweredBy, reason: String },
}
```
## The ledger row

```rust
pub enum OpOwner { Core, Product(String), Testkit }
pub enum OpState { Live, Retired }

pub struct LedgerEntry {
    pub op: u8,
    pub family: String,
    pub owner: OpOwner,
    pub state: OpState,
}
```
