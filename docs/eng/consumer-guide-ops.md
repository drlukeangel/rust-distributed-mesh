# Consumer guide: my own ops on Node RPC

How an application adds its own calls to RDM's Node RPC: declare a protocol, take an opcode, reserve
it in the op ledger, serve it, and call it. The contract this follows is the
[Node RPC envelope](../architecture/node-rpc-envelope.md) (Section 2, the body; the op ledger; the
ledger row) and [node-rpc.md §9](../architecture/node-rpc.md). This page is the how-to; the pages
are the rule. Back to the [consumer guide](consumer-guide.md).

The worked examples are [`demo/src/consumer_example.rs`](../../demo/src/consumer_example.rs)
(`ConsumerOrder` unary op `0x31` and `ConsumerProgress` streaming op `0x32`, which a product copies,
changing its protocols, the enums and the constants beside them) and the testkit's
[`rdm-test/src/families.rs`](../../crates/rdm-test/src/families.rs) (`BrokerData`, a unary op) and
[`rdm-test/src/originate.rs`](../../crates/rdm-test/src/originate.rs) (`Originate`).

## Before writing an op: choose its completion contract

The application owns the operation's meaning, durable work and result. RDM owns delivery, duplicate-delivery
suppression, framing, budgets and transport spans. Use the process's existing client and endpoint.

| Need | Shape and budget | What the consumer must decide |
|---|---|---|
| Read one value | Unary, `read_only(request) = true`, finite `Overall` budget | What to do with no answer; a new read may observe a different value |
| Change application state | Unary with a finite owner-chosen budget | Operation identity, recoverable acceptance, and how to discover a committed result after an uncertain answer |
| Deliver an owed report | Unary with `Budget::Owed` | Who owns the obligation, when it is cancelled or retargeted, and how it is reconstructed |
| Receive progress or many records | Streaming; finite budget or `Budget::Stream { send }` when the connection is the intended bound | Frame recovery, terminal result, cancellation, and protocol-specific reattachment |

A caller ending its wait is not proof that application work stopped. A mutating handler may have
committed already; its protocol decides whether accepted work finishes or can be cancelled. An Owed
call ends on its reply, proven receiver loss, or owner cancellation. A Stream budget binds the
invocation to its committed connection; a new invocation is not automatic application resumption.

`FORWARDABLE` only makes a unary protocol eligible for Forward 0x1A. It does not select a carrier
or enable automatic fallback. The owning workflow supplies the route policy; the runtime executes
it. Streams cannot be carried through Forward. Data-plane Proxy registration is separate.

## 1. Pick an opcode

An op is one family: its request enum's variants are the calls. Take a code no ledger row holds.

| codes | held by |
|---|---|
| `0x00` | never: an op of zero refuses the process |
| `0x01`, `0x1A` | RDM core (`ping`, `forward`; `0x11` `echo` reserved forever, never served) |
| `0x1B`–`0x22` | RDM (`status`, `build-claim`, `join`, `topology`, `build-facts`, `build`, `node-primary-update`, `node-command`) |
| `0x10`, `0x12`–`0x19` | product rows in `core_ledger()` (`0x16` `layout-reprovision` reserved forever, never served) |
| `0x70`–`0x7F` | the testkit only (`0x70`–`0x75` allocated in `core_ledger()`): a non-testkit owner there refuses the process |

An op that is withdrawn from service stays reserved forever, never served or reassigned. A wire-incompatible change to a family is a new op, never a
reuse. The current rows are the [op ledger table](../architecture/node-rpc-envelope.md#the-op-ledger) (`crates/rdm-core/src/catalog.rs` `core_ledger`).

## 2. Declare the protocol

A protocol is a type implementing `rdm_core::protocol::NodeProtocol`. The snippets here are
integration sketches, not a standalone executable: the application supplies the enums and application storage.
For complete Rust definitions, imports, classifiers and registration, start with
[`consumer_example.rs`](../../demo/src/consumer_example.rs) or [`BrokerData`](../../crates/rdm-test/src/families.rs). For a streaming protocol and both ends of a
call, see [`Count` and its cells](../../crates/rdm-transport/tests/streaming.rs).

Keep request/reply definitions and classification together. Derive `Serialize` and `Deserialize`
for the enums; make every success and named refusal explicit, and list each variant in
`rdm_core::protocol_variants!` in its wire position: the helper derives the variant counts.

A protocol declaration has this shape:

```rust
#[derive(Serialize, Deserialize)]
pub enum ShuffleRequest {
    Record { record: ShuffleRecord },
}

#[derive(Serialize, Deserialize)]
pub enum ShuffleReply {
    Recorded { offset: u64 },                     // position 0
    Stale { current: u64 },                       // position 1
    PeerUnresolved { reason: String },            // positions 2 to 7: the six reserved variants, in this order
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

pub struct CgShuffleRecord;

impl NodeProtocol for CgShuffleRecord {
    const OP: u8 = 0x25;
    const NAME: &'static str = "cg-shuffle-record";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;             // checked on the declared length
    const MAX_REPLY_FRAME_BYTES: usize = 4 * 1024;
    // const FORWARDABLE: bool = false;              opt-in: carried through a peer (Forward 0x1A)
    // const SERVED_WHILE_CONNECTIONS_DELETING: bool = false;
    type Request = ShuffleRequest;                // a postcard enum
    type Reply = ShuffleReply;                    // a postcard enum

    // Writes REQUEST_VARIANTS (1), REPLY_VARIANTS (8), classify_reply, reply_detail and the six
    // reserved constructors (peer_unresolved, not_ready, busy, connections_deleting, malformed,
    // unauthorized).
    rdm_core::protocol_variants! {
        request ShuffleRequest { Record }
        reply ShuffleReply {
            Recorded = "recorded" => ReplyKind::Success,
            Stale = "stale" => ReplyKind::new(Code::FailedPrecondition),
            @reserved,
        }
    }
}
```

- **Request and reply** are the protocol's own postcard enums. There is no success or error envelope:
  a refusal is a reply variant.
- **The six reserved variants** (`peer_unresolved`, `not_ready`, `busy`, `connections_deleting`,
  `malformed`, `unauthorized`) are variants of your reply that the codec requires. Four are refusals
  the receiver's runtime answers before your handler runs: `not_ready` while the node's RPC door is
  unopened, `busy` at its admission bound, `connections_deleting` while the node deletes its
  connections, `malformed` when the body is too large or does not decode. `unauthorized` holds its
  codec position only: a mesh receiver never sends it, since nothing inside the mesh checks who sent
  a call ([node-rpc.md §25](../architecture/node-rpc.md)). `peer_unresolved` is a codec position your reply
  must carry; a caller whose resolver names no node sends nothing and gets the `NotSent` outcome,
  never this reply.
- **`protocol_variants!`** lists each enum's variants in declaration order, `@reserved` standing for
  the six where your reply declares them. It adds no variant and moves none: the enums, the six
  included, are yours. Its matches carry no wildcard arm, so a variant declared and not listed, or
  listed and not declared, does not compile.
- **`classify_reply`** maps each variant to its code, and **`reply_detail`** gives each its name for
  spans: the helper writes both, from each listed variant's `= "name" => code`
  (`ReplyKind::Success`, `ReplyKind::new(Code::NotFound)`, ...) and the six's standard codes and names.
- **`read_only`** (default `false`): return `true` for a request that changes nothing, so a caller may
  retry it after an `Indeterminate`.
- **Evolution:** never append a field to an existing variant. New data is a new variant, listed in
  the helper in its position, and the variant counts step up. Never reorder or reuse a discriminant.

## 3. Reserve it in the ledger and serve it

The ledger is read once, when the process builds its catalog at boot. Add your row, then serve the op:

```rust
pub fn ledger_rows() -> Vec<LedgerEntry> {
    vec![LedgerEntry {
        op: 0x25,
        family: "cg-shuffle-record".to_string(),
        owner: OpOwner::Product("rshape-consumer".to_string()),
        state: OpState::Live,
    }]
}

pub fn register(b: rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder {
    b.ledger(ledger_rows())
        .serve::<CgShuffleRecord, _, _>(OpOwner::Product("rshape-consumer".to_string()), |peer, request| serve(peer, request))
}

async fn serve(peer: rdm_transport::PeerContext, request: ShuffleRequest) -> Result<ShuffleReply, rdm_transport::HandlerFault> {
    // peer: the caller's endpoint id as the transport proved it, the sanitized CallContext, the
    // request's delivery identity, and its acceptance boundary.
    record_durably(&request).await?;     // the accepted work is now recoverable from your own record
    peer.acceptance.accepted();          // the runtime acks the request here, not at the reply
    // ... do the work; return the reply. A HandlerFault is the handler's own failure and reaches
    // the caller as INTERNAL_RPC_FAILURE (423).
}
```

- **Recoverable acceptance** ([node-rpc.md §43](../architecture/node-rpc.md#43-owed-messages-delivery-is-the-runtimes)):
  the runtime resends an unacknowledged request and suppresses duplicates by delivery id, but the ack
  proves receipt, never execution, and it is sent only once the accepted work is recoverable. A
  handler that mutates anything records what it accepted first, then calls
  `peer.acceptance.accepted()`; a read-only request is accepted as it is dispatched; a handler that
  calls nothing is accepted at its first reply frame. Return the reply only after recoverable
  acceptance or a recorded result: a crash before either leaves the work with no guaranteed
  recovery, since nothing durable names it. Delivery identity is not operation identity: a duplicate delivery of the same
  request is suppressed by the runtime, a second request for the same operation is yours to
  recognise by its operation id (node-rpc.md §33). You define how your pending work is rebuilt from
  your record after a restart; the runtime holds no durable queue.
- **`ServerBuilder::ledger(rows)`** adds your ledger rows. **`ServerBuilder::serve::<P, _, _>(owner, handler)`**
  serves a unary op. **`ServerBuilder::serve_stream::<P, _, _>(owner, handler)`** serves a
  server-streaming op: the handler gets a `ReplySink` for its frames and returns the final reply. A
  streaming op is never forwardable.
- **Hand the builder to RDM** where the node starts: `rdm_node::run_with(role, types, node_hooks, serve_app)`,
  where `serve_app: FnOnce(ServerBuilder) -> ServerBuilder` adds your ops before the catalog is built
  (`run_with_alpns` also registers your data-plane protocols).
- **The process refuses to start**, naming the cause, on any of:
  - an op of zero, or a duplicate ledger row;
  - a testkit-range row not owned by the testkit, or the reverse;
  - a served op with no ledger row, or with a row that is not live;
  - an owner that differs between the row and the served entry;
  - a forwardable streaming op;
  - a zero request or reply ceiling.
- **An op this process does not serve** is answered `421 UNSERVED_OP`. A process may hold a row without
  serving it: the row reserves the code, and the serving decides what this process answers.

## 4. Call it

Use `running.node_rpc.client` on a `RunningNode`. When a handler must itself call another node,
`rdm_node::node::start_with_client` supplies that same client during registration. Capture an `Arc` of
the application service in the registration closure and clone it into each handler future;
do not create another endpoint or RPC runtime. The higher-level `run_with` example above is suitable
when registration needs only the builder. Products supplying storage backends use `run_on`.

Choose the target from topology or a row read according to the application's operation, then call through
that client. RDM does not select a broker, a copy of its data or a replacement for you. `ExactNode(node_id)`
identifies the node; if the operation requires an exact incarnation, carry and verify that birth
in its application contract too.

For example, with `client = &running.node_rpc.client`:

```rust
let (outcome, _evidence) = client
    .call::<CgShuffleRecord>(&target, &ShuffleRequest::Record { record }, &CallOptions::default())
    .await;
```

- **`target`** is a `NodeTarget`, usually the exact node (`NodeTarget::ExactNode(node_id)`), from the
  node's topology or a read ([query](../architecture/query.md)).
- **The outcome** is `RpcOutcome<P::Reply>`:
  - `Reply(replied)`: the handler answered (`replied.reply`), or the receiver's runtime answered one of its four
    refusals (`not_ready`, `busy`, `connections_deleting`, `malformed`);
  - `NotSent(not_sent)`: provably never delivered; safe to send again;
  - `Indeterminate(indeterminate)`: sent, answer unknown; retry only a `read_only` request;
  - `Unserved(unserved)`: the target does not serve the op (`421`);
  - `RejectedStale(stale)`: the caller's resolver stopped naming the node it dialled before the request went; never dispatched.
- **`CallOptions`** carries the budget. Under every budget the runtime resends the same delivery,
  with backoff, until it is acknowledged (the one delivery rule, node-rpc.md §43): never add a
  delivery loop of your own. The default is `Budget::Overall(10 s)`, one deadline over resolve,
  dial, send and reply. `Budget::Owed` removes the deadline, nothing else: the call ends only on its
  reply, on the runtime's proof that the receiver is gone, or when you drop it; for a report or
  completion you owe, never a default. The call waits for the reply, not for the ack.
- **Trace context** rides the call's `CallContext`; your serve span is the caller's child
  ([telemetry.md §3](../architecture/telemetry.md)).

## 5. A mutation that survives an uncertain answer

For an application mutation, put an application operation id in the request and define a way to read its
result. That id survives a new RPC invocation; the transport's delivery id does not replace it.

1. Validate the request. Look up its operation id in the application's durable records. An existing completed
   operation returns its recorded result; a pending one joins or reports the existing work rather
   than starting it again. Define a named refusal for reuse of an id with different input.
2. Record accepted work recoverably, then call `peer.acceptance.accepted()`. Alternatively, finish
   and record the result before replying. An acknowledgement is never an execution receipt.
3. Execute using the application's recovery mechanism. Record the effect and completion so a crash between
   the effect and its receipt cannot make recovery apply the effect twice. A pending-work record
   alone does not solve this: use the application's transaction or idempotent operation mechanism.
4. Return the recorded result. On restart, reconstruct pending work and finish or reconcile it
   before claiming success. No transport delivery loop belongs in this recovery code.
5. If the caller gets `Indeterminate`, it queries the operation result by id. It must not create
   another mutation merely because the reply was lost. Resubmission of the same operation is safe
   only when the application contract explicitly supports it.

RDM's recovery helpers, `rdm_node::operation`, make steps 1, 2 and 4 and the read of step 5 explicit
over persistence the application provides. Implement `OperationStore` (`get`, `put_accepted`,
`put_completed`, `pending`) on the application's record: persistence only, each put durable when it returns,
deciding nothing. Hold one `Operations` over it in the application service.
`Operations::decide(&peer.acceptance, id, input)` looks the id up, records a new operation and then
accepts the request, and answers `Recorded` (start the work), `InFlight`, `Completed(output)` (the
recorded result) or `Conflict` (answer your named refusal); `complete` records a result once; `read`
answers a read by id; `pending` names what a start resumes in `pre_node_ready`. Each is a span naming
the `operation` (`rdm.node_rpc.operation.create.via-accept`, `.update.via-complete`,
`.serve.via-read`, `.update.via-resume`). The work itself, and an effect recorded atomically with its
completion (step 3), stay the application's.

`record_durably` in the sketch is an application seam, not an RDM API. Its error must be mapped to
an appropriate named reply or `HandlerFault`; an expected domain refusal is not an internal fault.
Retain call evidence and record the operation id in application spans so a consumer can connect
an uncertain call to its eventual result.

## 6. Streaming: producer and consumer

Implement `rdm_core::streaming::StreamingProtocol::frame_kind` for the protocol. Classify every
reply as `Started`, `Data`, `Terminal` or `Refusal`. The legal order is a single refusal, or
`Started`, zero or more data frames, then one terminal reply.

On the server, register `serve_stream::<P, _, _>(owner, |peer, request, sink| ...)`:

```rust,ignore
// Application-specific frame constructors and error mapping are omitted here.
// Make accepted work recoverable before the first reply frame.
let mut sink = sink.started(started_reply).await.map_err(map_sink_error)?;
for frame in data_frames {
    sink.data(frame).await.map_err(map_sink_error)?;
}
Ok(terminal_reply) // return it; the runtime sends the terminal frame
```

On the caller, use `client.call_stream::<P>(target, request, options)`. Handle an opening failure
separately from a stream that opens and later fails. A target that does not serve the op (`421`)
shows up either way: as the open's `Unserved`, when its refusal arrives before the request is
written whole, or as the stream's first item, `StreamFailure::Unserved`, when it arrives after. Both
are the same definitive outcome: nothing was dispatched. Once it opens:

```rust,ignore
while let Some(item) = stream.next().await {
    match item {
        rdm_transport::stream::StreamItem::Frame(kind, reply) => {
            // Record progress/result with stream.position() (delivery id and sequence).
            // This is application code, including treatment of Refusal and Terminal.
            recover_frame(stream.position(), kind, reply).await?;
            stream.accepted().await;
        }
        rdm_transport::stream::StreamItem::Failed(failure) => {
            // Reconcile recorded progress; do not infer successful completion.
            return handle_stream_failure(failure);
        }
    }
}
```

The acknowledgement belongs to the caller's `ReplyStream`, never the server's `ReplySink`.
`accepted().await` acknowledges all frames handed over so far: call it only once all of those
frames are recoverable, including the terminal result. For a consumer that records nothing and
whose recovery boundary is reading the frame, use `stream.read_only()` instead. This consumer
choice is distinct from the request's `NodeProtocol::read_only` classification.

At most 64 response frames may be unacknowledged; a consumer that does not acknowledge eventually
holds the producer. Await `sink.data` for backpressure. Dropping the caller stream ends the
invocation; a producer write can return `SinkError::CallerGone`. That is not proof that already
accepted application work was undone. Handle cancellation according to the operation's contract;
map unexpected sink order/size errors deliberately. Connection recovery inside an invocation and
application resumption after a new invocation are different: define the latter in the protocol.

## 7. Checklist for a new op

1. A code no ledger row holds (§1), and its row in your product's ledger rows.
2. The protocol: ceilings, the six refusals as reply variants, and `read_only` where it is true;
   `rdm_core::protocol_variants!` writes the variant counts, `classify_reply`, `reply_detail` and the
   six refusal constructors from the variants in wire order (§2).
3. `serve` or `serve_stream` in the builder your `run_with` gets.
4. Spans named `<owner>.<domain>.<entity>.<action>.via-<reason>` for what the handler does
   ([telemetry.md](../architecture/telemetry.md), the envelope's runtime spans).
5. For a mutating handler: the durable record of the accepted work, `peer.acceptance.accepted()`
   after it, and how that work is rebuilt after a restart (§3).
6. A cell: a call answered, a refusal answered by name, the request over its ceiling refused
   `Malformed(TooLarge)`, and for a mutating handler the request accepted then the process restarted
   with the work still done.

## 8. Consumer tooling to build next

The protocol declaration helper is available: `rdm_core::protocol_variants!` (§2). The conformance
harness is available: `rdm_test::conformance::unary::<P>` and `rdm_test::conformance::streaming::<P>`
run a protocol by naming its type, given its registration and the requests it answers (named
refusals, frame limits, an unserved op, duplicate delivery, ordering, backpressure, cancellation, loss of the reply,
and for a stream the consumer's acknowledgement after its record); `BrokerData` and `Count` run it as
their own cells. The compiled consumer example is available:
[`demo/src/consumer_example.rs`](../../demo/src/consumer_example.rs) holds an application service in an
`Arc`, registration through `run_with` (its ledger rows, a unary op and a streaming op), target
selection from the topology, calls through the process's client with all five outcomes handled, a
mutating handler that records then accepts and a restart that resumes the pending work, both on the
recovery helpers; its ops run the harness as their cells ([`demo/tests/consumer_example.rs`](../../demo/tests/consumer_example.rs),
in the release gate), and `scripts/demo-estate.sh example` runs it on the demo estate. The recovery
integration helpers are available: `rdm_node::operation` (§5) makes acceptance and response recovery
explicit over persistence the application provides, without moving domain transactions into the
transport; the example's handler and restart are written on them, and its reply lost after acceptance
passes the harness through them.

The consumer acceptance test is an application developer adding, calling, diagnosing and recovering one
real operation without reading transport internals. The examples and helpers should be built to
meet that test, rather than asking every product to rediscover the same integration.
