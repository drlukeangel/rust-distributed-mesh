# Consumer guide: my own ops on Node RPC

How an application adds its own calls to RDM's Node RPC: declare a protocol, take an op code, reserve
it in the op ledger, serve it, and call it. The contract this follows is the
[Node RPC envelope](../architecture/node-rpc-envelope.md) (Section 2, the body; the op ledger; the
ledger row) and [node-rpc.md §9](../architecture/node-rpc.md). This page is the how-to; the pages
are the rule. Back to the [consumer guide](consumer-guide.md).

Rafka has no op of its own on Node RPC yet: Node RPC was built so it can, and its rows in the
ledger (`0x10`, `0x12`–`0x19`, `0x24`–`0x30`) reserve its codes. The worked examples that exist are
the testkit's: `rdm-test/src/families.rs` (`BrokerData`, a unary op) and
`rdm-test/src/originate.rs` (`Originate`), and Rafka's one serve today, `Ping`, in
`rafka-node-base/src/node_rpc.rs`. The code below is shaped like a Rafka op would be.

## 1. Pick an op code

An op is one family: its request enum's variants are the calls. Take a code no ledger row holds.

| codes | held by |
|---|---|
| `0x00` | never: an op of zero refuses the process |
| `0x01`, `0x1A`–`0x22` | RDM core (`ping`, `forward`, `status`, `build-claim`, `join`, `topology`, `build-facts`, `build`, `primary-update`, `node-command`); `0x11` retired |
| `0x10`, `0x12`–`0x19`, `0x24`–`0x30` | Rafka's rows today (`0x16` retired) |
| `0x70`–`0x7F` | the testkit only: a non-testkit owner there refuses the process |

A retired op is reserved forever. A wire-incompatible change to a family is a new op, never a
reuse. The current rows are the [op ledger table](../architecture/node-rpc-envelope.md#the-op-ledger).

## 2. Declare the protocol

A protocol is a type implementing `rdm_core::protocol::NodeProtocol`:

```rust
pub struct CgShuffleRecord;

impl NodeProtocol for CgShuffleRecord {
    const OP: u8 = 0x25;
    const NAME: &'static str = "cg-shuffle-record";
    const MAX_REQUEST_FRAME_BYTES: usize = MAX_MESH_FRAME_BYTES;   // checked on the declared length
    const MAX_REPLY_FRAME_BYTES: usize = 4 * 1024;
    // const FORWARDABLE: bool = false;              opt-in: carried through a peer (Forward 0x1A)
    // const SERVED_WHILE_CONNECTIONS_DELETING: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 9;
    type Request = ShuffleRequest;                // a postcard enum
    type Reply = ShuffleReply;                    // a postcard enum

    fn classify_reply(r: &ShuffleReply) -> ReplyKind { /* each variant's gRPC code */ }
    fn reply_detail(r: &ShuffleReply) -> &'static str { /* each variant's name */ }
    fn peer_unresolved(reason: String) -> ShuffleReply { ShuffleReply::PeerUnresolved { reason } }
    fn not_ready(reason: String) -> ShuffleReply { ShuffleReply::NotReady { reason } }
    fn busy(reason: String) -> ShuffleReply { ShuffleReply::Busy { reason } }
    fn connections_deleting(reason: String) -> ShuffleReply { ShuffleReply::ConnectionsDeleting { reason } }
    fn malformed(kind: MalformedKind) -> ShuffleReply { ShuffleReply::Malformed { kind } }
    fn unauthorized(reason: String) -> ShuffleReply { ShuffleReply::Unauthorized { reason } }
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
- **`classify_reply`** maps each variant to its code (`ReplyKind::Success`, `ReplyKind::new(Code::NotFound)`, ...),
  and **`reply_detail`** gives each its name for spans.
- **`read_only`** (default `false`): return `true` for a request that changes nothing, so a caller may
  retry it after an `Indeterminate`.
- **Evolution:** never append a field to an existing variant. New data is a new variant, and the
  variant counts step up. Never reorder or reuse a discriminant.

## 3. Reserve it in the ledger and serve it

The ledger is read once, when the process builds its catalog at boot. Add your row, then serve the op:

```rust
pub fn ledger_rows() -> Vec<LedgerEntry> {
    vec![LedgerEntry {
        op: 0x25,
        family: "cg-shuffle-record".to_string(),
        owner: OpOwner::Product("rafka".to_string()),
        state: OpState::Live,
    }]
}

pub fn register(b: rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder {
    b.ledger(ledger_rows())
        .serve::<CgShuffleRecord, _, _>(OpOwner::Product("rafka".to_string()), |peer, request| serve(peer, request))
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
- **A streaming op** serves with `serve_stream::<P, _, _>(owner, |peer, request, sink| ...)`: the
  handler writes each `Data` frame to the `ReplySink` as it has it and returns the terminal reply;
  the runtime acks the frames the caller recovers (`sink.accepted()` after your record of them) and
  the caller reads them in order. A streaming op is never forwardable.
- **The process refuses to start**, naming the cause, on any of:
  - an op of zero, or a duplicate ledger row;
  - a testkit-range row not owned by the testkit, or the reverse;
  - a served op with no ledger row, or with a retired row;
  - an owner that differs between the row and the served entry;
  - a forwardable streaming op;
  - a zero request or reply ceiling.
- **An op this process does not serve** is answered `421 UNSERVED_OP`. A process may hold a row without
  serving it: the row reserves the code, and the serving decides what this process answers.

## 4. Call it

From any node, through the process's one Node RPC client:

```rust
let (outcome, _evidence) = client
    .call::<CgShuffleRecord>(&target, &ShuffleRequest::Record(record), &CallOptions::default())
    .await;
```

- **`target`** is a `NodeTarget`, usually the exact node (`NodeTarget::ExactNode(node_id)`), from the
  node's topology or a read ([query](../architecture/query.md)).
- **The outcome** is `RpcOutcome<P::Reply>`:
  - `Reply(variant)`: the handler answered, or the receiver's runtime answered one of its four
    refusals (`not_ready`, `busy`, `connections_deleting`, `malformed`);
  - `NotSent`: provably never delivered; safe to send again;
  - `Indeterminate`: sent, answer unknown; retry only a `read_only` request;
  - `Unserved`: the target does not serve the op (`421`);
  - `RejectedStale`: the caller's resolver stopped naming the node it dialled before the request went; never dispatched.
- **`CallOptions`** carries the budget. Under every budget the runtime resends the same delivery,
  with backoff, until it is acknowledged (the one delivery rule, node-rpc.md §43): never add a
  delivery loop of your own. The default is `Budget::Overall(10 s)`, one deadline over resolve,
  dial, send and reply. `Budget::Owed` removes the deadline, nothing else: the call ends only on its
  reply, on the runtime's proof that the receiver is gone, or when you drop it; for a report or
  completion you owe, never a default. The call waits for the reply, not for the ack.
- **Trace context** rides the call's `CallContext`; your serve span is the caller's child
  ([telemetry.md §3](../architecture/telemetry.md)).

## 5. Checklist for a new op

1. A code no ledger row holds (§1), and its row in your product's ledger rows.
2. The protocol: ceilings, variant counts, the six refusals as reply variants, `classify_reply`,
   `reply_detail`, `read_only` where it is true.
3. `serve` or `serve_stream` in the builder your `run_with` gets.
4. Spans named `<owner>.<domain>.<entity>.<action>.via-<reason>` for what the handler does
   ([telemetry.md](../architecture/telemetry.md), the envelope's runtime spans).
5. For a mutating handler: the durable record of the accepted work, `peer.acceptance.accepted()`
   after it, and how that work is rebuilt after a restart (§3).
6. A cell: a call answered, a refusal answered by name, the request over its ceiling refused
   `Malformed(TooLarge)`, and for a mutating handler the request accepted then the process restarted
   with the work still done.
