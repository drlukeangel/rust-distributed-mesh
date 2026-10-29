# Bidirectional streaming for product operations

Status: proposed contract and implementation plan; not implemented. Requested by the user on
2026-10-29. Deferred by Q286 (2026-10-29): the broker re-sync is a pull over server streaming, each
topic enabled individually as its frames complete; this proposal is built, as written, when a re-sync
flame shows the opening request is the bottleneck. No product op is allocated by this proposal.

Rafka must be able to register a typed Node RPC operation whose caller and handler both produce
frames while consuming frames from the other side. The data plane already exposes `open_bi` and
`accept_bi`; Node RPC currently exposes only unary and server-streaming operations. After the
opening request, the server currently reads acknowledgements, not additional application input.

This remains one invocation of one operation. Independent operations remain independent calls;
the change does not add a session that switches operation codes between input frames. Unary and
server-streaming APIs and their wire formats remain supported. Q285's large list reads need only
server streaming and need not wait for this work.

## Public API

Add `Shape::BidirectionalStreaming` and a `BidirectionalProtocol: StreamingProtocol` trait.
The existing `Request` is the opening request. The trait adds a typed `Input` enum, its variant
count, encoder, decoder, per-frame byte bound and classifier (`Data` or `End`). Its reply grammar
remains `Refusal`, or `Started Data* Terminal`.

The proposed public entry points are:

```text
ServerBuilder::serve_bidi<P>(owner, handler)
handler(peer, opening_request, RequestStream<P>, ReplySink<P, NotStarted>) -> terminal_reply

NodeRpcClient::call_bidi<P>(target, opening_request, options)
    -> (RequestSink<P>, ReplyStream<P>, CallEvidence)

RequestSink::send(input_data)
RequestSink::finish(input_end)
RequestStream::next() -> input frame with explicit acceptance handle, or stream failure
```

These are proposed signatures, not callable APIs yet. `finish` consumes the sending half and
requires an `End` input. Both directions can be driven by separate tasks. The handler may send
`Started` and replies before it receives input `End`; it must not await the whole input before
dispatch. The client can continue receiving after finishing its input. Receiving input `End`
ends only application input; it does not close the invocation or suppress acknowledgements.

Each input is acknowledged only after its handler explicitly accepts it as recoverable, following
the existing delivery contract. Merely decoding or yielding it to the handler does not acknowledge
it. Dropping an unaccepted frame cancels the local invocation rather than advancing its accepted
position. A recovered duplicate is not delivered to the handler a second time.

A terminal reply ends the invocation, including any still-open input. A producer blocked on input
flow control wakes with the terminal outcome. Dropping the request sink before `finish` cancels the
invocation; dropping the reply stream also cancels it. An explicit cancellation path is exposed.
Returning a terminal reply is how a handler refuses further input after it has begun replying;
it cannot send a pre-start refusal after `Started`.

## Exact architecture changes proposed

In `docs/architecture/node-rpc.md` section 9, replace:

> A descriptor is the envelope's `NodeProtocol` declaration plus two runtime facts: unary or server-streaming, and its default admission (§17).

with:

> A descriptor is the envelope's `NodeProtocol` declaration plus its shape (unary, server-streaming or bidirectional-streaming) and its default admission (§17).

In section 10, replace the sentence:

> Client streaming is not supported.

with:

> A bidirectional-streaming operation accepts typed input frames after its opening request while its replies are streaming. Both directions belong to that one invocation and operation. Input completion ends only the input direction; the terminal reply ends the invocation. Unary and server-streaming operations accept no additional input frames.

Insert section 11.3 before section 12:

> ### 11.3 Bidirectional streaming
>
> A bidirectional operation has one opening request, followed by zero or more typed input Data frames and one typed input End. Its reply order is section 11.2's Refusal, or Started Data* Terminal. The handler starts after the opening request is admitted and decoded; it can reply while the caller continues sending input. Each direction is independently ordered and bounded per frame, with bounded outstanding bytes and frames and backpressure. There is no whole-invocation byte ceiling imposed by a frame limit.
>
> Input End is a protocol frame, not transport FIN. It leaves the reply direction open. The caller cannot send input after End. A terminal reply ends both directions; unsent input is not accepted. Input delivered to a handler is acknowledged only when the handler makes it recoverable and explicitly accepts it. Reply frames retain section 11.2's acceptance and ordering rules. A reply acknowledges the opening request implicitly, never later input frames.
>
> Disconnects retain the identities and positions of owed frames in both directions. A reconnect resends them under the same invocation; it does not open another handler or replay accepted application work. A permanent failure or deadline before a terminal reply is not successful completion. Cancellation wakes blocked producers and consumers and releases the invocation's admission and retained frames. Losing either exact birth does not resume the operation against a replacement birth.
>
> Bidirectional operations are not forwardable. Product registration supplies the typed input codec and order classifier in addition to the request and reply codecs; the runtime owns framing, delivery, limits and cancellation. Product code does not parse transport headers or receive raw QUIC streams.

In section 12's registration example, append:

```rust
builder.serve_bidi::<P>(...)
```

In `docs/architecture/node-rpc-envelope.md`, Transport frames, replace:

> Three shapes, tagged by kind, each field held once:

with:

> Four shapes, tagged by kind, each field held once:

Append after Ack in its transport header schema (the existing three positions stay unchanged):

```text
Input { sender: Birth, delivery_seq: u64, forget_below: u64, invocation_id: DeliveryId, seq: u32 } then input_len | input
```

Append after that schema:

> Input is transport-header position 3. It carries a bidirectional invocation's typed input frame. Its sender is the invocation's original calling birth, its invocation_id names the opening request, and seq starts at 0 independently of reply positions. Input Data and input End are classified by the registered protocol. The input frame has its own delivery identity and is acknowledged explicitly by the responder after recoverable acceptance. A response acknowledges only the opening request implicitly. Input is delivered to the existing invocation, never dispatched as a new call. It is refused as a protocol violation for a unary or server-streaming invocation, a wrong calling birth, or conflicting bytes at an existing position. An unknown invocation cannot receive input; the sender must establish the opening request before sending input. The caller waits for Started before sending its first input frame. End can become visible to the handler only after every preceding input position is present. Each input frame is bounded by the operation's input-frame limit before allocation; positions never wrap.

The framing diagram also gains:

```text
input frame: header_len | Input | input_len | input
```

This is a wire extension. A product uses a new op allocation for its new bidirectional contract;
an existing unary op is not silently changed. Old peers must not be sent Input unless the opening
request's new operation has been served and answered Started. Update the envelope's delivery prose
to include input wherever it currently describes only response ordering and resend.

## Implementation boundaries

- `rdm-core`: input classifier and typed protocol trait, catalog shape and non-forwardable check,
  appended transport header, bounded input decoding, sequence/order tests and wire fixtures.
- `rdm-transport`: typed request sink and stream, bidirectional registration and dispatch, a
  reader that handles Input and Ack independently, and input delivery accounting beside response
  accounting. Use the existing birth checks, admission, tracing and connection resolution.
- Delivery: keep separate request-input and reply ordering and credit for each invocation;
  acknowledgements must continue flowing even when either application consumer stops reading.
  Apply the existing 64-position reorder bound in each direction, with explicit byte accounting.
  Include both directions in admission-memory validation; do not allocate two unbounded queues.
- Client: an opening request retains existing NotSent/Indeterminate distinctions. A completed
  local `send` means the runtime holds the frame for delivery, not that application work committed.
  Expose acceptance separately; an End locally sent is not the operation's terminal outcome.
- Recovery: retain accepted input positions and owed input frames for the invocation lifetime.
  Replacement births end the invocation under existing target rules. No automatic re-execution
  of the product operation under a new invocation id.
- Product surface: export the traits and halves from the public crates. Add a product-owned
  test protocol through the catalog so the proof uses the same API Rafka will use.

## Required proof

1. The server sends a reply to input 1 before the client sends input 2 or End; the client receives
   that reply and then sends input 2. This proves simultaneous directions without whole-input buffering.
2. Input End leaves replies readable; an early server terminal stops further sends and releases
   pending writers. Empty input is valid. Frames after either direction's end are rejected.
3. Each direction transfers more than 64 KiB in aggregate while respecting its frame bound.
   Oversize input is rejected before allocation; oversize output follows the existing sink rule.
4. Pause either application consumer: its sender stops at bounded credit while the opposite
   direction and acknowledgement processing can continue. Cancellation releases both waits.
5. Cut the connection before and after input acceptance. Reconnect resends owed input, accepted
   frames are not applied twice, and the original handler is dispatched only once.
6. Wrong-birth input, conflicting duplicates, invalid ordering, unserved operations, refusal before
   Started, handler panic, deadlines and either birth's loss produce explicit outcomes.
7. Existing unary and server-streaming conformance remains green; wire fixtures prove their
   existing enum positions and formats unchanged. Run the fast gate after focused cells.

## Approval and landing

The exact architecture changes above need sign-off under `CONTRIBUTING.md` before implementation
changes their contract. This proposal intentionally leaves the normative pages and runtime
unchanged. Once approved, the contract and its implementation land together, with the product
API proof. No complete bidirectional capability should be claimed from trait declarations alone.
