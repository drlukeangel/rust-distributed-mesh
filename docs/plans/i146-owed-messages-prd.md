# i146 — owed messages: one delivery mechanism in Node RPC (R-MSG1)

**Status:** approved 2026-10-16 (Luke, Q15 of the i144 stream, arm A with the a1 architect's
corrections; ruling `docs/plans/rulings/i144-rulings-2026-10-16-delivery.md`). Page text landed on
main's working tree the same day (node-rpc.md §43 new, §33, §41; node-rpc-envelope.md "Caller
execution and certainty"). Nothing built. **Tracking issue:** [drlukeangel/rafka-v2#3129](https://github.com/drlukeangel/rafka-v2/issues/3129). **Record:** this file; every state or remaining-work update
goes in an epic's row below. **Owner:** the i143 lead; RDM engineers in pool lanes.

## Context

Three retry loops were growing in the node: the owed declaration (`node.rs:651-670`,
`declare_once :1100-1128`), the mesh-primary → fabric-primary round check-in riding it, and the
member → mesh-primary round check-in in `node_self::check_in` (:676-699), which had none and lost a
`StateCommitted` across a suspension (trace `22bb4c156839fb2a1a3c5e1ad3b5a11c`: the fabric sat in
state-commit for 120 s). Q13 proposed a fourth loop; Q14 a shape change. Luke cut both to one rule: "a
message needs a tiny ack to acknowledge receipt; otherwise keep trying with exponential backoff. It
doesn't matter if it's a message, a command or a stream: it's just a message from A to B", and
"retries are a framework item, not a one-off implementation".

The customer is whoever runs a fabric: a lost directed message between two nodes never stalls a
round, a command or a report; the sender keeps delivering until the receiver has it; nothing executes
twice because it was delivered twice; and no primitive carries a retry loop of its own.

## The rule (node-rpc.md §43)

Retry automatically in Node RPC. Send immediately. Without an ack, resend the same message with the
same delivery id: first retry at the publish cadence, then exponential backoff with jitter, capped at
the staleness floor; one attempt in flight per message. The receiver suppresses duplicate delivery
ids, including duplicates received while the original invocation is executing: before recoverable
acceptance they stay pending without an ack; after it they are acked again without invoking the
handler. Retry stops on the ack, on the receiver's proven exit or accepted removal, or on explicit
cancellation or retargeting by the owning workflow; never on a timer, never on absence from a view,
never on a newer message of its kind. An ack proves receipt, never execution. Replies and completion
frames ride the same mechanism; tiny ack frames do not. Primitives and workflows keep their operation
semantics and implement no delivery-retry loops.

## Requirements

| # | Requirement | State | Evidence today | Epic |
|---|---|---|---|---|
| R1 | every directed Node RPC message carries a delivery id; the runtime resends it on the schedule until acked; one in flight per id; the schedule is publish cadence, then exponential with jitter, capped at the staleness floor | BUILD | no delivery id; `RpcOutcome` ends NotSent / Indeterminate and nothing resends (`node-rpc.md §41` said "the runtime itself does not retry") | e1 |
| R2 | the receiver suppresses duplicate delivery ids: pending and unacked before recoverable acceptance, acked without dispatch after it; at most one dispatch per id | BUILD | `node-rpc.md §33` said no deduplication; every invocation dispatched | e1 |
| R3 | the ack is a tiny control frame carrying the id, sent by the runtime once the handler reports recoverable acceptance; a read-only request's acceptance is immediate; acks are never owed | BUILD | no ack frame | e1 |
| R4 | a named refusal ends the request's retry; the refusal response is itself delivered until acked | BUILD | refusals are single replies | e1 |
| R5 | every frame of a streaming op carries stream identity and sequence; a reconnect never delivers an old admission after the terminal frame | BUILD | `node-rpc.md §11` order only; 0x22 frames carry no sequence | e2 |
| R6 | an owed message ends on ack, the receiver's proven exit or accepted removal (the view's removal row, never silence), or the owner's explicit cancellation or retargeting | BUILD | nothing ends | e1 |
| R7 | each owner demonstrates reconstruction of its pending commands, check-ins and responses from durable evidence with operation identity, and regeneration of a lost reply's result, with cells, before its loop moves onto the mechanism | BUILD | the declaration loop and the round checklist are in memory | e3, per owner |
| R8 | the owed declaration, `node_self::check_in` and the mesh-primary's check-in ride the mechanism; their loops are deleted | BUILD | three loops (one missing) | e3 |
| R9 | every directed Node RPC protocol (0x01, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x21, 0x22) rides it; gossip does not; the Build's resubmit-by-`build_id` and the connections reconnect scheduler stay above it unchanged; a resend uses ordinary dialing and never depends on the application connections log | BUILD | — | e1, e2 |
| R10 | spans: one owed-message family on the sender (delivery id, attempt, backoff_ms, outcome ∈ {acked, receiver-gone, cancelled}) and the ack on the receiver (delivery id, acceptance ∈ {recorded, read-only, duplicate}) | BUILD | per-protocol spans | e1 |
| R11 | `rafka-mesh-audit` reseals the framing (the delivery id and ack frame are wire additions: `wire format moved`) | BUILD | — | e1 |

## § Open forks

- *Where does the delivery id live?* Not in the context section (envelope §1): that section is
  observability-only and a malformed part is dropped silently, which would break suppression (the
  dev's finding, 2026-10-16). It lives in a transport frame header on every frame, in both directions (envelope "Transport frames", approved 2026-10-16 with the a1 architect's three clarifications): `Request { delivery_id }`, `Response { delivery_id, invocation_id, seq }`, `Ack { delivery_id }`; one authoritative copy of each field; the target section is unchanged; a missing or malformed header is `424 PROTOCOL_VIOLATION`. One id per message, minted by the sender, carried by the ack. Responses are owed messages too: routed by `invocation_id` across reconnects, ordered by bounded `seq`, acknowledged by the caller only once its owner can recover them; a duplicate request's `Ack` then EOF completes nothing. The page text is applied; e1 freezes its byte layout against it.
- *Does the delivery layer persist anything?* No. Reconstruction is the owner's (R7). A sender that
  restarts has no in-flight table; it regenerates what it still owes from its own durable state.
- *A relaunch as a new birth.* Follows lifecycle recovery; the old birth's owed messages are gone
  with it, and the new birth's obligations come from its own start.

## § OTLP span-assertion bar

| Span | Owner | Attributes | Cells |
|---|---|---|---|
| `rdm.node_rpc.message.update.via-resend` | the sender | delivery_id, op, target, attempt, backoff_ms | every retry cell |
| `rdm.node_rpc.message.resolve.via-acked` | the sender | delivery_id, op, target, attempts, outcome ∈ {acked, receiver-gone, cancelled} | every cell |
| `rdm.node_rpc.message.update.via-ack` | the receiver | delivery_id, op, acceptance ∈ {recorded, read-only, duplicate} | every cell |
| `rdm.node_rpc.message.reject.via-duplicate-pending` | the receiver | delivery_id, op | duplicate-during-execution cell |
| control: no per-protocol retry span remains (`via-declare-own` retried by its own loop, etc.) | — | — | e3 cells |

## § Per-sprint seed-data obligation

NO NEW ENTITY. Wire additions: the delivery id in the request context and the ack control frame
(`wire format moved`, mesh-audit reseal, goldens by hand). No durable row: the delivery layer
persists nothing (R7). No new gossip frame and no new `MemberStatus`.

## § Test tier coverage

| Tier | What | Where |
|---|---|---|
| unit | the backoff schedule (publish cadence, doubling, jitter bounds, the cap); delivery-id minting; duplicate table semantics (pending vs acked) | `rdm-transport` |
| component (loopback endpoint) | lost message delivered; lost ack repaired by resend; duplicate during execution held then acked once; named refusal ends retry and its response is delivered; receiver-gone ends it; owner cancellation ends it; streaming frames sequenced, stale admission after terminal dropped | `rdm-transport` |
| live (test kit mesh) | the Q13 cell: a member suspended across the (3, 4) reconcile, continued, its check-in lost, ready-for-traffic reached; sender restart regenerates its owed declaration; receiver crash after ack recovers from its record; executor replacement recognises the completed operation by operation identity | `rdm-test` |

## Implementation plan

#### e1 — the delivery layer: id, ack, resend, duplicate suppression

- **Goal → gap.** node-rpc.md §43, §33, §41; envelope "Caller execution and certainty". The runtime
  has no delivery id, no ack and no resend.
- **Requirements.** R1, R2, R3, R4, R6, R9 (the protocols), R10, R11.
- **Reuse (Q16 addendum, Luke 2026-10-16).** What exists is used for what it provides: the ordinary
  dial and pool (`rdm-transport/src/client.rs:285` `call`, `pool.rs`: a superseded birth's connections
  evicted, `:14-15`, `:181`), so a resend is an ordinary call's dial; the publish cadence as the first
  retry interval (`rdm-core/src/cadence.rs:62` `gossip_interval`); the staleness floor as the cap
  (`:56` `staleness_floor`); the operation receipts for recovery (the round checklist counts a
  completion once, `rdm-admin/src/fabric_state.rs:426`; the Build's receipts). Added only: the `Ack`,
  delivery tracking (the per-receiver `delivery_seq` counter per (sender birth, receiver birth), the
  in-flight table, `forget_below`) and the duplicate window. Heartbeat sequence numbers are never
  reused for `delivery_seq`: they order heartbeats, not command receipt. Operation receipts and
  transport duplicate suppression both stay: the transport suppresses repeated delivery of one
  invocation; receipts stop an operation repeating during recovery.
- **Frozen bytes** (against node-rpc-envelope.md "Transport frames"). Every frame, both directions,
  begins with `header_len: LEB128 | header: postcard(TransportHeader)`, the header a section like
  every other, its declared length checked against 128 bytes before it is read:
  ```text
  TransportHeader (postcard enum, discriminant a varint position):
    0  Request  { sender: Birth, delivery_seq: u64, forget_below: u64 }                                   then sections 0-2
    1  Response { sender: Birth, delivery_seq: u64, forget_below: u64, invocation_id: DeliveryId, seq: u32 } then reply_len | reply
    2  Ack      { sender: Birth, delivery_seq: u64 }      the acknowledged frame's identity; nothing follows
  Birth      { node_id: NodeId, incarnation: IncarnationId }   each its existing postcard string (len + 12)
  DeliveryId { sender: Birth, delivery_seq: u64 }              u64 and u32 are postcard varints
  ```
  Encoded at most: Request 47 bytes, Response 88, Ack 37. A stream's first frame decides what it
  carries: a `Request` opens an invocation (its reverse direction carries the request's `Ack` and the
  response frames; its request direction then carries the caller's `Ack`s of those response frames
  and finishes once the terminal frame is acked); a `Response` first carries resent response frames
  after a reconnect; an `Ack` first is an Ack stream of the current connection. A stream's FIN ends
  that stream, never an invocation.
- **Bounds and refusals.** Reset `424 PROTOCOL_VIOLATION`: a header over 128 bytes, undecodable, or
  of an unknown kind; a stream whose first frame is a protocol section with no header; two different
  frames at one `(invocation_id, seq)`; a response frame more than 64 positions past the next
  expected `seq`, or any frame over the op's reply ceiling (the envelope's byte bound, per frame: with
  64 positions it bounds the held bytes, so no second byte check exists); a frame after
  `seq = u32::MAX`. `delivery_seq` starts at 1 per sender birth, from one counter for every receiver,
  and is never reused by the sending birth: each receiver sees its own frames in increasing order, and
  a carried call's identity is unique at its carrier and at its final target alike (per-receiver
  counters would mint the same `(origin, n)` twice at a receiver reached both directly and carried).
  `forget_below` is the lowest `delivery_seq` the sender holds unacked toward that receiver; a carried
  frame is held toward its carrier and its final target, so its watermark is valid at both. The receiver retires duplicate state only below the watermark, or on the sender birth's
  proven exit or accepted removal; never by time. A carried call keeps its origin's `(sender,
  delivery_seq)`: the window is per origin birth. Schedule: send at once; first resend at the publish
  cadence, then doubling with ±25 % jitter, capped at the staleness floor; one attempt in flight per
  delivery identity. Response and `Ack` frames reach the delivery demultiplexer before dispatch and
  before lifecycle admission.
- **Reseal.** `rdm-core` framing goldens by hand for the three header kinds and a request, response
  and ack frame; a mesh-audit ratchet `transport_header_reseal_is_atomic` pinning the kinds and the
  goldens; `PostcardSectionBounds` gains the 128-byte header bound; `docs/reference/rdm/wire.md`
  regenerated; `wire format moved`.
- **Acceptance cells, 1-21.** (1) a message whose connection drops before the ack is resent with the
  same identity and acked; (2) a lost ack is repaired by the resend and the handler runs once; (3) a
  duplicate during execution stays pending unacked, then is acked once without dispatch; (4) a
  read-only request is acked at once; (5) a named refusal ends the request's retry and its response is
  delivered until acked; (6) a receiver proven exited ends the owed message and a silent one does not;
  (7) an owner cancellation ends it; (8) the resend schedule is the publish cadence, then doubling with
  jitter, capped at the floor; (9) a resend uses the ordinary dial and reads no connections log; (10) a
  lost ack on a response frame is repaired by its resend; (11) a response frame is delivered on a new
  stream after a reconnect; (12) a duplicate request's `Ack` then EOF leaves the caller's invocation open
  and the original's result arrives; (13) two different frames at one `(invocation_id, seq)` are refused
  424; (14) a terminal is held while an earlier position is missing; (15) a response for a completed
  invocation is acked and dropped, and one for an invocation merely absent from the map is not; (16)
  duplicate state outlives the staleness floor until the sender's `forget_below` passes it; (17) a
  carried call's resend through another carrier is suppressed by its origin's identity; (18) an `Ack` on
  an Ack stream of the current connection is consumed and correlates by identity; (19) an endpoint
  that admits no new command (connections-deleting) still receives an owed response; (20) reordering
  past 64 positions or past its byte bound is refused 424; (21) a new birth of the sender starts its
  `delivery_seq` at 1 and the receiver keeps the old birth's window until its proven exit or removal.
  RED first.
- **Page text.** Landed 2026-10-16 (§43, §33, §41, the certainty paragraph, "Transport frames").
  Q18 (Luke 2026-10-16, arm A): yes to all three, landing with e1, applied (`add379d4`): the
  envelope's opening paragraph and frame diagram (:12-19; an invocation begins on the stream carrying
  its request frame, every frame begins with its transport header, FIN ends a stream never an
  invocation); the §1 context sentence (no delivery identity in the context, it is the transport
  header's); node-rpc.md §52's two forbidden designs replaced, not deleted (a delivery-retry loop in a
  primitive or a workflow; a cache of handler results).
  Q22 (Luke 2026-10-16, arm B with three edits, final): node-rpc.md §23, §27, §31 and §34 as the lead
  wrote them, landing with e1, except: §31's ack boundary reads "once its owner can recover the
  accepted progress or result (§43). Reading a frame alone does not establish that boundary."; §34
  line 896's last sentence reads "For Budget::Stream, closure of the connection on which the request
  committed ends the invocation as Indeterminate and ends its delivery obligation: the owner of such a
  call (the Build carrier, build.get's watch, the handover) declared the connection its bound and
  reattaches by its own rule, above this layer. For other budgets, connection loss alone does not
  cancel the invocation; owed delivery continues within the owner's bound."; §24's "connection/open
  failure before complete-send+finish" reads "connection/open failure before the complete request
  frame is sent". Lines 890 and 898 as written. Code before landing: `ReplyStream::next()` acks a
  frame before returning it (`a06f52cc`); the ack follows the owner's recoverable acceptance
  (recorded, or read-only), never the read. Cell: a response frame delivered to a consumer that has
  not yet recovered it is not acked, and is redelivered after a reconnect.
- **Build decisions (2026-10-16, the lead's, within the pages).**
  - The acceptance boundary: a protocol declares a read-only request (`NodeProtocol::read_only`,
    acked as it is dispatched); a handler that records the accepted work says so on
    `PeerContext::acceptance` (acked at once); otherwise the first response frame acks the request.
  - A call's budget is its owner's bound: when it lapses the call ends `NotSent` or `Indeterminate`
    and its request is owed no longer (the owner's cancellation, span `delivery=cancelled`). An owner
    that needs delivery past a budget passes a longer one; no call accumulates owed requests behind
    its caller. Before the commit the attempt task alone enforces the send bound, so the outcome of a
    dial the bound cuts is recorded (a carrier answers `CarrierEdgeLost` from it).
  - `Budget::Stream` keeps its meaning, no reply bound but the transport's liveness: a committed call
    lives as long as the connection its request committed on, and ends `Indeterminate` (`ReplyLost`,
    named) when that connection closes. `Overall` and `Split` calls wait out their reply budget for a
    resent response. A lost stream ends no invocation on its own (the envelope's transport frames).
  - The wait for a request's ack runs from the write, so a slow dial spends none of it.
  - A response frame is resent only when the stream it was written on dies (its request direction
    reset or lost: an ack is never lost on a live stream). A caller finishes its request direction
    only once it is done with the invocation, so a clean finish with a frame unacked is a caller that
    ended it (its budget lapsed, its stream dropped): nothing more is owed. A resend never abandons
    the earlier attempt's stream: it stays open, read beside the next attempt, so a handler slower
    than the ack wait answers on it. The first gate's own logs showed 344 response resends in
    node_stop from both; after both, 0.
  - A source that admits no new connection (connections-deleting) refuses the call at once,
    `NotSent(AdmissionBlocked)`: nothing was sent, and nothing is owed for a request the source
    refuses to originate.
  - An owner that asks once (`CallOptions::once`) is never resent for: its call ends when its one
    connect fails or its stream is lost, and otherwise waits for its answer within its budget. The
    tickle's direct rung and the seat watch's ask once (node-recovery.md step 2: "it makes one QUIC
    connect, direct; if that fails, the same kick through up to two eligible live peers"): the
    ladder retargets, never the runtime. The gate's `role_wedge` showed the direct ping resending
    for its whole budget (10 s against the page's one connect).
  - A carrier's one inner call answers from its first unsent attempt (`CallOptions::unsent_is_final`):
    a dial or reach that fails before the commit cut ends it `NotSent`, never resent, so the carrier
    refuses `CarrierEdgeLost` (or `InnerNotSent`) directly from its failed dial (node-rpc-envelope.md
    Forward, "directly from its failed dial"; connections.md §8, "refuses the carried call by name";
    node-rpc.md §36.1, "exactly one direct inner call"). Committed, it is owed as any call. The
    resend's stop is the owning workflow's: the named refusal is what retargets the source's route
    (connections.md §8 J). Gate 7 timed the misdial cells at 9.9 s and the Proxy-retirement cell at
    19.8 s and the unreachable-target cell at 9.9 s: the carrier resent its failed dial to its budget
    and answered inside the reply reserve. After: 0.02 s, 0.02 s and 3.0 s (the handshake's idle
    timeout at the gate's cadence, as on main); rdm-transport main 137/137 in 10.48 s. RED with the
    option off in `carry_once`: 5 resends in each misdial cell.
  - A carried inner call that ends by its budget (a committed request to a silent target with no
    proven exit) answers with what is left of the 100 ms reply reserve on the origin's clock
    (node-rpc-envelope.md:625). Gate 7's red (`caff0fc8`) was the origin's own `ReplyDeadline`
    winning that window: http://192.168.68.99:16687/trace/e1ce71f335858f6c0b916eaccb2443f1, the
    carrier's inner call 22 → 9920 ms, the origin's call 20 → 9921 ms of its 10 s.
  - A target the view does not name at the call is `NotSent(Resolve)` at once: nothing is minted or
    owed. Once owed, absence from the view ends nothing; `Gone` or another incarnation (the view's
    proof) ends it, `ReceiverGone`.
  - A process that is no node (a probe, `rdm-client`) mints its birth once; a node's clients and its
    server share one `Delivery` of its exact birth (`rdm-node` node.rs, `rdm-admin` boot.rs).
  - A caller that drops its invocation stops the stream (code 0): the responder drops what it owes
    for it, and a streaming handler's next `data()` answers `CallerGone`. A responder owes at most 64
    unacked frames of one stream; past that `data()` waits for an ack, and answers `CallerGone` only
    once the response is owed no longer.
  - A stream frame is acked once its owner can recover it (Q22; node-rpc.md §31), never on the read:
    `ReplyStream::next` keeps each frame's ack, `ReplyStream::accepted` writes them in order (the
    terminal's last, and only then is the invocation complete), and `ReplyStream::read_only` declares
    an owner that records nothing of the frames, whose reading is its recovery. A copy of a frame its
    owner holds unrecovered (resent after a reconnect) is never handed over again and moves the
    frame's ack to the stream it arrived on (`delivery.rs` `AckOnRecovery`). The owners: the topology
    read and the NodeCommand carrier accept once the frame is taken into the birth's view or the
    mesh-primary's records; the Build drive, `rdm-client`'s Build stream and `fetch_build_facts` read
    durable records (each Build frame's receipt is recorded by its executor before the frame is sent,
    `build_run.rs` `append_step_receipt`, `HeldRun::finish`; Build facts are the responder's log) and
    are read-only, as are the topology client read, the probe and the test consumers. Cell 22, RED
    first on `1b84e072` ("read, not yet recovered: neither frame is acked", left 0 right 2), GREEN in
    0.64 s.
  - A unary reply is acked as it is delivered to its owner's call (`delivery.rs` `on_response`): the
    call returns it to its owner in the owner's own process, with no reconnect between the two for a
    resend to arrive in, and an earlier birth's response never reaches a new birth (§43: "a relaunch
    as a new birth follows lifecycle recovery, never a replay").
  - The commit cut is the complete request frame written; the request direction stays open for the
    caller's acks (node-rpc.md §23, §24, §34, Q22).
- **Sizing.** Two seats; `rdm-transport/src/{client,server,framer,wire,stream}.rs`,
  `rdm-core/src/{framing,outcome,codes}.rs`, `tools/mesh-audit`.
- **State.** LANDED `ca27478f` (2026-10-16, through the land script on `i146-e1-delivery-layer`, lane e4s8; gate 67/67 units, 1690 passed, 0 failed, 376 s, `TIMINGS wall=101.2s units=476`), with Q18's page lines (`add379d4`) and Q22's (node-rpc.md §23, §24, §27, §31, §34, `bb447c5e`). Commits on main: the header and the rules `d530c07b`; the runtime, cells and ratchet `101b7b82`; fixes `9de62671`, `b6d1d30b`, `25627a4f`, `d639f4fd`; the bounded cells `90de16cb`, `71c4ab2c`; the forward cells' exit proof `aa910d8b`; the carrier's failed dial as its answer `277bd6e5`; the stream ack after the owner's recovery `522b7bf3`; the delivery layer's own names without a retired word `ca27478f`.
  - RED. The runtime was written before its cells, so each cell's RED is shown two ways. The first run of the 21 on the runtime: 6 red (5, 9, 10, 17, 20, 21) from three defects, fixed: a reply frame raced the end of the attempt task, a request frame that arrived whole skipped the ready gate, connections-deleting and admission checks, and a frame over the reply ceiling reached no invocation. Then each mechanism switched off in the tree, the cells run, the tree restored (`mutate.py`, the run's evidence in the lead's scratchpad): batch A (duplicate suppression, response order, completed invocations, ack-stream acks, read-only acceptance, the receiver-gone proof) turns 2, 3, 4, 5, 6, 7, 10, 12, 13, 14, 15, 16, 17, 18 red and the rdm-core order units; batch B (request resend, response resend, admission before the demultiplexer, sender-window retirement, the reorder bound and the reply ceiling, cancellation that stops the attempts) turns 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 19, 20, 21 red. Every cell fails under at least one; 8 and 20 hung under B until bounded (they now fail inside 4 s).
  - GREEN. 22/22; rdm-transport main 138/138 (10.52 s; 19.98 s before e1), rdm-core lib 108, main 61, rdm-mesh-audit 105, rdm-admin lib 439, main 410, rdm-node lib 21, rdm-test lib 38.
  - Cells whose contract the pages moved, restated, never loosened: `pool::a_connection_closed_under_a_committed_call_is_evicted_and_reported_broken_once` (a lost stream ends no invocation: the call waits out its own reply budget); the withhold seam of `status_declarations` holds the reply before it becomes a frame (the request stays unaccepted until the release); `forward::a_target_whose_exit_is_proven_after_it_applied_answers_the_origin_carried_receiver_gone` and `forward::a_carrier_whose_exit_is_proven_after_the_inner_call_applied_leaves_the_origin_indeterminate_receiver_gone` (a node's shutdown proves nothing: its exit, published in the caller's view, ends the owed call `ReceiverGone`, and the carrier carries that reason; RED with the receiver-gone proof off, both `ReplyDeadline` at 10 s). `node_stop::a_stop_parks_the_process_and_a_start_rejoins_it_as_the_same_birth` (`node_stop.rs:147`, read 2026-10-16 on its gate red `d58a7975` and i147's `03613e10`): the pool handed back no connection the cut should have retired. In i147's run the stop's transport cut at 0 ms, the mesh-primary resent a response it owed the node at 226.0 ms (`via-response-resend`, "the reply's write Gone(connection lost)") over a fresh dial at 226.1 ms (`via-dial`, accepted as connection `265667081306976`), and the start at 1508.5 ms rode that connection (`reused=true`, `attempts=1`). `get_or_dial` dials only when no live pooled connection holds the key (`pool.rs:326-345`), so the stop's connection was already gone (`close_pooled_to(.., "node.stopped")` after the stop's reply, `node_commands.rs`). The cell's `reused == false` was a proxy for "the stop's connection was closed": an owed message (§43) may dial the same live birth after the cut. Restated: the start dialled afresh, or rode a connection dialled after the stop's transport cut. Built: when the start's call reused a connection, the cell requires a `via-dial` by the call's caller (its `delivery_id` node, matched by its transport address in `local`) to the node's incarnation, `connected`, between the stop's `via-stop-transport-cut` and the call; the Q30 landing gate's red on `f7de8349` was this case again (the owed resend's dial 16.5 ms after the cut, the start's call at 1563.2 ms `reused=true`).

#### e2 — the ordered stream: identity and sequence on every frame

- **Goal → gap.** §43's stream sentence; node-rpc.md §11, §31. Since e1 (`ca27478f`) every frame of
  every stream, 0x22's included, carries its stream's identity and position in the transport header
  (`invocation_id`, `seq`): the receiver orders by `seq`, refuses a conflict or a frame after the
  terminal, never delivers a frame of a completed invocation, and hands a frame its owner holds
  unrecovered over once (e1 cells 13-15 and 22; node-rpc.md §11 states the rule). The runtime is
  proven once (§47), so the first two cells below are e1's. The gap left is the 0x22 owners': a
  `ConnectionsDeleting` the mesh-primary receives twice must be recorded once. A
  `node.connections.delete` that a later attempt re-issues is a new invocation the node runs again,
  with no receipt on the node: the attempt resumes from the Build's recorded step receipts and never
  runs a completed step again, and the one uncertain step it re-issues settles because
  `connections.delete` is naturally idempotent (node-rpc-envelope.md "Caller execution and
  certainty"; node-connections-delete.md step 5, a new attempt leaves connections-deleting).
- **Requirements.** R5, R9 (0x22).
- **Acceptance cells.** `every_frame_of_a_stream_carries_its_stream_id_and_sequence`;
  `a_reconnect_after_the_terminal_frame_delivers_no_old_admission`;
  `a_progress_frame_delivered_twice_is_suppressed_and_the_record_is_written_once`;
  `an_interrupted_connections_delete_resumes_from_the_builds_receipts_and_its_reissue_settles_it`
  (e2b's connections.delete). RED first.
- **Page text.** node-rpc.md §11 states the sequence rule ("the orders above are the sequence of an
  invocation's response frames by `seq` after that layer has deduplicated and ordered them") and §31
  the ack after the owner's recovery (Q22): nothing left to ask.
- **Sizing.** One seat; `rdm-transport/src/stream.rs`, `rdm-core/src/node_command.rs`.
- **State.** not started. This row's landing (with Q27's `forget_below` rename): its gate on tree
  `804af94f` red at `rdm-transport:main`, `collector_outage::an_unroutable_collector_changes_nothing`
  ("the child took 8.850569956s to end against http://10.255.255.1:4317: spawn to test entry 7 ms,
  telemetry init 5279 ms, test entry to exit begun 6820 ms, exit begun to process ended 2038 ms",
  `INIT_MS=5279 EMIT_MS=331 DRAINED_MS=3553`, `gates-e4s8-804af94f.log.detail`); the release gate
  `99f83210` had all three outage cells red at telemetry init 5880, 5498 and 5877 ms. Q33 (Luke
  2026-10-17, arm A): `init_evidence_telemetry` reports each step's time (evidence file, exporters,
  stderr sink, transport diagnostic, subscriber, watchdog), each step its own span
  (`rdm.mesh.telemetry.create.via-init-step`), and the cell's failure names the slowest with that
  span; no change to telemetry's behaviour or the bound. A recurrence comes as a packet naming the
  step. A synchronous disk wait at init would be Q30's defect in a second place: its own packet and
  fix, R25's rule and its unit cell's shape. Its next gate (`bac3a549`) red at `transport_diag`: the init step spans run on that
  cell's own thread, named after its module, and its leak check matched the bare word
  `transport_diag`; the check now matches the diagnostic target (`"target":"transport_diag"`). The gate
  after (`882abfe9`) red at `fabric_primary_handover.rs:516`: mesh3's only admin other than the holder
  heard "primary updated" on its mesh channel (`mesh3.admin.2`, `via=mesh-channel`, trace
  `2b5ad5013364284dc7c1b0bdfb4e0f6b`), the copy mesh3's primary carries from the backbone (gossip.md
  "Peer mesh primaries carry them under the forwarding rules"; `membership.rs` `forward_of`), and the
  backbone copy then a repeat of the same publication (i147 `0c7aff9e`); the cell's per-mesh backbone
  check raced that forward. Restated to the page: every surviving mesh hears it, on either channel,
  naming the exact birth and revision; the backbone proven by an admin other than the holder that heard
  it there (a peer mesh's primary, which hears it nowhere else: `mesh2.admin.1`, trace
  `a865e1a0ef1729a5a2b75e8c16ee8dab`).

#### e3 — the owners move onto it, one at a time, each after its reconstruction cells

- **Goal → gap.** R7, R8. Three loops, one missing; none demonstrates reconstruction.
- **Q29 (Luke 2026-10-16, arm C).** A node that parks declares `stopped` to its mesh-primary as its
  owed declaration, whether or not any command's stream is still open; the mesh-primary records it and
  publishes NodeStopped from the declaration, exactly as from the stop call's own `node.stopped`. The
  response drop at `delivery.rs:493-496` is correct and stays: the node's state does not travel on that
  response, it travels on the node's report. The Build side needs nothing new: the attempt ended
  Indeterminate at its step; the next resubmission reads the birth's recorded state (stopped) and
  resumes at start (Q2's rule: a completed stop is never run again). Cells: a stop committed, its
  owner's bound lapsing, the node continued, the stop run, the node declaring `stopped`, NodeStopped
  published, the resubmitted restart resuming at start, no person; its control, a stop answered within
  the bound publishes NodeStopped once, never twice from the reply and the declaration (the declaration
  of a state the row already holds is `AlreadyApplied`); and
  `node_stop::a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth`
  green on it (its gate red, `d58a7975`, trace `0bed69226abd2aa867c36bb7f2b0899d`, is this case).
  Page lines, exact old → new to Luke before landing: node-stop.md steps 4-5 gain that the mesh-primary
  records `stopped` from the node's declaration as well as from the stop stream; node-status.md's
  declaration section cites §43 for how the declaration is delivered.
- **Q36 (Luke 2026-10-17, arm A, made general).** `DeclareNodeState` gains `operation:
  Option<{ build_id, attempt, operation }>`: the operation that produced the declared state, when one
  did (a stop's Leaving; a start's Pending and ReadyForTraffic), `None` for a state the node measured
  itself. The mesh-primary records the state and, for a Leaving that names an operation, publishes
  NodeStopped with that identity exactly as it would from the stop stream's `node.stopped`; a state the
  row already holds is `AlreadyApplied` and publishes nothing twice (a stop answered within the bound
  publishes NodeStopped once, from whichever arrives first). Status `0x1B`: the field appended, reseal,
  wire format moved. Page lines, exact old → new to Luke before landing: node-rpc-envelope.md:679 (the
  `DeclareNodeState` schema) and node-stop.md step 5 (NodeStopped from the stop stream's frame or from
  the node's declaration naming the stop's operation). Cells: the Q29 cell as restated; the control; a
  declaration with no operation publishes no lifecycle frame.
  Built on `i146-e3-declaration`: `DeclaredOperation` on the wire (`rdm-core/src/status.rs`); the node
  declares Leaving naming its stop after its transport cut and before its reply (`node_self.rs` stop
  step 6), its start ends the stopped cycle's owed declaration, and its start's ReadyForTraffic names
  the start (`node.rs` `Becoming`); the mesh-primary's stop stream
  records its `stopped` through the same declared-state row (`StatusAuthority::stop_received`), and
  NodeStopped is published only when that row is applied (`persist_then_commit`), never for a
  restart's stop. Cells: `status_rpc::tests::a_stops_leaving_publishes_node_stopped_once_from_whichever_reaches_the_mesh_primary_first`
  (both orders, a measured Leaving and a restart's stop publish nothing);
  `node_stop::a_stop_whose_stream_stopped_is_held_is_published_stopped_once_from_the_births_declaration`
  (the faulted admin's `stopped` event cut holds the stream's record; the birth is listed parked from
  its declaration alone); the control on `stop_then_start`; and
  `node_stop::a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth`
  made deterministic (its attempt dispatched only once the mesh-primary marks the held birth silent,
  so no connection lives and the call is never sent). The SIGSTOP form of Q29's case (a committed
  stop-node whose bound lapses) cannot be held deterministic from a cell: the commit needs a live
  mesh-primary → node connection inside the idle timeout at the send (two runs `not-sent`, the dial
  failing at 3001 ms; the gates' `indeterminate` landed inside it). Open: a node-admin's own
  declarations stay `None` (its declarer, `status_declare.rs`, reports states from its view); a
  re-declaration that reaches a new mesh-primary between its seat and its seat fold would apply again.
- **Q37 (Luke 2026-10-17, A).** The four page lines landed with the code (node-rpc-envelope.md's
  `DeclareNodeState` schema and `DeclaredOperation`, node-stop.md steps 4-5, node-status.md's
  declaration). Next landing of e3: a node-admin's own declarations name their operation too
  (`status_declare.rs`), so one rule holds for every node. A re-sent Leaving reaching a new seat
  holder after its seat fold finds the declared row held (`AlreadyApplied`) and publishes nothing; were
  NodeStopped ever published twice, i147's receive rule makes the repeat of an operation fact do
  nothing at every receiver. Cell, same landing: a parked birth whose mesh seat moves re-declares its
  Leaving to the new holder, which publishes NodeStopped once across the move.
  The landing's gate on `4c8c3cb7` red three times, read: the declaration then went out after the
  reply, unawaited, so a restart's start (`begin_cycle`, `session.rs`) cleared the declared row and the
  late Leaving applied over it, refusing the start's ReadyForTraffic as backward
  (`build_op::a_successor_fabric_primary_starts_the_attempt_its_predecessor_claimed_and_no_step_runs_twice`),
  the declaration is now awaited before the reply, and a start ends the stopped cycle's owed
  declaration. `node_delete_pipeline::retire_runs_every_step_in_order_and_the_ports_it_held_are_released`:
  the rig admin's Status handler matched only declarations naming no operation (`rig.rs`, an edit of
  this landing), so the delete's Leaving naming its stop was answered `request-not-handled`; it matches
  every declaration. The never-admitted cell's longer hold put the held birth through the offline
  ladder (`os-stopped`, listed Dead) at gate cadence; it now waits out both unheard marks. With the
  declaration ahead of the reply, NodeStopped comes from it before the admission, which then sets
  nothing; the cell says so.
- **Order (Q29).** (1) the owed declaration (`node.rs:651-670`, `declare_once`), the first live proof
  being the node_stop cell above; (2) the member → mesh-primary round check-in (`node_self::check_in`);
  (3) the mesh-primary → fabric-primary check-in. For each, first the owner's cells: reconstruction of its pending messages from its
  durable state after a restart of the same birth, with operation identity; regeneration of a lost
  reply's result; the already-applied answer by operation identity on a resend that outlived the
  duplicate table. Then the loop deleted and the message handed to the layer. Then the control: the
  owner's old retry span is gone.
- **The Build watcher's reattach (Luke 2026-10-16).** Q22 made reattach by `build_id` the owner's
  job after a `Budget::Stream` connection closes; neither `rdm-client`'s `BuildStream` nor the cells'
  `until_complete` does it, both treat a break as a defect (build.md: "a live run is reattached, never
  re-claimed"). The Build watcher is an owner that demonstrates reconstruction: reattach by `build_id`
  after a connection close, resume from the receipts. Cell: a watched Build whose connection closes
  mid-stream reattaches and ends `Complete` with no step run twice. Not part of R24's landing.
- **Acceptance cells (per owner, named for it).** `…_reconstructs_its_owed_<message>_from_its_rows_after_a_restart`;
  `…_regenerates_the_lost_<reply>_from_its_record`; `…_answers_already_applied_by_operation_identity_to_a_late_resend`;
  `…_no_longer_runs_its_own_retry_loop` (control). Plus the Q13 five, live: lost completion delivered;
  lost reply, AlreadyApplied, count stays one; primary changes while owed (the new commander's replay
  rule); named refusal yields an unresolved result; the round action runs once. The flame: the Q13 cell
  reaching ready-for-traffic, against `22bb4c156839fb2a1a3c5e1ad3b5a11c` as the before.
- **Page text.** The round checklist (states.md), fabric-state-commit.md step 3, the state-sync and
  ready-for-traffic pages, node-status.md's declaration: each states what is owed and cites §43 for
  how; exact old → new to Luke at e3's landing.
- **Sizing.** Three seats, one per owner, in order.
- **State.** not started.

### Files likely to touch

`crates/rdm-transport/src/{client,server,framer,wire,stream,pool}.rs`, `admission.rs`;
`crates/rdm-core/src/{framing,outcome,codes,node_command,status}.rs`; `crates/rdm-node/src/{node,node_self}.rs`;
`crates/rdm-admin/src/{status_rpc,fabric_rounds}.rs`; `tools/mesh-audit`; `docs/reference/rdm/{wire,spans}.md`.

## Verification

- [ ] e1, e2, e3 green in the RDM gate (`rdm-gate-at.sh <lane> fast`, then `release`); each landing's
  flame cited in its row.
- [ ] The Q13 cell (`accepted_topology::the_build_fabric_build_id_names_is_the_one_accepted_topology`)
  green under suspension across the reconcile, in the release run.
- [ ] No per-protocol retry loop remains in rdm-node or rdm-admin (the e3 controls).

## § Dispatch ordering

| wave | unit | why |
|---|---|---|
| 0 | e1 | the layer everything rides |
| 1 | e2 | the stream rule; consumes e1's id |
| 1 | e3 owner 1 (member check-in) | the failing case; consumes e1 |
| 2 | e3 owners 2 and 3 | after owner 1's pattern lands |

Critical path: e1 → e3 owner 1; e2 beside it.

## Cross-references

- `docs/plans/rulings/i144-rulings-2026-10-16-delivery.md` (R-MSG1, Q13–Q15)
- `docs/architecture/node-rpc.md` §11, §31, §33, §41, §43; `node-rpc-envelope.md` "Caller execution and certainty"
- `docs/plans/i144-provider-v2-prd.md` (Q13's origin: the e2b release run's red)
- `docs/architecture/lifecycles/node-connections-delete.md` (0x22, the first stream this covers)
- `docs/plans/i147-gossip-conformance-prd.md` (gossip's receive rules: separate, best-effort, no delivery layer)
