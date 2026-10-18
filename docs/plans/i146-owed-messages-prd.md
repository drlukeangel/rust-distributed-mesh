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
| R5 | every frame of a streaming op carries stream identity and sequence; a reconnect never delivers an old admission after the terminal frame | DONE | e1 header/order; e2 `d64f6ae2`: production 0x22 positions, completed-invocation replay, one progress write, live uncertain reissue and completed-step reuse; 1,851/0 gate | e2 |
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
- **State (2026-10-17).** LANDED `d64f6ae2` through the land script on `c3860ff7`;
  remote main verified. Concurrent gate: 1,851 passed, zero failed or ignored, 137 s (`GATE_JOBS=256`),
  logs `gates-e7s3-d64f6ae2.log` and `.log.d/`. The transport
  hands its authoritative `(invocation_id, seq)` through `ReplyStream::position` to the mesh-primary's
  progress record, before `accepted()` acknowledges recovery. The existing e1 classifier remains the
  sole delivery dedupe; no owner retry, node receipt, body field, wire or durable format change.
  RED: the 0x22 receive span lacked the header identity (`/home/admin/rust-distributed-mesh-deploy/i146-e2-red.log`, trace
  `7e04c3aec55aa321f06dec640681e442`); the real mesh-primary recorded the replayed progress once but
  omitted that identity (`/tmp/i146-e2-owner-red.log`, trace `a72884f48029744b184bae94c27f62f5`).
  GREEN: 33 delivery cells; the owner cell loses the progress ack, observes its resend on a new
  stream and proves exactly one actual book write, with the invocation and position recorded;
  three existing live 0x22 controls unchanged. The live recovery cell holds the executor at the
  DeleteConnections receipt, then at Complete in its control, waits for its exported progress record
  before killing it, and proves respectively two fresh invocations of the same birth and one execution
  with the successor reusing the durable step receipt. The closure remains connections-deleting;
  there is one completed DeleteConnections receipt across attempts. Flames:
  `0a64d8ad0690d9c6d365a698867181a1` (uncertain closure reissued) and
  `2b691ca66a129d1b9c676a9178086c2c` (completed closure reused). No question or page line.
- **Accepted e2 follow-up (2026-10-17).** RED commit `7ffef110`, cell
  `stream::tests::a_frame_handed_without_its_position_ends_the_stream_by_name`, panics on the missing
  transport position; durable log `/home/admin/rust-distributed-mesh-deploy/i146-position-red.log`.
  The receive path now ends with `StreamFailure::Violation("a response frame carries no transport
  position")`, then delivers nothing further. Original e2 RED preserved at
  `/home/admin/rust-distributed-mesh-deploy/i146-e2-red.log`. From this follow-up onward each RED is
  committed before its receive-path change and cited by SHA. Candidate `55c015ab` held: its own
  missing-position cell is green; concurrent gate red outside this unit, no rerun.
- **Outside gate read (2026-10-17).** `probe_root_span::forty_probe_runs_direct_and_carried_each_export_their_root_span`, `crates/rdm-test/tests/probe_root_span.rs:101`: run 0 answered `Reply` (:99), including the server's unary proof-store `get` / `absent`, but left zero `.spans.jsonl` files (expected one); trace `9905221ef1e627287573a3a7d8c7812e`, only its transport diagnostic file exists; evidence `/home/admin/rust-distributed-mesh-deploy/kept/i146-position-55c015ab-red/`, gate `gates-e7s3-55c015ab.log.d/rdm-test:main.log`; cell untouched. This unary probe path does not enter the changed `ReplyStream::next` path. The separate status-string landing and then Q46 remain behind the first landing as directed.
- **Q46 read (2026-10-17).** The proposed missing first-export flush is not reproduced: the existing evidence force-flush already exports pending spans through first create/write. New test-only commit `d5f02515`, `telemetry_exit::a_process_that_emits_one_span_and_exits_immediately_leaves_its_span_file`, is green (1/0, 0.01 s) with production telemetry identical to main `d64f6ae2`; log `/home/admin/rust-distributed-mesh-deploy/i146-immediate-exit-read.log`. The failed probe cell checked JSON `Reply`, but neither checked the child's exit status nor retained stderr at its missing-file assertion. Per Luke's conditional stop, packet [Q46](rulings/i146-q46-probe-exit-evidence.md) brings that evidence gap before any production fix or new gate. Stop/start is Q47. First follow-up and status rename remain held.
- **Q46: A (Luke, 2026-10-17).** The occurrence above stands as seen, not as a diagnosed telemetry defect. Both probe-root cells now retain child exit status, stderr, stdout, trace and a sorted evidence-directory listing with byte lengths in every failure message; the successful-call shape also checks the child exit status. No bound or telemetry change. This diagnostic commit creates a new candidate for the first follow-up's land script; any recurrence is read from that process's preserved outcome and file state. Stop/start remains Q47.
- **First follow-up landed / split-read correction (2026-10-17).** Q46's diagnostic commit and the named missing-position refusal landed as `61ee19f5` (121 s concurrent gate, remote main verified) before the i144 dev's new read arrived. The i144 e6.1 gate `7d3795fd` failed this lane's `delivery::a_reconnect_after_the_terminal_frame_delivers_no_old_admission` at `delivery.rs:412`, “transport header does not decode”. Root: `node_command_stream` consumed the transport header but discarded its pending body bytes and left later request sections unread; the cell then parsed remaining request bytes as an ACK header. RED commit `d0af4b4b` forces one-byte header reads and reproduces the same failure, 0/1 in 0.01 s; log `/home/admin/rust-distributed-mesh-deploy/i146-reconnect-split-red.log`. The helper now consumes the full canonical request (target, context and body) before ACK parsing; the one-byte-read control stays. No production transport or bound change. GREEN: all 33 delivery cells, including the forced split-read control; durable log `/home/admin/rust-distributed-mesh-deploy/i146-reconnect-split-green.log`. This supplemental first-follow-up landing precedes the status rename.
- **Split-read correction landed / status spelling (2026-10-17).** Supplemental first follow-up `a9d738dc` landed through the concurrent gate (114 s, remote main verified). Status-spelling RED commit `db532dba`: `model::tests::view_spelling_matches_the_design_contract` reads JSON `draining` where the contract requires `connections-deleting`; durable log `/home/admin/rust-distributed-mesh-deploy/i146-status-spelling-red.log`. `NodeStatus` and `MemberStatus` now serialize the page's `connections-deleting`; both retain postcard position 2, explicitly checked by that cell. `offline.rs`'s named spelling, the Admin UI color reader and cells consuming the JSON view move with it. The declared-state input remains its existing protocol type. No enum position or wire-body bytes move. GREEN: the spelling/position cell passes (`/home/admin/rust-distributed-mesh-deploy/i146-status-spelling-green.log`); the Admin UI `npm run build` passes. LANDED in `ba3c7124` through the concurrent gate: 1,853 passed, zero failed/ignored, 113 s; remote main verified. Q47 is next, packet only.
- **Status candidate gate reads (2026-10-17).** RED commit `8cb814b4`, `gates-e7s3-8cb814b4.log.d/rdm-transport:main.log`: `telemetry_exit`'s collector-down shape assumed a 1 s sleep established its outage. The evidence write occupied the shared export thread for 1,432 ms; its named OTLP outage followed `EXITING`, so the :134 precondition failed. The child now waits for a pipe release from the parent after the parent reads that named outage; existing 20 s child and 2 s exit bounds stand, telemetry untouched. Same candidate, `build_op.rs:899` read `draining`: `node.get` uses the distinct `rdm_core::views::NodeStatus`, missed by the first spelling change. The DTO is corrected; stored model enum JSON stays intact and the contract test addresses the actual DTO plus `MemberStatus`. GREEN: four `telemetry_exit` cells in 2.02 s (`/home/admin/rust-distributed-mesh-deploy/i146-q42-outage-sequence-green.log`) and the actual view spelling/position cell (`/home/admin/rust-distributed-mesh-deploy/i146-status-spelling-green.log`); no bounds changed.
- **Earlier gate history.** This row's landing (with Q27's `forget_below` rename): its gate on tree
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
  fix, R25's rule and its unit cell's shape. The red again on i147's tree `c4536992`, the collector up throughout (Luke checked,
  42 h): all three outage cells, slowest step `evidence-file` at 7727, 7337 and 7696 ms, every other
  step 0 ms but one `transport-diagnostic` at 495 ms (traces `337a4ba7dd621901e7ec6a13800e7d99`,
  `318443778945d7f23274f7f37d38e949`, `0821208674cb2585fddd377da130d40c`; log
  `gates-e7s3-c4536992.log.d/rdm-transport:main.log`). That step held both the JSONL create and open
  and the export runtime's first start (a thread that builds a runtime while init waits for its
  handle) with the evidence processor's build, so it is split: `export-runtime`, `evidence-file` (the
  create and open alone), `evidence-processor`. Measured on this host, a create and open in `/tmp`
  (ext4) under 64 writers fsyncing took 6.3 ms at worst, under eight 256 MiB buffered writers with
  fsyncs 5.2 ms: no seconds-long wait reproduced, so the next red's split names the defect. Host
  read on 2026-10-17: no cgroup CPU throttling at any level of the user slice, memory pressure 0, io
  pressure `some avg300=7.82` at read time, and `kernel.task_delayacct=0`, so a process's
  `delayacct_blkio_ticks` (`/proc/<pid>/stat` field 42) reads 0 until delay accounting is on. The
  cell's failure names, beside the slowest step, `/proc/pressure/io` (both lines) at the child's start
  and at its exit and the child's block-IO delay ticks at its start, after init and at its exit, with
  the host's `task_delayacct`. If the next red names `evidence-file` with block-IO ticks covering the
  stall, it is a disk wait on the gate's write load and the fix is R25's: the open moves off the init
  path to the export thread. If it names `export-runtime`, the thread-start wait is the defect. Either
  way nothing waits at init: production calls it inside `run_with`, so a wait there delays every
  node's start. Fixed at the root (Luke 2026-10-17, "nothing waits at init"): init opens no file and waits for
  no thread. The evidence file is created and opened by its exporter's first export, on the export
  thread (`EvidenceFile::OnFirstExport`; an open that fails is named once on stderr and every export
  after fails with it); the export runtime is built on the caller's thread and handed to its own
  thread to run, so nothing waits for that thread to start (`ExportRuntime::get`); the transport
  diagnostic file is created, opened and headed on its log sink's thread (`LogSink::start_opening`).
  Cell `telemetry_init::telemetry_init_returns_while_its_files_cannot_be_opened`: the child's
  evidence file and diagnostic file are FIFOs with no reader, an open for writing that blocks until
  one comes; init returned in 0 ms, every step 0 ms, and the evidence file, opened on the export
  thread once a reader came, carried the child's span. RED against main's source: the child never
  left init (killed at the cell's 20 s bound, nothing printed). Q42 (Luke 2026-10-17), a regression of that fix:
  `probe_root_span.rs:128` red on Codex's `0dcb4495`; the refusing probe's evidence directory
  (`/tmp/rdm-probe-refusal-1777274`) held a 0-byte `spans.jsonl` beside its transport-diag header, so
  its first export opened the file and the process ended before the write landed. The exit drain
  (`Exporters::drain`) shut the whole provider down under `export::bounded`, which returns at once when
  the collector is down and otherwise waits `DRAIN_BOUND` (2 s): the evidence file's first export rode
  a wait made for the collector. The drain flushes the evidence processor first and on its own
  (`SharedEvidence`, as the test capture already does), waited whatever the collector's state, then the
  collector's shutdown, both within one deadline (`export::bounded_until`); an export thread not yet
  running at the drain is named. The probe's red kept no stderr, so whether it met the collector-down
  return or the bound is not known; 60 refusing probes under disk-write load lost nothing; the probe
  cell now carries the probe's stderr. Cells: `telemetry_exit::a_process_whose_collector_is_down_keeps_every_span_it_emits_before_it_exits_at_once`
  (RED against main: the burst incomplete after the named outage),
  `telemetry_exit::an_evidence_file_whose_open_blocks_at_the_exit_ends_the_exit_flush_at_its_bound_named`
  (RED against main: the blocked flush unnamed); the FIFO init cell and `probe_root_span` green.
  The first cell went red on Q45's gate (`db5f64bf`, `telemetry_exit.rs:125`, 4,736 of 5,000 burst
  spans): under the gate's disk load each evidence write stalled 244-315 ms, and the drain ended at its
  bound naming only the collector outage's drops. Luke (2026-10-17): the bound is the contract; a named,
  bounded loss is right, an exit that waits on the disk past its bound is not. The drain now names the
  evidence loss at its bound ("the evidence flush ended at its bound (2000 ms): N of M spans not
  confirmed written to the evidence file", from the processor's handed and written counts; a write the
  bound cuts counts unwritten), and names the collector outage's drops only for the collector's
  shutdown. The cells assert the contract: every span handed is in the file or named unwritten, and the
  exit ends within its bound. The split's next gate (`bac3a549`) red at `transport_diag`: the init step spans run on that
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

#### Q47 + Q49 — stop/start command progress and applied operation effects

- **Ruling:** Q47 A; Q49 A folds into Q47 (Luke, 2026-10-17). Stop/start move only their exact-node command hop to 0x22. Appended requests 1/2 and replies 11–16 retain every previous byte; Status requests 8/16 are reserved and refused by name, replies 17–19 stand. Q49 blindly puts each applied completion under `(birth, build_id, attempt, operation)`, retains it across cycle begins, folds it on a seat takeover, and forgets it only with its birth. Intermediate progress remains recovered by transport `(invocation_id, seq)`; it does not complete the operation.
- **RED:** `d37f4753` (pre-rebase `c5b5a3ac`) pins the appended request/frame sequence in hand bytes (old decoder refuses request 1, UnknownVariant); `/home/admin/rust-distributed-mesh-deploy/i146-q47-core-red.log`. `a481ab35` proves the previous stop declaration applies again after the next cycle begins; `/home/admin/rust-distributed-mesh-deploy/i146-q49-operation-red.log` (Applied where AlreadyApplied is required). The same cell covers declaration before frame and the no-operation transition control.
- **Receive effects:** the owner records stopping only on CommandAccepted, Pending only on NodeJoined and ReadyForTraffic only on NodeStarted. NodeStarted reaches it after the node’s start gates, before the outer Build completes. No pre-send state write or separate open-start Ready declaration; Q36’s stopped declaration stays. Admission and completion share operation serialization, so a late acceptance cannot regress a completed stop. The old join-inbox registration is retained (it writes no lifecycle state); removing it was read and fixed from `/home/admin/rust-distributed-mesh-deploy/i146-q47-live.log` (node.join refused: no deployed birth).
- **Cells and current evidence:** 46 status-owner cells green; replayed NodeJoined after lost ACK is handed/recorded once (`/home/admin/rust-distributed-mesh-deploy/i146-q47-replay-green.log`); a physical stream cut after StopConnectionsDeleted still delivers the operation-named Q36 declaration and recovers NodeStopped without a second apply/publication (`/home/admin/rust-distributed-mesh-deploy/i146-q47-lost-green.log`). The unchanged live same-birth stop/start control is green (`/home/admin/rust-distributed-mesh-deploy/i146-q47-live-join-fixed.log`).
- **Conformance fixture moves:** raw fixtures send their stop/start commands on 0x22; their lifecycle assertions and bounds stay. Assertions that required the retired open-start Ready declaration now read NodeStarted from the stream owner. Q39’s owed-declaration seat-move control uses first start (no open command stream), so it still proves cancellation to the old holder and one apply at the new holder. The never-admitted control still proves NotSent sets no stopping; its admitted control proves CommandAccepted can set stopping before, never after, NodeStopped. The final-admin deletion keeps its directed command path; a new, incorrect carrier role guard was removed after its receive-path read. No bounds widened.
- **Page set:** exact old → new at [Q47/Q49 page lines](proposal/i146-q47-page-lines.md), approved by Luke (2026-10-17), including Q49 A and the preserved refusal. Includes the existing living-birth invalid-transition refusal, appended at reply 16 (FAILED_PRECONDITION); the Status reserved positions, unchanged caller/Build legs, and the Q49 row. Candidate `fa15da57` read below; approved page replacements applied with the conformance fixes, landing gate next.
- **Read.** Stop and start each remain unary Status 0x1B; ReadyForTraffic is a separate declared state. The packet proposes the existing 0x22 stream on the mesh-primary → node hop, typed progress/terminal frames, reserved old Status slots 8/16 and Q36's retained owed stopped declaration, with existing operation receipts and bounds. The page sequences and the Q6/Q7 drift are cited before the arms.
- **Live evidence.** Latest same-birth stop/start control on `ba3c7124`: stop `3a92a5fac5162d9b0f34ddd888419c8e`, start `16a226cdbe8968f020962e9db247c3f1`, start ReadyForTraffic declaration `8caaf1608894be4608bda26f865f5b46`, receiver `dad572f36ea932bd89e20573069fef78`; canonical JSONL and OTLP import under `/home/admin/rust-distributed-mesh-deploy/kept/i146-q47-stop-start-ba3c7124-member/`. All links verified in Jaeger.

- **Candidate gate read (`fa15da57`, 126 s):** `rpc/status.rs:224` required the retired Ready declaration; now proves NodeStarted is recorded by the seat holder with no second declaration. `restart_one_operation.rs:587` expected the pipeline's retired join callback; its stream-owner fixture now records NodeJoined before NodeStarted. `node_delete_pipeline.rs:85` expected the pipeline to publish Leaving after the answer; now checks the owner-held Leaving and no pipeline publication. `status_declarations.rs:2528` exceeded its 5 s recovery bound after the physical cut; diagnostic read continues, bound unchanged. Logs: `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-fa15da57.log.d/`. The gate's failure-artifact copy also lacked exported DEPLOY in its job subprocess; export fixed so future reds retain their evidence.
- **Lost-stream read:** the new rig bound the primary’s key twice, with the open command on a client-only endpoint and its declaration server on another endpoint and Delivery. Owed responses reconnect by peer endpoint id; the fixture did not provide the runtime’s one router/shared Delivery holding the invocation. The command client now uses the primary server’s endpoint and Delivery; the physical cut, declaration-before-terminal assertion, once-only effect and 5 s bound stand.
- **Targeted confirmation:** the corrected shared-endpoint lost-stream cell is green in 0.02 s (`/home/admin/rust-distributed-mesh-deploy/i146-q47-lost-shared-endpoint.log`); the complete concurrent gate remains the load proof. Both format markers and Status/NodeCommand reseals are retained.
- **Landing gate `2b45cba3`:** one red, `node_delete_pipeline.rs:120`: the rig’s direct stream owner recorded frames 0/1/2, but the cell still selected the removed unary helper’s `via-command-sent`; trace `1f28c29271e1c13bc8f9f558f67122a5`. It now proves the actual owner’s ordered acceptance, closure and completion under the operation, retaining every step/receipt/storage assertion and bound. Status routing, operation effects, physical-cut recovery and all other gate jobs passed. Log `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-2b45cba3.log`; tree recorded red, never rerolled.
- **Live flame (`2b45cba3`):** stop `f1a56cba2ecf9e3cb5b690f2868fac09` has owner frames CommandAccepted/StopConnectionsDeleted/NodeStopped at 0/1/2; start `63febf04aa0b96f536fec7b3fc25c9de` has CommandAccepted/NodeJoined/NodeStarted at 0/1/2, all recorded by mesh1.admin.1. Jaeger queried and verified (114/80 spans). Canonical JSONL, OTLP and Jaeger JSON are retained in `/home/admin/rust-distributed-mesh-deploy/kept/i146-q47-stop-start-2b45cba3-member/`.

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
  failing at 3001 ms; the gates' `indeterminate` landed inside it).
- **Q37 (Luke 2026-10-17, A).** The four page lines landed with the code (node-rpc-envelope.md's
  `DeclareNodeState` schema and `DeclaredOperation`, node-stop.md steps 4-5, node-status.md's
  declaration). Next landing of e3: a node-admin's own declarations name their operation too
  (`status_declare.rs`), so one rule holds for every node. A re-sent Leaving reaching a new seat
  holder after its seat fold finds the declared row held (`AlreadyApplied`) and publishes nothing; were
  NodeStopped ever published twice, i147's receive rule makes the repeat of an operation fact do
  nothing at every receiver. Cell, same landing: a parked birth whose mesh seat moves re-declares its
  Leaving to the new holder, which publishes NodeStopped once across the move.
  Built: a node-admin's own declarations name their operation. Its declarer holds the operation of
  each own state of its current cycle (`Declarer::own_operations`): `begin_own_cycle` takes the start's
  operation, so its node.started names the start; its NodeSelf declares (`boot.rs`), so its stop's
  Leaving names the stop, owed in the declarer and sent before the stop's reply (the declarer's own
  turns are held while it is stopped); a state it measured itself (its birth's node.started, a
  Draining) names none; `via-declare` carries `operation`. Cells:
  `status_declare::a_node_admins_own_declarations_name_the_operation_that_produced_them`;
  `node_stop`'s stop-then-start asserts, for the rpc node and the node-admin alike, that the stop's
  Leaving names the stop and the start's applied node.started names the start (RED against main's
  rdm-admin: "mesh1.admin.2's stop declared its Leaving naming the stop"). Seat-move cell
  `node_stop::a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_seat` (two meshes;
  mesh2's non-primary rpc node stopped, then mesh2's primary, whose Build moves the seat first):
  NodeStopped for the stopped birth once, before the move; the new holder's seat fold holds it
  (`stopped`); the old holder's own Leaving names its stop, applied by the new holder, its NodeStopped
  once (flame `ca58d36f579bae426b20cf6db5055c32`). The stopped birth re-declares nothing across the
  move: its gossip was cut at its stop, so it hears no "primary updated" until its start rejoins (its
  `via-authority-moved` comes at the start, state Pending). The re-sent Leaving of the note does not
  occur; the new holder holds the row through its seat fold. The stop's transport-cut span ends with the cut now
  (`node_self.rs` step 5): with the admin's declaration after the cut, the span had stayed open across
  the declaration's round trip, and
  `node_stop::a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops` read the
  successor's "primary updated" (its holder-stopped at +196.9 ms) as inside a cut that ended at
  +198.4 ms, after the declaration. The gate on `6e5f7532` red at
  `fabric_primary_handover.rs:454` (two "primary updated" announcements): a deleted mesh's last admin,
  still its own mesh's primary, declared its stop to itself after its cut, so its own authority
  published its NodeStopped and its holder-stopped announcement on a parked channel, and its commander
  published NodeStopped again. node-stop.md step 4: after its cut it publishes nothing. An admin that
  is its own mesh's primary declares no stop (`rdm.node_admin.status.reject.via-own-mesh-primary`); the
  cell asserts that span and the last admin's NodeStopped once. Q39's occurrence:
  `node_stop::a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth`
  red once in three local runs at :573: the resumed birth's `declare_once` addressed only
  `mesh1.admin.2` (`RejectedNotAuthority`, the book not yet hearing `mesh1.admin.1`), so NodeStopped
  came from the stop stream (trace `2f1267dca2a797afb6fd18ae6325ac29`); the declaration's addressee is
  the admins the digest book hears, not the mesh's primary.
- **Q39 (Luke 2026-10-17, A).** "A node's declaration is owed to the holder of its mesh's primary seat,
  resolved from the seat (the LeadershipView), never to an admin its book happens to hear. Unreachable
  holder: the message stays owed under §43 and is delivered when the holder is reached, direct or
  carried through a live peer on Forward (the tickle ladder's route rule), never redirected to a
  non-primary. The seat moving is the one retargeting: the owner (the node) re-owes the declaration to
  the new holder when the seat row names it, and the old holder's copy is cancelled; a late delivery
  to the old holder is refused by its store fence (Q28) and recorded nothing." Cells: a suspended node
  that can reach only a non-primary admin on release still delivers its stopped declaration to the
  primary (direct when reachable, carried otherwise); a seat move while the declaration is owed
  delivers it once, to the new holder. Built: `declare_once` (`rdm-node/src/node.rs`) addresses the
  holder of the mesh's primary seat from the birth's seat record (`membership.seats()`), direct first;
  a direct attempt that sent nothing is carried by Forward through up to two live peers of the mesh at
  once (`route` on `via-declare-own`: `direct` or `carried:<peer>`); with no holder held nothing is
  sent and it stays owed. Every declaration path (the stop's, the start's node.started, the declare
  loop) runs under `declare_until_seat_moves`: the mesh's seat record naming another birth cancels the
  call in flight (`rdm.node_rpc.status.remove.via-seat-moved`) and the declaration stays owed to the new
  holder. The fault door gains a declaration cut (`CutSpec::Declaration`, through the status door's
  `wiring::DeclarationSeam`). Cells: the never-admitted cell asserts the stop's declaration addressed to
  mesh1's primary only and applied by it (its red occurrence: trace
  `2f1267dca2a797afb6fd18ae6325ac29`);
  `node_stop::a_declaration_owed_across_a_move_of_its_mesh_seat_is_delivered_once_to_the_new_holder`
  (two meshes: mesh2's primary holds the restart's node.started at its cut, so the restart's readiness
  wait fails naming the node.started its primary never took; mesh2's primary is stopped and the seat
  moves; the node's call is cancelled at the move and its node.started applied once by the new holder,
  never sent to it before its install; flame `89914ed455aa3e71a5a12b46220078f7`). The old holder's late
  delivery refused by its store fence (Q28) is e6's: the fence is built there, and that cell with it.
  `node_restart::a_restart_whose_stop_was_never_admitted_resumes_stop_then_start_on_resubmission` red
  once before this landing at :220 (the member listed Leaving, not Restarting): its attempt went out at
  the unheard mark, and the stop-node committed on a connection still alive (`indeterminate`, attempt
  2), running on release. The idle timer runs from the member's last packet, the unheard mark from its
  last digest, so the mark proves no connection is gone. It and the never-admitted stop cell now
  dispatch once the ladder records the member os-stopped, after its direct probe failed. The gate on
  `563b9d3b` red twice, both cells that held what Q39 changed: `status`'s ladder cell asserted the
  path-order ladder Q39 removed (now
  `status::a_node_declares_to_its_seat_holder_and_an_admin_holding_none_answers_its_own_seat_decision`:
  the restarted node's node.started to the seat's holder alone, and a declaration sent straight to the
  admin holding no seat answering its own seat decision, `ReceiverNotPrimary`, R24 Q11); and
  `admin_start_declares` commanded an admin's start from the root's client, skipping the pipeline's
  `begin_cycle`, so since e3's admin Leaving its node.started was refused backward
  (`RejectedInvalidNodeTransition { current: Leaving }`); the rig begins the start's cycle at the root's
  status authority first (`Running::status_authority`), as `send_command` does.
- **Q37's note withdrawn (Luke 2026-10-17).** The re-sent Leaving across a seat move does not occur: the
  holder already holds the row through its seat fold and a stopped birth re-declares nothing
  (`551d3f63`, the seat-move cell). The gate on `44488b29` red at
  `node_connections_delete.rs:204` (a mesh-primary's own All: its record at +7.88 ms before its
  report's span at +8.40 ms, trace `bc35a360fc2a6ac900320ea6250e2170`): the report's span opened
  after the report was sent, and the in-process recorder took it first. The span opens before the
  send now and records its outcome after.
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
  The gate after (`360c69d4`) red at `node_stop.rs:166`: the parked birth's post-cut dial for its
  declaration is the connection the start then rode; the cell counts a dial after the cut by either
  end. The same gate's `flood` red, its reason named (`Indeterminate(ReplyLost("the request's attempts
  ended"))`, trace `953088dfb155d2b745f9b890aef34316`), went to Luke as a packet (Q32's rule). Q38 (Luke
  2026-10-17, arm A): every return of the attempt task carries its path as a typed value
  (`AttemptEnd`, one per return: `complete-at-turn`, `send-deadline`, `committed-connection-closed`,
  `receiver-gone`, `hold-lost`, `unreached-final`, `ended-pre-commit`, `refusal-read`,
  `stream-complete`, `stream-violation`, `reset`, `lost-asked-once`), spanned under its call
  (`rdm.node_rpc.delivery.update.via-attempt-ended`, `path`; info, as `via-call`, so a gate's held
  flame carries it), and a call that ends Indeterminate because its attempts ended names that path in
  its reason (`the request's attempts ended (<path>)`), with `slot-closed` when the invocation's frame
  channel closed with no frame; a stream's `the stream ended before its terminal: its attempts ended
  (<path>)` the same. No change to when a call ends. The occurrence on `360c69d4`: 1 of 7480 calls,
  trace `953088dfb155d2b745f9b890aef34316`, one attempt, the handler served at +12.5 ms, the call
  ended 33 ms later with no resend either way; its path was not recorded (the attempt's return
  carried none). The next recurrence names its path; if it is `delivery.rs:516-520`'s order (the slot
  read, then `complete` marked by another path, the frame dropped as a response of a completed
  invocation), it is an ordering defect and gets its own packet. The call's frame arm is no longer
  polled once its channel closes: `None => continue` under `biased` took that arm on every turn until
  the scheduler's budget let the attempt channel's end through. Cell
  `delivery::every_return_of_a_requests_attempt_task_is_spanned_with_its_path`: a served call ends
  `stream-complete`, a reply direction reset with 423 ends `reset`, a proven exit ends
  `receiver-gone`, each under its own call (flame `41387062b3b5340debf1f58a1e3aa440`). A
  `stream-violation` end can go unspanned: the call ends on the violation it was handed and aborts
  the task at its reset write, and the call's own reason names the violation.
  The Q33 split's gate (`8360d55e`) red at
  `node_restart::a_restart_whose_stop_was_never_admitted_resumes_stop_then_start_on_resubmission:183`
  (red before e3 too: `184aa116`, `c393b290`): the suspended member's first stop-node is sent or not
  by timing, and when it is sent it runs on release and the member parks (listed Leaving, its
  declaration applied), not held restarting. The cell now dispatches the attempt only once the
  mesh-primary marks the held member unheard (the faulted fabric-primary's claimed-attempt cut), so
  its stop-node is never sent, and waits out the member's unheard marks before reading its status.
- **Owner (2), the member check-in (2026-10-17).** How it is lost: the mesh-primary marks a member's
  command `Answered` on its `Applied` (`fabric_rounds.rs` `spawn_command`, :716-719) and `take_due`
  never sends an `Answered` command again (`fabric_state.rs:274`), so the member's check-in
  (`node_self.rs` `check_in`, one call with `CallOptions::default()`, `Budget::Overall(10 s)`,
  `client.rs:95`) is the only path to the checklist; a call whose budget lapses ends
  `delivery=cancelled` and nothing sends it again. Lost-result cell
  `rounds::a_members_check_in_owed_while_its_mesh_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready`
  (two meshes; a resize of mesh2; the fault door's new `round-action` cut holds mesh2's other admin in
  its commit-state action; mesh2's primary suspended past the transport's idle timeout, the action
  released, the primary held past 10 s, continued): RED on `0d398dc6`, the check-in's call
  `NotSent`/`Deadline` at 10000 ms after 4 attempts, `delivery=cancelled`, its completion `NotSent`, the
  fabric in StateCommit 60 s later (flame `4944908eb4d8dea7ab16feb1ad947140`). With the primary
  suspended while a pooled connection was still live, the request committed into the suspended
  primary's receive buffer, the call ended `Indeterminate` at 10 s, and the primary applied it on
  continue: the loss is a request never written. Reconstruction after a restart of the same birth
  owes nothing here: a restart while the round is open is refused `build-in-progress`
  (`control_plane.rs:199-201`), and a later attempt supersedes the round (`fabric_rounds.rs:41-44`),
  whose check-in the primary answers `NotReady` naming the round it holds (`fabric_state.rs:428-436`);
  the member's round record (`rounds_seen`) lives with its birth across a stop and start, and a
  re-command is checked in from it without running the action again (`serve_round`'s `CheckIn`).
  Already-applied by operation identity: `fabric_state::tests::a_check_in_counts_only_as_the_planned_birth_of_the_open_round`
  (a repeat check-in of the round key and birth is `AlreadyApplied`) and
  `command_natural_key::a_round_sent_again_by_the_new_seat_holder_is_the_same_round_and_its_action_runs_once`.
  Every budget shape ended its call at a finite deadline (`client.rs:803-805`, `:1150`), so no call
  could be owed until its §43 ends alone. **Q44 (Luke 2026-10-17, A).** `Budget::Owed`: a fourth
  budget shape with no send or reply deadline. A call under it ends only on its reply, on the
  runtime's proof that the receiver is gone (exited, removed or superseded, `client.rs:1096`), or when
  the owner drops it; the resend schedule runs unchanged underneath it. Local to the client; no wire
  change. Its users are the reports and completions an owner owes with no bound of its own: the
  member's round check-in, the mesh-primary's check-in, the owed declaration; their loops are deleted.
  A command with an owner's bound keeps its finite budget; Owed is never a default. Page text applied
  to node-rpc.md §34 the same day (the budget list gains `Owed` and its paragraph; "an Owed call has
  no bound and lapses never; it ends as §43 says"). Cells: the failing cell above, green; a check-in
  to a primary proven exited ends receiver-gone, `UNAVAILABLE`, and the member owes it again to the
  new holder on the seat move (Q39's rule); an owner dropping its Owed call ends the resend; the
  control that a Split or Overall call still lapses as before.
  Built: `Budget::Owed` (`client.rs`: no send or reply deadline; each attempt is bounded by the floor,
  and an Owed attempt carried by a peer stamps what remains of it; `investigate.rs`'s carried budget
  keeps Owed). The member's check-in (`node_self.rs` `check_in`) is one Owed call; it is retargeted
  when the seat of its mesh names another node after the call began (the call dropped), or after a
  call that ended with no answer (`rdm.node_admin.status.update.via-completion-retargeted`, `why`
  `seat-moved` or the end); the hook runs once. The exit arm: the Q44 packet said `client.rs:1096`
  ended a call on its receiver's proven exit; it did not. The live resolver answers `Unavailable` for
  a birth whose exit is proven (`live.rs:273`) and `receiver_gone` ended only on `Gone` (the
  removal), so a call in flight was resent to a birth proven exited until its bound. Both checks
  (`client.rs` `receiver_gone`, `delivery.rs` `birth_ended`) now end on it, as node-rpc.md's outcome
  table states ("receiving birth proven exited or removed ... `UNAVAILABLE`"). Cells: `delivery` 24-28
  (`an_owed_call_outlives_every_bound_and_is_delivered_when_its_receiver_comes`; the Overall and
  Split control; the owner's drop; the removal and the proven exit, each `UNAVAILABLE`, 28 RED
  without the exit arm); `rounds::a_members_check_in_owed_while_its_mesh_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready`
  GREEN (the Owed call 13046 ms, 6 attempts, the backoff capped at the 3 s floor, applied; flame
  `5bd8ac58e3cb08a31432292058c8de23`);
  `command_natural_key::a_check_in_owed_to_a_removed_primary_ends_unavailable_and_is_owed_to_the_next_holder`
  (in process: the removal ends the owed call `UNAVAILABLE`, the seat's move retargets it, the next
  holder takes it, the action once); `rounds::a_members_check_in_owed_to_a_primary_that_dies_is_owed_to_the_new_holder`
  (live: the primary killed while the check-in is owed to it; the seat's move and the exit's proof
  reach the member a few milliseconds apart, the exit row first in one run (20826 against 20833) and
  the seat first in the next, so the live cell takes either end: retargeted, answered by the new
  holder, the fabric ready; flame `45f6e0eea6a17d42ecb3b81c9a767560`). Control: one completion call
  per owed check-in, its resends the layer's `via-resend`; the member had no loop of its own.
  Landed `4bab74ab`.
- **Owner (3), the mesh-primary's check-in (2026-10-17).** It rides the declarer (`status_declare.rs`
  `round`: one `CallOptions::default()` call per due key, sent again only on an eligible event).
  Lost-result cell
  `rounds::a_mesh_primarys_check_in_owed_while_the_fabric_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready`
  (mesh1 the fabric-primary alone; mesh2's primary held in its own commit-state action; the
  fabric-primary suspended past the idle timeout and 10 s): RED on `4bab74ab`, 2 of 2, but not on the
  check-in. The declarer held it while the fabric-primary was unheard, sent it on resume (`not-ready`:
  the round book had been cleared), and again on the re-command (`applied`). The fabric then held on
  `mesh1.admin.1(no-check-in)`: suspended past the floor, the fabric-primary is cut off and publishes
  as no primary (`boot.rs:2244-2250`), `climb_step` resets its climb and clears its fabric book
  (`fabric_rounds.rs:268-276`, `:391-394`); healed, it adopts state-commit and re-opens the round
  (`:278-287`) but re-spawns its own mesh's driver only when its mesh book does not hold the round
  (`:661-664`), and the book still holds the completed one, so its own `check_in_local` (`:840`) into
  the cleared book is never made (flames `d3e5b7cee23c2e01bd84b82e1c515b14`,
  `7ca8f6f37931b1803a56812bbd3264d4`). **Q45 (Luke 2026-10-17, A).** The climb reset on a cut-off
  clears the fabric-primary's own mesh book with the fabric book. On healing, the re-opened round
  re-commands every mesh primary and re-spawns its own mesh's driver: members already done answer
  AlreadyApplied and check in again from their record without re-running their action, the
  fabric-primary's own check-in is made again from its record, and the fabric book rebuilds. rdm-admin
  only; no wire or frame change. Built: `fabric_book_reset` closes the tenure's own mesh rounds
  (`rdm.node_admin.fabric.remove.via-tenure-ended`); a mesh round's own action is recorded per round
  key (`FabricRounds::acted`; `via-mesh-round` `own_action` `run` or `from-record`); and a mesh
  primary asked again for a round it completed owes the completion again from its record
  (`Declarer::owe_again`, `rdm.node_admin.status.update.via-asked-again`): its declarer re-owed only
  when the authority was another birth, and a healed fabric-primary is the same birth (the
  open-traffic shape: mesh2's primary answered the re-sent commit-state `already-applied` and never
  checked in again). Cells, each asserting every round action once per round key (the fabric-primary's
  and mesh2's primary's `own_action` `run` once, each member's `acted` once):
  `rounds::a_fabric_primary_cut_off_during_state_commit_re_runs_its_own_mesh_round_and_the_fabric_reaches_ready`
  (flame `10472d81ffe9df63c1fd604dfebf9928`), the same during open-traffic (flame
  `72d8353cea2ebd2be7481cdb8a66172e`), and the fabric-primary alone in its mesh, owner 3's lost-result
  cell (flame `e9fadecbfb812f8f526697d038aee08c`), all RED on `4bab74ab` and GREEN; the mesh
  primary's check-in rode the declarer's own resend in each. Owner 3's move onto `Budget::Owed`
  follows. Its first gate (`db5f64bf`) was red twice. `fabric_primary::losing_the_fabric_primary_mesh_moves_control`
  (`fabric_primary.rs:240`, no `via-recovered`): the cell stopped the fabric about 100 ms after the
  rebuilt mesh's new rpc.3 sent its first direct digest (`:73760` against `:73857`), before the mesh's new
  primary's status round completed and reported the mesh ready (`round.rs`: a fresh digest from each
  planned birth after the round began); the cell asserts that report and never waited for it, and now
  waits for the fabric to read ready-for-traffic before its stop. And `telemetry_exit` (main's; its own
  row in e2). The five check-in and cut-off cells (each about 55 s) moved from `rounds` to their own
  stem, `owed_check_ins`, release-only in the gate (Luke: "55 s cells belong in release"). Its second gate
  (`c911edb4`) was red in six stems; five were refusals I caused, editing `rdm-transport/src/telemetry.rs`
  in the lane while that gate's cells ran ("`faulted-node-admin` was built before ... `telemetry.rs`
  changed"). The sixth, `sticky_seat::a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts`
  (`sticky_seat.rs:579`), is the provider's: the recipe's `node.delete:mesh1.admin.1.old` failed
  `TerminateRuntime` with `stage=unproven` 74 ms in ("mesh1.admin.1.old still runs after the stop
  ladder"), the old holder being the estate's adopted Day-0 runtime; Q45 changes no termination. Open,
  to be read (trace `b241a08f397cdc549c10970b44268948`).
  Owner 3 moved onto the layer: each declaration the declarer sends (a mesh primary's round
  completion and Mesh report, an admin's own state, the fabric-primary's events) is one `Budget::Owed`
  call the declarer holds per key (`status_declare.rs` `round`, `Inflight`); a key whose call is in
  flight to the destination the view names is not sent again, a new destination drops the call and
  owes the key there (`rdm.node_admin.status.update.via-declare-retargeted`), and a withdrawn key aborts
  its call. The loop is deleted: `Sent.route_lost` and the eligibility it gave a failed delivery whose
  route returned. Control `status_declare::tests::a_failed_delivery_is_the_layers_and_the_declarer_sends_it_again_only_to_a_new_destination`
  (a returned route sends nothing again; the authority addressing this admin does). Lost-result proof:
  `owed_check_ins::a_mesh_primarys_check_in_owed_while_the_fabric_primary_is_suspended_is_delivered_and_the_fabric_reaches_ready`
  green on the Owed call (the release stem 5/5). The round book's down commands keep their own record and
  route-regained resend (`fabric_state.rs` `Sent`, `eligible`): the mesh primary's commands are the next
  owner, not this one's. Its first gate (`36aaae4b`) was red at
  `node_stop::a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_seat` (`node_stop.rs:1103`):
  the old holder's Leaving reached its successor `already-applied` 19 ms after the stop stream's
  `stopped` had recorded it; the cell asserted `applied`, the order the awaited call used to give. Q36
  publishes NodeStopped once from whichever arrives first, which the cell's next assertion holds; the
  declaration's own answer is either.
  A duplicate send by the declarer (2026-10-17, land gate `ad8633d6`,
  `admin_start_declares::a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again`:
  mesh1.admin.2's ReadyForTraffic declared twice to mesh1.admin.1 as two calls 16 ms apart, `applied` then
  `already-applied`): a call's end released the key's in-flight claim before it recorded the end, and a
  round whose `due` read predated the end found neither the claim nor the answer and sent the key again.
  The end is now recorded before the claim is released, and the send is decided under the in-flight
  lock from the key still being owed (`Declarer::still_due`). Cell
  `status_declare::tests::a_send_decided_before_the_keys_call_ended_is_not_made_once_that_end_finished_it`:
  RED on the old decision (the stale read sends the answered key again), GREEN after; rdm-admin lib
  458/458, main 421/421, `admin_start_declares::`, `status::`, `node_stop::`, `build_op::` 30/30.
- **Order (Q29).** (1) the owed declaration (`node.rs:651-670`, `declare_once`), the first live proof
  being the node_stop cell above; (2) the member → mesh-primary round check-in (`node_self::check_in`);
  (3) the mesh-primary → fabric-primary check-in. For each, first the owner's cells: reconstruction of its pending messages from its
  durable state after a restart of the same birth, with operation identity; regeneration of a lost
  reply's result; the already-applied answer by operation identity on a resend that outlived the
  duplicate table. Then the loop deleted and the message handed to the layer. Then the control: the
  owner's old retry span is gone.
- **Read for the rpc node's declaration owner (2026-10-17).** `fabric_primary::losing_the_fabric_primary_mesh_moves_control:164` red on main `ba3c7124` (docs-only gate `98a739db`): the estate's stop named "still running when the stop bound passed, killed: pids [413887, 441609]"; 441609 is the recovered `mesh1.rpc.1` (`fkxjbtgdfa93`), whose leave kept declaring `Leaving` to its primary `ka72skxtmtm0` while that primary was going down too, every carried route `NotSent: Deadline`, past the 30 s bound; the live-runtimes check ran right after the bound's `kill -9`. A node's local stop that waits on a declaration to a primary that is leaving is this owner's (the rpc node's declaration loop in `node.rs`).
- **The Build watcher's reattach (Luke 2026-10-16).** Q22 made reattach by `build_id` the owner's
  job after a `Budget::Stream` connection closes; neither `rdm-client`'s `BuildStream` nor the cells'
  `until_complete` does it, both treat a break as a defect (build.md: "a live run is reattached, never
  re-claimed"). The Build watcher is an owner that demonstrates reconstruction: reattach by `build_id`
  after a connection close, resume from the receipts. Cell: a watched Build whose connection closes
  mid-stream reattaches and ends `Complete` with no step run twice. Not part of R24's landing.
  Landed `bfb6078c` (reattach by re-submit). The envelope's reattachment section (node-rpc-envelope.md
  "Frames, durable receipts and reattachment": "`build.get` for the receipts, then the outer stream
  again, resuming from the receipts") makes each reattachment a `build.get` first:
  `BuildCarrier::reattach` reads the receipts, then re-submits from the current attempt they name, and
  `follow_with` keeps each completed receipt once as its step frame before reading the stream again;
  the cell helper (`Estate::follow`) reattaches the same way. Cell
  `build_op::a_watched_build_whose_connection_closes_mid_stream_is_reattached_and_completes_with_no_step_run_twice`
  now asserts a `build.get` per reattachment: RED on `afff4d44` (1 reattachment, only the cell's own
  read), GREEN after (flame: the reattachment's `build.get`
  http://192.168.68.99:16687/trace/e11ad210a16f1a5b2f91750bbd732055, then the outer stream again
  http://192.168.68.99:16687/trace/2f113923fc46c101a5f11cf4939bb4ec, `complete`); `build_op::` and
  `sticky_seat::` 17/17, rdm-client 3/3. Land gate `78bd4dc5` (2026-10-17) red on two stems, NOT LANDED, neither on a path this change executes: `rdm-transport:main`'s `collector_outage` ×3 ("the drain took until 6507 ms", Q42's drain bound, Codex's; red on gates `092666b8` and `e7e0ecc5` too; 4/4 on this branch alone); and `node_stop::a_declaration_owed_across_a_move_of_its_mesh_seat_is_delivered_once_to_the_new_holder` (`node_stop.rs:1166`, the stop of mesh2's primary refused `BuildInProgress`): the fabric-primary's drift opened attempt 3 of the cell's failed create Build, `surplus: mesh2.rpc.2` (the node the failed create left live), 16 ms before the cell's stop (+86976 against +86992 ms); the cell reads its streams with `WorkflowStream::next` and made no `build.get` and no reattachment, so `follow_with` never ran in it. The race is main's: the surplus retire of a failed create's live node against the cell's next submit (2 of 2 on this branch under a concurrent gate, load 831; 1 of 1 green on `afff4d44` at load 279, where no surplus attempt opened in the 30 s before the stop). Its re-land gate `82b2c5c7` (on `a5ecb27d`, the box quiet) red once in `rdm-test:main` on `admin_top_up::a_node_admin_tops_up_a_desynchronized_source_from_its_own_mesh_primary` (`admin_top_up.rs:206`: mesh1.admin.2's top-up refused `seat-holder-not-held`, "the seat record names mesh1:ye12hap5m0qg@g8texq0p448a (revision 1); no digest of that node is held"), an in-process cell that makes no Build call, green alone on the branch (0.42 s). Re-landed on this record's tree.
  **State: done.** `rdm_client::follow_with` / `BuildCarrier::follow` read the stream; a break
  (`Indeterminate`) or a `SeatMoved` frame reattaches by `build_id` with `resubmit(build_id, attempt)`,
  steps de-duplicated by (attempt, operation, step), ending on `Complete | Failed | Got | Deleted`;
  `Estate::follow` is the cells' watcher. Cell
  `build_op::a_watched_build_whose_connection_closes_mid_stream_is_reattached_and_completes_with_no_step_run_twice`
  (closes the pooled connection after the first frame): build_op 11/11 in one run (67.6 s). Flame
  http://localhost:16686/trace/0ae31b6ff68f027ebb2d870e284ceadb: Build `ahjmn2e1qvry`,
  `rdm.node_admin.build.serve.via-node-rpc` twice (the broken stream, the reattach), both `outcome=complete`.
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

- **Q52 = B** (arch, relayed by Luke, 2026-10-17; `docs/plans/rulings/i146-q52-owed-declaration-route.md`): the node's owed declaration is one `Budget::Owed` call; the owner supplies receiver and carrier policy, the route projection picks the carrier per attempt (§45), the Owed call re-resolves its route on every resend instead of once (`client.rs::owe`), direct first then carried; the declaration loop is deleted; the obligation ends on the holder's answer, proven exit or the owner's retarget on a seat move. Transport change authorized for this scope. Dev, lane e4s8.

- **The retired word in code (Luke, 2026-10-17: "draining is not a thing").** The JSON spelling moved to `connections-deleting` at `ba3c7124`, but 211 identifiers still say it: `NodeState::Draining` and `MemberStatus::Draining` (→ `ConnectionsDeleting`, postcard position 2 kept, checked by the existing spelling cell), `Node::drain` (→ the signal stop's connections delete), `drain_deadline_from_env` and the env var `RDM_DRAIN_DEADLINE_MS` (→ `RDM_CONNECTIONS_DELETE_DEADLINE_MS`, the page's one bound for `All` and for a signal stop), and the span and cell names that carry them. Beside it, the signal-stop path has no page: `MemberStatus::Leaving` / `NodeState::Leaving` and the `RDM_LEAVE_LINGER_MS` linger that announces Leaving on membership contradict node-stop.md ("the node gossips nothing") and gossip.md (`NodeLeaving`/`NodeLeft` are reserved, never published); both variants become reserved positions refused by name, like those frames, so nothing shifts. The dev lands all of it with Q55, since Q55 is in `drain` and `stop_inner`. CLAUDE.md's banned list now carries the name check's whole set.
- **Q55 recommendation (lead, 2026-10-17; awaiting Luke): x.** A signal stop is node.stop's own steps run by the node as its own subject (node-stop.md steps 2–4): `node.connections.delete { All }` under `RDM_CONNECTIONS_DELETE_DEADLINE_MS`, the transport cut, then `Stopped` declared to its mesh-primary as an owed message that the process exit drops (§34), no gossip, no linger. Neither A (keep Draining/Leaving, stop waiting) nor B (self-declared exit proof). The mesh-primary learns the exit as it learns any exit: the Stopped declaration if it lands, else the provider's proof (R-FM1/R-FM2). Commanded stop on 0x22 unchanged.

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
