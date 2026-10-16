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
- **Design points.** The id in the request context; the ack as a reserved control frame on the same
  connection (one frame, the id); the sender's in-flight table in memory, keyed by id, with the
  schedule; the receiver's duplicate table keyed by id with two states (pending, acked) and a bound
  (ids older than the staleness floor past their ack are forgotten: a resend after that is a new
  delivery the owner's operation identity recognises); the handler's acceptance report through
  `HandlerScope` (recorded / read-only); resends through the ordinary dial and pool, never the
  application connections log.
- **Acceptance cells.** `a_message_whose_connection_drops_before_the_ack_is_resent_with_the_same_id_and_acked`;
  `a_lost_ack_is_repaired_by_the_resend_and_the_handler_runs_once`;
  `a_duplicate_during_execution_stays_pending_unacked_then_is_acked_once_without_dispatch`;
  `a_read_only_request_is_acked_at_once`;
  `a_named_refusal_ends_the_requests_retry_and_its_response_is_delivered_until_acked`;
  `a_receiver_proven_exited_ends_the_owed_message_and_a_silent_one_does_not`;
  `an_owner_cancellation_ends_it`;
  `the_resend_schedule_is_publish_cadence_then_doubling_with_jitter_capped_at_the_floor`;
  `a_resend_uses_the_ordinary_dial_and_reads_no_connections_log`. RED first.
- **Page text.** Landed 2026-10-16 (§43, §33, §41, the certainty paragraph). The envelope's §1 context
  row for the delivery id and the reserved control frame: exact old → new to Luke at e1's landing.
- **Sizing.** Two seats; `rdm-transport/src/{client,server,framer,wire,stream}.rs`,
  `rdm-core/src/{framing,outcome,codes}.rs`, `tools/mesh-audit`.
- **State.** not started.

#### e2 — the ordered stream: identity and sequence on every frame

- **Goal → gap.** §43's stream sentence; node-rpc.md §11, §31. 0x22 frames carry order but no
  identity or sequence; a reconnect could replay an old admission.
- **Requirements.** R5, R9 (0x22).
- **Acceptance cells.** `every_frame_of_a_stream_carries_its_stream_id_and_sequence`;
  `a_reconnect_after_the_terminal_frame_delivers_no_old_admission`;
  `a_progress_frame_delivered_twice_is_suppressed_and_the_record_is_written_once`;
  `an_interrupted_stream_is_replayed_from_its_receipts_not_re_executed` (e2b's connections.delete). RED first.
- **Page text.** node-rpc.md §11 and §31 gain the sequence rule: exact old → new to Luke.
- **Sizing.** One seat; `rdm-transport/src/stream.rs`, `rdm-core/src/node_command.rs`.
- **State.** not started.

#### e3 — the owners move onto it, one at a time, each after its reconstruction cells

- **Goal → gap.** R7, R8. Three loops, one missing; none demonstrates reconstruction.
- **Order.** (1) the member → mesh-primary round check-in (`node_self::check_in`: the failing case);
  (2) the mesh-primary → fabric-primary check-in; (3) the owed declaration (`node.rs:651-670`,
  `declare_once`). For each, first the owner's cells: reconstruction of its pending messages from its
  durable state after a restart of the same birth, with operation identity; regeneration of a lost
  reply's result; the already-applied answer by operation identity on a resend that outlived the
  duplicate table. Then the loop deleted and the message handed to the layer. Then the control: the
  owner's old retry span is gone.
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
