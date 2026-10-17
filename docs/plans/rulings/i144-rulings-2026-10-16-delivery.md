# R-MSG1 — one delivery mechanism: a directed message is owed until acked (Luke 2026-10-16)

**Luke 2026-10-16:** "a message needs a tiny ack to acknowledge receipt; otherwise keep trying with
exponential backoff. It doesn't matter if it's a message, a command or a stream: it's just a message
from A to B." And: "shouldn't retries be a framework item and not a one-off implementation."
Contract approved as Q15 (arm A) with the a1 architect's two corrections to its durability
paragraph, the same day. Supersedes the Q13 addendum and Q14 of the i144 stream.

## The rule, as §43 opens

Retry automatically in `rdm-transport`:

- Send immediately.
- Without an ack, resend the same message with the same delivery id.
- First retry at the publish cadence; then exponential backoff with jitter, capped at the staleness floor.
- One attempt in flight per message.
- The receiver suppresses duplicate delivery ids, including duplicates received while the original
  invocation is executing. Before recoverable acceptance, duplicates remain pending without an ack.
  After that boundary, duplicates are acked again without invoking the handler again.
- Stop retrying on acknowledgement, proven receiver exit or accepted removal, or explicit cancellation
  by the owning workflow.

An ack ends delivery retry. It does not mean the command succeeded. Replies and completion frames use
the same delivery mechanism; tiny ack frames do not. Primitives and workflows retain their operation
semantics, and implement no delivery-retry loops of their own.

## The contract

1. **One boundary.** The transport (rdm-transport) owns delivery identity, acknowledgements, retransmission, backoff and
   duplicate suppression, including a duplicate that arrives while the first invocation is still
   executing (held pending, unacked, until recoverable acceptance; acked without a second dispatch
   after it). The runtime dispatches an invocation at most once per delivery id; it guarantees no
   completion, and recovery of a failed or interrupted execution is the owner's. Primitives and workflows own operation identity, execution,
   results and cancellation. An acknowledgement proves receipt, never execution.
2. **Receipt is not completion; durability is the owner's.** The transport holds no durable queue.
   The receiver acknowledges responsibility only after the operation owner has made the accepted
   work recoverable: a durable acceptance record, or an already-recorded result. The
   acknowledgement is not command completion. A read-only request needs no new durable write; its
   response is regenerated from the authoritative source. The transport sends the ack; the handler
   supplies the acceptance boundary and invents no persistence or operation semantics.
3. **Reconstruction is demonstrated, never assumed.** Each message owner defines how its pending
   commands, check-ins and responses are reconstructed from durable evidence, preserving their
   operation identity. An in-memory declaration loop or round checklist alone establishes no
   recovery. A process relaunched as a new birth follows lifecycle recovery, never a replay of the
   old birth's messages. For a lost reply the evidence must regenerate the result, not merely
   recognise that the operation already ran.
4. **Delivery identity is not operation identity.** A replacement executor is a new exact birth and a
   new delivery key; a completed stop, closure or round is recognised through the operation's own
   identity and receipts. Transport dedupe covers the resend of one invocation; operation
   idempotence stays for recovery and for new invocations.
5. **Acks are not owed.** A tiny ack is never retransmitted; a lost ack is repaired by the original
   being resent and acked again. A named refusal ends the request's retry; the refusal response is
   retransmitted until acked like any response, and that is never a second execution.
6. **The whole ordered stream.** Every frame of a server-streaming op carries stream identity and
   sequence; progress frames are deduplicated like any message; a reconnect never delivers an old
   admission after the terminal frame.
7. **No cancel on absence.** An owed message ends only on its ack, on the receiver's accepted
   removal or proven exit, or on explicit cancellation or retargeting by the owning workflow.
   Supersession is declared by the owner, never inferred from a newer message of its kind.
8. **Directed messages only; nothing exempt below.** Gossip stays best-effort (R-NS2). Every
   directed Node RPC protocol rides the mechanism. Above it, unchanged, sit workflow policies: the
   Build's resubmit-by-`build_id` at the claim door, and the application connections reconnect
   scheduler (connections.md §9), which keeps its own scheduler; delivery of control RPC never
   depends on the application connections log. The resend uses ordinary dialing.
9. **Backoff.** Exponential with jitter: first retry at the publish cadence, doubling, capped at the
   staleness floor. One in flight per delivery key; coalesced.

## Framing (approved 2026-10-16, three clarifications from the a1 architect)

The delivery id never enters the observability context. Every frame in both directions carries a
transport header (`Request { delivery_id }`, `Response { delivery_id, invocation_id, seq }`,
`Ack { delivery_id }`; re-frozen 2026-10-16 as `(sender birth, delivery_seq)` with `forget_below`: duplicate
state is retired only below the sender's watermark or on its birth's end, never by time), one
authoritative copy of each field, consumed below protocol parsing; the
target section is unchanged. Responses are owed messages: routed by `invocation_id` across
reconnects, ordered by bounded `seq` (no wrap, conflicting positions refused, a terminal completes
nothing while a position is missing), acknowledged by the caller only once its owner can recover
them, and acked-and-dropped only on the owner's completion, cancellation or supersession or the
receiving birth's end, never on absence from an in-memory map. A duplicate request's `Ack` then EOF
is delivery only and completes nothing. The protocol grammar (node-rpc.md §11) is unchanged. Page
text: node-rpc-envelope.md "Transport frames", node-rpc.md §11 and §43.

## Landing order

Before an existing loop (the owed declaration, `node_self::check_in`, the mesh-primary's check-in)
is replaced by the mechanism, its owner's reconstruction and lost-result cells are green: the common
mechanism must be shown to preserve the obligation, not merely to move its retry code.

## Cells

Q13's five (lost completion delivered; lost reply, AlreadyApplied, count stays one; primary changes
while owed; named refusal yields an unresolved result without a loop; the action runs once despite
repeated sends), plus: a duplicate during execution; sender restart; receiver crash after ack;
executor replacement; interrupted-stream replay; the backoff schedule. The live flame: the Q13 cell
(a member suspended across the reconcile, continued, its check-in lost) reaching ready-for-traffic.

## Pages

node-rpc.md gains §43 (the contract, once); §33 is rewritten around the transport duplicate boundary;
§41 keeps its table with the request-versus-response distinction; the envelope's "Caller execution and
certainty" keeps NotSent and Indeterminate as execution distinctions and says a retransmission
continues the same invocation, never a new execution. The round checklist, state-commit, state-sync,
ready-for-traffic and node-status pages cite §43. Luke's yes 2026-10-16 ("a"); §43, §33, §41 and the envelope paragraph applied the same day. The round-checklist, state-commit, state-sync, ready-for-traffic and node-status citations of §43 come with the dev's landing as exact old → new.
