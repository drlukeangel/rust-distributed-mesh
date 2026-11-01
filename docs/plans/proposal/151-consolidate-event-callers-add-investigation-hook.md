# i151 proposal: one `Resend` for every Node RPC call, and the `on_node_investigated` hook

Status: ruled by Luke 2026-11-01 (Q311 to Q316); reviewed against the code and pages by the lead
2026-11-01 (R-151.1 to R-151.13 below; R-151.6 ruled A by Luke, Q313; R-151.1 rewritten and R-151.13
added by Q314, Q315, Q316). A new initiative,
i151: nothing is broken today, so it starts after i150 (rdm#245) lands and never rides inside i150.
Origin: Luke, 2026-11-01: "we shouldn't have multiple ways to do the same thing; timelimit should be
inside resend; resend should have attempts"; `Budget` becomes `TimeLimit`, `Owed` becomes `Resend`
(CLAUDE.md naming canon, a63f75756); Rafka's broker routing needs a notice when a node is found down.

Every claim below is marked verified (read in code or page, cited), inferred, or proposed.

## Rulings

- **Q311 (Luke):** one `Resend { attempts, time_limit }` on every call; **3 attempts**; **one rule for every
  caller**: a call whose attempts end unanswered starts node investigation on that birth, the
  mesh-primary itself or, for any other caller, the mesh-primary it reports the birth to; the lead picks
  the time limit (§3).
- **Q312 (Luke):** yes to the hook, `on_node_investigated` (§7).
- **Q313 (Luke):** a call addressed to a primary counts its attempts per birth the view names for the
  role (§6, R-151.6).
- **Q314 (Luke):** a second hook, `on_call_failed`, at the caller, carrying the stored reason, the
  sender and the receiver (§7, R-151.13).
- **Q315 (Luke):** the caller owns the mechanism: `attempts` and `time_limit` are set by whoever opens
  the call, RDM's values the default (R-151.1).
- **Q316 (Luke):** no use case for an application inside RDM: no exception for Rafka's write leg; the
  application acts on the event and owns its duplicate boundary (R-151.1, §11).

## 1. What is built today (verified)

A call is "reach the node, send, get its answer, try again". Today that is five words (`Budget`,
`Overall`, `Split`, `Stream`, `Owed`), two flags (`once`, `unsent_is_final`) and these loops:

| # | Where | What it repeats or bounds |
|---|---|---|
| 1 | `rdm-transport/src/client.rs` `Budget` `Budget` | four ways to bound one call's time: `Overall(d)` (resolve, dial, send and reply in one deadline; the default, 10 s, `CallOptions::default`), `Split { send, reply }` (reply counted from the commit cut), `Stream { send }` (no reply bound: the connection the request committed on is the bound), `Owed` (no bound at all) |
| 2 | `CallOptions` `CallOptions.once` | "asked once": a request whose connect fails or whose stream is lost is not resent (the tickle's direct probe, `rdm-admin/src/offline.rs` `TickleClient::direct`; ten Rafka call sites) |
| 3 | `CallOptions.unsent_is_final` `CallOptions.unsent_is_final` | a carrier's one inner call ends `NotSent` from its first unsent attempt (`forward.rs` `carry_once`) |
| 4 | `until_end` `until_end` | the attempt loop: resends an unsent or unacked request on the resend schedule; re-dials when no connection is held; `receiver_gone()` checked before every attempt (`until_end`) |
| 5 | `until_resolved` `until_resolved` | an `Owed` request whose receiver this view does not hold yet: reads the view again on the schedule and on every change of the view |
| 6 | `hold_connection` `hold_connection` | acked, response incomplete, stream gone: keeps a connection so the responder can resend |
| 7 | `until_end`, `leg` + `RoutePolicy` (`RoutePolicy`) | the next attempt through a carrier once an attempt sent nothing (i146 Q52) |
| 8 | `rdm-admin/src/node_config.rs` `owed` `owed()` | a workflow loop over #4: sends a Status request again to the primary the view names next on `RejectedNotPrimary`, `NotReady` (root) or a call that ended with no reply |
| 9 | `rdm-admin/src/admin/recovery.rs` `report_terminate` `report_terminate` | a workflow loop with two kinds of turn: a domain answer that keeps the report owed (`BuildInProgress`, `unknown-node`, `NotFabricPrimary`) and a call that ended with no reply, sent again to the fabric-primary the view names |

The responder (`delivery.rs` `push_loop` `push_loop`, `server.rs` `answer_refusal` `answer_refusal`) resends its response
on the same schedule until it is acked, the response is caught up, or the receiver is gone: the same
rule in the other direction, with no attempt count.

The schedule (`delivery.rs` `Pacing` `Pacing`, `rdm-core/src/delivery.rs` `resend_after` `resend_after`): the first delay
is the publish cadence, each next one doubles, each carries ±25 % jitter, capped at the staleness floor.
Inside one attempt the wait for the ack is that attempt's delay (`until_end`, `ack_by`: `ack_by = now +
pacing.after(attempt)`), and the next attempt starts at `started + pacing.after(attempt)`
(`until_end`, `next_at`), the gap measured from the attempt's start, never from its end.

Non-test call sites in RDM (`grep -rn 'Budget::<V>' crates demo --include=*.rs`, tests excluded):
`Overall` 19, `Owed` 14, `Split` 12, `Stream` 17; `once: true` 1; `unsent_is_final: true` 1. Rafka:
45 `Budget::` sites (`rafka-node-base` write_hooks.rs 7, broker_data_op.rs 6, writer_target_op.rs 3,
topic_traffic_close_op.rs 2, broker_resync_step_op.rs 2, seven files with 1; `rafka-writer-ingress`
context.rs 2, dial_bound.rs 1; tests 8), `once: true` 10.

Owner loops that are not delivery loops and stay (node-rpc.md §52 bans "a delivery-retry loop in a
primitive or a workflow", never a loop on a domain answer):

- `rdm-client/src/build_stream.rs` `follow_with`: reattaches a Build stream by `build_id` after its
  connection closed (build.md "a live run is reattached, never re-claimed"; i146 Q22). A stream op is
  not resent after its commit (§2, R-151.3).
- `report_terminate`'s turns on `BuildInProgress`, `unknown-node` and `NotFabricPrimary`: answers, not
  silence. Only its "ended with no reply, sent again to the fabric-primary the view names next" turn is
  a delivery loop (§2, R-151.5).
- `node_config::owed`'s turns on `RejectedNotPrimary` and `NotReady`: the same.
- `fabric_handover.rs` `command_update_primary_in`, `update_node_type_primary_in` `call_once`: one call, its reply read; the hand-over's own rule above.

## 2. The one mechanism

```rust
/// How a call is resent: how many attempts it makes and how long one attempt may take.
pub struct Resend {
    /// Attempts in all, the first send included: the caller's; `Resend::default()` is 3 (Q311, Q315).
    pub attempts: u8,
    /// How long one attempt may take.
    pub time_limit: TimeLimit,
}

/// How long one attempt may take: resolve, dial and the complete send and, once acked, the reply.
pub struct TimeLimit(pub Duration);
```

- **R-151.1 The caller owns the mechanism (Q315, Q316).** `Resend { attempts, time_limit }` is set by
  whoever opens the call; `Resend::default()` is RDM's values (3 attempts, Q311; `TimeLimit` 10 s, §3),
  and every RDM call site passes its own by §3. No field is RDM-only and RDM makes no exception for an
  application's call: the tickle's direct probe passes `attempts: 1` because its page says one connect
  (node-recovery.md step 2; the ladder, not the call, goes on to the peers), and a 3-attempt probe
  inside an investigation that itself marks a spent call would be recursion. Rafka's write leg runs on
  the same `Resend` as every call; what Rafka does when it is spent (ask the next replica) and how it
  stays correct across a replica restart (the transport's duplicate window is in memory,
  `delivery.rs`; a resent frame after the replica restarted is a second delivery) are Rafka's, acted
  on through `on_call_failed` (R-151.13) and written in Rafka's proposal (§11). `once` is removed.
  `unsent_is_final` stays: it is the carrier's one inner call's rule (node-rpc.md §36.1), not a count.
- `CallOptions.budget` and `CallOptions.once` are replaced by `CallOptions.resend`.
- **R-151.2 What an attempt is and which clock runs inside it (verified against `until_end`).** An
  attempt is one send of the request: direct, or through the carrier the route policy names (#7),
  each counted. Inside an attempt the wait for the ack is the schedule's delay for that attempt
  (existing, `until_end`, `ack_by`); the `TimeLimit` bounds the attempt's resolve, dial and complete send
  (a connect that hangs ends the attempt at the limit, `NotSent(Deadline)`), and once the request is
  acked, the reply, counted from the ack (today's `Split.reply`, `call_stream`). The next attempt
  starts at the schedule's next delay from the previous attempt's start (existing, `until_end`, `next_at`),
  or at the previous attempt's end when the limit ran past it. A receiver this view does not hold yet
  ends that attempt `NotSent(Resolve(Unknown))` (#5 removed: an attempt, not a wait). Between
  attempts the call wakes early when the resolver changes (existing, `pause`) and checks
  `receiver_gone()` (existing, `until_end`). An acked request is never resent and spends no
  attempt: its wait for the response is bounded by the reply limit from the ack, and #6
  (`hold_connection`, today looping with no bound until the response completes or the receiver is
  gone) runs inside that limit and ends with it.
- **R-151.3 A stream op is never resent after its commit.** Its frames after the first live as long as
  the connection the request committed on (today's `Stream`, node-rpc.md §34: closure of that
  connection ends the invocation `Indeterminate` and the owner reattaches by its own rule); the
  responder suppresses a repeated identity after acceptance (§43), so a resent stream request would
  open no second stream. `Stream` needs no variant: the op's shape says it (`unary` is already on
  the call slot, `on_response`, the slot's `unary`). Before the commit a stream op's attempts count as any other's.
- **R-151.4 The end of the attempts is named by what crossed the commit cut, never by the last attempt
  alone.** No attempt committed: `NotSent` with the last attempt's reason. Any attempt committed and no
  reply within its limit: `Indeterminate` (node-rpc.md §34 "after that cut, reply timeout is
  Indeterminate"). Acked and the reply not within the limit: `Indeterminate`, and it counts as
  unanswered (the receiver took the command and did not answer it: Q182). `committed` is a loop
  variable set once (`until_end`) and must be read at the final send.
- **R-151.5 The owner never resends.** #8 and #9's "no reply, sent again to the primary the view names
  next" turns are removed; their turns on a domain answer stay as they are (§1). A command addressed
  to a role resolves its receiver at each attempt from the view: see R-151.6 for what that can and
  cannot follow.
- The response direction keeps its rule unchanged (R-151.9).

## 3. The time limit (lead's pick, Q311)

One rule: **an attempt's `TimeLimit` is how long the receiver takes to answer, plus 10 s for the
transport.**

- Every call whose handler answers at once (a status read, a declaration, a config apply): **10 s**,
  the default today (`CallOptions::default`).
- A call whose handler waits on its own bound before answering takes that bound plus 10 s:
  `node.connections.delete { All }` and node.stop wait `RDM_CONNECTIONS_DELETE_DEADLINE_MS` (5 s
  default), so 15 s (today `send_command_reaching` `Split { send: 5 s, reply: connections_delete_reply() }`);
  a Build step call waits its step's own bound (today `send_command_reaching` reply 60 s,
  `storage_change::command_reply` reply 30 s, `carry_lifecycle_reaching`).
- A stream op: 10 s to resolve, dial, send and receive its first frame; later frames as R-151.3.
- Behaviour change to name in a cell: today's `Split` gives the reply its own window after the commit;
  one `TimeLimit` per attempt gives it the limit counted from the ack, so a slow dial no longer eats the
  reply window and a limit is never shared between the two phases.

## 4. When the attempts end: node investigation (one rule, Q311)

- **R-151.7 Who is told (node-investigation.md "Caller": "the primary above the node: its mesh-primary
  for a member of any type, the fabric-primary for a mesh-primary or another mesh's node-admin").**
  Three cases, and the chain bounds the depth:
  1. the receiver is a node of a mesh that is not its mesh-primary: its mesh-primary investigates; the mesh-primary itself when it
     was the caller, otherwise the caller reports the birth to it;
  2. the receiver is a mesh-primary: the fabric-primary investigates (node-recovery.md step 2 "for an
     unheard mesh-primary the fabric-primary runs it, after the election has named the new
     mesh-primary"); a caller that is not the fabric-primary reports the birth to it; a node whose own
     mesh-primary is unanswered has no primary above it to report to and reports nothing: that
     mesh-primary's silence is the fabric-primary's by gossip and the election;
  3. the receiver is the fabric-primary: nothing starts; fabric recovery already owns it
     (fabric-recovery.md step 1, unheard past the floor).
  A `NodeTarget::Endpoint` call names no birth (`NodeTarget::Endpoint`) and starts nothing.
- **R-151.8 The report rides the existing Q182 record, never a new direct trigger.** The mesh-primary's
  `CommandView` already holds, per exact birth, the instant a command of this admin was left unanswered
  (`CommandView::unanswered_since` contract; `Looker::command_unanswered` reads it on the watch's tick; the tickle
  reads it so an answer from such a birth is not `reached`, `TickleClient::command_unanswered`). Under i151 the record is
  fed by two writers: this admin's own call whose attempts ended unanswered, and a report. One new
  Status request on the upward path the declarations use (node-rpc-envelope.md Status, `0x1B`):
  `ReportCallFailed { sender, receiver, op, failure }` (the same facts `on_call_failed` carries, Q314,
  so the primary stores the reason, not only the fact; `failure: CallFailure`, the typed reason only),
  sent with `Resend::default()` to the primary above the
  birth, answered at once (`Accepted`, or `SubjectUnknown` for a birth the primary does not hold, which
  ends the report by name). The primary writes the report into the same record; the watch's tick
  investigates each birth there as it does today, "once per silence (not again within a staleness
  floor, nor until it is heard ready or unheard again)" (node-recovery.md step 3): fifty callers that
  cannot reach one broker in the same second produce one investigation, not fifty, and the record is
  the one clock. **The clock changes (Q311 replaces Q182's):** today the record reads a birth as
  unanswered only once the command's send is a staleness floor old (`CommandView::unanswered_since` contract),
  and the watch's tick then marks it. Under i151 the instant the attempts are spent is the mark, the
  way node-recovery.md step 1 already marks a birth "at once when a command to it was never sent for a
  reachability reason". Step 2's half-floor wait does not apply to it: that wait exists so a node
  silent on gossip has time to come back, and a spent call is stronger evidence than silence (three
  sends went unanswered; Q311 says the spent call *starts* investigation). The watch investigates the
  birth on its next tick (every gossip interval, node-recovery.md "Detection"): at production cadence
  within 2 s of the spent instant, so ~8 s after a call whose connect is refused, ~16 s after one
  committed and never acked, ~32 s after one whose connect hangs, ~12 s after one acked and
  unanswered (§5), against today's send + 30 s + half a floor + the tick. A silence mark keeps its
  half floor. A birth the
  record already holds unanswered takes no new mark from a later spent call (one investigation per
  silence). The report is a notice and is itself never reported, marked or hooked: when its own attempts end,
  that end is spanned (`rdm.node_rpc.delivery.update.via-attempt-ended`) and the report is dropped; it
  is not resent to a next primary (case 2 above covers the next primary), it writes no record and it
  does not call `on_call_failed` (the hook is the application's event, and RDM's own report is not an
  application call). Q311's rule applies to the call that was spent, once; no report about a report.
- A data-plane call is no different from a command: a Rafka node's call to a broker that ends
  unanswered reports through the same request on the node's declaration path.
- Investigation itself is unchanged (node-investigation.md: connections index, runtime inspect,
  tickle, tickle through peers, one verdict).

## 5. The mechanism, quantified

Production cadences: publish cadence 2 s (`RDM_GOSSIP_INTERVAL_MS`), staleness floor 30 s
(`RDM_STALENESS_MS`); the test tooling's 500 ms and 3000 ms scale every row.

| Mechanism | Value | Scope | Starts / resets when | On exhaustion | Source and rationale |
|---|---|---|---|---|---|
| Attempts | the caller's; `Resend::default()` 3, the first send included; the tickle's direct probe 1 | one call (under R-151.6 arm A: one call to one resolved birth) | the call opens; never reset for an exact birth: a receiver re-resolved to another birth ends the call `ReceiverGone` (existing) and a retarget is the owner's new call; under arm A a new birth the view names for the role starts its own three | the call ends to its owner by name (R-151.4) and the birth is recorded unanswered (R-151.8) | Q311 (3), Q315 (the caller's, RDM's the default); node-recovery.md step 2 (the tickle's 1): existing |
| Delay before attempt n+1 | `cadence × 2^(n-1)`, ±25 % jitter, capped at the floor: 2 s, 4 s (8 s unused with 3 attempts) | one call | measured from attempt n's start | n/a | `rdm-core/src/delivery.rs` `resend_after`, `delivery.rs` `Pacing`: existing |
| Ack wait inside attempt n | that attempt's delay (2 s, 4 s, 8 s) | one attempt | the complete send | the next attempt, or the end at attempt 3 | `until_end`, `ack_by`: existing |
| `TimeLimit` | 10 s; the receiver's own bound + 10 s where it has one (15 s for connections delete and stop; a Build step's bound + 10 s) | one attempt: resolve, dial, complete send; then the reply from the ack | the attempt's start; the reply's from the ack | the attempt ends `NotSent(Deadline)` or `Indeterminate` | proposed (§3): the default is today's 10 s `Overall`; the per-class values are today's `Split.reply` values; validated by the flame of §8 step 4 |
| Early wake | the resolver's change, the call's own state change | one call | n/a | n/a | `pause`: existing |
| Report `ReportCallFailed` | one request, `Resend::default()`, to the primary above the birth | one caller, one birth | the attempts of a call to that birth end unanswered | spanned and dropped: the record is the primary's | proposed (R-151.8) |
| Coalescing | one unanswered record per exact birth; one investigation per silence | one primary | an answer from the birth, a digest of the exact birth, the birth leaving the view clear it | not a timing value | `CommandView::unanswered_since`, node-recovery.md step 3: existing |
| Investigation start | the watch's tick (every gossip interval): half a floor after a silence mark; the next tick after a spent-call mark | one primary | the mark: silence past the floor (existing, half floor), a command never sent for a reachability reason (existing), the attempts of a call spent (proposed, R-151.8, next tick) | n/a | node-recovery.md "Detection", step 1, step 2: existing; the spent-call mark and its next-tick start: proposed, by Q311's "starts" |
| Responder's resend of a response | the same schedule, no attempt count; ends on the ack, caught up, or the receiver gone | one response | the request's stream is lost | n/a (R-151.9) | `delivery.rs` `push_loop`, `push_resend`: existing |
| Response frames held for resend | no bound in items or bytes today | one process | n/a | n/a | `delivery.rs` holds `st.owed` with no cap: verified absent; a bound is proposed in R-151.9, measured before it is set |

How the budgets combine for one 10 s call to a birth that never answers, at production cadence, no
jitter:

| Failure mode | Attempt 1 | Attempt 2 | Attempt 3 | Spent at |
|---|---|---|---|---|
| connect refused at once (never committed) | 0 s, fails at once | 2 s | 6 s | **~6 s**, `NotSent` |
| committed, never acked | 0 s, waits 2 s | 2 s, waits 4 s | 6 s, waits 8 s | **~14 s**, `Indeterminate` |
| connect hangs to the limit | 0 to 10 s | 10 to 20 s | 20 to 30 s | **~30 s**, `NotSent(Deadline)` |
| acked, no reply | 0 s; ack; reply limit 10 s | n/a: acked requests are not resent | n/a | **~10 s**, `Indeterminate` |

The earlier text's "about 36 s" assumed every attempt waits its full limit; the loop does not (the ack
wait is the schedule's delay, `until_end`, `ack_by`). These numbers are what the code as it stands gives
once attempts are counted.

The user-visible target, one call to a birth that never answers, at production cadence: the call ends
to its owner at the "Spent at" instant above and `on_call_failed` is called then; the investigation
starts on the watch's next tick, within one gossip interval (2 s) (R-151.8); the verdict follows the
ladder's own bounds (node-tickle.md), and `on_node_investigated` is called once the verdict is
recorded. For the common case (connect refused: the process is gone) the owner knows at ~6 s and the
investigation starts by ~8 s, well inside one staleness floor; a birth that is silent on gossip too is
marked by the pruner at 30 s and investigated at 45 s as today, whichever mark comes first.

## 6. The design ceiling: three attempts end before the election (R-151.6: Luke ruled A, Q313)

Every row above ends between ~6 s and ~30 s. A primary is superseded only once it is unheard past the
staleness floor, 30 s (fabric-node-lifecycle-elections.md §4.1, §4.3), and the election then names the
next one. So a call addressed to a role ("the mesh-primary of mesh M", "the fabric-primary") whose
receiver dies spends its three attempts on the dead birth before any view can name another, and
"the owner never resends" (R-151.5) means the command ends to its owner with nothing done. That is
exactly what #8 and #9 do today: send again to the primary the view names next. The declarations
(`node.rs` `declare_once`, `Budget::Owed`, one in flight, the next on the previous one's answer by name,
node-rpc.md §43) are the same shape: a node's Stopped declaration during its mesh-primary's death
would end unanswered and the next declaration go out, the Stopped never recorded. i150 keeps the
declaration path: node-record.md R-NR3 (`docs/plans/proposal/node-record.md`, R-NR3 and its ownership table)
has `status`, `declared` and `operations` written by the node's mesh-primary, so the node still
declares to it (verified in the proposal text; the i150 PRD's R3 and R9 move the node's own property
write only). The ceiling stands after i150, and stands through #8 and #9 alone (an admin's report to
the fabric-primary) even if the declarations moved.

Arms:

A) **Attempts are counted per resolved birth, for a role-addressed call.** The call's receiver is the
   role; each birth the view resolves it to gets three attempts; a new birth resolved for the role
   (after the election) starts its own three; the call ends `ReceiverGone` only when the role has no
   birth and the view says the previous one is gone. Not a silent transfer: the owner named the role,
   never a birth, and the message is the same message under the same delivery id. Each spent birth is
   recorded unanswered (R-151.8). An exact-birth call (`ExactNode`) keeps three in all. Needs a
   `NodeTarget` for a role (today `CurrentPath`, `ExactNode`, `Endpoint`, `resolve.rs` `NodeTarget`): one new
   variant, `Primary { mesh: Option<String> }` (a mesh's mesh-primary; `None` the fabric-primary),
   resolved by the same resolver #8 reads today. Said plainly: a role-addressed call's total elapsed
   time is then unbounded across births (a role whose births keep dying keeps the call open), as #8
   and #9 are today. What stays bounded: three attempts and one `TimeLimit` per birth, one call's
   memory per owner, and one unanswered record plus one investigation span per spent birth, which is
   where an operator sees it; the call ends `ReceiverGone` when the role has no birth.
B) **The owner re-issues on the event "the primary changed".** No loop: the call ends after three
   attempts; the owner (the declaring node, the reporting admin) opens a new call when its view's
   primary for the role changes (`node.primary.update`, the election), carrying the same operation
   identity so the new primary's duplicate suppression and the Build's `build_id` cover a repeat. One
   event, one new call, per primary change. Keeps `NodeTarget` as it is; every role-addressed owner
   gains a watch on the role.
C) **Accept the loss.** Three attempts, the command ends, and whatever recovers the lost command does
   so by its own rule (the Build's resubmit by `build_id`, the node's next declaration, the atlas fold
   at takeover). Needs each of the fourteen `Owed` sites shown to survive a lost message; the Stopped
   declaration above does not.
X) Something else.

**Ruled A (Luke, 2026-11-01, Q313).** Dev recommendation was A: it is the smallest change that keeps Q311's "the owner never resends" true
and keeps the declaration and report paths correct through an election; B spreads a watch over every
owner (many moving parts), C is a correctness loss the pages do not allow. With A ruled, R-151.5's
"resolves its receiver at each attempt" reads "per resolved birth" and the §43 text in §9 carries the
sentence "a call addressed to a primary counts its attempts per birth the view names for it".

## 7. The hooks: `on_call_failed` (Q314) and `on_node_investigated` (Q312)

Two moments, two places, two hooks (Luke, 2026-11-01):

| Hook | Where | When | Who acts |
|---|---|---|---|
| `on_call_failed` | the calling node | its call's attempts are spent | the caller, now: Rafka's writer asks the next replica |
| `on_node_investigated` | the primary above the birth | the verdict is recorded | Rafka's node-admin: `write -> none` on `recreate` |

- **R-151.13 `on_call_failed`.** Called on the calling node, on a task of its own, once per spent call,
  after the call has ended to its owner; a notice, gates nothing. `CallFailedCtx { sender, receiver, op,
  failure, attempts, elapsed, context }`: `sender` and `receiver` each `(node_id, incarnation, path,
  name)` (the receiver's birth the one the call was addressed to; under Q313 the last birth resolved;
  none for a `NodeTarget::Endpoint` call, which names no birth); `op` the request's op; `failure: CallFailure`, the
  reason the call's span stores and nothing more (never `RpcOutcome`, which can carry a reply):
  `NotSent(NotSentReason)` (connect refused, connect timed out,
  endpoint unresolved, no active route, `rdm-core/src/outcome.rs`; today's `node_commands.rs` names
  them `connect-refused`, `connect-timed-out`, `endpoint-unresolved`) or
  `Indeterminate(IndeterminateReason)` (committed and never acked; acked and no reply within the
  limit); `attempts` made; `elapsed` from the call's open; `context` the call's trace context, so the
  application's action parents to the spent call. It runs on every node (the wiring every node has for
  `on_node_ready`, `rdm-node/src/node_hooks.rs` `on_node_ready`), not only admins. The writer reads `NotSent` as
  safe to ask another replica and `Indeterminate` as "the frame may have landed", its own idempotency
  rule deciding (Q316).

fabric-node-hooks.md, a new row in the hooks table:

> | [`on_call_failed`](#on_call_failed) | every node | after one of its calls ends with its attempts spent | nothing; it is a notice |

and a new section:

> ## on_call_failed
>
> When a call this node opened ends with its attempts spent ([node-rpc.md §43](node-rpc.md#43-resend-delivery-is-the-runtimes)),
> RDM calls `on_call_failed` once, with `CallFailedCtx { sender, receiver, op, failure, attempts,
> elapsed, context }`, on a task of its own, after the call has ended to its owner. `sender` and
> `receiver` are each the birth's `(node_id, incarnation, path, name)`; `failure` is the call's stored
> reason, `NotSent` with its reason (nothing reached the receiver) or `Indeterminate` with its reason
> (the request crossed the commit cut and no answer came). The notice gates nothing: the call's owner
> already has its outcome by name; the application acts by its own rule (ask another node, mark a
> route) and owns any duplicate boundary a later resend of its own crosses. It is not a verdict on the
> receiver: that is [`on_node_investigated`](#on_node_investigated), once the primary above the birth
> has investigated it.

### `on_node_investigated`

- **R-151.10 Called by the primary that investigated** (no `_failed` split of this hook: it fires on
  every verdict, `reached` included, and `verdict` and `next` say which, as `on_build_ended` carries
  Complete or Failed in one hook): each mesh-primary for a node of its mesh; the
  fabric-primary for a mesh-primary (R-151.7 case 2). `next` is `rejoin`, `recreate`, or `none` for a
  verdict RDM acts on by neither (fabric recovery's `running` exception, fabric-recovery.md step 3).
- RDM guarantees nothing between the notice's task and the re-create it names: Rafka orders its own
  `write -> none` write before the new birth's re-sync by its own rule (§10).

fabric-node-hooks.md, a new row in the hooks table:

> | [`on_node_investigated`](#on_node_investigated) | the primary that investigated: each mesh-primary for a node of its mesh, the fabric-primary for a mesh-primary | after it records a node investigation's verdict on that birth | nothing; it is a notice |

and a new section:

> ## on_node_investigated
>
> Once it records a node investigation's verdict ([node investigation](lifecycles/node-investigation.md)),
> the primary that investigated calls `on_node_investigated` once, with `NodeInvestigatedCtx { fabric_id,
> mesh_id, node_id, incarnation, path, name, verdict, next, context }`, on a task of its own. `verdict`
> is the investigation's (`reached`, `exited`, `os-stopped`, `running`, `unproven`); `next` is what RDM
> does with the birth: `rejoin` (commanded back through node.mesh.join), `recreate` (re-created at the
> same path.name with a new node.id) or `none` (fabric recovery's `running` exception). The notice
> gates nothing in RDM and orders nothing: a re-create may be under way when the application's hook
> runs. Rafka takes the broker out of its topics' write sets on `recreate`.

## 8. The responder and the store (R-151.9)

- Verified: a response for an invocation the caller ended (removed from `calls`) is acked, never
  dropped (`Delivery::on_response` with `Delivery::completed`: an own sender's ended invocation counts
  as completed), so a late response to a call whose attempts were spent is acked and the responder's
  `push_loop` ends `acked` (`push_resend`). Nothing new is needed there; the earlier text's "the
  response direction resends under the call's `Resend`" is withdrawn: the responder does not know the
  caller's `Resend`, and needs no attempt count because the ack of a late frame ends it.
- The owed-frame store (`st.owed`) has no bound in items or bytes (verified absent). Proposed: a bound
  measured first (the soak's peak, read from a `DeliveryStats` counter added for it), then set as a
  constant with its measurement in its comment; a frame past the bound is dropped, spanned, and the
  responder's invocation is settled `Ended` so the caller's resend is refused by name rather than
  held. Set in i151 only with the measurement; never an invented number.

## 9. Page text (exact; for Luke's yes on the build)

node-rpc.md §34, the budget shapes. Old (§34, the budget shapes):

> The generic API supports three budget shapes:
>
> ```text
> Overall { deadline }
> Split {
>     request_send_deadline
>     reply_budget_after_commit
> }
> Owed
> ```
>
> Overall is the default: [...]
>
> Split exists for domains such as writer ingress [...]
>
> Owed has no send or reply deadline. [...] Owed is never a default.

New:

> Every call carries one `Resend { attempts, time_limit }`. `attempts` is how many sends the call makes
> in all, the first included: three by default, set by whoever opens the call (the tickle's direct
> probe is one, node-recovery.md step 2). `TimeLimit` is how long one attempt may take: resolve, dial and the
> complete request and, once the request is acked, its reply counted from the ack; ten seconds for a
> call its receiver answers at once, the receiver's own bound plus ten seconds where it has one. A
> stream op's frames after its first live as long as the connection the request committed on, and a
> stream request is never resent after its commit: the owner reattaches by its own rule.

node-rpc.md §34, the rest of the section that names a budget. Old (the Build carrier paragraph):

> A carried call's inner budget is the `remaining_ms` the source stamps

New:

> A carried call's inner time limit is the `remaining_ms` the source stamps

Old (the commit-cut paragraph):

> A call's budget is its owner's bound: when it lapses the request is owed no longer (§43); an Owed call has no bound and lapses never; it ends as §43 says. For Budget::Stream, closure of the connection on which the request committed ends the invocation as Indeterminate and ends its delivery obligation: the owner of such a call (the Build carrier, build.get's watch, the handover) declared the connection its bound and reattaches by its own rule, above this layer. For other budgets, connection loss alone does not cancel the invocation; owed delivery continues within the owner's bound.

New:

> A call's `Resend` is its owner's bound: when its attempts are spent the request is resent no longer and the call ends as §43 says. For a stream op, closure of the connection on which the request committed ends the invocation as Indeterminate and ends its delivery obligation: the owner of such a call (the Build carrier, build.get's watch, the handover) declared the connection its bound and reattaches by its own rule, above this layer. For a unary op, connection loss alone does not cancel the invocation; the request is resent within its attempts and the response within the reply limit.

Old (the execution-deadline paragraph):

> The caller budget is not a generic remote execution deadline.

New:

> The caller's time limit is not a generic remote execution deadline.

§34's "A protocol that needs its own execution budget carries a protocol-specific field" and the
memory sense (§18 "request-memory budget" and `Busy { MemoryBudget }`, §22 "server-streaming
budget") are another sense of the word, not the call's `Budget` type, and stay.

node-rpc.md §43, title and first paragraph. Old:

> ## 43. Owed messages: delivery is the runtime's
>
> Retry automatically in Node RPC. Send immediately. Without an ack, resend the same message with the
> same delivery id: first retry at the publish cadence, then exponential backoff with jitter, capped at
> the staleness floor; one attempt in flight per message. [...] Retry stops on the ack, on the
> receiver's re-creation or accepted removal, or on explicit cancellation or retargeting by the owning
> workflow; never on a timer, never on absence from a view, never on a newer message of its kind.

New:

> ## 43. Resend: delivery is the runtime's
>
> Every call carries one `Resend` (§34). Send immediately. Without an ack, resend the same message
> with the same delivery id: the next attempt at the publish cadence, then doubling with jitter, capped
> at the staleness floor; one attempt in flight per message. [...] Resending stops on the ack, on the
> receiver's re-creation or accepted removal, on explicit cancellation by the owning workflow, or when
> the attempts are spent: the call then ends to its owner by name (`NotSent` when no attempt crossed
> the commit cut, `Indeterminate` otherwise) and the birth is recorded unanswered at the primary above
> it ([node investigation](lifecycles/node-investigation.md)). A receiver absent from the view spends an
> attempt, never a wait. The owner never resends a call; a call's receiver is resolved again at each
> attempt, and a call addressed to a primary counts its attempts per birth the view names for it,
> three per birth, and ends `ReceiverGone` when the role has no birth.

(the elided `[...]` sentences on duplicate suppression stay; the last sentence is Q313.)

node-rpc.md §43, the route policy paragraph. Old:

> The owner of an Owed request chooses its route policy: the one receiver it is owed to and the carriers it
> may travel through (§45).

New:

> The owner of a call chooses its route policy: the one receiver it is sent to and the carriers it
> may travel through (§45).

node-rpc.md §45, last sentence. Old:

> For an Owed request the owner chooses the route policy and Node RPC executes it on every resend (§43).

New:

> For every call the owner chooses the route policy and Node RPC executes it on every resend (§43).

node-recovery.md step 2, first sentences. Old:

> 2. **Investigation.** Only the mesh-primary acts. Half a floor after the mark it runs
>    [node investigation](node-investigation.md) on the exact birth:

New:

> 2. **Investigation.** Only the mesh-primary acts. Half a floor after a silence mark, and on its next
>    tick after a spent-call mark (three sends unanswered are stronger evidence than silence,
>    [node-rpc.md §43](../node-rpc.md#43-resend-delivery-is-the-runtimes)), it runs
>    [node investigation](node-investigation.md) on the exact birth:

node-investigation.md, the Q182 paragraph. Old:

> A node that does not answer a command is silent, whatever it gossips (Q182): an owed command it
> leaves unanswered past the staleness floor starts node investigation on its birth as silence does,

New:

> A node that does not answer a call is silent, whatever it gossips (Q182): a call whose attempts end
> unanswered records its birth unanswered at the primary above it (the caller itself when it is that
> primary; any other caller reports the birth to it with Status `ReportCallFailed`; nothing for the
> fabric-primary, whose silence is fabric recovery's), and the primary's watch starts node
> investigation on that birth on its next tick, once per silence,

node-rpc-envelope.md, Status (`0x1B`): one new request variant, **appended at position 38**, after
`ApplyNodeProperties` (37): the variants are positional on the wire (postcard), so nothing is inserted
and no existing position moves; its golden in `crates/rdm-core/tests/status_wire.rs` is written by
hand and its codec cell added.

> `ReportCallFailed { sender: Birth, receiver: Birth, op: u8, failure: CallFailure }` (`Birth` the
> `(node_id, incarnation, path, name)` of each end; `CallFailure` = `NotSent(NotSentReason)` or
> `Indeterminate(IndeterminateReason)`, the typed reason only, never a reply payload): a caller's
> report that its call to this exact birth ended with its attempts spent and no answer (node-rpc.md
> §43); answered `Accepted`, or `SubjectUnknown` for a birth the receiver is not the primary above.
> The report is itself never reported: its own spent attempts are spanned and end it.

Code identifiers that name this mechanism are renamed with the pages (CLAUDE.md naming canon), the
delivery sense only: `rdm-core/src/forward.rs` `OwedDelivery` becomes `ResendDelivery` (and the `owed` field of `Forward`), `client.rs` `OwedCall` becomes `ResendCall`, `owe_steps` becomes
`resend_steps`, `delivery.rs` `st.owed`, `owe_frame`, `owe_response`, `owe_response_in`,
`owe_again`, `owed_frames`, `owed_frames_of`, `with_owed`, `owed_drop` take `resend`/`held`
(`held_frames`: a response frame held for resend), `OwedOne`, and the node's `OwedDeclarations`
(`node.rs`) becomes `Declarations`; the 19 span texts in `rdm-transport/src` that say "owed" say
"resent"/"held for resend". The other sense stays as it is: a report or exit an owner owes
(`exits_owed`, `properties_owed`, `OwedExit`, `owed_submits`, `owes_full`, `OwedRow`, `owed_state`,
`paths_owed_replacement`, `owed_from_view`, `owed_refused`, `owed_direct_restored`); `OwedWake` goes
with #9's no-reply turn.

The words `owed` and `budget`, any case (14 architecture pages, 61 lines, `grep -rniw 'owed\|budget'
docs/architecture`), are replaced where they name this mechanism ("owed until acked" becomes "resent
until acked", `Budget::Owed` becomes `Resend`); the memory-limit sense of budget and a report an owner
owes stay, each line named in the landing. INDEX rows Q182 (an owed command unanswered past the
staleness floor starts investigation: the clock is now the spent instant, R-151.8), Q44
(`Budget::Owed`), Q52 (the owed declaration's carried route) and Q210 (each declaration one
`Budget::Owed` call) get "superseded in part by Q311: `Resend`".

## 10. RDM build

1. `rdm-transport`: `Resend`, `TimeLimit`; `Budget`, `once` removed; `until_end` counts attempts and
   ends by R-151.4; `until_resolved` folded into an attempt; R-151.6's arm as ruled. Cells: an
   unanswered call ends after 3 attempts by name at the §5 instants (fake clock); a call whose connect
   is refused ends `NotSent` at ~6 s and one committed and never acked ends `Indeterminate` at ~14 s; a
   receiver gone between attempts ends `ReceiverGone`; a stream lives past its limit while its
   connection does and is not resent after its commit; a slow dial no longer eats the reply window; a
   late response to a spent call is acked (R-151.9, existing behaviour pinned).
2. `rdm-admin`: #8 and #9's no-reply turns removed, their answer turns kept; every call site given its
   `Resend` (§3); an admin's own spent call writes the Q182 record; `ReportCallFailed` served into the
   same record; `on_node_investigated` called after each verdict, with its ctx in `rdm_admin::wiring`
   beside `on_build_ended` (`wiring.rs` `on_build_ended`); `on_call_failed` in `rdm-node`'s hook wiring beside
   `on_node_ready` (`node_hooks.rs` `on_node_ready`), called by the transport client's owner on every node. Cells:
   a spent call calls `on_call_failed` once with the stored reason, both births and the call's trace
   context; a call that is answered never calls it; a `ReportCallFailed` whose own attempts are spent
   ends spanned, writes no record, sends no report and calls no hook (item 5); a spent call's birth is
   investigated on the watch's next tick with no half-floor wait, and a silent birth still waits its
   half floor (R-151.8). Cells: fifty callers' reports of one birth within a floor
   start one investigation; a mesh-primary's spent call to a node and a peer node's spent call to the
   same node both end in the one record; a spent call to a mesh-primary is reported to the
   fabric-primary; the hook fires once per verdict with `next` as node-recovery.md step 3 decides.
3. Pages: §34, §43, node-investigation.md, node-rpc-envelope.md, fabric-node-hooks.md, the `owed`
   sweep, the three INDEX notes; rdm-docgen regenerated.
4. A Jaeger flame of a call that ends unanswered, the report, the investigation the watch starts and
   the notice, at the tip; the §5 instants read off it.

## 11. Rafka build

- Rafka's 45 `Budget::` uses move to `Resend` with the RDM pin: `Overall(d)` becomes
  `Resend { attempts: 3, time_limit: TimeLimit(d) }`, or `Resend::default()` where `d` is 10 s; its ten
  `once: true` sites choose their own `attempts` (Q315), each named in the landing with its reason. No
  site is told its number by RDM (Q316).
- The writer's leg policy (which replica is asked next, the route vote, `context.rs` `DataRpcFailure`) stays and
  is driven by `on_call_failed`'s `failure`: `NotSent` asks the next replica; `Indeterminate` goes
  through the writer's own idempotency rule. How a resent frame stays one write across a replica
  restart (the replica keeping the delivery it accepted durably, Kafka's producer id and sequence, or
  the writer's own frame identity) is Rafka's proposal, not this one.
- node-admin implements `on_node_investigated`: on `next = recreate` it writes the broker's
  `write -> none` rows (replica-routing.md §6 step 1), taking it out of its topics' write sets, and
  orders that write before the new birth's re-sync by its own rule (the re-sync stays on
  `on_node_ready` of the new birth; RDM orders nothing between the two).
