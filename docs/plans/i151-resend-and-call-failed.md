# i151 — one `Resend` for every Node RPC call; `on_call_failed` at the caller, `on_node_investigated` at the primary

**Initiative / Epic / Story ID:** `i151`, epics e0–e6, stories as the issue list below (one story = one issue = one agent)
**Sprint:** after i150 (rdm#245) lands; never inside i150
**Status:** opened 2026-11-01; BUILD, no implementation evidence claimed
**Architecture doc:** [node-rpc.md](../architecture/node-rpc.md) §34, §43, §45; [node-investigation.md](../architecture/lifecycles/node-investigation.md); [node-recovery.md](../architecture/lifecycles/node-recovery.md) step 1 and step 2; [fabric-node-hooks.md](../architecture/fabric-node-hooks.md); [node-rpc-envelope.md](../architecture/node-rpc-envelope.md) Status. The exact old/new text is in the [proposal](proposal/151-consolidate-event-callers-add-investigation-hook.md) §9 and §7 and lands with the code of the epic that changes the behaviour (e3 reconciles).
**Signed off:** blank until the adversarial read of this file and the proposal
**Approved:** Luke approved the proposal and asked for this PRD in this chat on 2026-11-01 (rulings Q311–Q316); no UTC time supplied. The page text in proposal §9 still needs his yes on the exact old/new when each epic lands it.
**Owner:** the RDM lead; one agent per story, dispatched in a lane after i150 lands; Rafka's own build (proposal §11) is the i141 lead's, in rafka-v2
**Feature:** `resend` (`docs/features/resend/`)
**Baseline:** the proposal at `ce98cbacc` on lane 1's i150 branch; code coordinates below are symbols in that tree and must be re-read at dispatch against origin/main after i150

Every claim below is verified (read in code or page, named by symbol), inferred, or planned. No test was run to author this PRD.

## Context

An embedding application calls nodes over Node RPC and today has five ways to bound a call (`Budget::Overall`, `Split`, `Stream`, `Owed`, and `once`) and no way to learn that a call failed except by the owner's own loop; two admin workflows loop by hand, and a node found down is known to the application only once it is re-created. After i151 every call carries one `Resend { attempts, time_limit }` the caller owns, a call whose attempts are spent ends to its owner by name and calls `on_call_failed` on the calling node, the birth is investigated by its primary on the next tick, and the primary calls `on_node_investigated` with the verdict. Rafka's writer and its node-admin act on those two notices by their own rule.

## Requirements

Every row is HOLDS (verified, by symbol) or BUILD (the gap and the epic). Coordinates are symbols, never lines (Luke 2026-11-01).

| ID | Requirement, in consumer terms | HOLDS / BUILD | Verified current evidence (symbol) | Owning epic |
|---|---|---|---|---|
| R1 | Every call carries one `Resend { attempts, time_limit }`; `Resend::default()` is 3 attempts and 10 s; the caller may set either (Q311, Q315) | BUILD | `rdm-transport/src/client.rs` `Budget` (four shapes), `CallOptions.budget`, `CallOptions.once`, `CallOptions::default` (10 s) | e1 |
| R2 | An attempt is one send; the next attempt starts at the schedule's delay from the previous attempt's start; the ack wait inside an attempt is that delay; the `TimeLimit` bounds resolve, dial and complete send, and the reply from the ack (R-151.2) | BUILD (the loop exists, the count does not) | `until_end` (`next_at`, `ack_by`, `deadlines`), `Pacing`, `rdm-core/src/delivery.rs` `resend_after` | e1 |
| R3 | A receiver absent from the view spends an attempt, never a wait | BUILD | `until_resolved` waits on the view | e1 |
| R4 | A stream op is never resent after its commit; its frames live as long as its connection; a slow dial no longer eats the reply window (R-151.3) | HOLDS in part (`committed_on` close ends the call) / BUILD (`Split`'s window) | `until_end` `committed_on`, `hold_connection`, `stream.rs` `call_stream` | e1 |
| R5 | A spent call ends `NotSent` with the last reason when nothing committed, `Indeterminate` otherwise; acked-and-unanswered within the limit is `Indeterminate` (R-151.4) | BUILD | `until_end` `committed` | e1 |
| R6 | A call addressed to a primary counts its attempts per birth the view names for the role and ends `ReceiverGone` when the role has no birth; an exact-birth call keeps three in all (Q313) | BUILD | `resolve.rs` `NodeTarget` (`CurrentPath`, `ExactNode`, `Endpoint`), `receiver_gone` | e1 |
| R7 | The owner never resends: the two admin no-reply loops are removed, their domain-answer turns kept (R-151.5) | BUILD | `node_config::owed` (`Hop`), `RecoverySubmitter::report_terminate` (`TerminateTurn::Owed`, `OwedWake`) | e2 |
| R8 | A spent call marks its birth at once at the primary above it, which investigates on its next tick; a silence mark keeps its half floor; one investigation per silence (R-151.7, R-151.8) | BUILD | `CommandView::unanswered_since` (a floor after the send), `Looker::command_unanswered`, `TickleClient::command_unanswered`, the primary watch's tick | e2 |
| R9 | A caller that is not the primary above the birth reports it with Status `ReportCallFailed` (position 38, typed `CallFailure`, `OpOwner` + op), answered `Applied` or `RejectedNotPrimary { why: SubjectUnknown }`; a report is never reported (R-151.8) | BUILD | `rdm-core/src/status.rs` `StatusRequest` (37 positions, `ApplyNodeProperties` last), `StatusReply::Applied`, `NotPrimary::SubjectUnknown`, `catalog.rs` `OpOwner` | e2 |
| R10 | `on_call_failed` is called once on the calling node per spent call with `CallFailedCtx { sender, receiver, owner, op, failure, attempts, elapsed, context }`; a notice (Q314) | BUILD | `rdm-node/src/node_hooks.rs` `OnNodeReady`, `NodeHooks` (the per-node hook wiring to mirror) | e2 |
| R11 | `on_node_investigated` is called once by the investigating primary after it records a verdict, with `NodeInvestigatedCtx { …, verdict, next }`, `next` ∈ `rejoin`, `recreate`, `none` (Q312, R-151.10) | BUILD | `rdm-admin/src/wiring.rs` `Wiring`, `BuildEndedNotice` (the notice shape to mirror); `node_investigation::investigate` (the verdict) | e2 |
| R12 | A late response to a spent call is acked, never dropped; the responder's resend needs no count (R-151.9) | HOLDS | `Delivery::on_response` with `Delivery::completed`; `push_resend` ends `acked` | e0 pins it with a cell |
| R13 | Pages and code say `resend` and `TimeLimit`, never `owed`/`Budget`, for this mechanism; the owner-owes sense stays (proposal §9 sweep lists) | BUILD | `OwedDelivery`, `OwedCall`, `owe_steps`, `st.owed`, `OwedDeclarations`, 19 span texts in `rdm-transport/src`; 61 page lines | e1 (code), e3 (pages) |
| R14 | The demo (a Rafka replica, no exception) moves to `Resend` and implements both hooks | BUILD | `demo/` `replica.rs`, `consumer_example.rs` `Budget::` sites | e4 |
| R15 | The seed, the canary and a flame at the tip prove it; fast gate per landing, release and the four soak jobs once at the tip | BUILD | `scripts/cells.sh`, `scripts/rdm-gate-at.sh`, `tools/mesh-audit/i143-acceptance-jobs.json` | e0, e5 |

## § Contract boundaries — no open fork

Every choice is ruled: Q311 (one `Resend`, 3, one rule), Q312 (`on_node_investigated`), Q313 (attempts per resolved birth for a primary-addressed call), Q314 (`on_call_failed` with the stored reason, sender and receiver), Q315 (the caller owns attempts and time limit), Q316 (no application exception in RDM). The lead's picks under Q311 are recorded in the proposal: the time limit (§3), the next-tick investigation start for a spent-call mark (R-151.8), the end naming (R-151.4). A story that finds a page silent files a numbered question with the ruling-packet skill; it does not decide.

## § Seed spec

**Ordering:** e0 writes the canary RED first, then the seed RED, both at test cadence (gossip 500 ms, staleness 3000 ms: `scripts/cells.sh`), before e1 starts. Compilation failure is not RED.

### Estate fixture and request

```text
Fixture: planned:crates/rdm-test/tests/resend.rs, `mod resend;` in crates/rdm-test/tests/main.rs
Starting point: crates/rdm-test/tests/estate_handover.rs (real processes, Estate::bootstrap_on)
Estate: one mesh, two node-admins (mesh1.admin.1 the mesh-primary and fabric-primary, mesh1.admin.2),
        one rpc node mesh1.rpc.1, one more rpc node mesh1.rpc.2 as the caller (testkit rpc node)
Action: mesh1.rpc.2 opens one Status call to mesh1.rpc.1 with Resend::default(); the test kills
        mesh1.rpc.1's process (kill -9) before the call; reads the call's end, the caller's
        on_call_failed, the mesh-primary's report receipt, the investigation and on_node_investigated
        from Estate::spans (JSONL) and the testkit's hook recorder.
```

### Suite assertion

Planned cell: `resend::a_spent_call_ends_by_name_calls_on_call_failed_and_its_birth_is_investigated_on_the_next_tick`.

```rust
/// CONTRACT (node-rpc.md §43, node-recovery.md step 2, fabric-node-hooks.md on_call_failed and
/// on_node_investigated; Q311, Q313, Q314): a call to a killed birth ends NotSent(connect refused)
/// after exactly 3 attempts at the schedule's instants (0, c, 3c at cadence c); the caller's
/// on_call_failed runs once with sender = the caller's birth, receiver = the killed birth,
/// failure = NotSent(Connection(..)), attempts = 3; the mesh-primary records the report
/// (rdm.node_admin.node.update.via-call-failed-reported) and starts node investigation within one
/// gossip interval of it (no half-floor wait); on_node_investigated runs once with verdict `exited`
/// and next = `recreate`. Must NOT happen: a fourth attempt; a second on_call_failed for the same
/// call; a second investigation of the same birth within a staleness floor; the owner resending.
```

### What a passing seed proves

An application's node that cannot reach a peer learns it by name after three sends, acts on it at once through `on_call_failed`, and the fabric finds and re-creates the peer within a gossip interval of the report rather than after a staleness floor plus half a floor. It does not prove the per-birth counting through an election, the slow-dial reply window, or the responder's late ack: the cells below do.

## § RED/GREEN evidence — acceptance by executable proof

| Cell | Tier | RED at | GREEN at | Artifact / Jaeger |
|---|---|---|---|---|
| canary `resend::a_spent_call_mark_survives_a_mesh_primary_change` | e2e | pending | pending | pending |
| seed `resend::a_spent_call_ends_by_name_calls_on_call_failed_and_its_birth_is_investigated_on_the_next_tick` | e2e | pending | pending | pending |
| every e1/e2 cell named in its story | functional | pending | pending | pending |

## § Surfaces — one operation and one shared implementation

| Operation | Node RPC shape | Typed client or hook | Shared implementation | Refusal |
|---|---|---|---|---|
| every call | `CallOptions.resend: Resend` | `NodeRpcClient::call`, `call_stream`, `call_routed` | `until_end` | `NotSent`/`Indeterminate` by name when spent |
| primary-addressed call | `NodeTarget::Primary { mesh: Option<String> }` | the admin's upward and downward Status calls, the node's declarations | the resolver `node_config::owed` reads today, per attempt | `ReceiverGone` when the role has no birth |
| report | Status `ReportCallFailed` (position 38) | sent by the transport's owner on every node | the primary's `CommandView` record | `Applied`; `RejectedNotPrimary { why: SubjectUnknown }` |
| caller notice | `on_call_failed(CallFailedCtx)` | `NodeHooks` on every node | one call site after the call ends to its owner | none; a notice |
| primary notice | `on_node_investigated(NodeInvestigatedCtx)` | `Wiring` on every node-admin | one call site after the verdict is recorded | none; a notice |

## § Restart-recovery canary — applicability

Nothing i151 adds is durable: the delivery layer is memory (`delivery.rs` module doc: "Nothing here is durable"), the spent-call mark lives in the primary's `CommandView`, and both hooks are notices. What crosses a process boundary is the mark's meaning when the primary changes: node-recovery.md step 1 says every node-admin keeps the silence bookkeeping so a new mesh-primary continues the window; a spent-call mark has no such copy, so the rule is: a caller whose primary changed reports again on its next spent call, and the new primary marks from that report.

Canary, written first: `resend::a_spent_call_mark_survives_a_mesh_primary_change`.

```rust
/// CONTRACT (node-recovery.md step 1 and step 2; R-151.7 case 1, R-151.8): mesh1.rpc.2's call to
/// the killed mesh1.rpc.1 is spent and reported to mesh1.admin.1; mesh1.admin.1 is killed (kill -9)
/// before its watch's next tick; mesh1.admin.2 becomes mesh-primary (unheard past the floor); the
/// caller's next spent call to mesh1.rpc.1 is reported to mesh1.admin.2, which marks and
/// investigates it on its next tick; exactly one investigation of mesh1.rpc.1 is recorded fabric-wide
/// after the primary change. Must NOT happen: the report answered by a birth that is not the primary
/// above mesh1.rpc.1 as Applied; an investigation by mesh1.admin.2 with no report or silence mark of
/// its own.
```

Reviewer grep for `fs::write`, `OpenOptions`, `tokio::fs` in the i151 diff returns nothing new; if it does, this section is wrong.

## § OTLP span-assertion bar

Names follow the tree's own convention (`rdm.<component>.<entity>.<action>.<reason>`, `via-` direct); every new name passes `scripts/name-check.py`. Attributes are the ones the cells assert; no payloads, no secrets.

| Span | Emitted from | Required attributes | Asserted by |
|---|---|---|---|
| existing `rdm.node_rpc.delivery.update.via-resend` | `until_end`, attempt > 1 | `delivery_id`, `attempt`, `backoff_ms` | e1 cells (attempt count 2 and 3 only) |
| existing `rdm.node_rpc.delivery.update.via-attempt-ended` | `AttemptTask::run` | `delivery_id`, `attempts`, `path` | e1 cells (`attempts = 3`, `path = attempts-spent`) |
| planned `rdm.node_rpc.delivery.remove.via-attempts-spent` | `until_end`, the end of attempt 3 | `delivery_id`, `attempts`, `failure` (`not-sent:<reason>` / `indeterminate:<reason>`), `receiver_node_id`, `receiver_incarnation`, `elapsed_ms` | seed, e1 cells |
| planned `rdm.node_rpc.delivery.update.via-receiver-changed` | `until_end`, a primary-addressed call resolved to another birth | `delivery_id`, `previous_node_id`, `node_id`, `attempts_on_previous` | e1 Q313 cell |
| planned `rdm.mesh.node.update.via-on-call-failed` (mirrors `via-on-node-ready`) | the node's hook task | `node_id`, `receiver_node_id`, `receiver_incarnation`, `op_owner`, `op`, `failure`, `attempts` | seed |
| planned `rdm.node_admin.node.update.via-call-failed-reported` (mirrors `via-returned-reported`) | the primary's `ReportCallFailed` handler | `node_id`, `incarnation_id`, `reported_by`, `failure`, `outcome` (`marked` / `already-marked` / `subject-unknown`) | seed, canary |
| planned `rdm.node_admin.node.update.via-spent-call-mark` | the primary's own spent call or the report, when it marks | `node_id`, `incarnation_id`, `source` (`own-call` / `report`) | e2 cells |
| existing node.tickle and node.runtime.inspect spans (node-tickle.md, node-runtime-inspect.md contracts) | the investigation | as their pages say | seed, canary (one investigation) |
| planned `rdm.node_admin.node.update.via-on-node-investigated` (mirrors `via-on-build-ended`) | the primary's hook task | `node_id`, `incarnation_id`, `verdict`, `next` | seed |

Every e2e cell: `Estate::spans`, `Estate::artifact`, `Estate::record_trace_url`; the trace URL on the Jaeger on `.99` (port 16687) is the deliverable, printed by the cell and pasted in the story's close comment.

## § Per-increment seed-data obligation

No new entity, no new fixture language. e2 adds the `ReportCallFailed` golden to `crates/rdm-core/tests/status_wire.rs` (by hand, position 38) and the testkit's hook recorder (e0) so cells read which hook ran with which ctx.

## § Test tier coverage

| Selected | Tier | Exact test file / target / filter | Proof |
|---|---|---|---|
| [x] | unit | `rdm-core/src/delivery.rs` inline (`resend_after`), `rdm-transport/src/client.rs` inline | schedule instants; end naming |
| [x] | functional | `crates/rdm-transport/tests/delivery.rs`, `owed_route.rs` (renamed `resend_route.rs` in e1), `pool.rs`, `streaming.rs`; `crates/rdm-core/tests/status_wire.rs`; `crates/rdm-admin` tests | attempts, per-birth counting, stream after commit, late ack, wire position, mark and tick |
| [x] | e2e | `planned:crates/rdm-test/tests/resend.rs`, target `main`, filter `resend::` | seed, canary, demo hooks |
| [x] | load / chaos | the four `i143-rshape-soak-*` jobs in `tools/mesh-audit/i143-acceptance-jobs.json`, once at the tip (Luke: release and soaks once after the merge) | nothing regresses under the new counts |

Focused run: `scripts/cells.sh rdm-test main -- resend::`; per landing `scripts/rdm-gate-at.sh /home/admin/rdm-lanes/<n> fast`; the release suite once at the tip.

## § Pre-implementation thoughts (2026-11-01)

- **Hard part:** the per-birth counting (Q313) inside `until_end` without a second loop: the attempt counter is keyed by the resolved birth, and `receiver_gone` must distinguish "this birth is gone and the role names another" (count restarts, `via-receiver-changed`) from "the role has no birth" (`ReceiverGone`).
- **What goes wrong first:** a call site today relying on `Owed`'s endless wait (the node's declarations, the admin's check-ins) now ends after three attempts and its owner must handle the by-name end; e1.s1 lists every `Owed` site and e2.s4 gives each its handling, no site left to "it was fine before".
- **Persistence path:** none; see the canary section.
- **Dependencies:** i150 landed (lane 1's branch holds the proposal and this PRD); Rafka's pin bump and its own §11 build after e5.
- **Risk:** the responder's held-frame store has no bound (R-151.9): measured in e5's soak, bounded only with the number.
- **Unknown:** whether any Rafka call site needs `attempts` other than 3; Rafka decides (Q315, Q316).

## Implementation plan

Each story below is one issue in `drlukeangel/rust-distributed-mesh`, one lane, one agent, one landing through `lane-dispatch.sh` / `post-land-gate.sh` / `lane-land.sh`. A story's brief is its block pasted verbatim plus the proposal. A story is done when its acceptance lines hold and its cells are GREEN in the fast gate at the lane's tip; the lead reviews and lands.

### e0 — Proof substrate (before any behaviour change)

#### i151.e0.s1 — the testkit records hooks and the estate stem exists
- **Files:** `crates/rdm-test/src/estate.rs` (the testkit rpc node's hook wiring), `crates/rdm-test/tests/main.rs` (`mod resend;`), `planned:crates/rdm-test/tests/resend.rs`.
- **Change:** a `HookRecord` the testkit node and node-admin append `(hook name, ctx as JSON)` to under `Estate::artifact("hooks.jsonl")`; the stem file with the canary and the seed, both RED (they assert spans and hook records that do not exist yet); `cargo test` discovers two cells under `resend::`.
- **Spans:** asserts none yet; reads `Estate::spans`.
- **Acceptance:** `scripts/cells.sh rdm-test main -- resend::` reports 2 cells, both failing on their first assertion, not on compile or fixture; the RED SHA and the failing assertion text in the issue.
- **Size:** one agent, small.

#### i151.e0.s2 — pin the late ack (R12)
- **Files:** `crates/rdm-transport/tests/delivery.rs`.
- **Change:** one cell `a_response_to_a_call_its_caller_ended_is_acked_and_the_responder_stops` (CONTRACT: `Delivery::on_response` for an invocation removed from `calls` answers `Ack`; `push_loop` ends `acked`; must NOT happen: `Drop`, a resend after the ack). GREEN on main today.
- **Acceptance:** cell GREEN at the lane tip; its CONTRACT line quotes node-rpc.md §43 "a response is itself an owed message … acknowledged by the caller".
- **Size:** one agent, small.

### e1 — `rdm-transport`: the one mechanism

#### i151.e1.s1 — `Resend` and `TimeLimit` replace `Budget` and `once`
- **Files:** `crates/rdm-transport/src/client.rs` (`Budget`, `CallOptions`, `CallOptions::default`), `stream.rs` (`call_stream`), `forward.rs` (`carry_once`), every RDM site: `grep -rn 'Budget::\|once: true' crates demo --include=*.rs` (19 `Overall`, 14 `Owed`, 12 `Split`, 17 `Stream`, 1 `once`).
- **Change:** `pub struct Resend { attempts: u8, time_limit: TimeLimit }`, `pub struct TimeLimit(Duration)`, `Resend::default()` = 3 and 10 s; `CallOptions.resend`; `Budget` and `once` deleted; each site mapped per proposal §3 (`Overall(d)` → `TimeLimit(d)`; `Split { send, reply }` → `TimeLimit(reply)` with the dial inside the attempt; `Stream { send }` → `TimeLimit(send)`; `Owed` → `Resend::default()`; the tickle's `direct` → `attempts: 1`). No behaviour change beyond the type in this story: `until_end` still ignores `attempts` (s2 counts them). The mapping table (site → value → reason) in the issue.
- **Spans:** none new.
- **Cells:** existing transport cells GREEN; `a_call_options_default_is_three_attempts_and_ten_seconds`.
- **Acceptance:** `grep -rn 'Budget\b\|once: true' crates demo --include=*.rs` empty; fast gate GREEN.
- **Size:** one agent, medium (mechanical, compile-driven).

#### i151.e1.s2 — `until_end` counts attempts and ends by name
- **Files:** `client.rs` `until_end`, `AttemptEnd`, `Attempted`, `AttemptTask::run`.
- **Change:** `attempt` is compared with `opts.resend.attempts` after each unsent or unacked attempt; at the limit the task sends `Attempted::Spent(failure)` and returns `AttemptEnd::AttemptsSpent`; `failure` = `NotSent(last)` if `!committed`, else `Indeterminate(NoAck)`; acked-and-no-reply within `time_limit` from the ack = `Indeterminate(NoReply)`; the `TimeLimit` bounds the attempt's resolve+dial+send (replaces `deadlines.0`) and the reply from the ack (replaces `deadlines.1`); `hold_connection` runs inside the reply limit. Mint `rdm.node_rpc.delivery.remove.via-attempts-spent`.
- **Cells (`crates/rdm-transport/tests/delivery.rs`, fake pacing `Pacing { cadence, floor, jitter: false }`):** `a_call_whose_connect_is_refused_ends_not_sent_after_three_attempts_at_the_schedule` (instants 0, c, 3c; ends at 3c); `a_committed_call_never_acked_ends_indeterminate_after_three_attempts` (ends at 7c); `a_connect_that_hangs_ends_each_attempt_at_the_time_limit` (ends at 3·limit); `an_acked_call_with_no_reply_within_the_limit_ends_indeterminate_and_is_not_resent`; `a_stream_call_is_not_resent_after_its_commit_and_lives_while_its_connection_does`; `a_slow_dial_does_not_eat_the_reply_window`. Each CONTRACT names R-151.2/R-151.3/R-151.4 and the must-NOT (a fourth attempt; a resend after ack).
- **Acceptance:** all six GREEN; `via-attempt-ended` carries `attempts = 3`, `path = attempts-spent`; fast gate GREEN.
- **Size:** one agent, medium.

#### i151.e1.s3 — a receiver absent from the view spends an attempt
- **Files:** `client.rs` `until_resolved`, `owe_steps`.
- **Change:** delete `until_resolved`; `reach` answering `Resolve(Unknown)` is `Attempted::NotSent(Resolve(Unknown))` and the attempt ends; the early wake on the resolver's change stays in `pause`.
- **Cells:** `a_receiver_not_in_the_view_spends_an_attempt_and_the_call_follows_the_view_when_it_arrives` (view arrives after attempt 1: attempt 2 reaches it, 2 attempts total); `a_receiver_never_in_the_view_ends_not_sent_endpoint_unresolved_after_three_attempts`.
- **Acceptance:** GREEN; `grep -n until_resolved` empty.
- **Size:** one agent, small.

#### i151.e1.s4 — `NodeTarget::Primary` and attempts per resolved birth (Q313)
- **Files:** `crates/rdm-transport/src/resolve.rs` `NodeTarget`, `NodeResolver` (a `primary(mesh: Option<&str>)` answer from the installed view: the one `node_config::owed` reads by `node_type_primary` / `fabric_primary`), `client.rs` `until_end`, `receiver_gone`.
- **Change:** `NodeTarget::Primary { mesh: Option<String> }`; in `until_end` the attempt counter is per resolved `(node_id, incarnation)`: when a `Primary` target resolves to another birth the counter restarts, `rdm.node_rpc.delivery.update.via-receiver-changed` is minted, the same delivery id continues; `ReceiverGone` when the role has no birth and the previous birth is proven gone; `ExactNode` unchanged (three in all). `CurrentPath` counts per birth the same way (applied Q313).
- **Cells:** `a_primary_addressed_call_restarts_its_attempts_on_the_birth_the_view_names_next` (3 unanswered to A, view names B, B answers on attempt 1 of its own; `via-receiver-changed` once with `attempts_on_previous = 3`); `a_primary_addressed_call_ends_receiver_gone_when_the_role_has_no_birth`; `an_exact_node_call_keeps_three_attempts_in_all_across_nothing`.
- **Acceptance:** GREEN; the Q313 sentence (proposal §9, §43 new text) quoted in the CONTRACT.
- **Size:** one agent, medium.

#### i151.e1.s5 — the identifier sweep: `owed` → `resend` in the delivery sense
- **Files:** `rdm-core/src/forward.rs` (`OwedDelivery` → `ResendDelivery`, `Forward.owed` → `resend`), `client.rs` (`OwedCall` → `ResendCall`, `owe_steps` → `resend_steps`), `delivery.rs` (`st.owed` → `held`, `owe_frame`/`owe_response`/`owe_response_in`/`owe_again`/`owed_frames`/`owed_frames_of`/`with_owed`/`owed_drop`/`OwedOne` per proposal §9), `rdm-node/src/node.rs` (`OwedDeclarations` → `Declarations`), the 19 span texts, `crates/rdm-transport/tests/owed_route.rs` → `resend_route.rs`. The owner-owes names listed in proposal §9 are NOT touched.
- **Change:** rename only; no behaviour. Forward's field rename is a wire-visible name in the envelope page (e3 lands the page line with it) and a durable-format commit marker per the canon if the serialized name changes; the story states which.
- **Acceptance:** `grep -rniw 'owed\|owe' crates/rdm-transport/src crates/rdm-core/src/delivery.rs crates/rdm-core/src/forward.rs` returns only lines the proposal §9 lists as the other sense; name check GREEN; fast gate GREEN.
- **Size:** one agent, small-medium (mechanical).

### e2 — `rdm-admin` and `rdm-node`: the report, the mark, the hooks

#### i151.e2.s1 — the two no-reply loops go; their answer turns stay (R7)
- **Files:** `crates/rdm-admin/src/node_config.rs` `owed`, `Hop`, `Owed`; `crates/rdm-admin/src/admin/recovery.rs` `report_terminate`, `terminate_once`, `TerminateTurn`, `OwedWake`.
- **Change:** both call with `NodeTarget::Primary { mesh }` and `Resend::default()`; the turn "ended with no reply → send again to the primary the view names next" is deleted (the transport does it, s4 of e1); the turns on `RejectedNotPrimary`, `NotReady` (root), `BuildInProgress`, `unknown-node`, `NotFabricPrimary` stay, each re-sending as one new call (a domain answer, node-rpc.md §52 allows); `OwedWake::View` goes with the deleted turn.
- **Cells (`crates/rdm-admin` tests, existing stems for these two):** `a_config_update_sent_to_a_dead_mesh_primary_reaches_the_one_the_view_names_next_inside_one_call` (reads `via-receiver-changed`, `sends = 1`); `a_returned_birth_report_answered_build_in_progress_is_sent_again_and_one_ended_with_no_reply_is_not_looped_by_the_submitter`.
- **Acceptance:** GREEN; `grep -n 'stays owed: sent again' crates/rdm-admin/src` empty.
- **Size:** one agent, medium.

#### i151.e2.s2 — the spent-call mark and the next-tick investigation (R8)
- **Files:** `crates/rdm-admin/src/node_commands.rs` `CommandView` (`unanswered_since`, `close_unanswered`), `primary_watch.rs` `Looker::command_unanswered` and the watch's tick, `offline.rs` `TickleClient::command_unanswered`.
- **Change:** `CommandView::mark_spent(node_id, incarnation, source)` records the spent instant; `unanswered_since` answers it at once (no floor) beside today's floor-old rule for a command's send; the watch's tick investigates a birth with a spent mark on its next tick (no half floor), a silence mark keeps its half floor; once per silence as today; mint `rdm.node_admin.node.update.via-spent-call-mark`. The admin's own spent calls mark through the transport's end (`Attempted::Spent` → the admin's `send_command_reaching` / `carry_lifecycle_reaching` callers).
- **Cells:** `a_spent_call_marks_its_birth_at_once_and_the_watch_investigates_on_its_next_tick` (fake clock: tick at ≤ 1 gossip interval after the mark; a silence mark at half a floor); `a_second_spent_call_to_a_marked_birth_adds_no_second_investigation_within_a_floor`; `an_answer_from_the_birth_clears_its_spent_mark`.
- **Acceptance:** GREEN; node-recovery.md step 2 new text (proposal §9) landed in the same commit, Luke's yes on it in the issue.
- **Size:** one agent, medium.

#### i151.e2.s3 — `ReportCallFailed` on the wire and at the primary (R9)
- **Files:** `crates/rdm-core/src/status.rs` (`StatusRequest`, appended at position 38; `CallFailure`; `Birth` if not already a type), `crates/rdm-core/tests/status_wire.rs` (golden by hand), `crates/rdm-admin/src/status_rpc.rs` (the handler), `docs/architecture/node-rpc-envelope.md` Status (the text in proposal §9, landed with this commit).
- **Change:** `ReportCallFailed { sender: Birth, receiver: Birth, owner: OpOwner, op: u8, failure: CallFailure }`; the primary above the receiver answers `Applied` and calls `CommandView::mark_spent(.., source = report)`; any other receiver answers `RejectedNotPrimary { why: NotPrimary::SubjectUnknown }`; mint `rdm.node_admin.node.update.via-call-failed-reported`. The wire marker per the canon in the commit.
- **Cells:** `status_wire::report_call_failed_round_trips_at_position_38_and_moves_no_earlier_position` (the golden; every earlier variant's bytes unchanged); `a_report_from_a_node_marks_the_birth_at_its_mesh_primary_and_is_refused_subject_unknown_elsewhere`.
- **Acceptance:** GREEN; the envelope page diff is exactly proposal §9's text.
- **Size:** one agent, medium.

#### i151.e2.s4 — every caller reports, the report never reports; every `Owed` owner handles its by-name end
- **Files:** `crates/rdm-node/src/node.rs` (`declare_once`, `declare_until_mesh_primary_moves`), the transport client's owner on every node (`rdm-node`, `rdm-admin` `send_command_reaching`, `carry_lifecycle_reaching`, `storage_change::command_reply`), `recovery.rs`, `fabric_handover.rs`, `build_op.rs`, `investigate.rs` (the 14 former `Owed` sites from e1.s1's table).
- **Change:** after a call ends `Spent`, the owner sends `ReportCallFailed` to the primary above the receiver per R-151.7 (a node of a mesh → its mesh-primary; mesh-primary → the fabric-primary; fabric-primary or `Endpoint` → nothing); the report itself carries a flag in the client (`CallOptions.is_report: true`, or the op) so its own spent end is spanned and dropped, never reported, marked or hooked; each former-`Owed` owner's by-name end is handled (the node's declaration: the next declaration goes, the spent one is the mesh-primary's to recover from the node's record at its next join, states.md; the admin's check-ins: the same). The table owner → handling in the issue, all 14 rows.
- **Cells:** `a_node_whose_call_is_spent_reports_it_to_its_mesh_primary_and_a_spent_report_is_dropped_by_name`; `a_mesh_primary_whose_call_to_a_member_is_spent_marks_without_a_report`; `an_admin_whose_call_to_a_mesh_primary_is_spent_reports_it_to_the_fabric_primary`; `a_spent_call_to_the_fabric_primary_reports_nothing`.
- **Acceptance:** GREEN; no `Report` of a report in any span of the cells' JSONL.
- **Size:** one agent, medium-large; split into s4a (reporting) and s4b (the 14 owners) at dispatch if the owner table exceeds a day.

#### i151.e2.s5 — `on_call_failed` (R10)
- **Files:** `crates/rdm-node/src/node_hooks.rs` (`OnCallFailed` trait beside `OnNodeReady`, `NodeHooks::with_on_call_failed`, `spawn_on_call_failed`), `crates/rdm-core` (`CallFailedCtx`), the admin's wiring for its own calls, `docs/architecture/fabric-node-hooks.md` (the row and section in proposal §7, landed with this commit), `crates/rdm-test/src/estate.rs` (the testkit implements it into the hook record).
- **Change:** one call site: after a call ends `Spent` to its owner, on a task of its own, `rdm.mesh.node.update.via-on-call-failed`; ctx fields per Q314; `context` = the spent call's trace context.
- **Cells:** `a_spent_call_calls_on_call_failed_once_with_the_stored_reason_both_births_and_the_calls_trace_context`; `an_answered_call_never_calls_on_call_failed`.
- **Acceptance:** GREEN; the hooks page diff is proposal §7's text; the seed's `on_call_failed` assertions pass.
- **Size:** one agent, medium.

#### i151.e2.s6 — `on_node_investigated` (R11)
- **Files:** `crates/rdm-admin/src/wiring.rs` (`NodeInvestigatedNotice` beside `BuildEndedNotice`, `Wiring.on_node_investigated`), `node_investigation.rs` / `primary_watch.rs` (the one call site after the verdict is recorded and `next` decided per node-recovery.md step 3), `fabric_recovery.rs` (the `running` exception → `next = none`), `fabric-node-hooks.md` (proposal §7 text), the testkit.
- **Change:** `NodeInvestigatedCtx { fabric_id, mesh_id, node_id, incarnation, path, name, verdict, next, context }`; `rdm.node_admin.node.update.via-on-node-investigated`; called by the primary that investigated (the fabric-primary for a mesh-primary).
- **Cells:** `an_investigation_calls_on_node_investigated_once_with_its_verdict_and_next` (verdicts `reached` → `rejoin`, `exited` → `recreate`); `the_fabric_primary_calls_it_for_a_mesh_primary_it_investigated`; `fabric_recovery_running_exception_is_next_none`.
- **Acceptance:** GREEN; the seed's `on_node_investigated` assertion passes; seed GREEN end to end at this story's tip.
- **Size:** one agent, medium.

### e3 — Pages, rulings, generated docs

#### i151.e3.s1 — the page sweep and the INDEX notes
- **Files:** `docs/architecture/node-rpc.md` §34, §43, §45 (proposal §9 exact text, where not already landed by e1/e2), the `owed`/`budget` sweep over 14 pages / 61 lines (`grep -rniw 'owed\|budget' docs/architecture`, each line kept or changed per the two senses, the list in the issue), `docs/plans/rulings/INDEX.md` notes on Q44, Q52, Q182, Q210 ("superseded in part by Q311: `Resend`"), `rdm-docgen` regenerated, `scripts/name-check.py` GREEN.
- **Acceptance:** `grep -rn 'Budget::' docs/architecture` empty; `grep -rniw owed docs/architecture` returns only owner-owes lines, each named; docgen diff reviewed; Luke's yes on each old/new recorded in the issue.
- **Size:** one agent, small-medium (text only; the lead may do it).

### e4 — The demo is a Rafka replica (R14)

#### i151.e4.s1 — demo on `Resend`, both hooks implemented
- **Files:** `demo/` `replica.rs`, `consumer_example.rs` (the `Budget::` sites), the demo's node-admin and broker wiring.
- **Change:** every call on `Resend::default()` or its own value with a one-line reason; the broker's writer-like path acts on `on_call_failed` (asks the next replica on `NotSent`, logs `Indeterminate` as the app's idempotency decision); the demo node-admin implements `on_node_investigated` and writes its own "out of the write set" mark on `recreate`. No rpc node in the demo (memory: demo is a Rafka replica).
- **Cells:** `resend::the_demo_broker_asks_the_next_replica_on_call_failed_and_its_admin_acts_on_recreate` in the estate stem (spans + hook record).
- **Acceptance:** GREEN; `scripts/demo-estate.sh up` runs with the new binaries.
- **Size:** one agent, medium.

### e5 — Proof at the tip

#### i151.e5.s1 — one exported run read as a flame, then release and soaks once
- **Change:** at the merged tip: the seed and canary run with `RDM_EVIDENCE_DIR` and OTEL to `.99`; the flame of the spent call → report → mark → investigation → both hooks read in Jaeger and linked; `scripts/rdm-gate-at.sh <lane> release` once; the four `i143-rshape-soak-*` jobs once; the held-frame store's peak read from a new `DeliveryStats.held_frames_peak` counter and recorded (R-151.9's bound is set in a follow-up issue with that number, never before).
- **Acceptance:** Jaeger trace URL in the issue; release GREEN; soaks GREEN or each red read (never re-rolled) and filed.
- **Size:** the lead.

### e6 — Documentation (final epic)

#### i151.e6.s1 — `docs/features/resend/`
- **Files:** `planned:docs/features/resend/overview.md` (what a call is now; the schedule; the two hooks; the report; what is not durable), `planned:docs/features/resend/how-to.md` (setting `Resend` per call, implementing `on_call_failed` and `on_node_investigated`, reading the spans), `planned:docs/features/resend/runbook.md` (a birth marked and investigated too often; a report refused `SubjectUnknown`; a primary-addressed call that never ends; the held-frame store growing), `docs/eng/consumer-guide.md` updated.
- **Acceptance:** the three files exist with the sections the template names, examples run against the tip, links resolve.
- **Size:** one agent, small-medium; written by the e2/e4 implementers while the context is fresh.

### Files likely to touch

`crates/rdm-transport/src/{client,resolve,stream,forward,delivery}.rs`; `crates/rdm-core/src/{status,forward,outcome,catalog}.rs`; `crates/rdm-admin/src/{node_config,node_commands,primary_watch,offline,status_rpc,wiring,investigate,node_investigation}.rs`, `admin/recovery.rs`; `crates/rdm-node/src/{node,node_hooks}.rs`; `crates/rdm-test/src/estate.rs`, `tests/{main,resend}.rs`; `demo/`; the pages named above.

## § Per-feature documentation deliverable — final epic

See e6.s1. Contract changes land with the epic that changes behaviour, never deferred to e6.

## Verification

- [x] Every requirement has an epic; no runtime claim without a run.
- [x] No open fork: Q311–Q316 cover every choice; the lead's picks are in the proposal.
- [ ] Canary RED first, then seed RED (e0.s1), both at test cadence.
- [ ] Every e1/e2 cell GREEN at its story's tip; fast gate GREEN per landing (`post-land-gate.sh`).
- [ ] The page text landed with the code that changes the behaviour; Luke's yes on each exact old/new recorded in the story's issue.
- [ ] The `ReportCallFailed` golden shows position 38 and no earlier position moved; the wire marker in the commit.
- [ ] `on_call_failed` once per spent call; `on_node_investigated` once per verdict; neither ever for a report.
- [ ] A primary-addressed call follows an election inside one call (`via-receiver-changed`); the two admin loops are gone.
- [ ] A spent-call mark is investigated on the next tick; a silence mark still waits half a floor.
- [ ] Release suite and the four soaks once at the tip; the held-frame peak recorded.
- [ ] Jaeger trace URL of the seed's flame in e5's issue.
- [ ] `docs/features/resend/` three files; name check GREEN; docgen regenerated.
- [ ] i143 PRD row and this PRD updated at every landing (the record).

### Source-doc propagation

- [ ] node-rpc.md, node-rpc-envelope.md, node-investigation.md, node-recovery.md, fabric-node-hooks.md reconciled (e3).
- [ ] rulings INDEX Q44, Q52, Q182, Q210 noted; Q311–Q316 carry the landing SHAs.
- [ ] `docs/eng/consumer-guide.md` and `CLAUDE.md` (no env var changes expected; verify).
- [ ] Rafka: the pin bump and proposal §11 tracked in rafka-v2 by the i141 lead; linked here when opened.

## § Dispatch ordering — last substantive section

At most three agents at once (the standing cap). Disjoint files only; shared files are one seat or sequenced.

| Wave | Stories | Files owned | Why this wave |
|---|---|---|---|
| 0 | e0.s1, e0.s2 | `rdm-test` estate/stem; `rdm-transport/tests/delivery.rs` | RED cells before behaviour; disjoint |
| 1 | e1.s1 | every call site (compile-driven) | one seat: touches every crate; nothing else lands meanwhile |
| 2 | e1.s2, e1.s5 | `until_end`; the rename (separate symbols, same file: sequenced s2 then s5 in one seat if the diff collides) | the count and the names |
| 3 | e1.s3, e1.s4, e2.s3 | `until_resolved`; `resolve.rs` + `until_end` per-birth; `status.rs` + `status_rpc.rs` + envelope page | e1.s3 and e1.s4 both touch `until_end`: one seat, s3 then s4; e2.s3 disjoint |
| 4 | e2.s1, e2.s2 | `node_config.rs`, `recovery.rs`; `node_commands.rs`, `primary_watch.rs`, `offline.rs` | needs `NodeTarget::Primary` (e1.s4) and the wire (e2.s3) |
| 5 | e2.s4, e2.s5 | the owners (node.rs, admin call sites); `node_hooks.rs`, hooks page | e2.s4 needs e2.s2 (the mark) and e2.s3 (the report); e2.s5 touches `node.rs` too: sequenced after s4 or one seat |
| 6 | e2.s6, e4.s1 | `wiring.rs`, investigation call site; `demo/` | the second hook; the demo after both hooks exist |
| 7 | e3.s1, e5.s1 | pages, INDEX, docgen; the tip run | text after the code; proof at the merged tip |
| 8 | e6.s1 | `docs/features/resend/` | last |

**Critical path:** e0.s1 → e1.s1 → e1.s2 → e1.s4 → e2.s3 → e2.s2 → e2.s4 → e2.s5 → e2.s6 → e5.s1 → e6.s1.

**Format changes:** `ReportCallFailed` appends a Status position (wire marker); `Forward.owed` → `resend` is a field rename whose serialized name e1.s5 checks (postcard is positional, so likely no byte change; the story states it).

## § Retro (on ship)

Not shipped. e5 and e6 record surprises, the seed's omissions, shipped SHAs, counts, durations, artifacts and the Jaeger links.

## Cross-references

- Design: [i151 proposal](proposal/151-consolidate-event-callers-add-investigation-hook.md) (R-151.1–R-151.13)
- Rulings: [INDEX](rulings/INDEX.md) Q311, Q312, Q313, Q314, Q315, Q316; superseded in part: Q44, Q52, Q182, Q210
- Contracts: [node-rpc.md](../architecture/node-rpc.md), [node-rpc-envelope.md](../architecture/node-rpc-envelope.md), [node-investigation.md](../architecture/lifecycles/node-investigation.md), [node-recovery.md](../architecture/lifecycles/node-recovery.md), [fabric-node-hooks.md](../architecture/fabric-node-hooks.md)
- Dependencies: [i150](i150-node-writes-lifecyle-updates.md) (lands first), [i143 PRD](i143-prd.md) (the record), Rafka's §11 build in rafka-v2 (i141 lead)
- Workflow: [CLAUDE.md](../../CLAUDE.md); template: rafka-v2 `docs/eng/templates/prd-seed-template.md` (adapted for RDM as i150's was)
