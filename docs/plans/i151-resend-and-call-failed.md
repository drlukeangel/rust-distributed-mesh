# i151 — one `Resend` for every Node RPC call; `on_call_failed` at the caller, `on_node_investigated` at the primary

**Initiative:** `i151`, five stories, one issue each
**Status:** opened 2026-11-01; BUILD; starts after i150 (rdm#245) lands
**Rulings:** Q311 (one `Resend`, three attempts, one rule for every caller), Q312 (`on_node_investigated`), Q313 (attempts per resolved birth for a primary-addressed call), Q314 (`on_call_failed`), Q315 (the caller owns attempts and time limit), Q316 (no application exception in RDM). Luke, 2026-11-01: build only what these say; no mechanism for a case nothing has shown.
**Pages:** node-rpc.md §34, §43, §45; node-investigation.md; fabric-node-hooks.md; node-rpc-envelope.md Status. Exact old/new text in the [proposal](proposal/151-consolidate-event-callers-add-investigation-hook.md) §9 and §7, landed with the story that changes the behaviour, Luke's yes on each.
**Owner:** the RDM lead; one agent per story, in a lane, after i150 lands

## What is built

1. `Resend { attempts, time_limit }` on every call, `Resend::default()` 3 and 10 s, set by whoever opens the call. `Budget` and `once` go. No other time words.
2. A call whose attempts are spent ends to its owner by name: `NotSent` with the last reason when nothing crossed the commit cut, `Indeterminate` otherwise.
3. A call addressed to a primary (`NodeTarget::Primary { mesh }`) counts its attempts per birth the view names for the role and ends `ReceiverGone` when the role has none. An exact-birth call keeps three in all.
4. The calling node calls `on_call_failed(CallFailedCtx { sender, receiver, reason, context })` once per spent call. `sender` and `receiver` are each `(node_id, incarnation, path, name)`; `reason` is the call's stored end (`NotSent(..)` or `Indeterminate(..)`); `context` the call's trace context. A notice.
5. The birth is investigated by the primary above it, as a birth whose command was never sent is today (node-recovery.md step 1's mark; no new timing rule). The mesh-primary marks its own spent call; any other caller reports the birth with Status `ReportCallFailed { sender, receiver, reason }`, answered `Applied`, or `RejectedNotPrimary { why: NotPrimary::SubjectUnknown }` for a birth the receiver is not the primary above. A spent report is dropped; nothing reports a report.
6. The investigating primary calls `on_node_investigated(NodeInvestigatedCtx { fabric_id, mesh_id, node_id, incarnation, path, name, verdict, next, context })` once after it records the verdict; `next` is `rejoin` or `recreate`. A notice.
7. The two admin workflow loops that resend on "no reply" (`node_config::owed`, `report_terminate`) go; their turns on a domain answer stay.
8. The demo (a Rafka replica) moves to `Resend` and implements both hooks.

Not built: no new wire fields beyond `Resend` and `ReportCallFailed`, no per-call route policy change, no new store, no bound on anything that has not been measured, no extra spans beyond the two hook notices.

## The numbers (production cadence; the test cadence scales them)

| Mechanism | Value | Source |
|---|---|---|
| Attempts | 3, the caller's | Q311, Q315 |
| Delay before attempt n+1 | cadence × 2^(n-1), ±25 % jitter, capped at the floor: 2 s, 4 s | `delivery.rs` `Pacing`, existing |
| Ack wait inside an attempt | that attempt's delay | `until_end` `ack_by`, existing |
| `TimeLimit` | 10 s default, the caller's; a call whose receiver waits on its own bound gives that bound plus 10 s | Q311, Q315; `CallOptions::default` existing |
| A spent call, connect refused | ends ~6 s | the three rows above |
| A spent call, committed and never acked | ends ~14 s | the three rows above |
| Investigation after the mark | as node-recovery.md step 2 has it today | existing |

## Stories

### i151.s1 — `rdm-transport`: `Resend`, the count, the per-birth rule
- `client.rs`: `Resend`, `TimeLimit`; `CallOptions.resend`; `Budget` and `once` deleted; every RDM call site mapped (`Overall(d)` → `TimeLimit(d)`; `Split { send, reply }` → `TimeLimit(reply + 10 s)`; `Stream { send }` → `TimeLimit(send)`; `Owed` → default; the tickle's direct probe `attempts: 1`, node-recovery.md step 2). `until_end` counts attempts against `resend.attempts`, ends by item 2, `until_resolved` goes (an unresolved receiver spends an attempt). `resolve.rs`: `NodeTarget::Primary { mesh }` with the per-birth count (item 3). Rename `OwedCall` → `ResendCall`, `OwedDelivery` → `ResendDelivery`; nothing else renamed.
- Cells (`crates/rdm-transport/tests/delivery.rs`, jitter off): a connect refused ends `NotSent` after three attempts at 0, c, 3c; committed and never acked ends `Indeterminate` after three; a primary-addressed call restarts its count on the birth the view names next and ends `ReceiverGone` when the role has none; an exact-birth call keeps three.
- Acceptance: `grep -rn 'Budget\b\|once: true' crates demo` empty; fast gate green.

### i151.s2 — `rdm-admin`, `rdm-node`: the loops go, the mark, the report
- `node_config::owed` and `report_terminate` call with `NodeTarget::Primary` and `Resend::default()`; the no-reply turns deleted, the domain-answer turns kept.
- A mesh-primary's own spent call marks the birth as a never-sent command marks it today (`CommandView`, node-recovery.md step 1); `ReportCallFailed` appended at Status position 38, golden by hand in `status_wire.rs`, served by the primary above the birth into the same mark; every other caller sends it after a spent call; a spent report is dropped.
- Cells: a mesh-primary's spent call to a member marks it and the watch investigates on the page's clock; a node's spent call is reported to its mesh-primary; a report to a birth the receiver is not the primary above is refused `SubjectUnknown`; a spent report is dropped by name; `status_wire` position 38 and no earlier position moved.

### i151.s3 — the two hooks
- `rdm-node/src/node_hooks.rs`: `OnCallFailed` beside `OnNodeReady`; called once after a spent call ends to its owner; span `rdm.mesh.node.update.via-on-call-failed`.
- `rdm-admin/src/wiring.rs`: `NodeInvestigatedNotice` beside `BuildEndedNotice`; called once after the verdict is recorded; span `rdm.node_admin.node.update.via-on-node-investigated`.
- fabric-node-hooks.md rows and sections (proposal §7, without `owner` and `op`), landed here.
- Cells: a spent call calls `on_call_failed` once with both births and the reason; an answered call never does; an investigation calls `on_node_investigated` once with `verdict` and `next`.

### i151.s4 — pages and the demo
- node-rpc.md §34, §43, §45, node-investigation.md, node-rpc-envelope.md Status: the proposal §9 text (the Q313 sentence inside §43); `owed`/`Budget` replaced where they name this mechanism; rdm-docgen regenerated; INDEX notes on Q44, Q52, Q182, Q210.
- `demo/`: every call on `Resend`; the broker asks the next replica on `on_call_failed`; the demo node-admin acts on `on_node_investigated` `recreate`.

### i151.s5 — the proof
- One estate cell, `planned:crates/rdm-test/tests/resend.rs`: a caller's call to a hung receiver (`ExactRuntime::hang`, existing) ends after three attempts; `on_call_failed` once; the receiver investigated and re-created; `on_node_investigated` once with `recreate`. Its flame read in Jaeger on `.99` and linked in the issue.
- Fast gate per landing; the release suite and the four soak jobs once at the tip.

## Cross-references

[proposal](proposal/151-consolidate-event-callers-add-investigation-hook.md) · [INDEX](rulings/INDEX.md) Q311–Q316 · [i150](i150-node-writes-lifecyle-updates.md) lands first · [i143 PRD](i143-prd.md)
