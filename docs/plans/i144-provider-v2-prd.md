# i144 provider-v2 — the deployment provider reports the exact runtime's true state, for pids and containers alike

**Initiative / Epic / Story ID:** `i144`
**Sprint:** post-i143 beta
**Status:** opened 2026-10-15
**Architecture doc:** `docs/architecture/deployment-provider.md` — the FINAL VISION this PRD's gaps reach (rewritten with this PRD; its text lands on Luke's yes); `docs/architecture/lifecycles/node-runtime-inspect.md`, `lifecycles/node-recovery.md`, `lifecycles/node-tickle.md`, `lifecycles/node-restart.md` (their lines, below, land with the code that changes them)
**Signed off:** qa [YYYY-MM-DD HH:MM UTC] · dev [YYYY-MM-DD HH:MM UTC]
**Approved:** Luke [YYYY-MM-DD HH:MM UTC]
**Owner:** the i143 lead, RDM engineers in pool lanes

> Record of the decision this PRD builds: the proposal `docs/plans/proposal/update-provider-abstraction-to-actually-work.md` with the lead's review and the a1 architect's review (Luke's tightened text of 2026-10-15): items 1-4 and 6 and Q1 yes; item 5 no (the existing recovery ladder, woken at once); item 7 no (the pending receipted step resumes); D1 struck; D2 and D2b are i143 e12.s89's rows and this PRD's dependency.

## Context

RDM's node-admin learns a node is in trouble two ways: the node stops answering (silence, a failed call, an `unreached` tickle), and the deployment provider that launched the node's runtime says what it sees of that exact runtime. Today the provider can say only `Running`, `Exited` or `Unknown`. A process the operating system holds suspended (SIGSTOP) and a paused container both read `Running`, so the mesh falls back to inferring from silence, through the staleness floor and the R-FM1 hold, what the provider could have read in one call. The R-shape release cell C17 (`mock_drain_and_retire_preserve_status_and_terminal_proof`, trace `3100a0ae262f62d82e02953ef3c98d36`) showed the cost: a SIGSTOP'd compute node was held `stopping` by a status set before the node admitted anything, never inspected for 70 s, and its restart refused when it came back. The customer is whoever runs a fabric: a suspended node must be seen as suspended, a hung one as hung, a gone one as gone, with the same words whether the runtime is a pid or a container.

## Requirements

> **CANON (Luke ruling 2026-09-11):** a PRD is the GAP LIST and the BUILD LIST. Every row resolves to **HOLDS** (file:line) or **BUILD** (file:line of today, and the epic that builds it). No deferrals.

1. When a node's runtime is suspended by the operating system, the mesh-primary knows it within one recovery tick of the first unreachable command, records it, cancels any hung-runtime hold already open on that birth, and neither holds nor terminates nor replaces the node for that reason.
2. When the runtime resumes, the mesh-primary sees it on its next inspection and the pending lifecycle step resumes from its receipts; nothing reopens a terminally failed attempt on its own, and nothing sends `node.start` because the provider said `Running`.
3. The provider's answers are identical in meaning for a process and a container: `Running`, `OsStopped`, `Exited { code }`, `Unknown`. A caller reads the common result, never `/proc` or Docker.
4. An unreadable or partial inspection is `Unknown`, never exit proof; a pid gone entirely with its start identity matched stays `Exited`.
5. The provider sends no signal but the stop ladder's and the terminate's; it never continues a suspended runtime.
6. A node-admin restarted at its data dir re-adopts every runtime from the published RuntimeFacts and gets the same inspect answer, through `node.get`.
7. The existing chaos suite (`crates/rdm-test`, the R-shape release jobs) passes unchanged on these primitives: this PRD adds no chaos cell.

| # | requirement row | disposition | coordinate | epic |
|---|---|---|---|---|
| R1 | `DeploymentStatus` carries four states | BUILD | `crates/rdm-admin/src/deployment/provider.rs:298` has three | e1 |
| R2 | process inspect reads the state letter across the thread group and tracked children | BUILD | `process.rs:127` `alive` reads only `Z`; `process.rs:443` `inspect` answers `Running` for any live task | e1 |
| R3 | pid gone with start identity matched is `Exited`; unreadable task files are `Unknown` | BUILD (partly HOLDS) | `process.rs:113` `is(pid, start)` HOLDS the identity; `process.rs:133-136` folds "no task dir and no /proc entry" into gone; nothing yields `Unknown` for a partial read | e1 |
| R4 | container inspect maps `paused` to `OsStopped` | BUILD | `container.rs:621` maps `paused` to `Running` | e1 |
| R5 | every match on `DeploymentStatus` handles `OsStopped` by name | BUILD | 42 `DeploymentStatus::Running` arms in rdm-admin and rdm-node; `container.rs:630` `signal_stop` signals `Running` only | e1 |
| R6 | `node.runtime.inspect` answers `os-stopped`; span verdict `os-stopped` | BUILD | `docs/architecture/lifecycles/node-runtime-inspect.md:7,27,31`; the span site in recovery | e2 |
| R7 | `os-stopped { since }` is a durable status-row fact on `node.get`, not a gossip status | BUILD | the `held-running` fact (R-FM1, landed `549493fe`) is the pattern; no `os-stopped` fact exists | e2 |
| R8 | the recovery tick re-inspects an `os-stopped` birth every tick and handles each transition | BUILD | `crates/rdm-admin/src/offline.rs:268` `tick` inspects only TRUE OFFLINE births | e2 |
| R9 | a reachability-related `NotSent` (connect refused, timed out, endpoint unresolved) marks the birth `PendingReconnect` at once as an observer mark that never overwrites an authored lifecycle status; a `NotSent` from a local cause marks nothing | BUILD | `recovery.rs:543` `watched_births` watches `PendingReconnect` and `Dead` only; nothing marks on NotSent; `NotSent` carries its cause on the call end | e2 |
| R10 | a new authority revalidates a stored `os-stopped` fact | BUILD | `recovery.rs:29-33` drops a gone birth's bookkeeping; no revalidation of a held fact | e2 |
| R11 | the pending receipted step resumes; never `node.start` on `Running` alone | HOLDS | `build.md:363` (a live run is reattached, never re-claimed; receipts decide); `control_plane.rs:611-613` serves a start only for a stopped node | e3 (cells) |
| R12 | a new `node.restart` against a stopped node is refused by name; a resubmission with a completed stop receipt continues at start | BUILD | `control_plane.rs:618` refuses every non-live restart, resumption included; the refusal name `node-not-live` is in no page | e3 |
| R13 | adoption from a RuntimeFact alone, foreign domain refused by name | HOLDS | `provider.rs:243-266` `adopt`; `AdoptRefusal::ForeignControlDomain` | e4 (canary) |
| R14 | the launch record is durable before the executable runs; `find` tells running, exited and never-launched apart | HOLDS | `process.rs:297` (`deployment.json`), `process.rs:424` `find`; `container.rs:439` `find_by_name`; cells `process.rs:566-658`, `tests/container_proof.rs:104-146` | e4 |
| R15 | the stop ladder and the terminate are the only signals | HOLDS | `process.rs:373-424`, `container.rs:630`; no other `kill(2)` or `docker` signal in `crates/rdm-admin/src/deployment` | e4 (cell) |
| R16 | D2 / D2b: `stopping` on admission, the `NodeLeaving` sender deleted | LANDED `69b3ced8` (i143 e12.s89; gate 66 s; RED `584a5e12`, GREEN `fcc63cb1`; traces `403bab4d0c31b1828d0558ca503c077b`, `4151b63acac7763e811ab7289c6ee5ac`) | `pipeline.rs` `stop_steps_as`, `runner.rs` `leaving` | edge into e2, e3 |
| R17 | `Running → OsStopped` cancels an open R-FM1 hold (row removed, pending terminate void); a resumed-but-unreachable runtime opens a fresh hold; the terminate step re-inspects and fires only on `Running` at its instant | BUILD | `recovery.rs:233-250` counts `Running` verdicts toward expiry; `recovery.rs:144` clears only on the birth's return; the terminate step does not re-inspect | e2 |
| R18 | an explicit `node.delete` of an `OsStopped` birth skips the Node RPC stop by name and begins at `TerminateRuntime`; recovery never does | BUILD | `control_plane.rs:716-724` routes a `held-running` birth's delete to its recoverer (the pattern); `runner.rs:498` reads the row; no `os-stopped` arm; node-delete.md has no such row | e3 |
| R19 | `Running` means nonterminal and not OS-suspended, nothing more; Docker `created` / `restarting` map to it conservatively | BUILD (page) / HOLDS (code) | `container.rs:621-623` already maps them; the page said "executing" | e1 (doc line with the enum) |
| R20 | the `os-stopped` fact is a typed keyed row: `NodeOsStoppedRow { node_id, name, incarnation_id, since, inspections }`, key the exact birth, under `os-stopped` with format marker `node-os-stopped/1`, writer the mesh-primary, removed on `Running` or `Exited`, folded by a successor only after its own inspection | BUILD | `status_storage.rs:171` `NodeHeldRunningRow` and `:520` `NODE_HELD_RUNNING_FORMAT` are the pattern | e2 (durable format addition: the landing carries the marker) |

## § Open forks

None reach Luke from this PRD. The tree answers the two that looked like forks:

- *Should the executor run its own tickle-and-inspect on NotSent?* No: `offline.rs:268` is the ladder, and C17's 70 s of silence was the stale `stopping` keeping the birth out of `watched_births` (`recovery.rs:543`), not a missing ladder. R9 wakes the existing one.
- *Does `Running` after `OsStopped` mean `node.start`?* No: `build.md:363` and the receipts decide. R11.

The D3 refusal text for node-restart.md (R12) is a page line and reaches Luke as exact old and new text at e3's landing, as every page line does.

## § Seed spec

> RDM has no theme YAML and no feature gate. The seed is a cell in a crate's test binary, run through `cells.sh` from a lane, and landed through `rebase-gate-land.sh`. The two-surface rule below is this PRD's surfaces rule.

### Suite assertion

```rust
// planned:crates/rdm-admin/tests/provider_state.rs — one behavior, two providers, identical assertion bodies
// CONTRACT (deployment-provider.md §2.5): a runtime the operating system holds suspended is OsStopped
// for a pid and for a container alike; resumed, it is Running; neither answer is exit proof.

mod process {
    #[tokio::test]
    async fn a_suspended_runtime_reads_os_stopped_and_running_again_when_continued() { /* spawn a child; SIGSTOP; inspect == OsStopped; SIGCONT; inspect == Running */ }
}
mod container {
    #[tokio::test]
    async fn a_suspended_runtime_reads_os_stopped_and_running_again_when_continued() { /* docker pause; inspect == OsStopped; docker unpause; inspect == Running; named skip without docker unless RDM_REQUIRE_CONTAINER=1 */ }
}
```

### What a passing seed proves

A fabric's authority can tell a suspended node from a hung one and from a gone one by asking the provider, with one vocabulary for processes and containers. That is the whole of what the person running the fabric needs from this initiative; everything else is the mesh acting on the answer correctly.

## § Surfaces — ONE capability, IDENTICAL on every surface

RDM's two surfaces for this capability are the two providers. One `DeploymentStatus` with one meaning per state is the shared core; the process provider and the container provider are thin readers over it (`/proc/<pid>/task/*/stat` and `docker inspect`). Every cell in this PRD that proves a state exists in `mod process` and `mod container` with identical assertion bodies; the only difference is how the suspension is produced (SIGSTOP / `docker pause`). The container module carries the named skip on a host without a daemon, failed by `RDM_REQUIRE_CONTAINER=1` in CI.

| operation (once) | process reader | container reader | core | caller |
|---|---|---|---|---|
| `inspect(handle)` | `process.rs:443` + the thread-group read (e1) | `container.rs:614` + `paused` (e1) | `DeploymentStatus` four states | `node.runtime.inspect` (e2) |
| `terminate(handle, mode)` | `process.rs:373` | `container.rs:584` | `TerminationMode` | `TerminateRuntime` step (R-FM1, landed) |
| `find(spec)` | `process.rs:424` | `container.rs:439` | `Found` | `DeployRuntime` (landed) |
| `adopt(fact)` | `provider.rs:243` | `provider.rs:243` | `AdoptRefusal` | a successor's management cut (landed) |

## § Restart-recovery canary

**(a) Persistence is in scope.** What crosses a process boundary: the launch record `<data_dir>/deployment.json` (`process.rs:21`), the exit record `<data_dir>/exit.json` (`rdm-core/src/runtime.rs:427`), the published RuntimeFact in membership, and the status-row facts `os-stopped`, `held-running`, `data-dir-lost`, `data-dir-unreadable` in the fabric's shared store root.

```rust
// planned:crates/rdm-admin/tests/provider_state.rs
// CONTRACT (deployment-provider.md §2, §2.5): a node-admin restarted at its own data dir re-adopts every
// runtime from the published RuntimeFacts and answers the same inspect for each, through node.get.
#[tokio::test]
async fn provider_state_survives_node_admin_restart_at_same_data_dir() {
    // a mesh with two Ready node-admins, one suspended member (os-stopped on node.get) and one running member;
    // node.primary.update moves the seat to the second admin (confirmed: node.primary.updated), then the
    // incumbent stops; the successor adopts both members from their RuntimeFacts and its first tick
    // re-inspects both (via-provider-inspection: os-stopped, running) and revalidates the os-stopped row;
    // the seat then returns to the first admin by another node.primary.update and it revalidates again.
    // A second cell relaunches a node-admin process at its data dir through the recovery identity contract
    // (a new node.id at the same path.name; never same-process node.restart) and asserts the same reads.
}
```

The second boundary in RDM is the member's own restart: `node_lifecycle__node_restart` (landed) already proves a node restarts as the same birth; this PRD adds nothing to it.

## § OTLP span-assertion bar

Every cell captures spans (`RDM_EVIDENCE_DIR`, `EvidenceCapture::start`) and prints its Jaeger link `http://192.168.68.99:16687/trace/<trace_id>`; the trace is the deliverable of each landing and is cited in the epic's PRD row.

| span name | emitted from | required attributes | asserted by |
|---|---|---|---|
| `rdm.node_admin.node.resolve.via-provider-inspection` | the caller of node.runtime.inspect | node, node_id, incarnation_id, inspector, handle, `verdict` ∈ {`running`, `os-stopped`, `exited`, `unavailable`}, exit_code, exit_proof, reason | every e2 and e3 cell |
| `rdm.node_admin.node.update.via-os-stopped` (new) | the mesh-primary writing or clearing the fact | node, node_id, incarnation_id, `since`, `outcome` (`recorded` / `cleared` / `revalidated`) | e2 cells |
| `rdm.node_admin.node.update.via-pending-reconnect` (new) | the executor on a reachability-related NotSent | node, node_id, incarnation_id, `reason` ∈ {`connect-refused`, `connect-timed-out`, `endpoint-unresolved`}, the command | e2 cells (and its absence on a local NotSent: control span the call's own `NotSent` end) |
| `rdm.node_admin.node.update.via-held-running` with `outcome = cancelled-os-stopped` (new outcome) | the recovery tick on `Running → OsStopped` | node, node_id, incarnation_id, the voided deadline | e2 hold-cancel cells |
| `rdm.node_admin.deployment.update.via-step` | the pipeline | step, outcome | e3 cells (`StopNode` run again; `StartNode` only after a stop receipt) |
| `rdm.node_admin.node.reject.via-not-live` | the fabric-primary's door | route, node, status, reason | e3 cells (fresh restart of a stopped node refused; resumption not refused) |

Absence assertions name their control span: an `os-stopped` trace contains `via-provider-inspection` and no `via-step` with `step = TerminateRuntime` and no `held-running`.

## § Per-sprint seed-data obligation

NO NEW ENTITY: `OsStopped` is a new value of an existing enum and the birth is an existing entity; no seed. The storage schema is settled anyway (R20): `os-stopped` is a new typed keyed row kind beside `held-running` (`NodeOsStoppedRow`, key the exact birth, format `node-os-stopped/1`, mesh-primary writer, removed on `Running` or `Exited`, revalidated by a successor's own inspection), so e2's landing carries the durable-format marker. Separately, and still true: no new gossip frame and no new `MemberStatus`.

## § Test tier coverage

- [x] unit — `#[cfg(test)]` in `process.rs` (the state-letter parser; the thread-group rule) and `container.rs` (the state-string map).
- [x] functional — `crates/rdm-admin/tests/provider_state.rs` (planned), `offline.rs` tick cells, `control_plane.rs` door cells.
- [ ] load
- [ ] chaos — none added. The R-shape release jobs (`tools/mesh-audit/i143-acceptance-jobs.json` `rshape_jobs`) and C17 are the chaos suite; they pass on these primitives or the primitive is wrong.

## § Pre-implementation thoughts (2026-10-15)

- **Hard part:** R8, R9 and R17 touch the recovery tick, the one place silence becomes action; the ordering with the R-FM1 hold must stay exact (an `OsStopped` verdict cancels the hold and never counts toward its verdicts; the terminate step re-inspects at its instant).
- **What I'd bet goes wrong:** a `Running` arm somewhere treats `OsStopped` as `Running` through a `_ =>` and terminates a paused container after the hold; R5's "by name" rule and a grep for `_ =>` on `DeploymentStatus` matches are the guard.
- **Persistence path:** the facts live on the status row in the shared store root; the launch and exit records in the data dir; rebuilt on boot by the successor's adopt and first inspect (the canary).
- **Dependencies:** i143 e12.s89's D2 and D2b fix (R16) lands first or e2's C17 criterion cannot be met; the R-FM1 and R-FM2 landings (`549493fe`, s87) are in.
- **Architectural risks:** none to gossip (no new frame, no new `MemberStatus`); the page rewrite moves anchors other pages link (`#1-…`, `#24-…` are kept verbatim).
- **What I don't know:** whether `docker inspect` reports `paused` promptly enough for a one-tick bound on every daemon version the gate runs; the container cell measures it.

## Implementation plan

> RDM has no `mesh-plan.json`, themes or four seats. The equivalents: a lane per epic (`/home/admin/rdm-lanes/<name>`, rebased on `origin/main`, a new branch), `cells.sh <package> main -- <stem>::` for one stem, `rdm-gate-at.sh <lane> fast` for the suite, `rebase-gate-land.sh <lane> <branch>` to land. Shapes: every cell here runs on the process provider in-process; the container cells need a daemon and are skipped by name without one.

#### e1 — the provider reports four states, for a pid and a container alike

- **Goal → gap.** deployment-provider.md §2.5-§2.7. The tree has three states and reads no state letter.
- **Requirements, with coordinates.** R1 `provider.rs:298`; R2 `process.rs:127` and `:443`; R3 `process.rs:113`, `:133-136`; R4 `container.rs:621`; R5 the 42 `Running` arms and `container.rs:630`.
- **Spans.** None new in e1; the states surface through e2's spans. The unit cells assert the parser.
- **Testing.** unit: the state-letter parser over fixtures (`R`, `S`, `D`, `T`, `t`, `Z`, a mixed group, an unreadable task); functional: `planned:crates/rdm-admin/tests/provider_state.rs` `mod process` / `mod container`.
- **Acceptance cells.** `a_suspended_runtime_reads_os_stopped_and_running_again_when_continued` (both mods); `a_sleeping_task_reads_running_and_a_created_container_reads_running` (both mods; R19); `a_thread_group_with_one_running_task_reads_running_while_another_is_stopped` (process); `a_partial_or_unreadable_task_read_is_unknown_never_exit_proof` (process); `a_pid_gone_with_its_start_identity_matched_is_exited` (process); `a_stopped_runtime_that_then_exits_reads_exited_with_its_code` (both mods). RED first.
- **Sizing.** One seat; files `provider.rs`, `process.rs`, `container.rs`, the 42 match sites.
- **Rows absorbed.** proposal items 1-3; a1 point 4.
- **State.** LANDED `e113d664` (2026-10-15, gate 93 s, through the land script), BUILT on `i144-e1-os-stopped` (lane e1s8): RED `efae8481` (`provider_state::` 4/8: both suspended cells and both stopped-then-exits cells read `Running`; `deployment::` 45/46 at container.rs:715, `paused`), GREEN `9222e1fc` (`provider_state::` 8/8: process 5, container 3 under `RDM_CONTAINER_PROOF=1`; rdm-admin main 404/404; `deployment::`, `seat_watch::`, `seat_revision::` 62/62). The thread-group cell stops one task with `PTRACE_INTERRUPT` (a tracer's `t`). The unreadable and partial reads are unit cells over a fixture `/proc` (`process.rs` `a_partial_or_unreadable_task_read_is_unreadable_and_a_gone_or_recycled_pid_is_gone`): no unprivileged host makes a live task file unreadable. Measured on this host (Docker 29.6.2): a paused container's process reads `S` in `/proc` (the daemon's state is its only reader), and `docker kill --signal TERM` unpauses a paused container, so `signal_stop` of an OS-stopped runtime is refused by name in both providers. R5 audit: no catch-all arm on `DeploymentStatus` in rdm-admin, rdm-node or rdm-test src; `fabric_handover.rs`'s catch-all on `Finding` is named. The gate's container-proof job runs `provider_state::container::` beside `container_proof::`. No span in e1 (its states surface through e2's spans). The first land gate (tree `602e212b`, recorded red) failed `fabric_primary::a_silent_member_whose_runtime_still_runs_is_held_not_replaced` (:278, the fence said `held-os-stopped`): the cell, and the four R-FM1 cells in node_replace.rs (release-only), modeled a hung runtime with SIGSTOP, which the provider now reports as `OsStopped`. The test kit gained a real hang (`ExactRuntime::hang`: every task but the leader held by a tracer, so the OS shows the group running while the runtime does nothing, §2.5 "What stays inference"), and those five cells run on it with their assertions unchanged: green, node_replace R-FM1 traces `d27aeffb639e220214de2fa2106e2a2d` and `358958693e1aaef7bc16decbd6a33bf7`.

#### e2 — the mesh records `os-stopped`, wakes recovery on NotSent, re-inspects every tick

- **Goal → gap.** deployment-provider.md §2.5 table; node-runtime-inspect.md's `os-stopped` result; node-recovery.md step 3; node-tickle.md step 1. The tree has no fact, no wake, no re-inspect.
- **Requirements, with coordinates.** R6 the inspect caller and `node-runtime-inspect.md:7,27,31,38`; R7 the `held-running` write as the pattern (R-FM1 landing `549493fe`); R8 `offline.rs:268`; R9 `recovery.rs:543` and the executor's NotSent arm in `pipeline.rs` `stop_steps_as`; R10 `recovery.rs:29-33`.
- **Dependency edge.** R16 (i143 e12.s89 D2/D2b) lands first: the C17 criterion needs the birth not held `stopping`.
- **Spans.** `via-provider-inspection` with `verdict = os-stopped`; new `rdm.node_admin.node.update.via-os-stopped` (`recorded` / `cleared` / `revalidated`); new `rdm.node_admin.node.update.via-pending-reconnect` (`reason = command-not-sent`). Control span for absence: no `via-step` `TerminateRuntime`, no `held-running` in an os-stopped trace.
- **Testing.** functional cells in `provider_state.rs` over a mesh of the testkit; the C17 criterion from the release job.
- **Acceptance cells.** `an_unreachable_not_sent_command_marks_the_birth_pending_reconnect_and_the_tick_inspects_within_one_floor`; `a_local_not_sent_marks_nothing` and `a_pending_reconnect_mark_never_overwrites_an_admitted_stopping` (R9 negatives); `an_os_stopped_verdict_cancels_an_open_hold_and_no_terminate_fires_past_the_old_deadline` and `the_same_after_a_successor_takes_the_seat` (R17); `a_resumed_but_unreachable_runtime_opens_a_fresh_hold_and_the_terminate_step_reinspects_before_firing` (R17); `an_os_stopped_birth_is_recorded_on_node_get_and_opens_no_hold_and_no_terminate`; `an_os_stopped_birth_that_resumes_is_cleared_on_the_next_tick`; `an_os_stopped_birth_that_exits_gets_its_exit_proof_and_ordinary_recovery`; `an_unknown_inspection_records_uncertainty_and_infers_nothing`; `a_successor_mesh_primary_revalidates_a_held_os_stopped_fact_with_its_own_inspection`; `an_os_stopped_verdict_never_counts_toward_held_running`. RED first.
- **Page text.** node-runtime-inspect.md (result, sequence step, verdict), node-recovery.md step 3 and step 4 (the hold cancelled on `OsStopped`; the terminate re-inspects), node-tickle.md step 1: exact old and new in this row for Luke's yes before the landing. The `NodeOsStoppedRow` landing carries `durable format moved` (a new row kind with its own marker).
- **Sizing.** One seat; files `offline.rs`, `recovery.rs`, the inspect caller in `admin.rs`, `pipeline.rs` (the NotSent arm), the three pages.
- **State.** BUILT on `i144-e2-os-stopped-fact` (lane e4s8), NOT LANDED: it lands only with the page lines below, on Luke's yes. RED `91ea95d3` (live `provider_state::` 0/2 on main `e113d664`: no `os_stopped` on `node.get`; main's inspection names the OS-stopped runtime only `not-exited`, trace `3163ff1c5f94448c4e48f369c4b869b4`), GREEN `a61e0c53` (live `provider_state::an_os_stopped_member_is_recorded_on_node_get_and_cleared_when_it_runs_again` trace `df4eedd6e0fe8b8edbfee13204eb101c`, `::an_os_stopped_member_that_exits_gets_its_exit_proof_and_ordinary_recovery` trace `b89f870258ee2bf4813d8c966e498e76`; rdm-admin lib 429/429, main 404/404, rdm-core main 58/58). The landing carries `durable format moved` (the `node-os-stopped/1` row kind) and `wire format moved` (`NodeView.os_stopped` on the Status op's `Nodes` reply; golden extended by hand).
  - Coverage, by planned cell: `an_os_stopped_birth_is_recorded_on_node_get_and_opens_no_hold_and_no_terminate` and `an_os_stopped_birth_that_resumes_is_cleared_on_the_next_tick` are the live `an_os_stopped_member_is_recorded_on_node_get_and_cleared_when_it_runs_again`; `an_os_stopped_birth_that_exits_gets_its_exit_proof_and_ordinary_recovery` is live as named (member); `an_os_stopped_verdict_never_counts_toward_held_running` with the record is unit `an_os_stopped_birth_is_recorded_and_opens_no_hold_and_never_counts_toward_held_running`; `an_os_stopped_verdict_cancels_an_open_hold_and_no_terminate_fires_past_the_old_deadline` and `a_resumed_but_unreachable_runtime_opens_a_fresh_hold...` are unit `an_os_stopped_verdict_cancels_an_open_hold_and_running_again_clears_it_and_opens_a_fresh_one` plus pipeline `the_holds_terminate_reinspects_and_never_fires_on_an_os_stopped_or_unknown_runtime` (the hold's terminate re-inspects, spanned `caller=terminate`); `a_successor_mesh_primary_revalidates_a_held_os_stopped_fact_with_its_own_inspection` is unit as named (the live seat-handover form is e4's canary); `an_unknown_inspection_records_uncertainty_and_infers_nothing` is unit as named; R9's `a_local_not_sent_marks_nothing` is `node_commands::a_reachability_not_sent_is_named_and_a_local_not_sent_marks_nothing`, `a_pending_reconnect_mark_never_overwrites_an_admitted_stopping` is `offline.rs` as named, and the tick's handling of a mark is `admin.rs` `an_unreachable_mark_watches_the_member_at_once_and_ends_when_it_is_heard_after_it`.
  - R9 finding (why its live cell is C17's release run, not a fast-gate cell): every node's QUIC `max_idle_timeout` is the staleness floor (`crates/rdm-transport/src/endpoint.rs:16-22`), so an executor that holds a pooled connection to a suspended member keeps it until the floor; a stop-node on it is written and ends `Indeterminate` (`ReplyDeadline`), never `NotSent`, and the natural mark comes first (measured: the first live R9 attempt, a stop of a SIGSTOP'd member at a 15 s floor, ended `Indeterminate` at 15.6 s). A fresh dial (no pooled connection, as in C17 `3100a0ae262f62d82e02953ef3c98d36`: `reused=false`, `Connection("timed out")`) is the case R9 accelerates; C17's release run is its live proof.
  - Decision (lead): the seat watch, on an `OsStopped` holder, cancels the hold it opened (`clear_held_running`) and records no `os-stopped` row of the holder: R20's writer is the birth's own mesh-primary.
- **Page text for Luke's yes (exact old → new; none applied until the yes, then landed with e2's code).**
  1. `lifecycles/node-runtime-inspect.md:7-8`. Old: "It answers `running` or `exit-proven { code }`." New: "It answers `running`, `os-stopped` or `exit-proven { code }`."
  2. `node-runtime-inspect.md:27` (Result). Old: "| Result | `running`: the exact runtime is alive, however silent the node; nothing is recovered or deleted on it. `exit-proven { code }`: the provider's terminal status for that exact handle, recorded durably as the birth's exit-proof row |" New: "| Result | `running`: the exact runtime is alive, however silent the node; nothing is recovered or deleted on it. `os-stopped`: the operating system holds the exact runtime suspended; it is neither hung nor gone and proves no exit ([deployment provider §2.5](../deployment-provider.md#25-runtime-state-what-the-provider-reports)). `exit-proven { code }`: the provider's terminal status for that exact handle, recorded durably as the birth's exit-proof row |"
  3. `node-runtime-inspect.md:31` (Spans). Old: "`verdict` (`running` / `exited` / `unavailable`)". New: "`verdict` (`running` / `os-stopped` / `exited` / `unavailable`)".
  4. `node-runtime-inspect.md:38-43` (sequence). Old steps 4-5: "4. `exit-proven { code }`: the caller writes … 5. The caller continues per its page: …". New: insert before them "4. `os-stopped`: the caller records the birth's durable `os-stopped { since }` fact and nothing else: it opens no hold, terminates nothing and asks again on its next tick ([node recovery](node-recovery.md) step 3)." and renumber `exit-proven` to 5 and "The caller continues" to 6.
  5. `lifecycles/node-recovery.md:20` (Detection). Old: "reads the members its view marks `PendingReconnect` (unheard past the staleness floor)". New: "reads the members its view marks `PendingReconnect` (unheard past the staleness floor, or sent a command that never reached them for a reachability reason: [node.tickle](node-tickle.md) step 1)".
  6. `node-recovery.md:35-36` (step 1). Old: "Each node's pruner marks a member `PendingReconnect` once it is unheard past the staleness floor. Nothing is sent; the mark comes from the digests not arriving." New: "Each node's pruner marks a member `PendingReconnect` once it is unheard past the staleness floor. Nothing is sent; the mark comes from the digests not arriving. The mesh-primary also marks a member `PendingReconnect` at once when a command to it was never sent for a reachability reason (the connect refused or timed out, the endpoint unresolved): an observer mark, never over a lifecycle status it authored, which ends when the member is heard again (its next digest, or an answer to its tickle) or the view no longer holds that birth."
  7. `node-recovery.md:54-55` (step 3). Old: "`Unknown` or an unreadable runtime row proves nothing and opens no hold." New: "`Unknown` or an unreadable runtime row proves nothing and opens no hold. When the provider reports `OsStopped`, the mesh-primary records the durable status-row fact `os-stopped { since }` (surfaced on `node.get`, never a gossip `MemberStatus`), opens no hold and runs no `TerminateRuntime`; a hold already open on the birth is cancelled (its `held-running` row removed, its pending `TerminateRuntime` void). It inspects the exact runtime again on every tick: `Running` again clears the fact and the rule below applies, `Exited` is the proof, `Unknown` records the uncertainty."
  8. `node-recovery.md:65` (step 4). Old: "`TerminateRuntime { node_id, incarnation, handle }`: the provider's" New: "`TerminateRuntime { node_id, incarnation, handle }`, which inspects the exact runtime again and fires only on `Running` at that instant (`OsStopped` or `Unknown` there fails it by name and terminates nothing): the provider's"
  9. `lifecycles/node-tickle.md:36-37` (step 1). Old: "a reason to ask: silence past the staleness floor, a silent seat holder, an investigation's clock, a successor that has not answered." New: "a reason to ask: silence past the staleness floor, a command to the node that was never sent for a reachability reason (the connect refused or timed out, the endpoint unresolved), which marks it `PendingReconnect` at once, a silent seat holder, an investigation's clock, a successor that has not answered."


#### e3 — resumption from receipts, and the fresh-restart refusal of a stopped node

- **Goal → gap.** build.md §4.1 receipts (HOLDS) and node-restart.md's refusal line (BUILD). The tree refuses every non-live restart at one door, resumption included, under a name no page holds.
- **Requirements, with coordinates.** R11 `build.md:363`, `control_plane.rs:611-613`; R12 `control_plane.rs:618` and `BuildReject::NodeNotLive` (`control_plane.rs:922`).
- **Spans.** `rdm.node_admin.node.reject.via-not-live` (fresh restart of a stopped node: refused); its absence on a resubmission with a completed stop receipt (control: `via-step` `StartNode`); `via-step` `StopNode` run again when no stop receipt exists.
- **Acceptance cells.** `an_explicit_delete_of_an_os_stopped_birth_skips_the_stop_by_name_and_terminates_the_exact_runtime` (both mods; R18) and `recovery_never_deletes_or_terminates_an_os_stopped_birth` (R18 negative); `a_restart_whose_stop_was_never_admitted_resumes_stop_then_start_on_resubmission`; `a_restart_with_a_completed_stop_receipt_resumes_at_start`; `a_fresh_restart_of_a_stopped_node_is_refused_by_name_and_node_start_is_served`; `a_returning_runtime_never_reopens_a_terminally_failed_attempt_on_its_own`. RED first.
- **Page text.** node-restart.md: the refusal of a fresh `node.restart` against a stopped node, by name; node-delete.md: the OS-suspended target row (drafted with this PRD); exact old and new in this row for Luke's yes.
- **Sizing.** One seat; `control_plane.rs`, `pipeline.rs`, node-restart.md.

#### e4 — the canary and the primitives' proof of invariance

- **Goal → gap.** deployment-provider.md §2, §2.8, §5. HOLDS today; this epic proves it at the new vocabulary.
- **Requirements, with coordinates.** R13 `provider.rs:243-266`; R14 `process.rs:297`, `:424`, `container.rs:439`; R15 `process.rs:373-424`, `container.rs:630`.
- **Acceptance cells.** `provider_state_survives_a_confirmed_seat_handover_and_its_return` (the canary, written first: two Ready admins, `node.primary.update` before the incumbent stops, the successor adopts and inspects both members, revalidation when the seat returns) and `provider_state_survives_a_node_admin_relaunch_under_the_recovery_identity_contract` (a new node.id at the path.name, never same-process `node.restart`); `the_provider_sends_no_signal_but_the_stop_ladder_and_the_terminate` (a `kill(2)` / `docker` call audit over the provider module's spans and a strace-free assertion through the testkit's fault door: a suspended child is never continued by RDM).
- **Sizing.** One seat; `tests/provider_state.rs` only.

### Files likely to touch

`crates/rdm-admin/src/deployment/{provider,process,container,pipeline}.rs`, `crates/rdm-admin/src/{offline,control_plane}.rs`, `crates/rdm-admin/src/admin/recovery.rs`, the inspect caller in `crates/rdm-admin/src/admin.rs`, `planned:crates/rdm-admin/tests/provider_state.rs`, `docs/architecture/deployment-provider.md`, `docs/architecture/lifecycles/{node-runtime-inspect,node-recovery,node-tickle,node-restart}.md`, `docs/reference/rdm/*.md` (regenerated), `CLAUDE.md` env table (no new variable expected).

## § Per-feature documentation deliverable

RDM has no `docs/features/<slug>/` tree. The three documents map to: **overview** = `docs/architecture/deployment-provider.md` (this PRD's rewrite); **how-to** = `lifecycles/node-runtime-inspect.md` (the one primitive a caller uses, with its answers); **runbook** = a new `docs/eng/provider-runbook.md` (`planned:docs/eng/provider-runbook.md`): what `os-stopped`, `held-running`, `data-dir-lost` and `data-dir-unreadable` look like on `node.get` and in Jaeger, what each means, and what a person does about each (continue the runtime, repair the disk, delete the node), with the exact span names.

## Verification

- [ ] Seed assertion (§ Seed spec) green in both mods at gate cadence; traces cited in e1's row.
- [ ] Every e1-e4 acceptance cell green; each row names its RED sha, GREEN sha, gate time and Jaeger trace.
- [ ] Restart-recovery canaries green (the handover canary and the relaunch canary); each trace shows the successor's first `via-provider-inspection` per member and the revalidated `os-stopped` row.
- [ ] `git grep -n 'DeploymentStatus::' -- crates | grep -v OsStopped` reviewed: no `_ =>` arm folds `OsStopped` into another state (the e1 row lists each match site).
- [ ] The R-shape release jobs (`rshape_jobs`) green in one release run after e2 and i143 e12.s89's D2 fix; C17's trace shows `verdict = os-stopped` within one tick of the NotSent and no `TerminateRuntime`.
- [ ] No new gossip frame, no new `MemberStatus`: `mesh-audit` catalog unchanged (the landing says so in its body).
- [ ] Page text landed with code for node-runtime-inspect.md, node-recovery.md, node-tickle.md, node-restart.md, each with Luke's yes on exact text recorded in the epic's row; deployment-provider.md landed on Luke's yes to this PRD's rewrite.
- [ ] `docs/reference/rdm/{spans,config,frames}.md` regenerated; `provider-runbook.md` landed.
- [ ] Every landing through `rebase-gate-land.sh`; no red re-rolled; the timings ledger carries each row.

## § Dispatch ordering

| wave | epic | files | why this wave |
|---|---|---|---|
| 0 | i143 e12.s89 D2/D2b | `pipeline.rs:2249`, `runner.rs:148-151` | the dependency edge into e2's C17 criterion; a gossip change with its own flame; lands first |
| 1 | e1 | `provider.rs`, `process.rs`, `container.rs`, the 42 match sites | no dependency; disjoint from wave 0's two files except `pipeline.rs` match arms (sequenced after wave 0) |
| 1 | e4 canary (RED) | `tests/provider_state.rs` | written first, red until e1 and e2 |
| 2 | e2 | `offline.rs`, `recovery.rs`, `admin.rs` inspect caller, `pipeline.rs` NotSent arm, three pages | consumes e1's enum; after wave 0 |
| 3 | e3 | `control_plane.rs`, `pipeline.rs`, node-restart.md | consumes e2's resumption path; its page line is a Luke yes |
| 3 | e4 green | `tests/provider_state.rs` | after e2 |

Critical path: wave 0 → e1 → e2 → e3. e1 and the red canary run concurrently with wave 0 in separate lanes.

## § Retro (YYYY-MM-DD, on ship)

- **What surprised me:**
- **What I predicted correctly:**
- **What the seed missed:**
- **What I'd do differently:**
- **Shipped SHAs:**

## Cross-references

- `docs/plans/proposal/update-provider-abstraction-to-actually-work.md` (the proposal; items 1-4, 6, Q1 approved; 5, 7, D1 not)
- `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-FM1, R-FM2 (landed `549493fe`, s87)
- `docs/plans/i143-prd.md` e12.s86, e12.s87 (landed), e12.s89 (the D2/D2b rows and C17), e12.s90 (superseded by this PRD's e1-e2 once approved)
- `docs/architecture/deployment-provider.md`, `lifecycles/node-runtime-inspect.md`, `lifecycles/node-recovery.md`, `lifecycles/node-tickle.md`, `lifecycles/node-restart.md`, `build.md` §4.1
