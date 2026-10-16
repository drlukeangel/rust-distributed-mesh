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

1. When a node's runtime is suspended by the operating system, the mesh-primary knows it within one recovery tick of the first unanswered command, records it, and neither holds nor terminates nor replaces the node for that reason.
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
| R9 | a `NotSent` command marks the birth `PendingReconnect` at once | BUILD | `recovery.rs:543` `watched_births` watches `PendingReconnect` and `Dead` only; nothing marks on NotSent | e2 |
| R10 | a new authority revalidates a stored `os-stopped` fact | BUILD | `recovery.rs:29-33` drops a gone birth's bookkeeping; no revalidation of a held fact | e2 |
| R11 | the pending receipted step resumes; never `node.start` on `Running` alone | HOLDS | `build.md:363` (a live run is reattached, never re-claimed; receipts decide); `control_plane.rs:611-613` serves a start only for a stopped node | e3 (cells) |
| R12 | a new `node.restart` against a stopped node is refused by name; a resubmission with a completed stop receipt continues at start | BUILD | `control_plane.rs:618` refuses every non-live restart, resumption included; the refusal name `node-not-live` is in no page | e3 |
| R13 | adoption from a RuntimeFact alone, foreign domain refused by name | HOLDS | `provider.rs:243-266` `adopt`; `AdoptRefusal::ForeignControlDomain` | e4 (canary) |
| R14 | the launch record is durable before the executable runs; `find` tells running, exited and never-launched apart | HOLDS | `process.rs:297` (`deployment.json`), `process.rs:424` `find`; `container.rs:439` `find_by_name`; cells `process.rs:566-658`, `tests/container_proof.rs:104-146` | e4 |
| R15 | the stop ladder and the terminate are the only signals | HOLDS | `process.rs:373-424`, `container.rs:630`; no other `kill(2)` or `docker` signal in `crates/rdm-admin/src/deployment` | e4 (cell) |
| R16 | D2 / D2b: `stopping` on admission, the `NodeLeaving` sender deleted | dependency | i143 `e12.s89` rows (`pipeline.rs:2249`, `runner.rs:148-151`) | edge into e2, e3 |

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
    // a mesh with one suspended member (os-stopped on node.get) and one running member;
    // stop the mesh-primary admin; start it again at the same data dir;
    // node.get for both members reads the same facts; the suspended one is still os-stopped and
    // the first tick after the restart re-inspected it (via-provider-inspection, verdict os-stopped).
}
```

The second boundary in RDM is the member's own restart: `node_lifecycle__node_restart` (landed) already proves a node restarts as the same birth; this PRD adds nothing to it.

## § OTLP span-assertion bar

Every cell captures spans (`RDM_EVIDENCE_DIR`, `EvidenceCapture::start`) and prints its Jaeger link `http://192.168.68.99:16687/trace/<trace_id>`; the trace is the deliverable of each landing and is cited in the epic's PRD row.

| span name | emitted from | required attributes | asserted by |
|---|---|---|---|
| `rdm.node_admin.node.resolve.via-provider-inspection` | the caller of node.runtime.inspect | node, node_id, incarnation_id, inspector, handle, `verdict` ∈ {`running`, `os-stopped`, `exited`, `unavailable`}, exit_code, exit_proof, reason | every e2 and e3 cell |
| `rdm.node_admin.node.update.via-os-stopped` (new) | the mesh-primary writing or clearing the fact | node, node_id, incarnation_id, `since`, `outcome` (`recorded` / `cleared` / `revalidated`) | e2 cells |
| `rdm.node_admin.node.update.via-pending-reconnect` (new) | the executor on NotSent | node, node_id, incarnation_id, `reason` = `command-not-sent`, the command | e2 cells |
| `rdm.node_admin.deployment.update.via-step` | the pipeline | step, outcome | e3 cells (`StopNode` run again; `StartNode` only after a stop receipt) |
| `rdm.node_admin.node.reject.via-not-live` | the fabric-primary's door | route, node, status, reason | e3 cells (fresh restart of a stopped node refused; resumption not refused) |

Absence assertions name their control span: an `os-stopped` trace contains `via-provider-inspection` and no `via-step` with `step = TerminateRuntime` and no `held-running`.

## § Per-sprint seed-data obligation

NO NEW ENTITY. `OsStopped` is a new value of an existing enum and `os-stopped` a new status-row fact of an existing birth row; no new record kind, no seed.

## § Test tier coverage

- [x] unit — `#[cfg(test)]` in `process.rs` (the state-letter parser; the thread-group rule) and `container.rs` (the state-string map).
- [x] functional — `crates/rdm-admin/tests/provider_state.rs` (planned), `offline.rs` tick cells, `control_plane.rs` door cells.
- [ ] load
- [ ] chaos — none added. The R-shape release jobs (`tools/mesh-audit/i143-acceptance-jobs.json` `rshape_jobs`) and C17 are the chaos suite; they pass on these primitives or the primitive is wrong.

## § Pre-implementation thoughts (2026-10-15)

- **Hard part:** R8 and R9 touch the recovery tick, which is the one place silence becomes action; the ordering with the R-FM1 hold must stay exact (an `OsStopped` verdict must not count toward `held-running`'s verdicts).
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
- **Acceptance cells.** `a_suspended_runtime_reads_os_stopped_and_running_again_when_continued` (both mods); `a_thread_group_with_one_running_task_reads_running_while_another_is_stopped` (process); `a_partial_or_unreadable_task_read_is_unknown_never_exit_proof` (process); `a_pid_gone_with_its_start_identity_matched_is_exited` (process); `a_stopped_runtime_that_then_exits_reads_exited_with_its_code` (both mods). RED first.
- **Sizing.** One seat; files `provider.rs`, `process.rs`, `container.rs`, the 42 match sites.
- **Rows absorbed.** proposal items 1-3; a1 point 4.

#### e2 — the mesh records `os-stopped`, wakes recovery on NotSent, re-inspects every tick

- **Goal → gap.** deployment-provider.md §2.5 table; node-runtime-inspect.md's `os-stopped` result; node-recovery.md step 3; node-tickle.md step 1. The tree has no fact, no wake, no re-inspect.
- **Requirements, with coordinates.** R6 the inspect caller and `node-runtime-inspect.md:7,27,31,38`; R7 the `held-running` write as the pattern (R-FM1 landing `549493fe`); R8 `offline.rs:268`; R9 `recovery.rs:543` and the executor's NotSent arm in `pipeline.rs` `stop_steps_as`; R10 `recovery.rs:29-33`.
- **Dependency edge.** R16 (i143 e12.s89 D2/D2b) lands first: the C17 criterion needs the birth not held `stopping`.
- **Spans.** `via-provider-inspection` with `verdict = os-stopped`; new `rdm.node_admin.node.update.via-os-stopped` (`recorded` / `cleared` / `revalidated`); new `rdm.node_admin.node.update.via-pending-reconnect` (`reason = command-not-sent`). Control span for absence: no `via-step` `TerminateRuntime`, no `held-running` in an os-stopped trace.
- **Testing.** functional cells in `provider_state.rs` over a mesh of the testkit; the C17 criterion from the release job.
- **Acceptance cells.** `a_not_sent_command_marks_the_birth_pending_reconnect_and_the_tick_inspects_within_one_floor`; `an_os_stopped_birth_is_recorded_on_node_get_and_opens_no_hold_and_no_terminate`; `an_os_stopped_birth_that_resumes_is_cleared_on_the_next_tick`; `an_os_stopped_birth_that_exits_gets_its_exit_proof_and_ordinary_recovery`; `an_unknown_inspection_records_uncertainty_and_infers_nothing`; `a_successor_mesh_primary_revalidates_a_held_os_stopped_fact_with_its_own_inspection`; `an_os_stopped_verdict_never_counts_toward_held_running`. RED first.
- **Page text.** node-runtime-inspect.md (result, sequence step, verdict), node-recovery.md step 3, node-tickle.md step 1: exact old and new in this row for Luke's yes before the landing.
- **Sizing.** One seat; files `offline.rs`, `recovery.rs`, the inspect caller in `admin.rs`, `pipeline.rs` (the NotSent arm), the three pages.

#### e3 — resumption from receipts, and the fresh-restart refusal of a stopped node

- **Goal → gap.** build.md §4.1 receipts (HOLDS) and node-restart.md's refusal line (BUILD). The tree refuses every non-live restart at one door, resumption included, under a name no page holds.
- **Requirements, with coordinates.** R11 `build.md:363`, `control_plane.rs:611-613`; R12 `control_plane.rs:618` and `BuildReject::NodeNotLive` (`control_plane.rs:922`).
- **Spans.** `rdm.node_admin.node.reject.via-not-live` (fresh restart of a stopped node: refused); its absence on a resubmission with a completed stop receipt (control: `via-step` `StartNode`); `via-step` `StopNode` run again when no stop receipt exists.
- **Acceptance cells.** `a_restart_whose_stop_was_never_admitted_resumes_stop_then_start_on_resubmission`; `a_restart_with_a_completed_stop_receipt_resumes_at_start`; `a_fresh_restart_of_a_stopped_node_is_refused_by_name_and_node_start_is_served`; `a_returning_runtime_never_reopens_a_terminally_failed_attempt_on_its_own`. RED first.
- **Page text.** node-restart.md: the refusal of a fresh `node.restart` against a stopped node, by name; exact old and new in this row for Luke's yes.
- **Sizing.** One seat; `control_plane.rs`, `pipeline.rs`, node-restart.md.

#### e4 — the canary and the primitives' proof of invariance

- **Goal → gap.** deployment-provider.md §2, §2.8, §5. HOLDS today; this epic proves it at the new vocabulary.
- **Requirements, with coordinates.** R13 `provider.rs:243-266`; R14 `process.rs:297`, `:424`, `container.rs:439`; R15 `process.rs:373-424`, `container.rs:630`.
- **Acceptance cells.** `provider_state_survives_node_admin_restart_at_same_data_dir` (the canary, written first); `the_provider_sends_no_signal_but_the_stop_ladder_and_the_terminate` (a `kill(2)` / `docker` call audit over the provider module's spans and a strace-free assertion through the testkit's fault door: a suspended child is never continued by RDM).
- **Sizing.** One seat; `tests/provider_state.rs` only.

### Files likely to touch

`crates/rdm-admin/src/deployment/{provider,process,container,pipeline}.rs`, `crates/rdm-admin/src/{offline,control_plane}.rs`, `crates/rdm-admin/src/admin/recovery.rs`, the inspect caller in `crates/rdm-admin/src/admin.rs`, `planned:crates/rdm-admin/tests/provider_state.rs`, `docs/architecture/deployment-provider.md`, `docs/architecture/lifecycles/{node-runtime-inspect,node-recovery,node-tickle,node-restart}.md`, `docs/reference/rdm/*.md` (regenerated), `CLAUDE.md` env table (no new variable expected).

## § Per-feature documentation deliverable

RDM has no `docs/features/<slug>/` tree. The three documents map to: **overview** = `docs/architecture/deployment-provider.md` (this PRD's rewrite); **how-to** = `lifecycles/node-runtime-inspect.md` (the one primitive a caller uses, with its answers); **runbook** = a new `docs/eng/provider-runbook.md` (`planned:docs/eng/provider-runbook.md`): what `os-stopped`, `held-running`, `data-dir-lost` and `data-dir-unreadable` look like on `node.get` and in Jaeger, what each means, and what a person does about each (continue the runtime, repair the disk, delete the node), with the exact span names.

## Verification

- [ ] Seed assertion (§ Seed spec) green in both mods at gate cadence; traces cited in e1's row.
- [ ] Every e1-e4 acceptance cell green; each row names its RED sha, GREEN sha, gate time and Jaeger trace.
- [ ] Restart-recovery canary green; its trace shows the first post-restart `via-provider-inspection` per member.
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
