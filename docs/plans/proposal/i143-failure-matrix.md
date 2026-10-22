# Proposal: the i143 failure matrix — every basic failure case has an owner, a doc and a test

Story i143.e11.s7. Once approved, this matrix lands as `docs/architecture/lifecycles/failure-matrix.md`.

## How to read it
- **Owner**: the tier that detects the failure → the tier that acts, with the RDM code. Every RDM path is read on RDM `origin/main` **63856583** and is relative to `rust-distributed-mesh/crates/`.
- **Doc**: the governing section, as a path under `docs/architecture/` or `docs/plans/rulings/`, with lines.
- **Test**: the cell that proves the row, with its line on 63856583. Flames are trace ids in ClickHouse `otel.otel_traces`; open one at `http://192.168.68.99:16686/trace/<id>`.
  - Spans carry no test attribute. So a flame here means: the latest span of that name and those fields on a live mesh. It is not proof that the named cell produced it. Where a cell exists but no span was found: "no flame yet".
- **Status**:
  - covered: owner, doc and test all exist on main;
  - gap: something is missing, and the row says what and where it goes;
  - needs ruling: the owner itself is undecided (packet below).

## The matrix

| # | Case | Owner (detects → acts) | Doc | Test · flame | Status |
|---|---|---|---|---|---|
| 1 | Member killed outside any workflow | Each node's pruner marks it PendingReconnect → the mesh-primary's tick: tickle, TRUE OFFLINE, provider proof, recovery Build (`rafka-node-admin-core/src/admin.rs:4638-4830`; `recovery_due` :1111; `inspect` :961-978; `submit` :992-1055) → the fabric-primary admits it and dispatches it back (R-ON8) → `recover-node` pipeline (`deployment/pipeline.rs:2135-2160`) | `lifecycles/node-recovery.md:28-70` | `rafka-test-scenario/tests/node_lifecycle__node_replace.rs:252` `any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir`; `rafka-node-admin-core/tests/drift_convergence.rs:454` · `via-node-recovery disposition=created outcome="complete: attempt 4"` in 39680bfe9ff742b0c42131eac4972b34 | covered (landed in land3, 6a4710b8; the story's issue is still open) |
| 2 | Member killed during a workflow (`hydrate_before_ready` included) | The workflow's Build ends `Failed { step, reason }` → the mesh-primary's next tick finds the member still proven exited and submits the recovery (`admin.rs:1205-1216` `settle`; tick :4809-4826) | `lifecycles/node-recovery.md:116-117` | `node_lifecycle__node_replace.rs:681` `an_exit_during_a_workflow_fails_that_workflow_and_the_next_tick_recovers_the_member` · no flame yet | covered |
| 3a | Mesh-primary killed; another admin of its mesh lives | The election seats the surviving admin (`election.rs:15-21`) → as the new mesh-primary it runs node recovery for the lost admin; the fabric-primary's drift skips it (`admin.rs:1408-1413`) | `lifecycles/mesh-recovery.md:8-9`; `fabric-node-lifecycle-elections.md:116-120` | `rafka-node-admin-core/tests/drift_convergence.rs:501` `a_lost_mesh_primary_admin_is_recovered_by_the_admin_its_mesh_seats_and_only_it`; `node_lifecycle__node_replace.rs:753` · `rdm.mesh.seat.update.via-announce seat=mesh-primary` in 9327d99697c385f861553966a4dac2dc | covered |
| 3b | Every node-admin of a mesh lost | The fabric-primary's peer-mesh investigation (`investigate.rs:49` rungs 10/15/20/30; verdict :418-427) → mesh recovery on the same Build and MeshId (`admin.rs:1396-1402`) → the recovering admin's entry sweep (`reenter.rs:11-14`, :193, :266) → unreached members go to node recovery | `lifecycles/mesh-investigation.md:15-39`; `lifecycles/mesh-recovery.md:13-38` | `rafka-test-scenario/tests/mesh_lifecycle__mesh_recover.rs:59` `a_lost_mesh_recovers_as_itself_under_the_same_build`; `i143_acceptance_2803.rs:66` · `rdm.node_admin.mesh.create.via-rebirth fabric_status=degraded` in de1ef68d5b10f40363ce77086b68ba1b | covered |
| 4a | Fabric-primary killed while idle; its mesh keeps an admin | Exact-birth proof moves the seat to the lowest Ready admin of the same mesh (`election.rs:15-21`, `holding` :108) → that admin, now mesh-primary, recovers the lost one as a member | `fabric-node-lifecycle-elections.md:111-124`, :155-160 | `mesh_elections__sticky_seat.rs:89` `a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh`; `drift_convergence.rs:527` · `rdm.mesh.seat.update.via-announce seat=fabric-primary` in 438fa7c0fcb172f7db88e5236b4cc308 | covered |
| 4b | Fabric-primary killed mid-Build (after the claim, before the dispatch) | The successor fabric-primary resumes from the Build projection and calls the executor the claim names (`build_drive.rs:586` dispatch) | `node-rpc-envelope.md:1055-1063` | `fabric_build__build_op.rs:350` `a_successor_fabric_primary_starts_the_attempt_its_predecessor_claimed_and_no_step_runs_twice` · no flame yet | covered |
| 4c | Fabric-primary's whole mesh lost | The seat leaves the mesh only when every admin birth of it is proven gone; then the lowest NodeId among the remaining mesh-primaries takes it (`election.rs:176-189`) | `fabric-node-lifecycle-elections.md:116-122` | `mesh_elections__fabric_primary.rs:247` `three_mesh_primaries_keep_the_day0_holder_and_elect_the_lowest_remaining_when_its_mesh_is_lost`; `:57` · no flame yet | covered |
| 5 | Seat holder drained, stopped, restarted or replaced by a Build | The executor N sends TakeSeats to the holder P → P commits one seat row (fence and commit) (`fabric_handover.rs:32-38`, :287) → the old driver's stream ends with the typed new-seat frame. A Build that would end the fabric-primary's last admin is refused by name (`build.rs:410` `BuildReject::LastAdmin`; `http.rs:508` 422 `last-admin`) | `fabric-node-lifecycle-elections.md:141-147`; `node-rpc-envelope.md:1237`; R-EL1a `plans/rulings/i143-rulings-2026-10-11-dev.md:71-121` | `rafka-node-admin-core/tests/fabric_handover.rs:746`, :848; `fabric_build__build_op.rs:429`; `mesh_elections__sticky_seat.rs:253` · `rdm.node_admin.build.reject.via-last-admin` in e3deffc1d2704f724d75fa65f55b124d | covered on main (land4, 6856b026, 8a04fe5f); the story's issue stays open for its fabric-shutdown election delta |
| 6 | Member hung but alive: tickle fails, provider says running | The mesh-primary: TRUE OFFLINE, writes Dead (`admin.rs:4790-4797`), inspects (`NotExited`, :957, :931), names the verdict once (:980-990), and does nothing else. Nothing is recovered and no one is told | `lifecycles/node-recovery.md:49-52` (no step after the verdict) | `node_lifecycle__node_replace.rs:549` `a_hung_member_whose_runtime_still_runs_is_never_recovered`; `mesh_elections__fabric_primary.rs:178` · `via-provider-inspection verdict=not-exited reason="its runtime is not proven exited: Running"` in 0550b35186f1563edea62bbbb4d909b3 | **needs ruling: R-FM1** |
| 6b | Seat holder hung but alive (mesh-primary or fabric-primary) | Seat watch: a running runtime ends the look and the seat stays (`seat_watch.rs:14-17`, :265). A hung mesh-primary keeps its seat, so its own members have no tick that recovers them | `fabric-node-lifecycle-elections.md:155-157`; R-SW1 `i143-rulings-2026-10-11-dev.md:213-240` | none for a hung holder (`sticky_seat.rs:192` is a cut-off holder, not a hung one) · no flame yet | **needs ruling: R-FM1** |
| 7a | One node cut off from its own mesh (process runs) | The same path as row 6, up to the not-exited verdict. On heal its digests return (`offline.rs:128` `via-offline-returned`) and the same birth serves | `lifecycles/node-recovery.md:49-52`; `gossip.md:825-833` | `rshape_burn_in.rs:5053` `mock_control_loss_recovers_without_status_heartbeat`; `i143_acceptance_2784.rs:195` (containers, release only) · `rdm.node_admin.node.update.via-offline-returned` in a0a6131bbaba2acfb2d8d81810186150 | covered |
| 7b | Peer mesh unheard (cut between meshes) | The fabric-primary's investigation holds the mesh (`investigate.rs:418-427`). R-D1: nothing of an unheard mesh is restarted, replaced or deleted (`admin.rs:1396-1402`) | `lifecycles/mesh-investigation.md:15-35`; `gossip.md:500-503` | `rshape_burn_in.rs:4845` `mock_partition_heal_recovers_coverage_and_routes`; `:5863` `mock_cut_kill_of_a_near_node_is_repaired_by_the_near_side_and_held_by_the_far_side`; `rafka-node-admin-core/tests/control_routes.rs:517` (REST replace refused by name) · `rdm.node_admin.build.reject.via-unheard-mesh` in b1d221d08fd688924b28e7099a4b0f31 | covered; see finding F3 |
| 7c | Cut-off fabric-primary view | A cut-off view claims and dispatches nothing; the drive pauses and resumes (`build_drive.rs:512` `via-cut-off`); the seat stays (`seat_watch.rs:14-17`) | `node-rpc-envelope.md:1073`; `fabric-primary.md:207` | `rafka-node-admin-core/tests/build_drive.rs:232`; `mesh_elections__sticky_seat.rs:192` · no flame yet (no `via-cut-off` span in 5 days) | covered |
| 8 | Data dir or disk lost | Recovery loads the key from the inherited dir and refuses by name when it is gone (`deployment/pipeline.rs:1063-1080`); the Build ends Failed and the next tick submits again (`admin.rs:1205-1216`) | `lifecycles/node-recovery.md:23`, :64-66 (no case for a lost dir) | `rafka-node-rpc-testkit/tests/recover_pipeline.rs:246` `a_recovery_whose_inherited_data_dir_has_no_endpoint_key_fails_by_name_and_mints_none` · `via-node-recovery outcome="failed: attempt 3 … the inherited data dir … does not exist"` in 3b983b72926d8d5e3629d7e3d1f264cf | **needs ruling: R-FM2** |
| 9 | Whole-fabric cold restart (every runtime lost) | A person starts one admin with both primary flags on the fabric-primary's data dir (`admin.rs:63-65`, :155) → durable map, own mesh first (`reenter.rs:1-14`) → adopts its own clock when no node holds rafka-time → restores the other meshes under the same Build | `node-admin-lifecycle.md:118-136`; `lifecycles/fabric-recovery.md:5-19` | `i143_acceptance_2803.rs:264` `fabric_recovery_restarts_one_admin_and_restores_every_missing_admin`; `:403` `fabric_recovery_with_no_running_node_adopts_the_holders_own_clock` · no flame yet | covered (needs the fabric-primary's data dir: see R-FM2) |
| 10 | Member killed and the mesh-primary changes mid-ladder | Every admin keeps `first_seen`; the new mesh-primary continues the window (`offline.rs:5-7`; tick :4666-4680) | `lifecycles/node-recovery.md:33-37` | `node_lifecycle__node_replace.rs:753` `a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery` · no flame yet | covered |
| 11 | Exit with the transport-stopped code (4) | Recovered like any exit, as a new node.id, never a restart (`rafka-mesh-entity/src/runtime.rs:396-399`; tick as row 1) | `lifecycles/node-recovery.md:26` | `drift_convergence.rs:613` `a_transport_stopped_exit_is_recovered_as_a_new_node_never_restarted` · no flame yet | covered |
| 12 | Executor lost mid-Build | The inner call ends Indeterminate; the next attempt is claimed only after departure proof (`build_drive.rs:640-652`, `via-departure-proven` :649); without proof the attempt is kept and the drive stalls by name (`build_drive.rs:519-523` `via-stalled`). A birth a lost executor launched that never joined is stopped and decided afresh (`deployment/pipeline.rs:1266-1273` `via-lost-executor`) | `node-rpc-envelope.md:1062-1063` | `fabric_build__build_op.rs:309`; `rafka-node-admin-core/tests/build_drive.rs:249`; `build_takeover.rs:191` · `rdm.node_admin.build.update.via-departure-proven` in c4c5d79a755d08778e7a2681f850261a | covered |
| 13 | Caller's stream cut | The Build keeps running; a resubmit by build_id reattaches, or answers AlreadyApplied after the end (`build_op.rs:199-216` `resubmit`) | `node-rpc-envelope.md:1050-1060`; `lifecycles/node-recovery.md:108-115` | `fabric_build__build_op.rs:212` `a_cut_outer_stream_leaves_the_build_running_and_a_resubmit_after_the_end_is_already_applied`; `rafka-node-admin-core/tests/status_declarations.rs:245` · no flame yet | covered |
| 14 | Repeat recovery submit for one exited birth | The fabric-primary answers AlreadyApplied or BuildInProgress (`build_state.rs:661-670` `recovery_of`; `http.rs:359` `open_recovery`) | `lifecycles/node-recovery.md:56-58` | `node_lifecycle__node_replace.rs:609`; `rafka-node-admin-core/tests/recovery_admission.rs:78` · `via-node-recovery disposition=already-applied` in 4781871544bf31bb4eb5e035189ce274 | covered |
| 15 | Provider cannot tell (`Unknown`), or the runtime row cannot be read | `NotExited` with the reason (`admin.rs:931`, :971); after that, the same as row 6 | `lifecycles/node-recovery.md:49-52` | no cell drives `Unknown` or an unreadable runtime row | gap: no test; the owner after the verdict is R-FM1 |
| 16 | The silence status cannot be written | `offline.rs::put_silence_status`: a refused put gets its own span `rdm.node_admin.node.reject.via-status-storage` (node, node_id, incarnation_id, status, store, cause); a record with no birth gets `via-no-birth`. PendingReconnect is written every tick; Dead and cleared are written once, on the tick that changes them, so a refused one leaves the row at its last status. No production code reads silence rows; the provider proof (step 3) never depends on them | node-recovery.md:47-52 | `offline` cells (RDM branch i143-s33-matrix-gaps a1b01ae7 → 26127bbc), trace d64d19ee8a8232dc653fb636f7f3b504 | covered on that branch (lands with it) |
| 17 | Node-admin restarts on its own data dir | It comes back with the same NodeId and a new incarnation that supersedes the last, seeded from the contacts it last heard, own mesh first (`admin.rs:3356-3375`, span `rdm.node_admin.node.update.via-restart`) | `node-admin-lifecycle.md:65` | `node_lifecycle__admin_restart.rs:49` · no flame yet | covered |

**Count:** 15 covered (1, 2, 3a, 3b, 4a, 4b, 4c, 5, 7a, 7b, 7c, 9, 10–14, 17), 2 gaps (15, 16), 3 rows that need a ruling (6, 6b, 8), in two packets (R-FM1, R-FM2).

Of the 15 covered, 9 have no flame yet: 2, 4b, 4c, 7c, 9, 10, 11, 13, 17. One gate run at the gate's cadence, with each cell's trace id recorded from its own artifacts, fills them.

## Findings while filling it

- **F1. Docs that no longer match main.** All three are under `docs/architecture/**`, so they are not edited here.
  - `lifecycles/implementation-gaps.md:17` lists node recovery by the mesh-primary as not implemented. It is on main (`admin.rs:4809-4826`, land3 6a4710b8).
  - `implementation-gaps.md:18` and `lifecycles/mesh-recovery.md:32-35` say the entry sweep sends unreached members to `decommission_unreached` (`reenter.rs:313-336`). Neither the function nor those lines exist: `reenter.rs` is 279 lines, and unreached members go to node recovery (`reenter.rs:11-14`).
- **F2. Issues open over landed code.** The node-recovery story (land3) and the R-EL1a seat-transfer story (land4) both landed on RDM main, and both issues are still open.
- **F3. A cell's contract text vs the code [inferred].** The contract at `rshape_burn_in.rs:5851-5862` says the near side's fabric-primary "opens the one proven-drift attempt and replaces the gateway under a new NodeId". On main, ordinary members are skipped by the fabric-primary's drift (`admin.rs:1403-1407`) and recovered by their mesh-primary. Either the text is stale or the cell asserts a path main no longer has. The cell's owner should read its assertion against `admin.rs:1403-1407`.

## R-FM1: who acts when a silent node's runtime still runs (a hung member, and a hung seat holder)?

### 1. Spec
`lifecycles/node-recovery.md:49-52`: "Still running is no proof, however long the silence: the node is hung or partitioned, not gone, and nothing is recovered." Also :47-48: "Dead is observer-inferred and is never itself a reason to act."

`fabric-node-lifecycle-elections.md:155-157`: "silence, an observer-inferred Dead or an unheard route is not proof: the holder keeps the seat."

Owner: the member's mesh-primary (detection, proof); the seat watch (seat holders). Sequence: PendingReconnect → tickle → TRUE OFFLINE (Dead) → provider: Running → nothing. **No spec covers the step after the not-exited verdict**: the node stays Dead with no end, no operator signal and no workflow.

### 2. Issue
A member whose process is hung (SIGSTOP) stays Dead and unrecovered for as long as it is hung. Its mesh-primary re-inspects it every tick and names the verdict once. Every time; `node_lifecycle__node_replace.rs:549` pins that nothing happens.

### 3. Flame
- http://192.168.68.99:16686/trace/0550b35186f1563edea62bbbb4d909b3 — the not-exited verdict:
  `rdm.node_admin.node.resolve.via-provider-inspection node=mesh1.broker.2 node_id=acajbqzse375 incarnation_id=8461206f6812fbe52e7eeaef00ef0ccc inspector=mesh1.admin.1 verdict=not-exited exit_proof=none reason="its runtime is not proven exited: Running"` (2026-10-11 12:48:46.178)
- Preceded by `rdm.node_admin.node.resolve.via-offline-tickle-failed` (12:48:46.167). It was followed 1 s later by `rdm.node_admin.node.update.via-offline-returned` (12:48:47.190): that birth came back by itself, so the only thing that ended the silence was the process.
- No flame of a hung seat holder exists.

### 4. Observations (RDM origin/main 63856583)
- [code] `rafka-node-admin-core/src/admin.rs:4790-4797`: on TRUE OFFLINE the tick puts status Dead on the birth's silence row.
- [code] `admin.rs:907-931` `exit_proof_of`: `Running` and `Unknown` both end as `Err("its runtime is not proven exited: …")`, so `Inspected::NotExited` (:957, :975-976).
- [code] `admin.rs:4816-4826`: only `Inspected::Exited` submits. `NotExited` writes the verdict into `verdicts` and ends.
- [code] `admin.rs:980-990`: the inspection span is emitted only when the verdict changes ("a hung member is inspected every tick and named once").
- [code] `seat_watch.rs:14-17`, :265: a seat holder whose runtime runs ends the look; the seat stays.
- [code] `admin.rs:1403-1413`: the fabric-primary's drift does not act on ordinary members or on admins of a mesh with a mesh-primary.
- [flame] trace above: the verdict and the return, as quoted.
#### Inferred
- [inferred] A hung mesh-primary keeps its seat (`seat_watch.rs:14-17`). Its members' recovery runs only on the seat holder's tick (`recovery_due(holds_seat, …)`, `admin.rs:1111`). The fabric-primary's drift skips them (`admin.rs:1403-1413`). So while the mesh-primary is hung, no member of that mesh is recovered by anyone. This comes from reading the three sites together; no cell drives it.
- [inferred] A hung fabric-primary keeps the seat and claims nothing. So no Build of the fabric moves while it is hung, recoveries included. This comes from `seat_watch.rs:14-17` and the fabric-primary being the one Build driver (`node-rpc-envelope.md` R-ON8).

### 5. Delta
No spec covers the step after a not-exited verdict; code writes Dead on the row, inspects again every tick, and acts on nothing, for members and seat holders alike.

### 6. Arms
Every arm assumes silence and a failed tickle stay non-proof (R-A2, node-recovery.md:47-52), and that RDM never kills a runtime it has not proven exited on its own.
- A: hold, and report it — the not-exited verdict becomes a durable, named "held-running" fact on the birth's status row and on node.get. Nothing is recovered; a person decides. Changes: the publisher (a status-row field), the node.get projection; no workflow.
- B: a bounded hold, then a provider-terminate step — after a ruled hold (in floors), the mesh-primary submits a Build whose first step asks the provider to terminate the exact runtime. Its proven exit then runs the ordinary recovery (row 1). Changes: a new Build step, a timer, the provider contract (terminate the exact runtime).
- C: as A for members; for a hung seat holder, an explicit yield on proof of a hang (a ruled signal stronger than silence) moves the seat under R-EL1. Changes: the tier (who may move a seat), the election input.
- x: something else.

## R-FM2: what does RDM do when a node's data dir, or the disk under it, is lost?

### 1. Spec
`lifecycles/node-recovery.md:22-23`: "Steps: node.delete of the exited instance, then node.create at the same path.name over the inherited data dir". "Retained: path.name, placement, inherited data dir, endpoint key, config (seeded from the Deleted row)".

:64-66: node.create "loads the endpoint key from that data dir and binds port 0, with a new node.id".

:116-117: "An exit during a workflow belongs to that workflow: its Build ends Failed, and the mesh-primary's next tick finds the member still proven exited and submits the recovery."

`node-admin-lifecycle.md:122`, :134: fabric recovery starts "on the fabric-primary's data dir".

Owner: the mesh-primary (member recovery); a person (fabric recovery). **No spec covers an inherited data dir that is gone.** The same holds for a fabric-primary data dir that is gone when every admin is lost.

### 2. Issue
A recovery whose inherited data dir does not exist fails at NodeDeleting by name: "the inherited data dir … does not exist: no node-key to load the endpoint key from". The exited member is never brought back. Seen in 3 runs of `every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle` (`mesh_identity__canonical_ids.rs:62`) on 2026-10-11, once per run.

### 3. Flame
- http://192.168.68.99:16686/trace/3b983b72926d8d5e3629d7e3d1f264cf (12:38:13.059):
  `rdm.node_admin.build.update.via-node-recovery node=mesh2.rpc.2 node_id=ngeaskfj6n3p incarnation_id=fef22c43e194495650b45d186b619c68 submitter=mesh2.admin.1 fabric_primary=mesh1.admin.1 outcome="failed: attempt 3 recover-node:mesh2.rpc.2 step attempt: recover-node:mesh2.rpc.2: NodeDeleting failed: mesh2.rpc.2: the inherited data dir /tmp/i143-every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle-3332743-0/mesh2.rpc.2-ngeaskfj6n3p does not exist: no node-key to load the endpoint key from"`
- Same outcome, one failure each, in http://192.168.68.99:16686/trace/8b44dbd598fbe3dc5b978d0a667e89ee (11:12:46) and http://192.168.68.99:16686/trace/4ae1d743d4bb869987b93730532f9b98 (11:29:20).
- The dir path names the cell's estate. Why the dir was gone (the cell's own lifecycle step, or its estate teardown) is not established: the ClickHouse window around it mixes concurrent estates under the same node names.

### 4. Observations (RDM origin/main 63856583)
- [code] `rafka-node-admin-core/src/deployment/pipeline.rs:1063-1080` `load_transport_key`: a missing dir or a missing `node-key` is refused by name, and no key is minted.
- [code] `deployment/pipeline.rs:2135-2160`: the recovery is one `recover-node:<path>` operation over the inherited dir; no step takes a new dir.
- [code] `admin.rs:1205-1216` `settle`: a Failed recovery ends the submit in an error, and the birth is released for the next tick.
- [code] `admin.rs:4809-4826`: the tick submits again for every TRUE OFFLINE member proven exited.
- [code] `build_state.rs:661-670` `recovery_of`: only a `NodeDeleted` answered by a `Complete` counts as applied. A recovery that failed before NodeDeleted leaves nothing that answers AlreadyApplied.
- [code] `rafka-node-rpc-testkit/tests/recover_pipeline.rs:246`: pins the refusal at the pipeline. No scenario cell loses a data dir on purpose.
- [flame] the three traces above: one failed attempt each.
#### Inferred
- [inferred] For a lost dir, each tick opens a new attempt of the accepted Build that fails at the same step. The loop continues for as long as the member stays TRUE OFFLINE, holding the one-Build-at-a-time door while each attempt runs. This comes from `admin.rs:1205-1216` + `:4809-4826` + `build_state.rs:661-670`. The flames show one failure per run, not the loop: each run ended first.
- [inferred] A lost fabric-primary data dir with every admin lost leaves no recovery path at all (`node-admin-lifecycle.md:134` names that dir as the only start). Under R14 (`plans/rulings/i143-r14-rdm-shared-store.md`), the durable map moves to the shared store root, but the start point is still that admin's data dir.

### 5. Delta
No spec covers an inherited data dir that is gone; code fails the recovery by name at NodeDeleting and the next tick submits the same recovery again.

### 6. Arms
Every arm assumes a lost endpoint key is never re-minted under the old key's identity (the key belongs to the data dir, `lifecycles/reference.md:199`).
- A: recover with a fresh data dir — when the inherited dir is proven gone, the same recovery creates the node at the same path.name over a new dir with a new endpoint key, and `hydrate_before_ready` runs with no inherited dir (Rafka resyncs a broker from its replicas). Changes: the recovery's PrepareStorage step, the HydrateContext (inherited = false), node-recovery.md "Retained".
- B: refuse durably and hold — the first failure records a named "data dir lost" fact on the exited birth, the tick stops resubmitting it, and a person replaces the node through an explicit Build. Changes: the publisher (a new durable fact), the tick's due rule.
- C: A for stateless kinds; B for kinds whose app declares inherited state (broker). Changes: a per-kind declaration in the deployment spec.
- x: something else.
