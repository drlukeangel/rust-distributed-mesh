# i143 - Build, prove, and export the generic Mesh product on RDM

## § Close-out plan (2026-10-17, the lead; Luke: "take stock of what's left in 143 and break it apart")

i143 pivoted mid-flight into standardising the Node RPC and gossip formats (i144 e2b/e5/e6, i146, i147). What is
left of i143 itself, by block, and who finishes it; the sequential tail (one green SHA → soaks and container
proof → export) is last and the lead's.

| block | left | disposition | owner | parallel with |
|---|---|---|---|---|
| e12.s90 | 13 criteria | **closed as superseded** by i144 (e1 the four states, e2 the fact, e3 resumption and delete, e4 canaries all landed); nothing builds here | lead (record) | — |
| e12.s57 | the two fabric_recovery cells hand the fabric seat over before every node-admin is lost | **done in the landing that carries this row** (lane e4s2): each cell runs node.stop of the fabric-primary's admin, whose first step moves its seats to its same-mesh successor (node-primary-update.md step 1), and takes it down only once that stop completes; then every node-admin is lost and the person relaunches one on the current fabric-primary's data dir (fabric-recovery.md steps 1-4, Nothing alive) | lead (e4s2) | — |
| e12.s23 | the sticky_seat.rs:551 read (a planned hand-over moves both seats in one row before the new one acts) | **absorbed by i144 e6.1**: `seat-moves/<T>` is that one row; the cell is e6.1's | i144 dev | — |
| e12.s75 | `NodeRpcServer::drain()` and its 9 callers (RDM); the `epoch` field in rafka-v2's reach-reroute and node-base | RDM rename **landed `d9f8235f`** (Codex, 2026-10-17); the rafka-v2 `epoch` rename **moved to rafka-v2 i142** | Codex | everything |
| e12.s88 | R14 item 6: the container provider's store-root qualification cell, the bind mount and `RDM_STORE_DIR` in the container env, the three container cells unskipped in the gate, green in release | **landed `8bd6681d`**, concurrent land gate GREEN 211 s; release provider qualification 12/12 and kill/recovery 6/6, corrected silence/flame controls 2/2; Q53/Q54 acknowledged exact-birth acquisition included. Q56 live Mesh-state acquisition remains i144's separate follow-on; expired-hold container case remains the recorded follow-on | Codex | the dev's e6 and i146 e3 |
| e12.s89 | the five `rshape-*-process` jobs green in one release run; each red read where it lives | most of its reds were the defects i144/i146/i147 fixed (C17 → i144 e2 landed; stopping on admission → D2 landed; identity after restart → Q47; authority hand-offs → e6; control loss hearing two births → i147). **One release run after Q47 and e6 land**, then the remaining reds split: authority and lifecycle jobs to the i144 dev, routing and partition jobs to Codex; the `node-not-live` page question is answered (node-restart.md's row, Q2). The s89 landing's first gate (`7f9b1f1d`) was red on `admin_start_declares`, i146 e3's cell: **Q58 = A** (2026-10-18, delegated; ruled by the lead; [ruling](rulings/i146-q58-cut-off-on-a-commanded-stop.md)): a view is cut off by silence no lifecycle gossip event it applied explains, never by a member it stopped; the dev builds it in e4s8 with the gossip.md §6 paragraph, exact in the ruling, for Luke's glance (i146 PRD bullet) | lead runs; dev + Codex read | — |
| e12.s69 | Rafka side: the entry pull off the rafka-mesh-ops projection frames onto node.topology.get; the belt and accept loop move under i145 (post-chaos) | **moved to rafka-v2 i142** ("Moved from RDM i143", 2026-10-17): the entry pull onto node.topology.get; the belt and accept loop stay under i145 | Codex, after s88 | the dev's e6 and i146 e3 |
| e12.s91 | the replace recipe's executable (143 agent's finding, 2026-10-22): `RecipeStep::Create` carries only the path.name, so the step launches the executing admin's own boot-time binding, and a resume on another admin or a later recovery launches a different binary than the Build asked for; node-replace.md:37 and :52 say the re-birth is with the specified bin | **landed 25df67ce8 (2026-10-23)**: `RecipeStep::Create { node, executable: Option<Binding> }` on the Build wire and the durable Build record (old records decode with none); `AdminRunner::create_template` launches the step's binding, else the binding the seeding row records (the row at the path, else `<name>.old`), else, only for a node no row records one for, the executing admin's own for its kind; a step or row binding is re-hashed at launch and a missing file, a hash mismatch or a launch id not the node's kind is refused by name, nothing launched; `NodeRow.executable` is written at the claim and kept by every row write (rename, config, tags, Deleted), so recovery over the inherited and the fresh directory runs it; durable format moved. Cell `node_replace::a_create_steps_executable_reaches_the_provider_at_every_admin_and_through_recovery` (RED first: the successor ran the admin's own rdm-rpc-node). A step's or row's binding is checked against the launching provider's image at launch as the startup set is (`Binding::check_image` in `verify_binding`; cell `admin::runner::tests::a_step_binding_is_refused_by_name_when_its_image_is_not_the_providers`, RED first: an image under the process provider was launched). **Left:** the container provider mounts only the startup set's executable directories, so a step binding outside them is not visible in a container; the cell is process-only | the 143 agent | landing 5 first |
| e12.s55 | R-HB3 | stays parked post-beta | — | — |
| e10.s2 | one green SHA, the import manifest, the parity scan, the export gate | **Exported: `abc1ec15` (2026-10-20), `export-manifest` eligible=true** (release 110 ok 0 FAIL, the export run 65 of 65, the soaks 12 of 12; 77 owners, 77 receipts, 2 gate logs, 81 §14 cells, 0 refusals; the import manifest names it, the manifest it wrote is `docs/plans/i143-export-manifest.json`). Ingestion readiness verified against i141/i142 row by row: [i143-ingestion-readiness.md](proposal/i143-ingestion-readiness.md) (demonstrated through the public API: e1, e2/e12, e3's Build path, e4 adoption, e6, e7 outcomes, e8, e11, the i142 entry-pull mechanism; not demonstrated by the demo: a consumer-bound storage backend, node-admin's app-embedding shape at estate scale, the load proof — rafka-v2's e3 s6/s8, e4 s2-s5, e9). **Live (Luke, 2026-10-20: "no chaos test runs til this is up and working"):** `scripts/demo-estate.sh up` brings the 2/2/3/3 × 2-mesh estate up from the consumer's executables with the Admin UI driving it (one Build through `/api/bootstrap`, 20 of 20 ready in 7-9 s, formation trace `48d840913d47bbd2eff9f6f02c40fd15`), `demo/tests/projections.rs` proves the projection chain (node-admin caches every kind; a node hydrates its own kinds on birth), and `scripts/demo-estate.sh stream` proves the data plane across the meshes (five `direct` streams answered by the far roles; readiness §4). At `768dfaee` (2026-10-20): release 110 ok, the export run 65 of 65, the soaks 4 ok 8 red (one product defect, Q85, three driver defects: e11.s12), `export-manifest` eligible=false (8 `receipt-not-ok`). At `4aa58cf0` (2026-10-20): release 110 ok, the export run 65 of 65, the soaks 9 ok 3 red (two driver defects; one product defect in RDM's gossip layer, unruled, Q86: a backbone neighbour pair silent in both directions while held up on both sides, §6's re-feed repairing nothing), `export-manifest` eligible=false (3 `receipt-not-ok`). Q86 ruled no (Luke): no RDM gossip repair; the soak's agreement rule scoped to the pages. At `5a220a6a` (2026-10-20): the release gate not run (a chain defect), the export run 65 of 65 with 2 refused (two cell defects: e11.s12), no soaks. At `abc1ec15` (2026-10-20, the cell fixes, the rounds stop race and Q86's rule landed): release 110 ok 0 FAIL, the export run 65 of 65 in 1,051 s, the soaks 12 of 12 in 5,847 s, `export-manifest` eligible=true. Q87 (2026-10-20) lands after it: the tail runs again at the SHA that lands Q87, and i141 pins that one | lead | — **At `dc3661eb4` (2026-10-23):** release 112 ok 0 FAIL; the export run 65 jobs, 12 refused. Five were cells behind the pages (2782, 2783, 2803-detect, 2900, rt6), fixed in `4b03ee372`; six were the rshape adversarial cells still on the pre-Q111/Q119/Q122 hold rules, rewritten to the Q4 four in `eb10d0ded` (cut-ops removed; cut-restart, cut-kill, cut-spawn, primary-victim and the partition cells pass; two-victims 2 of 4, its reds a convergence timeout after the cut-off victim's fabric recovery re-created the fabric-primary, open with Q142). The routing-chaos-container red (two exited superseded broker containers left) waits on a node-delete page sentence for a runtime's end (storage-by-object.md). The container heartbeat cell went red once after a heal (a mesh-primary's backbone delta carried a member Pending its own book held Ready: Q142, a gossip change, asked). Next: item 8 (`status.storage` out) lands, then release, export run, soaks and the export manifest at one SHA. **Item 8, `status.storage` out (storage-by-object.md; Q141 A, Q143 yes, 2026-10-23):** every fact lives on its object: mesh statuses on `mesh.storage` (`meshes/<mesh>/status/`, written by each state's setter), fabric events on `fabric.storage` (`fabric/events/`, the fabric-primary's; a mesh-primary's `ApplyFabricEvent` reads the fact, writing no copy), every fact on a birth on `nodes.storage` (`nodes/<mesh>/`: declared, durable before `Applied`, operations, silence, data dir lost/unreadable, cert and signing-cert dates); the cert and issuer receipt rows are gone, the written date is the receipt. Fast gate 73 ok 0 FAIL and release 112 ok on the lane before the landing, the release's one red `cohort_election`'s partition matrix (passes alone on the lane and on main; first sighting, open with Q142). |
| e11.s12 | the seeded soaks and the container proof on that SHA | **green at `abc1ec15` (2026-10-20): the twelve soaks 12 of 12 (5,847 s; every story's `invariant-results.json` holds with `violations: []`), the container proof in the release gate (`container-proof` 29 s) and the sixteen container receipts of the export run.** At `2a641bf6` (2026-10-20, Q87 and Q88): 11 of 12, `soak-authority-container` red, read in e10.s2's fifth run; the tail runs again at the follow-up landing's SHA. **The conformance review's dev list (2026-10-21, one lane and one short dev per item, landed serially under the turn lock):** item 5 (one owner of every round checklist, `RoundDriver` removed, one `publish_hook` and one `eligible` in rdm-node) landed at `7ff55c10`; items 13 and 14 (the refused dial's span names the presented cert's expiry and the clock, the inspect span names the handle; `rename_row` kept, it has a test caller; `ca_ttl_secs` reported, fabric-ca-renew.md is Luke's build-or-strike) landed at `3ba3eb02` on the second gate, the first (`d9e505aa`) red on `node_stop::a_stop_the_birth_never_admitted…` reading the released birth Pending at its commanded join: the cell fix written at `0c87d0e3` in lane e4s8, queued to land. Items 2 and 11 (a node-admin's signal is node.stop's steps; the failed start stops through them; one `leave_channels`) landed at `20e417ee` on the fourth gate: the first two (`153ad9fe`, `fbc04590`) died on the test binary's 300 s timeout, read by the dev as `cert_renewal::a_renewal_terminates_nothing_and_publishes_nothing` sitting in an echo on the wrong connection (its own retried open after the peer had admitted the first; green alone, red under the gate's load twice), a cell race on main fixed in the same landing (`bdf16ab7`: one open paired with its accept, every wait bounded); the third (`b24a6eca`) red on `node_heard_again`'s start cut short by the cell's own stop, main's race, fixed first from lane e4s3 (`3aab74f4`: the release waits for the mesh-primary's `via-cycle-begun`, not the released birth's stale ready digest). Q92 asked on the failed start's owed Stopped naming the start. Item 7 (WaitForMeshJoin reads the join's report, never the digest book) in lane e6s8: `b1ca7275`, then the admitted report made durable in the birth's runtime row (`4f2a17ff`: an admin restarted between the join and the read would otherwise fail the step by deadline on every attempt), then its first gate (`04983a90`) red on three cells: `executor_redecide` (the fault keyed on the digest read the step no longer makes, re-keyed, `587dd229`), `node_heard_again` (main's race, above) and `fabric_primary::three_mesh_primaries…` 1 in 4 on its tree against 0 in 10 on main, read as the mesh-primary's tick commanding node.mesh.join to a birth whose first start is still running (the create completes its join wait earlier now): the `create-in-flight` guard in `heard_again` (`Joins::creating`: a birth whose node.create is under way is that create's, never commanded back) closed it, six of six after; landed at `ac18a0b9` (`fc78d58b`, `ac18a0b9` and the two before them). Left as flags by the dev: the digest-book read kept for `claimed_allocation`; the narrower WaitForBind window of the same shape; the runtime row's two writers (the door first, the gossip task later, same content); the ordering `keep_admitted` before `install` that opens the millisecond window, a ruling if it is to move. Item 12 stopped before code: the repeat entry pull needs the `Joined` answer's control and floor, which no `TopologyReply` carries (Q91 asked). Q89 landed at `251d424e` (2026-10-21): the six gates of its delta and the eight changes its cells forced (the proposal's §5.1: the round action once per key across roles, the takeover's provider inspection before a release, the fabric-primary watching a mesh with no admin, the view gate measured at the first receipt after a silence, the hand-back by the exit row's name and across meshes with the lost mesh's seat, the standby's floor on the record and the ladder's role, the fabric-primary's refusal of a commanded join, a lone node's neighbour wait); fast gate 73 of 73 twice, the landing gate green. The eighth export tail at `251d424e` (lane e4s8): fast gate 73 of 73 (449 s); release gate 3 FAIL of 110 (541 s): `node_replace::a_hung_mesh_primary_held_past_the_hold_is_terminated_and_recovered_by_the_fabric_primary` (read: the fabric-primary's new watch of another mesh's admins marked the hung mesh-primary Dead in its own view when its standby blinked off ready under load, and the seat watch looks at no Dead holder, so the hung hold never expired into a recovery; the watch is now the tickle and the command back only, the marks stay the mesh's own), `rshape-partition-chaos-process/mock_partition_heal_recovers_coverage_and_routes` (the story counted the hand-back's drift attempt and the takeover's runtime adopt as deaths; its set is the page's five) and `i143-2784-chaos-container/container_network_partitions_heals_preserves_live_runtimes` (the wedge detector refused any move of the fabric seat across a stall; the playbook moves it and brings it home, so the detector reads "home after" and the soak's after read waits for it); the three fixes in lane e1s8 (`e1s8-fp-watch-marks`), to land, then the tail runs again at that SHA. Items 8, 3 and 10 with 4 start now in lanes e4s3, e6s7 and e7s3; 9 after 8 and 10; 1 last, alone. **The tail after Q89 (2026-10-21):** items 10 and 4 landed at `a8201c89` with the heard-again Pending race fixed at the join door (`815e222a`: the commanded join's cycle begins and its Pending is put before the join is answered; the gate at `a8201c89` was red on `node_heard_again`, the node's Ready put between the cycle begin and the Pending); item 8, one node.tickle, at `ce596f3c` to `281fa4cb` (its gate at `f8b0f6f5` red twice: `fabric_handover::a_successor_that_refuses…`, the cell's admins answering only the old Ping, moved to the page's ProbeNodeState; and `build_op::a_lost_executor…`, WaitForNodeReady with no digest of the restarted birth, not reproduced in six reruns at the gate's concurrency and unread, no artifacts kept); Q93 (Luke): fabric-recovery step 2 and node-start step 5 at `fb7a5d6b`, node-mesh-join steps 5, 7 and 9 with the member cert installed at every join and a node-admin's NodeJoined after its neighbour at `99d22fc5` and `074d0367`; Q94's and Q96's lines at `dce778ea`; item 9, one node.runtime.inspect, at `42220140`; Q95, a stop gossips nothing but R-NS1's final aggregate, at `2471ab81`; Q96's container cell at `bcf49ea7`; item 10's inherited-root exit-proof row from the inspect at `9aa4f7a2`. Open: Q97 with a dev in lane e4s3; item 12 stopped on Q98 (the no-pointer blocker names no Build and 0x1F reads one; its RED cell `7d20d64f` in lane e6s7); Q90's rejoin and cell in lane e4s8, the hang not reproduced in twelve runs on main, so no flame of the rejoin and the Q90 sentence held; item 1 last; then the export tail at one SHA. **Later the same day:** item 12 to Q98 and its addendum (FetchCurrentBuild) at `16d0ba3a`, Q100's lines at `49c56eb6`; item 1 (one member-status transition) at `94b70808`, `ee05e4a6`; Q97 at `dd5356c2` to `7ae484d2` with Q99's lines at `a3826e61`. **Q90 reset (Luke, d45f90e1):** gossip is eventual and unordered and no step waits on it; the start's neighbour wait and its lone-node exception are removed from both start paths, a node-admin reports NodeJoined on entering its channel as an rpc node does, node-mesh-join.md step 9 is "`node.mesh.joined`. The node's first `Digest` heartbeat follows."; Q101 to Q103 cancelled, lane e4s8's rejoin, seam and controlled cell not landed; two cells that bounded gossip in time are test defects (c6a8211f), fixed in the same landing. Open: the Q99 per-join fix (lane e7s3, `41ddddc0`), whose rdm-admin runs stalled 2 of about 30 against 0 of 30 on main; then the export tail at one SHA. | lead, with the dev | — |

Gate defect (2026-10-17, **fixed 2026-10-18**, lead): `rdm-gate-at.sh`'s test-discovery check refused any binary whose test count dropped, with no allowance for cells deleted with their code (i146 e3's linger cells). A landing now names each removed cell in a commit body, one per line, `removed: <module>::<cell>` (the name `--list` prints; the fn name alone also matches), read from `origin/main..HEAD`; only those may leave discovery (`[discovery] removed by name`), and the workspace total may fall by exactly that many. Checked on fixtures: an unnamed drop still refuses. 

**The gate's storm, read (2026-10-18, dev; one bounded read).** Every journal-space wait of a fast gate falls in one minute, the first after discovery, when the stems start together. On main `de7d94f8` that minute was 15:16:47 to 15:17:52, sampled every 2 s: up to 308 threads in `jbd2_log_wait_for_space` and 653 journal commits/s; there were none for the rest of the gate.
- In that minute 425 nodes were born (222 `rafka-rpc-node`, 124 `rafka-node-admin`, 79 `faulted-node-admin`) and 15 842 files were created:
  - 6 386 Build lane facts (40%, about 15 per birth); 2 261 logs; 1 865 other data-dir files; 1 240 Node rows; 975 status rows;
  - 847 evidence span files; 467 node keys; 460 runtime records; 388 deployment records; 243 seat moves; the rest under 110 each.
- One `accepted_topology` estate, traced with `strace` (about seven births over 37 s):
  - 48 fsyncs, 30 of them its 15 lane facts (two each: the fact's temp file and the lane directory);
  - 155 files created, 49 directories made, 52 links, 30 renames and 365 unlinks (280 of them its teardown);
  - 2.55 MB written, 2.03 MB of it evidence spans.
- Each lane fact is a temp file created, written and fsynced, linked to its key, the temp unlinked, then the lane directory fsynced. Every fact a birth writes is a step receipt §5.2 needs durable before the birth answers, so none is needless.
- [inferred] A birth appends its facts one at a time, each step waiting for its receipt, so a store's batch never holds two. Group commit (RED `b675fb4a`, GREEN `e35c407d`) merges nothing and is held, not landed. Its own fast gate was green with the same peak (310 waiters, 710 commits/s).
- What a root fix can remove without moving a key, a format or the insert-and-fail is the temp name: a create and an unlink in the lane directory per fact. A fact can instead be written as an unnamed file (`O_TMPFILE`) and linked to its key, with the same two fsyncs.
- This read's first land gate (`57131832`, on `de7d94f8`) was red on i148's `connections::` cells alone (tracing-subscriber: "tried to clone Id(57005), but no span exists with that ID"), routed to Codex. It is the third gate today with that red (`16a26f9e`, `b0af22c4`'s sampled gate).

**The finish, in order (the lead, 2026-10-18; Luke: chaos second to last, then the certs, then chaos again).** Every landing through the land script, RED first, with its flame; a red read where it lives, never re-rolled.

1. **The code epics, in parallel, now.** Dev (e4s8): the `fabric_primary.rs:204` acceptance wait (i144 e6: reproduce under gate concurrency with the journal-turn span, name the wait, fix the root); then every i146 e3 row built and not landed (the `78bd4dc5` red tree and the owner seats the e3 row lists), landed one at a time; i144 e2's four R21 cells stay release-only and are proven in step 2. Codex (e7s3): i148 e1 the surface and Direct → e2 the route and the spliced Proxy → e3 reconnect moves, one landing each, the API names Luke's to object to; i148's demo acceptance (its PRD's table) runs in the pinned demo consumer through RDM's public API and closes the epic. Lead: the records, the rulings (next free Q72), and the release run below the moment step 1 has no open landing.
1b. **The storage provider seam (Luke, 2026-10-18: "that storage provider can easily and seamlessly be replaced in 142/143 with topic-backed storage").** Rafka binds topic-backed implementations of RDM's six storage boundaries at i141; everything that reaches the file store past those boundaries breaks that. Read on `e8e4621c`: the six are traits with a memory and a file implementation each (`MeshStorage`, `NodesStorage`, `FabricStorage`, `StatusStorage`, `ConnectionsStorage`, `BuildStateAdapter`), but (1) `BuildStateAdapter::claim_in_flight` takes the concrete `MeshClaims` (built on `FileRecords`); (2) `deployment/pipeline.rs` (14 sites) and `deployment/provider.rs` (4) reach `StoreRoot` / `FileRecords` directly for runtime rows and claims; (3) `seat_revision.rs` (3) reads the store root directly; (4) `build_lanes.rs` and `mesh_claims.rs` are file implementations behind no trait. The block: every durable read and write in rdm-admin and rdm-node goes through one of the six traits; `StoreRoot` and `FileRecords` appear only inside the `File*` implementations; `claim_in_flight` takes a trait. Proof, one cell: a whole admin boots and runs the gate's scenario suite on the memory implementations alone, the file provider absent from the build. Dev, after group commit lands; i141 U3 binds behind the six traits and waits for this block.
   - **RED (dev, 2026-10-18; Arch: A, "StorageBackend").** The file backend sits behind a cargo feature, `file-backend`, on by default in rdm-node and rdm-admin: `record_store`, `store_root`, `build_lanes`, `FileConnectionsStorage` and its history log, the scratchpad's `StorePublication`, and the `File*` mesh, nodes, fabric, status and journal implementations with their formats and helpers. The default build is unchanged.
     - `cargo check -p rdm-admin --no-default-features` fails at every durable reach outside the boundaries: 35 errors in 13 files, first pass (the compiler holds some back until the first are fixed). rdm-node's 3 were stubbed out for this one build to reach rdm-admin's 32; the stubs were not kept.
     - rdm-node `node.rs:408`, `:411`, `:645`: the rpc node's boot opens the `StoreRoot`, builds `FileConnectionsStorage` and uses `StorePublication` as its commit-state action.
     - `admin/boot.rs` (11): `store_root::qualify` `:284`, `FileNodesStorage::own_in` `:290`, `StoreRoot::open` `:306`, `FileFabricStorage` `:311`, `FileMeshStorage` `:313`, `FileNodesStorage` `:319`, `FileJournal` `:321`, `build_lanes` `:804`, `:807`, `:808`, `FileStatusStorage` `:834`: the construction that becomes the file backend's.
     - `fabric_handover.rs` (6): the four `File*` storage types it holds `:166-172` and `record_store` `:188`, `:199`: the seat transfer's cut-off reaches the file stores.
     - `build_state.rs` `:1817`, `:1836` (3) and `fabric_storage.rs` `:658-666` (4): the lane types (`LaneId`, `LaneSeal`) live in the file module `build_lanes`, used by `LaneRevisions`, `SealsOwed` and `seals_owed`.
     - `mesh_claims.rs` (3: `record_store` `:19`, `store_root` `:68`, `:81`) and `build_claim.rs` `:27` (1): the in-flight claim and the attempt contexts are file-backed behind no trait; `BuildStateAdapter::claim_in_flight` takes the concrete `MeshClaims`.
     - `fabric_rounds.rs:34`: the commit-state publication (`StorePublication`).
     - `deployment/pipeline.rs:1604` (`FileNodesStorage`, runtime rows), `deployment/provider.rs:417` (`record_store`), `workflows/fabric_create.rs:9` (`FileFabricStorage`).
     - `seat_revision.rs`, named in this row, no longer appears: its reaches were the generic `blocking_io` and `StorageError`, which left the file store first (`88d411cd`).
   - **Second pass (dev, 2026-10-18).** With the 35 fixed, the gate's own `workspace-check` (`cargo check --workspace --tests --no-default-features`) failed in test code: 227 errors in rdm-admin's unit tests (the record store's test helpers `tempdir`, `runtime_runs_while_a_fifo_is_read`, `op_waits_on_the_blocking_pool`; the file-store cells of `storage.rs`, `fabric_storage.rs`, `build_state.rs`, `status_storage.rs` and seven `admin.rs` cells), 79 in rdm-admin's integration cells (eleven modules on `start_with` or a `File*` store) and rdm-node's `connection_reconnect`. The helpers move to `test_support`; the cells that exercise a file store require the feature.
   - **The seam (Arch, 2026-10-18, read against a topic backend).** One `StorageBackend` beside `Wiring` (`admin::start_on`, `entry::run_on`; `start`, `start_with`, `entry::run` pass `FileBackend`): `own_row(data_dir)` before anything opens, then `open(scope)` to the six boundaries, the commit-state action and the seat transfer's cut-off; connections open by `path.name` once it is decided. The scope names no file layout: `FileBackend` owns the store dir. Each mesh's in-flight row is a `MeshStorage` row and `claim_in_flight` takes `&dyn MeshStorage`; attempt contexts are the Build journal's; the seats a store reads are trait methods (`FabricStorage::set_seat_source`, `LocalBuildLog::hold_seats`), and the memory backend implements the cut-off and the stale-seat refusals, never a no-op. The thirteen `StatusStorage` methods whose defaults answered `Ok(None)` and `Ok(())` are required. The runtime rows a launched node-admin starts with are the provider's hand-off beside the launch record (`held-runtimes.json`), put through the launched admin's own `NodesStorage::put_runtime`. An rpc node opens its connections rows and commit-state action from a `NodeStorageBackend` (`node::start_on`, `rdm_node::run_on`).
   - **Proofs (dev, 2026-10-18).** Compile-time: the gate's `workspace-check` passes with the feature off. Runtime: `rdm-test` `memory_estate`, one in-process admin on `MemoryBackend` running birth, `node.create` of two rpc nodes (a stop moves the stopped node's cohort seat to a secondary of its type), `node.stop`, `node.start`, `node.restart` and `build.get` (Complete with its receipts); its data dir holds no store, every file under the store dir is the rpc nodes' own `connections/` rows, and the storage span says `writes_to=memory`. Green on the gate's build (2.1 s); built without the feature, `cargo test -p rdm-test --no-default-features --features testkit-skew,testkit-gossip,rig --test memory_estate` 104 s wall, 0.56 s of it the cell. Guards with no RED run (written after the fix): `the_memory_backend_cuts_off_a_moving_seats_writes_as_the_file_backend_does`, `the_memory_log_refuses_a_stale_seats_facts_as_the_file_journal_does`. `held-runtimes/1` is a new durable format, pinned by `the_held_runtimes_hand_off_is_held_runtimes_1_and_another_format_is_refused_by_name`.
   - **First land gate (`87c5dd09`, red, recorded).** Two of 1b's own: the journal-opening cell (`nothing_but_this_module_opens_a_build_journal`) and mesh-audit's `store_layout_reseal_is_atomic` named the files the code left (the journal now opens in the file backend's construction; the lane key formats live in `build_state.rs`, the in-flight row's in `storage.rs`); both resealed to the new homes, the bytes unchanged and the byte-for-byte cell green. Outside 1b: `rdm-transport` `app_channels::a_later_held_type_joins_through_the_peer_that_reported_it` ran past 300 s; `rdm-transport` depends on none of the changed crates, and the cell was red on e1s8's `e7199452` and e7s3's `880e2dcc` gates the same day.
   - **The seam per landing (Arch, 2026-10-18).** **Removed (Luke, 2026-10-18: it duplicated the release gate's feature-off check; rule, binary, cells and gate job gone in `50f24546`, the code fixes kept).** The feature-off `workspace-check` stays release-only: a second cargo shape in the fast gate thrashes the one incremental cache. The fast gate runs mesh-audit's `storage-seam` instead, a syn rule over the product crates' sources that refuses `FileRecords`, `StoreRoot`, `FileBackend`, `FileNodeBackend`, `FileJournal` and every `File…Storage` outside the file backend's own modules and outside `cfg(test)`, naming each site. RED on `1f105e30`: 10 sites (`boot.rs` and `entry.rs` constructing `FileBackend`, `fabric_handover.rs`'s seat stores, `rdm-node`'s `lib.rs` and `node.rs` constructing `FileNodeBackend`, `workflows/fabric_create.rs`'s re-export); the seat stores move into `storage_backend.rs`, the default constructions become `storage_backend::file_backend` and `file_node_backend`, and the re-exports go. First land gate `29833ccd` red, recorded: build_state's journal-opening cell found its own refused text in the seam rule's planted fixture; the fixture spells it in pieces.
   - **The Build journal seam assumes a log held in memory (found by DevX at i141 U3.2, 2026-10-18).** Rafka's read rule (rafka-v2 CLAUDE.md: a read is a read-through or a point read; nothing drains a plane into memory) meets two whole-log reads in block 1b's Build-journal seam. (1) The four birth reads: answered (Q71), one derivation as keyed entries (`11f194f3`). (2) `LocalBuildLog::derive_on_every_fact` runs `overlays_from_facts` (admin/boot.rs:197) over the whole log on every fact: `fold`, `in_flight_ops`, `forgotten_builds`. Read on `b7548453`: what it needs is bounded. Open operations come only from Builds with an attempt not yet Converged or Failed, and a held overlay ends on its own Build's facts, so the derivation can read the Builds still open and the Builds the book holds overlays of, through the adapter's per-Build reads, with the same overlays out. **Landed `618a2072`** (2026-10-18): `FactDerivation` names the Builds it reads (the new facts' own, the current Build, the Builds the book holds overlays of; only the current Build carries open attempts, build.md:153/:155); the memory and file adapters filter to them, the overlays are unchanged (the fast gate's overlay cells green), and a cell proves the reads. Rafka's `LocalBuildLog` still waits on e4 for `hold_seats` and the seals. The early connections-deleting write seen at `pipeline.rs:1785` (C17's tail) has no red or span behind it, so it is not raised (dropped until a run shows it).
   - **Second landing, Q66 = a (RDM INDEX `a41bc077`).** **Withdrawn (Q66, Luke, 2026-10-18: no machinery for a case nobody asked for; reverted in `255822dd`).** RDM's storage cells become one suite in `rdm-test`, generic over `StorageBackend`, each cell taking a backend factory, run in the gate against the memory and the file backend; it replaces the per-backend cells (`rdm-admin/tests` `build_claim.rs`, `connections_writer.rs`, `status_seat_fold.rs`, `fabric_storage.rs`'s unit cells, Q35's two-writers race), each moved with its meaning intact and named for its contract. Rafka runs the same suite against its topic backend at i141. It needs `MemoryBackend::new()` to be one store: each `open` one writer's view over shared rows, its own cut-off, seats and data-dir-local rows. Sized first by the memory journal: Q35's race needs shared lane keys with insert-and-fail in memory, and `status_seat_fold` needs status rows keyed by mesh.
2. **Chaos, first pass.** The demo build first: the demo consumer and the Admin UI (`demo/Cargo.toml`, `demo/admin-ui`) re-pinned to the one SHA step 1 ends on, lock refreshed, built by `scripts/i143-rshape-build-consumer.sh` (the `b0fc8871` preflight red: both manifests name the same revision). Then one release run (`rdm-gate-at.sh <lane> release`) at that SHA: the five `rshape-*-process` jobs, C17, the release-only cells (`node_replace`, `provider_state`, `owed_check_ins`). Its reds split as the s89 row says: authority and lifecycle jobs to the dev, routing and partition jobs to Codex; each fixed, RED first, then one more release run, until the release suite is green on one SHA.
3. **The certs, i145.** e1 the format moves into RDM (`CertChoice` is the material) → e2 the accept loop and `node.connections.admit` are rdm-transport's → e3 the cert row, `node.cert.renew`, `mesh.issuer.renew`, `node.issuer.update`, the policy and the watch → e5 the CA under the root (`fabric.ca.create`, `fabric.ca.renew`, `fabric.root.update`). Codex takes e1 and e2 (the transport), the dev e3 and e5 (the rows, the renew Builds, the policy). e4 (Rafka passes the root in; its minting, accept loop and belt go) is rafka-v2's and moves with i142.
   - **First release run, `b7548453` (2026-10-18): 17 red, read.** Log `release-b7548453.log`, kept evidence `kept/gate-gates-e4s3-b7548453release/`. Green: rshape-composition, rshape-partition, the demo consumer and its six i148 cells. No stem ran at production windows (release exports none; rdm-test's main sets 3000/500/500). The box stalled on disk throughout (3,790 slow evidence writes, up to 18.2 s). Product defects, the dev's: a node-admin stack overflow while it holds the fabric seat after serving a re-created admin's join (new since `8c2e79c6`; cohort_recovery, peer_mesh_investigation, fabric_takeover, 2784); a hung mesh-primary's recovery refused stale-seat in its own lane, never terminated (node_replace, red in `8c2e79c6` too); a cleared silence row written outside the seat guard (boot.rs:2154, node_restart:323); a drive paused by a cut-off never resumed (owed_check_ins state_commit); a node's ConnectionsDeleting declared after its Stopped (rshape-lifecycle). Cell defects: owed_check_ins's lone fabric-primary (fixed `12e5693e`, gossip.md §6); rshape-authority's per-lane attempt map and hand-off order; rshape-lifecycle's refusal window; probe_root_span's check order; mesh_delete_no_survivor's applied-only filter; container-proof's fix: none exists (the s88 branch has no commits past main), so it is written (plan-to-finish 1.8). Gemini: rshape-routing C11 (30 s bound against a 35.8 s backoff, and the dial in flight when the cut lifted), delivery and forward's 100 ms margins. Disk stalls, no product change: five probe cells, node_restart:252, store_root_boot, build_op's hash cell. Open for a ruling packet if the dev's read confirms it: a frame queued across a suspension counted as hearing its member (owed_check_ins open_traffic). Gate: release-only jobs get 900 s so a killed job keeps its summary. **Landed since (2026-10-18):** the stack overflow `4c5007a6` (recipe_step's create runs the create workflow, one run_op frame; bisect named `b31b4556` as the last few KB); the lone fabric-primary cell `12e5693e`; the hung mesh-primary's recovery in the fabric lane `f5c2d771` (R-FM1; mesh-investigation.md:204, node-recovery.md:174; R-D1 admits only that attempt's own retire and create-over; trace `a17d12d479ce3e8d6e098bf05d916234`) and `ac43ebf5` (an unreadable /proc read names its path and cause). New red, in no row before: `cohort_recovery::recovery_admin_restores_lost_cohort…` at :197, red since `602b1863` (plan-to-finish 1.5). The plan for everything left is `proposal/plan-to-finish-141-148.md`.
   - **Release runs since (2026-10-19):** `d829b383` (RDM box, lane e6s4, 949 s): 4 red: `cohort_election` (Q74's flame), `provider_state:495` (fixed `d313cc8e`), rshape-authority R3 (fixed `694355cd`), rshape-lifecycle C17 at `:4417` (a stale `applied`-only count, fixed `2a6563fb`; the early connections-deleting write at `pipeline.rs:1785` was already gone with `98254701`). `bd107007` (DevX box, lane rdm1, alone, GATE_JOBS=12, 721 s): **2 red**; every rshape job, provider_state, `cohort_election`, `i143-2784-chaos-container` and `i143-2780-process` green. Red: `rejoin_name.rs:136` (Q79's race: the stop's owed declaration served after the start's join, its span under the boot name; the fix, branch `i143-e4s2-start-ends-stop-declaration`, waits on Luke's yes on two page lines), and `rdm-transport:main` timed out at 300 s in the batch (81 of 201 passed, 20 cells past 60 s: `collector_outage::*`, `delivery::*`, one `app_channels` cell; alone 3 of 4 green, the fourth a tracing-subscriber `sharded.rs:306` "no span exists" panic at `connections.rs:519`; subagent B's). Evidence on the DevX box `/home/adsmin/rust-distributed-mesh-deploy/run6-bd107007/`. **The `bd107007` run is not a full release: the DevX box has no `nft`, so four netfault cells were skipped by name ("host-cannot-cut: … nft unavailable"): rshape-partition's `mock_partition_heal_recovers_coverage_and_routes` and `mock_control_loss_recovers_without_status_heartbeat`, and rshape-routing's `mock_routes_survive_connection_and_birth_supersession` and `mock_connection_churn_releases_superseded_resources`.** The release run that certifies e10.s2's SHA runs on the RDM box (real `nft`, as `d829b383` did) or on the DevX box once it has passwordless `nft`. bd107007's ran cells, one trace each (the cell's canary `trace-url.txt` where it writes one, else the state-climb trace with the most spans): authority `mock_authority_handoffs_preserve_one_effective_writer` `2f43e228f3aa08a2c1dd393fed1d69b0`, `mock_recovery_retains_build_and_advances_attempt` `9d5f71d3aace283524ee1be2e268e3d0`; lifecycle `mock_role_restarts_preserve_identity_and_birth` `57026586fd646b2fb629d69079cd100d`, `mock_node_replacement_creates_distinct_logical_identity` `dada5674df8fb61416f8300a39d9db73`, C17 `ae0c6283c94860967352869231c40891`; composition `bab1cd0e3ae991c20bf4f9bd55263b0f`, `f14f2db96edf0b7717eee0bc96d4d627`, `675a42fb37052ae4c4e81dbc9a2166d3`; routing `mock_proxy_cutback_requires_durable_retirement` `b090d1390292d84172433173cd5c56cf`; `i143-2784-chaos-container` 6/6 (`004f6444565e2439b0a349c4285321fa`, `7184f9519240a4ea03150d8f68eaae64`, `7c3eb37e98f0c435ff3faa9c44c991c9`, `38de548b624649b80328e3e23780555b`, `009060d54b23eb69907c59517ee4c838`, `879f3e9d07a5292ddc56ee11bccb6879`); `i143-2777-container` 2/2 (`d5a546239316f6faecbd28eac9fce4ba`, `ef71c71bb555d48d279724007a2848be`); `container-proof` 12 + 6 passed (`container_kill` canary `f8b41e7fd18ce0b6951299e5283e969c`).
   - **`ac34fb66` (RDM box, lane e6s7, real `nft`, GATE_JOBS=12, 927 s): 1 red of 105 jobs.** Every rshape-*-process job green with its netfault cells run (partition 450 s, routing 569 s, authority 534 s, lifecycle 624 s, composition 413 s), `cohort_election` and `cohort_recovery` green, `rdm-transport:main` green in 31 s (bd107007's batch stall did not occur), `rejoin_name` green this run. Red: `owed_check_ins.rs:239`, `a_fabric_primary_cut_off_during_state_commit_re_runs_its_own_mesh_round_and_the_fabric_reaches_ready`, "the cut-off ended mesh1.admin.1's tenure": the cut-off fired on the fabric-primary after its suspension and no `rdm.node_admin.fabric.remove.via-tenure-ended` followed (the cell's cut-off branch; d829b383 and bd107007 never took it). Subagent B's, read first. Log `gates-e6s7-ac34fb66release.log`.
   - **Landing state, 2026-10-19 ~08:10 (main `d55112a6`).** The RDM dev's b2 fix batch (cells 1-8 and the four that followed: manifest_replay key set, presence and process scope; proof_holder_lifecycle's seat-move attempt; the adv-cut cells' second gateway, R-A2 seat and lane reads; lifecycle-chaos-container's RuntimeFact lookup; 2784's both-paths assertion) is proven by its jobs' receipts at `6bd53d54`, `8218d720`, `ea7d227b` and `3bb66b9c`, and waits to land: its land gate at `42c9507e` was red only on `rejoin_name.rs:136` (Q79's race, the eighth landing it has stopped). Subagent B's `owed_check_ins:239` cell fix (a cut-off during the estate's own teardown is not the suspension's) is landing. i145 e1 → e2 → e3 is landing-ready on lane e6s4 (fast gate `8e932eea` green on `d55112a6`; e1 lands once, from the dev's `i145-e1-cert-format`) and waits for a green release, as the plan orders. The candidate for the next release and the 77-job export run is the first main that carries the fix batch and the owed fix.
   - **Rulings and fixes, 2026-10-19 afternoon.** Q74 (Luke): a death before the fabric's first state-commit is not healed; the fabric start fails and a new fabric is launched; nothing tests it and no chaos fault is armed before ready (the RDM dev's cleanup). Q75 (Luke): gossip is fire-and-forget, so the iroh-gossip fork never answers a Neighbor; with Q77 (the fork's connection_loop back to upstream's join!) it is one fork push and RDM pin bump (DevX). Q76: a departure ends every overlay of its birth at the receiver (gossip.md §3.1), the lead's fix, RED first. Q78 withdrawn: its premise was false; gossip.md:832 stands. Q79 (Luke): a node is renamed only live, after its connections are deleted and it has left its mesh, then stopped; a replace is a re-birth at the same path.name; the pages are on `lead/replace-pages` (`9357db01`) and land with the RDM dev's code; rejoin_name's renamed-while-stopped scenario goes with it. Q80: a re-born mesh-primary takes a member's node.started before its digest (states.md), landing. The candidate for the release and the 77-job export run (`scripts/i143-export-run.sh`, `7740fbc3`) is the first main carrying the container R-shape cells, the replace redesign and these fixes.
   - **The release at `7ae3aa83` (lane e4s3, 2026-10-19): red on `demo-rshape-consumer-app_channels` (3 red in 18 runs across e4s3 and e4s8; `28df61d0` 1 in 17: not a regression).** Q84: a joining node's gossip neighbour dropped 2-4 ms after it formed, the broker's first `demo.records` event published into the gap reached no one and the cell's pull timed out at `:65`/`:49`. Read from the spans, iroh-gossip's own log (`RDM_DEBUG_GOSSIP`, test tooling kept) and iroh-gossip's source: three node-admin callers dialled the fresh joiner at its join while it dialled its seed, and two connections registered in opposite orders are each closed by the other side (Q77's mechanism; nothing in iroh changed, Luke). Fixed to gossip.md §5/§6/§8 at the three callers (`hold_app_types_from`, the round's member dial gone); proof 31 runs with the join shape 0 (double dial 0, early drop 0, `:49`/`:65` 0); flame on .99: the join trace with no dial in it `f312b4853864a01dac394bd5f369f1e6`, the broker's one dial `438054dbb6ea44ccc9b42a9e1247bd50`, the first `demo.records` publish `0d3702b3c858d7aadc18479b31373d3c` and its frame on the admin 1.1 ms later `6dde73011522629b288e9d9eb28419ba`; fast gate `ed00b4ab` 73 ok 0 FAIL 248 s, its ClickHouse window 4,482 dial spans with `admin.round`/`apps.learn` 0, [proposal §4c](proposal/gossip-neighbour-drop-at-join.md). The cell's other red, `:313` (4 of the 31; 14 of the 47 old reds): the cell's own 4,096-event burst leaves ~2,000 pushes dropped at the publisher's per-peer gossip send queue, the large record's 3 chunks were published into it and dropped there (iroh's log, run 9), and a dropped push to an eager peer on a one-edge topic is announced to no one (plumtree's IHave goes to lazy peers; §7 repairs through the mesh, §8's pull recovers); the cell's incomplete-assembly shape now runs before the burst (its own commit; 11 of 11 runs green, the cut chunk `df80d1d8719e8d678033cb17b698eaee` and the expiry `6675d35ae119f41aaf8b567d27d00abb` on .99). Landed from e4s8: `224d2f8b` (X2) and `1feb0925` (the cell), gate `gates-e4s8-1feb0925` 73 ok 0 FAIL 196 s. **Release run on `1feb0925` (lane e4s8, 2026-10-20): 109 ok, 0 FAIL, 586 s** (`gates-e4s8-1feb0925release.log`; `[gate] head=1feb0925… dirty=0`), the consumer built from the landed SHA, `demo-rshape-consumer-app_channels` ok 35 s, every `rshape-*-process` job green: the first green release since `7ae3aa83`'s red. Follow-up: X1's `apps.subscribe`/`apps.learn` dial spans carry `node=` empty (the app hub's `node_name()` is blank), so an app-hub dial's side reads only from `peers`.
   - **`6123613b`: the first fully green release (RDM box, lane e4s3, 2026-10-19): 107 jobs ok, 0 FAIL, 1068 s**, real `nft` with `RDM_REQUIRE_NETFAULT=1`, `[gate] head=6123613b… dirty=0` (`gates-e4s3-6123613brelease.log`). The 77-job export run (`scripts/i143-export-run.sh`) follows at the same SHA.
   - **`1921b77b`: the release before certs, green (RDM box, lane e4s3, 2026-10-19): 107 jobs ok, 0 FAIL, 514 s**, `RDM_REQUIRE_NETFAULT=1`, `[gate] head=1921b77b… dirty=0` (`gates-e4s3-1921b77brelease.log`). Between `6123613b` and this commit: the iroh-gossip fork back-out to `508fb795` (Luke's permission, Q75/Q77), Q76's departure overlay fix, Q80's start-token read, bootstrap_pending, the os-stopped removal, the demo lock's gossip pin and the Chaos tab's Q74 kill witness. The export run at `6123613b` was stopped; the 77-job export run, the 12 soaks, the container proof and `export-manifest` run at the i145 cert commit (e1 → e2/e3 → e5.1, the RDM dev).
   - **The cert-commit tail, 2026-10-19 (lead and RDM dev).** i145 e1 `3d6e4695`, e2/e3 `e56f91e2`, e5.1 `4c706850` landed (Q81, Q82, Q83). The release at `4c706850` refused at consumer-build (rdm-core's exact `ed25519-dalek` pin: `fefdffda`; demo pin and lock `c69aba8a`); the demo's connections estate now runs with certs (`8535b44d`). Release reds fixed: `ensure_active` drove a Build failed between its reads (R-BI3, `86c2cdee`); a retried commanded cohort update conflicted with its own row (`ab7702e4`); cells: removed-primary check-in (`7a92ab09`), demo no-certs span (`88b3f895`), takeover handback wait (`bfea13d6`), hung-primary R-EL1 timing (`0a10139b`), refused-terminate later Build (`28df61d0`). Releases green at `bfea13d6`, `0a10139b` and `2191ffd7` (109 jobs once i143-2805 joined the gate). Export run at `0a10139b`: 59/65; its six refusals fixed: bootstrap_pending (`91ca6aac`, now gated), container address per birth (`1fbebd17`, a product fix), cutback second gateway (`7e9046df`), control-loss canon (`2191ffd7`); at `2191ffd7`: 64/65, the delta-gap cell's reader check (`5566b547`). Open: the R-FM1 recovery race (the re-born admin's row takes the name before the old birth's Deleted row, `name-held`, release at `28df61d0`), with the dev; then the release, the export run, `--soaks` and `export-manifest` at one SHA.
4. **Chaos, second pass.** The demo build again at the i145 SHA, then one release run; its reds read and fixed the same way, until green.
5. **The tail, the lead's.** The fast gate and the release suite green on one SHA → e10.s2 (the import manifest, the parity scan, the export gate) → e11.s12 (the seeded soaks and the container proof, with the dev). **Done 2026-10-20 at `abc1ec15`** (`export-manifest` eligible=true).
6. **Then rafka-v2: i141, then i142.** i141 imports the exported SHA (the pin e10.s2 names, once) and binds Rafka's adapters into it; i142 then migrates the product protocols on it: what the pin breaks (the removed `RunningNode::drain` and the linger helpers, the `epoch` → `revision` rename), the entry pull onto node.topology.get, and i145 e4. Nothing of i141 runs before step 5 ends.


**Initiative:** `i143`  
**Repository under build:** `drlukeangel/rust-distributed-mesh`  
**Status:** implementation-grade, final architecture reconciliation 2026-10-05  
**Milestone:** 59 - i143 Node RPC + generic Mesh product on RDM  
**Target architecture (rafka-v2):** `docs/architecture/connections.md`, `docs/architecture/build.md`, `docs/architecture/deployment-provider.md`, `docs/architecture/fabric-node-lifecycle-elections.md`, `docs/architecture/gossip.md`  
**Parity ledger:** [`transport-parity.md`](transport-parity.md)  
**Downstream import (rafka-v2):** `docs/plans/i141-node-rpc-substrate-prd.md`  
**Downstream migration (rafka-v2):** `docs/plans/i142-node-rpc-migrations-prd.md`

> This is the single binding i143 PRD. Former amendment files are folded here. The parity ledger remains separate because it records evidence, not alternate architecture.

---

<!-- section:0-purpose -->
## 0. Purpose

RDM is the implementation and proving ground for the generic Mesh product that Rafka imports. i143 must deliver one reusable generic control/runtime substrate, not a parallel Rafka-shaped implementation.

The exported boundary includes:

```text
logical Fabric / Mesh / Node identities
Iroh transport integration
hierarchical membership/topology
current RuntimeFact + required current runtime metadata per birth
current Fabric record + the current Build (read last in, first out)
Direct / Proxy connections
Node RPC contract/runtime/pool
Build / Reconciler
BuildStateAdapter contract
LeadershipResolver
DeploymentProvider / DeploymentPipeline
LifecycleTransitionPipeline
mesh investigation (a node-admin into a live mesh)
scenario/evidence/chaos support
```

No generic package imports a Rafka domain crate.

---

<!-- /section:0-purpose -->
<!-- section:1-locked-architecture-decisions -->
## 1. Locked architecture decisions

1. Build is the only generic topology mutation engine.
2. `rafka-node-admin-core` is the **single generic implementation owner** of Build reconciliation, leadership projection, executor/attempt rules, runtime adoption, generic Fabric/Mesh/Node lifecycle and recovery mechanics.
3. i141 binds Rafka policy and production storage/provider backends into that library. i142 consumes the resulting views/mechanisms and must not reimplement them.
4. There is one topology authority and one accepted topology:

```text
current fabric-primary = who may accept a topology change
the current Build      = the newest Build in builds.storage, read last in, first out, that is in progress or completed
```

5. RDM has no independent desired-topology authority beside the current Build. A Build is its own entity, written once; the Fabric row carries no pointer to the newest Build (at most the Build that created it, set once). Any internal shape cache is disposable derived state and has no revision, lineage, fork-winner or writer semantics.
6. Keep three distinct fact classes:

```text
the current Build (LIFO read) = accepted topology
membership + RuntimeFacts + current runtime metadata = exact current births/runtimes
builds.storage = each Build entity and the execution evidence of its attempts
```

7. Every change to the system's shape is its own Build with its own id: node.create, node.delete, the replace recipe, mesh.delete and fabric.delete. The current fabric-primary accepts it.
8. node.stop, node.start, node.restart, node recovery, authority movement and proven runtime drift leave the topology unchanged and open the next attempt of the current Build. An attempt lost with its executor leaves the Build in progress, and the next attempt resumes from its receipts; only a named terminal failure makes a Build failed. Reading last in, first out, the current Build is the first that is in progress or completed; a Build that failed before ever completing is skipped and only a caller's resubmit opens it again, and a Build that completed once stays current as completed when a later attempt of it fails (R-BI3 refinement, Luke 2026-10-12).
9. A second topology-changing request while the current Build is actively reconciling is refused by name with `409 build-in-progress`, naming that Build. RDM does not implement Build supersession/cancellation. Attempts are scoped per mesh (R-BI7, Luke 2026-10-12): the mesh-primary claims and runs the drift attempts of the current Build inside its own mesh on its own mesh's rows, without the fabric-primary; the fabric-primary claims and dispatches the attempts of a topology Build's operations; one attempt is in flight per mesh, and the refusal names the holder.
10. Build acceptance is crash-safe: the complete Build is durable in `builds.storage`, in its acceptance order, before any reader can read it as current. No second write publishes it.
11. Joining/restarting authority-bearing admins read the current Build before Ready. Older Build history is never replayed to reconstruct topology.
12. RDM owns the generic `fabric.storage`, `mesh.storage`, `nodes.storage`, `connections.storage` and `builds.storage` interfaces plus memory and durable file implementations used by restart/chaos proofs. `builds.storage` adopts the existing BuildStateAdapter memory/FileJournal implementation rather than rebuilding it.
13. Deployment provider is Fabric policy selected at first bootstrap; ordinary Build requests never choose it.
14. Every current managed runtime publishes a compact typed RuntimeFact bound to exact NodeId + IncarnationId + DeploymentId + provider + provider control domain + exact provider locator. A future eligible admin can adopt/manage the runtime without launcher-private Build receipts.
15. Provider/runtime bootstrap **produces or discovers** the exact provider locator. The node/runtime birth publishes the one normalized RuntimeFact through its own membership projection. There is no competing launcher-owned authoritative RuntimeFact stream.
16. Current operational metadata required by a successor, for example `data_dir`, is current Node/runtime projection state. It must converge to future authorities but is not part of RuntimeFact identity merely because it is needed for management.
17. The externally started Day-0 admin adopts/registers its own exact provider runtime and publishes the same RuntimeFact/current runtime metadata before authority-bearing Ready.
18. Process exact identity is provider-control-domain + PID + process-start token or stronger; PID alone is insufficient. Container identity is provider-control-domain + immutable container ID; deterministic container name alone is insufficient.
19. A provider locator is valid only inside its provider control domain. Current process/container proof is same-host/same-provider-domain. Future multi-host control requires either remotely controllable locators or domain-local exact-runtime execution. Provider locality never changes election priority.
20. Provider exact-handle terminal status can prove that exact runtime exited. RuntimeFact presence, provider-domain mismatch, RPC failure, failed dial, gossip silence, DNS disappearance, or partition cannot.
21. `LifecycleTransitionPipeline` owns Node/Mesh/Fabric transition policy and blocking hooks. `LifecycleTransition.transition_id` identifies one transition execution and keys hook receipts; Node/Mesh/Fabric status-control RPCs carry no transition id and are idempotent on their natural keys.
22. Normal joining/restarted admins do not self-Ready around missing control hydration. Day 0 and the explicitly fenced single-admin recovery-root are the only lifecycle roots that may establish Ready without an already-live Ready authority; election still runs afterward.
23. SN/MN/MM are minimum proof rungs, not caps.
24. Mesh creation is a Build. There is no mesh recovery: a mesh is alive while it holds a node, and a mesh with no node-admin gets one through mesh investigation, on the same Build and its own MeshId. Replace is Rafka's recipe of node primitives, submitted as one Build.
25. Fabric/Mesh views expose current owning node-admin control endpoints; tests never depend on hidden maps.
26. Node RPC certainty commits only after complete request send + request-direction finish. Incomplete send -> 499/NotSent.
27. Direct/Proxy connection truth is imported from the i66.e3 parity source; RDM does not create a second carrier-evidence or reconnect authority.
28. Domain routing selects WHICH final node. Connections selects HOW to reach it. Node RPC executes it.
29. Gossip/iroh-gossip owns current membership/status/runtime dissemination and repair. Explicit status/control Node RPC directly notifies the semantic authority. Neither plane proves, approves, attests or confirms the other.
30. Partition repair is **held-member stale-coverage repair**, not `neighbor_count == 0` as the only trigger.
31. Iroh owns authenticated transport identity and QUIC. Logical product identity is separate.
32. Logical `NodeId`, `MeshId`, `FabricId` are bare 12-character lowercase Crockford Base32 values carrying 60 random bits.
33. Only `NodeId` is an election ordering key. `MeshId` and `FabricId` are equality-only identities.
34. Iroh `EndpointId` is transport identity. It must never be represented by logical `FabricId`.
35. Canonical leadership is Ready candidates -> lowest complete NodeId. Ready is the only election eligibility bit.
36. For node-admins, Ready means authority-capable hydration is complete: the current Build, RuntimeFacts + required runtime metadata for held births it may have to manage, and provider-control-domain reachability or a valid domain-local execution path. These facts are readiness prerequisites, not election scoring.
37. Product chaos remains downstream i87; i143 proves generic deterministic + real process/container fault behavior.
38. Transitional Rafka legacy tags exist only as downstream i141/i142 adapters. Generic RDM has no Rafka legacy handlers.

---

<!-- /section:1-locked-architecture-decisions -->
<!-- section:2-substrate-ownership -->
## 2. Substrate ownership

```text
Iroh
    EndpointId authentication
    QUIC/path mechanics
    ALPN/Router multiplexing
    configured address lookup

iroh-gossip
    HyParView topic membership
    Plumtree eager/lazy dissemination and repair

RDM
    logical Fabric/Mesh/Node model
    current RuntimeFact + runtime-metadata projection
    current Fabric record + accepted Build
    topology and endpoint projection
    runtime incarnation + endpoint freshness
    Direct/Proxy effective route model
    Node RPC + semantic pool
    Build/Reconciler + drift trigger
    LeadershipResolver
    lifecycle/status certainty
    deployment/runtime adoption + provider-domain routing + proof
    recovery policy and proof
```

One Iroh Endpoint per process is preferred, multiplexing Node RPC + gossip. Exceptions require a named Iroh limitation and proof.

Canonical address authority is explicit product topology/EndpointAddr plus explicit gossip seeds. Optional mDNS is discovery only. No hidden N0 DNS/Pkarr authority is allowed in the canonical path.

The Node RPC semantic pool key includes peer EndpointId, runtime incarnation, endpoint slot, and slot freshness. EndpointId is not FabricId.

---

<!-- /section:2-substrate-ownership -->
<!-- section:3-gap-build-map -->
## 3. Gap/build map

| gap | requirement | owner |
|---|---|---|
| R1 | Node RPC contract/runtime | e5/e6 |
| R2 | Mesh identity/topology/reachability/freshness | e4 |
| R3 | Direct/Proxy connections parity | e0/e4/e6 |
| R4 | fabric-primary-only complete-Build topology mutation; each Build its own entity, the current one read last in, first out | e1/e3 |
| R5 | builds.storage (existing BuildStateAdapter) + fabric/mesh/nodes/connections.storage | e1/e3 |
| R6 | DeploymentProvider/Pipeline + exact runtime proof | e2 |
| R7 | LifecycleTransitionPipeline | e3/e4/e6 |
| R8 | canonical product IDs + transport identity separation | e4/e10 |
| R9 | hierarchical membership/backbone/forwarding | e4 |
| R10 | held-member partition repair | e4 |
| R11 | canonical deterministic leadership | e4 |
| R12 | definitive Node/Mesh/Fabric status certainty | e4/e6 |
| R13 | create, node recovery, the replace recipe; authority != executor | e4 |
| R14 | semantic target -> route -> Node RPC composition | e6 |
| R15 | iroh-gossip nonblocking send + immutable pin | e4 |
| R16 | one-process Endpoint/address boundary | e4/e6/e10 |
| R17 | stateful proof/evidence/scenario package | e7/e8 |
| R18 | load/soak reconciliation | e9 |
| R19 | machine-readable import/export gate, including no downstream duplicate control algorithms | e10 |
| R20 | current RuntimeFact publication + runtime-metadata convergence + successor adoption + provider control-domain semantics independent of Build history | e4/e2/e10 |
| R21 | current-Build read (LIFO) at bootstrap/catch-up + same-Build attempts for drift, restart and recovery | e1/e4/e10 |

---

<!-- /section:3-gap-build-map -->
<!-- section:4-package-architecture -->
## 4. Package architecture

```text
crates/rafka-node-rpc-contract/
crates/rafka-mesh-entity/
crates/rafka-mesh-transport/
crates/rafka-node-rpc/
crates/rafka-node-admin-core/
crates/rafka-node-admin-client/
crates/rafka-test-scenario/
crates/rafka-node-rpc-testkit/
crates/rafka-chaos/
```

`rafka-node-admin-core` exports the generic control brain. The concrete public API names may vary, but the semantic surface must let i141 bind:

```text
Fabric record read/write + the current-Build read (last in, first out)
fabric/mesh/nodes/connections/builds storage interfaces
complete Build acceptance + reconciliation + same-Build drift attempts
LeadershipView / resolver
Build attempt/executor rules
DeploymentProvider including current/published-runtime adoption + provider-domain execution
current RuntimeFact + runtime-metadata projections
Lifecycle transition hooks/status certainty
public Fabric/Mesh/Node projections
```

A downstream wrapper may add policy/evidence but may not reimplement the generic algorithm.

---

<!-- /section:4-package-architecture -->
<!-- section:5-accepted-topology-build-deployment-exact-runtime-proof-lifecycle -->
## 5. Accepted topology, Build, deployment, exact runtime proof, lifecycle

<!-- section:5-1-accepted-topology -->
### 5.1 Accepted topology

The current Build is the Fabric's accepted topology. Each Build is its own entity in `builds.storage`, written once in acceptance order.
A reader reads the Builds last in, first out, and the current Build is the first that is in progress or completed. There is no second
desired-topology record, revision, lineage, fork winner or pointer.

```text
current fabric-primary accepts a change to the shape
  -> compile the current Build's complete topology + the requested change
  -> persist the complete new Build B through builds.storage, in acceptance order
  -> B is accepted, and is the current Build while it is in progress or once it completes
```

Only the current fabric-primary may accept a topology-changing request. A non-primary refuses by name, naming the current
fabric-primary. Persisted copies are not writers.

Joining/restarting admins hydrate:

```text
Fabric record { id, name, path, state, status }
  -> the current Build (LIFO read)
  -> current membership/runtime facts
  -> authority-bearing Ready
```

Older Build history is not replayed to reconstruct topology.

Every change to the shape is its own Build: node.create, node.delete, the replace recipe (node.create of `<name>.new`, stop, update,
start, delete of `<name>.old`), mesh.delete and fabric.delete. node.stop, node.start, node.restart, node recovery, authority movement and
proven runtime drift change no shape and open the next attempt of the current Build.

While the current Build is in progress, another topology-changing request is refused by name: build.create answers
`BuildInProgress { current_build_id }`. RDM does not implement Build supersession/cancellation.

Storage ownership is generic RDM infrastructure:

```text
fabric.storage
mesh.storage
nodes.storage
connections.storage
builds.storage
```

RDM supplies the interfaces plus memory and durable file implementations for restart/chaos proof. Every interface is awaited
(`async_trait`, as `builds.storage` always was): a production backend reads and writes a durable medium, so nothing in node-admin holds
a lock or a sync callback across a storage call; the entry answer, which carries the Fabric record, is awaited too. `builds.storage` is
the existing BuildStateAdapter contract and its memory/FileJournal implementations adopted under the storage name, plus the acceptance
order the LIFO read uses. Rafka later binds production backends.

`fabric.storage` holds the Fabric record: id, name, path, state, status, and at most the Build that created the fabric (set once). It
holds no pointer to the newest Build.

Every admin persists the current Fabric record it learns. Replication preserves state across authority loss; it does not grant write
authority.

<!-- /section:5-1-accepted-topology -->
<!-- section:5-2-build-id-and-attempt-semantics -->
### 5.2 Build-id and attempt semantics

```text
a change to the shape
  -> new complete Build B, its own id
  -> persist B in acceptance order

Build B in progress + authority/executor movement, or an attempt lost with its executor
  -> B stays in progress
  -> the next attempt resumes from B's receipts; completed steps are replayed, never re-run

Build B completed + later proven runtime drift, restart or node recovery
  -> next attempt of B
  -> no new Build

Build B ends with a named terminal failure (Failed { step, reason })
  -> B is failed; the LIFO read skips it
  -> only a caller's resubmit opens its next attempt
```

A new Build id means the shape changed. Runtime drift, restart and node recovery mint no Build.

Attempts and receipts append to their Build. Evidence for drift carries `build_id=B`, `attempt=N`, `reason=proven-drift`; there is no
`reconcile_build_id`.

`build.delete` refuses by name the current Build (the first in progress or completed in the LIFO read). Older Builds become deletable
history once a newer Build is current.

<!-- /section:5-2-build-id-and-attempt-semantics -->
<!-- section:5-3-runtimefact-provider-current-runtime-metadata -->
### 5.3 RuntimeFact / provider / current runtime metadata

Conceptually:

```text
RuntimeFact {
  node_id
  incarnation_id
  deployment_id
  provider
  provider_control_domain
  locator
}
```

RuntimeFact is compact current membership/topology **identity** state. It is immutable for one incarnation and sufficient to identify where/how an eligible successor can adopt/control that exact runtime. It contains no provider secret/control credential.

Do not conflate locator acquisition with publication:

```text
provider / runtime bootstrap seam
  -> produces or discovers exact provider locator + provider_control_domain

node / runtime birth
  -> publishes ONE normalized RuntimeFact through its own membership projection
```

There is no launcher-owned competing authoritative RuntimeFact stream.

Examples:

```text
process
  runtime may self-discover PID + process-start token

container
  provider learns immutable container ID after docker run
  provider bootstrap record/channel makes it available to child before Ready

Day 0
  externally started admin adopts/self-discovers exact runtime before Ready
```

Operational metadata required by a successor is current Node/runtime projection state, not necessarily identity:

```text
RuntimeFact
  = exact runtime identity/fence

current runtime metadata
  = operational context needed to manage/restart the birth
  = e.g. data_dir / resolved storage or executable context where required
```

Required runtime metadata must converge to successor admins and cannot remain launcher-private. `data_dir` does not join RuntimeFact equality/fencing merely because it is needed by restart.

Exact locator semantics:

```text
process:
  provider_control_domain + PID + process-start identity/token

container:
  provider_control_domain + immutable container ID

future provider:
  provider_control_domain + equivalent non-reusable locator
```

Current RDM process/container proof is local to one provider control domain:

```text
process provider   -> one host/process namespace
container provider -> one reachable Docker daemon / host runtime
```

A PID/container ID from another provider control domain must be refused, not acted on. A future multi-host provider must either expose a remotely usable locator/control path or route/delegate exact-runtime actions to a valid executor/agent in that RuntimeFact's provider domain. Election authority remains NodeId-derived; provider locality changes readiness/execution routing only.

A managed process cannot receive the final PID in launch env because the PID does not exist until after spawn. The provider/pipeline owns the bootstrap/adoption seam that obtains/persists the final exact locator + provider domain and makes the matching fact available to the runtime before Ready.

Managed creation includes:

```text
DeployRuntime
RegisterExactRuntimeHandle
ResolveProviderControlDomain
MakeRuntimeFactAvailableToBirth
PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata
```

Day 0 performs:

```text
AdoptCurrentRuntime
RegisterExactRuntimeHandle
ResolveProviderControlDomain
PublishRuntimeFactAndCurrentRuntimeMetadata
```

A successor performs:

```text
current membership RuntimeFact + current runtime metadata
  -> resolve provider_control_domain
  -> provider adopt published runtime
     OR route to a valid domain-local executor
  -> inspect / drain / terminate / restart
```

The local launcher `handles` map may cache handles but is not authority.

A changed locator or provider control domain for the same NodeId+IncarnationId is a named inconsistency/refusal. Inability of one admin to control a nonlocal provider domain is not proof that the runtime is Dead.

<!-- /section:5-3-runtimefact-provider-current-runtime-metadata -->
<!-- section:5-4-lifecycle-and-ready-root-guard -->
### 5.4 Lifecycle and Ready root guard

Lifecycle transitions are explicit operations with blocking hooks. `LifecycleTransition.transition_id` identifies one lifecycle transition execution and keys once-only hook receipts. Node/Mesh/Fabric status-control RPCs do not carry that transition id; they are naturally idempotent on Node `(NodeId, IncarnationId, status)`, Mesh `(MeshId, status)`, and Fabric `(FabricId, event)`. Required semantic cuts include pre-eligibility, post-eligibility/pre-commit, after-transition, before-drain, after-drain.

Node-admin Ready is authority-capable readiness. It may not commit until the current Fabric record + the current Build (LIFO read), RuntimeFacts + required current runtime metadata for held births it may need to manage, and provider-control-domain reachability or a valid domain-local execution path are hydrated.

An ordinary joining/restarted admin may not self-Ready because hydration is unavailable. Only:

```text
Day 0
  no prior Fabric control authority exists
  bootstrap current control state and first Ready through Day-0 lifecycle

single-admin recovery root
  fenced rule proves no live Ready admin exists
  recovery-root self-apply grants Ready only
```

may establish Ready without an existing Ready authority. Neither root assigns leadership; the seat rule runs after Ready. A node-admin created into a live mesh by mesh investigation (lifecycles/mesh-investigation.md) does not use the recovery-root shortcut. The recovery root is fabric recovery (lifecycles/fabric-recovery.md), including the case where nothing is alive, which reuses the fabric id and MeshIds.

<!-- /section:5-4-lifecycle-and-ready-root-guard -->
<!-- section:5-5-fabric-shutdown -->
### 5.5 Fabric delete

`fabric.delete` is a workflow the fabric-primary runs as one Build (lifecycles/fabric-delete.md). It removes every mesh, leaf-inward,
the fabric-primary's own mesh last:

```text
fabric.delete
  > mesh.delete          each mesh, the fabric-primary's own mesh last; on its own mesh the seat stays put (no other
                         mesh is left to take it), so that mesh.delete skips its node.fabric-primary.update step
                         a mesh-primary that does not answer: reached through mesh investigation, whose outcome
                         during fabric.delete is the delete (a node-admin that answers runs mesh.delete; else the
                         fabric-primary tickles each remaining node and runs node.delete for each one it reaches)
  > node.delete          each other mesh's mesh-primary, after its mesh.deleted; the fabric-primary executes it and
                         proves its exit
    < fabric.deleted     reported by the fabric-primary, still alive
then a separate call from outside the fabric:
node.delete              the fabric-primary; node.deleted is its last reply and the caller takes its word
```

Every runtime the workflow deletes has exact provider exit proof, except the fabric-primary's own, which nobody is left to prove. A
seat moves only by its holder's yield, a committed transfer or a proven loss, so fabric.delete moves no seat. A local OS signal or a
node stop is node lifecycle and starts no fabric.delete.

---

<!-- /section:5-5-fabric-shutdown -->
<!-- /section:5-accepted-topology-build-deployment-exact-runtime-proof-lifecycle -->
<!-- section:6-hierarchical-topology-repair-ids-elections-recovery -->
## 6. Hierarchical topology, repair, IDs, elections, recovery

<!-- section:6-1-topology -->
### 6.1 Topology

```text
ordinary member
    -> own mesh membership channel

mesh primary
    -> own mesh aggregate on node-admin backbone

receiving peer mesh primary
    -> forward remote aggregate onto own mesh channel
```

Ordinary members never join the backbone. Only node-admins do. Publisher responsibility follows derived leadership. Current membership convergence carries the current RuntimeFact + required current runtime metadata for each birth.

<!-- /section:6-1-topology -->
<!-- section:6-2-held-member-partition-repair -->
### 6.2 Held-member partition repair

```text
held current member
+ known EndpointId/address
+ stale/missing coverage for repair window
    -> bounded join/refeed attempt
```

Accepted topology alone is not dial material. Failed dial is not death proof. Retries are bounded/backed off per peer/window. Exact-runtime/lifecycle retirement removes the peer from repair targets. Zero-neighbour repair may remain fallback. Relearned membership restores RuntimeFact/current runtime metadata.

<!-- /section:6-2-held-member-partition-repair -->
<!-- section:6-3-product-ids -->
### 6.3 Product IDs

```text
NodeId
MeshId
FabricId
format = 12 lowercase Crockford chars
alphabet = 0123456789abcdefghjkmnpqrstvwxyz
payload = 60 random bits
```

One shared primitive backs typed wrappers. BuildId, IncarnationId, DeploymentId, transition IDs, provider handles/control-domain values, EndpointId, and freshness tokens are not converted merely because they contain `Id` or identify something.

Current RDM transport identity formerly named `FabricId` must be renamed/retyped, preferably to Iroh EndpointId directly.

<!-- /section:6-3-product-ids -->
<!-- section:6-4-election-canon -->
### 6.4 Election canon

```text
eligible = Ready candidates
winner   = lowest complete canonical NodeId
```

Not election inputs:

```text
ready_since_ms
ordinal
mesh name
MeshId
FabricId
creation time
incarnation
deployment id
RuntimeFact/provider locator/provider_control_domain
current runtime metadata
the current Build
EndpointId
endpoint freshness
incumbency
```

Ownership:
1. node-admin owns each `(mesh, kind)` election;
2. `(mesh,node_admin)` winner is the mesh primary, same seat;
3. only mesh primaries own fabric-primary election;
4. fabric candidates are mesh-primary nodes and use the same comparator.

Day 0 naturally produces node-admin primary == mesh primary == fabric primary.

A living holder keeps its seat; a lower-NodeId candidate that later becomes Ready never displaces it. Silence, an observer-inferred Dead or an unheard route is not proof. A holder that yields, or whose exact birth is proven gone, hands the seat to the lowest Ready NodeId of its own mesh. Restart preserves NodeId; replacement mints a new NodeId (fabric-node-lifecycle-elections.md §5).

`is_primary`/`is_fabric_primary` are derived projections. Gossiping a claimed winner does not make it authoritative.

<!-- /section:6-4-election-canon -->
<!-- section:6-5-election-evidence -->
### 6.5 Election evidence

Required spans include:

```text
rafka.mesh.election.resolve.via-recompute
  election_level="node_type"
  mesh, kind, candidate_count, eligible_count
  winner_node_id, winner_path, previous_node_id
  election_key="node_id_crockford"

rafka.mesh.election.resolve.via-mesh-primary
  election_level="mesh_primary"
  mesh, winner_node_id, winner_path, previous_node_id
  source_kind="node_admin"

rafka.mesh.election.resolve.via-fabric-recompute
  election_level="fabric_primary"
  fabric, fabric_id, candidate_mesh_primaries
  winner_node_id, winner_path, winner_mesh, previous_node_id
  election_key="node_id_crockford"
```

The mesh-primary span is a role projection, not a second election.

<!-- /section:6-5-election-evidence -->
<!-- section:6-6-new-mesh-recovery-ordering -->
### 6.6 A mesh that loses its node-admins

There is no mesh recovery. A mesh is alive while it holds a node; a crash is not leaving. Only its mesh-primary leaves a mesh, always
last, at the end of mesh.delete; the mesh is then dead and never restarted (R-ML2). A dead mesh's name used again is mesh.create of a
new mesh with a new MeshId.

A peer mesh the fabric-primary stops hearing is mesh investigation (lifecycles/mesh-investigation.md), owned by the fabric-primary and
timed on elapsed time since the last direct backbone receipt:
- 20 s: probe 1, the exact mesh-primary first, then the other node-admins, each direct then via a peer;
- 30 s: the silent mark;
- 40 s: probe 2;
- an admin answer or a backbone receipt stops it;
- 60 s, the decision step, in two cases only:
  - members live: a carrier-edge-lost verdict;
  - fully crashed: the provider's exact exit proof of the mesh's node-admins, never silence.
  Either case runs node.create of a node-admin into the mesh, in process, on the same accepted Build and the mesh's own MeshId.
  Otherwise it holds.
- When a mesh is given a new node-admin, both the mesh and the fabric carry degraded until the new mesh-primary's report clears them
  (R-DG1).
- The created node-admin joins as the mesh's successor primary, adopts the mesh's held state, and re-gossips once established.
  Nobody publishes a mesh on its behalf (R-RX4). It brings back unreached members through node recovery.

**Authority != executor:** the fabric-primary owns the investigation and the accepted topology. The created node-admin is the local
executor of its own mesh's work.

Provider locality is another execution boundary, not an authority boundary. If a RuntimeFact belongs to another provider control
domain, the current authority/local Build executor must use the provider's remote-control path or delegate that exact runtime action to
a valid domain-local executor. It must not infer Dead because a PID/container ID is nonlocal.

Failed RPC/dial/silence/provider-domain mismatch never authorizes a delete. A delete requires exact provider exit proof.

---

<!-- /section:6-6-new-mesh-recovery-ordering -->
<!-- /section:6-hierarchical-topology-repair-ids-elections-recovery -->
<!-- section:7-definitive-state-over-short-node-rpc -->
## 7. Definitive state over short Node RPC

Gossip remains the dissemination/topology plane. Short exact-target Node RPC adds certainty only for explicit Node/Mesh/Fabric lifecycle/control events.

Canonical authority chain:

```text
ordinary node        -> elected mesh primary       DeclareNodeState
node-admin            -> fabric primary             DeclareNodeState
elected mesh primary -> fabric primary             DeclareMeshState
fabric primary        -> bootstrap node-admin       ApplyMeshState(Pending)
fabric primary        -> elected mesh primary       ApplyFabricEvent
```

Replies include:

```text
Applied
AlreadyApplied
RejectedStaleBirth
RejectedNotAuthority
RejectedInvalidTransition
NotSent
Indeterminate
```

Pending-before-Build is hard: only `Applied|AlreadyApplied` permits reconciliation to start. A retry resends the same naturally-idempotent operation with exact target/birth fencing. Status/control natural keys are Node `(NodeId, IncarnationId, status)`, Mesh `(MeshId, status)`, and Fabric `(FabricId, event)`; these calls carry no transition id.

After durable cutover, `Applied` means the authoritative write ACKed.

Single-admin restart may use the defined recovery-root Ready self-apply only if no live Ready admin exists. It grants Ready only; election still runs. Ordinary joining admins never use that as a fallback for failed hydration.

---

<!-- /section:7-definitive-state-over-short-node-rpc -->
<!-- section:8-semantic-target-connections-node-rpc -->
## 8. Semantic target -> connections -> Node RPC

```text
domain selector chooses ExactNode(target)
  -> connections effective_route(target)
      Direct | ViaPeer(exact carrier) | NoActiveRoute
  -> Node RPC executes exactly target
```

Connections never reselects a semantic destination. Node RPC never owns Proxy lifecycle. A carrier performs exactly one direct inner invocation to the original target.

---

<!-- /section:8-semantic-target-connections-node-rpc -->
<!-- section:9-node-rpc-runtime-pool-certainty -->
## 9. Node RPC runtime/pool/certainty

Generic proof ops may remain simple stateful test operations. Production contract requirements are:
- stable protocol catalog and typed unknown variant handling;
- exact final target preservation;
- complete-send commit cut;
- incomplete send -> 499/NotSent;
- post-commit reply ambiguity -> Indeterminate;
- no automatic replay of mutating Indeterminate;
- semantic pool keyed by process identity/incarnation and exact endpoint-slot freshness;
- stale slot cancellation does not evict valid sibling slots.

---

<!-- /section:9-node-rpc-runtime-pool-certainty -->
<!-- section:10-proof-shapes-and-mandatory-scenarios -->
## 10. Proof shapes and mandatory scenarios

Minimum generic rungs:

```text
SN = 1 node-admin + 1 ordinary RPC node
MN = 2 node-admins + 3 ordinary RPC nodes
MM = two MN meshes under one fabric
```

Mandatory proof includes:
- elastic grow/shrink;
- shape/resize assertions use accepted-Build per-cohort counts/settled shape rather than assuming surviving ordinals remain contiguous;
- identity/election/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
- no election test remembers an incumbent; expected winner is calculated from the current public eligible candidates;
- node restart and replacement;
- three-mesh election with names/ordinals/MeshIds deliberately shuffled;
- fabric-primary loss with proven runtime death;
- secondary admin-cohort loss with ordinary survivors;
- partial member survival;
- all-member loss under same MeshId;
- three-mesh cascading authority loss;
- partition/heal where both halves retain neighbours and held-member repair rejoins them;
- Pending bootstrap executor creates missing sibling admins only in its own mesh;
- Day-0 admin exact runtime adoption/death proof;
- Build-launched process/container produces/discovers exact locator and the runtime publishes the normalized RuntimeFact;
- non-launcher admin learns `data_dir`/required current runtime metadata without asking the original launcher;
- an admin born **after an older Build completed** reads the current Build (LIFO) + RuntimeFacts/runtime metadata + provider-domain capability, becomes Ready, and, once it holds a seat, manages older live runtimes without completed Build receipts; a lower NodeId never displaces a living seat holder;
- older Build history is forgotten, the current Build (LIFO read) remains readable, and later exact runtime death opens a new attempt of that same Build;
- PID reuse/container-name reuse cannot impersonate a previous exact runtime;
- process successor in the same host control domain can manage a non-launcher exact process runtime, while a PID from another control domain is refused;
- container successor sharing the same Docker control domain can manage a non-launcher immutable container ID, while a locator from another domain is refused;
- any multi-host proof demonstrates remotely controllable locators or domain-local exact-runtime execution without changing NodeId election ordering;
- ordinary joining admin cannot self-Ready around missing control hydration; only Day-0/fenced single-admin recovery-root semantics are allowed.

Blackbox tests calculate expected election winners independently from public NodeIds/status, then compare public views + OTLP.

---

<!-- /section:10-proof-shapes-and-mandatory-scenarios -->
<!-- section:11-gossip-substrate-gates -->
## 11. Gossip substrate gates

<!-- section:11-1-nonblocking-iroh-gossip-send -->
### 11.1 Nonblocking iroh-gossip send

The actor must never await capacity on one peer's bounded queue. Active/full peer enqueue drops that enqueue and continues; ordinary Plumtree repair owns convergence. Closed queue preserves cleanup.

The reciprocal queue-saturation regression is export evidence. No RDM resend/throttle workaround substitutes for the library fix.

<!-- /section:11-1-nonblocking-iroh-gossip-send -->
<!-- section:11-2-address-repair-boundary -->
### 11.2 Address/repair boundary

Canonical gossip bootstrap and rejoin use explicit EndpointId/address knowledge from product topology/seeds. Hidden N0 lookup is not repair authority.

Application repair uses held-member stale coverage as described in §6.2. Do not add another peer scheduler on top of iroh-gossip.

RuntimeFact is compact current-birth identity state and rides ordinary membership convergence. Required runtime-management metadata may ride beside it in the current birth/Node projection. Do not add a completed Build-receipt archive to the Build channel; the 4032-byte gossip frame budget remains a hard boundary.

---

<!-- /section:11-2-address-repair-boundary -->
<!-- /section:11-gossip-substrate-gates -->
<!-- section:12-chaos-and-observability -->
## 12. Chaos and observability

Layers:

```text
model/action + shrink
explicit failpoints
deterministic network/time where practical
real process/container fault backend
seeded soak
```

Wedge cuts include request pre-send/post-apply, Build/deployment stall, Build acceptance-order persistence and the current-Build read, build-in-progress refusal, RuntimeFact production/publication/adoption/provider-domain stall, current runtime-metadata hydration stall, lifecycle hook stall, Pending-before-Build stall, election authority movement, stale-slot race, partition/heal, exact-runtime exit, fabric.delete (including a mesh whose primary does not answer), and recovery authority/executor handoff.

Every E2E produces blackbox state plus OTLP artifacts. Fault injection itself is never the success criterion.

<!-- section:12-1-seeded-soak -->
### 12.1 Seeded soak (export bar)

One deterministic endurance gate, not a second scenario suite:

```text
shape:     multi-Mesh estate; enough admins for real mesh-primary and fabric-primary movement;
           ordinary rpc nodes in every Mesh
duration:  30 minutes
seed:      fixed, printed in the evidence, rerunnable exactly
faults, repeatedly:
    node-admin restart          ordinary node restart
    same-path replacement       exact runtime kill
    mesh-primary loss           fabric-primary loss
    temporary partition + heal  endpoint slot/freshness supersession
```

It passes on outcomes, never on faults detected:

- no hang or deadlock;
- no two current births for one path;
- no two accepted topology writers;
- no duplicate drift attempt opened for one proven loss;
- no Build minted for unchanged-topology drift;
- no stale-slot application dispatch;
- no permanent split after a partition heals;
- no loss of the current Build;
- no unexplained orphan attempt or claim;
- the final accepted topology converges, and every surviving expected path is reachable and current.

A named refusal the architecture calls for is a pass. An unexplained timeout, silent drop, duplicate authority action or non-convergence fails the soak.

<!-- /section:12-1-seeded-soak -->
<!-- section:12-2-container-proof -->
### 12.2 Container proof (export bar)

The named container cells, not the whole scenario gate rerun under `MESH_SPAWN_TYPE=container`:

- exact container identity is the provider control domain plus the immutable container ID;
- a successor in the same provider control domain adopts, inspects and controls the exact container;
- an authority in another provider control domain never treats the locator as local: it refuses by name or delegates to a domain-local executor;
- a real container kill: exact provider inspection proves it terminal, that is the death evidence, and recovery proceeds. This cell runs through the Build and runtime-adoption path, not the provider alone.

---

<!-- /section:12-2-container-proof -->
<!-- /section:12-chaos-and-observability -->
<!-- section:13-story-ordering-and-late-gates -->
## 13. Story ordering and late gates

Remaining work and its order: the open builder blocks below (each names what it waits on) and §13.3's rollout steps.

<!-- section:13-1-e11-the-integration-proof-of-concept-before-i141 -->
### 13.1 e11 — the integration proof-of-concept, before i141

The export gate proved the substrate on RDM's own proof nodes. Before i141 imports it into Rafka, this
epic proves the import pattern itself, in RDM, with RDM's own role binaries: the packaging, a
`rafka-node-base` that imports the packages the way Rafka's node-base will, the role binaries
(`broker`, `gateway`, `compute`, `registry`) built on that base, and one small test per Node RPC
feature Rafka extends. Each story is its test and its implementation together; a feature without a
cell is not integrated.

```text
e11.s1  packaging: the import manifest is the eight packages at one rev; `rafka-node-base` depends on
        them by workspace path exactly as rafka-v2 depends on them by git+rev; the dep-rules audit
        holds the import boundary (no RDM package imports node-base)
e11.s2  node-base imports: one Iroh Endpoint per process carrying the mesh, gossip and Node RPC ALPNs;
        one sealed effective catalog (core ping + the product's transitional adapters); one
        NodeRpcServer accepting on the Node RPC ALPN; one LiveNodeResolver fed by membership; one
        NodeRpcClient
        cells: a role process serves core ping on its one endpoint; a tag the catalog does not hold
        is 421; a product adapter is catalogued and unserved on the Node RPC ALPN
e11.s3  role binaries on the base: broker/gateway/compute/registry build on node-base, born by
        node-admin as NodeKind::{Broker,Gateway,Compute} path.names (`mesh1.broker.1`), publishing
        RuntimeFact and the kind's status like any rpc node
        cell: a Build of {node_admin:1, broker:1, gateway:1, compute:1} converges; each birth's ready
        span names its kind; a second Build removes the compute and the path is retired
e11.s4  leadership, lite: every role cohort's seat is the one election (`rafka-node-admin-core`
        `election.rs`, lowest Ready NodeId per (mesh, kind) cohort); a product reads a
        cohort's primary from node-admin's public view (`rafka-node-admin-client` `nodes()`,
        `is_primary`) and never computes it
        cell: kill the broker primary; the next-lowest broker is announced and is the view's primary;
        node-base holds no comparator (a cell reads its own source)
e11.s5  exact target + fence: a gateway calls a broker by ExactNode and by CurrentPath through its
        one client; after the broker restarts (same NodeId, new incarnation), the gateway evicts
        the old incarnation's pooled connection and the next call reaches the new birth; a dial to
        a birth that moves before it is pooled is RejectedStale (425), never dispatched
        cell: `node_rpc__restart_fence` on the role shape (the gateway carries the probe's calls);
        the in-flight arm is `rafka-node-rpc/tests/pool.rs`
e11.s6  certainty: a request cut before FIN is NotSent (499) and never applied; a reply lost after
        commit is Indeterminate; a handler fault is 423; the reply budget alone is not death proof
        cell: the four outcomes on a gateway->broker call over the gateway's own client, the
        broker staged by the proof family's keys (`proof:hang`, `proof:fault`), each asserted on
        its typed reason (`rafka-node-base/tests/role_process.rs`)
e11.s7  routing/proxy, lite: a gateway that cannot reach a broker directly invokes it ViaPeer through
        another gateway; the carrier makes exactly one inner invocation and never changes the final
        target; NoActiveRoute starts no leg
        cell: Direct, ViaPeer and NoActiveRoute, each on the role shape, with the carrier's
        `rafka.node_rpc.request.serve.via-forward` span
e11.s8  product family: a product-owned unary family (`broker_data`, forwardable) composed beside the
        core, ledgered under the product, served only by the broker and carried by every other role;
        a call to a compute for it is 421
        cell: the family round-trips to a broker and is unserved at a compute
e11.s9  observability pass-through: caller_system and W3C traceparent/tracestate/baggage ride the
        request header; a malformed value is dropped (`via-context-dropped`) and the outcome is
        unchanged; a carrier hands the original context through verbatim, so the broker's serve
        span and the gateway's carried invocation both descend from the caller's span in Jaeger
        cell: one call, one trace, three services (`node_rpc__role_context`); the malformed arm on
        the gateway's own client (`rafka-node-base/tests/role_process.rs`)
e11.s10 status/lifecycle on a role: a broker declares Ready to its mesh primary over the
        status op; a wedged broker is marked pending-reconnect at the floor, tickled (one connect,
        direct then via a peer) and marked dead; its runtime still runs, so it is held, never
        replaced; thawed, the same birth is back ready
        cell: `mesh_runtime__role_wedge`
e11.s11 what the import found: a product's durable backend is async, so the four storage
        interfaces (fabric/mesh/nodes/connections) are awaited like builds.storage, and the entry
        answer (which carries the Fabric record) is awaited; RDM's memory and file backends are
        unchanged in behavior
        cells: the existing storage, shutdown and restart cells on the awaited interfaces; the
        gate unchanged
```

Shape for every cell: one mesh, one node-admin, and the roles the cell names; the process provider.
Nothing of Rafka's domain (orgs, topics, writers) enters: the families are proof families. The epic is
done when the gate runs every cell green and the import manifest is unchanged by it.


<!-- /section:13-1-e11-the-integration-proof-of-concept-before-i141 -->
<!-- section:13-2-e12-conformance-to-the-approved-lifecycle-spec -->
### 13.2 e12 — conformance to the approved lifecycle spec (epic #2991)

Each story is its red cell, its fix and its Jaeger flame from a live mesh, in that order. The design
of record is the document each row names; a story with no document is not seated.
Every story's acceptance includes: its red cell first; the named document's sequence holding in a deterministic
cell with its trace; the affected stems green once at gate cadence; mesh-audit and the gate build set green; a
gossip or mesh change carries a Jaeger flame from a live mesh; it lands only through the land script.

```text
e12.s34 #3060  ops-naming step 1: stop parks, start rejoins, restart keeps the incarnation,
               NodeStopped from the mesh primary, delete with exit proof; R-FW1 ordered forward
               design: §13.3 step 1 (R-ON1, R-ON7); lifecycles/node-stop.md,
               node-start.md, node-restart.md, node-delete.md; gossip.md
e12.s35 #3061  Build path: Blocked framed only for a named Ready blocker; one topology Build in
               flight, refused BuildInProgress{current_build_id}, planned from the accepted topology
               design: node-rpc-envelope.md#build-op-0x20-rdm (R-ON8, one Build in flight); lifecycles/node-create.md
e12.s41 #3068  open: the builder block `e12.s41` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s41)
e12.s42 #3069  open: the builder block `e12.s42` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s42)
e12.s30 #3043  R-ON8 landing two: typed seat-move frame ends a Build stream when the
               fabric-primary seat moves mid-Build; acceptance test 5 (old FP dispatch Fenced)
               design: node-rpc-envelope.md#build-op-0x20-rdm (R-ON8); lifecycles/node-primary-update.md
               (op 0x21)
e12.s38 #3065  mesh leave 1/3: the fabric-primary seat is handed over (0x21) before a leave of
               its mesh; NewFabricPrimary on the backbone
               design: lifecycles/node-primary-update.md; gossip.md new-fabric-primary; R-ST2
e12.s40 #3067  mesh leave 3/3: peer meshes on mesh-leaving / mesh-left: routing hold (statuses
               unchanged), replay to joiners, Leaving/Dead external projection
               design: gossip.md "Mesh-admin hears mesh-leaving / mesh-left"; R-ST5; R-MD1
               (mesh-primary commits Dead at mesh.deleted before its handoff hook)
               (#2992 is split into s38-s40)
e12.s44 #3072  stopped-node garbage collector (R-GC1): stopped_node_ttl (RDM_STOPPED_NODE_TTL, blank = forever,
               RDM 30 min); the fabric-primary sweeps every ttl and submits one ordinary node.delete Build per
               node stopped longer than the ttl (age from the current stopped period's own stop receipt; births an
               open Build references skipped); the checks run again at the delete's admission
               design: node-identity-and-state.md §6.9; rulings 2026-10-11 dev page §R-GC1
e12.s45 #3073  one routing rule: application routing selects a birth only while it and its mesh are
               ready; anything else is a routing stop on receipt, never a barrier; a later
               ready restores selection; only application routing reads lifecycle; a leaving
               mesh is reported leaving by its own mesh-primary (R-MS1)
               design: lifecycles/states.md "Routing"; states.md:12 mesh states
e12.s46 #3074  open: the builder block `e12.s46` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s46)
e12.s47 #3075  open: the builder block `e12.s47` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s47)
e12.s50 #3079  generated docs site: mdBook + rafka-docgen (RDM tool, syn) writing wire / frames / statuses /
               spans / config reference pages into rafka-v2; generated SUMMARY from the lifecycle index; a gate
               regenerates and fails on a dirty diff; workflow step lists after the workflows-call-primitives move
               design: docs/plans/proposal/rdm-code-conformance.md §0.1; Luke 2026-10-11 "let's try mdbook"
e12.s51 #3081  delete timeout-strike pool eviction (pool.rs strikes, client.rs timed_out arm, the via-timeout-strikes
               span); the two pool tests flip to the ruled negative control
               design: node-rpc.md §21; node-rpc session rulings 2026-10-03 §4
e12.s52 #3082  R-HB1: the mesh-primary authors and publishes every member status through its own-mesh projection;
               a heartbeat is liveness only and is refused by name after node.stopped (beta set; gossip — flame at
               landing). Code owed: RDM `stopped_refuses` loses its heartbeat rejoin; `WireDigest` field 2
               `status: MemberStatus` becomes the liveness `node_state` (durable format moved, mesh-audit reseal)
               design: docs/architecture/gossip.md §3.1; lifecycles/states.md "Who sets each state";
               docs/plans/proposal/node-status-heartbeat-connections.md R-HB1
e12.s53 #3083  open: the builder block `e12.s53` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s53)
e12.s55 #3085  open: the builder block `e12.s55` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s55)
e12.s56 #3087  open: the builder block `e12.s56` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s56)
e12.s57 #3088  open: the builder block `e12.s57` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s57)
e12.s58 #3089  open: the builder block `e12.s58` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s58)
e12.s59 #3090  open: the builder block `e12.s59` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s59)
e12.s60 #3091  open: the builder block `e12.s60` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s60)
e12.s61 #3092  node.delete keeps storage
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s63 #3094  remove sender-identity gates inside the mesh; direction refusals stay
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s64 #3095  the RDM standalone store is written at state-commit, not from boot
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s65 #3096  one node.start workflow (workflows call primitives)
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s66 #3097  peer-mesh probe order: exact mesh-primary first, then the other admins, direct then via a peer
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s68 #3099  named typed refusals for an epoch overflow and a stale mesh-delete round
               design: docs/plans/proposal/rdm-code-conformance.md (row cited in the issue)
e12.s69 #3100  open: the builder block `e12.s69` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s69)
e12.s72 #3103  open: the builder block `e12.s72` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s72)
e12.s75 #3106  open: the builder block `e12.s75` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s75)
e12.s76 #3107  open: the builder block `e12.s76` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s76)
e12.s79 #3120  open: the builder block `e12.s79` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s79)
e12.s80 #3121  open: the builder block `e12.s80` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s80)
e12.s81 #3122  open: the builder block `e12.s81` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s81)
e12.s82 #3123  open: the builder block `e12.s82` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s82)
e12.s83 (issue to generate)  open: the builder block `e12.s83` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s83)
e12.s84 (issue to generate)  R-CY1: the canary red read and fixed (Root 1: cohort seat holder restart runs `node.primary.update` under `restart.attempt` ending `handed-off` and executes the restart steps under `reconcile.attempt = restart.attempt + 1`; RED 368d110a, landed 69752953, gate 39 s; trace `ef78ccae756293f65263737ffab36200`)
e12.s85 (issue to generate)  layout pre-step: `fence.rs` -> `seat_revision.rs`, `departed_status.rs` -> `deleted_status.rs`, and the 32 `i143_acceptance_*.rs` files renamed into their crates' test binaries with 100% discovery parity and 0 stem violations
e12.s86 (issue to generate)  R-FM1: hung runtime hold (`RDM_HUNG_HOLD_FLOORS`, default 10), `TerminateRuntime` first step of recovery Build, and `held-running` store fact (`NodeView`/`FabricView`, gossip wire untouched; RED 505d99eb, landed 549493fe, gate 59 s; traces `07f3b640645fb9621219d50fa3135604`, `6f2cee1c90c07a9a7ce03607ded20cdb`, `88f6435305841044a5468305d6c1f66e`, `ed394bca8f94b70380f4cf4ff249b2fa`)
e12.s87 (issue to generate)  R-FM2: gone vs broken inherited `data_dir` (`RDM_DATA_DIR_GONE_FLOORS`, default 2; `data-dir-lost` and `data-dir-unreadable` store facts + `node.get` fields, gossip wire untouched; RED 5068c55d; traces `3afc5ad0a17c48afa65aebd97fa550b4`, `b969d8fa8549ea89be5403ec5c232a8b`, `1d467f78873ef57f1d928f6e156e24d2`, `d7f84fcf6d32c35ceb719b6db6263a9c`, `3398e9d6a778824a3336aa83c29442ac`, `af93811d9ec896682b5c0ec249c0a3bd`, `2302862bb508b7b0584913877b8b491d`)
e12.s90 (issue to generate)  open: the builder block `e12.s90` below (R-PV1: the provider reports an OS-stopped runtime; os-stopped is a fact, never a hold or a terminate)
e12.s89 (issue to generate)  the R-shape release cells: nine red cells across the authority, lifecycle, routing and partition chaos-process jobs, each read from its own trace and fixed where it lives
e12.s49 #3078  mesh.delete ends at mesh.deleted: the fabric-primary's leave completes at the
               validated handoff; the final admin stays alive publishing MeshStatus Dead; its
               end is a separate node.delete (fabric-primary executor, exit proof, row Deleted,
               NodeDeleted departed); the step-5 shutdown of the final primary and the
               FinalExited resume arm are deleted; mesh-left frame unchanged until rollout 3/4
               design: ops-naming-proposal.md MESH/delete; R-NS1 items 1-3; R-MD1; R11
               after: s40 #3067 and s41 #3068 land (they hold the Dead writes)
e12.s23 #3016  open: the builder block `e12.s23` below (python3 scripts/prd.py brief docs/plans/i143-prd.md e12.s23)
```

<!-- /section:13-2-e12-conformance-to-the-approved-lifecycle-spec -->
<!-- section:13-3-ops-naming-rollout-and-acceptance -->
### 13.3 ops-naming rollout and acceptance

The contract is in the architecture docs (start at
[the lifecycle index](../architecture/fabric-mesh-node-lifecycles.md)); this block owns the order
the pieces land in and the tests that prove them. Four moves, each its own landing, none bundled
into another. Step 1 lands before the i143 soak (R-ON7 moved the `NodeStopped` variant from step 2
to step 1); steps 0, 2 and 3 land after the i141 pin, behind the typed objects, so they change no
Rafka caller.

```text
ops-naming rollout (order and cost)
step 0  REST out of RDM: code placement only (open: e12.s60)
        rafka-node-admin-core/src/http.rs, the axum/tower dependencies of that crate and their users
        (admin.rs, tests/control_routes.rs, tests/client_contract.rs, tests/drift_convergence.rs,
        tests/provider_policy.rs, rafka-node-rpc-testkit/src/admin_faults.rs) move to the demo crate
        as thin callers over the typed objects. admin_api_base, the REST base URL NewFabricPrimary
        carries in its contact addresses, becomes the demo's field.
        done when: grep -rn 'axum\|"/api/' crates/rafka-node-admin* returns nothing; that grep
        is then the CI guard (iroh exports a Router, so Router is not grepped). Until it lands, a PR that adds a NEW route to an RDM crate is refused.
step 1  typed objects, the node lifecycle and Build 0x20 (landed: e12.s34)
        additive build family, fabric-primary sole driver; 0x1B unchanged; node workflows are
        submitted through build.create and mapped onto the existing node ops. Lands together: stop stops and start rejoins; restart keeps the
        incarnation and the mesh transport's restart completion moves to the same incarnation;
        delete proves the exit; node.deleted fences the old incarnation in the peer-location book;
        the mesh-primary publishes NodeStopped from the stop call's stopped frame.
        NodeStopped is a postcard enum variant: this landing carries `durable format moved`
        (a stop that stops has no other publisher for its departure, so it cannot wait for step 2).
        `permanent` removal is a code step of this landing: ?permanent=true changes four things today,
        all deleted: release of the data dir and node-identity.json on exit
        (node-admin/src/lib.rs should_release_on_exit), PermanentRemoval broker departure rows
        (node-admin/src/main.rs), the spawn-registry reap (main.rs), the permanent flag on the
        gossiped pending-delete (main.rs). Delete releases identity, never storage.
        SpawnRequest is the create spec that node.create carries through build.create; it is not a
        chaos mechanism.
step 2  Build step keys: node.create:<path.name> replaces create-node:<path.name> (open: e12.s72, with step 4)
        keys are stored in accepted Builds and receipts: `durable format moved`. They are built and
        parsed in rust-distributed-mesh, which is checked first for anything that splits a key on "-".
step 3  split the remaining 0x1B calls into the node, mesh and fabric op families (open: e12.s53)
        a wire contract change hidden behind the typed objects: only the objects change
after 0  config in the digest: its own landing
        admin_api_base leaves the typed MeshDigest field (rafka-mesh-entity/src/digest.rs) and
        NewFabricPrimary's contact addresses and becomes a config key the demo owns; `extra` is
        renamed `config` (a name only; postcard is positional); the cap is enforced. Removing two
        fields moves positional rows: `durable format moved`. The architecture pages that still name
        admin_api_base as a typed field (gossip.md, node-rpc-envelope.md) are tidied after this
        landing.
step 4  gossip frames NodeDeleting, NodeDeleted and NodeRestarting follow the same naming;
        MeshLeaving / MeshLeave / MeshLeft are deleted (there is no mesh-leaving); only with Luke's
        go-ahead (a gossip change needs it) (open: e12.s72). NodeStopped landed with step 1 (R-ON1).
deferred node.storage is not built. If a consumer needs it, a write carries a declaration (kind, ttl,
        backing store), never projection rows, because a projection fills only through the read path.
```

```text
ops-naming acceptance (each test ships its Jaeger trace)
A1  a failure names its step: node.start on a node whose join fails -> stream carries Started, then
    Failed { step: "node.join", reason }; neither node.joined nor node.started is ever emitted; the
    node's status never reaches ready.
A2  a broken stream is not a failure: cut the caller's stream mid-workflow -> NotSent or
    Indeterminate, never an inferred step failure; for a Build-backed call build.get returns the
    receipts of exactly the steps that completed and resubmitting by build_id re-runs none of them.
A3  stop stops, start rejoins: node.stop then node.start -> the same node.id, incarnation and endpoint
    before and after; node.get answers Current { parked } in between; the mesh-primary's start
    reaches the stopped node over node-RPC at the same endpoint (a fresh dial, since
    connections.delete cut the old ones); peers' books show the same birth throughout.

Build carrier acceptance (R-ON8; each case ships its Jaeger flame from a live mesh)
B1  node.restart over 0x20 is one call with Started, all step frames and Complete; no build.get
    polling and no Build-topic wait before the first frame.
B2  fabric-primary death after claim and before dispatch: the successor's build.attempt.run starts
    the claimed work; resubmission by build_id streams through Complete, no completed step runs twice.
B3  executor loss ends the inner stream Indeterminate; a successor attempt is claimed only after
    departure proof; the caller receives the successor's frames.
B4  cutting the outer stream leaves the Build running; resubmitting build_id after completion returns
    AlreadyApplied with the terminal frame.
B5  a mid-Build seat handover ends the outer stream with the typed new-seat frame, not Indeterminate;
    old-seat dispatch is refused after the fence and its span records Fenced.
B6  build.attempt.run sent to a recipient the claim does not name, or with a stale folded claim,
    receives a typed named refusal.

Open
    mesh and fabric stop and start do not exist; add them only if something needs to park a whole mesh.
```

---

<!-- /section:13-3-ops-naming-rollout-and-acceptance -->
<!-- /section:13-story-ordering-and-late-gates -->
<!-- section:14-export-import-gate -->
## 14. Export/import gate

i141 pins RDM once: the exact 40-hex merged `main` commit on which the complete export gate below is green. There is no partial pin and no sequence of pins.

`eligible=true` requires one exact RDM SHA with proof that:

<!-- section:identity -->
### Identity
- NodeId/MeshId/FabricId are canonical Crockford60 typed logical IDs;
- EndpointId is distinct transport identity;
- no canonical 32-char hex logical product IDs remain;
- a live mesh keeps its MeshId, and a mesh created again under a dead mesh's name gets a new MeshId; restart preserves NodeId and incarnation; node recovery and replacement mint a new NodeId; nothing alive comes back under the same fabric id and MeshIds.

<!-- /section:identity -->
<!-- section:accepted-topology-build -->
### Accepted topology / Build
- the current fabric-primary is the only topology writer; non-primary topology mutation is refused by name;
- each Build is its own entity, written once in acceptance order; the current Build is the first in progress or completed, read last in, first out; no independent desired-topology authority and no pointer to the newest Build exists;
- a Build is durable before any reader can read it as current;
- every admin reads the Fabric record + the current Build before authority-bearing Ready;
- older Build history is not replayed to reconstruct topology;
- every change to the shape (node.create, node.delete, the replace recipe, mesh.delete, fabric.delete) is its own Build compiled from the current topology plus its change;
- a second topology-changing request while the current Build is in progress is refused `BuildInProgress { current_build_id }`;
- authority movement, restart, node recovery and proven drift use another attempt of the current Build; an attempt lost with its executor leaves the Build in progress; only a named terminal failure fails it, and a failed Build is skipped;
- nothing that moves a seat or writes caller state runs before the initial fabric is ready (`fabric-not-ready`);
- `build.delete` refuses the current Build by name;
- `fabric.get` answers the current Build, not a second stored desired-topology object;
- all-admin restart restores the current Fabric record and the current Build from RDM storage interfaces.

<!-- /section:accepted-topology-build -->
<!-- section:runtime-death-provider-domain -->
### Runtime/death/provider domain
- every current birth carries a compact typed RuntimeFact bound to exact NodeId + IncarnationId + DeploymentId + provider + provider control domain + exact locator;
- provider/bootstrap produces/discovers the locator and the node publishes exactly one normalized current RuntimeFact;
- no competing launcher-owned RuntimeFact authority exists;
- required current runtime-management metadata such as `data_dir` converges to non-launcher authorities without becoming RuntimeFact identity by default;
- late successor can adopt/manage current runtime without completed Build receipts;
- Day-0 externally started admin registers/publishes an exact provider runtime/current metadata;
- process identity resists PID reuse; container identity uses immutable container ID;
- no PID/container ID is assumed globally meaningful across provider domains;
- same-incarnation locator or provider-domain change is refused;
- current process/container proof demonstrates same-domain successor control and cross-domain refusal;
- any multi-host eligibility proves remotely controllable locators or domain-local exact-runtime execution;
- provider terminal state for the adopted exact runtime is available to lifecycle/recovery;
- failed RPC/dial/silence/provider-domain mismatch is never exported as death proof.

<!-- /section:runtime-death-provider-domain -->
<!-- section:leadership -->
### Leadership
- only Ready candidates are eligible;
- node-admin Ready includes the current Build + RuntimeFact/runtime-metadata + provider-domain authority-capable hydration;
- normal joining admins cannot self-Ready around missing hydration; only Day-0/fenced single-admin recovery root may establish Ready without existing Ready authority;
- complete NodeId is the only election key;
- provider control domain/locality/runtime metadata is not an election key;
- node-admin cohort winner == mesh primary;
- only mesh primaries own fabric election;
- a living seat holder keeps its seat (a lower NodeId never displaces it); a seat moves only by yield, committed transfer or proven loss;
- Day 0 + three-mesh + replacement + late-admin takeover proofs are green;
- no stale amendment/parallel election canon exists.

<!-- /section:leadership -->
<!-- section:repair-recovery -->
### Repair/recovery
- held-member repair heals a partition while each side retains neighbours;
- failed rejoin is bounded and never marks Dead;
- there is no mesh recovery: an unheard mesh is mesh investigation; a node-admin is created into a live mesh only on a carrier-edge-lost verdict or, fully crashed, on exact provider exit proof; both set degraded on the mesh and the fabric until the new mesh-primary reports;
- nobody publishes a mesh on its behalf; the new mesh-primary re-gossips once established;
- a node recovery proves the exact exit, then deletes and creates at the same path.name with a new node.id over the inherited data dir;
- exact-runtime work in another provider domain uses valid remote/domain-local execution, never local PID/container guessing;
- fabric authority and the local executor are distinct;
- nothing inside the mesh checks who sent a frame or call; an out-of-order gossip frame is ignored once, in the receive path;
- the current Build remains readable independently of older Build history.

<!-- /section:repair-recovery -->
<!-- section:downstream-binding -->
### Downstream binding
The i141 import manifest proves Rafka binds to the exported generic mechanisms rather than recreating them:

```text
one imported current-Build read (LIFO) at bootstrap/catch-up + same-Build drift mechanism
one imported Build/Reconciler
one imported LeadershipResolver
one imported RuntimeFact + current runtime-metadata adoption/provider-domain contract
one imported recovery/executor rule
one imported exact-runtime provider contract
one imported held-member repair mechanism
```

After i141 cutover, any active Rafka-local generic comparator, incumbent ladder, lowest-name fabric selection, shape/drift/recovery algorithm, launcher-receipt/runtime-metadata authority, host-local provider guess, or conflicting executor rule is a blocking import defect. Compatibility wrappers are allowed only as stateless delegates.

<!-- /section:downstream-binding -->
<!-- section:transport-node-rpc -->
### Transport/Node RPC
- one canonical Iroh Endpoint per process or named/proved exception;
- no second generic internal mTLS stack;
- no hidden N0 DNS/Pkarr product address authority;
- Direct/Proxy connections parity and target-preserving Node RPC composition are green;
- complete-send certainty, 499, pool supersession, and carried execution are green.

The final import also reconciles downstream `node-identity-and-state.md`, feature docs, tests, runbooks, and RDM `docs/plans/i143-design.md` so target vocabulary no longer uses legacy transport `fabric_id`, ready_since/lowest-name/incumbent election scoring, launcher-private completed Build receipts/runtime metadata as runtime authority, cross-host PID/container assumptions, zero-neighbour-only repair, ordinary self-Ready fallback, or silence/RPC failure as recovery death proof.

---

<!-- /section:transport-node-rpc -->
<!-- /section:14-export-import-gate -->
<!-- section:15-verification-artifacts -->
## 15. Verification artifacts

Each E2E/hostile run emits:

```text
scenario manifest + seed
the current Build, read from the Build facts (R-BI3)
accepted Build requests/folded attempt views
Fabric/Mesh/Node snapshots
membership RuntimeFact snapshots
leadership candidate/result evidence
runtime adopt/inspect/exit evidence
provider-domain resolution/execution evidence
lifecycle/Pending receipts
RPC ledger
partition/repair ledger
fault ledger
OTLP JSONL
trace URLs
final reconciliation report
```

Identity proof records `node_id`, `mesh_id`, `fabric_id`, `id_format="crockford60"`, and separate EndpointId evidence.

Node recovery proof ([node-recovery.md](../architecture/lifecycles/node-recovery.md)) records at least:

```text
fabric_id
mesh_id
mesh_name
build_id
attempt
provider
provider_control_domain_fingerprint
runtime_locator_kind
runtime_locator_fingerprint
adopter_node_id
execution_node_id
the deleted birth and the created birth at the same path.name (node.id, incarnation)
the provider's exact exit proof (its inspection verdict)
```

A mesh none of whose node-admins can be reached is [mesh investigation](../architecture/lifecycles/mesh-investigation.md): its proof is its probes' outcomes and its decision step (the node.create of a node-admin into the live mesh, or none). There is no mesh recovery.

Amended 2026-10-19 by the lead, against the canon: `FabricRecord snapshots including build_id` became the current Build read from the Build facts (R-BI3 deleted `Fabric.build_id`); `current runtime-metadata snapshot/digest`, `recovery_admin_node_id` and `runtime_metadata_digest` are dropped (nothing produces them; the runtime evidence is the provider's adopt, inspect and exit evidence above); the mesh-recovery counts (`desired_shape`, `live_before`, `preserved_count`, `tombstoned_count`, `created_count`, `live_after`) are dropped with mesh recovery itself (mesh-investigation.md).

---

<!-- /section:15-verification-artifacts -->
<!-- section:16-guardrails -->
## 16. Guardrails

- no second topology mutation/reconciliation engine;
- no downstream topology/drift engine beside the imported RDM current-Build control state;
- no downstream election comparator beside imported LeadershipResolver;
- no launcher-private handle map, runtime metadata, or completed Build receipts as current-runtime authority;
- no competing launcher-owned RuntimeFact publication stream;
- no completed Build history replay as the accepted-topology database;
- no independent desired-topology revision/lineage/fork authority or pointer beside the current Build;
- no ordinary self-Ready fallback when current control state cannot be hydrated;
- no character-sum/hash election score;
- no ready_since/ordinal/mesh-name/MeshId/FabricId/incumbent election priority;
- no RuntimeFact/provider locator/provider control domain/current runtime metadata/Build id as election priority;
- no logical FabricId used as EndpointId or transport key;
- one id type: BuildId, DeploymentId and IncarnationId are minted Crockford60 like NodeId/MeshId/FabricId, never hex (R-BI5);
- no recovery MeshId/FabricId remint;
- no PID-only process death identity;
- no container-name-only exact identity;
- no assumption that PID/container ID is globally meaningful across provider domains;
- no `data_dir` treated as exact runtime identity without a provider-specific reason;
- no Build before Pending Applied;
- no bootstrap executor treated as elected primary;
- no Pending executor outside its own mesh;
- no failed RPC/dial/silence/provider-domain mismatch interpreted as Dead;
- no zero-neighbour-only repair requirement;
- no application resend protocol beside iroh-gossip;
- no completed-runtime receipt archive on Build gossip;
- no exact surviving-ordinal/name assumption in elastic shape tests where cohort count is the semantic contract;
- no semantic target selection inside connections/Node RPC;
- no second internal mTLS stack;
- no hidden N0 address authority;
- no private/internal map as E2E truth.

---

<!-- /section:16-guardrails -->
<!-- section:17-definition-of-done -->
## 17. Definition of done

- exact eligible RDM revision exported;
- the current-Build read + same-Build drift, Build/Reconciler and LeadershipResolver live once in `rafka-node-admin-core`;
- RuntimeFact publication/adoption + runtime-metadata convergence + provider-control-domain handling is part of current membership/runtime state;
- provider/bootstrap locator production and node-owned RuntimeFact publication are distinct and proven;
- i141 has clear adapter APIs and no need to copy algorithms;
- NodeId/MeshId/FabricId and EndpointId meanings are unambiguous;
- Day-0 runtime adoption/control-root bootstrap and exact death proof are green;
- ordinary joining admin cannot self-Ready around missing control hydration;
- late non-launcher authority can manage current runtimes and required runtime metadata after completed Build history is gone;
- current process/container proof demonstrates same-provider-domain successor control and rejects cross-domain PID/container misuse;
- multi-host eligibility, when claimed, proves remote provider control or domain-local exact-runtime execution without changing election ordering;
- the current Build (LIFO read) remains readable after older Build history is forgotten and drives later recovery;
- hierarchical membership and held-member partition repair are green;
- canonical NodeId leadership is green at node-type, mesh-primary, and fabric-primary levels;
- Pending/status certainty is green;
- node.create, node recovery, the replace recipe and authority/executor separation are green;
- nothing inside the mesh checks who sent a frame or call, and an out-of-order gossip frame is ignored once, in the receive path;
- fabric.delete leaves no node, mesh or fabric row; a mesh with no answering primary is reached by investigation and deleted;
- every open builder block below has landed through the land script, with its flame where it is a gossip or mesh change;
- an in-progress Build survives authority movement and executor loss; drift, restart and node recovery are attempts of the current Build, and every change to the shape is its own Build;
- Direct/Proxy target-preserving Node RPC is green;
- process/container scenarios, deterministic faults, real chaos, and soak are green;
- parity ledger has zero blocking gaps;
- final manifest proves downstream import uses the library mechanisms rather than a parallel Rafka control brain.

<!-- /section:17-definition-of-done -->
<!-- section:the-plan-of-record -->
## The plan of record

This PRD and the architecture docs are the source of truth for i143 (Luke 2026-10-12: "the prd is the source of truth plus the
architecture"). A seat's brief is `python3 scripts/prd.py brief docs/plans/i143-prd.md <story>`, taken at launch from main. A seat
reads any other block with `python3 scripts/prd.py get docs/plans/i143-prd.md <id>`, and never the whole PRD. GitHub issues are
generated once from a block, for tracking only, and are never read back as the plan. The seat rules (the lead's scratchpad
seat-rules.md) apply to every seat. Each story's architecture names its lifecycle page. A doc line, ruling wording or code that
disagrees with that page is a defect to report.

<!-- /section:the-plan-of-record -->
<!-- section:story-bar -->
## Story bar

Every open story below meets this bar:
- its red cell comes first;
- the named document's sequence holds in a deterministic cell with its trace;
- the affected stems are green once at gate cadence;
- mesh-audit and the gate build set are green;
- nothing inside the mesh checks who sent a frame or call (R-RX1), and ordering lives only in the gossip receive path (R-RX3);
- a gossip or mesh change carries its Jaeger flame from a live mesh;
- it lands only through the land script.

<!-- /section:story-bar -->
<!-- epic:e12.s41 -->
## e12.s41 — node row: Deleted kept at node.delete with record and config; config seeded from the previous row; MetaFieldIsRdmOwned / NameHeld refusals; the node record's identity fields: path, name, node_type

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3068 (generated once; not read back) · **Architecture:** node-rpc-envelope.md#meta-and-config; lifecycles/node-delete.md step 3; node-identity-and-state.md §2 (the node entity) and :260 (identity fields); rafka record.rs:21 (Node.node_type); rulings R-MD1, R12/R13

### State

landed 1395a916 (s41), be6b5cf0 (departed-by-id), c7642f18 (config on the record, absorbed s36). The departed fence it built is an R-RX1 sender gate, removed by the s53 RX unit. R-NP1 Phase 0 LANDED e5b127d7 (2026-10-13, gate 140 s, its build 70.8 s wall): node.tags.create / node.tags.delete are their own 0x1B commands (CreateTags 26, DeleteTags 27, TagsCreated 45, TagsDeleted 46; durable format moved; mesh-audit 28/47), served by the mesh-primary through its held record (node_properties.rs), the label grammar is the envelope's (a-z 0-9 - _ . ~, no leading '.'), and node.config.update and the tags commands are refused fabric-not-ready until the initial fabric first reached ready (traces 742cce081fb7fd063c80a0fb68c286c8, 0226c97808812cf57436b34203faa6c2); part 5 built Phase 1 on i143-s41-name-update (RED d045661b `durable format moved`: UpdateName 20 carries the name only, NameUpdated, RejectedAdmittingConnections, RejectedName 47, BusyHoldingConnections 48, NodeRow.renamed_from; GREEN 053dd0f3: node_name_update.rs, the §2.1 validator order, the pending release finished by the next writer); Phase 1 LANDED 4ec1b96c (2026-10-13, gate 140 s, its build 72.6 s wall) with its remainder (the config and tags writers finish a pending release, RED 06cf0e2f GREEN 59fed18d; one grammar function for tag labels and node names) and the step io split (each pipeline step's span carries io_calls, io_queued_ms, io_ms: RED d4f49a9f GREEN 344c1112); its first gate's red, node_objects::a_create_whose_join_fails_names_the_join_step_and_emits_nothing_after_it, was PrepareStorage taking 4670 ms (busy 0) for one load_or_mint_key before the 5 s stream bound: not the park gate (park_gate.rs:126-144 waits only while parked), the root not reproduced in one diagnostic run; the bound stays and the next firing's step span names queue vs io; the rename admission block LANDED 1f8d6c14 (2026-10-13, gate 104 s, its build 36.5 s wall): a node's data-plane client refuses a dial after `All` (`at=dial`, pool.rs:295) and closes a connection that arrives after it (`at=pool`, pool.rs:406), each on `rdm.node_rpc.connection.reject.via-admission-block`, until the name apply resolves or the node starts; the control pool and gossip are untouched (RED 7daae8ba, GREEN 3e448570); and All reads the open connections after its wait, so a connection admitted work opened during the wait is closed (node-connections-delete.md step 4); reported, no work: no RDM process logs accepted connections (the inbound observer, server.rs:295, has a test caller only) and the admin process has no data-plane client; Phase 2 LANDED 97a59269 (recovery is node.delete then node.create; tags seeded with config at the recovery claim, indexed under the new id) and only a recovery's claim takes the Deleted row's tags, a plain create takes its create spec's LANDED 590a025e (2026-10-13; RED 1f5017fb). R-OW1 items 1-2 LANDED 95e10533 / 7c3b753c (2026-10-13, gate 83 s, build 67.3 s wall; RED f2dc3321): the mesh-primary writes its own node row on arrival (node_rows.rs:595, the `n.node_id != *me` filter gone) and the Day-0 admin claims its own path.name as the first write of its birth before Ready (admin.rs `claim_day0_name_in_boot`; cells `store_root_boot::the_day0_admin_claims_its_own_path_name_as_the_first_write_of_its_birth`, `node_rows::the_mesh_primary_writes_its_own_node_row_on_arrival`; trace 9507a0275e4661e978ea396aab8b661f). D2 replace inheritance LANDED e939a8de (RED 302726b1): `node-replace.md` inheritance covers both predecessor branches (`.old` row or `Deleted` row), rollback restores the predecessor's tag-index entries under its original id, and a repeated cutover preserves the successor's already-inherited values (cell `node_rows::repeated_replace_cutover_preserves_already_inherited_values_and_rollback_restores_predecessor_tag_index`). Cohort seats and `Seated` (Q2/Q2a) LANDED 933b0c61 / e7ffb680 / 908d01e8 (2026-10-14, gate 110 s, build 13.5 s wall; RED ad6af7ad): every `node_type` cohort keeps its own seat record, `node.primary.update` moves any cohort's primary (`fabric_handover::a_mesh_primary_moves_its_rpc_cohorts_primary_by_commanding_the_holder_with_its_committed_row` trace f5e608798264b071a22bcb4a3b8451a4, `::the_fabric_primary_commands_an_rpc_cohorts_primary_update_only_through_its_mesh_primary` trace 818f93d5bf219fe6f0248d682ce69b2d, `::an_rpc_cohort_with_no_other_node_of_its_type_refuses_node_primary_update_by_name` trace 263e0b0c741b92157bb04aa1148c7b0f), `Seated` is ordered by revision per seat key in `receive_order.rs` with out-of-order records ignored on `rdm.mesh.membership.reject.via-earlier-version` (`membership::tests::a_late_older_seated_record_is_ignored_by_the_receive_path_and_cannot_restore_an_exited_holder` trace 977340378600fe731b8c9bb8f623b74f span 6e218b197f6b3e28). Free-text `path`/`name` and immutable `node_type` LANDED e634b766 (2026-10-14, gate 149 s, `TIMINGS wall=71.2s units=455 max_concurrency=11 unit_sum=191s`; RED b0f888b0): `PathName` holds a free-text `name`, `NodeRow`/`NodeRecord`/`NodeView` carry `path`, `name`, and `node_type` as separate fields, `node.update` of `node_type` and invalid `node_type` values are refused by name (cells `node_rows::a_node_created_with_a_free_text_name_round_trips_name_path_and_node_type_through_create_get_stop_start_recovery_and_replace` trace 18ff5a0dfeba5c86f1e11a1565512626, `node_rows::node_update_of_node_type_and_invalid_node_type_are_refused_by_name` trace 2e6082eb62990e91f4621a360d23ec60). The proposal's §5 doc edits 1-7, 9 and 10 landed 5ed5413028 (node-name-update.md, node-config-update.md, the envelope's node properties and 0x1B frames)

### Remaining
- [x] node.update goes; every property has its own command (proposal `docs/plans/proposal/node-properties-own-commands.md`, Luke's yes 2026-10-12, §9 = A: any name within the grammar, unique within its path): `node.name.update` with its validator (exact birth, writer, `RejectedAdmittingConnections`, `RejectedName`, the adapter claim), 0x1B `UpdateName`/`NameUpdated`, `RejectedMetaFieldIsRdmOwned` and the client `NodeMeta`/`check_update`/`meta_of` deleted, `rafka_mesh_entity::meta::NodeMeta` → `PathConfig`, replace's two renames on the new object, recovery's step 6 deleted; the §5 doc edits in the same landing; `durable format moved`; sequenced behind s53 (shared files: node_update.rs, rejoin_name, the 0x1B replies); the rafka-v2 pin bump and harness `NameUpdate` agreed with the dev lead before either lands

- [x] a path is free text, and a node is created as <mesh>.<kind>.<name> (a name, not an ordinal); path and name are TWO fields, joined only as the path.name index key, never a field ("path.name is not a field, it's the path and the name field"); the node's type is its own field node_type (RDM `kind` takes Rafka's name), set by the caller at node.create, immutable, refused by node.update, copied by replace and recovery. RDM's PathName { mesh, kind: NodeKind, ordinal: u32, old, new } (rafka-mesh-entity/src/path.rs:71) is replaced. Large: PathName is used across RDM; the store keys that read it are durable (durable format moved) (landed e634b766, RED b0f888b0, gate 149 s)
- [x] every node_type cohort keeps a seat record like the node-admin cohort does, so node.primary.update moves any type's primary (R-MT1: "same for NA as it is for RPC node"). Part 13 on `i143-s41-cohort-seats` @6612fd57 (stacked on 2839ff30) and Part 14 @79e57295 landed 933b0c61 (2026-10-14, gate 110 s): every cohort's seat record held and read by the election (03bbff62, durable format moved; election.rs:138 `resolve_with`, admin.rs:892 `incumbency_of`, topology_read.rs:219), the mesh-primary gossips `Seated` for each cohort seat the election fills (6612fd57, admin.rs:934), and `node.primary.update` moves any cohort's primary per C1 (traces f5e608798264b071a22bcb4a3b8451a4, 818f93d5bf219fe6f0248d682ce69b2d, 263e0b0c741b92157bb04aa1148c7b0f, 2a742304f6581cb9dec07ef8774cb181, 6c9a48837d90fa8c3a27d0f0acb5e827)
- [x] R-OW1 items 1-2 (rulings 2026-10-11-dev :413): landed 95e10533 / 7c3b753c (RED f2dc3321, gate 83 s): the mesh-primary's own birth arrives and its row is written like any member's (node_rows.rs:595, the `n.node_id != *me` filter gone); the Day-0 admin's first write is the insert-and-fail claim of its own path.name before Ready (admin.rs `claim_day0_name_in_boot`, one `own_claim` path shared with the recovery claim); trace 9507a0275e4661e978ea396aab8b661f (claim +1.4 ms, ready +37 ms)
- [x] spec gap for Luke (resolved in node-config-update.md step 3, 2026-10-13): node-config-update.md step 3 completes a stopped node after the durable row write with nothing sent

- [x] D2 (docs updated 2026-10-13 on Luke's decision; code landed e939a8de, RED 302726b1): node-replace.md inheritance covers both predecessor branches (retained `<name>.old` row, or the Deleted row at `<name>`); the destination claim is the name move's first write; rollback restores the predecessor's tag-index entries under its original id, and a repeated cutover keeps the successor's already-inherited values (never overwrites a later update); node-config-update.md step 3 (a stopped node is sent nothing, the command completes after the durable row write)
- [x] Seated (Q2/Q2a) (docs updated 2026-10-13 on Luke's decision; code landed 933b0c61 / e7ffb680, RED ad6af7ad, and 3eaae3e6): gossip.md frame catalog row and order-key list; code: `Seated`'s order key is the holder record's revision per seat key (`(mesh, node_type)` / `fabric`) in receive_order.rs, ignored in the one receive path with the ignore span (`rdm.mesh.membership.reject.via-earlier-version`, trace http://127.0.0.1:16686/trace/977340378600fe731b8c9bb8f623b74f span 6e218b197f6b3e28: `rdm.mesh.membership.reject.via-earlier-version { node = "mesh1.rpc.1", channel = "mesh:mesh1", frame = "seated", source = "mesh-primary mesh1", held = "revision=2", offered = "revision=1" }`); membership.rs's `refused-not-newer` handler check is gone; the seat record's epoch is named revision (3eaae3e6); SeatedTogether is retired with TakeSeats; cells: records for different cohorts never suppress each other; a late older record cannot restore a holder whose process is gone; join and catch-up carry every cohort's holder, not only the admin's. Lesson for the seat (Luke): a gossip frame's reach is asked first and landed second
### Acceptance criteria

<!-- crit:e12.s41.1 -->
- [x] a node created with a free-text name round-trips name, path and node_type through create, get, stop/start, recovery and replace
<!-- /crit:e12.s41.1 -->
<!-- crit:e12.s41.2 -->
- [x] node.update of node_type is refused by name
<!-- /crit:e12.s41.2 -->
<!-- crit:e12.s41.3 -->
- [x] mesh-audit resealed
<!-- /crit:e12.s41.3 -->
<!-- crit:e12.s41.4 -->
- [x] the full fast gate green
<!-- /crit:e12.s41.4 -->
<!-- crit:e12.s41.5 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s41.5 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s41 -->
<!-- epic:e12.s42 -->
## e12.s42 — replace recipe per node-replace.md (R12 + R13) and the paths it exposed: the row-name view, the new seat resuming an unheard Build, the lost-executor create identity, the router on the exact birth, the id index at publication, the heir watch, the Build-log snapshot, the provider find states, the launch record before exec

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3069 (generated once; not read back) · **Architecture:** lifecycles/node-replace.md; node-start.md step 2; node-recovery.md (Detection); node-create.md step 1; node-rpc-envelope.md (TakeSeats, Build frames, fabric status rounds); rulings R12, R13, R-UC1, R-RX1

### State

landed b0b7f31d, eb785cd6 (parts 2-6) and 0cd2c3fa (parts 7-8: a superseded round's owed completion is withdrawn; begin() fails a step by name when a Build cannot be read; a hook Blocked with no reason fails the node by name; the scenario's hydrate knob is staged and renamed, never rewritten in place); landed 4704c6db (A2, R-UC1a and R-RX5): a start joins back over the endpoint of the call that commanded it (`NodeTarget::Endpoint`, no node id), the receiver-side 425 target check retired, the subject-itself refusals removed, a second start redirects the one in flight, the leave hands back over its call (live flame 5e9ca6c06ca10ee3c706753fadd25363); landed f0523ca8 (2026-10-14, gate 148 s, `TIMINGS wall=69.6s units=455 max_concurrency=11 unit_sum=188s`; RED b8a7fdd8): `Pool::purge_stale` adopts the join's endpoint-keyed pooled connection under the resolved birth and evicts it when superseded (`pool.rs:206-256`), `Pool::get_or_dial` reports the `Direct` connection fact once the commander's birth is resolved (`pool.rs:324-346`; cell `one_endpoint::a_join_to_an_endpoint_no_record_names_is_adopted_by_the_birth_and_purged_when_superseded` trace 25d1d6220a9f6166c8e439ae61fc5f14), and `rejoin_commander` 5/5 (traces ec45e5feba7f4a6f9d50f80940c3aa5c, 290521ccf3af059611eb3f2e6f6845d1) plus `mesh_delete_source` 3/3 (trace 282995d4256c9ba8a3e0b558bf787d46) are green at gate cadence

### Remaining
- [x] (done 2026-10-12, with the Fence→Target rename) docs (Luke's yes to the exact text): node-rpc-envelope.md §0's 425 row and its sentence "the socket a request arrives on names nothing: the fence does", the reserved-code 425 entry, node-rpc.md :480/:595 (RejectedStale as a receiver answer)
- [x] the join's pooled connection is adopted under the resolved birth and purged on supersession (`pool.rs:206-256`), and a join to a commander no record names at join time reports its `Direct` connection fact once the birth is resolved (`pool.rs:324-346`; landed f0523ca8, RED b8a7fdd8, cell `one_endpoint::a_join_to_an_endpoint_no_record_names_is_adopted_by_the_birth_and_purged_when_superseded` trace 25d1d6220a9f6166c8e439ae61fc5f14; live traces ec45e5feba7f4a6f9d50f80940c3aa5c, 290521ccf3af059611eb3f2e6f6845d1, 282995d4256c9ba8a3e0b558bf787d46)

### Acceptance criteria

<!-- crit:e12.s42.4 -->
- [x] a stopped node commanded by an admin born after its stop starts: rejoin_commander.rs (red cell on branch i143-s42-ruc1-red @ ac0d8493) is green
<!-- /crit:e12.s42.4 -->
<!-- crit:e12.s42.5 -->
- [x] mesh_delete_source.rs (~105) hands back to the caller's endpoint; node_stop, node_start_sequence and mesh_leave_resume green at gate cadence
<!-- /crit:e12.s42.5 -->
<!-- crit:e12.s42.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s42.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s42 -->
<!-- epic:e12.s46 -->
## e12.s46 — R-QH1: the QUIC join timeout — root cause found (host packet filter), and chaos faults that cannot outlive their owner

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3074 (generated once; not read back) · **Architecture:** rulings 2026-10-11 dev page §R-QH1; the packet scratchpad e12/r-qh1-packet.md (evidence chain)

### State

diagnostics landed 65122557. Root cause: leaked RAFKA-INB-692042-0 (lo udp dport 56958 drop) and RAFKA-PART-* chains left by killed chaos-kit processes (FirewallInbound / Partition cleanup was Drop-only). Kit fix landed 33ee1f85 / a17f7448 (RED 5efc16c4; merged onto main via 7b65e7d3 / 08948db5, 2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`): each fault is an nftables table held by a `nft -i` helper's stdin pipe so the kernel drops it when the owner dies; `/usr/sbin/nft` added to sudoers, leaked `RAFKA-*` chains removed, 127.0.0.1:56958 probe receives, and `chaos_kit__restored_faults` 6/6 (including `firewall_inbound_ends_with_the_process_that_installed_it`) is green on the host

### Remaining

- [x] remove the four leaked chains; add /usr/sbin/nft to sudoers. Then: re-probe 127.0.0.1:56958, run firewall_inbound_ends_with_the_process_that_installed_it on the host, land i143-qh1-kit (landed 33ee1f85 / a17f7448 via 08948db5, gate 152 s)

### Acceptance criteria

<!-- crit:e12.s46.1 -->
- [x] the 56958 probe receives
<!-- /crit:e12.s46.1 -->
<!-- crit:e12.s46.2 -->
- [x] the SIGKILL cell green on the host
<!-- /crit:e12.s46.2 -->
<!-- crit:e12.s46.3 -->
- [x] no RAFKA-* chain survives a killed cell
<!-- /crit:e12.s46.3 -->
<!-- crit:e12.s46.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s46.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s46 -->
<!-- epic:e12.s47 -->
## e12.s47 — a node names itself by the name it holds, everywhere: its connections end (R-CN1) and every span, executor and control-plane identity

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3075 (generated once; not read back) · **Architecture:** node-identity-and-state.md §8; lifecycles/node-update.md (rename); node-start.md step 2; rulings R12/R13, R-CR1

### State

parts 1-3 landed 0d772ec3; R-CN1 (Q6) LANDED 878d4473 / d4ef297c (RED 0a795fbd) and §9 due-time reconnect (Q7) LANDED b00db084 (2026-10-14, gate 102 s, build 67.2 s wall; RED 2d746961): an inbound accept writes no `Direct` entry and wakes the source's own dial whose `Direct Connected` retires the `Proxy` (live cell `i143_acceptance_2938::source_accept_wakes_its_own_dial_whose_direct_retires_proxy_before_cutback` traces `http://127.0.0.1:16686/trace/a49ad091afd37c5ec1507a860294e548` and `http://127.0.0.1:16686/trace/0c65e852045816acf1deb69d2bb6612a`; deterministic cells `connections_writer::an_accept_from_the_proxy_destination_writes_nothing_and_wakes_the_source_dial_whose_connected_retires_the_proxy` trace 470af165290965e49c2e7652bc9f2a73 / e8a218e9d062a2fe863e8df63960f0c3, `::a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row` trace 9b630285ae2cfc8b6681308c4e1b471c / 2d2220e13a684bc1c5dffc8c77f87391, `::repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff` trace 6dc00ceef79f4f7e41a5cd8cda700957 / 6e0bfbea7f1216da42e2b773103d9db0, `::an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled` trace 521a1da17272e96ae13280984c240317, and `::a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts` traces `http://127.0.0.1:16686/trace/9df1542e9a12dfc0cad3f459be21d74d`, `http://127.0.0.1:16686/trace/c2b0687c4d1dd0df7f249b25661c93e2`, `http://127.0.0.1:16686/trace/8db8fe4733dedc40902767162d4311e0`; verbatim span line: `rdm.node.connections.record.write.done.ok.via-recorded trace_id=8db8fe4733dedc40902767162d4311e0 span_id=f68452a9f06c598e rdm.node=mesh1.rpc.1 rdm.conn.id=mesh1.rpc.1->mesh1.rpc.2:direct:4 rdm.conn.peer=mesh1.rpc.2 rdm.conn.kind=direct rdm.conn.state=connected rdm.conn.attempt=4`). The boot-name sweep LANDED 6d264101 (gate 152 s) and bcfa253f (2026-10-14, RED 8aadd33c): control-plane, Build, row, handover, status, and admin spans read `held_name()` instead of the boot `PathName`, and `RunDoor`/`BuildExecutor` admit a renamed admin under its held name (cells `held_name_spans::`, `rejoin_name::a_node_renamed_while_stopped_starts_under_its_rows_name_and_config`)

### Remaining

- [x] the boot-name sweep — about 92 span sites and about 60 control-plane sites use the boot name; read the held name (`node_self.rs:312` `held_name()` on the node, `membership.rs:2996` `held_name(birth)` for another birth). `self.me` / `me: PathName` sites on RDM main 01a65889, by file: admin.rs 63, fabric_handover.rs 33, status_rpc.rs 30, build_drive.rs 29, admin/leave.rs 24, node_rows.rs 19, status_declare.rs 14, build_op.rs 14, round.rs 12, build_run.rs 10, hydrate.rs 9, join.rs 8, http.rs 8, build_claim.rs 6, storage.rs 5 (rafka-node-admin-core/src). Also BuildExecutor.executor, RunDoor (NotExecutor for a renamed admin) and about 30 `me` fields; assert the renamed-while-stopped cell's CONTRACT (no span names the boot name after the join) (landed 6d264101 and bcfa253f, RED 8aadd33c)

- [x] R-CN1 (Q6) (docs updated 2026-10-13 on Luke's decision; code landed 878d4473 / d4ef297c, RED 0a795fbd): connections.md §10 list and §13 G; the four cells, named or added in this unit: CONN-G (`i143_acceptance_2938::source_accept_wakes_its_own_dial_whose_direct_retires_proxy_before_cutback`); an accept followed by a failed source dial with Proxy still usable (`connections_writer::a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row`); repeated accepts coalesced into one pending attempt with backoff respected (`connections_writer::repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff`); an accept from a stale destination birth producing no attempt (`connections_writer::an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled`)
- [x] §9 due-time reconnect (Q7) (docs updated 2026-10-13 on Luke's decision; code landed b00db084, RED 2d746961, gate 102 s): connections.md §9.4 states one reconciler, two arming paths, one pending item; build: the reconciler attempts every due `reconnect_plan` item, one pending attempt per (destination, incarnation) shared with R-CN1's accept trigger (an accept coalesces into a pending item, never a second attempt, never skipping the backoff); a dial change, lands with its flame (`connections_writer::a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts`, traces `http://127.0.0.1:16686/trace/9df1542e9a12dfc0cad3f459be21d74d`, `http://127.0.0.1:16686/trace/c2b0687c4d1dd0df7f249b25661c93e2`, `http://127.0.0.1:16686/trace/8db8fe4733dedc40902767162d4311e0`, span `rdm.node.connections.record.write.done.ok.via-recorded trace_id=8db8fe4733dedc40902767162d4311e0 span_id=f68452a9f06c598e rdm.node=mesh1.rpc.1 rdm.conn.id=mesh1.rpc.1->mesh1.rpc.2:direct:4 rdm.conn.peer=mesh1.rpc.2 rdm.conn.kind=direct rdm.conn.state=connected rdm.conn.attempt=4`: Failed row, due time, attempt, then Direct Connected or the next Failed row); Luke's condition: a cell that needs Proxy to stay put seeds the Failed row with a due time beyond its window, no test-only knob
### Acceptance criteria

<!-- crit:e12.s47.1 -->
- [x] a renamed admin runs a claimed attempt under its new name (red today: NotExecutor)
<!-- /crit:e12.s47.1 -->
<!-- crit:e12.s47.2 -->
- [x] no span after the join names the boot name
<!-- /crit:e12.s47.2 -->
<!-- crit:e12.s47.3 -->
- [x] node_stop, node_replace, sticky_seat green. Mechanical: a Sonnet seat after s42/s56 land
<!-- /crit:e12.s47.3 -->
<!-- crit:e12.s47.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s47.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s47 -->
<!-- epic:e12.s53 -->
## e12.s53 — the 0x1B / status / connections cut, and no receiver authorization inside the mesh

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3083 (generated once; not read back) · **Architecture:** lifecycles/node-connections-delete.md; node-rpc.md §14; gossip.md §3.1 (receive path, order keys); lifecycles/states.md (stopped and deleted); docs/plans/proposal/node-status-heartbeat-connections.md §B; rulings R-CR4, R-CR5, R-ST1, R-DK1, R-RX1, R-RX2, R-RX3, R-RX4, R-TG2 (connections shape); the audit scratchpad e12/rx-authz-audit.md

### State

absorbed s54 (R-ST1 names), s77 (0x1B split), s48 (natural-key admission), s67 (stop/start replies only). RX removal landed 01a65889: the R-RX3 receive-path check (receive_order.rs) with the A1 sender gates removed, the A3 handover sender checks removed, 6 of 8 PeerUnresolved -> SubjectUnknown, the acting-primary publisher and `publishers_own` removed (R-RX4; A'4 has no case left), and a seat taker adopts the statuses its fold acquired into its book before its first projection (fabric-primary.md §7; s60's beee5a42, which the R-RX4 cut exposed: 2803 8/8, trace 5d2760030779eaaf011d78cb34b22662). The coordinated cut landed 0dd4ed15 (2026-10-13, gate 103 s, its build 20.1 s wall incremental; with the harness: the record_store cell exact, workspace crates incremental): node.connections.delete { All | [ids] } closes connections-log entries only (R-CR5), the control client and pool unlogged and a data-plane client inside the node logging its own (R-CR6), stop = All then its own transport cut, DrainNode/NodeDrained refused by name, ConnectionsReceipt (durable format moved), the deleted mesh's NodeStopped aggregate published from the stop's own status event (mesh-delete.md sequence; deleted_mesh_aggregate.rs), a start reissued by a later attempt of its Build settles AlreadyApplied and takes no node.joined, AwaitNodeLeft takes the StopNode receipt's Stopped as the left reply (a successor executor reusing a lost one's stop), the state-commit cell holds an originated data-plane connection (R-CR6). R-NS2 (Luke 2026-10-13, B: a stop does not wait for gossip delivery; final announcements are best-effort; committed facts recover through the read, the replay or the surviving publisher, the executing authority publishing as author) LANDED 34056198: when a primary holder's stop completes, its mesh-primary (the new holder, the seat row's next writer) says "primary updated" once as author before NodeStopped (pipeline.rs stop_completed, membership.rs announce_update_of_stopped_holder); the four cells drop the final announcements on purpose through a test-only hook armed by the fault door and assert convergence through the authority paths (node_stop :536, sticky_seat's hand-over, admin_restart, fabric_primary_handover); their gate skips came out; no wait, bound or flush on the stop path; gossip.md's sentence reads as best-effort (rafka-v2). Q12 = A (Members publisher reads its stamp inside the version lock, one critical section with `topology_version` and `snapshot_id`), R-RX3 (one held version per source across both receivers and the topology read), and the same-source handler ordering checks folded into `receive_order.rs` LANDED 4d6d0497 / 00c9f9e9 (2026-10-13, gate 77 s, build 13.7 s wall; RED c53a73eb; cell `membership::tests::interleaved_backbone_publishes_and_neighbour_up_replay_stamp_and_forward_in_version_order` trace `http://127.0.0.1:16686/trace/5716e3d0c404b73782c246a0b2ed2d4d` spans `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=b91eb571037da31e rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=1 rdm.target_version=2` and `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=da0a78724a2e9f44 rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=2 rdm.target_version=3`, `topology_read::a_topology_read_install_advances_the_held_version_so_an_older_gossip_frame_is_ignored`, and `rejoin_commander` 5/5 green with `NeighborUp` replay queued onto `ForwardPath::replay` under `forwarder.lock()`). R-CN1 (Q6) LANDED 878d4473 / d4ef297c (RED 0a795fbd; live `CONN-G` dial flame a49ad091afd37c5ec1507a860294e548 -> 0c65e852045816acf1deb69d2bb6612a) and Q7 due-time reconnect LANDED b00db084 (2026-10-14, gate 102 s, build 67.2 s wall; RED 2d746961; flame `http://127.0.0.1:16686/trace/8db8fe4733dedc40902767162d4311e0`). A2 (R-UC1a) landed 4704c6db / f0523ca8. R-BC1 (Luke 2026-10-14): 0x1C keeps `NotClaimant` as its one seat refusal; the page's `RejectedNotAuthority` line struck (docs only; the code never had it, landed 00d097cd). R-RX6 (Luke 2026-10-14): the four A' attribution items stay as landed (docs only, landed f1109974, gate 44 s). `MeshLeft`'s refusal deleted with `e12.s72` (`b81f66a0`). Left: none

### Remaining

- [x] R-BC1 (Luke 2026-10-14, option 1): `NotClaimant` stays the one seat refusal on 0x1C; the envelope's `RejectedNotAuthority { reason }` line (added in 8fc5be4c without a ruling) struck at :860 and :872; build_claim.rs unchanged
- [x] A2 waits on R-UC1a (landed 4704c6db / f0523ca8)
- [x] R-RX6 (Luke 2026-10-14, "i agree"; landed f1109974, gate 44 s): the four A' attribution items stay as landed (docs only): round `heard_from` keyed on the sender (`status_rpc.rs:582`), `Ready` round report attribution under `Reporter::Birth(incarnation)` or `Reporter::Endpoint(peer)` (`status_rpc.rs:671`), the probe wake `wake.addressed_by(sender, peer)` (`status_rpc.rs:1028`), and rafka-time lineage in `from_mesh_primary_digest` / `from_fabric_primary_members` admitting a stamp only when the digest or aggregate's own payload birth is the seat book's holder and `Ready` (`rafka-mesh-transport/src/discipline.rs:42-78`)
- [x] the same-source handler ordering checks fold into receive_order.rs (landed 4d6d0497 / 00c9f9e9)
- [x] the coordinated cut (landed 0dd4ed15): graceful All on the connections log, no new outbound connection through the name apply, DrainNode/NodeDrained retired, ConnectionsReceipt, node.connections.delete { All | connection | [connections] }, status names (Leaving -> stopping/stopped, departed -> deleted)
  - parts A and B on i143-s53-cut @ f96ebdb7 (lane e6s7), not landed: the control client and pool write nothing to the log (admin.rs:5107 `None`), the originate door's data-plane client (testkit node.rs:379) writes the Direct entries, `All` closes log entries only and cuts no pool and no gossip channel, stop = `All` then its own transport cut, DrainNode/NodeDrained refused by name, ConnectionsReceipt on ConnectionsDeleted and Stopped (`durable format moved`, 3d318e1e), rejoin_name runs `All` then ApplyNodeName. R-CR6 = A (Luke 2026-10-12): a data-plane connection may ride Node RPC framing when a second client inside the node originates and logs it; connections.md's control-plane sentence names "the node's control-plane Node RPC pool"
  - before it lands: node_lifecycle__node_replace deleting the stopped predecessor answers 422 node-not-live on the branch and passes on main f2d33a9a (1 run, 34 s); the cut span under `All` becomes the connection's own delete span, the pool's via-hard-cut only in the stop's transport cut; via-observed carries the entry id and peer; `park` at node_self.rs:695 and the replace cell's name lose the retired word; ProcessNodeRpc::new loses its unused observer parameter; rshape_burn_in.rs:3415 stops reading removed spans; the gate build rebuilt
  - not s53's: probe_root_span's 1-in-40 dial timeout is R-QH1 (e12.s46): the failed probe's local port was 127.0.0.1:56958, the leaked `lo udp dport 56958 drop` chain; the four leaked RAFKA-* iptables chains (RAFKA-PART-690706-0, RAFKA-INB-692042-0, RAFKA-PART-692070-0, RAFKA-PART-691826-2; installers gone) were removed 2026-10-13 (Luke) and the dropped ports deliver again, so probe_root_span runs in the gate; e12.s46's kit (each fault an nftables table its installer owns) keeps a fault from leaking again
- [x] mesh-delete.md step 7 LANDED b488b828 (the fabric-primary publishes a deleted mesh's aggregate from its last admin's stop: NodeStopped then NodeDeleted; never for a live mesh; the view keeps a deleted mesh deleted; flame e3fb77ec7d572bd2ea2d7257b43760ef vs main 8ffa0cdd41888f6cd5054095b8a2133d); the seat move during the retention LANDED 783143d3 (2026-10-13, gate 114 s, its build 31.6 s wall): the per-process marks are gone, any admin holding the seat reads each deleted mesh's aggregate from its view and book (57dfb1c1), and a mesh the view names no more keeps its departures listed until the deleted retention ends (handover stem's mesh2.admin.1 kept publishing mesh1 after the view dropped it)
- [x] MeshLeft's refusal of the deleted mesh's republishes (reject.via-mesh-left) goes with e12.s72's MeshLeft deletion (landed b81f66a0, gate 71 s)

- [x] Q12 = A (Luke 2026-10-13; gossip.md order-key list updated; landed 4d6d0497, RED c53a73eb, gate 77 s): the Members publisher reads its stamp inside the version lock, one critical section with `topology_version` and `snapshot_id` (membership.rs `publish`: `sent` is read under `next_seq`). Two publishes, the round loop (`publish_every`) and the admin's event-driven calls (admin.rs:3169, :4877, …), interleave and with the stamp under the lock both versions are stamped and installed in order (cell `membership::tests::interleaved_backbone_publishes_and_neighbour_up_replay_stamp_and_forward_in_version_order` trace `http://127.0.0.1:16686/trace/5716e3d0c404b73782c246a0b2ed2d4d` spans `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=b91eb571037da31e rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=1 rdm.target_version=2` and `rdm.gossip.topology.delta.receive.done.ok.via-forwarded-delta trace_id=5716e3d0c404b73782c246a0b2ed2d4d span_id=da0a78724a2e9f44 rdm.node=mesh1.rpc.1 rdm.mesh=mesh1 rdm.base_version=2 rdm.target_version=3`; live `rejoin_commander` 5/5 green)

- [x] R-RX3 gossip.md:201 (docs updated 2026-10-13 on Luke's decision; code landed 4d6d0497 / 00c9f9e9): one held version per source whichever channel or read it arrives by; the topology-read install advances the held version, so a frame older than a read-installed snapshot is ignored with the ignore span (`topology_read::a_topology_read_install_advances_the_held_version_so_an_older_gossip_frame_is_ignored`)
### Acceptance criteria

<!-- crit:e12.s53.1 -->
- [x] no receiver span refuses by sender
<!-- /crit:e12.s53.1 -->
<!-- crit:e12.s53.2 -->
- [x] out-of-order frames ignored only in receive_order.rs
<!-- /crit:e12.s53.2 -->
<!-- crit:e12.s53.3 -->
- [x] the fast gate green
<!-- /crit:e12.s53.3 -->
<!-- crit:e12.s53.4 -->
- [x] a live flame of a seat move with no refused frame
<!-- /crit:e12.s53.4 -->
<!-- crit:e12.s53.5 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s53.5 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s53 -->
<!-- epic:e12.s55 -->
## e12.s55 — parked post-beta: R-HB3 (digest_seq as evidence on four 0x1B replies)

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** none (the #3085 reference never resolved; parked post-beta, this row is the record) · **Architecture:** docs/plans/proposal/node-status-heartbeat-connections.md §B

### State

parked post-beta (Luke 2026-10-11); not seated. (The due-time reconnect reconciler Q7 and R-CN1 Q6 from the same connections proposal landed on main in b00db084 / 878d4473 / d4ef297c, 2026-10-14, gate 102 s, build 67.2 s wall, RED 2d746961 / 0a795fbd; traces 9df1542e9a12dfc0cad3f459be21d74d, a49ad091afd37c5ec1507a860294e548, 0c65e852045816acf1deb69d2bb6612a; tracked under e12.s47 and e12.s53.)

### Remaining

- [ ] none named

### Acceptance criteria

<!-- crit:e12.s55.1 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s55.1 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s55 -->
<!-- epic:e12.s56 -->
## e12.s56 — replace is Rafka's recipe (RDM's replace path deleted); the investigation arms that create a node-admin

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3087 (generated once; not read back) · **Architecture:** lifecycles/node-replace.md; build.md (BuildChange::Recipe); lifecycles/mesh-investigation.md steps 5-7 (members-live and fully-crashed arms); rulings R-BI1, R-DG1, R-ML3

### State

landed 08948db5 / 7cf280d9 / 49bf704b / 9bd1a1d8 / 2f7f85b4 / 7b65e7d3 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`; merging c2352312 parts 1-7 and parts 8-10 2db14fa1 / f843f14a / d67e2fb1 / de4bb246): recipe carrier, RDM's replace path deleted, the verdict's one-step Create recipe, the fully-crashed arm with its proof read in the drive covering every node of the mesh (`investigate::tests::an_unheard_mesh_with_exited_admins_and_a_running_member_holds_then_opens_create_when_all_exit`, `rdm.node_admin.mesh.update.via-crash-proof`), R-DG1 degraded set on both the mesh and the fabric when the fabric-primary gives a mesh a new node-admin on either arm and cleared by the new mesh-primary's report (`investigate::tests::both_investigation_arms_mark_mesh_and_fabric_degraded_until_the_new_primary_reports`), R-BI1 each topology operation (`node.create`, `node.delete`, the replace recipe, `mesh.delete`, `fabric.delete`) mints its own Build, `first_admin` rule, build-drive turn held across `via-drive` span close (7cf280d9), and `node_replace` 15/15, `sticky_seat` 5/5, `fabric_primary`, `mesh_recover`, `2803_detect` green at gate cadence

### Remaining

- [x] the crash proof covers every node of the mesh, not only its node-admins (mesh-investigation.md step 5, "Fully crashed": "Every node of mesh-x, its node-admins included, has crashed ... The fabric-primary acts on the provider's exact exit proof"). On c2352312 `crash_proven` (build_drive.rs ~721) and `crashed_peer_mesh` (admin.rs ~2371) read only `m.cohort(NodeKind::NodeAdmin)`, so a mesh whose node-admins exited while a member still runs gets a node-admin created in process; that member's mesh is the members-live arm (carrier-edge-lost only, else hold). Each node the accepted topology names is proven Exited by its exact birth, or NeverLaunched by the provider find when it has no birth; any other answer holds the mesh, named on `rdm.node_admin.mesh.update.via-crash-proof` (landed f843f14a / 08948db5, RED d67e2fb1, cell `investigate::tests::an_unheard_mesh_with_exited_admins_and_a_running_member_holds_then_opens_create_when_all_exit`)
- [x] R-DG1: degraded on BOTH the mesh and the fabric when the fabric-primary gives a mesh a new node-admin, on either arm, cleared by the new mesh-primary's report (landed 2db14fa1 / 08948db5, RED de4bb246, cell `investigate::tests::both_investigation_arms_mark_mesh_and_fabric_degraded_until_the_new_primary_reports`)
- [x] R-BI1: node.create, node.delete, the replace recipe, mesh.delete and fabric.delete each mint their own Build (build_op.rs:112-129, http.rs open_with; landed c2352312 / 08948db5)

### Acceptance criteria

<!-- crit:e12.s56.1 -->
- [x] node_replace 15, sticky_seat 5, fabric_primary, mesh_recover, 2803_detect green at gate cadence
<!-- /crit:e12.s56.1 -->
<!-- crit:e12.s56.2 -->
- [x] degraded set and cleared on both arms in a deterministic cell
<!-- /crit:e12.s56.2 -->
<!-- crit:e12.s56.4 -->
- [x] a deterministic cell: an unheard peer mesh whose node-admins all exited while one member still runs gets no node-admin created in process and holds, its via-crash-proof span naming the running member; with that member also exited, the create opens
<!-- /crit:e12.s56.4 -->
<!-- crit:e12.s56.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s56.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s56 -->
<!-- epic:e12.s57 -->
## e12.s57 — node recovery = node.delete + node.create + node.update copying caller-owned meta

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3088 (generated once; not read back) · **Architecture:** lifecycles/node-recovery.md; lifecycles/node-update.md; fabric-recovery.md (a relaunched node-admin is a new node.id)

### State

absorbed s33 (node recovery landed 4a48eaae) and s62 (restart one op landed 8ceb6b3d). Reader conformance (`wire.rs` refuses unknown output fields by name; RED 6c58a80a) landed 3be2092c. Relaunched node-admin recovery identity, one start path (`relaunch_exit_proof` / `RECOVERY_PULL_WITHIN` wait deleted), Q4/Q4a "Nothing alive" provider-confirmed exit proof, and the four `store_root_boot` replacement cells (`a_relaunched_admin_mints_a_new_node_id_and_recovers_its_inherited_birth`, `a_relaunched_admin_with_every_node_proven_exited_is_fabric_primary_from_start`, `a_relaunched_admin_on_a_root_holding_ready_knows_at_boot_the_fabric_first_reached_it`, `a_relaunched_admin_on_a_root_in_state_commit_does_not_know_it_reached_ready`) LANDED 95e10533 / 75219e6b (2026-10-13, gate 83 s, build 67.3 s wall; RED d6be024b; trace 893deceb77302425afbee48d97dcb764); `node_replace` and `mesh_recover` green at gate cadence (152cb4fd, f0523ca8 gate 148 s). Left: the two `i143_acceptance_2803` `fabric_recovery` cells (Category B in `FP_KILL_SKIP_ARGS`, pending Luke's call)

### Remaining

- [x] superseded 2026-10-12: `otel_debug` is call context, not a node property (Luke); recovery's step 6 goes with node.update (proposal node-properties-own-commands.md §8); nothing is carried between node.delete and node.create but the config seeded at the claim. Was: (superseded by the R-BI8 row once Luke says yes: step 6 goes) node[2].update copies the exited node's caller-owned meta as a blind put from the predecessor's Deleted row; the step's receipt carries no output
- [x] the admin boot restart mode still reuses the old node id and sets supersedes (admin.rs ~3740-3775); a relaunched node-admin is "a new node over its inherited data dir: new node.id and incarnation, same path.name and endpoint key, new port" (node-admin-lifecycle.md :75, :110), with no supersedes (node-recovery.md "Lineage"). Built on `i143-recovery-identity` @e581cda1 and LANDED 95e10533 / 75219e6b (2026-10-13, gate 83 s, build 67.3 s wall; RED d6be024b): the start on an inherited root mints a new node.id (admin.rs:5111), `rdm.node_admin.node.create.via-inherited-root`, the inherited birth written Deleted after its provider exit proof then the path.name claimed as a recovery seeded from it (node_rows.rs:737 `take_inherited_name`); trace 893deceb77302425afbee48d97dcb764; store_root_boot 6/6, node_restart + node_stop 10/10
- [x] one start for every node (Luke 2026-10-12: "fp mp node they all start the same way, fp mp have a couple of extra steps"): the boot-time exit-proof check of an old birth on the same data dir (`runtime_proof`, `relaunch_exit_proof`) is a second start path and code for "the fabric-primary died"; it goes. The old birth that does not answer is node recovery's (fabric-recovery.md: "node recovery for non-answers; exact exit proof required"; landed 95e10533 / 75219e6b, gate 83 s)
- [x] i143_acceptance_2803 `fabric_recovery_restarts_one_admin_and_restores_every_missing_admin` and `fabric_recovery_with_no_running_node_adopts_the_holders_own_clock` run node.fabric-primary.update first and take the former fabric-primary's admin down only after the confirmed transfer (Luke 2026-10-12): `hand_fabric_seat_over` (cohort_recovery.rs) stops the fabric-primary's admin by a Build whose first step moves its seats to its same-mesh successor, and waits for the successor's view to name it the fabric-primary; only then is every node-admin lost, and the person relaunches one on the current fabric-primary's data dir
- [x] superseded 2026-10-12: `otel_debug` is call context, not a node property (Luke); recovery's step 6 goes with node.update (proposal node-properties-own-commands.md §8); nothing is carried between node.delete and node.create but the config seeded at the claim. Was: R-BI8 (awaiting Luke's yes; a1 and the orchestrator recommend B, proposal `build-enitty-updates-plus-otherstuff.md` Part D): the claim at step 4 seeds config AND the caller-owned meta group from the Deleted row in one row replacement; recovery's step 6 goes; `NodeRow` grows the caller-owned meta group (`durable format moved`, unrecognised row refused by name); `meta_of`'s hardcoded `otel_debug: false` (node_update.rs:18) goes; the create carries the inherited level into the launch before FirstStart; replace inherits the same way; cells: interruption before and after the claim, executor replacement, repeated claim, each asserting the row value and otel_debug at FirstStart
- [x] reader conformance, separate from R-BI8: wire.rs ~511 `typed()` drops an output's unknown fields instead of refusing by name; the decoder refuses unknown fields naming the field; a Build-topic decoder change, so it lands with its flame (landed 3be2092c, RED 6c58a80a)

- [x] Q4/Q4a (docs updated 2026-10-13 on Luke's decision; code landed 95e10533 / 75219e6b, gate 83 s): origin-ordering.md and fabric-recovery.md "Nothing alive": fabric recovery is nothing alive, every node the store names confirmed gone by the provider (the old process on the inherited dir included), never by a wait; the launched admin is the only node, the fabric-primary, and its clock is rafka-time from the start; if any node answers, it takes rafka-time from the existing fabric-primary. Code: the own-clock branch keys on that provider confirmation (no new check); the root's RECOVERY_PULL_WITHIN wait goes; the inherited birth is written Deleted only after its provider exit proof (node recovery's node.delete)
### Acceptance criteria

<!-- crit:e12.s57.1 -->
- [x] (superseded 2026-10-12 with R-BI8 / step 6 removal: recovery seeds config at the claim) a recovered node carries its predecessor's caller-owned meta (node.update:<path.name> receipt, blind put) and config seeded from the Deleted row
<!-- /crit:e12.s57.1 -->
<!-- crit:e12.s57.2 -->
- [x] a relaunched node-admin has a new node.id and incarnation, the same path.name and endpoint key, and no supersedes
<!-- /crit:e12.s57.2 -->
<!-- crit:e12.s57.3 -->
- [x] node_replace and mesh_recover green
<!-- /crit:e12.s57.3 -->
<!-- crit:e12.s57.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s57.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s57 -->
<!-- epic:e12.s58 -->
## e12.s58 — mesh and fabric name claims; the Build is its own entity and the current Build is read LIFO

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3089 (generated once; not read back) · **Architecture:** node-identity-and-state.md §2.7-§2.8; build.md; rulings R-MN1, R-BI1, R-BI3 and the "Build is its own entity" direction

### State

The fabric_states__rounds red (no climb of the second Build) LANDED 6007ca6f (2026-10-13, gate 71 s): the fabric-primary put the fabric back to pending one status-loop turn after accepting a resize, so `fabric.get` answered ready under a Build never climbed; the reset now runs at the acceptance before the door answers (control_plane.rs:435 `resize` hook, fabric_rounds.rs:448 `accepted`, states.md "A resize or roll puts the fabric back to pending"); RED ace5e565; GREEN trace f1ab087f3bc4a2634feb8042c1199ac8 (reset .7337 s, door .7355 s); 8/8. landed 671d84fd, 500f610a, 231ec2a2, 9a414010, 95f01ece (MeshId at acceptance, create-mesh receipt, typed build_op cells, Leaving kept in the view, a delete's NodeDeleted step never skipped); landed a8b5f7eb (parts 6-8): R-BI3 (the current Build read last in, first out; no Fabric pointer rows; failed skipped; a once-completed Build stays current), R-BI4 (a lost participant hands the attempt back, the Build stays in progress), fabric.get answers current_build(), the 2780/2782 accept cuts on the one durable write, the telemetry flush counting a span only once it is queued, the leave wait ending on the final primary's exit proof (stems 141 s -> ~25 s), a deleting mesh gets a created node-admin only when no admin survives by exit proof, R-WF1 (WireFabric.build_id removed; 64 -> 34 bytes, traces d8ba2a35a7d3010704eccf449ebb5a9c / 5d8f74ea452eac1b1d1667a775b61285). Landed e8cd3ae1 (2026-10-13, gate 82 s) after s53: R-BI5 Crockford ids (BuildId, DeploymentId, IncarnationId; durable format moved), the probe refuses a view entry it cannot read naming node, field, value and parse error. `mesh.delete`'s lost report (`DeleteMesh`/`MeshDeleted` replay on re-admission) LANDED 9ec1b13b / 26227bcb (2026-10-13, gate 102 s, build 67.6 s wall; RED 07d0968e, GREEN 85fff0c9; cell `mesh_delete::tests::a_lost_mesh_deleted_report_is_replayed_to_the_next_attempt_and_resolves_it_exactly_once`). R-BI6 (`fabric/builds/` on the shared store root, `meshes/<mesh>/builds/` for a mesh lane's receipts, per-admin journal and `store_root.rs` exclusion deleted) LANDED db60161f / 275f9937 / e245ba90 (2026-10-13, gate 77 s, build 21.8 s wall; RED b97092ef; cell `store_root_boot::a_relaunched_admin_reads_its_build_from_the_shared_store_root_with_no_local_builds_dir`), and the single seat-source fix (`holds_fabric` and `holds_mesh` reading only `membership.book.seats`, removing `!launched` and `seats.has_publisher()`, with Nothing-Alive recovery loading once before locking `journal` to `membership.book.seats`) LANDED c01cb3ef (2026-10-14, gate 149 s, `TIMINGS wall=74.6s units=455 max_concurrency=10 unit_sum=208s`). R-BI5a (`BuildId::log()` -> `BuildIdLog` rendering `bid_xxxx-xxxx-xxxx` in `build_id` span fields while `Display`, durable row keys, stored `BuildFact` rows and `0x20` wire strings stay bare 12-character Crockford60; `BuildId::parse("bid_...")` refused by name; `node-identity-and-state.md` §2.0 updated on Luke's yes) LANDED 55131ee4 (2026-10-14, gate 46 s; RED 8f0e64d0; cell `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans`; live Jaeger trace `a592b745aaea8819ba14a5777842e24b`). Left: none
The `rounds.rs:201` red (`a_two_mesh_fabric_of_role_nodes_is_ready_only_through_the_commit_state_and_open_traffic_rounds`, "the application is called once per round of state-sync", left 2; gate `e4s2-604f6d1c`, kept `gate-gates-e4s2-604f6d1c/artifacts/fabric-states/…`): a product defect, not a re-run the page allows. The two-mesh Build was accepted 0.6 ms after the one-mesh Build's climb entered state-sync (`via-accepted` span `26b9c6a297b63ecd`). `resized()` (fabric_rounds.rs) cannot put the fabric back until the state it leaves went out (`may_advance`), and it answered "not resized", so `step_state_sync` called `sync_state` for the two-mesh Build at attempt 0, with no attempt claimed and mesh2 not ready, inside the one-mesh climb (span `9962960417b0f787`, trace `9449f79f…`). Once state-sync went out the reset came (`via-state-reset` `cf4de489821c4218`), and the two-mesh Build's own round called the application again (attempt 2, span `028747f16c582de1`). fabric-state-sync.md's sequence is "all planned meshes ready; enter state-sync", then "state-sync hook (round + trace context)": the first call was no round of that Build. Fix: `step_state_sync` and `step_state_commit` act on nothing of a Build whose topology is not the one the climb holds (`topology_moved`), so the reset waits for the state to go out and no round of the new Build runs inside the old one's climb; LANDED `20176c23`. RED `198950a0` (`fabric_rounds::tests::a_topology_accepted_before_state_sync_went_out_holds_the_climb_and_asks_the_application_nothing`: a sync_state round opened for (amrrv6tggykq, 0)); GREEN rdm-admin lib 470/470, `rounds::` 8/8 three times under the turn lock. Also seen: `step_state_sync` names the current Build's attempt even when it is 0, which `step_pending` refuses ("no-attempt-claimed"). With the hold above no run reaches it; not changed.

### Remaining

- [x] R-BI6 = x (Luke 2026-10-12; landed db60161f / 275f9937 / e245ba90, gate 77 s, RED b97092ef, and single seat-source fix c01cb3ef, gate 149 s): the Build journal's location is the adapter's and provider's choice; RDM's file adapter keeps it on the fabric's shared store root (`fabric/builds/` under the writer ladder, the fabric-primary its writer, and `meshes/<mesh>/builds/` for a mesh lane's receipts) so a successor reads it there; one fact gets one source (`membership.book.seats`); the per-admin journal and `store_root.rs`'s exclusion of it go; build.md:371 states the choice and promises nothing beyond the adapter's storage (proposal A6)
- [x] (on main at 87df0a35: `mint_crockford60`, rafka-mesh-entity ids.rs:50/:116; `bld-` only in the cells that refuse it, ids.rs:260, client lib.rs:204, tests/ids_crockford.rs:33) R-BI5 (Luke 2026-10-12: "build-id is a crockford id, we only have one id type, never a hex"): `BuildId::mint` (build.rs:26-32, `bld-<hex>`), `DeploymentId::mint` (model.rs:12-18) and the `opaque_id!` minters (rafka-mesh-entity ids.rs:178-204, IncarnationId and a minted EndpointId) mint Crockford60 through `mint_crockford60` and parse with its refusal by name; iroh's own EndpointId untouched; the `rafka-node-admin-client` `BuildId` (lib.rs:39) parses the same; a stored `bld-` id is refused by name, so the landing is `durable format moved`; build.md:196-198 replaced on Luke's yes (2026-10-14) with the proposal's text (one id type, bare 12-character Crockford60; transition ids, provider handles and control-domain values are not ids)
- [x] mesh.delete's lost report (Luke 2026-10-13, replacing "HANDOFF_WAIT goes"; landed 9ec1b13b / 26227bcb, gate 102 s, RED 07d0968e, GREEN 85fff0c9): the bounded wait for mesh.deleted stays (MESH_DELETED_WAIT; on expiry the attempt ends incomplete, HandedOff, receipts standing); re-admitting DeleteMesh replays the recorded MeshDeleted report naming the attempt its manifest was recorded under, with no member workflow run again; LeaveMesh -> DeleteMesh, MeshLeave -> MeshDeleted with the module, types, handlers, spans, stems and doc lines (durable format moved; cell `mesh_delete::tests::a_lost_mesh_deleted_report_is_replayed_to_the_next_attempt_and_resolves_it_exactly_once`). The MeshLeaving/MeshLeave/MeshLeft gossip frames, their hooks and spans are untouched (e12.s72)
- [x] R-BI6 mesh-lane receipts answered and landed in db60161f / 275f9937: RDM's mesh lanes (R-BI7) write their Build journal under `meshes/<mesh>/builds/` on the shared store root under the mesh-primary writer check while the fabric lane writes under `fabric/builds/`
- [x] the build.md / pointer doc sweep (A2): `grep -rn "Fabric.build_id\|FabricPointer" docs/architecture` returns only the no-pointer sentences; build.md:196-198 replaced and the six `bld-` example ids (node-rpc-envelope.md:364, mesh-delete.md:114/:138, node-mesh-leave.md:59, node-mesh-join.md:94/:155) made canonical 12-character Crockford60 on Luke's yes (2026-10-14)
- [x] R-BI5a (Luke 2026-10-14, "i agree"; landed 55131ee4, gate 46 s, RED 8f0e64d0; cell `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans`; live Jaeger trace `a592b745aaea8819ba14a5777842e24b`, span `c696c67b348c3a5e`): `BuildId::log(&self) -> BuildIdLog<'_>` (`rafka-mesh-entity/src/ids.rs:193-212`) renders `bid_xxxx-xxxx-xxxx` across `build_id = %<expr>.log()` span fields where `<expr>` is a `BuildId` (`rafka-node-admin-client`, `rafka-node-admin-core`), while `Display` (`ids.rs:151`), durable row keys (`node.primary.update:{build_id}:{run}:{op_key}`), stored `BuildFact` rows, `0x20` wire strings and page examples stay the bare 12 characters, and `BuildId::parse("bid_...")` refuses by name (`WrongWidth` / `NonCanonical`). Evidence-reader decisions: crate integration tests (`rafka-node-admin-core/tests/{build_facts_read,build_takeover,drift_convergence,node_objects,receipt_refusal,recovery_admission,store_root_boot}.rs`, `rafka-node-rpc-testkit/tests/{common/mod,node_delete_pipeline,recover_pipeline}.rs`) match `build_id.log().to_string()`; scenario JSONL readers (`Estate::spans` in `rafka-test-scenario/src/estate.rs:794-799` and `spans_of` in `rafka-test-scenario/tests/i143_acceptance_2777.rs:53-58`) normalize `bid_xxxx-xxxx-xxxx` on `fields.build_id` to bare 12-char for scenario REST/RPC string comparisons while on-disk `.spans.jsonl` files keep `bid_xxxx-xxxx-xxxx`; `/home/admin/rafka-v2/scripts` has no span `build_id` readers; `/home/admin/rafka-v2` has one `build_id = %` hit at `crates/rafka-entity-topology/src/builds.rs:346` (`build_id: &str` on `rafka.node_admin.build.resolve.via-build-fact-index`, untouched); `docs/architecture/node-identity-and-state.md:107-108` (§2.0) updated on Luke's yes (2026-10-15, to the exact sentence)
- [x] the gate of docs-only tree b3022dce went red on `node_lifecycle__node_restart::rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state` (canary.rs:157: no `NodeRestarting` step span under the restart call's attempt; the detail log shows that attempt ending `handed-off`), the one red of that stem in 645 gate runs on this box; the tree changed no code, so the red is main's (R-LD1) and the docs landed as a fast-forward. The read is its own RED-first landing with a flame, two candidate roots: the cell assumes the restart runs under `restart.attempt` while a cohort seat holder's restart runs `node.primary.update` under its own attempt first and the restart under the next; or the restart reply should name the attempt that executed it (a product question). Never a cell edit inside another landing. R-CY1 (Luke 2026-10-15): the read is block e12.s84 (Root 1 confirmed; RED 368d110a; trace `ef78ccae756293f65263737ffab36200`)

### Acceptance criteria

- [x] build_op, fabric_primary_handover, sticky_seat, fabric_states__rounds, accepted_topology green
- [x] a successor reads the in-progress Build and resumes it
- [x] a failed newest Build is skipped
- [x] The story bar holds (section story-bar).

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s58 -->
<!-- epic:e12.s59 -->
## e12.s59 — fabric.delete replaces FabricShutdown

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3090 (generated once; not read back) · **Architecture:** lifecycles/fabric-delete.md; lifecycles/mesh-investigation.md "During fabric.delete"; rulings R-FS1

### State

landed 45405284 (2026-10-14, gate 148 s, `TIMINGS wall=70.1s units=455 max_concurrency=11 unit_sum=188s`; RED 568a28eb): `FabricShutdown` deleted (`shutdown.rs`, the `Draining` freeze, `hold_seats`, `shutdown.json`, the `shutdown` field in the Build-topic message and the join answer); `fabric.delete` implemented per `lifecycles/fabric-delete.md` (`BuildChange::DeleteFabric`, `fabric_delete.rs`); every mesh other than the fabric-primary's is deleted by `mesh.delete` or reached through the one mesh investigation (`R-FS1`: answering admin runs `mesh.delete`; members live -> fabric-primary creates one node-admin that runs `mesh.delete`; every node proven exited -> rows written deleted on proof; unproven -> refused naming the unproven primary on `rdm.node_admin.mesh.reject.via-unproven-primary`); the fabric-primary's own mesh deletes its non-fabric-primary nodes first, writes the mesh and fabric rows `Deleted` (`rdm.node_admin.fabric.delete.via-build`), and only then runs the fabric-primary's own `node.delete` as the final step (cells `fabric_delete::fabric_delete_leaves_no_node_mesh_or_fabric_row_and_fabric_primary_awaits_its_own_node_delete` trace ac5d0d0751fac815568a3bd72aa7666d and `fabric_delete::unanswered_mesh_during_fabric_delete_creates_one_admin_when_members_live_or_deletes_rows_on_crash_proof_and_refuses_unproven` trace c97dc9dce2ed878fca1c24632fa26d45; `mesh-audit` resealed)

### Remaining

- [x] delete FabricShutdown (shutdown.rs, the Draining freeze, hold_seats, shutdown.json, the `shutdown` field in the Build-topic message and the join answer); fabric.delete per its page; an unanswered mesh is reached by the one mesh investigation (R-FS1 revised, docs landed): a node-admin that answers runs mesh.delete; members live -> the fabric-primary creates a node-admin that runs it; every node proven exited -> rows written deleted on proof; held -> named in the failure; the fabric-primary never runs node.delete on a mesh's nodes directly. mesh.delete of the fabric-primary's mesh calls `node.fabric-primary.update` with that mesh excluded (landed 45405284, RED 568a28eb, gate 148 s; traces ac5d0d0751fac815568a3bd72aa7666d and c97dc9dce2ed878fca1c24632fa26d45)

### Acceptance criteria

<!-- crit:e12.s59.1 -->
- [x] fabric.delete of a 2-mesh fabric leaves no node, mesh or fabric row
<!-- /crit:e12.s59.1 -->
<!-- crit:e12.s59.2 -->
- [x] the unanswered-mesh arm deletes by node.delete
<!-- /crit:e12.s59.2 -->
<!-- crit:e12.s59.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s59.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s59 -->
<!-- epic:e12.s60 -->
## e12.s60 — REST out of RDM (step 0); entity tags as their own property

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3091 (generated once; not read back) · **Architecture:** docs/plans/proposal/ops-naming-proposal.md "RDM has no REST" and step 0; node-rpc-envelope.md typed-object contract (node.get, node.tags.*); rulings R-TG2, R-TG2a, R-TG3, R-OW1

### State

landed 207172f5, 98b5fcb5 (node.get/mesh.get/fabric.get/connections.get over Status; tags field; admin_api_base no longer gates Ready). The 2803 recovery fix (567f3234, beee5a42: DigestBook::adopt_status at seat take) landed with the RX removal 01a65889. Landed f5ca6ab4 (parts 2b1-2b3): typed Estate, 24 acceptance files on the typed objects (2780/2782 over the shared 0x1F read, tests/build_log.rs), FabricView.build_id answered from the fabric record, six stale cells, build.create's RemoveNode of a deleted mesh's last admin; landed f2d33a9a (part 3): 12 more scenario files off REST and polling (accepted_topology, canonical_ids, node_stop, mesh_create, node_delete, proof_store, await_attempt, node_drain, restart_fence, admin_restart, shape_reconcile, role_wedge), the hydration wedge family's cell restored on a topology-read cut (2782, trace c07791bd34ff3a0a41f094c6bc7e61fc), build.delete of a history Build streams Started then Deleted (was a 424 reset), 2775 asserts a restart keeps its incarnation and awaits the fabric's first ready. Part 4 landed 47a9a6dc (2026-10-13, gate 84 s): accepted_topology awaits the late admin's completion fact, admin_restart anchors on the frame's version, proof_store submits the replace after the recovery attempt completed. Parts 6-9 (layout step 3 part A) landed 4c6be135 (2026-10-13, gate 153 s): canary, runner, soak, mesh_membership__backbone, mesh_leave_resume, admin-core fabric_not_ready/provider_policy, estate start/stop over fabric.get, rshape_burn_in off REST ("/api/ 364 → 238), a restart keeps its birth in rshape (C11, C18 rebuilt on node recovery), a non-fabric-primary admin's Node RPC Build refusal spanned; the estate's handover landed 71af3368; the estate's stop landed c7368e46 (2026-10-13, gate 114 s, its build 52.4 s wall): SIGTERM to every process, runtime and container of the estate, then a named kill past the bound, in place of the fabric-primary's POST /api/shutdown loop and the drop's raw POST; part 11 LANDED 6fbfe353 (2026-10-13, gate 84 s, its build 17.6 s wall): `Estate::enter(&PathName)` sets the entry by name, and poc_chaos_kit, late_authority, successor_adoption, mesh_recover, container_kill, fabric_primary, sticky_seat and fabric_primary_handover run on the typed objects (their `"/api/` sites 54 -> 0); parts 12+13 LANDED 9634d9c2 (2026-10-13, gate 140 s, its build 73.0 s wall): a cell follows the Build it started and no submit is refused and resubmitted (submit_change, Estate::submit; the estate's birth follows its Day-0 Build), node_replace 24 -> 0 `"/api/`, build_op to settles_to, the estate's REST methods and `http` field deleted (the scenario crate's `"/api/` is 0; `axum|"/api/` over crates 208 -> 122), node.get answers a node's config (NodeView.config, durable format moved; the config cell's two assertions back verbatim, RED f2d426f4, GREEN 1fcc2d9f); part 14 LANDED d2bbb305 (2026-10-13, gate 137 s, its build 70.5 s wall): the fault door is the testkit op 0x75 `fault-door` (arm, release, state; CutSpec crosses the wire through a serde mirror, RED 7eeaf1d0 GREEN 9776c45f), served through `Wiring.serve_app`, the 12 `Door::open` sites call the admin by path.name, the testkit's axum router and the scenario crate's reqwest deleted (`axum|"/api/` over crates 122 -> 115); part 15 LANDED 6e53b469 (2026-10-13, gate 146 s, its build 84.5 s wall): a member whose delete finished under the lost primary is published NodeDeleted by the amended primary from its finished delete's receipt (RED d347e271, GREEN 0466b5bc; live trace 9aee82adc25dc8156afe9344eafba656); the REST cut LANDED e2549838 / ac630d82 / bb265526 (2026-10-13, layout step 3 done for the crates: `grep -rn 'axum\|"/api/' crates` returns nothing): http.rs is control_plane.rs, the Build door's control plane only, its 14 routes, router, handlers and HTTP rendering of refusals deleted; the admin binds no control listener and its application contact `admin_api_base` is `rdm://<node id>`; NodeAdminClient and reqwest leave admin-client, reqwest leaves the testkit, axum/tower/http-body-util leave admin-core; control_routes.rs and client_contract.rs deleted with the routes; the probe enters at an admin's birth over Node RPC (ac630d82); `Estate::admin` retyped to the entry admin's `PathName` LANDED 748ef8f0 / 895c956d (2026-10-13, gate 101 s, build 17.3 s wall; `d1c9abe8`, `a6a7f1bc`)

### Remaining

- [x] 2782's pointer cells (`accept:build-durable-before-pointer`, `pointer:fabric-record-write`, FabricPointerCatchUp) cut the Fabric pointer, which R-BI3 deletes: they move to the Build's one durable accept write in the same landing as the R-BI3 code (e12.s58; landed a8b5f7eb)
- [x] the other scenario families onto the typed objects (landed f5ca6ab4, f2d33a9a, 47a9a6dc, 4c6be135, 6fbfe353, 9634d9c2, d2bbb305, e2549838, 748ef8f0)
- [x] part 4 (dba547a4, 14 files off REST) gate red, tree recorded: all three reds are main's cells, none in the branch's files, each green on the 24 gates before. Fixed on the branch in part 5 (landed 47a9a6dc):
  - fabric_build__accepted_topology.rs:110 reads the late admin's Build once, 50 ms after the create stream's terminal (FP 65704, read 65754), and finds attempt 1 running; the completion facts reach mesh1.admin.3 by 65866 (14 facts against 7). Build facts are broadcast once on append and absorbed by every admin, and progress waits on no gossip (fabric_builds.rs:5-15, :516-519), so the read awaits the fact with build_log::build_fact as step 1 does. Telemetry gap: absorbing a Build fact emits no span, so the arrival is not in the trace
  - node_lifecycle__admin_restart.rs:123 anchors "after the publish" on rdm.node_admin.node.update.via-node-restarting, which opens 34 ms after the publish it names: the step's receipt puts the hold into the book (membership via-node-restarting +0), the aggregate bumps to topology_version 8 (+15.6 ms) and forwards, and the frame carries that version (gossip.md:163), published at +34 ms (admin.rs:3468, pipeline.rs:1851-1852). The cell's bumped/installed/forwarded checks pass only on later unrelated spans; they anchor on the frame's topology_version (trace 7e135378efebf79f2685e3995e22e15a)
- [x] delete REST from node-admin-core, Peers::nodes, the demo connections view (landed e2549838 / ac630d82 / bb265526 / 748ef8f0)
- [x] R-TG2/R-TG3: <entity>.tags.get / create { label | [labels] } / delete { All | [labels] }, one keyed row per label (no duplicates; creating an existing label is ignored and named), grammar 1-64 of a-z 0-9 - _ . ~, no leading '.', max 32; UpdateNode drops its tags field; FabricView.build_id becomes the R-BI3 current Build (landed 207172f5, 98b5fcb5, a8b5f7eb)
- [x] rshape_burn_in.rs still asserts a new incarnation after a restart (node-restart.md:13: same node.id and incarnation): `Until::Reborn` (:1542, :1767) never holds after a restart; sites :1127/:1155, :1871/:1885, :1920, :2322, :2523, :2866, :2946, :3922, :4637 (release-only); a supersession cell needs a node recovery, not a restart (landed 4c6be135)

### Acceptance criteria

<!-- crit:e12.s60.1 -->
- [x] `grep -rn 'Router|axum|"/api/' crates/rafka-node-admin*` returns nothing
<!-- /crit:e12.s60.1 -->
<!-- crit:e12.s60.2 -->
- [x] every migrated file green at gate cadence
<!-- /crit:e12.s60.2 -->
<!-- crit:e12.s60.3 -->
- [x] tags cells for create/get/delete/filter and the grammar refusals
<!-- /crit:e12.s60.3 -->
<!-- crit:e12.s60.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s60.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s60 -->
<!-- epic:e12.s69 -->
## e12.s69 — Rafka side: the entry pull moves off the rafka-mesh-ops projection frames; no cert check between mesh nodes (R-SV1)

**Builder:** Rafka engineer (rafka-v2; coordinate the landing with the dev lead) · **Tracking issue:** #3100 (generated once; not read back) · **Architecture:** gossip.md §8; fabric-node-hooks.md; fabric-certs.md#on-a-connection; internal-mesh-auth.md; rulings R-SV1

### State

moved 2026-10-17 to rafka-v2 `docs/plans/i142-node-rpc-migrations-prd.md` ("Moved from RDM i143"); the belt and accept loop are i145's; this block builds nothing. Was: not started; absorbed s70

### Remaining

- [x] rafka-mesh-ops tags 16/18/19 (ControlFrame::ProjectionPull / ProjectionSnapshotPart / ProjectionRefused; rafka-node-base serve_projections, read_projection_answer) move to node.topology.get. The SVID belt, Rafka's accept loop and Rafka's node-admin minting do not get deleted here: they move to RDM under the cert epic (`docs/plans/i145-cert-ownership-prd.md`, from `docs/plans/proposal/cert-verification-ownership.md`; Luke 2026-10-16 superseded R-SV1's sentence). Rafka repo: coordinate the landing with the dev lead **Moved** to rafka-v2 i142 (the entry pull, i145 e4).

### Acceptance criteria

<!-- crit:e12.s69.1 -->
- [x] a Rafka node enters through node.topology.get **Moved** to rafka-v2 i142 (the entry pull, i145 e4).
<!-- /crit:e12.s69.1 -->
<!-- crit:e12.s69.2 -->
- [x] no `rafka.mesh.auth.*` span on rafka-mesh-v1; the admission fact and span are RDM's (`node.connections.admit`) **Moved** to rafka-v2 i142 (the entry pull, i145 e4).
<!-- /crit:e12.s69.2 -->
<!-- crit:e12.s69.3 -->
- [x] the Rafka gate green **Moved** to rafka-v2 i142 (the entry pull, i145 e4).
<!-- /crit:e12.s69.3 -->
<!-- crit:e12.s69.4 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s69.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s69 -->
<!-- epic:e12.s72 -->
## e12.s72 — the gossip cut: Build step keys renamed to the op names; the gossip frames MeshLeaving / MeshLeave / MeshLeft deleted (a mesh is deleted, never left; node.mesh.leave's 0x1B `MeshLeft` reply is e12.s75's); MeshStatus strings; NodeLeft refused by name

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3103 (generated once; not read back) · **Architecture:** docs/plans/proposal/rdm-code-conformance.md §1 (step-key and frame rows); gossip.md §3.1; lifecycles/mesh-delete.md; rulings R-CR4, R-ML1

### State

LANDED b81f66a0 (2026-10-14, gate 71 s, `TIMINGS wall=29.5s units=455 max_concurrency=11 unit_sum=94s`): Build step keys renamed to the op names (`node.create:`, `node.connections.delete:`, `node.delete:`, `mesh.delete:`, `fabric.delete:`, `node.stop:`, `node.restart:`); gossip frames `Frame::MeshLeaving`, `Frame::MeshLeave`, and `Frame::MeshLeft` deleted along with `publish_mesh_leaving`/`publish_mesh_left` and `reject.via-mesh-left`; a deleted mesh's last admin ends its publishing with its final aggregate (`status = Leaving`, `in_flight: [node.delete:<last_admin>]`) and the fabric-primary's `stamp_deleted_mesh` / acting deleted-mesh aggregate path is removed (`deleted_mesh_aggregate.rs`); `NodeLeft` refused by name; `AdminHandle::leave` self-publication and 250ms linger removed; `Forwarder::source` advances version on empty delta when `version > basis.version + 1` so multi-version jumps after `in_flight` removal forward the post-delete member projection (`node_lifecycle__admin_restart`, `mesh_shapes__live_resize`)

### Remaining

- [x] the fabric-primary deletes a mesh by command; the mesh-primary does the mesh (mesh-delete.md :19, :24, steps 2 and 5-7): the fabric-primary sends `DeleteMesh` and accepts `MeshDeleted`, and publishes nothing for the mesh; `Frame::MeshLeaving`, `Frame::MeshLeave`, and `Frame::MeshLeft` are deleted (landed b81f66a0, gate 71 s)
- [x] a mesh's last admin ends its publishing with its final aggregate (Luke 2026-10-14; gossip.md "Mesh delete", mesh-delete.md step 7, R-NS1): when the fabric-primary's node.delete reaches it, its aggregate carries the mesh deleted and itself deleting, its last gossip; the fabric-primary publishes no NodeStopped, NodeDeleted or aggregate for that admin or its mesh (`stamp_deleted_mesh` removed), and receivers hold the mesh for the deleted retention from that aggregate (landed b81f66a0, gate 71 s; `deleted_mesh_aggregate.rs`)
- [x] all of the above as one landing (step keys ride lifecycle frames, so every receiver moves together; durable format moved; landed b81f66a0, gate 71 s)

- [x] evidence for Luke (2026-10-13, from the s60 estate landing's gate red at 030e7466, cell `mesh_lifecycle__fabric_primary_handover::deleting_the_fabric_primarys_mesh_hands_the_seat_over_first_then_deletes_it` :448; artifacts scratchpad/e13/s60estate-red/): resolved by deleting `Frame::MeshLeft` (`refuse_left_mesh`) and `stamp_deleted_mesh` in b81f66a0

### Acceptance criteria

<!-- crit:e12.s72.1 -->
- [x] every step key reads its op name
<!-- /crit:e12.s72.1 -->
<!-- crit:e12.s72.2 -->
- [x] no Mesh*Leav* frame exists
<!-- /crit:e12.s72.2 -->
<!-- crit:e12.s72.3 -->
- [x] the fast gate green
<!-- /crit:e12.s72.3 -->
<!-- crit:e12.s72.4 -->
- [x] a live flame
<!-- /crit:e12.s72.4 -->
<!-- crit:e12.s72.5 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s72.5 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s72 -->
<!-- epic:e12.s75 -->
## e12.s75 — workflows call primitives: one workflow module per lifecycle page, primitives below; the code-only renames; the comment-citation sweep

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3106 (generated once; not read back) · **Architecture:** docs/plans/proposal/rdm-code-conformance.md §0.1, §1 (code-only rows), §4; fabric-mesh-node-lifecycles.md (the index)

### State

absorbed s71 and s74. The first Node RPC frame is the target: landed 87df0a35 (2026-10-13, gate 86 s, build 11.3 s): `Fence { target_node_id, op }` → `Target { node_id, op }`, `RequestHeader::target`, `MAX_TARGET_BYTES`/`peek_target`, the framing doc's `target_len`/`target`, four test fns, the stem `node_rpc__restart_fence` → `node_rpc__restart_target`; postcard positional, no wire move; `grep -rnwi fence` in rafka-node-rpc-contract + rafka-node-rpc 100 → 0 (seven substring hits left, all `BuildReply::Fenced`, the seat epoch). The land script refuses a diff that adds a retired word in an identifier or file name, or an id-named test file (layout §4 name check). `SeatHolder.epoch` → `revision` (`SeatEpoch` → `SeatRevision`, `RejectedEpochOverflow` → `RejectedRevisionOverflow`) landed e7ffb680 / 933b0c61 (RED ad6af7ad). `UpdateNode`/`NodeUpdated` → `UpdateName`/`NameUpdated` landed e634b766 (RED b0f888b0). The code-only renames (`Forwarded`, `ConnectionsDeleting`, `SeatYielded`, `recovery_round`) LANDED 3eaae3e6 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`). `node.mesh.join` (`JoinMesh` / `MeshJoined` on `0x1B`), `node.mesh.leave` (`LeaveMesh` / `MeshLeft` and `BusyHoldingConnections` on `0x1B`), their `rdm.node.mesh.*` spans, the reserved status/frame renames (`Reserved6`, `Reserved7`, `Reserved2`, `NodeConnectionsDeleting`, `NodeConnectionsDeleted`), and regenerated `docs/reference/rdm/*.md` LANDED e34468a0 (2026-10-14, gate 68 s, `units=455`; RED 9f2f9b55; cell `connections_all::node_mesh_leave_and_node_mesh_join_standalone_and_inside_stop` traces 686542851fe55fd7a6974693a1bbd7ce, d995f8f05f1b7d536ed20fb9787771ec, bc5f29b816021eecb9b07343f7e2a2d5, 88b538196f2a62b70768c2b8b2cf42fb and `node_start_sequence::every_start_path_runs_the_same_start_steps_in_the_same_order` trace 51dd1e00f0b75c2b7f972d7c4c1ef34b). `AdminHandle::leave` gossip self-publication & linger removal LANDED b81f66a0 (2026-10-14, gate 71 s). `crates/rdm-admin/src/admin.rs` split into `admin/{records,projection,rows_writer,recovery,reconcile,session,runner,boot,mesh_delete}.rs`, `workflows/*.rs` and `primitives/mod.rs` with conformance §1 code-only renames (`delete_mesh`, `lead_delete`, `admin_for_deleting_mesh`, `create_deleting_mesh_admin`, `revise_predecessor`, `revise_exited`, `delete_stopped`, `start_stopped`, `stopped_birth*`, `delete_node`, `clear_unwatched`, `DeleteRequest`, `DeleteStep`) LANDED 18cbfc5f (2026-10-15, gate 66 s). Left: `claim_revision` (coordinated with rafka-v2)


RDM admission API rename: `NodeRpcServer::begin_connections_delete()` and all ten current callers replace the retired server method. The switch, refusal and resume semantics and every wire byte stand. Compile RED `cb877dac` (`/home/admin/rust-distributed-mesh-deploy/s75-server-rename-red.log`); the existing connections-deleting refusal, Forward, stop/start and handover cells exercise the renamed calls. The Forward cell is `a_nested_forward_and_a_carrier_deleting_connections_are_still_refused_by_name`. The rafka-v2 recovery-round/claim-revision work stays with its lead. Landing gate follows.

### Remaining
- [x] the 0x1B node-update frames follow the node-properties proposal: `UpdateNode`/`NodeUpdated` → `UpdateName`/`NameUpdated` (landed e634b766, RED b0f888b0, with the s41 row above, listed here so the rename sweep does not touch them twice)
- [x] the seat record's counter is a **revision**, never an epoch (Luke 2026-10-12: an epoch is timekeeping; this is the count of seat moves): `rafka-mesh-entity/src/seat.rs:42` `SeatHolder.epoch` → `revision`, `SeatEpoch` → `SeatRevision`, `expected_epoch`, `RejectedEpochOverflow` → `RejectedRevisionOverflow`, the `SeatRow` key, spans and comments; the gossip source "epoch" (a new source primary's first full) is the same number and takes the same name; the serde field name changes in the JSON seat rows, so `durable format moved`. Docs done 2026-10-12 (envelope 0x21, elections §3/§4.4, gossip, fabric-primary-update, the node.primary.update proposal); the Kafka-domain producer/claim epochs in jobs, transactions and offsets are not this and keep their word (landed e7ffb680 / 933b0c61, RED ad6af7ad)
- [x] the first Node RPC frame is the **target**, never a fence (Luke 2026-10-12: a fence is an epoch comparison, this frame is the call's address): `framing.rs:26` `Fence { target_node_id, op }` → `Target { node_id, op }`, `RequestHeader::fence` → `::target`, `RpcOutcome::RejectedStale(s).target_node_id()`, the `BadTarget` arm's wording, every `fence` in comments/spans of rafka-node-rpc-contract and rafka-node-rpc; postcard is positional so the wire does not move; the word fence stays only for the seat epoch (`Fenced { node, by }`, the seat row). Docs done 2026-10-12 (envelope §0, "Target and context", node-rpc.md)

- [x] the Forward op's success reply is `Forwarded { inner }`, never Relayed (relay is banned; node-rpc-envelope.md Forward schema): `rafka-node-rpc-contract/src/forward.rs:55,147,197`, `outcome.rs:414`, `rafka-node-rpc/src/forward.rs:147`; variant position unchanged, no format move (landed 3eaae3e6, gate 152 s)
- [x] the runtime refusal while a node deletes its connections is `ConnectionsDeleting { reason }`, never Draining (connections are deleted, never drained; node-rpc.md §35, envelope readiness gate): every `Draining` variant and `draining` refusal string in the contract and runtime (224 sites at 152cb4fd), the lifecycle vocabulary table, the spans that name it; position unchanged, no format move (landed 3eaae3e6, gate 152 s)
- [x] a Build dispatch refused because the fabric-primary yielded its seat answers `SeatYielded { node, by }`, never Fenced (envelope Build op): `Fenced` in the Build reply enum and its 17 sites; position unchanged (landed 3eaae3e6, gate 152 s)
- [x] a connection's reconnect series is a **recovery round**, never an epoch (connections.md §9.2, the Direct Failed row): the `epoch` field and prose in the connections code (landed in RDM 3eaae3e6; rafka-v2 `rafka-reach-reroute` and `rafka-node-base` coordinated with the rafka-v2 lead); the durable-job claim counter is `claim_revision` (node-rpc.md durable-job mutation; rafka-v2 jobs code, 8 files, coordinated with the rafka-v2 lead) **RDM half landed** (`3eaae3e6`, `d9f8235f`, `4592e9ef`); Rafka's half is rafka-v2 i142 U2 (`77fdfd4948`).
- [x] landed 3fbba798 (2026-10-13, gate green): the comment-citation sweep after the docs move (RDM df378f7a): every `//!`/`///` that cites a page by its rafka-v2 path (`rafka-v2 docs/architecture/...`, `docs/plans/rulings/...`, `docs/plans/proposal/...`; 20 sites) cites the in-repo path; `rafka-mesh-ops/src/framer.rs:16` cites the deleted `docs/architecture/mesh.md` and cites the envelope page instead
- [x] done 2026-10-13 (RDM rulings landing; rafka-v2 5845639d09): the envelope page's rafka-owned op families (0x10 to 0x19) are rafka-v2 `docs/architecture/node-rpc-rafka-ops.md`, text unchanged; the core, rdm and testkit families stay in `docs/architecture/node-rpc-envelope.md`
- [x] node.mesh.leave is its own primitive (node-mesh-leave.md, R-MJ1). Nodes join and leave a mesh; a mesh is deleted, never left (the fabric-primary's mesh.delete is forwarded to the mesh-primary, which runs it). node.stop and node.delete both end their live part with the leave, after `node.connections.delete { All }`, and a caller can command it on its own: `LeaveMesh { node_id, incarnation, build_id, attempt, operation }` on 0x1B, answered `MeshLeft { node_id, incarnation, left }` (`own`, and `backbone` for a node-admin) or `BusyHoldingConnections { connections }`; an appended variant pair, `durable format moved`; spans `rdm.node.mesh.update.via-leaving`, `via-left` (mesh, mesh_id, which), `rdm.node.mesh.reject.via-busy-holding-connections` (0x1B `LeaveMesh`/`MeshLeft`/`BusyHoldingConnections`, `node.stop` sharing `leave_mesh`, and `rdm.node.mesh.*` spans LANDED e34468a0, RED 9f2f9b55, gate 68 s; traces d995f8f05f1b7d536ed20fb9787771ec and bc5f29b816021eecb9b07343f7e2a2d5; `AdminHandle::leave` self-publication and linger removal LANDED b81f66a0, gate 71 s). This 0x1B `MeshLeft` reply is the node's; it is not the gossip `Frame::MeshLeft` e12.s72 deletes
- [x] node.mesh.join's standalone command (node-mesh-join.md, R-MJ1): `JoinMesh { node_id, incarnation, build_id, attempt, operation }` on 0x1B, answered `MeshJoined { node_id, incarnation, joined }` (`WhichMesh`: `own`, and `backbone` for a node-admin), commanded by the node's mesh-primary after a tickle that reached the exact birth, the fabric-primary for a mesh-primary. The envelope's Status enums append `JoinMesh` then `LeaveMesh`, and `MeshJoined` then `MeshLeft`; `durable format moved` (landed e34468a0, RED 9f2f9b55, gate 68 s; traces 686542851fe55fd7a6974693a1bbd7ce, 88b538196f2a62b70768c2b8b2cf42fb, 51dd1e00f0b75c2b7f972d7c4c1ef34b)
- [x] the reserved positions carry no retired word (envelope Status enums; gossip.md "Reserved lifecycle frame positions"); positional, so no format move: `StatusRequest::DrainNode` → `Reserved6`, `NodeDrained` → `Reserved7`, `StatusReply::NodeDrainingApplied` → `Reserved2` (rafka-node-rpc-contract/src/status.rs), `Frame::NodeDraining` → `NodeConnectionsDeleting` and `Frame::NodeDrained` → `NodeConnectionsDeleted` (rafka-mesh-transport/src/membership.rs:501, :529), with their labels `node-draining` / `node-drained` (:2140-2142) (landed e34468a0, RED 9f2f9b55, gate 68 s)
- [x] src/workflows/<op> per lifecycle page; primitives (node.connections.delete, the transport cut, topology reads, ...); renames (mesh_leave -> mesh_delete, retire -> delete, park -> stop, crate::http -> crate::control, departed -> deleted; a mesh is never reborn: the span `rdm.node_admin.mesh.create.via-rebirth` (investigate.rs ~583) is the node.create of a node-admin into the live mesh (mesh-investigation.md step 6), and the "reborn mesh" wording goes from investigate.rs, admin.rs, round.rs, build_drive.rs and their cells (2803_detect 80, rshape_burn_in 35, drift_convergence 23, build_drive 19 hits on main 01a65889); a node's re-birth stays); ~120 comment citations to current doc paths. One mechanical landing after the cuts (landed 18cbfc5f, gate 66 s)

### Acceptance criteria

<!-- crit:e12.s75.1 -->
- [x] one module per op page
<!-- /crit:e12.s75.1 -->
<!-- crit:e12.s75.2 -->
- [x] no comment cites a removed page
<!-- /crit:e12.s75.2 -->
<!-- crit:e12.s75.3 -->
- [x] the fast gate green
<!-- /crit:e12.s75.3 -->
<!-- crit:e12.s75.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s75.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]

<!-- /epic:e12.s75 -->
<!-- epic:e12.s76 -->
## e12.s76 — docs only: the architecture docs carry every ruling

**Builder:** docs seat (rafka-v2 docs only) · **Tracking issue:** #3107 (generated once; not read back) · **Architecture:** the rulings pages (i143-rulings-2026-10-11-dev.md, i143-rulings-2026-10-12-rx-authz.md)

### State

done (R-BI9 landed on `docs/architecture/build.md`: A7, A8, A9, A10, A11, A13 applied; A6 at :371 and A14 already on main; A12 packages table deferred to the crate layout's packages-table landing after the `rdm-` rename); earlier this session: R-ML3 (fabric-recovery, mesh-investigation), R-PB1/R-PC1 (fabric-not-ready), stopped/deleted, R-TG2/R-TG3 tags, BuildChange::Recipe, HandedOff.to, the R-HB1 gossip rows, the R-RX docs; absorbed s8 and s32. Nothing left in this block (A12 tracked under the crate layout step 9).

### Remaining

- [x] build.md for R-BI1 (each topology change is its own Build) and R-BI3 (no Fabric pointer, LIFO read); mesh-investigation step 6 and :162 for R-DG1 (degraded on mesh and fabric, both arms); gossip.md ~:118 and ~:154 for R-RX4; the envelope 0x1C RejectedNotAuthority line (Luke's yes); the lifecycle index node.tags row; the consolidation's page renames; regenerate docs/reference/rdm after each RDM landing
- [x] fabric-node-lifecycle-elections.md §5 ("Election movement never changes Fabric.build_id") and every architecture line naming Fabric.build_id as a pointer: the current Build is read LIFO (R-BI3, R-BI4); build.md for R-BI4 (an attempt lost with its executor leaves the Build in progress)
- [x] (applied 2026-10-13 on Luke's direction, merged with that day's decisions Q2-Q10) the approved node.primary.update proposal's doc edits: a docs seat's attempt (97ebf365b1, 9e0e3bf757, 441f463f2a, 3d98dfd16c) was reverted (5f8db8a2d7..550c07cbe4) — it went beyond the listed edits and was relayed unread. Redo only on Luke's direction, each edit read by the lead before it is reported
- [x] (done 2026-10-12, with the Fence→Target rename) R-UC1a's doc lines (envelope §0's 425 row, the reserved-code 425, node-rpc.md :480/:595) and the R-BI3 pointer sweep (build proposal A2) on Luke's yes

### Acceptance criteria

- [x] R-BI9 (Luke 2026-10-15): build.md A7, A8, A9, A10, A11 and A13 land as the build proposal writes them, one docs landing; the dev re-verifies each line against main (A6 at :371 and A14 are already on the page) and records the exact diff here; A12 (packages table) deferred to the crate layout's packages-table landing after the `rdm-` rename:
  ```diff
  --- a/docs/architecture/build.md
  +++ b/docs/architecture/build.md
  @@ -3 +3 @@
  -This page owns the Build engine (the accepted topology, the Build/attempt/claim model and `BuildStateAdapter`), the node-admin storage interfaces and crash safety, and the RDM/Rafka boundary with its import rule. The deployment provider and the RuntimeFact contract are [deployment-provider.md](deployment-provider.md); this page's sections skip §7 and §8 so that §9 to §16 keep their numbers.
  +This page owns the Build engine (the accepted topology, the Build/attempt/claim model and `BuildStateAdapter`), the node-admin storage interfaces and crash safety, and the RDM/Rafka boundary with its import rule. The deployment provider and the RuntimeFact contract are [deployment-provider.md](deployment-provider.md).
  @@ -56 +56 @@ Allowed customer seams are:
  -- implementations of the imported Fabric/Mesh/Nodes/Connections/Build storage interfaces;
  +- implementations of the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6);
  @@ -99 +99 @@ Rafka imports these RDM packages at one exact revision:
  -| `rafka-mesh-entity` | the generic Mesh entity: exact-node identity, process incarnation, membership, endpoint-slot freshness, RuntimeFact and connection projections |
  +| `rafka-mesh-entity` | the generic Mesh entity: exact-node identity, process incarnation, membership, endpoint freshness, RuntimeFact and connection projections |
  @@ -102 +102 @@ Rafka imports these RDM packages at one exact revision:
  -| `rafka-node-admin-core` | generic Fabric/Mesh/Node control: Build/Reconciler and drift trigger, LeadershipResolver, the fabric/mesh/nodes/connections/builds storage interfaces with memory and durable-file implementations, and the lifecycle, deployment, runtime-adoption, provider-domain and recovery contracts |
  +| `rafka-node-admin-core` | generic Fabric/Mesh/Node control: Build/Reconciler and drift trigger, LeadershipResolver, the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6) with memory and durable-file implementations, and the lifecycle, deployment, runtime-adoption, provider-domain and recovery contracts |
  @@ -128 +128 @@ submitted change
  -The accepted topology names the exact logical slots/path.names that should exist, grouped by Mesh. Counts may remain product-facing resize input, but the compiler resolves a count change to exact paths before the Build is persisted.
  +The accepted topology names the exact path.names that should exist, grouped by mesh. Counts may remain product-facing resize input, but the compiler resolves a count change to exact paths before the Build is persisted.
  @@ -133,5 +133,5 @@ B.topology:
  -    admin.1
  -    admin.2
  -    rpc.1
  -    rpc.2
  -    rpc.3
  +    mesh1.admin.alpha
  +    mesh1.admin.beta
  +    mesh1.rpc.north
  +    mesh1.rpc.south
  +    mesh1.rpc.east
  @@ -139 +139 @@ B.topology:
  -submit: remove mesh1.rpc.2
  +submit: remove mesh1.rpc.south
  @@ -143,4 +143,4 @@ B'.topology:
  -    admin.1
  -    admin.2
  -    rpc.1
  -    rpc.3
  +    mesh1.admin.alpha
  +    mesh1.admin.beta
  +    mesh1.rpc.north
  +    mesh1.rpc.east
  @@ -309 +309,3 @@ pub trait BuildStateAdapter: Send + Sync {
  -    async fn publish_intent(&self, intent: &BuildIntentFact) -> Result<(), BuildStateError>;
  +    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
  +    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError>;
  +    async fn publish_fabric(&self, record: &FabricIdentity) -> Result<(), BuildStateError>;
  @@ -310,0 +313,3 @@ pub trait BuildStateAdapter: Send + Sync {
  +    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError>;
  +    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError>;
  +    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError>;
  @@ -312,0 +318 @@ pub trait BuildStateAdapter: Send + Sync {
  +    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError>;
  @@ -314,0 +321,6 @@ pub trait BuildStateAdapter: Send + Sync {
  +    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
  +    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
  +    async fn note_blocked(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str);
  +    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError>;
  +    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
  +    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError>;
  @@ -318 +330 @@ pub trait BuildStateAdapter: Send + Sync {
  -The exported semantic vocabulary is append/fold friendly:
  +The exported semantic vocabulary (`BuildFact` variants) is append/fold friendly:
  @@ -321 +333,2 @@ The exported semantic vocabulary is append/fold friendly:
  -BuildIntentFact
  +BuildAccepted
  +AttemptOpened
  @@ -325 +337,0 @@ BuildAttemptReceipt
  -BuildCompletionFact
  @@ -328 +340 @@ BuildCompletionFact
  -These facts describe reconciliation execution. The current Build is read from them newest first (§4); they also record what happened while executing it. They are not a second topology authority and not the current-runtime store. Completed Build records remain valid history for recovery, API views, diagnostics, audit and chaos evidence.
  +These facts describe reconciliation execution. The current Build is read from them newest first (§4, `current_build()` over the acceptance-order index, with `newest_accepted_at_ms()` as the acceptance high-water mark of every state and `recovery_of` answering the point read by exited birth); they also record what happened while executing it. They are not a second topology authority and not the current-runtime store. Completed Build records remain valid history for recovery, API views, diagnostics, audit and chaos evidence.
  @@ -336 +348 @@ RDM proves the contract without inventing a broker-backed database:
  -- optional local-file journal for same-node-admin restart, replay, and evidence artifacts.
  +- local-file journal for same-node-admin restart, replay, and evidence artifacts.
  @@ -347,2 +359,2 @@ Rafka must not persist one mutable Build row and increment `attempt` by read-mod
  -BuildIntentFact
  -    blind put once
  +BuildAccepted
  +    insert-and-fail on build_id
  @@ -350,2 +362,5 @@ BuildIntentFact
  -BuildAttemptClaim(build_id, attempt)
  -    insert-and-fail claim
  +AttemptOpened(build_id, mesh, attempt)
  +    insert-and-fail on (build_id, mesh, attempt)
  +
  +BuildAttemptClaim(build_id, mesh, attempt)
  +    insert-and-fail claim on (build_id, mesh, attempt)
  @@ -360,0 +376,6 @@ current attempt
  +
  +current Build
  +    current_build() over the acceptance-order index
  +
  +recovery index
  +    recovery_of(path, node_id, incarnation)
  @@ -392 +413 @@ No Build is current before it is recoverably durable.
  -Control-plane state lives behind RDM's five storage interfaces: fabric, mesh, nodes, connections and status. In RDM's file adapter they open under one store root per fabric, `<store_dir>/<fabric_id>/{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}`, where `<mesh>` is the mesh's logical name. Writers are the ladder: the fabric-primary writes `fabric/`, each mesh-primary its `meshes/<mesh>` and `nodes/<mesh>/`, each node its own `connections/<node>/`. Each node-admin also keeps its own Build journal (`builds.storage`). The generic code knows the interfaces, never a specific database; Rafka binds its system topics to them.
  +Control-plane state lives behind the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and the Build journal (`builds.storage`, `BuildStateAdapter`, on the fabric's shared store root per R-BI6). In RDM's file adapter they open under `<store_dir>/fabrics/<name>` and one store root per fabric, `<store_dir>/<fabric_id>/{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}`, where `<mesh>` is the mesh's logical name. Writers are the ladder: the fabric-primary writes `fabric/` (and `fabric/builds/`), each mesh-primary its `meshes/<mesh>` (including `meshes/<mesh>/claims/` and `meshes/<mesh>/builds/`), `nodes/<mesh>/` and `status/<mesh>/`, each node its own `connections/<node>/`. The generic code knows the interfaces, never a specific database; Rafka binds its system topics to them.
  @@ -394 +415 @@ Control-plane state lives behind RDM's five storage interfaces: fabric, mesh, no
  -`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once. It carries no pointer to a Build (RDM's `FabricRecord` in `rafka-node-admin-core`).
  +`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once; the fabric's name is claimed insert-and-fail at `<store_dir>/fabrics/<name>` above the per-fabric roots, and a deleted fabric releases it. It carries no pointer to a Build (RDM's `FabricRecord` in `rafka-node-admin-core`).
  @@ -508 +529 @@ fabric-primary accepts Build B19:
  -The fabric-primary decides that mesh2 exists and what it contains; mesh2's node-admin creates mesh2's nodes. mesh2 comes into being with the `node.create` of its first node-admin, whose sequence is [node.create's mesh-absent arm](lifecycles/node-create.md#mesh-absent).
  +The fabric-primary decides that mesh2 exists and what it contains; mesh2's node-admin creates mesh2's nodes. mesh2 comes into being with the `node.create` of its first node-admin ([mesh-absent arm](lifecycles/node-create.md#mesh-absent)); that is what `mesh.create` names: there is no separate op.
  @@ -528 +549 @@ fabric-primary accepts Build B20:
  -mesh1 is deleted by `mesh.delete` and mesh3 comes into being with the `node.create` of its first node-admin. mesh3 always has a new MeshId; a MeshId is never reissued, even when the new mesh takes mesh1's name after mesh1's row is deleted. A dead mesh is never restarted: mesh3 is mesh.create of a new mesh, and it does not preserve mesh1's logical identity merely to reuse its slot/name.
  +mesh1 is deleted by `mesh.delete` and mesh3 comes into being with the `node.create` of its first node-admin. mesh3 always has a new MeshId; a MeshId is never reissued, even when the new mesh takes mesh1's name after mesh1's row is deleted. A dead mesh is never restarted: mesh3 is the mesh-absent arm of `node.create` under a new MeshId, and it does not preserve mesh1's logical identity merely to reuse its name.
  @@ -534 +555 @@ fabric-primary accepts Build B21:
  -    mesh2 {rpc.1, rpc.2} -> mesh2 {rpc.1}
  +    mesh2 {mesh2.rpc.north, mesh2.rpc.south} -> mesh2 {mesh2.rpc.north}
  @@ -538,2 +559,2 @@ fabric-primary accepts Build B21:
  -The removal is the [node.delete](lifecycles/node-delete.md) of mesh2.rpc.2, which mesh2's primary
  -executes under the fabric-primary's claim; the fabric-primary never commands mesh2.rpc.2 and never
  +The removal is the [node.delete](lifecycles/node-delete.md) of mesh2.rpc.south, which mesh2's primary
  +executes under the fabric-primary's claim; the fabric-primary never commands mesh2.rpc.south and never
  @@ -622 +643 @@ The RDM-generic proofs of this page's mechanisms:
  -- elastic shape/grow/shrink tests assert accepted per-cohort counts or settled shape rather than requiring surviving ordinals to be contiguous; identity/leadership/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
  +- elastic shape/grow/shrink tests assert accepted per-kind counts or settled shape rather than requiring surviving names to be contiguous; identity/leadership/restart/replacement tests retain exact path/NodeId/IncarnationId assertions when identity is the behavior under test;
  ```
- [x] no architecture line contradicts a ruling
- [x] check-doc-symbols clean
- [x] The story bar holds (section story-bar).

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s76 -->
<!-- epic:e12.s78 -->
## e12.s78 — R-BI7: the mesh-primary self-heals its own mesh; attempts scoped per mesh

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3124 · **Architecture:** proposal `docs/plans/proposal/build-enitty-updates-plus-otherstuff.md` Part C; rulings R-BI7; node-recovery.md; node-rpc-envelope.md 0x1C

### State

landed 6d885a2d (2026-10-13, gate 140 s, its build 75.4 s wall), 47ae04f1 (gate 95 s, build 23.3 s wall), ed74c2ba (gate 136 s, build 64.1 s wall), 7f07059e (gate 147 s, build 73.8 s wall), and 5d53ecef: per-mesh drift lanes (`BuildProjection::in_lane`, `LaneBuilds`, `AttemptRuns` keyed `(build, lane, attempt)`, `RunDoor::attempt_run_in`), the mesh-primary's submit running recovery locally with no call to the fabric-primary (`RecoverySubmitter::submit_in` -> `run_recovery`; live acceptance 1 trace 0737e8226e22178ed1694061993b0d0f vs contrast 46ef274b500baa22b542e5b91c5772f7), a departed claimant taken over on its exact departure proof (trace a80d0b85869ad60ec6563f8c360ef615; case b open unclaimed drift attempt claimable by the successor mesh-primary), a proven exit's row recorded once on the tick that proved it (`record_verdict_once`), a member's run starting at its latest claimed open and the owed submit claiming the open attempt (7f07059e, RED 1510cab3), and item B disproved with a green regression cell (5d53ecef: a delete whose executor exits between its `NodeDeleted` receipt and the publish reuses the receipt on take-over and ends Complete). The 2938 `CONN-G` fork was ruled `R-CN1` and landed in 878d4473 / d4ef297c / b00db084 (tracked under e12.s47 and e12.s53)

### Remaining

- [x] claim key `(build_id, mesh, attempt)`; the mesh-primary claims drift attempts on its own mesh's rows (`meshes/<mesh>/` on the shared store root); the fabric-primary claims topology operations; `ClaimDoor`, `claim_attempt` / `open_attempt`, `store_root.rs`
- [x] the recovery `submit` (admin.rs ~1236) opens the attempt locally when this admin is the member's mesh-primary; no build.create to the fabric-primary
- [x] one attempt in flight per mesh: `current_settled` per mesh; `BuildInProgress { current_build_id, mesh, held_by }`; `NotClaimant { mesh, fabric_primary, mesh_primary }`; durable format moved
- [x] the Part C doc table (envelope 0x1C and refusals, node-recovery.md, build.md, fabric-mesh-node-lifecycles.md:108, fabric-primary.md, R-ON8 cross-reference) in the same landing
- [x] flame: a member exits while the fabric-primary is stopped; the mesh-primary's own claim and completed recovery, no fabric-primary call; the same scenario on the previous sha for contrast

### Acceptance criteria

<!-- crit:e12.s78.1 -->
- [x] a member exit with the fabric-primary stopped is recovered by its mesh-primary (cell + flame)
<!-- /crit:e12.s78.1 -->
<!-- crit:e12.s78.2 -->
- [x] a recovery in mesh 2 runs while a node.create runs in mesh 1
<!-- /crit:e12.s78.2 -->
<!-- crit:e12.s78.3 -->
- [x] a topology operation on mesh X and mesh X's drift attempt never both open; the loser is refused naming the holder
<!-- /crit:e12.s78.3 -->
<!-- crit:e12.s78.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s78.4 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; the landing carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s78 -->
<!-- epic:e12.s79 -->
## e12.s79 — layout step 2: the built-in set is the node-admin and one node executable; consumer-fixture, test-roles and scratch-json-dumper deleted

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3120 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §2 and §4 step 2

### State

landed 764238d0 (2026-10-13, gate 108 s; units 484 → 472): the built-in set is the node-admin and one node executable, `rafka-node` (rafka-node-base `src/bin/rafka_node.rs`, node_type from the launch); the demo's rshape-* binaries bound explicitly; 2776 and the acceptance gate on the binding; rafka-consumer-fixture, test-roles and scratch-json-dumper deleted. `rpc_node` keeps the testkit's rafka-rpc-node: a proof binary (§2 "Proof applications never link into a production crate"; step 5 makes it rdm-test's). NodeKind's Broker/Gateway/Compute variants went with e12.s41's node_type item (e634b766)

### Remaining

- [x] the process provider's built-in set (admin.rs:3622-3631 at f2d33a9a; estate.rs:309-316; faults.rs:114) is the node-admin and one node executable taking `node_type` from the launch; no product role named in RDM
- [x] the demo's `rshape-*` binaries bound explicitly through `bindings.resolve(kind)` (admin.rs:3617)
- [x] `i143_acceptance_2776.rs` and `scripts/i143-acceptance-gate.sh:94` repoint to the binding
- [x] `rafka-consumer-fixture`, `test-roles/*`, `scratch-json-dumper` deleted in the same landing

### Acceptance criteria

<!-- crit:e12.s79.1 -->
- [x] a mesh born with no explicit binding launches every non-admin node from the one node executable, node_type from the launch (cell + launch span)
<!-- /crit:e12.s79.1 -->
<!-- crit:e12.s79.2 -->
- [x] no product role or deleted package is named in crates, scripts, tools or the workspace manifest, except the demo's explicit bindings
<!-- /crit:e12.s79.2 -->
<!-- crit:e12.s79.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s79.3 -->

### Done when

- [x] Every acceptance criterion's cell is green and asserts its span; it lands only through the land script with its timings row; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s79 -->
<!-- epic:e12.s80 -->
## e12.s80 — build: the OTLP exporter on http-proto; tonic's transport and axum 0.7 leave the graph

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3121 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §5, §6 "What is obvious enough to do now"

### State

landed 8cb99b64 (2026-10-13, gate 157 s; units 495 → 483): the exporter on http-proto with the hyper client (axum 0.7, axum-core 0.4, matchit 0.7, tower 0.4, hyper-timeout and five more leave; 379 → 369 packages, 50 → 37 duplicate roots); the gate on the collector's OTLP/HTTP port 5318; found on the path and fixed: every RDM span since 13068501 reached the collector with no service.name (CountingBatchProcessor::set_resource forwarded nothing)

### Remaining

- [x] `rafka-mesh-telemetry`'s exporter from `grpc-tonic` to `http-proto`: the feature and the exporter builder
- [x] the endpoint the tests and the gate set (`OTEL_EXPORTER_OTLP_ENDPOINT`) on the collector's http port

### Acceptance criteria

<!-- crit:e12.s80.1 -->
- [x] `tonic` stays only as `opentelemetry-proto`'s codegen (features `codegen`, `prost`; no `transport`, hyper, h2 or axum): `http-proto` requires `gen-tonic-messages` (opentelemetry-proto-0.27.0/Cargo.toml:114-117); `cargo tree -i axum@0.7`, `-i axum-core@0.4`, `-i matchit@0.7`, `-i tower@0.4` match no package
<!-- /crit:e12.s80.1 -->
<!-- crit:e12.s80.2 -->
- [x] span files still land and spans reach the collector endpoint the tests set
<!-- /crit:e12.s80.2 -->
<!-- crit:e12.s80.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s80.3 -->

### Done when

- [x] Every acceptance criterion holds; it lands only through the land script with its timings row against the previous one; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s80 -->
<!-- epic:e12.s81 -->
## e12.s81 — build: the dev-dependency loops end; cross-stack cells move to the scenario crate

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3122 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §5, §6 "What is obvious enough to do now"

### State

landed 830846f4 (2026-10-13, gate 77 s): eight cross-stack cells moved to the scenario crate under mechanism names (mesh_membership__digest_sequence, node_rpc__apply_cuts, node_rpc__scheduler_replay, node_rpc__shared_dial), three id-named files gone from the lower crates; rafka-mesh-entity and rafka-node-rpc dev-depend on no workspace member; the gate runs rafka-mesh-entity's tests and the four stems

### Remaining

- [x] the cells in `rafka-mesh-entity/tests` and `rafka-node-rpc/tests` that need transport, rpc, scenario or admin-core move to `rafka-test-scenario/tests`, bodies unchanged
- [x] the dev-dependencies that formed the loops are deleted

### Acceptance criteria

<!-- crit:e12.s81.1 -->
- [x] `rafka-node-rpc`'s and `rafka-mesh-entity`'s dev-dependencies name no crate above them
<!-- /crit:e12.s81.1 -->
<!-- crit:e12.s81.2 -->
- [x] discovery counted per crate before and after; every moved cell named in the commit
<!-- /crit:e12.s81.2 -->
<!-- crit:e12.s81.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s81.3 -->

### Done when

- [x] Every acceptance criterion holds; it lands only through the land script with its timings row against the previous one; this block's State names the landing sha.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s81 -->
<!-- epic:e12.s82 -->
## e12.s82 — one test binary is one process: env set once in its ctor, no global subscriber

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3123 · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §3 ("One binary is one process") and §6 item 7

### State

part B (rafka-node-rpc-testkit) landed 38366fbb (2026-10-13, gate 76 s): 14 env sites to the ctor, the two cells needing different values each in their own process with its own env; part A landed e41a9e2d (2026-10-13, gate 83 s): mesh-transport's 18 env sites to its ctor, mesh-telemetry's runtime_watchdog on a thread-scoped subscriber (the watchdog enters the dispatcher in force at its spawn), s80's two telemetry cells; parts D and E landed b3b7e20a (2026-10-13, gate 89 s): EvidenceCapture (thread-scoped, its own file, OTLP export, flush and shutdown in its Drop; 14 transport own-process forks gone); admin-core's five and the chaos kit's five global-init callers moved; the cadence reader refuses a malformed value by name; the test-only process_holds and its racing cell deleted and part C landed 992e30bc (2026-10-13, gate 80 s): the connections-delete deadline is launch config read once, a malformed value refused by name, reaching every launched node (the provider had stripped it: launched nodes ran 5000 ms whatever the commander waited); admin-core's test env in its ctor; the testkit ctor no longer overrides a parent's env (RED/GREEN); role_build waits for each node's own digest before reading its load; part F landed 2737d1bb (2026-10-13): admin-core `tests/fabric_handover.rs`'s two set_global_default to a thread-scoped capture; its 11 own-process forks gone; part G landed e401ebc0 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`): `chaos_kit__restored_faults` passes `RDM_EVIDENCE_DIR` via `delegated_with_env` instead of calling `set_var` in the test body; part H landed d6b1b746 (2026-10-14, gate 77 s, `units=455`): replaced the five `set_var("RDM_EVIDENCE_DIR")` calls in `drift_convergence.rs` with `EvidenceCapture::start`. R-SF1 (Luke 2026-10-14, option A, landed f1109974, gate 44 s): `cadence::staleness_floor()` (`rafka-mesh-entity/src/cadence.rs:56`) stays process-wide; the four transport cells keep their own subprocess (`channel_stopped.rs:69`, `gossip_stats.rs:77`, `peer_location_book.rs:148`, `heartbeat_spans.rs:63`). Left: none

### Remaining

- [x] part A (landed e41a9e2d)
- [x] parts D and E (landed b3b7e20a); testkit transport_diag's two cells keep the product init on purpose (each proves the init's own environment-driven diagnostic layer, in its own process): the telemetry init's global try_init and propagator in a test process (rafka-mesh-telemetry lib.rs:146-148, :470-476), eighteen transport cells; mesh_membership__digest_sequence.rs:116's set_var
- [x] R-SF1 (Luke 2026-10-14, "i agree", option A, docs only; landed f1109974, gate 44 s): four transport cells need different staleness floors (60000 ms: `channel_stopped.rs:69`, `gossip_stats.rs:77`, `peer_location_book.rs:148`; 2000 ms: `heartbeat_spans.rs:63`), read once per process (`rafka-mesh-entity/src/cadence.rs:56`); each keeps running in its own subprocess with `RDM_STALENESS_MS` set through `own_process::delegated_with_env`; no floor argument on `Membership::join`
- [x] part C (landed 992e30bc): `connections_delete_deadline_from_env` read on every call (node_self.rs:62-63, :427; node_commands.rs:158,164; status_rpc.rs:769) becomes a value NodeSelf holds from the launch; admin-core's 17 test env sites
- [x] part F landed 2737d1bb (2026-10-13): admin-core `tests/fabric_handover.rs`'s two set_global_default to a thread-scoped capture; its 11 own-process forks gone
- [x] part G landed e401ebc0 (2026-10-14, gate 152 s) and part H landed d6b1b746 (2026-10-14, gate 77 s): `chaos_kit__restored_faults` passes `RDM_EVIDENCE_DIR` via `delegated_with_env` and `drift_convergence.rs` uses `EvidenceCapture::start` (`git grep 'set_var(\|remove_var(\|set_global_default' -- crates/*/tests` names 0 hits)

### Acceptance criteria

<!-- crit:e12.s82.1 -->
- [x] `git grep 'set_var(\|remove_var(\|set_global_default' -- crates/*/tests` names only ctors (and the two intentional `transport_diag` global-init proof cells)
<!-- /crit:e12.s82.1 -->
<!-- crit:e12.s82.2 -->
- [x] each binary green as listed and with `--test-threads 1`; `--list` counts unchanged
<!-- /crit:e12.s82.2 -->
<!-- crit:e12.s82.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s82.3 -->

### Done when

- [x] Every acceptance criterion holds; each part lands only through the land script with its timings row; this block's State names the landing shas.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s82 -->
<!-- epic:e12.s83 -->
## e12.s83 — node.create's name claim never returns: the PrepareStorage step idles for the whole deadline with no claim span

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `docs/architecture/lifecycles/node-create.md` step 1 (the name claim is the first durable write), `docs/architecture/build.md` §4 (a step's receipt is its commit), `crates/rafka-node-admin-core/src/deployment/pipeline.rs:1471-1495`, `crates/rafka-node-admin-core/src/node_rows.rs:277` (`NodeRows::claim`)

### State

landed d471da63 (2026-10-14, gate 152 s, `TIMINGS wall=76.9s units=455 max_concurrency=11 unit_sum=216s`; RED 1ab151af): `NodeRows::claim` opens `rdm.node_admin.node.create.via-node-row` before its first await (`writer_check` -> `put_seeded`) and records `stage`, `outcome`, and `reason` on the span at every exit, while `PrepareStorage` records `stage` across `claim`, `prepare_storage`, and `save_manifest` so any await held in `PrepareStorage` is named in the trace from the artifact alone (cell `node_objects::a_create_traces_every_prepare_storage_await_and_opens_the_claim_span_before_its_first_await`; `node_objects` 8/8 green at gate cadence with its 5 s bound unchanged)

### Remaining

- [x] read why: the first await inside the step (`rows.claim` → `writer_check` → `put_seeded`, or whatever precedes the `via-node-row` span) with a span or event on every await it holds, run once at gate cadence, the trace naming the await that does not return (landed d471da63, RED 1ab151af)
- [x] fix at the source; delete nothing that was coping; the cell stays as written (5 s) (landed d471da63)
- [x] the span that was missing is emitted before the first await, so a repeat is attributable from the artifact alone (`NodeRows::claim` opens `rdm.node_admin.node.create.via-node-row` before `writer_check`; landed d471da63, cell `node_objects::a_create_traces_every_prepare_storage_await_and_opens_the_claim_span_before_its_first_await`)

### Acceptance criteria

<!-- crit:e12.s83.1 -->
- [x] the trace of a create names every await of PrepareStorage, and the claim span opens before the claim's first await
<!-- /crit:e12.s83.1 -->
<!-- crit:e12.s83.2 -->
- [x] node-admin-core tests/node_objects.rs green at gate cadence, no bound changed
<!-- /crit:e12.s83.2 -->
<!-- crit:e12.s83.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s83.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s83 -->

<!-- epic:e12.s84 -->
## e12.s84 — R-CY1: the canary red is read; the restart of a cohort seat holder runs under the attempt after its hand-off

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-CY1; `lifecycles/node-primary-update.md` (the holder's own seat update runs under its attempt and ends HandedOff); `lifecycles/node-restart.md`; R-SB1

### State

done (closed 2026-10-15; RED 368d110a; landed 69752953, gate 39 s; Root 1 confirmed: `Estate::complete(&mut stream)` in `crates/rafka-test-scenario/src/typed.rs:626-633` returns `stream.accepted()`, which carries the `Started { build_id, attempt }` frame sent when the `node.restart` workflow stream is first admitted (`attempt == 2`). When the restarted `rpc` node holds the `rpc_node-primary` cohort seat, `build_drive.rs::seats_first` (`:1240-1299`) claims `restart.attempt` (`2`), dispatches `move_seats` (`rdm.node_admin.build.update.via-dispatch` -> `rdm.node_admin.seat.update.via-primary-update`), and completes `restart.attempt` with `BuildReply::HandedOff` (`outcome = "handed-off"`). The Build drive then claims `attempt == 3` (`restart.attempt + 1`) and executes `rdm.node_admin.build.update.via-reconcile` (`operations = "node.restart:<node>"`) and all five restart deployment steps (`NodeRestarting`, `StopNode`, `AwaitNodeLeft`, `StartNode`, `WaitForNodeReady`) under that reconcile's attempt. `canary.rs` now selects the `rpc` cohort primary seat holder deterministically (`is_primary == true`, lowest `node_id`), asserts the `restart.attempt` hand-off (`rdm.node_admin.build.update.via-dispatch` with `outcome == "handed-off"` and descending `rdm.node_admin.seat.update.via-primary-update` with `outcome == "updated"`), reads the executing attempt from `reconcile["attributes"]["attempt"]` (`restart.attempt + 1`), and asserts the five restart step spans under that attempt. Live Jaeger trace `ef78ccae756293f65263737ffab36200` on `build_id = 7wsh5fxxa8vt` (`bid_7wsh-5fxx-a8vt`): `rdm.node_admin.build.update.via-node-rpc` `b42fd02886983c02` (`attempt = "2"`), `rdm.node_admin.build.update.via-dispatch` `70e0f9c8179495bf` (`attempt = "2"`, `outcome = "handed-off"`), `rdm.node_admin.seat.update.via-primary-update` `50599e1d8b2c5042` (`attempt = "2"`, `outcome = "updated"`, `holder = "mesh1.rpc.1"`, `seat = "rpc_node-primary"`), `rdm.node_admin.build.update.via-reconcile` `7fb375cd7794c724` (`attempt = "3"`, `outcome = "converged"`, `operations = "node.restart:mesh1.rpc.1"`), `rdm.node_admin.node.update.via-build` `e5927972c77819a0` (`attempt = "3"`), `rdm.node_admin.node.update.via-step` `6541c02bddeb6259` (`attempt = "3"`, `step = "StartNode"`, `outcome = "complete"`), `rdm.node.lifecycle.start.via-rpc` `3042f51a7e6df98b` (`attempt = "3"`, `outcome = "started"`))

### Remaining

- [x] RED: a cell that gives the restarted rpc node the cohort primary seat deterministically (lowest node id, or the seat taken before the restart) and runs the canary body; it must fail at canary.rs:157 as the two gate reds did (RED 368d110a: `canary.rs` deterministically picks the `rpc` cohort primary holder `mesh1.rpc.1` and panics at `canary.rs:157:32` because `attempt == "2"` ends `handed-off`)
- [x] flame from a live mesh: the restart call's attempt running `node.primary.update` and ending `handed-off`, the restart's steps under the next attempt (Jaeger trace `ef78ccae756293f65263737ffab36200`: `attempt = "2"` runs `rdm.node_admin.build.update.via-dispatch` `70e0f9c8179495bf` -> `rdm.node_admin.seat.update.via-primary-update` `50599e1d8b2c5042` ending `handed-off`; `attempt = "3"` runs `rdm.node_admin.build.update.via-reconcile` `7fb375cd7794c724` -> `rdm.node_admin.node.update.via-build` `e5927972c77819a0` and the five restart steps)
- [x] name the root: (1) the cell's assumption (the restart runs under `restart.attempt`) is wrong and the cell reads the attempt from the reconcile that executed the restart; or (2) the restart reply should name the executing attempt, which is a ruling for Luke before code moves (write the packet, stop) — Root (1) confirmed: `stream.accepted()` returns the attempt opened when the stream was admitted (`restart.attempt`), which dispatches `node.primary.update` and ends `handed-off`, while the reconcile and its five restart steps execute under `reconcile.attempt = restart.attempt + 1`
- [x] the fix lands alone with that sentence in this State; the gate's red story row under e12.s58/s76 points here (landed 69752953, gate 39 s)

### Acceptance criteria

<!-- crit:e12.s84.1 -->
- [x] `node_lifecycle__node_restart` green at gate cadence with the seat-holder case covered deterministically, its trace cited here (`ef78ccae756293f65263737ffab36200`)
<!-- /crit:e12.s84.1 -->
<!-- crit:e12.s84.2 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s84.2 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s84 -->

<!-- epic:e12.s85 -->
## e12.s85 — layout pre-step: the two retired-stem files and the 32 id-named test files, before any crate directory moves

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** proposal `docs/plans/proposal/rdm-crate-and-test-layout.md` §3, §4 (name check), the lead's sequencing note on the rulings page (2026-10-15)

### State

done (closed 2026-10-15; pre-step `crates/rafka-node-admin-core/src/fence.rs` -> `seat_revision.rs`, `crates/rafka-mesh-transport/tests/departed_status.rs` -> `deleted_status.rs`, and all 32 `i143_acceptance_*.rs` test files renamed into their crates' `tests/main.rs` binaries by mechanism (`nodes_snapshot`, `status_reinforcement`, `refeed_scheduler` in `rafka-mesh-transport`; `status_rpc`, `executor_redecide`, `connections_reconnect`, `gossip_frames_wire` in `rafka-node-admin-core`; `proof_store` in `rafka-node-rpc-testkit`; and 24 `<family>__<case>` modules in `rafka-test-scenario`); extraction map refreshed at `f5446971`, test discovery ratchet added to `rdm-gate-at.sh`, and ungated main reds fixed including the `rafka-mesh-telemetry` OTLP HTTP exporter URI `/v1/traces` path pin LANDED `f7f41b29` [gate 62 s]; Step 4a `rdm-core` fold LANDED `a005cda8` [gate 63 s]; Step 4b `rdm-client` fold LANDED `cd9e18b3` [gate 66 s]; Step 5 `rdm-node` extraction LANDED `4d9a794c` [gate 60 s]; Step 6a `rdm-transport` fold LANDED `93747854` [gate 61 s]; Step 6b `rdm-admin` fold LANDED `6a8ba1c1` [gate 67 s]; Step 6c `rdm-test` fold LANDED `15d8a582` [gate 61 s]; Step 7 `admin.rs` split into `admin/{records,projection,rows_writer,recovery,reconcile,session,runner,boot,mesh_delete}.rs`, `workflows/*.rs` and `primitives/mod.rs` with conformance §1 code-only renames LANDED `18cbfc5f` [gate 66 s]; Step 8 test family layout across `crates/rdm-test/tests/{build,chaos,elections,fabric,gossip,mesh,node,rpc,runtime}/` and `crates/rdm-admin/tests/` LANDED `a982a84c` [gate 83 s, after `bce35b63` hit the gate's stem filter where `routing::` matched `role_routing::`, fixed at `rdm-gate-at.sh:239`]; Step 9 `rdm-mesh-audit` and `rdm-docgen` tool package prefix renames and regenerated `docs/reference/rdm/*.md` LANDED `1bcc2200` [gate 69 s]; `accepted_topology` 170 s timeout at `a269d7e5` (red climb trace `http://192.168.68.99:16687/trace/6d1b67fa8fd0191d4a75f4c7e5b4c8bc`: `RoundBook::take_due` discarded `route_lost` while `commit-state` was `Command::Out` when `mesh1.admin.2` crossed the 3000 ms staleness floor at `17.268 s` before `ReplyLost` at `17.327 s`, and `RoundBook::undelivered` did not wake the round driver when `addressed` was `false`) plus `boot.rs:1616-1645` Ready status race fixed LANDED `689a94d6` [gate 59 s, live fix flame `http://192.168.68.99:16687/trace/a7ce40d701966474a9fbd0105ebdcf3b` (`event=reachable-again` on `rdm.node_admin.fabric.update.via-round-command`), climb flame `http://192.168.68.99:16687/trace/f3921d858ea5e6b5fe4096402c8b752c`, cell trace `http://192.168.68.99:16687/trace/5b5cbe684e21ea4a0c8f90525eb35aa6`]; `node_stop` red at `a269d7e5` (ClickHouse traces `http://192.168.68.99:16687/trace/edccc8a30252ff9245b6b54339212f3a` [client `nodes.stop` + `via-build-stream`] and `http://192.168.68.99:16687/trace/10b22166562bc5e985c4de7038b81b41` [`NA2` `mesh2.admin.2` `via-reconcile` -> `via-step` `StopNode` -> `PrimaryUpdated`]) measured: after `NA2` (`mesh2.admin.2`) completed `P`'s stop at `15441.3 ms` and published `PrimaryUpdated` at `15441.4 ms`, 1-hop `mesh2.rpc.1` heard `via-primary-updated` at `+0.6 ms` (`15442.0 ms`, trace `http://192.168.68.99:16687/trace/bfe52e53b09ab8987a3875df9610b2be`), 1-hop `mesh1.admin.1` heard `via-primary-updated` on `backbone` at `+89.8 ms` (`15531.2 ms`, `+8.7 ms` after `nodes.stop` returned `Frame::Complete` at `15522.5 ms`, trace `http://192.168.68.99:16687/trace/10b22166562bc5e985c4de7038b81b41`) and queued `Job::Carried` onto `mesh:mesh1` behind `NodeLeaving` and `MembersDelta(v2)` (which reached 2-hop `mesh1.rpc.1` at `+172.0 ms` and `+172.4 ms`), and the cell tore the estate down (`estate.stop()` at `15623.9 ms`, `+182.5 ms` after `NA2` published and `+92.7 ms` after `mesh1.admin.1` forwarded, trace `http://192.168.68.99:16687/trace/8303054bc6c9930179bc114084c475ec`) before normal 2-hop gossip delivery completed; replaced the silent loop in `crates/rdm-test/tests/node/node_stop.rs:582-607` with a bounded assertion (`seat_bound = 5 s`) of the wait's own condition before `estate.stop()` LANDED `3446ca7c`, verified in trace `http://192.168.68.99:16687/trace/95ddfcee08c1e7981168b1c32a92a5ab` (`mesh2.rpc.1` `+87.3 ms` trace `http://192.168.68.99:16687/trace/8e6199f578563cc8fb36b647616143bc`, `mesh1.admin.1` `+88.2 ms` trace `http://192.168.68.99:16687/trace/abf7a4f4419cb0c874c903152267deaa`, `mesh1.rpc.1` `+177.1 ms` trace `http://192.168.68.99:16687/trace/fe43fb7b4d2b46d297236ab0f22f3c86`); 100% `--list` test discovery parity maintained across every landing (`1,714 -> 1,715` tests across the six `rdm-*` crates and three `rdm-*` tools); 0 stem violations across `git ls-tree -r --name-only HEAD -- crates tools`)

### Remaining

- [x] `crates/rafka-node-admin-core/src/fence.rs` → `seat_revision.rs` (not `seat_fence.rs`: the name check refuses `fence` in any stem); its `mod` line and citations follow
- [x] `crates/rafka-mesh-transport/tests/departed_status.rs` → `deleted_status.rs`
- [x] the 32 `i143_acceptance_*.rs` files (4 admin-core, 3 mesh-transport, 1 rpc-testkit, 24 test-scenario) split into their crates' existing test binaries by what each cell proves (`<family>__<case>` names for the scenario crate for now; the `<family>/<module>` mirror is step 8, one crate at a time after it settles); `tools/mesh-audit/i143-acceptance-jobs.json`, `rdm-gate-at.sh` stems and `scripts/i143-acceptance-gate.sh` repointed in the same commit, the old → new map in the commit body
- [x] discovery parity: `--list` count per test binary before == after; no cell renamed to a retired word; `python3 name-check.py origin/main HEAD` clean before the gate
- [x] after this landing, §4 steps 4-9 in the proposal's order; the fold creates the six crates under their `rdm-` names from step 4 (rafka-v2 is not consuming RDM yet: its pins and the demo's are rewritten once, at step 9 or when it starts) (landed `f7f41b29`, `a005cda8`, `cd9e18b3`, `4d9a794c`, `93747854`, `6a8ba1c1`, `15d8a582`, `18cbfc5f`, `a982a84c`, `1bcc2200`, `689a94d6`, and `node_stop` bounded assertion)

### Acceptance criteria

<!-- crit:e12.s85.1 -->
- [x] `git ls-tree -r --name-only HEAD -- crates tools | python3 name-check-stems` names 0 files the name check would refuse on a directory move
<!-- /crit:e12.s85.1 -->
<!-- crit:e12.s85.2 -->
- [x] every test binary's discovery count unchanged, recorded in the commit body
<!-- /crit:e12.s85.2 -->
<!-- crit:e12.s85.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s85.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s85 -->

<!-- epic:e12.s86 -->
## e12.s86 — R-FM1: a hung runtime holds for a ruled hold, then the provider terminates the exact runtime and the ordinary recovery runs; a hung fabric-primary is held and named, never killed from below

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, lane e4s3) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-FM1; `lifecycles/node-recovery.md` step 3 and the new step 4; `fabric-node-lifecycle-elections.md` :145-153; `deployment-provider.md`; packet `docs/plans/proposal/i143-failure-matrix.md` R-FM1

### State

completed 2026-10-15 (RED `505d99eb`, landed `549493fe`, gate 59 s; traces: hold cell `07f3b640645fb9621219d50fa3135604` / `f3c8fbc570cb17483d61289b3ca056c3`, member terminate+recover `6f2cee1c90c07a9a7ce03607ded20cdb` [40 spans], mesh-primary terminate+recover `88f6435305841044a5468305d6c1f66e` [49 spans] + election `d4be92cf02a4676738d20b70f42fdedd`, fabric-primary held-running `ed394bca8f94b70380f4cf4ff249b2fa`). Mechanism: `NodeHeldRunningRow` (`status/<mesh>/held-running/<node_id>--<incarnation>.json`) recorded on `NotExited { Running }` verdicts and surfaced on `NodeView::held_running` / `FabricView::held_running` (store fact + `node.get`/`fabric.get` field, gossip wire untouched); `OfflineTickle::reprobe` keeps probing `held-running` births every tick; when `RDM_HUNG_HOLD_FLOORS` (default 10) expires with the provider still reporting `Running`, `DeploymentPipeline::delete_hung` runs `TerminateRuntime` first on the exact runtime handle, records `exit_proof` (`proof = "terminate-runtime"`), clears `held-running`, and completes `NodeDeleted -> RemoveTopologyMembership -> node.create` in the same recovery attempt; for a hung mesh-primary, `seat_watch` on the fabric-primary runs the hold and submits the recovery Build, seating the standby on the exit proof only; for a hung fabric-primary, watching admins record `held-running` and publish `Concern` without ever terminating from below.

Page diffs (delegated ruling `R-FM1`):
- `docs/architecture/lifecycles/node-recovery.md` step 3–4:
  - Old: `3. **Provider proof.** On TRUE OFFLINE the mesh-primary asks the provider (the deployment backend that launched the runtime) about the exact runtime recorded at launch. Exited is the proof, recorded durably. Still running is no proof, however long the silence: the node is hung or unreachable, not gone, and nothing is recovered.`
  - New: `3. **Provider proof.** ... When the provider reports Running for the exact runtime handle, the mesh-primary records the durable status-row fact held-running { since, verdicts } (surfaced on node.get, never as a gossip MemberStatus: the gossip wire is untouched) and opens the hold (step 4).` + `4. **Hung runtime hold and terminate (R-FM1).** During the hold (RDM_HUNG_HOLD_FLOORS staleness floors, default 10 = 300 s at the 30 s floor), the mesh-primary keeps running the investigation ladder every tick; an answer from the exact birth or its returning digest ends the hold and clears held-running ... When the hold expires with the provider still reporting Running, the mesh-primary submits the recovery Build for that birth whose first step is TerminateRuntime { node_id, incarnation, handle } ...`
- `docs/architecture/fabric-node-lifecycle-elections.md` §5:
  - Old: `- silence, an observer-inferred Dead or an unheard route is not proof: the holder keeps the seat;`
  - New: `- silence, an observer-inferred Dead, a held-running hold or an unheard route is not proof: the holder keeps the seat; - for a hung mesh-primary (R-FM1), the fabric-primary runs the RDM_HUNG_HOLD_FLOORS hold and, when it expires with the provider still reporting Running, submits the recovery Build whose TerminateRuntime step proves the exit; the seat moves only once that exit proof is recorded, never before the proof; - for a hung fabric-primary (R-FM1), no node below it commands its termination; watching admins record held-running { since, verdicts } on the holder's status row (surfaced on node.get and fabric.get), publish Concern as today, and the fabric seat never moves on silence;`
- `docs/architecture/deployment-provider.md` §2.4:
  - Old: `A dead or wedged node cannot block an authorized delete forever, and silence on a desired running node never authorizes replacement.`
  - New: `... When a TRUE OFFLINE member or mesh-primary is inspected as Running throughout the RDM_HUNG_HOLD_FLOORS hold (held-running { since, verdicts }, R-FM1), the recovery Build's node.delete runs TerminateRuntime { node_id, incarnation, handle } on the exact runtime handle recorded at launch (terminate(handle, Graceful) then Forced after the stop grace), records exit_proof durably and clears held-running before completing NodeDeleted and node.create. A refused or unproven terminate ends the attempt Failed { step: TerminateRuntime, reason } by name.`
- `CLAUDE.md` env table: added `RDM_HUNG_HOLD_FLOORS` (default `10`).

### Remaining

- [x] the `held-running { since, verdicts }` fact: a durable status-row fact written by the mesh-primary on a `NotExited { Running }` verdict, cleared on the birth's answer or returning digest, surfaced on `node.get` (and `fabric.get` for the fabric seat's holder); not a gossip `MemberStatus` (the wire is untouched)
- [x] the hold: `RDM_HUNG_HOLD_FLOORS` (default 10 staleness floors) in the env table of CLAUDE.md and `config.md`; the ladder keeps running every tick during the hold; `Unknown` or an unreadable runtime row opens the fact and no hold
- [x] the `TerminateRuntime { node_id, incarnation, handle }` step as the first step of the recovery Build the mesh-primary submits when the hold expires: provider `terminate(handle, Graceful)` then `Forced` after the stop grace, `exit_proof` recorded durably, then the ordinary recovery in the same attempt; a refused or unproven terminate ends the attempt `Failed { step, reason }` by name
- [x] a hung mesh-primary: the fabric-primary runs the hold and submits the Build; R-EL1 seats the successor on the exit proof only
- [x] a hung fabric-primary: the watching admins write `held-running` on its row and publish Concern as today; nothing terminates it; a person decides
- [x] page text (node-recovery.md step 4, elections :155-157, deployment-provider.md terminate, env table) lands with the code; the exact old and new text recorded here (Luke delegated the ruling; the diff is for his glance)
- [x] cells: SIGSTOP'd member recovers after the hold with the terminate receipt and exit proof in its trace; SIGSTOP'd mesh-primary likewise with the successor seated after the proof; SIGSTOP'd fabric-primary shows `held-running` and nothing else; `node_lifecycle__node_replace.rs:549` becomes the hold's cell; each with its Jaeger flame

### Acceptance criteria

<!-- crit:e12.s86.1 -->
- [x] the three cells green at gate cadence, traces cited here; `held-running` visible on `node.get` in the member trace before the terminate step
<!-- /crit:e12.s86.1 -->
<!-- crit:e12.s86.2 -->
- [x] no path terminates a runtime the provider has not reported `Running` for the exact handle, and none below the fabric-primary terminates it
<!-- /crit:e12.s86.2 -->
<!-- crit:e12.s86.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s86.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s86 -->

<!-- epic:e12.s87 -->
## e12.s87 — R-FM2: a data dir proven gone is recovered over a fresh dir with a new key; a dir present but unreadable is held and named, never resubmitted every tick

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, lane e4s3) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-FM2; `lifecycles/node-recovery.md` "Retained" and the new "Lost data dir" paragraph; `deployment-provider.md` PrepareStorage; `lifecycles/reference.md:199`; packet `docs/plans/proposal/i143-failure-matrix.md` R-FM2

### State

completed 2026-10-15 (RED `5068c55d`; traces: gone cell `via-data-dir-lost` `3afc5ad0a17c48afa65aebd97fa550b4` [span `77b5bfc8cf49465e`], `via-node-recovery` submit `b969d8fa8549ea89be5403ec5c232a8b` [39 spans], `via-hydrate-attempt` (`inherited = false`) `1d467f78873ef57f1d928f6e156e24d2` [41 spans]; broken hold cell `via-data-dir-unreadable` `d7f84fcf6d32c35ceb719b6db6263a9c` [span `bffcbae7314d1ad6`, 26 spans, `submits count = 1`]; repaired cell `via-data-dir-unreadable` `3398e9d6a778824a3336aa83c29442ac` [26 spans], `via-data-dir-repaired` `af93811d9ec896682b5c0ec249c0a3bd` [span `f7061fcec7ef1d27`], `via-hydrate-attempt` (`inherited = true`, retained key `8bdd412ada2035bf941753b21a98bf9652e544db7df800455a3879b029575451`) `2302862bb508b7b0584913877b8b491d` [46 spans]). Mechanism: `inspect_transport_key(data_dir)` classifies `std::fs::metadata(data_dir)` `NotFound` as `InheritedDirError::Gone` and any failure reading `<data_dir>/node-key` inside an existing directory (`NotFound` on `node-key`, `EACCES`, `EIO`, non-32-byte payload) as `InheritedDirError::Broken`; in `PrepareStorage`, inherited transport key inspection runs before `rows.claim` so a broken `data_dir` leaves the exited birth's `Deleted` row intact; `NodeDataDirLostRow` (`status/<mesh>/data-dir-lost/<node_id>--<incarnation>.json`) and `NodeDataDirUnreadableRow` (`status/<mesh>/data-dir-unreadable/<node_id>--<incarnation>.json`) are durable status-store facts surfaced on `NodeView::data_dir_lost` and `NodeView::data_dir_unreadable` (`node.get` fields, never gossip `MemberStatus` — the gossip wire is untouched); `RecoverySubmitter::recover_if_proven` holds an absent `data_dir` for `RDM_DATA_DIR_GONE_FLOORS` (default 2) consecutive ticks before submitting `create_over` with `data_dir = None` (fresh directory beside the old path, newly minted endpoint key, `RDM_INHERITED_DATA_DIR=0` → `HydrateCtx { inherited: false }`), and on a broken `data_dir` records `data-dir-unreadable` once and suppresses `owed_submits` while the fact stands, clearing it and resuming with the retained key (`inherited = true`) once `<data_dir>/node-key` reads cleanly.

Page diffs (delegated ruling `R-FM2`):
- `docs/architecture/lifecycles/node-recovery.md` "Retained" and "Lost or broken data dir (`R-FM2`)":
  - Old: `| **Retained** | The node's path (`<fabric>.<mesh>.<node>`), its configuration and tags, its `data_dir` on disk, and its transport endpoint key (`<data_dir>/node-key`). |`
  - New: `| **Retained** | The node's path (`<fabric>.<mesh>.<node>`), its configuration and tags, and — when the exited birth's `data_dir` is present and readable — its `data_dir` on disk and its transport endpoint key (`<data_dir>/node-key`). See *Lost or broken data dir (`R-FM2`)* below when `data_dir` is absent or unreadable. |` + new section `### Lost or broken data dir (R-FM2)` documenting Gone (`NotFound` on `data_dir` for `RDM_DATA_DIR_GONE_FLOORS` consecutive ticks, default 2 → `data-dir-lost { path, since }`, fresh dir beside old path, newly minted key, `HydrateCtx { inherited: false }`) vs Broken (`data_dir` exists but `<data_dir>/node-key` missing/unreadable/corrupt → `PrepareStorage` fails by name, `data-dir-unreadable { path, cause }` recorded once, recovery held without resubmitting every tick until repaired; neither `data-dir-lost` nor `data-dir-unreadable` is a gossip `MemberStatus` — the gossip wire is untouched).
- `docs/architecture/deployment-provider.md` §3.1 step 1 (`PrepareStorage`):
  - Old: `1. **PrepareStorage** — creates the node's storage directory and per-node runtime layout. Recovery passes the exited incarnation's data_dir through on CreateNodeRequest so the new incarnation opens the existing directory and its <data_dir>/node-key; if that file is missing or unreadable in a recovery create, PrepareStorage fails rather than minting a silent new identity.`
  - New: `1. **PrepareStorage** — creates the node's storage directory and per-node runtime layout. Recovery passes the exited incarnation's data_dir through on CreateNodeRequest when that directory is still present so the new incarnation opens the existing directory and its <data_dir>/node-key (RDM_INHERITED_DATA_DIR=1, HydrateCtx { inherited: true }). PrepareStorage distinguishes a gone directory from a broken one (R-FM2): - **Broken (data_dir exists, <data_dir>/node-key missing, unreadable, or corrupt):** PrepareStorage fails by name before claiming the new birth row ... the mesh-primary records data-dir-unreadable { path, cause } on the exited birth (surfaced on node.get, never as a gossip MemberStatus) and holds recovery without resubmitting every tick until the directory reads cleanly again. - **Gone (data_dir path itself absent for RDM_DATA_DIR_GONE_FLOORS consecutive ticks, default 2):** the mesh-primary records data-dir-lost { path, since } on the exited birth (surfaced on node.get) and submits the recovery node.create with data_dir = None under ClaimFor::Recovery ...`
- `CLAUDE.md` env table: added `RDM_DATA_DIR_GONE_FLOORS` (default `2`).

### Remaining

- [x] gone versus broken by the error at `deployment/pipeline.rs:1046-1048`: the dir path absent (`NotFound` on the dir) is gone; `node-key` missing inside an existing dir, `EIO`, `EACCES` or any other read failure is broken
- [x] gone: after `RDM_DATA_DIR_GONE_FLOORS` consecutive ticks (default 2; env-table row) the same recovery creates the node at the same path.name over a fresh dir beside the old path with a newly minted endpoint key, `hydrate_before_ready` with `inherited = false`, and `data-dir-lost { path, since }` recorded durably on the exited birth
- [x] broken: `data-dir-unreadable { path, cause }` recorded durably on the first failure; the tick's due rule holds that recovery (not due while the fact stands) instead of resubmitting; `node.get` shows it; the fact clears when the dir reads and the recovery proceeds
- [x] RDM deletes nothing on disk; the old key is never re-minted
- [x] page text (node-recovery.md, deployment-provider.md, env table) lands with the code; exact old and new text recorded here
- [x] cells: dir removed after exit → recovery at the same name with a new key and `inherited = false` in the hydration span after two floors; dir made unreadable → the fact and no second recovery submit span; repair → the fact clears and the recovery proceeds; each with its flame

### Acceptance criteria

<!-- crit:e12.s87.1 -->
- [x] the three cells green at gate cadence, traces cited here
<!-- /crit:e12.s87.1 -->
<!-- crit:e12.s87.2 -->
- [x] a recovery over a lost dir never appears twice in the Build journal for one exited birth while the dir is unreadable
<!-- /crit:e12.s87.2 -->
<!-- crit:e12.s87.3 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s87.3 -->

### Done when

- [x] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s87 -->

<!-- epic:e12.s88 -->
## e12.s88 — R14 item 6: container provider store-root qualification and mount for the container cells

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `crates/rdm-node/src/store_root.rs:163-175` (R14 item 6); `crates/rdm-admin/src/deployment/container.rs`; `deployment-provider.md`; §12.2 Container proof

### State

**LANDED `8bd6681d` (2026-10-17), land script, all fast-gate jobs concurrent, GREEN 211 s.** Combined with main `04667eed`; transport process controls 167/167, scenario main 187/187 (including all forty Direct/carried roots), rounds 8/8. Log `/home/admin/rust-distributed-mesh-deploy/s88-combined-land.log`; per-job evidence `gates-e7s3-8bd6681d.log.d/`. Q54's approved page line and exact-birth declared-state acquisition landed with s88. Release qualification and kill/recovery proof, current nonempty route flames and prior REDs remain recorded below. Q56's separately owned live Mesh-state acquisition and the expired-hold follow-on are not represented as resolved. i144's rebase notice is queued in `/home/admin/lane-queue/outbox/to-i144-q56.md`. Codex proceeds to i147 e5.

Codex i147 e5: not landed; concurrent canonical gate `820166de` RED, no unchanged re-gate. Own Wiring audit and late-held-channel connectivity repairs have RED `c806f20d`, audit 10/10 and channels 11/11 GREEN. Q59 (asked as Q58, renumbered) asks the declaration authority before Day-0 Ready; Q59 = x (Luke, 2026-10-18): the Day-0 admin holds the declaration at the Join and writes it with its first write after it takes the seat, no new writer. Other gate reads and pending attribution are recorded in the e5 row. Public consumer qualification is GREEN (combined 1/1, 3.57 s), verified flames retained. Evidence: [i147 e5](i147-gossip-conformance-prd.md#e5--app-types-on-the-envelope-channel-event-received-luke-2026-10-17-thats-clean-im-into-it).


Landing preflight `8a05b16a` found main advanced to `04667eed` (the stop/join primitives). It aborted on generated spans.md before any gate ran. Rebased manually, regenerated that document from the combined source, and repinned all consumer manifests to the resulting candidate. The two live controls above remain attributed to their actual pre-rebase source; the land gate verifies the combined source next.


**Corrected candidate: both live silence/flame controls GREEN together (43.82 s).** Cuts 16225 ms / 16178 ms, plan 23000 and hold 30000 unchanged; every value, exact birth, provider, routing and named-span check stands. Current raw evidence is `kept/s88-current-runtime-green/wire/`, current results beside it; `route-flames/` holds 820 imported spans and nonempty queried traces (`s88-current-runtime-flames.log`). Examples: [Direct refusal](http://localhost:16686/trace/c598becb1e0d211bdb6a9e05362b69c8), [ViaPeer refusal](http://localhost:16686/trace/6ba0a53c3af22bad556c63600b10066f), [NoActiveRoute](http://localhost:16686/trace/b99c2de839c70536ddbf0b8f02d956f0). First invocation omitted `MESH_SPAWN_TYPE=container` and was refused immediately at :273 before any scenario; its log `s88-current-runtime-silence-controls.log` is retained separately from the actual container proof `s88-current-runtime-container-silence-controls.log`. No failed scenario was rerolled unchanged. The land script now verifies the changed probe runtime and exact Q54 row assertion concurrently.


Probe startup correction `aa1d3dac` and the Q54 rounds assertion `edc2ab08`: all three controls GREEN together, 7.28 s (`s88-probe-runtime-and-rounds-controls.log`), including all forty Direct/carried probe roots. Syscall observation of the same refused-argument invocation records 21 created threads before, 1 after (the log writer), with identical Refused JSON and exit 2; both `s88-probe-workers-{before,after}.log` are retained. This proves unnecessary startup workers were removed, not that the concurrent export failure is resolved. Next: the two live silence/flame cells, then the concurrent land gate on this changed source.


Probe startup read: even rejecting `--nope` created 21 OS threads on this 20-CPU host (`s88-probe-workers-before.log`, syscall observation, unchanged exit 2 and Refused JSON). The CLI executes one async call sequence, yet constructed a host-sized multithread runtime. It now runs that same sequence and its asynchronous I/O tasks on a current-thread runtime; endpoint close → runtime drop → telemetry drop is unchanged. This removes unnecessary per-probe workers at their source. It is not yet claimed to resolve the concurrent export red; all forty Direct/carried calls, unconditional file/root checks, and the new rounds assertion must verify it. No mesh receive path, telemetry deadline or missing-span assertion changed.


**Landing `89d6ded2` RED (195 s): two owned cells, no outside disposition.** Transport process checks 167/167 GREEN. `rounds.rs:390` asserted the old no-row-before-state-commit rule; its four observed files are exactly Q54's four acknowledged Ready declarations under `status/<mesh>/declared`. The conformance assertion now requires that exact set of four birth-keyed rows, while every other held-row, connection, name-claim and subsequent publication assertion stands. RED source is `89d6ded2`, log `s88-resumed-named-land.log` and per-cell `gates-e7s3-89d6ded2.log.d/rounds.log`. The probe remains an owned read: run 11, exit 0, Reply, no new file, 20/20 unconfirmed at 2000 ms, trace `dfc38c2384ff83302cc162c1e4914311`; `gates-e7s3-89d6ded2.log.d/rdm-test:main.log`. No bounds or file/span assertions changed.


Land preflight `c0b26b39` refused the newly added held-log test identifier's word `drain` before a gate ran. Renamed both the cell and its self-invocation to `a_held_log_writer_ends_the_exit_flush_at_its_one_bound_named`; no behavior or bound change. Original RED remains cited above.


**Work resumed; rebased on main `9e977175`.** No Q56 handoff is treated as a halt. Exit-timing diagnostics (`049dedda`, original run `8dc83d7a`) preserve every assertion and bound. Controls: probe root 2/2 GREEN including all forty real Direct/carried calls (`s88-probe-root-timing-controls.log`, 1.28 s), exit controls 4/4 GREEN under syscall observation (`s88-child-exit-timing-controls.log`, 2.03 s; raw `s88-exit-syscalls.log`). These isolated controls do not dispose of the concurrent missing-file red. Q54 acquisition code/page and the full s88 package remain unlanded until the land script succeeds; the next attempt uses the new main and diagnostic candidate, all gate jobs concurrent.


**Concurrent release `a44e4be5` completed RED, 782 s.** All 92 jobs started concurrently. Provider qualification 12/12 and container kill/recovery 6/6 GREEN; container creation and three of five container chaos cells GREEN. The first silence cut lasted 17703 ms (plan 23000, hold 30000), and all nine route results were correct before release; its unchanged named-span assertion at container_fault_backend.rs:613 saw only three Direct and one ViaPeer spans. The recovered Mesh-state acquisition remains Q56, assigned to i144; it does not halt independent reads. Raw log `s88-shared-deadline-concurrent-release.log`, immutable evidence `kept/gate-gates-e7s3-a44e4be5release/`, named reads `s88-a44e4be5-red-reads.txt`, all under `/home/admin/rust-distributed-mesh-deploy/`. Outside cells are unchanged.

Telemetry library 154/154 GREEN, including the held-log deadline fix; process exit controls still have three reds. Immediate exit names the export thread not yet running and 9/9 unconfirmed at 2000 ms. The probe root cell at probe_root_span.rs:141, run 1, returns Reply and exits 0, but creates no new span file (trace `b1ee47f3fb119cf3981a0620a953404c`; 20/20 unconfirmed). The FIFO cell's 11.687 s is measured by the parent's observation of exit, not a child-side flush measurement. Collector-down reports a 12154 ms first evidence open/write and a 1248 ms later write. This diagnostic commit separates open from write timing and records child-side flush elapsed time; assertions and bounds stand. No cause is inferred from the combined times, and no missing named span is accepted in place of the assertion.


**Deadline correction verified; restarting concurrent verification.** Candidate `c1373b39` was stopped during consumer preflight before test jobs began: the new store stages needed to share the original single ten-second participant deadline, rather than each receiving ten seconds. Corrected in `ad431b86`; the fifteen-second parent phase bound is unchanged. Store/status controls 6/6 GREEN on the correction (`s88-functional-deadline-controls.log`, 0.64 s; atomic phase 39.22 ms). The preflight log `s88-corrected-concurrent-release.log` is retained as interrupted, not represented as a completed or green gate. The next gate has a new source tree and includes the telemetry root fix and the call-outcome observation fix, with no changed test assertion or bound hidden by a rerun.


**Concurrent release next on the corrected candidate** (2026-10-17). Rebased on main `5bda0e6b`. RED run `208a37fe` completed all 92 jobs concurrently, 960 s, with original logs/evidence preserved (`s88-rebased-concurrent-release.log`, `kept/gate-gates-e7s3-208a37ferelease/`, named reads `s88-208a37fe-red-reads.txt`). Neither silence cell was passed off as an outside red: their real probe calls had produced all expected outcomes, but each sample waited for endpoint shutdown and evidence flush before the next call. The probe now prints its unchanged answer at call completion and still closes its endpoint before runtime and telemetry shutdown; `ProbeObservation` retains and reaps every real child and checks successful exit before the cell passes. Direct → ViaPeer → NoActiveRoute and the three concurrent samples, all value/provider/birth/route assertions and the 23 s / 30 s bounds stand. Controls 2/2 GREEN in 25.85 s (`s88-observed-call-controls.log`), actual cuts 16180 / 16271 ms. Probe root checks 2/2 GREEN, including all forty Direct/carried invocations (`s88-probe-root-controls.log`); their unconditional file assertions stand.

Store qualification is functional: three explicit A→B→A→B cross-container interleavings replace the repeated 64-cycle fsync workload. Each proves the complete old value while a complete synced replacement is staged, a whole value concurrent with rename, and the complete new value after directory fsync; hard-link create_new/refusal, both-direction reads/listings and every fsync remain. The existing 10 s participant and 15 s phase bounds stand. Qualification plus exact-birth status controls 6/6 GREEN (`s88-functional-store-and-status.log`); nine published whole-file observations, atomic phase 39.89 ms. This is not claimed as a concurrent full-gate result yet.

Separate telemetry root defect fixed ahead of the landing: the process drain bounded evidence and collector waits, then called an unbounded log flush. RED `a341c700` (original run `d06be392`) deterministically holds a log writer after it entered; the child never returns and is killed at the unchanged 2.5 s cell bound (`held-log-drain-red.log`). Fix `71df2501` makes both logs use the same remaining two-second deadline, naming unconfirmed lines. Unit controls 3/3 GREEN (`held-log-drain-green.log`); existing exit controls 4/4 GREEN (`s88-exit-controls.log`). No evidence-file assertion, flush deadline, telemetry init or wire format changed. The full release remains the next required check; Q56 stays with i144 and does not halt these reads or verification.


**Resumed; concurrent verification next.** Rebased onto main `8dd18c2e`, including i146 e3. Main's shared ruling index already assigns Q55 to the i144 signal-stop ruling; this branch's separate held-Mesh-state question is corrected to Q56. Q54 is ruled A, as Luke instructed. The i144 handoff does not halt the remaining verification or reads. The fixture changes still need the complete concurrent release; then the land script decides the landing. No early durability is added for any other held row.


**Q54 code complete; s88 held on [Q56's i144 read](proposal/i143-q56-held-mesh-state-acquisition.md)** (2026-10-17). Product fix `a8bc1681`, consumer candidate `3cad5423`; Q53/status acquisition 5/5 green, verified non-zero fold and `holding=false`. Release `3cad5423` ran all 92 jobs in one concurrent batch (`GATE_JOBS=256`, containers required), finished 877 s, RED: canonical `/home/admin/rust-distributed-mesh-deploy/s88-q54-release.log`, immutable raw evidence `kept/gate-gates-e7s3-3cad5423release/`. The lost-peer-mesh cell passes healthy setup, then fails estate.rs:405 at its 300 s recovery bound: its new admin finishes its own-mesh sweep but names missing locally applied Mesh Pending as the remaining Ready blocker ([flame](http://localhost:16686/trace/b1291e06069d27cd81e2a5014c5abb51)); Build floor completed. Q56 is assigned to i144 by Q54, not implemented here; no other held store was made durable early. No land-script push or i147 e5 code yet.

Fixture corrections `e5518eee`: the shared-root reader observes kernel filesystem notifications while all 64 atomic replacements and file/directory fsyncs remain; it no longer floods the mount with tens of thousands of redundant 128 KiB reads. Qualification control 1/1 green, 67 complete reads, 1.03 s (`s88-event-store-control.log`). Release now runs container qualification once in container-proof, retaining its fast-gate coverage in rdm-admin:main. Each silence observation reads the exact containers in one daemon query, and independent local admin views run concurrently. Both silence controls 2/2 green in 55.36 s (`s88-q54-silence-container-controls.log`): actual cuts 19249 / 18763 ms, unchanged plan 23000 ms, hold 30000 ms; every provider, birth, value, route and bound assertion stands. The first isolated silence command omitted MESH_SPAWN_TYPE and refused before setup; its log `s88-q54-silence-controls.log` is kept; the correctly configured invocation is separately named. A future concurrent release must still prove these fixture changes; the isolated controls are not reported as a full green gate.

Release outside-path reads: `/home/admin/rust-distributed-mesh-deploy/s88-3cad5423-red-reads.txt` retains each named cell, source line and exact failure text; original per-job logs and the common raw evidence remain beside the gate. Telemetry exit at telemetry_exit.rs:198 reported 9/9 spans not confirmed at 2000 ms and an evidence write stalled 10781 ms; the blocked-open cell at :183 observed 3.767 s from exit marker to the parent's process observation. Those timing observations do not by themselves identify which unbounded step or scheduling interval accounts for the excess; no bound is moved and no runtime telemetry change is smuggled into Q54. The transport/probe logs retain the missing-file occurrence for its separate read.


**Q54 A implemented; Q53 controls GREEN** (Luke, 2026-10-17): only `status/<mesh>/declared` opens directly on the shared root and finishes its write before Applied, including before state-commit; same row, address and format. The approved states.md Rounds line accompanies it. The successor folds only exact births held by its incoming view (declared and operation rows). Q53 RED `291f705c` / timed `6e1a69ee`; unheld-incarnation control RED `f6c2dd43`, `/home/admin/rust-distributed-mesh-deploy/q54-unheld-red.log`. [Verified successor fold flame](http://localhost:16686/trace/7400c308327e158d15947c2e22ced439) records `holding=false`, acknowledged put and `node_declared_rows=1`; raw and queried evidence is kept under `/home/admin/rust-distributed-mesh-deploy/kept/q54-successor-green/`. Status acquisition cells 5/5 GREEN in `/home/admin/rust-distributed-mesh-deploy/q54-status-green.log`; both publication cases fold one row and first project Ready, and an independent disk reader sees the declaration immediately after Applied. The mesh-status control stays held until publication. Q56: broader scratchpad inheritance is assigned to the i144 owner for its own read and packet; Q54 changes none of those held rows. s88's qualification and live controls follow, then the land script, then i147 e5.

Historical reads before Q54's ruling:


**Q53 cause read / held for [Q54](proposal/i143-q54-acknowledged-precommit-status.md)** (2026-10-17). RED `291f705c`, timed evidence `6e1a69ee`, cell `status_seat_fold::an_acknowledged_ready_birth_is_in_its_successors_first_complete_projection`: StatusAuthority Applied after FileStatusStorage acknowledged a put held only in its private pre-commit scratchpad. Ending that writer without publication leaves no shared-root row: successor fold 0 / first projection Pending. The identical published control folds 1 / Ready. [Unpublished flame](http://localhost:16686/trace/371d28b9a8272d5934de9a6b98325460); [published control and non-zero fold](http://localhost:16686/trace/b96f9ba477d979f3ec152f41aaac4753). Both named RED logs and raw/measured/queried evidence are kept under `/home/admin/rust-distributed-mesh-deploy/q53-successor-{red,timed-red}.log` and `kept/q53-successor-red/`. The old live primary opened scratchpad and its commit-state call ended Indeterminate; the new primary opened store-root and folded zero. The old archive does not directly instrument its put, so the phase-loss cause is proven by the reproducer, not inferred from a missing export. R14 item 9 explicitly keeps every row in the scratchpad before state-commit: write-through before Applied requires the precise exception in Q54, or Q53 must be scoped to published facts. Q53's mesh permission stands. No wait, re-fold, fallback or product durability change added; only put-path observability and the RED cell. s88 and i147 e5 remain unlanded.

Kept final Q50 control evidence: `/home/admin/rust-distributed-mesh-deploy/kept/s88-open-tools-green/`. The completed release’s 35 named red reads are retained for their owners at `/home/admin/rust-distributed-mesh-deploy/s88-5b485f22-red-reads.txt`, with the original per-job logs beside `gates-e7s3-5b485f22release.log`; outside cells and bounds remain unchanged.

**Held for [Q51](proposal/i143-q51-successor-member-status.md)**, 2026-10-17. Q50’s fixture work is complete: pre-opened exact-namespace tools, mesh/node healing controls 2/2 green in 55.78 s, actual cuts 19274 ms / 18898 ms below the unchanged 23 s plan and 30 s hold; `/home/admin/rust-distributed-mesh-deploy/s88-open-tools-controls.log`, code `c5c2a4e6`. Release RED `5b485f22` stays kept, including the separate product regression: the successor folded 0 declared rows, then its version-1 full changed the same ready RPC births to Pending. Verified [prior Ready](http://localhost:16686/trace/e83755251052bae5522808e8bbace483), [zero-row fold](http://localhost:16686/trace/15cb9cd91901c07ab8111839908d878b), [regressing full](http://localhost:16686/trace/1b56aa5ce40f7d173648b8955b64b6ef). Q51 asks whether Codex fixes the existing durable-state acquisition path here or i144 lands that fix first. No product change for it, no bound change, no landing yet; i147 e5 follows s88’s landing. Full raw/queried traces and gate read: `/home/admin/rust-distributed-mesh-deploy/kept/s88-q51/`. Expired-hold follow-on remains separate.

Release RED `5b485f22`, `/home/admin/rust-distributed-mesh-deploy/s88-aligned-release.log`: qualification and kill controls green; mesh-cut release completed at 52.212 s after its 23 s deadline, beyond the 30 s hold. Each filter command still started a Docker exec process inside the cut. The test now opens one exact-namespace privileged tool process per member before timing starts; install/list/lift use its existing pipes, with the same filters and assertions. No product runtime or bound change. A distinct peer-mesh setup red is being brought as Q51: typed.rs:425 held the same RPC births Pending after 60 s, although their Ready declaration applied; a successor’s version-1 full moved them from Ready to Pending. No product fix is included without the ruling.

Release `b0fc8871` stopped at consumer preflight: the demo and Admin UI pinned different revisions after rebase. No test job ran. Both manifests and their lock now name the same reachable `e2cdfa2f`; the builder’s equality check remains intact. Log `/home/admin/rust-distributed-mesh-deploy/s88-exact-namespace-release.log`.

Exact-namespace controls GREEN: `/home/admin/rust-distributed-mesh-deploy/s88-root-namespace-live.log`, 2/2, 56.16 s. Mesh cut 19.412 s and node cut 19.046 s, both below the unchanged 23 s plan and 30 s hold; every provider, identity, value, route and bound assertion stands. Rebased on main `ac1acf0d` (i146 e3 included). Reachable RED commits: qualification `2a0634af`, prebuilt acceptance runner `a3be1e58`, post-barrier evidence `e60afc2f`; pre-Q50 code `ba944971`; privilege control RED `d8e40951`. The consumer pin names reachable runtime `e2cdfa2f`. Required full concurrent release and land-script gate remain pending.

Exact-namespace control RED `fdb979a8`: privileged exec inherited the node's non-root user and iptables refused Permission denied before applying any filter; `/home/admin/rust-distributed-mesh-deploy/s88-existing-namespace-live.log`. The fixture command now explicitly runs its privileged filter process as user 0; the node's own user/capabilities do not change.

The earlier redundant-inspect removal in `terminate` is withdrawn: termination is unchanged from main in this qualification landing. Its 2 s miss is retained for i144, not treated as a qualification result. The native fixture establishes the mounted store; the cut fixture no longer creates tool containers within the timed cut. Full outside-path reads for release `127fb05e` are retained at `/home/admin/rust-distributed-mesh-deploy/s88-127fb05e-red-reads.txt`; no outside assertion or bound changed.

Release `127fb05e`: native namespace qualification green; 2777 2/2 and kill/recovery 6/6 green; chaos 4/5. The mesh cut refused at container_fault_backend.rs:404: its release began at the 23 s deadline but completed at 30.940 s, beyond the unchanged 30 s hold. Each filter inspection/removal launched a new Docker container. The fixture now executes the same selected host xtables implementation with privileged exec in the exact existing container; the node's process, capabilities, filters and provider identity stay unchanged. Independent final view/Build/provider reads run concurrently; all assertions and bounds stand. Log `/home/admin/rust-distributed-mesh-deploy/s88-native-release.log`, job logs `gates-e7s3-127fb05erelease.log.d/`, prior RED logs kept. Outside-path reds include telemetry_exit.rs:197 (one immediate span absent), :183 (exit flush 8.821 s), provider_state.rs:133 (Immediate 6.864 s); full reads retained beside the gate log.

Release `6e8b92ab` retained the store qualification’s 15 s refusal before any reported filesystem primitive: Docker exec start-to-exit was 16.244 s, exit 0; the host spawned Docker in 10 ms. This does not identify a slow fsync. The fixture now runs two workspace-built native Rust participants as the containers’ processes, with the same 15 s phase deadlines, both 10 s coordination bounds, exact listings, collision refusal, and all 64 synced atomic replacements. No per-phase interpreter/process startup is part of the filesystem measurement. Native control green (1/1, 1.04 s), 59,097 whole-file reads, log `/home/admin/rust-distributed-mesh-deploy/s88-native-store-controls.log`. Earlier RED logs remain; reachable RED commits are `dcda5a5c` (mount), `4905afd8` (prebuilt runner), `10826c0b` (evidence preservation), and `09a3d401` (pre-Q50 cut). Release proof remains pending.

Q50 A fixture implemented; held for Q51 (Codex, 2026-10-17). Compile/test RED `cc957171` proves the absent mount and acceptance of a missing root (`/home/admin/rust-distributed-mesh-deploy/s88-store-root-red.log`). The mounted-store cell and missing-root control are green before enabling Container (`/home/admin/rust-distributed-mesh-deploy/s88-store-root-mounted-proof.log`, 2/2, 1.54 s): two independent containers, exclusive hard-link insertion, collision leaves the existing value unchanged, 64 atomic 128 KiB replacements with file/directory fsync, 9,967 concurrent whole-file reads, coherent reads/listings in both directions. The provider validates the supplied absolute directory, never creates a missing root, mounts it writable in ordinary nodes and admins, and passes its unchanged `RDM_STORE_DIR` environment. The proven Container provider is admitted by `store_root::qualify`. The three container jobs are enabled (`RDM_CONTAINER_STORE_QUALIFIED=1`), require Docker (`RDM_REQUIRE_CONTAINER=1`), and the release gate runs every job in one concurrent batch with its existing cap (Luke's all-suite-concurrent rule). No wire or row format changes. s75 landed `d9f8235f` first (name check and concurrent fast gate green).

Release preflight `de027293` stopped at discovery: i144 e6.1 landed while the candidate was built, so main had eight new cells not yet rebased into the candidate. No test jobs ran. Rebase preserves them. The concurrent release builds consumer binaries and their import manifest before test jobs start; UI tests run alongside R-shape jobs, without deleting or rematerializing their binaries during a live cell. The intermediate `ec6030a6` run was cancelled before tests to complete this prerequisite change; its build completed without source mutation.

Consumer preflight `01576088` refused the fixture’s stale lock under `--locked`: the imported rdm-admin adds `futures-util`, and rdm-transport adds `chrono`. The deliberate offline lock refresh changed exactly those two dependency edges, zero package versions; the demo fixture is pinned to landed main `2f10e419f21e4ddee019be0d107ac68443d5b4ea`. The release builder continues to require `--locked`, and materializes its candidate revision against that updated lock. Log `/home/admin/rust-distributed-mesh-deploy/s88-consumer-lock-refresh.log`.

Release read `e7e0ecc5`: the acceptance runner's `I143_ACCEPTANCE_SKIP_BUILD` skipped only its preamble; all 131 registry commands still ran Cargo, serializing on its package/build locks and recompiling during live tests. The run was cancelled and its owned runtimes stopped; logs retained. New runner RED `4c8f66b9` proves this directly with a Cargo executable that refuses invocation (exit 77), versus the supplied test artifact (`/home/admin/rust-distributed-mesh-deploy/s88-prebuilt-runner-red.log`). The runner now executes the exact package/target artifact from the gate build, preserving environment, cwd and libtest arguments; no fallback compilation. The run also recorded a 15 s store-root command bound, container immediate termination at 4.07 s against 2 s, and telemetry evidence/exit bounds; these are reads to confirm after the execution defect is removed, not disposed as load. The qualification failure message now names its phase without changing its bound. The consumer build includes UI test executables in its one graph; each UI target joins the same concurrent test batch without invoking Cargo. Runner contract cells: 7/7 green (`/home/admin/rust-distributed-mesh-deploy/s88-prebuilt-runner-verified.log`).

Release `499d699d`: provider/qualification cells 12/12 green (76.40 s), including the previous 15 s qualification and 2 s immediate-terminate controls; transport 163/163 green, including both earlier telemetry exit reds, unchanged bounds. The live container jobs then exposed `rdm-client/src/peers.rs:55`: `Peers::of` binds its client to `127.0.0.1`, so its calls cannot route to the container bridge births. Both first live cells booted and printed exact births, then timed out at `typed.rs:90` waiting for `fabric.get` (2777 birth `1kwmgephtqqf/jgsbncn8gabr`; 2784 birth `c1sdft8mzx4g/cwd287dy95cg`). The client now binds `0.0.0.0:0`, retaining the OS-assigned port and exact target birth. This known-red run was cancelled to fix that path; logs remain at `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-499d699drelease.log.d/` and each acceptance cell's `gate.log`. Unchanged-path reads also retained there: provider_state.rs:100 recorded but no cleared span; node_restart.rs:53 and node_stop.rs:38 lost Build replies; build_op.rs:101 lost a watched Build reply; store_root_boot.rs:451/:668 lacked boot/storage spans; rafka_time_discipline.rs:118 held -418 ms. No bounds or those assertions changed.

The attempted unspecified client bind (`e30e230a`) was refused immediately by the existing R-B2 guard: the network monitor is disabled, so every transport must bind a concrete address. The read is `peers.rs:55`, named “network monitor is disabled”; the existing endpoint cells prove that refusal. Corrected at construction: `Peers::at(exact_birth)` asks the OS route to the declared destination for its concrete local address (no datagram, no interface enumeration), drops that temporary query socket, then binds the client's one endpoint on that address at port 0. Both the estate and Admin UI enter through that exact-birth constructor. No monitor setting, target identity, bound or wire byte changes.

Concrete-route control `e772f35a`: container restart canary green; container kill/recovery reached the admin but held PendingReconnect until its unchanged 93 s bound. Read `/home/admin/rust-distributed-mesh-deploy/s88-entry-route-live.log` and its retained container log: the Day-0 admin marks the member stale at `staleness_ms=30000`, while the harness's declared floor is 3000. `spawn_admin_in_container_within` forwarded telemetry and hold settings but omitted the six cadence variables native children inherit. It now hands those declared values into the container, with explicit birth settings taking precedence; no timeout, assertion or product default changes.

With matching cadence, container kill/recovery completed under its original bound (`42e95a74`), then the cell's old span selector failed at `container_kill.rs:95`: it expected `via-proven-drift` and the retired cohort-count scope. The evidence holds the completed exact-birth `via-node-recovery`, same accepted Build `v1m5ywaq4smm`, provider exit 137, and the new birth's boot, trace `adc30aa2ecde703c4fb2f04e3fba2681`. The cell now selects that named recovery record by Build and exited birth, checks the replacement's new node id as node-recovery.md requires, and retains the existing provider-proof, same-Build, create-step, survivor and cleanup assertions and all bounds. Log `/home/admin/rust-distributed-mesh-deploy/s88-container-kill-cadence.log`.

Targeted live confirmation: restart canary green (`/home/admin/rust-distributed-mesh-deploy/s88-entry-route-live.log`); container kill/recovery green, 46.22 s, original bounds (`/home/admin/rust-distributed-mesh-deploy/s88-container-kill-green.log`). The demo pin includes the concrete-entry client and cadence fixes; next proof is the full concurrent release batch, including all three enabled container jobs.

Concurrent release `700ab971` retained at `/home/admin/rust-distributed-mesh-deploy/kept/s88-700ab971/`: `i143-2777-container` green (2/2, 227 s); chaos-container 2/5. Reads: `container_fault_backend.rs:495` asked isolated admins over the host UDP leg the fault blocks (their local views were “unanswered”); the observer now runs the same typed Node RPC reads inside each exact container, bound to its held address, without relaxing the filters. `typed.rs:332` read mesh2.rpc.2 Pending immediately after the Build terminal; setup now waits on its projected ready row through the existing 60 s settling helper. The retirement cell at `estate.rs:405` waited 60 s for the final mesh-primary to exit after mesh.deleted, although mesh-delete.md steps 5–7 explicitly leave it alive for a separate node.delete. Trace `92813bb120b83cfd291d9464e9c9c07f` holds mesh.deleted complete and the four other births exited; the cell now issues the separate delete and associates its termination proof with that delete’s Build. No product lifecycle path or bound changes.

The same release’s provider proof read: `provider_state.rs:133` immediate termination took 2.45971328 s against 2 s; Immediate obtained the daemon’s exit code through docker wait and then issued a redundant inspect. It now uses wait’s typed exit code directly and refuses a malformed code by name. The qualification writer completed in 13.242 s while the tight whole-file reader exceeded its unchanged 15 s helper bound; the reader now yields after each complete checked read, retaining all 64 replacements, fsyncs, coherence assertions and bounds. Full run log `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-700ab971release.log`, per-job reads beside it. Other red jobs are retained there; none is treated as evidence that the required container proof passed.

Held for [Q50](proposal/i143-q50-silent-container-recovery.md), candidate `425a27d9`. Current branch RED commits after rebase: mounted namespace `3a0cce7e`; prebuilt acceptance execution `904cad92`. Provider controls green, 12/12 in 3.89 s (`s88-container-provider-controls.log`); Immediate terminate 526 ms. Container kill/recovery and its controls green, 6/6 in 45.80 s (`s88-container-kill-current.log`). Five fault cells ran concurrently: four green, single-node silence/heal red at `estate.rs:405` (150 s healed-view wait, 210.84 s total), `/home/admin/rust-distributed-mesh-deploy/s88-container-observers-live.log`. Its cut crossed the declared 30 s R-FM1 hold and the mesh-primary correctly terminated and recovered the birth; its same-birth assertion then could not hold. Flame [terminate/recovery](http://localhost:16686/trace/152a6c37517d0c0efdd97aa79411e186), [held-running](http://localhost:16686/trace/92c86f4de4c21a14e33aa13552296d89); raw/queried evidence `/home/admin/rust-distributed-mesh-deploy/kept/s88-q50/`. No hold, timeout or recovery policy changed. s88 is not landed; the release acceptance criterion remains open. The preceding release’s green 2777 flames are [restart canary](http://localhost:16686/trace/e06174453aadc4cb647f29b375c54515) and [seed runner](http://localhost:16686/trace/745a23b3472699c9358c330986b6a034), canonical evidence and verified imports in `kept/s88-700ab971/2777-flames/`.

Q50 A implementation: the planned cut is one declared staleness floor plus 20 s and is asserted below `RDM_HUNG_HOLD_FLOORS × floor` before any filter is installed. An independent release thread lifts the cut at that deadline or sooner when the observations finish; the receipt records the planned, declared-hold and actual durations. All three route samples run concurrently, together with their exact provider/filter and typed-view reads; each sample retains its Direct → ViaPeer → NoActiveRoute sequence. Every provider, identity, value, routing assertion and existing bound stands. Concurrent probes append each complete RPC-ledger record in one write. No runtime or hold policy change; the expired-hold case remains the recorded follow-on. RED stays in `/home/admin/rust-distributed-mesh-deploy/kept/s88-q50/cells.log` and `s88-container-observers-live.log`; its code is the branch’s pre-fix commit `a689c070` (pre-rebase `425a27d9`).

Q50 control `3d98692b`: the cuts lifted at 16.831 s and 18.051 s under the 30 s hold, preserving every birth. The new code also parallelized the legs inside each sample; that changed the existing Direct-before-ViaPeer ordering and both cells correctly refused the resulting ViaPeer `Indeterminate(ReplyDeadline)` at `container_fault_backend.rs:558`. The fix retains the sequence inside each complete sample and runs the three samples concurrently as ruled. RED log retained as `/home/admin/rust-distributed-mesh-deploy/s88-q50-parallel-leg-red.log`; all assertions and bounds stand.

Q50 A live controls GREEN: `/home/admin/rust-distributed-mesh-deploy/s88-q50-sequence-controls.log`, 2/2, 60.57 s, all assertions unchanged. The node cut lifted at 20.504 s and the mesh cut at 22.082 s, both under the declared 30 s hold and the 23 s plan; Direct and ViaPeer served the original births and values after healing, all three held ViaPeer samples refused `NotSent(CarrierEdgeLost)` by name, no recovery or new attempt. The original RED remains kept. Rebased onto i144 e6.2 (`7cbec256`, main `afff4d44`), preserving its new fabric_takeover gate job while keeping acceptance execution on the suite’s prebuilt artifacts; demo fixture pins the rebased runtime ancestor `ed939c9e`. Next proof: all required container jobs in the concurrent release suite.

Concurrent release `c0cfc6d4` read, 695 s; log `/home/admin/rust-distributed-mesh-deploy/s88-q50-release.log`. 2777 container green 2/2; chaos-container 3/5. The mesh cut cell failed in setup, `typed.rs:566`: `node.create:mesh2.rpc.1` exited before `runtime.json` was available within its 10 s birth bound, trace `2f865632d170c7ebacb8a61b71c50d54`; no cut was installed. The node cut’s independent release fired at its declared deadline and the original hold did not expire; `container_fault_backend.rs:404` refused unfinished observations. Provider proof: `provider_state.rs:133` immediate termination 2.987 s against 2 s; `container_proof.rs:341` the reader’s 10 s deadline fired before the writer finished. Both bounds and assertions stand. The existing 2777 receipt and all per-job logs are retained under `gates-e7s3-c0cfc6d4release.log.d/`.

Gate defect found at `scripts/rdm-gate-at.sh:366`: each red copied the entire 2.0 GB scenario artifacts directory while sibling cells remained live. This run produced roughly 60 GB of duplicate captures; `container-proof`’s 108.41 s executable became a 254 s job including its copy. Preservation now follows the concurrent test barrier and runtime cleanup, copies one immutable snapshot, and gives each refused job a link to it plus its own log. RED commit `10826c0b`, 0/2 (`s88-evidence-preservation-red.log`); GREEN 2/2 (`s88-evidence-preservation-green.log`), including a source rewrite after capture that cannot alter the kept evidence. This removes the gate’s multiplying disk-copy work from every live cell’s bound; it does not dispose of a red as load sensitivity. Container provider and kill proof executables now also run together; a provider refusal never prevents the kill cells from running. No test bound, hold or runtime policy changed. Next gate is a changed implementation, not a rerun of `c0cfc6d4`.

Post-barrier release `49eac561` completed in 632 s. Evidence preservation ran once after the test barrier (1 s); no per-red copy ran during a live cell. Container kill/recovery controls GREEN 6/6, 131.06 s; 2777 GREEN 2/2; chaos-container 4/5 (node silence/heal, peer-mesh recovery, mesh retirement and exact-container pause/kill GREEN). The mesh-cut cell missed its 90 s unheard-view window at `estate.rs:405`: provider/container address inventory consumed part of the timed silence, then namespace installation, listing, inspection and restoration each walked the members sequentially. Preparation now resolves the exact inventory before the cut clock starts; independent namespace actions and inspections run concurrently, with successful installs still restored on any failure. The 23 s plan, 30 s declared hold and every assertion/bound stand. Both live cut controls GREEN 2/2, 57.70 s (`s88-parallel-namespace-controls.log`), actual cuts 19.291 s and 19.648 s.

R14’s release failure remains read, not dismissed: `container_proof.rs:339`, first hard-link phase exceeded 15 s; `provider_state.rs:120`, its child wrote no heartbeat before its existing bound. A registered-cell syscall diagnostic (same code, diagnostic Docker wrapper only) passed in 2.57 s with 138 fsync calls and slowest recorded filesystem syscall 23.761 ms; this does not reproduce or dispose of the release miss (`s88-store-syscall-cell.log`, `s88-store-syscalls.log`). The qualification helper now retains partial child stdout/stderr even when its unchanged absolute 15 s deadline fires, names host process-spawn time and logs entry/before/after sync stages; a timed-out CLI is killed rather than left running. Capture control GREEN 1/1 in 1.46 s (`s88-store-output-capture-control.log`). No runtime or bound change. Next release will retain Docker exec event times as well, to distinguish dispatch/startup from the filesystem stage.

Preflight `38093c90` was cancelled after build/discovery and before any test jobs started: review caught output draining outside the command’s 15 s deadline. Process exit and both output drains now share the original absolute deadline. A diagnostic CLI that emits one stdout and one stderr marker then never executes the Python phase is refused at 15 s with both markers retained (`s88-output-timeout-negative-control.log`, 15.78 s including fixture cleanup). This is an intentionally injected negative control, not an acceptance pass; it proves the failure message names what ran. Preflight log and owned-process cancellation record are kept as `s88-physical-cut-release.log` and `s88-38093c90-preflight-cancel.json`. No test bounds changed; no result from that preflight is used as conformance evidence.

### Remaining

- [x] A functional cell proving hard-link insertion (`create_new`), atomic rename (`replace`), file and directory `fsync`, and coherent reads and listings across containers sharing the container provider's mounted `RDM_STORE_DIR` (`R14 item 6`). **Landed (record of 2026-10-18):** on main (`container_proof.rs:256`, `container.rs:222`, `rdm-gate-at.sh:337`).
- [x] `ContainerDeploymentProvider` bind-mounts the fabric's `RDM_STORE_DIR` into launched containers and passes `RDM_STORE_DIR` in the container environment; `store_root::qualify(ProviderKind::Container)` enabled once the qualification cell holds. **Landed (record of 2026-10-18):** on main (`container_proof.rs:256`, `container.rs:222`, `rdm-gate-at.sh:337`).
- [x] Unskip `i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` (`RDM_CONTAINER_STORE_QUALIFIED=1`) in `rdm-gate-at.sh` and record their Jaeger flames here. **Landed (record of 2026-10-18):** on main (`container_proof.rs:256`, `container.rs:222`, `rdm-gate-at.sh:337`).
- [x] **Q50 = A** (Luke, 2026-10-17; `docs/plans/rulings/i143-q50-silent-container-recovery.md`): `container_node_unheard_heals_serves_same_birth` held its cut 30.5 s against the 30 s R-FM1 hold, so mesh2.admin.1 correctly terminated and recovered a new birth ([flame](http://localhost:16686/trace/152a6c37517d0c0efdd97aa79411e186)); the cell asserts its planned silence is under `RDM_HUNG_HOLD_FLOORS × floor` before the cut and collects its three route observations concurrently; every assertion and bound kept; no runtime change. **Landed** in s88 (`0abdf4b4` and its siblings).
- [x] A container cell proving the expired hold ending in `TerminateRuntime`, the provider's exit proof and a new birth with the value retained (the case this flame showed): `container_fault_backend::container_node_held_past_the_hold_is_terminated_and_recovered_serving_its_value`, release-only in `i143-2784-chaos-container`; green 35.1 s, the terminate 25.4 s after the cut against a 6 s hold, flame http://192.168.68.99:16687/trace/ba470037a15c96c4d4142b05d6aeea8e. Landed in the landing that carries this row.
- [x] **Q53 = A** (Luke, 2026-10-17; `docs/plans/rulings/i143-q53-successor-member-status.md`; Codex filed it as Q51, renumbered): `container_peer_mesh_lost_recovers_under_same_mesh_id` red in its healthy setup because the successor mesh-primary's seat fold read zero declared rows and its first complete version published Pending over two births' applied Ready ([fold](http://localhost:16686/trace/15cb9cd91901c07ab8111839908d878b), [regression](http://localhost:16686/trace/1b56aa5ce40f7d173648b8955b64b6ef)). Codex names the cause and fixes the root in the existing acquisition path, RED first with the exact-birth successor cell; a mesh change, permitted by the ruling; lands before s88. **Landed** in s88 (recorded `8bd6681d`).

### Acceptance criteria

<!-- crit:e12.s88.1 -->
- [ ] The container store-root qualification cell (`R14 item 6`), `container-proof` (`container_proof` + `container_kill`), `i143-2777-container`, and `i143-2784-chaos-container` green in the release suite with their Jaeger traces cited here.
<!-- /crit:e12.s88.1 -->
<!-- crit:e12.s88.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s88.2 -->

### Done when

- [ ] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 12-2-container-proof]
<!-- /epic:e12.s88 -->

<!-- epic:e12.s89 -->
## e12.s89 — the R-shape release cells: each red read from its own trace and fixed where it lives

**Builder:** the lead with RDM engineers in pool lanes (one per job) · **Tracking issue:** to generate · **Architecture:** §14 Export/import gate; `crates/rdm-test/tests/rshape_burn_in.rs`; `tools/mesh-audit/i143-acceptance-jobs.json` `rshape_jobs`; [node-restart.md](/home/admin/rust-distributed-mesh/docs/architecture/lifecycles/node-restart.md); [build.md](/home/admin/rust-distributed-mesh/docs/architecture/build.md); [connections.md](/home/admin/rust-distributed-mesh/docs/architecture/connections.md)

### State

open (added 2026-10-15 from the `7257ff09` release run). Landed: `f877a260` (Forward back on the control-plane client, connections.md "Two planes, two words"); `0f400016` (one executor per attempt of each Build lane, build.md §4.1); `50213bd5` (the AfterStop Ready names node_id and incarnation_id, trace `http://192.168.68.99:16687/trace/54973f48031aa2b78c86410b6b759a89`); `ad0c5d7b` (a view read at a named admin makes the births it lists reachable); `1bbfc1c7` (drift attempts in their mesh's lane, seat-first restart attempt per node-stop.md Seat holder, C14 kept birth, C17 resubmit after a Failed attempt); `c2126546`, `5d4d8c0f` (a gateway's Direct edge is its own originated connection, never a carried call's: connections.md §1, §5, §6, §8, R-CR6; composition asserts `direct-unknown` after carrying); `6c500183` (C8 reads its canary's handler span before the kill); `894bae34` (the replace recipe's operations read from the attempts it opened). Release run at `894bae34`: 81 ok, 4 red (`gates-e4s3-894bae34release.log`); composition 3/3 at `5d4d8c0f`. Open, each a membership, status or connections mechanism for Luke: (1) `peer_mesh_investigation` `an_admin_that_answers_probe_two_is_not_recovered_for_probe_ones_edge_lost` (1 in 2 release runs): the carrier holds no pooled connection to the frozen admin at probe 1, its inner dial times out, and the edge check wired for every carried call (`node.rs:461` `Some(connections)`) answers `CarrierEdgeLost` to the fabric-primary's control-plane Status probe (trace `9ba5d0175c10a47b3034f849aad3fc07`); connections.md §8 defines carrier-edge-lost for a Proxy's carried call; (2) C17 `mock_connections_delete_then_node_delete_preserve_status_and_terminal_proof`: a restart whose stop was not admitted (NotSent at AwaitNodeLeft) leaves the member `Leaving` with nothing withdrawing it, so the node stays not live and no resubmit is accepted (trace `3100a0ae262f62d82e02953ef3c98d36`); (3) C19 `mock_authority_handoffs_preserve_one_effective_writer`: a restarted mesh-primary admin took node.started while the held member status stayed Pending, and the successor's WaitForNodeReady failed (trace `6cf4929cf65bd829247d936ae07312c6`); (4) R3 `mock_recovery_retains_build_and_advances_attempt`: the re-born executor admin's own view lists `mesh2.broker.1` as `restarting` after the successor completed its restart (restart attempt 7 trace `7f2bb821f37197d6c94e8803f8e31faf`).

### Remaining

- [ ] `i143-rshape-authority-chaos-process` (the i144 dev's, read 2026-10-17). Release-run-8c2e79c6's red was `mock_authority_handoffs` at `rshape_burn_in.rs:442`: the probe's file held two traces. Each red, its fix and its read:
  - **Fixed `72bcaed1`, the probe's two traces:** the probe reports its telemetry init steps inside its own `via-probe` root (`init_evidence_telemetry_for_one_call`, `report_init_steps_here`).
  - **Fixed `b59a14c8`, the seat given back (node-primary-update.md step 6)** (`mock_authority_handoffs`, `:1972`): restart attempt 5's `Stopped` row was refused under the successor's own cut-off from attempt 2 ("this store's writes are cut off ... by node.primary.update:…:2:node.restart:mesh2.admin.2"). An admin that moved its mesh seat away kept that transfer's cut-off when a later row gave the seat back. The successor's install now restores the seats the row moves (`WriteCutOff::Restore`, which acts only on a store a transfer cut off). Cell `node_restart::a_mesh_primary_restarted_and_given_its_seat_back_writes_its_mesh_rows`: RED `702cdca3` trace `391c9e461210235e6efb2509b60242cf`, GREEN trace `81b597f1205493b7396f9bcb59f8ea81`.
  - **Fixed in this landing, the start's reads to its latest commander (node-start.md steps 2 and 4; node-topology-get.md: the read is the mesh-primary's)** (`mock_recovery_retains_build_and_advances_attempt`, `:1972`, twice):
    - Attempt 6's executor, the old `mesh2.admin.1`, commanded the broker's start; the start joined it at 18.43 s.
    - The cell then killed that executor, and the start's topology read stayed on the dead birth's endpoint until its 10 s reply deadline.
    - Attempt 7's executor, `mesh2.admin.2`, commanded the start again and was answered that start's `StartFailed` at `node.topology`.
    - The start's topology read and rafka-time refresh now go to the admin whose call commanded it last, as the join does. A read under way to a commander a later call replaced is made again to the later one.
    - Cell `rejoin_commander::a_start_whose_commander_is_lost_after_its_join_reads_its_topology_from_the_later_commander`: RED (StartFailed `node.topology` on the lost commander, `NotSent`). The lost-commander rejoin cells end with their successor's start (60 s each before, on the lost call's budget).
  - **Fixed `694355cd` (RED `85e3540b`), a restart reused by a later attempt (gossip.md:276)** (release run `d829b383`, `mock_recovery_retains_build_and_advances_attempt`, `:1838`): the re-born `mesh2.admin.1` held `mesh2.broker.2` restarting from its own Build facts. Attempt 5 wrote `NodeRestarting`; attempt 6 reused that step (`already-applied, reused`), completed and converged, but `in_flight_ops` recorded the end only under attempt 6, so the marker keyed `(2dqrfv6d4de2, 5, node.restart:mesh2.broker.2)` never closed. A later attempt that carried the operation now records its `Complete` or `Converged` under the restart's own stamped attempt (`build_state` only; no transport or key change). Cell `build_state::a_restart_reused_and_ended_by_a_later_attempt_is_completed_under_its_own_attempt`; R3 green twice at seed 1431101 on the live box, both through the reuse.
  - **Read, for Luke (gossip: evidence only, no change):**
    - `mock_authority_handoffs`, run alone on `368e491d`, failed at `:2063` (checkpoint `C4 mesh1 after mesh1.admin.1`, look 2): `mesh2.admin.2`'s own view held `mesh1.admin.2` `pending-reconnect`.
    - `mesh1.admin.1`'s commanded stop ran `node.mesh.leave` at 22.83 s; it was ready again at 24.26 s.
    - `mesh2.admin.2` heard none of mesh1's members from about 22.97 s to 26.05 s, when it marked all of them stale at 3,075 ms silent (trace `6858cf17a6f20c2ef10d22b306c7a794`). `mesh2.admin.1` heard mesh1 throughout.
    - The first run on `18bf0ffa` passed this checkpoint.
    - Read: one backbone member's feed from mesh1 was not repaired for 3 s after one of mesh1's admins left its gossip neighbours and rejoined.
  - The s89 landing's first gate (`7f9b1f1d`) was red on `admin_start_declares`, i146 e3's cell, not this landing's: four landing gates red, the mechanism read from its own spans for the first time. It is tied to Q58 (`i146-owed-messages-prd.md`).
  - Its second gate (`b3891e0b`) was red on `fabric_primary::losing_the_fabric_primary_mesh_moves_control` (`fabric_primary.rs:204`, i144 e6's cell; kept `gate-gates-e4s8-b3891e0b-fabric_primary`), the same mechanism as Q56's gate `1dbe530c`:
    - The new fabric-primary's change spent 6,729 ms in acceptance (`via-accepted` 42.934 s, busy 57 ms, idle 6,673 ms). Its lane seal came at 49.193 s, and its drive opened at 50.003 s, after the verdict (49.769 s). So the drive never met the fence the cell asserts.
    - Both times the acceptance ended within 0.3 s of the investigation's verdict.
    - Every holder of the Build journal's writer lock (`build_state.rs` `FileJournal`) does file I/O only (sync, append, seal). None waits on the network.
    - Each writer's turn of the Build journal is on its own span (`rdm.node_admin.build.update.via-journal-turn`: `waited_ms`, the turn named in `held_by`, `held_ms`, `lane_ms`), so a wait on another turn is told apart from a wait on the store root. A clean isolated run (13 ms acceptance, no turn over 200 ms) does not reproduce the wait; the next red names its holder. i144 e6's, owned by this dev.
  - Its third gate (`299ab00f`, the merge of main with Q58) was red on `accepted_topology::the_build_fabric_build_id_names_is_the_one_accepted_topology` (`typed.rs:641`; kept `gate-gates-e4s8-299ab00f-accepted_topology`). It is the Build journal wait with its holder named:
    - The created `mesh1.admin.3` waited 10 s for its `runtime.json` and exited (code 3). Its pipeline stalled 11.4 s between `DeployRuntime` (done 8.137 s) and `RegisterExactRuntimeHandle` (19.537 s). The other steps' gaps, which are their receipt appends, are 9–60 ms.
    - The gap is `DeployRuntime`'s receipt append into the Build lane `fabric/builds/rev-…01` (`pipeline.rs` `step_with`, `append_step_receipt`). It ended 5 ms after the fabric-primary's commit-state publication ended (`via-round-action`, 4.097–19.532 s, trace `7ec3cdbf2268c21cefa2b33036b24eae`).
    - That publication wrote 90 rows in 15.4 s, 63 of them into that lane. `publish_held` (`record_store.rs:567-592`) holds the store's write lock through the blocking job that writes every held row, each a temp write, fsync, rename and directory fsync (`write_file`, `:812-820`). A lane append (`writer()`) waits for the whole store.
    - `fabric_primary`'s 6.7 s acceptance (gates `1dbe530c`, `b3891e0b`) overlaps no publication: its holder is named by the journal-turn span on its next red.
  - Its fourth gate (`8ad4defc`, run with no other gate on the box) was red on `accepted_topology` (the same publication wait) and `mesh_create::creating_a_mesh_hands_its_members_to_its_own_primary` (`mesh_create.rs:215`: mesh2's own primary found no row from the fabric-primary's claim, `outcome=row-written`).
    - Neither cell emits a seat cut-off, restore or seat-row span, so s89's install-time restore did not run in either.
    - The same two cells, in one parallel run, on main `8ecdd1bb` and on `8ad4defc`: main failed `mesh_create` at `mesh_create.rs:215` and passed `accepted_topology`; s89 passed both. `mesh_create`'s red is main's (evidence kept in the scratchpad, `s89cmp/main-mesh_create-artifacts`).
  - `mock_authority_handoffs_preserve_one_effective_writer` red on the DevX box's release run at `d8c2e8ac` (`rshape_burn_in.rs:1838`, authority not stable in 30 s; mesh2.admin.2 refused mesh1.admin.2's full v5 65 times, `reject.via-inconsistent{frame=members}`). Read 2026-10-19 here (lane e6s4, main `c7104450`, three green runs under the turn lock, each refusing the same way: every member of mesh1 refused mesh1.admin.2's full v6 for 49 s and every member of mesh2 its v7 for 29 s, healed only when a later handoff brought a new publisher). The delta 4 → 5 (mesh1.admin.2's, span `adfa59d620950e77`, trace `fb45310e7b0f0c610f03a703b7b1addc`) carries `departed: [node.delete:mesh1.admin.1 @ 0s1cwtzhp4m0/2]` and nothing else. v4 held two overlays of the departing birth: NodeDeleting (`node.delete:mesh1.admin.1`) and NodeStopped (`node.stop:mesh1.admin.1`, published at `90b6e13aeeb08b93`), installed at mesh1.broker.1 as `in_flight=2` (`d0225905593210ca`). The publisher's book ends both at the departure (`DigestBook::depart_at`, `membership.rs:4503-4504`: `end_stop`, then the key's overlay), so its full v5 is `in_flight=0, departed=1` (`a3599f17cc2289fb`). A receiver's `Full::apply` (`snapshot.rs`) removes only the overlay keyed as the departure, so its delta-built v5 keeps the NodeStopped overlay (`9bcea0dd347aa5c6`); `same_topology` then fails on `in_flight` and every full of that version is refused as inconsistent (`snapshot.rs:604-605`, first `a890de731cf2634f`). The field that differs is `in_flight`: the departed birth's NodeStopped overlay. A unit probe reproduces it (not landed). Page: gossip.md:278-280, "NodeStopped stays in the in-flight overlay until the next lifecycle op on the same `(node.id, incarnation)` … node.delete replaces it with NodeDeleted's deleted retention." The receive path breaks that line; the fix changes how a receiver applies a MembersDelta, so it goes to Luke (a receive rule): no change made.
- [x] `i143-rshape-lifecycle-chaos-process` (the i144 dev's, read 2026-10-17 from `release-run-8c2e79c6.log`): green 3/3 on `18bf0ffa`; red in B's release run at `d829b383` on C17 (`rshape_burn_in.rs:4417`, fixed by `2a6563fb`); **green** in DevX's release run at `bd107007` (423 s).
  - Its three reds in that run, and the authority job's `mock_recovery_retains_build_and_advances_attempt`, failed one precondition, `rshape_burn_in.rs:323`: the cell's provider state counted every `rshape-*` process on the box, so another job's estate's node-admins were taken for its own.
  - A cell's provider state is now its own estate's: a process whose environment names the estate root, a container mounting under it, and the provider actions read under it (`e3942be0`).
  - The `node.connections.delete`-then-`node.delete` cell is renamed for what it runs (Q55; `368e491d`).
- [ ] `i143-rshape-routing-chaos-process`: `mock_connection_churn_releases_superseded_resources` (`:2220`: attempt 1 executed by `mesh1.admin.1` and `mesh2.admin.1`); `mock_routes_survive_connection_and_birth_supersession` (`:2228`: attempt 2 began before attempt 1); `mock_proxy_cutback_requires_durable_retirement` stays green on `f877a260`.
  - C11 in release run `b7548453` (`rshape_burn_in.rs:4557`, "mesh2.compute.1's Direct fact toward mesh1.broker.1 never became Connected on the new birth within 30s"): the cell's bound, not the product. connections.md §9.5 step 3, "Perform one bounded direct transport connection attempt", and §9.3, a failed attempt reschedules from its durable row at `next_due`, exponentially backed off and jittered; no page line says a dial in flight connects when the path returns. The run: one originate call (10 s) made four dials (31.212, 34.219, 37.241, 40.246 s), each bounded by the QUIC idle timeout, the staleness floor (`endpoint.rs` `transport_config`; 3 s at the gate cadence), so the series reached `attempt_ordinal` 4 (`dial deadline`, span `4c62cd8ee3fc1aac`, trace `dbe3e3d047b08ae181c828aebbb83c4d`) in 10 s. The cut lifted about 0.8 s before the fourth dial's bound; its Initial went at +0.08 s and its PTO probes at +1.12/+1.20 s, the next probe past the bound (`mesh2.compute.1` transport-diag), so it ended `Failed("timed out")` at 3105 ms (span `3500d9a3bdb72869`, trace `bf59faebc5189930c13f084bafe1793f`). The series' next attempt was due 32 000 + 3 789 ms later; its recovery dial ran on time at 09:17.011 (span `ea449f63ffc2a46c`, trace `d6d0f7abf08ef07d43939a85ed9efa63`, `due_ms` 1792368557005), after the cell's 30 s. The cell now bounds the wait by the latest Failed row's `next_due` plus one bounded attempt and 10 s to observe the fact, and refuses a Failed row written after that window (an attempt on an open path). Green alone at the job's env (lane e6s4, consumer at its HEAD, 112.7 s): the same ordinal-4 row, Connected 35.96 s after the release by the series' dial (`via-recovery-dial` `fact=connected`, span `cd5f0c0922b1180b`, trace `a372f120f0cbcaaa27727dd5bcd68d62`), which the old 30 s would have refused. Seen, for the lead: one call's back-to-back dials advance the series' ordinal, so a single 10 s call puts the next attempt about 36 s out at the gate cadence; §9.3 permits it.
  - `mock_proxy_cutback_requires_durable_retirement` red on the DevX box's release run at `d8c2e8ac` (`rshape_burn_in.rs:4914`, "the destination's accept woke one attempt of the source's own", outcome `direct-connected`): the cell's order, not the product. connections.md §9.4: the destination falling due under its Failed row and its accept "arm that one reconciler", whichever comes first, and the accept "never creates a second attempt". The cell's Failed(1) row falls due 4 s plus jitter after it is recorded, and the destination's dial probe spent about a second in reads before its accept, so the source's own dial connected first. RED here with the accept held 6 s past the record (outcome `direct-connected`, trace `0257b6e66567db62e6f7d8e237dc933a`). The cell now asserts one dial of the source's own, connected, and the first accept's outcome against that dial's Direct Connected entry: `pending` or `coalesced` before it, `direct-connected` after it, and never a second armed attempt. Green both ways: accept first (`coalesced`, trace `31c08759b2daf3efcc87f017781fb54d`) and with the accept held 6 s (`direct-connected`, trace `c16825596b3b3ea809d3763047b9366b`).
- [x] `i143-rshape-partition-chaos-process`: `mock_control_loss_recovers_without_status_heartbeat` (`:5306`: the observer heard both births of the restarted broker: one incarnation). **Green** in release run `b7548453`.
- [x] C17 (`mock_connections_delete_then_node_delete_preserve_status_and_terminal_proof`, trace `3100a0ae262f62d82e02953ef3c98d36`), read 2026-10-15 (R-PV1 note): the mesh-primary sets `stopping` before the node admits the stop (pipeline.rs:2249, runner.rs:151) and publishes the reserved `NodeLeaving` frame (runner.rs:148; gossip.md:160, R-CR4). Fix: `stopping` on the stop call's admission (states.md:30), never before; the `NodeLeaving` sender deleted, the variant kept (R-G3 pattern), the frame's publish in the C17 trace its flame (gossip change: Luke's permission is R-PV1's note). D1 is not a defect (R-BI1: an attempt of the accepted Build, no Build minted); D3 falls with D2 (`is_live`, model.rs:46). D2/D2b LANDED `69b3ced8` (2026-10-15, gate 66 s, through the land script) from `i143-s89-d2-stopping-on-admission` (lane e4s8): RED `584a5e12` (`node_stop::a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth`, node_stop.rs:480, status Leaving after the release; traces `74310aa8548306715f34179ae4c5752e`, `b6406c2e29b0ebda95d2e999c9333d47`), GREEN `fcc63cb1` (unadmitted stop `403bab4d0c31b1828d0558ca503c077b`: `via-command-sent` not-sent, no Leaving, no frame; admitted stop `4151b63acac7763e811ab7289c6ee5ac`: `via-command-sent` stopped, then `via-stop-admitted` and `from-command-admitted` Leaving, then NodeStopped). The flame also shows the node declaring its own Leaving to its mesh-primary inside the stop (`via-declare-own`, `from-status-report` at 0.043 s), so stopping is visible from the node's report on the admitted call. NodeLeaving keeps position 11 and is refused by name (`rdm.mesh.membership.reject.via-node-leaving`), never forwarded. No new gossip frame, no new MemberStatus. The first land gate (tree `a56309dc`, recorded red) failed `mesh_delete_resume::a_resumed_leave_keeps_the_exited_members_receipts_and_runs_only_the_rest` (:413): its observer has no Node RPC client, so every stop-node is NotSent, and the cell asserted the member shown not live, which held only through the pre-send Leaving; corrected to states.md (the unproven member keeps its status), rdm-admin main 396/396. Nothing is recorded before a `node.connections.delete` command leaves (`98254701`, i144 R22): the mesh-primary records connections-deleting on the node's `ConnectionsDeleting` frame. B's release run at `d829b383` was red at `rshape_burn_in.rs:4417`, the node's Stopped declaration answered `already-applied` (node-stop.md step 5, Q36/Q49), which the cell counted only as `applied`; `2a6563fb` counts both. **Green** in DevX's release run at `bd107007` (`rshape-lifecycle-chaos-process` ok, 423 s)
- [x] for Luke (one doc line or a ruling): `node-not-live` is in no page; states.md:108 says a lifecycle op reaches a stopping or stopped birth, while control_plane.rs:618 refuses a restart of a parked node ("a restart needs a live node"). Is a restart of a stopped node refused, or served as its start? **Answered** by `node-restart.md:21` (Q2).
- [ ] An assertion that states a rule the architecture does not hold is corrected to the doc line it cites; one the architecture does hold and the code breaks is fixed in the code, with the red and green traces cited here.

### Acceptance criteria

<!-- crit:e12.s89.1 -->
- [ ] The five `rshape-*-process` jobs green in one release run, each fix cited here with its doc line and its Jaeger trace.
<!-- /crit:e12.s89.1 -->
<!-- crit:e12.s89.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s89.2 -->

### Done when

- [ ] Every acceptance criterion holds; every landing carries the flame of the fixed path and lands only through the land script; this block's State names the landing shas and the mechanism found per cell.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 14-export-import-gate]
<!-- /epic:e12.s89 -->

<!-- epic:e12.s90 -->
## e12.s90 — R-PV1: the deployment provider reports an OS-stopped runtime; os-stopped is a recorded fact, never a hold or a terminate

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** to generate · **Architecture:** `docs/plans/rulings/i143-rulings-2026-10-14-g3-claim-ids.md` R-PV1; proposal `docs/plans/proposal/update-provider-abstraction-to-actually-work.md` items 1-4, 6, Q1; `deployment-provider.md`; `lifecycles/node-runtime-inspect.md`; `lifecycles/node-recovery.md` step 3; `lifecycles/node-tickle.md` step 1

### State

closed as superseded 2026-10-17: i144 e1–e4 landed (`e113d664`, `3110984e`, `6ff3a672`, `08dfc3a5`-era), see the close-out plan; superseded 2026-10-15 by `docs/plans/i144-provider-v2-prd.md` (e1 the four states, e2 the fact and the recovery tick, e3 resumption and the delete of a suspended runtime, e4 the canaries): this block builds nothing; its rows are i144's R1-R20

### Remaining

- [x] `DeploymentStatus::OsStopped` (provider.rs:298); every match on `DeploymentStatus` (42 `Running` arms across rdm-admin and rdm-node) handles it by name, container.rs:630 `signal_stop` included (it signals a `Running` runtime only) **Superseded** by i144 e1-e4 (the os-stopped fact, `71ae4b73`).
- [ ] process.rs `inspect` (:443) and `alive` (:127): the state letter of every task of the live thread group and of tracked children; all live tasks `T`/`t` → `OsStopped`; any other live task → `Running`; none live → `Exited`; start identity unchanged. container.rs:621: `paused` → `OsStopped`
- [ ] `node.runtime.inspect` answers `os-stopped`; span verdict `os-stopped` (node-runtime-inspect.md:31)
- [ ] the durable status-row fact `os-stopped { since }` written by the mesh-primary, surfaced on `node.get`, not a gossip `MemberStatus` (wire untouched); cleared when the runtime runs again; no R-FM1 hold and no `TerminateRuntime` from this observation (Q1)
- [ ] the recovery tick re-inspects an `os-stopped` birth every tick: `Exited` → the ordinary exit proof; `Unknown` → the report, no hold; `Running` → RPC recovery permitted, fact cleared
- [ ] a `NotSent` command marks the birth `PendingReconnect` at once (recovery's ladder now, not after the floor); no second ladder in the executor; node-tickle.md step 1 gains the reason
- [ ] the pending-action rule: `node.start` only when start is the pending receipted step; a restart whose stop was never admitted resumes stop-then-start through the resubmit or the lane's next attempt
- [ ] page text (deployment-provider.md inspect, node-runtime-inspect.md result/sequence/verdict, node-recovery.md step 3, node-tickle.md step 1) lands with the code; exact old and new text recorded here for Luke's yes before the landing
- [ ] cells: the process provider on a child it SIGSTOPs (`os-stopped`) and SIGCONTs (`running`), with a second thread running during the stop of one (`Running`, the thread-group rule); the container provider on `docker pause` / `unpause` (named skip on a host without docker, `RDM_REQUIRE_CONTAINER`); a stopped birth re-inspected to `Exited` after SIGKILL; each with its Jaeger flame

### Acceptance criteria

<!-- crit:e12.s90.1 -->
- [ ] the four cells green at gate cadence, traces cited here; no `TerminateRuntime` and no `held-running` in the os-stopped traces
<!-- /crit:e12.s90.1 -->
<!-- crit:e12.s90.2 -->
- [ ] C17's os-stopped window shows `rdm.node_admin.node.resolve.via-provider-inspection` with verdict `os-stopped` within one tick of the NotSent (depends on e12.s89's D2 fix landing first)
<!-- /crit:e12.s90.2 -->
<!-- crit:e12.s90.3 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e12.s90.3 -->

### Done when

- [ ] Every acceptance criterion holds; the landing carries the flame of the fixed path; it lands only through the land script; this block's State names the landing sha and the mechanism found.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s90 -->
<!-- epic:e12.s23 -->
## e12.s23 — seats and the Build-door admission: a seat moves only by explicit yield or proven loss (R-EL1 = Y + D, R-MT1 node.primary.update), and nothing that moves a seat or writes caller state runs before the initial fabric is ready

**Builder:** RDM Rust engineer (/home/admin/rust-distributed-mesh, a pool lane) · **Tracking issue:** #3016 (generated once; not read back) · **Architecture:** fabric-node-lifecycle-elections.md §5-§6; lifecycles/states.md "The initial fabric before ready" (#fabric-not-ready); lifecycles/fabric-primary-update.md (precondition); node-rpc-envelope.md Build refusals (#fabric-not-ready-refusal); rulings R-EL1, R-EL1a, R-PB1, R-PC1

### State

R-D1 drive landed bd201c0c; R14 shared store landed fbcb087b (absorbed s43); landed a35ad7f6 (parts 1-2, R-PB1/R-PC1): one admission check `http.rs::admit_once_ready`, `Rejected { reason: "fabric-not-ready" }` on the Build carrier, `StatusReply::RejectedFabricNotReady` for node.update (durable format moved), the `FirstReady` latch on the ready fabric-event row (the fabric-primary now puts its own mesh's row), the typed live cell (7 refusal traces, e.g. 38fb1708b064d59ec282dc4f965b6486), the estate's REST polling wait deleted. R-MT1 landed 6aae90ee (2026-10-13, gate 77 s): node.primary.update and node.fabric-primary.update (the holder writes the one committed row, the successor installs from it, the holder announces "primary updated"; the 0x21 Take/Taken frames, the SeatTransfer step and the fabric/seats rows gone), R-PB1 decided on the fabric-primary's ready row put before its latch (C'), the door's acceptance its last write, the holder-lost case (the successor of a proven-exited holder and a restarted holder say "updated"), a proven-exited member is not another node of its mesh, the fabric seat moves alone before a mesh retire of its own mesh. Red 2 and the drive's park-gate turn landed 7b9ff487 (gate 104 s); Red 1 (the fabric-primary's own seat update runs under its claimed attempt and ends `HandedOff` after step 7's "primary updated", with the successor reattaching at step 8) landed 3bb4b346 (gate 147 s), and the executor's own run gated landed b3cfde0d. R-SB1 (Q3: seat-holder stop/restart refusals, `fabric.delete` and final-admin exceptions, and the four `store_root_boot` replacement cells) LANDED 95e10533 / 75219e6b (2026-10-13, gate 83 s, build 67.3 s wall; RED d6be024b; trace 893deceb77302425afbee48d97dcb764). R-PU2 (Q8-Q10: `the new holder's process is gone, provider-confirmed, after the committed row`, run component in `node.primary.update:<build_id>:<run>:<op>`, 0x21 `Failed { step, reason }`, `via-seated-again` writing P's record before announcing after `via-successor-ladder` ends exit-proven; `mesh-audit` resealed) LANDED 19f268be (2026-10-13, gate 107 s, build 77.1 s wall; RED 8e77f712; traces e5dc9045a468c89e4055913d7f46cefb and 0eb2c0ebcd667d476c5b55753c5d87e2). The 13 Category A hand-off scenario cells (`mesh_elections__sticky_seat`, `mesh_elections__fabric_primary`, `mesh_elections__late_authority`, `mesh_runtime__successor_adoption`, `mesh_lifecycle__mesh_recover`, `mesh_membership__backbone`, `fabric_build__build_op` x3, `mesh_runtime__container_kill` x2, `drift_convergence` x2) were rewritten to transfer `fabric-primary` before taking the former holder down and unskipped in the gate LANDED dc980004 (2026-10-14, gate 151 s, `TIMINGS wall=74.7s units=455 max_concurrency=11 unit_sum=215s`; traces 0893da23d65473468a7342f8bc36eb7c / 30318e3cb83fa8a98823f60d15ed80d0, deb531511853a6e96cb77df3f263c358 / ff2785acd660abc9cc406b207d2c09e2, 0c9d702b0726dd0a00c1943b03cffa9c / 569f12075e78b27cbf29d3a8430df88c, bc9c3ff62530a00571d0938c97a7f202 / 558adaaabe9dc35e78a8f985ac31bc84, 29f67173633cc218f1467ebc8e52349e, b17ff10db9820837661e9b7730e3f9dd / d6601deacd9c6f9b3cfef39d543bcafe, 755c304cbbeba96fd6ab12b166a5e7ab / 3450b8a41ed9f126a47ced8d6806d115). Day-0 seat installation at `Ready` (before `until_ready` returns), `StopNode` projecting the stopped state and waiting for the mesh-primary's `NodeStopped` row before returning `Complete`, and restoring `sticky_seat`'s exact `attempt == transfer_attempt` assertion and `stop_and_rename` / `until_ready` cells without extra waits LANDED c01cb3ef (2026-10-14, gate 149 s, `TIMINGS wall=74.6s units=455 max_concurrency=10 unit_sum=208s`). The remaining 6 cells in `FP_KILL_SKIP_ARGS` (`mesh_elections__fabric_primary::losing_the_fabric_primary_mesh_moves_control`, `fabric_build__build_op::a_successor_fabric_primary_*` x3, and `i143_acceptance_2803` `fabric_recovery_*` x2) plus built-in `node.primary.update` / `node.fabric-primary.update` C2 step-1/step-2 and dual-seat resolution in `serve_update_primary` and `NodeRpc` / `Nodes` cleared `FP_KILL_SKIP_ARGS=""`. R-G3 (Luke 2026-10-14, arm A; rulings page i143-rulings-2026-10-14-g3-claim-ids.md): peers resolve a holder's contacts from the digest; `Frame::NewFabricPrimary` is a reserved position (14), its dead sender, receiver and `SeatBook` contacts side-table deleted, gossip.md's row, example and section gone; no wire change. The three lines that named the announcement (build.md:554, node-rpc-envelope.md:1418-1419/:1423) now name the digest alone (Luke's yes 2026-10-14). Left: nothing

### Remaining

- [x] R-MT1, design of record the approved proposal `docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md` (Luke yes 2026-10-12): `node.primary.update` and `node.fabric-primary.update` (which replaces `fabric.primary.update`, its 0x21 TakeFabricPrimary/FabricPrimaryTaken and its page). The cohort's row writer (the holder, for the node-admin cohort) pre-checks a successor, publishes "primary updating", cuts off and drains, writes the one committed row (`meshes/<holder mesh>/seat-transfers/<operation>`, one or both seats; the `fabric/seats` rows go), then calls PrimaryUpdate on the successor, which installs from the row and answers `node.primary.updated`; "primary updated" follows, the committed revision their order key in the gossip receive path (SeatedTogether retires: a gossip change with its flame). No successor-side refusal but conflicting payload and fabric-not-ready (no WrongBirth). After the row the successor is the holder whatever the call returns; discovery is not installation. Refusals with no successor use Q7's exact texts (landed 6aae90ee, 3bb4b346, 19f268be; non-admin cohorts landed e7ffb680 / 933b0c61 in e12.s41)
- [x] R-G3 (Luke 2026-10-14, arm A): "updated" carries no contact address; peers resolve a holder's `endpoint_id` and `transport_addr` from its `Digest` and `Members` projection through the live resolver (node_rpc.rs `feed_once`, live.rs). `Frame::NewFabricPrimary` keeps position 14 as a reserved position, never published, refused by name on receive; `announce_new_fabric_primary`, `note_new_fabric_primary`, `FabricContacts`, `attach_fabric_contacts` and `fabric_contacts` deleted; gossip.md loses the catalog row, the decoded example and the "New-fabric-primary and contact addresses" section, and names position 14 in its reserved-positions row and section; frames.md / spans.md regenerated. Flame (Jaeger 192.168.68.99:16687): 0e8028bd75dde6f1fa7a9805b1fa08b7, eaa335e5935a3af0beeb581f5f85076e, 9171f3aca1237368a47452e4a68d98f9 (same mesh); 01d16476139da2d8c64a88852eae5c8a, 33cf0bfa84389031b4123798469eb8f7, 7f2c45aaff44122ff1aa07187f107808 (cross mesh); none carries a `via-new-fabric-primary` span
- [x] build.md:554 and node-rpc-envelope.md:1418-1419, :1423 say the contacts come from the exact birth's digest alone (Luke's yes on the exact text 2026-10-14); `git grep new-fabric-primary-and-contact-addresses -- docs/architecture` names 0 hits
- [x] the gate skips the ten 0x21 TakeFabricPrimary/FabricPrimaryTaken cells that send a Take directly and then read the incumbent: node.fabric-primary.update replaced 0x21 Take/Taken, deleted the ten 0x21 cells, and removed their gate skips (landed 6aae90ee)
- [x] the cells that kill the fabric-primary's admin with no planned hand-off (Luke's ban; audit 2026-10-12): 13 Category A cells (`mesh_elections__sticky_seat::a_dead_fabric_primary_hands_the_seat...`, `mesh_elections__fabric_primary::losing_the_fabric_primary_mesh_moves_control` and `::three_mesh_primaries_keep_the_day0_holder...`, `mesh_elections__late_authority::a_lower_node_id_admin_born_after_a_build...`, `mesh_runtime__successor_adoption::a_successor_manages_births_it_never_launched`, `mesh_lifecycle__mesh_recover::a_lost_mesh_recovers_as_itself...`, `mesh_membership__backbone::membership_rides_mesh_channels_and_the_admin_backbone`, `fabric_build__build_op::a_successor_fabric_primary_starts...`, `::a_successor_fabric_primary_reads_the_create_mesh_receipt...`, `::a_successor_fabric_primary_settles_a_create_mesh...`, the two `mesh_runtime__container_kill` sigkill cells, and `drift_convergence::a_durable_runtime_row_of_a_departed_birth...` + `::a_restarted_mesh_primary_recovers...`) were rewritten to run `node.fabric-primary.update` first and unskipped in the gate (landed dc980004, gate 151 s); `store_root_boot`'s four restarted-admin cells were replaced and unskipped in 95e10533 (gate 83 s); `poc_chaos_kit::poc_rp1_kill_every_admin...`, `estate_teardown::a_dropped_estate...`, `drift_convergence::a_lost_fabric_primary_is_recovered...`, and `build_takeover::a_successor_admin_completes_the_same_build...` landed 533cd70c; the remaining 6 cells in `FP_KILL_SKIP_ARGS` (`losing_the_fabric_primary_mesh_moves_control`, `fabric_build__build_op::a_successor_fabric_primary_*` x3, and `i143_acceptance_2803` `fabric_recovery_*` x2) plus built-in `node.primary.update` and `node.fabric-primary.update` C2 step-1/step-2 and dual-seat resolution in `serve_update_primary` and `NodeRpc` / `Nodes` cleared `FP_KILL_SKIP_ARGS=""`
- [x] the post-landing reds (part 11 found both roots). Red 2, an own-mesh member's status held twice in the digest book, fixed on i143-s23-after-land (RED e1eb014e, GREEN bf44123e, a4402742; build_op lost-executor flame 29b68b2dadc1698aab747c3cde0ebf0a, no Pending revert after node.started); its landing gate (342c0434) went red on park_gate_quiesce, and so did s60 part 11's (09348873, scenario-only) on main c7368e46: main's Day-0 Build drive runs outside the gate across the stop's park and transport cut (41 ms and 499 ms past the cut; the second's claim broadcast refused after the cut), the cell's Must-NOT; red 2 and the drive's root fix LANDED 7b9ff487 (2026-10-13, gate 104 s, its build 43.0 s wall): each pass of a Build drive is a turn of its admin's park gate (build_drive.rs:770 `turns.turn("build-drive")`), so a stop's quiesce waits for the pass in progress and a stopped admin begins none (RED cc3b9f58); park_gate_quiesce runs in the gate again. Red 1, sticky_seat :332: the successor's drive dispatches the holder's stop on its own view before node.primary.updated is acknowledged, so the holder's "primary updated" goes out on parked channels; part 13 built the successor announcing before its dispatch (RED d75fb818, GREEN b1db8031, not landed); the lead held it: the design's step 8 ("Only then does the commanding Build continue", after step 7's "primary updated" by the holder, R-MT1 "NA1 gossips") and the build rule ("A live run is reattached, never re-claimed"; build.md:365) put the root at the Build continuing before the holder's step 7 (N claimed attempt 3 at +92.9 ms while P's step-6 call was in flight), and the successor announces only as the next writer after the holder's departure proof; part 14 (lane storage) fixes it at the claim or the handoff, and gates the executor's own run (build_run.rs:414, the same class as the drive). The gate skips a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts until part 13 lands (red on gates 6bc92cf8 and 0021b424) Red 1 LANDED 3bb4b346 (2026-10-13, gate 147 s, its build 74.4 s wall) at its source: the fabric-primary's own seat update runs under the attempt it claims for itself and ends HandedOff after its step 7 "primary updated"; the successor reattaches and claims the next attempt only then (step 8), and announces "updated" only after the holder's proven exit; the holder's own attempt releases its mesh's in-flight row at its verdict; with it, part 16's holder retry through node-recovery's tickle ladder on the successor's exact birth (the seat vacates only on the provider's exit proof) and a refusal before the row failing the Build by name. sticky_seat's hand-over cell and node_stop's mesh-primary stop cell are tied to R-NS2 (a "primary updated" sent 38.7 ms before the holder's transport cut is lost on the backbone at the cut). The executor's own run gated LANDED b3cfde0d (a run served for a call takes its turn of the park gate; a drive pass's own dispatch runs under the pass's turn, never waiting). R-PU2 (A) and R-SB1 (x) ruled 2026-10-13, their units seated (lanes e1s8, storage); fork: after the successor's proven exit the ordinary rule can seat the stopping holder again under the same operation key (A: the step fails by name, a resubmit gets a new key; recommended).
- [x] resolved by R-RX3 (one held version per source across both receivers and the topology read; landed 4d6d0497 / 00c9f9e9, RED c53a73eb, in e12.s53): a non-primary admin keeps one held Members version per source across backbone_rx, mesh_rx, and the topology read so an older frame on the other channel cannot revert a newer read-installed snapshot

- [x] node.primary.update docs applied (docs updated 2026-10-13 on Luke's decision; code landed 6aae90ee, 3bb4b346, 19f268be, 95e10533): lifecycles/node-primary-update.md (fabric-primary-update.md deleted), the envelope's 0x21 PrimaryUpdate block, the lifecycle index, node-stop/node-connections-delete/states/mesh-delete/elections/build.md/node-identity lines
- [x] R-PU2 (Q8-Q10) (docs updated 2026-10-13 on Luke's decision; code landed 19f268be, RED 8e77f712, gate 107 s; traces e5dc9045a468c89e4055913d7f46cefb and 0eb2c0ebcd667d476c5b55753c5d87e2): the failure reason reads `the new holder's process is gone, provider-confirmed, after the committed row`; no "exited" state and no invented names (SuccessorExited etc. go from i143-rpu2: `SeatMoveEnd::SuccessorExited`, `Ended`, span texts "exit-proven"/"successor-exited"); the row key's second component is the run (`node.primary.update:<build_id>:<run>:<op>`, the first attempt of the series a submission opened), named run in code, docs and spans; the commanded arm: 0x21 gains `Failed { step, reason }` (the Build carrier's terminal), `Incomplete` is never used for an ended step, the commander fails the step on the variant and claims the attempt at the claim door before commanding (wire change: mesh-audit reseal, durable format moved, flame of the commanded arm ending Failed); checks on the landing's spans: `via-seated-again` writes P's record before it announces, for each seat given back and no other; no new "epoch" span text; the install only after `via-successor-ladder` ends exit-proven
- [x] R-SB1 (Q3) (docs updated 2026-10-13 on Luke's decision; code landed 95e10533 / 75219e6b, RED d6be024b, gate 83 s; trace 893deceb77302425afbee48d97dcb764): node-stop.md seat-holder row and node-restart.md row (refusal texts; never yields a seat without a successor; the two exceptions: fabric.delete and the final-admin deletion after mesh.delete's confirmed fabric handover — verified Q7 refusal does not fire on those paths); fabric-recovery.md "Nothing alive"; the four retired store_root_boot names left the gate's skip list in the commit that registered their replacement cells
- [x] The gate red at `sticky_seat.rs:551` (`a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts`; read 2026-10-16 from i147's gate `be5102b5`, evidence kept at `/home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e7s3-be5102b5-sticky_seat/`): a defect the gate's timing exposes, not a legitimate vacancy; the cell's assertion stands. The resolver's vacancy is "vacant while the view has not heard the recorded birth" (`rdm-admin/src/election.rs:146-156`, `Holding::Unseen`). The observer `mesh1.admin.1.new` (`gkvdn6yjs5jd`), a birth still inside its own `node.start` topology step (`via-topology-taken` 231.0-255.96 ms past 23:25:39), resolved mesh1 correctly at 241.4 ms (`mesh1.admin.1`, `7yyns8c7sykk`); its topology read re-installed mesh1 snapshot 7 at 248.5 ms (`channel topology-read`, `refreshed true`, `new_revision false`, 6 members); its next view rebuild held 2 of mesh1's 6 members (node-admin inputs `mesh1.admin.1.new=gkvdn6yjs5jd:Pending` only; rpc inputs `mesh1.rpc.3` only) and resolved both cohorts empty at 249.4 ms (vacancy flame http://192.168.68.99:16687/trace/6c6c55baa54c041220d5ff72cc0aabf5); `via-vanished-from-view` (`boot.rs:2374-2380`) named `mesh1.admin.1`, `mesh1.admin.2`, `mesh1.rpc.1`, `mesh1.rpc.2` with the same reason each: the book holds the digest `Ready`, unheard 2 ms, not departed, routable, `removed: []`, `records: none`. The next rebuild (296-306 ms) listed all six and resolved the same holders: no seat moved; the hand-over's own steps came later (revision 2 installed at 23:25:41.099). The view had heard the holder; the projection (`rdm-admin/src/admin/projection.rs` `project_decided_at`) dropped four heard members for one rebuild. Ruled out in the code: the age (`DigestBook::get_at` saturates), `publishes` (age within the floor, no exit, no offline mark), `removed` (empty), the read's own-mesh install (`apply_full`, `(Side::Read, true)` records forwarded, removes nothing). The line that drops them is not yet pinned. The view and the topology read are node-admin mechanism (CLAUDE.md rule 1): no edit without Luke; the next step is his. **Absorbed** by i144 e6.1 (`8c090b74`); `sticky_seat` green in release run `b7548453`.
### Acceptance criteria

<!-- crit:e12.s23.1 -->
- [x] a red cell for the fabric-not-ready check first
<!-- /crit:e12.s23.1 -->
<!-- crit:e12.s23.2 -->
- [x] a seat move and a mesh.delete during the initial state-sync are refused by name with no attempt claimed
<!-- /crit:e12.s23.2 -->
<!-- crit:e12.s23.3 -->
- [x] sticky_seat, build_op, fabric_primary_handover green at gate cadence
<!-- /crit:e12.s23.3 -->
<!-- crit:e12.s23.4 -->
- [x] The story bar holds (section story-bar).
<!-- /crit:e12.s23.4 -->

### Done when

- [ ] Every acceptance criterion's cell is green and asserts its span; a gossip or mesh change carries its Jaeger flame from a live mesh; it lands only through the land script; this block's State names the landing sha and what is left.

**Epic metadata**

- sections: [the-plan-of-record, story-bar]
<!-- /epic:e12.s23 -->

<!-- epic:e10.s2 -->
## e12.s91 — 2026-10-25/26: one read per scope (Q165), the flat create spec (Q166), the store root (R-SR1..R-SR5), Day 0 (Q170) and connection writes that never block (Q169)

### State

Landed from `047db41f7` to `9938fb9f5`, each through the fast gate:

- **Reads (Q165).** One read per scope: `node.get { id }` returns one node or NOT_FOUND, `mesh.get { name }` one mesh, and `mesh.nodes.get` / `fabric.nodes.get` the list (`ba0369835`, `170f95efb`).
- **The create spec (Q166).** `NodeSpec { path, storage, placement, executable, tags, config }`, flat, with no nested configuration type (`487cb9b43`). A create spec's config is the node's first config (`01603c8ed`). `node.config.update` is admitted at any time (`b34284b46`). A Reconcile applies every property that differs from the node's row, each by its own operation (`099797139`). The last write wins while a Build runs too (`0f5ea6025`..`8b7da244b`; build.md §1).
- **The status loop.** A status turn reads the current Build once (`90ad4e710`); the status loop's wait ends on a node's ready (`f25914f49`).
- **The store root (R-SR1..R-SR5).** The `layout` marker; one JSONL file per store (fabric, meshes, nodes, connections, builds); `locks/` for name claims and the attempt in flight; whole-record append-only lines; a malformed line refuses the open (`2d518f912`..`fec54f390`, R14 item 9).
- **Day 0 (Q170).** Day 0 is a single fabric-primary and its own mesh. The Day-0 Build is Complete only from its own ready. fabric.delete ends an attempt the fabric-primary itself executes (`a5cf5c6dc`..`60f951b94`).
- **The view (`a6502701a`).** `DigestView::get_by_endpoint` and `get_by_path_name` are map reads.
- **Connection writes (Q169).** Every write is owed on the node's one ordered writer queue, retried with backoff and never awaited. node.connections.delete closes the connections open now and answers without waiting for a row. A rename and a leave decide from live connections. The reconciler decides from the dial's own result (`bd787d434`..`9938fb9f5`; node-connections-delete.md, node-mesh-leave.md).
- **Withdrawn.** Q168 was withdrawn (Luke 2026-10-25: state-sync is the fabric's state; nodes keep no attempt or storage of it). Its code was reverted (`558578449`, `e0e7b3cb9`, `d62755100`).
- **Docs.** The runbooks (fabric recovery, node storage), `docs/architecture/telemetry.md`, and `docs/eng/consumer-guide.md` with its Tags section.
- **141.** 141 was told every SHA that affects Rafka. Its pin is `9938fb9f5`; it drops its interim `open_direct` patch.

### Remaining

- **Held by Luke:**
  - (corrected 2026-10-27: Q167 was ruled by Luke on 2026-10-25, INDEX row Q167; not held. Built as #3234, lane 6: one plural read per entity, the filter a property match, R-RD1 to R-RD3 of docs/plans/proposal/q167-one-read-per-entity.md, onto node-rpc-envelope.md's read rows and consumer-guide.md);
  - the unlanded `fabric-get-is-the-fabric` branch, superseded by Q165.
- **Unattributed under release load, read on the tail, never re-rolled:**
  - fabric_takeover `a_backbone_executor…`;
  - the heard-again create-in-flight race (`heard_again.rs:96`, `sent=false outcome=create-in-flight`);
  - the peer_mesh_investigation and executables span cells.
- **Store-root follow-ups:**
  - container_proof and store_root_probe still prove R14 item 5's old guarantees;
  - `demo/tests/connections.rs` is built only by the release gate;
  - no cell covers a re-born node's connection facts;
  - no compaction.
- **Rafka's, reported to 141:**
  - the ~100 ms broker read path;
  - the multi-mesh fabric.delete stall. Rafka's storage reads (`<sys>.ops.nodes`, `ops.builds`) count replicas on mesh2's deleted brokers toward coverage. fabric-delete.md deletes the fabric-primary's own mesh last, so the fix is placement and coverage in Rafka's adapter, not an RDM ruling.
- **The tail at one SHA:** fast, release, export run, soaks, `export-manifest --candidate <sha>` (e10.s2, e11.s12).

## e12.s92 — 2026-10-26: Q171, the fabric goes from pending to ready

### State

Luke, 2026-10-26: state-sync, state-commit, the `sync_state` hook, the scratchpad and `fabric-not-ready` are
removed (Q171, Q171.1; the gossip change approved; proposal
[remove-state-sync-and-state-commit](proposal/remove-state-sync-and-state-commit.md)). Landed as `688c32241`
through the fast gate:

- **States.** The fabric is pending, then ready once every mesh the accepted Build names is
  ready and the open-traffic check-ins complete. No compatibility read: a row naming
  `state-sync` or `state-commit` is from a fabric that no longer runs (`durable format moved`).
- **Rows.** Every writer writes to the store root from Day 0; the scratchpad and its publication are gone.
- **Hooks.** `traffic_opened` is the application's one notice (`FabricHooks { traffic_opened }`, fields
  `TrafficOpened`); Rafka's Day 0 is its own. `before_ready` stays.
- **Refusals.** `fabric-not-ready` is gone; the Day-0 Build in progress still refuses every other Build
  `build-in-progress` (Q170).
- **Wire.** StatusRequest 12 and 13, StatusReply 44, NodePrimaryUpdateReply 2 and gossip Frame 15 and 16
  are reserved positions, refused by name.
- **Flame.** Before, at `093f7e071`, trace `3551d75e1956f75cca006b62afab14ea`: the Day-0 change took
  393.5 ms (state-sync 0-160 ms, the commit-state checklist 160-286 ms, open-traffic 286-392 ms). After:
  at `688c32241`, trace `f81152d14ec4fb89ac70cdabd96807e2`: 6.8 ms (every mesh ready at 0, the open-traffic
  checklist 0.1-2.9 ms, the ready row 3-6.3 ms, ready at 6.3 ms, the `traffic_opened` notice at 6.5 ms).
- **141** was told the SHA.

### Remaining

- The tail at one SHA: fast, release, export run, soaks, `export-manifest --candidate <sha>`.
- Read on the 093f7e071 tail, unattributed: `backbone` (a read through mesh1.admin.1 ended `not sent:
  Deadline` while mesh1.admin.2 was isolated) and `i143-rshape-adv-two-victims-process` (mesh1.admin.1,
  no victim, did not answer `mesh.nodes.get` while two admins were cut); `i143-2784-chaos-container`
  `container_primary_mesh_deleted_hands_over_fabric_primary_before_its_containers_end`.

## e12.s93 — 2026-10-26: Q174, the open-traffic checklist is deleted; Q175, the hooks are pre_node_ready, on_node_ready, on_mesh_ready, on_fabric_ready

### State

Luke, 2026-10-26. Q174: "this whole thing gets deleted"; "you create/start a node you dont tell it to open
traffic". Q175 (locked): `pre_node_ready` blocks (no timer); `on_node_ready`, `on_mesh_ready` and
`on_fabric_ready` are fire-and-forget notices. Landed 4514ebd03..400da3ebd.

- **The fabric's ready.** A node is ready once its `pre_node_ready` returned Ok and
  it reported node.started; a mesh once every planned birth is; the fabric once every mesh the accepted
  Build names is and its attempt is claimed. No node is commanded to open traffic and none checks in.
  `lifecycles/fabric-ready.md` folds into states.md "The fabric's ready".
- **The ready row and FirstReady** go with fabric-not-ready (decided by Q171.1).
- **Wire.** StatusRequest 14 and 15 and gossip Frame 17 and 18 are reserved positions, refused by name;
  `rdm.fabric.traffic.opening` and `.opened` are refused before decode.
- **The lifecycle pages name their hooks** (node-start, node-create, node-restart, node-recovery,
  mesh-create, states).
- **Q178** (a create that fails after its launch) is decided by node-create.md and build.md §4.1: outside
  a delete the launched birth is surplus and drift deletes it (node_stop's cell). **Q179**, decided by
  build.md §1 ("launch evidence is retained"): under a fabric.delete the failed create keeps its claim, so
  the teardown's roster holds the birth and deletes it.
- **An interrupted Build's Failed names the delete** when an operation admitted before it fails within
  its bound (build.md §1).
- **Flame.** Before Q171 393.5 ms (trace `3551d75e1956f75cca006b62afab14ea`), after Q171 6.8 ms
  (`f81152d14ec4fb89ac70cdabd96807e2`), after Q174 0.83 ms (`8941aa144cd8595b4eadf37dea0c23ef`):
  every-mesh-ready at 0, ready entered at 0.1 ms, on_fabric_ready at 0.3 ms.
- **141** was told the SHA and the renames Rafka makes.

### Remaining

- Q176 (the state named `ready`), Q177 (the LifecycleTransitionPipeline hooks) and the per-run
  `fabric-<hex>` / `mesh-<hex>` estate names wait on Luke.
- The tail at the landed SHA: fast, release, export, soaks, manifest.

## e10.s2 — export: one RDM SHA on which the whole export gate is green, the import manifest, and the final parity scan

**Builder:** the lead (gate runs) with an RDM engineer for fixes · **Tracking issue:** #2790 (generated once; not read back) · **Architecture:** §14 Export/import gate; §15 Verification artifacts; §17 Definition of done; transport-parity.md

### State

**Exported (2026-10-20, lead): `abc1ec1570c983f419fcb3b43e6adbe8b5712ce8`.** The fast gate `gates-e4s8-abc1ec15.log` (73 ok, 0 FAIL) and the release gate `gates-e4s8-abc1ec15release.log` (110 ok, 0 FAIL), the 65-job export run (none refused, 1,051 s), the twelve soaks (12 of 12, 5,847 s; e11.s12), then `export-manifest --candidate abc1ec15… --repo /home/admin/rafka-v2`: `eligible=true`, 77 owners, 77 receipts certified, 2 gate logs, 81 §14 cells proven, 0 refusals, parity through rafka-v2 `2a02378db4` with 0 MIRROR pending (ledger `sha256:48b134a2…`). The manifest it wrote is committed as `docs/plans/i143-export-manifest.json` (each receipt and gate log with its sha256); the import manifest (`i143-import-manifest.md`) names the SHA, its gate record and the pins (iroh-gossip `508fb795`). The runs before it, each read from its own evidence and fixed before the next (never re-rolled): `768dfaee` (soaks 4 of 12), `4aa58cf0` (9 of 12), `5a220a6a` (two export cells refused) — the row above and e11.s12.

The parity scan has run before (a ledger row per boundary commit, no MIRROR pending). The open e12 builder blocks land first. The export certifier is built (2026-10-19, lane e6s4; its RED first in this landing): `tools/mesh-audit/src/export.rs` and its binary `export-manifest` certify one 40-hex candidate only when `parity::scan` is eligible and every owner's job receipt (each job of the acceptance registry's `jobs` and `rshape_jobs`; the receipt `scripts/i143-acceptance-gate.sh` writes) is `ok` at that SHA, written with no uncommitted path, with every recorded artifact still hashing; each failure is refused by name (`parity-mirror-pending`, `parity-unclassified`, `receipt-missing`, `receipt-not-ok`, `receipt-at-another-sha`, `receipt-dirty`, `receipt-cell-skipped` (a cell skipped by name under an `ok` receipt proves nothing: no container proof without Docker, no cut without `nft`), `artifact-changed`, `artifact-missing`, `candidate-not-a-sha`). Its cells `export_manifest_rejects_pending_or_unclassified_parity` and `export_manifest_certifies_one_candidate_with_all_owner_receipts` (`tools/mesh-audit/tests/export_certifier.rs`) are job `i143-2790-export` (layer export). Run in the lane against main's registry and ledger it names 77 owners, parity clean, every receipt missing. The fast and release gate logs at the candidate prove the import manifest's §14 cells (`378da19f`, RED `ed2015e7`): a log certifies when its first line names the candidate as its full HEAD with no uncommitted path (`[gate] head=<40-hex> dirty=<n>`, which `scripts/rdm-gate-at.sh` writes) and it reaches `DONE` with no `FAIL`; a cell is proven when it is one discovered test a job of a certified log selects, its libtest summaries `ok` with nothing ignored; refused by name: `gate-log-missing`, `gate-unreadable`, `gate-at-another-sha`, `gate-dirty`, `gate-red`, `discovery-missing`, `cell-not-discovered`, `cell-ambiguous`, `cell-run-by-no-job`, `cell-job-ignored`, `manifest-cells`. Both gate stems export `RDM_REQUIRE_NETFAULT=1`, so a cell that cannot cut fails instead of passing with a named skip. The 30 non-soak jobs no receipt on the box had covered ran at `ac34fb66`: 20 green, 10 red; eight were cells stale against the pages, fixed on `i143-e7s3-b2-cell-fixes`; the partition- and routing-chaos-container cells that refuse the container provider are subagent B's (the container cut and descriptor census). The 11 soak jobs wait for one green SHA.

### Remaining

- [ ] Every open e12 builder block has landed.
- [x] One RDM main SHA on which the fast gate and the release suite are green (§14 every subsection), with no red anywhere left without an owning block: `abc1ec15` (2026-10-20).
- [x] The import manifest names that SHA (`abc1ec15`, 2026-10-20); the parity ledger has zero blocking gaps (**2026-10-19: zero**, review-through tip `2a02378db4`, 262 boundary commits, 262 rows, 0 MIRROR pending, `eligible=true`, ledger `sha256:48b134a2…`; the six rows after `77fdfd4948` read by the lead (`d280bf4d`): `267fadf88e` MIRROR on RDM's own endpoint dial and dispatch cells, five RAFKA DOMAIN; 2026-10-18: zero through `77fdfd4948`, 256 rows; the 52 rows after `67755f3cdf` read by the rafka-v2 agent, U5, and applied by the lead; the scanner's ledger and connections defaults now RDM's own copies); i141 pins exactly that SHA, once.

### Acceptance criteria

<!-- crit:e10.s2.1 -->
- [x] `eligible=true` for one exact 40-hex RDM main SHA, every §14 checklist item proven with its artifact (§15): `abc1ec1570c983f419fcb3b43e6adbe8b5712ce8`, 81 cells, `docs/plans/i143-export-manifest.json` (2026-10-20)
<!-- /crit:e10.s2.1 -->
<!-- crit:e10.s2.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e10.s2.2 -->

### Done when

- [x] The export gate's artifacts are posted against the SHA (the gate logs, the receipts and `i143-export-manifest.json`, 2026-10-20) and the manifest is committed; the §17 list holds at `abc1ec15` as the import manifest records it (multi-host eligibility not claimed, as §17 allows).

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 14-export-import-gate, 15-verification-artifacts, 17-definition-of-done]
<!-- /epic:e10.s2 -->

<!-- epic:e11.s12 -->
## e11.s12 — the seeded soaks and the container proof, after the whole suite is green on one SHA

**Builder:** the lead (long runs are the lead's) · **Tracking issue:** #2949 (generated once; not read back) · **Architecture:** §12 Chaos and observability; §12.1 Seeded soak; §12.2 Container proof

### State

**Green (2026-10-20, lead): `abc1ec15`**, the fourth run at one green SHA and the first with every soak green. No soak ran until the fast gate and the release suite were green on one SHA (Luke 2026-10-09); chaos ran last.

**Q87 (2026-10-20, Luke): every node coming to ready from any other state connects to its node-admin for its cert, its time and its cache, in its `before_ready` hook; node-admins included, the Day-0 fabric-primary excepted.** Asked from the demo surface plan ([i143-demo-surface-plan.md](proposal/i143-demo-surface-plan.md), the projection chain under chaos): a node wedged and released, or cut and healed, was never stopped, so node-start.md's hook ran only at a first start and after a stop. Read against the code once ruled, it was the smell Luke named (a step the framework owns reached by a workflow that avoids it): node-mesh-join.md ended the standalone command at step 9 before the hook, node-tickle.md's "commands node.mesh.join" was sent by nothing (the ladder recorded `reachable-silent, no action`), and two hand-rolled rejoins stood in for it (`node.rejoin` in the rpc node's probe handler, `admin.republish` on an admin). Landed from lane e4s8 (the fast gate green at `88141bc4`, 73 stems; the five pages' text Luke's yes, 2026-10-20): the commanded `node.mesh.join` runs node.start steps 2 to 5 on the live birth (`serve_join_mesh`: the birth leaves its channels, says Pending, joins back over the commanding call, its hook, topology, floor and time, ready, and owes its Ready as a start after a stop does; the command is answered on the mesh held again); one function (`heard_again.rs`, `rdm.node_admin.node.update.via-mesh-join-commanded`) registers the birth's join, begins its next cycle and commands it, run by the mesh-primary's ladder once a kick answered `Current` and the ladder folded the birth as reached (never inside a probe the ladder may cancel) and by the fabric-primary's seat watch; the round's down op stays a read (round.rs said in its own words that the probe was relied on to make a member "reassert itself": the hand-rolled rejoin ran on it; the ladder covers the member it reached); to a birth the view holds off ready (the topology decides it, not what a mesh-primary just started has heard: it holds every member from the projection until their digests reach it), once per silence (not within a floor, nor until heard ready or unheard again), by the authority above the birth alone (the mesh-primary its members, the fabric-primary a seat holder: every admin looks, one commands), never a fabric seat holder, never a birth with a command of its authority open or in flight; a held birth heard again by its digest is commanded too; a commanded start that stops at a step leaves the birth off its channels for the next command; the hand-rolled rejoins removed; a probed birth, rpc node and node-admin alike, keeps re-publishing its presence on the channel it is on (the check-in a probed member owes, states.md "The round checklist": the round's down op reads it). Read from the evidence on the way (each a defect of the change, never re-rolled): an admin commanding again, every pass, the start it had itself commanded, a healthy member commanded by the round's first down op, a commanded start whose ready declaration stayed owed because its start was handed a progress stream, a command cancelled with the probe the ladder ended, a held birth that heard again by its digest before the tickle was due (commanded by nothing), a mesh-primary just restarted commanding a healthy member it had not heard yet, a command landing on a birth its authority was starting, a mesh-primary that had just got its seat back commanding the other admin from its first round's down op (the op is a read; the command taken out of it), and a commanded start whose topology read the caller refused as a stale target (its pool dropped the commander's birth, projected then heard, mid-read: the birth leaves its channels and is commanded again), a recovery admin's first round held open past its cell because the probed member re-published nothing (the presence re-publish kept), a commanded start whose join was labelled `node.start` (it names the command's operation), and a commanded join answered on the topic's re-subscription before its join was recorded (answered on the start's own held-again signal: back on its topic with a neighbour, Pending published). The last of them was read from a restarted mesh-primary commanding the other admin while the restart Build was stopping it, and went to a ruling: **Q88 (2026-10-20, Luke: yes)**, a channel reopened after a park dialled only the peers it was seeded or repaired through (a mesh's first admin's seed is the fabric-primary, not on the mesh topic), so a restarted node-admin had no mesh neighbour for 3.3 s, its pruner marked its healthy members stale, and on taking its seat back it read them as heard again. The reopen and the no-neighbours fallback now dial the held members of the mesh too; the book's silence clocks restart at the unpark as before (a restart at the channel's first neighbour after none was tried and read wrong in the provider-state cell: the one admin left with both members suspended forgot their silence and cleared their os-stopped rows as returned). One run (lane e4s8 reg31, `restarted_member_rejoin::restarted_member_joins_mesh_channel_within_gossip_interval`) is recorded as read and unattributed: mesh1.rpc.1, restarted in place, reopened its channel dialling mesh1.admin.1 and mesh1.rpc.2 (the only dial in the window), admin.1 recorded it as a neighbour and replayed its held state to it, rpc.1 recorded admin.1 up and down within 1 ms and kept no neighbour, twenty fallback refeeds over 60 s were each accepted (`joined=true`) and formed no neighbour, and the restart Build's start step ended on its deadline; RDM's spans end there (who closed what is iroh-gossip's own debug layer, `RDM_DEBUG_GOSSIP`, not captured in that run). The same set run again with that layer on was 94 of 94 green; the occurrence is not called a flake and not attributed. The pages: fabric-node-hooks.md, node-recovery.md, node-mesh-join.md, node-tickle.md. RED first: `node/node_heard_again.rs` (an rpc node and the non-primary admin held past the floor and released: the view flipped back to ready on the bare digest in 13 s with nothing commanded); GREEN: both commanded back, each running the start steps its first start ran under one `via-start` root, a fresh cert at the join, the same birth. The export tail runs again at the SHA that lands it.

**The fourth run (2026-10-20, lead): `abc1ec15`** (the two export-cell fixes, the climb's stop race and Q86's soak rule landed): release `gates-e4s8-abc1ec15release.log` 110 ok 0 FAIL, the export run 65 of 65 in 1,051 s, then `--soaks`: 12 jobs in 5,847 s, **12 ok**: the consumer build 160 s; `i143-2787-soak-process` and `-container` 1,876 s each; the five stories in both providers (`soak-lifecycle-process` 1,943 s, `soak-routing-process` 1,949 s, `soak-authority-process` 1,952 s, `soak-routing-container` 1,960 s, `soak-lifecycle-container` 1,968 s, `soak-partition-container` 1,996 s, `soak-randomized-process` 1,891 s, `soak-randomized-container` 1,905 s, `soak-authority-container` 1,930 s, `soak-partition-process` 1,975 s). Each story's `invariant-results.json` (written at the run's end, 12:04-12:38 UTC) holds every invariant with `violations: []` and `ledger_violations: []`, over 30 (routing process) to 350 (partition process) rounds. Then `export-manifest`: `eligible=true` (e10.s2). The container proof ran in the same release (`container-proof` 29 s) and the export run's sixteen container jobs (`i143-2776/2777/2784/2787/2805-container`, the rshape fast, composition, four chaos and five soak stories in the container provider) all `ok`.

**The fifth run (2026-10-20, lead): `2a641bf6`** (Q87 and Q88 landed): release `gates-e4s8-2a641bf6release.log` 110 ok 0 FAIL (512 s), the export run 65 of 65 in 1,376 s, then `--soaks`: 12 jobs in 5,858 s, **11 ok, 1 red**: `i143-rshape-soak-authority-container` at round 97 of 97 (1,266 s in, 0 violations before it). Read from its spans: the hand-off of mesh2's seat deleted mesh2.admin.1 (Build `ncne4k6j9ftj`, complete at 22:00:39.6 UTC), the node.create that follows it (`xe9j47ft88rn`, accepted at 22:00:39.8) was reconciled by the fabric-primary at 22:00:39.996 against its view, which still held the deleted admin (mesh2 at 10 members until 22:00:40.214, 9 after), so the Build planned no operation (`via-reconcile operations="" outcome=converged`) and completed; in the 150 s the soak waited for the rebirth no drift pass re-created the path, and the duration invariant ended the run at 1,435 s of 1,800. No command, dial or re-publish of Q87 or Q88 is in the window: the shape is the Build planner reading the observed view inside the window between a deletion's completion and the view dropping the birth. `export-manifest` was not run: the tail is not green. **The i141 pin stays at `abc1ec15`.** The same day an Opus red-team review of the landing (lane e6s7, `target/review-q87-q88.md`) named five defects in the landed code, four confirmed from code and spans by the lead (no Pending published for a commanded birth; a commander that was itself held commands every healthy member on resume; the command-open guard latches, `CommandBook::close` has no caller; a commanded start whose hook was Blocked and that fails a later step stays Pending on its channels) and one in the lead's own code (`begin_cycle` before the command is sent, never undone): a follow-up landing fixes them RED-first, and the tail runs again at that SHA. **The follow-up (2026-10-20, lead, lane e4s8):** four of the five fixed RED-first and landed: the command key closes when its carry ends, admitted or not (`CommandBook::close` at the two carry sites; the cell `a_birth_restarted_by_a_build_and_held_later_is_still_commanded_back` went red `command-open` and green); the commanded join's cycle begins and Pending is persisted when the join door admits the birth, on the join report the runner registered (`via-mesh-join-admitted`; the heard-again cell asserts the order and the publish); a commanded start that fails after a Blocked hook leaves its channels from the spawned path, rpc node and node-admin alike. The second finding (a commander itself held commands every healthy member on resume) waits on Q89. The soak's red is covered by the cell `node_create_after_delete` (a hand-off's deletion then a node-admin create at once), which passes without a planner change because the fabric-primary's view dropped the deleted admin before the delete completed in that estate; the window the soak hit (the view 0.6 s behind the completion) is not reproduced by it, so the planner reading `observed.node(p).is_live()` as satisfying a planned path stays open, the soak's spans its RED. Broad set 151 of 151. **The sixth tail (2026-10-20, lead): `12b5a0ec`** (the follow-up and the S6 floors story landed): fast gate 200 s and release gate 634 s green; the export run 64 of 65 in 1,494 s, one red read: `i143-2900-process` (`mesh_member_recovers_delta_gap_from_own_primary`) counted one delta per release of a held rpc node and one join per member, and under Q87 a released node re-enters (Pending, then Ready: two deltas) and a member heard again after a heal joins once more; the cell is brought to the page, not the code. The soaks were refused by `i143-export-run.sh` itself: its gate-log glob matched lane e1s8's fast log at the same SHA (the S6 landing's) and found no release log under that lane's name; the script now reads its own lane's logs. `export-manifest` not run; the pin stays at `abc1ec15`; the tail runs again at the next landed SHA. **The seventh tail (2026-10-21, lead): `8fbec6ed`**: the land's fast gate green; the release gate 110 ok, 1 FAIL (`rdm-test:main`: `admin_start_declares::a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again`, the start never finished: reply deadline); the export run 65 of 65 in 1,114 s, none refused; the soaks refused on the red release stem. The red's shape is the one that red the S6 landing's gate (`c90f5db3`) and the heard-again cell in lane e1s8's regression: a node-admin's mesh channel reopens after its stop or its re-entry and gets no neighbour while its mesh-primary's own mesh subscription ends in the same milliseconds, both having dialled the same pair (the §6 repair dial from the primary, the reopen dial from the returning admin); the start then waits for a neighbour without bound. Intermittent, on main before Q89. RDM's spans end at the dials; the cell runs again with `RDM_DEBUG_GOSSIP=1` for iroh-gossip's dial, accept and close lines before any dial-list change is proposed (Q77, Q84: RDM's dial list is the fix). The pin stays at `abc1ec15`. **The hang read (2026-10-21, lead):** eight fast gates at `10674b84` with `RDM_DEBUG_GOSSIP=1` did not reproduce it (two other load races did, both read: a view read before the started node's first digest, and the released birth's Pending before its ready, each a cell brought to the page in lane e4s8, not landed). The fourth sighting came without the debug layer, on the landing gate of those cells (`0c87d0e3`, `node_start_sequence::every_start_path_runs_the_same_start_steps_in_the_same_order`, the admin's start answers: Elapsed), and it has no second dial in it: mesh1.admin.2 parked at 54.343, its mesh-primary logged the backbone neighbour-down for it and no mesh-topic one; it started again 26 ms later, its backbone reopened and had its neighbour in 10 ms, its mesh channel reopened dialling the primary and the rpc node and every dial for a minute reported joined, and no neighbour-up came on either side. Read as: the primary's gossip still held the admin as a mesh-topic neighbour from before the park, so the rejoin formed no new neighbour, and `wait_for_neighbour` waits for a neighbour-up and nothing else while frames arrive on the channel (the heard-again flame of lane e1s8's regression shows mesh-status frames received at neighbours 0). Q90 asked (its row is in lane e1s8's rulings INDEX, after Q89): the lead's first recommendation, the first frame received as proof, was reviewed and withdrawn the same day (plumtree adds an eager peer only on IHave, Graft or neighbour-up, `src/proto/plumtree.rs:574,630,650,665` at `508fb79`: a node passed on a frame heard is heard by no one). The RED cell is written (lane e4s8 branch `e4s8-q90-restart-within-interval` at `02838920`, `node_start_sequence::a_node_stopped_and_started_within_a_gossip_interval_is_a_neighbour_on_both_sides_within_a_floor`: the admin and an rpc node stopped and started back to back; a neighbour-up on the restarted node's `mesh:mesh1` channel and on the mesh-primary's for it within a floor of the join, the start answered within two floors, Ready as the same birth). Plain runs: RED 2 of 6 (both reds: the mesh-primary emitted no neighbour-down for the restarted admin on either topic after its channel-stopped and no neighbour-up within 4 s; the start answered Started, nothing Elapsed); every green shows the down on both topics within 1.4 ms and the up within 35 ms. With `RDM_DEBUG_GOSSIP=1` on, 9 of 9 runs were green, so the actor read the architect asked for has no red run to read yet (no cause attached to the layer's effect). Its two arms stand: RDM's ordering of the park's leave, or iroh-gossip's actor (Luke's permission).

**Q89 (2026-10-20, Luke): the loss of the fabric-primary is one playbook whatever stopped it; its seats move at the staleness floor to the lowest Ready node-admin of its own mesh, else at mesh investigation's decision step to the lowest Ready mesh-primary on the backbone; the successor brings the lost fabric-primary back and hands every seat home once the node at its path.name has been Ready for one floor.** Asked from the review's second finding. Read against the record before it was asked: Q24 ruled this loss on 2026-10-16, but its packet defined lost as the whole mesh unreached and uninspectable past the hung hold, proposed only from another mesh, and the code conforms to the packet in six places (seat_watch.rs `takeover_due` and its finding match; fabric_handover.rs `take_over`'s successor with the lost mesh excluded and `release_lost_admin` at takeover time; election.rs `named_fabric_successor` and `fabric_seat`; accepted.rs `handback_due`), so a held fabric-primary with a live secondary kept its seats for ten floors and the hand-back never reached a same-mesh or returned birth. Read against the flame (lane e4s8 reg32, both cells): the resumed fabric-primary's own view went cut off and, inside that cut-off, it commanded its healthy members by Q87's heard-again-by-digest path 3 ms after their first digest (seq 9 before the hold, 55 after); gossip.md §6's cut-off view is the self-detection and the Q87 command path never asked it. The pages carry the rule (lane e1s8, Luke's yes on the exact text): fabric-recovery.md's new section with the timing table, elections §1, §4.3, §4.4, §5 bullets 2 and 5, §6, mesh-investigation.md, node-recovery.md step 2, gossip.md §6, node-admin-lifecycle.md rule 6, the index, node-rpc-envelope.md. Remaining, in the same landing (the dev's delta in [i143-q89-fabric-primary-loss.md](proposal/i143-q89-fabric-primary-loss.md) §5): the six gates, the hand-back on both paths to the returned or re-created node, the view gate on the command paths, and the cells RED first; the export tail runs again at the SHA that lands it. The lane's earlier page commit (3a222bae, every seat superseded at the floor, a Q89 row recorded before the text had a yes) was not taken: the branch `e1s8-q89-fabric-primary-loss` starts from origin/main.

**Q93 to Q97 (2026-10-21, delegated by Luke, ruled by the architect):** Q93, the architect's drafted lines are the yes: fabric-recovery.md step 2 and node-start.md step 5 land alone, node-mesh-join.md steps 5, 7 and 9 and the gossip.md §6 bullet with lane e7s3's code, the Q90 sentence with the Q90 code; Q94, elections §1 line 25 and node-rpc-envelope.md line 1546 stand as landed in `ae547cab`; Q95, a stop publishes no aggregate except R-NS1 item 2's, on node.delete of a deleted mesh's last admin (code change); Q96, fabric-recovery.md step 4 names the hand-back's Build attempt, `handback:<path.name>@<T>` (page line); Q97, the mesh-primary records and publishes a joining node's Pending once, on accepting its join report, before it answers; every later report of that join belongs to it (page step 10 with its code). Amended the same day on Architect 1's review: line 1546 gains "(the fabric-primary's own mesh first)"; Q96 takes A1's sentence; Q97 promises no delivery order, since a member's status has no order key of its own. **Q98 and Q99 (2026-10-21, delegated, ruled by the architect):** 0x1F gains `FetchCurrentBuild` (reply `NoCurrentBuild`), the `no-pointer` blocker reads it and the repeat JoinNode goes (with conformance item 12); node-mesh-join.md's Completion row and states.md's node pending row take Q97's words, one Pending per join (with Q97). **Open for the export tail (2026-10-21):** Q90 held until a controlled regression drives the rejoin and its trace is read (the rulings row); Q99's door tells a retry of a join (it keeps that join's Pending) from a new commanded join (another Pending, even while the birth is still Pending), keyed on the command's natural key (R-DK1), both shapes in the failing cell; the lost-executor red of item 8's gate (a restart resumed by a successor waiting for a Ready that never came) is unresolved and unread: six passing reruns explain nothing, and its next failure keeps its artifacts; two findings of the Q97 agent stay open, not cleared by the absence of a cell: a start after a stop does not clear an older row another mesh's former primary wrote, and a moved fabric seat can leave one cleanup retrying every pass. The rows: [rulings INDEX](rulings/INDEX.md).

**The tail after Q97 (2026-10-21, lead).** Landed on main: Q97 (`dd5356c2`..`7ae484d2`) and Q99 one Pending per join (`4bf6fd6b`); item 1 (`94b70808`, `ee05e4a6`); item 12 with Q98, 0x1F's `FetchCurrentBuild`/`NoCurrentBuild` (`3d7055f4`, `16d0ba3a`; Q100's lines `49c56eb6`); Luke's reset of Q90 (`d19c7c25`, rulings `d45f90e1` and `c6a8211f`): no neighbour wait on either start path, the node-admin's `NodeJoined` on entering its channel, node-mesh-join step 9 "`node.mesh.joined`. The node's first `Digest` heartbeat follows.", the Q90 cells dropped and Q101 to Q103 cancelled; the four forks merged with their upstreams (`388339b7`; the kept and dropped patches in [the fork list](proposal/iroh-noq-netwatch-gossip-fixes.md); the consumer fixture's lock `4590a517`); sticky_seat's observer-moment check removed as a test defect (`ce251b3f`). At `ce251b3f`: the fast gate green (`gates-e1s8-ce251b3f.log`, 191 s), the release red with 2 (`gates-e1s8-ce251b3frelease.log`, 580 s): `rdm-test:main` (node_heard_again asserted an arrival order, a test defect under `c6a8211f`) and `i143-2784-chaos-container` (the container cut cell cut mesh2 off before fabric ready, the takeover was not gated, the hand-back was refused `fabric-not-ready` and never planned again). The 25 chaos jobs of the export run, run by hand at `ce251b3f`: 21 green, 4 red: `2784-container` and the three rshape-adv cut jobs (Q105). **Q104** (rulings `f94c6f8c`, `1dd6d183`) fixes both release reds and **landed at `fd50d11f`** (lane e1s8, fast gate green, 188 s) with **the election survivor rule**: Q104's first landing gate (`836cda13`) was red on build_op's successor cell, read from its spans as `election.rs` `fabric_seat`'s survivor check (R-A2, `7ce94a9d`) leaving the living holder out, so another mesh's view that held the takeover's holder `pending-reconnect` and the lost birth gone seated its own primary (mesh2.admin.1 at 11.937 s), against elections §4.3 ("if only Pending admins survive the seat waits") and §5 (a holder keeps its seat through silence); the same sequence is in a run of main's code, which passed on timing; fixed RED first (`4554ab41`, `fd50d11f`). **Open:** Q105, the cut takeover's seat scope (the three cut jobs); a code-vs-page difference held for Luke: the takeover's row moves the mesh seat with the fabric seat but announces only the fabric seat (`fabric_handover.rs`, `announce_seat(Seat::FabricPrimary, …)`), where §4.3 says the row is published (a gossip change); the Q99 branch's stall (2 in about 70 runs on the branch, 0 in 30 on main), unread. **rafka-v2 (Luke, 2026-10-21):** i141 starts on its own box against `1dd6d183` with the four forks; its pin moves once more, to the export SHA. The tail reruns at the SHA that carries Q104: the release, the 65-job export run, the 12 soaks, `export-manifest`.

**Q109, storage decides nothing (2026-10-22, lead).** L1 landed `98f2fef7`: A, the seat moves (the writer folds every earlier move and writes its row at `seat-moves/<T>--<node.id>--<incarnation>`; every reader folds the rows and ignores one that does not follow), and C, the Build log's seat refusal, out of every store; the refusal span `rdm.node_admin.build.reject.via-seat-yielded`; pages items 1 to 5 with item 2 in Luke's words. Its first gate (`a3dc9429`) was red in node_stop, rounds and the store-layout ratchet: one writer's two rows took one `t` (a fold during the held rows' publication saw none of them, and a writer's commits ran at once), so the second replaced the first and the heir's fold read mesh2's seat at revision 0; fixed RED first (`fabric_storage::tests::one_writers_commits_made_at_once_each_stand_at_their_own_t`: sixteen commits all at `t` 1 without the fix). D1: the mesh, node-row and fabric name claims are their writer's (`claim_mesh`, `claim_node_row`, `hold_node_row`, `release_node_row`, `claim_identity`, `release_name`, each a read and a plain write), and no claim is decided again at a publication; pages items 7 to 11 and `build.md` line 415. D1 landed `db14c898`; its first gate (`d6cff91e`) was red in `i143-2805-process`: the bootstrap admin aborted on a stack overflow at the first step of a recovery's create, the create pipeline's state machine at the edge of a tokio worker's 2 MiB stack, fixed by boxing it in `create_with_claim`. No sealing of anything: the op ledger and the hook registry are built at boot (`bffe2ebb`, Luke's words "Each registry is built at boot"), and the Build lane's seal goes with its spans, its cells and its page lines (node-primary-update.md, fabric-state-sync.md, build.md, node-rpc-envelope.md BuildClaim's takeover). Open: D2, the in-flight row and the Build lane's appends as plain writes (items 12 and 13): today an executor appends its receipts to a lane under the seat holder's revision and a mesh's in-flight row is taken by the fabric-primary and by that mesh's primary, so a plain write there loses a fact; asked of Luke before any change. B, the seat cut-off, waits for his yes on node-primary-update.md step 4. S2 landed `a70bb78b`. The cut cells landed `75d2671a`: at that tree the cut jobs kill, ops, restart and spawn are green on the process provider; under the cut the near mesh's primary repairs its killed gateway, and the far side, holding the fabric seat from its decision step (Q89), holds the near gateway while the near mesh is unheard and does nothing from there. At main `09bebf1d` the release suite is green (`gates-e7s3-09bebf1drelease.log`, 794 s, no FAIL) and the export run is 65 of 65 (`i143-export-run`, 1,837 s, refused: none). The soaks stay held until Luke's open items are done (Q113, B, the node investigation page, (c), (d), (f), the heard-again join answer). The made-up words (authority, seat, committed seat, cohort, lane, cut-off, leadership): their origin, the terms they stand for, every page, identifier, wire type and durable format that carries them, and the removal order are `docs/plans/proposal/i143-made-up-words-removal.md` (2026-10-22). The pages landed first, on Luke's instruction of the same day: every architecture page in the terms (the fabric authority page is `fabric-primary.md`, node investigation is its own page, who is primary is the election's result recorded as `is_primary`/`is_fabric_primary` on node rows by the mesh-primary (the write records the role, it does not grant it), the wire is `NotPrimary`/`Primaries`/`FabricPrimaryMoved`), CLAUDE.md's vocabulary retires the words, and the name check refuses them; the code follows in the proposal's §7 order. Q122 (Luke, 2026-10-22): nobody takes the fabric-primary while its node is lost; fabric recovery is one workflow from its own mesh or the backbone, re-creating the node-admin at the fabric-primary's path.name, which is mesh-primary and fabric-primary again when Ready; the take-over, the hand-back and the same-mesh return go (pages landed the same day).

**The made-up words out, landing 1 of 5 (2026-10-22, lead; Luke's final direction for the dev, [the removal](proposal/i143-made-up-words-removal.md) §7):** the write cut-off out of the code, lane e7s3. Gone: `SeatCutOff`, `WriteCutOff`, `CutOffWrites`, every store's cut-off check, the held rows a cut-off store kept and dropped, the lift and the restore (the door's, `seated_again`'s, the two in `admin::boot`'s election loops), `StorageError::CutOff`, `NotSeatHolder`, the fabric store's seat source and the record store's publish gate, and the words "committed seat" in code text ("written"). In their place node-primary-update.md step 3: the holder runs `FinishWrites` before its row (the file backend takes each record store's write lock once, `finish_issued_writes`; a memory write is one step under its lock), under `rdm.node_admin.primary.update.via-issued-writes-finished`. Twelve cells that proved only the cut-off are removed by name; three keep their operation under new names. Q109 (storage decides nothing) supersedes i146 Q39's late delivery "refused by its store" ([i146 record](i146-owed-messages-prd.md), the e6s4 cell): that cell, `status_rpc::tests::a_declaration_late_to_the_old_mesh_primary_is_refused_by_its_store_and_the_new_holder_applies_it_once`, goes with the cut-off. Its first gate (`b09f6b3e`) was red at discovery: the three renamed cells' old names had no `removed:` line; the landing names them. Its second (`594785a6`) was green in 73 units and red in one, `receive_order::tests::every_frame_kind_has_a_row_in_the_receive_rules`: main's own red since `f61f470e`, where gossip.md's receive rules dropped the `Seated`, `PrimaryUpdating` and `PrimaryUpdated` rows and the code keeps the three frames, which feed every election (`membership.rs:4361`) until landing 4 records the role on the rows. Landings 1 to 4 are stacked in order in lane e7s3 and land together once landing 4 removes the frames. Landings 2 to 5 follow: lanes, `authorizes()` and the cut-off view, seat and authority, the exit-proof machinery.

**The run (2026-10-19, lead).** `scripts/i143-export-run.sh --soaks` (`91b6216b`) runs the twelve soak jobs the export run holds, the consumer build first, in three batches (about 30 min each), and is refused by name unless HEAD's fast and release gate logs are green (`[gate] head=<HEAD> dirty=0`, DONE, no FAIL): refused at `3d6e4695` (no release), planned at `1921b77b` (fast `gates-e4s8-1921b77b.log`, release `gates-e4s3-1921b77brelease.log`, both green). It runs at the i145 cert commit, after that commit's release and the 77-job export run.

**The first run at one green SHA (2026-10-20, lead): `768dfaee`**, release `gates-e4s8-768dfaeerelease.log` 110 ok 0 FAIL, the export run 65 of 65 in 1,494 s, then `--soaks`: 12 jobs in 5,759 s, **4 ok** (`i143-2787-soak-process`, `soak-routing-process`, `soak-routing-container`, `soak-partition-process`), **8 red**, read from their own evidence (`target/i143-rshape/<job>/<cell>/failure.json`, `actions.jsonl`, the node logs), four causes, one of them RDM's:
- **Product (Q85, RDM):** `soak-authority-process` round 17: a mesh-primary restarted through `node.restart` hands its seat to its mesh's other admin; that successor is killed; the restarted admin wins the seat back by the ordinary election row and every mesh-row write of its stays refused by its own earlier transfer's cut-off (338 refused exit-proof puts in 150 s, no re-birth). Fixed RED first (`a_restarted_mesh_primary_seated_again_after_its_successor_dies_writes_its_mesh_rows`; flame `b8a04f2f8cbaa63aedfd369d07c82be0`): the election row that seats an admin again restores that seat's writes (`WriteCutOff::Restore` after `elect` in `admin::boot`).
- **Driver, the container "kill" (`soak-authority-container` round 2, `i143-2787-soak-container` round 53):** the driver's container kill was `docker network disconnect`; the unplugged admin ran until the estate stopped (its own log: `via-mesh-silent`, then the teardown signal at 05:54:27), its mesh-primary read it `Running` and held it under R-FM1 (`via-held-running` verdicts 1..15; node-recovery.md steps 3-4), and the 150 s `birth-sequence-converges` bound passed with no rebirth: the product did what the page says for a silent running runtime. The driver's kill is now `docker kill` of the exact container (`ExactContainer::apply(Fault::Kill)`, SIGKILL to its init, acknowledged by the daemon), as the process provider's SIGKILL; the #2787 contract says so. Proved by a 150 s container run of the authority story at the 768dfaee consumer executables: the kill acknowledged `exited` (137), no `held-running` on any admin, and its one red the Q85 cut-off (560 refused writes on the re-seated mesh2.admin.2). The partition story's 150 s container run: 48 rounds, cuts counted, green.
- **Driver, a restart of a cohort's sole node (`soak-lifecycle-process`/`-container` round 72, `soak-randomized-process`/`-container` round 29):** after a shrink left one compute in mesh2, the draw restarted it and node-admin refused by name, `no-secondary: cannot transfer: no secondary of type compute in mesh mesh2` (node-primary-update.md "No-successor cases"): the page's refusal, judged a `topology-action-accepted` violation. The driver's live refusals now redraw a restart or replace of a non-admin seat holder with no other ready node of its kind in its mesh, as they redraw the fabric-primary.
- **Driver, the container silence row (`soak-partition-container`):** 1,800 s, 267 cut/heal rounds, 0 violations, then "S3: the mesh was cut, held and healed at least once" failed with `cuts: 0`: the container `unheard` row carried no `cut_at_ns`, which the cell counts cuts by; the process row's `udp-drop` did. The container row now carries `fault: silence` and `cut_at_ns`, so the run's cuts are counted and the "no lifecycle mark during a cut" check covers them.
The tail runs again at the SHA that lands these: release, the 65-job export run, the 12 soaks, `export-manifest`.

**The second run at one green SHA (2026-10-20, lead): `4aa58cf0`** (Q85 and the three driver fixes landed), release `gates-e4s8-4aa58cf0release.log` 110 ok 0 FAIL, the export run 65 of 65 in 1,122 s, then `--soaks`: 12 jobs in 5,823 s, **9 ok**, **3 red** (`export-manifest` eligible=false, 3 `receipt-not-ok`), read from their own evidence, three causes, two the driver's and one RDM's, unruled:
- **Driver, the cut wait's evidence read (`soak-partition-process` round 41, `cut-leaves-own-mesh-ready`: "mesh1.admin.1 does not answer node.get during the cut"):** the fabric-primary answered every request that reached it (its `fabric.serve.via-get` and `node.serve.via-get` spans run through the window, none over 0.5 s, through its 3 s investigation dials of the cut mesh's admin); none reached it for 3.5-5 s at a time, 84 such gaps in the run. The driver's re-feed wait parsed the whole evidence tree every 500 ms (`Estate::spans`: 1.5 GB, 1.58 M spans by round 41, 4-5 s a parse), each parse a stall of the driver's own reads, through which the cut waits grew from 10 s (round 1) to 72 s (round 15) and round 41's `node.get` was not answered within its 2 s. The wait now tails the node-admins' evidence files from the cut on (`soak.rs` `SpanTail`: the new bytes of each `*node-admin*.spans.jsonl` per poll, never the tree).
- **Driver, an action on a peer mesh the fabric-primary holds unheard (`soak-authority-container` round 139, `topology-action-accepted`: `node.restart mesh2.admin.2` refused `unheard-mesh`, "not heard for 2570 ms"):** the draw before it held mesh2's mesh-primary (`docker pause` at 09:06:11, released once the view marked it `pending-reconnect`, 3.85 s); every view converged, the restart followed 6 s after the hold, and the fabric-primary's investigation ladder had not heard mesh2 since: the released primary's frames resumed 3.3 s after the release (`via-unheard` 1005 ms at 09:06:15.7, the refusal at 09:06:17.3, `via-unheard` 3007 ms marked at 09:06:17.7). The page's refusal (mesh-investigation.md: a mesh is held from the track rung until heard again; control_plane.rs's Build door). The driver's convergence after every action now also waits until the fabric-primary holds no peer mesh unheard (`held_unheard`: its `via-unheard` and `via-probe-verdict` rows read as they are written; a marked or decided mesh waits for its `heard-again` verdict, a tracked mesh with no later row past the mark rung was heard again before its first probe), recorded as `held_unheard_ms` on the row.
- **Product, unruled (Q86, RDM's gossip layer; `soak-randomized-container` round 80, `birth-sequence-converges`):** after `node.restart mesh1.broker.1` (Build `sppcqz2pqdz1` attempt 4) every admin's view held mesh1.broker.1 ready and the same births, except `mesh1.admin.1` (not the fabric-primary: `mesh1.admin.3` was), which held all eight nodes of mesh2 `pending-reconnect` for the run's last 3 minutes. At 08:50:49 its backbone pair with `mesh2.admin.2` (mesh2's primary, endpoint `d66cc5…`) went silent in both directions (its last frame from `d66cc5…` 08:50:49.3; mesh2.admin.2's last frame from it 08:50:48.9; its own frames last reached `mesh1.admin.3` in that minute), while both sides kept the neighbour up (`via-neighbour-down` only at the teardown, 08:55:40) and it went on hearing `mesh1.admin.3`'s frames. gossip.md §6's repair ran as the page says, 53 re-feeds of each mesh2 admin at 3 s (`rdm.mesh.connection.update.via-refeed` `stale-held-member`, `joined=true`, `coverage_age_ms` 3,496 → 160,381), and repaired nothing: `join_peers` of a peer iroh-gossip already holds as a neighbour hands it nothing new. The fabric was unhurt (the fabric-primary heard everything, the soak's own invariants held, the Build completed); what fails is "every live admin holds the same births", which the design needs (a node reads its projections from its node-admin). No flame: the soaks export no spans to .99; the evidence is the node logs and `actions.jsonl` under `target/i143-rshape/i143-rshape-soak-randomized-container/`. **Q86, ruled no (Luke, 2026-10-20):** gossip repair is iroh-gossip's, never RDM's (RDM joins a topic or leaves it, nothing else); a member that stops gossiping is evicted after the staleness configuration, and a node that is not gossiping is triaged by node-recovery.md, node-runtime-inspect.md and mesh-investigation.md. Read against them: no node stopped gossiping (mesh1.admin.1's digests reached mesh1.admin.3 on the mesh channel throughout, seq 399 → 957 over the five minutes; mesh2.admin.2's reached mesh1.admin.3 on the backbone throughout), every authority heard what it is promised to hear, and mesh1.admin.1's pruner marked what it did not hear and re-fed it, as gossip.md §6 says; no page promises more of a node-admin's copy of a peer mesh it has not heard. The soak's rule over-promised: `converged_everywhere` now lets a node-admin that is not the fabric-primary hold a peer mesh unheard whole (every node of it `pending-reconnect`) and agree on the rest, recorded on the row as `peer_mesh_unheard_at`; the fabric-primary and every admin's own mesh are held to the full agreement as before.

**The third run (2026-10-20, lead): `5a220a6a`** (the two driver fixes landed): the release gate did not run (the gate shim takes the lane's path, the chain passed its name: rc 127, read from `tail3-release.log`), the export run 65 jobs in 1,099 s with **2 refused**, both cells read from their evidence as cell defects, no product change; `--soaks` refused for the missing release log; `export-manifest` not reached.
- `i143-rt6-process` `born_node_joins_its_maker_then_reads_the_makers_topology_and_installs_it`: "mesh1.rpc.1 installs exactly the meshes the maker's single read streamed: 2 ≠ 1". The second `via-read-install` (mesh2 v5, 1 s after the birth, its own trace `b924f613`, under its own `serve.via-read` at mesh1.admin.1) is the delta-gap top-up (`topology_read::top_up_fetch`), a read of its own that the page allows; the birth's read installed one mesh, as the maker's serve said. The cell now counts the installs of the birth's trace (the trace of the node's `via-membership-pulled`).
- `i143-2900-process` `mesh_member_recovers_delta_gap_from_own_primary`: "mesh1's primary replayed its forwarded fulls to the member when it came back as a neighbour: []". The replay row is there (`via-neighbour-replay` at mesh1.admin.1, `mesh:mesh1`, the member's peer, `full_chunks=2`, 10:09:27.516) 2 ms before the member's install the cell had just waited for; the cell read the primary's evidence once, right then, and the primary's export thread had not appended the row yet (`EvidenceFile`: appended on the export thread). The cell waits for the row, as it waits for every row of another process.
- The landing of those two fixes (`ce9224ae`, lane e4s8) met a red fast gate, `rounds::a_two_mesh_fabric_of_role_nodes_is_ready_only_through_the_commit_state_and_open_traffic_rounds`: "open-traffic: mesh2.admin.1 carried mesh2.broker.1's traffic-opened onto the backbone", read from the kept evidence (`kept/gate-gates-e4s8-ce9224ae-rounds`): mesh2.broker.1's hook reached its primary on the mesh channel at 10:24:01.438 (`carried=carried`), 71 ms after the fabric-primary completed the round on the member's declaration (10:24:01.367), and the climb stopped the estate at 10:24:01.493, before mesh1's admins heard the backbone hop (mesh1.broker.1's, 90 ms earlier, had landed everywhere). A stop race in the climb, no product change: `two_mesh_climb` now stops the estate once every member's `traffic-opened` has made its last hop (heard on the peer mesh's channel, forwarded by the peer's primary), bounded at 15 s; a hook that has not landed by then fails the assertions by name as before.
Proof runs of 150 s (seed 1431101, the registered env): the partition story's 17 cuts held 6.0-6.6 s each, flat (the run at `4aa58cf0`: 9.6 s at round 1, 72.6 s at round 15), 0 violations; the authority story's hold of `mesh2.admin.1` and restart of `mesh2.admin.2` four seconds later (rounds 80, 84 of the proof: the shape that failed) waited 2,011 ms for mesh2's `heard-again` verdict and the restart was accepted, 0 violations. The tail runs again at the SHA that lands the two driver fixes; a recurrence of Q86's silence is read from its own run.

**The soak driver's proving runs (DevX box, seed 1432787; not the e11.s12 soak, which still waits for one green SHA).** Each red is a product defect, fixed RED first:
- `z5dwdq96cky0` came back as an arrival at `mesh1.rpc.1` once the book forgot its departure (a non-executor admin's records outlived the delete): fixed `e6999467` (`Records::remove_deleted`, `rdm.node_admin.topology.remove.via-node-deleted`).
- Full process run on `fd700d55` (wall 1048 s, red at 933 s, round 113, ledger clean: 3201 ops, 0 violations): `node.delete:mesh1.admin.2`'s TerminateRuntime refused "the provider cannot inspect its runtime after the stop ladder", because the leader's `/proc/<pid>/stat` start-token read answered `ESRCH` as the node exited (`returns-unknown.via-proc-unreadable`). Fixed `3c62e6ee` (RED `01ce7c78`): the start-token read maps `task_exited` to Gone, as the per-task reads have since `b0543a96` (deployment-provider.md §2.6). Evidence `/home/adsmin/rust-distributed-mesh-deploy/soak-fix/full3-1432787-cell` on the DevX box. The three container estate cells (`i143-2777-container`, `i143-2784-chaos-container`, and `container_kill` in `container-proof`) are tied to `e12.s88` (`R14 item 6`: container provider store-root qualification and mount).
- `soak-floors-container` at `b6695529` (seed 1431101): mesh2.admin.1, holding the seats after its backbone takeover from mesh1.admin.1, handed them back at 158.720 s while its own command back to mesh1.admin.1 (node.mesh.join, 154.726 s, first dial timed out at 3,013 ms) was still out; served at 158.793 s, the join went through mesh2.admin.1, whose writes the hand-back had cut off, and the start failed at 162.135 s (the cert row refused five times); mesh1.admin.2 took the seats at 162.061 s and the seats were home again at 167.4 s. Read by the lead from the spans: the command back was sent at 154.726 s and still undelivered (first dial timed out) when the hand-back ran; mesh1.admin.1 served it between "primary updating" and "primary updated". `6bc58298` (the hand-back held while that call was unanswered, from the holder's in-memory state) is gone with the hand-back itself (Q122, landing 5 `ae325052c`; its last binding out in the words landing); the root is in the view: a birth held PendingReconnect and heard again is not Ready until its node.mesh.join has run its before_ready hook and published it ready (fabric-node-hooks.md "Where it runs"; node-recovery.md step 1). The cell's red was the wedge judge at step 33 (`62f5e1a3`), not this chain.

### Remaining

- [x] The release suite (fast + release stems) green on one RDM main SHA: `abc1ec15` (2026-10-20; e12.s88 landed `8bd6681d`, the container cells in the gate).
- [x] The §12.1 seeded soak profiles and the §12.2 container proof on that SHA, each with its receipts: 12 of 12 soaks and `container-proof` at `abc1ec15`, the receipts hashed in `docs/plans/i143-export-manifest.json` (2026-10-20).

### Acceptance criteria

<!-- crit:e11.s12.1 -->
- [ ] Every §12.1 soak invariant holds on the SHA; the container proof runs the real provider; every soak red is explained from its own evidence with an owning block
<!-- /crit:e11.s12.1 -->
<!-- crit:e11.s12.2 -->
- [ ] The story bar holds (section story-bar).
<!-- /crit:e11.s12.2 -->

### Done when

- [ ] The soak and container receipts are posted against the SHA that e10.s2 exports.

**Epic metadata**

- sections: [the-plan-of-record, story-bar, 12-chaos-and-observability, 12-1-seeded-soak, 12-2-container-proof]
<!-- /epic:e11.s12 -->

**The release suite after landing 5 (2026-10-23, lead).** Landing 5 (`ae325052c`, Q111 and Q122) passed the fast gate and the land gate, but the release run on it (`gates-e4s8-ae325052crelease.log`) failed ten jobs: release-only cells still asserting the removed take-over and hold, and three regressions. `bf25f1805` fixes the code to the pages and rewrites the cells: an unheard mesh-primary is investigated and recovered by the fabric-primary once the election has named the next one (node-recovery.md step 2); an unreached birth whose runtime lives in another control domain is deleted with nothing terminated (node-recovery.md step 3); the admin a person launches on the fabric-primary's data dir restores itself as mesh-primary and fabric-primary when no admin runs the workflow, and asks every node the store names for rafka-time at once ("Nothing alive"); a silent admin of a deleting mesh that answers a ping survives (mesh-delete.md step 2). Left, after every other task (the tail): the 2784 container chaos cells and the rshape partition chaos cell still assume the hold. Not stated by any page, reported, not built: (1) after node recovery reads a container `exited`, the exited container is left on the daemon (the old delete removed it as part of the terminate; no page says it is removed); (2) an admin of the fabric-primary's mesh cut off from it past a floor runs fabric recovery as the pages say, and node investigation reads a live fabric-primary it cannot reach `running`, so the recovery terminates it.

**The release suite after landing 5, second pass (2026-10-23, lead).** The release run on `bf25f1805` left seven jobs; `1f8c28492` fixed four: a create launching the step's or the row's binding now emits the binding launch event (an e12.s91 gap the rshape cells caught); the fabric-primary's submit of an unheard mesh-primary's recovery is owed until opened, with a doubling delay (it was refused BuildInProgress once and never made again); node_replace's lost-claimant cell and the rshape authority cell now say the fabric-primary opens that recovery (node-recovery.md step 2; the rshape cell cited a mesh-recovery.md that does not exist); the 2780 failpoint cell measures Ready against the node's own Pending apply. The release run on `1f8c28492`: 110 ok; left, for the tail, the cells built on the removed hold (task list: the three 2784 container chaos cells, rshape `mock_connections_delete_then_node_delete_preserve_status_and_terminal_proof`, the rshape partition cell). Q127 is open for Luke: `backbone::gossip_repairs_a_lost_push_and_an_isolated_admin_executes_nothing` is red whenever its isolated standby runs fabric recovery before the heal and terminates the live fabric-primary, as the pages now direct.

**The chaos cells on Q111/Q122 (2026-10-23, lead; task #38).** Branch `chaos-cells-q111` at e6s8 (068781ef4, 1918053cb): the container chaos cells, the rshape partition cell and the rshape lifecycle hold read node investigation's verdict. A held or cut node is re-created at its path, and the partition cells carry Q4's four assertions without pinning which side survives; a re-feed within one mesh is of or by a path the cut's recovery re-created (gossip.md section 6: its mesh-mates hold the terminated birth while the executor's NodeDeleting cannot cross the cut). Green at e6s8: i143-2784-chaos-container 6/6, i143-rshape-partition-chaos-process 2/2, i143-rshape-lifecycle-chaos-process 3/3. The release run on 068781ef4 left `backbone::gossip_repairs_a_lost_push_and_an_isolated_admin_executes_nothing` (Q127). Q127 evidence, rshape partition cell at 1f8c28492: under the mesh-to-mesh cut the cut side's mesh-primary `mesh2.admin.1` submitted a recovery of the live fabric-primary `mesh1.admin.1`, verdict `running`, refused only `BuildInProgress`; the fabric-primary's mesh investigation of mesh2 re-created `mesh2.admin.1` over a new data dir on verdict `running` (its `deployment.delete` span names it), as mesh-investigation.md step 5 directs. **Landed 1b69062e2** (91ca2926b, b2011c6c8, 1b69062e2), on a direct and a land gate green after the two reds below were read and recorded. The first land gate, at 1918053cb, was red on two cells its changes do not reach (it changes cells only), both green on every earlier gate, recorded red and not gated again:
- `memory_estate::a_node_admin_on_the_memory_backend_runs_the_node_lifecycle_and_writes_nothing_to_the_file_store`: a stop of `mesh1.rpc.1` failed `cannot transfer: no secondary of type rpc-node in mesh mesh1` 140 ms after `mesh1.rpc.2` was applied Ready at the mesh-primary. The mesh-primary's pre-check passed; the rpc primary ran its own election over its own view (`member_handover.rs:233-238`), where node-primary-update.md :30-31 and :47-53 have the writer name the successor and the primary only make the call. Q129 (`proposal/i143-q129-commanded-successor.md`), ruled A by Luke (2026-10-23; B refused as a coping layer): `UpdatePrimary` carries `successor`, the birth the mesh-primary's pre-check named; the rpc primary calls it with no election of its own (`member_handover.rs`), and a command naming none at a non-admin primary is refused by name; node-rpc-envelope.md :1543-1547 and node-primary-update.md step 4 changed with the code; wire fixture `handover_wire.rs` moved; cell `fabric_handover::an_rpc_primary_whose_own_view_has_not_heard_the_successor_hands_over_to_the_one_its_mesh_primary_named` (RED first: `cannot transfer: no secondary of type rpc-node in mesh mesh1`, the memory_estate refusal). The release run on the lane (dc2b20be7) found 026448f1a's image check refusing every container recovery: a container estate's built-in binding recorded no image on the node's row; fixed in the same landing (e928cbb3c: the built-in binding records the runtime image the container provider runs it in), container_kill 6/6, container_proof 13/13, i143-2784-chaos-container 6/6.
- `node_delete::deleting_a_node_is_a_pre_notice_then_a_proven_departure_every_mesh_hears`: `mesh1.rpc.1` did not retain the departure of `mesh2.rpc.2` within 30 s; its held view of mesh2 kept the `node.delete` and `node.stop` overlays. `mesh1.admin.1` held the departure at +03.073 and forwarded `departed=1` into mesh1; the NodeDeleted was sent again every 500 ms for 12 s. Read from the gate's kept spans: `mesh1.rpc.1` took a version gap at 10:18:03.072, dropped the delta to v10 (`departed=1`) as `already-desynced` at .073 (gossip.md §3.3), and its top-up from `mesh1.admin.1` installed v9 at .074, one version behind what that admin forwarded in the same millisecond; no forwarded full followed, because the forwarder sends one only when it takes the role, where gossip.md :155 and :610-612 send one every `FULL_EVERY` rounds as passive anti-entropy. Q130 (`proposal/i143-q130-forwarded-full-every.md`), open for Luke: a gossip change. First sighting; 0 in 5 runs alone (earlier node_delete reds were WaitForBind exits and a broken Build stream).


**Q129, Q131, the backbone cell; the release suite green at one SHA (2026-10-23, lead).** Q129 landed 1319b066b: the mesh-primary command names the successor and a non-admin primary runs no election of its own (node-rpc-envelope.md :1543-1547, node-primary-update.md step 4); with it, the container built-in binding records the runtime image it runs in (026448f1a had refused every container recovery). Q131 landed 6573160e6 (ruled 25f0d508): a node that was not running for longer than the staleness floor restarts every member silence on waking (`via-woken`); before, in node_heard_again at 0acba6d97, the thawed mesh1.admin.2 read the live fabric-primary silent at once and opened node.delete:mesh1.admin.1; after, `via-woken gap_ms=6072`, no recovery opened, the node commanded back. The backbone cell is rewritten on Q4 (aeba81110): the isolated admin refuses build.create (asserted from its own span; the carrier follows the refusal redirect), every view holds one primary per cohort, recovery follows the verdict, and after the heal every pre-cut path holds one live birth, the same in every admin view (the estate is entered again through an admin the cut never touched). The rshape partition and control-loss cells read a terminated birth and a lost seat from spans (1decfbdfd). Release on aeba81110: 112 ok, 0 FAIL. Not built (Luke): landing 2b and Q130. Open: Q128 report-and-kill (ruled; building).
**Landing 2b, Q128, Q133, Q135, Q130 (2026-10-23, lead).** Q128 landed 3a6ca00e7 (a returning deleted birth refused, reported as build.create Terminate, killed through TerminateRuntime). Landing 2b (Q118 A; Q134 A: land with the open reds recorded) landed with Q135 at `a2b23a42b` (88b018c06..a2b23a42b): a mesh-primary that is not the fabric-primary reports a member's recovery up as build.create { Change(Recover) }; the fabric-primary opens it at (build_id, mesh, attempt), decides the mesh-primary's BuildClaim (0x1C) and runs it with build.attempt.run naming the mesh (`AttemptRun.mesh`), writing every receipt the executor reports up the stream (`Step.receipt`, Q133, page line 17c45fa7) and the verdict; a lost run ends only on its executor's exact departure proof, and the reporter's next report reattaches to a live run; durable format moved. Q135 (packet ec052809b; arm A, Luke's yes): a restart's stop is held by NodeRestarting, never published as NodeStopped (gossip.md "Node-stopped lifecycle overlay"); it closed `build_op::a_lost_executor…` (seen since 2026-10-16, first kept flame at e4s8-1b4c68f91: the members refused the restarted birth's digests after the executor that took its node.joined was lost); RED `node_restart` (the restart canary). Cell fixes in the same landing: backbone's healed read starts at an admin the cut never touched (fabric recovery may re-create the entry admin under a new node id); the container node-unheard cell's after read waits for the re-entry (node-mesh-join.md step 10); the hand-over cell names the row it last read and the mesh rows writer spans each row it puts (the hand-over red at a08442c80 unattributed until those show it). Q130 (pages 163289522, b767fb971, c139e3589) arm A landed `4c64567ff`: a node whose pull installs a copy older than a delta it dropped while the pull was in flight pulls again, on the one backoff rule (the node_delete red at e6s8-1918053cb, 1 in 41 runs that day: the pull read v9 21 µs before its server installed v10); node_delete checks every ordinary node of the peer mesh. **Open:** Q136, the mesh-primary's pull from a source mesh's primary (gossip.md :609-613): the backbone carries only complete `Members` snapshots, so no gap is noticed there; the lead's recommendation is the page line naming that. The container chaos red at `a16d80ce3` (`container_network_partition_of_a_peer_mesh_heals_with_one_live_birth_per_path`, also red before 2b at e6s7-5fb3ef5a2 and e6s8-dc2b20be7) is read: fabric recovery's `run_in` re-creates a lost fabric-primary the investigation reached (its owed-again arm fires with no submit made); the fix and its assertion are in lane e4s8 branch `fr-reached`. Then the release suite and the tail at one SHA; rafka-v2's pin stays at 3171f17b until then (141 told).

**Storage as whole records, built (2026-10-23, lead; task #46; [the proposal](proposal/storage-whole-records.md), Q144, Luke's rule of 2026-10-23).** One cached object, one writer, one whole write: the Node record at its node id (its births' facts, at most two, the `predecessor` reference), the node name claim, the `path.name` and tags indexes written from the moves RDM computes (owed moves retried with the node's next write); the Mesh record at its mesh id with its name claim; the Fabric record rewritten whole with each event. Every mutation takes the copy its writer holds (`record_cache`), applies its change and writes the whole object under that object's lock; storage is read only to fill the copy when the writer takes the object's writing. Q144: the fabric-primary decides a mesh's state and writes nothing; the mesh-primary records it on its mesh's record, and the declaration stays owed until that write lands. The mesh-primary writes another admin's signing cert dates as its `IssuerUpdated` returns through it as the carrier (`rdm_transport::CarriedReplies`). The pages of the proposal's "Page lines" land with the code; `fabric-certs.md:107` ("Every cert row is in the ...") is not among them and is unchanged. On the way, the debug rdm-node-admin overflowed its 2 MiB worker stack in a recipe's rename step (`node_replace::a_replaced_nodes_successor_takes_its_config_and_tags_at_its_create_and_the_retained_row_leaves_the_tags_index`, read from its core: `run_op` 444 KiB and `recipe_step` 472 KiB frames, 2.09 MiB in all, against 352 and 408 KiB at `b426caf0c`); `run_op`, `recipe_step` and `create_node` now box their state machines where they are made, as `create_with_claim` already did (208 KiB for `recipe_step`, below main's).

**The final tail after whole records, and the roll (2026-10-24, lead).** Landed after `fb7a22871`: `d3f771f00` (routing-chaos: a stored reply from a target the cell killed is recorded by its provenance; the killed process takes its unwritten spans with it), `2d18742e8` (i143-2805: mesh2's primary answers AlreadyApplied when it holds the fabric event from the Fabric record; await_attempt restarts the rpc node without its cohort's seat, refused no-secondary before), `7ba36d0b6` (the investigation's admin-alive verdict span is emitted under the ladder lock, never after the heard-again that follows it: the partition soak's heal wait). At `7ba36d0b6`: fast 73 ok, release 112 ok; the export run follows. The soaks wait on **Q147** (asked 2026-10-24, recommendation A): the wedge detector (`wedge.rs`, `wedge_detector_never_judges_a_running_silent_runtime_dead`) and the soak S6 oracles assert that a held silent runtime is never replaced, while Q110/Q111 and node-recovery.md re-create every birth whose investigation is not `reached`; four of five process soaks at `fb7a22871` broke `silence-is-not-death` on exactly that. **Roll (open, not started):** Luke ruled arm (a) on 2026-10-22 (rafka-v2 `36781090eb`, i141 B3): RDM owns the roll; a roll Build binds the target executable set, replaces members with new identities through the mesh-primaries, resumes without duplicating completed replacements, and completes after the replacement check-ins and fabric rounds (states.md "Resize and roll"). RDM main has no roll Build kind; Rafka's B3 switch waits for it.

**Rafka cutover dependencies named to 143 (2026-10-24, from 141; rafka-v2 i141-b3-switch.md §5, Luke 2026-10-24: "A missing RDM input or operation is a named dependency reported to 143"). Order: Q142 lands, the soaks and the export, then these.**
- **R-D1 executable replacement** (Luke 2026-10-24, Q157; proposal `docs/plans/proposal/executable-bindings-at-create.md`, which replaces the operation landed `614d01653..5e070d089` and the deleted `roll.md`): the executable is a field of each path's config, named per node by the request that creates the path, recorded on the row (R6), re-hashed at the launch, replaced only by a request naming a new one for that path (node.delete then node.create in RDM's order), read from the row on recovery, returned by `node.get` and `fabric.nodes.get` from the rows. New owning page `docs/architecture/build-deployment.md`; every page change in the proposal's §3 for Luke's yes on exact text, landing with the code. Decided 2026-10-24 (the architect, delegated): `RDM_EXECUTABLE_BINDINGS` and `RDM_EXECUTABLE_CANDIDATE` go, the built-in launch mode with them; `RDM_EXECUTABLE_DIRS` is a containerized admin's launch configuration; `RDM_BIN_DIR` only locates `rdm-node-admin`. The request is the complete topology, an array of nodes each with `path`, `name`, tags, storage, placement and executable (Luke 2026-10-24), the count forms reserved; `PathName`'s `ordinal`/`label` → `name` and `on_retire` → `on_delete` are preserved as is for the name sweep (made-up-words-removal.md), no code now. Landing 1 of 3 landed `71e0e9ef3..ffe1f893f` (2026-10-25, fast and release gates green; 141 told): `Reconcile { based_on, nodes }`, `PathConfig.executable` and `tags` (durable format moved), the per-path replacement in RDM's order, `NodeView.executable`, `fabric.nodes.get` from the rows, executables off gossip (the stored Build holds them), the fabric-primary never moved to a Dead or deleting mesh's admin, a Dead mesh never planned again. Landing 2 landed `ad03e8441..c1979034b` (2026-10-25, fast and release gates green): the harness (`rdm-test` `Executables`, `Estate::bootstrap_on`) and the demo name every path's executable, the counts expanded at the door; `rdm-client` `Nodes::create_named`; the node-admin created for a deleting mesh that lost its admins runs its lost predecessor's row's executable (decided by Q157 and node-recovery.md "Retained"), refused by name when the row records none; an amended final primary admits the leave from its own facts only when they hold every predecessor's receipts (mesh-delete.md step 3), else from the fabric-primary's. Landing 3 landed `01318acda..98c12b546` (2026-10-25, fast and release gates green; 141 told): `Change::Roll`, the per-kind set, the built-in launch mode, `RDM_EXECUTABLE_BINDINGS`/`CANDIDATE` and the client's count forms removed (wire positions 0, 1, 2, 4 and 14 reserved, refused as an unknown variant); `RDM_EXECUTABLE_DIRS`; an Ephemeral path's replacement over a newly allocated data dir; the proposal's §3 page text, with node-recovery.md's Retained row conformed to the proposal's decided refusal and the envelope's reserved-position comment conformed to §3.10(a) (the drafted "two slots after Restart" was the lead's wording and wrong). R-D1 done. Owed to name sweep 2: "roll" in node-identity-and-state.md:636, node-primary-update.md:132, states.md:6, fabric-mesh-node-lifecycles.md:13.
- **Name sweep 2 (round, climb, roll):** pushed to main `2be9435f9..da169e899` on Luke's order 2026-10-25 (direct fast gate green; release gate not run, nightly only). `fabric_rounds.rs` → `fabric_status.rs`, the state types, spans and cells renamed, page prose conformed; the state-hook CloudEvents subject `round/` → `state/` (durable format moved, rebirth); two cell races fixed (fabric_states origination waits on its node's own resolver; accepted_topology resumes the stopped admin at the acceptance). 141 told. Owed, unread: the earlier `rounds.rs:436` reds (a Node record read during state-sync without Ready in its declared states, against states.md:51 "durable before the reply"); the rdm-transport `delivery` red at 08b8f2293, unattributed, 0/20. Kept as is: `recovery_round` (a page term and a durable field) and the soak harness's `round` (evidence format).
- **Q163 withdrawn, the adopter's runtime (decided by deployment-provider.md:266):** landed `b1f4d1ce5..7adeecd20` 2026-10-25 (fast gate green): an externally started admin (Day 0 or an adoption launch) keeps its runtime row on the store root (`runtimes/`), so a later launch resolves a lost one; the roster is captured from the node rows, a create in flight included (its claim's birth), its runtime found from its launch record; `find`'s Exited carries the exact handle its record names, so a successor's delete inspects that runtime and its exit is the proof (node-delete.md "Crashed target"). The adoption-lost-midway cell is back in the gate. Page lines conformed: node-admin-lifecycle.md:56 (`runtimes/` on the store root), deployment-provider.md:66 (Exited carries the handle).
- **A fabric-primary hand-over no longer resets the fabric (landing-1 regression):** landed `80a27de15` 2026-10-25. The heir compared the Build heard on gossip (no executables) with the store root's copy of the same Build and reset a ready fabric to pending as "the accepted topology changed"; with a node.stop in flight the fabric held in state-commit on a stopped planned member (node_primary_update `a_lost_fabric_primary_is_succeeded_by_the_surviving_admin_of_its_own_mesh`, 2 of 34 gates). Now only a different Build that changes the topology resets it (states.md, Resize and executable replacement). Also: the fabric_states cell reads a mesh-primary's own Ready from its record once written (states.md:51 durability is before an Applied reply, which its own declaration has none of).
- **A Day-0 admin's exit after SIGTERM hung on a full stderr pipe (attributed and fixed 2026-10-25, branch `signal-stop-exits`)** (store_root_boot `an_admin_answering_before_the_adopters_first_mutation_refuses_the_adoption_and_nothing_is_deleted`, rdm-admin:main 300 s twice under load; passes alone). Timeline (one loaded hang, pid 3671963): SIGTERM 14:45:46.333; the stop steps (connections delete, quiesce, transport cut, leave tasks: 17 aborted) end 46.334; `rdm.mesh.node.update.via-leave-shutdown` (settled + router shutdown) ends 46.489 `outcome=closed`; `rdm.mesh.node.delete.via-signal` closes 46.489: the signal stop finished in 156 ms and the router waited on nothing. Stack (gdb, the cell's hang and a direct probe alike): main in `TelemetryGuard::drop` → `flush_before_exit` → `flush_log_sinks` → `eprintln!` (`telemetry.rs:130`) waiting on std's stderr lock; the `rdm-log` thread holding it, in `write(2)` on fd 2 (`anon_pipe_write`). Cause: the cell pipes the admin's stderr and reads it only after the exit; the admin logs ~57 KiB/min at INFO, so a run stretched by load fills the 64 KiB pipe, the stderr log's flush ends at its 2 s bound, and the line naming that bound waited on the held writer for good. Fix: every line the exit flush names a bound or a loss with goes through `export::name_on_stderr` (a thread of its own, waited for at most 250 ms; once one is not written, no later line waits): `telemetry.rs` (export thread, evidence bound, log-sink bound) and `export::name_abandoned`; contract cell `telemetry::evidence_tests::an_exit_flush_on_a_full_stderr_pipe_nobody_reads_ends_at_its_bound` (RED before: the child killed at 3.5 s with 64 KiB of stderr unread); the cell's `Day0::stop` now waits at most the connections-delete deadline + the 2 s exit flush + 8 s and fails by name.
- **Final tail at main 119663355 (2026-10-25):** fast gate green, release gate green (12 jobs, 512 s), export run green (64 jobs, 1668 s); soaks 4 of 14 green (lifecycle and partition, process and container); export-manifest eligible=false, 13 refusals. Owed, by group: (1) a Build that never completes (authority-process build-completes, routing-container and 2787-container topology-action-accepted "still reconciling", authority-container and 2787-process birth-sequence-converges): the drive race Rafka reported, being fixed in lane e4s3 (fabric-reset-and-drive-race); (2) a held node not marked unheard within 59 s, every view still ready (routing, floors, randomized process): lane land (held-node-marked-unheard); (3) floors-container: the container fault backend's iptables-restore ruleset refused at line 7 ("Bad argument `IP'"): lane noci; randomized-container: no failure record, being read. Manifest: two section 14 cell names corrected (64db29c38); container_kill refused because the container-proof job ran one cell marked ignored (declared_secrets container restart, a host-process admin cannot bind a container address): the job leaves it out (lane e6s7). The soaks and the manifest run again at one SHA once these land. The soaks ran beside seven agents' builds: box load is read, not assumed, per group.
- **Landed after the tail at 119663355, to main 80a867008 (2026-10-25):** the Build drive race, the leaked in-flight row and the exit hang (7bbd4b3ca, 68120f3c9, 8517bd904); a later attempt of a completed Build is never held for the fabric's change (1c746afed); the Day-0 root, an admin re-created over its inherited data dir and an adoption launch issue their own member certs (1ae22ae7b, 04d377ac5); the container silence reads a listed container's address with its running state (5b281c165, 9f3577537); a held birth is marked unheard by its own row (c0316ff2a); issuance cells (84e4707f8, 030b6b05a); Luke's test-gap matrices for what Rafka 141 found: C, an admin commanding its own stop or delete (f9f9af102); B, the Reconcile variants, with a storage-, placement- or tags-only change no resize and no replacement (056a6e55a, 9e0cc6cad, 74801368d); A, app-protocol dials by birth kind, with a dropped or refused Direct dialled again and only a Failed one waiting for its reconnect series (b448f156d..80a867008; connections.md section 5.1 on Luke's yes, 2026-10-25); RUST_BACKTRACE on the pass-through list (4450c7b49); the topology/view sweep, "book" banned and the topology-view "membership" renamed topology (0ce54a57d..e77178c0b); the fabric recovery runbook docs/eng/fabric-recovery-runbook.md (bb2bbef29), and fabric-recovery.md and node-admin-lifecycle.md section 4.4 naming the adoption launch (851e91390, Luke's yes 2026-10-25). Owed: a lifecycle attempt on a Build that never completed is still held; a primary-flags-only recovery start over a new data dir holds no member cert; the randomized-container driver panic (`mesh1.admin.1 does not answer node.get`), unattributed; the tail (fast, release, export run, soaks, export-manifest) at 80a867008 is running.
- **R-D2 contact storage out:** `NodesStorage::own`, `put_own`, `contacts`, `put_contact`, `remove_contact` (storage.rs:835-843 at cd7eba8d, 27 call sites in rdm-admin) go, their callers on the whole-record and own-seed paths; Rafka then deletes `ops.nodes.contacts`. **Landed `3f384b778` (2026-10-24), cell fix `a90ecbcee`; 141 told.** A birth's address and primaries are on its Node record (`node-row/3`), the own row in the data dir (`put_own_row`); the recorded primaries are read off every record, Deleted included (Q149 arm A); the fabric-primary's rows writer writes no mesh whose mesh-primary its book holds (one writer per record; `rounds` red at bf35d4d27). Open: `rdm-admin:main` stalled once in the fast gate at 3f384b778 (8 test threads, no stack; not reproduced in the next two gates with a stack watcher attached); 327 test and code function names carry banned words, a sweep for Luke; the span `via-authority-moved` carries a banned word. Proposal owed for Luke's yes before any build.
- **The name sweep (2026-10-24, lead; [the removal](proposal/i143-made-up-words-removal.md); Luke: before the tail run):** landed `ee365abc3` (8 commits): 193 function names and 205 cell names in the terms, the span `rdm.node_rpc.status.update.via-authority-moved` renamed `via-mesh-primary-moved`; 204 `removed:` lines in the commit bodies. Left by rule: wire, durable and serde names (`SeatYielded`, `CurrentParked`, the admin-ui field `seat`); four public items Rafka consumes (`BeforeReadyCtx::authority_not_ready`, `Seats::cohort`, `BuildProjection::lane_open`, `status_authority`); page prose (connections.md, states.md, node-rpc.md, build.md, node-recovery.md), Luke's yes owed; the machinery the removal's §7 takes out (seat watch, the Build-log fence, lanes, exit proof), never renamed to an equivalent; file and module names. Unmapped: `refuse_retired`, the gossip receive record's `fence`, iroh's `relay`.
- **The seat watch and a resumed stop (2026-10-24, lead), landed `6441432ae` (three commits):** the seat watch's resubmit loop ends once the birth is heard again or its mark ends (node-recovery.md step 1); the fabric-primary investigates only an unheard mesh-primary, never a standby of its own mesh (step 2); a completed `node.stop` receipt is not taken into the book for a birth whose runtime exited (node-stop.md, Entry live; receipts validated before reuse). Read from `accepted_topology` and `cohort_recovery` release reds; RED cells for each; flame `01e237c1d9d8cfcc8014f346ee1f9bf1`. Named, not changed: a tickle answer alone does not end the loop (the tick's reached result is local to `boot.rs`); the view keeps a reached birth marked by book age only (`projection.rs`).
- **Two intermittent reds, read (2026-10-24, lead).** `node_replace:249` ("the open's parent"): the cell read before the fabric-primary's evidence batch (200 ms, `telemetry.rs:296`) wrote the serve span; the waits now hold it, landed `c06095cd1`. `app_channels::a_late_declaration_…` (`:236`): a product defect, `held_topology` (`membership.rs:3074-3085`) served the own mesh's live member list under its last published `topology_version`, against gossip.md §3.1 and its receiver rule; fixed by serving the last publication as published, landed `344a2d5bd` (RED cell `served_topology.rs`; flame trace `f307d4d4af427fffee65951d5c771a7c` on the .99 Jaeger). Not covered by the fix, named: an unpublished primary still serves its own mesh from its book at version 0 (`92cdd344b`), where node-rpc-envelope.md:1209 says a mesh with no published snapshot is absent.
- **Q152, a never-ready fabric's deletion (Luke agreed 2026-10-24):** fabric.delete is the one Build that may interrupt the Build in progress; no build.stop. Its acceptance record is the durable intent, admitted operations end within their bounds, the interrupted run then gets `Failed` naming the delete, the teardown runs mesh by mesh, a successor fabric-primary continues it; its own mesh.delete and node.delete steps alone are exempt from fabric-not-ready. Three page sentences (build.md §1, states.md, fabric-delete.md; exact text in the rulings index) land with the code: R-PC1 and the build-in-progress check exempt a Build whose plan holds the fabric delete. Not built; owner to be assigned. Landed 328002997 (stack 328002997..c280efa34, 2026-10-24), fast and release gates green; 141 told.
- **Q153, the backbone arm on a `running` read (Luke yes 2026-10-24):** from the backbone a `running` verdict ends in the investigation's report, nothing written: no row, no name release, no book mark, no create; `exited`, `os-stopped`, `unproven` keep Q148's delete without terminate; the own-mesh arm unchanged. fabric-recovery.md step 3's Q148 sentence replaced and node-investigation.md line 17 extended (exact text in the rulings index), landing with the code in the R-D3 lane; the kept run `cc88ff3fe` with its full cut is the red. **Landed `2959670ff` (2026-10-24)**, with the two page sentences; cell `fabric_takeover::a_backbone_executor_that_reads_the_fabric_primary_running_writes_nothing` (the full 15 s cut, RED on the old code). Its release run's one red, `accepted_topology` (an extra mesh1 recovery), is main's: the seat watch's resubmit loop recovers a birth that answered its tickle (`seat_watch.rs:309-328`, since `1f8c28492`), against node-recovery.md steps 1-2; fixed in lane e1s8.
- **Q154, a Build's Complete is the fabric ready (Luke yes 2026-10-24):** every Build that puts the fabric to pending completes only after its own ready state is set, blocked `fabric-change` meanwhile as the landed code already does, failed on a failed round or hook; build.get's shape unchanged. states.md "Resize and roll" and build.md §10 sentences (exact text in the rulings index) land with the code. Not built; owner to be assigned. **Landed `e23671222` (2026-10-24)**; 141 told.
- **Q155, fabric recovery under an in-force delete (Luke yes 2026-10-24):** X, the executor fabric recovery names holds the fabric-primary's roles to the end and continues the delete; nothing re-created; a mesh left with no admin deleted in process at the executor. Sentences for fabric-recovery.md step 4 and "Nothing alive" and fabric-delete.md "Failure" (exact text in the rulings index) land with the Q152 lane's code; the lane's two red cells are the reds. Landed 41c3a8225 (stack 328002997..c280efa34, 2026-10-24), fast and release gates green; 141 told.
- **Q156, a node-admin created into a live fabric that has none (Luke yes 2026-10-24):** the adoption launch, `RDM_NODE_NAME` with no launcher plus fabric id, mesh and store root that must hold the fabric; adopts, mints no fabric or mesh id, new birth, custody from env or data dir with named refusals, the claim before any other mutation and one attempt for the exact lost birth, Q149 inputs first, the role by step 4, refused if any admin answers; Day 0 refused when a given id's store already holds a fabric. The "Nothing alive" opening replaced (exact text in the rulings index), landing with the Q152 lane's code. Not built. Landed f644d4ac2 (stack 328002997..c280efa34, 2026-10-24), fast and release gates green; 141 told.
- **Q158 (Luke yes 2026-10-24):** Q154's clause reworded, waiting is Blocked naming the blocker, a reported round or hook failure fails the Build; `sync_state`'s result gains a failure by name (build.md:67). Lands with Q154's code; the Blocked reason is `fabric-change` (Luke). **Landed `8ffce544e` (2026-10-24)**: `sync_state` returns `StateSyncAnswer = Result<StateSyncRound, StateSyncFailed>`, a failure fails the Build on `fabric-change` with the reason; 141 told. Day 0 unchanged (Luke). Named: the failure answer has no message shape on fabric-state-sync.md:54-70 or the envelope's state-sync handoff.
- **Q159 (Luke yes 2026-10-24):** the claimed teardown adoption is the one create a delete admits, at a new path.name in a not-yet-deleted mesh; refused while an admin lives; fabric.deleted reported alive then the outside caller's delete; no mesh left means the outside caller's delete alone. fabric-recovery.md bullet replaced, node-recovery.md's no-retry exception scoped (exact text in the rulings index). Lands with the Q152 lane. Landed 123b46596 (stack 328002997..c280efa34, 2026-10-24), fast and release gates green; 141 told. Owed, a follow-up landing in lane e6s4: the cells for a fabric with no mesh left completed by the outside caller's node.delete, and for the successor lost midway with the next one finishing. The owed cells landed `e6d564db2..9bf1b57a4` (2026-10-25): no mesh left (passed on main), the backbone executor lost midway (green: the delete plans from the Mesh rows on the store and the in-process delete resumes from its captured roster, mesh-delete.md:63-76), an adoption launch lost midway (ignored on Q163, asked: a person-launched adopter no live node heard has no inspectable runtime; the delete fails its step by name meanwhile, Q160). Fixed with them: an explicit delete of an unreached os-stopped birth terminates it (deployment-provider.md:217); an unproven lost birth under a delete is no refusal; a stalled teardown fails its step by name. Owed after Q163: the roster captures at admin/mesh_delete.rs:177 and :862 read the view, so a dead member not yet Deleted (mesh2.admin.1 in the backbone cell) gets no node.delete receipt.
- **Q160 (Luke yes 2026-10-24):** under a delete the executor takes the fabric-primary role and the lost mesh's mesh-primary role by one claim and one recorded transition, keeps its own mesh's, creates no admin, deletes the admin-less mesh in process; stale writes of the returning primary ignored by Q109's fold and its claims refused StaleClaim. mesh-delete.md step 2 and fabric-recovery.md step 4 sentences (exact text in the rulings index). Lands with the Q152 lane. Landed c280efa34 (stack 328002997..c280efa34, 2026-10-24), fast and release gates green; 141 told. Owed, a follow-up landing in lane e6s4: the cells for the successor lost midway through the cleanup, and a paused-and-resumed fabric-primary end to end (the stale writes are proven today by the journal fold and the run door, `build_run.rs:1316`). `build.md:384` (the next attempt after exact departure proof) is the general rule; under a delete the claim follows Q160 (`fabric-recovery.md:79`), which ignores a returning fabric-primary's writes. The paused-and-resumed fabric-primary cell landed `e6d564db2..9bf1b57a4` (2026-10-25) with its fix: a delete-mesh carrying an attempt the Build has moved past is refused by name (admin/mesh_delete.rs:657), so the stale fabric-primary deletes nothing. Unattributed: release gate fe64d3570 timed out node_stop at 300 s on `a_declaration_owed_across_a_move_of_its_mesh_primary_is_delivered_once_to_the_new_mesh_primary` (normal 70 s; not reproduced in four lane runs; its evidence lost); the cell's Build-stream reads are now bounded (9bf1b57a4) so a recurrence names its stream and keeps its evidence.
- **R-D3 the deployment provider's per-process launch:** `ResolvedNodeLaunch.env` carries a per-process map today. Owed: the KMS master key (`RAFKA_SYSTEM_KEY`, or `RAFKA_ALLOW_INSECURE_DEV_KEY` in dev) to compute, gateway and broker only, through the launch environment only (i50 #2045, Q3 arm (b)); the spawn env allow-list and its redaction, the restart env override, the port lease (non-conflicting, released on termination, bound ports on the node view), cpu-set, transport pin and mesh-endpoint placement; a setting with no slot refused by name. Proposal owed for Luke's yes before any build; the lead writes it (assigned 2026-10-24). Settled already, cited not asked: the KMS key through the launch environment only (i50 Q3 arm (b)); non-secret per-node settings through the row's `config` (node-config-update.md, the demo-surface plan A2); a setting with no slot refused `no-rdm-slot` at the front door (i141 B3 packet §4); node-admin assigns every listener endpoint and the transport binds port 0 and reports at join (deployment-provider.md, `RDM_ENDPOINTS`). To resolve, not redefine: `node.restart` retains the process (states.md), so a "restart env override" is a `node.config.update` or a replace, never a restart. Open for the proposal: the allow-list's owner and precedence and the redaction point before any span or log; the endpoint assignment's lifetime across a failed launch, exit, admin restart and a container, reserved versus advertised distinguished, with the no-code arm (what RDM does today) led and a gap named only with evidence; the no-slot refusal naming setting and provider before anything launches. Secrets never in a row, a Build record, a receipt or a span. Q150 (R-D3.1) ruled A by the architect 2026-10-24 with three amendments (declared names removed from the inherited environment, no host tier in the order, a name RDM sets or passes through refused), the §2.3 page sentence approved by Luke 2026-10-24 (exact text in the rulings index), landing with the lane's code; R-D3.2 and R-D3.3 arm A, no code, accepted as read from the pages. Q151 (a product kind's listeners) ruled A by the architect 2026-10-24 with A1's requirements: declared in the Q150 wiring, assigned with port 0 in `RDM_LISTENERS`, bound and reported at join, a malformed report refused by name, published on the row; no page text; CLAUDE.md's `RDM_ENDPOINTS` row corrected to `RDM_LISTENERS` in the landing (D1). The proposal revised 2026-10-24 on the architect's review of `708afd101` and A1's recommendations: values fixed for the fabric's life and kept beside the data dir as the root is; RDM's diagnostics scrubbed before a receipt or span; the container's `-e K` form and the daemon's record named as the boundary; listener addresses checked with the names, the whole join refused; a duplicate declaration refused; the three env table rows added; D7, the stale HTTP control-API rows and their dead reads; on A1's third review the custody document is versioned JSON refused by name when malformed, a changed secret is a new fabric in §0.3 and §2 alike, and the secrecy cells assert what the contract permits. R-D3 builds. **Proposal written 2026-10-24: `docs/plans/proposal/per-process-launch.md`.** R-D3.1 (the allow-list's owner and precedence, the redaction point) recommended A, for Luke; R-D3.2 (the endpoint assignment's lifetime) and R-D3.3 (the no-slot refusal) recommended A, no RDM code. Page-vs-code differences D1-D6 named in its §7. **Landed `2587e891e`..`411c94d3c` (2026-10-24): Q150 declared secrets as revised (custody JSON, data-dir file, scrub, `-e K`), Q151 named listeners with the node-rpc-envelope JoinNode text (Luke's yes), D1/D6/D7 cleanup, and two cell races read and fixed (demo proxy-restore read from held connections; the rshape control-loss cut dropped below the decision step). 141 told.** Named, not changed: an unpublished primary's version-0 read (node-rpc-envelope.md:1209); `deployment-provider.md:225` "exactly" vs the inherited host names; a container-provider admin restart in the harness (`estate.rs:950`) cannot bind its container address; the node view's listeners only on the launching admin.

## e12.s94 — 2026-10-26: the tail at de55ea949 read; 3c9f4326f lands its cell and export-run fixes; Q180 asked, Q181 drafted

The tail at de55ea949 (release, export, soaks, manifest) was red; every red was read from its spans:

- release peer_mesh_investigation and export 2803-detect (three cells): since Q174 the fabric is ready at
  the fabric-primary's apply of the mesh's ready, the moment the mesh's primary sends it; the cells killed
  that primary in that window, so the survivor adopted `pending` and the send's span was never flushed.
  The fixture now waits for the send and its first repeat (3c9f4326f).
- release own_delete_matrix and export 2784 (container): each read the fabric-primary's view, or sent
  the next delete, within tens of ms of the Build's completion, before the mesh's publication reached
  the fabric-primary. Each cell now waits for the deleted nodes to leave the view (3c9f4326f).
- export 2902 partition_rejoin: a `-p rdm-admin` job in the same batch relinked target/debug/rdm-node-admin
  under the running cell (executable hash refused). The export runner's batches now hold one cargo
  package (3c9f4326f).
- the import manifest's fabric-not-ready row named two cells Q171 removed; the row goes (Q170's
  build-in-progress refusal is the row above).
- release node_restart: Q180, asked (the fabric-primary closes a restart hold on the Build facts before
  it hears node.started; gossip.md:276).
- export two-victims (red since f71cc9c62): Q181, drafted, asked after Q180 (the cut victim of the
  fabric-primary's mesh is its lowest ready node-admin and re-creates the running fabric-primary, as
  fabric-recovery.md step 3 says; the cell says nothing is replaced from silence).

Remaining: the full tail at the SHA holding this record; Q180 and Q181.

## e12.s95 — 2026-10-26: the fabric's `storage_provider` (proposal, for Luke)

Q171 removed state-sync, state-commit and the scratchpad, and with them, unnamed, the requirement that
RDM's records reach the application's store established after Day 0; Luke was not told. Luke directed
the replacement: `storage_provider` is a property of the Fabric, on the Fabric's row and queryable,
changed live by a Build that carries it fabric-primary → mesh-primary → node, the fabric-primary
keeping access to previous providers. Proposal:
[fabric-storage-provider](proposal/fabric-storage-provider.md). Revised the same day with Luke, Architect 1's two reviews and the dev's: the fabric-primary copies every store first and closes claims with one row, each node then switches with its own tail, no dual write and no restart (R-SP6 to R-SP9, R-SP13); `topology_storage_provider` (renamed from `storage_provider`, the node's own storage being another thing) is a node-row property too (R-SP4); the fabric stays ready (R-SP12); the node's `storage` is renamed `storage_persistence` (R-SP16); nine mechanisms considered, the first shape among them. Owner: the lead. State: proposed, nothing built.

## e12.s96 — 2026-10-26: i149 (topology_storage_provider) recorded as an i143 dependency

Luke's i149 plan ([i149-topology-storage-provider](i149-topology-storage-provider.md), ebf2ba3f1, with
storage.md §2's final contract, ba3d50631 and 4b984a779) restores the requirement Q171 dropped: a
fabric moves RDM's five record stores to the application's provider while it serves. i143's
remaining work depends on it. State: plan pushed; e1–e6 not dispatched; the dispatch and landing SHAs
are recorded here as they happen.

## e12.s97 — 2026-10-26: storage change simplified (Luke); Q182; docs aligned

storage.md §2 "Changing it" is block, finish outstanding writes, copy, switch, unblock, one
insert-and-fail `commit` record as the switch, no Build but fabric.delete during a change, a node that
will not block or unblock re-created (f3b9be06f). Q182 (node-investigation.md): a node answering no
command is silent. The i149 plan (R9–R11 restated, R18–R19 added, canaries, spans, e4), the proposal
and the inventory's dating note follow it. Dispatched: i149-e1 (lane e4s8), q182-unanswered-command
(lane e6s4). Landed for the soaks: 491e871ab (an attempt's end releases the in-flight row it took),
dcfbd6933 (the soak driver resubmits on build-in-progress).

## e12.s98 — 2026-10-26: rulings Q176, Q181, Q183, Q184, Q185 recorded; Q182 landed

- Q176 (Luke): A, the state `ready` is named `ready` (node, mesh, fabric; wire and stored
  values, old values still decoding; `durable format moved`). Owner: the lead; not built.
- Q181 (Luke): a cut-off node is replaced and its old birth refused, so it never acts. Building:
  q181-cut-node-replaced, lane e4s2x.
- Q183 (Luke): node.restart is removed; callers run node.stop then node.start (Q180 decided by it).
  Building: q183-node-restart-removal, lane e6s4; 141 told to move restart_node.
- Q184 (Luke): topology_storage_provider never rides gossip; it is on the Fabric and node records.
- Q185 (Luke): the Build gossip channel is removed. Building: q185-build-channel-removal, lane e6s8.
- Q182 landed aa3967715 (a node answering no command is silent); 141 told.
- Q177 decided by build.md §9: `Wiring.hooks` is no application door; the code conforms (owner: the lead, queued after Q183/Q185).

## e12.s99 — 2026-10-26: Q185 landed, the Build gossip channel removed

Landed 2e270943b..3e69b2623 (lane e6s8): every admin reads the Build records from the store root;
the channel, its catch-up, `publish_fabric` and their cells are removed (declared in the commits);
gossip.md, build.md and CLAUDE.md lose the channel. Q186 (Luke): a successor's creates name the
stored Build's executables (build_takeover). The land gate's red cert_renewal cell read a member's
view and its peer's digest without waiting: the root admin's start is faster without the channel
(523 ms against 1814 ms) and the members' first ready declaration is owed again; the cell now waits
for both (3e69b2623). Flame: f1330d5a5b25ccdf3f8efeb31cfa97cf (the successor claims and converges the
delete Build from the store; no Build channel span). 141 told.

## e12.s100 — 2026-10-26: Q183 landed, node.restart removed

Landed 436ea2433..c0ad86379 (lane e6s4): node.restart, its NodeRestarting notice and hold, the
client's restart and the restart pipeline are removed; callers run node.stop then node.start; removed
positions reserved and refused by name; durable format moved; node-restart.md deleted, the pages and
CLAUDE.md name stop then start; 27 cells removed, 18 renamed (declared). Found and fixed red-first:
a second stop or start of the same birth in one Build reported Complete without running (9589869bf).
Q189 asked: a start right after a stop of a node in another mesh can be refused node-not-live while
the fabric-primary's view lags. 141: message pending (Remote Control disconnected).

## e12.s101 — 2026-10-26: i149 e1 lands, the customer names and reads topology_storage_provider

i149 e1 (lane e4s8, branch i149-e1, rebased on 2dab38ef0): named providers registered per executable
(`TopologyStorageProviders` / `NodeStorageProviders`; `admin::start_on` takes the registry), Day 0's
`RDM_TOPOLOGY_STORAGE_PROVIDER` written on the Fabric row, `build.create`'s Reconcile names
`topology_storage_provider` (omitted keeps the current one; an unregistered name is refused at
acceptance by name), `fabric.get` and every node view read it; `storage` → `storage_persistence`,
`on_retire` → `disposition`. A Build message carries no provider (Q184). e1's three acceptance cells
green; the restart-recovery canary and the happy-path seed stay RED for e4, kept on branch
i149-e4-cells. The live switch (block, finish writes, copy, switch, unblock) is e2–e4.

Landing gate red at 53518fc44: `rdm-admin:main` timed out at 300 s, its 8 test workers stuck at once on
8 unrelated cells, every other tree at its usual time; host load average 13 at the time, source not
attributable afterwards; the hang watcher ran the same binary 8 times on the lane, 8/8 green (448
cells, 31.6 s each), no stack. Read and unattributed; the watcher stays on every tail.

## e12.s102 — 2026-10-26: Q181 lands (a cut node is replaced, a deleted birth opens nothing); Q190's own-mesh arm fix

Lane e4s2x, branch q181-cut-node-replaced. Q181: a mesh-primary's one ladder is the verdict for every
member, its own node-admins included (the pre-Q111 hold-down and second ladder removed); a deleted
birth's claims and recovery opens are refused by name (node_rows::deleted_birth). Q187 and Q188 cells
as ruled. Q190, decided by fabric-recovery.md step 3: the own-mesh arm writes nothing on a running
fabric-primary it reached through no peer (74be797fa red: the isolated admin terminated the running
fabric-primary, trace cdf581fab5b3dc42c6e2ae919de2de27; 3942dd6e7 green). The two-victims cut now
isolates the victims from every port, nodes born after the cut included, and prints the cut it
applies (0e65ba807 red, 56e812c8a); the cell's runs at 3 s and 30 s floors before the fix are in the
Q190 packet. The two-victims export job proves the fix once the consumer build runs at this landing's
SHA (the rshape consumer bins were built at b3b82de).

## e12.s103 — 2026-10-26: i149 waves 2–4 dispatched

Per the i149 plans dispatch ordering: e2 (agent, lane e4s8, branch i149-e2) owns the shared storage
files this wave; e3 (lane e4s2x, branch i149-e3) authors its four acceptance cells RED and a
disjoint-file inventory only, its implementation after e2 lands; e4s eight acceptance cells, both
restart canaries and the seed are authored RED alongside (lane e7s3, branch i149-e4-red), the spans
as the plans OTLP bar names them. Q177 (Wiring.hooks removal, decided by build.md §9) runs in lane
e6s4. Q189 is open with Luke.

## e12.s104 — 2026-10-26: soaks on the canonical shape; release reds fixed; two rdm-admin:main hangs recorded

The two #2787 driver soaks run the canonical R-shape, two meshes of 2 node-admins, 2 compute, 3 gateway,
3 broker (8934f7d25; 120 s proof: 20 nodes formed, 24 rounds, 0 violations), as the rshape soaks already
did (Luke: "make sure that soak is running on a 2,2,3,3 x2 meshs"). The release run at 31666a3c3 was red on
six cells, all read: three still counted node.restart's single attempt (fixed to stop then start, Q183);
the container node cell asserted pre-Q181 behaviour (now follows the page, Q195), both landed 076c56ee2;
two Builds failed on a recovered-away birth listed at a stopped birth's path (projection, fix in flight).
Landing gate red twice on rdm-admin:main timeouts, unattributed: at 53518fc44 eight workers stuck at once
(8/8 clean reruns), at e400d832e one cell (`app_protocol_dials::a_day0_root_holds_a_chained_member_cert_and_its_app_protocol_dials_are_admitted_both_ways`)
past 300 s with the host's load average at 19 under parallel agents' estates; read off the critical path.

- Landing gate red on the Q201 rename (e4s8 tree 7e255fdc1; the direct fast gate on the same branch green at 120 s one rebase earlier), read and unattributed: `store_root_boot::an_admin_started_on_a_prepared_root_boots_as_a_new_node_in_recovery_with_its_fabric_and_mesh_ids` (store_root_boot.rs:458) saw only `rdm.mesh.entry.update.via-rafka-time-adopted reason=nothing-alive` and no `rdm.node_admin.node.create.via-inherited-root` in 30 s; the rename touches no boot path. The next tree (3ec7a3d9a) timed out `rdm-admin:main` at 300 s with nine cells past 60 s at once; unattributed: eight cells never finished (`admin_app_alpns::an_admins_app_alpn_dial_passes_admit`, `app_protocol_dials::the_day0_root_and_an_ordinary_node_it_launched_dial_each_other_on_an_app_protocol`, five `drift_convergence` cells, `topology_storage_provider::topology_storage_state_survives_role_node_restart`). An earlier line here attributed both trees to machine load with no span showing it; that was a guess and is withdrawn (Luke).

- 2026-10-26 night, read from spans: the release reds of tail 1ecc14207 are (1) a fabric-primary tokio worker stack overflow on the Build create/recipe path (node_replace rolled-back, mesh_delete_no_survivor; reproduced at RUST_MIN_STACK=1 MiB; futures boxed, lane e4s2); (2) cells behind Q183/Q188 and node-recovery.md step 1 (fabric_takeover carriers, node_replace recovery submit, rshape R3 and C17; fixed cells); (3) a NodeDeleted executed outside the subject's mesh never reaching it (rshape authority red 2; fix to gossip.md:161, lane e4s2x); (4) the seed's config read from the digest (Q202-Q204, lane e7s3). Landing reds read: store_root_boot (a pooled dial held its call's span open 30 s; dial span linked, D2), mesh_create (Day-0 mesh-state write before the mesh claim; lane land), app_protocol_dials hang (lane storage), fabric_delete paused fabric-primary's stale attempt ending the Build (first in 145 runs, 2/2 reruns green; lane e6s8). Decisions: docs/plans/lead-decisions-2026-10-26.md.

- 2026-10-27, from Rafka 141 (#3193), owed on the RDM side: (1) demo/tests/fabric_hooks.rs:144-153 launches a node by hand after stop_reconciling(), which aborts only the drift loop (admin/boot.rs:150-151), not a running Build's drive (:3205); the Day-0 Build's attempt 2, planned after the launch, deletes the hand-launched node as surplus, and with an of_this_process RuntimeFact that is a SIGTERM to the test process (deployment/process.rs:512). (2) A declare-node-state reply that finds its connection gone at node stop ends CANCELLED caller-gone at 3000 ms (rdm-transport client.rs:739, delivery.rs:997), a 3 s tail at stop. Traces .99 Jaeger 447b70d402fa6dc7c3f601b684d6d8f4, ddd17b3e4a0455c5359b87393f377433. Defect 1 landed c6d4b85e9; its red once at cd3838dbf (an attempt run a drive dispatched before the stop ran on past it) is fixed on i143-3209-q206-signal-stop 96b3d79be: stop_reconciling ends the runs too and returns once every drive and run it ended has ended, the cell 20 of 20. Defect 2 built under Q206 on the same branch (88cf46d59, 49bf94d4c): a stop's cut spares the connection to its mesh-primary, and a signal stop's process ends once each owed declaration is answered or its receiver is proven gone, that wait bounded by `RDM_CONNECTIONS_DELETE_DEADLINE_MS`; the stop ends 8.6 to 28.7 ms after its `node.connections.delete { All }` with no caller-gone tail. The signal-stop cell was green 3 of 5, its reds a birth's concurrent declarations applied out of order by the mesh-primary (an overtaken one refused rejected-invalid-node-transition): Q210, built on the same branch (80549a08c), a node's declarations queued in the order owed, one in flight, the next sent on the previous one's answer by name; the cell 20 of 20, one run's spans Ready applied 4.28 ms, ConnectionsDeleting 12.54 ms, Stopped 17.24 ms after the All, the process ended at 18.7 ms. Owed: Q210's three determinism cells (a seam holding the first send, across a mesh-primary change, across a dropped request ack). Q210's two §43 queue points (e9806c1a2), built on the same branch, RED first: a declaration refused `RejectedNotPrimary` is answered by name and the next one owed is sent (rdm-node, `OwedDeclarations::replied`); a node-admin's own declarations to its mesh-primary go in the order owed, one in flight, the next on the previous one's answer by name (rdm-admin, `Declarer::due`); rdm-node lib, rdm-admin lib, rdm-mesh-audit main and the name check green. The signal-stop cell's 20 in a row and the stop cells are owed once #3210's cell fix lands.
- 2026-10-27, Q205 (Luke): node.stop calls its primitives (node.connections.delete, node.mesh.leave) and streams each one's acknowledgement and completion; owed: the stop's stream, standalone LeaveMesh on 0x22 with CommandAccepted, the mesh-primary's Leaving/Left row writes, the pages (node-stop.md, node-mesh-leave.md, node-rpc-envelope.md:217/308/311, node-connections-delete.md:7), and an adversarial review of every workflow for the same gap (D34).

- 2026-10-27, Q207 (Luke, #3213, #3211 red 3): a caller birth a primary refuses as one it does not hold (a declaration refused `SubjectUnknown`, a deleted birth's Digest) is deleted or re-admitted by its path.name and the node-admin's atlas; built in lane 2 (branch i143-3213-refused-caller): one atlas for every node-admin over the meshes it is primary for, filled once as each is taken (`FabricAtlas::scope`), one decision for both doors, a node-admin that is not the fabric-primary verifying at the root with `fabrics.atlas.get`, a deleted caller reported for `node.terminate`, a re-admitted one commanded back through node.mesh.join; the atlas row never read from the gossip view. Pages in the lane: node-investigation.md, node-recovery.md, fabric-recovery.md, mesh-investigation.md, node-rpc-envelope.md. A caller whose record is deleted is deleted, its runtime terminated, never re-admitted; a path still needed is recovered by re-create through node recovery (Q217, Luke, 2026-10-27; `delete-deleted-birth`). The fabric-primary's test doors (drift_convergence, node_objects, fabric_delete) read node records, never the view's birth (the hang inbox 0007 named). Owed: Luke's yes on the page text and on the FabricAtlas-naming lines (the FabricAtlas-naming lines decided by Q224, below).
- 2026-10-27, Q224 (Luke, "a"; issue #3223): after Q207 every node-admin holds one atlas, so `FabricAtlas` becomes `Atlas` (`crates/rdm-admin/src/atlas.rs`) and prose says "a node-admin's atlas"; `fabrics.atlas.get` keeps its name, the fabric-primary's atlas. Built in lane 6 (branch i143-3223-atlas): the type and module renamed, no wire change, no span renamed, spans.md and wire.md regenerated; node-rpc-envelope.md:314 and :371 read "the fabric-primary from its atlas" / "in its atlas"; node-name-update.md:26 and node-config-update.md:13 named the type too and read "in its atlas" (Luke's yes on those two lines owed). The rdm-test cell file `tests/fabric/fabric_atlas.rs` keeps its name: it is the fabric's read. Landing owed (the lead).

## e12.s105 — 2026-10-27: the gates documented; Q209 and Q210 ruled by the lead

build.md §4.4 "The gates before an operation" landed 7cd531aa3 (Luke: the gates before an event
belong in build.md): each gate at the fabric-primary in the code's order, the source it reads and its
refusal; the lifecycles index and node-start.md's Admission row point at it. Q189 stays closed by Luke:
admission reads the fabric-primary's view. Q209 (recovery submits behind a cut) and Q210 (a node's
declarations in owed order) ruled A by the lead, delegated by Luke (INDEX); both land page and code
together: Q209 from the held commit 57fab58bb (branch q118-no-local-recovery-2) with the page
sentence, Q210 as a new unit (one declaration task per node, the §43 sentence, the RED cells).
Q212 (a refused dialer no view names: span only, no row) ruled A by the lead, decided by the i145 record
(INDEX); the page row lands as docs, the held commit 83167e17e is dropped.
Q213 (the proxy-retirement obligation: only the source's own Direct Connected creates it; an accept from the
destination is a recovery trigger) ruled A by the lead from D15 with an arch-1 review (INDEX); connections.md
requirement 16 restated to §13 G in the ruling's landing; no code, no cell.
Q215 (the demo UI's `rdm.ui.*` span goes, the log line stays; §9 rule 1) and Q216 (the seven page-named spans
no code emits: five page lines take the code's or a conforming name, landed; `rdm.node.roots.update.via-installed`
and the two renamed cert spans wait on i145 e5; the absent-birth replace of node-admin-lifecycle.md:155 is a code
unit owed with its span) ruled A by the lead with an arch-1 review (INDEX). Owed code, one unit each: the demo span
removal with spans.md regenerated (Q215); the absent-birth replace (Q216). The "160 branches on origin" claim is
false: origin holds `main` alone (`git ls-remote --heads origin`), rafka-v2's two; nothing to delete. The §43 queue
findings (RejectedNotPrimary advances the declaration queue; node-admin's own declarations queue too) are Q210's
unit, already ruled. The chaos reds `process_fault_backend` and `wedge_detector` are read from spans first, the
standing rule. Q214 is unrecorded: lane 4's branch `i143-q214-deleted-not-departed` carries the number with no INDEX
row; its owner records it.
Q218 (a Build's action pinned to the gated birth: node id + incarnation on every action targeting an existing birth,
verified by the executing mesh-primary, mismatch refused `birth-replaced`; `durable format moved`) ruled A by the lead
with an arch-1 review (INDEX); lands in #3215's unit with its RED cell. Q217 is unrecorded like Q214.
Q219 (a Dead status is never exit proof; gate 5 reads the node investigation's `exited` verdict, one gate on all three
delete paths; a silent Dead birth fails admission until investigation gives the verdict) ruled A by the lead, arch-1
concurring (INDEX); lands in #3215's unit with its RED cell.
Q221 (gate 6 for a Build: at acceptance from the fabric-primary's view and again per operation before claim or
dispatch; the one exception is mesh-investigation's decided operation on the unheard mesh's node-admin) ruled X by the
lead, arch-1 concurring (INDEX); no page change; lands in #3215's unit with its RED cells. Q220 (a delete admits a live birth, which its own sequence stops first; Dead still fails, Q219) ruled A by the lead
(INDEX); build.md gate 5's cell landed with the ruling; the code already conforms.
Q223 (gate 4 refuses `unknown-node` for a steps Build too; `RecipeNodeUnknown` folds in on lane 6 before Q211 lands)
ruled A by the lead, arch-1 concurring (INDEX). Q211 is unrecorded like Q214 and Q217.
Q225 (the envelope's Step list gains DeleteConnections and LeaveMesh, node-replace.md's own steps) ruled A by the lead
(INDEX); the line lands in Q211's unit on lane 6. Q224 (FabricAtlas becomes Atlas) ruled A by Luke (INDEX), built on lane 6 (#3223).
Q230 (the data plane speaks proxy only: `Alpn { name, proxy, carriers }`, `proxy_stream_dir`, the page's Proxy lines say
carry or proxy; the carrier-kind list stays, the application's per-protocol policy per connections.md:373 and §14; Q227
was mis-cited for its removal) ruled A for the words by the lead (INDEX); a code unit owed, its page text reviewed with
the landing.
Q222 (gate 1 refuses `not-fabric-primary`, the envelope page's and the wire's name) ruled A by the lead, arch-1 concurring
(INDEX); build.md gate 1's cell landed with the ruling. Q214 and Q217 are unrecorded.
- 2026-10-27, e12.s10, Q208 (Luke; asked by Rafka 141, #3201; issue #3212): the bootstrap's first step mints the node-admin's id. Built in lane 4 (`i143-3212-bootstrap-mints-id`), landed 77aa77ea9 (CLAUDE.md row 43094af7d): `AdminConfig::node_id`, minted in `from_env` with no launcher, a launch's `RDM_NODE_ID` otherwise; `start` runs as it (boot.rs mints no node id); `entry::run_on` reads the configuration before the telemetry, whose resource carries `service.instance.id` (`rdm_transport::init_evidence_telemetry_for_instance`), and the deployment and boot spans carry `node_id` from their start. Pages: node-identity-and-state.md "Day zero", fabric-create.md Identity order, fabric-primary.md §1.1 and §9, telemetry-patterns.md §3 (Luke's yes on the exact text owed before landing). Owed: the CLAUDE.md `RDM_NODE_ID` row (an agent does not edit CLAUDE.md), and Rafka's node-admin calling the first step before its OTLP provider. Cells: `admin::tests::the_bootstraps_first_step_holds_the_node_id_a_launched_admin_its_launchs`; `store_root_boot` Day-0 and inherited-root cells read the deployment span's `node_id`.
- 2026-10-27, Q211 (Luke, from D33; issue #3216): the Build's ordered list is steps, one item a step. Built in lane 6 (`i143-3216-recipe-to-steps`): `BuildChange::Recipe` → `Steps`, `RecipeStep` → `Step`, `rdm_client::Nodes::recipe` → `steps`, `rdm_test::recipes` → `rdm_test::steps`; the build refusals `empty-recipe` / `recipe-step-repeated` → `empty-steps` / `step-repeated` and their spans, and `recipe-node-unknown` folded into `unknown-node` (Q223: `BuildReject::UnknownNode { node, step }`, the detail naming the step and the target, span `build.reject.via-unknown-node`; RED first, `accepted::tests::a_build_plans_its_steps_in_order_and_never_reruns_a_completed_one` and `node_replace::steps_naming_an_unknown_node_are_refused_unknown_node_naming_the_step_and_execute_nothing`; `BuildReject` reaches the wire only as its reason and detail strings and is stored nowhere, so no durable format moves); `build.create.via-recipe` / `build.update.via-recipe` → `via-steps`, `via-recipe-unplannable` → `via-steps-unplannable`; the attempt reason and action name in views and spans `recipe` → `steps`. The positional wire moves nothing; the Build journal's JSON tags `"recipe"` stay (`#[serde(rename)]`, as `roll`), proven by `build_state::tests::an_attempt_of_steps_keeps_the_journal_tags_written_before_the_rename`. Pages: build.md, fabric-mesh-node-lifecycles.md, gossip.md, node-identity-and-state.md, node-rpc-envelope.md (its Step line also lists `DeleteConnections` and `LeaveMesh`, conformed to Q225 and node-replace.md:30-31), lifecycles/node-replace.md, node-mesh-leave.md, node-name-update.md; wire.md and spans.md regenerated; `recipe` / `recipes` banned in the name check and CLAUDE.md. Rafka 141 owes the same renames on its side. Landing owed (the lead).
- 2026-10-27, Q227 (Luke; issue #3222, with #3229): X. No node reads anything from the store on create, start or rejoin; every node pulls from its mesh-primary or the fabric-primary. The fabric-primary is the one reader: re-born, it reads the latest Build records to learn what is in flight, and the node records too in the extreme case that it is the only node of a single mesh. Reads of connection facts ahead of a Proxy or an investigation go (`connections_writer.rs` `hydrate()` and `refresh_fleet_directs()`, `storage_backend.rs` `fleet_directs()`); an ordinary node writes its own connection facts and reads none back; the mesh-primary's node investigation reads them when it runs (`offline.rs:397`). Q229 is decided by Q227. Built in lane 4 (#3229) with the connections.md text (§3 carrier discovery, §4.1, §6, requirements 7, 9, 14, 15), exact old and new text to Luke. #3222's open-time refusal remains only where the fabric-primary or an investigation opens a store. The data plane's `forwardable` / `carriers` (`Alpn`, `CarrierPolicy`) carry the management plane's word and a carrier-kind list no ruling approved; untangled next, its own question.
- 2026-10-27, #3228 connections coverage landed 27150f6d4 (`docs/plans/connections-coverage.md`, one row per requirement of connections.md and the connection lifecycle pages). Q230's unit is #3230, lane 3, sequenced after #3229 (Q227 X), which edits the same carrier pick.
- 2026-10-27, Q227 owed (the lead, from #3229's inventory): node-admins still read the store at their start, each read needing a pull from the mesh-primary or the fabric-primary that does not exist yet, so each is its own unit after #3229: rdm-admin admin/boot.rs:489 `TopologyStorageHandle::open` (folds fabric, meshes, nodes and builds .jsonl at open); :365-381 lone_birth (own row, live records, fabric); :509-512 held runtimes into the nodes store; :547-575 the inherited start's Fabric record, own mesh row and contradictory-start check; :630-650 the adoption reads (Q156, the fabric-primary's recovery path, kept by Q227); :866, :918 own runtime rows; :1169 the Fabric record; :1262, :1268 current_build after the entry pull; :1343 meshes; :1959 runtimes; status_rpc.rs:214 `Declared::rehydrate`. The re-born fabric-primary's Build and (single-mesh) node reads stay (Q227).

### Open work, 2026-10-27 (the lead): every unit has its issue; this list is the record

Each open i143 unit, its rafka-v2 issue and where it stands. A unit leaves this list only by its landing SHA. Lane state: `scripts/lane-status.sh`.

| issue | unit | state |
|---|---|---|
| #3211 | release-gate reds: red 1 (backbone heal) and red 3 | red 2 landed 5ddffdefa; red 1 read (caused by red 2's command, refused at HEAD); the cell's wait fixed in lane 7 (2999ba9aa), 10-run proof then landing; red 3 read from the soaks (rdm#80) |
| #3215 | build.md §4.4 gates: Q218, Q219, Q220's cell, Q221, Q228 | built in lane 1 (11bb3b90d); RED only on rdm#78's race; lands after #3237 |
| rdm#78 | the store reader's 40 ms refusal | built on origin/i143-3222-store-reader (33774971b), parked; lands first of the node store reads (Q237); next free lane |
| #3229 | Q227 X: ordinary nodes read no connections, the carrier from topology | **landed 529b978dd** (connections.md rewritten; `NoActiveRoute` when every carrier refuses) |
| #3230 | Q230: the data plane speaks proxy (words only, the carrier list stays) | **landed 96e30265a** (§8 c42873da5) |
| rdm#79 | the carrier's via-tunnel span and splice outlive a Proxy deleted by id | open; next free lane |
| rdm#80 | learning-soak findings: a peer mesh unheard after heal; a wedged admin re-born; a held birth no attempt acted on | routed: heal to #3211 (red 1's mechanism), re-born and held to #3215 (Q218/Q219); read again after #3215 lands |
| #3233 | a Build after a node.stop loops node.create on the stopped path | lane 2: cause found (accepted.rs:1336 counts only a live birth), REDs queued |
| #3234 | Q167 reads with the property filter; Q232 local query; Q233 read side | **landed 1b8a8ddd1** (query.md; io_calls=0 on every read) |
| rdm#81 | Q210's owed point: a signal stop waits for the declaration queue | open; next free lane |
| rdm#82 | Q235/Q236: a node's own config pushed up; a Build's config supersedes | open; the proposal docs/plans/proposal/q235-node-config-flow.md (P1-P5 decided) |
| #3237 | Q227/Q234: node-admins read no store at start, join and hydration; OwnRow | lane 3: inventory accepted (item 7 moved to rdm#84), building the race path first |
| rdm#83 | Q233 code: the row's status from the node's ack and completion; delete the mesh-primary's | open; next free lane |
| rdm#84 | a mesh-primary's Atlas filled from the fabric-primary; put_runtime only by primaries; rehydrate unused | open; after #3237 |
| rdm#85 | Q237: a node's point read of another node's row from the store | built in lane 3 (i143-rdm85-node-row-read): `RunningNode::node_row(id | name)`; the row family and its file reader moved to `rdm_node::nodes_storage`, the store opened on the first read; query.md §1's node paragraph and line; for the lead's review |
| rdm#86 | Q224: a mesh-primary's own Atlas answers its mesh's reads, passing others up | open; with rdm#84 |
| rdm#87 | Q167 R-RD5: builds.get with the filter | open; its source waits on Luke's answer below |
| rdm#214 | mesh-x's MeshStatus degraded never sent on either investigation verdict (mesh-investigation.md:65, :168; states.md:12); found by Q272 | open |
| Q268 | the gossip receive path stops copying the view per MeshStatus frame (DigestView counts under its lock, topology.rs:2028 and :2441); the take_snapshot Arc change waits for L1 at production cadence; Luke 2026-10-29 "yes" | landed: #1 at bfb995949; #2 (take_snapshot Arc) not built: at production cadence take_snapshot is 0.0 % of the idle node-admin, the idle rpc node and the node under M6 (perf, 1a5439c4d/dfad9d9d6) |
| Q269 | snapshot chunking finds each run by search instead of re-encoding after every item (chunking::pack_in_order, shared by the Node RPC read, the gossip publisher and the Build facts; identical runs and bytes); Luke 2026-10-29 "yes" | landed cd99bfe4e (#212); E9/E10: CPU per read 0.47 to 0.42 ms |
| Q270 | RDM's architecture pages stop pointing at rafka-v2: every Rafka citation removed (arm A), RDM's own rafka-time section written from RDM's code; Luke 2026-10-29 "a", text "yes" | landed 017ea94d1 (#213) |
| Q272 | a successor mesh-primary publishes no MeshStatus until it holds one (handed by the fabric-primary's one ApplyMeshState, or heard); adopts it exactly, changed_at preserved, silent until its checklist, then the held status or the ordinary forward change; a Pending handed with 0 (Day 0, a new mesh's first admin) is the mesh's first state at the receiver's instant, sent at once; the adoption launch holds its Mesh record's state at its own instant and republishes after its checklist; the hand-off stays one Status call (arm A); the lead 2026-10-29 with arch-1 | ruled; builds and lands in lane 3 with 4d6bb4e95 (the #2805 process cell is red there); the mesh-x degraded MeshStatus (mesh-investigation.md:65) not sent: rdm#214 |
| Q273 | node investigation inspects first: node.runtime.inspect before the tickles, its exited/os-stopped the verdict with no tickle sent, the ladder on running/unavailable and the inspect's answer the verdict on unreached; the inspect bounded at 2 s in the same unit (expiry is unavailable, never exit; the subprocess ended; the tickles follow); budgets and M4 targets unchanged (arm X, inspect first; arch-1's concurrency dropped, its bound kept); the lead 2026-10-29 | landed at 2286991c5 (#226, lane 6), the eleven page lines with it; M4 at the production cadence owed once at the tip |
| Q271 | incumbency_of reads supersedes under the view's lock, nothing copied (about 6.5 % of a node-admin under M6, the dev's profile); the lead with arch-1 | landed at 3ba31243a (#227); the M6 profile before and after owed with the load runs |
| Q274 | a node.delete whose executor's view does not hold the pinned birth yet ends incomplete and resumes on that same birth when the view moves, never a newer birth at its name (Q218, Q253); build.md:338 text applied as conforming | landed at 73e56c3ba (lane 7, Gemini, via lane 6; #216 unit A) |
| Q275 | CA custody after fabric.ca.renew: the CA material rides node.issuer.update with the issuing material (no new UpdateCa pair); a successor not holding the row's CA renews again from the root; the lead with arch-1 | landed at f02a0622f with fabric.ca.renew (#225); the wire carries ca_key only |
| Q276 | a primary taking its role reads the accepted Build from the fabric-primary (0x1F) before counting its fresh checklist, owed until answered, never counted or completed from lagging facts; the lead with arch-1 | landed at b870cc6d7 (#228) |
| Q277 | node-investigation.md:42: a caller whose record the admin holds deleted stays deleted (Q217); conforming text applied | landed with this docs commit; code conforms |
| certs: fabric.ca.renew and fabric.root.update | both specified (fabric-ca-renew.md, fabric-certs.md Custody) and unbuilt, found by Gemini coverage batch 1; Luke 2026-10-29: fix it. Steps: 1 node.roots.update, UpdateRoots / RootsUpdated / RejectedRootStillChained on Status 0x1B, every node installs its root set (gating, lane 3); 2a fabric.ca.renew on the fabric-primary's tick, renewable issuer material, every mesh re-minted under the new CA (lane 4); 2b the CA row on the fabric record and the CA custody to every admin, a new Status primitive needing page text; 3 fabric.root.update, UpdateRoot on Status to the fabric-primary, the roots row minted / distributed / ca-rotated / retired, the phase driver, the root secret kept as ca-secret-next until retired; 4 fabrics.get roots and ca, and the page's two proof cells | landed: step 1 node.roots.update fe0d7726f; step 2 fabric.ca.renew, the CA row and Q275 f02a0622f; step 3 fabric.root.update (Q278 wire) 7314b8e1a; step 4 fabrics.get roots/ca 743ccf2f5; #225 closed; the end-to-end cell rotates a fabric through its four phases |
| caller ops: mesh.update, fabric.update, mesh.config.get/update, fabric.config.get/update | the op ledger lists all six as typed objects (node-rpc-envelope.md:244-251, :330-332: "fabric.update puts the full fabric meta as a blind put and never changes status; fabric.config.get/update hold the application's attributes"; the same for the mesh); no code names any of them (found 2026-10-29 while choosing fabric.root.update's wire, which the ledger calls "a single call like fabric.update") | open: the wire on Status 0x1B beside node.config.get/update, one lane, cells per op |
| Q267 | backbone default 5 s (gossip.md:510 and cadence.rs; gossip 2 s, staleness 30 s unchanged); the load harness at the production cadence, each row recording it; E1-E8 marked as taken at the test cadence (500 ms / 500 ms / 3 s); Luke 2026-10-28 | open (lanes 3 and 4) |
| Q266 | the topology read stops copying what it drops (E8: 60 % of a node-admin's CPU under M6, 0.59 ms a read): incumbency once per read, the stored map only for a mesh with no held snapshot, the view filtered before it is copied; Luke 2026-10-28 "make the fix" | landed c2cbe3ed2 (#196); the clean before/after M6 owed on a quiet box |
| rdm#187 | Q265 step 1: rdm-load, the harness and L1 (M1, M2, M5, M8); docs/plans/proposal/load-testing.md ruled 2026-10-28 by the lead, delegated by Luke, with arch-1: 200 nodes across 10 meshes the first size, targets as targets (M4 from the kill on node-recovery.md's timeline, M6 one node-admin at a time, 20-sample targets on the maximum, M2/M9 no target, M10 growth limits, M1 per profile, frozen conditions) | open; after the open i143 units above land (the reds first), never ahead of them; then L2-L4 each red row a unit, M9, M10, chaos at scale, multi-host |

Open with Luke or the architects (no unit until ruled):
- Q231: decided A (Luke, 2026-10-28): the caller's trace context passes end to end and is handed to the application at its notices (LifecycleOp, the accepted Connection); building rdm#115.
- rdm#87 (builds.get's source): decided by Luke's atlas words ("the fabric atlas is the union of what ops are currently executing and the current topology state"; "external query read from atlas"; "only FP reads the builds"): current and in-flight Builds from the fabric-primary's atlas, history from its builds journal.
- The source of builds.get and connections.get (R-RD5 store-backed stream, R-RD7 the node's own log) against "reads answer from the Atlas".
- states.md:13 names the node's state `ready`; Q233 and the query page say `ready`.
- fabric-recovery.md:60-61 (a reached lost fabric-primary "is commanded back through node.mesh.join") against node-mesh-join.md:22 (only the fabric-primary commands a mesh-primary); the code follows node-mesh-join.
- The proposals docs/plans/proposal/query-page.md (query.md) and q235-node-config-flow.md: the architects' review and Luke's yes on the page text.
- Q262 (RDM's docs tooling, delegated to the lead): rustdoc for the API and an mdBook guide from this repo, built locally by scripts/docs-site.sh, no CI (rdm#184 landed 0e0a8953e); the rdm-docgen trim and the move of the wire, frame and status tables onto rustdoc are owed, no unit until sized.

Running outside the units: the learning-soak loop in lane 5 (each iteration kept under kept/soak-<sha>-<n>); the final tail at one SHA once the units above have landed.

#### The audit of every ruling and proposal, 2026-10-27 (the lead)

Every INDEX row whose state was not landed, decided or withdrawn (Q63-Q214, 48 rows) and every file in docs/plans/proposal/ was read against main. Rows found done are done; Q63, Q64, Q65, Q67, Q139 and Q140 are rafka-v2's. What remains, each now tracked:
- rdm#88, the vocabulary sweep: banned words written before the bans remain in crates/ (Q214's departed tail, Q111's exit-proof residue, and the rest by count).
- To Luke, a ruling or a withdrawal: the failure matrix (docs/plans/proposal/i143-failure-matrix.md says it lands as lifecycles/failure-matrix.md once approved; no ruling, no page); store-root-one-directory-per-entity.md (asks Luke's yes on R-SR1 to R-SR3; the code uses them and Q227 cited them; no INDEX row); Q110's open point (in standalone, an old node can write to the shared directory before it is refused; no recorded answer); ops-naming-proposal.md item 4 (gossip frame naming, waits on Luke's go-ahead); node-status-heartbeat-connections.md R-HB3 (parked post-beta).
- Page sentences a ruling named and main no longer carries, each likely superseded by a later rewrite, to confirm: Q96 (the hand-back key), Q108 (the vacated seat line; the elections page was rewritten by Q107), Q94 (the envelope's "calls the winner"), Q76 (gossip.md §3.1's "node.delete replaces NodeStopped"), Q131 (the thawed-node rule has no page line).
- 2026-10-27, open-work additions after the audit: rdm#88 (the vocabulary sweep), #3245 (the gate takes the box's turn lock; the learning-soak loop in lane 5 is stopped until it lands, because every gate ran beside a soak batch at load 346), docs/eng/consumer-guide-ops.md (33e41b590: an application's own Node RPC ops). #3218 and #3203 closed (done on main).
- 2026-10-27, the ops guide's §8 tooling (docs/eng/consumer-guide-ops.md, landed 1cf00038e; Luke's expansion 0b7612c10; arch-1's corrections), each its own issue, in order of value, none blocking a first op: rdm#90 the reserved-variants helper; rdm#91 the compiled consumer example, built into the demo (Luke: not a separate CI crate); rdm#92 the conformance harness (Count and BrokerData pass unchanged; Rafka's first op runs it); rdm#93 recovery helpers over application persistence. rdm#89 (split from #3237): a node-admin's topology stores open without folding. The guide's query link flips to query.md in #3234's landing (lane 6, inbox 0005).

- 2026-10-27, RDM's issues move home (Luke): CLAUDE.md said since df378f7a2 (2026-10-13) that issues live in rafka-v2, so every RDM unit since 2026-10-14 was filed there; RDM's own repo had them to #77. The 16 units not yet started were copied with their comments to drlukeangel/rust-distributed-mesh and closed in rafka-v2 with a pointer: #3222→rdm#78, #3231→rdm#79, #3232→rdm#80, #3235→rdm#81, #3236→rdm#82, #3239→rdm#83, #3240→rdm#84, #3241→rdm#85, #3242→rdm#86, #3243→rdm#87, #3244→rdm#88, #3246→rdm#89, #3247→rdm#90, #3248→rdm#91, #3249→rdm#92, #3250→rdm#93. In flight, finished where they are: rafka-v2 #3211, #3215, #3230, #3233, #3234, #3237, #3245 (#3245 also points the lane scripts at RDM's repo). `rdm#N` is drlukeangel/rust-distributed-mesh#N.
- 2026-10-28 (night of 10-27), landed through the lanes, each told to 141: rdm#83 b25be8d23 (Q233 writes), rdm#78 b5fa9e0c7 (store reader), rdm#82 b8f050a1b (a node's own config pushed up), #3211 red 1 d4136c512, rdm#85 391a860ad (a node reads another node's row from the store), #3233 2278a6abd (the stopped-path create loop), rdm#95 051431adf (NoActiveRoute direct), rdm#96 a4cb863ac (the gate's exact module skips), rdm#81 205519f76 (signal-stop span), rdm#89 1d161e42e (stores open without folding), rdm#94 bff5753cd (probe cell), rdm#98 aaeefb94a (cert_renewal race), rdm#84 e1c5cdde5 (one seat-take read; runtime rows by their launcher), rdm#79 7ccf29941 (the carrier's tunnel span). The box's turn lock is gone (ff5b82e3f, 0491ebf87): gates and cells run without it; the learning soaks stay stopped until the units land.
- Decided: Q242 B (Luke, 2026-10-28): no node-admin reads the store at start; it takes its primaries, meshes' ids and Build floor from its mesh-primary or the fabric-primary; building rdm#97. Q239, A narrowed (Luke, 918e6dd45): exited or os-stopped is departure proof, never running, unproven or silence; built in rdm#108. Open for Luke: Q240 (a stopped node and its mesh's ready, states.md:15-16 vs :168; asked 2026-10-27 as "Q238", renumbered: Q238 is the config-writer question, decided A), rdm#87 (builds.get's source). Q-86a decided as Q243: a non-primary node-admin forwards the Status reads up like every node; building rdm#116.
Q239 (an os-stopped executor's unfinished attempt: `exited` and `os-stopped` are departure proof, `running` and
`unproven` are not; the next attempt is claimed and the mesh recovers) ruled A, narrowed, by the lead (INDEX);
build.md:424 landed with the ruling; `DepartureProof::proven` and its RED cell are a code unit owed.
Q241 (a renewed cert row's issued_at is the cert's own: `NodeCert` and `IssuingCert` carry `issued_ms`, the installer
writes the cert's dates, never a clock; `durable format moved`) ruled A by Luke (INDEX): admit verifies the signed field and adds no
clock-based rejection, renew_at derives from the cert's dates and the renewal inputs alone; a code unit owed with its
RED cells and the marker.
Q240 (a stopped birth does not hold its mesh below ready nor a checklist) ruled A by the lead with the flame
(INDEX); states.md:16 landed; built in rdm#114 (lane 1): `Topology::mesh_unready` and the adopted checklist's
`missing_members` leave a stopped birth out; RED cells node_stop::a_resize_after_a_stop_reaches_ready_* (the resize
Blocked on the fabric's change naming the stopped birth not-ready) and the delete-after-stop cell, now asserted Complete. Seat check: lane 2
(rdm#93) finished its gate at 083d88fe5 and is writing its hand-over, waiting on nothing.
Q248 (a starting frame after the join's pending moves the row nowhere, the stop's rule on the start's page) ruled A by
the lead (INDEX); node-start.md landed; the cell accepts the order, in lane 6's unit.
Q249 (rdm#140: an executor whose replayed start is answered already-applied for a birth its view holds stopped clears
the stop mark and publishes the clear) ruled A by the lead with the flame (INDEX); node-start.md landed; the code unit
is the landing blocker, owed first. Lane 5 (rdm#87) rebases onto 9cd422ab2 and gates again; its own red is fixed in 43aafb405.

Q252 (Q245's heal and a caller's own node.create: one failed create is created again by the next attempt; the same
failure twice running ends the Build Failed { step, reason } naming the step and its concrete reason, never a stall Blocked
incomplete, since build.create refuses BuildInProgress for as long as a Build sits in progress) ruled B by the lead with
arch-1, decided by node-rpc-envelope.md:267, :1342, :1351 and :338 (INDEX); no page changes; built in rdm#152 (lane 3),
which also brings Q250's section stall (build_drive.rs:1944) to the same terminal; the drive's nothing-moved stall
(build_drive.rs:1512, a Blocked frame not from Status Blocked) is recorded in INDEX as owed.
Q253 (after a hand-back with nothing moved, when the next attempt is claimed) ruled A by the lead with arch-1 (INDEX):
the wait as built is on a named world (the operation left, its receipts, its mesh's members, fabric ready), re-read every
reconcile pass, reconstructed by a successor's one claim; builds.get shows Pending and the HandedOff receipt; no new field or
frame; two cells owed to the lead (an unrelated mesh's change claims nothing; a successor claims once and waits).
Q253 amended by Luke (2026-10-28, "I hate timers; make this event driven"): the reconcile pass runs on events only, a view
install (`Records::view_tick`), a Build submitted or attempt opened, a fabric state change (a new watch); the 300 ms arm at
boot.rs:3289 goes; rdm#164 in lane 7 (seat i143-164), RED cells on the loop's wait, then the fast gate and one exported
R-shape flame counting passes per admin. The other timers (boot.rs:2288; the gossip cadences; investigate.rs's interval) are
each their own question, the gossip ones with a flame first.
rdm#164 landed c1ca20870 (Q253 amended, Q254 A): the reconcile loop runs a pass on an event only; six sources, every pass spanned
via-reconcile-woken; five causes the tick hid fixed at their roots (a submit lost during a pass, a peer-mesh decision, a drive's end,
a drift attempt's row release, an event during a running drive). Owed: the container provider's exit source; the view rebuild's
200 ms VIEW_POLL (projection.rs:637); a mesh-primary's own recovery's row release. rdm#169: demo/admin-ui red against main's rdm-client.
Q255 A (the release suite on main, its reds dispatched): the four red jobs of gates-1-5cdd6d360release landed, each its own root.
rdm#168 7acc0a04f (consumer connections cells take Q227's page: no active route; a restarted source holds no series); rdm#167
1fc25e8d0 (three fabric_takeover/proof-store cell premises, read from spans); rdm#166 d1edca7ca (the lost final primary's
Deleted row is written by its mesh's one writer: the fabric-primary's explicit delete ends at the exact Exited when, at the
write, it does not write that mesh's rows, and the new mesh-primary runs the delete on in the same attempt; R-OW1, mesh-delete.md
resume steps 1 and 3; the 20543a306 regression). In flight: rdm#170 (lane 6, a stopped node's connections reconciler ends at the
stop, states.md "Stopped nodes"); rdm#171 (lane 3, NodeRows::status takes the §4.3 writer check). Then the one-SHA tail.
rdm#170 855de1e42 (a stopped node's reconciler ends at its stop), rdm#171 0a6030abd (one writer predicate for a node row's
Deleted row and status, §4.3; mesh-delete step 7's writes decided by mesh-delete.md:52 and states.md:35), rdm#172 13230e8e6 (Q254 A:
the admin view rebuild waits on events only, VIEW_POLL gone; idle rebuilds 4.8/s per admin -> 0). Recorded, not changed: three
projection inputs with no local event ride the hierarchy turn's view install every gossip interval (boot.rs:3128), a gossip cadence
and its own question with a flame; the pruner loop is watch_meshes at 250 ms (topology.rs:2867), Q254's "5 s sweep" is the
staleness sweep's own cadence of marking, not the loop's. The tail at 855de1e42: fast GREEN, release 118 ok 0 FAIL; the export
tail runs again at the tip after rdm#172.
Q258 (Build notices and a member's self-heal) ruled A by the lead with arch-1, decided by R-BI7 and Q256 (INDEX): a mesh's
own recovery attempt reopens nothing and sends no notice; item G withdrawn; build.md:289 conforms to R-BI7's approved line, in
rdm#173 (lane 7, note 0005). Found: R-BI7's page text never landed on node-recovery.md:22, :72-80 and node-rpc-envelope.md:993,
rdm#174 with the lead's conforming text.
