# Playbook conformance review: the lifecycle pages against the code (Q89 follow-up)

Asked by Luke, 2026-10-20, after Q89 showed one page (the loss of the fabric-primary) built as a
scenario in six places instead of one sequence. The question: where else does the code run a page's
steps as a copy or a scenario branch instead of calling the page's workflow or primitive, and which
page lines no longer match the code. Baseline: [rdm-code-conformance.md](rdm-code-conformance.md) §0.1
(2026-10-11): every op is one workflow, a sequence of calls to primitives in its page's order; a workflow
never re-implements another's steps.

Four kinds of finding, kept apart:
(A) a workflow or primitive re-implemented elsewhere: a code change for dev;
(B) a gate narrower than the page, usually locked by a cell: a code change plus the cell rewritten;
(C) page text wrong or stale against the code: an exact old/new delta for Luke's yes;
(D) page text with no code behind it: Luke's call, build or strike.

Every (A) and (B) item below was read at the cited line by the lead, or is marked "locator-reported" where
it was not. Locator subagents only pointed at lines; a verdict rests on nothing reported on their word alone.

Open conformance stories, named once so the list below does not re-specify them: e12.s41 (#3068, the node
rows, name claims and config writes move from the fabric-primary to the mesh-primary) and e12.s42 (#3069,
the row-name view) are open in the PRD; e12.s64 (#3095) and e12.s68 (#3099) have no state recorded; e12.s57's
PRD row is the fabric_recovery cells' hand-over, not §2 item 4's "recovery = delete + create + update".

## 0. The §0.1 relations today

| relation (§0.1) | today | where |
|---|---|---|
| node.create calls node.start `FirstStart` | call | `admin/boot.rs:2857`, `rdm-node/src/node.rs:798` |
| a start after a stop calls node.start `AfterStop` | call (the plug is still named `rejoin`) | `admin/session.rs:356`, `rdm-node/src/node.rs:880` |
| node.restart = node.stop then node.start | call: `stop_steps_with(.., StopEnd::Park)` then the start | `deployment/pipeline.rs:1970..2000` |
| node.delete of a live birth calls node.stop | call: `stop_steps_with(.., StopEnd::Terminate)` | `deployment/pipeline.rs` `retire_steps` |
| mesh.delete deletes each member by node.delete | call: `shutdown_one` → `AdminRunner::delete_birth` | `admin/mesh_delete.rs:684..706` |
| node.create over an inherited dir deletes the proven-gone birth first | call: `delete_crashed` then `create_over`; the exit-proof put is inline | `admin/runner.rs:452..464` |
| recovery = node.delete + node.create | the Build's two steps, keyed `node.delete:<path>` and `node.create:<path>` | `build_run.rs::run_recovery` |

The §0.1 shape holds for the start, stop, restart, delete and mesh.delete workflows. It does not hold for
the three primitives below, nor for the seat workflows Q89 read.

## 1. Code changes for dev, by playbook

### 1.1 node.tickle (node-tickle.md): one ladder on the page, three in the code (A)

The page: one reach ladder on an exact birth, direct then both via-peer kicks at once, `reached` or
`unreached`, "run by recovery, investigation, the seat watch and the primary-update successor ladder",
with its spans `rdm.node.tickle.resolve.via-direct`, `via-peer`, `returns-unknown.via-unreached`.

The code has none of those three span names. It has three ladders:

| ladder | where | shape |
|---|---|---|
| the member ladder | assembled inline in the recovery tick, `admin/boot.rs:2120..2160` (`silent_set`, `kick_of`, `tickle.reprobe`) from the pieces in `offline.rs` (`via_peer_verdict` at 185, `tickle_round` at 122) | direct, then via two peers; spans `via-offline-tickle-*`, `via-peer-tickle*` |
| the seat watch's ladder | `seat_watch.rs:212..258` (`look`) and its own generic `ladder` at 322 | direct, via peers, then a provider rung the page does not give the tickle; spans of its own |
| the investigation's probe | `investigate.rs:775..800` per admin: direct, then its own Ping round and `Forward` through the member that ponged | a different via-peer policy (Ping a few, Forward through one) |

The page's fourth runner, the primary-update successor ladder, runs the seat watch's (`fabric_handover.rs:1110..1130`
matches on `seat_watch::Finding`), so node-primary-update.md line 75 ("runs node-recovery's tickle ladder")
names the wrong one.

Change: one `tickle(birth, view, client) -> Reached | Unreached` in `offline.rs` (the primitive module
`workflows/node_tickle.rs` re-exports it), emitting the page's three spans; the recovery tick, the seat
watch's `look` and `investigate.rs`'s probe call it. The provider rung leaves the tickle: it is
node.runtime.inspect (1.2), called by the watcher after an `unreached`. The investigation's "Ping a few
members then Forward" is either the page's via-peer kick or a page change; recommend the kick (two peers,
at once), since mesh-investigation.md step 1 already says "the established direct-then-via-peer policy".
Cells: the three runners' existing cells keep their contracts; one new unit cell on the primitive's spans.

### 1.2 node.runtime.inspect (node-runtime-inspect.md): one primitive on the page, three copies (A)

The page: the one exit proof, run by delete, recovery, investigation, mesh.delete and fabric.delete. Two
modules each name a different function as the primitive: `workflows/node_runtime_inspect.rs` re-exports
`seat_watch::inspect_birth`; `primitives/mod.rs` re-exports `admin::exit_proof_of` and `nodes_exit_proof`.

Three functions each do adopt → `provider.inspect` → `exit_proof`, with three result types:

| copy | where | returns |
|---|---|---|
| `inspect_birth` | `seat_watch.rs:271..305` | `Finding` (Answered / Running / OsStopped / Exited / Unproven) |
| `inspection_of` (wrapped as `exit_proof_of`) | `admin/rows_writer.rs:405..440`, `:444` | `Result<(code, source), NotProven>` |
| `prove_predecessor_exit` | `mesh_delete.rs:548..575` | `Result<TerminalReceipt, String>` |

Beside them the create fence inspects through its own `PathProbe` (`seat_revision.rs:88..100`), and
node.delete's `until_exited` loops on `provider.inspect` (`deployment/pipeline.rs`, `retire_steps` tail).

Change: one `inspect_birth(provider, fact, data_dir, incarnation) -> Inspected { Running | OsStopped |
Exited { code, source } | Unknown }` in `deployment/provider.rs` beside `exit_proof`; the three callers map
it to their own result types; `primitives/mod.rs` and `workflows/node_runtime_inspect.rs` name the same
function. No behaviour change; the cells that read `Finding` and `NotProven` stay.

### 1.3 node.delete's two rows written outside node.delete (A)

node-delete.md: the exit-proof row and the `Deleted` row are node.delete's (steps NodeDeleted, the
Deleted row). Writers outside the pipeline (`deployment/pipeline.rs:2146`, `:2317` are the pipeline's own):

| writer | where | why it is there |
|---|---|---|
| the takeover's release of the lost admin | `fabric_handover.rs:1519..1545` (`release_lost_admin`: `put_exit` with proof `takeover { revision }`, then `rows.deleted`) | Q89 item 2: runs only on the uninspectable arm |
| the handover's exit record of a successor found exited | `fabric_handover.rs:1126` | the seat move proving a successor's loss on its own |
| mesh.delete's roster row | `mesh_delete.rs:915..932` (`birth_row_deleted`), `:1723`, `:1737` | the member delete already ran `delete_birth`, which writes the row; this is a second writer for births the view no longer names |
| the create fence | `admin/runner.rs:461` (`put_exit` of the previous birth, then `delete_crashed`) | the proof put precedes the delete it hands to |
| the drift pass | `admin/reconcile.rs:141`, `:563` (`record_verdict_once`, `nodes_exit_proof`) | the reconciler proving exits in its own pass |

Change: the exit-proof row is written by one function (`Records::put_exit`, already) and only from
node.runtime.inspect's `Exited` (1.2); the `Deleted` row is written by node.delete alone. `birth_row_deleted`
goes unless a cell shows a roster birth the pipeline's `deleted_row` cannot reach; the handover's `:1126`
and the fence's `:461` put through 1.2's result. The takeover's release stays on Q89's uninspectable arm.

### 1.4 The seat workflows (B): Q89's six gates

Already specified, [i143-q89-fabric-primary-loss.md §5](i143-q89-fabric-primary-loss.md): `takeover_due`
and the seat watch's finding match, `take_over`'s successor and `release_lost_admin`, `named_fabric_successor`
and `fabric_seat`, `handback_due`, the view gate on the command paths, the investigation's decision step.
Not repeated here.

### 1.5 mesh.delete (mesh-delete.md): node.delete's steps re-run inside it (A)

The member delete itself is a call (`shutdown_one` → `delete_birth`, §0). Around it, `mesh_delete.rs` runs
node.delete's and node.runtime.inspect's steps on its own, read at the lines:

| step of another page | where | what it does |
|---|---|---|
| the exit-proof row | `mesh_delete.rs:1702..1708`, a local `put_exit` | builds a `NodeExitRow` by hand (`proof: "terminal-receipt"`, `observed_at_ms: 0`) and puts it on status storage, bypassing `Records::put_exit` |
| the exit proof | `mesh_delete.rs:548..575` `prove_predecessor_exit` | the third copy of adopt → inspect → `exit_proof` (1.2) |
| the `Deleted` row | `mesh_delete.rs:915..932` `birth_row_deleted`, run from `members_exited` at `:867` and `:874` and for every roster birth of an unanswered mesh at `:1770` | a second writer of node.delete's row; on the unanswered-mesh path no node.delete runs at all |
| `NodeDeleted` | `mesh_delete.rs:876` `lifecycle.deleted(op)` inside `members_exited` | node.delete's publish, made again by the caller |
| node.delete's step receipt | `mesh_delete.rs:465..475` (`record_predecessor_exit`) and `:1806..1818` | appends a `RetireStep::TerminateRuntime` receipt under `<outer>/<path>` from outside node.delete |

Change: the unanswered-mesh arm (`:1695..1800`) runs node.delete per roster birth as a crashed target
(`retire_steps` with `req.exit`, which already takes a proof of exactly that birth) instead of writing
rows and receipts itself; `members_exited` stops publishing `NodeDeleted` and writing `Deleted`, since
`delete_birth` did both; `prove_predecessor_exit` and the local `put_exit` go through 1.2 and
`Records::put_exit`. The page's step keys stay `node.delete:<path.name>`; the `<outer>/<path>` key the
predecessor receipts use (`:55..57`) goes with them.

Page against code (C): mesh-delete.md's telemetry table names ops and no span, while the code emits
41 `rdm.node_admin.mesh.update.via-*` and `mesh.create.via-*` spans for it (`via-shutdown`,
`via-leave-tier`, `via-handoff-sent`, `via-predecessor-exit-recorded`, ...); the page needs its span
table like the other pages. Code concepts the page does not name: `MeshLeft`, `OtherMembersExited`,
`LeavingRoster`, `AcceptedRoster`, the 120 s `MESH_DELETED_WAIT`.

### 1.6 The fabric rounds (fabric-state-sync.md, fabric-state-commit.md, fabric-ready.md): one round on the pages, two drivers and three duplicated pieces in the code (A)

The climb itself is one sequence, `fabric_rounds.rs` (`step_pending` at 457, the round turn at 613, the
mesh driver at 824), and the page steps map onto it one to one; the locator found no copy of the climb.
Around it, read at the lines:

| piece | copy 1 | copy 2 |
|---|---|---|
| the round-hook publication (the frame built, the one span `rdm.node_admin.fabric.update.via-round-hook`) | `fabric_rounds.rs:259..277` `publish_hook`, with `fabric_state::hook_frame` | `rdm-node/src/node_self.rs:801..816` `check_in`, with `rounds::hook_frame` |
| "act once, then check in" (locator-reported, not read by the lead) | `fabric_rounds.rs:892..906` (`acted`) | `rdm-node/src/node_self.rs:728..771` (`rounds_seen`); both emit `via-round-action-failed` |
| the delivery rule R-S2 (`Sent` and `eligible()`) | `fabric_state.rs:20..40` | `status_declare.rs:114..134`, identical |
| a second round driver | `round.rs:235` `RoundDriver::step`, R-S3 "the round a new primary runs before it republishes": its own checklists, its own down op (`ProbeNodeState`, `send_down` at 319), run from `admin/boot.rs:2614..2624` when an adopted status awaits a round | `fabric_rounds.rs` runs the climb's rounds with their own books; both emit `rdm.node_admin.fabric.update.via-round-complete` (`round.rs:299`, `fabric_rounds.rs:622`) |

Change: one `hook_frame` and one `publish_round_hook` in `rdm-node` (the admin is a node kind; its
rounds plug in); one `Sent`/`eligible` in `rdm-node::rounds`, re-exported by `status_declare`; the
new primary's re-round (R-S3) runs as a `FabricRounds` round with the adopted key and a fresh checklist
instead of `RoundDriver`, so one driver owns every checklist, with Q87's rule kept: the round's down op
is a read. states.md "The round checklist" is the page for R-S3 and names no second driver.

Role branches read and judged necessary (the page names them): `fabric_rounds.rs:286` (not the
fabric-primary: serve the heard state), `:1005` (the fabric-primary refuses the down op), `:1008`
(mesh-primary or member), `:1050`/`:1054` (which check-ins this admin counts), `admin/boot.rs:2530`,
`:2604`. One to read against the page: `status_declare.rs:628..635`, the fabric-primary's
`ApplyFabricEvent` fan-out to every mesh primary at first ready, which fabric-ready.md does
not mention (C below).

### 1.7 node.create, mesh.create, node.replace, node.name.update, node.config.update: conformant, with the open story named

- **node.create** runs one pipeline (`deployment/pipeline.rs:1360` → `create_steps` → `create_to_ready_claimed` at 1528: AllocateIdentity, the name claim inside PrepareStorage at 1611..1627, DeployRuntime at 1693, WaitForBind, WaitForMeshJoin, WaitForNodeReady). node.start `FirstStart` runs in the launched process (`admin/boot.rs:2857`, `rdm-node/src/node.rs:798`), which is what "composes" can mean across a process boundary; the page wording is tightened below (C).
- **node.create step 5 reads gossip where the page says it never does (B).** node-create.md step 5: WaitForBind and WaitForMeshJoin complete "from the validated report, never from gossip". The pipeline's `WaitForMeshJoin` (`deployment/pipeline.rs:1828..1836`) polls `observer.joined`, and `MembershipObserver::joined` (`admin/session.rs:177..182`) reads the digest book (`self.book.get(node_id)`) and compares the runtime fact the heartbeat carries. The join door already holds the validated report (`join.rs:177..194`, `Joins`); the wait completes from it. Change: `WaitForMeshJoin` reads the join door's record of the exact birth, the book read goes; the create cells keep their contracts.
- **mesh.create** is node.create's mesh-absent arm as the page says (`accepted.rs:747..759` emits `CreateMesh`; `rows_writer.rs:271` `create_mesh_step`; `ApplyMeshState(Pending)` at `runner.rs:1177..1240`). The fabric-primary writing the new mesh's row at `CreateMesh` (`rows_writer.rs:229`) is the open story e12.s41, not a new finding.
- **node.replace**: RDM's own replace path is gone as rdm-code-conformance.md §2 item 3 required: `ReplaceNode`, `RenamePredecessor`, `HandoffStorage`, a drift `Replace` arm have no definition or use in `crates/`; the recipe's successor claim exists (`pipeline.rs:1375`, `runner.rs:1097`, `node_rows.rs:626`). Only the test harness keeps an `Action::Replace` (`rdm-test/src/model.rs`, `soak.rs`).
- **node.name.update**: one door, `node_name_update.rs:66..236`; the precondition `admission_of` (88..115) refuses a parked birth, no `node.connections.deleted` of `All`, held connections, not left: the page's "ready is refused" holds through "not left". `NodeRows::rename_row` (`node_rows.rs:395..418`) has no caller: dead, remove.
- **node.config.update**: one door, `node_config.rs:66..180`; the other row puts carry config unchanged.

### 1.8 node.stop, node.connections.delete, node.mesh.leave, node.restart: one sequence on the command path, two paths around it (A, B)

The sequence is one function, `rdm-node/src/node_self.rs::stop_steps` (1162): `node.connections.delete { All }`
(the primitive `delete_connections_asked` at 462, called at 1199), the tasks parked, the Leaving mesh topology
(1230..1255), the transport cut `leave_mesh` (979, called at 1264), the owed Stopped (1277). Every commanded
stop reaches it: the mesh-primary's `StopNode` on an rpc node (`node.rs:1358`) and on an admin
(`status_rpc.rs:1593`), the primary as its own subject in process (`status_rpc.rs:1031`), the restart's stop
half and node.delete's stop (`pipeline.rs:2000`, `:2245`, one `StopNode` each), mesh.delete's member delete
through node.delete. The rpc node's signal reaches it too (`node.rs:165..174` → `stop_on_signal` → `stop_steps(StopBy::Signal)`).

Two paths do not:

| path | where | what it skips |
|---|---|---|
| **a node-admin's own signal (B)** | `entry.rs:120..131` → `admin/boot.rs:134` `leave` / `:143` `leave_after_stop` → `:148` `leave_with`: aborts the executor, hierarchy, anti-entropy and publisher tasks, settles open calls, shuts the router down; its doc says "Nothing is announced" | no `node.connections.delete { All }`, no `RDM_CONNECTIONS_DELETE_DEADLINE_MS` bound, no `leave_mesh`, no owed Stopped: the admin kind has a copy of "stop on a signal" that is not the stop. node_self.rs's own words (`StopBy::Signal`, i146 Q55): "One workflow serves both" |
| **a commanded start that fails after its hook re-parks the birth (A)** | `node_self.rs:1400..1412`: `delete_connections_asked(All)`, `park_tasks`, `leave_mesh`, called one by one | no `StopConnectionsDeleted`, no owed Stopped, no Leaving mesh topology: the stop's primitives re-sequenced by hand, as the comment says ("through the stop's own primitives") |

Two more copies of the channel leave, read: `admin/session.rs:539..550` `AdminLeave::run` parks membership
and backbone inline instead of `leave_channels`; `node_self.rs:997..1021`, the `session = None` arm of
`leave_mesh`, parks inline. The locator also reports `rdm-transport/src/connections.rs:733`
`ConnectionsRegistry::delete_connections` with its own block and drop recording and no production caller;
not read by the lead, a dead-code candidate to confirm at landing.

Change: a node-admin's signal runs `stop_steps(StopBy::Signal)` as the rpc node's does, then `leave_with`
(the task abort and the router shutdown are the process end after the stop, not the stop); the failed
start's re-park calls `stop_steps` with a `StopBy` naming the failed command, so the receipt, the owed
Stopped and the mesh topology are the stop's; `AdminLeave::run` and the `None` arm call `leave_channels`.
Cells: the signal cell on an admin (today's `the_provider_sends_no_signal_but_the_stop_ladder_and_the_terminate`
is the provider side; a RED cell for an admin SIGTERM asserting the connections-delete receipt, the Leaving
mesh topology and the owed Stopped), and the commanded-start-failed cell asserting the stop's frames.

### 1.9 node.mesh.join, node.start, node.status, node.topology.get: one start, one join, and member status written in twenty places (A, B)

- **node.start and node.mesh.join are one each.** `node_start(` has four callers, the two kinds' first start and start after a stop (`rdm-node/src/node.rs:798`, `:880`, `admin/boot.rs:2857`, `admin/session.rs:356`); `node_mesh_join(` has one, node.start step 2 (`node_start.rs:308`). The standalone commanded join reaches it through `rejoin` (`node_self.rs:1075`). Conformant.
- **A join call outside the join (A, small).** `admin/session.rs:807..815` `entry_repeat`: `call_join`, adopt rafka-time, `take_entry_control`, no channel step, no hook: `JoinNode` used as the entry pull. rdm-code-conformance.md §2 item 25 moves the entry pull to node.topology.get; this is the RDM-side remnant.
- **The node publishes its own Pending (B).** node-mesh-join.md lines 67 to 68 and 256: the Pending publish is the mesh-primary's, "never from the node"; node-primary-update.md step 3: the mesh-primary is "the sole author of member status". `rdm-node/src/node.rs:978..986` and `admin/session.rs:486..488`: after `wait_for_neighbour` the node sets its digest Pending and publishes it itself. The mesh-primary persists Pending on the `NodeJoined` frame (`status_rpc.rs:1080..1084`, `:953..956`). Change: the node's self-publish of Pending goes; the `NodeJoined` frame is the report and the mesh-primary's publish is the one Pending, as the page says. Cell: the heard-again cell already asserts the order and the publish (the Q87 follow-up); it gains "and no Pending digest authored by the node". (2026-10-21, Luke's reset of Q90, rulings d45f90e1: the start's `wait_for_neighbour` is removed from both start paths, with its lone-node exception; the start finishes on its direct calls and the node is back in gossip whenever gossip gets there; node-mesh-join.md step 9 is "`node.mesh.joined`. The node's first `Digest` heartbeat follows.")
- **Member status is set in about twenty places (A).** `lifecycle.rs`'s own doc: "Node, Mesh and Fabric status changes are explicit transitions with registered hooks, never incidental field writes"; `LifecycleTransitionPipeline::transition` has one caller, `admin/runner.rs:418`. Beside it, located by the subagent, three of them read by the lead (`status_rpc.rs:1080..1084`, `rdm-node/src/node.rs:978..986`, `admin/session.rs:486..488`), the rest locator-reported: `status_rpc.rs:1074` (Leaving on `CommandAccepted`), `:1080..1084` (Pending on `NodeJoined`), `:1094..1097` (Ready on `NodeStarted`), `:925..947` (`begin_cycle`), `:953..956`, `:1397..1400` (Stopped), `:264..290` (`SeatFold::adopt_status`); `membership.rs:4818..4848` (`started`), `:4988..5030` (`joined`), `:5040..5056`, `:5066..5080`, `:5107..5131`, `:5281..5292` (`hand_out` overrides own-mesh status on reads), `:5703..5712` (a no-status join digest set Pending at the primary); the nodes' own sets at `node.rs:646`, `:1158`, `:1166`, `node_self.rs:993`, `:1062`, `:1240`, `:1381`, `session.rs:595`, `boot.rs:1786..1789`; the projection's marks at `admin/projection.rs:333..491`. This is where H1 of the review lived (no Pending for a commanded birth) and where the next one will. Change: every member-status write at the authority goes through one `MemberStatus` transition (the pipeline `lifecycle.rs` already defines, or one function in `status_rpc.rs` that it calls), with the frame or declaration that caused it as the receipt; the node's own digest status is set in one place, the start's steps. The projection's observer marks (`PendingReconnect`, `Dead`, `Restarting`, `Leaving`) stay reads.
- **node.topology.get: the pages disagree with each other.** node-topology-get.md lines 7 to 8 and 61 to 62: the read carries the held source version and goes to the node's own mesh-primary. node-start.md step 4 and `node_start.rs:435`: the start's read goes to the authority that answered the join with `since = None`, and at a first start that is the launcher. One of the two pages yields (C below).

### 1.10 The certificate pages, node.runtime.inspect's callers, node.connections.admit, states.md

- **node.cert.renew, node.issuer.update, mesh.issuer.renew** map step for step onto `cert_renewal.rs` (the tick at `admin/boot.rs:2394..2403`, `renew_members` 199..227, `renew_one` 240..302, `renew_issuers` 330..345, `update_one` 407..472) and `certs.rs`; the policy is read once by the bootstrap admin and carried in the join answer (`records.rs:186..189`, `boot.rs:954..955`). Conformant. One dead policy field: `RDM_CA_CERT_TTL_SECS` is parsed into `ca_ttl_secs` (`rdm-core/src/certs.rs:466`) and read nowhere; the CA mint uses the constant `CA_CERT_TTL_SECS` (`certs.rs:144`).
- **fabric.ca.renew, fabric.root.update, node.roots.update have no code (D, §3).** The only CA mint is day 0 (`certs.rs:144`); `StatusRequest` has `RenewCert` and `UpdateIssuer` only (`rdm-core/src/status.rs:492`, `:504`); `RootSet::replace` is called at boot alone (`rdm-transport/src/connections.rs:573`).
- **node.runtime.inspect's callers (adds to 1.2).** Own inspection read by the lead at `admin/runner.rs:602` (`delete_crashed`, on a proof already in hand); locator-reported at `deployment/pipeline.rs:1541`, `:1785`, `:1836`, `:1875` (the create's waits watching their launched process), `:2324`, `:2442`, `:2511`. The span `rdm.node_admin.node.resolve.via-provider-inspection` is emitted at `recovery.rs:578` and `pipeline.rs:2519` (read), both without the `handle` field the page requires (node-runtime-inspect.md lines 33, 109). `unavailable { reason }` (page line 30) is `DeploymentStatus::Unknown` with no payload (`provider.rs:313`, locator-reported).
- **node.connections.admit** is one path (`admit.rs:93..105` verify, `:172..227` admit, mounted per registered ALPN at `connections.rs:838..842`). Two things to read against the page: a `Tunnel { via: None }` splice has the carrier present its own cert to the destination (`connections.rs:1074..1078`, locator-reported, not read by the lead), so the origin's cert is verified on the first leg only; and the span `via-refused` hardcodes `cert_expires_at = 0u64` and `clock = "no-certs"` (`admit.rs:253..254`, read), `via-presented` the same at `connections.rs:1085`, `:1538` (locator-reported), which makes the evidence wrong where it matters. The second is a dev item: record the real fields.
- **states.md against the enums (C, §2).** Mesh `deleting`/`deleted` are `Leaving`/`Dead` in code (`rdm-core/src/model.rs:60`, `:62`); node `starting`, `stopping`, `deleting`, `connections-deleted` have no variant (`stopped` is `status.rs:56` only); `degraded` is a `ScopeStatus` variant (`model.rs:64`) where the page says it is not a lifecycle status; `Leaving`, `Restarting`, `ScopeStatus::Dead` and `FabricEvent::ShutdownInitiated` are in code with no page entry.



## 2. Page deltas for Luke's yes (C)

Mechanical checks over all 32 lifecycle pages (the lane at 282217ab): every span name a page cites was
grepped in the code; every link and anchor was resolved.

| page | finding | proposed delta |
|---|---|---|
| node-tickle.md | its three span names exist nowhere in the code (`rdm.node.tickle.resolve.via-direct`, `via-peer`, `rdm.node.tickle.returns-unknown.via-unreached`); the code emits `rdm.node_admin.node.resolve.via-offline-tickle-*`, `via-peer-tickle*`, `rdm.node_admin.mesh.update.via-probe` | no page change: 1.1 lands the page's spans |
| node-primary-update.md line 75 | "the workflow runs node-recovery's tickle ladder on the successor's exact birth" | after 1.1: "runs node.tickle on the successor's exact birth" |
| node-recovery.md step 2 | the seat watch's timing is now stated (Q89); the member ladder's "half a floor" stands | none beyond Q89's |
| fabric-recovery.md | cites `rdm.node_admin.build.update.via-claim-request`; the code has `via-claim-decision` only | OLD `rdm.node_admin.build.update.via-claim-request`, `rdm.node_admin.build.update.via-claim-decision` NEW `rdm.node_admin.build.update.via-claim-decision` |
| mesh-create.md | cites `rdm.node_admin.build.update.via-accepted`; no such span | OLD `rdm.node_admin.build.update.via-accepted` NEW the span the code emits at acceptance (dev to name it from `build_op.rs`; candidate `via-claim-decision`) |
| mesh-investigation.md | cites `rdm.node_admin.mesh.create.via-rebirth`; the code has `rdm.node_admin.mesh.create.via-recovery` | OLD `via-rebirth` NEW `via-recovery` |
| fabric-state-sync.md, fabric-state-commit.md, fabric-ready.md | none of the three has a span table; the code emits `via-state-climb`, `via-state-entered`, `via-blocked`, `via-state-adopted`, `via-state-reset`, `via-sync-state-hook`, `via-round-opened`, `via-round-command`, `via-mesh-round`, `via-round-complete`, `via-round-action`, `via-member-round`, `via-ready-row-put`, `via-traffic-opened-hook`, `via-first-ready` and more | add the span table to each page from the code's names (dev lists them; Luke's yes on the list) |
| fabric-ready.md | the code puts the ready row before entering the state (`fabric_rounds.rs:633`) and fans `ApplyFabricEvent` out to every mesh primary at first ready (`status_declare.rs:628..635`); the page says neither | one sentence each, or the code drops the fan-out; Luke's call |
| fabric-create.md lines 25 to 26 | names the ops `fabric.root.create` and `fabric.ca.create`; the code's spans are `rdm.node_admin.cert.create.via-day-zero-root` and `via-ca-create`, the op names only in log text | the page keeps the op names and gains the two span names |
| fabric-create.md lines 32 to 33 | "each mesh's primary creates its rows"; the code has the fabric-primary create the other meshes' rows at `CreateMesh` (`admin/rows_writer.rs:229`, `:151..156`) | no page change: this is the open story e12.s41 (rows move to the mesh-primary), rdm-code-conformance.md §2 item 5 |
| node-create.md lines 5 to 6 | "composes the whole node.start workflow with start_kind = FirstStart"; the pipeline launches the process and waits (WaitForBind, WaitForMeshJoin, WaitForNodeReady), and the process's first start runs node.start | OLD "creates a logical node and composes the whole [node.start] workflow with `start_kind = FirstStart`" NEW "creates a logical node, launches its process and waits for the [node.start] workflow its first start runs with `start_kind = FirstStart`" |
| mesh-create.md line 37 | "at the mesh's acceptance the fabric-primary mints `mesh2`'s issuing cert"; the code mints it in the first admin's node.create step (`admin/runner.rs:292..296`, from `:481`) | OLD "at the mesh's acceptance" NEW "in the node.create of the mesh's first node-admin" |
| mesh-create.md line 45 | "within 20 s of the launch"; the 20 s runs from the first Ping (`admin/runner.rs:1202`) | OLD "within 20 s of the launch" NEW "within 20 s of its first Ping" |
| node-stop.md line 43 | "The node gossips nothing for this step"; the stop publishes its Leaving mesh topology before the cut (`node_self.rs:1230..1255`, rdm-code-conformance.md §2 item 21, gossip.md "the last mesh topology carries itself Leaving") | OLD "The node gossips nothing for this step." NEW "A mesh-primary publishes its last mesh topology, carrying itself Leaving, before the cut; the node gossips nothing after it." |
| node-mesh-leave.md lines 79 to 81 | "`node.mesh.left` is the frame before `node.stopped`"; `NodeCommandReply` has no such variant (`rdm-core/src/node_command.rs:112..195`: CommandAccepted … StopConnectionsDeleted, NodeStopped, NodeJoined, NodeStarted, Failed) | OLD "`node.mesh.left` is the frame before `node.stopped`" NEW "the leave's completion is carried by `node.stopped`; no frame of its own is sent inside a stop" (or dev adds the frame; Luke's call) |
| node-connections-delete.md lines 122, 181 | "the mesh-primary sets connections-deleted"; `MemberStatus` has `ConnectionsDeleting` only (`rdm-core/src/digest.rs:40`); the primary stores the receipt (`status_rpc.rs:1236..1244`) | OLD "sets connections-deleted" NEW "records the `node.connections.deleted` receipt; the member status stays `connections-deleting` until the stop or start that follows" |
| node-stop.md, node-connections-delete.md, node-restart.md | no span table; the code emits `rdm.node_admin.node.connections.delete.via-stop`, `via-stop-quiesce`, `via-leaving-aggregate`, `via-stop-transport-cut`, `rdm.node_admin.status.update.via-stop-node`, `rdm.node_rpc.status.update.via-owed-on-stop`, `via-owed-on-signal`, `via-connections-deleting`, `via-delete-connections`, `rdm.node_admin.connection.delete.via-connections-delete`, `via-refused-drop`, `via-restart-resumed`, `rdm.mesh.node.delete.via-signal` | add the span table to each page from the code's names |
| node-mesh-join.md lines 52 to 56 | the member cert "is issued on a first start only"; `join.rs:317..318` issues it on every `Standing::Deployed`, which Q87 requires for a re-entry | OLD "on a first start only" NEW "on every join of the exact birth, a first start and a commanded re-entry alike (Q87)" |
| node-mesh-join.md lines 48 to 49 | "the node reports to the authority named in `NotAuthority`"; `rdm-node/src/join.rs:80` maps `NotAuthority` to `Refused` and the join ends | either the node re-reports (dev) or OLD "reports to the authority it names" NEW "is refused by name; the next attempt of the commanding workflow names the authority" (Luke's call) |
| node-mesh-join.md line 57 | the join answer's snapshot lists "shutdown if any"; `JoinControl` (`rdm-node/src/wire.rs:30..51`) has no such field since fabric.delete replaced the shutdown | OLD ", shutdown if any" NEW "" |
| node-start.md lines 47 to 48 | "no separate status report follows node.started"; the node declares Ready as an owed declaration (`node.rs:1158..1169`, `status_declare.rs:588..600`, Q36's owed declarations) | OLD "no separate status report follows" NEW "its Ready is declared as an owed declaration, delivered until the authority acknowledges it (Q36)" |
| node-topology-get.md lines 7 to 8, 61 to 62 against node-start.md step 4 | the read "goes to its own mesh-primary" with its held version; a start's read goes to the authority that answered the join with no version (`node_start.rs:435`) | OLD "goes to its own mesh-primary" NEW "goes to the authority that answered the node's join, its mesh-primary once it is on its mesh"; and "supplies its held source version" gains ", none at a start" |
| states.md lines 12 to 13 and the table | mesh `deleting`/`deleted` are `Leaving`/`Dead` in code; node `starting`, `stopping`, `deleting`, `connections-deleted` have no code state; `degraded` is a `ScopeStatus` variant | one table from dev of page name to code variant; Luke rules which side renames (the vocabulary page is the contract, so the enums take the page's names unless a name is retired) |
| node-runtime-inspect.md lines 30, 33, 109 | `unavailable { reason }` is `Unknown` with no payload; the span lacks `handle` | dev records `handle` and the fields; the page keeps its words |
| node-replace.md | link to `node-identity-and-state.md#22-the-node-pathname-is-what-placement-stores--the-endpoint_id-is-resolved-at-dial-time` resolves to no heading | re-point to that page's current §2.2 anchor |

## 3. Page text with no code behind it (D)

| page | text | code |
|---|---|---|
| fabric-ca-renew.md | `fabric.root.update` (the caller op), `node.roots.update` (the primitive), spans `rdm.fabric.root.update.via-caller`, `rdm.node.roots.update.via-installed`, `rdm.fabric.ca.update.via-renewal` | no op, no handler, no span; `node.roots.update` is named only in three comments (`rdm-node/src/node.rs:454`, `rdm-transport/src/connections.rs:459`, `:577`, `connections/admit.rs:47`) as the thing that would replace the root set |
| node-tickle.md | the primitive's own spans | see 1.1 |
| fabric-state-sync.md lines 12 to 22 | "Every fabric runs day 0, by script or by attaching its saved WAL image", "passes through day0-complete and seed-complete", and the action names of lines 17 to 22 | `FabricState` has four values (`fabric_state.rs:50..60`); none of the action names occurs in the code |
| mesh-create.md lines 57 to 58 | "the fabric-primary ... applies `ApplyMeshState(Ready)`" | the only `ApplyMeshState` built is Pending at create (`admin/runner.rs:1193`); the mesh's ready reaches the fabric-primary as the mesh-primary's `DeclareMeshState` (`status_declare.rs:609..612`) and nothing is applied down |
| node-name-update.md line 16 | "No new connection is admitted from the row write through `ApplyNodeName`" | nothing in `node_name_update.rs` blocks admission; `AdmissionBlocked` exists only in `rdm-transport/src/client.rs:374`, `:604` |
| node-mesh-leave.md line 110 | "A mesh that cannot be left is reported by name with which one, the primitive stays `leaving`, and the workflow's next attempt resumes it" | `leave_mesh` (`node_self.rs:979..1031`) cannot fail; `left` is derived from whether a backbone exists (`:1023..1026`), not from what left |
| fabric-ca-renew.md, `fabric.ca.renew` steps 1 to 2 and the policy `RDM_CA_CERT_TTL_SECS` | the CA rotated internally at `RDM_CERT_RENEW_AT` under the same root | no code beyond the day-0 mint; the TTL policy field is parsed and never read |
| mesh-issuer-renew.md lines 143, 145, 181 | step keys `mesh.issuer.renew:<mesh>`, "complete when every admin in the view has one", `Failed { step: node.issuer.update:<path.name> }` | no Build keys, no completion; a refusal records `outcome = refused` on the span only (`cert_renewal.rs:445..447`) |
| node-mesh-join.md lines 27, 64 to 65, 262 to 264 | "a mesh with no live listener is created again by this first listener"; "the next attempt resumes from the accepted report" | no code found by the locator; not read by the lead |
| fabric-create.md lines 5, 15, 28, 20/40/52 | `fabric.create(spec)` as an entry, the spec naming each mesh and its first node-admin, "node.create for each mesh's first node-admin", `fabric.created` | no entry function; the Day-0 Build is the own mesh with one node-admin (`admin/boot.rs:969..982`), other meshes come by a later Build; `fabric.created` is emitted nowhere |

## 4. Coverage

All 32 lifecycle pages were touched; two only in part. Read against the code by the lead in full: node-tickle,
node-runtime-inspect, node-recovery (step 2), node-primary-update, node-delete (the row writers),
mesh-investigation (the fabric-seat arm), elections' seat rules. fabric-recovery was read for its Q89 arm
only: its relaunch steps 1 to 4 (the GetTopology-first read, the own-mesh sweep, restoring the secondary)
and the façade's `PeerRecovery` / `restore_peer_recovery` were not swept. fabric-delete came only through
the mesh.delete locator, which reported one gate on it (`admin/mesh_delete.rs:110`); its own steps were
not swept. Located by one short
subagent each and verified by the lead at every line that appears in §1: mesh-delete and fabric-delete;
the three fabric round pages and fabric-create; node-stop, node-connections-delete, node-mesh-leave,
node-restart; node-create, mesh-create, node-replace, node-name-update, node-config-update;
node-mesh-join, node-start, node-status, node-topology-get; node-cert-renew, node-issuer-update,
mesh-issuer-renew, fabric-ca-renew, node-connections-admit, states. Lines a locator reported that the
lead did not read are marked so in the text (two: the transport's `delete_connections`, the join page's
listener and resume sentences). The mechanical checks (span names, links) ran over all 32.

Not done here, by design: no page was edited; every (C) row is for Luke's yes; no cell was written.
