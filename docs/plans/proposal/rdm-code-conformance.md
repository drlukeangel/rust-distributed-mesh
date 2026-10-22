# RDM code conformance: what changes so the code matches the architecture docs

**Proposal.** It is the code side of the 2026-10-11 architecture-doc pass: the docs under
`docs/architecture/` state the ruled design (start at
[the lifecycle index](../../architecture/fabric-mesh-node-lifecycles.md)); this page lists where
`/home/admin/rust-distributed-mesh` (RDM, audited at `a207a281`, landings since in §0) does not match
them yet, what to change, and the open proposals that change the plan (§5). Evidence, file by file with
symbols: [rdm-code-conformance-findings.md](rdm-code-conformance-findings.md).

Rules that bind every change below:
- **Gossip and the mesh change only on Luke's yes, with a Jaeger flame** (CLAUDE.md rule 1). Every
  item marked **[gossip]** is a proposal for that yes, not a change to start.
- Durable renames go in their [ops-naming rollout step](ops-naming-proposal.md) (now in the i143 PRD
  §13.3): step 0 REST out of RDM; step 1 typed objects + Build 0x20; step 2 Build step keys
  (`durable format moved`); step 3 the 0x1B split (wire); step 4 gossip frame names (Luke's go-ahead).
- Story ids are the i143 PRD's. "new story" means no row covers it yet.
- The code-only renames in §1 land as one landing after s40, s41 and s49 land (those seats work in
  `mesh_leave.rs`, `admin/leave.rs` and the leave tests); rows marked with a rollout step land in that step.

## 0. RDM state since the audit (2026-10-11)

| landed on RDM main | what | story |
|---|---|---|
| `65122557`, `62f306d4` | QUIC handshake diagnostics on both ends, evidence runs only (R-QH1); the plb cell's setup race | e12.s46 #3074 |
| `3693defb`, `b2ff768d` | a node's own connections end follows its held name (R-CN1); the rename precondition R-CR1a / R-CR2 / R-CR3 (§2 item 19) | e12.s47 #3075 |
| `a207a281`, `e8e86195` | one routing rule; a stop quiesces the stop gate before its final aggregate and cut (§2 item 21) | e12.s45 #3073 |
| `d5d09cf8`, `bd201c0c`, `4925e59a` | the offline tickle's window; the R-D1 fence on elapsed t; the recovery loop on the gossip-interval grid | e12.s23 #3016 |
| `53bfee29`, `806df1bc` | admission on the natural key (R-DK1): commands, rounds, mesh-delete; the drain bound | e12.s48 #3076 |
| `cb2596ac` | the last admin publishes MeshStatus Dead, held by mesh id (R-NS1, R-MD1) | e12.s40 #3067 |
| `062bc85b` | rafka-docgen: the generated wire, frames, statuses, spans and config pages | e12.s50 #3079 |
| `f1fd416b` | timeout-strike pool eviction deleted (§2 item 12) | e12.s51 #3081 |
| `f7d850d7` | node.delete keeps storage (§2 item 9) | e12.s61 #3092 |
| `8ceb6b3d` | restart is one operation (§2 item 10); the admin boot restart mode stays until Luke's doc answer | e12.s62 #3093 |
| `b46b9e3a` | no sender-identity gates inside the mesh (§2 item 11) | e12.s63 #3094 |
| `67532dbe` | peer-mesh probe order (§2 item 16) | e12.s66 #3097 |
| `aa16abaa` | one node.start workflow for the four starts (§2 item 14 part 1; the order per node-start.md is part 2) | e12.s65 #3096 |
| `207172f5` | node.get, mesh.get, fabric.get, node.connections.get as Status calls (§2 item 8 part 1) | e12.s60 #3091 |

| in flight | what | story |
|---|---|---|
| `i143-s41-land` | rows/claims/config by the mesh-primary; lost set from durable exit-proof rows; NodeDeleting closes at its attempt's end; ensure-Dead AlreadyApplied | e12.s41 #3068 |
| `i143-s42-row-name-view` | node-row id index; the view reads a renamed birth by id, before the view lock (lands after s41) | e12.s42 #3069 |
| `i143-s57-recovery-steps` | recovery = node.delete + node.create + node.update (§2 item 4) | e12.s57 #3088 |
| `i143-s64-store-at-commit` | the standalone store written at state-commit; one writer per key (§2 item 13) | e12.s64 #3095 |
| `i143-s68-typed-refusals` | typed refusals for an epoch overflow and a stale mesh-delete round (§2 item 20) | e12.s68 #3099 |

## 0.1 Code shape: workflows call primitives (Luke 2026-10-11; #3096 node_start, #3106 the rest)

Every op is ONE workflow, and a workflow is a sequence of calls to primitives, in its lifecycle page's
order. A primitive does one thing (join, the `hydrate_before_ready` hook, topology get, the floor,
rafka-time, `node.connections.delete` (R-CR4: it is the drain; `All` or a list of ids, connections only
per R-CR5), the transport cut (gossip neighbours and the Node RPC pool), the exit proof, a row
claim/put/tombstone, a status apply, publish NodeStopped). A workflow never re-implements another workflow's steps: it calls that workflow
(node.create calls node.start; node.stop calls `node.connections.delete { All }`, then the transport
cut, then reports stopped; node.delete calls node.stop for a live node; node.restart calls node.stop and
node.start; node recovery, replace and mesh.delete call node.delete). A node kind (node-admin, rpc node, Rafka roles) differs only by what it
plugs into a primitive (a trait), never by a copy of the sequence.

What not to do — the start sequence as the audit found it, four copies each stepping join → hook →
topology → floor → rafka-time on its own (one `node_start` workflow replaced them in aa16abaa; its order
per node-start.md is #3096 part 2):

| caller | where | start kind |
|---|---|---|
| node-admin, first start (create) | `admin.rs` `start_booted` | FirstStart |
| node-admin, start after stop | `admin.rs` `AdminSession::rejoin` | AfterStop |
| rpc node (testkit), first start | testkit `node.rs` `start_booted` | FirstStart |
| rpc node (testkit), start after stop | testkit `node.rs` rejoin path | AfterStop |

Target: one `node_start` workflow taking `StartKind { FirstStart, AfterStop }`, calling the primitives
once; node.create and node.start (on a stopped node) call it; both node kinds plug in. The same shape
applies wherever a sequence is copied today — the leave's member stop (`shutdown_member` drains and
stops instead of calling node.delete, §2 item 1), restart's stop half receipted as `retire-node:`
(§2 item 10), recovery and replace each running their own delete steps (§2 items 3, 4).

The code follows the lifecycle index ([fabric-mesh-node-lifecycles.md](../../architecture/fabric-mesh-node-lifecycles.md)):
each op page under `docs/architecture/lifecycles/` is one workflow module, named after its page, that calls primitives in the
page's order.

| lifecycle page | in the code |
|---|---|
| fabric-create, fabric-delete, fabric-primary-update, fabric-recovery | workflow modules `fabric_create`, `fabric_delete`, `fabric_primary_update`, `fabric_recovery` |
| fabric-state-sync, fabric-state-commit, fabric-ready-for-traffic | workflow modules `fabric_state_sync`, `fabric_state_commit`, `fabric_ready_for_traffic` (the fabric rounds) |
| mesh-delete, mesh-investigation, mesh-recovery | workflow modules `mesh_delete`, `mesh_investigation`, `mesh_recovery` |
| node-create, node-start, node-stop, node-restart, node-delete, node-update, node-recovery | workflow modules `node_create`, `node_start`, `node_stop`, `node_restart`, `node_delete`, `node_update`, `node_recovery` |
| node-replace | no RDM module: replace is Rafka's recipe over node.create / node.stop / node.update / node.start / node.delete |
| node-connections-delete (R-CR4; the page node-drain.md becomes) | primitive `node.connections.delete` (`All` or a list of ids, connections only, R-CR5) |
| node-topology-get, node-status | primitives: the topology read, and the status report/apply |
| states | no module: the states and who sets them, which the workflows apply |

Other primitives with no page of their own: join, the `hydrate_before_ready` hook, the Build floor, rafka-time, the transport cut (gossip
neighbours and the Node RPC pool), the exit proof, a row claim/put/tombstone, and publishing NodeStopped.
Suggested layout: `src/workflows/<page>.rs` and `src/primitives/*`, folded into the §1 rename landing.

## 1. Renames (names follow the op; code-only #3102, step keys #3103, frame deletions #3104, statuses #3084, the 0x1B split #3117)

| old | new | step |
|---|---|---|
| `src/mesh_leave.rs`, `mod mesh_leave`; `src/admin/leave.rs`, `LeaveDoor` | `src/mesh_delete.rs`, `mod mesh_delete`; `src/admin/mesh_delete.rs`, `MeshDeleteDoor` | code-only |
| `MeshLeaver`, `LeaverSlot`, `leave_mesh`, `mesh_leave`, `LeaveKey/Book/Runs/Admission/Outcome` | `MeshDeleter`, `DeleterSlot`, `delete_mesh`, `mesh_deleted`, `DeleteKey/Book/Runs/Admission/Outcome` | code-only |
| `AdminRunner::shutdown_mesh`, `lead_leave`, `admit_leave`, `run_leave`, `leave_members`, `shutdown_one`, `record_mesh_left`, `admin_for_leaving_mesh`, `create_leaving_mesh_admin` | `delete_mesh`, `lead_delete`, `admit_delete`, `run_delete`, `delete_members`, `delete_one`, `record_mesh_deleted`, `admin_for_deleting_mesh`, `create_deleting_mesh_admin` | code-only |
| `BuildOperation::ShutdownMesh`, `build::shutdown_mesh` | `DeleteMesh`, `delete_mesh` | 2 (`BuildOperation` is `#[serde(tag = "op", rename_all = "snake_case")]`: the variant name is in every JSON plan and receipt — `durable format moved`, unless the serialized name is kept) |
| `RetireKind`, `RetireRequest`, `retire()`, `RetireNode`, `RetireStep` | `DeleteRequest`, `delete()`, `DeleteNode`, `NodeStep…`; `RetireKind` deleted | code-only |
| `Parking`, `Cycle::Parked`, `is_parked`, `ParkGate`, `Node.parked`; `LiveNodeResolver::park/parked/is_parked` | stopped vocabulary (`stop`, `stopped`, `is_stopped`, `StopGate`); the wire tokens `CurrentParked` / `Current { parked }` keep | code-only |
| `FabricShutdown`, `ShutdownControl`, `shutdown.rs`, `Family::FabricShutdown` | deleted; `fabric.delete` workflow, `Family::FabricDelete` | with §2 item 7 |
| module `crate::http` | `crate::control` keeps `ControlPlane`, `Refusal`, `Opened`, `Recovery`, `ActionKind`; the axum router and handlers move to the demo | 0 |
| Build step keys `create-node:`, `restart-node:`, `drain-node:`, `stop-node:`, `start-node:`, `retire-node:`, `shutdown-mesh:`, `fabric-primary-handover:` | `node.create:`, `node.restart:`, `node.connections.delete:` (R-CR4 ruled A), `node.stop:`, `node.start:`, `node.delete:<path.name>`, `mesh.delete:<mesh_id>`, `fabric.primary.update:` | 2 **[gossip]** (the key rides inside lifecycle frames) |
| `replace-node:`, `recover-node:` | removed: replace is Rafka's recipe; recovery is `node.delete` + `node.create` + `node.update` steps | with §2 items 3, 4 |
| completion tokens `node-drained`, `node-left`, `node-started`; `StatusRequest::op()` labels | `node.connections.deleted` (R-CR4 ruled A), `node.stopped`, `node.started`; `node.connections.delete`, `node.stop`, `mesh.delete`, `node.start`, `node.update` | 1–2 |
| `DrainNode` / `NodeDrained` (0x1B, ~160 / ~80 sites, they move with every landing), `DrainReceipt`, `fn drain`, `RDM_DRAIN_DEADLINE_MS` | retired (R-CR4 ruled A); `node.connections.delete { scope }` (R-CR3) carries the graceful cut, its completion names each closed connection; the receipt's facts move to it; `RDM_CONNECTIONS_DELETE_DEADLINE_MS` | 3 (wire) |
| node statuses `MemberStatus` / `NodeStatus` / `NodeState`: `Pending`, `Draining` (41), `Leaving` (~95) | §5 status names: `starting`, `pending`, `connections-deleting`, `connections-deleted`, `stopping`, `stopped` | 2 (rows: `durable format moved`; the digest is positional) |
| MeshStatus values `leaving`, `dead` (strings on the gossip wire, ~30 sites for `"dead"`) | `deleting`, `deleted` | 4 **[gossip]** |
| `MeshLeft` receipt step, `OtherMembersExited`, `LeavingRoster`, `all/other-members-exited` | `MeshDeleted`, `MembersDeleted`, `DeletingRoster`, `members-deleted` (`AcceptedRoster` keeps) | 2 |
| spans `…mesh.update.via-shutdown/-leave-members/-leave-tier/-leaving-row/-mesh-left-receipt`, `node.create.via-leaving-mesh`, `status.update.via-drain-node/-stop-node/-start-node`, `fabric.{update,reject}.via-shutdown*`, `build.{create,update}.via-rest`, `connection.delete.via-channel-parked`, `mesh.create.via-rebirth` + attribute `rebirth_decided` | `mesh.delete.via-build/-members/-tier`, `mesh.update.via-deleting-row`, `mesh.delete.via-mesh-deleted-receipt`, `node.create.via-deleting-mesh`, `node.connections.delete/stop/start.via-command`, `fabric.delete.via-…`, `build.*.via-node-rpc`, `…via-channel-stopped`, `mesh.create.via-recovery` + `recovery_decided` (mesh-investigation.md says "recovery verdict") | with their code |
| `ForwardReply::Relayed`, `relayed()` | `Forwarded`, `forwarded()` (same position, not durable) | 3 (#3117) |
| `StatusRequest::LeaveMesh / MeshLeave`, `FabricEvent::ShutdownInitiated` | mesh-family / fabric-family variants | 3 (#3117) |
| `Frame::MeshLeaving`, `MeshLeave`, `MeshLeft` and their hooks | deleted | 4 **[gossip]** |
| tests and scenarios: `mesh_leave_resume.rs`, `mesh_leave_member.rs`, `mesh_lifecycle__mesh_leave_{resume,no_survivor}.rs`, `retire_pipeline.rs`, `retire_admission.rs`, `node_lifecycle__node_drain.rs`, `park_start.rs`, `park_gate_*.rs`, `channel_park.rs` | `mesh_delete_*`, `node_delete_pipeline.rs`, `node_delete_admission.rs`, `node_lifecycle__node_connections_delete.rs`, `stop_start.rs`, `stop_gate_*.rs`, `channel_stopped.rs` (+ test-runner lists) | with their code |

## 2. Behaviour changes, largest first

| # | change | doc or ruling that requires it | landing |
|---|---|---|---|
| 1 | mesh.delete deletes each member with the whole node.delete (stop, exit proof, NodeDeleting/NodeDeleted, row Deleted written by the mesh-primary) | lifecycles/mesh-delete.md steps 3–4 | s41 (in flight: exit proof + Deleted row), s49 (#3068, #3078) |
| 2 | mesh.delete ends at mesh.deleted: remove the in-workflow final-primary stop, `OwnerResume::FinalExited`, and MeshLeft-after-exit; the final primary's end is a separate node.delete, fabric-primary executor; from its stop the fabric-primary publishes that mesh's aggregate (a stopped final primary is no primary, R-NS1 item 2) | mesh-delete.md steps 5–7; R-NS1; R-MD1 | s49, after s41 (#3078) |
| 3 | delete RDM's replace path (`ReplaceNode`, `RenamePredecessor`, `HandoffStorage`, drift `Replace`); replace is Rafka's recipe over node.create(`<name>.new`) / stop / update / start / delete | lifecycles/node-replace.md; envelope `BuildChange` drops `Replace` | new story; until it lands, item 19 makes RDM's replace stop the successor before the rename (#3087) |
| 4 | node recovery is delete + create + `node.update` copying caller-owned meta (the copy step does not exist); a lost mesh opens a recovery, not a Replace | lifecycles/node-recovery.md | s33 follow-up (#3088) |
| 5 | node rows, name claims (claimed before launch) and config writes move from the fabric-primary to the mesh-primary; mesh and fabric name claims added | node-identity-and-state.md §2.7/§2.8; envelope #meta-and-config; R-OW1 | s41 (in flight), s36; mesh/fabric claims a follow-on (#3068, #3089) |
| 6 | the dead mesh stays published with its last admin; the mesh-primary writes the mesh row Dead at mesh.deleted; Dead held by mesh id | mesh-delete.md 5–7; gossip.md "Mesh delete"; R-MD1 | s40 landed cb2596ac (#3067); s41 in flight (#3068) |
| 7 | `fabric.delete` replaces FabricShutdown (shutdown.rs, the Draining freeze, `hold_seats`, `shutdown.json`, the `shutdown` field in the Build-topic message and the join answer) | lifecycles/fabric-delete.md | new story, last **[gossip]**, durable, wire (#3090) |
| 8 | REST out of RDM: axum handlers to the demo; reads become `node.get`, `mesh.get`, `fabric.get`, `node.connections.get` first; `rafka-test-scenario` (65 files) is the largest consumer | ops-naming step 0 | step 0, before the §1 renames (#3091) |
| 9 | `node.delete` keeps storage: remove `ReleaseStorage` and its `remove_dir_all` | node-delete.md | landed f7d850d7 (#3092) |
| 10 | restart is one operation (`node.restart:`), its stop half no longer receipted as `retire-node:`; remove the exit-and-relaunch restart remnants (`supersedes` lineage, `restart_of`, admin boot restart mode) | node-restart.md | s34 follow-up (#3093) |
| 11 | remove sender-identity gates (`Unauthorized`, `SenderNotSubject`, caller `PeerUnresolved`) in join, build_claim, fabric_handover (x3), status_rpc and node commands; direction refusals (`RejectedNotAuthority`) stay | node-rpc.md §14 (no auth layer inside the mesh) | new story; §3 Q1. Seen live: a stopped node refuses the created admin's stop as `SenderNotSubject` (s41 no_survivor) (#3094) |
| 12 | delete timeout-strike pool eviction and flip its two tests to the ruled negative control | node-rpc.md §21 | landed f1fd416b (#3081) |
| 13 | the RDM standalone store is written at state-commit, not continuously from boot | states.md; fabric-state-commit.md | new story (s43 area) (#3095) |
| 14 | one node.start workflow (§0.1): the four start sequences (node-admin `start_booted` and `AdminSession::rejoin`, the rpc node's `start_booted` and rejoin path) become one `node_start` taking `StartKind { FirstStart, AfterStop }`, in node-start.md's order (join → hook → topology / floor → rafka-time last); node.create calls it with FirstStart; the starting → pending status changes live in it once | node-start.md; node-create.md; §5 status names | new story (#3096) |
| 15 | `admin_api_base` must not gate Ready; later it leaves the digest and NewFabricPrimary as a config key | ops-naming "config in the digest" | small now; field move durable **[gossip]** (#3091) |
| 16 | peer-mesh probe: exact mesh-primary first, then the other node-admins, direct then via a peer; thresholds in elapsed time | mesh-investigation.md; R-CK1 | landed in part (bd201c0c fence on elapsed t); probe order landed 67532dbe (#3097) |
| 17 | stop/start frames are the node's only reports; the mesh-primary sets the status from them; delete of a crashed process skips the stop sends; a received `NodeLeft` is refused by name and its carry arms go | node-stop.md, node-start.md, node-delete.md | with item 14; NodeLeft refusal **[gossip]** (#3098) |
| 18 | per-caller admission stays keyed by the caller's EndpointId, as the code has it; the doc now says so | node-rpc.md §17.1 | none (answered, §3 Q4) |
| 19 | rename precondition: node.update's name change is refused by name unless the node is not ready-for-traffic and holds no connection (stopped, or `node.connections.deleted`); `ApplyNodeName` answers `Busy` naming held routes; `node.connections.delete { scope: All \| Connections([id, …]) }` as its own call, empty list refused, completion names closed and unknown ids; node.stop's cut calls the same core with All | R-CR1, R-CR1a, R-CR2, R-CR3 (rulings page) | landed b2ff768d (#3075) |
| 20 | named typed refusals for an epoch overflow and a stale mesh-delete round (no codec variant today; never NotReady) | node-rpc-envelope.md Status | new story (#3099) |
| 21 | a stop quiesces the stop gate (no gated turn in progress) before its final aggregate and cut | node-stop.md step 4; R-ON1; gossip.md (last aggregate carries itself Leaving) | landed e8e86195 (#3073) |
| 22 | the election's lost set is read from durable exit-proof rows (never an in-memory feed); exit-row reads happen before the view lock; a proof once read is never withdrawn | CLAUDE.md read rule; elections §4.1 | s41 (in flight) |
| 23 | the view reads a stopped birth's renamed name by node id through the node-row id index, before the view lock | CLAUDE.md read rule (indexes); R12 | s42 follow-up (after s41; `durable format moved`: the id index) |
| 24 | the graceful connections cut replaces the drain (R-CR4 ruled A; R-CR5 ruled A: `All` is just connections) | rulings page R-CR4, R-CR5 | #3083 |
| 25 | Rafka side: the entry pull moves from `ControlFrame::ProjectionPull` / `ProjectionSnapshotPart` / `ProjectionRefused` (rafka-mesh-ops tags 16, 19, 18; rafka-node-base `serve_projections` in `crates/rafka-node-base/src/birth.rs` and `read_projection_answer` in `crates/rafka-node-base/src/lib.rs`) to node.topology.get | gossip.md §8; fabric-node-hooks.md | Rafka story (#3100) |
| 26 | Rafka side (R-SV1): delete `dialer_send_svid` / `acceptor_verify_svid` and the admission verdict; `connect_verified` becomes a plain connect; the first-bi-stream cert frame on rafka-mesh-v1 goes | fabric-certs.md#on-a-connection; internal-mesh-auth.md | Rafka story; wire change, the mesh moves together (#3101) |

## 3. Questions and their answers

1. Status-op sender checks. Answered: nothing inside the mesh checks who is calling; `Unauthorized`,
   `SenderNotSubject` and a caller-side `PeerUnresolved` keep their codec positions and a mesh
   receiver never sends them; `RejectedNotAuthority` stays as a direction rule (the receiver is not
   the authority). Source: node-rpc.md §14 and the no-auth-inside-the-mesh ruling; applied to
   node-rpc-envelope.md and node-rpc.md.
2. Step 2 (Build step keys) changes the `operation` string inside lifecycle frames. Answered: the
   key is `<op>:<path.name>`, an opaque string whose dots do not collide (ops-naming-proposal.md,
   "Dots are allowed in op names").
3. The pool key. Answered: `(scope, peer node, incarnation)`, as `PoolKey` in `rafka-node-rpc`; slot
   and freshness are per-entry facts the caller checks before dial (node-rpc.md §20).
4. The per-caller admission key. Answered: the caller's transport-proven `EndpointId` (node-rpc.md
   §17.1).
5. The node.id / incarnation a relaunched node-admin holds. Answered: a new node over its inherited
   data dir: new node.id and incarnation, same path.name and endpoint key, new port (ops-naming
   identity table, recover row).
6. The recovery Build's three step keys. Answered: `node.delete:<path.name>`,
   `node.create:<path.name>`, `node.update:<path.name>` (lifecycles/node-recovery.md).
7. A stopped node refuses a command from an admin it never heard of. Answered by 1: a stopped
   endpoint answers anyone who reaches it; nothing checks the caller.
8. The stop's end. In [node-status-heartbeat-connections.md](node-status-heartbeat-connections.md) R-HB1
   (Luke's direction 2026-10-11 evening, on §A.1): the mesh-primary authors and publishes every member
   status; a heartbeat is liveness only and is refused by name after node.stopped; only the
   mesh-primary's node.joined or node.delete ends a stop.
9. Answered (R-SV1): no cert is verified between mesh nodes; the internal data plane carries no
   SVID cert belt ([fabric-certs.md](../../architecture/fabric-certs.md#on-a-connection)).

Open in the findings, carried here until answered (IDs as in
[rdm-code-conformance-findings.md](rdm-code-conformance-findings.md)):
- CODE-2 Q4: gossip.md names both `drain-node:` and `node.drain:` for the drain step key.
- CODE-2 Q5: `RecoverySubmitter::recover_if_proven` is named in the findings but is not in RDM.
- CODE-2 Q6: NodeRestarting is published once or twice per restart.
- CODE-2 Q7: step 2 (Build step keys) is gossip content, so it needs Luke's yes with a flame (Q2 above
  answers only the dot collision, not this).
- CODE-3 flags 24, 26, 31, 33 and CODE-4 forks 2, 3, 4, 5c, 41: open as written in the findings.

Answered without a question (conformance): the golden-attach clear resets the lane's store for a
fresh fabric and is not a name release inside a live fabric (only a delete releases a name); the
step-0 CI guard greps `axum` and `"/api/`, not `Router` (iroh exports a Router);
`ForwardReply::Relayed` is renamed (CLAUDE.md bans relay; renames table, step 3).

## 4. Comment citations (comment-only landing, #3105)

About 120 RDM comment sites, and six rafka-v2 sites, cite doc pages or sections that were renamed,
removed or renumbered (`mesh-leave.md`, `node-decommission.md`, `fabric-primary-handover.md`,
`fabric-open-traffic.md`, `node-lifecycle-elections.md`, `node-rpc-rdm-ownership.md` / "ownership
amendment §N", `fabric-mesh-ops.md`, `node-rpc.md §38/§48/§5–6`, `fabric-primary.md §11.1`,
`FML-n`, `docs/i143/design.md`, `ops-naming-proposal.md`). The findings file maps each to its current
`path#section`. It is one comment-only RDM landing after the §1 renames, so it is done once.

## 5. Open proposals that change the plan

[node-status-heartbeat-connections.md](node-status-heartbeat-connections.md) holds who says what and the
names: R-HB1 (the mesh-primary authors and publishes every member status through its own-mesh projection;
a heartbeat is liveness only), R-HB3 (evidence numbers, parked), R-CR4 (ruled A: the drain is
`node.connections.delete`), R-CR5 (ruled A: `All` is just connections) and R-ST1 (status names follow their op, taken as arm
A). Items in §1 and §2 that depend on an open one say so.

### Evidence held for Luke (no change)

- **Join handshake (R-QH1):** the diagnostics caught both ends of a failed join (gate 50d7ec36,
  `mesh_shapes__live_resize`): the node bound 127.0.0.1 port 56958 sent its Initials five times and received
  nothing; the maker received them and answered every time to [::ffff:127.0.0.1] port 56958 — the reply was
  sent and not received. Port 56958 is the receiving port in all three incidents on record (this gate,
  f37635a9, and R-NW1 #3045): a box-level drop or diversion on that port is the lead.
- **Gossip delivery:** after a backbone neighbour-down, no backbone frame of a deleting mesh's last admin
  reached a peer for ~75 ms with four neighbours up; a mesh-channel topology v8 missed by mesh2's rpc nodes
  and recovered by the pull (af872623).
- **A mesh lost entirely is held:** mesh-investigation.md step 5 needs a live member's
  `carrier-edge-lost` to recover; with every runtime gone nothing can answer.
