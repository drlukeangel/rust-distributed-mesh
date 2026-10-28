# RDM code conformance: findings

Evidence for [rdm-code-conformance.md](rdm-code-conformance.md). Four read-only audits of
`/home/admin/rust-distributed-mesh` at `a207a281` against the architecture docs as corrected on 2026-10-11.
Each finding quotes the code, the doc rule it misses, the proposed change and its landing.


---

## CODE audit 1 — mesh delete (RDM @ a207a281, /home/admin/rust-distributed-mesh)

READ-ONLY audit. Docs compared: /home/admin/docs-pass (mesh-delete.md, states.md, gossip.md "Mesh delete" and
"External topology when a mesh is deleted", node-rpc-envelope.md "Mesh delete command pair", fabric-primary-update.md,
fabric-delete.md, ops-naming-proposal.md, R-MD1/R-NS1/R11/R-MN1). PRD: /home/admin/rafka-v2/docs/plans/i143-prd.md
(e12.s49 #3078 exists only there; docs-pass copy stops at s48).

Landing legend. N = Rust-only name (file, module, type, fn, var, span string, test name); no postcard, wire or gossip
byte moves; lands as a rename-only commit (it belongs with rollout step 1, where the typed `mesh.delete` object
appears). S2 = rollout step 2 (Build step keys / receipt step names: durable, "durable format moved"). S3 = rollout
step 3 (0x1B split). S4 = rollout step 4 (gossip frames, Luke's go-ahead). B = behaviour story.

What already conforms (no change): peers hold NO mesh-level routing hold (`View::mesh_leaving`, `held_by_leaving`,
`dead_meshes` do not exist; `DigestBook::routable`, membership.rs, reads birth + mesh ready = the
states.md routing rule); the mesh-primary writes its own mesh row Leaving at admission (`admit_leave`,
admin/leave.rs) and the view projects it so its own MeshStatus send announces it; roster capture-once,
`RosterAmended` chain, `AlreadyApplied` replay, second amendment refused by name, unproven predecessor refuses by
name (mesh_leave.rs `amend_roster`, `record_predecessor_exit`, `resume_after_loss`); tiers ordinary-then-admins in
parallel (`members_exited`); manifest validated by the fabric-primary against roster + amendments
(`handoff_verdict`, `first_mismatch`); fabric.primary.update spans match fabric-primary-update.md "Observability"
(`rdm.node_admin.fabric.update.via-handover-committed` / `-confirmed`, `rdm.node_admin.fabric.reject.via-handover`).

---------------------------------------------------------------------------------------------------------------

### A. Behaviour the docs rule out or require (class: behaviour)

### 1 · behaviour · `lead_leave` in `crates/rafka-node-admin-core/src/admin/leave.rs` (step 5) + shutdown_mesh_in
Code: `// Step 5: the final primary drains and stops under the owner's authority.` ...
`self.pipeline(&template).shutdown_member(build_id, attempt, outer, &mut node, &final_handle)` ...
`self.record_mesh_left(key, mesh, all, event)` (the `MeshLeft` receipt is recorded only after the final primary's exit).
Also `OwnerResume::FinalExited` (mesh_leave.rs, `owner_resume`) and the drift arm admin.rs.
Doc: mesh-delete.md intro + step 5 + step 7: "The mesh-primary is not deleted inside its own mesh.delete ... a
separate node.delete, with the fabric-primary as executor, deletes it afterwards"; "mesh.deleted: when every member
has its exit proof, the mesh-primary, still alive, writes the mesh row Dead and reports mesh.deleted". PRD e12.s49:
"the step-5 shutdown of the final primary and the FinalExited resume arm are deleted".
Proposal: delete the step-5 stop of the final primary from `lead_leave`; the validated handoff (`validate_handoff` ->
`LeaveBook::resolve`) IS mesh.deleted: record the terminal receipt there (rename of `STEP_MESH_LEFT`, see 22) with
`final_runtime` still `Some` (primary still running) and finish the Build step. Delete `OwnerResume::FinalExited`,
`mesh_left_manifest`, the `FinalExited` branch of `shutdown_mesh_in`, `after_mesh_exit(&node,"mesh-leave")` for the final
primary, and the matching clause in `reconcile_drift` (admin.rs). `first_mismatch(.., with_final)` loses the
`with_final=true` caller. The primary's end becomes an ordinary node.delete Build (item 3) that the fabric-primary
executes.
Landing: B · e12.s49 #3078 (after s40/s41) · no durable change for the deletion itself; the receipt payload loses
the final-primary Exited (S2 if the step is renamed in the same commit) · no gossip change.

### 2 · behaviour · `lead_leave` in `crates/rafka-node-admin-core/src/admin/leave.rs` (step 2) / admin.rs `GossipLifecycle::mesh_leaving`
Code: `self.lifecycle_events.mesh_leaving(&event).await;` -> `Frame::MeshLeaving { .., publisher: self.me }` published
by the fabric-primary "on the backbone and this mesh's channel" (admin.rs).
Doc: ops-naming-proposal.md mesh.delete entry: the mesh-primary's mesh topology carries the mesh Leaving, "replaces the
fabric-primary's old mesh-leaving gossip (backbone + own mesh)"; gossip.md "Mesh delete": "at mesh.deleting the
mesh-primary publishes its mesh's state, Leaving, in its own mesh topology; peers learn it only there".
Proposal: delete the `mesh_leaving` hook call, `LifecycleEvents::mesh_leaving`, `GossipLifecycle::mesh_leaving`,
`NoLifecycleEvents`/testkit stubs, and the peer handler `Frame::MeshLeaving` (membership.rs; its own comment
already says it marks nothing). The Leaving row + MeshStatus send in `admit_leave` is already the doc's mechanism.
Landing: B + S4 (a gossip Frame variant; removing it moves postcard variant 14 and the byte fixture
lifecycle_frames_wire.rs) · gossip change: NEEDS LUKE'S YES (CLAUDE.md rule 1) and a Jaeger flame of the live mesh
showing peers learn the mesh Leaving from the mesh topology alone · durable format moved.

### 3 · behaviour · `shutdown_one` in `crates/rafka-node-admin-core/src/admin/leave.rs` + `shutdown_member` in `deployment/pipeline.rs`
Code: `shutdown_member` = `drain_steps` + `stop_steps` only (`begin(.., member_operation(outer, path))`); no
`NodeDeleting`/`NodeDeleted` frames, no `ReleaseStorage`, no name/identity release; the Deleted row is written only for
operations `retire-node|recover-node|replace-node` (node_rows.rs), so a mesh member never gets row Deleted.
Doc: mesh-delete.md step 3-4: the mesh-primary "runs the whole node.delete for every ordinary node ... then for the
other node-admins"; Targets/Rows: "Each member's row Deleted (its node.delete)"; Build step key "each member's step is
its own `node.delete:<path.name>`"; node-delete.md steps 1-5 (stop, exact Exited proof, row Deleted, node.deleted,
fence the old (node.id, incarnation) in the peer-location book).
Proposal: `shutdown_one` runs the node.delete workflow (`Pipeline::retire_steps` Removal path, i.e. NodeDeleting ->
stop -> TerminateRuntime -> NodeDeleted frame -> row Deleted -> topology removal), executor = the mesh-primary, under
the key `node.delete:<path.name>` correlated to the outer `mesh.delete:<mesh_id>`. Delete `shutdown_member` and the
`("retire-node","NodeDeleted")`-only gate in node_rows.rs: add the mesh-delete member operation to its table, writer =
mesh-primary (R-OW1=A). Peers then see NodeStopped/NodeDeleted per member (states.md routing table) instead of the
one-shot `mesh_left` departure sweep (item 6).
Landing: B · e12.s41 #3068 (node row: Deleted rows) + e12.s49 · member op key is S2 (durable) · NodeStopped/NodeDeleted
frames already exist (no new gossip).

### 4 · behaviour · MISSING · mesh row Dead written by the mesh-primary at mesh.deleted
Code: no `MeshState::Dead` write anywhere in crates/rafka-node-admin-core/src (only `Leaving` via
`commit_mesh_state`, admin/leave.rs; the projection reads Dead at admin.rs, and nothing produces it).
Doc: R-MD1 item 1, states.md "Who sets each state" (mesh Dead: "the mesh-primary, still alive, on the mesh row (the
fabric-primary as acting primary only when no primary survived)"), mesh-delete.md step 5.
Proposal: in the mesh-primary path (`run_leave` after `leave_members` returns Ok, before `hand_off`) call
`status_rpc::commit_mesh_state(.., MeshState::Dead)` and `refresh_view()`; span `rdm.node_admin.mesh.update.via-dead-row`.
The fabric-primary's acting-primary Dead write (fabric-primary is the writer of a mesh with no primary,
storage.rs) is `AlreadyApplied` when the mesh-primary wrote it, and a write when no primary survived (R11
no-survivor path, where the admin the fabric-primary created is the reporter).
Landing: B · e12.s40 #3067 / s41 #3068 (R-MD1 "Changes for the seats": they hold the Dead writes) · no new format
(MeshState::Dead exists, status.rs MeshState discriminant 3) · no gossip change.

### 5 · behaviour · MISSING · the dead mesh stays in every topology; the mesh-primary keeps publishing; fabric-primary publishes after its stop
Code: the final primary is stopped in-workflow (item 1) and `Frame::MeshLeft` then removes the mesh from every peer
(`DigestBook::mesh_left`, membership.rs: "Nothing is kept of the mesh"; handler 1960-1982 drops
`forwarder`, `mesh_rx`, `backbone_rx` entries). `overlays_of` (membership.rs) publishes only for the publisher's
own mesh and there is no "publish for a mesh I am acting primary of" path (grep for acting/primaryless in
membership.rs and admin.rs: none).
Doc: gossip.md "Mesh delete": "After mesh.deleted the mesh's last node-admin, still its primary, keeps publishing its
mesh topology on the backbone with itself as the only member and the mesh status Dead ... From the stop on the
fabric-primary is that mesh's publisher of record ... `digests` empty, the stopped admin in `in_flight` as NodeStopped
... until a start hands publishing back or a delete moves the birth to `departed` as NodeDeleted";
"After departed retention the mesh is empty". R-NS1 items 1-3.
Proposal: (a) the mesh-primary keeps its mesh topology publisher running after the handoff until stopped/deleted (no
shutdown of the MeshTopology publisher at mesh.deleted); (b) add to the MeshTopology publisher path "publish for a mesh I am
acting primary of", gated on that mesh having no primary and the fabric-primary holding the stop receipt / exit proof
for the birth it lists (`overlays_of` unchanged); (c) the receive side keeps the Dead mesh with its one member until
`departed` retention ends.
Landing: B · e12.s40 #3067 (R-NS1 story; its red) · gossip.md already describes it, but a MeshTopology publisher change is a
mesh/gossip change -> NEEDS LUKE'S YES + flame (CLAUDE.md) · no frame variant added.

### 6 · behaviour · `Frame::MeshLeft` in `crates/rafka-mesh-transport/src/membership.rs`, DigestBook::mesh_left, handler 
Code: `MeshLeft` "the mesh left: its births departed, its source projections are removed, nothing of it is kept".
Doc: mesh-delete.md/gossip.md: no MeshLeft frame in the design; ops-naming "old MeshLeave / MeshLeft"; a deleted mesh's
members reach `departed` one by one through each member's NodeDeleted; the Dead mesh persists with its last admin.
Proposal: delete `Frame::MeshLeft`, its forward arm (membership.rs), handler, `DigestBook::mesh_left`,
`MeshLeftApplied`, `GossipLifecycle::mesh_left` (admin.rs), `LifecycleEvents::mesh_left`, the call
`self.lifecycle_events.mesh_left(..)` in `record_mesh_left`. Positional postcard: `Frame::NewFabricPrimary` is
variant 17 "appended after MeshLeft (16)" (seat_frames_wire.rs), so removal renumbers or leaves a reserved slot.
Landing: S4 + B · NEEDS LUKE'S YES · durable format moved · PRD s49 says "mesh-left frame unchanged until rollout 3/4":
keep it until then, but items 1/3 make it redundant, so it is dead-on-arrival code after s49.

### 7 · behaviour · `hand_off` in `crates/rafka-node-admin-core/src/admin/leave.rs` -> `lifecycle_events.mesh_leave` / `Frame::MeshLeave`
Code: `hand_off` publishes `Frame::MeshLeave` (gossip, "mesh-leave published on the backbone and this mesh's channel",
admin.rs) before calling 0x1B `MeshLeave`.
Doc: gossip.md has no MeshLeave/MeshLeft/MeshLeaving frames in the catalog; mesh.deleted travels as the 0x1B
`MeshLeave` call (envelope "Mesh delete command pair"), not gossip.
Proposal: delete the `mesh_leave` hook and `Frame::MeshLeave` (variant 15) with 2 and 6; keep `call_mesh_leave` (0x1B).
Landing: S4 + B · NEEDS LUKE'S YES · durable format moved.

### 8 · behaviour · crates/rafka-node-admin-core/src/executor.rs `BuildOperation::ShutdownMesh => fabric()`
Code: `BuildOperation::CreateNode | CreateMesh | ShutdownMesh => fabric()` (the fabric-primary executes the whole
workflow and sends `LeaveMesh` to the mesh-primary; `prepare` hands the seat over first).
Doc: mesh-delete.md Operation contract: "Executor: The mesh-primary, for each member's node.delete"; step 1: the
fabric-primary "claims the attempt for the mesh-primary as executor". envelope also keeps "LeaveMesh ... admission
starts mesh.deleting".
Proposal: VERIFY, do not change yet. After rollout step 1 (Build 0x20) the claim for a mesh.delete step names the
mesh-primary as executor (`executor_for`), with LeaveMesh remaining the admission body. Today's split (fabric-primary
owns, mesh-primary runs members) is the same split described in different words; ask whether the docs mean the Build
`executor_for` changes. Doc fork for Luke/architect, not a code change on its own.
Landing: B (step 1) · no format change.

### 9 · behaviour · crates/rafka-node-admin-core/src/admin.rs `prepare` / fabric_handover.rs `HandoverDoor::confirmed`
Code: `prepare` hands the fabric seat over whenever the fabric-primary sits inside the mesh being retired.
Doc: fabric-delete.md: "Fabric-primary seat: stays put on its own mesh ... that mesh.delete skips its
fabric.primary.update step" (when no other mesh is left).
Proposal: skip `hand_over`/`confirmed` for the last mesh of a fabric.delete (a fabric.delete story, new typed object);
no change for a plain mesh.delete.
Landing: B · new story (fabric.delete) · no format change.

### 10 · behaviour · `operation` in `crates/rafka-node-admin-core/src/mesh_leave.rs` / member_operation (operation keys)
Code: `format!("shutdown-mesh:{mesh_id}")`, `format!("{outer}/{path}")` (also build.rs `BuildOperation::key`).
Doc: mesh-delete.md "Build step key `mesh.delete:<mesh_id>`; each member's step is its own `node.delete:<path.name>`".
Proposal: outer key -> `mesh.delete:<mesh_id>`; member key -> `node.delete:<path.name>` (parsers that split on `/` or
`-`: drift_inputs admin.rs `starts_with(format!("{op}/"))`, `exit_receipts` mesh_leave.rs, wire.rs,
reconcile_drift admin.rs, build.rs/310/866, executor.rs, status.rs doc comments, every test
literal listed in the callers table).
Landing: S2 (ops-naming step 2: "Build step keys ... stored in accepted Builds and receipts") · durable format moved.

---------------------------------------------------------------------------------------------------------------

### B. Renames (class: rename) — file / module / type / fn / variant / constant / span / key

### 11 · rename · crates/rafka-node-admin-core/src/mesh_leave.rs -> mesh_delete.rs
Code: `pub mod mesh_leave;` (src/lib.rs); 1039 lines.
Doc: ops-naming-proposal.md "Mesh shutdown has six names today ... mesh-leave.md"; mesh-delete.md.
Proposal: `git mv src/mesh_leave.rs src/mesh_delete.rs`; `lib.rs` -> `pub mod mesh_delete;`; rewrite every
`crate::mesh_leave::` path. Callers (all `crate::mesh_leave` / `rafka_node_admin_core::mesh_leave` / `use mesh_leave`):
 - src/admin/leave.rs (use), 53, 57, 59, 60, 64, 66, 112-114 (module path in-body: `crate::mesh_leave::operation`,
   `owner_resume`, `OwnerResume::{Left,FinalExited,Lead}`, `admitted_of`, `prove_predecessor_exit`, `span_unproven`,
   `record_predecessor_exit`, `leaving_mesh_admin_path`, `resume_after_loss`, `LeaveAdmission`, `exited`, ...)
 - src/admin.rs, 1706, 1707, 1710, 1715, 2283, 2285, 4205, 4342, 4343
 - src/status_rpc.rs (`crate::mesh_leave::LeaverSlot`)
 - src/wire.rs (`use crate::mesh_leave::{ExitManifest, RosterAmended}`), 490 (4 STEP_ constants), 493
 - src/accepted.rs calls `crate::build::shutdown_mesh` (not the module; see 13)
 - tests/mesh_leave_resume.rs (whole file), tests/main.rs (`mod mesh_leave_resume;`), and the helper reuse
   `crate::mesh_leave_resume::{capture, trace_id}` in tests/node_rows.rs; tests/status_seat_fold.rs;
   tests/seat_transfer_step.rs; tests/fabric_handover.rs (move `capture`/`trace_id` to a neutral test
   support module when the file is renamed)
 - tests/drift_convergence.rs (many `rafka_node_admin_core::mesh_leave::` paths), tests/build_drive.rs
 - crates/rafka-node-rpc-testkit/src/admin_faults.rs (`MeshLeaveEvent` path), tests/mesh_leave_member.rs,
   tests/replace_pipeline.rs, tests/recover_pipeline.rs.
Landing: N (rollout step 1).

### 12 · rename · crates/rafka-node-admin-core/src/admin/leave.rs -> admin/mesh_delete.rs
Code: `mod leave; pub use leave::LeaveDoor;` (src/admin.rs).
Proposal: file `admin/mesh_delete.rs`; `mod mesh_delete; pub use mesh_delete::MeshDeleteDoor;` (admin.rs);
`LeaveDoor { runner }` constructed at admin.rs.
Landing: N.

### 13 · rename · identifiers carrying leave/shutdown (src)
(old -> new; every one in mesh_leave.rs unless stated)
 - `MeshLeaver` trait / `LeaverSlot` / `leaver` field (status_rpc.rs; admin.rs,
   4223,4347,6670; wiring) -> `MeshDeleter` / `DeleterSlot` / `deleter`. Methods `leave_mesh` / `mesh_leave` ->
   `delete_mesh` / `mesh_deleted` (they serve the 0x1B bodies LeaveMesh / MeshLeave; bodies keep their names until S3).
 - `LeaveKey` -> `DeleteKey` (field docs "shutdown-mesh:<mesh_id>"); `LeaveBook` -> `DeleteBook`; `LeaveRuns` -> `DeleteRuns`;
   `LeaveAdmission` -> `DeleteAdmission`; `LeaveOutcome` -> `DeleteOutcome`; `OpenLeave`/`LeaveRun` -> `OpenDelete`/`DeleteRun`;
   `leaving_roster` -> `deleting_roster`; `leaving_mesh_admin_path` -> `deleting_mesh_admin_path`; `HandoffVerdict` keeps.
 - `OwnerResume::{Left, FinalExited, Lead}`: `Left` -> `Deleted`; `FinalExited` DELETED (item 1).
 - `ExitManifest`, `RosterBirth`, `RosterAmended`, `Admitted`: keep (names the docs use or neutral).
 - `AdminRunner::{shutdown_mesh, shutdown_mesh_in, lead_leave, admit_leave, run_leave, leave_members, shutdown_one,
   hand_off, call_mesh_leave, validate_handoff, record_mesh_left, admin_for_leaving_mesh, create_leaving_mesh_admin,
   amend_after_loss, forget_mesh_id}` (admin/leave.rs) -> `delete_mesh`, `delete_mesh_in`, `lead_delete`, `admit_delete`,
   `run_delete`, `delete_members`, `delete_one` (becomes the node.delete call, item 3), `hand_off`, `call_mesh_deleted`,
   `validate_handoff`, `record_mesh_deleted`, `admin_for_deleting_mesh`, `create_deleting_mesh_admin`, `amend_after_loss`.
 - `AdminRunner` fields `leaves` / `leave_runs` (admin.rs) -> `deletes` / `delete_runs`.
 - `Pipeline::shutdown_member` (deployment/pipeline.rs) DELETED (item 3); `RetireStep`, `RetireKind` untouched.
 - `MeshLeaveEvent` (pipeline.rs) DELETED with the hooks (items 2, 6, 7); until then `MeshDeleteEvent`.
 - `BuildOperation::ShutdownMesh { mesh, mesh_id }` (build.rs, key ; executor.rs; build_drive.rs;
   admin.rs; accepted.rs; tests build.rs, build_drive.rs) -> `DeleteMesh`;
   `build::shutdown_mesh()` (build.rs; accepted.rs) -> `delete_mesh()`.
 - `BuildOperation::RetireNode` (build.rs; admin.rs) -> `DeleteNode` (it already emits `node.delete.via-build`);
   same rollout item, node.delete audit owns it.
 - Constants: `STEP_MESH_LEFT = "MeshLeft"` -> `STEP_MESH_DELETED = "MeshDeleted"` (mesh.deleted is the terminal event;
   callers admin.rs, wire.rs, leave.rs, tests) [S2: stored step name]; `STEP_OTHER_MEMBERS_EXITED`
   ("OtherMembersExited") -> `STEP_MEMBERS_DELETED` [S2]; `STEP_ACCEPTED_ROSTER`, `STEP_LEAVING_ROSTER` ->
   `STEP_DELETING_ROSTER` [S2], `STEP_ROSTER_AMENDED` keeps (the doc's name). `manifest_reference(.., "all-members-exited"
   | "other-members-exited")` literal strings -> `members-deleted` (stored in references; S2).
 - fabric_handover.rs: `operation()` returns `fabric-primary-handover:{mesh_id}` (); doc names the operation
   `fabric.primary.update:<id>` (fabric-primary-update.md "Command and completion messages") -> change literal (stored in
   HandoverRow.operation, fabric_storage.rs HANDOVERS_DIR "fabric/handovers", HANDOVER_FORMAT "fabric-handover/1":
   S2, durable). Module/type names `fabric_handover`, `HandoverDoor`, `hand_over` need NOT change: the docs name only the
   op (`fabric.primary.update`) and the wire bodies (TakeFabricPrimary / FabricPrimaryTaken stay).
Landing: N for Rust identifiers (step 1); S2 for the stored strings marked.

### 14 · rename · span names containing leave / shutdown (grep-able strings; tests assert them)
 - `rdm.node_admin.mesh.update.via-shutdown` (leave.rs shutdown_mesh) -> `rdm.node_admin.mesh.delete.via-build`
   (pattern of the existing `rdm.node_admin.node.delete.via-build`, admin.rsff)
 - `...mesh.update.via-leave-members` -> `...mesh.delete.via-members`; `...mesh.update.via-leave-tier` ->
   `...mesh.delete.via-tier`
 - `...node.update.via-shutdown-member` -> goes away with item 3 (the member span becomes node.delete's own)
 - `...mesh.update.via-leaving-row` -> `...mesh.update.via-deleting-row`
 - `...mesh.update.via-mesh-left-receipt` -> `...mesh.delete.via-mesh-deleted-receipt`
 - `...node.create.via-leaving-mesh` -> `...node.create.via-deleting-mesh`
 - `...mesh.update.via-handoff-sent|-validated|-exit-manifest-recorded|-accepted-roster|-retained-receipts|
   -owner-build-facts|-owner-admitted-roster|-recorded-final-exit|-predecessor-exit-recorded|-roster-amended|
   -second-amendment|-unproven-primary`: no leave/shutdown word; keep, except `via-recorded-final-exit` (DELETED with
   FinalExited) and `via-exit-manifest-recorded` -> `via-members-deleted-recorded` (to follow STEP rename).
 - frame spans `rdm.node_admin.mesh.update.via-mesh-leaving|-mesh-leave|-mesh-left` (admin.rs) and
   `rdm.mesh.membership.update.via-mesh-leaving|-mesh-leave|-mesh-left` (membership.rs) go with their
   frames (S4, items 2, 6, 7).
 - `StatusRequest::name()` strings "leave-mesh" / "mesh-leave" (status.rs; span attributes and log text): S3 with
   the variants.
 - Refusal text that someone greps: build.rs "...only through TakeFabricPrimary inside a mesh-leave", asserted by
   tests/control_routes.rs (`contains("mesh-leave")`), tests/fabric_handover.rs, rafka-test-scenario
   node_lifecycle__node_replace.rs -> "inside a mesh.delete" (N, with the three assertions).
Landing: N for the first six; S4 for frame spans; S3 for name() strings.

### 15 · rename · wire bodies `StatusRequest::LeaveMesh` / `MeshLeave` (rafka-node-rpc-contract/src/status.rs, 213)
Doc: the envelope keeps these names today ("Status bodies LeaveMesh and MeshLeave on op 0x1B, positions 10 and 11",
node-rpc-envelope.md "Mesh delete command pair", mesh-delete.md "Command, completion and gossip messages").
Proposal: NO rename now. Rename only at rollout step 3 ("Split the remaining 0x1B calls" -> `mesh` family, e.g.
`mesh.delete` / `mesh.deleted`). Callers then: status.rs, tests/status_wire.rs,
status_rpc.rs, admin/leave.rs (build and match), mesh_leave.rs (`MeshLeaver`), testkit.
Landing: S3 (wire change) · positional postcard unchanged if only the variant names move (names are not on the wire); an
op split moves them.

---------------------------------------------------------------------------------------------------------------

### C. Comments that cite a renamed/removed page or section (class: comment)

Replace `mesh-leave.md` with `lifecycles/mesh-delete.md`; `fabric-primary-handover.md` with
`lifecycles/fabric-primary-update.md`; `node-rpc-envelope.md "Mesh leave command pair"` with
`node-rpc-envelope.md#mesh-delete-command-pair` (anchor `mesh-leave-command-pair` also remains); "Resume after the
primary's loss" -> `mesh-delete.md#resume-after-the-primarys-loss`; "Completion proof" / "Failure/certainty behavior" /
"Operations and lifecycle events" / "Every stopping member publishes its own Leaving" (sections of the old page that
no longer exist in mesh-delete.md) -> `mesh-delete.md#failure-and-receipts` / the "Numbered end-to-end sequence" step.
Landing for all: comment-only.
 16 · crates/rafka-node-admin-core/src/mesh_leave.rs: 16 hits (lines 1-2 header, 34, 36, 39-40 `STEP_*` docs, 60, 130-140,
      240, 255, 262, 325, 345, 355, 392, 478-503, 508, 534-545, 555, 597, 620-630, 690, 760, 780)
 17 · crates/rafka-node-admin-core/src/admin/leave.rs: 5 (module header ; ; ; ; )
 18 · crates/rafka-node-admin-core/src/admin.rs, 1699 (mesh-leave.md), 3090, 6396 (fabric-primary-handover.md), 941
 19 · crates/rafka-node-admin-core/src/build.rs, 379; http.rs, 474; fabric_handover.rs ("before a mesh that
      holds the fabric seat leaves", "may start mesh-leave"),  ("one per shutdown"); fabric_storage.rs, 591, +2
 20 · crates/rafka-node-rpc-contract/src/status.rs, 210-212; handover.rs; crates/rafka-mesh-transport/src/
      membership.rs, 528-531, 553-556 (doc comments on the three frames, "gossip.md, mesh-leaving / mesh-leave /
      mesh-left"),  ("lifecycles/mesh-leave.md")
 21 · tests: tests/drift_convergence.rs (also cites `node-status-change.md`, a page that no longer exists
      -> `lifecycles/node-status.md`); tests/fabric_handover.rs; tests/mesh_leave_resume.rs,
      1049; rafka-node-rpc-testkit/tests/mesh_leave_member.rs; rafka-test-scenario/src/wedge.rs (1);
      tests/mesh_lifecycle__mesh_leave_resume.rs; __mesh_leave_no_survivor.rs; __mesh_replace.rs
 Also stale (node.delete audit, listed for completeness): `node-decommission.md` -> `lifecycles/node-delete.md` at
 node_rows.rs, accepted.rs, pipeline.rs, tests/stopped_delete.rs, tests/node_rows.rs,
 rafka-test-scenario node_lifecycle__node_stop.rs; `node-status-change.md` at admin.rs.

---------------------------------------------------------------------------------------------------------------

### D. Tests and scenarios named or asserting the old shape (class: test)

### 22 · test · file renames
 - crates/rafka-node-admin-core/tests/mesh_leave_resume.rs -> `mesh_delete_resume.rs` (+ `mod mesh_delete_resume;` in
   tests/main.rs and the five `crate::mesh_leave_resume::` helper imports listed in item 11).
 - crates/rafka-node-rpc-testkit/tests/mesh_leave_member.rs -> `mesh_delete_member.rs` (+ tests/main.rs); test
   `a_mesh_leave_member_shutdown_publishes_node_stopped_from_its_stopped_reply` -> `a_mesh_delete_member_publishes_node_stopped_...`.
   It asserts the member runs `shutdown_member`; after item 3 it asserts node.delete's own NodeStopped/NodeDeleted.
 - crates/rafka-test-scenario/tests/mesh_lifecycle__mesh_leave_resume.rs -> `mesh_lifecycle__mesh_delete_resume.rs`;
   `mesh_lifecycle__mesh_leave_no_survivor.rs` -> `mesh_lifecycle__mesh_delete_no_survivor.rs`; `mesh_lifecycle__mesh_replace.rs`
   keeps its name (a replace composes create + delete) but its body changes; + tests/main.rs;
   tools/test-runner/src/lib.rs (two lists name both stems; also any Rafka-side `feature-registry` reference by stem).
 - fn names: `a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin`,
   `a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates`,
   `a_resumed_leave_keeps_the_exited_members_receipts_and_runs_only_the_rest`,
   `an_owner_resuming_after_the_final_primary_exited_records_mesh_left_from_the_receipts` (DELETED with FinalExited),
   `a_primary_lost_mid_leave_is_replaced_by_a_named_amendment_and_the_leave_completes_over_both_primaries`,
   `a_leaving_primary_that_has_not_heard_the_build_runs_the_leave_over_the_owners_facts`,
   `a_final_primary_that_holds_no_fact_of_the_build_ends_the_open_leave_incomplete`,
   `a_created_admin_of_a_leaving_mesh_...`, `an_incomplete_tier_is_refused_...`, and in drift_convergence.rs,
   1439, 1508, 1584 (`a_removed_mesh_still_live_with_no_mesh_left_receipt_opens_its_shutdown_again`, `..._shutdown_again`,
   `a_resumed_leave_that_fails_...`, `a_removed_mesh_whose_final_primary_exited_without_mesh_left_...`): leave -> delete,
   shutdown -> delete, mesh_left -> mesh_deleted.
Landing: N (names) · the assertions below are B.

### 23 · test · assertions on the old shape (change with the behaviour items)
 - mesh_lifecycle__mesh_replace.rs, 147-166, 184-215: asserts the owner drains+stops the final primary after the
   handoff ("drained after every other exit and after the validated handoff", ), a `MeshLeft` receipt with 4
   receipts incl. the final primary (, ), and the gossip hooks `via-mesh-leaving|-mesh-leave|-mesh-left`
   () heard by peers. Under items 1/2/6/7: receipts = 3 members at mesh.deleted; the final primary is stopped
   by a SEPARATE node.delete Build; the mesh Dead row precedes the handoff; no mesh-leaving/left frames; peers hold the
   Dead mesh with its one member while it lives (R-NS1 acceptance, R-MD1 acceptance incl. AlreadyApplied for the
   fabric-primary's ensure-Dead).
 - mesh_lifecycle__mesh_leave_resume.rs: "the view holds no mesh2 birth after MeshLeft" and "MeshLeft
   covers both primary births" -> after s49 the view holds the Dead mesh with its final admin until its node.delete; the
   terminal receipt covers the roster plus amendments, the lost predecessor's Exited included, final primary not exited.
   Fault `door.arm(.. "operation": "shutdown-mesh:{id}/mesh2.rpc.2")` () -> `node.delete:mesh2.rpc.2` (S2).
 - mesh_lifecycle__mesh_leave_no_survivor.rs: same MeshLeft/roster text.
 - rafka-mesh-transport membership.rs unit tests  `mesh_left_departs_every_birth_for_the_departed_retention_and_keeps_nothing`,
   , ,  (feed `MeshLeaving`/`MeshLeft`), tests/lifecycle_frames_wire.rs,
   tests/seat_frames_wire.rs: DELETED or re-pointed with the frames (S4). The R4 contract tests (, ) conform
   and keep (comment repoint only).
 - tests/status_wire.rs (`shutdown-mesh:` operation literal, `other-members-exited` reference): S2 literals.
 - node-rpc-contract status.rs unit test , same literals.
 - tests/build_drive.rs `starts_with("shutdown-mesh")`, build.rs, executor.rs test
   `a_mesh_retire_is_the_fabric_primarys`: S2 key + N rename.
 - tests/fabric_handover.rs `operation_of`: operation key literal (S2).
 - A red cell needed for item 4 (mesh-primary Dead row precedes its handoff; fabric-primary ensure-Dead AlreadyApplied)
   and item 5 (peer holds the Dead mesh and its final admin; NodeStopped via the fabric-primary after the stop): R-MD1 and
   R-NS1 "Acceptance". These belong to s40/s41/s49 and use Jaeger traces (CLAUDE.md "THE TRACE IS THE DELIVERABLE").

---------------------------------------------------------------------------------------------------------------

### E. Doc inconsistencies found while auditing (for the docs lead; not code)

 a. docs/plans/rulings/i143-rulings-2026-10-11-dev.md R-NS1 (, ) and R-MD1 (, "Scope") still describe
    `MeshLeft`, "mesh-leaving routing hold intact" and the owner's in-workflow final stop ("the current complete leave
    workflow still owns its final stop"). mesh-delete.md / gossip.md / states.md now say the opposite (no MeshLeft frame,
    no mesh-level hold, separate node.delete). Code follows the R4 no-hold ruling already.
 b. PRD docs-pass copy lacks e12.s49 (rafka-v2's has it). s39 title still reads "leave-mesh / mesh-leave / stop-node /
    node-left / mesh-left", s40 "peer meshes on mesh-leaving / mesh-left: routing hold".
 c. node-rpc-envelope.md:~765 link text "Mesh leave, resume after the primary's loss" points at mesh-delete.md's anchor
    (`#resume-after-the-primarys-loss`), the text should read "Mesh delete".
 d. mesh-delete.md says the claim names "the mesh-primary as executor" while envelope/code keep LeaveMesh as admission
    from the fabric-primary-owned Build (item 8).
 e. mesh-delete.md (line 85-86) and R11 step 5 differ: R11 says "The fabric-primary stops the new final primary and
    obtains its exact Exited" (inside the leave); mesh-delete.md moves that to the separate node.delete (step 7).

### Summary

Counts: behaviour 10 (items 1-10), rename 5 (11-15) + ~40 identifier/span/key renames inside them, comment 6 groups
(16-21; ~46 `.rs` citation sites in 17 files), test 2 groups (22-23; 6 files renamed, ~14 tests renamed, 8 files with
old-shape assertions).

Renames (old -> new, all places listed above)
| old | new | landing |
|---|---|---|
| src/mesh_leave.rs, `mod mesh_leave` (lib.rs) | mesh_delete.rs, `mod mesh_delete` | N |
| src/admin/leave.rs, `mod leave`, `LeaveDoor` | admin/mesh_delete.rs, `MeshDeleteDoor` | N |
| `MeshLeaver`/`LeaverSlot`/`leave_mesh`/`mesh_leave` | `MeshDeleter`/`DeleterSlot`/`delete_mesh`/`mesh_deleted` | N |
| `LeaveKey/Book/Runs/Admission/Outcome` | `DeleteKey/Book/Runs/Admission/Outcome` | N |
| `AdminRunner::shutdown_mesh`, `lead_leave`, `admit_leave`, `run_leave`, `leave_members`, `shutdown_one`, `record_mesh_left`, `admin_for_leaving_mesh`, `create_leaving_mesh_admin` | `delete_mesh`, `lead_delete`, `admit_delete`, `run_delete`, `delete_members`, `delete_one`, `record_mesh_deleted`, `admin_for_deleting_mesh`, `create_deleting_mesh_admin` | N |
| `BuildOperation::ShutdownMesh`, `build::shutdown_mesh` | `DeleteMesh`, `delete_mesh` | N |
| `shutdown-mesh:<mesh_id>` (+ `/<path>`) | `mesh.delete:<mesh_id>` (+ `node.delete:<path.name>`) | S2 |
| `STEP_MESH_LEFT "MeshLeft"`, `OtherMembersExited`, `LeavingRoster`, `AcceptedRoster`, ref `all/other-members-exited` | `MeshDeleted`, `MembersDeleted`, `DeletingRoster`, (AcceptedRoster keep), `members-deleted` | S2 |
| `fabric-primary-handover:{mesh_id}` | `fabric.primary.update:<id>` | S2 |
| spans `mesh.update.via-shutdown/-leave-members/-leave-tier/-leaving-row/-mesh-left-receipt`, `node.create.via-leaving-mesh` | `mesh.delete.via-build/-members/-tier`, `mesh.update.via-deleting-row`, `mesh.delete.via-mesh-deleted-receipt`, `node.create.via-deleting-mesh` | N |
| `Frame::MeshLeaving/MeshLeave/MeshLeft`, hooks, `MeshLeaveEvent`, their spans | deleted | S4 + Luke |
| `StatusRequest::LeaveMesh/MeshLeave`, `name()` "leave-mesh"/"mesh-leave" | mesh-family names | S3 |
| tests/mesh_leave_resume.rs, testkit mesh_leave_member.rs, scenario mesh_lifecycle__mesh_leave_{resume,no_survivor}.rs (+ main.rs, test-runner lists) | mesh_delete_* | N |
| comment citations `mesh-leave.md`, `fabric-primary-handover.md`, `node-decommission.md`, `node-status-change.md` | `lifecycles/mesh-delete.md#...`, `fabric-primary-update.md`, `node-delete.md`, `node-status.md` | comment |

Behaviour changes, largest first
 1. Members deleted by the full node.delete workflow, executor the mesh-primary, row Deleted, NodeDeleting/NodeDeleted
    (item 3; s41 + s49).
 2. mesh.delete ends at mesh.deleted: remove the in-workflow final-primary stop, `FinalExited`, MeshLeft-after-exit;
    the final primary's end is a separate node.delete by the fabric-primary (item 1; s49).
 3. Dead mesh persists with its last admin publishing; fabric-primary publishes the primaryless mesh's topology
    after its stop (items 5; s40; MeshTopology publisher = Luke's yes + flame).
 4. Mesh-primary writes mesh row Dead before the handoff; fabric-primary ensure-Dead as the AlreadyApplied/acting-primary
    path (item 4; s40/s41).
 5. Remove fabric-primary `MeshLeaving` gossip, `MeshLeave` and `MeshLeft` frames (items 2, 6, 7; S4; Luke).
 6. Step/operation key rename `mesh.delete:<id>` / `node.delete:<path.name>` (item 10; S2, durable).
 7. fabric.delete: last mesh skips fabric.primary.update (item 9; new story).
 8. Verify executor wording (item 8; doc fork).

---

## CODE-2 · Node lifecycle ops — RDM code vs the corrected docs

Read-only audit. Code: `/home/admin/rust-distributed-mesh` at `a207a281`. Docs: `/home/admin/docs-pass`.
Paths below are relative to `crates/` unless a leading `docs/` or a bare doc name. Rollout steps are those of
`ops-naming-proposal.md` "Rollout and cost": 0 REST out, 1 typed objects + node behaviour + Build 0x20,
2 Build step keys (= `durable format moved`), 3 split of 0x1B (wire), 4 gossip frame names (Luke's go-ahead).

### Already conformant (so nobody re-does it)

- `NodeStopped` exists, is authored by the executor (the mesh-primary of an ordinary node) from the matching `Stopped` reply
  (`stop_steps_with` in `rafka-node-admin-core/src/deployment/pipeline.rs`, `GossipLifecycle::stopped` in `admin.rs`), the node gossips nothing for its stop
  (`park` in `node_self.rs`), `NodeLeft` is refused by name (`status_rpc.rs`) and never sent.
- Restart is stop then start on the same process (`restart_in_place` in `admin.rs`, `restart` in `pipeline.rs`), keeps node.id and incarnation; the book closes a restart on the same incarnation's rejoin (`close_restarts` in `membership.rs`).
- `node.deleted` fences `(node.id, incarnation)` in the peer-location book (`fence_incarnation` in `membership.rs`, `register_location` refuses by name `rdm.mesh.connection.reject.via-fenced-incarnation`).
- Receipt STEP strings (`DrainNode`, `AwaitNodeDrained`, `StopNode`, `AwaitNodeLeft`, `TerminateRuntime`, `NodeDeleting`, `Complete`, …) are durable but the docs name only events and operation keys; step 2 moves keys only. Do not bundle a step-string rename into it.
- `executor_for` in `executor.rs`: a seat holder's drain/stop/delete is executed by its same-mesh successor after TakeSeats (node-drain.md step 0), a member's by its mesh-primary, the fabric-primary's own by the admin the election seats; recovery routes to the mesh-primary. Only its doc cites (finding 21) and `RetireNode`/`ReplaceNode` variant names (findings 9, 10) are stale.
- Typed objects and event names (`rafka-node-admin-client/src/names.rs`): `node.drain/create/stop/start/restart/delete`, `node.draining/drained/connections.deleted/stopped/joined/started/created/deleted/restarting/restarted`.
- `StatusRequest`/`StatusReply` (`rafka-node-rpc-contract/src/status.rs`) match the envelope schema: 21 request / 31 reply variants, positional order, `ApplyNodeName`/`UpdateNode`/`NameHeld`/`CurrentParked`/`Blocked`.
- Node recovery by the mesh-primary (tickle, provider proof, `RecoverNode` Build) exists (`RecoverySubmitter` in `admin.rs`, `offline.rs`); no `RetireKind::Relaunch` remains.
- Step-2 parse-site check the rollout asks for ("anything that splits them on `-`"): no site splits an operation key on `-`. Every parser is `starts_with("<verb>-node:")`, `strip_prefix`, or `split_once(':')` against a literal verb (listed in findings 4, 5, 6, 7, 20, 21, 22). Nothing else needs to change for step 2 beyond those literals.

---

### A. Build step keys and operation strings (step 2, `durable format moved`)

### 1 · rename · `BuildOperation::key` in `rafka-node-admin-core/src/build.rs` (262-275)
Code: `create-node:{node}`, `replace-node:`, `restart-node:`, `drain-node:`, `stop-node:`, `start-node:`, `recover-node:`, `retire-node:`, `shutdown-mesh:{mesh_id}`, `create-mesh:{mesh}`.
Doc: ops-naming-proposal.md "How an op works on node-RPC": "the Build step key: `node.create:<path.name>`"; fabric-mesh-node-lifecycles.md "Terminology firewall" lists `node.create/stop/start/restart/delete/drain:<path.name>`; mesh-delete.md "`mesh.delete:<mesh_id>`; each member's step is its own `node.delete:<path.name>`".
Proposal: `create-node:`→`node.create:`; `restart-node:`→`node.restart:`; `drain-node:`→`node.drain:`; `stop-node:`→`node.stop:`; `start-node:`→`node.start:`; `retire-node:`→`node.delete:`; `shutdown-mesh:`→`mesh.delete:`. `replace-node:` and `recover-node:` have no mechanical name (findings 8, 9). `create-mesh:` is a mesh-area key; envelope node-create.md#mesh-absent says "There is no mesh create call", so it belongs to the mesh audit, not renamed here.
Landing: rollout step 2 · durable format moved (keys live in accepted Builds, receipts, `MeshTopology.in_flight` `LifecycleOp.operation`, `BuildStepReceipt.operation`) · no wire-schema change · GOSSIP CONTENT CHANGE, needs Luke's yes: `LifecycleOp.operation` rides inside NodeDeleting/NodeDeleted/NodeRestarting/NodeStopped/NodeDraining/NodeLeaving/NodeDrained and every receiver parses it (`LifecycleOp::is_stop/is_restart`, `stopped` in `membership.rs`, `parked_unheard`); frame names stay (step 4) but the bytes differ (see Q7).

### 2 · rename · rafka-node-admin-core/src/deployment/pipeline.rs (Run operation strings: 1103 doc, 1235, 1638, 1713, 1742, 2091, 2190)
Code: `create_steps`: `let op = if req.restart_of.is_some() { "restart-node" } else { "create-node" }`; `restart`: `format!("restart-node:{name}")`; `retire_steps`: `format!("retire-node:{name}")`; line 1742 `operation: format!("restart-node:{name}")`; `replace_steps`: `replace-node:`; `recover_steps`: `recover-node:`; `drain`/`stop`/`start`: `drain-node:`/`stop-node:`/`start-node:` (1575-1620).
Doc: as 1.
Proposal: render every key from the one typed name (`rafka-node-admin-client::NodeOp::name()` + `:` + path.name), delete the literals. `drain_steps`/`stop_steps`/`start_steps` get their key from `command_context` (line 1763-1771 already calls `DrainContext::operation()`, `StopContext::operation()`, `StartContext::operation()`), so changing those three in `rafka-node-admin-client/src/calls.rs` moves the command keys.
Landing: step 2 · durable format moved · no wire change.

### 3 · rename · rafka-node-admin-client/src/calls.rs (172, 188, 211, 217, 234, 238, 255)
Code: `DrainContext::operation()` `format!("drain-node:{}", self.node)`, `StopContext::operation()` `"stop-node:{}"`, `StartContext::operation()` `"start-node:{}"`; `HeldView::overlay` doc `(`stop-node:`, `restart-node:`)`.
Doc: ops-naming "One canonical name, derived everywhere": the key is the typed name.
Proposal: derive from `NodeOp::Drain/Stop/Start.name()`; fix the doc comments. These three strings are also the `operation` field of `DrainNode/StopNode/StartNode/NodeDrained` requests on 0x1B, so peers compare them: the whole fleet moves together.
Landing: step 2 · durable format moved · wire bytes change (string content only, no schema change) · no gossip schema change.

### 4 · rename · rafka-node-admin-client/src/workflow.rs (WorkflowKind doc 106-120; `retire_rows` 148-157; `rows()` 172-206; `operation_matches` 210-216; module doc "the keys themselves are untouched")
Code: rows keyed `"retire-node"` (Delete, and the stop half of Restart), `"create-node"`, `"stop-node"`, `"start-node"`, `"restart-node"`; `Restart(PathName)` doc: "`retire-node:<path.name>` then `restart-node:<path.name>`"; `fold` is "the one place a Build's receipt step keys meet the canonical names".
Doc: node-restart.md "Operation contract" (one workflow, `node.restart:<path.name>`), node-delete.md (`node.delete:`), ops-naming "One canonical name, derived everywhere".
Proposal: `retire-node`→`node.delete`, `create-node`→`node.create`, `stop-node`→`node.stop`, `start-node`→`node.start`, `restart-node`→`node.restart`. With finding 7 (restart no longer keyed under retire) `Restart` rows all carry `node.restart`, and `operation_matches` becomes one equality for every kind (the `create-node:*` prefix match stays for create).
Landing: step 2 · durable format moved · caller-visible frames unchanged.

### 5 · rename · rafka-node-admin-core/src/accepted.rs (400-402, 468-470, 533-551; unit tests 789-1114)
Code: `AttemptAction::Drain => (format!("drain-node:{path}"), "DrainNode")`, `Start => start-node:`, `_ => stop-node:`; `BuildOperation::{DrainNode,StartNode,StopNode,RestartNode,ReplaceNode,RetireNode,RecoverNode,CreateNode}`; tests assert `.key() == "drain-node:mesh1.rpc.2"`.
Doc: as 1.
Proposal: keys from the single renderer; rename the enum variants `BuildOperation::{RetireNode→DeleteNode}` (the others are already the verb). The `serde(tag = "op", rename_all = "snake_case")` tags (`retire_node`, `replace_node`, `recover_node`, `create_node`…) are in the type's serialized form: check whether any serialized `BuildPlan` is stored before renaming a variant (none found stored: `BuildPlan` is built per attempt in `plan` in `build.rs`/`plan_with` in `accepted.rs` and described in span attrs only).
Landing: step 2 for the key strings (durable format moved); variant rename is code-only if `BuildPlan` is never stored.

### 6 · rename · rafka-node-admin-core/src/build_state.rs (58-66 AttemptReason docs; 485-560 `in_flight_ops`, `restarts_completed`; 626-660 `DELETING_WORKFLOWS`; tests 1114-1303, 1422-1524)
Code: `s.operation.starts_with("restart-node:")` (505); `const DELETING_WORKFLOWS: [&str; 3] = ["recover-node:", "replace-node:", "retire-node:"]` (637); `recovery_deleted`, `recovery_completed`.
Doc: as 1; node-recovery.md "Failure and receipts" (an exit during a workflow belongs to that workflow).
Proposal: `restart-node:`→`node.restart:`; `DELETING_WORKFLOWS` becomes `["node.delete:"]` once recover is its per-primitive steps and replace leaves RDM (findings 8, 9); until then `["recover-node:", "replace-node:", "node.delete:"]`. `AttemptReason` kebab strings (`restart`, `replace`, `drain`, `stop`, `start`, `recovery`, `proven-drift`) are not named by the docs: no change required.
Landing: step 2 · durable format moved.

### 7 · behaviour · `retire_steps` in `rafka-node-admin-core/src/deployment/pipeline.rs` (1713-1790) and ::restart (1633-1650)
Code: every `RetireKind` run begins `self.begin(.., format!("retire-node:{name}"))`; `restart()` runs `retire_steps(.. kind: RetireKind::Restart)` (so the drain/stop half of a restart is receipted under `retire-node:<path>` with steps `NodeRestarting, SeatTransfer, DrainNode, AwaitNodeDrained, StopNode, AwaitNodeLeft, Complete`) and only then opens `restart-node:<path>` for the start half. `workflow.rs` Restart rows and `build_state.rs` tests (`facts.push(with("retire-node:mesh1.rpc.3", "NodeRestarting"…))`) encode this.
Doc: node-restart.md "Operation contract": "`node.restart` is a composed workflow: NodeRestarting + the entire node.stop + the entire node.start + node.restarted", one Build operation `node.restart:<path.name>`; node-delete.md "composes the whole node.stop".
Proposal: one `begin(.., "node.restart:<path>")` for the whole restart; its stop and start legs are steps of that operation (the shared drain/stop/start leg functions already exist: `drain_steps`, `stop_steps_with`, `start_steps`). `RetireKind::Restart` then has no user in `retire_steps` and is deleted (finding 10). The restart-hold derivation (`in_flight_ops` in `build_state.rs`, `restart_hold_from_facts.rs`) closes on `node.restart:<path>`'s `Complete`, with no `retire-node` operation in the middle.
Landing: two landings, as the rollout requires. Step 1: one `begin()` for the whole restart under the existing `restart-node:<path>` key, no `retire-node:` half (receipts of the stop half change operation) · durable format moved. Step 2: that key becomes `node.restart:<path>` (finding 1). No wire change. i143 story: e12.s34 #3060 covers "restart keeps the incarnation"; the key correction is new work under it.

### 8 · behaviour · `recover_steps` in `rafka-node-admin-core/src/deployment/pipeline.rs` (2156-2260) and accepted.rs (`RecoverNode`, 475-481), `recover` in `admin.rs` (2670-2740)
Code: recovery is ONE fused operation `recover-node:<path>`: `NodeDeleting → TerminateRuntime → NodeDeleted → [after_deleted fence] → AllocateIdentity … WaitForNodeReady → Complete`. No `node[2].update` step.
Doc: node-recovery.md "Steps": "node.delete of the exited instance, node.create at the same path.name over the inherited data dir, then node[2].update with the caller-owned meta"; "Retained: … config (seeded from the Deleted row)"; ops-naming "recover … node[1].delete / node.create / node[2].update".
Proposal: express the recovery Build as three primitive operations in order, `node.delete:<path>` (crashed target: no stop frames, `node.deleted` from the recorded exit proof), `node.create:<path>` (inherited data dir, FirstStart), `node.update:<path>` (copy caller-owned meta only, `otel_debug` etc.). The workflow-level key `recover-node:` disappears; `BuildChange::Recover`/`AttemptAction::Recover`/`AttemptReason::Recovery` stay as the Build's request (the docs call the request `build.create(recover <path.name>, exited birth)`). The caller-owned meta copy step does not exist anywhere in code today (grep `otel_debug` in `pipeline.rs`/`admin.rs`: none on the recover path). Docs fix the three steps but not their keys: the keys above are the mechanical rendering and need the doc owner's confirmation.
Landing: new story under e12.s33 #3051 (node recovery by the mesh-primary) · durable format moved (operation strings and step list) · no wire / gossip change.

### 9 · behaviour · replace is still an RDM operation: `BuildOperation::ReplaceNode` in `rafka-node-admin-core/src/build.rs` (183-195), accepted.rs (500-536), `replace` in `pipeline.rs` (2059-2150, `ReplaceStep::RenamePredecessor/HandoffStorage` 233-250, `hand_over_storage` 2290-2330), `replace` in `admin.rs` (2655-2670, 3141), node_rows.rs (`replace-node` arms 105-112, `moved_to_old`), node_update.rs, `http.rs`, `BuildChange::Replace` in `rafka-node-rpc-contract/src/build.rs`
Code: RDM runs `replace-node:<path>` as one operation: drain/stop the predecessor, `RenamePredecessor` (to `<path>.old`), allocate the successor AT `<path>` (not `<name>.new`), `TerminateRuntime`, `HandoffStorage` (RDM moves the predecessor's partition dirs into the successor's), deploy, join, ready, then `NodeDeleted` of the old identity and `ReleaseStorage`.
Doc: node-replace.md: "Replace is Rafka's recipe, submitted as one Build. RDM executes its primitives … it has no replace opcode or knowledge of Rafka data"; successor "Born first as `<name>.new`"; steps `node.create(<name>.new)`, `node[1].stop`, `node[1].update(name.old)`, `node[2].update(name)` (ApplyNodeName cutover), `node[2].update` caller-owned meta, then "Rafka's own" data step; ops-naming "Decommission moves no data … Only today's replace moves data … keeping the old node around for data transfer is Rafka's call".
Proposal: delete the RDM replace path: `ReplaceNode`, `ReplaceStep`, `RenamePredecessor`, `HandoffStorage`, `hand_over_storage`, `DeploymentPipeline::replace`/`ReplaceRequest`, `AttemptAction::Replace`, `AttemptReason::Replace`, the drift Replace arm (finding 15), the `replace-node` arms of `NodeRows::on_step`, and the REST route. Replace becomes a Build the caller (Rafka, or a demo preset) composes from `node.create`/`node.stop`/`node.update`/`node.delete`, which already exist (`node_update.rs` is the `.old`/`<name>` rename, `ApplyNodeName` the cutover). `scratch-json-dumper` and the unit tests that assert `ReplaceNode` plans go with it. DOC CONFLICT to settle first: node-rpc-envelope.md "Build schema" (about 1179) still lists `BuildChange::Replace { node, incarnation }`; node-replace.md says there is no replace opcode. The code can only follow one.
Landing: new story ("the replace recipe story that owns lifecycles/node-replace.md", named in rulings/i143-rulings-2026-10-11-dev.md R12+R13 "Stories") · durable format moved if `BuildChange`/`AttemptAction` lose a variant (postcard positional) · 0x20 wire change (BuildChange) · no gossip change. Rafka-side recipe is out of RDM.

### 10 · rename · `RetireKind` in `rafka-node-admin-core/src/deployment/pipeline.rs` (505-515), RetireRequest (850-860), `retire`, `retire_steps`, `RetireStep`; admin.rs `retire`, `retire_with`, `RetireNode` dispatch (3159), `after_exit` (2779-2800), `standalone` (2796-2830); `http.rsff`; tests `stopped_delete.rs`, `receipt_refusal.rs`, `seat_transfer_step.rs`, `i143_acceptance_2805.rs`, `rafka-node-rpc-testkit/tests/retire_*.rs`
Code: `enum RetireKind { Removal, Restart }`; `standalone()` builds `RetireRequest { kind: RetireKind::Restart }` to run a plain `node.drain` or `node.stop` (admin.rs); `after_exit` branches `kind != Removal ⇒ "restart"` for a runtime exit that a restart no longer has.
Doc: "retire" is not a doc word; node-delete.md / node-stop.md / node-drain.md name the ops.
Proposal: delete `RetireKind` (after finding 7 only delete uses it; a standalone stop/drain takes its own request type, `StopRequest`/`DrainRequest`, which already exist as the `drain()`/`stop()` entry points). `RetireRequest`→`DeleteRequest`, `retire`/`retire_steps`/`retire_with`→`delete`/`delete_steps`/`delete_with`, `BuildOperation::RetireNode`→`DeleteNode`, `RetireStep` is renamed for what it holds (drain, stop and terminate steps shared by three workflows) once `retire` is gone. `after_exit`'s `restart` branch and its `reason = "restart"` attr go.
Landing: code-only rename (no durable move) after finding 7; the step names inside (`DrainNode`, `AwaitNodeLeft`, …) are receipt strings, not required by the docs (preamble).

### 12 · rename · rafka-node-admin-core/src/node_commands.rs (1-60, 132, 205-232, 277-299), stopped_gc.rs (189, 197), wire.rs (458), rafka-mesh-entity/src/lifecycle.rs (66-80), `on_step` in `node_rows.rs` (82-112)
Code: `NodeCommand::operation_prefix` → `"drain-node"`, `"stop-node"`, `"start-node"`; `completion()` → `"node-drained"`, `"node-left"`, `"node-started"`; `starts_with("stop-node:")` (node_commands.rs, stopped_gc.rs, wire.rs, lifecycle.rs `is_stop`), `starts_with("start-node:")`/`"restart-node:"` (stopped_gc.rs, lifecycle.rs `is_restart`); `on_step` matches `("retire-node" | "replace-node" | "recover-node", "NodeDeleting")`, `("retire-node","NodeDeleted")`, …
Doc: ops-naming "Naming rules" (`node.drain` completes as `node.drained`); gossip.md Node-stopped overlay ("`node.stop:mesh1.rpc.1`" in the example, ).
Proposal: prefixes become `node.drain`, `node.stop`, `node.start`, `node.restart`, `node.delete`; completion tokens `node.drained`, `node.stopped` (replacing `node-left`), `node.started`. `LifecycleOp::is_stop/is_restart` test the new prefixes; add `is_delete` rather than treating "not restart, not stop" as removal.
Landing: step 2 · durable format moved (`LifecycleOp.operation` is persisted in receipt `output` and gossiped inside the frames) · gossip frame NAMES unchanged (step 4) but frame CONTENT string changes: tell Luke, because the frame bytes differ.

### 13 · rename · `StatusRequest::op` in `rafka-node-rpc-contract/src/status.rs`() (382-405), `StatusReply::name()` (~760-800) and `rafka-node-admin-core` span attrs `op = req.op()`
Code: `"drain-node"`, `"node-drained"`, `"stop-node"`, `"node-left"`, `"leave-mesh"`, `"mesh-leave"`, `"start-node"`, `"update-node"`, `"apply-node-name"`, … used as the `op` attribute of `rdm.node_admin.status.update.via-command` and the `RejectedUnmatchedCompletion` text.
Doc: ops-naming: `node.drain` replaces "old drain-node / DrainNode / NodeDrained", `node.stop` replaces "old stop-node / StopNode"; the variants keep their names until the 0x1B split.
Proposal: `op()` returns the typed name for the node/mesh variants (`node.drain`, `node.drained`, `node.stop`, `node.start`, `node.update`, `mesh.delete` for LeaveMesh/MeshLeave). The enum variant names (`DrainNode`, `NodeDrained`, `StopNode`, `NodeLeft`, `StartNode`, `UpdateNode`, `ApplyNodeName`, `LeaveMesh`, `MeshLeave`) stay as postcard identities until step 3.
Landing: strings only, not postcard: ride step 2 (or 3) · no durable move · no wire schema change.

### 14 · rename · tests and fixtures carrying old keys (step 2 sweep list)
Files with `create-node|retire-node|drain-node|stop-node|start-node|restart-node|replace-node|recover-node` literals (counts from grep): rafka-node-admin-core/tests: `restart_hold_from_facts.rs` (11+), `node_objects.rs` (25), `i143_acceptance_rw1.rs` (20), `node_rows.rs` (17), `recovery_admission.rs` (12), `fabric_handover.rs` (10), `build_drive.rs`, `build_catch_up.rs`, `build_takeover.rs`, `stopped_gc.rs`, `receipt_refusal.rs`, `seat_transfer_step.rs`, `drift_convergence.rs` (8), `topology_read.rs`, `node_rpc_objects.rs`, `i143_acceptance_2805.rs`, `i143_acceptance_2892.rs`; rafka-node-rpc-testkit tests `replace_pipeline.rs`, `retire_pipeline.rs`, `recover_pipeline.rs`, `retire_admission.rs`, `rejoin_name.rs`, `park_start.rs`, `hydrate_before_ready.rs`, `status_probe_apply.rs`, `mesh_leave_member.rs`; rafka-mesh-transport tests `lifecycle_frames_wire.rs` (byte fixtures), `round_frames_wire.rs`, `i143_acceptance_2900.rs`; rafka-test-scenario tests `i143_acceptance_2780/2782/2783/2784/2775/2776/2803/2942.rs`, `rshape_burn_in.rs` (about 25), `node_lifecycle__node_delete.rs`, `__node_stop.rs`, `__node_drain.rs`, `__node_replace.rs`, `__admin_restart.rs`, `fabric_build__await_attempt.rs`, `fabric_build__accepted_topology.rs`, `mesh_lifecycle__mesh_replace.rs`; rafka-test-scenario/src/canary.rs; `scratch-json-dumper/src/main.rs`; unit tests inside `build_state.rs`, `build_run.rs` (12), `fabric_builds.rs`, `node_commands.rs`, `accepted.rs`, `membership.rs` (11), `status.rs`, `storage.rs`, `status_rpc.rs`.
Doc: as 1. The byte fixtures in `lifecycle_frames_wire.rs`/`round_frames_wire.rs`/`status_wire.rs`/`topology_wire.rs` embed the old operation string: regenerated together.
Proposal: replace each literal with the typed renderer (`NodeOp::Stop.name()` + path) so the next rename is one edit.
Landing: step 2, same landing as the code · tests ride the durable-move commit.

### B. Spans (no durable move; proposals follow ops-naming "the span: `rdm.node_admin.node.<verb>.via-…`")

### 15 · rename · rafka-node-admin-core/src/node_self.rs (353, 427, 505) and rafka-test-scenario tests that name them
Code: `rdm.node_admin.status.update.via-drain-node`, `…via-stop-node`, `…via-start-node` (the node's serve spans); tests `node_lifecycle__node_delete.rs`, `__node_stop.rs` look them up by name.
Doc: node-rpc-envelope.md "Runtime spans": "Each completed workflow step has its event, a span under `rdm.node_admin.node.<verb>.via-…`".
Proposal: `rdm.node_admin.node.drain.via-command`, `rdm.node_admin.node.stop.via-command`, `rdm.node_admin.node.start.via-command` (the node-side serve of the command; `via-call` is already the caller's, `NodeOp::span` in `names.rs`). `rdm.node_admin.status.update.via-command` (status_rpc.rs, the admin-side decision) stays: it is the status op's, not a workflow step's.
Landing: rides step 2 · no durable move · test updates.

### 16 · rename · membership.rs and admin.rs span names carrying the old vocabulary
Code → proposal:
- `rdm.mesh.connection.delete.via-channel-parked` (membership.rs) → `…via-channel-stopped` (R12+R13 ruling: "Prose vocabulary 'stopped' in spans, errors and comments; wire tokens unchanged").
- `rdm.mesh.connection.update.via-restart-address-retired` (membership.rs) → keep only if a restart can still move an address; with restart keeping the process and port it cannot (finding 22): delete with its branch.
- `rdm.node_admin.node.update.via-restart` (admin.rs, admin boot "restarting as the same logical node") → deleted with finding 21.
- `rdm.node_admin.node.update.via-replace-rename` (pipeline.rs), `rdm.node_admin.node.update.via-rename` (node_rows.rs, replace's `RenamePredecessor`) → deleted with finding 9 (`node_update.rs`'s `rdm.node_admin.node.update.via-update-node` is the one rename span).
Doc: gossip.md Examples lists only `rdm.node_admin.node.update.via-node-restarting`, `rdm.mesh.membership.update.via-node-restarting`, `rdm.node_admin.node.delete.via-node-deleted`, `rdm.mesh.membership.remove.via-node-deleted`; the code already emits exactly these (conformant, do not rename).
Landing: comment/rename rides the story that removes the branch · no durable move.

### C. node_self.rs (the node's side of stop/start/drain)

### 17 · behaviour · `park` in `rafka-node-admin-core/src/node_self.rs` (452-457)
Code: `if let Some(declare) = &self.declare { declare(NodeState::Leaving).await; }` (the stopping node declares Leaving upward before it cuts) and `let _ = (self.set_status)(MemberStatus::Leaving);` after the cut.
Doc: states.md "Who sets each state": "node Leaving | the node's mesh-primary | the matching stopped frame on the stop call's stream"; node-stop.md step 4-5 ("the node gossips nothing for this step … the mesh-primary takes the matching stopped frame").
Proposal: the node reports only through the stop call's stream (`Stopped { receipt }`, `node.stopped`); the mesh-primary writes Leaving when it takes that frame (executor side, where `lifecycle.stopped(&op)` is called after `Completion::Received`, `stop_steps_with` in `pipeline.rs`). Delete the node's own `DeclareNodeState(Leaving)` call. The node's local `set_status(Leaving)` stays (its own admission).
Landing: step 1 follow-up under e12.s34 #3060 · no durable move · wire: the `DeclareNodeState{Leaving}` request is no longer sent (enum unchanged).

### 18 · behaviour · `AdminSession::rejoin` in `rafka-node-admin-core/src/admin.rs` (2086-2200) and `rejoin` in `rafka-node-rpc-testkit/src/node.rs` (728-822) and the create path (node.rs, admin.rs)
Code: order is join → `adopt_join_answer` (rafka-time) → `membership.unpark` → `get_topology` + `adopt_rafka_time` → `join_peers` → publish Pending → `hydrate_before_ready` → ready. Topology is read BEFORE the hook; rafka-time is adopted from the topology read before the hook; no Build-floor fetch after the hook.
Doc: node-start.md steps 2-5: join, `node.joined`, hook ("subscribes its kind topics through the joined channel and reads no topology snapshot as a prerequisite"), THEN `node.topology.get`, Build facts below the authority's floor (FetchBuildFacts), "Refresh rafka-time **last**", then `node.started`. ops-naming `start` row says this "moves fabric-node-hooks.md (, ) and node-create.md step 6 … they change when step 1 lands".
Proposal: reorder rejoin and the create-path boot to join → hook → topology → floor facts → rafka-time → `node.started`. `StartFailed.step` values become `node.join` (kept), `node.hydrate` (hook), `node.topology` (kept, now after the hook).
Landing: step 1 · new story (not in the e12 list: e12.s34 #3060 names "stop parks, start rejoins" but not the order) · no durable move · no wire schema change (call order only).

### 19 · behaviour · `AdminSession::rejoin` in `rafka-node-admin-core/src/admin.rs` `ready` closure (2159-2170) and status_declare.rs
Code: on hook Ok the node sets its own digest status `Ready` (`source = "self-published-membership"`), and `status_declare.rs` owes `DeclareNodeState(Ready)` upward.
Doc: node-start.md step 5: "node.started … is the node's report: the mesh-primary sets Ready on that exact birth through the node status apply … The node sends no separate status report after node.started."
Proposal: the `node.started` reply frame is the report; the mesh-primary sets Ready from it and the node's digest takes the status from the mesh-primary's apply (`ApplyNodeState` already exists in `status_rpc.rs` as the down-op). The separate declaration path for Ready is removed. Needs a design check against `node-status.md` "Status report" (it still describes "a node reports to its mesh's primary, which sets its status"): the two docs describe the same hop; the code has two reporters (digest + declaration) where the docs have one.
Landing: new story under step 1 · no durable move · no schema change.

### 20 · comment · rafka-node-admin-core/src/node_self.rs (1-3, 33-39, 49-56, 66-72, 134-145, 285, 293-300), model.rs, status.rs (293, 298, 558, 564, 573) and the ~110-count "park/parked" vocabulary (`admin.rs` 113, `membership.rs` 61, `park_gate.rs` 29, `pipeline.rs` 27, `live.rs` 26, `admin_faults.rs` 25, …)
Code: `Parking`, `parking()`, `park()/unpark()/is_parked()`, `Cycle::Parked`, `Node.parked`, `ParkGate`, span `rdm.mesh.connection.delete.via-channel-parked`, `Frame` doc "the birth is parked", test names "parked".
Doc: rulings/i143-rulings-2026-10-11-dev.md R12+R13 "Vocabulary: STOPPED is the verb. 'Parked' is gone from prose. The wire tokens `Current { parked }` / `CurrentParked` keep their spelling until Luke rules a rename."
Proposal: prose, spans, errors and comments say stopped; identifiers `Parking→Stopping`-free naming is a code rename: `Cycle::Parked`→`Cycle::Stopped`, `is_parked`→`is_stopped`, `Node.parked`→`Node.stopped` (not postcard: the control projection is JSON, check `fabric_projection` before renaming that field), `ParkGate` keeps its job under the name `StopGate`. Keep `StatusReply::CurrentParked` and the `current-parked` reply name as the ruling says.
Landing: code-only rename; step 7 of the R12+R13 "Changes" list · no durable move (except `Node.parked` if it appears in a stored JSON: verify).

### 21 · comment · doc citations in the node-lifecycle files that name a removed page or section
Rows `old citation (file:line) → new`:
- `mesh-leave.md` (build.rs, 379; status.rs; membership.rs, 5270 `lifecycles/mesh-leave.md`; admin.rs, 1699; `drift_convergence.rs` 1371, 1430, 1499) → `lifecycles/mesh-delete.md` (and `#resume-after-the-primarys-loss`); owned by the mesh audit, listed because they sit in my files.
- `node-decommission.md` (accepted.rs, 798; node_rows.rs; pipeline.rs; `stopped_delete.rs`; `node_rows.rs` tests 2, 189; `node_lifecycle__node_stop.rs`) → `lifecycles/node-delete.md` (steps 1-3) and `lifecycles/node-stop.md#numbered-end-to-end-sequence`.
- `fabric-primary-handover.md` (admin.rs, 6396; fabric_handover.rs; fabric_storage.rs; handover.rs; `fabric_handover.rs` tests) → `lifecycles/fabric-primary-update.md` and `node-rpc-envelope.md#fabric-primary-handover-op-0x21-rdm`.
- `fabric-open-traffic.md` (status.rs; fabric_rounds.rs; fabric_state.rs; `fabric_states__rounds.rs`) → `lifecycles/fabric-ready.md`.
- `node-status-change.md` (admin.rs; `drift_convergence.rs`) → `lifecycles/node-status.md#node-status-change`.
- `node-ready.md` (rafka-node-rpc-testkit/src/node.rs; `join_before_ready.rs`) → `lifecycles/node-status.md` / `lifecycles/node-start.md` step 5.
- `node-lifecycle-elections.md` (election.rs; `rshape_burn_in.rs`, `mesh_elections__cohort_election.rs`) → `fabric-node-lifecycle-elections.md`.
- `node-build-hydration.md`, `node-birth.md` (app_hydration.rs "`node-birth.md` step 6, 'then hydrate'"; `join_before_ready.rs`; `mesh_leave.rs`) → `fabric-node-hooks.md#hydrate_before_ready` and `lifecycles/node-start.md` step 3.
- `fabric-mesh-ops.md §3`, `node-rpc.md §38` (status.rs, status_rpc.rs) → `node-rpc-envelope.md#status-op-0x1b-rdm` (node-rpc.md has no §38; §37 then §39).
- `fabric-node-lifecycle-events-ops-gossip.md` (investigate.rs, `node_lifecycle__admin_restart.rs`, `i143_acceptance_2803_detect.rs`) → `gossip.md#31-frame-catalog` and `lifecycles/mesh-investigation.md`.
- `node-rpc-rdm-ownership.md §8-§12` (rafka-node-rpc/src/server.rs, pool.rs, `pool.rs` test, rafka-node-base/src/lib.rs) → `node-rpc.md` §12 (sealed registration) and §20 (pool).
- `fabric-node-lifecycle.md §7.3` as "the offline tickle" (offline.rs, admin.rs, ping.rs, cadence.rs, soak.rs, testkit node.rs) → `lifecycles/node-recovery.md#detection-and-proof` (§7.3 is now "Delete releases identity; storage stays").
- `fabric-node-lifecycle.md §4.3` / `4.3` as "the writer ladder" (status_rpc.rs, 1023; admin.rs, 5680; store_root.rs; status_storage.rs, 277) → `fabric-node-lifecycle.md#25-one-system-org-one-projection-one-writer-per-mesh` (§4.3 is now "A returning broker re-syncs per topic").
- `fabric-node-lifecycle.md` bare "Restarting is commanded silence" (membership.rs; admin.rs; model.rs) → `lifecycles/node-restart.md`.
Doc: the docs-pass tree has none of the old pages.
Proposal: repoint each citation to the new path#section above; where the cited section no longer says what the comment claims, say so in the commit.
Landing: comment-only · none.

### D. Behaviour that the docs rule out

### 22 · behaviour · `release_storage_step` in `rafka-node-admin-core/src/deployment/pipeline.rs` (2020-2051, called at 1780 and 2149), `StorageDisposition` (524-537), `ReleaseStorage` row in `workflow.rs`, `wire.rs` `Produces::Storage`, tests `stopped_delete.rs`, `node_rows.rs` ReleaseStorage frames
Code: every removal runs `ReleaseStorage`: `StorageMeta::Ephemeral` or `Persistent { on_retire: Release }` → `std::fs::remove_dir_all(node.data_dir)`.
Doc: node-delete.md: "It releases node.id and name, **not storage**. The data dir and node-identity.json / endpoint key stay with that data dir. Application storage removal is separate work"; ops-naming "Stop is not decommission" table: delete storage "kept: it belongs to the spec, not the record"; "The data dir is not released by delete at all … removing the data dir is the app's step"; fabric-node-lifecycle.md §7.3 "Delete releases identity; storage stays".
Proposal: delete the `ReleaseStorage` step, `StorageDisposition`, `release_storage_step`, and the `on_retire` consumption in the delete path. Node recovery's "inherited data dir" depends on it (`recover_steps` calls `load_transport_key(&inherited)`), so the fix removes a hazard: a delete can currently erase the key a recovery needs. `StorageMeta::Persistent { on_retire }`/`PersistentRetireDisposition` in the accepted Build topology (`rafka_mesh_entity::meta`) lose their reader; removing the field moves the accepted-Build row.
Landing: behaviour story, new (closest is e12.s41 #3068 "node.delete keeps the row", node-delete.md step 3) · durable format moved if `on_retire` is removed from `StorageMeta` (leave the field, drop the reader, if no move is wanted) · no wire / gossip change.

### 23 · behaviour · rafka-node-admin-core/src/node_rows.rs (module doc 1-40; `NodeRows` struct; `on_step`; `Records::install_projected`), admin.rs `NodeRows` wiring
Code: "The node row at the fabric-primary … The fabric-primary writes a node's row here from the record it holds"; `me_id` "writes only while its view names that birth the fabric-primary"; the name claim is made by the fabric-primary AFTER it sees the node in its view ("A birth the fabric-primary's own view holds for the first time … its row is claimed at its `path.name`"); `NodeDeleted` row write at `on_step("retire-node","NodeDeleted")`.
Doc: node-rpc-envelope.md#meta-and-config: "written by the node's mesh-primary, the only writer of its mesh's rows, its own row included (the fabric-primary only as acting primary of a mesh with no primary yet)"; node-create.md step 1: "Before this step the node's row writer claims the name by insert-and-fail inside the store adapter; `NameHeld { by: node.id }` refuses a held name and nothing is launched"; node-delete.md step 3 "The node's row writer writes the node's row with status Deleted"; rulings R12+R13 "Changes" 1-3, R-OW1.
Proposal: move row writing and the name claim from the fabric-primary to the mesh-primary of the node's mesh; the fabric-primary writes only as acting primary of a mesh that has no primary. Claim at the step before launch, not after the view hears the node. The mesh-primary writes its own row (Day 0 bootstrap admin claims `(mesh1.admin, 1)` as the first write of its birth). `NodeRows` is renamed `NodeRowWriter` and owned by the mesh-primary seat.
Landing: e12.s41 #3068 (named in the rulings page: "node row and claim move to the mesh-primary, adapter claim") · durable format moved: no new row type, but who writes changes; `ApplyNodeName` wire addition already landed · no gossip change. Acceptance in rulings: "no fabric-primary writes a row of a mesh that has a primary".

### 24 · behaviour · rafka-node-admin-core/src/node_config.rs (1-60, 66-90)
Code: "`node.config.update` at the fabric-primary … The fabric-primary is its only writer"; `RejectedNotAuthority { needed: "the fabric-primary" }` for a non-fabric-primary; `PeerUnresolved` when the node is not in the fabric-primary's view; row written by the fabric-primary, then told through the node's mesh-primary by Forward.
Doc: node-rpc-envelope.md#meta-and-config: "node.config.update is a single call, not a Build: caller → the node's mesh-primary (Forward 0x1A when not directly reachable), which writes the record → ApplyNodeConfig to the node, which applies it → node.config.updated"; rulings R12+R13 "Changes" 5: "Drop the FP hop."
Proposal: serve `UpdateNodeConfig` at the node's mesh-primary from its held record (as `node_update.rs` already does for `UpdateNode`); delete the fabric-primary check and the `PeerUnresolved` guards (a mesh-primary holds the record of every member). The module and type move: `NodeConfigs` → mesh-primary config door; the `StatusRequest::UpdateNodeConfig` arm of `StatusAuthority::apply` (status_rpc.rs) moves accordingly.
Landing: e12.s36 #3062 (design line already says "caller→MP→node") · no durable move · no schema change (`UpdateNodeConfig` exists).

### 25 · behaviour · `serve_command` in `rafka-node-admin-core/src/status_rpc.rs` (529-545), `StatusAuthority::sender_of` (~365), `serve` in `node_self.rs` (213-216)
Code: a node command (`DrainNode/StopNode/StartNode`) is served only when `sender` resolves to a node-admin in the receiver's view and is not itself; otherwise `RejectedNotAuthority { SenderNotSubject { sender: "unknown peer" } }`; the commander's connection to spare is derived from that view lookup.
Doc: ops-naming "A stopped node stays reachable": "A stopped node … answers node-RPC from anyone who reaches it … Nothing checks who is calling: there is no auth layer inside the mesh, and the fence stays target node id + op"; node-rpc-envelope.md#caller-execution-and-certainty "A stopped endpoint answers anyone who reaches it: the mesh adds no generic caller check or authorship validation".
Proposal: serve the three commands on `(node_id, incarnation)` only; take the spared connection from the call's own peer (`PeerContext`), not from a view lookup; delete the `SenderNotSubject`/`ReceiverNotPrimary { needed: "the subject node itself" }` arms for commands (a wrong `node_id` is the fence's `425 STALE_TARGET`; a wrong incarnation stays `RejectedStaleIncarnation`). The same reasoning applies to `build_claim.rs`, `fabric_handover.rs, 693, 764` and `join.rs` (`Unauthorized` replies): those are outside my files, flagged for the audit owner of each op. Docs still carry `Unauthorized`/`PeerUnresolved`/`RejectedNotAuthority` variants in the schemas (envelope 490, 526, 654, 802; status NotAuthority enum), so removing the CHECK keeps the enum positions. DOC TENSION: states.md/node-status.md keep direction rules for declarations (a node declares to its own primary), which are routing, not authentication; keep those.
Landing: step 1 follow-up · new story · no durable move · no wire schema change · answers for non-admin callers change (rejection becomes execution).

### 26 · behaviour · `BookView` in `rafka-mesh-transport/src/membership.rs` `restarting` (3694-3702) and `close_restarts` (3794-3800)
Code: `restarting()` returns false when `held.node.supersedes == Some(op.incarnation)`; `close_restarts` closes an open restart when `op.incarnation != d.node.incarnation` (a later birth of the same node.id) as well as on the same incarnation's rejoin.
Doc: node-restart.md "Kept: Same node.id, incarnation"; "No supersedes lineage or new incarnation is introduced"; ops-naming "Restart is not a re-birth … the exit-and-relaunch case that rule was written for is now Rafka's recover recipe — a new node over the old data dir. supersedes lineage does not apply to it".
Proposal: delete the `supersedes` guard in `restarting` and the `op.incarnation != d.node.incarnation` arm of `close_restarts`; a different incarnation of the same node.id cannot appear under a restart (recover mints a new node.id). Keep the `supersedes` field itself: the docs still carry it (gossip.md, node-rpc-envelope.md).
Landing: step 1 follow-up under e12.s34 #3060 · no durable move · gossip behaviour (not frame) change — needs Luke's yes only if the receiver rule counts as a gossip mechanism (CLAUDE.md rule 1): treat as yes-needed.

### 27 · behaviour · `CreateRequest` in `rafka-node-admin-core/src/deployment/pipeline.rs`.restart_of (829, 1230, 1235, 1294-1300) and ::Identity.supersedes (1050, 1300), join.rs `Deployed.supersedes`, admin.rs `deployed_for*` (2833, 2890)
Code: `CreateRequest.restart_of: Option<Node>` reuses the prior node.id, mints a new incarnation and sets `supersedes`; `create_steps` keys such a run `restart-node:`. Every non-test caller passes `restart_of: None` (admin.rs → `self.create(.., None, ..)`; `admin/leave.rs`; `recover_steps` 2250).
Doc: as 26; ops-naming "Identity … restart: node.id same, incarnation same, process never exits".
Proposal: delete `restart_of`, the `supersedes` production in `AllocateIdentity`, the `"restart-node"` branch of `create_steps` and the `restart` span attribute of `pipeline_span`. `JoinRequest.supersedes`/`Deployed.supersedes` stay (field in the join schema, envelope 861) and are always `None`.
Landing: step 1 follow-up · no durable move.

### 28 · behaviour · rafka-node-admin-core/src/admin.rs boot (3588-3700, "restart" mode) and tests `node_lifecycle__admin_restart.rs`
Code: an admin started over its own `nodes.storage` row reuses its node id and starts "under a new incarnation that supersedes the one it last ran" (`rdm.node_admin.node.update.via-restart`: "restarting as the same logical node from nodes.storage").
Doc: node-admin-lifecycle.md §3 "Start modes": Day 0, Create, Recover (new NodeId and incarnation), Mesh recovery, Fabric recovery; "node.restart launches nothing". There is no mode that relaunches an exited admin as the same logical node. fabric-recovery.md does not say whether the relaunched fabric-primary keeps its node.id.
Proposal: raise to the doc owner before code: either fabric-recovery.md (and node-admin-lifecycle.md §3/§4.4 "authorized relaunch on its durable data dir") states the id/incarnation rule of a relaunched admin, or the boot path becomes Recover (new node.id). Code cannot be changed without that sentence.
Landing: doc gap first; then a behaviour story · durable format moved if node.id/incarnation rules change.

### 29 · behaviour · `reconcile_drift` in `rafka-node-admin-core/src/admin.rs` (1526-1800) and `AttemptAction::Replace` use at admin.rs, 1784, 2929
Code: the fabric-primary's drift pass opens `AttemptAction::Replace { path, from_incarnation }` (reason `ProvenDrift`) for a proven-exited node-admin of a mesh that lost its mesh-primary; the comment says "a proven-exited birth has no process to restart (node-restart.md): it is replaced".
Doc: mesh-recovery.md steps 1-2: the fabric-primary "runs node.create for the mesh's first recovering node-admin into the mesh (its MeshId, seed and mesh-primary launch mode)"; step 6 "Every node the sweep did not reach goes to node recovery"; node-admin-lifecycle.md §3 "Mesh recovery". Nothing in the docs calls this a replace.
Proposal: with finding 9 deleted, the lost-mesh arm opens the recovery action for the exited admin (node.delete of the exited instance + node.create into the mesh's own MeshId) executed by the fabric-primary; `AttemptAction::Replace`, `AttemptReason::Replace`, the `"replace"` span attribute and the `ProvenDrift` span name `rdm.node_admin.build.update.via-proven-drift` follow (rename to `…via-mesh-recovery`: mesh-recovery.md "Mesh recovery telemetry chain" lists `rdm.node_admin.node.update.via-build` and `…deployment.update.via-pipeline`, not `via-proven-drift`).
Landing: e12.s33 #3051 design line: "the FP's node-level drift Restart/Replace deleted" (not finished for the lost-mesh arm) · durable format moved (AttemptAction variants) · no wire / gossip change.

### 30 · behaviour · rafka-mesh-transport/src/membership.rs (NodeLeft arms 489-499, 1231, 1814, 1895-1918, 2647, 3298-3303), rafka-node-admin-core/src/admin.rs, rafka-node-rpc-testkit/src/rig.rs
Code: `Frame::NodeLeft` is forwarded to peer meshes, carried onto the backbone and handled by the admin's "completion frame" hook as if a node could publish it; status.rs and status_rpc.rs refuse the call.
Doc: gossip.md frame catalog: "`NodeLeft` … reserved position; never published … a stopping node gossips nothing and its mesh-primary publishes NodeStopped"; envelope: "NodeLeft keeps its position and is never sent; a receiver refuses it by name."
Proposal: keep the variant (positional) and refuse a received `NodeLeft` by name at the frame receiver (second-order rule: "a reader that does not recognise a value must REFUSE"); delete the forwarding and carry arms. The doc comment at membership.rs ("sent its `node-left` completion") is wrong text.
Landing: step 4 territory (gossip frames) — needs Luke's go-ahead for the receiver change · no durable move.

### 32 · behaviour · `retire_steps` in `rafka-node-admin-core/src/deployment/pipeline.rs` removal path vs node-delete.md "Crashed target"
Code: a delete of a birth that is not marked `parked` runs `drain_steps` then `stop_steps_with(.., Terminate)` then `TerminateRuntime`, i.e. it still sends `DrainNode`/`StopNode` to the process (to a crashed one the sends end `NotSent`, `Completion::NotAwaited`, and the run continues); only `req.node.parked` skips straight to `terminate_step` (line 1752).
Doc: node-delete.md "Operation contract": "Crashed target … Stop steps are already done; no call is required from an exited process"; ops-naming `delete`: "On a node whose process already exited (a crash), the stop steps are trivially done; the executor proves the exit and publishes deleted."
Proposal: when the provider's exact inspection already says Exited, `delete_steps` (renamed `retire_steps`) goes straight to `TerminateRuntime`/`NodeDeleted` and sends nothing, as the parked arm does, instead of attempting two calls that cannot succeed and recording their receipts.
Landing: step 1 follow-up under e12.s34 · no durable move.

### E. Admin recovery plumbing (RecoverySubmitter, reconcile_drift, restart_of)

### 34 · comment · `RecoverySubmitter` in `rafka-node-admin-core/src/admin.rs` (1105-1135), offline.rs (module doc), docs cite `OfflineTickle::tick` and `RecoverySubmitter::recover_if_proven`
Code: `RecoverySubmitter` exists; there is NO `recover_if_proven` method; node-recovery.md "Detection and proof" cites `RecoverySubmitter::recover_if_proven` in `rafka-node-admin-core/src/admin.rs`.
Doc: node-recovery.md cites a symbol the code does not have.
Proposal: either the doc cites the real symbol (docs repair, other owner) or code names the entry `recover_if_proven`; look for the method that inspects then submits (`inspect`, `submit` in admin.rsff).
Landing: doc/comment-only.

### 35 · test · rafka-node-admin-core/tests/ and rafka-test-scenario/tests asserting the old shape
- `stopped_delete.rs`: asserts the delete of a stopped birth is `[NodeDeleting, TerminateRuntime, NodeDeleted, ReleaseStorage, RemoveTopologyMembership, Complete]` (storage released) and cites `node-decommission.md`; update with findings 10, 22, 21.
- `node_rows.rs`: asserts the fabric-primary writes the rows (fixture "mesh1 has the fabric-primary `mesh1.admin.1`, its mesh primary `mesh1.admin.2`"), `create-node:`/`retire-node:`/`replace-node:`/`recover-node:` frames and `ReleaseStorage`; rewrite for the mesh-primary writer (finding 23) and for no `replace-node` (finding 9); the claim-before-launch cell is new.
- `drift_convergence.rs`: asserts `recover-node:` and `retire-node:` keys, the fabric-primary's drift on admins, and `AttemptAction::Replace` (findings 8, 9, 29); header comment cites `node-status-change.md`, `mesh-leave.md`.
- `build_admission.rs`: conformant (cites ops-naming "one Build in flight"); no change.
- `rafka-test-scenario/tests/node_lifecycle__node_replace.rs` (886 lines): drives `replace-node:` with `RenamePredecessor`, `HandoffStorage` and `recover-node:` in one file named for replace. After findings 8, 9 split it: `node_lifecycle__node_recovery.rs` (the recovery Build and its three steps) and drop the RDM replace cells (a Rafka recipe test lives with Rafka). Rename to match the docs' page names.
- `node_lifecycle__node_delete.rs`: drives `DELETE /api/nodes/...` (step 0), asserts `retire-node:` (138) and `rdm.node_admin.status.update.via-drain-node/-stop-node` (159-171); asserts storage handling? (192 steps list includes none of `ReleaseStorage`; check). Update for findings 2, 15, 22.
- `node_lifecycle__node_stop.rs`: asserts `via-stop-node`, `"stop-node:"` overlay prefix (105, 122), `kind == "node-left"` count 0 (139); conformant behaviour, update literals (findings 12, 15).
- `node_lifecycle__node_drain.rs`: asserts `drain-node:<path>` and REST `POST /api/nodes/{name}/drain`; update literals and step 0.
- `node_lifecycle__node_restart.rs` / `canary.rs`: conformant (restart is stop then start on the same process).
- `node_lifecycle__admin_restart.rs`: cites a deleted doc and drives `restart-node:`; its premise is finding 28.
Landing: rides each code story; test naming follows `<actor>_<action>_<expected_outcome>`.

### F. Step 0 (REST out) — noted, not audited here

`rafka-node-admin-core/src/http.rs` (832 lines, 14 `/api/` routes) and `axum`/`tower` in `rafka-node-admin-core/Cargo.toml` (lines 28, 46): the step-0 move. `http.rs` carries the old keys in doc comments (367, 445-453) and constructs `AttemptAction::Restart/Replace` (296, 347-348, 659, 681); they go with step 0, not step 2. About 98 files outside `src/http.rs` mention `/api/` or `axum` (tests, testkit `admin_faults.rs`, scenario tests): the CI guard in ops-naming step 0 turns on after that landing.

---

### Summary

Counts by class (numbers 31 and 33 are unused):

| class | findings |
|---|---|
| rename | 1, 2, 3, 4, 5, 6, 10, 12, 13, 14, 15, 16 (12) |
| behaviour | 7, 8, 9, 17, 18, 19, 22, 23, 24, 25, 26, 27, 28, 29, 30, 32 (16) |
| comment | 20, 21, 34 (3) |
| test | 35 (1) |

### Renames table (old → new, all places)

| old | new | where | step |
|---|---|---|---|
| `create-node:<p>` | `node.create:<p>` | build.rs, pipeline.rs, workflow.rs, build_run.rs, accepted tests, ~20 test files | 2 |
| `restart-node:<p>` | `node.restart:<p>` | build.rs, pipeline.rs/1742, build_state.rs, stopped_gc.rs, lifecycle.rs, membership tests | 2 |
| `drain-node:<p>` | `node.drain:<p>` | calls.rs, accepted.rs, node_commands.rs, status.rs doc comments, gossip.md/343/351/417 (doc text) | 2 |
| `stop-node:<p>` | `node.stop:<p>` | calls.rs, accepted.rs, node_commands.rs/132, stopped_gc.rs, wire.rs, lifecycle.rs, membership.rs, admin.rs/2802 | 2 |
| `start-node:<p>` | `node.start:<p>` | calls.rs, accepted.rs, node_commands.rs, stopped_gc.rs | 2 |
| `retire-node:<p>` | `node.delete:<p>` | build.rs, pipeline.rs, node_rows.rs/96, build_state.rs, workflow.rs `retire_rows` | 2 |
| `replace-node:<p>` | removed (Rafka recipe) | build.rs, pipeline.rs, node_rows.rs, build_state.rs | new story |
| `recover-node:<p>` | `node.delete:<p>`, `node.create:<p>`, `node.update:<p>` (steps) | pipeline.rs, node_rows.rs/103, build_state.rs, admin.rs | story s33 |
| `shutdown-mesh:<id>` | `mesh.delete:<id>` | build.rs, membership tests (mesh audit) | 2 |
| `create-mesh:<m>` | (no mesh create call; mesh audit) | build.rs | — |
| `node-drained` / `node-left` / `node-started` (completion tokens) | `node.drained` / `node.stopped` / `node.started` | node_commands.rs, span attr `command`/`completion` | 2 |
| `StatusRequest::op()` `drain-node`,`node-drained`,`stop-node`,`node-left`,`start-node`,`update-node` | `node.drain`,`node.drained`,`node.stop`,`node.stopped`(never sent),`node.start`,`node.update` | status.rs | 2 or 3 |
| variants `DrainNode`,`NodeDrained`,`StopNode`,`NodeLeft`,`StartNode`,`LeaveMesh`,`MeshLeave` | unchanged until the 0x1B split | status.rs | 3 |
| `rdm.node_admin.status.update.via-drain-node` / `-stop-node` / `-start-node` | `rdm.node_admin.node.drain.via-command` / `node.stop.via-command` / `node.start.via-command` | node_self.rs/427/505 + tests | 2 |
| `rdm.mesh.connection.delete.via-channel-parked` | `…via-channel-stopped` | membership.rs | with finding 20 |
| `RetireKind` / `RetireRequest` / `retire()` / `RetireNode` / `RetireStep` | `DeleteRequest` / `delete()` / `DeleteNode` / `NodeStep…`; `RetireKind` deleted | pipeline.rs, admin.rs, accepted.rs, build.rs, ~12 tests | code-only |
| `Parking`/`Cycle::Parked`/`is_parked`/`ParkGate`/`Node.parked` | stopped vocabulary; wire tokens `CurrentParked`/`Current { parked }` keep | node_self.rs, model.rs, park_gate.rs, ~30 files | code-only |
| doc cites `mesh-leave.md`, `node-decommission.md`, `fabric-primary-handover.md`, `fabric-open-traffic.md`, `node-status-change.md`, `node-ready.md`, `node-lifecycle-elections.md`, `node-build-hydration.md`, `node-birth.md`, `fabric-mesh-ops.md`, `fabric-node-lifecycle-events-ops-gossip.md`, `node-rpc-rdm-ownership.md`, `fabric-node-lifecycle.md §7.3 / §4.3 (as the tickle / the writer ladder)` | see finding 21 | ~60 comment sites | comment-only |

Step ledger. Step 2 (`durable format moved`): findings 1-6, 12-14 and the spans of 15; it also carries finding 7 and 8 (their operation strings). Step 3 (0x1B split): only the variant names, nothing else in this area needs it. Step 4 (gossip frame names, Luke's go-ahead): none of the frame names change in this area; findings 12 (frame CONTENT string) and 30 and 26 touch gossip receiver behaviour and need Luke's yes.

### Behaviour changes, largest first
1. Finding 9: delete the RDM replace path (`ReplaceNode`, `RenamePredecessor`, `HandoffStorage`, drift Replace); replace is a Rafka recipe over `node.create(<name>.new)`/`stop`/`update`/`delete`. DOC CONFLICT with the envelope's `BuildChange::Replace`.
2. Finding 23: node rows and the name claim move from the fabric-primary to the mesh-primary; claim before launch (s41 #3068).
3. Finding 8: recovery as `node.delete` + `node.create` + `node.update` with the caller-owned meta copy (s33 #3051).
4. Finding 7: restart is ONE operation `node.restart:<path>`; today its stop half is receipted under `retire-node:` (s34 #3060 follow-up).
5. Finding 22: `node.delete` keeps storage; remove `ReleaseStorage` (and its `remove_dir_all`).
6. Finding 18: start order join → hook → topology/floor/rafka-time last.
7. Finding 24: `node.config.update` served at the mesh-primary, FP hop dropped (s36 #3062).
8. Finding 29: lost-mesh drift opens a recovery, not a Replace (s33).
9. Findings 26, 27, 28: remove the vestigial exit-and-relaunch restart (supersedes lineage, `restart_of`, admin boot restart mode — 28 needs a doc sentence first).
10. Finding 25: drop the sender gate on node commands (no auth layer inside the mesh).
11. Findings 17, 19: the stopped frame / started frame are the node's only reports; mesh-primary sets Leaving / Ready.
12. Finding 32: delete of a crashed process skips the stop sends.
13. Finding 30: refuse a received `NodeLeft` by name; delete its carry arms (needs Luke).

### Questions that need the doc owner before code (listed once)
- Q1 (finding 9): `BuildChange::Replace` is in the envelope's Build schema but node-replace.md says there is no replace opcode.
- Q2 (finding 8): step keys of the recovery Build's three steps are not written in the docs.
- Q3 (finding 28): what node.id / incarnation a relaunched node-admin holds (fabric-recovery.md and node-admin-lifecycle.md §3 are silent).
- Q4 (doc text): gossip.md, 343, 351, 417-418 and node-rpc-envelope.md/351 still show `drain-node:` / `stop-node:` as the operation strings while gossip.md, 430, 491 show `node.delete:`, `node.stop:`, `node.restart:`; the corrected docs contradict themselves on `NodeDraining`/`NodeLeaving`/`NodeDrained` operation strings.
- Q5 (finding 34): node-recovery.md cites `RecoverySubmitter::recover_if_proven`, which does not exist.
- Q7 (rollout wording): ops-naming "Rollout" step 2 labels the key rename `durable format moved` but does not say it also changes gossip frame content (finding 1); CLAUDE.md rule 1 makes a field value peers parse a gossip mechanism, so step 2 needs Luke's yes, not only a commit marker.
- Q6 (restart gossip): node-restart.md says the mesh-primary publishes NodeRestarting at step 1 AND instead of NodeStopped for the stop's stopped; code publishes it once, as the pre-event (`pipeline.rs` 1740-1750, `StopEnd::Park { publish_stopped: false }`). Say whether the second publication is an observable event.

---

## CODE-3 — fabric, rounds, elections, storage, REST (RDM code vs the corrected docs)

READ-ONLY audit. Code: `/home/admin/rust-distributed-mesh` (HEAD a207a281). Docs: `/home/admin/docs-pass`
(`ops-naming-proposal.md`, `docs/architecture/**`). All paths below are under
`crates/rafka-node-admin-core/` unless a full crate path is given. Line numbers are at that HEAD.

Reading rules used: a finding is only written when I opened the code at the cited line and the doc at
the cited section. Items I could not settle from the text are marked FLAG and sit in the last section.

Order of the sections: A shutdown family (fabric.delete), B REST out of RDM (rollout step 0),
C investigation, D rounds and storage, E elections, F status op, G tests, H comments/citations,
I docs that disagree with each other (for the lead), J verified conformant.

---

### A. FabricShutdown family → `fabric.delete`

### 1 · behaviour · src/shutdown.rs (whole file, 274 lines) + `shutdown` in `src/http.rs` (l.779) + `AdminStopper` in `src/admin.rs` (l.3273-3292) + src/admin.rs l.5486-5525
Code: `//! Only the fabric-primary accepts /api/shutdown. It persists FabricShutdown through fabric.storage and disseminates it on the fabric control channel. Every admin that learns it ... stops reconciling ... and gossips Draining.` Then `drain()` waits for `freeze_barrier` (every live admin `Draining`), each mesh-primary stops "ordinary members, then its non-primary admins" by `provider.terminate(&handle, Graceful{grace: 8s})` (`AdminStopper::stop`), the fabric-primary waits until each mesh holds "no runtime but its mesh-primary", terminates the other mesh-primaries, and the whole thing ends with `control.shutdown.notify_waiters()` which makes the fabric-primary leave itself (`entry.rs` l.83-109).
Doc: `lifecycles/fabric-delete.md` "Operation contract": steps are "mesh.delete of each mesh, the fabric-primary's own mesh last; then node.delete of each other mesh's mesh-primary, after its mesh.deleted"; "Completion: fabric.deleted, reported by the fabric-primary, still alive"; "After completion: a separate node.delete of the fabric-primary, called from outside the fabric"; "Exit proof: Every runtime the workflow deletes has its exact provider exit proof". `ops-naming-proposal.md` FABRIC / delete: "old fabric shutdown (FabricShutdown, leaf-inward ...)". `states.md` "Rounds"/"Leadership and draining": "A local OS signal or a node stop is node lifecycle and starts no fabric.delete" (fabric-delete.md l.39).
Proposal: delete `shutdown.rs` (`ShutdownControl`, `ShutdownPhase`, `Incomplete`, `ShutdownProgress`, `Publish`, `freeze_barrier`, `own_mesh_drain`, `drained_to_primaries`, `drain_bound`, `Stopper`, `drain`, `stop_and_wait`), the `/api/shutdown` route and `ShutdownSeat`/`ControlPlane::fabric_shutdown`/`ControlPlane::shutdown`, and `AdminStopper`. Replace with the `fabric.delete` workflow on the Build family: the fabric-primary is the driver and runs one Build whose steps are `mesh.delete` per mesh (own mesh last, that mesh.delete skipping its `fabric.primary.update` step), then `node.delete` of each other mesh-primary after its `mesh.deleted` (fabric-primary executor, exact exit proof), ending `fabric.deleted` (or `Failed { step, reason }`); the fabric-primary is then deleted by a separate `node.delete` from outside. No every-admin Draining gossip, no freeze barrier, no direct `provider.terminate` (members die through `node.delete` steps with their `node.stopped` frame, row `Deleted`, `NodeDeleted`). The 60 s `drain_bound` ("a bound that fires is evidence") disappears with it: `Failed{step}` names the runtime that did not exit.
Landing: new behaviour story (no PRD row covers fabric.delete; PRD §5.5 still describes FabricShutdown). It cannot land before `mesh.delete` (e12.s39 #3066, s40 #3067, s49 #3078 "mesh.delete ends at mesh.deleted"), `node.delete` with exit proof (e12.s34 #3060, s41 #3068) and the seat skip of `fabric.primary.update` (e12.s38 #3065, s30 #3043). Size: shutdown.rs 274 lines + ~20 production sites + 11 test files (finding 8). Wire/gossip/durable: YES — it removes the `shutdown` field from the Build-topic message and from the join answer, and the `shutdown.json` row (findings 3, 4): durable format moved, gossip change = Luke's permission first (CLAUDE.md "no change to gossip or the mesh"), and the proposal needs its Jaeger flame before it is a recommendation.

### 2 · behaviour · src/admin.rs (freeze + hold-seats plumbing) — l.324-336 `Seats`, l.512-526 `Records::hold_seats/held_seats`, l.900-925 projection override, l.1440 `SeatGate::frozen`, l.5424-5441 drift loop `frozen.held()`, l.5471-5482 stopped-node sweep `frozen.held()`; src/build_drive.rs l.47-49 `Gate::frozen`, l.516 `DriveEnd::Paused`
Code: `/// ... A fabric shutdown runs no election because admins go Draining: when this admin learns one it holds the seats of its last view in which no admin was Draining` (admin.rs l.324-329); `records_w.hold_seats(); abort.abort(); runs_w.abort_all(); d.status = MemberStatus::Draining; membership_w.publish(&d)` (l.5512-5518).
Doc: `fabric-node-lifecycle-elections.md` §6 "fabric.delete moves no seat: A seat moves only by its holder's yield, a committed transfer (§4.4) or a proven loss, never by a status (§5)"; `states.md` "Leadership and draining": "Node status alone does not surrender a primary seat." There is no freeze state in any lifecycle page.
Proposal: delete the freeze machinery with finding 1: `Seats`/`Records::seats`, `hold_seats`, `held_seats`, the override block in `project_decided_at`, `SeatGate::frozen` + `Gate::frozen` + `DriveEnd::Paused` (if nothing else produces Paused), the two `frozen.held()` early returns, the `Draining`-publishing task. The "hold the seats so a Draining status does not move them" mechanism is a coping layer for a status-moves-seat flaw that e12.s23 (#3016 sticky seat: "holding() ignores status") removes at the root (election.rs already does: `Holding::Yielded` only for replaced/gone births, l.102-116). After s23 and fabric.delete there is nothing for it to cover.
Landing: part of finding 1's story (delete after s23 #3016 is on main) · no durable/wire/gossip change by itself.

### 3 · behaviour (durable) · src/fabric_storage.rs — l.1-8 module doc, l.232-246 `FabricShutdown`, l.261/264 `FabricStorage::{shutdown, put_shutdown}`, l.289/318-322 `MemoryFabricStorage`, l.369-377 `SHUTDOWN_KEY`/`SHUTDOWN_FORMAT = "fabric-shutdown/1"`, l.495-500 `FileFabricStorage`, tests l.534-547/680-687
Code: `/// Hold shutdown if none is held (insert-and-fail on its key): the first shutdown held is kept, a later one never replaces it, and it is never removed.` The file is `<store root>/fabric/shutdown.json`.
Doc: `mesh-control-plane.md` §5.3: "`fabric.storage` holds the Fabric as an immutable identity row, `FabricIdentity { fabric_id, name }`, written once, and one pointer row, `FabricPointer { build_id, submitted_at_ms }`, per move of `Fabric.build_id`" — no shutdown row; `fabric-node-lifecycle.md` §2.8: the fabric row "Nothing writes the row again until fabric.delete".
Proposal: remove `FabricShutdown`, the two trait methods, both impls, `SHUTDOWN_KEY/FORMAT`, the module-doc paragraph (l.4-8), the `a_held_shutdown_is_never_replaced_or_removed` tests; follow through: `rafka-node-rpc-testkit/src/admin_faults.rs` l.423-426, `tests/fabric_pointer_rows.rs` l.39-42, `Wiring.fabric_storage` doc (wiring.rs l.121, "before the accepted-Build pointer and the shutdown control take it"). `fabric.delete` writes the fabric row's end state through the ordinary fabric write path (what that row records at the end is for the story; the docs say only "until fabric.delete").
Landing: with finding 1 · durable format moved (a `fabric-shutdown/1` file on a store root becomes an unrecognised file, which `FileFabricStorage::open` refuses by name — acceptable only because the rule is rebirth). No wire/gossip.

### 4 · behaviour (gossip + wire) · src/fabric_builds.rs l.52-54 `BuildMessage.shutdown`, l.71-73 `encode_shutdown`, l.287/305/322-325/349-360 (receive + neighbour-up replay), l.484-488 `publish_shutdown`; src/wire.rs l.687/697/706 `WireMessage.shutdown`, l.733-734/795/815 `JoinControl.shutdown`; src/admin.rs l.2224 and l.4089-4092 (join answer carries and hydrates it); src/fabric_handover.rs / `rafka-mesh-entity` unchanged
Code: `/// A fabric shutdown in force: Fabric control state, sent alone when initiated and to a new neighbour first (fabric-mesh-lifecycle.md §11.1). Never a Build fact.`
Doc: no frame in `gossip.md` §3.1 frame catalog carries a shutdown; `node-rpc-envelope.md` l.837 still says the JoinNode answer's control holds "provider, optional Fabric and shutdown facts" (conflict, section I-3).
Proposal: remove the `shutdown` field from `BuildMessage`, `WireMessage` (postcard positional), `JoinControl`, the `reject.via-shutdown-unpersisted` span and the replay-first branch in the neighbour-up catch-up. The envelope text at l.837 changes in the same landing.
Landing: with finding 1 · durable format moved (Build-topic frame and join answer are positional postcard) · gossip change AND wire change: needs Luke's permission before any edit.

### 5 · rename/behaviour (wire) · rafka-node-rpc-contract/src/status.rs l.64-82 `FabricEvent::ShutdownInitiated { initiated_by }`, `FabricEvent::name()`; src/status_rpc.rs l.309-332 (`ApplyFabricEvent` arm, `event_key`); goldens `rafka-node-rpc-contract/tests/status_wire.rs` l.31, l.163; status.rs l.938
Code: `FabricEvent::ShutdownInitiated { .. } => "shutdown-initiated"`; `event_key`: `format!("shutdown-initiated:{initiated_by}")`.
Doc: `ops-naming-proposal.md` FABRIC / status: "the fabric's status, set only by the fabric-primary (old ApplyFabricEvent)"; fabric.delete replaces FabricShutdown. `node-rpc-envelope.md` l.581-584 still lists `FabricEvent { Ready, ShutdownInitiated }` (conflict, I-1).
Proposal: remove the `ShutdownInitiated` variant (leaving `FabricEvent::Ready`), `event_key` arm, golden vectors; if the ops-naming "old ApplyFabricEvent" is the target, `ApplyFabricEvent` and `FabricEvent` leave with the 0x1B split (finding 24).
Landing: rollout step 3 (0x1B split = wire contract move) — the variant itself may go earlier with finding 1 because nothing else sends it · wire change, positional enum (removing the last variant does not move the earlier ones' discriminants) · no gossip.

### 6 · rename · shutdown span / env / harness names
Code: spans `rdm.node_admin.fabric.update.via-shutdown`, `-via-shutdown-learned`, `-via-shutdown-frozen`, `-via-shutdown-drain`, `-via-shutdown-incomplete`, `rdm.node_admin.fabric.reject.via-shutdown-not-authority`, `-via-shutdown-unpersisted`; env `RDM_SHUTDOWN_DRAIN_BOUND_MS` (shutdown.rs l.183, `rafka-test-scenario/tests/rshape_burn_in.rs` l.714-740, `src/estate.rs` l.1066); `rafka-test-scenario/src/wedge.rs` l.91/117/143 `Family::FabricShutdown` ("fabric-shutdown"); `/api/fabric` field `shutdown` (http.rs l.718-720).
Doc: span grammar in `ops-naming-proposal.md` "How an op works": `rdm.node_admin.<noun>.<verb>.via-…`; every step visible with its own span.
Proposal: all `via-shutdown*` spans die with finding 1; `fabric.delete` steps emit `rdm.node_admin.fabric.delete.via-…` (step names from the Build) and `mesh.delete`/`node.delete` spans of their own pages; `RDM_SHUTDOWN_DRAIN_BOUND_MS` deleted (rshape `provider_left` waits on the fabric.delete terminal frame plus the exit proof instead of "three drain bounds"); `Family::FabricShutdown` → `Family::FabricDelete` (`"fabric-delete"`), and `ExactRuntimeExit` cell reads stay.
Landing: with finding 1 · no durable (spans are strings someone greps: note in the commit) · no wire.

### 7 · behaviour · src/entry.rs l.74 `println!("RDM_NODE_ADMIN_API_BASE=...")`, l.77-109 fabric-shutdown wake, l.121-131 `rdm.mesh.node.delete.via-signal` with `fabric_shutdown` field
Code: `// Leave when this admin's part of a fabric shutdown is done (the fabric-primary, after it has stopped the mesh-primary spine; fabric-mesh-lifecycle.md §11.1), or on a signal.` `let fabric_shutdown = tokio::select! { _ = &mut by_route => true, () = stop_requested() => false, ...}`; `running.leave().instrument(info_span!("rdm.mesh.node.delete.via-signal", fabric_shutdown))`.
Doc: `fabric-delete.md`: the fabric-primary stays alive after `fabric.deleted` and ends only by a separate `node.delete` from outside; `node-stop.md`/`node-delete.md` own how a process ends (stop leaves a live stopped process; delete proves exit). `states.md` "Stopped nodes".
Proposal: delete the `control.shutdown` Notify branch and the `fabric_shutdown` span field; the process ends on the signal (local OS stop) or on its own `node.delete`. The line-74 stdout handoff is finding 11.
Landing: with finding 1 · none of durable/wire/gossip.

### 8 · test · tests that name or assert the FabricShutdown shape
- `tests/control_routes.rs` l.294 `shutdown_and_runtime_fault_routes_stay_outside_build`, l.322-340 `only_the_fabric_primary_begins_a_fabric_shutdown` (also REST, finding 15).
- `tests/client_contract.rs` l.53 and `tests/receipt_refusal.rs` l.282, `tests/recovery_admission.rs` l.246, `tests/build_catch_up.rs` l.32, `tests/build_takeover.rs` l.167 (`ShutdownControl::memory` as a constructor argument).
- `tests/join_wire.rs` l.6/16, `tests/i143_acceptance_rw1.rs` l.25/317 (`FabricShutdown` in a join answer / Build topic), `tests/i143_acceptance_2805.rs` l.787-810 (`FabricEvent::ShutdownInitiated` sent as the "new work" of a fabric-primary-move cell: replace the second event with another legal natural key or a second `Ready` repeat).
- `rafka-node-rpc-contract/tests/status_wire.rs` l.31/163 goldens.
- `rafka-test-scenario/src/estate.rs` l.1008-1075 `Estate::stop` (POST `/api/shutdown` to the fabric-primary, waits for the bootstrap admin), l.1288-1300 `Drop` (raw HTTP `POST /api/shutdown`); `tests/rshape_burn_in.rs` l.714-740; `src/wedge.rs` l.91/117/143.
- `src/admin.rs` l.6466-6520 `a_fabric_shutdown_keeps_every_seat_with_its_holder_and_re_elects_nothing` (tests `hold_seats`, finding 2).
Proposal: delete or rewrite each with finding 1; `Estate::stop` becomes: call `fabric.delete`, await its terminal frame, then `node.delete` the fabric-primary from outside, then wait for exit proof. The `Drop` best-effort stop becomes process-level (`kill`) only, as for non-bootstrap estates.
Landing: with finding 1.

### 9 · comment · citations of the removed mechanism
`src/shutdown.rs` l.1 `fabric-mesh-lifecycle.md §11.1, FML-21..27`; `src/fabric_storage.rs` l.2; `src/fabric_builds.rs` l.53, l.484; `src/entry.rs` l.77; `src/http.rs` l.777; `src/admin.rs` l.3956, l.5437, l.5510 (`FML-26`), l.6466 (`FML-26`); `src/election.rs` l.689-693 (test comment naming "FabricShutdown this observer has not installed yet"); `tests/rshape_burn_in.rs` l.5235 (`FML-19`).
Doc: §11.1 of `fabric-mesh-lifecycle.md` is now "fabric.delete" (a pointer to `lifecycles/fabric-delete.md`); no `FML-n` identifiers exist anywhere in the docs (`grep FML-` over docs-pass: none).
Proposal: these comments go with their code (finding 1). The ones that survive: `rshape_burn_in.rs` l.5236 `FML-19` ("a same-path replace against an unchanged accepted topology opens a new attempt of the Build `Fabric.build_id` names") → `mesh-control-plane.md` §4.1 + `fabric-mesh-lifecycle.md` §13; `election.rs` l.688-693 test comment → "a Draining holder keeps both seats" with the gate reference only.
Landing: comment-only for the survivors; rest with finding 1.

---

### B. REST out of RDM (rollout step 0)

### 10 · behaviour · src/http.rs (832 lines) — it is two things, and only one of them moves
Code: `pub fn router(cp, runtime_routes) -> Router` with 14 routes (l.814-833: `/api/build`, `/api/builds`, `/api/nodes`, `/api/nodes/spawn`, `/api/nodes/{name}`, `.../restart|replace|drain|stop`, `/api/meshes`, `/api/meshes/{id}`, `/api/fabric`, `/api/connections`, `/api/shutdown`), the `axum` handlers l.620-812, `impl IntoResponse for Refusal` l.540-572. But the same file holds `ControlPlane` (l.35-139), `ControlPlane::{authority, current_settled, submit, submit_in, submit_checked, open_attempt, open_attempt_in, open_recovery, forget_build}` (l.113-470), `Refusal`, `Opened`, `Recovery`, `ActionKind`, `reject_span`, `last_admin`. Those non-REST parts are used by `src/build_op.rs` l.21/83/122 (the 0x20 Build carrier), `src/stopped_gc.rs` l.13/205/330, `src/admin.rs` l.30/1228/3305/4175.
Doc: `ops-naming-proposal.md` "RDM has no REST (target)": "no RDM crate serves HTTP, depends on an HTTP server, or defines a route"; Rollout step 0: "`http.rs` ... move to the demo crate as thin callers over the typed objects"; `mesh-control-plane.md` §2: "RDM creates, drives, reads and deletes Builds only through the Build carrier". The Build admission rules (one Build in flight, `BuildInProgress { build_id }`, fabric-primary only, last-admin refusal) are not REST.
Proposal: step 0 is a split, not a move of `http.rs`. (a) Rename the module `http` → `control` (or `front_door`): it keeps `ControlPlane`, `Refusal` (without `IntoResponse`), `Opened`, `Recovery`, `ActionKind`, `reject_span`, `last_admin`; the span in `submit` (`rdm.node_admin.build.create.via-rest`, l.146) and `open_attempt` (`...build.update.via-rest`, l.265) rename to `via-node-rpc` (the carrier already uses `rdm.node_admin.build.update.via-node-rpc`, build_op.rs l.92). (b) The `axum` handlers, `router`, `SpawnBody`, `BuildQuery`, `ReplaceQuery`, `get_nodes`, `get_mesh`, `get_fabric`, `get_connections`, `IntoResponse for Refusal` move to the demo (its `admin-ui` and `src/bin/rshape_node_admin.rs` already hold an axum surface). (c) Delete the shutdown-specific `ShutdownSeat` (finding 1). (d) The ops-naming citation `http.rs` for `BuildInProgress` repoints to the new module (`ControlPlane::current_settled`).
Landing: rollout step 0 · none of durable/wire/gossip. Precondition from `src/objects`: the typed reads `node.get`, `node.connections.get`, `mesh.get`, `fabric.get` must exist over node-RPC first (finding 13), or step 0 deletes the only way to read a view.

### 11 · behaviour · REST serving and its config — src/admin.rs l.30 `use crate::http::{router, ControlPlane}`, l.81-82/133-136/197 `AdminConfig.api_bind` (`RDM_NODE_ADMIN_API_BIND`), l.3303 `Running.api_base`, l.3665-3702 and l.4455 (`control` listener slot), l.5528-5532 `axum::serve`, l.5534 `node-admin.json`; src/entry.rs l.74; src/deployment/endpoint.rs l.32 `NODE_ADMIN: KindSpec { listeners: &["control"] }`; src/deployment/pipeline.rs l.1010; rafka-mesh-entity/src/launch.rs l.232; src/model.rs l.130-135 `admin_api_base`/`listeners`; src/storage.rs l.119-121 `NodeRecord.listeners`; Cargo.toml `axum = "0.8.9"`, dev `tower`, `http-body-util`; RDM `CLAUDE.md` l.370 and l.393 (env table)
Code: `let app = router(control.clone(), axum::Router::new()); ... axum::serve(listener, app)`; `"RDM_NODE_ADMIN_API_BASE={}"`; `json!({ "api_base": api_base, "node": name })` written to `node-admin.json`.
Doc: "RDM has no REST": after step 0 `grep -rn 'Router\|axum\|"/api/' crates/rafka-node-admin*` returns nothing; "`admin_api_base` ... is the demo's field".
Proposal: remove the bind, the listener, `axum::serve`, `api_base`, `node-admin.json`, the stdout line, the `control` listener slot and `KindSpec.listeners` for node-admin, `Node.listeners`/`NodeRecord.listeners` (serde `default`, JSON row: read-compatible, writing simply stops), the three env names `RDM_NODE_ADMIN_API_BASE`, `RDM_NODE_ADMIN_API_BIND` and the table rows in RDM `CLAUDE.md`. `rafka-test-scenario/src/estate.rs` l.67/279/204 parses that stdout line and sets the bind: it needs another handover (finding 15). `tools/test-runner/src/lib.rs` l.50-51 strips both env names.
Landing: rollout step 0 (the `admin_api_base` field itself: the later "config in the digest" landing, finding 12) · `NodeRecord.listeners` is a JSON row field with `#[serde(default)]`: removing it is read-compatible, so NO durable marker for it · no wire/gossip.

### 12 · behaviour · `admin_api_base` is checked by RDM — src/admin.rs l.2007-2009, src/topology.rs l.270-272 (+ typed field in src/topology.rs l.105/124/221/226/307/320, src/model.rs l.130, src/reenter.rs l.45/71/96/119/146 `MapNode.admin_api_base`, src/fabric_handover.rs l.198/617, rafka-mesh-entity/src/digest.rs l.53, wire.rs l.44/79/116, rafka-mesh-transport/src/membership.rs l.585/1006/2030/3352 `Frame::NewFabricPrimary { admin_api_base }`)
Code: `if node.kind == NodeKind::NodeAdmin && d.admin_api_base.is_none() { return Err(format!("{} serves no control API yet", node.name)); }` (a node-admin is not Ready until it advertises a REST base); `V::AdminPrimaryWithoutControlEndpoint` in `Topology::violations`.
Doc: ruling R-OW1 (`i143-rulings-2026-10-11-dev.md`): "`admin_api_base` is config, never checked"; ops-naming "Node data": "RDM never reads config to decide anything ... Config on the row, `admin_api_base` included, is stored and gossiped as given; nothing checks it"; `mesh-control-plane.md` §14: "`admin_api_base` is application config on the node record".
Proposal: delete the Ready gate and the `AdminPrimaryWithoutControlEndpoint` violation (they are checks on a value RDM must not read; with no REST in RDM no admin has the value at all, so the gate would block every Ready). Then the "config in the digest" landing: `admin_api_base` leaves `MeshDigest` (digest.rs l.53) and `NewFabricPrimary`'s contact addresses and becomes a config key the demo owns; `extra` renamed `config`.
Landing: gate/violation deletion = step 0 (must land with finding 11 or Ready breaks); the field removal = the separate post-step-0 "config in the digest" landing · durable format moved (positional postcard) · gossip change (`NewFabricPrimary` frame, `MeshDigest`): Luke's permission.

### 13 · behaviour · rafka-node-admin-client — src/lib.rs (REST client, l.380-530: `NodeAdminClient::{spawn, remove, restart, drain, stop, replace, replace_birth, build, build_view, forget, nodes, mesh, fabric, create_mesh, remove_mesh, shutdown, connections}`; 17 `Router|axum|"/api/` hits, `reqwest` dep) and src/objects.rs l.264-300 `Nodes { admin: NodeAdminClient, carrier }`, l.309-335 `Nodes::select`, l.355-365 `connections_get`
Code: `/// The node objects: workflows through the fabric-primary's Build family, reads over one admin's control API.` `let all = self.admin.nodes().await?;`
Doc: step 0 guard also covers `crates/rafka-node-admin*` (the glob includes this crate); ops-naming "Node": `node.get -> reply` (by id, by mesh or by tags), `node.connections.get`; Mesh `mesh.get`; Fabric `fabric.get`, `fabric.meshes.get`, `fabric.nodes.get`.
Proposal: the typed `Nodes::get` and `connections_get` read REST today, and no `Meshes`/`Fabrics` objects exist. Before step 0: node-RPC read ops for node/mesh/fabric views and connections (the node-RPC side of `get_nodes`, `get_mesh`, `get_fabric`, `get_connections`, including load and gossip stats), the typed objects over them, then delete `NodeAdminClient`'s HTTP methods and the `reqwest` dependency, keeping the DTO types (`NodeView`, `MeshView`, `FabricView`, `ConnectionsView`) the objects return. The client's doc l.1-9 ("The node-admin control API, as a client sees it") is rewritten.
Landing: the node-RPC reads and typed objects are step 1 (which lands first, per Rollout and cost); step 1 as written names workflows and Build 0x20 but not these read ops, so the doc should add them to step 1. The HTTP deletion is step 0 and needs them. New wire ops for the reads: wire change · gossip none.

### 14 · behaviour · rafka-node-rpc-testkit/src/admin_faults.rs l.18-20/620-669 (`axum::Router`, `router(faults)`, `axum::serve`), Cargo.toml l.14 `axum`; src/node.rs / src/rig.rs only use iroh's `Router`
Code: `Router::new()...` fault-injection routes served beside the admin.
Doc: ops-naming step 0 lists `rafka-node-rpc-testkit/src/admin_faults.rs`; `mesh-control-plane.md` §2: "Application bootstrap and fault injection are application work, not Build calls."
Proposal: move to the demo as listed; `runtime_routes: Router` parameter of `router()` (http.rs l.814) goes with the split (finding 10).
Landing: step 0.

### 15 · test · who else drives RDM over HTTP — not all in the step-0 list
Code/facts: in `crates/rafka-node-admin-core/tests` the REST users are exactly `control_routes.rs` (65 axum/`/api/` hits), `provider_policy.rs` (8), `client_contract.rs` (2) and `drift_convergence.rs` (2) — the four the doc lists. NOT in the doc's list: `crates/rafka-test-scenario` (65 files, 475 `/api/` occurrences; `src/estate.rs` `Estate::{get, post, delete, bootstrap, stop}` with `reqwest`, `src/runner.rs`, `src/soak.rs`, `src/canary.rs`, and every `tests/*.rs` that calls `estate.post("/api/build" ...)`, `estate.post("/api/nodes/{n}/restart" ...)`, `estate.delete("/api/nodes/{n}")`, `estate.get("/api/fabric" | "/api/nodes")`), plus 4 testkit/process tests that set `RDM_NODE_ADMIN_API_BIND` (`tests/store_root_boot.rs` l.20/106, `tests/restart_hold_from_facts.rs` l.37, `tests/park_gate_view_lock.rs` l.27, `rafka-node-rpc-testkit/tests/hydrate_before_ready_admin.rs` l.21, `member_cert_admin.rs` l.25).
Doc: step 0 names four test files and the testkit; the "grep returns nothing" guard covers `crates/rafka-node-admin*` only.
Proposal: state in step 0 that `rafka-test-scenario` is the largest REST user and says where it goes (typed objects, the demo's routes, or both): without that, deleting the RDM routes turns ~65 scenario files red. The scenario `Estate` is a REST client of the very surface step 0 removes. FLAG for the lead: which way it goes is a doc decision, not mine.
Landing: step 0 (size: L).

### 16 · behaviour(doc guard) · the step-0 CI guard `grep -rn 'Router\|axum\|"/api/' crates/rafka-node-admin*`
Code: 12 test files and `src/admin.rs` (l.37 `use iroh::protocol::Router as IrohRouter`, l.3318, l.3794) contain `Router` from **iroh**, not axum: `tests/admin_serves_forward.rs`, `build_catch_up.rs`, `build_claim.rs`, `build_facts_read.rs`, `build_takeover.rs`, `fabric_handover.rs`, `i143_acceptance_2805.rs`, `node_objects.rs`, `node_rpc_objects.rs`, `receipt_refusal.rs`, `recovery_admission.rs`, `status_declarations.rs`, `topology_read.rs`.
Doc: "When this lands, `grep -rn 'Router\|axum\|"/api/' crates/rafka-node-admin*` returns nothing, and that grep becomes the CI guard."
Proposal: the guard as written can never pass: change the pattern to `axum\|"/api/\|axum::Router` (or exclude `iroh::protocol::Router`) in `ops-naming-proposal.md` Rollout step 0 before it is wired to CI.
Landing: doc fix with step 0.

### 17 · rename · REST-named spans — `via-rest`
Code: `rdm.node_admin.build.create.via-rest` (http.rs l.146), `rdm.node_admin.build.update.via-rest` (l.265); asserted in 20 files / 58 occurrences (`tests/control_routes.rs` l.153/179/205, `tests/drift_convergence.rs` l.1087, `rafka-test-scenario/tests/*`, `rshape_burn_in.rs` doc l.19 "REST `build.create.via-rest`").
Doc: no REST in RDM; the carrier's span is `rdm.node_admin.build.update.via-node-rpc`.
Proposal: `via-rest` → `via-node-rpc` in both spans and in the 20 files that assert them.
Landing: step 0 · none of durable/wire/gossip (span strings someone greps: commit message says so).

### 18 · comment · REST citations
`src/http.rs` l.1 `The generic control API (PRD §7; mesh-control-plane.md §2)`: §2 is now "Public Build calls" (the 0x20 carrier) → deleted with the split. `rafka-test-scenario/src/lib.rs` l.5 and `src/estate.rs` l.3 cite `docs/i143/design.md` (§4 "node-admin control API"): that file does not exist in either repo → repoint to `docs/architecture/node-rpc-envelope.md#build-op-0x20-rdm` after step 0. `src/accepted.rs` l.163-190, `src/build.rs` l.120-167, `src/build_state.rs` l.199/235, `src/stopped_gc.rs` l.327 name `POST /api/build`, `DELETE /api/nodes/{name}` etc. as the origin of each `TopologyChange`/`BuildOperation` variant → name `build.create` / `node.delete` / `mesh.delete`. Landing: comment-only, with step 0.

---

### C. Investigation (peer-mesh) — src/investigate.rs

### 19 · behaviour · `Rungs`/`Ladder` count backbone rounds, not elapsed time — investigate.rs l.18-26 module doc, l.38-56 `Rungs::RULED = { track: 2, probe1: 10, mark: 15, probe2: 20, decide: 30 }`, l.318-348 (`rounds = unheard.as_millis() / round_ms`), `check_idle_below_probe2` l.60-73
Code: `//! Time is counted in backbone rounds, so a cell at 500 ms rounds runs the same ladder as production at 2 s`.
Doc: `lifecycles/mesh-investigation.md` l.10-15: "Let t be the elapsed monotonic time since the fabric-primary's last valid direct backbone receipt ... the backbone interval or a delayed tick never moves a threshold: the thresholds below are elapsed time, not round counts. 1. At 20 s ... 2. At 30 s ... 3. At 40 s ... 5. At 60 s".
Proposal: the doc is explicit, so the code conforms: the rungs become fixed 20 s / 30 s / 40 s / 60 s of monotonic time since the last direct backbone receipt, independent of `RDM_BACKBONE_INTERVAL_MS`; `Rungs` holds durations, `rounds` leaves `Step`, and `check_idle_below_probe2` compares the staleness floor with 40 s. Cost for the lead to absorb: the gate's 500 ms backbone round no longer shortens the ladder, so cells that wait on a decision wait real seconds (60 s to a verdict); how to carry that in the cells is the lead's call.
Landing: new behaviour story (small) · no durable/wire/gossip.

### 20 · behaviour · probe contacts one admin by Ping+Forward, not "direct first, then via-peer, primary first then the other admins" — investigate.rs l.476-516 `probe()`
Code: `let admin = view.cohort(mesh, NodeKind::NodeAdmin)... .min_by_key(|a| (!a.is_primary, a.name.to_string()))` (one target), then `Ping` up to `PROBE_FANOUT = 3` members and `call_via::<Status>(carrier, &admin.node_id, ProbeNodeState)`; there is no direct `ProbeNodeState` attempt and no second admin.
Doc: `mesh-investigation.md` step 1: "probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other node-admins, with ProbeNodeState. Each target follows the established direct-then-via-peer policy: direct first; when that fails, Ping a few mesh-x members in parallel ... and, on a pong, use that member as the carrier for Forward." Table row 20 s: "ProbeNodeState direct; else Ping a member and, on pong, Forward carrying ProbeNodeState".
Proposal: make `probe()` iterate the mesh's node-admins in the doc's order (exact mesh-primary, then the others), each with a direct `ProbeNodeState` first and the Ping+Forward carrier path only after the direct call fails; stop at the first answer. `classify()` stays. The unit/ladder tests do not exercise `probe()`; add the red cell.
Landing: new behaviour story (small); `rshape` cells `mock_cut_*` already exercise the ladder end to end · no durable/wire/gossip.

### 21 · rename · "rebirth/reborn" in the investigation and round vocabulary
Code: `Step::Decide { rebirth, .. }` (l.117-121, 240-242, 423-425), `State.verdict` "whether it was a rebirth" (l.143), `PeerMesh.rebirth_decided` (l.159-160, 204), `Ladder::rebirth_decided` (l.191-196) — also used by `src/admin.rs` (drift check holds the rebirth back "for every peer mesh it hears" until decided); doc/comments l.1-14, 374, 385 "the reborn mesh's primary reported its round complete"; `src/round.rs` l.3, 113, 185, 420-433 (`reborn`, "the mesh a fabric-primary decided to rebirth"); `src/reenter.rs` l.3 "a fabric primary reborn into its own mesh"; `src/fabric_rounds.rs` l.874-885 test names `reborn`.
Doc: ops-naming "Naming rules": "No 'birth' or 're-birth' in RDM ... Birth and re-birth are Rafka's names for its own workflows"; mesh-investigation.md uses "recovery verdict", "Recovery ONLY if", "recovering admin"; node-admin-lifecycle.md l.37 "Mesh recovery".
Proposal: identifiers `rebirth` → `recovery` (`Step::Decide { recovery, .. }`, `rebirth_decided` → `recovery_decided`, `PeerMesh.recovery_decided`), prose "reborn/rebirth" → "recovered/recovery". EXCEPTION the doc makes itself: the span `rdm.node_admin.mesh.create.via-rebirth` is kept verbatim in mesh-investigation.md's span table (l.67, the "60 s, recovery verdict" row) — leave it unless the lead rules otherwise. (Exact-birth vocabulary — `planned birth`, `exact birth`, `(node.id, incarnation)` — is current doc vocabulary and stays.)
Landing: rename-only, no step (module-local) · none of durable/wire/gossip.

### 22 · comment · investigate.rs l.1-2 `rafka-v2 fabric-node-lifecycle-events-ops-gossip.md, "Mesh Recovery" -> "Detection: the peer-mesh investigation"`
Doc: page is `docs/architecture/lifecycles/mesh-investigation.md#mesh-investigation`.
Proposal: repoint; also `tests/rshape...`/`node_lifecycle__admin_restart.rs` l.2 and `i143_acceptance_2803_detect.rs` l.1 cite the same dead page (other areas).
Landing: comment-only.

---

### D. Rounds, storage, entry

### 23 · behaviour · RDM standalone writes its Mesh/Node/Fabric/connections rows from boot, not at state-commit — src/admin.rs l.4459-4484 (`MeshRowsWriter::round`, "written the moment the book first holds it or it changes"), l.3613-3624 and 3709/4012 (file storages opened on the store root at boot), l.3565 `round_actions ... unwrap_or_else(NoScratchpad)`; src/fabric_rounds.rs l.37-62 `RoundActions`/`NoScratchpad` ("no scratchpad publication is bound: this node holds nothing to publish")
Code: `// ... written while this admin holds its mesh's primary seat (the writer ladder; MeshRowsWriter), as bootstrap hints for a later restart of any node of the fabric.` Nothing gates that loop on the fabric status; the commit-state round action defaults to a no-op.
Doc: `fabric-node-lifecycle.md` §2.7 "The write": "The mesh-primary writes the Mesh row and every member's Node row once the fabric is state-commit ... every node writes its connections rows; each finishes its writes before its state-committed check-in. Before then `system.ops.nodes` and `system.ops.meshes` do not exist."; `node-admin-lifecycle.md` §7: "RDM standalone keeps the same records in the fabric's one store root (`store_dir`): scratchpad before state-commit, then each writer writes what it holds to the root, and writes go there directly afterwards."; R14 item 9 (`i143-r14-rdm-shared-store.md`): "Before state-commit nothing changes: rows stay in the scratchpad. At commit-state each writer writes what it holds to the shared root; after it, writes go there directly."; `fabric-state-commit.md` step 2.
Proposal: RDM binds a real `RoundActions` of its own for standalone: until the fabric reaches state-commit the writers (`MeshRowsWriter`, the nodes/mesh/fabric/connections file adapters) hold rows in memory (scratchpad); `commit_state(node)` writes what the node holds to the store root and only then checks in; after the round, writes go straight through. The file-adapter open at boot keeps the store root check (missing `store_dir` still refuses by name) but writes no row.
Landing: e12.s43 #3070 (R14, shared store root; item 9 is in its ruling) if that story's acceptance includes item 9, else new story · verify with a Jaeger look before relying on this: I read the code path (no gate on fabric state in the writer loop, `NoScratchpad` default) but did not run it · durable layout unchanged; no wire/gossip.

### 24 · rename (wire, step 3) · the fabric round and status ops — rafka-node-rpc-contract/src/status.rs `StatusRequest::{CommitState, StateCommitted, OpenTraffic, TrafficOpened}` (positions 12-15) and `ApplyFabricEvent`/`FabricEvent::Ready`; src/fabric_state.rs l.65-113 `RoundKind` command/completion names `commit-state`, `state-committed`, `open-traffic`, `traffic-opened` and operations `commit-state:<fabric_id>`, `open-traffic:<fabric_id>`; src/status_declare.rs l.409-420 (the fabric-primary owes `ApplyFabricEvent{Ready}` to every other mesh-primary after the rounds)
Doc: `ops-naming-proposal.md` FABRIC / status: "set only by the fabric-primary (old ApplyFabricEvent) ... (old sync-state / state-synced) ... (old CommitState / StateCommitted) ... (old OpenTraffic / TrafficOpened) ... These are status changes, not calls". But `lifecycles/fabric-state-commit.md`, `fabric-ready.md` and `node-rpc-envelope.md` l.683-709 ("Fabric status rounds") still define CommitState/StateCommitted/OpenTraffic/TrafficOpened as the live wire pairs with operations `commit-state:` / `open-traffic:` (I-2).
Proposal: nothing to change until the lead rules which text is the target. If ops-naming wins: the four round pairs and `ApplyFabricEvent` become status changes carried by FabricStatus plus check-ins, renamed with the 0x1B split. If the lifecycle pages win: no rename, and ops-naming's "old …" labels are wrong. Today the code matches the lifecycle pages.
Landing: FLAG; if renamed, rollout step 3 · wire change (positional) · gossip: the round hook frames (`StateCommitting` … `TrafficOpened`) are gossip frames, step 4 / Luke's go-ahead.

### 25 · behaviour · mesh and fabric name claims do not exist — src/storage.rs (`MeshStorage::put_mesh` keyed by mesh name, no claim), src/fabric_storage.rs (`put_identity` is insert-and-fail on the identity key, not on `(path, name)`); `claim_node_row` (storage.rs l.201/218-225) exists for nodes only
Doc: `fabric-node-lifecycle.md` §2.7/§2.8: "The mesh's primary node-admin claims the mesh's name and writes its row ... insert-and-fail on the key `(path, name)` ... `NameHeld` names that holder. The same MeshId re-claiming its own name ... is idempotent"; same for the fabric. Rulings 2026-10-11 "Notes for s36 / s41": "Code: the mesh and fabric name claims do not exist today; add them on the same claim path as node rows (R-OW1)"; R-MN1.
Proposal: add `claim_mesh_row` / `claim_fabric_row` with `NameHeld { by }`, used by `MeshRowsWriter` and the fabric-primary's Day-0 and state-commit writes; acceptance in the ruling (creation then state-commit with no self-conflict; duplicate mesh name refused `NameHeld`; a name reused after Dead claims cleanly with a new MeshId).
Landing: PRD e12.s41 #3068 carries the node-row half; this half has no row → extend s41 or a new story · durable: the key shape is `(path, name)`, same rows as today (R14: no row format moves) · no wire/gossip.

### 26 · behaviour(FLAG) · on-disk "last-ready snapshot" relaunch copy — src/admin.rs l.4122-4155 (`TopologySource::DurableMap(&nodes_storage.contacts())`), src/storage.rs l.1-17 header, MeshRowsWriter (finding 23)
Doc: `node-admin-lifecycle.md` rule 5 and §7: "The attached snapshot is the last ready fabric, not the latest state"; "That copy is a snapshot taken when the fabric last reaches ready, not a continuously overwritten latest-state copy ... The next ready round replaces the on-disk snapshot"; R14 / R-SC3: "every node-admin keeps a transient on-disk copy that only a person-started recovery reads".
Code: the durable map a relaunched fabric-primary reads is the continuously written `nodes.storage` contacts (every birth the book holds, written on each change). I found no snapshot taken at ready and no separate "transient copy".
Proposal: only if §7 is meant to bind RDM standalone (it says "Inside Rafka ... RDM standalone keeps the same records ..." and then states the snapshot rule for "every node-admin"): take the snapshot in the ready round and have `TopologySource::DurableMap` read it. FLAG: I could not settle from the text whether the snapshot is Rafka-only.
Landing: FLAG · none of durable (new files) / wire / gossip.

### 27 · comment · fabric-state / rounds citations
`src/fabric_state.rs` l.2, `src/fabric_rounds.rs` l.2, `crates/rafka-test-scenario/tests/fabric_states__rounds.rs` l.2: `fabric-open-traffic.md` → `docs/architecture/lifecycles/fabric-ready.md`; `rafka-node-rpc-contract/src/status.rs` l.263 same. `fabric_state.rs` l.1/2 and `fabric_rounds.rs` l.1/2 "states.md; fabric-state-sync.md, fabric-state-commit.md" → add the `lifecycles/` prefix. `src/wiring.rs` l.49-50 `(fabric-state-sync.md)` same. Landing: comment-only.

### 28 · comment · stale storage/entry descriptions
`src/storage.rs` l.5-8: "a file implementation in the admin's own data dir" — the file adapters open on the fabric's store root (`StoreRoot`, R14; `node-admin-lifecycle.md` §2); only the node's own row, runtime rows and the Build journal live in the data dir. `src/reenter.rs` l.7: "When the catalogued topology read (op 0x1E) lands, this function is the one swap" — `LocalNode` already reads `topology_read::read_topology` (op 0x1E, reenter.rs l.150). Landing: comment-only.

---

### E. Elections

### 29 · comment · src/election.rs l.1 `Elections (docs/architecture/node-lifecycle-elections.md in rafka-v2; ruling R-A2)`
Doc: `docs/architecture/fabric-node-lifecycle-elections.md`. Also `rafka-test-scenario/tests/mesh_elections__cohort_election.rs` l.2 `rafka-v2 docs/architecture/node-lifecycle-elections.md`. The other election citations (l.199 §4.4, l.688 §5-6, `topology.rs` l.401 §5, `executor.rs` l.54/112/477, `fabric_handover.rs` l.257) name sections that exist (§2, §4.1, §4.3, §4.4, §5, §6 all verified). Landing: comment-only.

### 30 · test · mesh_elections__cohort_election.rs — REST driver and drift vocabulary
Code: header l.1-2 old page; `estate.post("/api/build" ...)` (l.75), `/api/nodes/{n}/restart` (l.192, 202), `estate.delete("/api/nodes/{removed}")` (l.239, 368), `GET /api/fabric` (l.304, 385); comments "drift recovery rebirths the path within seconds" (l.131), "The rebirth is drift recovery's" (l.223), "drift recovery restores the count with a new NodeId at a free path" (l.211-212), "the repair of the same Build rebirths the killed path" (l.350).
Doc: node recovery is the dead member's mesh-primary (tickle, provider exit proof, one recovery Build: `node.delete` then `node.create` at the same path.name, new node.id, same endpoint key; `lifecycles/node-recovery.md`); a fabric-primary drift restart/replace is gone; restart keeps node.id AND incarnation; elections §5 "restart preserves NodeId; a restarted node ... never displaces the holder".
Proposal: calls go through the typed objects / demo routes after step 0; replace "drift recovery rebirths ... at a free path" with "the mesh-primary's node recovery re-creates the member at its same path.name with a new node.id". The assertions themselves (restart keeps the NodeId; the seats are the computed ones; the successor is announced by `via-recompute`) match elections §3-§5. Add the incarnation to "restart keeps the NodeId" (`incarnation_id` unchanged, node-restart.md).
Landing: step 0 for the calls; comment/assertion edit with the node-recovery story e12.s33 #3051 · none of durable/wire/gossip.

---

### F. Status op (flag)

### 31 · behaviour(FLAG) · src/status_rpc.rs l.226-318 sender gating — "authenticated peer resolved to a birth in this admin's view"; `RejectedNotAuthority { SenderNotSubject / ReceiverNotPrimary / SubjectUnknown }` (l.232-318, l.533-540, l.679)
Code: `The sender is never read from the request: it is the authenticated peer, resolved to a birth in this admin's own view` (l.4-5); `"unknown peer"` → `SenderNotSubject`.
Doc: `ops-naming-proposal.md` "A stopped node stays reachable": "Nothing checks who is calling: there is no auth layer inside the mesh, and the fence stays target node id + op"; ruling "no authN/authZ layer inside Node RPC between mesh nodes". Against that, `node-rpc-envelope.md` l.560-563 still describes Status as having "variant-specific direction, authorization, exact-birth and transition checks" and lists `RejectedNotAuthority`.
Proposal: no change proposed from this file alone: the checks are "declaring authority" rules (who may declare a state: the subject, the mesh-primary, the fabric-primary), and the docs keep them as typed refusals. If the lead reads the no-auth-layer rule as covering sender resolution (an unresolved endpoint must not be a refusal reason), the `"unknown peer"` arms (l.235, l.540) are the candidates. The real auth-layer pieces (`Reply::Unauthorized`, `PeerUnresolved`) are in `fabric_handover.rs` l.535-538/693/764-767/1025-1026, `join.rs` l.256/405, `build_claim.rs` l.185-194, `node_config.rs` l.74-87, `build_facts_read.rs` l.233 — other areas, listed here so they are not lost.
Landing: FLAG.

---

### G. Tests and citations in the named test files

### 32 · test · crates/rafka-test-scenario/tests/fabric_states__rounds.rs
Code: header l.2 `fabric-open-traffic.md`; `estate.post("/api/build", ...)` (l.102, 147, 149), `estate.get("/api/fabric")` (l.54, 330); test names `..._open_traffic`/`..._the_commit_state_and_open_traffic_rounds`.
Doc: `lifecycles/fabric-ready.md`; assertions match the docs: hook-frame order `StateCommitting, StateCommitted, TrafficOpening, TrafficOpened` (l.267), `FabricStatus` only from the fabric-primary (l.302), climb `state-sync, state-commit, ready` (l.311), exactly two rounds per mesh (l.296), a check-in never re-sent after `Applied` (l.287).
Proposal: header repoint (finding 27); REST calls follow step 0 (finding 15). No assertion changes. Landing: comment + step 0.

### 33 · test+comment · crates/rafka-test-scenario/tests/rshape_burn_in.rs (6741 lines)
Citations: l.5234-5236 `Canon: fabric-mesh-ops.md section 6 and proof 10 ... fabric-mesh-lifecycle.md FML-19` — `fabric-mesh-ops.md` is gone and `section 6`/`proof 10` have no counterpart page (the content, "unplanned loss produces one NodeDeleted for the old identity, from the replacement attempt's executor", is `lifecycles/node-recovery.md` + `node-delete.md`): FLAG which page; l.5622 `fabric-node-lifecycle-elections.md section 1` ✓ (exists); l.5852 `node-admin-lifecycle.md rule 4` ✓; l.2613/3726/5130/5189/5231 `mesh-recovery.md` / `node-recovery.md` ✓ (line 8-9 of mesh-recovery.md is the "(A lost member ... is node recovery)" sentence); l.471/971/2855/4381 `connections.md` (other area); l.714 `shutdown::drain_bound` (finding 6).
Old shapes asserted (other areas but found here, so named): the node ladders `NodeRestarting, DrainNode, AwaitNodeDrained, StopNode, AwaitNodeLeft, TerminateRuntime` for a restart (l.3047, 3130) and for a retire (l.3309-3345, 4007, 4068) — restart is `node.stop` then `node.start` on the live process now, no `TerminateRuntime`, and stop emits `node.stopped`, not `NodeLeft`; step keys `retire-node:`/`replace-node:`/`recover-node:`/`restart-node:` (l.3324-3328, 2620, 3745; rollout step 2); `mock_replace_racing_proven_drift_births_the_path_once` (l.5222-5241) and the C8 "drift" cells (l.3670-3750) which assert a fabric-primary `proven-drift` attempt for a death (node recovery is the mesh-primary's: the file itself asserts `proven.is_empty()` at l.3733/5202, so the cell mixes both shapes); `rebirth_executors`/"re-born" wording (l.2528-2622).
Proposal: repoint citations; the ladder/step-key assertions belong to the node-lifecycle and step-1/2 stories that change them (name: e12.s34 #3060, s33 #3051); this file only needs the citation edits for this area.
Landing: comment-only for the citations; the rest with the stories above.

---

### H. RDM's own architecture doc

### 34 · comment · /home/admin/rust-distributed-mesh/docs/architecture/mesh.md l.3
Code/text: `the current target architecture is temporarily canonical in rafka-v2/docs/architecture/, including node-rpc-envelope.md, mesh-control-plane.md, and fabric-mesh-ops.md.`
Doc: `fabric-mesh-ops.md` is not a page; the index is `docs/architecture/fabric-mesh-node-lifecycles.md`.
Proposal: replace `fabric-mesh-ops.md` with `fabric-mesh-node-lifecycles.md` in the banner; the rest of the page is declared historical and is left alone (it says nothing on REST, shutdown or rounds).
Landing: comment-only.

---

### I. Places where the docs disagree with each other (for the lead; I proposed nothing across them)

1. `node-rpc-envelope.md` l.581-584 still lists `FabricEvent { Ready, ShutdownInitiated { initiated_by } }`, l.837 says the join answer's control holds "optional Fabric and shutdown facts", and `docs/plans/i143-prd.md` §5.5 (l.415-454) / l.253 / l.733 / l.1078 still describe `FabricShutdown`, `/api/shutdown` and `/api/fabric` shutdown phase — while `fabric-delete.md` and ops-naming say fabric.delete replaces it. Findings 1-5 follow the lifecycle pages; the envelope and PRD need the same edit when the story lands. (The PRD copy in `/home/admin/rafka-v2/docs/plans/i143-prd.md` has e12.s49 #3078; the `docs-pass` copy does not.)
2. Round op names: ops-naming marks `CommitState/StateCommitted/OpenTraffic/TrafficOpened` and `ApplyFabricEvent` as "old", the lifecycle pages and envelope §"Fabric status rounds" use them as the live pairs (finding 24).
3. `mesh-investigation.md` (l.43, 67, 96, 138) authors "FabricStatus degraded", but `states.md` lists the fabric states as `pending → state-sync → state-commit → ready` and the `gossip.md` §3.1 FabricStatus row carries no `degraded` value. The code carries `ScopeStatus::Degraded` and publishes it (`admin.rs` l.930, `investigate.rs` l.429). Add `degraded` to the states page or say how it is carried.
4. `ops-naming-proposal.md` step 0 guard pattern matches iroh's `Router` (finding 16).
5. Rollout and cost puts step 1 first ("steps 0, 2 and 3 land after the i141 pin, behind the typed objects"), but step 1's text names workflows and Build 0x20 and never names the read ops (`node.get`, `mesh.get`, `fabric.get`, `node.connections.get`) that step 0 depends on: `Nodes::get` and `connections_get` are REST-backed today and no mesh/fabric objects exist (finding 13). Step 1 should list them.
6. `node-admin-lifecycle.md` §7 snapshot rule vs the continuously written store (finding 26) — whether it binds RDM standalone.

---

### J. Verified conformant (opened code and doc; no change)
- Store root layout `<store_dir>/<fabric_id>/{fabric, meshes/<mesh>, nodes/<mesh>/, connections/<node>/, status/<mesh>/}`, missing `store_dir` refused by name, no fallback to the data dir (`store_root.rs`, `tests/store_root_boot.rs`; `node-admin-lifecycle.md` §2).
- `fabric.storage` rows: `FabricIdentity` once, `FabricPointer` per move folded to the greatest `(submitted_at_ms, build_id)` (`mesh-control-plane.md` §5.3).
- Election rule: holder keeps the seat, vacancy filled by the lowest `Ready` NodeId of the holder's mesh, fabric seat leaves a mesh only when every node-admin birth of it is gone, a later birth of the NodeId replaces the holder, an unheard holder keeps the seat vacant (`election.rs` l.1-30, `resolve_with`; elections §3-§5, §10); `holding()` ignores status.
- Entry sweep: `SWEEP_DEADLINE` 2 s, 8 at once, one Ping each, no reply = "not yet reached" (`reenter.rs` l.25-27; `mesh-recovery.md` step 5). `apply_mesh_state` 20 s wait (`admin.rs` l.3172-3197; `states.md` "Who sets each state"). Doc code pointers `get_topology` in `reenter.rs`, `enter_existing_mesh`, `start_booted` in `admin.rs`, `rebuild_absent_birth`, `apply_mesh_state` all exist.
- Round semantics: the fabric-primary alone moves the fabric; a check-in counts only as the planned exact birth and only after the primary received this round's down op; nothing leaves a checklist by a timer; `sync_state` once, `traffic_opened` one notice that gates nothing (`fabric_rounds.rs`, `fabric_state.rs`, `round.rs`; `states.md` "The round checklist", `fabric-node-hooks.md`).
- Investigation ladder at the default 2 s round = 20 s / 30 s / 40 s / 60 s; latest completed probe decides; backbone receipt cancels; `degraded` cleared only by the current mesh-primary's report after the decision (`investigate.rs`, `round.rs` `recovered`; mesh-investigation steps 2-7), spans `mesh.update.via-unheard|via-probe|via-probe-verdict`, `mesh.create.via-rebirth`.
- Build admission: fabric-primary only, one Build in flight (`BuildInProgress { build_id }`), last-admin refusal — all in the non-REST half of `http.rs` and stay.

---

### Summary

Counts by class (each finding counted once, under its main class):
- behaviour: 16 (findings 1, 2, 3, 4, 7, 10, 11, 12, 13, 14, 19, 20, 23, 25, 26, 31)
- rename: 5 (5, 6, 17, 21, 24)
- comment: 7 (9, 18, 22, 27, 28, 29, 34)
- test: 5 (8, 15, 30, 32, 33)
- doc fix for the lead: 1 (16, the step-0 guard pattern) + the 6 conflicts in section I
- Total findings: 34. FLAG (needs a ruling before any change): 24, 26, 31, and the page choice in 33.

### Renames (old → new, all places)
| old | new | places |
|---|---|---|
| `FabricShutdown` / `ShutdownControl` / `shutdown.rs` | deleted; `fabric.delete` workflow (Build steps `mesh.delete`, `node.delete`; terminal `fabric.deleted`) | shutdown.rs; fabric_storage.rs; fabric_builds.rs; wire.rs; admin.rs (≈12 sites); http.rs; entry.rs; 11 test files |
| `POST /api/shutdown` | `fabric.delete` typed call | http.rs l.779/829; client lib.rs l.526; estate.rs l.1026/1295 |
| `FabricEvent::ShutdownInitiated` | deleted (step 3) | status.rs l.69/80/938; status_rpc.rs l.331; status_wire.rs l.31/163; i143_acceptance_2805.rs l.801 |
| spans `rdm.node_admin.fabric.{update,reject}.via-shutdown*` (7 names) | `rdm.node_admin.fabric.delete.via-…` of the workflow steps | shutdown.rs; fabric_builds.rs l.324; http.rs; admin.rs l.5521 |
| `RDM_SHUTDOWN_DRAIN_BOUND_MS` | deleted | shutdown.rs l.183; estate.rs l.1066; rshape_burn_in.rs l.714-740 |
| `Family::FabricShutdown` / `"fabric-shutdown"` | `Family::FabricDelete` / `"fabric-delete"` | wedge.rs l.91/117/143 |
| `rdm.node_admin.build.{create,update}.via-rest` | `…via-node-rpc` | http.rs l.146/265; 20 files, 58 occurrences |
| module `crate::http` | `crate::control` (keeps `ControlPlane`, `Refusal`, `Opened`, `Recovery`, `ActionKind`); `router` + handlers leave for the demo | admin.rs l.30; build_op.rs l.21; stopped_gc.rs l.13 |
| `Step::Decide.rebirth`, `rebirth_decided`, `PeerMesh.rebirth_decided` | `recovery`, `recovery_decided` (span `mesh.create.via-rebirth` kept: the doc keeps it) | investigate.rs; admin.rs (drift check); round.rs prose |
| `admin_api_base` (typed), `extra` | demo-owned config key; `extra` → `config` | digest.rs; wire.rs; membership.rs; topology.rs; reenter.rs; fabric_handover.rs |
| `fabric-open-traffic.md` | `lifecycles/fabric-ready.md` | fabric_state.rs; fabric_rounds.rs; fabric_states__rounds.rs; status.rs |
| `fabric-mesh-ops.md` | `fabric-mesh-node-lifecycles.md` (and `lifecycles/node-status.md#node-status-declaration` for the status op) | status.rs; status_rpc.rs; rshape_burn_in.rs (FLAG); RDM docs/architecture/mesh.md |
| `node-rpc.md §38` | `node-rpc-envelope.md#status-op-0x1b-rdm` | status.rs; status_rpc.rs (also `rafka-node-rpc/src/server.rs` `§38.1`) |
| `node-lifecycle-elections.md` | `fabric-node-lifecycle-elections.md` | election.rs; mesh_elections__cohort_election.rs |
| `fabric-node-lifecycle-events-ops-gossip.md` | `lifecycles/mesh-investigation.md` | investigate.rs (+ 2 scenario files) |
| `fabric-primary-handover.md` | `lifecycles/fabric-primary-update.md` | admin.rs; fabric_storage.rs; fabric_handover.rs; tests/fabric_handover.rs; admin/leave.rs; rpc-contract/src/handover.rs |
| `mesh-leave.md` | `lifecycles/mesh-delete.md` | (other area: mesh_leave.rs, admin/leave.rs, admin.rs/1699, build.rs, status.rs, membership.rs, tests) |
| `fabric-mesh-lifecycle.md §11.1` / `FML-n` | `lifecycles/fabric-delete.md` / delete | shutdown.rs; fabric_storage.rs; fabric_builds.rs; entry.rs; http.rs; admin.rs; rshape_burn_in.rs |
| `docs/i143/design.md` (does not exist) | `node-rpc-envelope.md#build-op-0x20-rdm` | rafka-test-scenario/src/lib.rs; estate.rs |
| `node-decommission.md`, `node-birth.md`, `node-ready.md`, `node-status-change.md`, `node-build-hydration.md` (not pages any more) | other area (node-delete.md, node-create.md/node-start.md, …) | node_rows.rs; stopped_delete.rs; accepted.rs; pipeline.rs; app_hydration.rs; admin.rs; mesh_leave.rs |

### Behaviour changes, largest first
1. `fabric.delete` replaces FabricShutdown (findings 1, 2, 3, 4, 5, 6, 7, 8): L, new story, depends on s34/s38/s39/s40/s41/s49 and s23; durable + wire + gossip (Luke's yes, flame first).
2. REST out of RDM (10, 11, 13, 14, 15, 16, 17): L, step 0; needs node-RPC reads and typed objects first; `rafka-test-scenario` (65 files) is the biggest consumer and is missing from the doc's step-0 list.
3. RDM standalone store written only at state-commit (23) and, if it binds RDM, the ready snapshot (26): M, e12.s43 #3070 or new.
4. Mesh and fabric name claims (25): M, extend e12.s41 #3068.
5. `admin_api_base` gate and violation removed (12); the field then leaves digest and `NewFabricPrimary` ("config in the digest"): S for the gate (must ship with step 0), M for the field (durable + gossip).
6. Peer-mesh probe: direct first and the other node-admins (20): S, new story.
7. Ladder thresholds in elapsed time, not rounds (19): S, new story.
8. Status-op sender gating (31) and round-op naming (24): FLAG, lead rules first.

---

## CODE-4 — Node RPC runtime, wire contract, testkit, and every remaining doc citation

Scope: `/home/admin/rust-distributed-mesh` crates `rafka-node-rpc`, `rafka-node-rpc-contract`,
`rafka-node-rpc-testkit`, `rafka-node-base`, plus the citation sweep over ALL of RDM (crates, demo,
tools, CLAUDE.md). Docs read: `node-rpc.md`, `node-rpc-envelope.md`, `mesh-control-plane.md`,
`fabric-node-lifecycle*.md`, `node-admin-lifecycle.md`, `lifecycles/*.md`, `docs/plans/proposal/ops-naming-proposal.md`,
`docs/plans/rulings/node-rpc-session-rulings-2026-10-03.md`, `i143-rulings-2026-10-11-dev.md`.
READ-ONLY; nothing was edited.

Paths below drop the `crates/` prefix and use `rpc` = `rafka-node-rpc`, `contract` = `rafka-node-rpc-contract`,
`testkit` = `rafka-node-rpc-testkit`, `admin-core` = `rafka-node-admin-core`.

Doc-state note: `node-rpc-rdm-ownership.md` was deleted from `/home/admin/docs-pass` (staged) while I worked; I
read its HEAD copy (`git show HEAD:docs/architecture/node-rpc-rdm-ownership.md`) to map the `ownership §N` cites.
`node-rpc-session-rulings.md` is now `docs/plans/rulings/node-rpc-session-rulings-2026-10-03.md`;
`ops-naming-proposal.md` is `docs/plans/proposal/ops-naming-proposal.md`.

### 0 · Verified clean (no change needed)

- `ResetCode` in `contract/src/codes.rs` = 421/422/423/424/425/499 with `STALE_TARGET`; the envelope "Reserved stream codes" table matches. No `STALE_SLOT` code, variant or reset exists anywhere in RDM code (the only `stale-slot` strings are the stale rows in RDM `CLAUDE.md`, finding 40, and prose in two test comments).
- `contract/`Fence` in `src/framing.rs` { target_node_id, op }`, and `check_fence` in `rpc/src/server.rs`/`FenceMismatch::NodeId`: the fence is node id + op, nothing about slot/incarnation. Matches envelope "Section 0: the fence".
- `contract/`core_ledger` in `src/catalog.rs`()` rows = the envelope "The op ledger" table (0x01, 0x10-0x19, 0x1A-0x21, 0x70-0x74; 0x11 and 0x16 retired). `CatalogBuilder::seal` refuses every condition the envelope lists (op 0, duplicate ledger row, testkit range, unledgered, retired, owner mismatch, core transitional, forwardable stream, zero ceiling).
- `StatusRequest` in `contract/src/status.rs` (21 variants) and `StatusReply` positions equal the envelope Status schema, `NodeLeft` is documented as reserved/never sent, and `admin-core/src/status_rpc.rs` refuses it by name.
- No `Relaunch`, no `tailer`, no `child mesh` in the RPC crates.

### 1 · Behaviour (code does what the docs rule out, or the doc/code fork)

### 1 · behaviour · `Pool::timed_out` in `rpc/src/pool.rs`, TIMEOUT_STRIKES_BEFORE_EVICT, Inner.strikes (lines 24-27, 33, 101, 340-366); rpc/src/client.rs
Code: "Timeout strikes (two consecutive reply-deadline expiries on one entry) evict a poisoned connection. They are transitional restart detection (ownership §9) … the strikes are deleted once that cut is proven end to end." Span `rdm.node_rpc.connection.evict.via-timeout-strikes`; observer text "two reply deadlines in a row evicted the connection".
Doc: node-rpc.md §21 "Node RPC may evict because: Iroh reports the connection closed/unusable; `open_bi` fails before commit; resolver/current transport state proves the exact incarnation or slot/freshness was superseded; a peer answers typed `Draining`; topology departure invokes the pool purge hook. It must not evict merely because: no RPC succeeded recently; gossip appears stale; a local health score fell; a custom ping timed out." Also §52 "treating gossip staleness as an RPC eviction rule". Ruling: `docs/plans/rulings/node-rpc-session-rulings-2026-10-03.md` §4 ("Timeout-strike EvictionGuard is transitional"; §4.2 "two consecutive RPC round-trip timeouts … must not evict a healthy pooled connection"; §4.3 required negative control "call 1/2/3 -> Indeterminate … pooled connection identity unchanged"). All seven cut conditions of §4.1 are in the tree: incarnation in `PoolKey` (pool.rs), supersession cancel (`is_current`, pool.rs), eviction on change (`close`/`stale` at pool.rs:~150-170, test `a_new_incarnation_evicts_its_predecessors_connection_without_a_failed_call`, tests/pool.rs). Restart now keeps node.id and incarnation and never exits the process (node-rpc-envelope.md "Node RPC readiness gate" / node.restart), so no restart exists that only a failed call can reveal.
Proposal: delete `strikes`, `TIMEOUT_STRIKES_BEFORE_EVICT`, `Pool::timed_out`, the `healthy` strike-clear, `strikes` removals in `broken`/`close_all`/`close_peer`; client.rs drops the `timed_out` arm so a reply-deadline stays `Indeterminate` on the pooled connection; delete `direct_broken(.. "two reply deadlines in a row ..")` use for timeouts. Delete span `…evict.via-timeout-strikes` and attribute `strikes`; the envelope "Runtime spans" list (node-rpc-envelope.md, attribute list ) and RDM `CLAUDE.md` still name them, so both docs lose the row in the same landing.
Landing: behaviour story, new (node-rpc pool cleanup; session-ruling §4 is the spec) · no durable/wire/gossip change. Tests: finding 31.

### 2 · behaviour · `PoolKey` in `rpc/src/pool.rs` (lines 44-51) and `ResolvedNode` in `rpc/src/resolve.rs` (24-37) — DOC/CODE FORK
Code: `PoolKey { scope, peer, incarnation }`; `ResolvedNode` has no endpoint slot or slot freshness; tests/pool.rs "Pool identity is `(scope, peer, incarnation)`: a connection is to a process birth".
Doc: node-rpc.md §20 "Pool identity: (pool_scope, peer EndpointId, IncarnationId, endpoint slot, slot freshness)", §4 "endpoint slot / slot freshness", §17.2 "Caller-side wait state is exact slot/freshness-scoped", §20.1/§20.3; ruling 2026-10-03 §3.3 "Endpoint-slot freshness stays per slot … Do not turn an endpoint-slot move into whole-node eviction". Against it: node-rpc-envelope.md "Section 0" "One process has one endpoint, so the socket a request arrives on names nothing".
Proposal: this is a fork, not a code change I can derive: either (a) node-rpc.md §4/§17.2/§20/§20.1/§20.3 drop slot/freshness (one process = one endpoint, the key is already exact) or (b) the code gains the slot dimension. The code is internally consistent with the envelope; the docs lane must pick. Not a gossip/mesh change.
Landing: doc decision first; if (b), new behaviour story · no durable change.

### 3 · behaviour · rpc/src/server.rs and rpc/src/admission.rs, 36-60 (`Admission::try_admit(op, &peer.to_string())`)
Code: per-caller admission key is the caller's Iroh `EndpointId` string; module doc says "per-(protocol, caller fabric id) fairness cap".
Doc: node-rpc.md §17 "optional per-(protocol, caller NodeId) cap"; §17.1 "The per-caller cap uses the authenticated/resolved logical NodeId from PeerContext, not EndpointId, slot or freshness. A permanent logical replacement has a new NodeId and receives a fresh allowance." (§17.2 calls the server key "fabric-identity-scoped", which contradicts §17.1 inside the doc.)
Proposal: key by the resolved NodeId (the server already holds a `NodeResolver` view in the node base; a caller the resolver does not know keeps an EndpointId-scoped key by name), and change "caller fabric id" to "caller NodeId" in admission.rs ("fabric id" is a dead word: FabricId is never EndpointId, fabric-node-lifecycle.md §2). Docs lane: reconcile §17.1 vs §17.2.
Landing: new behaviour story · no durable change.

### 4 · behaviour · `invocation` in `rpc/src/server.rs` (lines 576-600), dispatch order
Code: ready gate first, then Draining, then admission.
Doc: node-rpc.md §16 "read the fence (421 / 425 / 424) -> runtime Draining guard (never the Status family) -> readiness guard (typed NotReady) -> admission -> start one supervised invocation task".
Proposal: swap the two guards (Draining before NotReady) or make §16 say the door gate comes first; the envelope "Node RPC readiness gate" ("refuses every op, Ping included") is the weaker reading and matches the code. Low impact; surface to the docs lane rather than change code.
Landing: doc decision · comment-only if the doc moves.

### 5 · behaviour (cross-area, found here) · contract/src/*::Unauthorized/PeerUnresolved reply variants and the sender gates that return them
Code: every family carries `PeerUnresolved` and `Unauthorized` replies (`contract/src/{ping,forward,status,join,topology,build,build_claim,build_facts,handover}.rs`, `testkit/src/*_probe.rs`, `node-base/src/families.rs`); `contract/src/protocol.rs` "The reply for an unauthorized caller. fn unauthorized". Gates that SEND them: `admin-core/src/join.rs` (`peer != d.node.endpoint_id` -> Unauthorized), `admin-core/src/build_claim.rs` (sender not in view / not a node-admin / names another node or birth -> Unauthorized), `admin-core/src/fabric_handover.rs, 692-696, 763-768` (via `sender_of`, 407), `admin-core/src/status_rpc.rs sender_of`, ; `contract/src/status.rs` "the receiver takes [the sender] from the authenticated `PeerContext` and resolves it through its own view".
Doc: node-rpc.md §14 "The transport proves one fact: the caller's iroh `EndpointId`, which Node RPC hands each handler … Nothing inside the mesh checks who is calling; the fence is target node id + op." The envelope keeps `PeerUnresolved`/`Unauthorized` in every reply enum and `RejectedNotAuthority`/`NotAuthority::SenderNotSubject` in Status (node-rpc-envelope.md Status schema), so the doc contradicts itself; the brief's rule (no authorize gate, no PeerUnresolved guard, Luke 10-09) agrees with §14.
Proposal: (a) stop SENDING `Unauthorized` and the sender-identity `PeerUnresolved` ("calling endpoint is not a node of this admin's view") from the gates above; the handler acts on the fence and the body. (b) The reply variants are postcard positions: keep them in the schema as reserved/never sent (the way `NodeLeft` is), delete the `unauthorized`/`peer_unresolved` trait constructors only with a wire step; the lines `PeerUnresolved { reason: "node … is not in the mesh-primary's view" }` in `admin-core/src/node_update.rs` and `node_config.rs` are domain answers about a subject, not caller gates, and stay. (c) The docs lane must delete the contradicting envelope text (`RejectedNotAuthority`, "variant-specific direction, authorization …", node-rpc-envelope.md area) or ruling-restate §14.
Landing: behaviour, new story (admin-core owners must confirm each gate; I did not trace the callers of `sender_of`) · gates are code-only; variant removal is a wire move (step 3) · no gossip change.

### 6 · behaviour · testkit/src/rig.rs and admin-core/src/node_commands.rs (dead `NodeLeft` completion path)
Code: rig.rs `let (R::NodeDrained{..} | R::NodeLeft{..}) = &req else {…}`; node_commands.rs `accept_completion` matches `StatusRequest::NodeLeft`; admin.rs `Frame::NodeLeft`.
Doc: node-rpc-envelope.md "Node drain and stop command pairs": "NodeLeft keeps its position and is never sent; a receiver refuses it by name"; gossip.md `NodeLeft` "reserved position; never published".
Proposal: remove the `NodeLeft` arm from rig.rs and `accept_completion` (the real server already refuses it, status_rpc.rs); the testkit admin side serves `NodeDrained` only.
Landing: behaviour (small), new story or fold into node.stop story e12.s34 (#3060) · no wire change.

### 7 · behaviour · `StatusRequest::` in `contract/src/status.rs`{LeaveMesh, MeshLeave} doc comments (196-232) and the `operation: "shutdown-mesh:<mesh_id>"` strings (206, 220, 943-952)
Code: "the fabric-primary tells the leaving mesh's primary to run the mesh-leave workflow (`leave-mesh`; mesh-leave.md) … the departure is complete only when the fabric-primary records `MeshLeft`"; `MeshLeave`: "The primary keeps running: the outside owner stops it next."
Doc: node-rpc-envelope.md "Mesh delete command pair": "LeaveMesh … its admission starts mesh.deleting. MeshLeave … carries mesh.deleted … The workflow identity is `(mesh_id, build_id, attempt, operation)`; the operation is `mesh.delete:<mesh_id>`"; "The mesh-primary commits the mesh row Dead before it calls MeshLeave … a separate node.delete, with the fabric-primary as executor, proves [the mesh-primary's exit]". lifecycles/mesh-delete.md steps 5-7. No MeshLeft frame in the design.
Proposal: comments -> mesh.delete vocabulary and mesh-delete.md; `operation` value `shutdown-mesh:<id>` -> `mesh.delete:<mesh_id>` (stored in receipts: durable). Variant names stay (envelope keeps `LeaveMesh`/`MeshLeave` until step 3).
Landing: step 2 (Build step keys; operation strings are stored in accepted Builds) for the string; comment-only for the prose · string = durable format moved.

### 8 · behaviour/rename · `StatusRequest::op` in `contract/src/status.rs`() (lines 381-406) span/reply labels
Code: `"drain-node"`, `"stop-node"`, `"node-left"`, `"leave-mesh"`, `"mesh-leave"`, `"open-traffic"`, `"start-node"`, `"update-node"`, …
Doc: ops-naming-proposal.md "Naming rules" `noun.verb` (`node.drain`, `node.stop`, `mesh.delete`, `node.start`) and the rollout step 1 sentence "callers stop seeing the ad hoc names straight away"; the labels are the spelling of refusals and spans, which someone greps.
Proposal: relabel to the ops-naming names where one exists (`drain-node`->`node.drain`, `stop-node`->`node.stop`, `leave-mesh`->`mesh.delete`, `mesh-leave`->`mesh.deleted`-report, `start-node`->`node.start`, `update-node`->`node.update`, `open-traffic`/`commit-state` stay: fabric round names are in the doc as-is). Callers: grep `\.op()`/the strings in `admin-core/src/status_rpc.rs`, `node_commands.rs`, tests (`retire_pipeline.rs`, `status_probe_apply.rs`).
Landing: step 1 · not durable (labels only), a grep-able literal moves · no wire change.

### 2 · Rename

### 9 · rename · `ForwardReply::Relayed` in `rpc-contract/src/forward.rs` (55-58, 146, 196), rpc/src/forward.rs, contract/src/outcome.rs (`relayed()` fns), contract/tests/forward_wire.rs
Code: `Relayed { inner }`, `Committed::relayed`, `Early::relayed`.
Doc: node-rpc-envelope.md (Forward schema) still spells `Relayed { inner: Vec<u8> }` — so this is the doc's wire name today. The Rafka rule "`relay` is BANNED — use reroute / forward / route" applies to Rafka; RDM `CLAUDE.md` has no such ban.
Proposal: rename variant `Relayed` -> `Forwarded` (the carried reply) and the two `relayed()` fns -> `forwarded()`, in the same landing that edits envelope.md. A variant rename is positional-safe (postcard encodes index 0, fixture "00 01 01" unchanged).
Landing: step 3 (wire-contract tidy; doc first) · NOT durable (position unchanged) · wire name only.

### 10 · rename · `LiveNodeResolver::park` in `rpc/src/live.rs`, `parked` set, `is_parked` (live.rs, 164-179, 230-243, 399-418), rpc/src/hydration.rs, rpc/src/client.rs, contract/src/build.rs
Code: "The exact birth … is parked: its stop was published (`NodeStopped`)".
Doc: ruling `i143-rulings-2026-10-11-dev.md` "Vocabulary: STOPPED is the verb. 'Parked' is gone from prose. The wire tokens `Current { parked }` / `CurrentParked` keep their spelling until Luke rules a rename." ops-naming "A stopped node stays reachable" ().
Proposal: comments and test names say "stopped"; identifiers `park`/`parked`/`is_parked` -> `stop`/`stopped`/`is_stopped` in live.rs (callers: `admin-core/src/node_rpc.rs`). Wire tokens `CurrentParked` stay. Whole-RDM `parked` count is 427 hits (admin-core `park_gate.rs`, `ParkGate`, `membership.rs park()`, `tests/channel_park.rs`); the ruling covers prose only, so identifier renames elsewhere are a Luke call — list, do not assume.
Landing: comment-only for prose; rename story for identifiers (new) · not durable.

### 11 · rename · testkit/tests/mesh_leave_member.rs (file, test fn `a_mesh_leave_member_shutdown_publishes_node_stopped_from_its_stopped_reply`, `mod mesh_leave_member` in tests/main.rs)
Code: asserts a member's shutdown in a mesh leave = `shutdown_member(&leave_build, 1, "shutdown-mesh:mesh1", …)`, events `NodeDraining:drain-node:…`, `NodeLeaving:stop-node:…`, `NodeStopped:stop-node:…`; recorder stubs `mesh_leaving/mesh_leave/mesh_left`.
Doc: mesh-delete.md "Operation contract": "Build step key `mesh.delete:<mesh_id>`; each member's step is its own `node.delete:<path.name>`"; "Every member except the mesh-primary is deleted with node.delete".
Proposal: file -> `mesh_delete_member.rs`; operations -> `node.delete:<path.name>` under `mesh.delete:<mesh_id>`; event list = exactly what node.stop and node.delete publish (node-stop.md, node-delete.md); delete the `MeshLeaving/MeshLeave/MeshLeft` recorder arms (the `LifecycleEvents` trait methods are admin-core, see CODE-1/2: no MeshLeft frame in the design). Cite repoint: line 2 and 46 -> `lifecycles/mesh-delete.md#command-completion-and-gossip-messages`.
Landing: tests: with the mesh.delete behaviour stories e12.s39 (#3066) / s40 (#3067) / s49 (mesh.delete ends at mesh.deleted); names/keys at step 2 · test-only.

### 12 · rename · testkit/tests/retire_pipeline.rs, retire_admission.rs, src/rig.rs (`retire`, `AwaitNodeLeft`, `retire-node:`)
Code: header "DrainNode, AwaitNodeDrained, StopNode, AwaitNodeLeft, TerminateRuntime, ReleaseStorage, RemoveTopologyMembership, Complete"; operation asserted `"retire-node:mesh1.rpc.1"`; `("stop-node","node-left","stop-node:mesh1.rpc.1","stopped")` completion rows; header text "the node-left completion call".
Doc: ops-naming `delete  node.delete  old retire-node, delete-node, decommission` (); node-delete.md; envelope "NodeLeft … is never sent".
Proposal: rename files `retire_*` -> `node_delete_*`; assertions on `retire-node:<path>` -> `node.delete:<path.name>`; "node-left completion call" text -> "the `Stopped` reply of `StopNode`". `RetireStep`/`AwaitNodeLeft` themselves are admin-core (CODE-1/2).
Landing: step 2 (step keys) with test follow · durable string only in the receipt assertions.

### 13 · rename · testkit/src/admin_faults.rs (axum door `router()`, lines 18-20, 626-633; testkit Cargo.toml `axum`) and its users
Code: `POST /faults/arm`, `POST /faults/release`, `GET /faults` over `axum::Router`.
Doc: ops-naming-proposal.md "Rollout" step 0: "REST out of RDM … `rafka-node-rpc-testkit/src/admin_faults.rs` … move to the demo crate … `grep -rn 'Router\|axum\|"/api/' crates/rafka-node-admin*` returns nothing" (that grep covers `rafka-node-admin*`; admin_faults.rs is named explicitly in the move list).
Proposal: move `router()` and the `axum` dep out of `rafka-node-rpc-testkit` to the demo; the decorators (`FaultedBuilds`, `FaultedProvider`, …) stay.
Landing: step 0 · not durable.

### 3 · Comment (stale doc cites in the rpc / contract / testkit / node-base crates)

Target forms: `RPC` = `docs/architecture/node-rpc.md`, `ENV` = `docs/architecture/node-rpc-envelope.md`.

| # | where | cite now | should name |
|---|---|---|---|
| 14 | rpc/src/lib.rs; contract/src/lib.rs | "ownership amendment §6–§9", "§6–§7, §12" | RPC §8-§12, §24-§28 (+ `mesh-control-plane.md#packages` for the crate boundary) |
| 15 | rpc/src/client.rs | "node-rpc.md §20–§24; ownership amendment §6" | RPC §20-§24 and §34 (budgets); drop "ownership amendment" |
| 16 | rpc/src/client.rs | "The two budget shapes (ownership amendment §6.1)" — the enum has THREE shapes (`Overall`, `Split`, `Stream`) | "Budget shapes (node-rpc.md §34 Deadlines and cancellation; `Stream` is the Build-carrier no-overall-deadline case in §34)"; also delete the duplicated `Split` doc line (client.rs) |
| 17 | rpc/src/server.rs | "node-rpc.md §12–§19, §26–§32; ownership §6.3" | RPC §12-§19, §26-§32 (drop "ownership §6.3": old §6.3 was Reconciliation, unrelated) |
| 18 | rpc/src/server.rs | "node-rpc.md §38.1" | `ENV#runtime-spans` (RPC §39 points there) |
| 19 | rpc/src/server.rs | "node-rpc-rdm-ownership.md §12" | `ENV#the-op-ledger` (sealed catalog) + RPC §12 |
| 20 | rpc/src/pool.rs; tests/pool.rs | "node-rpc-rdm-ownership.md §8–§9" | RPC §20-§21 (pool identity, eviction reasons) and `docs/plans/rulings/node-rpc-session-rulings-2026-10-03.md` §3-§4 |
| 21 | rpc/src/pool.rs | "(ownership §9)" | same ruling §4 (goes away with finding 1) |
| 22 | rpc/src/resolve.rs | "node-rpc.md §5–§6" (no such sections) | RPC §4-§4.2 |
| 23 | rpc/src/route.rs | "node-rpc.md §36–§37" | RPC §36 (§37 is now "Broker data is one protocol family") |
| 24 | rpc/src/admission.rs | "(node-rpc.md §17) … caller fabric id" | RPC §17.1 and "caller NodeId" (finding 3) |
| 25 | rpc/tests/context.rs | "node-rpc.md §48" (no such section) | `ENV#runtime-spans` (trace edge between caller and server) |
| 26 | rpc/tests/catalog_seam.rs | "ownership amendment §12" | `ENV#the-op-ledger` |
| 27 | contract/src/catalog.rs,  | "ownership amendment §10–§11, §14; node-rpc.md §9, §12", "(ownership amendment §10)" | `ENV#the-op-ledger` (seal rules, Owners paragraph) + RPC §9, §12 |
| 28 | contract/src/codes.rs | "ownership amendment §12" | `ENV#reserved-stream-codes`; also the `StaleTarget` doc "or a port it does not serve" (codes.rs) names a fence field that no longer exists -> "names a node this process is not" (`ENV#section-0-the-target`) |
| 29 | contract/src/dispatch.rs; framing.rs; outcome.rs | "ownership amendment §6–§7, §11", "§7", "§6" | RPC §16, §23, §24, §26, §40 / `ENV#section-0-the-target`; drop ownership |
| 30 | contract/src/context.rs | "node-rpc.md §11, §33" (§33 = no deduplication) | `ENV#section-1-the-context` + RPC §10.1 |
| 31 | contract/src/status.rs | "node-rpc.md §38; fabric-mesh-ops.md §3" | `ENV#status-op-0x1b-rdm` (op 0x1B family) |
| 32 | contract/src/status.rs, ,  | "mesh-leave.md", "fabric-open-traffic.md", "(`node-stop.md`)" | `lifecycles/mesh-delete.md`, `lifecycles/fabric-ready.md`; node-stop.md is current |
| 33 | contract/src/handover.rs | "lifecycles/fabric-primary-handover.md; R-ST2" | `lifecycles/fabric-primary-update.md`; `ENV#fabric-primary-handover-op-0x21-rdm` is still the anchor |
| 34 | contract/src/ping.rs | "mesh primary's 45 s offline tickle (fabric-node-lifecycle.md §7.3)" (§7.3 is now "Delete releases identity; storage stays") | `lifecycles/node-recovery.md#numbered-end-to-end-sequence` step 2 (Tickle) |
| 35 | node-base/src/lib.rs | "`node-rpc-rdm-ownership.md` §12" | `ENV#the-op-ledger` |
| 36 | node-base/src/families.rs | "typed Node RPC refusals (node-rpc.md §34)" (§34 = deadlines) | `ENV#section-2-the-body` ("Every protocol supplies typed refusals") / RPC §25 |
| 37 | testkit/src/node.rs; testkit/tests/join_before_ready.rs | "`node-ready.md` step 1; `node-birth.md` step 6" | `lifecycles/node-create.md#bound-endpoint-and-join-admission` (join admission), `fabric-node-hooks.md#hydrate_before_ready` (the hydrate step); `ENV#join-op-0x1d-rdm` |
| 38 | testkit/tests/mesh_leave_member.rs,  | "mesh-leave.md, 'Operations and lifecycle events'" | `lifecycles/mesh-delete.md#command-completion-and-gossip-messages` |
| 39 | rpc/src/live.rs,  | "ops-naming 'A parked node stays reachable'", "node-replace.md" | `docs/plans/proposal/ops-naming-proposal.md` "A stopped node stays reachable" (line 436) |

### 40 · comment/doc · RDM `CLAUDE.md` node_rpc span rows (the project span vocabulary table)
Code: emits `…request.reject.via-unserved-op`, `…connection.reject.via-stale-target` (attrs `decided_by, op, peer, node_id, mismatch, receiver_node_id`), `…connection.evict.via-incarnation-superseded`; no `via-stale-slot`, no `via-slot-superseded`, no `slot`/`tag` attributes.
Doc: node-rpc-envelope.md "Runtime spans": `via-unserved-op`, `via-stale-target`, `via-incarnation-superseded`, attribute `op`.
Proposal: rewrite CLAUDE.md rows 159-165 to the envelope's names (`tag`->`op`, `via-unserved-tag`->`via-unserved-op`, `via-stale-slot`->`via-stale-target`, drop `via-slot-superseded`, drop `slot`, drop the strikes row with finding 1). Also: `test-scenario/tests/rshape_burn_in.rs` and `i143_acceptance_2787.rs` say "no stale-slot dispatch" for a 425 refusal -> "no stale-target dispatch" (comment + invariant text).
Landing: comment-only (rows), with finding 1 (strikes row).

### 41 · doc gap (for the docs lane, not code) · envelope "Runtime spans" omits spans the code emits
`rdm.node_rpc.connection.create.via-accept`, `…connection.create.via-dial`, `…connection.delete.via-hard-cut`, `…request.reject.via-hydration-not-passed`, `…route.resolve.via-held-projection`, `…status.update.via-authority-moved`. Either the envelope lists them or the code stops emitting; the grep surface is the doc.

### 4 · Test (asserting the old shape) in this area

### 42 · test · rpc/tests/pool.rs `repeated_timeout_evicts_poisoned_connection_without_marking_node_unreachable`,  `two_reply_deadlines_that_evict_the_pooled_connection_report_it_broken`
Code: asserts `next.connection != first.connection, "the poisoned connection was evicted"` and `"the second strike evicted the connection"`.
Doc: ruling 2026-10-03 §4.3 "call 1/2/3 -> Indeterminate; Assert: pooled connection identity unchanged, no timeout-strike eviction, no redial caused solely by those reply timeouts".
Proposal: replace both with the §4.3 negative control (three reply-deadline Indeterminates, same `connection`, `reused == true`, no `…evict.via-timeout-strikes` span); delete the observer-`broken` test.
Landing: with finding 1.

### 43 · test · testkit/tests/{retire_pipeline.rs, retire_admission.rs, recover_pipeline.rs, mesh_leave_member.rs} — see findings 11 and 12; `recover_pipeline.rs` records `NodeRestarting` for a replace, and states `mesh_leaving/mesh_leave/mesh_left` "not recorded" — drop the three mesh arms with the trait methods; the assertion "A recovery publishes no stop, drain, leaving or mesh-leave event" -> recovery is the mesh-primary's tickle -> provider proof -> one recovery Build (node-recovery.md), keep.

### 44 · test/comment · mesh-transport/src/membership.rs (CONTRACT of `a_node_joining_after_the_leaving_meshs_members_said_connections_deleting_and_leaving_installs_their_own_states_and_routes_none_of_them`)
Code: "lifecycles/mesh-leave.md 'Every stopping member publishes its own Leaving … carries that same Leaving digest in its final `MeshTopology`'" — a quote of text that no longer exists.
Doc: gossip.md "`NodeLeft` … reserved position; never published … a stopping node gossips nothing and its mesh-primary publishes NodeStopped"; mesh-delete.md step 2 and states.md "one routing rule" (no mesh-level hold; a peer learns a deleting mesh only from the mesh-primary's own mesh topology, `gossip.md#mesh-delete`).
Proposal: re-derive the test's premise: a member says Draining/Leaving only where it stays published by its mesh-primary's mesh topology, not as its own final `MeshTopology` frame. Repoint cite to `gossip.md#mesh-delete` + `lifecycles/states.md#routing-stop`. (mesh-transport is outside this area; flagged because the cited text is gone.)
Landing: test/behaviour check, e12.s45 (#3073 one routing rule) is the closest story.

### 5 · Citation sweep across ALL of RDM (crates, demo, tools, CLAUDE.md)

Method: grep every `*.md` token and every `node-rpc.md`/`ownership`/`fabric-*.md`/`mesh-control-plane.md`/`node-admin-lifecycle.md`
citation in `.rs/.json/.sh/.toml` + `CLAUDE.md`; checked the cited page exists in `/home/admin/docs-pass/docs/architecture` and
that a cited `§N` is a heading. The 99-line `rdm-cites.txt` is a subset; below are all removed/renamed targets with every location
(and new ones). Demo has one cite (`demo/src/bin/rshape_node_admin.rs`), everything else is crates.

Locations use `admin-core/`, `node-rpc/`, `rpc-contract/`, `rpc-testkit/`, `test-scenario/`, `mesh-transport/`, `node-base/` for the crate dir.

### 5.1 Removed or renamed pages -> current path#section

| old cite | new target | locations (file:line) |
|---|---|---|
| `mesh-leave.md` (all sections) | `lifecycles/mesh-delete.md`; steps 1,3,4 -> `#numbered-end-to-end-sequence` steps 1,3-4,5; "Resume after the primary's loss" -> `#resume-after-the-primarys-loss` (steps 1-4 keep their numbers); "Completion proof" -> `node-rpc-envelope.md#mesh-delete-command-pair` + sequence step 5; "Failure/certainty behavior" / "Missing receipts" -> `#failure-and-receipts`; "Operations and lifecycle events" -> `#command-completion-and-gossip-messages` | admin-core/src/admin.rs; admin/leave.rs; build.rs; mesh_leave.rs; tests/drift_convergence.rs; tests/mesh_leave_resume.rs; rpc-contract/src/status.rs; rpc-testkit/tests/mesh_leave_member.rs; test-scenario/tests/mesh_lifecycle__mesh_leave_no_survivor.rs, __mesh_leave_resume.rs, __mesh_replace.rs; mesh-transport/src/membership.rs (see 44) |
| `node-decommission.md` (steps 1-3, "6.9 Effect") | `lifecycles/node-delete.md` (step 3 row write -> `#numbered-end-to-end-sequence`; stop steps -> `node-stop.md`); "6.9 Effect" is `fabric-node-lifecycle.md#stopped-node-garbage-collector` Effect row, valid as is | admin-core/src/accepted.rs; deployment/pipeline.rs; node_rows.rs; tests/node_rows.rs; tests/stopped_delete.rs; test-scenario/tests/node_lifecycle__node_stop.rs |
| `fabric-primary-handover.md` (step 3, step 6) | `lifecycles/fabric-primary-update.md` (`## Sequence`; durable per-stage rows in the sequence) | admin-core/src/admin.rs; admin/leave.rs; fabric_handover.rs; fabric_storage.rs; tests/fabric_handover.rs; rpc-contract/src/handover.rs |
| `fabric-open-traffic.md` | `lifecycles/fabric-ready.md` | admin-core/src/fabric_rounds.rs; fabric_state.rs; rpc-contract/src/status.rs; test-scenario/tests/fabric_states__rounds.rs |
| `fabric-mesh-ops.md` (§3, §6, proof 10) | §3 (status family on 0x1B) -> `node-rpc-envelope.md#status-op-0x1b-rdm`; §6/proof 10 (unplanned loss, one NodeDeleted from the replacement attempt's executor) -> `lifecycles/node-recovery.md` + `lifecycles/node-replace.md` | admin-core/src/status_rpc.rs; rpc-contract/src/status.rs; test-scenario/tests/rshape_burn_in.rs; tools/mesh-audit/i143-rshape-matrix.json |
| `node-build-hydration.md` ("Facts are absorbed into the existing Build log") | `node-rpc-envelope.md#build-facts-op-0x1f-rdm` ("Facts are absorbed through the existing Build log") and `lifecycles/node-start.md` step 45 text | admin-core/src/mesh_leave.rs; tests/mesh_leave_resume.rs |
| `node-status-change.md` ("a failed Build … gets its next …") | `lifecycles/node-status.md#node-status-change` (anchor kept; section "Status apply") | admin-core/src/admin.rs; tests/drift_convergence.rs |
| `node-ready.md` step 1, `node-birth.md` step 6 | see finding 37 | rpc-testkit/src/node.rs; rpc-testkit/tests/join_before_ready.rs; admin-core/src/app_hydration.rs |
| `node-lifecycle-elections.md` / "`docs/architecture/node-lifecycle-elections.md` in rafka-v2" | `fabric-node-lifecycle-elections.md` | admin-core/src/election.rs; test-scenario/tests/mesh_elections__cohort_election.rs |
| `node-rpc-rdm-ownership.md` and "ownership §N"/"ownership amendment §N" | see §3 table rows 14-21, 26-30, 35 and below; the Packages table is `mesh-control-plane.md#packages` (rafka-mesh-entity row for `ownership §4, §5.1`) | node-rpc/src/pool.rs; node-rpc/src/server.rs; node-rpc/src/lib.rs; node-rpc/src/client.rs; node-rpc/tests/pool.rs; node-rpc/tests/catalog_seam.rs; rpc-contract/src/{lib.rs, dispatch.rs, catalog.rs, framing.rs, outcome.rs, codes.rs}; node-base/src/lib.rs; admin-core/src/model.rs; mesh-entity/src/lib.rs; tools/mesh-audit/src/legacy.rs ("PRD §4 and ownership §17": ownership had no §17; cite the i143 PRD row only) |
| `node-rpc.md §38`, `§38.1`, `§48`, `§5-§6`, `§33`(context), `§34`(refusals), `§37`(routing) | non-existent or re-meant sections; see §3 rows 18, 22, 23, 25, 30, 31, 36 | rpc-contract/src/status.rs; admin-core/src/status_rpc.rs; node-rpc/src/server.rs; node-rpc/src/resolve.rs; node-rpc/src/route.rs; node-rpc/tests/context.rs; rpc-contract/src/context.rs; node-base/src/families.rs |
| `ops-naming-proposal.md`, "ops-naming" sections | `docs/plans/proposal/ops-naming-proposal.md`; section names: "Node data: meta, config and exe" (kept), "The name is claimed by insert-and-fail" is a bullet in "Decisions and why" (), "A rename is the same write" a bullet in "Node data" (), "A parked node stays reachable" -> "A stopped node stays reachable" (), "Identity" = "Identity: what each op keeps" () | admin-core/tests/build_admission.rs; tests/drift_convergence.rs; tests/status_declarations.rs; src/status_rpc.rs; src/node_rows.rs; src/node_config.rs; tests/node_rows.rs; tests/node_objects.rs; rpc/src/live.rs; rpc-contract/src/status.rs; rpc-testkit/src/node.rs; rpc-testkit/tests/status_probe_apply.rs; mesh-transport/src/membership.rs; admin-client/src/objects.rs; test-scenario/tests/node_lifecycle__node_{stop,replace,delete}.rs |

### 5.2 Cites to pages that exist but with a wrong or shifted section

| # | where | cite | current truth -> should name |
|---|---|---|---|
| 45 | admin-core/src/admin.rs; tests/status_seat_fold.rs; src/status_rpc.rs; tests/fabric_handover.rs; src/store_root.rs; tests (R14 refs) | `fabric-node-lifecycle.md §4.3` / `4.3` for "the mesh-primary writes every status/row of its mesh" | §4.3 is now "A returning broker re-syncs per topic". The one-writer rule is `fabric-node-lifecycle.md#25-one-system-org-one-projection-one-writer-per-mesh` (§2.5) and the Node-row writer bullet at fabric-node-lifecycle.md (which sits, probably by mistake, under §4.3: docs lane to move it). Repoint to §2.5 |
| 46 | admin-core/src/offline.rs; mesh-entity/src/cadence.rs; admin.rs; rpc-testkit/src/node.rs; test-scenario/src/soak.rs; rpc-contract/src/ping.rs | `fabric-node-lifecycle.md §7.3` (offline tickle, status kick, i77 row 18) | §7.3 is "Delete releases identity; storage stays". Tickle is `lifecycles/node-recovery.md#numbered-end-to-end-sequence` step 2; the offline record is §7.3's table, so offline.rs's record cite may keep §7.3 for the record only |
| 47 | mesh-entity/src/cadence.rs | `fabric-node-lifecycle.md` (line number) | backbone cadence is `gossip.md#32-cadence-timers-only-for-heartbeat-and-anti-entropy` (§3.2) and gossip.md |
| 48 | admin-core/src/shutdown.rs; fabric_builds.rs; entry.rs; admin.rs; fabric_storage.rs; http.rs; demo/src/bin/rshape_node_admin.rs | `fabric-mesh-lifecycle.md §11.1, FML-21..27, §13` | §11.1 is fabric.delete (valid). `FML-NN` ids exist nowhere in the page: drop them. Add `lifecycles/fabric-delete.md` as the sequence page. §13 = "Node-admin storage, restart, and recovery" (valid for fabric_storage.rs) |
| 49 | tools/mesh-audit/i143-rshape-matrix.json | `fabric-mesh-lifecycle.md FML-19` | no FML ids; `fabric-mesh-lifecycle.md#5-the-fabrics-mesh-roster-comes-from-the-accepted-build` |
| 50 | admin-core/src/deployment/mod.rs; deployment/provider.rs; deployment/pipeline.rs; lifecycle.rs; readiness.rs; build_state.rs | `mesh-control-plane.md §5–§6`, `§6`, `§6`, `§7`, `§7`, `§8`, `§4` | headings (unchanged since HEAD): §5 BuildStateAdapter, §6 LeadershipResolver, §7 Deployment provider, §8 DeploymentPipeline/RuntimeFact, §9 LifecycleTransitionPipeline, §10 Readiness gates. So: deployment/mod.rs -> §7-§8; provider.rs -> §7; pipeline.rs -> §8; lifecycle.rs -> §9; readiness.rs -> §10; build_state.rs -> §5. (build.rs §1, fabric_builds.rs §3-§4.1, http.rs §2 are correct) |
| 51 | admin-core/src/reenter.rs; executor.rs; admin.rs; accepted.rs | `node-admin-lifecycle.md §4`, `§4.3`, `4.2`, `4.2` | §4.1/4.2/4.3 are now pointers into `lifecycles/mesh-recovery.md` steps 2-7; "durable runtime record names its exact runtime (4.2)" and "replacement of a birth the view does not hold (4.2)" no longer live at §4.2 -> `lifecycles/node-recovery.md` / `mesh-control-plane.md#8-deploymentpipeline-runtimefact-…` (§8.1 RuntimeFact). admin.rs (§2),  (§3), store_root_boot.rs (`node-admin-lifecycle.md` line cite) re-check: no "Restart | its own data dir" row at line 65 |
| 52 | admin-core/src/executor.rs; seat_watch.rs | `fabric-node-lifecycle-elections.md section 2`, `§4.1` | both correct (Eligibility: Draining is no candidate; §4.1 points to the tickle ladder). The rest of the `fabric-node-lifecycle-elections.md` cites (§1, §4.3, §4.4, §5, §5-6) resolve to the right text |
| 53 | test-scenario/tests/rshape_burn_in.rs | "rafka-v2 docs/architecture/connections.md, section 10" | `connections.md#10-any-direct-connected-retires-the-proxy` (line cite moved; drop the lines) |
| 54 | RDM `CLAUDE.md`; test-scenario/src/lib.rs; estate.rs | `docs/i143/design.md` (§2.1, §4, §7) | no such file anywhere (RDM `docs/i143/` and docs-pass have none). Repoint to the owning architecture page or the i143 PRD |
| 55 | tools/mesh-audit/src/parity.rs | `connections.md §13 … '## Connections parity' row` | connections.md §13 "Required acceptance tests" exists; there is no `## Connections parity` heading in it — audit tool looks for a table the doc does not have (check the RDM-side parity doc) |

### 5.3 Docs-side forks to hand the docs lane (found while mapping)
- node-rpc-envelope.md still lists `drain-node:mesh1.rpc.1` as an operation example (~line 351) next to ops-naming `node.drain`; `commit-state:<fabric_id>`/`open-traffic:<fabric_id>` operation strings are unchanged in ops-naming (the fabric round names are the new `state-commit`/`ready` status names: decide whether the operation string follows).
- The envelope's "Mesh delete command pair" anchor id is still `mesh-leave-command-pair`; the doc links to `#mesh-delete-command-pair` (works as heading slug; keep one).
- node-rpc.md §14/§46 ("nothing inside the mesh checks who is calling") vs envelope `Unauthorized`/`RejectedNotAuthority` (finding 5).
- node-rpc.md §17.1 vs §17.2 (NodeId vs fabric-identity key; finding 3); §4/§20 slot/freshness (finding 2).
- Envelope Runtime-spans list vs spans the code emits (finding 41).

### Summary

Counts (numbered findings 1-55): behaviour 8 (1-8), rename 5 (9-13), comment 27 (14-40) plus 11 sweep rows (45-55) plus the §5.1 table (about 120 locations), test 3 (42-44), doc gap 1 (41). Doc/code forks needing the docs lane: findings 2, 3, 4, 5(c), 41 and the §5.3 list.

Renames table (old -> new, all places):

| old | new | where | step |
|---|---|---|---|
| `ForwardReply::Relayed`, `relayed()` x2 | `Forwarded`, `forwarded()` | contract forward.rs; outcome.rs; rpc forward.rs; forward_wire.rs; envelope.md | 3 (position unchanged, not durable) |
| `LiveNodeResolver::park/parked/is_parked` | `stop/stopped/is_stopped` | rpc live.rs (+ admin-core node_rpc.rs); wire `CurrentParked` stays | identifier story |
| `shutdown-mesh:<id>` operation | `mesh.delete:<mesh_id>` | contract status.rs; testkit mesh_leave_member.rs | 2 (durable) |
| `drain-node:`/`stop-node:`/`start-node:`/`retire-node:` operations | `node.drain:`/`node.stop:`/`node.start:`/`node.delete:<path.name>` | contract status.rs; testkit retire_*.rs, rig.rs | 2 (durable) |
| `StatusRequest::op()` labels | `node.drain`, `node.stop`, `mesh.delete`, `node.start`, `node.update` | status.rs | 1 (not durable; grep literals) |
| `mesh_leave_member.rs`, `retire_pipeline.rs`, `retire_admission.rs` | `mesh_delete_member.rs`, `node_delete_pipeline.rs`, `node_delete_admission.rs` | testkit/tests | with 2 |
| REST fault door `admin_faults::router` + `axum` dep | demo crate | testkit admin_faults.rs, Cargo.toml | 0 |
| `via-stale-slot`, `via-unserved-tag`, `tag`/`slot` attrs in RDM `CLAUDE.md` span table | `via-stale-target`, `via-unserved-op`, `op` | CLAUDE.md; rshape_burn_in.rs; i143_acceptance_2787.rs | comment-only |

Behaviour changes ordered by size:
1. (5) Remove the sender-identity gates and stop sending `Unauthorized`/caller `PeerUnresolved` (join, build_claim, fabric_handover x3, status_rpc sender_of); keep variants reserved; docs contradiction to resolve first. New story.
2. (1)+(42) Delete timeout-strike eviction (pool.rs, client.rs, span, CLAUDE.md row) and flip the two pool tests to the ruled negative control. New story; ruling already written.
3. (3) Per-caller admission keyed by NodeId, not EndpointId (and drop "fabric id").
4. (2) Pool key vs slot/freshness: doc/code fork, docs lane decides.
5. (6) Remove dead `NodeLeft` completion arm (node_commands.rs, rig.rs).
6. (7)/(8) mesh.delete operation strings (step 2, durable) and Status op labels (step 1).
7. (4) Draining vs NotReady order: doc decision.

Return to lead (under 200 words) is in the SubagentHandback message.
