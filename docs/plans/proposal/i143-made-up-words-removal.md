# Proposal: the made-up words, where they came from, and their removal (i143)

Luke, 2026-10-22: "we have terms fabric-primary, mesh-primary, node-type primary; use the terms, do not
make new terms", "there is no authority, there is no committed seat", "there is no race because there is
always one writer for each item", and "do not replace the removed mechanisms with renamed equivalents".
This page is the removal written at the root: where each word came from, what it stands for in the
terms we have, every page and code file that carries it, the landing that stops it spreading today, and
the order the rest comes out in.

## 1. The finding

None of these words is Luke's. His quoted words in `docs/plans` say mesh-primary, fabric-primary,
"a secondary node-admin in its mesh", "taken over fabric-primary", "NA1 calls node.primary.update on
NA2", "the ruling is storage is for persistence, never business rules". Not once: seat, authority,
cohort, lane, cut-off, committed. ("Leadership election" is his, Q107, 2026-10-21: "this is leadership
election primary ... it happens for every node type"; the word stays in that phrase.) Each word below entered through a docs commit an agent
landed under his git user, or through a ruling an architect or the lead wrote, and every later page,
ruling, identifier, wire frame and store key took it from there. The pages are the contract, so once a
word was on a page, code was written to conform to it, and the word became "canon" by its own weight.

## 2. Origin, per word

| Word | First appears | Commit | Landed by |
|---|---|---|---|
| authority (the mesh's, the fabric's) | "the authority" as plain English | rafka-v2 `7b213d6eda`, 2026-06-06, the v1 flat copy | v1 |
| `fabric-authority.md`, "authority" as a thing | the page | rafka-v2 `99fb3d70c8`, 2026-10-05, "make RDM the single leadership and reconciliation owner" | agent docs commit under Luke's git user |
| `NotAuthority`, `RejectedNotAuthority`, `AuthorityReady/Changed/Addressed` on the wire | code conforming to the page | RDM, from 2026-10-05 | agents |
| seat ("fabric-primary seat") | node-lifecycle §2.7 | rafka-v2 `d812359d32`, 2026-09-28, i138 | agent docs commit under Luke's git user |
| "mesh-primary seat", "take over on seat gain" | node-lifecycle §2.7 | rafka-v2 `966f06822b`, 2026-10-02, i138 | agent docs commit under Luke's git user |
| "seat row", "moves a holder's seats" | R-EL1a | rafka-v2 `e18f9b6d32`, 2026-10-11 | agent docs commit under Luke's git user |
| committed seat (row, record, revision) | the elections page and six others | moved into RDM with `9527d75b`, 2026-10-13; older than this repo's history | the spec move |
| cohort | plain English in v1 | rafka-v2 `7b213d6eda` | v1 |
| `LeadershipView`, "resolver" (not "leadership election", which is Luke's, Q107) | the elections page | rafka-v2 `5ec878990f`, 2026-10-05, "bind elections to imported RDM leadership resolver" | agent docs commit under Luke's git user |
| lane (`build_lanes.rs`, "its mesh's lane") | e6.1 | RDM `8c090b74`, 2026-10-17, under Q24 and Q28 | the lead; the word is never inside Luke's quoted words in those rulings |
| write cut-off (`SeatCutOff`, `WriteCutOff`) | R14 item 4 | RDM `f63a8ca2`, 2026-10-11; `9c0a9965`, 2026-10-18 | architect 2's ruling; ruled gone by Luke, Q109 B, 2026-10-21 |
| cut-off view, `Membership::authorizes()` | PR 36 | RDM `acea859e`, 2026-10-05; widened by Q58, `72a7d753`, 2026-10-18 | an agent PR; Q58 ruled by the lead under delegation |

## 3. What each word stands for, in the terms we have

Every destination is one of two things: a term already on a page, cited, or Luke's own phrase of
2026-10-22. Where neither covers the thing, the thing goes with the word. There is no third option; a
line that would need one is listed in §6 as his call.

| Removed word | Stands for | Term, and where it is defined |
|---|---|---|
| the mesh's authority | the mesh-primary | `fabric-authority.md` §2.3.1 and `build.md:218` already say "the current fabric-primary" / "the mesh-primary" in the same sentences |
| the fabric's authority | the fabric-primary | same |
| a cohort's primary, "cohort seat" | the node-type primary | Luke, 2026-10-22; not yet on any page |
| cohort | the nodes of one type in one mesh (`(mesh, node_type)`); its primary is the node-type primary of that mesh, a node of that type, a node-admin only for the `node-admin` type | Q107 (1): "every cohort `(mesh, node_type)` runs one election"; `node-rpc-envelope.md` JoinNode `types` |
| mesh-primary seat, fabric seat; "holds the seat" | being the mesh-primary, being the fabric-primary | the terms themselves |
| holder | the primary (mesh-primary, fabric-primary, node-type primary) | the terms themselves |
| seat row, seat move, committed seat row, `seat-moves/<T>…` | the node row's `is_primary` and `is_fabric_primary`, written by `node.primary.update` and `node.fabric-primary.update` (the update sequence below) | `node-rpc-envelope.md:362-363`: "`is_primary` … `node.primary.update`; never a put", "`is_fabric_primary` … `node.fabric-primary.update`" |
| committed (seat, row, transfer, takeover) | written; one writer writes the row | Q109 A: one writer per row, the storage decides nothing |
| takeover, take seats, `TakeSeats` | take over | Luke's verb ("taken over fabric-primary", 2026-10-20); 26 page lines already use it |
| seat revision | the primary's `revision` | `revision` is the vocabulary (CLAUDE.md: "revision, never epoch") |
| `Seated` frame | the `primary updated` announcement | `gossip.md:157` already carries `primary updating` / `primary updated` for the same operation; one of the two goes (§6) |
| seat watch, `seat_watch.rs` | the staleness-floor rule of every primary: a primary unheard past the floor is superseded by the lowest ReadyForTraffic node of its type in its mesh | Q107 (1); `fabric-node-lifecycle-elections.md` §4.3 for the fabric-primary; named by the operation it leads to, `node.primary.update` or `node.fabric-primary.update` |
| `LeadershipView`, resolver | the leadership election and its result | Luke, Q107: "leadership election"; `fabric-node-lifecycle-elections.md` §1 |
| lane, `LaneId`, `rev-<R>/<seq>` | the Build journal's records at their own keys | `build.md:413` "the Build journal"; Q109 items 12-13: one row at build_id, one at (build_id, mesh, attempt), one claim at (build_id, mesh, attempt) |
| write cut-off, `StorageError::CutOff`, held rows, lift, restore | nothing: a writer finishes the writes it issued before it writes its row | Q109 B, Luke's sentence |
| cut-off view, `authorizes()`, healing window | nothing: a node-admin acts on its view and answers a question by its direct operations | R-RX1 (Luke, 2026-10-12): no authorization in the framework |
| `NotSeatHolder` (the fabric store's refusal by seat record) | nothing: storage decides nothing | Q109 |


**The rule the sequence serves (Luke, 2026-10-22; Q89; Q107 (4)).** The mesh-primary and the
fabric-primary are transferred together: the fabric-primary is the mesh-primary of one mesh. They are
separated in one case only: no node-admin of that mesh can be identified from the backbone. Then the
mesh-primary of another mesh takes the fabric-primary temporarily, stays mesh-primary of its own mesh,
and re-creates the lost mesh's node-admins; once the node at the first primary's path.name, returned
or re-created, has been ReadyForTraffic for one floor, the fabric-primary returns to that mesh together
with that node being its mesh-primary.

**The update sequence that replaces the seat rows.** Who is primary is one fact with one writer, so it
is one field on one row, written once. The Mesh row (`meshes/<mesh>`) is the mesh-primary's row and
"lifecycle truth" (`fabric-authority.md:178`); the fabric row (`fabric/`) is the fabric-primary's
(`node-identity-and-state.md:264, 314`). Each carries its primaries:

- the Mesh row: the mesh-primary and each node-type primary of that mesh, as `(node_id, incarnation,
  revision)` per type;
- the fabric row: the mesh whose mesh-primary is the fabric-primary, and, while that is a takeover,
  `first_primary`: the path.name the fabric-primary is handed back to (Q107 (4)'s words). Because the
  fabric row names a mesh and not a node, the fabric-primary follows that mesh's mesh-primary by
  construction, and the combined transfer of the rule above is one write.

1. A mesh-primary changes by `node.primary.update`, whether it hands over itself or is unheard past
   the staleness floor: the successor writes the Mesh row once, naming itself, since it is that row's
   writer from then. If its mesh holds the fabric-primary, it is now the fabric-primary too: the fabric
   row names its mesh and does not change. A node-type primary of any other type changes by the same
   operation, the Mesh row written by the mesh-primary.
2. The one exception, `node.fabric-primary.update` from the backbone: when no node-admin of the
   fabric-primary's mesh is available to the backbone, the lowest ReadyForTraffic mesh-primary on the
   backbone (Q89) writes the fabric row once, naming its own mesh and setting `first_primary` to the
   lost primary's path.name if `first_primary` is absent. An existing `first_primary` is preserved: a
   takeover of a takeover (A's mesh lost, B took it, B's mesh lost before A's returned, C takes it) keeps
   A's path.name, so the hand-back destination is never lost. The temporary holder's mesh-primary may
   change by step 1 meanwhile; the fabric-primary follows it and the fabric row does not change.
3. The hand-back (Q107 (4)): once the node at `first_primary` has been ReadyForTraffic for one floor,
   the temporary holder writes the fabric row once, naming that node's mesh and clearing
   `first_primary`. The fabric-primary is that mesh's mesh-primary from then, which is that node by the
   rule above. `handback_due` reads `first_primary` from the fabric row instead of the seat-move log; a
   restart of the temporary holder reads the same field.
4. A planned `node.fabric-primary.update` to another mesh (mesh.delete of the fabric-primary's mesh is
   the one page case) names the destination mesh and clears `first_primary`: the command decides where
   the fabric-primary lives, so it cancels an outstanding hand-back, stated here so it is never silent.
5. Every one of these is one write of one row: the row lands or answers a named error (R14's fs
   contract, as node-primary-update.md already states for the one row); on a named error nothing
   changed and the step fails by name. There is no second write to resume. A node-admin that restarts
   reads the Mesh row and the fabric row and holds what they say.
6. The operation then announces `node.primary.updated` (gossip.md, "primary updated"); the announcement
   is never the fact. `is_primary` and `is_fabric_primary` are properties `node.get` answers from the
   Mesh row and the fabric row (`node-rpc-envelope.md:362-363`, "never a put"); no node row is touched by
   a primary change.
7. A joining or restarting node reads the fact, never gossip: `node.start` step 4 reads its projections
   from its node-admin; a join sent to a node that is not the primary is answered
   `ReceiverNotPrimary { needed }` naming the one to report to (`node-rpc-envelope.md:709`,
   node-mesh-join.md step 3).
8. A reader of the store folds nothing: the rows say who is primary. The `seat-moves/<T>` log, its fold
   and its revision bookkeeping go; `revision` stays on the row as the primary's revision.

The Build records are the fabric-primary's. It writes every Build record (acceptance, attempts, claims,
step receipts, results) through the configured Build storage (`build.md:218`: "Authority is the current
fabric-primary ... never creates a second writer"). A mesh-primary runs its mesh's attempt and reports
each receipt up the attempt's own `build.attempt.run` stream; running an attempt does not make it a
Build-record writer. It writes its own mesh's rows: the Mesh row, its members' node rows and status rows.
`build.md:350`'s "one claimant per mesh for drift (its mesh-primary, on its mesh's in-flight row)" is
rewritten to this.

## 4. Inventory

### 4.1 Pages (`docs/architecture`), lines carrying each word

| Page | authority | seat | committed seat | cohort |
|---|---|---|---|---|
| build.md | 30 | 5 | 0 | |
| gossip.md | 25 | 23 | 5 | |
| fabric-authority.md (the page itself goes or is renamed by its content) | 24 | 2 | 0 | |
| node-rpc-envelope.md | 21 | 35 | 6 | |
| lifecycles/node-mesh-join.md | 18 | 0 | 0 | |
| fabric-node-lifecycle-elections.md | 8 | 44 | 6 | |
| lifecycles/node-cert-renew.md | 8 | 0 | 0 | |
| deployment-provider.md | 7 | 0 | 0 | |
| node-identity-and-state.md | 6 | 12 | 1 | |
| fabric-node-hooks.md | 5 | 0 | 0 | |
| node-rpc.md | 4 | 0 | 0 | |
| fabric-mesh-node-lifecycles.md | 4 | 5 | 1 | |
| fabric-certs.md | 4 | 2 | 0 | |
| lifecycles/node-primary-update.md | 3 | 41 | 2 | |
| lifecycles/node-recovery.md | 3 | 4 | 0 | |
| lifecycles/node-start.md | 3 | 0 | 0 | |
| node-admin-lifecycle.md | 2 | 8 | 0 | |
| lifecycles/fabric-recovery.md | 1 | 16 | 0 | |
| lifecycles/mesh-investigation.md | 2 | 2 | 0 | |
| lifecycles/node-status.md | 2 | 0 | 0 | |
| lifecycles/mesh-create.md | 0 | 11 | 0 | |
| lifecycles/states.md | 0 | 10 | 0 | |
| lifecycles/mesh-delete.md | 0 | 6 | 0 | |
| lifecycles/fabric-delete.md | 0 | 5 | 0 | |
| lifecycles/node-tickle.md | 1 | 4 | 0 | |
| lifecycles/node-connections-delete.md | 0 | 3 | 0 | |
| lifecycles/fabric-create.md | 1 | 2 | 0 | |
| one line each: node-runtime-inspect, node-replace, node-name-update, node-issuer-update, node-create, connections (authority); node-stop, node-restart, node-name-update, node-connections-admit (seat) | | | | |
| **total lines** | **188** | **244** | **21** | **45** |

Also: "lane" on build.md (269, 323, 413), node-recovery.md (23, 114, 133, 155, 185),
node-rpc-envelope.md (990); "cut-off" on gossip.md §6 (774-786, 965), fabric-authority.md:110,
node-admin-lifecycle.md:38, fabric-recovery.md:28, node-rpc-envelope.md:1363 and the eight lines of
node-primary-update.md already listed in `i143-q109-b-cut-off-page-text.md`; "leadership" on 61 lines;
"holder" on 133.

### 4.2 Code

Identifiers, by distinct name (uses across `crates/`):

| Family | Identifiers (uses) |
|---|---|
| seat | `seat` 3741, `seats` 1239, `Seat` 480, `SeatHolder` 176, `seated` 174, `mesh_seat` 99, `CommittedSeat` 97, `fabric_seat` 95, `SeatMoved` 68, `Seated` 68, `seat_move` 64, `SeatMoveEnd` 51, `SeatMove` 48, `SeatTaken` 43, `seat_of` 43, `SeatLost` 43, `seats_as_expected` 42, `Seats` 39, `WireSeat` 38, `SeatBook` 38, `seat_moves` 37, `seat_revision` 35, `SeatYielded` 33, `seat_watch` 33, `holds_fabric_seat` 32, `SeatTransferRow` 28, `holds_seat` 28, `seat_transfers` 25, `SeatUpdate` 24, `SeatSource` 24, `holds_mesh_seat` 24, `BothSeats` 24, `SeatRevision` 23, `SeatRecord` 22, `FabricSeat` 20, `SeatMoveRow` 18, and the long tail |
| authority | `authority` 1220, `NotAuthority` 115, `RejectedNotAuthority` 72, `Authority` 59, `StatusAuthority` 43, `rejected_not_authority` 42, `NotTheAuthority` 17, `bounded_not_authority` 14, `authorities` 14, `authoritative` 12, `authority_of` 11, `authority_may_move` 11, `NoAuthority` 10, `AcceptingAuthority` 8, `status_authority` 7, `authority_moved` 7, `resolve_authority` 5, `authority_with_store` 5, `authority_incarnation` 5, `ReportsToTheAuthority` 3 |
| committed seat | `CommittedSeat` 97, `MoveCommit` 21, `commit_seat_move` 17 |
| lane | `build_lanes.rs`, `LaneId`, `LaneKey`, `lane_dir`, `lane_of`, `lane_seat`, `REV_PREFIX`, `rev_dir`, `seq_key` |
| cut-off | `SeatCutOff`, `WriteCutOff`, `CutOffWrites`, `cut_off_writes`, `cut_off_by`, `cut_off_other_meshes`, `lift_other_meshes`, `drop_other_meshes`, `cut_off_own_labels`, `lift_cut_off`, `drop_held`, `cut_off_and_drain`, `StorageError::CutOff`; `CutOff`, `authorizes`, `cut_off_view`, `view_authorizes` |
| leadership | `leadership` 33, `LeadershipView` 2 |

Files named by a word (renamed or removed): `crates/rdm-core/src/seat.rs`, `crates/rdm-admin/src/seat_watch.rs`,
`seat_revision.rs`, `build_lanes.rs`, `crates/rdm-admin/tests/pipeline_moves_no_seat.rs`,
`status_seat_fold.rs`, `crates/rdm-transport/tests/seat_frames_wire.rs`,
`crates/rdm-test/tests/elections/late_authority.rs`, `sticky_seat.rs`, `cohort_election.rs`,
`role_cohort.rs`, `crates/rdm-test/tests/mesh/cohort_recovery.rs`, `crates/rdm-test/tests/gossip/seat_replacement.rs`,
and the two `rafka-test-scenario` artifact names under `mesh-elections/`.

**Wire types and durable formats: marked, because they are the cost.** These are not renames of a local
name. Each is a wire frame, a typed reply or a format on the shared store root, and changes under the
canon's durable-format rule and its commit marker (`durable format moved`); a retired format's decoding
stays.

| Kind | Item | Where |
|---|---|---|
| wire, enum | `Seat { MeshPrimary, FabricPrimary, Cohort(NodeKind) }`, `SeatHolder`, `CommittedSeat` | `rdm-core/src/seat.rs` |
| wire, 0x21 PrimaryUpdate | `SeatRevision`, `SeatUpdate`, `RejectedNotAuthority` | `rdm-core/src/handover.rs` |
| wire, join | `JoinReply::NotAuthority { primary }` | `rdm-core/src/join.rs:76` |
| wire, status | `NotAuthority { SubjectUnknown, ReceiverNotPrimary { needed } }`, `AuthorityChanged`, `AuthorityReady`, `AuthorityAddressed` | `rdm-core/src/status.rs:826, 1339-1348` |
| wire, topology | `Seats { … }` | `rdm-core/src/topology.rs:171` |
| wire, Build | `SeatYielded`, `SeatMoved` | `rdm-core/src/build.rs:493, 539` |
| gossip frame | `Seated { seat, holder }` | `gossip.md:158`; `rdm-transport` |
| durable format | `seat-move/1`, `seat-move/9` at `seat-moves/<T>--<node.id>--<incarnation>.json` | `rdm-admin/src/fabric_storage.rs:13, 499` |
| durable format, retired already | `fabric-seat/1`, `fabric-seat-transfer/1` (decoding stays) | `fabric_storage.rs:486` |
| durable format | `build-fact/1` at `rev-<R>/<seq>.json` (lanes) | `rdm-admin/src/build_lanes.rs` |

### 4.3 The record and the rulings

`docs/plans` carries the words 795 (seat), 436 (authority), 372 (lane), 128 (cut-off) and 27
(committed seat) times, across 20 ruling files and the PRD. Those rows are the record of what was said
when it was said and stay as written. Every new row, packet and ruling uses the terms of §3.

### 4.4 Scripts and CLAUDE.md

`scripts/name-check.py` and `test_name_check.py` (§5), and the words in comments or job names of
`cells.sh`, `rdm-gate-at.sh`, `rebase-gate-land.sh`, `i143-export-run.sh`, `rustc-wrapper.sh`.
`CLAUDE.md` line 104 ("any admin that takes the fabric seat") and line 102.

## 5. Stop the propagation today: the name check

`scripts/name-check.py` refuses a landing that adds a retired word to a definition name or a file name.
It is scoped to names the diff introduces: a definition on an added line that no removed line defined,
and an added or renamed file. Prose, string literals and existing identifiers in a touched file are never
matched, so every ordinary edit to the 3,741 `seat` uses still lands; only a new `seat`, `authority`,
`cohort`, `lane` or `committed` name is refused.

Landing 0, recommended first and alone: add `seat`, `seats`, `seated`, `authority`, `authorities`,
`authoritative`, `cohort`, `cohorts`, `lane` and `lanes` to `RETIRED`, with their cases in
`test_name_check.py`, and the same words to the vocabulary list in CLAUDE.md. Not added: "cut" (the cut
jobs and cells are named by it with another meaning), "committed" (`StateCommitted`, the state-commit
publication, 755 uses with no seat in them) and "holder" (`holder_of`, `record_holder`, 2,591 uses):
those three go by removal in landings 1 and 4, not by the check. From landing 0 on, no agent can add one
of the checked words to the code again, whatever page it reads it on.

## 6. Family words, Luke's call, recommendation first

These have no life apart from seat. Recommendation: they go with it, in the same landings.

- **holder** (133 page lines): "the primary".
- **`LeadershipView`, resolver** (35 code uses): "the leadership election" and its result, Luke's phrase.
- **`Seated` or `primary updated`**: two announcements for one operation. Recommendation: `Seated` goes;
  `node.primary.updated` is the operation's own announcement and is already on the wire.
- **sticky** (3 lines, "the sticky holder"): elections §3's sentence "a primary stays until it is proven
  unable" says it without the word.
- **yield, yielded** (`SeatYielded`, "the holder's yield"): the primary hands over by
  `node.primary.update`; no second word.

## 7. Order of landings

0. The name check (§5). Cheap, today, stops the spread before any removal.
1. The write cut-off and the words "committed seat" out of code and pages (lane e7s3, proposal B's eight
   lines plus the 21 lines of §4.1's third column).
2. Lanes out: Build records at their own keys through the configured Build storage, every one written
   by the fabric-primary; a mesh-primary runs its mesh's attempts and reports receipts up the attempt's
   stream (§3's writer rule). Q113 withdrawn. `build-fact/1` under the durable-format rule.
3. `authorizes()` and the cut-off view out, with their 18 callers, 11 page lines and 9 cells, and a
   before-and-after flame, since it is gossip code.
4. seat, authority and cohort out, page by page, each page's old and new lines listed in that landing's
   file, the wire and durable items of §4.2 under the durable-format rule in the same landing as the page
   that names them. Q110 (Luke, 2026-10-21) lands in the same pass: the pages still say a primary changes
   only on "the provider's exit proof" or "a proven loss" (elections page lines 92, 171, 224 and nine more;
   57 lines on twelve pages; 29 source files with `is_proven_gone` and the exit-proof hold), and Q110 ruled
   that an unreachable node is re-created with a new node id, refused at admission when it comes back, then
   killed, with nothing kept verifying it. Those lines are rewritten once, in the terms of §3, never in the
   old words.

5. Q110's machinery out (§8): the exit proof as the condition of any primary change or recovery, the
   hung hold, the held-running and exit rows, and the sticky file whole.

Every landing lands its page lines and code together. Cells that prove the operations stay: a primary
changes, a mesh keeps repairing, a Build recovers, both storage backends. Cells whose only purpose was a
removed word's machinery go with it. Nothing removed gets a renamed equivalent.

## 8. Q110: the exit-proof machinery, and the sticky file

Q110 (Luke, 2026-10-21): "node is unreachable, you create a new node; you don't keep verifying forever;
if it comes back it's denied because its cert doesn't match the node-id that is in the topology anymore,
then you kill it." Q111 (Luke, the same day): "anything that requires proof of termination is removed";
"this whole 'you must prove this thing is dead': that's what the node investigation is for". Q111's row
counts about 94 statements on 20 pages and says the lead lists them by line before the page text comes to
Luke; this section is that list's start, by file, and the line list follows it. Both rows still read "the
page text for Luke's yes, then the code": nothing of either has landed. What stands in the pages and the code instead is the older rule, that only the provider's proof
of a runtime's exit lets a primary change or a recovery start, and everything built to produce, hold and
wait for that proof. All of it goes; the kill stays.

**What goes.**

| Thing | Where | Uses |
|---|---|---|
| the exit proof as a condition: `is_proven_gone`, `exit_proof`, `ExitProofs`, `exit_proof_row(s)`, `put_exit_proof`, `nodes_exit_proof`, `exit_row_of_birth` | `election.rs`, `seat_watch.rs`, `seat_revision.rs`, `fabric_handover.rs`, `recovery.rs`, `offline.rs`, `records.rs`, `views.rs` | 41 `exit_proof`, 13 `ExitProofs` |
| the exit rows: `NodeExitRow`, `put_node_exit(_of)`, the `exits` status directory, format `node-exit/1` | `status_storage.rs:721-746`, `rdm-core/src/status.rs` | 87 `NodeExitRow` |
| the hung hold (R-FM1): `RDM_HUNG_HOLD_FLOORS`, `held_running`, `NodeHeldRunningRow`, `HeldRunning`, `put/remove_node_held_running_of`, `clear_held_running`, the `held-running` status directory | CLAUDE.md:102, `records.rs`, `status_storage.rs:755`, `rdm-core/src/views.rs:70` | 73 `held_running`, 34 `NodeHeldRunningRow` |
| the os-stopped verdict as a hold input: `OsStopped`, `NodeOsStoppedRow`, `os_stopped_confirmed`, `put/remove_node_os_stopped_of`, `clear_os_stopped` | `rdm-core/src/views.rs:100`, `status_storage.rs`, `offline.rs` | 118 `OsStopped` |
| the "proves the exit" role of `TerminateRuntime` and of `node.runtime.inspect`: the wait for the proof after the terminate, the `exit-proven { code }` answer as a precondition | `deployment/pipeline.rs`, `deployment/provider.rs`, `build_drive.rs`, `recovery.rs` | 218 `TerminateRuntime` |
| the sticky file, whole: `crates/rdm-test/tests/elections/sticky_seat.rs` (747 lines, six cells) | | |

**What stays.** Node investigation, whole: it is Q111's one decision. `node.runtime.inspect` as the
investigation's provider step and as node.delete's primitive (an `os-stopped` runtime skips the stop by
name). `TerminateRuntime { node_id, incarnation, handle }` as a Build step: it is Q110's "then you kill
it". R-FM2, the data-dir-lost recovery (`RDM_DATA_DIR_GONE_FLOORS`), which is about an absent data dir,
not about proof. The provider's exit code where a Build step records what it saw, as a plain fact.

**Pages.** The elections page lines 25, 92, 115, 123, 157, 158, 159, 171, 224, 237 and 256;
node-runtime-inspect.md ("It is the only thing in the design that proves a runtime is gone", 15 lines);
node-recovery.md (14 lines: the hold, the terminate "which proves the exit", `held-running`);
mesh-investigation.md (12 lines); deployment-provider.md (7); states.md, node-primary-update.md,
mesh-delete.md (5 each); fabric-recovery.md (4); fabric-delete.md (3); node-identity-and-state.md,
node-rpc-envelope.md, gossip.md, fabric-certs.md (2 each); node-delete.md (1). CLAUDE.md line 102.
Under Q107, Q110 and Q111 the sequence for an unheard primary is, in order:

1. Unheard past the staleness floor, the lowest ReadyForTraffic node of its type in its mesh takes over
   by the one row write of §3 (Q107 (1); Q89 for the fabric-primary, whose successor is its mesh's
   lowest Ready node-admin). No investigation runs before this write (Q107 (2)).
2. The mesh-primary, a node-admin, runs node investigation on the unheard node as a member, whatever
   the node's type (Q107 (2); Q111: "that's what the node investigation is for"). When the unheard
   node was the mesh-primary, the node-admin that took over in step 1 is the mesh-primary and runs it;
   when it was an rpc or other node-type primary, the mesh-primary that was already there runs it. Its
   verdict is the one decision: reached, the mesh-primary commands the node back through
   `node.mesh.join`; any other verdict, the mesh-primary re-creates it at its path.name with a new node
   id (Q110), over the inherited data dir when the investigation's provider step found the old runtime
   exited and over a newly allocated one otherwise (Q111, R-FM2). When no node-admin of the mesh is
   available to the backbone, the temporary fabric-primary of §3 step 2 does this for that mesh.
3. An old birth that comes back is refused at admission by its cert (Q106, `superseded-birth`); the
   refusing node reports it to the fabric-primary, which terminates its runtime (Q111).

Nothing holds and nothing waits for proof: what goes is the proof as a precondition of steps 1 and 2 and
the hold around it, never the investigation.

**The sticky file's six cells, and what becomes of each.**

| Cell | Proves today | Under the removal |
|---|---|---|
| `a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh` | Q89: the mesh's lowest Ready admin takes over | kept as a proof, written again under a name in the terms of §3; the file does not move |
| `a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone` | a primary change is published across the backbone | same |
| `a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat` | the cut-off view and the hung hold | goes with landings 3 and 5 |
| `a_mesh_cut_off_whole_from_the_backbone_loses_the_fabric_seat_at_the_decision_step_and_gets_it_back_after_the_heal` | Q89's decision step and the seat's return after a heal | both stay as proofs: the decision step (Q89, §3 step 2) and the hand-back (§3 step 3) of the fabric-primary to the first primary's mesh, together with the node at that path.name being its mesh-primary, once it has been ReadyForTraffic for one floor (Q107 (4)); only the cell's cut-off-view assertions go |
| `a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts` | node.primary.update writes one row before the successor acts | kept as a proof: a same-mesh successor's one Mesh row write moves both primaries, the fabric row unchanged (§3 step 1), landed before the successor acts |
| `a_claim_in_flight_commits_before_the_old_holders_seat_row_and_none_is_decided_after` | the Build lane's in-flight claim against the seat row | goes with landing 2 |

The cut-off view's cell and the in-flight claim's cell go with their machinery. The four proofs that
remain are proofs of operations (a primary takes over, a primary change is published, a hand-over writes
one row first, the fabric-primary is handed back) and move to a file named by the operation, not by
"sticky", keeping the assertions that prove the operation and dropping those that prove removed
machinery.
