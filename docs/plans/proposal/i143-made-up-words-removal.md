# Proposal: the made-up words, where they came from, and their removal (i143)

Luke, 2026-10-22: "we have terms fabric-primary, mesh-primary, node-type primary; use the terms, do not
make new terms", "there is no authority, there is no committed seat", "there is no race because there is
always one writer for each item", and "do not replace the removed mechanisms with renamed equivalents".
This page is the removal written at the root: where each word came from, what it stands for in the
terms we have, every page and code file that carries it, the landing that stops it spreading today, and
the order the rest comes out in. Nothing here starts until Luke has read it.

## 1. The finding

None of these words is Luke's. His quoted words in `docs/plans` say mesh-primary, fabric-primary,
"a secondary node-admin in its mesh", "taken over fabric-primary", "NA1 calls node.primary.update on
NA2", "the ruling is storage is for persistence, never business rules". Not once: seat, authority,
cohort, lane, cut-off, committed, leadership. Each word below entered through a docs commit an agent
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
| leadership, `LeadershipView`, "resolver" | the elections page | rafka-v2 `5ec878990f`, 2026-10-05, "bind elections to imported RDM leadership resolver" | agent docs commit under Luke's git user |
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
| cohort | the node type (`node-admin`, `rpc`) | `node-rpc-envelope.md` JoinNode `types`; `NodeKind` in code |
| mesh-primary seat, fabric seat; "holds the seat" | being the mesh-primary, being the fabric-primary | the terms themselves |
| holder | the primary (mesh-primary, fabric-primary, node-type primary) | the terms themselves |
| seat row, seat move, committed seat row, `seat-moves/<T>…` | the row `node.primary.update` writes: `is_primary` and `is_fabric_primary` on the node row | `node-rpc-envelope.md:362-363`: "`is_primary` … `node.primary.update`; never a put", "`is_fabric_primary` … `node.fabric-primary.update`" |
| committed (seat, row, transfer, takeover) | written; one writer writes the row | Q109 A: one writer per row, the storage decides nothing |
| takeover, take seats, `TakeSeats` | take over | Luke's verb ("taken over fabric-primary", 2026-10-20); 26 page lines already use it |
| seat revision | the primary's `revision` | `revision` is the vocabulary (CLAUDE.md: "revision, never epoch") |
| `Seated` frame | the `primary updated` announcement | `gossip.md:157` already carries `primary updating` / `primary updated` for the same operation; one of the two goes (§6) |
| seat watch, `seat_watch.rs` | the staleness-floor rule of the fabric-primary | `fabric-node-lifecycle-elections.md` §4.3; named by the operation it leads to, `node.fabric-primary.update` |
| leadership, `LeadershipView`, resolver | the election | `fabric-node-lifecycle-elections.md` §1: "Election is a deterministic projection" |
| lane, `LaneId`, `rev-<R>/<seq>` | the Build journal's records at their own keys | `build.md:413` "the Build journal"; Q109 items 12-13: one row at build_id, one at (build_id, mesh, attempt), one claim at (build_id, mesh, attempt) |
| write cut-off, `StorageError::CutOff`, held rows, lift, restore | nothing: a writer finishes the writes it issued before it writes its row | Q109 B, Luke's sentence |
| cut-off view, `authorizes()`, healing window | nothing: a node-admin acts on its view and answers a question by its direct operations | R-RX1 (Luke, 2026-10-12): no authorization in the framework |
| `NotSeatHolder` (the fabric store's refusal by seat record) | nothing: storage decides nothing | Q109 |

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
`authoritative`, `cohort`, `cohorts`, `lane`, `lanes`, `committed`, `leadership` and `holder` to
`RETIRED`, with their cases in `test_name_check.py`, and the same words to the vocabulary list in
CLAUDE.md. "cut" is not added: the cut jobs and cells are named by it with another meaning. From that
landing on, no agent can add one of these words to the code again, whatever page it reads it on.

## 6. Family words, Luke's call, recommendation first

These have no life apart from seat. Recommendation: they go with it, in the same landings.

- **holder** (133 page lines): "the primary".
- **leadership, `LeadershipView`, resolver** (61 page lines, 35 code uses): "the election" and its result.
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
2. Lanes out: Build records at their own keys through the Build storage contract, one writer each, no
   routing through the fabric-primary; Q113 withdrawn. `build-fact/1` under the durable-format rule.
3. `authorizes()` and the cut-off view out, with their 18 callers, 11 page lines and 9 cells, and a
   before-and-after flame, since it is gossip code.
4. seat, authority and cohort out, page by page, each page's old and new lines listed in that landing's
   file, the wire and durable items of §4.2 under the durable-format rule in the same landing as the page
   that names them.

Every landing lands its page lines and code together. Cells that prove the operations stay: a primary
changes, a mesh keeps repairing, a Build recovers, both storage backends. Cells whose only purpose was a
removed word's machinery go with it. Nothing removed gets a renamed equivalent.
