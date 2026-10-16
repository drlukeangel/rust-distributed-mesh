# i147 — gossip conformance: no stale frame undoes a newer fact, no required fact is lost to arrival order

**Status:** approved 2026-10-16 (Luke, Q17 of the i144/i146 stream, arm A on the a1 architect's shape).
Nothing built; the first landing is one proven regression. **Tracking issue:** [drlukeangel/rafka-v2#3130](https://github.com/drlukeangel/rafka-v2/issues/3130). **Record:** this file; every state or
remaining-work update goes in an epic's row below. **Owner:** the i143 lead; an RDM engineer in a pool
lane. **Separate from i146:** gossip stays best-effort (gossip.md §7, R-NS2); this unit fixes what
receivers do with the frames they receive. No global counter, no acknowledgements, no reliable-gossip
layer.

## Context

The receive path has one check (`crates/rdm-transport/src/receive_order.rs`), but only 9 of the 22
frame kinds carry an order key it can apply, each with its own field and its own idea of "source"
(Digest; Members and MembersDelta; NodeDeleting, NodeRestarting, NodeStopped; Seated; PrimaryUpdating
and PrimaryUpdated). The other 13 (NodeDeleted; MeshStatus, FabricStatus; Concern;
NodeConnectionsDeleting, NodeConnectionsDeleted; NodeLeaving, NodeLeft; NewFabricPrimary;
StateCommitting, StateCommitted, TrafficOpening, TrafficOpened) pass the check untouched and are
taken as they arrive, each handler applying whatever data rule it already has (receive_order.rs:25-26,
cell :471-472). For a keyed frame, an equal key is accepted and the handler absorbs the copy. So gossip
has five different staleness shapes across nine kinds and nothing at all for thirteen, and no
duplicate rule anywhere.

Two questions were being asked by one check. **Duplicate**: the same frame delivered again by the
fan-out; answered by the frame's own identity (the exact operation and birth, the chunk's key), not
by a transport counter. **Stale**: a frame about a subject older than what the receiver holds about
that subject; answered by the subject's own revision, which differs by category. A per-author
sequence was considered and refused (Luke, a1 2026-10-16): it would drop a terminal fact because an
unrelated newer frame arrived first.

## The rule

No stale frame may undo a newer fact, and no required fact may be lost merely because another frame
arrived first. Every active frame kind has an explicit stale/duplicate rule, stated on gossip.md and
covered by a cell.

## The frame-contract categories

| Category | Required behaviour |
|---|---|
| Replacement state | Install only a newer revision of the same source projection |
| Snapshot or delta | Install atomically against the correct source revision and base; equal-key chunks remain valid |
| Operation fact | Match the exact operation and birth; a duplicate does nothing; an unrelated message cannot supersede it |
| Reserved frame | Refuse by name and apply nothing |

The table over all 22 kinds landed on gossip.md §3.1 "Receive rules: stale, duplicate and gap, per frame"
(Luke's yes 2026-10-16, with the a1 architect's seven corrections). e1's deliverable is the first proven
regression against it; e2's are the cells for every row.

## Requirements

| # | Requirement | State | Evidence today | Epic |
|---|---|---|---|---|
| R1 | the frame-contract table: every frame kind maps to its receive category, subject, ordering and identity | HOLDS | gossip.md §3.1 approved table; `every_frame_kind_has_a_row_in_the_receive_rules` covers all 22 Frame variants; e1 gate GREEN | e1 |
| R2 | replacement-state frames install only a newer revision of the same source projection; an equal revision is a duplicate and does nothing | BUILD | equal keys accepted and "absorbed" | e2 |
| R3 | snapshot and delta frames install atomically against the correct source revision and base; reordered chunks still complete; equal-key chunks valid | BUILD | §3.3 states the top-up; the atomic rule is unproven | e2 |
| R4 | operation facts match the exact operation and birth; duplicates do nothing; an unrelated newer frame never supersedes a terminal fact | BUILD | 13 kinds taken as they arrive | e2 |
| R5 | reserved frames (NodeLeaving, NewFabricPrimary, and every other reserved position) are refused by name and mutate nothing | BUILD (check) | `rdm.mesh.membership.reject.via-node-leaving` exists for one | e2 |
| R6 | a publisher change: a delayed predecessor's frame cannot overwrite the successor's projection | BUILD | the seat-revision rule covers Seated only | e2 |
| R7 | recovery through a topology read followed by stale gossip: no rollback | BUILD | — | e2 |
| R8 | duplicated or contradictory checks removed from the receive path; a field added only where a demonstrated gap cannot be closed with the information already carried, each as its own packet with reseal and marker | BUILD | five key shapes | e2 |
| R9 | gossip stays best-effort: no global counter, no acks, no reliable layer; nothing from i146 enters it | HOLDS | §7 | every epic |
| R10 | `MeshStatus` and `FabricStatus` carry no publisher revision, so a delayed predecessor's frame is not refused by revision; closed by its own packet (a field, or the status riding the publisher's projection), reseal and marker if a field | BUILD (gap, recorded on gossip.md) | `(publisher: String, changed_at_rafka_ms)` only | e2 |
| R11 | `Concern` is an idempotent investigation wake for the current episode: a repeat while the observer's silence mark stands does nothing; the episode ends when the receiver hears the birth again; it never establishes exit or vacancy | BUILD | the tuple is deduplicated without an episode | e2 |
| R12 | a delta whose base is ahead of the installed version is a gap: repair requested (§3.3, §6), spanned `via-gap`, never logged stale; equal revision with different content is `via-inconsistent`, refused by name | BUILD | mismatched base handling unproven; no inconsistency refusal | e2 |

## § OTLP span-assertion bar

| Span | Owner | Attributes | Cells |
|---|---|---|---|
| `rdm.mesh.membership.reject.via-stale` | the receiver | frame, source, held_revision, frame_revision | the newer-then-older cells |
| `rdm.mesh.membership.update.via-duplicate` | the receiver | frame, operation or key, outcome `ignored` | the repeat cells |
| `rdm.mesh.membership.reject.via-reserved` (the existing `via-node-leaving` generalised) | the receiver | frame position | the reserved cells |
| control: a terminal fact applied after an unrelated newer frame carries its ordinary apply span | — | — | the out-of-order fact cell |

## § Per-sprint seed-data obligation

NO NEW ENTITY. No wire change unless a demonstrated gap needs a field (then `wire format moved`,
mesh-audit reseal, as its own packet). No new gossip frame and no new `MemberStatus`.

## Implementation plan

#### e1 — the table and the first proven regression

- **Goal → gap.** The frame-contract table on gossip.md; one unkeyed MeshStatus with equal revision and different content refused as inconsistent, with its RED and GREEN flames (Luke, Q19=A).
- **Deliverables.** The red cell of the regression (the table is on the page); its fix in the receive
  path by the frame's own identity; the flame before and after; the gossip.md section as exact old →
  new to Luke; a gossip change, so Luke's permission is Q17 and the flame is required.
- **Acceptance cells.** `an_unkeyed_mesh_status_with_equal_revision_and_different_content_is_refused_inconsistent`
  (the regression); `every_frame_kind_has_a_row_in_the_receive_rules` (a unit cell over the catalog against the page's table).
  RED first.
- **State.** **LANDED** through `rebase-gate-land.sh`, 2026-10-16, lane `e7s3`, branch `codex/i147-e1-gossip-conformance`, Q19=A. RED SHA `35af8ca1`; GREEN/landed SHA `1da16141`. Before trace `f6fc35d858049ff6c75782276d66a969`; after trace `7af27c38a6aa16ca6f5dbf040b0ebd1e` shows `rdm.mesh.membership.reject.via-inconsistent` with frame `mesh-status`, subject `mesh1 mesh1.admin.1`, revision `100`, held status unchanged. Verification: live regression 1 passed, 0 failed (0.06 s); receive-order unit stem 8 passed, 0 failed (including the catalog); fast landing gate 1,623 passed, 0 failed, 0 ignored across 62 libtest summaries, 261 s total. Discovery: 1,774 tests against 1,772 on main; release-only jobs excluded by the fast gate. Gate log: `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-1da16141.log`. The receive framework classifies against the held status under its installation lock, replacing the MeshStatus handler check. Span reference regenerated. The approved architecture table was already on main at `ab129725`; code now meets its named inconsistency refusal for MeshStatus. No architecture or wire change. No unanswered packet; R10 and e2 remain outside this landing.

#### e2 — every row, one defect at a time

- **Goal → gap.** Each remaining row of the table whose rule the code does not meet is a defect with
  its own red → green cell; rows the code already meets get their cell and no change.
- **Acceptance cells (the a1 set, each against its defect).** newer state then older: no regression;
  a terminal fact after an unrelated newer frame: applied; a fact repeated: no duplicate effect;
  snapshot chunks reordered: complete and atomic; publishers change: the predecessor's delayed frame
  does not overwrite; recovery through a topology read, then stale gossip: no rollback; each reserved
  frame: named refusal, no mutation.
- **Order.** Finish MeshStatus named stale and duplicate outcomes, then FabricStatus; operation facts and Concern episodes; replacement state; snapshot or delta; reserved positions; recovery through a topology read. Each row lands separately and stops for review. R10 stays open with no new field.
- **State.** MeshStatus named stale/duplicate outcomes are **GREEN**, including the e1 inconsistency cell. Landing candidate `c393b290` was refused by the gate: node_restart sampled the release timestamp after Continue; provider_state tore down before recovery settled. Those two cell sequencing defects are corrected without increasing bounds; the 6 restart/provider cells passed in 41.56 s. No additional production change. Next: FabricStatus, not started; all later rows remain not started. R10 stays open, with no wire field or inferred publisher revision. Stop after this landing for review.

| Frame row | State | Evidence |
|---|---|---|
| `MeshStatus` | GREEN | Lane `e7s3`, branch `codex/i147-e2-mesh-status`. RED SHA `09d42858` (1 passed, 2 failed); GREEN code SHA `2697fa33` (3 live cells passed, 0 failed, 0.06 s). Stale before `ecc86037d4bf8ea46dff8faeb0c9a95c`, after `cea209eb2b76c8e7512a2161c0a12859`; duplicate before `7f4660d772157e251095c8c1cbfdcb79`, after `e7c2c444cc889b9d71011b5fb287a03e`. Stale names frame, subject, held key and frame key; duplicate names frame, declared identity and outcome `ignored`. Both assert the held projection and its change-notification count do not move. Forwarding metadata changes the wire bytes to exercise RDM dedup without changing authored identity/content. Classification stays in the receive framework under the status installation lock; the handler's generic debug substitute is removed, module documentation updated and span reference regenerated. Pre-landing fast gate at `2697fa33`: 1,633 passed, 0 failed, 0 ignored across 62 recorded libtest summaries, 84 s; discovery 1,776 against main's 1,774. Gate log `/home/admin/rust-distributed-mesh-deploy/gates-e7s3-2697fa33.log`. Delivery uses `rebase-gate-land.sh`, including its gate. Candidate `c393b290` gate RED (node_restart, provider_state), no landing: RED restart flame `8aa0dd8f1269ee29e7469d44ea485ae1`; RED recovery flame `4a08384b10a43575caf30bb032c4fa98` (central Jaeger `http://192.168.68.99:16687`). Recovery teardown aborts tasks at 21:47:48.664 after WaitForNodeReady starts at 21:47:48.631 and before the final receipt. The restart release timestamp moves before Continue; recovery teardown waits for the terminal receipt within the original deadline. Corrected stems: 6 passed, 0 failed (41.56 s), resumed-restart GREEN flame `099bb149d21bb334d598222bf24dd3fd`, settled-recovery GREEN flame `72517d6fa18433f79fa1f15b8e05cb7b` (local Jaeger `http://localhost:16686`). No architecture or wire change; R10 remains open; no ruling packet needed. |


## Verification

- [x] e1 green in the RDM gate with both flames cited in its row; the approved gossip.md receive table is on main.
- [ ] Every row of the table has a green cell; no frame kind is "taken as it arrives" without a stated rule.
- [ ] receive_order.rs has one shape per category and no contradictory check.

## Cross-references

- `docs/plans/i146-owed-messages-prd.md` (the directed-message unit this is separate from)
- `docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md` R-RX1 (data validity stays: version ordering of the same source, duplicate receipt keys, backward transitions)
- `docs/architecture/gossip.md` §3.1, §3.3, §7; `crates/rdm-transport/src/receive_order.rs`
- `docs/plans/i143-prd.md` e12.s72 (the gossip cut), for the catalog as it stands
