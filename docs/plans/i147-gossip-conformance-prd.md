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
| R1 | the frame-contract table: every frame kind in one of the four categories with its exact stale and duplicate rule | BUILD | receive_order.rs keys 9 of 22; gossip.md §3.1 states no receive rule | e1 |
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

- **Goal → gap.** The frame-contract table on gossip.md; one unkeyed frame applied out of order, with
  its flame (the dev's offer, 2026-10-16).
- **Deliverables.** The red cell of the regression (the table is on the page); its fix in the receive
  path by the frame's own identity; the flame before and after; the gossip.md section as exact old →
  new to Luke; a gossip change, so Luke's permission is Q17 and the flame is required.
- **Acceptance cells.** `an_unkeyed_<frame>_delivered_after_a_newer_frame_about_its_subject_is_refused_stale`
  (the regression); `every_frame_kind_has_a_row_in_the_receive_rules` (a unit cell over the catalog against the page's table).
  RED first.
- **State.** in progress in lane `e7s3`, branch `codex/i147-e1-gossip-conformance`: catalog cell added; investigating equal-revision MeshStatus content disagreement with a live receive-path cell before selecting the e1 regression. No receive-path fix or landing yet; RED/GREEN trace ids pending.

#### e2 — every row, one defect at a time

- **Goal → gap.** Each remaining row of the table whose rule the code does not meet is a defect with
  its own red → green cell; rows the code already meets get their cell and no change.
- **Acceptance cells (the a1 set, each against its defect).** newer state then older: no regression;
  a terminal fact after an unrelated newer frame: applied; a fact repeated: no duplicate effect;
  snapshot chunks reordered: complete and atomic; publishers change: the predecessor's delayed frame
  does not overwrite; recovery through a topology read, then stale gossip: no rollback; each reserved
  frame: named refusal, no mutation.
- **Order.** Operation facts first (the thirteen unkeyed kinds), then replacement state, then
  snapshot or delta, then the reserved sweep. Each landing: its cell, its receive-path change, its
  doc line, and a reseal only if a field was needed.
- **State.** not started.

## Verification

- [ ] e1 green in the RDM gate with its flame cited; the gossip.md section landed with it.
- [ ] Every row of the table has a green cell; no frame kind is "taken as it arrives" without a stated rule.
- [ ] receive_order.rs has one shape per category and no contradictory check.

## Cross-references

- `docs/plans/i146-owed-messages-prd.md` (the directed-message unit this is separate from)
- `docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md` R-RX1 (data validity stays: version ordering of the same source, duplicate receipt keys, backward transitions)
- `docs/architecture/gossip.md` §3.1, §3.3, §7; `crates/rdm-transport/src/receive_order.rs`
- `docs/plans/i143-prd.md` e12.s72 (the gossip cut), for the catalog as it stands
