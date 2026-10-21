# Q89 — the loss of the fabric-primary: one playbook, the seats move at the floor, the seats go home

Ruled by Luke, 2026-10-20 (rulings INDEX, Q89). Asked from the red-team review of the Q87/Q88 landing
(lane e6s7, `target/review-q87-q88.md`, finding 2). This file is the packet as ruled and the dev's delta.

## 1. The rule

[fabric-recovery.md, "The loss of the fabric-primary while a node-admin lives"](../../architecture/lifecycles/fabric-recovery.md#the-loss-of-the-fabric-primary-while-a-node-admin-lives)
is the page. In one line: a fabric-primary unheard past the staleness floor is superseded; the lowest
ReadyForTraffic node-admin of its own mesh writes the one row at the first tick after the mark, every seat
the holder held; outside the mesh, every mesh-primary runs mesh investigation's ladder on the holder's mesh
and the backbone rule writes only at the decision step, when no admin of that mesh answered either probe;
the successor is the authority above the lost birth and the standing playbook runs down from it; once the
node at the lost fabric-primary's path.name, returned or re-created, has been Ready for one floor, the
holder hands every seat home by the committed handover of §4.4. The holder held the seats, never the
storage.

Settled with it: the floor before the hand-back is one staleness floor; no tie-break beyond the store's authority
fence; both seats go home in the one row (a fabric-primary is a mesh-primary); no new timer, the backbone's
wait is mesh investigation's ladder scaled by the floor (20/30/40/60 s by default, 2/3/4/6 s in the gate);
the self-detection of a hold is gossip.md §6's cut-off view, already in the core, gated on every authority
act by node-admin-lifecycle.md §1 rule 6.

## 2. The record

Q24 (2026-10-16) ruled this loss. Its packet defined "lost" as every node-admin of the mesh unreached and
uninspectable past the hung hold (`RDM_HUNG_HOLD_FLOORS`, ten floors), proposed only by another mesh's
primary, and the code conforms to the packet, not to the ruling's subject. Q89 is the clarification: lost
means the fabric-primary unheard past the staleness floor, by any admin's seat watch, its own mesh first.

## 3. The evidence (lane e4s8, reg32)

Both cells, `a_fabric_primary_cut_off_during_open_traffic…` and `a_hung_fabric_primary_is_marked_held_running…`,
on the resumed fabric-primary's own clock:

| cut-off cell | hung cell | span |
|---|---|---|
| 18:48:10.523 | 18:47:51.264 | `rdm.mesh.membership.update.via-cut-off` start |
| 18:48:10.537 | 18:47:51.269 | `rdm.node_admin.fabric.remove.via-tenure-ended` |
| 18:48:10.652 | 18:47:51.333 | first digest heard from mesh1.admin.2 (seq 55; the last before the hold, seq 9) |
| 18:48:10.655 | 18:47:51.344 | `via-mesh-join-commanded` mesh1.admin.2, `held=ReadyForTraffic` |
| 18:48:10.667 | 18:47:51.354 | `via-mesh-join-commanded` mesh1.rpc.1 |
| 18:48:10.775 | 18:47:51.515 | `via-cut-off` stop |

No tickle span in either window: the command came by Q87's heard-again-by-digest path, inside the
node's own cut-off, 3 ms after the first digest. The members' heartbeats were not queued through the
hold: seq 9 to 55 across an 18 s hold at 2 to 4 digests a second. The seat never moved in either cell
(the Q24 trigger).

## 4. The pages (landed text, Luke's yes 2026-10-20)

fabric-recovery.md (the opening, the new section with its timing table, the takeover bullet under
"Nothing alive"); elections §1, §4.3 (the trigger and the hand-back), §4.4 (the successor the holder names:
the node at the path.name that lost the seat, returned or re-created; the departing mesh excluded under
mesh.delete only), §5 bullets 2 and 5, §6; mesh-investigation.md (the fabric-seat mesh); node-recovery.md
step 2 (the seat watch at the mark, no half-floor wait; outside the mesh the ladder is investigation's);
gossip.md §6 (what a cut-off or healing view may not do gains the node-admin's command and seat write);
node-admin-lifecycle.md §1 rule 6; fabric-mesh-node-lifecycles.md; node-rpc-envelope.md (the takeover's
trigger line). Unchanged on purpose: mesh-delete.md (the departing mesh stays excluded there).

## 5. The delta for the dev

Six gates encode Q24's lost-whole-mesh case; each changes to the page, no new mechanism, no new env var.

1. **`seat_watch.rs::takeover_due`** (and `run`'s call to it). Trigger: the fabric-primary `PendingReconnect`
   in this admin's view, the record's birth; no `unreachable` hold, no `hung_hold_ms`. The proposer may be in
   the holder's own mesh: the standby proposes at the first tick after the mark, to itself. Outside the
   holder's mesh an admin proposes only from mesh investigation's decision step (no admin of that mesh
   answered either probe). The finding match stays for `held-running` recording and the exit-proof row; no
   finding cancels the takeover any more. The watch acts on nothing while `membership.authorizes()` is false.
2. **`fabric_handover.rs::take_over`** (and `propose_take_over`, the second caller). The successor is
   elections §4.3 with the holder excluded, own mesh first: a new `election::fabric_successor_after(nodes, inc,
   &lost)` (the lowest Ready node-admin of `lost.mesh` other than `lost`, else the lowest Ready mesh-primary
   elsewhere); `fabric_successor_excluding` stays for mesh.delete only. The row moves both seats to the
   successor when the lost birth held the mesh seat too (node-primary-update.md "Both seats on one holder"),
   never to no holder. `release_lost_admin` runs only when no admin of the lost mesh was reached and the
   provider cannot inspect the birth (the whole-mesh arm, R-FM2); otherwise no exit proof and no Deleted
   at takeover, the birth stays a member of the view for the playbook.
3. **`election.rs::named_fabric_successor`**: a same-mesh name is allowed; the condition is a ReadyForTraffic
   node-admin at that path.name, not `is_primary` (the row makes it primary). **`fabric_seat`**: unchanged
   in rule, but verify by cell that the recorded holder stays seated across the one-row commit when both
   seats move together; a holder of the fabric seat that is not its mesh's primary never arises.
4. **`accepted.rs::handback_due`** (and `reconcile.rs`, `build_drive.rs` callers): owed whenever a committed
   row moved a seat off a path.name, by takeover or by the ordinary rule on a proven loss, to the node now
   at that path.name, returned (same node.id) or re-created (new node.id), once it has been ReadyForTraffic
   for one staleness floor; drop `holder.mesh != lost.mesh` and `recreated.node_id != lost.node_id`; the
   handover row moves every seat the takeover took. The lost path.name comes from the row (`from`), not
   from `taken` alone.
5. **The view gate.** `heard_again.rs::command_mesh_join`, the seat watch's verdicts and the round's down op
   return without acting while `membership.authorizes()` is false (gossip.md §6, rule 6), with a named
   outcome (`view-cut-off`, `view-healing`). The guard "fabric-primary: no authority above it" stays for the
   seated fabric-primary; a superseded one (the record names another birth) is a member and is commanded.
6. **`investigate.rs`**: every mesh-primary runs the investigation on the fabric-primary's mesh when the
   fabric-primary is unheard (today it is the fabric-primary's probe of a peer mesh); its decision step,
   when no admin of that mesh answered, is the backbone proposal of item 1, and the fully-crashed arm's
   node.create follows the committed row with the takeover's proof as today.

Cells, RED first, each written to the page before the code moves:

- `a_fabric_primary_held_past_the_floor_is_superseded_by_its_meshs_standby_and_gets_its_seats_back`:
  mesh1.admin.1 (both seats) SIGSTOPped; within a floor and a tick mesh1.admin.2 holds both seats by one row
  and mesh1's aggregate is published again; on continue admin.1 commands nobody (its view cut off, then
  healing), re-enters through node.mesh.join (hook, cert, rafka-time); one floor after it is Ready the
  hand-back row moves both seats back; no terminate and no new birth within the hung hold. Must NOT happen:
  the held node commanding a member, two seat rows at one revision, the fabric seat on a node that is not
  its mesh's primary.
- `a_fabric_primary_lost_with_its_mesh_is_taken_over_at_the_decision_step_by_the_backbone`: the existing
  lost-mesh cell re-timed: mesh2's primary probes at two thirds and four thirds of a floor, writes at two
  floors, re-creates mesh1's admin over a new dir (`inherited = false`), hands both seats to it one floor after
  Ready.
- `a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh`, extended: the re-created
  admin at the original path.name gets both seats back one floor after Ready.
- `a_node_admin_resumed_from_a_hold_commands_nobody_until_its_view_heals`: finding 2's cell, over the
  cut-off and hung shapes; asserts no `via-mesh-join-commanded` with `held=ReadyForTraffic` and the named
  outcome instead.
- The hung cell rewritten as `a_hung_fabric_primary_is_superseded_at_the_floor_and_terminated_only_after_the_hung_hold`;
  sticky_seat's `a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat` turns around to the new
  contract; `fabric_takeover`'s four kept (the fence, one row from two noticers, the old primary refused past
  the seal, the held takeover); `seat_watch`'s and `accepted`'s unit cells follow their functions.

### 5.1 What the cells found (2026-10-21, lead), and the three changes they forced

Read from the first run of the cells above on the delta's code (lane e1s8, `target/q89/cells1.log`, five green,
five red):

- **A node's round action ran again when its role changed.** `a_fabric_primary_held_during_state_commit…`:
  mesh1.admin.2 acted as a member for round key (build, attempt 1, commit-state) under the fabric-primary,
  then, holding both seats, ran its own action for the same key again (`via-mesh-round own_action=run`
  twice for mesh1; the returned fabric-primary acted again as a member for a key it had run as the
  primary). The member path (`rdm-node` `node_self.rs` `rounds_seen`) and the primary's own action
  (`fabric_rounds.rs` `acted`) kept two records in one process. Change: one `rdm_node::rounds::ActedRecord`
  per process, read and written by both, so the node whose role changes checks in from its record
  (states.md "The round checklist": once per round key). The conformance review's item 5 (one round
  driver) subsumes it when it lands.
- **The takeover released a lost admin's name it could have inspected.** `a_fabric_primary_alone_in_its_mesh…`:
  mesh2's primary took the seat at the decision step and, mesh1 having no other node-admin, released
  mesh1.admin.1's name and re-created it over a new dir (`via-takeover-release`, `via-takeover-recreate`)
  while the birth was merely suspended on the same host. The page's step 3 releases the name only when
  nobody is able to inspect the birth. Change: `take_over` asks the provider (`seat_watch::inspect_birth`)
  before releasing; `Running`, `OsStopped` and `Exited` leave the birth a member of the view for the
  standing playbook (held-running toward its terminate; node recovery over its dir); only `Unproven`
  releases (R-FM2). The `HandoverDoor` carries the provider and nodes.storage for it.
- **Nobody commanded the lost fabric-primary back when its mesh had no other admin.** The recovery tick's
  watched set is the admin's own mesh (`watched_births`); the lost fabric-primary, no seat holder any
  more, was outside every tick. Change: the fabric-primary's tick also watches the node-admins, held off
  ready-for-traffic, of every mesh with no ReadyForTraffic node-admin
  (`recovery::admins_of_meshes_without_a_ready_admin`): it tickles them and commands them back (step 3,
  "the authority above the lost birth").
- **An admins-only UDP cut is not the loss.** `sticky_seat`'s cut-off cell cuts the two meshes' node-admins
  and leaves the rpc nodes: mesh2's primary's probe reached mesh1.admin.1 through mesh1.rpc.1
  (`admin-alive (via mesh1.rpc.1)`) and the investigation stopped, as the page says ("An answer from any of
  them ends the investigation and the seats stay in that mesh"). That cell keeps its contract in the page's
  words; the playbook's outside arm with living nodes is a new cell that cuts every node of the mesh from
  every node of the other (`a_mesh_cut_off_whole_from_the_backbone_loses_the_fabric_seat_at_the_decision_step_and_gets_it_back_after_the_heal`).
- The heard-again cell's red was Q90's (the commanded join never answered; its span has no outcome), not Q89's.
- **The view gate was observed only by a tick, so a resumed admin acted before it registered as cut off.**
  Second run, `a_fabric_primary_alone_in_its_mesh…`: mesh1.admin.1, continued after 18 s, probed mesh2 and
  commanded mesh2.admin.1 and mesh2.admin.2 back through node.mesh.join within 20 ms of its first receipt,
  the same shape as Q89's own flame (3 ms after the first digest), while its view never spanned a cut-off:
  `watch_meshes` computes "heard members and hears none" per tick, a held process ticks nothing, and by its
  first tick after the resume the book had fresh receipts. The interim it commanded restarted its channels,
  went cut off itself and was superseded in turn. Change: the book measures §6's cut-off on its own receipts
  at the moment they arrive (`DigestBook::note_heard`): a receipt that ends a silence of a staleness floor
  or more over a book that held members is the heal of a cut-off view, spanned `via-cut-off` start and stop,
  and `Membership::authorizes` is false for one floor after it. No tick, no provider, no clock beyond the
  monotonic receipt instants the book already keeps; the heartbeat-sequence measure of §5 is not needed.
- **The hand-back could not find a re-created birth.** `a_hung_fabric_primary…`: the standby held both seats
  and the hung birth was terminated and re-created at its path.name as a new node.id; `handback_due` looked
  for the lost node.id in the view (gone) and fell back to the lost mesh's primary, which was the holder
  itself. Change: the exit-proof row the projection read names the birth's path (`Records::exited_name`),
  and the hand-back is owed to the node now at that name; the whole-mesh arm, whose name was released,
  keeps the ordinary-rule fallback.
- **A superseded holder healing from a cut commanded the new holder back, and then took the seat back.**
  Third run, `cohort_election` and the whole-mesh cut: the cut-off side's admin (still the fabric-primary
  in its own stale book) tickled the members it had held unheard the moment the cut healed, commanded the
  new holder through node.mesh.join before the seat announcement reached it (the new holder restarted its
  channels, went cut off and was superseded in turn), and, a second later, having learned the record and
  still holding the new holder `PendingReconnect` from the cut, proposed and committed a counter-takeover.
  Two changes, both the page's: the fabric-primary refuses a commanded join by name (the receiver's own
  record holds the seat: "no authority above it", `serve_join_mesh`), and a standby proposes the takeover
  only once its book has held the record's holder as the holder for a staleness floor
  (`SeatBook::fabric_held_for`): a record learned a moment ago names a holder whose silence predates it.
- **The heard-again cell raced its own stop.** The released birth's first digest still said ReadyForTraffic
  (its own stale state), the helper took that for the re-entry and the cell stopped the estate 4 ms before
  the commanded join was admitted; read as Q90 at first, it was the cell's. The helper now waits for the
  mesh-primary's `via-cycle-begun` of the released birth, then its ready.
- **The re-creation of a lost mesh's admin was gated on the takeover row alone.** `crashed_peer_mesh` and the
  drive's `crash_proven` re-created the lost mesh's first node-admin on any takeover from it; a lost birth
  the view still holds as a member (held-running, os-stopped, proven exited) is the standing playbook's and
  is re-created beside itself by nothing. Change: both gates also require the lost birth gone from the view
  (its name released).

Gate: one stem at a time with `scripts/cells.sh`, then `scripts/rdm-gate-at.sh /home/admin/rdm-lanes/e1s8 fast`,
then one exported run read as a flame before any cohort, then release; landing by `rebase-gate-land.sh e1s8
e1s8-q89-fabric-primary-loss`. The export tail runs again at the SHA that lands it.
