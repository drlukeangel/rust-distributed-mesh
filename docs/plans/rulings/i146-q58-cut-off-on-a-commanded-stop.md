**Q58: A — Luke, 2026-10-18, delegated; ruled by the lead**, worded in the pages' terms (the packet's arm A uses a banned word for a deleted birth, and "expects to hear", which no page says). A node's view is cut off while it hears no other member of its mesh and holds a member whose silence no lifecycle gossip event it applied explains. Expected silence never cuts a view off: a member whose node.stop this view admitted or applied (stopping; stopped, `NodeStopped` until node.joined), whose node.restart is open (`NodeRestarting`: gossip.md, "no Gone from expected silence") or whose deletion it applied (`NodeDeleted`, for the deleted retention) left the view's current members through an event the view applied, and is neither stale nor counted. A view that holds no other member, or whose other members all left it that way, is not cut off: a mesh-primary whose mesh has no other live member keeps its seat, publishes as its primary and serves its mesh's topology and rafka-time, and a stopped member's start reads them from it (node-start.md step 4). One rule governs the stale observation (`via-member-stale`) and the cut-off count; the `Leaving` skip in `watch_meshes` is the stopping half of it and is subsumed. Because no cut-off starts, no healing window opens (`CutOff::authorizes`: one silence window after a heal), so the start's ReadyForTraffic lands on a primary. B refused: a coping layer over a wrong fact (the mesh-primary flag, the mesh rows writer, the Members projection and Build dispatch all follow the dropped flag, and would still), and in a real cut-off a stale view served as complete topology, which fabric-authority.md §2.3.1 forbids. No wire, frame, timer or gossip change: rdm-transport's `watch_meshes`, the book and their cells. The gossip.md §6 paragraph below lands with the code. Dev, lane e4s8.

---

# Q58 — a mesh-primary whose only other member it stopped by command counts its view cut off

Asked 2026-10-18 by the dev ([packet](../proposal/i146-q58-cut-off-on-a-commanded-stop.md), lane e4s8
`b3891e0b`). Ruled the same day under the standing delegation (Luke, 2026-10-15: "you rule them, I trust
your judgement", with the robustness bias he gave it), inside his rules: no gossip or wire change, nothing
unproven destroyed, no change to who commands.

## 1. The reading

Kept span file of gate `7f9b1f1d`
(`/home/admin/rust-distributed-mesh-deploy/kept/gate-gates-e4s8-7f9b1f1d/testkit/a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again/testkit.2531028-4026531836.spans.jsonl`),
seconds from the cell's first span. The root is mesh1.admin.1, mesh1's mesh-primary; the member is
mesh1.admin.2.

```text
0.704  via-stop-node                                   the stop is commanded
0.705  via-left ×2                                     the member leaves its mesh and the backbone (node.stop step 3)
0.708  from-command-admitted status=Leaving            the root holds the member stopping
0.743  remove.via-node-stopped was_held=true held_digest_seq=2
                                                       the root's book drops the stopped birth; NodeStopped is its overlay
0.789  via-start-node                                  the start is commanded
0.796  reject.via-stopped-birth door=join digest_seq=0 the start's join digest is refused: the stop stands until node.joined
0.820  rafka-time adopted via=join                     the join itself is answered
0.870  topology.serve outcome=served                   the start's first topology read is served
0.878  reject.via-stopped-birth door=heartbeat digest_seq=3
0.882  via-member-stale silent_ms=0                    the 250 ms watch tick: the member left `heard`, with no silence to report
0.882  via-cut-off role=start                          the root has heard another member and hears none
0.894  via-node-joined stops_cleared=node.stop:mesh1.admin.2 held_again=true
                                                       the stop ends, 12 ms too late
0.905  topology.serve outcome=unknown-mesh             the projection round read authorizes() == false and dropped the
                                                       mesh-primary flag (boot.rs:2268-2269); held_topology adds the own
                                                       mesh only under that flag (membership.rs:3147)
0.921  command-frame frame=failed                      StartFailed { step: "node.rafka-time" }
```

The contradiction the packet flags, `via-member-stale silent_ms=0`, is the root. `remove.via-node-stopped`
took the birth out of the book (`membership.rs:4783`), so `book.get` answers nothing and the loop reports
0 ms, while the previous tick's `heard` still listed the member. The member was not unheard: it was stopped,
by this view's own command, and the view held that. The loop already skips a member held `Leaving` and a
deleted one (`membership.rs:2870`); the stopped birth, the one case that removes the digest, is the one it
does not skip, and `others == 0` then counts the view cut off (`:2879-2880`).

The window is not the defect. A stopped node may stay stopped for an hour (states.md: a live, stopped
process; node-stop.md step 6). Its mesh-primary would count itself cut off for that hour: publish as no
primary, dispatch nothing (`build_drive.rs:859`), reconcile nothing (`boot.rs:2502`), and on the start heal
into one more silence window of nothing. The gate's concurrency only made the 150 ms version of it visible.

## 2. The ruling

A, in the pages' words.

1. A node's view is cut off while it hears no other member of its mesh and holds a member whose silence no
   lifecycle gossip event it applied explains. It ends when the view hears a member again, or when the last
   such silence is explained by an event the view applies; either way one silence window of healing follows,
   as today (`CutOff::authorizes`).
2. Expected silence never cuts a view off and is never reported stale: a member whose node.stop this view
   admitted or applied (stopping; stopped, `NodeStopped` until node.joined), whose node.restart is open
   (`NodeRestarting`), or whose deletion it applied (`NodeDeleted`, for the deleted retention). Today's
   spellings in the book: `MemberStatus::Leaving`, the `StopMark`, the open restart overlay, the deleted set held for the retention.
3. A view that holds no other member, or whose other members all left it through such events, is not cut
   off. A mesh-primary whose mesh has no other live member keeps its seat, publishes as its primary and
   serves its mesh's topology and rafka-time; a stopped member's start reads them from it (node-start.md
   step 4, node-topology-get.md).
4. One rule, one scope: the stale observation (`via-member-stale`) and the cut-off count read the same set.
   `heard_others` and the `Leaving` skip are parts of it and go.
5. Unchanged: every cut-off produced by silence with no event (a member suspended, a transport cut, a crash) is
   still a cut-off, with today's consequences; the pages' statements about what a cut-off or healing view
   may not do (fabric-authority.md §2.3.1, build.md §4, gossip.md §12) are neither restated nor widened. No
   wire, frame, timer or gossip change: rdm-transport's `watch_meshes` and the book.

B refused. It leaves the wrong fact standing: the view is not cut off, and everything that reads the flag
(the mesh rows writer `own_seat_tx`, `backbone.set_mesh_primary`, the Members projection through
`held_sources`, Build dispatch, the stopped-node sweep) would still act on it; the start's ReadyForTraffic
would land on an admin publishing as no primary, and the heal would cost one more silence window. And it is
wrong on its own terms: a mesh-primary that *is* cut off serving its own mesh's topology from its seat record
is the stale view fabric-authority.md §2.3.1 says refuses, handed to a starting node as complete.

Not asked, not ruled: whether a genuinely cut-off mesh-primary may serve its own mesh's topology (B's
premise); and whether a mesh-primary whose every other member falls silent with no event should inspect the
exact runtimes through the provider (R-FM1, node-runtime-inspect.md) before counting itself cut off, which
would let a mesh of one admin recover a crashed member without a person (today `boot.rs:2502` gates the
reconciliation pass on `authorizes()`). The second is a candidate question; it takes the next free number if
Luke wants it asked.

## 3. Page text (lands with the code; the exact diff for Luke's glance)

[gossip.md §6](../../architecture/gossip.md#6-repair-held-member-stale-coverage-not-zero-neighbours), after
"The exact span name follows repository observability canon." and before "### 6.1", one new paragraph:

> **A cut-off view.** A node's view is cut off while it hears no other member of its mesh and holds a member
> whose silence no lifecycle gossip event it applied explains; it ends when the view hears a member again or
> when that silence is explained, and one silence window of healing follows. What a cut-off or healing view
> may not do is [fabric-authority.md §2.3.1](fabric-authority.md#231-topology-authority),
> [build.md §4](build.md#4-the-accepted-topology-is-the-fabric-primarys-current-build) and §12. Expected
> silence never cuts a view off and is never stale: a member whose node.stop this view admitted or applied
> (stopping; stopped, `NodeStopped` until node.joined), whose node.restart is open (`NodeRestarting`) or whose
> deletion it applied (`NodeDeleted`, for the deleted retention) left the view's current members through an
> event the view applied, and is not counted. A view that holds no other member, or whose other members all
> left it that way, is not cut off: a mesh-primary whose mesh has no other live member keeps its seat,
> publishes as its primary and serves its mesh's topology and rafka-time, and a stopped member's start reads
> them from it ([node.start](lifecycles/node-start.md) step 4).

One flag, not a question: `cut-off` already names the seat move's write cut-off
([node-primary-update.md](../../architecture/lifecycles/node-primary-update.md) step 4). The view's cut-off
and the writer's cut-off are two things under one word (R-ST1). No rename is proposed here.

## 4. Cells

- `admin_start_declares::a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again`
  green under the gate's concurrency (the red needed the 250 ms watch tick inside the 150 ms between
  `remove.via-node-stopped` and `via-node-joined`).
- rdm-transport, the book: a view holding one other member whose NodeStopped it applies emits no
  `via-cut-off` and no `via-member-stale`, `authorizes()` stays true, and node.joined holds the birth again
  with no healing window; the same view's one member going silent with no event is a cut-off.
- Unchanged and still green: `a_healed_view_authorizes_nothing_until_every_member_could_be_heard_again`
  (membership.rs), `build_drive::a_cut_off_view_dispatches_nothing_and_the_drive_goes_on_once_it_authorizes`,
  Q45's `owed_check_ins::a_fabric_primary_cut_off_*` (SIGSTOP),
  `sticky_seat::a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat` (a UDP cut). Each
  produces silence, not an event.

## 5. Record

INDEX row Q58; the i146 PRD's `admin_start_declares` bullet; this file. The lane's `b3891e0b` (the packet,
the INDEX row, the i146 bullet) is reproduced on main with this ruling; its one i143-prd line sits in the
lane's s89 block and stays there. At the lane's rebase the INDEX row and the i146 bullet conflict on Q58:
take main's.
