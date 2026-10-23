# i143 conformance sweep of six pages (task #25), main at 1705ba478

Each difference: page:line, what it says, what the code does (file:line on main).

## node-runtime-inspect.md

1. :16-17, :25 (Caller), :30 (What follows): the boot of a node-admin on an inherited data dir inspects
   the old birth of that dir. Code: the inherited-root boot writes the old birth's row `Deleted` with no
   inspection (`node_rows.rs:931` `take_inherited_name`, called from the boot); landing 5 removed the
   exit-proof read there. `admin/boot.rs` still says "the provider also proves the inherited birth exited
   before `take_inherited_name` writes its row" (stale comment).
2. :32 (Spans) and the observability table: `exit_code` and `reason` on every inspection span. Fixed in
   code, `dcea3d950`: node recovery's `TerminateRuntime` read (`deployment/pipeline.rs` `inspected_at`)
   now carries both. It also carries `caller` (`terminate` or `delete`), which the page does not list.

## node-tickle.md

3. :5-7 and :26 (Ladder): the kick goes through "up to two eligible live peers of the node's mesh". Code
   (`offline.rs` `tickle`): the leads first, nodes in any mesh holding a connection to the birth (the
   by-destination index, node-investigation.md step a), then the mesh's carriers, the two together cut
   to `VIA_PEER_TICKLE_FANOUT` = 2. node-tickle.md names no leads.
4. :12-13: mesh investigation is this primitive run by the fabric-primary. Code: mesh investigation's
   probes (`investigate.rs`, `rdm.node_admin.mesh.update.via-probe`) are their own ladder (ProbeNodeState
   direct, else Ping a member and Forward through it), never `offline::tickle`.
5. :15: "the successor ladder of node.primary.update after the row". Code: node.primary.update runs no
   tickle (`fabric_handover.rs`, `member_handover.rs` call `call_and_look`, no ladder).
6. :39 and the observability table (span fields): code adds `caller` and `leads` on `via-direct` and
   `via-unreached`, `lead` on `via-peer`; the fields the page lists are all present.

## node-recovery.md

7. :65-66: a birth that comes back after it was re-created is "refused at admission (`superseded-birth`),
   reported to the fabric-primary, and its runtime terminated". Code: membership refuses it
   (`rdm.mesh.membership.reject.via-departed-birth`), nothing refuses it at admission, nothing reports or
   terminates it (cell `node_replace::a_superseded_birth_that_comes_back_is_observed`). Q128 proposal.
8. :19 (Execution) "the fabric-primary writes the Build records" and build.md §5.3 ("the fabric-primary
   writes ... every Build record"). Code: a member's recovery is an attempt of its mesh's lane, opened and
   claimed by the mesh-primary (`control_plane.rs` `open_recovery`: "the mesh's lane is written by its
   primary"; spans `via-claim-decision node=mesh2.admin.2 lane=mesh2`), its receipts written by the
   executor. Open with Q118 (landing 2b).
9. :77 `TerminateRuntime` only on `running`; an `os-stopped` old runtime is left running and returns
   (item 7). Code matches the page; Q128 arm A changes both.

## fabric-recovery.md (read at landing 5, Q122)

10. :26-28 / step 2 "broadcasts it on the backbone, the fabric `degraded` with the workflow named
    (R-DG1)". Code: the degraded publication exists only while a fabric-primary is in view and names no
    attempt (`fabric_recovery.rs` publishes nothing of its own).
11. Step 2 "Any other admin that notices tells it; it acts only when its own view agrees; repeats
    answer `AlreadyApplied`". Code: no op tells the executor; each admin decides from its own view.
12. The page names no lane for the workflow's attempt. Code: the re-create is an attempt of the target
    mesh's lane (`control_plane.rs` `open_recovery`, route `fabric-recovery`).
13. Step 3 and :164, :182 "refused at admission ... and terminated (Q110)": as item 7 (Q128).
14. Step 2 with Q111: an own-mesh admin cut off from a live fabric-primary runs the workflow and
    terminates it (backbone step 7). The pages direct it; Q127.
15. "Nothing alive": the launched admin restores itself when no admin runs the workflow
    (`fabric_recovery.rs` `runs`, 1f8c28492... bf25f1805); the page says it "is the fabric-primary" and
    does not say how its records come to name it.

## fabric-node-lifecycle-elections.md

16. :115-117 (§4.1): after the election, "the mesh-primary then runs node investigation on the unheard
    node, whatever its type" (the former mesh-primary included). node-recovery.md:50 and :72: "for an
    unheard mesh-primary, the fabric-primary opens it". The pages disagree; the code follows
    node-recovery.md since `bf25f1805` (`seat_watch.rs` plan, `control_plane.rs` `open_recovery`).
17. :149 (§4.3) "The fabric is `degraded` meanwhile": as item 10.
18. :176-178 (§5) "an old birth that comes back is refused at admission and its runtime terminated": as
    item 7 (Q128).

## mesh-investigation.md

19. Step 5 and :58-59 "A mesh-x node that comes back after it was re-created is refused at admission and
    its runtime terminated (Q110)": as item 7 (Q128). The rungs (`investigate.rs` `Rungs::at`: 2/3, 1,
    4/3 and 2 floors) and both decision arms conform.

---

# Proposed page text where the page should say what the code does (for Luke's yes)

A. node-runtime-inspect.md (item 1). Delete :16-17
   `- the boot of a node-admin on an inherited data dir, on the old birth of that dir`
   `  ([fabric recovery](fabric-recovery.md)).`
   In :25 (Caller) delete `, the booting node-admin for the old birth on its own data dir`; in :30 (What
   follows) delete `; a boot on an inherited dir proceeds as recovery`. (fabric-recovery.md "Nothing
   alive" already says every node the store names is re-created, never waited for.)
B. node-runtime-inspect.md :32 (item 2): after `verdict (... / unavailable),` add `` `caller` (`terminate` or `delete` when a delete asks), ``.
C. node-tickle.md (item 3), :26 Ladder:
   old `Direct first. On failure, the same probe carried by [Forward](...) through up to two eligible live peers of the node's mesh, both at once; one failed peer probe does not cancel the other`
   new `Direct first. On failure, the same probe carried by [Forward](...) through up to two peers at once: the leads first (nodes in any mesh holding a connection to the birth, [node investigation](node-investigation.md) step a), then eligible live peers of the node's mesh; one failed peer probe does not cancel the other`
D. node-tickle.md (items 4, 5): delete :12-13 (the mesh investigation bullet), :15 (the
   node.primary.update bullet) and :17-19 ("Mesh investigation is this primitive ... the primitive does
   not."); add to the run-by list `- [node.delete](node-delete.md)'s path fence before a create over an
   unheard birth (through node investigation).`

# Decisions open (Luke)

- Q118 (item 8, landing 2b): who writes a mesh lane's Build records.
- Q127 (item 14): an isolated own-mesh admin's fabric recovery terminates a live fabric-primary.
- Q128 (items 7, 9, 13, 18, 19): what refuses a returning birth, and whether an os-stopped runtime is
  terminated (proposal `docs/plans/proposal/i143-q128-returning-birth.md`).
- Item 16: which page owns an unheard mesh-primary's investigation (recommendation: node-recovery.md,
  the code's; elections §4.1 :115 then reads "The mesh-primary then runs node investigation on an
  unheard member, whatever its type; an unheard mesh-primary's is the fabric-primary's
  ([node recovery](lifecycles/node-recovery.md) step 2)").
- Items 10-12, 15: fabric recovery's degraded publication, the "tells it" op, the lane, the launched
  admin's self-restore: page text to match the code, or code to match the page.
