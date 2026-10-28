## Q105: does a backbone takeover of the fabric seat also move the lost holder's own mesh seat while that mesh is alive across a cut?

### 1. Spec
- `fabric-node-lifecycle-elections.md` §4.3 (lines 116–120): "A fabric-primary unheard past the staleness floor is superseded (Q89): the lowest Ready node-admin of its own mesh writes the seat row at the next revision, every seat the holder held in the one row, and publishes it; the seat leaves the mesh only when no node-admin of the mesh answers mesh investigation's ladder by its decision step: then the lowest Ready mesh-primary on the backbone writes it, and its mesh holds the seat".
- Same page, §1 (line 25): "every other seat moves only on yield, a committed transfer or a proven loss, or with the fabric seat, when both are held by the lost mesh's node-admin and one takeover row moves both."
- Same page, §5 (line 156): "silence … is not proof of death … a holder keeps its seat through it. The fabric-primary alone is superseded at the staleness floor by §4.3, every seat it held in one row".
- `lifecycles/fabric-recovery.md` step 2 (lines 41–45): "Outside its mesh: … No answer from any of them by the decision step is the backbone rule, and the lowest Ready mesh-primary writes the row."

Owner: the far mesh's primary, which writes the backbone row. Sequence: the ladder gets no answer, then the decision step, then the backbone row moves every seat the lost holder held, then the lost holder's lane is sealed by that move.

The cell under test still states the pre-Q89 rule (`rshape_burn_in.rs:6295–6297`): "The fabric seat stays on the near side (R-A2) … the far side investigates nothing of the near mesh".

### 2. Issue
During a cut, the near mesh's primary is alive (it is the lost fabric-primary, mesh1.admin.1). It cannot recover its killed gateway: every recovery Build is refused on a sealed lane until the cut heals.
- `i143-rshape-adv-cut-kill-process`: 0 of 1 at `ce251b3f` and at `9cb7ed88`.
- `cut-ops` and `cut-restart`: 0 of 1 at both SHAs.

### 3. Flame
CONTRADICTION: `rdm.node_admin.seat.update.via-takeover` `detail="[CommittedSeat { seat: FabricPrimary, holder: HandoverBirth { mesh: \"mesh2\", … }, revision: 2 }]" outcome="committed" t="9"`. The detail names only the fabric seat, yet the spans below show mesh1's lane at revision 1 sealed by this same seat move 9.

Source: the `9cb7ed88` export run, spans under `target/i143-rshape/i143-rshape-adv-cut-kill-process/mock_cut_kill_of_a_near_node_is_repaired_by_the_near_side_and_held_by_the_far_side/estate/…/spans/`. Traces: http://192.168.68.99:16687/trace/169e7ea81aa4c174635b7b3c3627b1b0 (the takeover) and http://192.168.68.99:16687/trace/55055ddac33cc88159613a097b985c70 (the first refusal). Whether the export run reached the .99 collector is not verified.

```
 19.740 rdm.node_admin.seat.update.via-takeover {"expected_revision": "1", "lost": "mesh1:4qrkp58ycykk:162xexwhafjr", "node": "mesh2.admin.1", "outcome": "committed", "proposer": "itself", "successor": "mesh2:1t3y4z77kd5g:qc767v4he3tt", "t": "9"}
 32.106 rdm.node_admin.build.update.via-node-recovery {"exit_proof": "none", "node": "mesh1.gateway.1", "outcome": "not-ready: NotReady { reason: \"the Build state: stale seat at meshes/mesh1/builds/rev-00000000000000000001/00000000000000000001 (revision 1): revision 1 is sealed by seat move 9 (mesh2:1t3y4z77kd5g:qc767v4he3tt); nothing is written\" }", "submitter": "mesh1.admin.1"}
131.615 rdm.node_admin.build.reject.via-stale-seat {"at": "meshes/mesh1/builds/rev-00000000000000000001/00000000000000000001", "detail": "revision 1 is sealed by seat move 9 (mesh2:1t3y4z77kd5g:qc767v4he3tt)", "revision": "1"}
```

The run holds 400 sealed refusals in all. The cell fails at `estate.rs:405`: "timed out after 120s waiting for: mesh1's mesh-primary recovers mesh1.gateway.1 under the cut".

### 4. Observations (commit `b6695529`; `fabric_handover.rs` last changed at `ccf85379`)
- [code] `fabric_handover.rs:1548–1553`: the takeover row adds `MeshPrimary { mesh: lost.mesh }` whenever the lost birth holds that mesh's record. Its holder is `same_mesh.then(me)`, so a backbone takeover moves that seat to no holder. The comment at 1545–1547 reads: "the lost mesh's next admin fills it by the ordinary rule, and this fabric-primary writes that mesh's rows until it does".
- [code] `fabric_handover.rs:1592`: `self.builds.seal_moved(&moved, me)` seals the lost authority's Build lane at the move.
- [code] `fabric_handover.rs:1599–1603`: in the same mesh the taker takes `MeshPrimary`; otherwise it calls `book.vacate(Seat::MeshPrimary, &lost.mesh, …)`.
- [code] `fabric_handover.rs:1609`: only the fabric seat is announced.
- [flame] After t=9, mesh1.admin.1, the lost holder, keeps submitting mesh1's recovery Builds at revision 1, from 32.1 s to 131.6 s.

#### Inferred
- [inferred] mesh1.admin.1 never learns of the row across the cut: only the fabric seat is announced, and the backbone is cut. So it keeps acting as mesh1's primary at revision 1. This is read from the refusals continuing to 131 s.
- [inferred] The takeover's `detail` lists only the seats it gives a holder, so the vacated mesh seat is missing from `CommittedSeat`. This is read from the code at 1551–1553 against the seal at revision 1.
- [inferred] `cut-ops` and `cut-restart` are the same takeover seen through the Build's attempt count: 3 and 4, where the cells expect 2. This was read at `ce251b3f` and not re-read at `9cb7ed88`.

### 5. Delta
No spec covers a backbone takeover whose lost holder's mesh is alive across a cut; the code moves that mesh's seat to no holder and seals its lane, so the alive mesh cannot write its own Builds until the heal.

### 6. Arms
Every arm assumes the far side still takes the fabric seat at the decision step (Q89).
- A: the backbone row moves the fabric seat alone. The lost mesh keeps its mesh seat and its lane, and that seat moves only on yield, a committed transfer or a proven loss. Changes:
  - the takeover row and its seal (`fabric_handover.rs:1551–1553`, `:1592`, `:1599–1603`);
  - the hand-back's scope;
  - four page lines: elections lines 25, 120 and 156, and fabric-recovery lines 44–45.
- B: the pages stand and the backbone row keeps moving both seats. The three cut cells are rewritten to Q89: the near side repairs only after the heal. Changes the cells only (`rshape_burn_in.rs:6295…` and the cut-ops and cut-restart contracts).
