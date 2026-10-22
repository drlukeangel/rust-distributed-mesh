# Proposal: B, the seat cut-off leaves node-primary-update.md (Q109 B; Q110)

For Luke's yes on the exact text. Q109 B (Luke): "`SeatCutOff` and every cut-off check in the stores"
go; "The one rule left is RDM's own: ... a holder finishes the writes it has issued before it writes
the seat row that moves its seat." Q110: "no cut-off". The write cut-off is on node-primary-update.md
only; every other "cut-off" on the pages is gossip.md §6's cut-off view, which stays. Step 4's new text
is Luke's sentence; every other change below deletes the cut-off and keeps the rest of the line, except
the failure contract's second bullet, whose new words are the lead's and are marked.

On a yes, the code goes with it: `SeatCutOff` and its checks in the stores (`fabric_handover.rs`,
`fabric_storage.rs`, `storage.rs`, `status_storage.rs`), the record store's cut-off
(`record_store.rs` `cut_off`, `lift_cut_off`, `drop_held`, `cut_off_and_drain`), the cut-off wiring
and the held-row drop (`storage_backend.rs`, `WriteCutOff`), keeping the wait for the writes already
issued (I/O), and the cells that test the cut-off.

## The lines

1. Line 37-38 (step 2). Old: "If it names none, the step is refused by name and nothing has changed, no
   cut-off, no row ([no-successor cases](#no-successor-cases))." New: "If it names none, the step is
   refused by name and nothing has changed, no row ([no-successor cases](#no-successor-cases))."

2. Lines 42-45 (step 4). Old: "4. For the `node-admin` cohort, NA1 sets its write cut-off and finishes
   every store write already issued (R14). The mesh seat's cut-off covers the mesh's rows
   (`meshes/<mesh>`, `nodes/<mesh>/`); the fabric seat's covers `fabric/` and Build acceptance; when both
   move, one cut-off covers both. A non-admin holder has no mesh rows to finish writing (its own
   `connections/<node>/` rows are its own, R14)." New: "4. For the `node-admin` cohort, NA1 finishes the
   writes it has issued before it writes the seat row that moves its seat (Q109). A non-admin holder has
   no mesh rows to finish writing (its own `connections/<node>/` rows are its own, R14)."

3. Line 61-62 (failure contract, first bullet). Old: "refused by name, nothing changed, the Build's step
   fails by name. Nothing was cut off, so nothing is restored." New: "refused by name, nothing changed,
   the Build's step fails by name."

4. Lines 63-69 (failure contract, second bullet; the lead's words). Old: "**The seat-row write fails
   between the cut-off and the commit** (steps 4-5, `node-admin` cohort). The row is a local write, temp
   file, fsync, rename (R14's fs contract), and it answers a named error or succeeds. A named error is a
   confirmed non-commit: the step fails by name naming the error, and NA1 lifts its cut-off under the
   unchanged revision, because a primary that cannot write is a worse state than a failed step. The one
   uncertain case is NA1 dying between the cut-off and the rename; NA1 resolves it itself on restart by
   reading the row (committed: it is no longer primary; absent: it is, under the unchanged revision)."
   New: "**The seat-row write fails** (step 5, `node-admin` cohort). The row is a local write, temp file,
   fsync, rename (R14's fs contract), and it answers a named error or succeeds. A named error is a
   confirmed non-commit: the step fails by name naming the error, and NA1 is primary under the unchanged
   revision. The one uncertain case is NA1 dying before the rename; NA1 resolves it itself on restart by
   reading the row (committed: it is no longer primary; absent: it is, under the unchanged revision)."

5. Line 110 (no-successor cases). Old: "The holder stays primary, nothing is cut off, no row is written,"
   New: "The holder stays primary, no row is written,"

6. Line 142 (the fabric seat's search). Old: "The holder stays fabric-primary, nothing is cut off, no row
   is written," New: "The holder stays fabric-primary, no row is written,"

7. Line 173 (the sequence). Old: "writer                    write cut-off, issued writes finished
   (node-admin cohort); the one seat row at the next revision(s)" New: "writer                    issued
   writes finished (node-admin cohort); the one seat row at the next revision(s)"

8. Line 195 (the diagram). Old: "    Note over NA1: write cut-off, issued writes finished, the one seat
   row (commit)" New: "    Note over NA1: issued writes finished, the one seat row (commit)"
