## Q113: who writes a Build lane and a mesh's in-flight row?

### 1. Spec
- Q109 (rulings INDEX, Luke 2026-10-21): "Every row has one writer: the fabric-primary for the fabric's rows and the Build state, each mesh-primary for its mesh's rows and its mesh's Build lane (R-OW1). One writer cannot collide with itself, so the storage settles nothing: the in-flight claim (`build_state.rs` `take_in_flight`) and the seat-move row are plain writes."
- Items 12-13, approved page text not yet applied: build.md lines 360, 363, 366 become "one row at build_id", "one row at (build_id, mesh, attempt)", "one claim row at (build_id, mesh, attempt)"; node-rpc-envelope.md BuildClaim becomes "The claimant writes the mesh's in-flight row (`meshes/<mesh>/claims/in-flight` on the shared store root), which names the lane key its claim takes, and writes its claim at that key".
- node-rpc-envelope.md:990, BuildClaim Direction: "the claimant's own claim door: the fabric-primary for a topology Build's attempts, the mesh's mesh-primary for a drift attempt inside its mesh."

Owner: the lane's seat holder (fabric-primary for `fabric/builds/`, each mesh-primary for `meshes/<mesh>/builds/`). Sequence: the writer reads the lane, then writes the next key; the claimant writes the in-flight row, then its claim.

### 2. Issue
Items 12-13 cannot land as plain writes without losing a fact where two writers meet at one key; no run has recorded such a meeting (0 in the 65-job export at `09bebf1d`), and no span would record one.

### 3. Flame
No flame. A lane key taken by another writer answers `LaneTaken` (`build_state.rs:2676`) and is retried with no span; `rdm.node_admin.build.update.via-in-flight-void` (`build_state.rs:2723`) appears in none of the export run's evidence at `09bebf1d`. The gap is between the ruling and the code, not a red.

### 4. Observations (commit `2dab632b`)
- [code] `build_state.rs:2556-2563`: `FileJournal::lane_of` routes a step or attempt receipt that names no mesh to the writer's own mesh's lane when it does not hold the fabric seat, so an executor admin appends receipts to its mesh's lane.
- [code] `build_state.rs:2569`, `seat_watch.rs:91`: every birth writes a lane under that lane's seat record's revision (`lane_seat(record)`), the holder's revision, whether or not it holds the seat.
- [code] `build_lanes.rs:192`: an append is `insert_claimed` at the revision's next sequence: insert-and-fail; a lost race answers `Taken` and the writer reads and retries.
- [code] `storage.rs:300-302`: `take_in_flight` inserts `meshes/<mesh>/claims/in-flight`, insert-and-fail.
- [code] `build_claim.rs:135`: both claim doors (the fabric-primary's for a topology attempt, a mesh-primary's for its drift attempt) take the mesh's in-flight row through `claim_in_flight`.
#### Inferred
- [inferred] An executor of a fabric-lane attempt that is not its mesh's primary and that mesh's primary append to the same `meshes/<mesh>/builds/rev-<R>/` at once can pick the same next key; a plain write there replaces one fact with the other. From the four code lines above.
- [inferred] The fabric-primary's topology claim and the mesh-primary's drift claim on one mesh, from two processes, can both read the in-flight row free and both write it; a plain write lets both win, against R-BI7 caveat 3 ("whichever claims first on mesh X's rows wins, the other is refused build-in-progress"). From `storage.rs:300` and `build_claim.rs:135`.

### 5. Delta
Spec says each lane and each mesh's in-flight row has one writer; code has an executor that holds no seat append to its mesh's lane, and two claim doors write one mesh's in-flight row.

### 6. Arms
Every arm assumes the storage decides nothing (Q109) and no lane fact or claim is lost.
- A: one writer each: an executor's receipts go back to the lane's writer on the attempt's own `build.attempt.run` stream, which appends them; every mesh's in-flight row is written by the fabric-primary alone, a drift claim through its claim door; items 12-13 land as approved — changes publisher (receipts), tier (in-flight row writer), node-rpc-envelope.md BuildClaim Direction.
- B: insert-and-fail stays at exactly the lane key and the in-flight row, the two places two writers meet; items 12-13 are not applied — changes nothing in code; items 12-13 withdrawn.
- C: each lane fact and each in-flight row is keyed by its writer's birth, as the seat moves are (Q109), and every reader folds them in key order — changes the durable format of the lanes and the in-flight row, and the readers.
- x: the delta itself is the fix: executors stop writing lanes — changes tier.
