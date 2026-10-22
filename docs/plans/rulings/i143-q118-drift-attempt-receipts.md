## Q118: on which call does a mesh-primary's drift attempt report its receipts to the fabric-primary?

### 1. Spec
- build.md:350 (§5.1): "the fabric-primary writes every Build record (acceptance, attempts, claims, step receipts, results) through the configured Build storage; a mesh-primary runs its mesh's attempt and reports each receipt up the attempt's `build.attempt.run` stream".
- build.md §5.3: the Build journal holds "one at `(build_id, mesh, attempt)`, one claim at `(build_id, mesh, attempt)`", written by the fabric-primary.
- node-rpc-envelope.md:990 (BuildClaim 0x1C): "a mesh-primary claims its mesh's drift attempt through it ... a mesh-primary runs its drift attempt itself, reporting its receipts up the attempt's stream."
- node-rpc-envelope.md:1299: "A drift attempt of the current Build inside a mesh (a node recovery) is run by that mesh's primary, which claims it through the fabric-primary (BuildClaim) and reports its receipts up the attempt's stream".
- node-rpc-envelope.md:1317: `Step` "on the inner attempt stream, the executor reporting the step's result upstream to the fabric-primary".
- node-recovery.md:22, :74, :119: "reports its receipts up the attempt's stream".

Owner: the fabric-primary writes; the mesh-primary runs the drift attempt. Sequence: the mesh-primary claims through the fabric-primary, runs the attempt, its receipts go up the attempt's `build.attempt.run` stream, the fabric-primary writes them.

### 2. Issue
Found reading for landing 2 of the final direction (the fabric-primary writes every Build record); no runtime symptom.

### 3. Flame
No flame: nothing runs yet on this path. The gap is between the pages and the wire schema.

### 4. Observations (commit 26f3ff6b)
- [code] crates/rdm-core/src/build.rs:232: `AttemptRun { build_id, attempt, executor, context, intent, operations, hand_off_to }` carries no mesh; node-rpc-envelope.md:1422 is the same schema.
- [code] crates/rdm-core/src/build_claim.rs:38: `ClaimAttempt` is a unary request; its reply is `Won { context }` and the refusals.
- [code] crates/rdm-admin/src/build_state.rs:740: the fold numbers a drift attempt per mesh, `(build_id, mesh, attempt)`, apart from the topology attempts.
- [code] crates/rdm-admin/src/control_plane.rs:713 `open_recovery`, crates/rdm-admin/src/build_run.rs:762 `run_recovery`: today the mesh-primary opens, claims and runs its drift attempt and writes its records itself.
- [code] node-rpc-envelope.md:1412: `BuildChange::Recover { node, node_id, incarnation }` exists on build.create.
#### Inferred
- [inferred] Without the mesh, a drift attempt's number in `AttemptRun` names a topology attempt of the same Build, so the executor's `StaleClaim` check reads the wrong attempt.

### 5. Delta
Spec says a mesh-primary's drift attempt reports its receipts up that attempt's `build.attempt.run` stream (build.md:350); `AttemptRun` carries no mesh, so it cannot name a drift attempt, which the fold numbers per mesh `(build_id, mesh, attempt)`.

### 6. Arms
Every arm assumes the fabric-primary is the one writer of every Build record and calls `build.attempt.run` downward on the executor.
- A: `AttemptRun` gains `mesh: Option<String>`, appended. The mesh-primary submits `build.create { Change(Recover) }` (the fabric-primary opens the attempt at `(build_id, mesh, attempt)`), claims it through 0x1C (`ClaimAttempt` already carries `mesh`), and the fabric-primary calls `build.attempt.run` on it naming the mesh; the `Step` frames come up that stream and the fabric-primary writes each receipt. Changes wire (one appended field) and the `AttemptRun` schema line (node-rpc-envelope.md:1422).
- B: drift attempts take the Build's one attempt numbering, so `AttemptRun` names them as it stands. Changes pages (build.md §5.2, §5.3: the attempt key loses its mesh) and the fold.

**Recommendation: A.** One appended field; every other part is on the pages already (build.create `Recover`, 0x1C with its `mesh`, `build.attempt.run` and its `Step` frames), and commands keep going down.
