# Proposal: build.md brought under the Build rulings, the one id type, and the a1 review

Two reviews of `/home/admin/rafka-v2/docs/architecture/build.md` (the orchestrator's adversarial read and architect a1's), read against the rulings and the RDM tree at `0d772ec3` (main at the first review; RDM main is `f5ca6ab4` since the RX removal and s60 landed, and line references such as `http.rs:208` have moved) and the s58 part 6 branch `i143-s58-part6 @ 023b96ca` (the R-BI3 code), folded into one work list for dev. Every file:line here is evidence at the sha named; dev re-pins every one at its landing sha, not only A7's. a1's second review and dev's review of this proposal are folded in; their verdicts are the last two sections. Every item names its line, what is wrong, the action and what done looks like. Architecture doc edits land only on Luke's yes to this proposal; the code items are conformance to rulings already given.

## Docs of record

- `docs/plans/i143-prd.md` §0 rules 7-9 (R-BI1, R-BI3, R-BI4): every change to the shape is its own Build; node.stop, node.start, node.restart, node recovery, authority movement and drift open the next attempt of the current Build; the current Build is read last in, first out, no Fabric pointer; an attempt lost with its executor leaves the Build in progress.
- `docs/plans/rulings/i143-rulings-2026-10-11-dev.md`: R-BI5 (a Build id is a Crockford id; one id type, never hex), R-FN1 (fabric name claim at `<store_dir>/fabrics/<name>`), R-ML2 (no mesh recovery; a dead mesh's name used again is mesh.create with a new MeshId), R14 (one store root per fabric; the Build journal stays per admin), R-EL1a (TakeSeats sent by the executor).
- `docs/architecture/node-rpc-envelope.md`: the op table (:151-153), Build 0x20 (:1088-1104), ClaimAttempt 0x1C (:252, :805), HandedOff (:1129), resubmission (:202).
- `docs/architecture/lifecycles/mesh-investigation.md` and `lifecycles/node-recovery.md` for the recovery arms.
- Luke 2026-10-12: a path is free text; a node is created as `<mesh>.<kind>.<name>`, a name, not an ordinal; `path` and `name` are two fields.

## Part A — build.md edits (Luke's yes to this proposal is the yes to each)

### A1. One Build model on the page (R-BI1, R-BI4; a1 P1-1; orchestrator finding 1)

The page carries both models. Lines 156 and 230 give every workflow, restart and recovery included, its own build_id; line 169 forbids minting a Build for restart or drift, and line 279 says drift never mints one. The code is the attempt model: a recovery opens the next attempt of the accepted Build (`rafka-node-admin-core/src/http.rs:361-396`, `accepted.rs:1113` `AttemptReason::Recovery`), keyed on the exited birth with a repeat answering AlreadyApplied (`build_state.rs:662` `recovery_of`).

- :156 — replace with: "node.create, node.delete, the replace recipe, mesh.delete and fabric.delete are each their own Build with their own id, accepted by the current fabric-primary. node.stop, node.start, node.restart, node recovery, authority movement and proven runtime drift leave the topology unchanged and open the next attempt of the current Build. One node.create is recovery, not a change to the shape: the node-admin mesh investigation creates into a live mesh that lost its node-admins runs on the same accepted Build, as its next attempt, under the mesh's own MeshId ([mesh-investigation.md](lifecycles/mesh-investigation.md) steps 5-6, :55, :173; fabric-primary.md:349)."
- :158 — strike "the recovery Build"; the recovery is "the recovery attempt (`node.delete` then `node.create` at the same path.name, keyed on the exited birth)".
- :230 — replace "Restart and node recovery are each submitted as their own Build and keep that exact topology map unchanged" with "Restart and node recovery open the next attempt of the current Build and change nothing in its topology map".
- :183-185 (§2) — the sentence "§1 decides whether an input compiles a new Build or opens an attempt of the current one" stays; it is now true.
- a1's recommendation to keep restart and recovery as their own "workflow Builds" pinned to a topology Build, and to remove the prohibition at :169, is rejected: it is the opposite of R-BI1/R-BI4.

Done when: the page names exactly the PRD's five Build-minting workflows once, the forbidden list at :169 stands, and no line gives restart or recovery a build_id.

### A2. No Fabric pointer; the current Build is read LIFO (R-BI3, R-BI4; orchestrator finding 2; a1 P1-2, P1-4)

The page is built on `Fabric.build_id` as a pointer. Under R-BI3 a Build is its own entity written once (`publish_accepted`, insert-and-fail on its id) and the current Build is the newest in acceptance order that is in progress or completed, failed skipped. The s58 part 6 branch (`i143-s58-part6 @ 023b96ca`) carries the code; this is the doc half the PRD lists at :1842-1843.

Lines that move, and what each becomes:

- :14-15, :36 — "fabric-primary + the current Build (LIFO read)"; "the current Build is the newest accepted Build that is in progress or completed".
- :116, :119-126 — the flow: apply the change once to the current Build's topology, persist B' as accepted (insert-and-fail on its id), reconcile B'. No pointer step.
- :205-230 (§4) — "Desired topology" is the current Build; strike the four pointer bullets at :223-226 in favour of: a reader reads the Builds last in, first out; an authority-capable admin can read the current Build from `builds.storage`; a leadership change does not change it; later runtime loss is reconciled against it.
- :236-244 (§4.1) — the active-Build list loses "build_id (Fabric.build_id)" and gains "the Build's acceptance order"; :249-279 read "the current Build" where they read `Fabric.build_id`.
- :283-296 (§4.2) — the JoinNode answer carries the fabric identity only (s58); the Build comes from the Build topic catch-up and FetchBuildFacts 0x1F.
- :367-391 (§5.2) — the two-phase "persist the Build, then move the pointer" crash story is deleted. Replacement: "A Build is accepted by one durable write, `publish_accepted`, insert-and-fail on its id, carrying its acceptance stamp. There is no second write to make it current. A crash before that write leaves nothing. A crash after it leaves an accepted Build on every journal that holds it: the successor reads it when its own journal holds it or the Build topic / FetchBuildFacts brought it (A6 states what a successor can and cannot promise)." `build.delete` at :369 refuses by name the current Build (the first in progress or completed in the LIFO read).
- The LIFO read, stated precisely (`build_state.rs:324-364` at `023b96ca`, `current_of`): the Builds are read newest acceptance stamp first; a Build Pending or Running is current (in progress); a Build Complete is current (completed); a Build whose state is Failed but that completed at least once (`completed_at` set: it realized its topology and a later attempt, a restart, stop or recovery, failed) is current as completed, because a realized topology is not un-realized by a later attempt's failure; a Build that failed before ever completing is skipped and never current, and only a caller's resubmit opens its next attempt (`drivable`, :367-380). The PRD rule 8 sentence "a failed Build is skipped" is sharpened to this at `i143-prd.md:68` and :1094 in the same landing.
- :397 (§5.3) — the `FabricPointer` row and its fold are deleted with the code (s58 part 6: pointer rows and writer deleted, a store holding `fabric/pointers` refused by name). `fabric.storage` holds the identity row, the R-FN1 name claim (A11) and the seat rows (`SeatRow`, `SeatTransferRow`). The `FabricShutdown` row is not listed: e12.s59 deletes it, fabric.delete replaces it.
- :462, :479 (§10, §10.1) — "the current Build (LIFO read)".
- :595 (§15 item 2) — "one current-Build read (LIFO) exists, and one drift mechanism opens attempts of it".
- :622 (§16) — "the current Build is still the current Build after the restart".
- Add R-BI4 where the attempt rules live (§4.1 after :279): "An attempt lost with its executor leaves the Build in progress; the next attempt resumes from its receipts. Only a named terminal failure makes a Build failed, and a failed Build is skipped by the LIFO read until a caller resubmits it."

a1 P1-2 (timestamp order selects the wrong Build) is defended in code. On main (`http.rs:208`) the acceptor clamps above the current Build; on the branch (`http.rs:221-224` at `023b96ca`) it clamps above the newest Build accepted, whatever its state, through `newest_accepted_at_ms`, which is the right high-water mark: with B1 at 100, B2 at 101 failed before completing and so skipped, clamping above the current B1 would let B3 take 101 and tie with B2. The page says, where it states the acceptance order (the §5.2 replacement above): "The acceptor assigns `submitted_at_ms = max(rafka_time, newest_accepted_at_ms + 1)`, the newest accepted Build of any state included, so successive acceptances are strictly increasing and never equal; acceptance order is `(submitted_at_ms, build_id)`." That strictness holds for a successor that holds the newest accepted stamp (its own journal, or the Build topic / 0x1F); A6 covers the successor that does not. a1's fenced logical revision is not adopted: it is a second ordering mechanism. a1's "checked overflow with a named refusal" on the stamp is not adopted either: a `u64` of milliseconds does not overflow in this universe, and a refusal for it is a guard with no defect under it. Cells dev adds: a failed newest Build followed by an acceptance (the stamp is above the failed one), and rafka-time behind the newest stamp (the stamp is newest plus one).

a1 P1-4 (unpointed workflow Builds, orphan prepared Builds) is moot under A1 and this item: a stop is an attempt of the current Build, and there is no prepare-then-point two-phase to leave an orphan.

**The once-completed rule is a refinement of R-BI3's text, for Luke's yes.** R-BI3 says "a failed Build is skipped"; the code's rule is "a Build that failed before ever completing is skipped; a Build that completed once stays current as completed even when a later attempt failed". Without it a failed node.stop would roll the known state back to the previous topology. Dev records it on the rulings page as an R-BI3 refinement once Luke says yes to this proposal (that yes is the ruling; nothing is written as ruled before it), and build.md and the PRD rule 8 state it in the same landing.

**The pointer lives in seven other pages; the sweep is one landing.** `Fabric.build_id` appears in fabric-primary.md (11 lines), gossip.md (11 hydration lines), fabric-node-lifecycle-elections.md (:75, :120, :152), node-identity-and-state.md:407 (the Fabric fields list), lifecycles/fabric-create.md:29, node-rpc-envelope.md:1104 (build.delete) and node-admin-lifecycle.md:13. If only build.md moves, the pages contradict each other. Dev's drafted sweep (171 changed lines over those eight files, `/tmp/claude-1000/-home-admin-rafka-v2/cdaf5d3c-9da9-474f-8c23-804393ba8652/scratchpad/bi3docs/rbi-docs.diff`) is folded into this item and lands with the s58 part 6 code as one commit; the diff is reviewed against this proposal's text before it lands, and every line keeps the same scope A6 gives the durability promise.

Done when: `grep -rn "Fabric.build_id\|FabricPointer" docs/architecture` returns only the sentences saying there is no pointer.

### A3. One Build in flight, stated (orchestrator finding 4; a1 P1-4's serialization point)

:154 and :170 refuse only "a second topology-changing request" while a Build reconciles. The code refuses every submission while the current Build is Pending or Running (`http.rs:131-139` `current_settled`), and the recovery path inherits it (`http.rs:389-396`): a proven member exit arriving mid-attempt is refused `BuildInProgress { current_build_id }` and the mesh-primary resubmits (`admin.rs:1236`).

- After :154 add: "The same refusal answers a non-topology workflow (node.stop, node.start, node.restart, a recovery) while the current Build is in progress: an attempt running, or one lost with its executor and not yet resumed (R-BI4). One Build is in flight at a time, and one attempt of it; the submitter resubmits once the attempt ends or the lost one is resumed and ends."

The wording is the ruling's ("in progress"), not the code's state names: `current_settled` refuses on Pending or Running, and a Build whose executor was lost is still in progress with no attempt running.

Done when: the page states the one-in-flight rule in R-BI4's words and names the refusal.

### A4. Departure proof versus live handoff (a1 P1-5)

:365 "claims the next attempt only after executor departure proof" is overbroad. The envelope already defines the three paths: reattachment through build.get and the attempt.run receipt replay; an attempt ending `HandedOff { to }` whose next claim picks the executor of the remaining operations (`node-rpc-envelope.md:1129`); and departure proof only for an executor lost with unfinished work (R-BI4).

- :365 — replace with: "The next attempt is claimed after the running attempt's terminal receipt (Complete, Failed or HandedOff), or, when the executor is lost with the attempt unfinished, after exact departure proof. A live run is reattached, never re-claimed: the caller resubmits the same build_id through build.create and the executor's build.attempt.run replays the held receipts ([Build carrier](node-rpc-envelope.md#build-op-0x20-rdm), :202); build.get is a read of the evidence, not a reattachment. Claims are insert-and-fail on `(build_id, attempt)`, which prevents two claims of one attempt; a call carrying a claim that is not the current folded attempt of the receiver's projection is refused `StaleClaim` naming the held and carried attempts (`build_run.rs:402` at `023b96ca`), and `adopt_claim` moves an admin's view only to a claim the fabric-primary decided Won, so a delayed dispatch or receipt of an earlier attempt never reopens a terminally closed run."

Done when: :365 cites the envelope, names the three paths, and attributes duplicate-claim prevention and stale-execution refusal to their own mechanisms.

### A5. Receipts are keyed on the exact birth (a1's closing point)

:160 says completed step receipts prevent repeating satisfied work, without saying what identifies a receipt. Two keys exist in code and the page names both:

- the RPC completion inbox of the executing admin matches a completion to its open command by `(build_id, attempt, operation, node_id, incarnation)` (`node_commands.rs:8`); it is in memory and writes nothing durable;
- the durable step receipt is `BuildStepReceipt { build_id, attempt, operation, step, outcome, output, executor }` (`build_state.rs:119-140`), and the exact target it acted on is the fenced `AttemptAction` the attempt carries (`AttemptOpened.action`, `BuildProjection::carried(attempt, action)`, `build_state.rs:90-102`, `:262-305`) and the recorded output, never the path.name alone; the recovery index answers by the exited birth (`recovery_of`, `build_state.rs:662`).

Receipt reuse is scoped by the **run**, and the page needs that word. In code `completed_in_run` (`build_state.rs:312` on RDM main) counts a Complete receipt only from attempts since the run's `AttemptOpened`. The current Build is long-lived under R-BI1, so the same step keys (`node.create:<path.name>`, `node.delete:<path.name>`, node-recovery.md:24) repeat inside it; read without the run, a second recovery at the same name would reuse the first recovery's receipt and skip its create.

- :160 — append: "A step receipt is `(build_id, attempt, operation, step)` with its output; the exact birth it acted on is the attempt's fenced action. Each workflow's `AttemptOpened` starts a run; a resume after the executor is lost (R-BI4) continues that run; a receipt is reused only within its run. A later attempt of the same run replays a held Complete receipt for the same operation and step. A receipt of an earlier run at the same path.name satisfies nothing for a later run: a second recovery at the same name opens its own run and runs its own steps."
- Cells dev adds: two successive recoveries at the same path.name, each its own run with its own receipts, the second's create not skipped; a takeover after a completed step, the successor continuing the run, replaying that step's receipt and running only the rest.

### A6. Durability of Build facts (a1 P1-3) — the one open ruling, R-BI6

:371 says a recovery-critical fact "is not committed until it is durable in storage that survives the loss of the current fabric-primary", then names the Build topic and FetchBuildFacts as how. Under R14 the Build journal is per admin, never in the shared root, and gossip carries no acknowledgement, so the sentence promises what the mechanism does not do.

**Ruled 2026-10-12, R-BI6 = x:** the journal's location is the adapter's and provider's choice; the fabric-primary writes it; RDM's file adapter keeps it on the fabric's shared store root; :371 states that and promises nothing beyond the adapter's storage. The arms below are kept as the record of what was asked; A and B are withdrawn.

Arms, as asked:

- **A (recommended):** state the truth, and its consequences. A fact is committed when durable on its author's journal. The Build topic and 0x1F spread it to every admin that is up. The guarantee is scoped to surviving journals: a successor reads the accepted Build, its claims and its receipts from its own journal or from any admin's that holds them; what no surviving journal holds is lost. Observed runtimes cannot reconstruct a lost requested topology, and a resubmit of a build_id no surviving journal holds is refused `UnknownBuild { build_id }` (`rafka-node-rpc-contract/src/build.rs:393`), never silently recompiled: the caller recovers its intent by a new build.create of the same change, which compiles against the current Build and gets a new id. A successor computes remaining work of a Build it does hold from observed state, and receipts only skip work proven satisfied (what :160 and :340 already say). The strict acceptance order of A2 holds for a successor that holds the newest accepted stamp; one that does not accepts above what it holds, which is the same loss stated once. The first sentence of :371 is replaced by that paragraph, and the §5.2 crash sentence of A2 carries the same scope. No mechanism changes. Cell dev adds: delay propagation, accept a Build, lose its sole journal holder, and assert the successor answers `UnknownBuild` to the resubmit and accepts the caller's new build.create.
- **B:** a1's adapter-level acknowledgement cut, a fact committed only once another admin holds it. A delivery mechanism beside gossip.
- **x:** something else.

A is a weaker durability guarantee than :371 promises today, and A2's crash sentence is written to it; if B or x is chosen, A2's crash sentence and this cell change with it. Dev does not touch :371 until Luke answers R-BI6; every other item in this proposal is independent of it.

Dev also chooses arm A, with one condition for A to be true: the caller's `Started { build_id }` goes out only after `publish_accepted` is durable on the author's journal, so a Started the caller holds names a Build at least one journal held (the rest of dev's sentence did not reach this page; dev completes it on the row).

### A7. The §5 trait and vocabulary match the code (orchestrator finding 3)

:310-317 shows `publish_intent(BuildIntentFact)`. `BuildIntentFact` (:323, :349) and `BuildCompletionFact` (:327) have no struct in the tree; :350 says the intent is a "blind put once" while the code is insert-and-fail on the id. The contract the page states is the one of the R-BI3 landing, `build_state.rs:735-800` at `023b96ca`, pinned here and re-pinned to the landing sha when it lands:

- `publish_accepted(BuildAccepted)`, insert-and-fail on the id, carrying the acceptance stamp;
- `open_attempt(AttemptOpened)`, insert-and-fail on `(build_id, attempt)`, carrying the fenced action;
- `current_build() -> CurrentBuild { InProgress | Completed | None }`, the LIFO read over the acceptance-order index, with the failed-but-once-completed rule of A2; `current()` is that read then a point read of the Build;
- `newest_accepted_at_ms()`, the acceptance high-water mark of A2, every state included;
- `read_build`, `list_active`, `facts`, `forget`;
- `claim_attempt`, the fabric-primary's insert-and-fail on `(build_id, attempt)` on its own log; `adopt_claim`, an admin's own record of a claim decided Won;
- `append_step_receipt`, `append_attempt_receipt`, `note_blocked` (live, never durable);
- `recovery_of(path, node_id, incarnation)`, the point read by exited birth of the workflow that deleted it;
- `publish_fabric(FabricIdentity, Option<&BuildId>)`, what the Build topic carries. The `WireFabric.build_id` field of that gossip frame (`wire.rs:686` on RDM main) is **open**: the s58 seat fills it from `current_build()` and no receiver reads it; whether the field stays or goes is a gossip frame change, so it needs Luke's yes and a flame either way (CLAUDE.md gossip rule; PRD s58 row). This proposal does not decide it.

- :308-328 — the trait as the landing has it, and the fact vocabulary as `BuildAccepted`, `AttemptOpened`, `BuildAttemptClaim`, `BuildStepReceipt`, `BuildAttemptReceipt` (`BuildFact` variants).
- :346-363 — the Rafka adapter column: `BuildAccepted` insert-and-fail on its id; `AttemptOpened` and `BuildAttemptClaim` insert-and-fail on `(build_id, attempt)`; receipts append; the current attempt is the folded winning claim; the current Build is `current_build()` over the acceptance-order index; the recovery index answers `recovery_of`.

Done when: every identifier in §5 resolves by name in `rafka-node-admin-core/src/build_state.rs` at the landing sha, and §5 states the current-read, acceptance-order and recovery-index semantics, not only the names.

### A8. Storage interfaces, one list (orchestrator finding 5)

:56 and :102 name Fabric/Mesh/Nodes/Connections/Build; :395 names fabric, mesh, nodes, connections and status with the Build journal separate. The code has six traits: `FabricStorage`, `MeshStorage`, `NodesStorage`, `ConnectionsStorage`, `StatusStorage` (`status_storage.rs:170`), `BuildStateAdapter`.

- :56, :102, :395 — "the five storage interfaces, fabric, mesh, nodes, connections and status, on the fabric's shared store root, and each admin's own Build journal (`builds.storage`)". Same wording in all three places.

### A9. The Build journal is required, not optional (orchestrator finding 6)

:338 "optional local-file journal" contradicts :371 and :395, where each admin keeps its own journal and failover depends on it. Strike "optional"; the journal is each admin's own, required.

### A10. mesh.create, defined once (orchestrator finding 7; R-ML2)

:511 says a mesh comes into being with the node.create of its first node-admin; :531 says "mesh3 is mesh.create of a new mesh". There is no mesh.create in the envelope's op table or in the workflow list. R-ML2 uses the phrase for a dead mesh's name used again.

- :511 — "mesh2 comes into being with the node.create of its first node-admin ([mesh-absent arm](lifecycles/node-create.md#mesh-absent)); that is what mesh.create names: there is no separate op."
- :531 — keep "mesh3 always has a new MeshId", and say "mesh3 is the mesh-absent arm of node.create under a new MeshId".
- :156's list is unchanged by this (mesh.create is not a Build-minting workflow of its own).

### A11. §5.3 carries R-FN1 (orchestrator finding 8)

:395-397 give `<store_dir>/<fabric_id>/…` and an identity row "written once" with no claim. R-FN1: the fabric's name is claimed insert-and-fail at `<store_dir>/fabrics/<name>` above the per-fabric roots; a deleted fabric releases it. Add that sentence at :397, and the `fabrics/<name>` entry to the layout at :395. The code is already on RDM main (`fabric_storage.rs:91`, `:110` `claim_fabric_against`, `:300-306` `claim_identity`); only the doc half is left.

### A12. The packages table is what Rafka imports (orchestrator finding 9)

:94-105 omits `rafka-node-base` (115 Cargo references in rafka-v2) and `rafka-mesh-ops` (83). Add both rows with one line each on what they hold; `rafka-node-base` is already described at :618. `rafka-mesh-telemetry` is also an RDM crate, and no Cargo file in rafka-v2 names it, so it stays out of the table, which lists what Rafka imports; the sentence after the table says so, with `rafka-chaos`, so a reader does not take the table for RDM's crate list.

### A13. Names, not ordinals; no slots (orchestrator finding 10; Luke 2026-10-12)

"slots" at :99, :129, :531 and "ordinals" at :625, and the `admin.1` / `rpc.2` examples at :133-147, :537, encode the ordinal model the PRD e12.s41 row calls a conformance defect.

- :129 — "the exact path.names that should exist, grouped by mesh".
- :531 — "reuse its name".
- :99 — "endpoint freshness".
- :625 — "assert accepted per-kind counts or settled shape rather than requiring surviving names to be contiguous" (and drop "ordinals").
- :133-147, :537 — example names as created: `mesh1.admin.alpha`, `mesh1.rpc.north` (free text, `<mesh>.<kind>.<name>`), or whatever the dev's cells already use.
- :3 — delete "this page's sections skip §7 and §8 so that §9 to §16 keep their numbers". The renumbering of §9-§16 to §7-§14 is NOT done in this landing: about 120 code comments cite build.md sections by number, and e12.s75 owns the comment-citation sweep; the renumbering and its inbound-anchor fixes (`fabric-create.md:29` links `#101-ready-root--deadlock-guard`; grep `build.md#` across docs and `build.md §` across both trees) go in that sweep. Until then the gap in the numbering stays, without the sentence explaining it.

### A14. The claim op is 0x1C (orchestrator finding 11)

:300 places the fabric-primary's claim under the Build carrier 0x20. The envelope puts the attempt claim on ClaimAttempt 0x1C (`node-rpc-envelope.md:252`, `:805`) and the dispatch on 0x20. :300 — "The fabric-primary's claim ([ClaimAttempt 0x1C](node-rpc-envelope.md#buildclaim-op-0x1c-rdm)), its dispatch of build.attempt.run …".

### A15. Control values are ids (R-BI5; orchestrator finding 12)

:198 reads: "The control values this page introduces (BuildId, DeploymentId, transition ids, provider handles and control-domain values) are opaque and are not Crockford product ids (RDM's `BuildId` in `rafka-node-admin-core`)."

Replace with: "BuildId, DeploymentId and IncarnationId are minted like every other id, bare 12-character Crockford60 (`rafka-mesh-entity/src/ids.rs`); there is one id type. Transition ids, provider handles and control-domain values are not ids: they are the provider's or the pipeline's own strings."

## Part B — code (conformance; the PRD rows carry state)

### B1. One id type, never hex (R-BI5) — PRD e12.s58 row at `i143-prd.md:1628`

- `rafka-node-admin-core/src/build.rs:26-32` `BuildId::mint` (`bld-<24 hex>`) → `mint_crockford60`, parse with `parse_crockford60`'s refusal by name.
- `rafka-node-admin-core/src/model.rs:12-18` `DeploymentId::mint` (32 hex) → the same.
- `rafka-mesh-entity/src/ids.rs:178-204` `opaque_id!` minters (`IncarnationId`, and `EndpointId` when RDM mints one) → the same; the `EndpointId` iroh issues is iroh's and untouched.
- `rafka-node-admin-client/src/lib.rs:39` `BuildId` parses the same way.
- A stored `bld-` id is refused by name: the landing commit carries `durable format moved`.
- `IncarnationId` is on the wire: it rides every gossip Digest and Members frame and the Node RPC birth fields, so its format change is a gossip and wire format change. R-BI5 is the permission; the landing still carries its Jaeger flame from a live mesh (CLAUDE.md gossip rule) as well as `durable format moved`.
- Done when: `grep -rn "hex::encode" crates/*/src` returns no id minter, every id cell asserts `id_format="crockford60"`, and the landing's row carries the flame link.

### B2. R-BI3 landing (s58 part 6) — already on `i143-s58-part6 @ 023b96ca`

Part A2 follows that landing; the doc edit and the code land together so the page never describes a pointer the code no longer has.

### B3. Nothing else in this proposal changes code

A3, A4, A5 state what the code does. A6 arm A changes no mechanism; arm B would, and is not recommended.

## Part C — R-BI7: the mesh-primary self-heals its own mesh (Luke 2026-10-12, "ruled MP self heals with caveats mentioned")

### The ruling

The mesh-primary is the reconciler of its own mesh and does not wait for the fabric-primary to heal it. The fabric-primary can be offline; the mesh-primary sits with its nodes in their network and manages its team. The caveats, as named in the exchange:

1. **Shape unchanged, no new Build.** The accepted topology stores path.names and the MeshId only (`accepted.rs:27-35`, `:96-101`), never a node id, so a recovery that puts a new birth at the same path.name changes no shape and is an attempt of the current Build (R-BI1, R-BI4). A new Build is needed only when the shape changes, and that stays the fabric-primary's.
2. **Attempts are scoped per mesh.** The claim key becomes `(build_id, mesh, attempt)`. Each mesh-primary claims drift attempts for its own mesh by insert-and-fail on its own mesh's rows, under its mesh on the fabric's shared store root, which is the writer ladder of R-OW1 and R14: no new writer, no shared key. The fabric-primary keeps what is fabric-wide: accepting shape changes, and claiming and dispatching the attempts of a topology Build's operations.
3. **One attempt in flight per mesh, not per fabric.** A topology Build's operation on mesh X and mesh X's own drift attempt cannot both be open: whichever claims first on mesh X's rows wins, the other is refused build-in-progress naming the holder. A recovery in mesh 2 is no longer refused because a node.create runs in mesh 1.
4. **Until otherwise noted** (Luke's words): the rule stands for every drift the mesh-primary proves inside its mesh; anything it cannot settle inside its mesh (a shape change, a mesh with no primary) is the fabric-primary's as before.
5. **Untouched:** election, gossip, status authorship (the mesh-primary is already the sole author), the Build topic, the accepted topology and the LIFO read. mesh-investigation.md still covers the mesh that has no primary; the fabric-primary acts there only.

It is a change to how the mesh works, so the landing carries its Jaeger flame from a live mesh: a member exits while the fabric-primary is stopped, and the trace shows the mesh-primary's proven exit, its own claim on its mesh's rows, and the recovery completing with no fabric-primary call. The same scenario on today's code shows the attempt not opened until the fabric-primary returns; both traces go on the row.

### Mesh doc updates (Luke's yes to this ruling is the yes to each; dev lands them with the code)

| page | line(s) | today | becomes |
|---|---|---|---|
| `node-rpc-envelope.md` | :801-816 (BuildClaim 0x1C) | "the fabric-primary's own claim door … insert-and-fail on its Build log"; `NotFabricPrimary` refusal | the claim door is the claimant's own: the fabric-primary for a topology Build's operations, the mesh-primary for a drift attempt inside its mesh; the key is `(build_id, mesh, attempt)` on the claimant's mesh rows; the refusal for a wrong claimant is `NotClaimant { mesh, fabric_primary, mesh_primary }`, naming who may claim on that mesh |
| `node-rpc-envelope.md` | :1297 and the Build refusals | `BuildInProgress { current_build_id }` fabric-wide | `BuildInProgress { current_build_id, mesh, held_by }`: per mesh, naming the holder (the fabric-primary's operation or the mesh-primary's attempt) |
| `node-rpc-envelope.md` | :252, :1101-1104 (Build carrier) | "fabric-primary accepts and drives every Build" | the fabric-primary accepts every Build and drives topology Builds; a mesh-primary drives drift attempts of the current Build inside its mesh |
| `lifecycles/node-recovery.md` | :23, :59-62, :78-79 | "The mesh-primary submits the recovery as one Build to the fabric-primary, which admits it and dispatches it back to the mesh-primary as executor"; `mesh-primary → fabric-primary: build.create(recover …)`; `fabric-primary → mesh-primary: build.attempt.run` | the mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's rows, then run; no call to the fabric-primary; the sequence loses the two arrows and the telemetry chain loses the fabric-primary's claim spans; `AlreadyApplied` / `BuildInProgress` (:61) answered from the mesh's own rows |
| `lifecycles/node-recovery.md` | :19 (table, Execution row) | fabric-primary admits | mesh-primary claims and executes; the fabric-primary learns it from the Build topic |
| `build.md` | §1 :154 and A3's new sentence | one Build in flight for the fabric | one attempt in flight per mesh (caveat 3); a second topology-changing request is still refused fabric-wide while a topology Build is in progress |
| `build.md` | §4.1 :268-275 (drift flow) | "reconcile B123 against observed state … append receipts" with the fabric-primary implied | "the mesh-primary of the member's mesh opens the next attempt of B123 on its mesh's rows and performs only unsatisfied work" |
| `build.md` | §4.3 :300, §5.1 :340, §5.2 :365 | "the fabric-primary's claim"; "single-writer safety comes from the one fabric-primary" | the claim is the claimant's (caveat 2); single-writer safety comes from the writer ladder: one claimant per mesh for drift, the fabric-primary for topology operations |
| `build.md` | §15 item 2, item 13 | "one drift mechanism that opens attempts of the current Build"; "no Rafka-local executor rule overrides the imported recovery authority" | unchanged words, with "driven by the mesh-primary of the drifting mesh" added to item 2 |
| `fabric-mesh-node-lifecycles.md` | :108 (0x20 row) | "fabric-primary claim-and-dispatch, recovery and reattachment" | "fabric-primary claim-and-dispatch of topology operations; mesh-primary claim-and-run of drift attempts; reattachment" |
| `fabric-primary.md` | the Build-authority lines (dev's sweep names them) | the fabric-primary as sole Build driver (R-ON8) | R-ON8 amended: sole driver of topology Builds; drift attempts are the mesh-primary's inside its mesh |
| `lifecycles/mesh-investigation.md` | none | the mesh with no primary is the fabric-primary's | unchanged; one sentence may say "a mesh with a primary heals itself (R-BI7)" |
| `docs/plans/rulings/i143-rulings-2026-10-11-dev.md` | R-ON8 entry | fabric-primary sole Build driver | cross-reference to R-BI7 appended |

Code the ruling moves (RDM main at `f5ca6ab4`; dev re-pins): `build_claim::ClaimDoor` (the claim key and who may call it), `http.rs` `current_settled` and the recovery `submit` path (`admin.rs:1236` submits to the fabric-primary today), `build_state.rs` `claim_attempt` / `open_attempt` key, `store_root.rs` (claims under `meshes/<mesh>/`), the `BuildReply` refusals, and the drift tick in `admin.rs`. PRD §0 rule 9 is reworded per mesh and a story block carries the work.

## Part D — R-BI8: where the exited node's caller-owned meta travels during recovery (awaiting Luke's yes; a1 and the orchestrator both recommend B)

### The defect

node-recovery.md runs node.delete (step 3, the row turns Deleted and keeps the record and its config), node.create (step 4, the claim replaces the Deleted row and seeds config from it), then node[2].update (step 6, copies the exited node's caller-owned meta, today `otel_debug`). Step 6's input is the predecessor record, which step 4 destroyed: the code copies from `req.from`, the record the executing admin holds in memory (`deployment/pipeline.rs:2497-2504` on RDM main), so an attempt resumed by another admin after step 4 has nothing to copy. The s57 seat's trace `905d7e06d98336303a9fcb848372b734` ("records no Deleted row") is the seat's evidence; the lab Jaeger did not answer from this box, so the code path is the orchestrator's.

Carrying the value in step 3's receipt output does not work either: a NodeDeleted step's output rides the Build topic as the typed `WireOutput::LifecycleOp` (`wire.rs:489`, `:565`), and `LifecycleOp` (`rafka-mesh-entity/src/lifecycle.rs:16`) has no caller-owned field; the decode at `wire.rs:511` is a plain `from_value`, so an extra field is dropped silently. Widening it is a Build-topic frame change.

Two further facts the packet did not carry:

- `otel_debug` has no durable home in RDM today. `NodeRow` holds `config` and `tags` (`storage.rs:270-283`) and no caller-owned meta group; `meta_of` answers `otel_debug: false` for every node (`node_update.rs:18`); step 6 logs `written = false`. The copy copies nothing. Rafka's node record has no such field either; Rafka reads `RAFKA_OTEL_DEBUG=1` from the process environment (`crates/rafka-telemetry/src/jsonl_artifact_layer.rs:1196`).
- The envelope defines the group and its semantics: "Caller-owned, copied on recover and replace: `otel_debug` and whatever else is added here; settable by put", and "`otel_debug` turns one node's OTel debug level up or down without a restart" (`node-rpc-envelope.md:278-280`). It is meta, not config, and it is a live toggle.

### Arms

- **A (the seat's):** the NodeDeleted output variant on the Build topic also carries the deleted record's caller-owned group; the decoder refuses unknown fields by name; step 6 reads the group from the replayed receipt. A Build-topic frame change: `durable format moved`, before and after flames; node-recovery.md unchanged.
- **B (recommended by a1 and the orchestrator):** the claim at step 4 preserves config and the caller-owned meta group from the Deleted row in the same committed row replacement, under the existing row writer (the mesh-primary, R-OW1, R-BI7). Recovery's step 6 disappears. No Build-topic change; no receipt carries entity state.
- **x:** something else.

### B's implementation contract (a1's, with the orchestrator's additions in the last three items)

1. The claim preserves config and the caller-owned meta group in the same committed row replacement, under the existing row writer.
2. Successor identity, runtime fields and status come from the create; its name is never copied.
3. Repeating the same successor's claim preserves its existing settings.
4. A resumed executor reads the successor row; it needs neither the predecessor nor an earlier executor's memory.
5. Recovery's step 6 disappears. Ordinary node.update is unchanged; the live toggle without a restart stays its put.
6. **The inherited level reaches the new process before FirstStart.** The row value alone establishes nothing: the create reads the seeded caller-owned group into the launch (for Rafka's roles, `RAFKA_OTEL_DEBUG=1` in the process environment; for RDM's roles the equivalent launch setting) before the FirstStart hook runs. The cell asserts `otel_debug=true` at FirstStart, not only after the recovery completes.
7. **The node row grows the caller-owned meta group.** `NodeRow` gains it beside `config` and `tags`; a row this build does not recognise is refused by name; the landing carries `durable format moved`. The same group is what `meta_of` answers and what node.update puts, so `otel_debug: false` hardcoded at `node_update.rs:18` goes.
8. **Replace inherits the same way.** The envelope says "copied on recover and replace"; node-replace's successor claim takes the group from the predecessor's row by the same mechanism, and its later copy step goes too.

### Docs B moves (Luke's yes is the yes to each)

- `lifecycles/node-recovery.md`: :24 the step list loses `node.update:<path.name>`; :25 Retained gains "caller-owned meta (seeded from the deleted row with the config)"; :66-68 step 4 says config and caller-owned meta are seeded from the deleted row and the launch carries the inherited level; :71-72 step 6 deleted; the arrows at :82 and :107 deleted.
- `lifecycles/node-replace.md`: the successor's copy step, the same way.
- `node-rpc-envelope.md:278`: the group's row reads "Caller-owned, seeded into the successor's row on recover and replace".
- `node-identity-and-state.md:157-158`: "keeping the record, its config and its caller-owned meta … seeds its config and caller-owned meta".

### Cells (dev)

- interruption immediately before the claim: the Deleted row still holds the group; the resume claims and seeds it;
- interruption immediately after the claim, resumed by another admin: the successor row holds the group, the new process starts with it;
- executor replacement mid-attempt (seat moves): same assertions;
- repeated claim of the same successor: its settings unchanged;
- each asserts the retained row value and `otel_debug=true` at FirstStart.

### Tracked separately, not part of R-BI8

- **Reader conformance:** `wire.rs:511` `typed()` decodes a step output without refusing unknown fields, so an unrecognised field is dropped instead of refused by name (the CLAUDE.md second-order rule). Its own PRD row on s57; a decoder change on the Build topic, so it carries a flame when it lands.
- The Build-topic `WireFabric.build_id` field (A7) stays open as before.

## Rejected from the a1 review, with the reason

- P1-1's recommendation (restart and recovery as their own workflow Builds pinned to a topology Build; remove the :169 prohibition): against R-BI1/R-BI4.
- P1-2's fenced logical acceptance revision: the `http.rs:208` clamp already makes acceptance order strictly increasing; a second ordering mechanism is a castle.
- P1-4's "recover all admitted active workflow Builds through `list_active`" and orphan-Build handling: no such Builds and no orphans exist under R-BI1/R-BI3.
- P1-3's acknowledgement cut (arm B of R-BI6): a delivery layer beside gossip; recommended against, Luke decides.

## Not reported as defects

- :73 "No RDM crate serves HTTP" while `rafka-node-admin-core` depends on axum and has `src/http.rs`: the page states the target and the PRD tracks the removal at :994-999 and :1696.
- All 33 links and anchors on the page resolve.

## a1's second review (of this proposal): verdicts

a1 withdrew its separate restart/recovery Builds recommendation (A1 stands under R-BI1/R-BI4) and treats authorization as granted: there is no node-level authN/authZ inside the mesh, and primary ownership, exact-birth matching and stale-claim rejection are execution-safety rules. Each remaining point, checked against `023b96ca`:

| # | finding | verdict | folded where |
|---|---|---|---|
| 1 | A6 arm A (a Build can be lost with its sole holder) contradicted A2's unconditional "a crash after acceptance leaves a Build the successor reads"; resubmitting an unknown id answers `UnknownBuild`, it does not recompile | good: both true (`build.rs:393`) | A2's §5.2 crash sentence scoped to surviving journals; A6 arm A states the loss, `UnknownBuild`, the caller's new build.create and the cell |
| 2 | A2 described the main-tree clamp (above the current Build); the branch clamps above the newest accepted Build of any state, which is the correct high-water mark (B1 100, B2 101 failed, B3 would tie at 101) | good: `http.rs:221-224` | A2's clamp sentence; two cells |
| 2b | checked overflow of the stamp with a named refusal | ugly: a `u64` of milliseconds does not overflow; a refusal for it is a guard with nothing under it | not adopted, reason in A2 |
| 3 | "failed Builds are skipped" is coarser than the code: a Build that completed once and whose later attempt failed stays current as completed (`current_of`) | good: `build_state.rs:358-364` | A2's LIFO paragraph; PRD rule 8 sharpened in the same landing |
| 4 | A5 cited the in-memory RPC completion key as the durable receipt identity | good: `node_commands.rs:8` is the inbox; `BuildStepReceipt` is `(build_id, attempt, operation, step)` with the target in the fenced action | A5 rewritten with both keys, reuse and exclusion rules, two cells |
| 5 | A4 credited insert-and-fail on the claim key with stale-message safety; that only prevents duplicate claims, `StaleClaim` and `adopt_claim` refuse stale execution; "reattachment through build.get" was wrong, build.get reads and build.create resubmission reattaches | good: `build_run.rs:402`, envelope :202 | A4's replacement text |
| 6 | A7 listed the main-tree trait, not the landing's (`current_build`, `newest_accepted_at_ms`, `current`, the new `publish_fabric`) | good | A7 pinned to `023b96ca` with the semantics, re-pinned at landing |

Nothing in the second review was bad (wrong). a1's closing recommendation stands: findings 1-3 are resolved in the text above before the page's crash-safety contract is published, and A6 arm A is described with its consequences wherever the page makes a durability promise.

## Dev's review (of this proposal): verdicts

Twelve points, each checked against RDM `origin/main` at `f5ca6ab4`, the branch at `023b96ca`, and the docs:

| # | finding | verdict | folded where |
|---|---|---|---|
| F1 | receipt reuse is scoped by run (`completed_in_run`, `build_state.rs:312`); A5 left the run out, and node-recovery.md's path.name step keys repeat inside the long-lived current Build | good | A5: run vocabulary, the three rules, cells |
| F2 | A1 lists node.create as its own Build; the mesh-investigation create runs on the same Build as its next attempt (mesh-investigation.md :55, :173; fabric-primary.md:349) | good | A1's :156 text, one clause |
| F3 | "one in flight" worded on the code's states; under R-BI4 a Build lost with its executor is in progress with nothing running | good | A3 reworded in the ruling's words |
| F4 | the pointer is in seven other pages; dev's 171-line sweep over eight files | good | A2: one landing with the s58 part 6 code, diff reviewed against this text |
| F5 | the once-completed rule refines R-BI3's text and belongs on the rulings page | good, with one correction: it is written as ruled only after Luke's yes to this proposal | A2 |
| F6 | A7 decided the `WireFabric.build_id` gossip field silently; it is open and needs Luke's yes plus a flame | good | A7 names it open |
| F7 | §5.3 must not list the shutdown rows; e12.s59 deletes `FabricShutdown` | good; the seat rows are `SeatRow` / `SeatTransferRow` on main, not `put_seat` / `seats` | A2's :397 line |
| F8 | A11's "code is #3089 part 2" is out of date; the R-FN1 claim is on RDM main | good (`fabric_storage.rs:91`, `:110`, `:300-306`) | A11 |
| F9 | `IncarnationId` rides gossip and wire frames; the R-BI5 landing is a gossip format change and carries a flame | good | B1 |
| F10 | renumbering §9-§16 breaks ~120 code-comment citations; do it in e12.s75's sweep or skip | good | A13: deferred to s75's sweep |
| F11 | `rafka-mesh-telemetry` missing from the packages table | half: it is an RDM crate, but no rafka-v2 Cargo file imports it, so it stays out of an imports table and is named below it | A12 |
| F12 | pins are out of date (RDM main is `f5ca6ab4`); re-pin every reference at landing | good | the header |

Dev's choice on R-BI6 is arm A, with the Started-after-durable condition (A6). Nothing in dev's review was bad or ugly.

## Order of work for dev

1. B1 (ids) in RDM, its own landing, `durable format moved`, with its flame.
2. A1, A3, A4, A5, A7 (the trait; the gossip field stays open), A8-A12, A13 without the renumbering, A14, A15 as one build.md commit on Luke's yes to this proposal.
3. A2 with the s58 part 6 landing, as one commit with dev's eight-file pointer sweep and the PRD rule 8 sharpening; the R-BI3 refinement goes on the rulings page in that commit.
4. A6 when R-BI6 is answered.
5. A13's renumbering in e12.s75's citation sweep.
