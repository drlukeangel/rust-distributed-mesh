# Plan to finish i141–i148

Written 2026-10-18 by the i143 lead, against RDM `origin/main` `4c5007a6` and rafka-v2 `origin/main`
`be50596c87`. Every remaining item of the eight PRDs, in the order they finish, with one owner each.
The PRDs stay the record: each item names its PRD line, and its state is written there when it moves.

## The team and the two boxes

| who | lane / box | owns |
|---|---|---|
| **Lead** | `rdm-lanes/e4s3`, RDM box | the records, rulings packets to Luke, release runs, the gate, the e10.s2 export, the e11.s12 soaks |
| **RDM dev** | `rdm-lanes/e4s2`, RDM box | authority, lifecycle, storage, Build, provider, certs rows (i145 e3/e5) |
| **Gemini** | `rdm-lanes/e7s3`, RDM box | transport, routing, gossip, data plane, connections, the cert format and accept loop (i145 e1/e2) |
| **DevX** | rafka-v2 lanes, DevX box | everything in rafka-v2 (i141, i142, i145 e4) |

**Boxes.** The RDM box runs the three RDM lanes' fast gates and every landing. Its disk is the
measured bottleneck: release run `b7548453` logged 3,790 stalled evidence writes, up to 18.2 s, and
eight of its 17 reds were lost to them. So the release runs and the soaks move to the DevX box (item
0.2), and the RDM box keeps the fast gates. rafka-v2's gate stays on the DevX box.

**Rules that hold for every item.** RED first; a red gate is read, never re-rolled; a gossip or mesh
change carries its Jaeger flame and Luke's yes; a page line changes only on Luke's yes on the exact
old and new text, and lands with its code; no new machinery without telemetry that proves the problem;
the vocabulary bans; the next free ruling number is **Q72**.

**The pin rule, reconciled.** DevX's preparation units (U-prep) run on a rolling pin to RDM main.
i141 proper (binding the adapters into running Rafka nodes, deleting Rafka's duplicates) pins once, to
the SHA e10.s2 exports (phase 5), and i142 follows it.

---

## Phase 0: now, alongside phase 1

- **0.1 Records (Lead).** One docs landing that brings the records to main:
  - i143-prd: :39 and INDEX :60 next free → Q72; :60 records `4c5007a6`, the `cohort_recovery.rs:197` red, and "no container-proof fix exists" (the s88 branch has no commits past main); :12 says the s57 restatement never happened; :15 Q56 landed `602b1863`; :40 group commit is recorded as dropped; :2684 is unchecked (rshape-lifecycle red at `b7548453`); the stale boxes :2619–2624, :2689, :2691, :2723–2747, :1833–1852, :1923/:1948, :2775/:2793 are ticked or marked moved, each with its SHA.
  - i144: the State lines :200, :51/:212, :54, :55, :253, :295/:375/:573, :429–453, :476–486, :513–516, :522–528 get their landing SHAs.
  - i146: :5, :311/:313, :439, :735, :753, :787, :790, :811, :815, :827, :861, :862, :870 get their SHAs.
  - i147: :54, :62, :106, :312, :342/:357, :395/:402, :413–421, :608 get their SHAs. :520–532 cell names follow the code. rafka-v2#3130 is closed with e1–e5's SHAs.
  - i148: :3 "not started" → e1 `f2f4bc8d`, e2 `dd60d1fb`, e3 `d67e987f`, the demo `4c324354`; :160/:175 done; :253 the demo pin `644f28a3`.
  - INDEX: Q21, Q41, Q43, Q48, Q50/Q53, Q57, Q60, Q62 (`6a3f65c6`, `702ddd9e`), Q64 and Q69 get their landed SHAs; Q58's row says landed.
  - Done when: no State line or box contradicts `origin/main`.
- **0.2 A release lane on the DevX box (Lead + DevX).** Clone RDM there and add one lane and a deploy folder, using the same `scripts/` (rdm-gate-at.sh, the consumer build, the container jobs need docker). Done when one release run there reports its jobs, so phase 1's reruns stop contending with the RDM box's fast gates.

## Phase 1: the reds of release run `b7548453` (finish-plan step 2)

Release run `b7548453` is read in i143-prd :60. Its fixes, per owner, in order:

**RDM dev (lane e4s2)**
1. **Hung mesh-primary recovery** (i143 :60, i144 :216 R21; `node_replace::a_hung_mesh_primary_held_past_the_hold…`). The fabric-primary claims it in the fabric lane (control_plane.rs:769, built uncommitted). `build_run.rs:781-784` runs a fabric-lane recovery. `build_drive.rs` `recovery_admits` admits that R-FM1 attempt's own TerminateRuntime, delete and create in the unheard mesh. Done when the cell is green with a flame of TerminateRuntime, the exit proof, then the seat moving.
2. **The cleared silence row** written outside the seat guard (`admin/boot.rs:2154-2156`; `node_restart.rs:323`). Done when :323 is green.
3. **A drive paused by a cut-off never resumes** (`owed_check_ins` state_commit; `build_drive.rs:859-864`, `boot.rs:2581-2593`). Done when the cell is green.
4. **ConnectionsDeleting declared after Stopped** (rshape-lifecycle C17; i144 :202/:526 R9; the stop workflow's two owed declarations, Q55). Also the cell's own refusal window (`rshape_burn_in.rs:4352`) and the early connections-deleting write at `pipeline.rs:1785`. That last one is evidence for Luke; the Lead brings it. Done when C17 is green with `verdict=os-stopped` within one tick and no TerminateRuntime.
5. **`cohort_recovery::recovery_admin_restores_lost_cohort…` at :197** ("members that answered are neither retired nor created again"). Red since `602b1863`; in no PRD line yet. Done when green and recorded.
6. **The two `fabric_recovery_*` cells** (i143 :1681/:1703; `cohort_recovery.rs:252`, `:385`). They still kill the fabric-primary with no hand-off, which is banned, and they run because `FP_KILL_SKIP_ARGS=""` (rdm-gate-at.sh:257). They are restated to the takeover path (`fabric_takeover`), or deleted with a `removed:` line naming `fabric_takeover` as their cell.
7. **Cell defects**: rshape-authority's per-lane attempt map (`rshape_burn_in.rs:2826`) and its hand-off order against node-primary-update.md steps 5/8 (`:2374`); `mesh_delete_no_survivor.rs:38`'s applied-only filter, if the page lets the entry pull adopt first.
8. **The container-proof cell** (`provider_state.rs:133`, 2.17 s against 2 s; i144 :272). No fix exists anywhere, so it is written: the terminate takes `wait`'s typed exit code and drops the separate inspect at `container.rs:620`. Done when green in release.

**Gemini (lane e7s3)**: Luke pastes the two prompts.
1. **rshape-routing C11** (`rshape_burn_in.rs:4557`; i143 :2688). A 30 s bound against `reconnect_backoff(4)` = 32 s + jitter, and the dial in flight when the UDP cut lifted ended "timed out". The page decides: connections.md §9, product or cell.
2. **`delivery::a_read_only_request_is_acked_at_once`** (delivery.rs:541) and **`forward::an_unreachable_target_under_a_500ms_budget…`** (forward.rs:237): both on 100 ms margins, each read from its spans.
3. **`probe_root_span.rs:154`**: the check order means a loss named whole can never pass.
4. **`app_channels::a_later_held_type_joins…`** (Q72 = b, Luke): the cell skips an event at or below sequence 1 after the rejoin and asserts event 2.
5. Reads for Luke, only if their spans confirm them: L2655's backbone feed unrepaired 3 s after an admin rejoined; L2651 (1) CarrierEdgeLost answered to a Status probe (`node.rs:461`).

**Lead**
1. **The frame queued across a suspension** (owed_check_ins open_traffic): if the dev's read of the spans confirms it, a packet to Luke. Otherwise nothing.
2. **Release reruns** (on the DevX box once 0.2 holds): after each batch of fixes, one release run at main; its reds are read and split the same way, until one SHA is green.
3. **Disk-stall reds** (the five `probe_*`, `node_restart:252`, `store_root_boot:451`, `build_op:101`): no product change. If any is red again on the DevX box, it is read where it lives.

**DevX (rafka-v2), now, on the rolling pin**
1. **The op 69 vocabulary** (in progress, laneG1): `CloseIncarnation` and the op's whole vocabulary, its spans, cells and stored entity. Page lines go to Luke.
2. **The `[now]` docs and names**: `recovery_epoch` in two cell names (`connection_reconnect.rs:1071`, `connection_rows_direct_failure.rs:90`); `hydration` → `before_ready` at PREP :63/:80 and i141 :728; `replica-routing.md` :107/:452 to one statement; the `node-lifecycle.md` stub's links fixed and the stub deleted; the stale lines of i141 (:242–253, :680–797), i142 (:571/:585, :588, :600, :641–657), PREP :25 and TBS :3–4/:228–230; the module table at `node-admin/src/storage.rs:7–20`.
3. **The connections adapter** (TBS §2.5 against `storage.rs:539–696`): §2.5 amended to what is built, or the adapter built to it.

Phase 1 is done when one release run is green on one SHA (i143 :2697, :2812's first half).

## Phase 2: the rest of the open i143–i148 rows (in parallel with phase 1 where the owner is free)

**RDM dev**, after phase 1's list:
1. **i144 e4**, three cells: `provider_state_survives_a_confirmed_seat_handover_and_its_return`, `…_a_node_admin_relaunch_under_the_recovery_identity_contract`, `the_provider_sends_no_signal_but_the_stop_ladder_and_the_terminate`, with the successor's `via-provider-inspection` traces (i144 :283–288, :523–524).
2. **i144 e6.3 handback** (:452): the fabric-primary hands the seat back to the re-born admin once it is Ready, through a Build-carried hand-off op. One release-only `fabric_takeover` cell completes :421–423.
3. **The container estate** (i143 s88): the expired-hold container cell (TerminateRuntime, the exit proof, a new birth; :2623). Then `container-proof`, 2777-container and 2784 green in one release with traces (:2629–2637).
4. **i146 e3**: the round book's resend on route return (`fabric_state.rs:15-41`, `route_lost`) onto `Budget::Owed`; a control cell shows no retry span (:878, :711). The Q39 cell: a late declaration to the old mesh-primary is refused by its store check and applied once by the new holder (:525). The Q13 cell with a suspension across the reconcile, or `owed_check_ins:72` recorded as its form (:876, :776).
5. **i146 :699–700** `sticky_seat.rs:579`'s 74 ms unproven terminate, and **i146 :804** the socket-table read: each read from its spans, fixed RED first or recorded as not a defect.
6. **`blocking_io` panics on a cancelled disk call** (i144 :491/:502, `blocking.rs:28`): a packet first (44 sites, the storage seam's signature). Done when it returns a named refusal and no teardown panic shows.
7. **The provider runbook** `docs/eng/provider-runbook.md` (i144 :461, :529): os-stopped, held-running, data-dir-lost and data-dir-unreadable, as `node.get` and Jaeger show them.

**Gemini**, after phase 1's list:
1. **i147 e3's classifier cell** `the_classifier_decides_every_category_from_the_envelope_alone` (i147 :306): written, or the row names its substitute.
2. **i147 :219–222**: a successor publisher re-entering the same operation, as a cell, or the read closed.
3. **i146 e1's span bar** (:65–71) restated from the code, or the two missing spans added; **`delivery.rs:704`** read from its spans (:793).

**Lead**
1. **The Build-journal seam** (i143 :56, Q71): `LocalBuildLog::derive_on_every_fact` stops reading the whole log. The derivation reads the Builds the new facts touch, the Builds whose overlays the book holds, and `current_build`. The book's calls are keyed and additive, so partial derivations compose. Open first: which Builds carry open operations when the book is empty at startup (whether a drift lane stays open past a newer accepted Build, R-BI7). The Lead designs it; the dev builds it. It must be in the e10.s2 SHA, because i141's `LocalBuildLog` binds to it.
2. **Q58's candidate** (i146 :862, whether a primary inspects through its provider before counting itself cut off): asked only if a run shows it.
3. **The fsync cost jump** (i146 :807/:814): read once on the DevX box, where nothing else contends.

## Phase 3: i145 certs (finish-plan step 3; starts once phase 1 is green)

Order: e1 → e2 and e3 side by side → e5; e4 is DevX's, in phase 6.
- **e1, the format into RDM** (Gemini, with the dev on `certs.rs`/`wiring.rs`), i145 :113. `rdm_core::certs` signs and verifies. `CertChoice` becomes Ca / Issuer / NoCerts. `CertSigner` and `rdm-test/src/test_certs.rs` go. Five cells.
- **e2, the accept loop admits on a valid cert** (Gemini), :131. i148 e1 built the loop, the exchange and the spans. Left:
  - a cert-verifying `Admitter`, wired;
  - the six refusal reasons by name;
  - a dialer that presents nothing within the floor recorded as refused;
  - the dialer's Failed row on refusal;
  - real `cert_expires_at`/`clock` on the span;
  - the control cell (no admission span on Node RPC or gossip);
  - a module name that doesn't collide with the server's admission module.
- **e3, the cert row and renewals** (RDM dev), :156. `node-cert/1` at JoinNode; `NodeView.cert`; `node.cert.renew`; `mesh.issuer.create`/`renew`; the TTL and renew-at policy in the entry pull; the stall on `fabric.get`. Ten cells; durable and wire format moved.
- **e5, the CA under the root** (RDM dev), :183. `fabric.ca.create`/`renew`, `fabric.root.update` through its five phases, `node.roots.update`, `RDM_ROOTS` and `RDM_CA_SECRET` at launch, `roots`/`ca` on `fabric.get`. Ten cells.
- Done when i145 :222–226 hold: e1–e3/e5 green fast then release with flames, and the control holds in every landing.

## Phase 4: chaos, second pass (finish-plan step 4)

The Lead re-pins the demo and the Admin UI to the i145 SHA in one commit, builds the consumer and runs
one release run on the DevX box. The dev and Gemini fix its reds RED first. Done when release is green
at the i145 SHA.

## Phase 5: the tail (finish-plan step 5, the Lead's)

1. **Every open e12 block landed** (i143 :2811): s57, s88's acceptance, s89.
2. **One SHA, fast and release green** (:2812, :2844), with no red left unowned.
3. **e10.s2, the export** (:2813–2826):
   - `i143-import-manifest.md` is rewritten from `5532aabd` and the `rafka-*` names to the SHA and the six `rdm-*` crates;
   - the parity ledger is re-scanned through rafka's final tip (it reviews through `77fdfd4948`; `267fadf88e` and `cc10512f15` need rows);
   - every §14 item is proven with its §15 artifact; `eligible=true`; the artifacts are posted.
4. **e11.s12, the soaks and the container proof** (:2845–2858), on the DevX box with the dev:
   - the 30-minute seeded soaks `soak_driver_reconciles_fixed_seed_thirty_minute_{process,container}_run`;
   - the real-provider container proof;
   - every invariant holds, and the receipts are posted against the exported SHA.
5. rafka-v2#3129 is closed with the SHA.

## Phase 6: rafka-v2, i141 then i142 (DevX; starts at the exported SHA)

**i141** (pinned once to the e10.s2 SHA; PREP U1 ends):
1. **e0**, four RED canaries (:680): #2871, #2872, #2873 and #2874. The s4/s5 REDs exist unlanded (`e39ada951e`, `e31e3de52b`, `53900c6bb3`).
2. **e1.s4** #2879 (:685): the transport `fabric_id` becomes EndpointId (RED at `9822e98544`).
3. **e2** (:690): `NodeConnection` onto RDM's Direct/Proxy and route values (§14).
4. **e3**:
   - s6 #2918, the DeploymentProvider binding; `plan_one` goes (:695, :759);
   - s7 #2919, /api/nodes;
   - s8 #2920, local election, shape, drift and executor go (:757–758).
5. **e4**:
   - s1 #2922, the Day-0 process cell `7a7876a3f2`;
   - s2–s5 #2928–#2931: Day-0 self-adoption, RuntimeFact, the before_ready hook (re-sync after ready-for-traffic, Q69 = b), the Pending gate;
   - `attempt_mesh_ready_for_traffic` goes (:700, :761).
6. **U3**, the bindings with their own cells:
   - `Wiring` (types, `serve_app`, `before_ready`);
   - the deployment provider;
   - the leadership view. The Lead names the RDM API to bind; no type of that name exists at the pin.
7. **U3.2's remaining rows**:
   - **The StorageBackend itself** (a missing row): `qualify`, `own_row`, and `open` → all six boundaries (`OpenConnections`, `LocalBuildLog`, `RoundActions`, `CutOffWrites`, `writes_to`). It returns status, nodes, mesh and builds, which are built.
   - **`LocalBuildLog`**, on phase 2's seam.
   - **The Build executor's continuation** across an all-admin restart, on Rafka's backend (:797).
   - **Held until a run shows two writers of one key**: e4 (writer-ingress admission, the seat writer and op 69's closure) and the fabric name and seat-move row (:792, :796). `release_name` may be split off and built, since it has no writer question.
8. **e5** elastic floors (:705), **e6** integration §11–§13/§18 (:710), **e7** certainty and pool freshness (:715), **e8** seed and restart with OTLP (:720), **e9** the load proof (:725).
9. **e11**, entity gossip through `Channel` (:728–730): kind_topics, broadcast_cells_frame, ChunkAssembler, the digest loop, the projection variants and `kind_set` go; no crate names iroh-gossip; one write→apply flame across two nodes.
10. **e12**, the data plane through `Connections` (:733–735; i148 :262): effective, select_carrier, ReachHooks, start_accept_loop, `connection_reconnect.rs` and `crates/rafka-node-transport` go; no crate names iroh; one source→carrier→destination flame. The Lead says whether the root `[patch.crates-io]` iroh entries may stay.
11. **e10**, the 12 inventory rows deleted behind their green cells (:742, :751–762; `graceful_shutdown_bound` at `rafka-node-base/src/lib.rs:12787` with them); the doc reconciliation; Rafka's import manifest.
12. **U5**: the parity ledger extended to the final rafka tip with zero MIRROR pending (the Lead writes RDM's rows).

**i142** (on i141):
1. **The opening audit** #2951 (:78): every tag 0x10–0x19 dispositioned with one owner.
2. **U1** #2952 (0x15), **U2** #2953 (0x13/0x14), **U3** #2954 (UpgradeFrameworkOrg/ProvisionFramework).
3. **U6** #2955: broker_data direct with certainty; 0x17/0x18 onto one-hop Forward; the peer_tickle audit; op 69 as a broker_data op.
4. **The entry pull** (:585), tags 16/18/19 onto a Rafka projection op through `serve_app`. :571 and :585 name it two ways; DevX settles which and fixes the other line.
5. **i145 e4** (:659; i145 :200): Rafka passes the root in once (`CertChoice::Ca`); its minting, `start_accept_loop` and the belt go; the `RAFKA_*` cert env goes unread. Done when `every_data_plane_dial_is_admitted_through_rdms_step` and `no_rafka_mesh_auth_span_remains` pass.
6. **i144 e5 R7** (i144 :353): the gateway maps a mesh outcome's code to its client status.
7. **Q63's proof** (INDEX :43): a mesh birth through rafka-v2's gate on the fork revs, with its flame. **Q65 and Q67's proofs** come with e4, if it is ever built.

i141 and i142 are done when their §23 and acceptance lists hold on rafka's gate and a live mesh, and
rafka-v2 names no iroh, iroh-gossip or iroh-base crate.

---

## What runs in parallel

| phase | Lead | RDM dev | Gemini | DevX |
|---|---|---|---|---|
| now | 0.1 records, 0.2 DevX-box lane, reruns | phase 1 list | phase 1 list (on Luke's paste) | op 69, `[now]` items |
| after phase 1 items | the Build-journal seam design | phase 2 list, the seam build | phase 2 list | connections adapter, i141 prep on the rolling pin |
| phase 3 | records, release run | i145 e3, e5 | i145 e1, e2 | idle by plan, or U3 prep |
| phase 4 | the run | its reds | its reds | — |
| phase 5 | e10.s2, e11.s12 | soaks with the Lead | — | parity notices |
| phase 6 | rows, reviews | — | — | i141, i142 |
