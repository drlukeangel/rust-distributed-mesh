# R-G3 — peers resolve a seat holder's contact addresses from the digest; `NewFabricPrimary` is a reserved frame position (Luke 2026-10-14, "yes to all", arm A)

**Docs of record:** `gossip.md` §3.1 (frame catalog, reserved lifecycle frame positions); `lifecycles/node-primary-update.md`.

**Ruling (Luke 2026-10-14, on the R-G3 packet: arm A).**
- A seat update is announced by "primary updating" and "primary updated" only (node-primary-update.md). Neither carries a contact address, and nothing else announces one: every birth's `endpoint_id` and `transport_addr` ride its `Digest` and its mesh-primary's `Members` projection, and a Node RPC caller resolves a holder through the live resolver fed from the digest book (`node_rpc.rs` `feed_once`, `live.rs` `LiveNodeResolver`).
- `Frame::NewFabricPrimary { holder, endpoint_id, transport_addr, admin_api_base }` keeps its position (14) as a reserved frame position, never published, with its byte fixture, like the four R-CR4 positions. A receiver refuses it by name and applies nothing. Positions 15-21 keep their tags: nothing moves on the wire.
- The dead sender (`Membership::announce_new_fabric_primary`), the dead receiver (`note_new_fabric_primary`) and the `SeatBook` contacts side-table (`FabricContacts`, `attach_fabric_contacts`, `fabric_contacts`) go. gossip.md loses the `new-fabric-primary` catalog row, its decoded example and its "New-fabric-primary and contact addresses" section; the reserved-positions row and section name position 14.
- Flame (Jaeger 192.168.68.99:16687, lane e4s3 at a2448aac): same-mesh hand-over `0e8028bd75dde6f1fa7a9805b1fa08b7` (row written), `eaa335e5935a3af0beeb581f5f85076e` (heard on the backbone), `9171f3aca1237368a47452e4a68d98f9` (recompute); cross-mesh `01d16476139da2d8c64a88852eae5c8a`, `33cf0bfa84389031b4123798469eb8f7`, `7f2c45aaff44122ff1aa07187f107808`. No `via-new-fabric-primary` span in any of them.
- Not covered by this yes: `build.md:554` and `node-rpc-envelope.md:1419, :1423` still link the deleted anchor and name the announcement; their exact old and new text is the next ask.

# R-BC1 — 0x1C has one seat refusal, `NotClaimant` (Luke 2026-10-14, "yes to all", option 1)

**Docs of record:** `node-rpc-envelope.md` BuildClaim 0x1C.

- `BuildClaimReply::NotClaimant { mesh, fabric_primary, mesh_primary }` is the one refusal of a receiver that may not claim in the lane: it names who may, as the receiver sees it. The `RejectedNotAuthority { reason: String }` line that 8fc5be4c added to the 0x1C block without a ruling is struck; the code never had it. No wire change.

# R-BI5a — `bid_xxxx-xxxx-xxxx` is a log rendering only (Luke 2026-10-14, "i agree")

**Docs of record:** `node-identity-and-state.md` §2; `build.md` §3; `i143-prd.md` `e12.s58`.

**Ruling (Luke 2026-10-14: "bid_xxxx-xxxx-xxxx but only in logs otherwise we just use the raw-crockford as its a bigint storage value at 12"; agreed 2026-10-14 to build).**
- On the wire, in every store row and in every page example a Build id is the bare 12-character Crockford60 (R-BI5): a 60-bit integer at 12 characters (`BuildId`'s `Display` in `rafka-mesh-entity/src/ids.rs:151` stays bare).
- A log line or `build_id` span field renders it `bid_xxxx-xxxx-xxxx` (the same 12 characters in three groups of four, `BuildId::log()` -> `BuildIdLog` in `rafka-mesh-entity/src/ids.rs:193-212`) for the eye. Nothing parses that form (`BuildId::parse("bid_...")` refuses by name with `IdParseReason::WrongWidth`), and nothing stores it.
- Cell: `ids_crockford::a_dashed_bid_build_id_is_refused_by_parse_and_emitted_only_in_build_id_spans` (RED `8f0e64d0`, landed `55131ee4`, gate 46 s). Flame (Jaeger 192.168.68.99:16687): trace `a592b745aaea8819ba14a5777842e24b` (`rdm.node_admin.build.create.via-accepted` span `c696c67b348c3a5e` carries `"build_id": "bid_293h-reeg-et79"` while the Build row key `node.primary.update:293hreeget79:1:node.stop:mesh1.admin.1`, stored `BuildFact` JSON and the `0x20` `BuildReply::Started` wire carrier stay bare `293hreeget79`).

# R-RX6 — the four A' attribution items in `e12.s53` stay as landed (Luke 2026-10-14, "i agree")

**Docs of record:** `node-rpc.md` §14; `gossip.md` §3.1; `i143-prd.md` `e12.s53` (landed `f1109974`, gate 44 s).

- The four places that read the transport-authenticated sender or the seat book to attribute an incoming message or sample are not receiver authorization and stay as landed:
  1. round `heard_from` keyed on the sender (`status_rpc.rs:582`);
  2. the `ReadyForTraffic` round report recorded under `Reporter::Birth(incarnation)` or `Reporter::Endpoint(peer)` (`status_rpc.rs:671`);
  3. the probe wake `wake.addressed_by(sender, peer)` (`status_rpc.rs:1028`);
  4. rafka-time lineage: `from_mesh_primary_digest` and `from_fabric_primary_members` admit a stamp only from the digest or aggregate whose own payload birth is the seat book's holder and `ReadyForTraffic` (`rafka-mesh-transport/src/discipline.rs:42-78`). That is content against committed record, not sender attribution.

# R-SF1 — the staleness floor stays process-wide (Luke 2026-10-14, "i agree", option A)

**Docs of record:** `i143-prd.md` `e12.s82` / `e12.s83` (landed `f1109974`, gate 44 s).

- `cadence::staleness_floor()` (`rafka-mesh-entity/src/cadence.rs:56`) stays read once per process.
- The four transport cells that need another floor keep running in their own subprocess with `RDM_STALENESS_MS` set through `own_process::delegated_with_env`: `channel_stopped.rs:69`, `gossip_stats.rs:77`, `peer_location_book.rs:148` (`60000 ms`), and `heartbeat_spans.rs:63` (`2000 ms`).
- No floor parameter is added to `Membership::join`.

# e12.s55 — the PRD row is the record (Luke 2026-10-14, "i agree")

**Docs of record:** `i143-prd.md` `e12.s55` (landed `f1109974`, gate 44 s).

- Leave the `e12.s55` PRD row as the record for parked post-beta `R-HB3` (`digest_seq` as evidence on four `0x1B` replies) and file no issue.
- Replace the dead `#3085` tracking reference in the `e12.s55` header with `Tracking issue: none (the #3085 reference never resolved; parked post-beta, this row is the record)`.


# R-BI9 — build.md A7–A13 land as the build proposal writes them (Luke 2026-10-15, "yea")

**Docs of record:** `build.md` §5, §5.3, the storage list (:56, :102), :128, :336, :528, :622; proposal `build-enitty-updates-plus-otherstuff.md` Part A.

- One yes to A7 (the §5 trait names and semantics: `publish_accepted`, `open_attempt`, `current_build`, `claim_attempt`, the receipt appends; no `publish_intent`, `BuildIntentFact` or `BuildCompletionFact`), A8 (one storage list in all three places, six traits with Status, the Build journal on the shared store root per R-BI6), A9 (strike "optional"), A10 (mesh.create defined once as the mesh-absent arm of node.create), A11 (the R-FN1 name claim at `<store_dir>/fabrics/<name>` in §5.3) and A13 (names, not ordinals or slots). A6 and A14 are already on the page.
- A12 (the packages table) is deferred to the crate layout's packages-table landing after the `rdm-` rename, so the table is written once.
- One docs landing; the dev re-verifies every line against main (the proposal's numbers have drifted) and records the exact diff in the e12.s76 row.

# R-CY1 — a red is read in its own landing; the canary red names its root (Luke 2026-10-15, "yes")

**Docs of record:** `i143-prd.md` e12.s84; `lifecycles/node-primary-update.md`; `lifecycles/node-restart.md`.

- `node_lifecycle__node_restart::rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state` (canary.rs:157) went red twice in 646 gate runs, last on the docs-only tree b3022dce (R-LD1: main's red). The restarted node had won the rpc cohort's primary seat, so the Build ran `node.primary.update` under the attempt the call opened (ended `HandedOff`) and the restart under the next attempt; the cell asserts one attempt.
- It is read as its own landing: RED first, a live flame of the hand-off then the restart, then the fix naming the root. Root 1: the cell's assumption (the restart runs under `restart.attempt`) is wrong; the cell edit lands alone. Root 2: the restart reply should name the attempt that executed the restart; that is a ruling for Luke before code moves. A cell edit is never bundled into another landing as a "flake fix".

# R-FM1 — a hung runtime: hold, then the provider terminates the exact runtime; never from below the fabric-primary (Luke 2026-10-15, delegated: "you rule them, I trust your judgement"; ruled by the lead)

**Docs of record:** `lifecycles/node-recovery.md` step 3 (a new step 4 follows it); `fabric-node-lifecycle-elections.md` :155-157; `deployment-provider.md` (terminate of the exact runtime); packet `i143-failure-matrix.md` R-FM1.

**Ruling: arm B for members and for a hung mesh-primary, arm A for a hung fabric-primary.**
- Silence, a failed tickle and the observer-inferred Dead stay non-proof (R-A2, node-recovery.md:47-52); nothing moves on them. The provider's word is what changes: `Running` for the exact runtime recorded at launch opens a **hold**; `Unknown` or an unreadable runtime row (matrix row 15) opens nothing but the report.
- **Hold.** On a `NotExited { Running }` verdict the mesh-primary records a durable named fact `held-running { since, verdicts }` on the birth's status row (a store fact and a `node.get` field, not a new gossip `MemberStatus`: the gossip status stays Dead, nothing changes on the wire) and keeps running the investigation ladder every tick. An answer from the exact birth or its returning digest ends the hold and clears the fact (`reachable-silent` / `offline-returned`, no action). The hold is `RDM_HUNG_HOLD_FLOORS` staleness floors, default 10 (300 s at the default 30 s floor); it is a named row of the env table.
- **Terminate.** When the hold expires with the provider still saying `Running`, the mesh-primary submits the recovery Build for that birth whose first step is `TerminateRuntime { node_id, incarnation, handle }`: the provider's `terminate(handle, Graceful)` then `Forced` after the stop grace (the contract that `deployment/{process,container}.rs::terminate` already implements), on the exact runtime handle recorded at launch and no other. The provider's exit proof is recorded durably (`exit_proof`), then the ordinary recovery continues in the same attempt (matrix row 1: a new node.id at the same path.name; never a restart of the hung birth). A terminate that the provider refuses or cannot prove ends the attempt `Failed { step: TerminateRuntime, reason }` by name; the fact stays and a person decides.
- **A hung mesh-primary** (row 6b): the fabric-primary runs the same hold and submits the same Build (commands go down); on exit proof R-EL1 seats the successor by the ordinary rule. The seat never moves before the proof.
- **A hung fabric-primary:** no node below it commands its termination (commands go down; with the shared store's writer ladder an automatic kill from below is the split-brain path) and no Build acceptor exists. Every seat watch publishes its Concern as today, the `held-running` fact is written by the watching admins on the holder's row and surfaced by `node.get` and `fabric.get`, and a person decides. R-EL1 is untouched: the fabric seat never moves on silence.
- Cells: a SIGSTOP'd member (process provider) and a SIGSTOP'd mesh-primary recover after the hold with the terminate step's receipt and the exit proof in the trace; a SIGSTOP'd fabric-primary shows `held-running` and nothing else; `node_lifecycle__node_replace.rs:549` ("nothing happens") becomes the hold's cell. Each carries its Jaeger flame. Not a durable format move (a step name and a status-row fact are added, nothing repositioned).

# R-FM2 — a lost data dir: gone is recovered over a fresh dir; broken is held and named (Luke 2026-10-15, delegated; ruled by the lead)

**Docs of record:** `lifecycles/node-recovery.md` ("Retained", a new "Lost data dir" paragraph); `deployment-provider.md` PrepareStorage; `lifecycles/reference.md:199` (the key belongs to the data dir); packet `i143-failure-matrix.md` R-FM2.

**Ruling: arm A for a dir proven gone, arm B for a dir present but unreadable, for every kind.**
- **Gone versus broken is the error, not a guess.** The inherited dir's path absent (`NotFound` on the dir itself, `pipeline.rs:1046`) is *gone*. The dir present with `node-key` missing (`pipeline.rs:1048`), or any `EIO`, `EACCES` or other read failure, is *broken*.
- **Gone (A).** When the dir is absent on `RDM_DATA_DIR_GONE_FLOORS` consecutive ticks (default 2 staleness floors, to ride out a remount; an env-table row), the same recovery Build creates the node at the same path.name over a fresh data dir with a newly minted endpoint key (the old key is never re-minted: `reference.md:199`), `hydrate_before_ready` runs with `inherited = false` (the application resyncs; a broker from its replicas), and the exited birth's row records `data-dir-lost { path, since }` durably. The fresh dir is created beside, never over, an existing path.
- **Broken (B).** The first failure records `data-dir-unreadable { path, cause }` durably on the exited birth; the tick's due rule holds that recovery (a birth with the fact is not due) instead of resubmitting it every tick; `node.get` shows the fact. A person repairs the disk or replaces the node through an explicit Build; when the dir reads again the fact clears and the next tick's recovery proceeds as today. RDM deletes nothing on disk, ever.
- Cells: a member whose data dir is removed after exit recovers at the same name with a new key and `inherited = false` in the hydration span after two floors; a dir made unreadable holds with the fact and no resubmit span after the first; the fact clears on repair. Each carries its flame. Not a durable format move.

# Lead's sequencing notes (not rulings), 2026-10-15

- **Crate layout order (Luke: yes).** One pre-step landing does only what the name check forces before any directory move: `fence.rs` → `seat_revision.rs` (not `seat_fence.rs`: refused by the same check), `departed_status.rs` → `deleted_status.rs`, and the 32 `i143_acceptance_*.rs` files split into their crates' existing test binaries with discovery counted. Then §4 steps 4–9 as written; the 44 `__` files and the `<family>/<module>` mirror move one crate at a time after each settles (step 8); the admin.rs split stays at step 7. The pasted plan's inversion (all tests first, then the split, then extraction) is not adopted.
- **rafka-v2 pins (Luke: "rafka-v2 isn't using this yet").** The fold creates the six crates directly under their `rdm-` names from step 4; rafka-v2's eight pins (`Cargo.toml:26-33`) and the demo's are rewritten once, at step 9 or when rafka-v2 starts consuming RDM, whichever is first. No cross-repo gate before step 4.
