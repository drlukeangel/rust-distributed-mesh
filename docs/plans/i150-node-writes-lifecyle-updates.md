# i150 — nodes persist their properties; lifecycle reads follow the current primary

**Initiative / Epic / Story ID:** `i150`, epics e0–e7
**Sprint:** initiative delivery increments 0–7; dates assigned at dispatch
**Status:** opened 2026-10-31; implementation started in lane 1 on `codex/i150-node-records`, rdm#245; runtime acceptance remains BUILD
**Architecture doc:** [Node identity and state](../architecture/node-identity-and-state.md) §2.5, [node config update](../architecture/lifecycles/node-config-update.md), and [storage](../architecture/storage.md) §2. These current contracts need coordinated amendments; the target design is [node-record.md](proposal/node-record.md), R-NR1–R-NR8.
**RED/GREEN evidence:** pending; canary first, then seed, then implementation
**Approved:** Luke requested this PRD and approved the serialized-write regain correction in this chat on 2026-10-31, committed as `4dbf01ebaf99ca5e17d4e2ba9cfb78ebf8fe2a65`. No UTC approval time was supplied. That approval does not constitute approval of the unresolved forks or all architecture deltas below.
**Owner:** Codex implements the initial storage/canary increment in lane 1; subsequent story owners assigned at dispatch; Rafka integration coordinated with the i141 lead
**Feature:** `node-records`
**Baseline:** `4dbf01ebaf99ca5e17d4e2ba9cfb78ebf8fe2a65`, verified with `git cat-file -e`; no accompanying uncommitted contract edits are incorporated
**Record:** this file owns i150 gaps, story status and evidence. e0 updates the dependency in [i143-prd.md](i143-prd.md); e6 coordinates the i141 consumer record and [i149](i149-topology-storage-provider.md).
**Source template:** [Seed-first PRD](../eng/templates/prd-seed-template.md). The requested filename `i150-node-writes-lifecyle-updates.md` is intentional.

## Context

An embedding application's node currently sends its own property update through its mesh-primary and fabric-primary, while readers maintain copies whose freshness depends on propagation. A delayed predecessor write can also disturb path lookup, and repeated Build application must not erase an update made after that Build applied its entry. This initiative makes the node persist its own properties, resolves a path/name through its claim, and isolates lifecycle writes by the writing primary's birth. RDM supplies identity, persistence, execution and recovery semantics; application-specific port names and configuration policy stay in Rafka.

Statements marked **verified** below identify inspected source or document text, not executed tests. Proposed tests, APIs and record additions are marked planned. No runtime test was run to author this PRD.

## Requirements

Every gap has a BUILD owner. Existing behavior used as a dependency is source-verified, but no row is labelled HOLDS without its executable regression evidence. All rows therefore remain BUILD until the implementing epic supplies that evidence or records a passing existing control.

| ID | Requirement, in consumer terms | HOLDS / BUILD | Verified current evidence or source anchor | Owning epic |
|---|---|---|---|---|
| R1 | References carry separate free-text path and dot-free name, with mesh/type separate | BUILD | `crates/rdm-core/src/path.rs:91` stores mesh/kind/ordinal/label/old; `:228` serializes one string | e1 |
| R2 | A late predecessor property write cannot hide the successor | BUILD | `crates/rdm-admin/src/storage.rs:605` exposes node lookup, `:609` node_at, `:617` index_path, `:625` name_claim | e2 |
| R3 | A node persists its properties directly; success follows durable persistence | BUILD | `docs/architecture/lifecycles/node-config-update.md:33` sends the own update upward; `crates/rdm-test/tests/node_own_config.rs:22` asserts that route | e3 |
| R4 | Lifecycle records are keyed by subject node ID and writer node ID/incarnation; flags are not counters | BUILD | `crates/rdm-node/src/nodes_storage.rs:200` has one combined NodeRow; proposal R-NR2/R-NR3 specify the split | e2, e3 |
| R5 | Readers select the election's current primary, never the most recently written lifecycle record | BUILD | `crates/rdm-admin/src/atlas.rs:874` reads node_at; proposal R-NR2 requires election resolution then exact record read | e3, e5 |
| R6 | A regained primary cannot overtake an unresolved earlier write to its own lifecycle key | BUILD | Proposal R-NR2 and approved `4dbf01eba`; current provider methods promise durability, not this cancellation ordering (`storage.rs:613`) | e0, e3 |
| R7 | Builds override all entry-owned properties of new and existing nodes through appropriate operations | BUILD | `crates/rdm-admin/src/accepted.rs:1042` properties_owed; `node_rows.rs:1321` seeds properties; proposal R-NR5 | e4 |
| R8 | A lost apply reply or executor crash cannot make the same Build overwrite a later update | BUILD | Proposal R-NR5 uses receipt-after-reply; `accepted.rs:1043` checks completed receipts; fork F1 | e4 |
| R9 | A stopped-node update survives restart, and Ready cannot clear a newer unapplied update | BUILD | `node-config-update.md:23` acknowledges durable writes for stopped nodes; proposal R-NR5 adds owed map but clears on Ready; F2 | e4 |
| R10 | Takeover reconstructs valid lifecycle evidence without promoting a late predecessor write | BUILD | Proposal R-NR8 reads earlier records and observations; F3; `atlas.rs:500` currently folds records | e3 |
| R11 | node.get and acceptance read current records; missing or failed evidence never becomes a fabricated live/dead result | BUILD | `docs/architecture/node-rpc-envelope.md:346` currently serves node.get from atlas; `atlas.rs:877` raw path lookup | e5 |
| R12 | Config, tags, ports, executable, storage, placement and identity have explicit owners and restart semantics | BUILD | `crates/rdm-node/src/nodes_storage.rs:200` and proposal R-NR3 field table; allocation and issuer facts must enter one lifecycle write path | e2, e3, e4 |
| R13 | File and application providers implement the same split-record contract; storage migration includes both record kinds | BUILD | `crates/rdm-admin/src/storage.rs:596` NodesStorage; `crates/rdm-node/src/storage_backend.rs`; i149 moves topology stores | e2, e6 |
| R14 | Rafka publishes its ports without an admin round trip and consumes the new reference/read shape | BUILD | Proposal Rafka section; actual consumer call sites and current pin must be verified by e6 before changes | e6 |
| R15 | Real-process recovery, registered fast/release/soak coverage and feature docs prove the behavior | BUILD | `crates/rdm-test/tests/main.rs:50`, `:268`; `crates/rdm-test/src/estate.rs:647`; acceptance registry | e0, e7 |

## § Open forks — evidence before a ruling

The writer-birth key, no node revision/counter, Builds overriding properties, and serialized regain writes are settled directions, not new forks. The following unresolved boundaries are assigned work, not deferred gaps.

| Fork | Positive surface opened and what it does | Architecture search and exact passage | Outlier document and exact conflicting or missing passage | Why the existing contract does not answer |
|---|---|---|---|---|
| F1: lost apply acknowledgement | `crates/rdm-core/src/status.rs` ApplyNodeConfig and `crates/rdm-client/src/calls.rs:373` config_apply; current request carries revision | `node-config-update.md:24`: completion after Applied or AlreadyApplied; current installs ordered by revision at `:14` | Proposal R-NR5 “written when the node's completion arrives”; R-NR6 permits repeated map writes | Removing revision and recording only the later Build receipt leaves apply → lost reply → local update → repeated apply unresolved |
| F2: update during startup | `node-config-update.md:23` stopped update completes after durable row write | `rg -n 'stopped|durable|Applied' docs/architecture/lifecycles/node-config-update.md`; `:26` next start receives config | Proposal R-NR5 “clears the owed map on the node's ready” | Environment A can be read before acknowledged update B; Ready for A cannot prove B applied |
| F3: source of takeover state | `atlas.rs:500` folds earlier records; `:874` composes admission state | Q109 requires all readers to apply the same fold; Q197 requires operations as action evidence | Proposal R-NR8 “predecessor's records ... join reports, the digest” | An old mutable record can be changed after supersession; copying it under the successor's key would promote stale state |

**F1 arms:** A: persist apply identity and effect together at the node, reuse the existing Build/operation/target identity on repeats; B: retain an ordered update mechanism; X: another proof of durable duplicate recognition. Recommend A, with no new monotonic node counter. e4 owns the schema, crash proof and exact API delta. A completion for an offline update must distinguish durable acceptance from node application without falsely recording the latter.

**F2 arms:** A: identify each owed update and clear only the one acknowledged as applied; B: serialize startup with all incoming property commands, retaining their obligations; X: equivalent ordering proof. Recommend A, using existing operation identity where sufficient, not a row revision. e4 must account for config and tag operations, a second update during startup, re-birth and caller cancellation.

**F3 arms:** A: reconstruct from durable committed lifecycle operations/receipts; B: use a stable handover snapshot with an explicit committed boundary; X: equivalent provenance proof. Recommend A where existing receipts cover the fields; e3 must name any missing evidence and implement its source rather than guess from gossip. This is required for correctness but not yet approval of a new storage schema.

The exact architecture deltas implementing these answers require approval before landing, not before preparing code and proofs. e0 reads the positive surfaces and records one numbered ruling packet per unresolved choice under the existing workflow. No speculative framework is authorized by this PRD.

## § Seed spec

**Ordering:** e0 supplies the real-process fault seam first. Prove the restart-recovery canary behaviorally RED, then the seed RED, before feature implementation. Compilation failure or an unavailable fixture is not RED evidence.

### Estate fixture and request

```text
Fixture: planned:crates/rdm-test/tests/node_records.rs
Starting points: crates/rdm-test/tests/estate_handover.rs (real process/public API)
                crates/rdm-test/tests/node_own_config.rs (functional own-update control)
Harness: crates/rdm-test/src/estate.rs, Estate::bootstrap_on, Owner,
         Estate::artifact, Estate::spans, Estate::record_trace_url
Request decoder: rdm_core::read::ReadRequest, crates/rdm-core/src/read.rs:36
Consumer action: create nodes through the fixture's existing BuildCarrier path,
                 update properties, replace one node, read it through Read/Get.
Expected public result: successor node ID; intended config and tags; actual reported
                        ports; no reapplication after a completed same-Build apply.
```

Complete current typed read request (source-shape checked against `read.rs:36` and its existing example at `:439`; no invented wire JSON):

```rust
rdm_core::read::ReadRequest::Get {
    entity: rdm_core::filter::Entity::Nodes,
    filter: rdm_core::filter::Filter::new().eq("mesh", ["mesh1"]),
}
```

The **planned extension**, not a current decoder claim, changes the filter to `path=mesh1.rpc` and `name=north`. e1 supplies and round-trips its decoder cases before the fixture uses them. Build requests are constructed using the existing `BuildCarrier` and bindings from the fixture; this PRD does not supply a standalone create JSON or pretend a future Launch struct compiles today.

Highest feature estate: two meshes, three admin nodes in mesh1, one admin in mesh2, and one RPC application node in mesh1. This supports primary change/regain and a distinct FP reader. Smaller seed cases reuse it. Use actual `rdm-node-admin` and `rdm-rpc-node` bindings from `binding_set_from_build_manifest` (`estate.rs:448`), validated by `bootstrap_on` (`:712`); file topology storage is the initial backend. e6 adds the actual Rafka gateway consumer and registered application backend instead of claiming the RPC node is a gateway. Process deployment is required first; container deployment is required at final acceptance because the reproduced failures involved pause/resume. Record candidate SHA, executable hashes, backend names, shape and provider in the estate manifest.

Owned artifacts use `Estate::artifact` under `RDM_ARTIFACTS_DIR`/estate root, retained storage under `Estate::store_dir`, and `Estate::spans` for captured JSONL. No fixed host address or shared disposable data directory is embedded in the test.

### Suite assertion

Planned cell: `node_records::a_build_applies_properties_and_a_nodes_later_update_stands`, in planned `planned:crates/rdm-test/tests/node_records.rs`, registered as `mod node_records;` in the existing test target `main` (`crates/rdm-test/Cargo.toml`). Use the neighboring `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` syntax from `estate_handover.rs`; do not confuse the in-process node_own_config cell with estate proof.

Setup a node with old ports, config and tags. Submit replacement with an entry omitting old ports. Observe the new ID and its initial properties, cause its own bound-port update, resume the same Build, and read all properties through public Read/Get. Then submit a later Build with different properties and prove they apply. Assert old ID history separately; no successor lookup may select the predecessor. Positive own-write spans plus absence of the old upward config route prove the route removal. Existing lifecycle operations retain exact-birth admission.

### What a passing seed proves

An embedding application can publish its own properties durably, replace a node using the Build entry, and inspect the successor without waiting for gossip or copied atlas properties. A later Build still overrides existing nodes. The seed does not prove acknowledgement-loss, restart, delayed-provider, takeover-source, or startup-update races; the canaries and epic cells below do.

## § RED/GREEN evidence — acceptance by executable proof

Executed first-increment cells are recorded under Implementation plan; they establish the
storage substrate and own-record persistence only. The end-state rows below remain pending
where route removal, new pair decoding or full Build application is not implemented.

All names below are **planned** until e0 registers and discovers them. Every row is pending. Preserve the same behavioral assertion across baseline and implementation; schema adapters for baseline tests must be recorded.

| Requirement / epic | Exact planned test and command | RED source SHA, test revision and expected assertion | GREEN source SHA and result | Evidence / trace / duration |
|---|---|---|---|---|
| R1/e1 | `scripts/cells.sh rdm-core lib -- path::tests::separate_path_and_name_round_trip` | Baseline + test patch; pair serialization/validation fails | pending | pending |
| R2/e2 | `scripts/cells.sh rdm-test main -- node_records::a_deleted_birth_cannot_hide_its_successor` | Baseline + patch; public pair lookup selects wrong birth | pending | pending |
| R3,R7/e3,e4 | `scripts/cells.sh rdm-test main -- node_records::a_build_applies_properties_and_a_nodes_later_update_stands` | Baseline + patch; route/property assertion | pending | pending |
| R6/e3 | `scripts/cells.sh rdm-test main -- node_records::a_regained_primary_waits_for_its_earlier_put` | Delayed put; takeover must not complete early | pending | pending |
| R8/e4 | `scripts/cells.sh rdm-test main -- node_records::an_applied_build_update_survives_node_restart` | Apply committed, reply lost, later update; replay must preserve later value | pending | pending |
| R9/e4 | `scripts/cells.sh rdm-test main -- node_records::a_start_preserves_a_newer_owed_update` | B acknowledged after A handed to launch; Ready must not erase B | pending | pending |
| R10/e3 | `scripts/cells.sh rdm-test main -- node_records::takeover_survives_mesh_primary_restart` | Interrupted takeover and late predecessor put; valid lifecycle reconstructed | pending | pending |
| R11/e5 | `scripts/cells.sh rdm-test main -- node_records::current_node_reads_survive_fabric_primary_restart` | Fresh FP with retained store resolves all records without gossip | pending | pending |
| R12,R13/e2,e6 | `scripts/cells.sh rdm-test main -- node_records::all_node_fields_survive_node_restart` | Full field readback, not sampling | pending | pending |
| R14/e6 | planned Rafka public consumer cell, exact target resolved by e6.s1 | Pin baseline and consumer test SHA before executing | pending | no inferred Rafka test target |
| R15/e7 | fast/release plus existing acceptance jobs below | Existing passing controls need no artificial RED | pending | pending |

Documentation-only e0/e7 stories use link, reference, schema-example and registration checks. No runtime evidence is claimed for this PRD itself.

## § Surfaces — one operation and one shared implementation

| Operation | Node RPC op/request and direction | Typed client or embedding hook | Shared implementation | Eligibility, target and refusal |
|---|---|---|---|---|
| Node's own config update | Current Status/UpdateNodeConfig becomes a local durable write; explicit approved exception to command routing | `RunningNode::config_update`; current functional cell in node_own_config.rs | planned node-record writer in rdm-node, bound by storage backend | Own exact node; error is not durable success; preserve named I/O failure |
| Caller/Build config apply | Status/ApplyNodeConfig, FP → MP → exact node | `crates/rdm-client/src/calls.rs:373` config_apply | Same node writer, plus e4 durable apply recognition | Exact birth; repeat returns Applied/AlreadyApplied semantics without repeating effect; F1 determines exact enum delta |
| Tags and other entry properties | Existing property commands; replacement for fields requiring it | Existing client/property surfaces audited in e4 | `crates/rdm-admin/src/node_properties.rs`, workflows, node writer | No new product policy; no direct executable overwrite bypassing replacement |
| node.get | Read 0x23/Get Nodes | `calls.rs:486` read | One claim/current-primary/record resolver, shared by acceptance | Missing current lifecycle evidence is unavailable; I/O error never becomes empty success |
| Create/replace | Existing Build request/carrier | BuildCarrier | Existing create pipeline, changed bootstrap data/record ownership | Existing claim NameHeld remains; exact target pinned; unrelated successor untouched |
| Primary change | Existing node.primary.update op | Existing lifecycle entry | Existing election decides role; lifecycle writer records result | No record-number election; flags remain flags |

Entity reads do not resume a Build. Command completion, durable offline acceptance and actual node application are distinct evidence. e4 must state which existing reply proves each case before changing its decoder. Demo and Rafka adapters consume these implementations; no parallel execution engine.

## § Restart-recovery canary — required applicability analysis, first test

Persistence/recovery is required at all three process boundaries. Preserve store roots, exact keys and the required data directories. Run clean termination and forced termination at the relevant boundary; distinguish same-birth stop/start from new-ID recovery.

| Process boundary | Surviving state and obligation | Durable location and owner | Exact planned canary / public assertion |
|---|---|---|---|
| ordinary node | applied properties, apply identity if F1 selects it, later own update | node record in configured shared backend; node writes | `an_applied_build_update_survives_node_restart`: kill after apply persistence before reply; compare entire public config/tags and identity after replay |
| mesh-primary | lifecycle source, owed stopped-node updates, unresolved writer state | lifecycle/Build evidence, current election identity; MP workflow | `takeover_survives_mesh_primary_restart`: force exit midway through takeover; every required lifecycle value reconstructed or explicitly unavailable, never invented |
| fabric-primary | accepted Build and exact-target work; current node resolution | existing Build store plus claims and lifecycle records | `current_node_reads_survive_fabric_primary_restart`: fresh process reads successor with no new gossip and resumes only unfinished work |

**Required harness primitive:** e0.s2 owns delayed provider completion, lost apply reply and exact persistence-boundary process termination. Reuse `testkit-store` only after checking its capabilities; if it cannot separate client cancellation from eventual provider commit, extend it. An in-process barrier proves ordering but does not replace the real-process restart canary. No fixed sleeps establish ordering. Underlying I/O still unresolved after client cancellation must remain represented until its outcome is known, as approved; e3 must prove this across the actual supported backend's process boundary.

## § OTLP span-assertion bar

Use `Estate::spans` (`estate.rs:958`), `artifact` (`:851`) and `record_trace_url` (`:994`). Retain JSONL and import/export through the repository's actual OTLP tooling; e0 records the exporter command after discovery. A local trace ID alone is not proof Jaeger has it. Query the configured collector and retain the retrieved trace. No hardcoded endpoint.

| Span | Emitting process/path | Mandatory attributes | Observable control |
|---|---|---|---|
| existing `rdm.node_admin.config.update.via-call` | existing own-config test asserts this old path | node/exact target, outcome | Local-write seed produces the new write span; own-update trace contains no old upward call |
| planned `rdm.node.record.update.via-properties` | planned rdm-node record writer | node_id, incarnation_id, path, name, operation identity when present, backend, durable outcome | Own update public value and durable success |
| planned `rdm.node_admin.node.resolve.via-lifecycle-record` | shared resolver | subject ID, selected writer ID/incarnation, outcome, reason | Late old write is positively recorded; read selects current writer |
| planned `rdm.node_admin.node.update.via-lifecycle-record` | serialized lifecycle writer | subject, writer birth, phase, completion outcome | Delayed put and regained takeover order visible |
| planned `rdm.node.record.update.via-already-applied` | e4 duplicate recognition if F1 A selected | exact target and original operation identity, outcome | Deliberately replay A after B; public B remains |

Final names pass `scripts/name-check.py`. Do not log config maps, secret environment, credentials or unrestricted application data. Preserve causal parentage from public request through persistence and acknowledgement. Background own updates retain their authored trace; do not fabricate a common parent for unrelated operations.

## § Per-increment seed-data obligation

- e1: path/name serde cases, free-text names, empty/invalid grammar decisions, `.old` path move, every reference shape and environment pair; explicit historical compatibility/refusal decision.
- e2: node and writer-birth lifecycle record decoders; name claims; each field in proposal R-NR3; a current and deleted node plus two lifecycle writers. Include complete data in the functional seed, not only config.
- e3: earlier writer, current writer and same-birth regain; retained roots and failed/unknown I/O.
- e4: create entry, same-Build apply, later Build, own update, stopped owed update A/B, tags and reply-loss records. Track acknowledgement and application separately.
- e5: full node.get filters including path/name/id/tags, missing lifecycle record, Deleted history and current claim.
- e6: real Rafka gateway's dynamic ports and actual registered application storage provider, plus i149 copy inventory containing both record kinds and unresolved obligations.

No new fixture language. e0 owns harness registration, e2 owns record decoder fixtures, and e6 owns consumer/provider fixture registration.

## § Test tier coverage

| Selected | Tier | Exact test file / target / filter | Proof |
|---|---|---|---|
| [x] | unit | `crates/rdm-core/src/path.rs` inline tests; planned tests in node-record writer | Pair grammar, writer selection, duplicate application |
| [x] | functional | planned `crates/rdm-node/tests/node_records.rs`; main module registration audited by e0 | Real file persistence; overwriting fake provider; delayed put; entire field mapping |
| [x] | e2e | planned `planned:crates/rdm-test/tests/node_records.rs`, target main, `node_records::` | Seed and three real-process restart boundaries |
| [x] | load | existing `crates/rdm-test/tests/rshape_burn_in.rs`, registry jobs below | Sustained public reads/writes and provider pressure; retain timing/resource evidence |
| [x] | chaos | node_records cells plus authority/randomized process/container jobs | Lost reply, paused predecessor, regain, interrupted takeover |

### Fast smoke, release and scheduled coverage

After e0 registers the stem, focused invocation is `scripts/cells.sh rdm-test main -- node_records::`. The implementation checkout's assigned name is substituted in `scripts/rdm-gate-at.sh <assigned-checkout> fast` and `... release`; no lane is claimed by this PRD. Register the short seed in the existing fast gate and long restart/fault coverage in release without dropping existing checks.

Existing, verified job IDs in `tools/mesh-audit/i143-acceptance-jobs.json`:

```sh
scripts/i143-acceptance-gate.sh i143-rshape-soak-authority-process
scripts/i143-acceptance-gate.sh i143-rshape-soak-authority-container
scripts/i143-acceptance-gate.sh i143-rshape-soak-randomized-process
scripts/i143-acceptance-gate.sh i143-rshape-soak-randomized-container
```

These jobs declare seed `1431101` and 1800 seconds; retain their budgets and assertions. Registry presence is verified, successful execution is not. e7 checks their consumer binaries match the implementation SHA. Do not rerun an unchanged red to erase it; inspect its evidence and fix the cause.

**Nightly gap:** no `.github` directory exists at this baseline, and the inspected gate/document references do not establish an active nightly schedule. This does not prove no external scheduler exists. e7.s2 discovers the deployed scheduler, records its configuration and last execution, and adds the feature to that existing schedule; if absent, prepares the required scheduled-run integration and obtains any operational approval. No automation is created by this PRD.

## § Pre-implementation thoughts (2026-10-31)

- **Hard part:** preserving acknowledged property effects and valid lifecycle evidence while removing copied rows and per-write revisions.
- **Likely failure:** lost apply reply repeats a Build map; Ready clears a newer owed update; takeover copies stale predecessor state; timed-out provider write lands after regained takeover.
- **Persistence path:** node command/local update → serialized node writer → configured provider → durable acknowledgement; lifecycle operation → one writer for its ownership identity → lifecycle record → current-primary resolver.
- **Dependencies:** existing election and exact-birth admission; claim primitive remains unchanged; i149 migrates the newly split stores; i141 consumes wire/storage changes. Q109/Q197 remain explicit constraints.
- **Architectural risks:** one writer per key must be true for launch, fabric recovery, primary flags and normal MP updates; async cancellation does not cancel a backend mutation; environment maps have finite OS capacity; records may grow with retained operation identities and writer births.
- **Uncertainty to resolve:** e2 measures serialized record/environment sizes and defines named oversize behavior without silent truncation; e3 verifies write-lifetime ordering on both backends; e4 settles F1/F2; e3 settles F3. No arbitrary runtime capacity or deadline is invented here.

No new background retry loop, queue or gossip channel is authorized. Existing call budgets remain until evidence supports an explicit change. Any introduced waiting/admission mechanism must publish its items/bytes, concurrency, deadline, retries and exhaustion behavior in the owning story before implementation approval; unresolved provider outcome means unavailable, never permission to overwrite.

## Implementation plan

Initial implementation increment: `rdm-node::node_records` introduces split record schemas and
provider-neutral persistence traits with memory/file adapters. The node's own config update now persists its property record before the existing admin delivery;
admin reads/lifecycle selection and removal of that delivery route are still outstanding. A test-only StoreLog append hold exposes cancellation ordering;
storage tests cover late writers, exact writer incarnation, clean process exit, forced process
exit and corrupt evidence. This is foundation work for e0.s2/e2.s1–s2, not a claim that the feature
seed, node-runtime canaries, or end-to-end migration are GREEN. Initial focused evidence (2026-10-31, lane 1): four node-record storage tests passed (0.04 s),
28 store-log tests passed (0.21 s), two existing provider-change tests passed (0.50 s), and two
existing own-config regression cells passed (2.63 s). The real-process canary
`node_records::own_properties_survive_node_restart` reached RED at its missing node-owned record
assertion (14.48 s), then passed after node persistence was connected (14.49 s). The first GREEN
attempt lost the killed process's batched span; the final cell observes export before SIGKILL,
while the separate 100-record clean/SIGKILL storage canary uses no telemetry wait. Source baseline
was `4dbf01eba` plus the new schema/canary substrate; substrate commit is `2822b5186`.
Logs are retained under lane 1 `target/evidence/i150/`; raw own-write trace ID:
`c38df8a07ff405033cbf620b8177d93a`, JSONL retained; collector retrieval is not yet claimed for that earlier run. The post-land run exported and
retrieved trace `15ecea45ac91e366af021edd0e449663` (41 spans); its own-write span records
`outcome=durable` and 434 microseconds. Retrieved JSON is retained as
`target/evidence/i150/jaeger-own-write.json`.
The intermediate run refused stale binaries after a source edit during its build and is not
counted as a behavioral RED or GREEN. The first fast gate at `ca00b3b1a` ran in 393 s and reported 2513 passed,
1 failed, 13 ignored in its test summaries. The sole failure was the new acceptance job's
registration: the existing schema requires `i143-245-process`, the standard exact-test command,
and an explicit test name for runner evidence. The registration was corrected and its focused
audit test passed 1/1. The gate must run on that corrected commit; the red is not rerun unchanged.
The 13 ignored cells are existing suite output, not new i150 acceptance.
No final gate, application-provider qualification, Rafka
integration, or full i150 acceptance is claimed by these focused passes.

Stories below are the dispatchable gap list. Each starts BUILD, with implementation owner unassigned; no issues or agents are created by this document. Story completion requires its exact test, evidence and contract delta, not only merged code.

### e0 — Executable canaries and contract decisions precede implementation

- **Goal → gap:** deliver R6,R8–R10,R15; source/design anchor: `Proposal R-NR2/R-NR5/R-NR8; estate.rs:647, :851, :958`.
- **Detailed requirements / stories:**
  - **i150.e0.s1 — BUILD:** Inventory source and exact architecture deltas; record F1–F3 decisions and i143/i141/i149 dependencies. Approval timestamps are recorded only when supplied.
  - **i150.e0.s2 — BUILD:** Extend existing store/reply fault seams for delayed provider completion, lost completion and retained-storage process restart. Prove faults reached the intended persistence boundary.
  - **i150.e0.s3 — BUILD:** Register node_records in main, gate and acceptance metadata; author canary RED first and seed RED second; verify nonzero discovery.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** All canaries and seed in the evidence table; RED must reach public assertions.
- **Estate plan:** Full two-mesh feature estate; initial file/process backend, container capability proved before final acceptance.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: crates/rdm-test/src/estate.rs; crates/rdm-test/tests/main.rs; planned:crates/rdm-test/tests/node_records.rs; tools/mesh-audit/i143-acceptance-jobs.json.
- **Rows absorbed:** R6,R8–R10,R15; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e1 — Consumers address separate path and name

- **Goal → gap:** deliver R1; source/design anchor: `crates/rdm-core/src/path.rs:91 and :228`.
- **Detailed requirements / stories:**
  - **i150.e1.s1 — BUILD:** Replace parsed mesh/kind/ordinal address with path/name pair; preserve separate mesh/type data and validate dot-free name.
  - **i150.e1.s2 — BUILD:** Update serde, claims, launch environment, references, client and public filter decoding together; identify all old-field callers by compile/source audit.
  - **i150.e1.s3 — BUILD:** Move predecessor path to .old with its name unchanged; preserve claim ownership and exact identity across interrupted moves.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** path::tests::separate_path_and_name_round_trip; node_records::a_path_move_preserves_the_node_identity (planned).
- **Estate plan:** Feature estate, one node is moved then replaced; complete public pair readback.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: crates/rdm-core/src/path.rs; crates/rdm-core/src/launch.rs; crates/rdm-client/src/calls.rs; crates/rdm-node/src/nodes_storage.rs.
- **Rows absorbed:** R1; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e2 — Records cannot redirect a path merely by being written

- **Goal → gap:** deliver R2,R4,R12,R13; source/design anchor: `crates/rdm-admin/src/storage.rs:596–630; crates/rdm-node/src/nodes_storage.rs:200`.
- **Detailed requirements / stories:**
  - **i150.e2.s1 — BUILD:** Define node-property and lifecycle schemas/traits, exact key encoding and complete field map; retain no node revision/counter.
  - **i150.e2.s2 — BUILD:** Bind file and functional overwriting providers; resolve pair through claim; remove index_path/node_at only when every caller has its replacement.
  - **i150.e2.s3 — BUILD:** Preserve tag indexing, Deleted reads, claim release and partial-write recovery; measure encoding and environment capacity; inventory i149 copy obligations.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** node_records::a_deleted_birth_cannot_hide_its_successor; node_records::all_node_fields_survive_node_restart.
- **Estate plan:** Feature estate with current/deleted IDs and exact birth readback; functional overwriting backend checks preserved values.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: crates/rdm-admin/src/storage.rs; crates/rdm-admin/src/storage_backend.rs; crates/rdm-node/src/nodes_storage.rs; crates/rdm-node/src/storage_backend.rs; crates/rdm-node/tests/node_records.rs.
- **Rows absorbed:** R2,R4,R12,R13; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e3 — Nodes write properties and lifecycle succession preserves valid evidence

- **Goal → gap:** deliver R3–R6,R10,R12; source/design anchor: `node-config-update.md:33; proposal R-NR3/R-NR6/R-NR8; atlas.rs:500`.
- **Detailed requirements / stories:**
  - **i150.e3.s1 — BUILD:** Bind node-owned writer and one serialized lifecycle writer; route allocation and fabric-primary-flag facts through that writer; explicitly assign fabric recovery ownership.
  - **i150.e3.s2 — BUILD:** Resolve existing election result then writer-birth key; keep flags boolean; make missing current records unavailable; settle F3 source evidence and implement takeover.
  - **i150.e3.s3 — BUILD:** Retain ownership of unresolved provider writes across timeout; same-birth regain cannot overtake earlier puts. Audit process-exit/provider-late-commit behavior rather than relying on task locks alone.
  - **i150.e3.s4 — BUILD:** Remove upward own-property route and copied rows only after new writer/reader paths and takeover controls pass.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** node_records::a_regained_primary_waits_for_its_earlier_put; node_records::takeover_survives_mesh_primary_restart; seed own-write control.
- **Estate plan:** Highest feature estate enables A→B→A primary ownership; process and container pause, delayed provider completion.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: crates/rdm-node/src/node.rs; crates/rdm-admin/src/node_rows.rs; crates/rdm-admin/src/workflows/node_primary_update.rs; crates/rdm-admin/src/workflows/node_config_update.rs; crates/rdm-admin/src/atlas.rs.
- **Rows absorbed:** R3–R6,R10,R12; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e4 — Build changes apply once and offline changes survive startup

- **Goal → gap:** deliver R7–R9,R12; source/design anchor: `accepted.rs:1042; node_rows.rs:1321; proposal R-NR5`.
- **Detailed requirements / stories:**
  - **i150.e4.s1 — BUILD:** Settle F1; preserve exact-Build/operation/birth application evidence through reply loss and restart. Keep properties_owed with correct evidence rather than deleting its responsibility.
  - **i150.e4.s2 — BUILD:** Settle F2; persist stopped-node config and tag obligations, hand the selected update to launch, acknowledge and clear only the applied obligation.
  - **i150.e4.s3 — BUILD:** Create/replace from the full Build entry; recovery from predecessor properties as specified; later Build overrides existing properties via correct operations.
  - **i150.e4.s4 — BUILD:** Cover crash after property persistence before completion, local update before replay, startup A/B race, recovery with owed changes and failed launch; exact outcomes follow approved F1/F2 text.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** node_records::an_applied_build_update_survives_node_restart; node_records::a_start_preserves_a_newer_owed_update; happy seed.
- **Estate plan:** Feature estate; retain same-ID stop/start separately from new-ID replacement/recovery.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: crates/rdm-admin/src/accepted.rs; crates/rdm-admin/src/deployment/pipeline.rs; crates/rdm-admin/src/node_properties.rs; crates/rdm-node/src/node.rs; crates/rdm-core/src/status.rs; crates/rdm-client/src/calls.rs.
- **Rows absorbed:** R7–R9,R12; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e5 — Current reads and acceptance share one record resolver

- **Goal → gap:** deliver R5,R11; source/design anchor: `atlas.rs:874; node-rpc-envelope.md:346; calls.rs:486`.
- **Detailed requirements / stories:**
  - **i150.e5.s1 — BUILD:** Provide one claim/election/record resolver for node.get and admission; remove raw last-written-path shortcuts and atlas held property rows.
  - **i150.e5.s2 — BUILD:** Preserve complete filters, stable ordering, Deleted history, named unavailable/error outcomes and exact-birth execution revalidation.
  - **i150.e5.s3 — BUILD:** Prove new successor acceptance before gossip and fresh FP readback after restart; update demo adapters through the same client implementation.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** node_records::current_node_reads_survive_fabric_primary_restart; node_records::a_deleted_birth_cannot_hide_its_successor.
- **Estate plan:** Feature estate with distinct reading FP; suppress new-birth gossip without suppressing committed operations.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: crates/rdm-admin/src/atlas.rs; crates/rdm-admin/src/control_plane.rs; crates/rdm-core/src/read.rs; crates/rdm-client/src/calls.rs.
- **Rows absorbed:** R5,R11; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e6 — Rafka and both storage backends consume the new contract

- **Goal → gap:** deliver R13,R14; source/design anchor: `Proposal Rafka section; docs/plans/i149-topology-storage-provider.md`.
- **Detailed requirements / stories:**
  - **i150.e6.s1 — BUILD:** Verify actual Rafka integration files, test targets and pin; record absolute consumer checkout and baseline before dispatch. Do not assume the proposal call-site coordinates still match.
  - **i150.e6.s2 — BUILD:** Update consumer pair/env wiring and direct property write; prove real gateway ports, later Build override and no stale own-map retry.
  - **i150.e6.s3 — BUILD:** Implement application-provider split-record binding and i149 migration inventory; copy lifecycle ownership keys and outstanding apply/update evidence; test switch/restart without loss.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** Planned consumer cell a_gateways_own_ports_survive_build_resumption; exact executable target owned by e6.s1; migration full-state regression.
- **Estate plan:** Real Rafka gateway plus registered application backend; process then container; retain full exported manifest.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: RDM interfaces from e2/e4 (interface changes return to those owners); tools/mesh-audit/i143-acceptance-jobs.json; docs/plans/i149-topology-storage-provider.md; Rafka files assigned after e6.s1 audit.
- **Rows absorbed:** R13,R14; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### e7 — A consumer can use and recover the feature with proven release coverage

- **Goal → gap:** deliver R15; source/design anchor: `acceptance registry authority/randomized jobs; template documentation obligation`.
- **Detailed requirements / stories:**
  - **i150.e7.s1 — BUILD:** Run final fast/release, exact seed/canaries, process/container authority/randomized soaks once at final tip; investigate each red; compare complete results and retrieve Jaeger traces.
  - **i150.e7.s2 — BUILD:** Verify deployed nightly scheduler, add feature coverage or supply the missing scheduled-run integration; retain configuration, discovery and one completed scheduled run.
  - **i150.e7.s3 — BUILD:** Write overview/how-to/runbook with executed examples; reconcile architecture, environment, wire/span references, rulings and parent initiative records.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** All named cells, fast/release and four existing soak jobs; document links/examples and active scheduled-run evidence.
- **Estate plan:** Full feature estate plus canonical soak shapes as registered; no fixture reduction to make a red pass.
- **Persistence/restart:** apply the three-boundary canary matrix to touched state; list retained roots and kill instant in the story receipt. Documentation-only stories record applicability and link/example checks.
- **Sizing/file ownership:** one implementing owner per story; shared files are sequenced by the dispatch table. Owned surfaces: planned:docs/features/node-records/overview.md; planned:docs/features/node-records/how-to.md; planned:docs/features/node-records/runbook.md; docs/architecture/node-identity-and-state.md; docs/architecture/node-rpc-envelope.md; scripts/rdm-gate-at.sh; tools/mesh-audit/i143-acceptance-jobs.json.
- **Rows absorbed:** R15; Q235/Q305/Q308 and rdm#239 are source-described dependencies where relevant, not claims that their issues were independently inspected or closed.
- **State/evidence:** BUILD; owner unassigned; RED SHA/test revision, GREEN SHA/count/duration, artifacts and Jaeger URL pending. No story is complete until its requirement rows and evidence are updated.

### Files likely to touch

- `crates/rdm-core/src/path.rs`, `crates/rdm-core/src/launch.rs`, `crates/rdm-core/src/status.rs`, `crates/rdm-core/src/read.rs` — identity, launch and public wire shapes.
- `crates/rdm-node/src/node.rs`, `crates/rdm-node/src/nodes_storage.rs`, `crates/rdm-node/src/storage_backend.rs` — node writer and record bindings.
- `crates/rdm-admin/src/storage.rs`, `crates/rdm-admin/src/node_rows.rs`, `crates/rdm-admin/src/atlas.rs`, `crates/rdm-admin/src/accepted.rs` — claims, writer ownership, resolver and Build application.
- `crates/rdm-admin/src/deployment/pipeline.rs`, `crates/rdm-admin/src/workflows/node_config_update.rs`, `crates/rdm-admin/src/workflows/node_primary_update.rs` — bootstrap, property command and takeover sequencing.
- `crates/rdm-client/src/calls.rs` — typed consumer API.
- `crates/rdm-test/src/estate.rs`, `crates/rdm-test/tests/main.rs`, `crates/rdm-test/tests/node_own_config.rs`, `crates/rdm-test/tests/node/node_replace.rs` — harness and retained regression controls.
- `planned:crates/rdm-test/tests/node_records.rs`, `crates/rdm-node/tests/node_records.rs` — e0/e2 new acceptance and functional cells; register in existing target arrangements.
- `tools/mesh-audit/i143-acceptance-jobs.json`, `scripts/rdm-gate-at.sh` — existing coverage registration.

## § Per-feature documentation deliverable — final epic

- `planned:docs/features/node-records/overview.md` — ownership, key model, desired entry/application semantics, election-selected reads, failure and recovery.
- `planned:docs/features/node-records/how-to.md` — executable pair/create/update/read examples, process and container prerequisites, full readbacks and named refusal handling.
- `planned:docs/features/node-records/runbook.md` — missing lifecycle evidence, unresolved provider writes, lost apply replies, owed updates, current-primary resolution, exact restart/resume procedures and trace retrieval.

The e7 implementer writes these from working examples and evidence. Contract changes land with their implementing epic, not delayed to e7. Update the existing consumer guide and runbooks instead of adding another generic operational framework.

## Verification

Authoring checks and implementation acceptance are deliberately distinct.

- [x] Every requirement has a BUILD owner; no unsupported passing-runtime claim.
- [x] Forks identify positive source and the conflicting proposal passage; implementation owners assigned by epic.
- [x] Baseline resolves; estate helper and request field names checked against source. The displayed request is a current typed shape, not a future compile claim.
- [ ] Canary RED first, then seed RED; retain identical assertions through GREEN.
- [ ] All three real-process boundaries pass with retained storage, including clean and forced exit.
- [ ] Claim/current-primary resolver is shared by reads and acceptance; adapters do not add alternate semantics.
- [ ] Named refusal and unchanged-state controls pass; I/O failure never means absence.
- [ ] Nonzero test discovery and feature registry execution verified.
- [ ] Fast, release, process/container soaks and scheduled coverage pass at the recorded implementation SHA.
- [ ] Durable apply, startup-update and takeover/regain race proofs pass.
- [ ] File and actual application backend bindings and i149 migration preserve full records/obligations.
- [ ] Production spans, parentage, JSONL and retrieved Jaeger traces retained.
- [ ] Workspace compile, repository name checks and required gates pass.
- [ ] Three feature documents and executed examples land; links resolve.
- [ ] Approved architecture deltas, wire/durable markers and deployment procedure accompany code.
- [ ] Final evidence and approval are recorded only when actually obtained.

### Source-doc propagation

- [ ] Reconcile node identity, storage, config, replace, start, recovery, primary change and lifecycle index pages.
- [ ] Update `docs/eng/consumer-guide.md`, shared runbooks and `CLAUDE.md` environment table.
- [ ] Discover and run existing wire/span-reference generators; retain output in the owning story.
- [ ] Update proposal, applicable rulings and i143/i141/i149 dependency records with exact landings.
- [ ] Search all references to old PathName fields, node_at/index_path, revisions and Q235/Q305 ownership; reconcile each.
- [ ] Record counts, duration, failure investigation and scheduled-run evidence in epic rows.
- [ ] Reconcile per-crate documentation and Rafka public examples against the actual pin.

## § Dispatch ordering — last substantive section

This is a dependency plan, not authorization to launch agents. Follow the existing lane workflow in `CLAUDE.md`; no new worktree is required. Dispatch at most four implementers only when named files are disjoint; default to one owner for the shared writer/schema changes.

| Wave | Epics/stories | Exact files owned | Dependency / sequencing |
|---|---|---|---|
| 0 | e0.s1–s3 | proposal/architecture delta packet, estate.rs, tests/main.rs, planned node_records.rs, registry | Contract decisions and fault substrate; real canary and seed RED before implementation |
| 1 | e1.s1–s3 | core path/launch, reference schemas and client call sites | One coordinated pair-format change; do not concurrently edit shared core/node storage files |
| 2 | e2.s1–s3 | admin storage, node schemas/backend, planned storage tests | Freeze record/trait contract before writers; coordinate i149 inventory |
| 3 | e3.s1–s4 | node.rs, node_rows.rs, atlas.rs, primary/config workflows | Single writer/takeover owner; source-evidence and unresolved-put rules proved before removing old protection |
| 4 | e4.s1–s4 | accepted.rs, pipeline.rs, node.rs, status.rs, calls.rs | Depends on e2/e3 and F1/F2 approval; avoid overlap on node writer and core API |
| 5 | e5.s1–s3; e6.s1 | atlas.rs/control_plane.rs/read.rs/calls.rs; Rafka audit only | RDM resolver work and read-only consumer audit can be separate; no simultaneous client edits |
| 6 | e6.s2–s3 | verified Rafka consumer/provider files; registry coordinated with lead | Pin completed RDM interfaces; storage migration passes before final consumer acceptance |
| 7 | e7.s1–s3 | feature docs, final architecture reconciliation, registry/gate/schedule metadata | One final tip for regression, export and docs examples |

**Critical path:** e0 → e1 → e2 → e3 → e4 → e5 → e6 → e7. F1/F2 block e4 contract landing; F3 and delayed-provider lifetime evidence block e3 acceptance.

**Format changes:** separate path/name references and split node/lifecycle storage are incompatible unless explicitly implemented otherwise. Use the repository's `durable format moved` marker and wire marker requirements; regenerate goldens. e0 specifies the fresh-estate/coordinated consumer-pin procedure and named rejection of unsupported stored layouts. No mixed-version correctness is claimed. Fresh-estate deployment tests do not replace retained-format restart tests within the new version.

## § Retro (on ship)

Not shipped. e7 records actual surprises, predicted failures, seed omissions, process changes, shipped SHAs, counts/durations, artifact locations, Jaeger links and the completed scheduled run. Do not populate this section with predicted success.

## Cross-references

- Design: [node-record proposal](proposal/node-record.md)
- Contracts: [node identity](../architecture/node-identity-and-state.md), [node config update](../architecture/lifecycles/node-config-update.md), [node replace](../architecture/lifecycles/node-replace.md), [Node RPC envelope](../architecture/node-rpc-envelope.md), [storage](../architecture/storage.md)
- Rulings: [index](rulings/INDEX.md), Q109/Q197/Q227/Q234/Q235/Q236/Q242/Q305/Q308; exact scope and approvals recorded at e0
- Dependencies: [i143](i143-prd.md), [i149](i149-topology-storage-provider.md); Rafka i141 integration record located and checked by e6.s1
- Repository workflow: [CLAUDE.md](../../CLAUDE.md)
- Public API guidance: [consumer guide](../eng/consumer-guide.md)
- Source template: [seed-first PRD](../eng/templates/prd-seed-template.md)
