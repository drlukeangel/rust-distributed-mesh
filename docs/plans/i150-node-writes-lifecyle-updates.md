# i150 — nodes persist their properties; lifecycle reads follow the current primary

**Initiative / Epic / Story ID:** `i150`, epics e0–e7
**Sprint:** initiative delivery increments 0–7; dates assigned at dispatch
**Status:** opened 2026-10-31; implementation started in lane 1 on `codex/i150-node-records`, rdm#245; runtime acceptance remains BUILD
**Architecture doc:** [Node identity and state](../architecture/node-identity-and-state.md) §2.5, [node config update](../architecture/lifecycles/node-config-update.md), and [storage](../architecture/storage.md) §2. These current contracts need coordinated amendments; the target design is [node-record.md](proposal/node-record.md), R-NR1–R-NR8.
**RED/GREEN evidence:** pending; canary first, then seed, then implementation
**Approved:** Luke requested this PRD and approved the serialized-write regain correction in this chat on 2026-10-31, committed as `4dbf01ebaf99ca5e17d4e2ba9cfb78ebf8fe2a65`. No UTC approval time was supplied. That approval does not constitute approval of the unresolved forks or all architecture deltas below.
**Owner:** CLI implementation in lane 1 on `codex/i150-node-records`; desktop documentation audit in the same unit; Rafka integration coordinated with the i141 lead
**Feature:** `node-records`
**Baseline:** `4dbf01ebaf99ca5e17d4e2ba9cfb78ebf8fe2a65`, verified with `git cat-file -e`; no accompanying uncommitted contract edits are incorporated
**Record:** [i143-prd.md](i143-prd.md) is the current remaining-work record. This file defines i150 requirements, stories and acceptance evidence; e6 coordinates the i141 consumer record and [i149](i149-topology-storage-provider.md).

**Append-only storage:** lifecycle changes append records. `Deleted` is the status in the latest appended lifecycle record in the election-selected writer’s stream for that node, not removal of that record or its history. A later append by a former writer does not supersede the selected writer’s stream. Replacement changes only the predecessor’s name to `<name>-old`, at its immutable path; the successor has a new globally unique node ID and claims the original separate `(path, name)` fields. An explicit predecessor reference records node ID and incarnation, not authorization.
**Source template:** [Seed-first PRD](../eng/templates/prd-seed-template.md). The requested filename `i150-node-writes-lifecyle-updates.md` is intentional.

## Context

An embedding application's node currently sends its own property update through its mesh-primary and fabric-primary, while readers maintain copies whose freshness depends on propagation. A delayed predecessor write can also disturb path lookup, and repeated Build application must not erase an update made after that Build applied its entry. This initiative makes the node persist its own properties, resolves a path/name through its claim, and isolates lifecycle writes by the writing primary's birth. RDM supplies identity, persistence, execution and recovery semantics; application-specific port names and configuration policy stay in Rafka.

Requirement-table source anchors describe the inspected baseline `4dbf01eba`, not line numbers
in the changing implementation. The implementation record below identifies later commits and
executed increment controls. Planned acceptance tests and fields are not completed behavior.

## Requirements

The location and identity contract is [R-NR1](proposal/node-record.md#requirements):
`path` is where a node is; `name` is its name, unique within that path. They remain separate in
storage, wire objects, launch inputs, references, queries and diagnostic fields. A structured
argument or composite `(path, name)` index may contain both. Neither field supplies identity,
mesh, type or ordinal; code selects `node.id` first and reads explicit metadata. Replacement
keeps the predecessor's path unchanged, changes its name to `<name>-old`, and creates the
successor with the original name at that same path. Architect 1's review includes all these surfaces.

Every gap has a BUILD owner. Existing behavior used as a dependency is source-verified, but no row is labelled HOLDS without its executable regression evidence. All rows therefore remain BUILD until the implementing epic supplies that evidence or records a passing existing control.

| ID | Requirement, in consumer terms | HOLDS / BUILD | Baseline evidence or target contract | Owning epic |
|---|---|---|---|---|
| R1 | Code selects globally unique node IDs first; location arguments carry separate immutable path and mutable name, with explicit mesh/type metadata | BUILD | `crates/rdm-core/src/path.rs:91` stores mesh/kind/ordinal/label/old; `:228` serializes one string | e1 |
| R2 | A late predecessor property write cannot hide the successor | BUILD | `crates/rdm-admin/src/storage.rs:605` exposes node lookup, `:609` node_at, `:617` index_path, `:625` name_claim | e2 |
| R3 | A node persists its properties directly; success follows durable persistence | BUILD | `docs/architecture/lifecycles/node-config-update.md:33` sends the own update upward; `crates/rdm-test/tests/node_own_config.rs:22` asserts that route | e3 |
| R4 | One node record keyed by node ID, append-only, latest line current: the node writes its properties, its mesh-primary writes status and role flags; no writer in the key, no author field, flags not counters (Q323) | BUILD | `NodeLifecycle` in `rdm-node` `node_records` is keyed by writer birth today; proposal R-NR2 as Q323 rewrote it | e2, e3 |
| R5 | Readers read the node record's latest line at the node ID; no current-writer resolution (Q323) | BUILD | `current_record.rs` resolves a writer today; proposal R-NR2 as Q323 rewrote it | e3, e5 |
| R6 | A regained primary cannot overtake an unresolved earlier write to its own lifecycle key | BUILD | Proposal R-NR2 and approved `4dbf01eba`; current provider methods promise durability, not this cancellation ordering (`storage.rs:613`) | e0, e3 |
| R7 | Builds override all entry-owned properties of new and existing nodes through appropriate operations | BUILD | `crates/rdm-admin/src/accepted.rs:1042` properties_owed; `node_rows.rs:1321` seeds properties; proposal R-NR5 | e4 |
| R8 | A lost apply reply or executor crash cannot make the same Build overwrite a later update | BUILD | Proposal R-NR5 uses receipt-after-reply; `accepted.rs:1043` checks completed receipts; fork F1 | e4 |
| R9 | A stopped-node update survives restart, and Ready cannot clear a newer unapplied update | BUILD | `node-config-update.md:23` acknowledges durable writes for stopped nodes; proposal R-NR5 adds owed map but clears on Ready; F2 | e4 |
| R10 | Removed (Q323): no takeover preparation; the successor installs the election's result the call carries and writes its nodes' records from then on (node-primary-update.md as main has it) | — | 415f1caef and e63570305 reverted 2026-11-01 | — |
| R11 | node.get and acceptance read current records; missing or failed evidence never becomes a fabricated live/dead result | BUILD | `docs/architecture/node-rpc-envelope.md:346` currently serves node.get from atlas; `atlas.rs:877` raw path lookup | e5 |
| R12 | Config, tags, ports, executable, storage, placement and identity have explicit owners and restart semantics | BUILD | `crates/rdm-node/src/nodes_storage.rs:200` and proposal R-NR3 field table; allocation and issuer facts must enter one lifecycle write path | e2, e3, e4 |
| R13 | File and application providers implement the same split-record contract; storage migration includes both record kinds | BUILD | `crates/rdm-admin/src/storage.rs:596` NodesStorage; `crates/rdm-node/src/storage_backend.rs`; i149 moves topology stores | e2, e6 |
| R14 | Rafka publishes its ports without an admin round trip and consumes the new reference/read shape | BUILD | Proposal Rafka section; actual consumer call sites and current pin must be verified by e6 before changes | e6 |
| R15 | Real-process recovery, registered fast/release/soak coverage and feature docs prove the behavior | BUILD | `crates/rdm-test/tests/main.rs:50`, `:268`; `crates/rdm-test/src/estate.rs:647`; acceptance registry | e0, e7 |

## § Contract boundaries — decisions, implementation and acceptance

The writer-birth key, no node revision/counter, Builds overriding properties, and serialized regain
writes are settled directions. The existing election `Primary.revision` remains separate from
node-property and lifecycle records. F1's atomic-recognition implementation is recorded below;
its combined process proof and the F2/F3 boundaries remain assigned work. Architecture wording
requires the exact-text approval and coordinated landing recorded in e7.

| Fork | Positive surface opened and what it does | Architecture search and exact passage | Outlier document and exact conflicting or missing passage | Why the existing contract does not answer |
|---|---|---|---|---|
| F1: lost apply acknowledgement | `crates/rdm-core/src/status.rs` ApplyNodeConfig and `crates/rdm-client/src/calls.rs:373` config_apply; current request carries revision | `node-config-update.md:24`: completion after Applied or AlreadyApplied; current installs ordered by revision at `:14` | Proposal R-NR5 “written when the node's completion arrives”; R-NR6 permits repeated map writes | Removing revision and recording only the later Build receipt leaves apply → lost reply → local update → repeated apply unresolved |
| F2: update during startup | `node-config-update.md:23` stopped update completes after durable row write | `rg -n 'stopped|durable|Applied' docs/architecture/lifecycles/node-config-update.md`; `:26` next start receives config | Proposal R-NR5 “clears the owed map on the node's ready” | Environment A can be read before acknowledged update B; Ready for A cannot prove B applied |
| F3: source of takeover state | Closed by Q323 (Luke, 2026-11-01): no takeover preparation, no frozen snapshot, no election record; R10 removed |

**F1 disposition:** A is implemented in the property-writer increment: persist config, tags and
`PropertyApplication(build_id, semantic entry operation)` atomically at the exact node; attempts
are excluded. The existing target receipt and workflow receipts remain, but a missing reply cannot
erase durable recognition. Recovery preserves it with the acknowledged properties. No new node
counter is introduced. The combined real-process lost-reply/later-update/recovery/replay proof and
final API documentation are still owed by e4. Durable acceptance of a deferred update is distinct
from node application.

**F2 arms:** A: identify each owed update and clear only the one acknowledged as applied; B: serialize startup with all incoming property commands, retaining their obligations; X: equivalent ordering proof. Recommend A, using existing operation identity where sufficient, not a row revision. e4 must account for config and tag operations, a second update during startup, re-birth and caller cancellation.

**F3:** closed by Q323; no arms remain.

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
| R1/e1 | `scripts/cells.sh rdm-core lib -- path::tests::separate_path_and_name_read_back_as_written` (renamed from `separate_path_and_name_round_trip`, 79008bdaa) | Baseline + test patch; pair serialization/validation fails | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`) | pending |
| R2/e2 | `scripts/cells.sh rdm-test main -- node_records::a_deleted_birth_cannot_hide_its_successor`; by id, `node_records::a_deleted_birth_is_read_by_its_id` | Baseline + patch; public pair lookup selects wrong birth; by id: RED at 29e85b71b (`node.get [id]` answered `[]`) | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`); by id GREEN at 7e354b334 | pending |
| R3,R7/e3,e4 | `scripts/cells.sh rdm-test main -- node_records::a_build_applies_properties_and_a_nodes_later_update_stands` | Baseline + patch; route/property assertion | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`) | pending |
| R6/e3 | `scripts/cells.sh rdm-test main -- node_records::a_regained_primary_waits_for_its_earlier_put` | Delayed put; takeover must not complete early | removed (lead-decisions-2026-11-02 D5; Q322/Q323: no takeover preparation, no writer key; a late line stands until the next write); the cell is not on the lead lane | — |
| R8/e4 | `scripts/cells.sh rdm-test main -- node_records::an_applied_build_update_survives_node_restart` | Apply committed, reply lost, later update; replay must preserve later value | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`) | pending |
| R9/e4 | `scripts/cells.sh rdm-test main -- node_records::a_start_preserves_a_newer_owed_update` | B acknowledged after A handed to launch; Ready must not erase B | GREEN at fd9e14750 (lane 4's fast gate before its landing; with rdm#260's start-declares-Ready, Q324) | pending |
| R10/e3 | `scripts/cells.sh rdm-test main -- node_records::takeover_survives_mesh_primary_restart` | Interrupted takeover and late predecessor put; valid lifecycle reconstructed | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`); R10 removed by Q323: the cell now proves each surviving node's latest line keeps its fields across the first primary's restart | pending |
| R11/e5 | `scripts/cells.sh rdm-test main -- node_records::current_node_reads_survive_fabric_primary_restart` | Fresh FP with retained store resolves all records without gossip | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`) | pending |
| R12,R13/e2,e6 | `scripts/cells.sh rdm-test main -- node_records::all_node_fields_survive_node_restart` | Full field readback, not sampling | GREEN at 8b964c423 (lead lane fast gate, `gates-7-8b964c423.log`) (file backend; R13's application backend is e6.s3) | pending |
| R14/e6 | planned Rafka consumer cell `a_gateways_own_ports_survive_build_resumption`, package `rafka-e2e`, target `node-lifecycle__replace`, run through Rafka's `scripts/run-estate-cells.sh` (e6.s1, `docs/plans/i150-e6-rafka-audit.md`) | Pin baseline and consumer test SHA before executing | pending | no inferred Rafka test target |
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
| planned `rdm.node.record.update.via-already-applied` | e4 durable duplicate recognition | exact target and original operation identity, outcome | Deliberately replay A after B; public B remains |

Final names pass `scripts/name-check.py`. Do not log config maps, secret environment, credentials or unrestricted application data. Preserve causal parentage from public request through persistence and acknowledgement. Background own updates retain their authored trace; do not fabricate a common parent for unrelated operations.

## § Per-increment seed-data obligation

- e1: path/name serde cases, free-text names, empty/invalid grammar decisions, immutable path and predecessor name `<name>-old`, every reference shape and environment pair; explicit historical compatibility/refusal decision.
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
provider-neutral persistence traits with memory/file adapters. The node's own config update now persists locally without admin delivery; caller config/tag commands terminate at the same own writer. Admin property reads and Build property comparisons read that record. Exact lifecycle selection and deletion of all legacy row machinery remain outstanding. A test-only StoreLog append hold exposes cancellation ordering;
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

### Local property routes and startup evidence (2026-10-31)

Ordinary and admin nodes use their shared own writer for downward config/tag commands. Own config
writes no longer traverse the primary chain. The ordinary node's obsolete upward config queue,
reply installer and retry branch have been removed; lifecycle declarations retain their existing
queue. Public reads overlay the exact node-owned record, with identity filters applied before
property I/O. Legacy fixture branches and copied lifecycle/property row structures still need removal.

Launches carry initial config/tags as `RDM_NODE_PROPERTIES`. The deployment pipeline resolves the
newly prepared exact claim, so recovery receives its inherited snapshot and Build creation receives
its entry. Both admin and ordinary startup bind actual endpoint/type/held metadata and persist
before Ready. A same-ID start preserves retained config/tags and apply recognition. The actual
application map survives an own update while stopped and the subsequent rejoin.

Focused evidence at the current implementation checkout (uncommitted when executed):
- `local-admin-property-controls.log`: two own-write/public-read controls passed, 2.69 s.
- `local-route-cleanup.log`: ten storage/writer lifetime controls passed, 0.05 s.
- `initial-properties-process.log`: Build apply/own-update/later-Build and forced recovery cells
  passed, 2/2, 14.57 s.
- `initial-properties-storage.log`: eleven storage/writer/tag controls passed, 0.04 s.
- `initial-properties-launch.log`: six launch environment controls passed, including nonempty
  map/set round-trip and named malformed-input refusal.
- `local-stop-start.log`: strengthened same-birth stop/start application-map control passed, 0.87 s.

Luke clarified during implementation: reuse the existing silence/unanswered-command timers,
node investigation and recovery. Investigation reaches its existing lifecycle result: the node
rejoins, or it is re-created. Storage migration adds no separate investigation verdict or indefinite
unknown-investigation disposition and does not wait on a new proof-of-termination condition.
Q110/Q111 and the shared lifecycle functions remain in force. No investigation or recovery timer
was removed; only the obsolete own-config delivery backoff was deleted with that delivery route.

Architect 1 identified that the existing storage-change block covered primary-issued writes,
whereas direct own writes now require node-level admission freezing before copy. That extension
and its cancellation/migration proof remain BUILD. Existing handle replacement counting alone is
insufficient. No final gate, production Build application identity, lifecycle activation, pair
migration or Rafka acceptance is claimed by these focused passes.

### Storage block and existing investigation integration (2026-10-31)

Node-owned property writes now participate in the existing storage change. `StorageBlock` is an
appended NodeCommand request, with `StorageBlocked` as its completion. It retains the own writer's
existing serialization lock through copy and the exact change's move or rollback. No additional
queue, retry, timer or investigation verdict was introduced. The existing `StorageMoves` retains
freeze/move transitions through caller cancellation, recognizes completed Build/change identities
and destinations, refuses conflicting repeats, and admits a fresh birth already on the decided
provider. Same-provider rollback releases its matching hold and invalidates cached properties.

Both storage commands use `CommandView`'s existing unanswered-command clock, including cancellation
at the original send instant. A failed freeze hands the attempt back before closing normal command
admission, so the existing investigation/recovery can claim the mesh. Once every required freeze
answers, the existing mesh command/row block completes. `build_drive::world` now includes the mesh
of BlockStorage and UnblockStorage, so its existing changed-world wait resumes after recovery.

Focused evidence at the working checkout (not a final committed gate):
- `stopped-node-storage-command-fixed.log`: the actual stopped process answers the storage command
  on its open Node RPC endpoint, holds an own write, releases it after rollback, and preserves it
  through same-birth start; 1/1 passed, 0.86 s. An earlier compile failed on a test-only dependency
  name; the test now uses the crate's existing futures-lite dependency.
- `storage-command-clock.log`: 4/4 command clock/cancellation controls passed.
- `storage-freeze-replay.log`: 2/2 retained-transition and exact-change controls passed.
- `storage-block-normal-investigation-exact.log`: paused before migration; normal investigation and
  recovery re-create it before the held copy is entered; inherited acknowledged properties remain;
  the same storage Build completes on the destination; thawing the deleted birth cannot make it
  current. 1/1 passed, 42.80 s. The first command used an incorrect module prefix and discovered zero
  tests; only the corrected nonzero exact run counts.
- `storage-freeze-recreate.log`: existing refused-move recreation control passed, 12.46 s.
- `storage-freeze-rollback.log`: existing failed-copy rollback control passed, 12.03 s.
- `storage-block-wire-fixed.log`: 5/5 wire/code controls passed. The preceding run exposed the omitted
  independent expected-code fixture for the appended success reply; its RED log is retained.
- `storage-freeze-role-restart.log`: existing switched and failed-copy physical restart cases
  passed in the combined control, 85.29 s. This restarts the RPC role node, not the mesh-primary;
  primary takeover coverage remains pending.
- `launch-env-wire-boundary.log`: both wire-JSON negative controls passed, 14.97 s.
- `storage-wire-ratchet-fixed.log`: planted wire-count violation control passed, 5.43 s. Earlier audit
  found test-only tag assertions in the runtime scan and JSON environment encoding classified as
  wire encoding. Assertions now compare the whole tag set; Launch is correctly classified as an
  environment-only core type, alongside the existing custody codec. RPC codecs stay postcard.

Retained process evidence is under the storage feature's normal artifact directory, with raw spans
and node logs. Trace `a100cd1cd47f9122d79a1283f77baf58` was recorded locally; collector retrieval did
not find it, so no retrieved Jaeger proof is claimed for that run. The final committed gate must
export and retrieve its flame.

Role acquisition remains a separate BUILD requirement: a newly acquired primary must restore an
in-progress block from existing Build receipts before enabling lifecycle writers. Architect 1
identified the existing hierarchy acquisition and explicit handover preparation boundary as the
shared integration point. No new persisted counter/state machine is needed. This focused evidence
does not close i150, its takeover requirement, or its Rafka integration.

### Build entry execution through the shared writer (2026-10-31)

The mesh-primary reads the immutable full entry from the shared Build row. Config and tag diff
steps share `PropertyApplication::entry` and one exact target pinned by an existing typed Build
receipt before the request. The pin survives a failed child step and same-attempt retries. An
unapplied ambiguous request cannot be redirected to a new occupant of the path. The node persists
config, tags and application recognition in one effect through `NodePropertyWriter`.

The existing claim contains the immutable launch snapshot, including application evidence.
Create/replacement snapshots mark their entry; recovery captures the predecessor's current map,
tags and existing evidence from the same read. Repeating a claim hands on that original snapshot.
The node initializes its first record from it before Ready. Recognition suppresses config/tag replay
in the planner; storage and placement differences remain owed. Metadata completion on a recovered
birth requires its inherited durable application evidence and never resends the old property call.
No new retry queue, timer or separate store is used.

Working-checkout evidence (`target/evidence/i150/`, not a final gate):
- `entry-properties-writer-final.log`: 12/12 passed, 0.05 s; lost acknowledgement, later local
  config/tags, writer recreation and a following Build's empty-map/empty-tags override.
- `entry-properties-recovery-claim-fixed.log`: 11/11 passed; recovered application planning and
  immutable recovery claim snapshot included. The first claim test omitted its fixture's path index;
  the fixture now uses the same `write_record` operation as existing claim fixtures.
- `entry-properties-node-endpoint.log`: ordinary node's actual Node RPC endpoint, complete entry
  apply, later own map/tag update, repeat recognition; 1/1 passed, 0.05 s.
- `entry-properties-target-pin.log`: existing create/pipeline cell includes same-attempt pin reuse,
  failed child-step reuse and refusal of a different occupant; 1/1 passed, 0.01 s.
- `entry-properties-metadata-refusal.log`: committed application evidence, refused lifecycle write,
  repeat finishes metadata with no resolvable node endpoint and preserves later properties;
  1/1 passed, 0.02 s.
- `entry-properties-claims-fixed.log`: 32/32 claim/recovery controls passed, 0.02 s. The deliberately
  fabricated lost-executor claim now includes the launch snapshot a real claim writes.
- `entry-properties-process.log`: existing real-process Build override/own-write and restart controls,
  2/2 passed, 14.60 s. This is not the missing combined process reply-loss/recovery proof.
- `entry-properties-wire.log`: 7/7 frozen Status controls passed; request position 37 is appended.
- `entry-properties-launch.log`: 6/6 launch environment controls passed, including application
  evidence round-trip.

Architect 1 inspected the replay/metadata distinction and found no further concrete flaw in that
delta; the combined process proofs remain required. A broader admin-unit run passed 620/623 and
found one missing-birth classification regression (fixed to existing `Incomplete`) plus two old
workflow fixtures expecting the removed own-config callback/up-chain behavior. Those two remain
owned by i150.e3.s4's deletion of the legacy routes/fixtures; they must pass before landing.
The complete initiative, final gate, lifecycle takeover and Rafka integration remain BUILD.

### Exact lifecycle subject lookup (2026-10-31)

`NodeRecordsStorage::lifecycles_of(node.id)` returns the exact subject's writer records without
inferring a mesh from its path. Provider selection remains persistence-only: the domain reader must
resolve the explicit MeshId through the mesh record/name claim and choose the installed primary's
exact birth. The file adapter uses `StoreLog::latest_under` over the existing ordered fold; it adds
no persisted index or copied view. `latest_of` shares that range implementation with an empty prefix.
The ordinary and admin provider handles forward this read through their existing current-provider
handle. Lifecycle activation and installed-primary selection remain BUILD.

`lifecycle-subject-range.log`: 12/12 node-record controls passed, 0.05 s. The late-writer contract
also checks exact-subject enumeration with an unrelated subject/mesh present and absence for a
missing ID, on file and memory backends. Architect 1 reviewed this read shape against R-NR2.
The name check found an earlier new test identifier containing the retired word `round`; its name
now says `encode_decode`. Final branch name check and the landing gate remain required.

### Explicit declared node type (2026-10-31)

`Node` stores its declared `node_type` separately from its path/name and runtime `kind`.
`with_node_type` persists the normalized declaration instead of only validating it; its getter
and immutable-update refusal read that field. `NodeLifecycle` distinguishes runtime `kind` from
the declared type string, so its schema can preserve a custom declaration. The existing atlas
conversion initializes the field until its lifecycle replacement removes that copied-row path.
The full explicit mesh/kind launch and gossip plumbing and free pair cut remain e1 BUILD.

`node-type-explicit-field-fixed.log`: the custom type survives a path change, serialization and an
attempted type update; 1/1 passed. `explicit-node-type-records.log`: 12/12 storage controls passed.
`explicit-node-type-create.log`: the existing free-text create/stop/start/recovery/replace control
passed. The first compile identified the one exhaustive atlas constructor missing the new field;
it now initializes it. This does not close the complete path/type migration.

### Explicit Build entry ownership (2026-10-31)

`NodeSpec` requires its owning `mesh` and runtime `kind`; counts expansion supplies them, and
`StorageMeta::for_kind` owns the kind defaults. The accepted topology, complete-topology request,
private Build wire spec and testkit request builder carry the same fields. Acceptance groups and
validates by the explicit mesh, rejects duplicate pairs across meshes, counts by explicit kind,
and removes actual members on shrink. Membership and executable lookup use accepted entries.
A stored entry missing explicit ownership is refused by its missing field; no path inference
fills that omission. The Build wire has independent frozen bytes for the new shape.

Architect 1 found and reviewed the remaining path-derived validation and reconstructed-name
shrink; both are fixed. `explicit-spec-ownership-fixtures.log`: 2/2 core metadata controls;
`explicit-spec-wire.log`: 10/10 Build wire controls; `explicit-spec-accepted-complete.log`:
40/40 acceptance controls; `explicit-spec-gossip-wire.log`: 3/3 Build/gossip codec controls.
`explicit-spec-process-selected.log`: real-process named create/Build override control passed,
1/1 in 10.59 s. The first process selection matched zero tests; it is not counted as proof.
The misleading-path control covers explicit mesh/kind, membership, executable lookup, actual
shrink, duplicate refusal and kind-change classification. This is an e1 substrate increment:
launch, gossip, lifecycle operations, roster and pin ownership, section routing, and the path/name
pair format remain BUILD. In particular, deleted/renamed subjects need the existing exact-birth
pin to retain explicit mesh ownership; routing must never reconstruct it from their path.

### Explicit launch and create-step ownership (2026-10-31)

`CreateRequest` and `Launch` carry mesh and runtime kind explicitly; `RDM_MESH` and
`RDM_NODE_KIND` preserve them independently of name spelling. Ordinary startup and same-process
start use the declared kind. Admin startup refuses another kind. Executable validation, listener
selection, declared-secret selection and first-admin handoff use the supplied ownership.

A topology create reads the accepted entry from the shared Build. A primitive create carries
explicit ownership in `Step::Create`; its executor reads the immutable action recorded for the
invoked attempt through the existing lane projection. It never substitutes a topology entry for
that step. Recovery uses the exact predecessor. A create redirected over an unreached predecessor
retains its existing request and claim disposition through the inherited/fresh-directory decision;
true recovery retains predecessor properties and application knowledge. The runner reuses the
existing `create_with_claim` pipeline rather than maintaining a second creation implementation.

Architect 1 reviewed the operation/attempt/lane selection and redirect. Evidence:
`explicit-launch-step-ownership.log`: 6/6 launch codecs; `explicit-launch-step-wire.log`: 10/10
Build wire contracts; `explicit-launch-admin-kind.log`: launched-admin kind refusal, 1/1;
`explicit-launch-provider.log`: provider receives explicit ownership despite a misleading name,
1/1; `explicit-launch-step-core.log`: core Build controls. Real-process controls:
`explicit-launch-create-process.log`, normal Build create, 1/1 in 10.63 s;
`explicit-launch-steps-process.log`, primitive replacement, 1/1 in 9.73 s;
`explicit-launch-unlisted-step-recovery.log`, a create outside accepted topology followed by total
fabric loss/recovery, 1/1 in 32.79 s. Initial compilation found a shadowed recovery boolean, and the
first launch test run found a manual environment fixture missing the now-required fields; both are
fixed. Neither failed run is counted as behavioral RED evidence.

The e1 end-state remains BUILD. Gossip/private digest wire, lifecycle operations, roster/pin
ownership, section/operation routing, the free path/name pair and consumer migration are pending.
The final proof still needs conflicting step/entry ownership across immutable attempts and equal
attempt numbers in different lanes, plus requested create semantics on both recovery directory
arms. Existing recovery-marker controls remain evidence for inheritance, not a substitute for
that complete process matrix.

### Explicit digest ownership and domain projection (2026-10-31)

`MeshNode` and its private positional `WireNode` carry explicit mesh and runtime kind. Both codec
conversions preserve them. Ordinary/admin boot supplies launch/config ownership, and digest-backed
projection restores it for live, stopped-digest and projected-unheard births. Mesh-ID indexing,
recorded-primary capture/readback, declarations and the entry hook read the explicit owner. A name
that suggests another mesh or kind is no topology violation; an actual unknown mesh still is.

Architect 1 reviewed both wire conversions and found the remaining primary/declaration aliases and
three domain conversions; those are fixed. The new `digest_projection_preserves_explicit_ownership_and_primary_selection`
control passes a deliberately conflicting name through the actual digest codec and projection,
checks stored primary birth/revision in both directions, and verifies MeshId/type selection.
`explicit-digest-projection-reviewed.log`: 1/1; `explicit-digest-topology-wire.log`: 6/6 frozen topology
wire controls; `explicit-digest-gossip-wire.log`: 3/3 codec controls; `explicit-digest-topology.log`:
8/8 topology controls. `explicit-digest-create-process.log`: real-process create/Build control,
1/1 in 12.94 s, with OTLP export enabled.

Retrieved and read [the create flame](http://192.168.68.99:16687/trace/f4eff64acd2b82b73070cd8d4d5314a2),
153 spans, saved as `target/evidence/i150/explicit-digest-create-trace.json`. Its RPC-node pipeline
records WaitForBind complete at 68.320 ms, WaitForMeshJoin complete, and WaitForNodeReady complete at
128.012 ms; the node-admin creation completes its corresponding steps. This is current increment
control evidence, not the final-tip free-pair/recovery acceptance flame.

LifecycleOp-only stopped births, Deployed verification, roster/pin ownership, legacy Atlas and
record lookup, free-pair serialization and consumer migration remain BUILD. Path-based fixture
conveniences still exist until that coordinated pair cut; no final compatibility layer is accepted.
Initial compiles caught integration-test crate paths, one shorthand-field fixture edit and one
non-digest branch receiving digest assignments; these compiler errors are fixed, not behavioral REDs.

### Join and lifecycle ownership evidence (2026-10-31)

Deployment registration carries the explicit mesh and kind, and join validates both against the
report. Lifecycle operations carry the subject mesh through their existing carriers; readers
route by that field. Same-birth rename does not change it. Certificate issuance and verification
check the explicit mesh, and admin signing dates follow the verified kind. Architect 1 found the
two remaining name-derived certificate checks; both are corrected. The join cell exercises a
misleading name and refuses wrong mesh/kind; its actual JoinDoor proof issues a certificate and
records signing dates for an admin with an RPC-looking name in another apparent mesh.

Proofs: `explicit-join-certs-reviewed.log` 9/9; `explicit-cert-owner.log` 23/23;
`explicit-lifecycle-wire.log` 3/3 independent positional fixtures;
`explicit-lifecycle-topology-wire.log` 6/6. These are ownership increment controls, not
end-state lifecycle activation or pair-reference acceptance. The LC-only stopped projection
uses its operation's mesh; its kind still awaits the lifecycle reader.

Luke's completion deadline, received during implementation, is 2026-10-31 20:11 UTC
(one hour from the 19:11 UTC clock observation). Final acceptance and landing remain owed.

### Shared property writer implementation evidence (2026-10-31)

The node runtime now has one `NodePropertyWriter` shared by ordinary own-config and caller config
application. Its admitted task owns serialization, provider admission, the actual put and local
publication through caller cancellation. Property records carry the applied Build/operation set
with the effect; attempts do not enter duplicate recognition. Reopening the file proves a repeated
application preserves a later own update, while a different Build can still override it. Production
Build commands carry the same typed entry identity through `ApplyNodeProperties`; e4 remains BUILD
until its process loss/recovery and startup obligation proofs are complete.

The new record trait requires every returned put, including a failed one, to have finished its
backend operation: it cannot still commit later. An error may conceal an already committed effect;
the runtime invalidates its cached snapshot and resolves the exact record before another mutation.
The file and memory adapters meet this contract. Rafka's current bounded writer does not establish
it; e6 must retain and resolve the original operation, and cannot treat a current-value miss as
proof that it never committed. No new helper retry or delivery queue is introduced. The existing
runtime admits commands; this writer executes one at a time, without another deadline or retry.

Both record kinds now enter provider copy inventory, including retained application identities.
The admin handle exposes the same records through provider migration. Node startup refreshes bound
addresses and declarations while preserving acknowledged config, tags and apply recognition.
Focused storage evidence: ten node-record tests passed (0.04 s), including provider migration held
behind a cancelled caller's actual append, four storage-copy controls passed
(0.01 s), seven admin backend controls passed (0.50 s). The existing own-config controls passed
(2.69 s) and the real-process recovery canary passed (14.52 s) before startup binding was added;
those controls must run again after the coordinated route change.

The in-place Build seed reached behavioral RED in 9.09 s: Build A applied config/tags, an own update
stood after reattaching A, and Build B overrode the existing node. It then failed the assertion that
own updates produce no upward config delivery. Retained log: `target/evidence/i150/build-seed-red.log`.
The replacement portion and lost-reply runtime proof remain required. Removing the old route,
activating current record reads and tag writes, lifecycle takeover, the reference migration, Rafka
integration and final acceptance are still BUILD. This evidence is not full-initiative completion.

### Latest correction: immutable path, replacement name

Luke corrected the earlier replacement requirement: a node is tied to its path and the path
cannot change. Replacement renames the predecessor's **name** to `<name>-old` at the same path;
the successor claims that path with the original name. Earlier `.old` path-move statements and
proofs in this work log are superseded. The separate-property and globally unique ID-first
requirements below remain in force. Replacement provenance comes from the admitted Build,
never merely from an ordinary name ending in `-old`.

The implementation now rejects changed paths at node apply, stored-row rename and Build-step
admission, and rejects invalid generated rename targets before a Build executes any step. Added
regressions cover those boundaries and ordinary `-old` names without replacement provenance.
Architect 1 reviewed this correction. The latest source batch persists explicit replacement/rollback intent by original step position and passes it through the existing rename writer; ordinary `-old` names preserve their ordinary meaning. Receipt replay now reads that admitted intent. Review found completion-key mismatch and stale rename replay: the shared admitted-step key is now used by planning and completion recognition; replay ordering remains under review. These changes and new regressions are uncompiled. The architecture text changes remain
in the proposal's exact-text review scope. The focused cell attempt failed compiling existing
joined-string callers after removal of `PathName`'s parser/formatter; no tests ran and these new
regressions are unverified (`target/evidence/i150/immutable-path-check.log`).

The subsequent `node-references-check-3.log` run compiled core, transport, client and node libraries,
then failed node test fixtures and admin. No tests executed. After those diagnostics, the source
batch added ID-based accepting-primary and redirect references, typed scoped handover replay payloads,
separate NameHeld path/name fields, and persisted rename intent through the existing writer.
`admitted_step_key` now supplies execution and completion recognition consistently. Added regressions
remain unexecuted. Architect 1 identified a remaining correctness blocker: Atlas replay of an old
rename can overwrite a newer name after restart/ABA; in-memory ordering alone does not resolve it.
Current-row read/publication or durable applied rename evidence must replace that receipt-derived
name mutation. Final compilation, test execution and initiative acceptance remain owed.

The next source batch changes standalone action planning to exact NodeId reads, reuses one atlas
row projection, and keeps accepted operations as typed subjects/identities and created locations as
structured pairs. Lifecycle receipt readers no longer split operation strings; effect ordering
retains original step position before attempt. Architect 1 found stale accepted entries, omitted
exact-birth lookups, and stale-writer folding bypassed by the ID route; source fixes and regressions
were added. Further review found payload incarnation validation must precede receipt acceptance;
that correction remains in progress. `atlas-identity-check.log` is the current compilation attempt,
not test evidence. Rename replay ABA remains open; no acceptance status advances.

The completed `atlas-identity-check.log` compile failed admin (3262 library / 3817 test-target
errors); no tests executed. Further edits validate carried lifecycle evidence through the pipeline's
existing retry boundary, with explicit Build/lane scope and admission pins taking precedence over
payloads. The committed-receipt index remains an assertion extractor, not a provenance validator.
Creates now carry an exact admitted predecessor birth through request and durable allocation.
Seeding, executable lookup and marking read that ID first, preserving predecessor identity across
successor ownership changes. Fallback discovery uses explicit replacement provenance, excludes
deleted rows and refuses ambiguity; names do not select a predecessor. Architect 1's final source
review and `predecessor-identity-check.log` compilation are pending. These are source changes and
unexecuted regressions, not passing acceptance evidence. Rename replay ABA remains open.

`predecessor-identity-check.log` then failed admin (3179 library / 3752 test-target errors);
no tests ran. Architect 1 found successor node-type copying and a cross-mesh writer violation.
The source now removes the successor type copy and refuses cross-mesh replacement at admission,
with writer-side guards. Same-mesh replacements may change kind while carrying the exact
predecessor ID. Receipt-index slots are typed composites, and 263 diagnostic locations now expose
separate path/name fields. Those changes are under compilation in `structured-diagnostics-check.log`;
regressions and final review remain outstanding. No acceptance state advances.

`structured-diagnostics-check.log` failed admin with 2013 library / 2580 test-target errors,
without test execution. Architect 1 then accepted source corrections selecting Create ownership by
original step position and validating retained predecessor state inside the existing writer lock.
Additional planner operation comparisons and node-row diagnostics/fixtures are migrated; recovery
alone may inherit a predecessor's explicit node type. The active `node-row-diagnostics-check.log`
compile has not supplied passing evidence. Rename replay ABA and the broader initiative remain open.

### Explicit correction: separate properties and ID-first code references

Luke clarified during CLI continuation that `path` is where the node is in the tree and `name`
is the node's name. They are two properties, never a joined `path.name` identifier, display
format, serialized string or string to split. This applies to storage, wire objects, launch
inputs, references, queries and diagnostic fields. A structured argument may contain both.
Names are unique within a path, so a composite `(path, name)` lookup index and name claims remain
valid. `node.id` is globally unique and is the first choice for code references and lookups;
a caller-supplied location is resolved through the composite index, then operations carry the ID.
Neither property supplies mesh, type, ordinal or identity. The latest correction above governs
replacement: immutable path, predecessor name `<name>-old`, successor original name.

Architect 1 audited the correction and found joined assumptions in PathName FromStr/Display,
public `name: PathName` carriers, storage index/claim keys, operation strings, receipt parsing,
transport observer comparisons and diagnostic fields. The joined parser and formatter are now
removed; the connected caller/schema migration is in progress and currently uncompilable. Earlier
pair round-trip evidence allowed joined parsing/formatting and does not prove this corrected rule.
The lifecycle election-output storage additions are also uncompiled and not yet activated.

### CLI continuation — 2026-10-31 20:28 UTC, uncommitted and not accepted

The pair-only PathName migration now threads explicit mesh/kind through node operations,
immutable attempt actions, pinned births, runtime records and resolved deployment launches.
Replacement deletion retains the predecessor's kind/mesh; creation uses the new entry. Claims
retain both the executor's owning mesh and the actual in-flight mesh scope. Replay reads the
committed claim, refuses a different executor, and compares exact scope metadata before taking
another in-flight row. Cleanup releases the winning claim's actual scope even when its mesh is
absent from the current view. These changes remain uncommitted and unproved at runtime.

Architect 1 reviewed replacement routing, exact claim scope, replay and the resolved launch
metadata. Its latest source review found no additional critical defect in those focused paths;
it explicitly withheld runtime and landing approval. Compiler-driven fixture migration continues:
`target/evidence/i150/pair-build-20.log` records a failed workspace build with 123 distinct compiler
errors (not behavioral RED evidence). Later builds passed those compiler stages and exposed
integration-fixture failures: build 23 reported 157 admin integration errors and three testkit unit
errors. Builds 24–27 exposed and corrected further API and fixture errors. Build 28 completed the
whole-workspace compile and passed the one pair round-trip test (`pair-build-28.log`); it is not a
suite or gate result. Its optional ledger append was sandbox-refused after the test passed. The next
run obtained filesystem approval to write that ledger. No prior-commit gate applies to this checkout.

The next connected source batch removes the upward own-config route, pending push map, `Hop::Push`,
and completion-side config installation. Wire position 34 retains its payload as `Reserved34` and
is refused. Config commands require the bound node-owned property writer and still contact stopped
processes; there is no unbound combined-row fallback. Missing lifecycle metadata after a property
apply is a named failure. New bound-writer conformance replaces obsolete copied-row/push assertions.
Architect 1 reviewed the design and applied source with no further concrete defect. The full
workspace build and both focused property-writer tests passed in `property-route-cut.log` (2/2).
This is focused evidence, not full acceptance. A further source audit found old-looking path seeding
without replacement provenance; that seed and retained-row marking now require the exact recorded
origin. The full workspace build and all four node-row tests pass in `pair-provenance-tests.log`,
including the negative fixture and recovery snapshot preservation. Broader core tests reached
154 passed / 3 failed on obsolete string-name fixtures; those fixture encodings are being updated.
The broader admin library run is still in progress and has failures to diagnose.

All epics remain BUILD. Lifecycle writer activation and election-install ordering, current-reader
conversion and removal of copied rows, startup A/B obligations, combined process-fault proofs,
Rafka provider/pin integration, exact architecture-text approval and final gates remain owed.
The requested 20:11 UTC deadline was missed. Full closure has not been established; implementation
continues without claiming a passing gate, landing or issue closure.

The subsequent semantic review found and corrected two additional gaps. Replacement provenance
now lives in a distinct `replacement_from` pair on the exact subject record, independent of the
latest claim-release source. Name-only edits preserve it; exact rollback clears it. Path changes
are admitted only by the pinned Build runner through the same validator used for external names.
A second move first finishes any earlier pending name release under the existing node lock. The
final-admin generator again chooses above all exactly matching generated pairs with checked
exhaustion. Architect 1 reviewed these source changes; their regression tests have not run.
`NodeLifecycle` now has the same provenance field for the pending activation cut.

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
  - **i150.e1.s3 — BUILD:** Keep path immutable; rename predecessor name to `<name>-old` at that path; refuse path changes and preserve claim ownership and exact identity across interrupted renames.
- **Spans:** assert the applicable production/control spans in the OTLP table; add the exact emit-site mapping and mandatory attributes before GREEN. e0 registers capture, e7 verifies retrieved traces.
- **Testing by tier:** unit/functional rules first where applicable, then the registered real-process cells; canary RED precedes seed RED. e7 covers load/chaos. No zero-test or ignored acceptance.
- **Acceptance tests by name:** path::tests::separate_path_and_name_round_trip; node_records::a_predecessor_rename_preserves_path_and_identity (planned); node_records::a_rename_refuses_a_different_path (planned).
- **Estate plan:** Feature estate, one predecessor is renamed at its unchanged path then replaced; read back path and name separately.
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
  - **i150.e3.s2 — BUILD (as Q323 rewrote it):** `NodeLifecycle` keyed by `node_id` alone, no `writer` in the key and no author field; `current_record.rs` resolver removed; `lifecycle_record` loses its writer argument; `views.rs`, `atlas.rs`, `node_rows.rs` read the latest line at the node id; "missing current-writer record" answers become "no record yet"; `durable format moved` in the commit; the `node_records` cells follow. No takeover preparation, no election record.
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

### e6 — Rafka on the RDM tip (after i151; the i141 lead, in rafka-v2)

Order (Luke, 2026-11-02): this epic runs **after i151 lands**. Rafka's pin moves whenever a landed RDM
SHA is worth taking (Q336 A, 2026-11-02: at Q331's SHA, then at the i151 tip), each a pin bump and a
rebirth; "moves once" was the e6.s1 audit's preference, never Luke's. Source of every
file named below: [the e6.s1 audit](i150-e6-rafka-audit.md) (a source read; the compile at the bumped
pin is the complete inventory). One Rafka owner per story, in sequence: they share
`node-admin/src/storage.rs` and `day0_storage.rs`.

#### i150.e6.s2 — one pin, the compile, the pair and the own write
- **Change:** `Cargo.toml` and `Cargo.lock`: the six RDM crates (`rdm-core`, `rdm-transport`, `rdm-client`, `rdm-node`, `rdm-admin`, `rdm-test`) to one `rev`, the RDM `origin/main` SHA after i151's last landing; the pin line in `docs/plans/i141-node-rpc-substrate-prd.md`; `durable format moved` in the commit (`ops.nodes` rows and the `PathName` object change) and a fresh estate. Then the compiler's list, fixed site by site with no new behaviour: the separate `PathName { path, name }` with explicit mesh and kind (audit §2.5: `rafka-node-base` `rdm_topology.rs`, `rdm_node.rs`, `topology.rs`, `launch_env.rs`; `node-admin` `front_door.rs`, `build_call.rs`, `start.rs`, `system_image.rs`, `day0.rs`, `wiring.rs`; `mesh-node/src/main.rs`; `compute`; the `Launcher` and `Launch` literals; `NodeSpec::default_for`); `ConfigUpdated` and `ApplyNodeConfig` without `revision` (§2.1, `rdm_node.rs` `update_own_config`); `OwnRow` without `revision`, `JoinControl` without `config` (§2.2); `NodeView.properties` rendered by name in `front_door.rs` `node_row` and `mesh_nodes_filter` (§2.3); the node's own config as a local durable put with no upward call (§2.4, `boot_hydration.rs`, `api_base.rs`, `listen_ports.rs`); a peer gateway's endpoints read from its record at its node id, a record not yet written excluding the peer by name (`rdm_node.rs` `peer_config`, `peer_endpoints.rs` `resolve_one`); `rdm_client` `Tracked` and `overlay` on `OperationId` (§2.9).
- **i151's part, same story:** every `Budget::` site (45: `write_hooks.rs`, `broker_data_op.rs`, `writer_target_op.rs`, `topic_traffic_close_op.rs`, `broker_resync_step_op.rs`, `context.rs`, `dial_bound.rs` and the single-site files) to `Resend`, `Overall(d)` → `TimeLimit(d)`, the ten `once: true` sites choosing their own `attempts`, each named with its reason; the writer's leg policy kept, driven by `on_call_failed`'s reason: `NotSent` asks the next replica, `Indeterminate` goes through the writer's own idempotency rule; node-admin implements `on_node_investigated` and on `recreate` writes the broker's `write -> none` rows (replica-routing.md §6 step 1) before the new birth's re-sync.
- **Acceptance:** the workspace compiles at the one SHA; `rafka-node-base`, `rafka-node-admin` and `rafka-gateway-core` lib and functional cells green; `node-lifecycle__replace` `a_gateways_own_ports_survive_build_resumption` green on `multi-node`, process then container, its Jaeger trace showing the successor's own `rdm.node.record.update.via-properties` as the Build's last property write and no `rdm.node_admin.config.update.via-call`; the pin SHA recorded in the i141 record.

#### i150.e6.s3 — the application provider's node records and the Day-0 copy
- **Change:** `node-admin/src/storage.rs`: `RafkaStorageBackend::open` supplies `StorageBoundaries.node_records`, a `NodeRecordsStorage` over a Rafka plane keyed by node id as Q323 left it (one record per node, latest line current), whole records stored and returned as given; `RafkaNodesStorage` and `Forward<dyn NodesStorage>` lose the four index methods (`node_at`, `index_path`, `index_tags`, `tagged`); `crates/rafka-entity-topology/src/{record,store}.rs` carry the record kind. `rdm_node.rs` `prepare_node_process` and `node-admin/src/start.rs` `providers`: the launched node registers its `NodeStorageBackend` on the same plane under the provider name the launch carries, so a gateway's property write lands where its admin and peers read (audit §2.8: otherwise the admin answers the gateway's properties unavailable, storage.md §2's per-process case). `day0_storage.rs`: the Day-0 switch copies `node-records.jsonl` whole through RDM `storage_copy::NodeRecords`, no path or tags index copy.
- **Acceptance:** `rafka-node-admin` `functional` storage backend cells and `node-admin/tests/day0_file_then_topics.rs` green; a gateway's own config read back through `node.get` on the topics provider; the i149 migration regression on a real gateway green; the consumer cells registered in RDM's `tools/mesh-audit/i143-acceptance-jobs.json` with the lead.

Rafka's cells prove only that its backend stores and returns RDM's records whole (i141 U3.2); the R13 RDM-side cell `node_records::all_node_fields_survive_node_restart` stays RDM's.

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

**Format changes:** separate path/name references and split node/lifecycle storage are incompatible unless explicitly implemented otherwise. Use the repository's `durable format moved` marker and wire marker requirements; derive wire golden expectations independently from the declared encoding. e0 specifies the fresh-estate/coordinated consumer-pin procedure and named rejection of unsupported stored layouts. No mixed-version correctness is claimed. Fresh-estate deployment tests do not replace retained-format restart tests within the new version.

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

### Startup correction: one final topology fetch — BUILD

Luke's direction: after the hook, satisfy the required Build-facts floor, then fetch topology
once at the end. Install complete topology and adopt current Rafka-time from that same reply
before reporting started. No preliminary topology fetch and no second topology call solely for
time. Preserve existing startup deadlines and the shared workflow across node kinds.

The current `rdm-node/src/workflows/node_start.rs::AfterTheHook::steps` still fetches topology
before `after.floor`, then calls `read_topology` again for time. The CLI must reorder the shared
workflow and audit floor/readiness dependencies on early topology installation. Proof owed:
startup trace shows the Build-floor check before exactly one topology request, that reply supplies
time, and no Started/Ready occurs before both complete. Existing gap repair remains separate.


Current compilation status: `node-row-diagnostics-check.log` failed admin with 1767 library /
2322 test-target errors; no tests ran. The next batch uses structured locations in BuildReject,
including NodeSpecConflict carrying indexed location, declared location and explicit mesh. Status
span helpers take PathName explicitly. Added serialization regression is unexecuted; the batch is
compiling in `rejection-location-check.log`. Architect 1 reviews the changes and remaining
claimant/executor references. Atlas rename replay ABA and the full initiative obligations above
remain open.


`rejection-location-check.log` failed admin with 1729 library / 2282 test-target errors; no tests
ran. The connected claim/dispatch source migration now uses exact executor Birth, claimant NodeId,
ID-based future handoff, and exact runtime departure evidence. Local dispatch and run admission
compare full Birth. Fabric/drift recovery uses recorded claim scope and executor departure, with
claimant identity retained for row release. Many callers/fixtures remain unmigrated; the current
`claim-birth-check.log` attempt and Architect 1 review are not passing evidence.


`claim-birth-check-4.log` reached admin and failed with 1690 library / 2300 test-target errors;
no tests ran. Driver changes now read the winning committed Birth after either Won or Lost,
without treating an in-flight claimant as an executor; section dispatch uses the same evidence.
Own synthetic claims use boot-established identity. Source review and `claim-driver-check.log`
are active; recovery callers, older driver paths, fixtures and full initiative proof remain owed.

### Primary reference correction (2026-10-31, unverified)

The operation follow-up compilation failed with 1565 admin library / 2183 test-target errors; no
tests ran. Primary-record destinations, decision gates, role-yielded replies and recovery executor
selection now carry NodeId directly. Primary selection no longer needs a digest to recover a name.
The first build of this batch stopped on two core wire fixtures; they are migrated. Architect 1
is reviewing identity and incarnation handling. Caller migration and all prior initiative obligations
remain open; this is not passing evidence. Approved architecture packet remains unchanged.

Architect 1 found same-ID/new-incarnation permission gaps in the ID-only primary transition and
recovery checks. These now retain exact Birth for permission; future handoff still uses NodeId.
Recovery checks compare established local incarnation, including WrittenRows.writer for row/status
permission. A same-ID/new-incarnation gate regression is added but unexecuted. Last completed admin
build (`primary-id-check-2.log`) failed 1553 library / 2177 test-target errors. Exact-birth follow-up
`primary-birth-check.log` is compiling; no passing test evidence or closure claim.

### Runtime identity follow-up (2026-10-31, unverified)

Architect 1 source review accepted exact-birth permission and committed delete-claim takeover changes.
The latter releases only the recorded claimant's matching row in the recorded scope; its test now uses
different claimant and executor IDs. Last build failed 1545 library / 2164 test-target errors; no tests ran.
Default physical data directories now share a NodeId-based helper across launch, adoption and fallback
reads. Explicit persisted/inherited directories retain precedence. Lifecycle transition receipt identity
now takes typed NodeId and incarnation. Existing tests updated; runtime compilation/review pending.
The durable-format rollout and architecture correction packet remain outstanding; no compatibility
or passing-test claim is made for these uncommitted changes.

### Exact runtime review correction (2026-10-31, unverified)

Architect 1 found and source-reviewed fixes for runtime lookup: claimed launches honor exact durable
directory evidence and explicit node directories, digest directories require matching incarnation,
and absent births are read by admitted NodeId even after rename. A retained birth now uses its exact
durable RuntimeRow.runtime; disk fallback requires an allocation for its incarnation and matching
deployment. The inherited-directory regression refuses newer runtime evidence then verifies the
old durable runtime is used. No tests ran: latest compilation failed 1485 library / 2095 test-target
errors; `runtime-proof-check.log` is active. All previously recorded initiative obligations remain.

`runtime-proof-check.log` completed with 1485 admin library / 2095 test-target errors; no tests ran.
Architect 1 source review found the exact-runtime correction addresses its finding. Full diagnostics
are saved in `target/evidence/i150/runtime-proof-diagnostics.jsonl`. `git diff --check` passes. No build
is active. Implementation and documentation audit remain incomplete; none of the final gates ran.

### Recovery admission continuation (2026-10-31)

Luke explicitly instructed continuous work until completion; persistent goal is active. ControlPlane
now binds the boot incarnation, checks its exact local birth, selects recovery destinations by NodeId,
and freezes executor Birth and mesh from the selection snapshot before admission mutations. Architect 1
identified the earlier cross-snapshot bug and reviewed this fix. Orphan claim release uses matching
committed claimant/scope and exact executor departure proof. AttemptOpened/WireOpened.opened_by now
carry NodeId; production opener callers and fixtures are still being migrated.

A diagnostic helper accidentally replaced the control-plane test suffix; the immediate compile caught
the syntax error. The section is restored from committed tests with pre-edit compiler source evidence
and explicit-field migration. All ten original test functions remain. The helper's suffix variable is
fixed; a restored-file snapshot is under target/evidence/i150. No test passed or was removed as a fix.
Current build: recovery-admission-check-2.log, session28154. No Rust edits while it runs.

Recovery admission compile finished with 1415 library / 2042 test-target errors; no tests ran.
The follow-up completes production adoption opener-ID migration and further migrates control-plane
fixtures to explicit locations and metadata (including creation test tuples). Current build is
`control-fixture-check.log`, session28544. No Rust edits while active.

### Adoption identity and receipt continuation (2026-10-31)

Adoption claim APIs now take the established boot Birth. Committed-claim equality is checked before
taking an in-flight row; takeover releases only the recorded claimant/scope for the exact lost birth.
NodeDeleted receipts now contain full LifecycleOp metadata and the planner's typed node-delete ID;
create completion retains its stable location operation. Answers returns Result<Vec<NodeId>, String>;
errors cannot invent answering IDs. Genuine answers hand off; real errors fail the attempt Unavailable
while fabric_delete_in_force retains the deletion intent. Architect 1 rechecked and accepted that
existing retry mechanism, withdrawing an earlier cancellation concern.

Adoption checks now cover unreadable probes and decoding the exact deletion birth in its mesh lane.
All remaining literal opened_by fixtures now use explicit node IDs; drive diagnostics split path/name.
Latest completed build failed 1365 library / 1965 test-target errors; no tests ran. Current build is
drive-diagnostics-check.log, session21558. No Rust edits while active.


Continuation: drive-diagnostics-check completed with 1318 admin library / 1916 test-target errors;
no tests ran. Build replay now retains typed FrameKey fields instead of joined operation strings
and splitn parsing. Primary-update claim/handoff use NodeId, synthetic failure receipt has no
invented endpoint provenance, and operation tracing exposes structured components. Architect 1
found no concrete source regression and requested focused replay/dedup coverage. A completion
regression covers attempt, recipe position, location and Blocked distinction; frame-identity-check
is compiling and has not run tests. Full i150 remains incomplete, including the remaining caller
migration, Atlas ABA correction, lifecycle activation and final gates.


Continuation: frame-identity-check failed compilation with 1287 library / 1885 test-target errors,
so no test executed. The next source batch replaces joined stall keys with typed variants,
represents a missing section operation with Option, and distinguishes attempt-verdict publication
failure using an explicit unscoped attempt operation. Tests now cover exact frame deduplication,
recipe/attempt distinctions, opaque non-node delimiter text and absent versus unscoped operation.
Architect 1 is reviewing this delta; section-identity-check is compiling. Primary handover still
has joined operation/run parsing to migrate; no completed implementation or gate is claimed.


Continuation: section-identity-check compilation failed at 1261 library /1854 test-target errors;
no tests ran. Exact publication-failure classification is shared across local/remote verdict and
section paths, with no synthetic step receipt. Primary handover now carries a typed role plus full
preceding OperationId, preserving preceding action, subject and accepted step position. Build and
submission run remain separate and scope memo acknowledgements. Wrappers and RoleGate pass run;
PrecedingUpdate carries it. Joined update strings and run parsing removed. Added core roundtrip,
ordering and memo scope regressions; existing integration fixtures need migration. Architect 1 is
reviewing this source delta and handover-identity-check is compiling. Full i150 remains incomplete.


Continuation: handover-identity-check failed compilation with 1182 library /1775 test-target errors;
no tests ran. Architect 1 accepted the typed handover identity/memo/run delta and found a remaining
location-based primary selection. That boundary now carries the admitted OperationSubject, using
exact Birth when pinned and NodeId otherwise. Shared Topology::node_subject does not fall back from
ID/Birth to the location's successor. role_move and acknowledgement both use it. A topology regression
covers renamed predecessor, replacement at the old pair, different incarnation and missing old ID.
Architect 1 is reviewing; primary-subject-check is compiling. Integration fixtures and the full i150
remaining implementation and gates remain outstanding.


Continuation: primary-subject-check failed compilation at 1182 library /1774 test-target errors;
no tests executed. Architect 1 reviewed ID-first primary selection with no new defect. Status effect
tracking now uses a typed composite key (node ID, incarnation, Build, attempt, structured operation),
with no joined-string encoding. A regression covers name/state invariance and all identity components.
Status diagnostics use separate path/name fields or NodeId; declaration fault selectors receive the
known NodeId directly. Testkit selector fixtures still need migration. status-identity-check is
compiling and Architect 1 is reviewing. Full i150 remains incomplete.


Continuation: status-identity-check failed compilation with 1060 library /1660 test-target errors;
no tests ran. Status effect map uses typed HashMap keys because incarnation IDs have equality but no
ordering semantics. Declaration fault seam now receives typed NodeId and optional separate location;
selectors prefer the ID and use a structured location only when no ID is provided. This preserves the
cut armed before a node is allocated. All three declaration-cut fixtures updated, selector regression
added; broader integration fixtures still need migration. declaration-selector-check is compiling;
Architect 1 is reviewing. Full i150 remains incomplete and no gate status advances.


Continuation: declaration-selector-check failed compilation with 1028 library /1610 test-target
errors; no tests ran. Historical location fallback removed from status cleanup and deleted-birth
investigation: only exact retained released records prove deletion; successor lookup is diagnostic.
Regressions cover renamed live predecessor, successor claim, missing record and released states.
Termination receipts now keep a structured path/name pair, and provider spawn errors retain that
pair with labelled diagnostics. Constructors migrated, launch test claims bound to real launcher
Birth, separate launch path/name assertions added. Added terminal receipt roundtrip/rejection test.
deployment-identity-check is compiling; Architect 1 reviewing. Full i150 remains incomplete.

### Continued structured receipt and journal migration

The deployment compilation failed with 970 library / 1538 test-target errors; no tests ran.
The resulting 20 PathName diagnostic field-access mistakes are corrected. `ReceiptKey` now
retains structured operation and step fields through primary transfer and replay. The journal
indexes their structured encoding with an opaque hash while retaining every field in the fact.
Build/mesh/attempt scope stays separate. This is an incompatible durable format change:
`build-record/2`, with the `durable format moved` marker and audit ratchet updated. Added tests
cover distinct fields, nested primary updates, output-independent indexing, scope and rejection
of the old format. Existing journal fixtures now use explicit IDs, Birth claims, structured
locations and typed operations. Architect 1 found no defect in the initial identity/index delta;
format/fixture review and `receipt-identity-check.log` compilation are in progress. No new test
has executed. Full i150 completion, architecture correction approval, gates and landing remain owed.

### Continued observer and property-diagnostic migration

Receipt compilation finished at 941 library / 1431 test-target errors; no tests ran. Build-state
had one remaining handoff fixture conversion, now fixed. Architect 1 accepted the journal format
refusal and fixture changes; an unrelated executable hash-label substitution was restored.
Property-command diagnostics now use explicit receiver/subject/primary IDs, with optional location
fields separate. ElectionLog selects its observing admin by NodeId even after a rename or a new
occupant at its former location; winner path/name remain separate. Added a regression for that
case and migrated election fixtures to structured pairs. Observer compilation and Architect 1
review are in progress. The Atlas historical rename replay defect remains open: old receipt
mutation must be replaced by ordered current-row publication or equivalent durable evidence.

### Continued runtime handle identity migration

Observer compilation finished at 866 library / 1340 test-target errors; no tests ran. Architect 1
accepted the observer/property diagnostic semantics and found one identifier typo, now corrected.
Runtime handles now use NodeId as their index, verifying incarnation on pinned reads. Renaming
a predecessor cannot hide its runtime behind the successor's location. Cleanup removes only the
matching ID/incarnation under one lock, preserving a later incarnation. Added a regression for
rename, replacement and stale cleanup. Remaining pre-admission location discovery compares the
structured pair. `runtime-index-check.log` compilation and Architect 1 review are in progress.
The Atlas review confirmed that legacy row revisions cannot solve ordering for split lifecycle
records, which have no revision counter; current-row publication ordering remains required.

### Runtime review corrections

Runtime-index compilation finished at 728 library / 1196 test-target errors; no tests ran. It
reported no compiler errors in runner, election, node_config or build_state. Architect 1 found
ambiguous cached-location selection after replacement and a known-birth rejoin that re-resolved
location. Both are corrected: unpinned steps read the current composite name claim and then use
NodeId; rejoin retains the caller's exact NodeId/incarnation. Metadata refresh filters incarnation,
and cached handle lookup requires a known incarnation. The regression covers duplicate cached
locations, absent claims, current successor claims and stale cleanup. Architect 1 reviewed these
corrections without further findings. `runtime-claim-check.log` is running; no tests are green.

### Continued manifest-reference migration

Runtime-claim compilation remained at 728 library / 1196 test-target errors; no tests ran. Architect 1
accepted the exact-birth runtime corrections. MeshDeleted now carries a structured ManifestReference
with Build, attempt, OperationId and explicit scope; validation compares fields directly rather than
joining/splitting a string. The 128-byte reference bound remains, measured on its postcard encoding.
Added field distinction/reject-joined-string tests and adapted the maximum-size wire fixture. Roster
delete receipts now select node IDs; fabric-delete receipts use the typed Fabric subject. Manifest
compilation and Architect 1 review are running. Wire goldens and all execution evidence remain owed.

### Mesh-delete exact-reference continuation

Manifest compilation finished with 629 library / 1093 test-target errors; no tests ran. Architect 1
accepted structured matching and identified synthetic receipts carrying node IDs as endpoint provenance;
those receipts now omit executor. Mesh-delete runtime insertion uses NodeId, retained deletion matching
uses typed node operations, and silent-member lookups require both node ID and incarnation. The unused
outer operation argument was removed from exit receipt lookup. Diagnostics use IDs or labelled separate
path/name fields. Follow-up review and compilation remain pending; full i150 closure is still outstanding.

### Build dispatch exact-claim continuation

Mesh-delete compilation finished with 548 library / 1010 test-target errors; no tests ran. Its only
remaining compiler sites were two diagnostic substitutions, now corrected. Architect 1 also found
one synthetic MeshLeft receipt still carrying a node ID as endpoint provenance; it now omits executor.
Drift dispatch now freezes the committed executor Birth selected by reporter NodeId, keeps committed
claimant/mesh for release, and rechecks full claim identity before evidence and terminal publication.
Shared frame/receipt validators reject mismatched Build, attempt, mesh, operation and step without
rewriting receipt scope; malformed/unwritten receipts are not acknowledged as accepted evidence.
Core Build headers carry executor Birth as fields; executor filtering uses node ID. Reply chunk sizing
uses encoded structured receipts. Scope regression and Architect 1 follow-up review remain unexecuted
and pending respectively. All remaining i150 work and full gates remain open.

### Structured run replay and client executor continuation

The next compile stopped first at one core wire fixture, then at two client executor-field sites;
no tests ran and the last full admin count remains 548 library / 1010 test-target errors. These
fixtures/client fields now carry Birth. Updated fixed Got and MeshDeleted wire expectations by
independent field encoding, without using serializer output. Architect 1 accepted drift Running
checks; regressions cover ended claims and changed birth/claimant/scope. RunLog blocked/failed state
now retains OperationId, and recovery dispatch compares exact executor Birth and releases the
committed claimant/mesh. A replay identity regression was added; source review is pending.
Atlas root review requires shared WrittenRows per-ID locks across read-to-publication, current-row
refresh instead of synthetic rename replay, and fill that cannot overwrite newer publications.
This is still open, along with full migration, execution evidence, architecture approval and closure.

### Recovery claim and replay fixture continuation

Full compilation reached admin again: 472 library / 933 test-target errors; no tests ran. Recovery
now reads committed_claim for the exact opened Build/lane/attempt and validates it before execution,
freezing claimant and claimed mesh for release. The two executor verdict-publication failures use
the shared exact attempt marker. Run replay fixtures now carry explicit unrelated actor births,
structured locations and operations, and typed retained-receipt keys. The ordinary read fixture
uses explicit mesh metadata rather than splitting a node name. Review and compilation are pending.
An unapproved draft exact-text architecture correction is in proposal/node-reference-fields.patch;
architecture pages and the previously approved architecture packet are unchanged. The draft is not
the complete cross-page audit, which remains in target/evidence/i150/architecture-joined-reference-audit.txt.

### Property diagnostics and retained publication failure

Recovery-claim compilation finished with 468 library / 882 test-target errors; no tests ran. No
compiler errors were reported in build_run or build_op. Architect 1 accepted the frozen recovery
claim and found that verdict-publication failure still released its in-flight row; that exact
terminal now returns Broke before release, retaining the claim. Rename/tag diagnostics identify
nodes by ID, with requested/from/to/subject path and name recorded separately. Declaration logging
retains structured operations and recipient IDs. This delta is awaiting review and compilation.

### Typed drift decision continuation

Property-diagnostic compilation finished with 418 library / 832 test-target errors; no tests ran.
Architect 1 accepted diagnostic/reference changes and retained recovery publication failures. Drift
now compares inspected exact node ID/incarnation pairs, keys proven births by NodeId, and retains
location separately for create admission/order. Typed target/input/memo values replace concatenated
name strings and string sentinels; completed steps match operation fields directly. Surplus deletion
suppression requires successful scoped lifecycle evidence for the exact birth, not matching JSON
fields alone. Added equal-incarnation/different-node regression and migrated drift fixtures. Review
and compilation pending. Legacy tag combined-row fallback and Atlas current-row publication remain
open; no closure or full gate result is claimed.
