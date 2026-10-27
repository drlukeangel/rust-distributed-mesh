# i149 — change topology storage while the fabric keeps serving

**Initiative / Epic / Story ID:** `i149`
**Sprint:** initiative i149; implementation epics e1–e6
**Status:** opened 2026-10-26; requirements recorded, implementation acceptance outstanding
**Architecture doc:** [Storage](../architecture/storage.md) §2 — the final contract this initiative implements
**Testing discipline:** red-green, per epic: the epic's acceptance cells are written first and run red at the lane's base, the red output (assert and SHA) recorded in the epic's evidence; the implementation turns those exact cells green; a cell changed to pass needs Luke's yes; each landing passes the direct fast gate, the release suite runs nightly. No QA sign-off.
**Approved:** pending Luke's approval of this PRD
**Owner:** i143 lead; implementation owner assigned per epic
**Feature slug:** `topology-storage-provider`
**Baseline:** `ba3d50631a590805357f222562b56282bae5dda6`, with the accompanying working changes to storage.md and fabric-storage-provider.md. Those contract corrections must land before implementation acceptance.
**Source template:** [Seed-First PRD](../eng/templates/prd-seed-template.md), adapted to this repository's RDM surfaces and estate harness.
**Record:** this file owns i149 requirements, epic state and evidence. The i143 lead records the dependency and landing SHA in i143-prd.md when dispatching and landing it.

## Context

An application can establish its topology store only after its initial nodes exist. Its fabric must
therefore start on an available backend and later move RDM's records to the application's backend
without restarting nodes or losing history. Today storage opens once during boot; changing a
provider requires a live protocol for copying records, transferring claims and recovering a lost
executor. The application submits the Build when its store exists; RDM implements that Build.

This initiative moves the five RDM record stores. Application data files remain governed by
`storage_persistence`. The separate storage_persistence field rename is tracked in e1 so its public
names and compatibility work cannot be lost between initiatives.

**Migration sequence:** block new changes, finish outstanding writes and claim operations, copy the
stable records and held claims, switch providers, then unblock waiting changes. Reads and serving
processes continue. This is one short blocking migration; there is no background copy or later
catch-up stage. Measure its duration rather than using a one-second timer as a safety rule.

## Requirements

A disposition is **HOLDS** with current evidence or **BUILD** with an owning epic. A passing unit
cell does not establish a live migration. Coordinates below were read at the baseline; line numbers
are source anchors rather than assertions that the missing capability already exists.

| ID | Customer requirement | Disposition | Evidence today / implementation anchor | Epic |
|---|---|---|---|---|
| R1 | Name topology storage independently of application data-dir persistence; query both properties | BUILD | `crates/rdm-core/src/meta.rs:16`, `crates/rdm-core/src/views.rs:86`, `crates/rdm-admin/src/fabric_storage.rs:107` | e1 |
| R2 | Register named backend openers in every participating executable; refuse unsupported names without moving data | BUILD | `crates/rdm-admin/src/wiring.rs:156`, `crates/rdm-admin/src/storage_backend.rs:77`, `crates/rdm-node/src/storage_backend.rs:21` | e1 |
| R3 | Submit a complete-topology Build changing only topology_storage_provider; omission preserves the current value | BUILD | `crates/rdm-admin/src/accepted.rs:161`, `crates/rdm-core/src/build.rs:289` | e1 |
| R4 | Every active reader and writer uses the same replaceable provider handle on its node | BUILD | `crates/rdm-admin/src/admin/boot.rs:380`, `crates/rdm-node/src/node.rs:468`, `crates/rdm-node/src/connections_writer.rs:46` | e2 |
| R5 | Preserve all record history, including dead writers, and current values through copying | BUILD | `crates/rdm-node/src/store_log.rs:110`, `crates/rdm-admin/src/build_state.rs:237` | e3 |
| R6 | Preserve source order per key across writer changes; reject a destination hole instead of replaying stale state | BUILD | `crates/rdm-node/src/store_log.rs:240`, Storage §2, Changing it, Copy | e3 |
| R7 | Keep one claim decision point, transferring every held name and in-flight claim before cutover | BUILD | `crates/rdm-admin/src/storage.rs:177`, `crates/rdm-admin/src/mesh_claims.rs:1` | e3 |
| R8 | Block new writes and claims, finish issued operations before copying, and keep changes blocked across restart | BUILD | `crates/rdm-admin/src/storage_backend.rs:54`, `crates/rdm-node/src/connections_writer.rs:46`; Storage §2, Changing it | e4 |
| R9 | A caller's command and a node's declaration are answered only once their line lands; connection events stay owed on the node's writer queue (Q169) and never block an open | BUILD | `crates/rdm-node/src/connections_writer.rs:46`, `crates/rdm-node/src/node.rs:468` | e4 |
| R10 | The switch is one insert-and-fail `commit` record for (build_id, change_id); recovery reads it; a provider that does not answer, or a timeout, never means not switched | BUILD | `crates/rdm-admin/src/build_state.rs:198`, `crates/rdm-admin/src/build_drive.rs:136`; Storage §2, Decision | e4 |
| R11 | Recover an unfinished change with the same change_id; a node refuses a command of an older change_id by name | BUILD | `crates/rdm-admin/src/build_state.rs:100`, `crates/rdm-admin/src/build_state.rs:124` | e4 |
| R12 | Restart, new birth, adoption and fabric.delete resolve the current migration before issuing ordinary writes | BUILD | `crates/rdm-admin/src/admin/boot.rs:296`, `crates/rdm-node/src/node.rs:468`; Storage §2 | e4 |
| R13 | Serving nodes keep their identities and ready-for-traffic state throughout a successful change | HOLDS | 72de000e1: `every_serving_node_keeps_its_birth_and_ready_for_traffic_through_a_change` (the fast gate's cell of the stem); green again at 27de0479e | e5 |
| R14 | Keep previous providers readable and unchanged after cutover; a later Build can return to one | HOLDS | 72de000e1: `a_previous_provider_remains_unchanged_after_cutover`, `a_customer_returns_to_a_previous_topology_provider`; green again at 27de0479e | e5 |
| R15 | Exercise the capability in a fast smoke and the full scheduled suite, with process and container coverage | BUILD | 72de000e1: the fast gate runs `topology_storage_provider_fast`, the release suite the whole stem. Owed: the two container cells are ignored by name (the testkit's `test-target` place is mounted into no role node's container; D14), and RDM has no nightly runner to schedule the release suite | e5 |
| R16 | Publish usable instructions and recovery procedures with the final implementation evidence | HOLDS | 394edf9f5: `docs/features/topology-storage-provider/{overview,how-to,runbook}.md`, each example beside its cell, linked from `docs/eng/consumer-guide.md`; the stem at 27de0479e: 15 passed, 2 ignored (the container cells), 29 s | e6 |
| R17 | Ordinary Build execution retains its existing exclusive attempt and receipt model | HOLDS | `docs/architecture/build.md:405`, `crates/rdm-admin/src/mesh_claims.rs:1`; migration-specific extensions are R7–R12 | e4 |
| R18 | No Build is admitted during a change, a change back included; only fabric.delete, which interrupts it; a step failing before the switch fails the Build once no `commit` is read, and every node unblocks on the old provider | BUILD | `crates/rdm-admin/src/control_plane.rs` Build admission; Storage §2, Changing it | e4 |
| R19 | A node that will not block or unblock is a lost node (node-investigation.md, Q182): investigated and re-created at its path.name on the provider of its step; after the switch the Build completes once every node has unblocked or been re-created | BUILD | node investigation and node recovery; Storage §2, Changing it | e4 |

## § Inventory

The e2/e3 inventory, read at 7d0b316eb: [i149-inventory](i149-inventory.md) (every storage handle owner, write path, claim, finish-writes gap, copy fact and cross-writer reader, with file:line). Its §7 lists 16 places today's code differs from Storage §2; e2–e4 resolve each with a cell. Four were checked by the lead against the code: the fabric name claim is read-then-put (`fabric_storage.rs:73-89`), Build claims are decided under a process-local mutex (`build_state.rs:2444-2476`), observed connection writes are queued in memory (`connections_writer.rs:342-346`), and finish-writes only takes and drops the append mutex (`store_log.rs:637-639`).

## § Open forks

No new product ruling is requested by this PRD. The accompanying architecture changes settle ordered
ordered copy, blocking changes across restart and per-run decisions. Implementation discoveries that contradict that contract
must be recorded with positive code evidence and the exact outlier page before a ruling is asked.

Provider names identify registrations, including their locations; two locations use two registered
names under Storage §2. e1 must validate this resolution against the source proposal's remaining
`file:<dir>` alternative and reconcile that proposal to the architecture contract. The application
chooses its backend and the moment it submits the Build. RDM does not choose application readiness.

## § Seed spec

The restart-recovery canary below is authored and demonstrated RED first. The happy-path seed is
then authored and demonstrated RED before feature implementation. Planned tests are requirements,
not claims that tests currently exist or have run.

### Theme manifest fragment

RDM's harness uses `Estate`, not Rafka's ThemeSpec YAML. There is no YAML fragment to load here.
Use the existing bootstrap and explicit Build setup from
`crates/rdm-test/src/estate.rs:642` and `crates/rdm-test/tests/fabric/named_listeners.rs:26`.
The executable fixture belongs in `planned:crates/rdm-test/tests/fabric/topology_storage_provider.rs`,
registered in the existing test main. It creates two named, separate durable provider namespaces,
`file` and `test-target`, and two meshes. The second registration exercises the same backend
interface as an application's provider; it must store real records and implement exclusive claims.

The request's feature fragment is:

```json
{"topology_storage_provider":"test-target"}
```

This is an extension to the complete accepted topology supplied by the fixture, not a standalone
build.create request. e1 adds and decoder-checks this field against the actual accepted topology
and client schema; the fixture retains every existing node's executable, config, tags, placement
and storage_persistence. No guessed client method or runnable YAML is specified here.

### Suite assertion

`planned:crates/rdm-test/tests/fabric/topology_storage_provider.rs` contains
`a_customer_changes_topology_storage_without_restarting_members`:

1. Start the real two-mesh estate; retain every node's id, incarnation, ready state, config and tags.
2. Create connection history and records of a deleted birth and mesh; capture all public history.
3. Submit the complete topology with the target provider through the real Build carrier.
4. Keep connection activity running; submit a config change during migration and verify it waits
   until the provider switch completes.
5. Require Complete, the target on the Fabric and every node, unchanged serving births and states,
   intact record history/current values, and unchanged previous-provider records after completion.
6. Assert block, copy, decision and unblock spans on one causal trace; read results through the
   public entity reads available at landing, including arbitrary config-property matching.

### What a passing seed proves

An application maintainer can move a serving fabric's topology records to an available backend
with one Build. Every participating process uses the destination, retained history remains readable,
and no healthy member was replaced merely to change persistence location.

## § Surfaces — one capability, identical semantics

RDM exposes Build and entity reads through Node RPC and rdm-client. Kafka and REST are Rafka
application decoders, not RDM public surfaces; this PRD does not invent those handlers. The shared
core must be usable by either application's decoder without implementing a second migration.

| Operation | Node RPC decoder | Typed client | Shared core | Permission/eligibility |
|---|---|---|---|---|
| Change topology provider | existing Build op, complete topology | existing Build submission | accepted topology and Build executor | current fabric-primary; normal Build admission |
| Inspect provider and progress | entity reads and Build receipts | corresponding entity/Build methods | same views and receipt fold | existing read eligibility |
| Execute migration steps | fabric-primary → mesh-primary → exact node | internal typed calls | same migration-run state | exact change identity and claimed execution |

The new entity reads may land through Q167; e1 must preserve the existing readable fields while
that dependency lands. No second read implementation is introduced. Process/container deployment
must expose identical outcomes and refusal reasons. Unsupported provider is a named refusal, not
an empty read or successful no-op.

## § Restart-recovery canary

**FIRST test:** `topology_storage_state_survives_admin_restart_at_same_data_dir` in the planned
estate stem. Persist that changes are blocked for the change, kill the real fabric-primary process, start recovery over
the same data dir, and resolve the same change_id. Assert all public records survive, a stale launch
environment does not unblock writes, and the decision is read from the `commit` record alone (found:
switched; none on an answering provider: not switched). Cover clean restart and forced termination; compare the complete captured dataset.

A second named canary, `topology_storage_state_survives_role_node_restart`, kills a role process while changes are blocked, with a write owed. Restart at the same data dir and
verify it stays blocked, reads the `commit` record, and its writes land exactly once on the provider
chosen. Exercise both a switched change and a step failed before the switch (every node unblocked on
the old provider). A new birth with a different data dir must not be described as restart coverage.

This generic RDM initiative uses test role nodes instead of depending on Rafka broker/gateway
internals. Rafka's consuming integration must separately prove broker and gateway restart through
its own public surfaces when it supplies its system-topic backend. e5 records that consumer handoff
and the exact RDM revision; RDM acceptance requires its real admin and role process boundaries.

## § OTLP span-assertion bar

Every live cell captures the production RDM spans with the estate's existing evidence mechanism
(`crates/rdm-test/src/estate.rs:793`), checks attributes, exports JSONL evidence, and records a Jaeger
trace link with the same trace id. A successful data read alone is insufficient. Proposed span names
below must pass the repository name check before implementation; steps remain children of the
existing Build execution context across fabric-primary → mesh-primary → node.

| Span | Required attributes | Acceptance control |
|---|---|---|
| `rdm.node_admin.build.update.via-storage-open` | build_id, change_id, node_id, provider, outcome | unsupported provider test sees its named refusal |
| `rdm.node_admin.build.update.via-storage-copy` | build_id, change_id, source, destination, source_position, copied, outcome | ordered copy and destination-hole tests |
| `rdm.node_admin.mesh.update.via-block` | build_id, change_id, mesh, node_id, in_flight, outcome (`blocked`) | a mesh-primary replies `blocked` once its in-flight commands complete, and issues no new command until unblocked |
| `rdm.node_admin.build.update.via-storage-switch` | build_id, change_id, provider, commit_found, outcome | a lost commit answer resolved by reading the record |
| `rdm.node_admin.mesh.update.via-unblock` | build_id, change_id, mesh, node_id, provider, moved, left, outcome (`unblocked`) | a mesh-primary moves its nodes onto the provider the `commit` record decided and replies `unblocked`; a node that does not answer its move is a lost node |
| `rdm.node.build.update.via-storage-move` | build_id, change_id, node_id, provider, outcome (`moved`, `rejected-stale-change`) | a node refuses by name a command of an older change |
| `rdm.node.store.resolve.via-fold` | provider, bytes, lines, duration, invalid_offset when refused | reopen and history checks |

Absence assertions require a successful control span for the request under test. In particular,
attempt a write while changes are blocked, verify it waits, and verify its durable result after
unblocking; a quiet trace alone proves nothing.
Provider secrets and connection credentials never enter attributes or record-copy diagnostics.

## § Per-sprint seed-data obligation

The durable migration-run and decision records are new state. e1 defines their explicit format and
identity; e4 supplies their real persistence and recovery. e5's estate fixture seeds live history,
deleted births, dead meshes, occupied names, an active in-flight attempt and a pending event. It
asserts each through the appropriate surface or functional storage contract before migration.
The RDM fixture and its seeder are Rust in the planned estate stem; there is no Rafka theme schema
in this repository. Adding a consumer YAML fixture belongs to the consumer handoff recorded by e5.

## § Test tier coverage

- [x] unit — schema/defaults, registrations, decision identity and copy-prefix validation
- [x] functional — real durable providers, exclusive claims, lost write answers and restart state
- [x] e2e — real estate Build, reads, process termination and container namespace qualification
- [x] load — reads and event generation while changes are blocked, with durable pending events
- [x] chaos — delayed appends, lost decision replies, executor death and delayed old-run commands

These checkmarks commit to deliverables; they are not passing results. RDM's test-main module and
fast/release job discovery are the executable registration. e5 must show the discovery evidence;
no nonexistent Rafka feature registry is assumed.

## § Pre-implementation thoughts (2026-10-26)

- **Hard part:** the decision's durability and discoverability must survive the executor changing
  while the journal itself is being moved.
- **Likely failure:** copying all operation identities yet exposing a stale current record because
  writer histories were appended out of source order.
- **Persistence path:** five provider stores, exclusive claims, local with changes blocked-state and pending-event
  logs, and one immutable decision per `(build_id, change_id)`. Restart resolves that decision before
  opening ordinary writes. Provider omission never silently switches an established fabric to file.
- **Dependencies:** R-SR append-only stores and exclusive-claim primitives; Q167 public property reads;
  existing Build claims, receipts and recovery. e1 verifies exact landed interfaces before editing.
- **Architectural risks:** source data and destination history must form an ordered prefix per key;
  a corrupt or non-prefix destination is refused without silently rewriting history.
- **Implementation audit:** enumerate every stored backend handle and every issued-write path in e2;
  enumerate every claim and every copier in e3. Evidence resolves mechanics within this contract.

## Implementation plan

Write the canary and seed RED, implement e1–e4 in dependency order, exercise e5, and finish e6.
Each epic starts with an executable failing behavior and closes with its changed-code checks.
The epic blocks below are the dispatch briefs; no epics are dispatched by this document creation.

### e1 — the customer names and reads the provider

- **Goal → gap:** Storage §2 properties and registration. Boot accepts one preselected backend and
  topology/Fabric/view types have no migration property.
- **Detailed requirements:** R1–R3; extend `accepted.rs:161`, `fabric_storage.rs:107`,
  `rdm-core/src/meta.rs:16`, `rdm-core/src/views.rs:86`, and `wiring.rs:156` under their full crate
  paths in the requirements table. Preserve omitted-property semantics and historical decode
  defaults separately. Define change_id and provider names, add registered openers, and implement
  storage_persistence/disposition naming compatibility under the repository format-marker rule.
- **Spans:** storage-open and its named refusal, with Build context and no provider secret values.
- **Testing:** unit schema/decoder tests and the planned estate stem's first RED canary and seed.
- **Acceptance names:** `a_customer_reads_each_nodes_topology_provider`,
  `an_unsupported_provider_changes_nothing`, `an_omitted_provider_preserves_the_current_provider`.
- **Mesh plan:** `multi-mesh` for the planned estate fixture; e5 supplies the shared setup. Build and
  inspect both process and container variants using existing Estate setup. Run with
  `scripts/cells.sh rdm-test main -- topology_storage_provider::`. No Rafka mesh-plan generator is
  present here; the setup records the realized shape in its artifact via Estate::artifact.
- **Sizing / files:** one implementer; core metadata/views, accepted topology, Fabric record,
  wiring/backend registration and client decoders. These are shared with e2/e4, so sequence them.
- **Rows absorbed / state:** R1–R3; BUILD. No external issue is asserted without a verified link.

### e2 — every node uses one replaceable storage handle

- **Goal → gap:** Storage §2 live handles. The admin boot at `admin/boot.rs:380` and ordinary node
  open at `rdm-node/src/node.rs:468` hand long-lived storage objects to their callers.
- **Detailed requirements:** R4; inventory all Arc storage owners, replace through one per-node
  access point, open providers off the boot path, and preserve the owning writer's object cache.
  Finish-issued-writes must include every operation already admitted through the old handle.
- **Spans:** storage-open; functional tests correlate each operation with its selected provider.
- **Testing:** real file-backed functional tests; `planned:crates/rdm-node/tests/storage_provider.rs`
  and admin inline tests. No replacement succeeds while an unaccounted issued write can complete.
- **Acceptance names:** `a_node_uses_the_replaced_provider_for_every_storage_call`,
  `a_provider_change_waits_for_already_issued_writes`.
- **Mesh plan:** same multi-mesh estate at integration; unit/functional tests require no estate.
  Focused dispatch uses `scripts/cells.sh rdm-node lib -- storage_provider` after discovery confirms
  the module filter, then the shared estate invocation from e1.
- **Sizing / files:** one implementer; both storage_backend modules, connections_writer.rs,
  node.rs and admin boot/callers discovered by the handle inventory.
- **Rows absorbed / state:** R4; BUILD.

### e3 — copy the complete history and held claims in order

- **Goal → gap:** Storage §2 Copy. Existing store-log identities do not implement a
  cross-provider copy or verified progress.
- **Detailed requirements:** R5–R7; use `rdm-node/src/store_log.rs:110` and `:240`,
  `rdm-admin/src/storage.rs:177` and Build facts at `build_state.rs:237`. Copy every retained kind,
  preserve source order per key across ownership changes, persist verified progress, and reconcile
  uncertain copy answers without duplicate appends. Refuse destination holes/reordering before
  cutover. Copy starts only after changes are blocked and outstanding writes have finished; transfer claims
  with the stable records and verify exact holders.
- **Spans:** storage-copy, with verified source position and copied count; refusal identifies the
  key and mismatch without dumping records containing application settings.
- **Testing:** `planned:crates/rdm-node/tests/storage_copy.rs`, shared estate history assertions.
- **Acceptance names:** `a_copy_preserves_current_values_across_writer_changes`,
  `a_copy_refuses_a_destination_hole_without_replaying_stale_state`,
  `a_copy_resumes_after_a_lost_answer_without_duplicate_records`,
  `a_provider_change_preserves_held_names_and_attempts`.
- **Mesh plan:** functional providers need no estate; multi-mesh integration uses e1's invocation.
- **Sizing / files:** one implementer; store-log adapter, admin storage and Build journal copy paths;
  shared production files with e4 mean e4 follows this epic.
- **Rows absorbed / state:** R5–R7; BUILD.

### e4 — block the mesh-primaries, switch by the commit record, and unblock (Q198)

- **Goal → gap:** Storage §2 Block/Copy/Switch/Unblock (Q198) and per-change identity. Build claims
  exist, but there is no storage change execution or recovery path.
- **Detailed requirements:** R8–R12, R17–R19 as Storage §2 states them after Q198. Record the
  change_id with its Build before its first command. Each step is an operation of the change's Build,
  claimed and dispatched by the fabric-primary (`build.attempt.run`) and ending in its receipt:
  `mesh.block` at each mesh-primary (it waits until its in-flight commands complete, replies `blocked`,
  gossips a current aggregate of its mesh and issues no new command until unblocked); the copy and the
  switch at the fabric-primary once every mesh-primary replied `blocked` (`commit` inserted with the
  store's insert-and-fail primitive, then the Fabric row's line; recovery reads only that record);
  `mesh.unblock` at each mesh-primary (it moves its nodes onto the provider the record decided, each
  node's storage handle replaced on the node command op 0x22, replies `unblocked`, issues commands
  again). A step failing before the switch fails the Build once no `commit` is read, `mesh.unblock`
  sent on the old provider. No Build but fabric.delete is admitted during a change; fabric.delete
  sends `mesh.unblock` onto the provider the record decided before its teardown. A mesh-primary that
  does not reply is a lost node (Q182). A node refuses by name a command of an older change
  (`rejected-stale-change`). No timer.
- **Spans:** `mesh.block` / `mesh.unblock` (`rdm.node_admin.mesh.update.via-block` / `via-unblock`),
  the switch, and a node's move (`rdm.node.build.update.via-storage-move`), with exact change_id.
- **Testing:** planned estate stem plus functional tests owned by this epic.
- **Acceptance names:** `a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read`,
  `a_lost_commit_answer_is_resolved_by_reading_the_commit_record`,
  `a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider`,
  `a_build_other_than_fabric_delete_is_refused_during_a_change`,
  `an_old_change_cannot_unblock_writes_for_a_new_change`,
  `a_connection_event_during_a_change_lands_once_after_the_unblock`,
  `a_node_that_never_unblocks_is_recreated_on_the_new_provider`,
  `a_delete_resolves_the_storage_change_before_teardown`.
- **Mesh plan:** multi-mesh, process/container, same shared fixture and test invocation as e1.
- **Sizing / files:** one implementer; Build state/drive, the mesh-primaries' command gate, node
  command dispatch, and the change's operations.
- **Rows absorbed / state:** R8–R12, R17; BUILD.

### e5 — prove the customer workflow and preserve continuous verification

- **Goal → gap:** all of Storage §2 through real process boundaries. No test currently proves live
  provider migration; R13–R15 require observable acceptance and test discovery.
- **Detailed requirements:** finish the planned estate stem, register it in test main, capture full
  record history and trace evidence, prove previous-provider immutability and a go-back Build.
  Add `a_customer_changes_topology_storage_without_restarting_members` as a fast smoke. Keep full
  failure/load/process/container coverage in the release suite. Locate the actual nightly runner
  and retain its schedule and complete suite: record its configuration, discovery and a successful
  scheduled invocation. An existing schedule is not claimed active until verified; an absent runner
  is this epic's defect to resolve with the lead, never silently replaced by the smoke.
- **Spans:** every span in the OTLP table; assert causal Build context and actual public outcomes.
- **Testing:** real two-mesh estate, waiting changes during copying, delayed outstanding writes, abrupt process
  death, claim competition, restart and go-back. The setup is shared across all e2e cells; faults do
  not choose unrelated shapes. Both deployment providers implement the same behavior matrix.
- **Acceptance names:** seed and both restart canaries above,
  `a_previous_provider_remains_unchanged_after_cutover`,
  `a_customer_returns_to_a_previous_topology_provider`,
  `a_storage_change_preserves_application_config_filter_results`.
- **Mesh plan:** multi-mesh (node-admin, compute, gateway and broker roles present; at least two
  admins per mesh for successor tests). Use Estate's supported setup; preserve the fixture across
  the epic, restart only the subjects named by the cells. Run focused stem through cells.sh, then
  `scripts/rdm-gate-at.sh` fast and release with the implementation owner's existing checkout name.
- **Sizing / files:** one test implementer; planned estate stem, tests/main.rs, fixtures/fault seams,
  scripts/rdm-gate-at.sh and the located nightly runner. Shared production seams are sequenced with
  e4 rather than edited concurrently.
- **Rows absorbed / state:** R13–R15 and integration proof for R1–R12; BUILD.

### e6 — publish the working API and recovery instructions

- **Goal → gap:** Storage §2 contract with implementation evidence and usable consumer guidance.
- **Detailed requirements:** R16; reconcile the source proposal with the final architecture, update
  consumer guide and env table, regenerate applicable wire/span references, and record exact SHAs,
  test counts, durations and traces. Do not replace final architecture rules with progress notes.
- **Spans / testing:** document the actual landed span families, error reasons and completed e5
  evidence; verify all runnable examples and local links against the landing revision.
- **Acceptance names:** documentation checks plus successful execution of the how-to workflow and
  one runbook restart/decision recovery procedure against the estate.
- **Mesh plan:** reuse e5's multi-mesh setup for example verification; no fresh topology required.
- **Sizing / files:** one documenter; the three feature docs, architecture/storage.md,
  eng/consumer-guide.md, CLAUDE.md env table, proposal and initiative evidence.
- **Rows absorbed / state:** R16; BUILD. Final epic, authored with implementation context fresh.

### Files likely to touch

- `crates/rdm-admin/src/storage_backend.rs`, `crates/rdm-node/src/storage_backend.rs` — registrations and replaceable boundaries.
- `crates/rdm-admin/src/accepted.rs`, `crates/rdm-admin/src/fabric_storage.rs`, `crates/rdm-core/src/meta.rs`, `crates/rdm-core/src/views.rs` — public/durable properties.
- `crates/rdm-admin/src/build_state.rs`, `crates/rdm-admin/src/build_drive.rs` — run identity, progress and decision recovery.
- `crates/rdm-node/src/store_log.rs`, `crates/rdm-node/src/connections_writer.rs`, both boot paths — ordered copy, blocked changes and replay.
- `planned:crates/rdm-test/tests/fabric/topology_storage_provider.rs` — customer acceptance, canaries and failure matrix.
- `scripts/rdm-gate-at.sh` — smoke and release discovery; nightly configuration located and verified in e5.

## § Per-feature documentation deliverable

The final epic creates all three substantive documents:

- `planned:docs/features/topology-storage-provider/overview.md` — purpose, request/persistence/failure paths, crate boundaries, surface map and cross-references.
- `planned:docs/features/topology-storage-provider/how-to.md` — executable registration, submit/read examples, omitted-property behavior, config-filter example and common named refusals.
- `planned:docs/features/topology-storage-provider/runbook.md` — blocked changes and decision health, unavailable provider, restart/adoption, switched versus failed before the switch, refused history and previous-provider handling.

Examples use the actual RDM client/public read shapes at landing. Internal diagnostics never become
an alternative customer API. The runbook explicitly distinguishes pending observed-event durability
from unanswered caller retries and gives procedures for the precise provider decision states.

## Verification

- [ ] First restart canary RED before the happy-path seed; both RED before implementation.
- [ ] All BUILD requirements have an implementing epic and are converted to HOLDS only with evidence.
- [ ] Decoder round trips preserve explicit provider and omission semantics; unregistered names refuse by name.
- [ ] Ordered copy, source-prefix refusal, lost copy answers, claims and the history of dead births and deleted nodes pass functional coverage.
- [ ] Outstanding operations settle before confirmation that changes are blocked; restart cannot unblock changes before resolving the decision.
- [ ] Switch tests cover a lost commit answer, a provider not answering, a failed write and successor recovery, the decision always read from the `commit` record.
- [ ] A step failing before the switch unblocks every node on the old provider; no Build but fabric.delete is admitted during a change.
- [ ] A node that will not block or unblock is re-created on the provider of its step (Q182).
- [ ] Commands of an older change_id cannot write, unblock or decide for a later one.
- [ ] All observed events and public record history survive the admin and role restart canaries.
- [ ] The smoke is discovered and passes in the fast gate; full failure/load/container coverage passes release.
- [ ] Nightly configuration and active schedule are verified; its existing coverage is preserved and the migration suite discovered.
- [ ] Process/container results and refusal semantics agree, with real durable provider namespaces.
- [ ] Jaeger trace and JSONL evidence prove the causal Build path and required attributes.
- [ ] Full public reads, including application config filters, agree before and after migration.
- [ ] Previous provider is unchanged after commit; go-back Build succeeds without history reordering.
- [ ] The three feature docs land and their examples run; no promised file or API is asserted to exist early.
- [ ] Repository name/document-coordinate checks and the appropriate fast/release gates pass at one implementation SHA.
- [ ] Every epic's red output (assert and SHA) and its green run are recorded; Luke's approval is recorded explicitly.

### Source-doc propagation

Update Storage §2, Build storage boundaries, lifecycle index, node properties, node/admin boot
contract, consumer guide, env table and generated references with their actual changes. e6 records
which sources changed and why. The source proposal is marked implemented only with a landing SHA
and evidence; progress remains in this initiative's epic rows and the i143 dependency record.

e6's record (394edf9f5):

| source | changed | why |
|---|---|---|
| `docs/architecture/storage.md` §2 | by e4 (e0346c135, 0c9c897fa; Q198), and D1's sentence after step 4 in the D1 stack | the steps, `rejected-stale-change`, a node that refuses its move |
| `docs/architecture/node-rpc-envelope.md`, NodeCommand op `0x22` | by e4 (e0346c135) | `StorageStep`, `StorageStepped`, `RejectedStaleChange` |
| `docs/eng/consumer-guide.md` | by e6 (394edf9f5) | three rows linking the feature docs |
| `docs/features/topology-storage-provider/` | by e6 (394edf9f5) | the overview, how-to and runbook |
| build.md §5.3, the lifecycle index | unchanged | §5.3 names the five stores; the index names Storage and the change |
| node-rpc-envelope.md, Node properties; node-admin-lifecycle.md (boot) | unchanged, owed | neither names `topology_storage_provider`, which the node row carries (`NodeView`) and the launch writes (`RDM_TOPOLOGY_STORAGE_PROVIDER`): architecture text, Luke's |
| the env table (`CLAUDE.md`) | unchanged | `RDM_TOPOLOGY_STORAGE_PROVIDER` is already in it; `RDM_STORE_DIR`, named by storage.md, is not: Luke's to add |
| `docs/reference/rdm/` | unchanged | generated into rafka-v2 by `scripts/docs-site.sh`; this repository's copy predates the storage spans |
| `docs/plans/proposal/fabric-storage-provider.md` | unchanged | marked implemented only once R15 holds |

## § Dispatch ordering — maximum parallelization, four available implementers

| Wave | Epics | File ownership / reason |
|---|---|---|
| 1 | e1 | establish schema, registrations and RED canary/seed; shared metadata and boot interfaces |
| 2 | e2 | consumes e1 types; handle inventory and issued-write accounting precede blocked changes |
| 3 | e3 | consumes e2 handles; order/progress/claims are prerequisites of the decision |
| 4 | e4 | consumes e3 verified-copy state; shared storage and Build files require sequencing |
| 5 | e5 | full acceptance consumes e4; fixture tests may be authored earlier against agreed interfaces |
| 6 | e6 | final docs and evidence consume passing e5; authored by the builder or with its complete evidence |

Critical path: e1 → e2 → e3 → e4 → e5 → e6. Up to four implementers are available, but shared files
are never dispatched concurrently without an explicitly disjoint inventory. Test authoring can
proceed alongside production work once the fixture interfaces are fixed; this does not weaken RED
ordering. Any durable format move follows the repository marker and fresh-estate rule in its own
landing wave before subsequent acceptance attaches. This PRD creates no worktrees or tasks.

## § Retro (on ship)

Complete at shipment with evidence: what surprised the implementer, which predicted failures
occurred, what the seed missed and how it was strengthened, process changes, shipped SHAs, gate
counts/durations, scheduled-run evidence and trace links. No shipment or passing result is asserted
by this initial PRD.
