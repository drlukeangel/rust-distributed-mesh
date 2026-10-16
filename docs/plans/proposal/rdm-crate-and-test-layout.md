# RDM crate and test layout

**Proposal.** This page sits one layer above
[rdm-code-conformance.md](/home/admin/rafka-v2/docs/plans/proposal/rdm-code-conformance.md).
That page says what each workflow, primitive and name becomes (its §0.1 workflows call primitives,
§1 renames, §2 behaviour). This page says which crate and which test file each one lives in, what
leaves the RDM crates for the demo, how the build gets fast, and the order of the landings that get
there. Line counts measured at RDM main `f2d33a9a`; the build (finding 10) at `f5ca6ab4`
(`/home/admin/rust-distributed-mesh`). R1 and R3 are answered (§7).
Folded in: the orchestrator's and a1's reviews (2026-10-12) and Luke's direction the same day: there
is no API in RDM, the API is built in the demo; every gateway, broker and compute thing goes to
`demo/`; the repo stays tight and small; the build has to be fast.

## 1. What is

The workspace has 20 members. Line counts are `crates/<m>/src` and `crates/<m>/tests`.

| member | src | tests | normal internal dependencies |
|---|---|---|---|
| rafka-node-admin-core | 54,743 | 23,401 | entity, telemetry, transport, admin-client, rpc, rpc-contract |
| rafka-mesh-transport | 10,941 | 3,898 | entity, telemetry |
| rafka-test-scenario | 6,912 | 27,309 | admin-client, rpc-contract, chaos |
| rafka-node-rpc-contract | 6,408 | 1,018 | entity |
| rafka-node-rpc-testkit | 5,794 | 8,350 | admin-core, rpc, rpc-contract, entity, transport, telemetry |
| rafka-node-rpc | 3,541 | 4,726 | rpc-contract, entity, telemetry |
| rafka-mesh-entity | 3,517 | 715 | none |
| rafka-node-admin-client | 2,778 | 0 | entity, rpc, rpc-contract |
| rafka-mesh-telemetry | 1,891 | 272 | none |
| rafka-chaos | 1,494 | 0 | none |
| rafka-node-base | 401 | 323 | rpc-contract, entity, transport, rpc, **rpc-testkit**, admin-client, telemetry |
| rafka-mesh-ops | 276 | 0 | none |
| scratch-json-dumper | 147 | 0 | entity, transport, rpc-contract, admin-core |
| rafka-consumer-fixture | 33 | 0 | node-base, admin-core |
| test-roles/gateway, /broker, /compute (packages `rafka-gateway`, `rafka-broker`, `rafka-compute`) | 10 each | 0 | node-base |
| tools: rafka-mesh-audit, rafka-docgen, rdm-test-runner | 2,444 / 2,020 / 1,531 | 2,318 / 0 / 0 | docgen uses mesh-audit |

Findings:

1. **There is no node crate.** The process every broker, gateway and compute runs is split between
   the admin crate and the test kit.
   - `rafka-node-base/src/lib.rs` (lines 20–157) boots a role through
     `rafka_node_rpc_testkit::node::{RunningNode, start_hydrating, listen_for_stop, stop_requested}`,
     `proof_store::FileProofStore` and `node_rpc::ProcessNodeRpc`.
   - The test kit in turn takes `node_self`, `join`, `app_hydration`, `topology_read`, `rafka_time`,
     `deployment`, `certs` and five more modules from `rafka-node-admin-core`.
   - So a role process links the 55k-line admin crate and the test kit.
   - [build.md, "Packages"](/home/admin/rafka-v2/docs/architecture/build.md) says the test kit is
     "the generic RPC proof node, proof protocols and probes": a proof package, not the product runtime.
2. **Node code sits in the admin crate.**
   - `node_self.rs` (1,100 lines; "Every node kind runs this one logic"), `node_name.rs`
     (`ApplyNodeName` at its subject), `node_rpc.rs` (the process's Node RPC client), `join.rs`,
     `app_hydration.rs` (the `hydrate_before_ready` hook), `topology_read.rs` and
     `connections_writer.rs`.
   - `node_self.rs` reaches into `fabric_rounds`, `fabric_state`, `model` and `connections_writer`;
     `join.rs` holds the subject's join and the admin's admission in one file. Files cannot move whole
     (§4 step 1).
3. **One file is a fifth of the admin crate:** `admin.rs` is 10,346 lines.
4. **There are three copies of an empty consumer.**
   - `rafka-consumer-fixture` (4 binaries) and `test-roles/*` (3 binaries) are each one call to
     `rafka_node_base::run(Role::<kind>())`. `demo/` is the consumer.
   - The test-roles packages are named `rafka-gateway`, `rafka-broker` and `rafka-compute`, the names
     of Rafka's product crates.
5. **Names collide with Rafka's own crates.** rafka-v2's workspace has its own `crates/rafka-node-base`
   and `crates/rafka-mesh-ops`. RDM's crates of the same names are different code.
6. **Dev-dependency loops.**
   - `rafka-node-rpc`'s tests depend on `rafka-test-scenario`, whose tests depend on
     `rafka-node-admin-core`.
   - `rafka-mesh-entity`'s tests depend on `rafka-mesh-transport` and `rafka-node-rpc`.
   - So a lower crate's tests build the crates above it, and cargo cannot start the lower crate's
     test build until the upper crates are done.
7. **An HTTP API lives in RDM.** `rafka-node-admin-core/src/http.rs` with `axum 0.8` and `tower`
   (`Cargo.toml:28, :46`); `rafka-node-admin-client`, the test kit and the scenario crate carry
   `reqwest` to call it. build.md line 73: RDM's only surface is Node RPC and the typed objects over
   it; an application that wants routes builds them in its own crate. Luke 2026-10-12: "there is no
   API; the API is built in the demo". PRD rows 994–999 and 1696 already track the removal.
8. **Tests.**
   - There are 232 test files holding 903 test functions, plus 678 unit tests inline in 96 src files.
   - Ten packages already set `autotests = false` with one explicit `[[test]]` target, `tests/main.rs`,
     that pulls the files in as modules: one test binary per crate, which is the right shape (the
     same one rafka-broker reached in rafka-v2 `ccd4bc7874`). The cost of that shape: a moved or added
     file that `main.rs` does not name silently stops being a test.
   - 38 test files carry an initiative or issue id in their name instead of a mechanism
     (`i143_acceptance_2805.rs`): 37 in `crates/` (24 of them in rafka-test-scenario) and 1 in
     `tools/mesh-audit`.
9. **Retired concepts are still in file names**, for example `node_lifecycle__node_drain.rs`,
   `mesh_lifecycle__mesh_recover.rs`, `node_rpc__restart_fence.rs`, `park_gate.rs`, `shutdown.rs`,
   `fabric_handover.rs` and `node_commands.rs`.
   - Their deletion or rename belongs to conformance §1 and §2 (PRD e12.s72–s77).
   - This page only places what remains.
10. **The build, measured.** `cargo build --tests --timings` on RDM main, sccache warm, this box
    (20 CPUs): **85 s wall, 5 m 34 s of CPU, 517 units, max concurrency 12 with `jobs = 10`**
    (`.cargo/config.toml:8`). The critical path is one crate: `rafka-node-admin-core` lib 25.1 s, then
    its lib-test 26.6 s, then the `main` test binaries of scenario (11.9 s), the test kit (11.0 s) and
    admin-core (8.8 s): about 60 of the 85 seconds are serial behind the admin crate (53,714 lines at `f5ca6ab4`). The next
    biggest unit is `aws-lc-sys`'s build script at 17.8 s, rustls's C library through iroh, which
    sccache cannot cache and which reruns on every fresh target. The graph is 524 packages; 38 crates
    appear in two or three versions (`cargo tree -d`): two `axum` and two `axum-core` (`axum 0.7`
    through `tonic 0.12` through `opentelemetry-otlp`'s gRPC exporter in `rafka-mesh-telemetry`,
    which every crate above it links; `axum 0.8` is finding 7's API), three `hashbrown`, two
    `getrandom`, two `indexmap`, two `matchit`, two `netlink-packet-core`, and more. A cold sccache
    or a gate that builds several lanes at once turns 85 s into the minutes Luke sees.

## 2. Target: six crates, and the demo

Rules:
- A role process links `node` and what sits below it, never `admin`.
- Nothing in crates 1–5 depends on `test`, including in dev-dependencies.
- Clients never depend on `admin`; `admin` may depend on `client`.
- No crate of RDM serves or defines a route, opens an HTTP listener, or depends on an HTTP server or
  HTTP client of its own.
- Each workflow lives where its lifecycle page says it runs: on the subject node → `node`; at the
  mesh-primary or fabric-primary → `admin` (conformance §0.1: `src/workflows/<page>.rs`, `src/primitives/*`).
- Proof applications never link into a production crate.
- `core` and `client` carry no iroh and no runtime: their front ends build in parallel with the
  networking stack instead of behind it.

| crate | holds | comes from |
|---|---|---|
| 1. `rdm-core` | ids and identity, the Fabric → Mesh → Node model (`path_config` included), membership and RuntimeFact types, the Node RPC contract (framing, outcomes, reserved codes, body schemas, the typed-object DTOs), span names and conventions; no socket, no iroh, no tokio runtime (`EndpointId` is bytes here, iroh's type only in `transport`) | rafka-mesh-entity, rafka-node-rpc-contract, the names half of rafka-mesh-telemetry, `model.rs` from admin-core, the DTOs of rafka-node-admin-client |
| 2. `rdm-transport` | gossip on iroh (the digest book, frames, topology snapshots), the Node RPC runtime (client, server, pool, admission), data frames, the OTLP span exporter over HTTP | rafka-mesh-transport, rafka-node-rpc, rafka-mesh-ops, the exporter half of rafka-mesh-telemetry |
| 3. `rdm-client` | the typed calls over the op families: build.create and the node, mesh and fabric objects a caller uses to drive a node-admin; what the demo, Rafka's harness and the tools link to talk to a node-admin | rafka-node-admin-client minus its DTOs (→ core) and minus `reqwest` |
| 4. `rdm-node` | what every node runs: the role boot; the subject's side of node.start, node.stop and `node.connections.delete`; `ApplyNodeName`; the subject's join; the `hydrate_before_ready` hook; the topology read; the rafka-time client; the connections writer; the process Node RPC client; `RunningNode` and hydration | rafka-node-base; from admin-core `node_self`, `node_name`, `node_rpc`, the subject half of `join`, `app_hydration`, `topology_read`, `connections_writer`; from the test kit `node.rs` (`RunningNode`, `start_hydrating`) and `node_rpc.rs` (`ProcessNodeRpc`) |
| 5. `rdm-admin` | node-admin: Build (acceptance, drive, run, executor, state, the Build topic), the fabric states and rounds, elections and the seat watch, mesh.delete and mesh investigation, rows and storage, the status apply at the mesh-primary, admission (the admin half of `join`), deployment providers and pipeline, certs, the stopped-node collector; no HTTP | the rest of rafka-node-admin-core minus `http.rs`, `axum`, `tower` |
| 6. `rdm-test` | test-kit probes and proof protocols, the proof store and oracle handlers, the scenario runner and evidence, chaos backends, the proof-node binaries that compose `rdm-node`'s runtime with their test handlers; dev-only | rafka-node-rpc-testkit (minus what moves to `node`), rafka-test-scenario, rafka-chaos |

Dependency direction: `core` ← `transport` ← `client` ← `node` ← `admin`; `test` depends on all
five, none depends on it. A node-admin is a node, so `admin` depends on `node`.

**What leaves the RDM crates for `demo/`:**
- the HTTP API: `http.rs`, its routes, `axum`, `tower`, and every `reqwest` that exists to call it.
  The demo builds its routes as thin callers over `rdm-client`, which is what build.md line 73
  already says an application does;
- the gateway, broker and compute role binaries: `rafka-consumer-fixture`'s four and `test-roles/*`'s
  three become the demo's binaries, each `rdm_node::run(Role::<kind>())` plus whatever the demo
  wants on top. RDM keeps no product role names;
- `scratch-json-dumper`.

The three tools stay in `tools/` with the prefix. The RDM repo then holds six crates, three tools
and `demo/`, and nothing in it knows a gateway from a broker except as a `node_type` string.

## 3. Tests

The tiers are CLAUDE.md's: unit, functional, e2e. No other tier name on this page or in a file name.

**The one rule (Luke 2026-10-12): the test layout mirrors a layout that already exists.** The src layout was
being broken out by family (`build_claim`, `build_drive`, `build_state`, `fabric_rounds`,
`fabric_storage`) and admin-core's functional tests started mirroring it one for one
(`tests/build_claim.rs`, `tests/connections_writer.rs`, `tests/fabric_handover.rs`); then the id files
(`i143_acceptance_2805.rs`) broke the mirror, and the scenario crate never had one
(`mesh_elections__cohort_election.rs`, thirteen `i143_acceptance_*.rs`). The rule restores it:

| tier | mirrors | where | what |
|---|---|---|---|
| unit | the function | inline `#[cfg(test)]` in `src` | one function's behaviour |
| functional | **the src layout**, path for path | `<crate>/tests/<family>/<module>.rs` proves `<crate>/src/<family>/<module>.rs`; modules of the crate's one `tests/main.rs` binary; in-process; only crates below | one src module, one test file, same path. A src module with no mirror is a visible gap; a test file with no src module is a misplaced test |
| e2e | **the architecture page that owns the mechanism**: the lifecycle index (fabric-mesh-node-lifecycles.md) for every op, and gossip.md §14, fabric-node-lifecycle-elections.md and deployment-provider.md for the framework mechanisms no op owns | `rdm-test/tests/<page>/<op or mechanism>.rs`; modules of `rdm-test`'s binary; live processes | one page, one proof file, same name: the reviewer opens both. A mechanism that spans modules or pages goes in the file of the one that makes the decision, never in a third file |

The src families become directories so the mirror has something to mirror (step 7 does it for
`admin`; the other crates are already small enough to do in their fold):

```text
rdm-admin/src/                      rdm-admin/tests/
  build/                              build/
    accepted.rs  claim.rs  drive.rs      accepted.rs  claim.rs  drive.rs
    run.rs  state.rs  op.rs  facts_read.rs   run.rs  state.rs  op.rs  facts_read.rs
  fabric/                             fabric/
    builds.rs  rounds.rs  state.rs       builds.rs  rounds.rs  state.rs
    storage.rs  handover.rs             storage.rs  handover.rs
  election/ mesh/ status/ deployment/  election/ mesh/ status/ deployment/
  workflows/<page>.rs  primitives/      workflows/<page>.rs  primitives/

rdm-test/tests/
  node/     create.rs start.rs stop.rs restart.rs delete.rs connections_delete.rs
            primary_update.rs name_update.rs topology_get.rs status.rs
  mesh/     delete.rs investigation.rs
  fabric/   create.rs state_sync.rs state_commit.rs ready_for_traffic.rs primary_update.rs
            delete.rs recovery.rs
  build/    create.rs attempt_run.rs get.rs delete.rs
  gossip/   membership_backbone.rs held_member_repair.rs unheard_peer_mesh.rs   (gossip.md §14)
  elections/ sticky_seat.rs cohort_election.rs late_authority.rs            (elections page)
  deployment/ container_proof.rs process_locator.rs                          (deployment-provider.md)
```

Nothing else: no area prefix with a double underscore, no initiative or issue id in a name, no file
that proves "a scenario". Test functions are named `<actor>_<action>_<expected_outcome>`.

- **One test binary per crate stays**, and every crate gets it: `autotests = false`, one `[[test]]`
  target, `tests/main.rs` naming the modules. `cargo test -p <crate> <module>::` still runs one mechanism.
- **One binary is one process, so every module is safe beside the others** (rafka-v2's fold, landed 2026-10-12: test-binary disk
  63.8 → 14.7 GB and test build CPU 1,137 → 568 s across 20 crates). RDM already runs one binary per crate and already breaks
  this at `f2d33a9a`: 48 `std::env::set_var` calls in 30 test files (`RDM_STALENESS_MS`, `RDM_EVIDENCE_DIR`, …) and 3
  `set_global_default` (`rafka-mesh-telemetry/tests/runtime_watchdog.rs:28`, `rafka-node-admin-core/tests/fabric_handover.rs:316`
  and `:1253`, two in one binary). The rules:
  - process env is set once, in the binary's `main.rs` before any thread exists (a `#[ctor]`), never by a test;
  - a set-once registry registers in that same ctor, never per module;
  - a test captures tracing with a thread-scoped `set_default`, never `set_global_default` or a telemetry `init`;
  - a telemetry provider's owner calls `force_flush()` then `shutdown()` in its own `Drop`, before the subscriber guard drops
    (rafka-v2's hang: a provider dropped while tracing rebuilt its callsite interest cache ran `shutdown` under the registrar
    lock and every test thread parked in `DefaultCallsite::register`);
  - a module owns its ids and clears only what it created;
  - `#[serial]` only for a cell that faults a shared plane, and a cell holding two keys takes them in the order every other
    cell does (serial_test locks in declaration order; an inversion deadlocks).
- **Accepting a fold or a merge of test files.** rafka-v2 accepted its fold with the test list compared before and after, 50
  unfiltered runs and 20 shuffled (`-Zunstable-options --shuffle`, each seed recorded), each under gdb
  (`timeout -s INT <s> gdb -q -batch -ex run -ex 'thread apply all bt 40' --args <bin>`) with the OTLP endpoint set and every
  log kept. RDM's rule is one run per change; which binds a layout landing is Luke's call. The list comparison binds either way.
- **Discovery is checked on every move.** Before and after each move: `cargo test --no-run -- --list`
  per crate, counts compared, every removed cell accounted for by name in the commit message.
- **The 38 id-named files and the `__` files are split onto the two trees.** Each cell moves into
  the src-mirror file (functional) or the lifecycle-page file (e2e) it proves, body unchanged, under
  conformance §1's names; the old file is then deleted. A cell that proves no src module and no
  lifecycle page is named in the commit as the question it is.
- **Cells for retired behaviour are deleted, not renamed**, in the conformance §2 landing that removes
  the behaviour.
- **The dev-dependency loops end.** A crate's tests use only the crates below it. Cells that need the
  whole stack live in `rdm-test`.
- **Gate stems follow the renames in the same landing.** The commit message carries the old → new stem map.

## 4. Order of landings

Each landing goes through the land script with one gate. The four branches in flight (`i143-s53-cut`,
`i143-s58-ids`, `i143-s60-part4`, `i143-s23-primary-update`) land first; no new RDM seat is opened
until step 3 is done. That is the lead's sequencing, not a ruling.

| step | what | size |
|---|---|---|
| 1 | **The extraction map**, before anything moves: for `admin.rs`, `join.rs`, `node_self.rs`, `topology_read.rs`, `connections_writer.rs` and the test kit's `node.rs`, every `pub` symbol with its target crate (shared types → `core`; subject and client behaviour → `node` / `client`; admission, deployment and reconciliation → `admin`; proof → `test`) and the `use` edges between them. This is R3's evidence (§7) | reading, one seat, no code change |
| 2 | **Replacements before deletions.** The test-role packages are live today: with no explicit binding, the node-admin's process provider launches its built-in set by product name, `rafka-broker`, `rafka-gateway`, `rafka-compute` (`admin.rs:3622-3631` at `f2d33a9a`; `estate.rs:309-316` and `faults.rs:114` name them for rebuilds and refusals). The replacement: the built-in set is two executables, the node-admin and one node executable that takes its `node_type` from the launch (`rdm_node::run`), so RDM names no product role; the demo's `rshape-*` binaries are bound explicitly through the existing binding (`bindings.resolve(kind)`, `admin.rs:3617`). Then `i143_acceptance_2776.rs` and `scripts/i143-acceptance-gate.sh:94` (which build and launch `rafka-consumer-fixture`'s four) repoint to the binding, and `rafka-consumer-fixture`, `test-roles/*` and `scratch-json-dumper` are deleted in the same landing | seat work: the built-in set, the launch's node type, five call sites |
| 3 | **The HTTP API out.** `http.rs`, `axum`, `tower` leave admin-core; the demo gets its routes over the client; `reqwest` leaves the client, the test kit and the scenario crate (PRD 994–999, 1696). `grep -rn 'axum\|"/api/' crates` returns nothing | s60's remaining REST-out work (PRD rollout step 0): at `f2d33a9a` `http.rs` is 933 lines and 14 routes, and about 339 `"/api/"` call sites remain (scenario crate 244, admin-core tests 66, client 17, tools 12); `reqwest` in the scenario crate, the client and the test kit, `axum` in admin-core and the test kit |
| 4 | **Extract `rdm-core` and `rdm-client`** from the map: entity, contract, the telemetry names, `model.rs`, the DTOs; the client over them. Mechanical once the map exists | one landing each |
| 5 | **Extract `rdm-node`** per the map; `rafka-node-base` becomes it; the test kit keeps probes, proof protocols, the proof store and the oracle; `admin` and `test` depend on `node` | the real work, one seat |
| 6 | **Fold the rest** into `transport`, `admin` and `test`: `git mv` plus path and `use` rewrites, one landing per fold, no behaviour change; the exporter half of telemetry to `transport` on the HTTP exporter (§5) | mechanical |
| 7 | **Split `admin.rs`** into conformance §0.1's workflows and primitives, with conformance §1's code-only renames | seat work |
| 8 | **Tests per §3**, one crate at a time once that crate has settled: `main.rs` per crate, the id-named files split, discovery counted | seat work |
| 9 | **The prefix**: `rafka-*` → `rdm-*` in one landing with rafka-v2's pin bump (`/home/admin/rafka-v2/Cargo.toml:26-33`), agreed with the rafka-v2 dev lead before it lands | mechanical, both repos |

**Name check.** From step 2 on, the land script refuses a diff that adds an id-named test file or a
retired word in an identifier or a filename. Scoped to identifiers and filenames: prose, quoted
evidence, rulings and commit bodies are not matched (the "durable format moved" substring lesson).
From R24 onward, the land script also rejects added runtime calls to `unauthorized(..)` and added runtime
construction of `NotAuthority::SenderNotSubject`. Codec definitions, classifiers, reserved-byte fixtures and
test assertions are exempt; comments are not matched. Location under `rdm-core/src` or `rdm-test/src` alone
does not exempt executable handler code. Reserved discriminants remain inert.

**Durability.** Pure relocation preserves formats: a package, crate, module or file move changes no
postcard row, topic address, descriptor or catalog output, and each such landing proves it by
comparing the wire bytes of one frame of each family, the sealed catalog and the store-root paths
before and after. A conformance rename of a serde-tagged name landed in the same commit does carry
`durable format moved`; keep them in separate commits so the marker is never a guess. The prefix
rename changes every `Cargo.toml` that names a package, in RDM and in rafka-v2's pins: one landing on
both sides.

**Who does what.** The lead runs steps 2, 3, 4, 6 and 9 as scripted moves (`git mv`, `sed` over paths and
`use`, `cargo check`, the gate). Steps 1, 5, 7 and 8 each go to one seat per landing; the brief names
the files, the target row in §2 and the conformance section, and nothing else to read.

**Docs.** build.md's "Packages" table and the i143 PRD §4 list the old packages. Their new text goes
in on Luke's yes to the exact wording, after R3.

## 5. The build: 85 seconds to tens of seconds

Finding 10 is the baseline: 85 s wall, 334 s of CPU, concurrency capped at 10 on a 20-CPU box, and a
critical path that is one crate. What makes it fast, in the order it lands:

| cause | measured | fix | step |
|---|---|---|---|
| `jobs = 10` in `.cargo/config.toml:8` on 20 CPUs | max concurrency 12; half the box idle during the dependency phase. The cap is deliberate: a memory budget so four agent lanes and a gate fit the box (measured 2026-10-08, the comment above the line) | keep the budget, make it per invocation: the lane scripts pass `--jobs` from the lanes actually running (`20 / (lanes + 1)`), so one agent alone gets the box and four share it; the config line stays as the floor | §6 |
| one 55k-line crate | admin-core lib 25.1 s, then its lib-test 26.6 s, then three test binaries behind it: ~60 s of the 85 s is serial. rustc's front end (parse, resolve, type-check, borrow-check) is one job per crate; nothing else waits on a crate the way everything waits on this one | six crates: six front ends in parallel, `admin` the only large one, and after step 7 `admin` itself is workflows and primitives instead of one 10,346-line file | 4–7 |
| dev-dependency loops | the entity and rpc crates' tests cannot start until scenario and admin-core are built | tests use only crates below; cross-stack cells in `rdm-test` | 8 |
| `aws-lc-sys` build script | 17.8 s, the single slowest unit after admin-core; a C build sccache cannot cache, rerun on every fresh target | keep per-lane targets warm (`scripts/warm-bin-store.sh` already does for bins); check whether iroh's rustls can take the `ring` provider instead, which removes the C build entirely; if iroh forces aws-lc, it is iroh's and stays | 6 |
| the gRPC OTLP exporter | `opentelemetry-otlp`'s `grpc-tonic` pulls `tonic`'s transport, `hyper`, `tower 0.4` and `axum 0.7` into every crate above telemetry | the `http-proto` exporter (hyper client); `axum 0.7`, `axum-core 0.4`, `matchit 0.7`, `tower 0.4` and `hyper-timeout` leave; `tonic` and `prost` stay as `opentelemetry-proto`'s message codegen, which `http-proto` requires; the exporter lives in `transport`, not `core` | 6 |
| the HTTP API | `axum 0.8`, `tower`, and three crates' `reqwest` exist for routes RDM should not have | out to the demo | 3 |
| 38 crates in two or three versions | each version compiled once more | after steps 3 and 6, `cargo tree -d` is read again and the remaining duplicates pinned to one version in the workspace `Cargo.toml`; a duplicate iroh forces is left | 6 |
| `core`, `client` and the contract behind iroh | the leaf crates' tests and the client pull iroh and tokio, so even they wait on the networking stack | `rdm-core` and `rdm-client`'s DTOs carry no iroh and no runtime | 4 |
| test-only items behind a feature | rafka-v2: a workspace-wide `cargo check --tests` passed while `cargo check -p <crate> --all-targets` failed (feature unification turned the feature on) | RDM has no `test-helpers` feature today; when one appears, the gate checks per package and each caller's feature forwards the callee's | — |
| build identity | a git SHA stamped by a build script changes every binary on every commit and defeats artifact caching | RDM stamps none today (its `build.rs` files are the Build module, not build scripts); a stamp is a content key or nothing | — |
| `[profile.dev.package."*"] opt-level = 2` | every dependency optimised in a debug build; right for the cells (the comment in `Cargo.toml:93-96` says why) and cached by sccache | kept; it only costs on dependency churn, which steps 3 and 6 reduce | — |

**Measured in a lane, 2026-10-12 (e7s3 @ `f2d33a9a`, the gate's selection; two seats building beside it):**

| step | incremental off (today's config) | workspace crates incremental |
|---|---|---|
| no change | 0.5 s | 0.3 s |
| edit `admin.rs` | 55.3 s: admin-core lib 37.8 s, its lib-test 47.3 s in parallel, three test binaries behind it | 8.5 s: lib 4.8 s, lib-test 4.8 s |
| edit a function body (`node_name.rs`) | — | 8.9 s |
| first build of the workspace crates | 55.8 s | 69.8 s |
| `cargo test -p rafka-node-rpc-testkit --no-run` after the gate's build, no change | 49.4 s | — |
| `cargo test -p rafka-node-admin-core --lib --no-run` after that, no change | 44.1 s | — |

Two causes of the ten-minute lane build, both fixed in the lane scripts the same day:
- **One copy of admin-core per `-p` set.** Each selection resolves its own features (the testkit's `testkit-skew` default,
  `rig` only with the scenario crate, every dev-dependency's features), so a seat alternating `cargo test -p A`, `-p B` and a
  scenario build compiles admin-core once per shape, after every edit. The runner (`cells.sh <package> [<target>|lib] -- <args>`)
  always builds the gate's one selection and runs the named binary from it: switching crates costs 0.4 s.
- **`incremental = false` for workspace crates**, whose comment reasons that sccache cannot cache incremental units. Workspace
  crates never go through sccache (`scripts/rustc-wrapper.sh` sends them to plain rustc), and cargo never applies incremental to
  registry or git crates, so incremental costs sccache nothing. The runner and the gate export `CARGO_INCREMENTAL=1`: an edit is
  8.5 s; the incremental cache is 5.1 GB per lane. RDM's own config moves to the same value in one landing once main is green
  (after s60), and the export comes out of the scripts with it.

**The suite, measured the same day (fast gate on `f2d33a9a`, lane e7s3):** 119 s, its critical path admin-core's test job
(116 s). Its two binaries ran chained in one job: the lib 39.7 s, all of it one cell
(`record_store::tests::a_list_racing_a_removal_answers_the_records_that_remain`: 20 rounds of 400 fsync'd writes and removes
raced against a listing loop), and main 30.4 s, all of it one cell (`i143_acceptance_2805::pending_handoff_ends_ok_only_on_certainty_…`,
which waits out the hard-coded 20 s reachability bound at `admin.rs:4481` and a lost-reply deadline). The other 754 cells sum
to under 70 s. Since then:
- the gate runs one job per test executable: 119 → 89 s;
- the store cell proves the vanished-file skip exactly (two writes, 0.04 s, red without the `NotFound` arm), on `i143-harness`;
- the 2805 cell waits on a wall-clock constant: making the reachability bound a config value (20 s by default) is a node-admin
  parameter and waits for Luke's yes;
- `-j 20` against `-j 10` on a clean rebuild of the workspace crates: 65.1 s against 69.7 s. The critical path is admin-core's
  lib (40 s) then its lib-test (52 s), each one crate, so the job count is not the lever; the crate split and incremental are.

Measure, not guess: every landing from step 3 on records `cargo build --tests --timings` in its row
(wall, CPU, max concurrency, the critical path the report names), and the number only goes down. The
target Luke set: a full test build in tens of seconds on a warm sccache, and `cargo test -p <crate>`
for one crate well under that.

## 6. Scripts: what changes so a lane sees the fast build

Luke 2026-10-12: the ten-minute build was one agent alone, so it is not contention. A lane pays for
what the warm checkout did not: a cold target, cold workspace crates (the rustc-wrapper sends only
vendor crates through sccache, workspace crates always compile), the `aws-lc-sys` C build, and any
flag drift between invocations. The scripts own that, and they change with the layout:

| script | today | becomes |
|---|---|---|
| `scripts/lane-prep.sh` / the RDM lane bring-up | prepares the lane and births its mesh; the first `cargo test` in the lane compiles everything | runs `cargo build --tests --timings` as the lane's last prep step, so the seat's first command finds a warm target and the lane's row carries the cold number; `warm-bin-store.sh` warms the test artifacts, not only the node bins |
| the gate (`rdm-gate-at.sh`, per-lane flock) | builds and runs the stems | records `--timings` for its build and writes wall, CPU, max concurrency and the critical-path crate into the landing row; refuses a landing whose full test build is slower than the previous one by more than the noise floor it measures, so the number only goes down |
| `.cargo/config.toml` `jobs = 10` | one cap for every invocation | stays as the floor; the lane scripts pass `--jobs` computed from the lanes running, so one agent alone gets the box |
| `scripts/rustc-wrapper.sh` | vendor crates through sccache, workspace crates plain | unchanged; workspace crates compile incrementally (`.cargo/config.toml` `incremental = false` goes; the lane scripts export `CARGO_INCREMENTAL=1` until it does) |
| the seat's own cargo invocations | `cargo test -p <crate>` per cell, a different selection per crate | `cells.sh`: every cell, suite and stem through the gate's one selection, scenario stems at the gate cadence |
| the lane's shell | `RUSTFLAGS`, `CARGO_TARGET_DIR` or feature flags set per command invalidate the previous build | `lane-env.sh` sets one environment for every cargo invocation in the lane and refuses a seat command that overrides `CARGO_TARGET_DIR` or `RUSTFLAGS` (the sccache-key lesson of 2026-10-06) |
| `scripts/i143-acceptance-gate.sh`, `i143_acceptance_2776.rs` | launch `rafka-consumer-fixture` and `test-roles/*` | launch the demo's binaries (step 2) |
| the land script | the name check (§4) | adds the discovery count (§3) and the timings row |

**What is obvious enough to do now, before R3**, each its own small landing with its timings row:

1. the per-invocation `--jobs` from the lane scripts (no code change);
2. the test-artifact warm in lane prep and the timings row in the gate (scripts only);
3. the dev-dependency loops: the cells in `rafka-mesh-entity/tests` and `rafka-node-rpc/tests` that
   need transport, rpc, scenario or admin-core move to `rafka-test-scenario/tests` as they are; the
   loops end and the leaf crates' test builds stop waiting (test placement, no behaviour change);
4. the OTLP exporter from `grpc-tonic` to `http-proto` in `rafka-mesh-telemetry`: one feature flip and
   the exporter builder, measured by the packages that leave the graph (`axum 0.7`, `axum-core 0.4`,
   `matchit 0.7`, `tower 0.4`, `hyper-timeout`; `tonic` keeps only its codegen);
5. the HTTP API out of admin-core (step 3, the PRD row already owned);
6. `admin.rs` into workflows and primitives (step 7, e12.s75, already a story): the one change that
   shortens the critical path without the crate split, because a 10,346-line module is where rustc's
   front end spends its time in that crate.

7. the process-global mutations in the test binaries (§3): the 48 `set_var` calls to each binary's ctor, the 3
   `set_global_default` calls to thread-scoped captures. Test code only, one landing per crate, the discovery count unchanged.

Not now, and named so nobody starts it: `rafka-mesh-transport/src/membership.rs` is 7,967 lines of
gossip. Splitting it into modules is a file move with no behaviour change, but it is gossip, so it
waits for Luke's yes to that exact split and lands with its flame.

## 7. Rulings, one at a time

**R1 — the crate prefix: `rdm-` (Luke, answered).** Every RDM package, and the three tools, take `rdm-`:
it matches the `rdm.*` spans and `RDM_*` env vars, and ends the collision with rafka-v2's own
`rafka-node-base` and `rafka-mesh-ops`.

**R3 — the shape: six crates and the demo (Luke, answered: A).** `core`, `transport`, `client`, `node`,
`admin`, `test`, with the HTTP API and every product role binary in `demo/`, and the proof applications
in `test` (§2). Step 1's map is [`rdm-extraction-map.md`](rdm-extraction-map.md).

R2 (the branches in flight) is withdrawn: it is the lead's sequencing, stated at the top of §4.
