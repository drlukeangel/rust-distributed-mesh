# Test harnesses: the estate, the rig and node-admin

RDM's test harness is the `rdm-test` crate (`crates/rdm-test`): test-kit probes, proof
protocols, the estate and scenario runner, evidence, and the chaos kit's fault backends (its
`Cargo.toml` description). A cell drives a fabric through node-admin's public surface, Node RPC,
and proves what happened from replies, views and spans.

## 0. Read the module docs first

Each harness module opens with a `//!` doc that states what it is and what it refuses. Read the one
you use before writing a cell, and grep `crates/rdm-test/src/` for a helper before adding one.

| module | what it is |
|---|---|
| `crates/rdm-test/src/estate.rs` | the blackbox estate: real `rdm-node-admin` processes started through their launch contract, reached only through public surfaces |
| `crates/rdm-test/src/typed.rs` | the estate's typed handles on the fabric (`Nodes`, `Meshes`, `Fabrics`, `Builds`) over Node RPC |
| `crates/rdm-test/src/rig.rs` | the launcher half of a functional fabric: an in-process admin side (a gossip seed, a Node RPC client, the join and topology doors a launched node calls first) |
| `crates/rdm-test/src/executables.rs` | where each created path's executable comes from |
| `crates/rdm-test/src/scenario.rs`, `runner.rs` | declarative scenario files and their runner ([testing-strategy-seed.md](testing-strategy-seed.md)) |
| `crates/rdm-test/src/soak.rs` | the soak driver: a seeded schedule of legal actions judged on invariants |
| `crates/rdm-test/src/faults.rs` and the `*_faults.rs`, `netfault.rs` | the chaos kit ([docs/architecture/chaos-kit.md](../architecture/chaos-kit.md)) |
| `crates/rdm-test/src/test_app.rs`, `test_certs.rs` | the test application's hooks and certificate material |

## 1. Two layers

```text
#[tokio::test] fn a_cell()
  │
  ├── Estate (estate.rs, typed.rs) ── live processes: rdm-node-admin, and every node it launches
  │     a cell submits Builds and reads views over Node RPC; it never reads an internal map
  │
  └── rig (rig.rs) ── an in-process admin side and a real node runtime, for a functional cell
        that needs to stand in for node-admin's side of a launch, a join or a gossip seed
```

**An estate cell** is e2e: it starts the first node-admin of a new fabric and grows the fabric
through Builds that node-admin runs. Everything goes through public surfaces: the
`rdm-node-admin` process and its Node RPC surface, the `rdm-rpc-probe` binary and the JSONL
evidence files (`estate.rs`, module doc).

**A rig cell** is functional: `rdm_test::rig::admin_side(ip, fabric)` (and its variants:
`admin_side_serving`, `admin_side_issuing`, `admin_side_deaf_to_joins`, …) gives an in-process
`AdminSide` that a real node runtime joins; `LiveMesh` holds the topology view, a Node RPC client
and a static resolver, and `echo` sends one Ping. `crates/rdm-test/tests/common/mod.rs` is the
deployment smoke shared by both providers on this rig.

A cell never starts a node by hand outside these two: every node of an estate is created by a
Build node-admin runs.

## 2. node-admin's surface

node-admin serves Node RPC only. `RDM_NODE_ADMIN_API_BIND` names the address its transport binds;
no HTTP server is bound (CLAUDE.md env table). Once serving, a node-admin prints two lines on
stdout, which is how an estate learns where it is:

- `RDM_NODE_BIRTH=<json>`: its birth, the `MeshNode` its Node RPC door serves under
  (`rdm_client::BIRTH_LINE`, `crates/rdm-client/src/peers.rs`);
- `RDM_NODE_ADMIN_API_BASE=rdm://<node id>` (`crates/rdm-admin/src/admin/boot.rs`).

`estate.rs::spawn_admin` waits up to 30 s for both and refuses an admin that prints its base
without a birth.

The client is `rdm-client` (`crates/rdm-client/src/lib.rs`): the typed objects `Nodes`, `Meshes`,
`Fabrics` and `Builds`, the `BuildStream` and `WorkflowStream` a submitted Build or workflow
answers with, and the views (`FabricView`, `MeshView`, `NodeView`, `ScopeStatus`). Each operation
is the one its lifecycle page names ([fabric-mesh-node-lifecycles.md](../architecture/fabric-mesh-node-lifecycles.md)).

From outside a test binary, `rdm-rpc-probe` makes one call over real Node RPC given the entry
node-admin's birth line (`crates/rdm-test/src/bin/rdm_rpc_probe.rs`, module doc):

```text
rdm-rpc-probe --admin-birth <birth> get|put|delete|cas --target exact:<node_id>|path:<path.name> --key <k> …
rdm-rpc-probe --admin-birth <birth> topology --target … [--mesh <name>] [--since …]
rdm-rpc-probe --admin-birth <birth> fabric-delete --target path:<the entry admin>
rdm-rpc-probe --admin-birth <birth> node-delete   --target path:<path.name>
```

It prints one JSON line, `{"outcome": "Reply"|"NotSent"|"Unserved"|"Indeterminate", …}`.
`exact:` is exactly that logical node and never follows a replacement; `path:` is whoever holds the
path now.

## 3. The estate's primitives

| call | what it does |
|---|---|
| `Estate::bootstrap(owner, fabric, mesh)` | starts the first node-admin of `fabric` on the provider `MESH_SPAWN_TYPE` selects and waits for its birth |
| `Estate::bootstrap_with_env(…, extra_env)` | the same, the first admin also given settings it hands on to every admin it launches |
| `Estate::bootstrap_on(…, set, candidate_sha, required)` | the first admin from an explicit executable binding set, validated before anything starts; a refusal is returned by name with nothing launched |
| `estate.until_fabric_ready(within)` | waits for the fabric's first ready; only then does `estate.fabric_ready()` hand out the witness every kill takes |
| `estate.submit(&spec)`, `submit_change(change)`, `create_node(mesh, kind)` | submit a Build or a workflow; the reply is a stream the cell awaits to its terminal |
| `estate.nodes_obj()`, `meshes_obj()`, `fabrics_obj()`, `builds_obj()` | the typed objects at the entry admin; a Build call the entry does not own follows the redirect to the fabric-primary |
| `estate.fabric_view()`, `mesh_view(mesh)`, `node_views()`, `node_view(name)` | the views, read over Node RPC |
| `estate.until_view(what, within, check)`, `until_node(name, what, within, check)`, `settles_to(…)` | wait, under the cell's own bound, for a view to satisfy `check` |
| `estate.enter(admin)`, `enter_fabric_primary()` | change the admin the typed objects start at |
| `estate.kill_node(node)`, `kill_pid(pid)`, `kill_bootstrap()` | SIGKILL (nothing flushed) of a node's runtime, a pid or the Day-0 admin, each only once the fabric has first been ready |
| `estate.restart_admin(data_dir)` | start a node-admin again on its own data dir |
| `estate.spans()` | every span every process of the estate wrote ([test-otlp-capture.md](test-otlp-capture.md)) |
| `estate.stop().await`, `estate.shutdown().await` | stop every runtime of the estate through its own provider and wait for each to exit |

`rdm_test::estate::wait_for(what, within, check)` polls until `check` yields `Some` and fails
naming `what` at the bound; `wait_for_observed` fails naming the last `Err` it saw as well, so a
wait that times out names what it last saw. A wait is for an outcome the cell then checks: a cell
may wait, under its own bound, for an eventual gossip fact in order to check a property that fact
carries, and never asserts when gossip arrives (rulings INDEX, Q90).

**Teardown never leaks.** `EstateScope::begin(root)` starts a reaper in its own session: when the
test binary dies, however it dies, it kills every process whose environment names the estate's
root (`RDM_ESTATE_ROOT`, or an `RDM_DATA_DIR` under it), removes the container fabric recorded in
`<root>/fabric_id` and its network, and removes the root (`estate.rs`, `REAPER`). The gate then
fails any node process of the lane's build still running after the jobs end
(`scripts/rdm-gate-at.sh`, `leaked-processes`).

**The binaries a cell runs** come from the build the cell was built with: `bin_dir()` is
`RDM_BIN_DIR`, else the target profile directory of the test executable (`estate.rs::bin_dir`).
The test kit's own executables are `rdm-node-admin` (from `rdm-admin`), and from `rdm-test`:
`faulted-node-admin` (the node-admin runtime with the test kit's stall door, op `0x75`),
`rdm-rpc-node` (the generic RPC proof node), `rdm-node` (the test kit's node executable for every
other node type), `rdm-rpc-probe`, `rdm-scenario` and `rdm-store-root-probe`
(`crates/rdm-test/Cargo.toml`).

## 4. Where cells run

| how | what runs | source |
|---|---|---|
| `scripts/cells.sh <package> lib\|main -- <filter>` | one test executable from the gate's build, at the gate's cadence | [testing-lane-scripts.md](testing-lane-scripts.md) §3 |
| `scripts/rdm-gate-at.sh <lane dir> fast\|release` | the whole suite | [testing-lane-scripts.md](testing-lane-scripts.md) §4 |
| `scripts/i143-acceptance-gate.sh <job>` | one registered acceptance job with its receipt | `tools/mesh-audit/i143-acceptance-jobs.json` |
| `scripts/i143-export-run.sh` | every registered job at HEAD, in batches | [testing-lane-scripts.md](testing-lane-scripts.md) §6 |
| `rdm-test-runner` (`tools/test-runner`) | an HTTP server, `RDM_TEST_RUNNER_BIND_ADDR` (default `127.0.0.1:19190`), behind `/api/tests/*`: the inventory of every test executable of `RDM_TESTS_TREE` (default the main checkout), runs of chosen tests, their files and traces | `tools/test-runner/src/main.rs`, `lib.rs` |

Each crate has one integration-test executable, `main` (`tests/main.rs`, `autotests = false`),
plus `rdm-test`'s `memory_estate`. In `rdm-test`'s `main`, every file in `tests/` and its family
directories is a flat module, so a stem runs alone as `main <stem>::`; when `<stem>` is the suffix
of another module's name, add `--skip <other>::` (`crates/rdm-test/tests/main.rs`, module doc).

## 5. Reproducing a gate red

The gate's log `$HOME/rust-distributed-mesh-deploy/gates-<lane>-<sha>.log` names each job
`ok`/`FAIL <job> <seconds>`. `<log>.d/jobs` holds each job's exact command line, and
`<log>.d/<job>.log` its whole output; read that log before anything else.

- A scenario stem `<stem>` ran as `rdm-test`'s `main --test-threads=8 <skips> <stem>::`: run it
  with `scripts/cells.sh rdm-test main -- --test-threads=8 <stem>::`, adding the same `--skip`
  arguments from its jobs line.
- `rdm-test:main0..3` are the shards of `rdm-test`'s other cells; the jobs line names the modules
  each ran.
- A crate's `lib`, `main` and bin executables each ran as `<package>:<target>`.
- `rdm-admin:race`, `rdm-admin:storage` and `rdm-admin:lib` are three jobs over `rdm-admin`'s lib
  executable, split by filter.
- The fast gate exported the test cadence; `cells.sh` exports the same values. A release-only job
  ran with no cadence exported.

A red is read and fixed in the lane, never run again to see whether it passes
([testing-lane-scripts.md](testing-lane-scripts.md) §4).

## 6. The release suite

`scripts/rdm-gate-at.sh <lane dir> release` runs the jobs the fast gate leaves out (its
`RELEASE_ONLY` pattern: the container proofs, the long stems, the R-shape jobs, doctests and the
no-default-features check), exports no cadence, runs 12 jobs at once and gives each release-only
job 900 s. It first builds the R-shape consumer (`scripts/i143-rshape-build-consumer.sh`) and runs
the consumer's UI tests. A landing runs the fast gate only. No script in `scripts/` schedules the
release suite; it is started by hand.
