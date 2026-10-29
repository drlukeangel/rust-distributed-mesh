# The load harness

RDM's load harness measures RDM at scale: how long a fabric takes to come up across each phase, how fast it
creates and deletes nodes, how fast topology views converge across node-admins, how fast a killed node
is marked stale and recovered, what Node RPC and steady-state gossip cost per node, and what an idle node
costs in CPU and memory. It measures; the behaviour cells prove the behaviour
(`crates/rdm-test/tests/load/mod.rs:1-4`). The plan and its targets are
[load-testing.md](../plans/proposal/load-testing.md) (Q265), and every run is recorded in
[load-experiments.md](../plans/reports/load-experiments.md).

## Where it is

| what | where |
|---|---|
| the cells | `crates/rdm-test/tests/load/mod.rs:384-435`, in `rdm-test`'s `main` test target (`load_profile_<l1\|l2\|l3\|l4>_baseline` and `load_profile_<l1\|l2\|l3\|l4>_no_evidence`) |
| the driver | `scripts/rdm-load.sh <L1\|L2\|L3\|L4> [--debug] [--no-evidence] [--perf=<dir>]` (`scripts/rdm-load.sh:13-23`) |
| the rows | `$HOME/rust-distributed-mesh-deploy/load/<short sha>.jsonl`, one JSON object per measure row, appended by `Rows::finish` (`crates/rdm-test/tests/load/mod.rs:101-124`) |
| the record | `docs/plans/reports/load-experiments.md`: one entry per run (`docs/plans/reports/load-experiments.md:1-6`) |

Every load cell is annotated with `#[ignore]` (`crates/rdm-test/tests/load/mod.rs:383-432`), so no gate or
release run starts one: a run takes minutes and loads the box. Only `scripts/rdm-load.sh` runs them, passing
`--ignored` to `scripts/cells.sh` (`scripts/rdm-load.sh:36`).

## Running it

```bash
scripts/rdm-load.sh L1                                               # the 20-node baseline with span evidence
scripts/rdm-load.sh L1 --debug                                       # the same on target/debug, for comparison
scripts/rdm-load.sh L1 --no-evidence                                 # RDM_EVIDENCE_DIR unset: the pass M6 and M8 are read from
scripts/rdm-load.sh L1 --no-evidence --perf=<dir>                    # also CPU profiles (perf record -F 499 -g) into <dir>
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 scripts/rdm-load.sh L1  # export spans and logs over OTLP/HTTP to Jaeger
```

The driver (`scripts/rdm-load.sh:13-36`):

1. builds the nodes under load as a **release** build of the checkout (`cargo build --release --bins -q`,
   `scripts/rdm-load.sh:28-30`) unless `--debug` is passed (`scripts/rdm-load.sh:18`, `:25-26`). The gate's
   build is a debug build, and a debug build measures its own overhead on CPU-bound paths, not RDM's
   (`docs/plans/proposal/load-testing.md:28-30`). `Rows::new` records `"build": "release"` when
   `rdm_test::estate::bin_dir()` contains `"/release"` and `"debug"` otherwise
   (`crates/rdm-test/tests/load/mod.rs:52`);
2. exports the **production cadence** (`RDM_STALENESS_MS=30000`, `RDM_GOSSIP_INTERVAL_MS=2000`,
   `RDM_BACKBONE_INTERVAL_MS=5000`: staleness 30 s, gossip 2 s, backbone 5 s; `scripts/rdm-load.sh:34`, Q267)
   before invoking `scripts/cells.sh`, overriding the fast test cadence that `scripts/cells.sh` sets only when
   unset. `cadence()` records `{"staleness_ms": 30000, "gossip_ms": 2000, "backbone_ms": 5000}` on every row
   (`crates/rdm-test/tests/load/mod.rs:112`, `:129-132`);
3. points the estate at the node binaries with `RDM_BIN_DIR="$bins"` and runs
   `load::load_profile_<lower>_<cell>` through `scripts/cells.sh`, printing each `[load]` summary line, the
   rows file path, and any missed-target assertion failure (`scripts/rdm-load.sh:36`,
   `crates/rdm-test/tests/load/mod.rs:118-123`).

### Options and environment

- `--no-evidence` selects `load_profile_<lower>_no_evidence` (`scripts/rdm-load.sh:19`, `:36`), which passes
  `evidence = false` to `run_profile` and starts the estate with `[("RDM_EVIDENCE_DIR", "")]`
  (`crates/rdm-test/tests/load/mod.rs:391-435`, `:450`; `Estate::bootstrap_with_env` leaves an environment
  variable with an empty value unset, `crates/rdm-test/src/estate.rs:696-698`). With `evidence` false, no node
  writes span JSONL files: M6 runs only on this pass and M8 is read from it so JSONL writer CPU and I/O do not
  inflate them (`crates/rdm-test/tests/load/mod.rs:437-438`, `:477-512`), M3 polls `node.topology.get` instead
  of reading spans (`crates/rdm-test/tests/load/mod.rs:547-550`), and the span-only rows (M2, M7, and M4's
  stale-mark split) are omitted (`crates/rdm-test/tests/load/mod.rs:464-468`, `:515-524`, `:576-594`).
- `--perf=<dir>` exports `RDM_LOAD_PERF=<dir>` (`scripts/rdm-load.sh:20`). When `RDM_LOAD_PERF` is set,
  `profile_one` runs `perf record -q -F 499 -g -p <pid> -o <dir>/<label>.<pid>.perf.data -- sleep <secs>`
  (`crates/rdm-test/tests/load/mod.rs:200-212`) on one live process of each role (`node-admin` and `rpc-node`)
  over M8's 20 s idle window (`profile_one_per_role`, `crates/rdm-test/tests/load/mod.rs:193-196`, `:362-368`,
  `:472`) and, on the `--no-evidence` pass, on M6's target `node-admin` (`m6-<at>`) over the 32-caller 60 s
  window (`crates/rdm-test/tests/load/mod.rs:487-491`). `Rows::finish` marks every row of the run
  `"profiled": true` (`crates/rdm-test/tests/load/mod.rs:113`).
- `OTEL_EXPORTER_OTLP_ENDPOINT` configures OTLP/HTTP export for Jaeger or another collector
  (`docs/architecture/telemetry.md:14-15`). `scripts/cells.sh` exports no `OTEL_EXPORTER_OTLP_ENDPOINT`
  (`docs/eng/testing-lane-scripts.md:124`); when set in the caller's environment, the estate and each
  `node-admin` pass it through (`crates/rdm-test/src/estate.rs:231`,
  `crates/rdm-admin/src/admin/records.rs:31`) to `init_evidence_telemetry`, which ships spans to
  `<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/traces` and log records to `<OTEL_EXPORTER_OTLP_ENDPOINT>/v1/logs`
  whether or not `RDM_EVIDENCE_DIR` is set (`crates/rdm-transport/src/telemetry.rs:645-678`).

No lock is taken (`scripts/rdm-load.sh:9-10`). A run may share the host with other work: `box_context()`
records the host's `/proc/loadavg` and the busy worktrees from `scripts/lane-status.sh` at the start and end
of the run (`box_start` and `box_end`, `crates/rdm-test/tests/load/mod.rs:53`, `:102`, `:114-115`, `:163-172`),
so a row taken under background load is never read as a quiet-host baseline. Never run a gate in the same
checkout while a load run is active: the gate's leaked-process step kills the run's nodes
(`docs/plans/proposal/load-testing.md:106`).

## Profiles

Each `Profile` (`crates/rdm-test/tests/load/mod.rs:11-23`, `:384-435`) defines the number of meshes, the
`node-admin` and `rpc-node` count per mesh, and M1's bring-up target:

| profile | meshes | node-admins per mesh | rpc nodes per mesh | total nodes | M1 target | cells (`crates/rdm-test/tests/load/mod.rs`) |
|---|---|---|---|---|---|---|
| `L1` | 2 | 2 | 8 | 20 | 15 s (`:20`) | `load_profile_l1_baseline` (`:384`), `load_profile_l1_no_evidence` (`:391`) |
| `L2` | 4 | 2 | 10 | 48 | 25 s (`:21`) | `load_profile_l2_baseline` (`:398`), `load_profile_l2_no_evidence` (`:405`) |
| `L3` | 10 | 2 | 8 | 100 | 40 s (`:22`) | `load_profile_l3_baseline` (`:412`), `load_profile_l3_no_evidence` (`:419`) |
| `L4` | 10 | 3 | 17 | 200 | 60 s (`:23`) | `load_profile_l4_baseline` (`:426`), `load_profile_l4_no_evidence` (`:433`) |

A profile runs once the one before it passes (`docs/plans/proposal/load-testing.md:51-52`, `:97-99`).

## Where the rows go and what a row holds

`Rows::finish` creates `$HOME/rust-distributed-mesh-deploy/load/` if needed and appends one JSON line per
measure row to `$HOME/rust-distributed-mesh-deploy/load/<sha[..9]>.jsonl`, where `sha` is
`rdm_test::faults::candidate_sha()` (`crates/rdm-test/tests/load/mod.rs:51`, `:101-124`). After writing the
file, `Rows::finish` prints `[load] rows: <path>` and asserts `self.missed.is_empty()`, panicking with every
missed target if any check failed (`crates/rdm-test/tests/load/mod.rs:122-123`).

```json
{"measure":"M5","what":"one node.create Build","unit":"ms","n":20,"p50":226.0,"p99":247.0,"max":247.0,
 "target":5000.0,"pass":true,"slowest_trace":null,"sha":"6cb82a385…","profile":"L1","build":"release",
 "evidence":false,"cadence":{"staleness_ms":30000,"gossip_ms":2000,"backbone_ms":5000},"profiled":false,
 "box_start":{"loadavg":"3.10 2.80 2.40","busy_lanes":["2","7"]},"box_end":{"loadavg":"2.90 2.75 2.40","busy_lanes":["2"]}}
```

Row shapes emitted by `Rows` (`crates/rdm-test/tests/load/mod.rs:57-116`):

- `Rows::time` (`crates/rdm-test/tests/load/mod.rs:57-66`): sorts `samples` (in `ms`) and records `n`, `p50`,
  `p99`, `max`, `target`, `pass` (`max <= target_ms` when `target_ms` is `Some`, or `null` when `None`), and
  `slowest_trace`. With 10 or 20 samples the target is checked against `max`
  (`docs/plans/proposal/load-testing.md:93-95`).
- `Rows::time_p99` (`crates/rdm-test/tests/load/mod.rs:88-98`): records a timed row and checks `pass` as
  `n >= 100 && p99 <= target_ms` (`samples_enough`, `crates/rdm-test/tests/load/mod.rs:134-136`), setting
  `target_p99` to `target_ms` (used by M6 at 32 callers over hundreds of thousands of calls).
- `Rows::value` (`crates/rdm-test/tests/load/mod.rs:69-75`): records a single number `value` in `unit`, an
  optional upper bound `target`, and `pass` (`v <= target`).
- `Rows::at_least` (`crates/rdm-test/tests/load/mod.rs:78-84`): records a rate `value` in `unit`, a lower
  bound `target_at_least`, and `pass` (`v >= floor`).
- Common metadata added to every row by `Rows::finish` (`crates/rdm-test/tests/load/mod.rs:107-116`):
  `sha`, `profile`, `build` (`"release"` or `"debug"`), `evidence` (`true` or `false`), `cadence`
  (`{"staleness_ms", "gossip_ms", "backbone_ms"}`), `profiled` (`true` when `RDM_LOAD_PERF` is set),
  `box_start`, and `box_end`. A row is compared only with one taken under the same `profile`, `build`,
  `evidence`, and `cadence` (`crates/rdm-test/tests/load/mod.rs:38-42`, `:127-128`).

## Measures (M1–M8)

`run_profile` (`crates/rdm-test/tests/load/mod.rs:439-612`) runs the measures in the order below:

| measure | pass | rows (`what`) | how it is measured and code lines | target |
|---|---|---|---|---|
| **M1** (bring-up) | both | `bring-up: day-0 admin up`<br>`bring-up: the Day-0 Build complete`<br>`bring-up: every node ready in the view`<br>`bring-up, launch to ready` | Wall-clock elapsed (`ms`) from before `Estate::bootstrap_with_env` (`t0`, `crates/rdm-test/tests/load/mod.rs:446`) to: (1) the bootstrap admin up (`:447-448`, `:460`), (2) `Estate::until_complete` on `estate.submit_shape("fabric1", &shape)` (`:449-451`, `:461`), (3) `estate.settles_to(&shape, Duration::from_secs(300))` (`:452-453`, `:462`), and (4) `estate.until_fabric_ready(Duration::from_secs(300))` (`:454-455`, `:463`). The three phase rows isolate which phase grew when bring-up moves. | phase rows: reported (`None`)<br>`bring-up, launch to ready`: `15000` ms (`L1`), `25000` ms (`L2`), `40000` ms (`L3`), `60000` ms (`L4`) (`:20-23`, `:463`) |
| **M2** (create rate) | `evidence` only | `node creates per second` | Count of `rdm.node_admin.deployment.update.via-step` spans with `attributes.step == "Complete"` and `attributes.outcome == "complete"` divided by `bring_up.as_secs_f64()` (`crates/rdm-test/tests/load/mod.rs:464-468`). | reported (`None`, `creates/s`) |
| **M8** (idle cost) | both (read from `--no-evidence`) | `<role> idle cpu`<br>`<role> idle rss`<br>for `node-admin` and `rpc-node` | After a 10 s settle sleep (`crates/rdm-test/tests/load/mod.rs:471`), `idle_cost` samples `/proc/<pid>/stat` (`cpu_ticks`, user + system ticks at 100 Hz, `:175-179`) and `/proc/<pid>/statm` (`rss_gb`, resident pages × 4096 / 10⁹, `:182-186`) over 20 s across `estate.live_runtimes()` and records the maximum CPU cores and RSS GB per role (`:214-221`, `:361-380`, `:472-475`). | `idle cpu`: `<= 0.10` cores (`:473`)<br>`idle rss`: `<= 0.5` GB (`:474`) |
| **M6** (Node RPC) | `--no-evidence` only | For `callers` in `32` and `128`:<br>`topology.get at <at>, <callers> callers, completions`<br>`…, calls without a reply`<br>`…, the node's cpu`<br>`…, the callers' process cpu`<br>`topology.get at <at>, <callers> callers` | `rpc_load` (`crates/rdm-test/tests/load/mod.rs:228-257`) spawns `concurrency` tasks calling `NodeRpc::topology_get` directly (`NodeTarget::CurrentPath`) for `at`'s own mesh against the last mesh's last `node-admin` (`<last_mesh>.admin.<admins>`, `:482`) for `M6_WINDOW` (`60 s`, `:260`, `:481-512`). Records completion rate (`/s`), failed calls (`count`), target node CPU (`cores`), test process CPU (`cores`), and latency (`ms`). | at `32` callers (held concurrency): `>= 2000.0` `/s` (`Rows::at_least`, `:495`) and `p99 <= 50.0` ms (`Rows::time_p99`, `:507`); at `128` callers (saturation probe) and CPU/error rows: reported (`None`, `:497-510`) |
| **M7** (gossip cost) | `evidence` only | `largest node, all channels`<br>`largest node, backbone`<br>`largest node, mesh` | Sleeps `STEADY` (`300 s`, `crates/rdm-test/tests/load/mod.rs:263`, `:517`), then `gossip_bytes` sums `attributes.bytes` across every `rdm.mesh.topology.update.via-frame` span in `[from, to)` by `(node, channel_kind)`, where `channel_kind` maps `mesh:<name>` to `"mesh"` and preserves `"backbone"` (`:332-359`, `:515-524`). | `largest node, all channels`: `<= 50.0` KB/s (`:520`)<br>per-channel rows: reported (`None`, `:522`) |
| **M3** (view convergence) | both (span-based with `evidence`, polled without) | `a stop to every node-admin's own view`<br>`a start to every node-admin's own view` | 20 stop-then-start cycles on `mesh1.rpc.1` (`subject`, `crates/rdm-test/tests/load/mod.rs:530`, `:535-556`). A stop reaches a node's own topology projection as `"leaving"` and is waited on at the fabric-primary with `until_stopped_at_the_fabric_primary` (`:536-546`).<br>• **With `evidence` (`spread_from_spans`, `:301-321`, `:551-554`)**: from the `mesh-primary`'s write span (`rdm.mesh.topology.update.from-command-admitted` with `status == "leaving"` for a stop; `rdm.mesh.topology.update.from-node-started` with `status == "ready"` for a start) to the last `node-admin` in `admins` applying it (`rdm.mesh.topology.update.via-member-state`, `rdm.mesh.topology.update.via-projected-status`, or the write span itself).<br>• **Without `evidence` (`every_admin_holds`, `:271-289`, `:543`, `:547-550`)**: from the workflow call's completion to every `node-admin` returning `subject` with that status in `estate.held_values_at` (`node.topology.get`), polled every 50 ms. | `n = 20` each, `max <= 4000.0` ms (2 gossip intervals at 2 s, `:557-558`) |
| **M4** (SIGKILL recovery) | both (`to_mark` and `mark_to_ready` with `evidence` only) | `the kill to Ready`<br>`the kill to the stale mark` (`evidence`)<br>`the stale mark to Ready` (`evidence`) | 10 `SIGKILL`s (`estate.kill_node`, `crates/rdm-test/tests/load/mod.rs:567-572`) of RPC nodes spread across every mesh (`meshes[k % meshes.len()]`). `every_admin_holds` polls every `node-admin` until `victim` is held `"ready"` with a `node_id` different from `before` (`:271-289`, `:570-573`). With `evidence`, reads the earliest `rdm.mesh.topology.update.via-member-stale` span for `victim` at or after `killed_ns` (`:576-588`) to split `the kill to Ready` at the stale mark. | `n = 10`:<br>`the kill to Ready`: `max <= 60000.0` ms (`:590`)<br>`the kill to the stale mark`: `max <= 32000.0` ms (`:592`)<br>`the stale mark to Ready`: `max <= 25000.0` ms (`:593`) |
| **M5** (Build latency) | both | `one node.create Build`<br>`one node.delete Build` | 20 iterations adding one RPC node to `mesh1` (`estate.submit_shape("fabric1", &bigger)` to `Estate::until_complete`) and then removing it (`estate.submit_shape("fabric1", &shape)` to `Estate::until_complete`, `crates/rdm-test/tests/load/mod.rs:596-611`). | `n = 20` each, `max <= 5000.0` ms (`:610-611`) |

## Adding a measure

- Measure from RDM's spans (`rdm_test::estate::named` over `estate.spans()`,
  `crates/rdm-test/tests/load/mod.rs:6`, `:335`, `:466`, `:578`) or `/proc`
  (`crates/rdm-test/tests/load/mod.rs:175-186`), never from a print or a timer added to the code under test
  (`docs/plans/proposal/load-testing.md:33-35`).
- Add it with `Rows::time`, `Rows::time_p99`, `Rows::value`, or `Rows::at_least`
  (`crates/rdm-test/tests/load/mod.rs:57-98`), with its target from
  [load-testing.md](../plans/proposal/load-testing.md), or `None` when it is reported only.
- Record the run that introduced it in [load-experiments.md](../plans/reports/load-experiments.md).
