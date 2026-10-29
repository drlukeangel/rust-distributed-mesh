# The load harness

RDM's load harness measures RDM at a size: how long a fabric takes to come up, how fast it creates and
deletes nodes, what an idle node costs, and (as the harness grows) how views converge, how fast a lost
node comes back, and what Node RPC and gossip cost per node. It measures; the behaviour cells prove the
behaviour. The plan and its targets are [load-testing.md](../plans/proposal/load-testing.md) (Q265), and
every run is recorded in [load-experiments.md](../plans/reports/load-experiments.md).

## Where it is

| what | where |
|---|---|
| the cells | `crates/rdm-test/tests/load/mod.rs`, in `rdm-test`'s `main` test target |
| the driver | `scripts/rdm-load.sh <profile>` |
| the rows | `$HOME/rust-distributed-mesh-deploy/load/<short sha>.jsonl`, one row per measure, appended |
| the record | `docs/plans/reports/load-experiments.md`: one entry per run |

The cells are `#[ignore]`d, so no gate or release run starts one: a run is minutes long and loads the
box. Only the driver runs them (`--ignored`).

## Running it

```bash
scripts/rdm-load.sh L1            # the 20-node baseline
scripts/rdm-load.sh L1 --debug    # the same on the gate's debug build, for comparison
scripts/rdm-load.sh L1 --no-evidence   # no node writes span evidence: the pass M6 and M8 are read from
scripts/rdm-load.sh L1 --no-evidence --perf=<dir>   # also CPU profiles (perf record -g): one node of each role
                                                    # over M8's idle window, and M6's node over its 32-caller
                                                    # window; the run's rows are marked profiled
```

The driver:

1. builds the nodes under load as a **release** build of the lane's commit (`cargo build --release --bins`).
   The gate's build is a debug build, and a debug build measures its own overhead, not RDM's. A row
   names the build it ran on (`build: release` or `debug`). `--debug` runs on the gate's build instead;
2. builds the test executable through `scripts/cells.sh` (the gate's build, as every cell is) and
   points the estate at the release node binaries with `RDM_BIN_DIR`;
3. runs the profile's cell and prints one line per measure, then the path of the rows file.

It needs no lock. A run may share the box with other lanes' work: each row records the box's load
average and the lanes that were busy at the start and end of the run, so a row taken under other load
is never read as a quiet-box baseline. Never run a lane gate in the load run's own lane: the gate's
leaked-process step kills the run's nodes.

## Profiles

| profile | meshes | node-admins per mesh | rpc nodes per mesh | nodes | M1 target |
|---|---|---|---|---|---|
| L1 | 2 | 2 | 8 | 20 | 15 s |
| L2 | 4 | 2 | 10 | 48 | 25 s |
| L3 | 10 | 2 | 8 | 100 | 40 s |
| L4 | 10 | 3 | 17 | 200 | 60 s |

A profile runs only once the one before it passes. L2 to L4 are added as the harness reaches them
(load-testing.md, Steps).

## What a row holds

```json
{"measure":"M5","what":"one node.create Build","unit":"ms","n":20,"p50":1840,"p99":2310,"max":2310,
 "target":5000,"pass":true,"sha":"…","profile":"L1","build":"release","evidence":true,
 "box_start":{"loadavg":"3.10 2.80 2.40","busy_lanes":["2","7"]},"box_end":{…}}
```

- `pass` is `true` or `false` against the target, or `null` for a measure reported with no target.
- `evidence` says whether the nodes wrote span evidence. With it off (`--no-evidence`, the cell
  `load_profile_<profile>_no_evidence`, the estate started with `RDM_EVIDENCE_DIR` unset) M2, counted from
  the spans, is not reported; M6 and M8 are read from this pass. A row is compared only with one taken
  the same way.
- A timed measure carries `p50`, `p99` and `max` over its `n` samples. With 20 samples the target is
  on the max; a p99 is a target only at 100 samples (Q265).
- A run that misses a target fails, naming every missed measure. The slowest sample's trace is the
  next thing to read (load-testing.md: a red row is read as a flame, never a target moved to pass).

## What L1 measures today

| measure | what | target |
|---|---|---|
| M1 | bring-up: the Day-0 Build of the whole profile, from the first admin's launch to the fabric ready | per profile |
| M2 | node creates per second during bring-up | reported |
| M6 | Node RPC: callers sending `node.topology.get` of its own mesh to one non-primary node-admin directly for 60 s, at 32 callers (the held concurrency) and 128 (the saturation probe, reported); completions per second, latency, calls ended without a reply, the node's and the callers' CPU; evidence-off pass only | at 32 callers: at least 2,000/s, p99 under 50 ms |
| M7 | gossip bytes per second each node takes in over 5 min steady (`rdm.mesh.topology.update.via-frame` `bytes`), the largest node's total and per channel kind; evidence pass only | 50 KB/s per node |
| M5 | one-node `node.create` and `node.delete` Builds, 20 each, accepted to complete | max under 5 s |
| M8 | idle CPU (cores) and RSS (GB), the largest per role, over 20 s once settled | 0.10 cores, 0.5 GB |

M3, M4, M9 and M10 come next, and M6 at 60 s (load-testing.md, Steps 2 to 4).

## Adding a measure

- Measure from RDM's spans (`rdm_test::estate::named` over `estate.spans()`) or `/proc`, never from a
  print or a timer added to the code under test.
- Add it with `Rows::time` (samples in ms) or `Rows::value` (one number), with its target from
  load-testing.md, or `None` when it is reported only.
- Record the run that introduced it in load-experiments.md.
