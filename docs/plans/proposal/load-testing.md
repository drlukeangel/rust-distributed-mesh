# Proposal: load testing RDM at scale

## Problem

No RDM test measures RDM at scale. Every gate, release and export job proves behaviour, and none
proves a size or a rate:

| what exists | what it measures | its scale |
|---|---|---|
| `crates/rdm-transport/tests/flood.rs` | Node RPC between two endpoints: 32 concurrent callers must complete 200 round trips in 10 s | 2 endpoints |
| the R-shape estates (`rshape_burn_in.rs`, the soaks) | lifecycles, chaos and invariants on the Rafka shape | 2 meshes, 20 nodes |
| the release suite | behaviour of each lifecycle under faults | ≤ 20 nodes per estate |

So nothing can say how many nodes or meshes a fabric holds, how long bring-up or recovery takes as it
grows, what the gossip and Node RPC rates are at that size, or what a node-admin costs in CPU and memory.
The i143 record names the load proof as not demonstrated (i143-prd.md e10.s2: "not demonstrated by the
demo: … the load proof").

## Proposal

A load harness, `rdm-load`, runs estates of growing size on the process provider and measures each
lifecycle and rate from RDM's own telemetry. It asserts behaviour nowhere a cell already does: it
measures, and a run passes when every measure is inside its target (§Targets).

### The harness

- **Where:** a new test target `crates/rdm-test/tests/load/` and a driver `scripts/rdm-load.sh <profile>`,
  built from the gate's build (`scripts/cells.sh`), so it measures exactly what the gate proves.
- **Estates:** `rdm_test::estate::Estate` with a profile `{ meshes, admins_per_mesh, nodes_per_mesh }`
  on the process provider. The container provider is a second pass, once the process numbers hold.
- **Measurement:** spans only, read from the evidence JSONL and from ClickHouse on .99, plus `/proc`
  for CPU and RSS per process, sampled at the start, end and each step of a run. No timer in the code
  under test and no print statements.
- **Output:** one row per (profile, measure) in `rust-distributed-mesh-deploy/load/<sha>.jsonl`: the
  profile, the measure, p50, p99, max, the target, pass or fail, and the trace id of the slowest
  sample. The driver prints the table and a summary line; a failing measure names its slowest trace.
- **Placement:** outside the gate and the release suite (a run takes tens of minutes). It runs on
  demand and at each release candidate, after the export run.

### Profiles

| profile | meshes | node-admins per mesh | other nodes per mesh | total nodes |
|---|---|---|---|---|
| L1 | 2 | 2 | 8 | 20 |
| L2 | 4 | 2 | 10 | 48 |
| L3 | 10 | 2 | 8 | 100 |
| L4 | 10 | 3 | 17 | 200 |

L1 is the R-shape size and the baseline every later profile compares to. A profile runs only when the one
before it passes.

### What each profile measures

| # | measure | how, from spans | |
|---|---|---|---|
| M1 | bring-up | the Day-0 Build's acceptance to the fabric's ready (`via-on-fabric-ready`) | per profile |
| M2 | create rate | node creates per second across the Day-0 Build's sections (`deployment.update.via-step` Complete per node) | per profile |
| M3 | view convergence | a node's change (a stop, a start) to the last node-admin's view holding it (`topology.update.via-view-rebuild changed=true`) | 20 samples |
| M4 | recovery | a SIGKILLed node's stale mark (`via-member-stale`) to its re-created birth Ready, across all meshes | k kills per profile |
| M5 | Build latency | a single-node `node.create` and `node.delete` Build, accepted to `on_build_ended` | 20 samples |
| M6 | Node RPC | calls per second and p50/p99 per op (Status, Build, Join, Topology), direct and carried, at a held concurrency per node-admin | 60 s per op |
| M7 | gossip cost | bytes per second per node on the mesh channel and the backbone (`channel.update.via-publish` bytes) at steady state | 5 min steady |
| M8 | node-admin cost | CPU cores and RSS per node-admin and per node, idle and during M2/M4 | `/proc` samples |
| M9 | data plane | connection open latency and bytes per second, Direct and Proxy, N sources × M destinations | 60 s |
| M10 | soak at scale | M7 and M8 over 4 h at the largest passing profile: RSS, open files and span volume never grow without bound | L4 or the largest passing |

### Targets

Proposed, for this box (20 cores, 121 GB); each is to be confirmed or replaced (Q265):

| measure | target at L4 (200 nodes, 10 meshes) |
|---|---|
| M1 bring-up | under 60 s |
| M3 view convergence | p99 under 2 gossip intervals (4 s at the 2 s default) |
| M4 recovery | p99 under the staleness floor + 10 s (40 s at the defaults) |
| M5 Build latency | p99 under 5 s for one node |
| M6 Node RPC | at least 2,000 calls/s per node-admin at p99 under 50 ms (Status, direct) |
| M7 gossip cost | under 50 KB/s per node at steady state |
| M8 node-admin cost | idle under 0.10 cores and 0.5 GB (the alert thresholds in CLAUDE.md's env table); no growth over M10 |

### Chaos at scale

Once a profile passes, the release suite's chaos families (the R-shape adversarial cuts, partitions,
kills and holds) run once at that profile, with the same invariants their cells assert at 20 nodes. This
is the "chaos" lift (Q246): chaos at scale, not more chaos at 20 nodes.

### Multi-host

After L4 passes on one box, the same profiles run across hosts (the container provider on two or more
machines), measuring M3, M4, M6 and M7 with real network latency. This needs a second host and is a
separate step.

## Steps

1. The harness and L1, with M1, M2, M5, M8: the baseline row at L1 must match today's R-shape numbers
   (bring-up of the 2/2/3/3 × 2 estate is about 7-9 s, i143-prd.md e10.s2).
2. M3, M4, M6, M7 at L1.
3. L2, then L3, then L4. Each red row is a unit: the trace of its slowest sample read as a flame, the
   cause fixed at its root (no tuning a constant to pass without the flame that shows why).
4. M9, then M10 at the largest passing profile.
5. Chaos at scale at that profile.
6. Multi-host.

## Doc changes on acceptance

- `docs/eng/testing-strategy.md` gains a "Load" section: the harness, the profiles, how to run it and
  where the rows go.
- `docs/eng/testing-lane-scripts.md` §6 gains the `scripts/rdm-load.sh` row.
- No architecture page changes: the harness measures; it changes no contract.

## Open questions

- **Q265: the targets.** Are the targets above the right line, and is 200 nodes across 10 meshes the size
  RDM must hold first? Dev recommendation: yes, these on this box first, then multi-host.
