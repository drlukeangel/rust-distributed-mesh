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
measures, and a profile passes when every measure with a target is inside it (§Targets, "Passing and advancing").

### The harness

- **Where:** a new test target `crates/rdm-test/tests/load/` and a driver `scripts/rdm-load.sh <profile>`.
  The nodes under load run a frozen **release** build of the SHA (`cargo build --release --bins`): the
  gate's build is a debug build, and a debug build measures its own overhead on the CPU-bound paths
  (gossip encode, postcard, hashing), not RDM. Each row names the build it ran.
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
| M2 | create rate | node creates per second across the Day-0 Build's sections (`deployment.update.via-step` Complete per node) | per profile; reported, no target |
| M3 | view convergence | a node's change (a stop, a start) to the last node-admin's view holding it (`topology.update.via-view-rebuild changed=true`) | 20 samples |
| M4 | recovery | a SIGKILLed node: the kill (the harness's own span) to its re-created birth Ready, with the kill to the stale mark (`via-member-stale`) and the mark to Ready each reported on their own; kills spread across every mesh | 10 kills per profile |
| M5 | Build latency | a single-node `node.create` and `node.delete` Build, accepted to `on_build_ended` | 20 samples |
| M6 | Node RPC | completions per second and p50/p99 per op (Status, Build, Join, Topology), direct and carried, at a held concurrency against one node-admin at a time while the others idle; errors and timeouts counted and reported, never counted as completions. One further run loads every node-admin at once and is reported with no target | 60 s per op |
| M7 | gossip cost | bytes per second per node on the mesh channel and the backbone (`channel.update.via-publish` bytes) at steady state | 5 min steady |
| M8 | node-admin cost | CPU cores and RSS per node-admin and per node, idle and during M2/M4 | `/proc` samples |
| M9 | data plane | connection open latency and bytes per second, Direct and Proxy, N sources × M destinations | 60 s; reported, no target |
| M10 | soak at scale | M7 and M8 over 4 h at the largest passing profile; the first 30 minutes are not read, and between the first and the last hour after them every process's RSS grows under 10 %, its open-file count under 5 %, and the span volume per node per minute stays within 10 % of the first hour's | once, at the largest passing profile |

### Targets

Ruled Q265 (the lead, delegated by Luke, with arch-1, 2026-10-28). The targets are targets, not measured
capacity: nothing has been measured above 20 nodes, and a measure outside its target is a finding whose
slowest trace is read as a flame (Steps, 3), never a target moved to pass. For this box (20 cores, 121 GB):

| measure | L1 (20) | L2 (48) | L3 (100) | L4 (200) |
|---|---|---|---|---|
| M1 bring-up | under 15 s | under 25 s | under 40 s | under 60 s |
| M3 view convergence | under 2 gossip intervals (4 s at the 2 s default) | same | same | same |
| M4 recovery, the kill to Ready | under 60 s | same | same | same |
| M4 recovery, the kill to the stale mark | under the staleness floor + 1 gossip interval (32 s) | same | same | same |
| M4 recovery, the stale mark to Ready | under half a staleness floor + 10 s (25 s) | same | same | same |
| M5 Build latency | under 5 s for one node | same | same | same |
| M6 Node RPC | at least 2,000 completions/s against one node-admin at p99 under 50 ms (Status, direct) | same | same | same |
| M7 gossip cost | under 50 KB/s per node at steady state | same | same | same |
| M8 node-admin cost | idle under 0.10 cores and 0.5 GB (the alert thresholds in CLAUDE.md's env table) | same | same | same |
| M10 soak | the growth limits above, at the largest passing profile | | | |

M4's three targets follow [node-recovery.md](../../architecture/lifecycles/node-recovery.md) step 1 and 2: a
killed node is heard by silence, its stale mark comes once it is unheard past the staleness floor, and the
investigation runs half a floor after the mark; so the kill to Ready is the floor, half a floor, the
verdict and the create, and no shorter at the defaults.

**Samples.** Where a measure has 20 samples (M3, M5) or 10 (M4) its target is on the maximum; p50 and p99
are reported beside it and the row carries n. At the largest passing profile M3 and M5 run once more with
100 samples and the target is read on p99 there, before M10.

**Passing and advancing.** A profile passes when M1, M3, M4, M5, M6, M7 and M8 are inside their targets at
that profile. M2 and M9 are reported with no target. M10 runs once, at the largest passing profile. The
next profile runs only when the one before passes.

**Conditions.** Every run is on the frozen release build of the SHA, at the defaults
(`RDM_GOSSIP_INTERVAL_MS` 2000, `RDM_STALENESS_MS` 30000, the cert TTLs), with OTLP export to the
collector on .99, and alone on the box: the driver refuses to start while any lane is busy
(`scripts/lane-status.sh` lists a process in it) and names the busy lane, and no gate, soak or cell runs
beside it (the gate's leaked-process step kills a run's nodes). Each profile runs twice: once with
`RDM_EVIDENCE_DIR` set, for the measures' traces (the slowest sample's flame), and once with it unset,
for M6 and M8, which the evidence JSONL's own CPU and I/O would otherwise inflate. A row is compared only with a row
taken under the same conditions; the row carries the SHA, the profile and the box. A changed cadence,
build, telemetry setting or host is a new baseline, never a comparison.
M8's thresholds are CLAUDE.md's alert thresholds, set against a release-build baseline (about 0.02 cores,
0.06 GB), the build the load runs on. The gate's debug build's idle cost is read once at L1 beside it, as a
comparison and never a target; an idle release node-admin above the threshold is a finding read as a flame
before any target moves.

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
   (bring-up of the 2/2/3/3 × 2 estate is about 7-9 s, i143-prd.md e10.s2). rdm#187.
2. M3, M4, M6, M7 at L1. M6 runs first, short, to read the order of magnitude: today's only Node RPC
   load cell (`crates/rdm-transport/tests/flood.rs`) asks 200 round trips in 10 s (about 20/s from 32
   callers) against a 2,000/s target, so a gap of that size is a finding read as a flame at L1, before any
   profile ramps.
3. L2, then L3, then L4. Each red row is a unit: the trace of its slowest sample read as a flame, the
   cause fixed at its root (no tuning a constant to pass without the flame that shows why).
4. M9, the 100-sample M3 and M5 run, then M10 at the largest passing profile.
5. Chaos at scale at that profile.
6. Multi-host.

## Doc changes on acceptance

- `docs/eng/testing-strategy.md` gains a "Load" section: the harness, the profiles, how to run it and
  where the rows go.
- `docs/eng/testing-lane-scripts.md` §6 gains the `scripts/rdm-load.sh` row.
- No architecture page changes: the harness measures; it changes no contract.

## Rulings

- **Q265: the targets.** Ruled by the lead, delegated by Luke, with arch-1 (2026-10-28): 200 nodes across
  10 meshes is the first size, on this box, then multi-host; the targets above stand as targets, not
  measured capacity. arch-1's five points, each kept and applied above except where the page decides
  otherwise: (1) M4 is measured from the kill, with the kill to the mark and the mark to Ready reported on
  their own; arch-1's 40 s from the kill is under node-recovery.md's timeline, which (the mark at the floor, the
  investigation half a floor later) puts the kill to Ready at about 50 s at the defaults, so the target is
  60 s from the kill and 25 s from the mark; (2) M6 loads one node-admin at a time, counts completions and
  reports errors and timeouts, the all-at-once run reported with no target; (3) a 20-sample target is on
  the maximum, a p99 target only at 100 samples; (4) M2 and M9 are reported with no target and M10 has
  growth limits after its first 30 minutes; (5) M1 has a target per profile, the other targets are the
  same at every profile, and the measures that pass a profile are named. The conditions are frozen.
