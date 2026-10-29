# Testing strategy: RDM

How RDM's tests are organized, named, run and judged. CLAUDE.md's "Lanes, gate, landing" section
is the short form; this page is the long one.

**Read with it:**
- [testing-naming-conventions.md](testing-naming-conventions.md): test, file and module names, and
  what the name check refuses.
- [telemetry-patterns.md](telemetry-patterns.md): how RDM code emits telemetry, and the span-name
  rules.
- [test-otlp-capture.md](test-otlp-capture.md): capturing a cell's spans and asserting on them.
- [testing-harnesses.md](testing-harnesses.md): the estate, the rig and node-admin's surface.
- [testing-with-node-admin.md](testing-with-node-admin.md): driving a fabric through node-admin,
  and what its readiness means.
- [testing-mesh-lifecycle.md](testing-mesh-lifecycle.md): an estate's life inside a cell.
- [testing-strategy-seed.md](testing-strategy-seed.md): declarative scenario files.
- [testing-lane-scripts.md](testing-lane-scripts.md): which script runs at which step.

## Host prerequisites

The committed `.cargo/config.toml` uses two optional tools, each when it is in `PATH`, and falls back
without it, naming the fallback once at the build's start (README.md, "Build and test"):

- `sccache`: the rustc wrapper (`scripts/rustc-wrapper.sh`) sends vendor crates through it, and
  `CC`/`CXX` (`scripts/cc-wrapper.sh`) are `sccache cc`/`sccache c++`; without it, plain `rustc`,
  `cc` and `c++` compile;
- `mold`: the linker on Linux, passed by the rustc wrapper; without it, the default linker links.

The gate (`scripts/rdm-gate-at.sh`) requires both and refuses by name without them.

It also sets `jobs = 10` per cargo invocation and `incremental = true`.

Cells that need more than that refuse by name when the gate or the export run requires them:

- the container provider needs a reachable Docker daemon; with `RDM_REQUIRE_CONTAINER=1` a host
  without one fails the container smoke instead of skipping it (CLAUDE.md env table);
- the netfault cuts need `nft`, as root or through `sudo -n`; with `RDM_REQUIRE_NETFAULT=1` (the
  gate sets it) a cell that cannot cut fails by name (`scripts/rdm-gate-at.sh`;
  [docs/architecture/chaos-kit.md](../architecture/chaos-kit.md));
- `scripts/netless.sh` needs root or `sudo`.

## Quick reference

```bash
# one cell or stem, from the gate's build, at the gate's cadence (agent in a lane)
flock -o /tmp/rdm-gate-turn.lock scripts/cells.sh rdm-test main -- node_stop::
flock -o /tmp/rdm-gate-turn.lock scripts/cells.sh rdm-admin lib -- status_declare::

# the names a diff adds (before committing)
python3 scripts/name-check.py origin/main HEAD

# the whole suite (the lead; never the agent in a lane)
scripts/rdm-gate-at.sh . fast
scripts/rdm-gate-at.sh . release

# one registered acceptance job, and every job at HEAD
scripts/i143-acceptance-gate.sh <job-id>
scripts/i143-export-run.sh --plan
```

Never `cargo test -p` in a lane (each `-p` selection rebuilds the stack), never an exported
`CARGO_TARGET_DIR` (CLAUDE.md).

## Tiers and where they live

The tiers are CLAUDE.md's: `unit`, `functional`, `e2e`, `load`, `chaos`. RDM has no `load` tier.
Its long runs are the soak driver (`crates/rdm-test/tests/fabric/seeded.rs`,
[testing-strategy-seed.md](testing-strategy-seed.md) §2) and the R-shape composition and traffic
cells (`crates/rdm-test/tests/rshape_burn_in.rs`, run by the registry's `i143-rshape-*` jobs).

| tier | where | what it proves |
|---|---|---|
| unit | inline `#[cfg(test)] mod tests` in `<crate>/src/*.rs`, run by the crate's `lib` executable | one function's behaviour |
| functional | `<crate>/tests/*.rs`, each a module of the crate's one `tests/main.rs` executable: `rdm-admin`, `rdm-core`, `rdm-node`, `rdm-transport`, `tools/mesh-audit` | one internal mechanism, in-process; a cell may read an internal map or field when no public surface can express the assertion |
| e2e | `crates/rdm-test/tests/<family>/<file>.rs`, modules of `rdm-test`'s `main` executable; families `build/`, `connections/`, `elections/`, `fabric/`, `gossip/`, `mesh/`, `node/`, `rpc/`, `runtime/`; some files still sit at the top of `tests/` | an operation or mechanism on live processes, through public surfaces only |
| chaos | `crates/rdm-test/tests/chaos/` (the fault backends' own cells), the chaos-kit backends in `crates/rdm-test/src/*_faults.rs`, and the registry's `chaos-process`/`chaos-container` jobs | faults applied to a running estate |

`rdm-test` also has a second executable, `memory_estate` (`tests/memory_estate.rs`), which builds
without the `file-backend` feature: the memory backend's estate proof alone
(`crates/rdm-test/Cargo.toml`).

**Functional or e2e.** A cell that reads an internal map or field is functional and lives in its crate's
`tests/`. A cell that drives real processes through Node RPC, the probe and the evidence files is
e2e and lives in `rdm-test`. A crate's tests use only the crates below it; a cell that needs the
whole stack lives in `rdm-test`.

**One executable per crate.** Every crate with integration tests sets `autotests = false` and
declares one `[[test]]`, `tests/main.rs`, naming the modules. One executable is one process, so:

- process environment is set once, in the executable's `#[ctor]` in `tests/main.rs`, before any
  thread exists; a cell never sets or removes a variable (`crates/rdm-admin/tests/main.rs`,
  `crates/rdm-transport/tests/main.rs`, `crates/rdm-test/tests/main.rs`);
- a cell captures spans with a thread-scoped capture (`EvidenceCapture`, `set_default`), never a
  global subscriber or a telemetry `init_*` ([test-otlp-capture.md](test-otlp-capture.md));
- a cell owns its ids and its estate root, and clears only what it created.

## Cadence

The test executables of `rdm-test`, `rdm-admin` and `rdm-transport` set the test cadence in their
`#[ctor]` when the environment names none: `RDM_STALENESS_MS=3000`, `RDM_GOSSIP_INTERVAL_MS=500`,
`RDM_BACKBONE_INTERVAL_MS=500`. The fast gate and `cells.sh` export the same values. The release
suite exports none: each stem runs at the cadence it sets, and a node a cell launches with a
cleared environment runs at the production defaults (staleness 30 s, gossip and backbone 2 s;
`scripts/rdm-gate-at.sh` lines 22-34). The fast gate's proofs are about ordering and outcomes, not
the length of a window; a stem that still takes long at the test cadence is a defect to read.

## The gate and the record

- **A landing runs the fast gate**, through `rebase-gate-land.sh`; the release suite is a separate
  run ([testing-lane-scripts.md](testing-lane-scripts.md) §2, §4).
- **A red gate is read, never run again.** `rebase-gate-land.sh` records a red's content in
  `red-trees` and refuses to gate it again.
- **Discovery never drops silently.** The gate compares every test executable's `--list` with
  `origin/main`'s; a cell that left must have moved, or be named in a commit body as
  `removed: <module>::<cell>`.
- **Every red is green or owned.** A red has a row in the record,
  [docs/plans/i143-prd.md](../plans/i143-prd.md), stating what makes it green (CLAUDE.md "The spec
  and the record").
- **A bound that fires is evidence.** A cell is never made green by widening its bound, a retry
  loop or `#[ignore]` (rafka-v2 CLAUDE.md "Fix the root"). An `#[ignore]` names its reason and its
  owner; RDM has three today, each with its reason in the attribute
  (`crates/rdm-test/tests/fabric/declared_secrets.rs`, `crates/rdm-test/tests/fabric/topology_storage_provider.rs`).
- **Prove it works before chaos.** Fast gate, then one exported run read as a flame, then the
  long runs; soak jobs are held by `i143-export-run.sh` until the fast gate and the release suite
  are green on one commit.

## Test defects that report confidence they do not have

Name the species when you find one; each needs a different fix.

1. **Cannot fail**: the setup guarantees the asserted property. An absence assertion ("the call
   was refused", "no row was written") passes for free once the code refuses for any reason. Fix:
   make the setup able to produce the failure, and assert the specific refusal.
2. **Cannot pass**: the assertion reads something the cell does not own (a span file another
   cell writes). Fix: own it (the cell's own `EvidenceCapture`, its own estate root).
3. **Cannot discriminate**: an assertion that passes for any failure in the layer. Fix: assert the
   typed reply or refusal variant, the gRPC `code` and `detail`, or the span's reason.
4. **Pins the defect**: the assertion certifies the bug, so the fix reads as a regression. Fix:
   keep the cell's intent, correct its input and add the negative.
5. **Unexercised**: an assertion behind a check that always fails first never runs. Fix: order it
   so it fires against today's behaviour, and watch it fire.

An absence is only meaningful once the work that would have produced the evidence has finished:
a JSONL line is written at a span's end ([test-otlp-capture.md](test-otlp-capture.md) §2).

## Moving and deleting cells

- A cell that moves keeps its assertion lines verbatim; the gate's discovery check accepts a cell
  that left one executable for another under the same name.
- A cell deleted with the behaviour it proved is named in the commit body,
  `removed: <module>::<cell>`.
- Never leave a cell that does not compile; `#[ignore]` does not excuse it.
- RED first where the behaviour predates the fix: the cell fails on the code before it.

## Writing a cell

```rust
// crates/rdm-<crate>/src/<module>.rs — unit
#[cfg(test)]
mod tests {
    #[test]
    fn <actor>_<action>_<expected_outcome>() { /* … */ }
}
```

```rust
// crates/rdm-test/tests/<family>/<file>.rs — e2e, registered in crates/rdm-test/tests/main.rs
use rdm_test::estate::{Estate, Owner};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn <actor>_<action>_<expected_outcome>() {
    let estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
    // submit through the typed objects, await the stream to its terminal, read views and spans
}
```

A new e2e file is a module in `crates/rdm-test/tests/main.rs`
(`#[path = "<family>/<file>.rs"] mod <file>;`); a new stem the gate runs on its own goes in
`SCENARIOS` in `scripts/rdm-gate-at.sh`. A cell that starts processes owns an estate root
(`estate_root`, `EstateScope`) so nothing it started outlives it.
