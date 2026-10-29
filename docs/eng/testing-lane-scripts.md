# The lane scripts: which one runs at which step

Every step of a unit has a script, from claiming a lane to landing its branch. Use the script: a
hand-rolled equivalent (a raw `git push`, a hand-written plan, `cargo test -p` in a lane) skips the
refusal, the gate proof or the record the script keeps.

Every script lives in this repo's one `scripts/` folder, with the helpers it calls
(`name-check.py`, `timings_row.py`, `lane-lib.sh`). Which copy runs matters:

- the gate runs the **lane's own** `scripts/rdm-gate-at.sh`, so a lane's edit to the gate is the
  gate it is proved with;
- the landing runs **origin/main's** `scripts/rebase-gate-land.sh` (`lane-land.sh` reads it with
  `git show origin/main:scripts/rebase-gate-land.sh`), so a branch cannot change the rules it lands
  under;
- the two files in `$HOME/lane-queue/i143-lead/` named `rdm-gate-at.sh` and
  `rebase-gate-land.sh` are four-line shims that do exactly that; a change goes in `scripts/`,
  never in a shim.

## 0. Where things are

| what | where | source |
|---|---|---|
| a lane | `$HOME/rdm-lanes/<n>`, a folder holding a checkout of this repo, reused for every unit | `scripts/lane-lib.sh` (`LANES_ROOT`) |
| the lane's lead-side files: `plan.md`, `inbox/`, `seat-rules.md`, `branch`, `issue` | `$HOME/lane-queue/rdm/<n>/`, never inside the lane, so the lane stays clean for its gate | `lane-lib.sh` (`STATE_ROOT`) |
| the main checkout | `$HOME/rust-distributed-mesh`: holds `main`, is not a lane | `lane-lib.sh` (`MAIN`) |
| gate logs, `green-trees`, `green-ranges`, `red-trees`, `timings-ledger.txt`, `discovery-<sha>.json`, `kept/` | `$HOME/rust-distributed-mesh-deploy/` | `lane-lib.sh` (`DEPLOY_ROOT`), `rdm-gate-at.sh` (`DEPLOY`) |
| the agent types the lead launches | the main checkout's `.claude/agents/i143-<issue>.md` | `lane-lib.sh` (`AGENTS_DIR`) |

A lane is **busy** while any process has its working directory inside it: a build, a cell, a gate
or a node (`lane-lib.sh::lane_busy`). A lane is **dirty** when a tracked file has an uncommitted
change; untracked files do not count (`lane-lib.sh::lane_dirty`). A lane whose folder moved has
test executables built with the old `CARGO_MANIFEST_DIR`; `lane_target_at_path` records where the
target was built (`target/.lane-path`) and cleans the workspace crates once on a mismatch.

## 1. A unit, end to end

| step | who | command |
|---|---|---|
| Is the lane free? | lead | `scripts/lane-status.sh [lane…]`: one row per lane with its branch, commits ahead of `origin/main`, dirty files, live processes, last commit age, issue and agent type. No lane named: every numbered lane under `$HOME/rdm-lanes`. |
| Claim the lane on a fresh branch | lead | `scripts/lane-prep.sh <lane> <branch>`: `git switch -c <branch> origin/main`. Refuses a busy lane (exit 3), a dirty lane (exit 6) or a branch name that exists (exit 4). A prep is the branch alone and takes seconds: it builds nothing (rdm#139); the seat's first `cells.sh` run builds the lane. It moves the previous unit's numbered inbox files to `inbox/done-<previous branch>/` and clears its `plan.md`, agent type and issue. |
| Hand the unit over | lead | `scripts/lane-dispatch.sh --lane <n> --issue <N> --name <name> --title <title> --design <file> [--model opus\|sonnet]`. Refuses a lane not prepped, a lane on another branch, a lane that already has an agent type (prep it again), or an issue that is not open. It writes `plan.md` (a header naming the issue, lane, branch and base commit, then the design file), writes `seat-rules.md` (the standing rules the agent reads first; item 7: the agent ends its unit with its own evidence), writes the agent type `i143-<issue>` with the model, and comments the issue with the lane and branch. |
| Launch the agent | lead | Only after `lane-dispatch.sh` returns: the Agent tool with `subagent_type: i143-<issue>` and `description: "lane <n> i143-<issue>-<name>"`, as its `SEAT TYPE` line prints. A type it calls `registered` launches at once; a `NEW` type launches only after Claude Code's "New agent types are now available" notice names it (no notice after 2 min: `touch` the file and wait again). Never headless, never in a terminal multiplexer. |
| Send the agent a note | lead | `scripts/lane-note.sh <lane> <note-file\|->`: the one way an update reaches the agent. Each note is its own numbered file `inbox/NNNN.md` (the next number after the highest), headed `# LEAD update NNNN — <date>`, written to a temporary file and moved into place. An empty note, or a lane not prepped, is refused. The agent reads every inbox file above the last one it acted on and names that number in its `STATUS` line. |
| Run cells | agent in the lane | `scripts/cells.sh <package> lib\|main -- <filter>` (§3), with no lock; independent runs go in parallel. Never `cargo test -p` in a lane, never an exported `CARGO_TARGET_DIR`. A unit's proof is its cells and the fast gate: no release, chaos, export or consumer-build run per unit. |
| Check names before committing | agent in the lane | `python3 scripts/name-check.py origin/main HEAD` (§5). |
| Prove the tip | agent in the lane | `bash $HOME/rdm-lanes/<lane>/scripts/post-land-gate.sh <lane>` (§2), not under the turn lock, at the end of its unit. Its `GREEN` line is the landing's proof. |
| Land and close the issue | lead | `scripts/lane-land.sh <lane> <branch> <issue> <close-comment-file>` (§2). `LAND_NOCLOSE=1` comments on the issue instead of closing it. |

The agent in a lane never lands, never pushes and never switches branch; it ends every turn with
`STATUS: <done|blocked|working> inbox <NNNN> commits <sha,…>`, and ends its unit with its own
evidence, which the lead reviews before anything lands: its own `post-land-gate.sh` `GREEN` line,
the cells it ran with their results, and the Jaeger links of the traces that show the fix working
(the rules `lane-dispatch.sh` writes into `seat-rules.md`, item 7).

## 2. Proving and landing

**`scripts/post-land-gate.sh <lane>`** proves the tip that will land, in the lane:

1. refuses a busy lane (exit 3) or a dirty one (exit 6);
2. rebases on `origin/main` unless the branch already contains it. A conflict only in the
   generated reference pages (`docs/reference/rdm/*.md`) is resolved by regenerating them with the
   lane's `target/debug/rdm-docgen` over the commit being applied, leaving only the pages it writes
   (spans.md, config.md), and the rebase continues; a conflict in any other file aborts the rebase
   and is refused, naming the files (exit 4);
3. cleans a moved lane's workspace crates (`lane_target_at_path`, exit 7 on failure);
4. runs the lane's own `scripts/rdm-gate-at.sh <lane dir> fast`;
5. prints `GREEN lane <n> <sha> tree <hash>` when the gate recorded the commit's content in
   `green-trees`, else `RED …` (exit 2): read the red and fix it in the lane; never run it again;
6. with each `GREEN`, records the proven range in `green-ranges`: the tree, the patch-id of
   `origin/main..HEAD` (`git patch-id --stable` of the whole diff), the `origin/main` it was
   proven on and the tip.

**`scripts/lane-land.sh <lane> <branch> <issue> <close-comment-file>`**, from the lead's session:

1. refuses a missing comment file (exit 2), a busy lane (exit 3), a lane on another branch
   (exit 3) and a tip with no `green-trees` entry (exit 8). A landing never proves the lane;
2. when `origin/main` has moved past the tip, the `GREEN` stands for the rebased tip only when
   the branch's patch is unchanged (the patch-id of `origin/main..HEAD` after the rebase equals
   the proven range's in `green-ranges`) and main's movement since the proof touches none of the
   branch's files. Then it rebases, records the rebased tree in `green-trees` (`stands-on <proven
   tree>`) and `green-ranges`, and goes on without a second gate. Otherwise it refuses, naming the
   files both moved where that is why: "prove it again" (exit 8);
3. runs origin/main's `scripts/rebase-gate-land.sh <lane> <branch>`, which finds the green entry
   and pushes without gating again;
4. closes the issue with the comment file, `@SHA@` replaced by the landed commit
   (`LAND_NOCLOSE=1`: comments instead), and clears the lane's agent type.

**`scripts/rebase-gate-land.sh <lane> <branch>`** is the landing itself (CLAUDE.md's "landing"):

1. one landing at a time box-wide: it holds `flock -o /tmp/rdm-land.lock` for its whole run;
2. refuses a lane on another branch (exit 3);
3. rebases on `origin/main` (a branch that already merged main merges it again instead of
   flattening its merges); a conflict aborts and names the files (exit 1);
4. runs `name-check.py origin/main HEAD`; any hit refuses the landing (exit 5);
5. refuses content recorded in `red-trees` (exit 4: "fix the red, do not re-gate it");
6. content recorded in `green-trees` lands without a second gate; anything else runs the lane's
   fast gate;
7. appends the gate's `TIMINGS` row to `timings-ledger.txt` whatever the verdict;
8. on any `FAIL` line, records the content in `red-trees` and stops (exit 2);
9. pushes `HEAD:main` from the lane; the remote takes only a fast-forward, and the main checkout
   is never touched.

## 3. Cells: `scripts/cells.sh`

`scripts/cells.sh <package> [<test target>|lib] [-- <libtest args>]` builds exactly what the gate
builds (every workspace member, `cargo build --tests --bins`, one feature resolution) and then runs
the one test executable named, from that build:

```bash
scripts/cells.sh rdm-test main -- node_stop::
scripts/cells.sh rdm-admin lib -- status_declare::
scripts/cells.sh rdm-transport main -- delivery::
```

- Why: each `-p` selection resolves its own features, so a different selection compiles the whole
  stack again; one selection keeps one copy and an edit is paid once (the script's header).
- It unsets `CARGO_TARGET_DIR` and sets `CARGO_BUILD_INCREMENTAL=true` (never `CARGO_INCREMENTAL`,
  which sccache refuses), the gate's values.
- It refuses a package that is not a workspace member (exit 2), a target this build did not
  produce (exit 2), and prints the compiler's errors on a failed build (exit 1).
- It runs the executable in its crate's directory with `CARGO_MANIFEST_DIR` set, at the gate's
  test cadence: `RDM_STALENESS_MS=3000`, `RDM_GOSSIP_INTERVAL_MS=500`,
  `RDM_BACKBONE_INTERVAL_MS=500`.
- It exports no `OTEL_EXPORTER_OTLP_ENDPOINT`: a cell run through it exports to the collector only
  if the caller's environment names one ([test-otlp-capture.md](test-otlp-capture.md)).
- Cells take no lock (Luke, 2026-10-27 and 2026-10-28: "remove the lock"; "land the lanes on the fast
  path, merge the code, then run them"). `/tmp/rdm-gate-turn.lock` is taken only by
  `i143-export-run.sh`'s batches, always `flock -o`.

## 4. The gate: `scripts/rdm-gate-at.sh <lane dir> fast|release`

The gate builds once and runs every check and stem in parallel, each from a prebuilt executable.

| what | how | source (`scripts/rdm-gate-at.sh`) |
|---|---|---|
| a private copy | the script copies itself to `/tmp/rdm-gate-run.*` and runs the copy, so an edit mid-run changes nothing | line 2 |
| one gate per lane | `flock -n -o /tmp/rdm-gate-<lane>.lock`; a second gate aborts by name (`ABORT: a gate is already running on lane <n>`) | lines 17-21 |
| fast or release | `fast` exports the test cadence (staleness 3000 ms, gossip and backbone 500 ms); `release` exports none, so each stem runs at the cadence it sets | lines 28-34 |
| no skipped cut | `RDM_REQUIRE_NETFAULT=1`: a cell that cannot cut fails by name instead of skipping | line 37 |
| telemetry | `OTEL_EXPORTER_OTLP_ENDPOINT=http://192.168.68.99:5318` | line 42 |
| parallelism | `GATE_JOBS`, default 20 (fast) or 12 (release) | line 48 |
| the log | `$HOME/rust-distributed-mesh-deploy/gates-<lane>-<short sha>[release].log`, first line `[gate] head=<sha> dirty=<n>`, details in `<log>.detail`, per-job logs in `<log>.d/` | lines 53-56 |
| one build | `cargo build --timings --message-format=json --tests --bins` over every member; nothing compiles after it | line 74 |
| the timings row | `scripts/timings_row.py` over `target/cargo-timings/cargo-timing.html` | line 76 |
| one set of test executables | any test executable in `target/debug/deps` this build did not produce is deleted | lines 80-95 |
| discovery ratchet | every test executable's `--list` on the candidate against `origin/main`'s; a test that left must have moved, or be named in a commit body as `removed: <module>::<cell>` | lines 97-245 |
| scenario stems | each name in `SCENARIOS` runs as its own job: `rdm-test`'s `main` filtered to `<stem>::` | lines 58, 279-291 |
| `rdm-test`'s other cells | `main` split into `RDM_TEST_MAIN_SHARDS` jobs (default 4), dealt by test count | lines 326-349 |
| one job per test executable | every other package's executables, each its own job | lines 301-378 |
| acceptance jobs | `scripts/i143-acceptance-gate.sh <job>` for the named registry jobs | lines 379-389 |
| static checks | `dep-rules`, `address-lookup` (the `rdm-mesh-audit` binaries) | lines 420-421 |
| release-only jobs | the `RELEASE_ONLY` pattern (containers, the long stems, doctests, the no-default-features check, the R-shape jobs) is removed from the fast gate | lines 449-450 |
| the job bound | a job past `GATE_JOB_SECS` (300 s; release-only jobs `GATE_HEAVY_JOB_SECS`, 900 s) is killed with its whole process group and named `FAIL(timed-out)` | lines 431-442, 490 |
| leaked processes | a node process of this lane's build adopted by `systemd`/`init` after the jobs end is named `FAIL leaked-processes` and killed | lines 494-498 |
| evidence | on any `FAIL`, one snapshot of the scenario artifacts is kept under `$HOME/rust-distributed-mesh-deploy/kept/` (`scripts/keep-gate-evidence.py`) | lines 502-504 |
| green record | a fast gate with no `FAIL` appends the commit's content hash to `green-trees` | line 506 |

A red gate is read, never run again (CLAUDE.md). A landing runs the fast gate only; the release
suite, the export run and the soaks run once at the merged tip (`rdm-gate-at.sh <lane dir> release`, then
`i143-export-run.sh`, then `i143-export-run.sh --soaks`), never per unit.

## 5. The name check: `scripts/name-check.py <base> <head>`

Run in the lane, it prints one line per hit and nothing when clean. It reads only `.rs` files the
diff adds or changes, so a docs-only diff prints nothing. It refuses:

1. an added or renamed `.rs` file whose name holds a retired word (the `RETIRED` set:
   `drain`, `park`, `fence`, `relaunch`, `reborn`, `cluster`, `relay`, `operator`, `seat`,
   `authority`, `cohort`, `lane`, `sticky`, `book`, `roll`, `climb`, `round` and their forms);
2. an added test file under `/tests/` named by an issue id (`i<N>_acceptance`, or a four-digit
   number as a word of the name);
3. a definition (`fn`, `struct`, `enum`, `mod`, `trait`, `type`, `const`, `static`) the diff adds,
   and no removed line defined, whose words hold a retired word, or whose name carries
   `_acceptance_<N>`;
4. an added runtime construction of a permission code (`PermissionDenied`, `Unauthenticated`,
   `Unauthorized`, `SenderNotSubject`, `CallerRefused`), outside enum definitions, imports, test
   functions, match patterns and assertions.

`scripts/test_name_check.py` holds the check's own tests; `scripts/test_lane_verbs.py` holds the
lane verbs' (`python3 scripts/test_lane_verbs.py`: each verb against a scratch estate under a
scratch `$HOME`, with a stub gate, `gh` and `rdm-docgen`). Detail and examples:
[testing-naming-conventions.md](testing-naming-conventions.md).

## 6. Other scripts

| script | what it is for |
|---|---|
| `scripts/i143-export-run.sh [--soaks] [--plan]` | every job of the acceptance registry (`tools/mesh-audit/i143-acceptance-jobs.json`) at HEAD, in batches of at most `I143_EXPORT_BATCH` (6), each batch under `flock -o /tmp/rdm-gate-turn.lock`; one receipt per job. Refuses a checkout with uncommitted paths and stops when HEAD moves between batches. Soak jobs are held unless `--soaks`, which needs a green fast and release gate log at this HEAD; `I143_SOAKS_LEARN=1` with `--soaks` is a learning run: the soaks run without that bar, every receipt is marked `learning: true`, and `export-manifest` refuses to certify a learning receipt. `--plan` prints the batches and runs nothing. Output: `target/i143-export/<sha>/` |
| `scripts/i143-acceptance-gate.sh <job>` | runs one registered job and leaves its receipt under `target/i143-acceptance/`; `--verify-receipt <path>` checks one |
| `scripts/netless.sh <command…>` | runs a command in a fresh network namespace with loopback only (needs root or `sudo`) |
| `scripts/demo-estate.sh up\|stream\|down [<root>]` | the R-shape consumer's live estate by hand, with the demo admin UI ([testing-mesh-lifecycle.md](testing-mesh-lifecycle.md) §5) |
| `scripts/i143-rshape-build-consumer.sh` | builds the independent consumer workspace (`demo/`) at one exact RDM commit; the release gate runs it first |
| `scripts/docs-site.sh [--check]` | builds the docs site from this repository into `target/docs-site`: regenerates `docs/reference/rdm` (spans.md, config.md) with `rdm-docgen`, builds the API docs with `cargo doc` (`-D warnings`) and the guide with mdBook, lists dead links; `--check` fails on a dirty generated page or any dead link |
| `scripts/prebuilt-test.py` | runs a `cargo test` selection from the gate's existing artifacts, without cargo |
| `scripts/rustc-wrapper.sh` | the rustc wrapper: vendor crates through sccache, workspace crates through plain rustc, every link through mold; without sccache or mold, plain rustc and the default linker, named once per cargo run |
| `scripts/cc-wrapper.sh` (and its link `scripts/c++-wrapper.sh`) | `CC` and `CXX`: `sccache cc` / `sccache c++` when sccache is in `PATH`, plain `cc` / `c++` otherwise |
