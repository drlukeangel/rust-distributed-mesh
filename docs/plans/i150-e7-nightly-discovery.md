# i150 e7.s2: discovery of the deployed nightly scheduler (rdm#265)

Read-only discovery on 2026-11-01 at RDM `63cf18e17`. Nothing was created, enabled, changed or run.

## Result

One scheduler exists on this box. It is a user crontab line at 03:00 that runs **rafka-v2's**
nightly gate (`scripts/nightly-gate.sh`), not RDM's. Nothing schedules RDM's release suite, its fast
gate, `node_records::` or the four soak jobs. Since 2026-10-12 the scheduler has also run nothing at
all: every night is refused by its parked lane.

## Schedulers checked

| place | finding |
|---|---|
| user crontab (`crontab -l`) | one line (below) |
| `systemctl --user list-timers --all` | two timers, `snap.firmware-updater.firmware-notifier.timer` and `launchpadlib-cache-clean.timer`; neither is RDM's. No unit in `~/.config/systemd/user` other than `default.target.wants` and `timers.target.wants` |
| `/etc/crontab`, `/etc/cron.d` (anacron, e2scrub_all, sysstat), `/etc/cron.{hourly,daily,weekly,monthly,yearly}` | system housekeeping only (logrotate, apt, dpkg, man-db, sysstat, google-chrome, quota, apport); nothing names RDM or rafka. `/var/spool/cron/crontabs` is not readable (no sudo); the user's own crontab was read with `crontab -l` |
| `cron` service | `systemctl is-active cron` = active |
| CI configuration | no `.github` in this repo (lane 7) and none in `/home/admin/rafka-v2` |
| Claude Code routines and scheduled triggers | no `scheduled-tasks`, `routines` or `triggers` directory under `~/.claude`. Only `~/.claude/scheduled_tasks.lock` exists (a session lock: `sessionId 8842f3ab-9a1d-40fa-b300-d246a89b56fa`, `pid 19279`, `acquiredAt 1786221289999`); `~/.claude/jobs` holds two job folders (`24d82858`, `2ebb87c7`) and `pins.json`, not read as schedules. Remote triggers held by the service rather than on disk cannot be seen from files |
| this repo's `scripts/` and `docs/eng/` | no scheduler; `docs/eng/testing-harnesses.md` §6 says "No script in `scripts/` schedules the release suite; it is started by hand." |
| `/home/admin/rust-distributed-mesh-deploy/` | release-gate logs are by-hand runs (below); no scheduler file |

## The one scheduler: rafka-v2's nightly gate

Configuration, in the order cron reaches it.

1. User crontab line (`crontab -l`):
   `0 3 * * * PATH=/home/admin/.cargo/bin:/usr/bin:/usr/local/bin:/usr/bin:/bin NIGHTLY_LANE=/home/admin/lane-queue/laneG7 NIGHTLY_MILESTONE='nightly — one issue per night with a problem' /home/admin/lane-queue/nightly/cron-entry.sh`
   It matches the line in `/home/admin/rafka-v2/docs/eng/testing-harnesses.md` §7 (the section "The nightly gate — what runs at 03:00, and where").
2. `/home/admin/lane-queue/nightly/cron-entry.sh` (modified Sep 27): `git -C /home/admin/rafka-v2 fetch origin main`, exports `origin/main`'s `scripts/nightly-gate.sh` and `scripts/lane-env.sh` into `/home/admin/lane-queue/nightly/run/`, and `exec bash run/nightly-gate.sh`.
3. `scripts/nightly-gate.sh` (rafka-v2 `origin/main` `bf130c077`): refuses without `NIGHTLY_LANE`; `cd`s to the lane; `git checkout -q --detach origin/main` (refuses, exit 3, if the lane holds changes); runs `GATE_MODE=nightly scripts/post-land-gate.sh`; then for the rungs `single-node multi-node multi-mesh` runs `nightly_shape` (`scripts/lib/nightly-shape.sh`: seed ladder with the golden image off, the deployment suite and phase-run); opens one issue in `drlukeangel/rafka-v2` in `NIGHTLY_MILESTONE` for a night with a problem.
4. The lane it runs in, `/home/admin/lane-queue/laneG7`, is a checkout of `git@github.com:drlukeangel/rafka-v2.git` (remote `github`). RDM's `rdm-gate-at.sh`, `cells.sh`, the `i143-*` registry and `rdm-test` appear nowhere in this chain.

### Last executions (log path, date, result)

Logs: `/home/admin/lane-queue/nightly/gate-<UTC date>.log`, one per night from 2026-09-14 to 2026-11-01.

- Last night that ran checks: `gate-2026-10-11.log`, 2026-10-11 03:05, 66111 bytes, `nightly-gate rc=1 wall=313s`, `post-land-gate @ 44fcc7ffb9: RED` (28 checks). The log shows `rustc-wrapper.sh: line 162: exec: sccache: not found` (exit status 127) under cron's PATH, and the three seed ladders red ("day 0 did not settle ... the fabric's system org is not held after 900s"; rows `seed-ladder-single-node`, `-multi-node`, `-multi-mesh`, logs `seed-<rung>-2026-10-11.log`). The issue step then failed: "could not add to milestone 'nightly — one issue per night with a problem': ... not found".
- Every night from 2026-10-12 to 2026-11-01 (21 logs, 578 bytes each, e.g. `gate-2026-11-01.log`, 2026-11-01 03:00) is the same refusal: `error: Your local changes to the following files would be overwritten by checkout: scripts/check-dead-code.sh` then `nightly-gate: cannot check out origin/main in /home/admin/lane-queue/laneG7 — the lane holds changes:  M scripts/check-dead-code.sh (branch )`, then "could not open the night's issue in milestone ...". Today `git -C /home/admin/lane-queue/laneG7 status --short` still shows ` M scripts/check-dead-code.sh` (1 line changed) and untracked `deploy.pid` and `docs/plans/prompts/output/laneG7-i138-e0-2852-fast-departure-fence-plan.md`.
- So the scheduler fires nightly (the logs prove cron runs it) but has run no check for 21 nights, and the milestone it files into is not found. No suite or seed directory `suite-*` exists under `/home/admin/lane-queue/nightly/`; the last seed logs are 2026-10-11.

## What RDM runs, and when

RDM has no scheduled run. Its release runs are by hand: `scripts/rdm-gate-at.sh <lane> release` writes `/home/admin/rust-distributed-mesh-deploy/gates-<lane>-<sha>release.log`. The latest: `gates-3-65bc46e7frelease.log`, 2026-10-31 20:09, head `65bc46e7f2d4cf936948b9265143a6d4209a4680`, no FAIL rows, `DONE 530s`. Earlier release logs by hand (`gates-3-907f6820erelease.log` 2026-10-31 15:14, `gates-3-62514f4eerelease.log` 13:33, `gates-1-36ed1d0c8release.log` 2026-10-30 00:49 with three FAIL rows). The timings ledger `/home/admin/rust-distributed-mesh-deploy/timings-ledger.txt` has its last row 2026-10-31T19:58:33-04:00 (`65bc46e7f`), the red-tree list is `red-trees`, the cell log is `cells.jsonl`. These are lane runs, not scheduled ones; none is a "completed scheduled run".

## Coverage of `node_records::` and the four soak jobs

- `node_records::` (`crates/rdm-test/tests/node_records.rs`, registered as `mod node_records;` in `crates/rdm-test/tests/main.rs`, two cells: `a_build_applies_properties_and_a_nodes_later_update_stands`, `own_properties_survive_node_restart`) runs inside the `rdm-test:main0..3` module jobs of `rdm-gate-at.sh fast` and `release` (`RELEASE_ONLY` does not exclude them), so any by-hand gate covers it. A schedule does not exist to run those gates. The registry `tools/mesh-audit/i143-acceptance-jobs.json` names it once, job `i143-245-process`, cell `own_properties_survive_node_restart`; the other cell has no registry job.
- The four jobs are in `rshape_jobs` of the registry, each seed `1431101`, `RDM_RSHAPE_SOAK_SECONDS=1800`: `i143-rshape-soak-authority-process` and `-container` (cell `rshape_burn_in::mock_soak_primaries_preserve_continuous_invariants`), `i143-rshape-soak-randomized-process` and `-container` (cell `rshape_burn_in::mock_soak_randomized_preserves_continuous_invariants`). `RELEASE_ONLY` has `rshape-[a-z-]+` (the chaos jobs), which does not match `i143-rshape-soak-*`; the release suite does not run the soaks. They run from `scripts/i143-acceptance-gate.sh <job>` and `scripts/i143-export-run.sh`, by hand.
- Last by-hand receipts (`<lane>/target/i143-rshape/jobs/<job>.json`, lane 3, `source_sha 65bc46e7f2d4cf936948b9265143a6d4209a4680`): randomized-process 2026-10-31 21:33, randomized-container 2026-10-31 23:49, authority-process started 2026-11-01T03:49Z finished 04:23Z outcome `ok`, authority-container 2026-11-01 00:58. Only authority-process was opened for its outcome here; the others were not read.

## The exact change

Not applied; text only. The feature goes into the schedule that exists, which is a rafka-v2 schedule. RDM cannot join it by editing this repo, so the choice below is Luke's, with operational approval (crontab and `/home/admin/lane-queue/nightly/` are outside any lane).

**Step 0, the existing schedule is not running.** Before any addition, the nightly lane must be able to check out `origin/main`: `/home/admin/lane-queue/laneG7` holds ` M scripts/check-dead-code.sh`, and the milestone `nightly — one issue per night with a problem` is not found in `drlukeangel/rafka-v2`. Cron's PATH also lacks `sccache`, which `scripts/rustc-wrapper.sh` execs (2026-10-11 log).

**Arm A, a second crontab line for RDM** (smallest; nothing in rafka-v2's nightly changes). Add one line, run by a new file `/home/admin/lane-queue/nightly/rdm-cron-entry.sh` and a parked RDM lane with no seat:

```
30 4 * * * PATH=/home/admin/.cargo/bin:/home/admin/.local/bin:/usr/bin:/usr/local/bin:/bin /home/admin/lane-queue/nightly/rdm-cron-entry.sh
```

with `rdm-cron-entry.sh`:

```
#!/usr/bin/env bash
set -u
R=/home/admin/rust-distributed-mesh   # fetch only
L=<a parked RDM lane: /home/admin/rdm-lanes/<n> with no seat>
D=/home/admin/lane-queue/nightly
S=$(date -u +%F); LOG=$D/rdm-gate-$S.log
git -C "$L" fetch -q origin && git -C "$L" checkout -q --detach origin/main || { echo "refused: $L" > "$LOG"; exit 3; }
cd "$L" && {
  scripts/rdm-gate-at.sh "$L" release
  scripts/cells.sh rdm-test main -- node_records::
  for j in i143-rshape-soak-authority-process i143-rshape-soak-authority-container i143-rshape-soak-randomized-process i143-rshape-soak-randomized-container; do scripts/i143-acceptance-gate.sh "$j"; done
} > "$LOG" 2>&1
```

Four soaks at 1800 s each plus the release suite (about 530 s) is over two hours; the lane must be one nobody uses, and rdm-gate-at's leaked-process step kills other runs on the lane. A lane is a numbered folder `1`-`7` that CLAUDE.md says is reused, so a parked RDM lane needs Luke's yes. The run's result file is `rdm-gate-<UTC date>.log`; a completed scheduled run is the first such log whose gate line has no FAIL and whose four soak receipts show `"outcome": "ok"` at that night's SHA.

**Arm B, add RDM to rafka-v2's nightly** (one schedule): in rafka-v2's `scripts/nightly-gate.sh`, after the seed ladders, call the same three RDM commands in an RDM checkout. That edits rafka-v2's script and a page of `testing-harnesses.md` §7 there, out of this repo's reach.

Either arm adds no `node_records` job of its own: the cell is already in `rdm-test:main*`, and a registry job for the second cell is a separate registry edit.

## Not done

No run of the release suite, a soak or a cell; no crontab, timer, trigger, script or page change; no `post-land-gate` (no code changed). Not verified: any scheduler held by the Claude service rather than on disk, the contents of `/var/spool/cron/crontabs` for other users, and the other three soak receipts' contents.
