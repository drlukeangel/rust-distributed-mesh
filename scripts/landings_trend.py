#!/usr/bin/env python3
"""The landings ledger (lane-land.sh's landings.jsonl): where a unit's time went, every gate it ran, and the
trend over the last N landings, as markdown tables.

usage:
  landings_trend.py record <landings.jsonl> <deploy-root> <lane> <issue> <branch> <sha> <plan-epoch> <launched-epoch|-> <first-commit-epoch|-> <commits>
      reads this lane's post-land-gate.sh logs written since the plan, appends one line, then prints
  landings_trend.py <landings.jsonl> [N]
      prints the newest landing and the last N"""
import glob
import json
import os
import re
import sys
import time


def hm(s):
    s = max(0, int(s))
    return f"{s // 3600}h{s % 3600 // 60:02d}m"


def clock(t):
    return time.strftime("%H:%M:%S", time.localtime(t))


def gate_runs(deploy, lane, since):
    """Every post-land-gate.sh log of `lane` written since `since`: its SHA, start, end, seconds, result and
    the stems that failed. A log with no DONE seconds ended early (a build or discovery stop) and is red."""
    runs = []
    for g in glob.glob(os.path.join(deploy, f"gates-{lane}-*.log")):
        end = os.stat(g).st_mtime
        if end < since:
            continue
        text = open(g, errors="ignore").read()
        done = re.findall(r"^DONE (\d+)s$", text, re.M)
        secs = int(done[-1]) if done else 0
        failed = re.findall(r"^FAIL(?:\([^)]*\))? (\S+)", text, re.M)
        sha = os.path.basename(g)[len(f"gates-{lane}-"):-len(".log")]
        runs.append({"sha": sha, "start": int(end - secs), "end": int(end), "secs": secs,
                     "result": "red" if failed or not done else "green", "failed": failed})
    return sorted(runs, key=lambda r: r["end"])


def cells_runs(deploy, lane, since):
    """This lane's cells.sh runs since `since`, from the cells ledger cells.sh writes."""
    out = []
    try:
        for line in open(os.path.join(deploy, "cells.jsonl")):
            try:
                c = json.loads(line)
            except ValueError:
                continue
            if c.get("lane") == str(lane) and c.get("start", 0) >= since:
                out.append(c)
    except OSError:
        pass
    return out


def last_landing(path, lane):
    """When `lane` last landed, from the landings ledger; 0 when it never did."""
    at = 0
    try:
        for line in open(path):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if str(r.get("lane")) == str(lane):
                at = max(at, int(r.get("landed_at", 0)))
    except OSError:
        pass
    return at


def record(path, deploy, lane, issue, branch, sha, plan, launched, first, commits):
    now = int(time.time())
    # A lane's plan file stays until the next dispatch: a unit the lead lands without one starts at the
    # lane's last landing, so the earlier unit's gates and time are never counted again.
    plan = max(plan, last_landing(path, lane))
    launched = launched if launched is None else max(launched, plan)
    first = first if first is None else max(first, plan)
    runs = gate_runs(deploy, lane, plan)
    cells = cells_runs(deploy, lane, plan)
    row = {"landed_at": now, "lane": lane, "issue": int(issue), "branch": branch, "sha": sha, "plan_at": plan, "launched_at": launched,
           "first_commit_at": first, "commits": int(commits), "gates": len(runs),
           "red": sum(r["result"] == "red" for r in runs), "gate_s": sum(r["secs"] for r in runs),
           "total_s": now - plan, "gate_runs": runs,
           "cells": len(cells), "cells_red": sum(c["rc"] != 0 for c in cells),
           "cells_s": sum(c["end"] - c["start"] for c in cells)}
    with open(path, "a") as f:
        f.write(json.dumps(row) + "\n")


def phases(r):
    """The landing's time in phases, from the plan, the first commit, the gates and the landing."""
    runs = r.get("gate_runs") or []
    plan, land = r["plan_at"], r["landed_at"]
    launched = r.get("launched_at") or plan
    first = r["first_commit_at"] or launched
    first_gate = runs[0]["start"] if runs else land
    greens = [g for g in runs if g["result"] == "green"]
    last_green = greens[-1]["end"] if greens else land
    gating = r["gate_s"]
    return [
        ("Waiting for a seat (plan to launch)", launched - plan),
        ("Seat start to its first commit", first - launched),
        ("Building to the first gate", first_gate - first),
        ("Rework after a red gate (not gating)", max(0, last_green - first_gate - gating)),
        ("Gating", gating),
        (f"Cells runs ({r.get('cells', 0)} runs, {r.get('cells_red', 0)} red; overlaps the phases above)", r.get("cells_s", 0)),
        ("Last GREEN to the landing", land - last_green),
    ]


def show(path, n):
    rows = [json.loads(line) for line in open(path) if line.strip()]
    if not rows:
        print("no landings recorded")
        return
    r = rows[-1]
    print(f"TIME-SPENT lane {r['lane']} issue #{r['issue']} {r['sha']}: {hm(r['total_s'])} from the plan to the landing, {r['commits']} commits")
    print()
    print("| Phase | Time |")
    print("|---|---|")
    for name, s in phases(r):
        print(f"| {name} | {hm(s)} |")
    runs = r.get("gate_runs") or []
    if runs:
        print()
        print("| Gate | Start | End | Took | Result | Failed |")
        print("|---|---|---|---|---|---|")
        for g in runs:
            print(f"| {g['sha']} | {clock(g['start'])} | {clock(g['end'])} | {hm(g['secs'])} | {g['result'].upper()} | {', '.join(g['failed']) or '–'} |")
    last = rows[-n:]
    print()
    print(f"TREND, last {len(last)} landings")
    print()
    print("| Issue | Lane | Total | Gating | Gates | Red | First commit |")
    print("|---|---|---|---|---|---|---|")
    for x in last:
        fc = "–" if x["first_commit_at"] is None else hm(x["first_commit_at"] - x["plan_at"])
        print(f"| #{x['issue']} | {x['lane']} | {hm(x['total_s'])} | {hm(x['gate_s'])} | {x['gates']} | {x['red']} | {fc} |")
    tot = sum(x["total_s"] for x in last) // len(last)
    gat = sum(x["gate_s"] for x in last) // len(last)
    print(f"| **Mean** | | **{hm(tot)}** | **{hm(gat)}** | | **{sum(x['red'] for x in last)} of {sum(x['gates'] for x in last)}** | |")


def main():
    a = sys.argv[1:]
    if a and a[0] == "record":
        path, deploy, lane, issue, branch, sha, plan, launched, first, commits = a[1:11]
        opt = lambda v: None if v in ("", "-") else int(v)
        record(path, deploy, lane, issue, branch, sha, int(plan), opt(launched), opt(first), commits)
        show(path, 10)
    else:
        show(a[0], int(a[1]) if len(a) > 1 else 10)


if __name__ == "__main__":
    main()
