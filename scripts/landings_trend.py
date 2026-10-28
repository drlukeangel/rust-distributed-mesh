#!/usr/bin/env python3
"""The landings ledger (lane-land.sh's landings.jsonl): the newest landing's time spent, then the last N
landings and their trend.
usage: landings_trend.py <landings.jsonl> [N]"""
import json
import sys


def hm(s):
    return f"{s // 3600}h{s % 3600 // 60:02d}m"


def main():
    path, n = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 10
    rows = [json.loads(line) for line in open(path) if line.strip()]
    if not rows:
        print("no landings recorded")
        return
    r = rows[-1]
    first = "none" if r["first_commit_at"] is None else hm(r["first_commit_at"] - r["plan_at"]) + " after the plan"
    print(f"TIME-SPENT lane {r['lane']} issue #{r['issue']} {r['sha']}: {hm(r['total_s'])} from the plan to the landing")
    print(f"  first commit {first}; {r['commits']} commits; {r['gates']} gates ({r['red']} red), {hm(r['gate_s'])} gating; the rest {hm(r['total_s'] - r['gate_s'])} building, cells and waiting")
    last = rows[-n:]
    print(f"TREND, last {len(last)} landings:")
    print("  issue   lane  total   gating  gates  red  first-commit")
    for x in last:
        fc = "-" if x["first_commit_at"] is None else hm(x["first_commit_at"] - x["plan_at"])
        print(f"  #{x['issue']:<6} {x['lane']:<5} {hm(x['total_s']):<7} {hm(x['gate_s']):<7} {x['gates']:<6} {x['red']:<4} {fc}")
    tot = sum(x["total_s"] for x in last) // len(last)
    gat = sum(x["gate_s"] for x in last) // len(last)
    reds = sum(x["red"] for x in last)
    gates = sum(x["gates"] for x in last)
    print(f"  mean {hm(tot)} per landing, {hm(gat)} of it gating; {reds} of {gates} gates red")


if __name__ == "__main__":
    main()
