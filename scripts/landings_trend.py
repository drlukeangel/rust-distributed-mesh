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
    first = "none" if r["first_commit_at"] is None else hm(r["first_commit_at"] - r["plan_at"])
    print(f"TIME-SPENT lane {r['lane']} issue #{r['issue']} {r['sha']}")
    print()
    print("| Metric | Value |")
    print("|---|---|")
    print(f"| Total, plan to landing | {hm(r['total_s'])} |")
    print(f"| First commit after plan | {first} |")
    print(f"| Commits | {r['commits']} |")
    print(f"| Gates run | {r['gates']} |")
    print(f"| Red gates | {r['red']} |")
    print(f"| Time gating | {hm(r['gate_s'])} |")
    print(f"| Building, cells, waiting | {hm(r['total_s'] - r['gate_s'])} |")
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
    reds = sum(x["red"] for x in last)
    gates = sum(x["gates"] for x in last)
    print(f"| **Mean** | | **{hm(tot)}** | **{hm(gat)}** | | **{reds} of {gates}** | |")

if __name__ == "__main__":
    main()
