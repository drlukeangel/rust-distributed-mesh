#!/usr/bin/env python3
"""The gate's discovery check (rdm#96): every rdm-test test is selected by exactly one module job.

usage: gate-module-coverage.py <jobs-file> <rdm-test main executable> [module ...]

The modules named after the executable are the ones the gate keeps out of its module jobs on purpose
(the gate's SCENARIO_SKIP stems that are not SCENARIOS: acceptance, release and soak stems with their own runs);
they are not checked.

Reads the gate's jobs file, simulates libtest's selection (a positional filter is a substring of the
full test name, or equal to it under --exact; a --skip likewise) for every job that runs the
executable, and compares it with `<executable> --list`. A test selected by no job, or by two module
jobs, is named and the exit status is 1. The named-cell jobs (container-proof, the fast storage cell)
are a second run of cells a module job owns, or the only run of a module no module job owns
(container_kill): they may repeat a test but never stand in for a module job when a module job owns it.
"""
import shlex
import subprocess
import sys

NAMED_CELLS = {"container-proof", "topology_storage_provider_fast"}
PUNCT = set("();&|<>")


def selected(names, args):
    exact = "--exact" in args
    filters, skips, it = [], [], iter(args)
    for a in it:
        if a == "--skip":
            skips.append(next(it, ""))
        elif a.startswith("-"):
            continue
        else:
            filters.append(a)

    def hit(pat, n):
        return n == pat if exact else pat in n

    return {n for n in names if (not filters or any(hit(f, n) for f in filters)) and not any(hit(s, n) for s in skips)}


def main(jobs_path, binary, elsewhere=()):
    names = [l.rsplit(": ", 1)[0] for l in subprocess.run([binary, "--list"], capture_output=True, text=True).stdout.splitlines() if l.endswith(": test")]
    names = [n for n in names if n.split("::", 1)[0] not in set(elsewhere)]
    module_runs = {n: [] for n in names}
    named_runs = {n: [] for n in names}
    for line in open(jobs_path):
        job, _, what = line.rstrip("\n").partition(" ")
        lex = shlex.shlex(what, posix=True, punctuation_chars=True)
        lex.whitespace_split = True
        toks = list(lex)
        i = 0
        while i < len(toks):
            if toks[i] != binary:
                i += 1
                continue
            j = i + 1
            while j < len(toks) and not (set(toks[j]) <= PUNCT):
                j += 1
            runs = named_runs if job in NAMED_CELLS else module_runs
            for n in selected(names, toks[i + 1:j]):
                runs[n].append(job)
            i = j
    bad = []
    for n in names:
        m, c = module_runs[n], named_runs[n]
        if len(m) > 1:
            bad.append(f"{n}: run by {len(m)} module jobs ({', '.join(m)})")
        elif not m and not c:
            bad.append(f"{n}: run by no job")
    for b in bad:
        print(b)
    print(f"module coverage: {len(names)} tests, {len(bad)} not run exactly once")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3:]))
