#!/usr/bin/env python3
"""One timings row from cargo's --timings report: wall, units, max concurrency, the sum of unit times, the five longest units.
usage: timings_row.py <target>/cargo-timings/cargo-timing.html"""
import re, json, sys
h = open(sys.argv[1]).read()
g = lambda k: (re.search(k + r":</td><td>([^<]*)", h) or [None, "?"])[1]
d = json.loads(re.search(r"const UNIT_DATA = (\[.*?\]);\n", h, re.S).group(1))
top = sorted(d, key=lambda u: -u["duration"])[:5]
def short(u):
    n, t = u["name"].replace("rafka-", ""), u["target"].strip()
    if not t: return n + " lib"
    if "build-script" in t: return n + " build-script"
    if '"' in t:
        first, kind = t.split()[0], t.split('"')[1]
        return n + (" lib-test" if kind == "lib" else f" {kind}:{first}")
    return n + " " + t
if sum(u["duration"] for u in d) < 1:
    print(f"TIMINGS wall={g('Total time').split(' ')[0]} units={g('Total units')} fresh: nothing compiled"); sys.exit(0)
print(f"TIMINGS wall={g('Total time').split(' ')[0]} units={g('Total units')} max_concurrency={g('Max concurrency').split(' ')[0]} "
      f"unit_sum={sum(u['duration'] for u in d):.0f}s top=" + ", ".join(f"{short(u)} {u['duration']:.1f}s" for u in top))
