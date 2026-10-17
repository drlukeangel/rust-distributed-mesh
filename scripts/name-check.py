#!/usr/bin/env python3
"""Refuse a landing that ADDS a retired word in an identifier or a file name, or an id-named test file
(rdm-crate-and-test-layout.md §4 "Name check"). Scoped to names the diff introduces: a definition name on an
added line that no removed line defined, and an added or renamed file. Prose, quotes and commit bodies are
never matched. usage: name-check.py <base> <head>  (run in the lane)"""
import re, subprocess, sys
base, head = sys.argv[1], sys.argv[2]
RETIRED = {"drain", "drains", "draining", "drained", "park", "parked", "parking", "fence", "fenced", "fencing",
           "relaunch", "relaunched", "reborn", "rebirth", "cluster", "relay", "tailer", "sidecar", "sysadmin",
           "viewer", "operator", "departed", "retire", "retired"}
DEF = re.compile(r'\b(?:fn|struct|enum|mod|trait|type|const|static)\s+([A-Za-z_][A-Za-z0-9_]*)')
def words(name):
    parts = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower().split('_')
    return {p for p in parts if p}
def git(*a):
    return subprocess.run(["git", *a], capture_output=True, text=True, check=True).stdout
hits = []
for line in git("diff", "--name-status", "-M", f"{base}..{head}").splitlines():
    f = line.split("\t"); st, path = f[0], f[-1]
    if not (st.startswith("A") or st.startswith("R")) or not path.endswith(".rs"):
        continue
    stem = path.rsplit("/", 1)[-1][:-3]
    bad = words(stem) & RETIRED
    if bad: hits.append(f"file {path}: retired word {sorted(bad)}")
    if "/tests/" in path and (re.search(r'i\d+_acceptance', stem) or re.search(r'(^|_)\d{4}($|_)', stem)):
        hits.append(f"file {path}: named by an issue id")
added, removed = set(), set()
for l in git("diff", "-U0", f"{base}..{head}", "--", "*.rs").splitlines():
    if l.startswith("+++") or l.startswith("---"): continue
    if l.startswith("+"): added |= set(DEF.findall(l))
    elif l.startswith("-"): removed |= set(DEF.findall(l))
for n in sorted(added - removed):
    bad = words(n) & RETIRED
    if bad: hits.append(f"identifier {n}: retired word {sorted(bad)}")
    if re.search(r'_acceptance_\d+', n): hits.append(f"identifier {n}: named by an issue id")
# The authN/authZ audit (Luke 2026-10-16, R24, Q12): a landing that adds a runtime call to `unauthorized(..)`
# or runtime construction of `NotAuthority::SenderNotSubject` is refused, as a retired word is. Exempt by the
# kind of code, never by directory: a codec definition (`fn unauthorized(`, the variant's own line in its enum),
# a classifier (a pattern: a match arm `=>`, a `let`/`if let`, `matches!`), reserved-byte fixtures and test
# assertions (a test file, or a `#[cfg(test)]` module of a source file), comments and string literals. Location
# under rdm-core/src or rdm-test/src alone exempts nothing. The reserved discriminants stay, inert.
CALL = re.compile(r'(?<![A-Za-z0-9_])unauthorized\s*\(')
DEFN = re.compile(r'\bfn\s+unauthorized\s*\(')
SNS = re.compile(r'\bSenderNotSubject\s*\{')
VARIANT_DEF = re.compile(r'^\s*SenderNotSubject\s*\{\s*$')
PATTERN = re.compile(r'=>|^\s*(?:if\s+let|while\s+let|let)\b|matches!\s*\(')
STRING = re.compile(r'"(?:\\.|[^"\\])*"')
def test_ranges(text):
    """Line ranges (1-based, inclusive) of `#[cfg(test)]` modules, by brace matching from the mod's `{`."""
    lines = text.split("\n"); out = []
    for i, l in enumerate(lines):
        if l.strip().startswith("#[cfg(test)]"):
            j = i + 1
            while j < len(lines) and lines[j].strip().startswith("#["): j += 1
            if j < len(lines) and re.match(r'\s*(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{', lines[j]):
                depth = 0
                for k in range(j, len(lines)):
                    code = STRING.sub('""', lines[k].split("//", 1)[0])
                    depth += code.count("{") - code.count("}")
                    if depth == 0 and k >= j:
                        out.append((j + 1, k + 1)); break
    return out
for line in git("diff", "--name-only", f"{base}..{head}", "--", "*.rs").splitlines():
    path = line.strip()
    if not path: continue
    is_test_file = "/tests/" in path or path.startswith("tests/")
    try:
        text = git("show", f"{head}:{path}")
    except subprocess.CalledProcessError:
        continue
    ranges = [] if is_test_file else test_ranges(text)
    hunk_line = None
    for l in git("diff", "-U0", f"{base}..{head}", "--", path).splitlines():
        m = re.match(r'@@ -\S+ \+(\d+)(?:,\d+)? @@', l)
        if m: hunk_line = int(m.group(1)); continue
        if l.startswith("+++") or l.startswith("---") or hunk_line is None: continue
        if l.startswith("+"):
            n = hunk_line; hunk_line += 1
            if is_test_file or any(a0 <= n <= b0 for a0, b0 in ranges): continue
            code = STRING.sub('""', l[1:].split("//", 1)[0])
            if CALL.search(code) and not DEFN.search(code):
                hits.append(f"{path}:{n}: a runtime call to unauthorized(..): {l[1:].strip()[:120]}")
            if SNS.search(code) and not VARIANT_DEF.match(code) and not PATTERN.search(code):
                hits.append(f"{path}:{n}: runtime construction of NotAuthority::SenderNotSubject: {l[1:].strip()[:120]}")
        elif not l.startswith("-"):
            hunk_line += 1
print("\n".join(hits))
