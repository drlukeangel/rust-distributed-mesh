#!/usr/bin/env python3
"""Refuse a landing that ADDS a retired word in an identifier or a file name, or an id-named test file
(rdm-crate-and-test-layout.md §4 "Name check"). Scoped to names the diff introduces: a definition name on an
added line that no removed line defined, and an added or renamed file. Prose, quotes and commit bodies are
never matched. usage: name-check.py <base> <head>  (run in the lane)"""
import re, subprocess, sys
base, head = sys.argv[1], sys.argv[2]
RETIRED = {"drain", "drains", "draining", "drained", "park", "parked", "parking", "fence", "fenced", "fencing",
           "relaunch", "relaunched", "reborn", "rebirth", "cluster", "relay", "tailer", "sidecar", "sysadmin",
           "viewer", "operator", "departed", "retire", "retired",
           # the made-up words (docs/plans/proposal/i143-made-up-words-removal.md §5): the primaries are the
           # fabric-primary, the mesh-primary and the node-type primary; nothing is a seat, an authority or a cohort
           "seat", "seats", "seated", "unseat", "authority", "authorities", "authoritative", "authorizes",
           "cohort", "cohorts", "lane", "lanes", "sticky", "book", "books", "roll", "rolls", "rolled", "rolling", "climb", "climbs", "climbed", "climbing", "round", "rounds",
           # Q211: the Build's ordered list is steps, one item a step
           "recipe", "recipes",
           # Q297: edge is gossip's word; a connection is a Direct or a Proxy (gossip's code defines no edge name)
           "edge", "edges"}
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
# R24 and e5 share one syntax-aware auth check. A directory exempts no function.
# e5: reserved gRPC auth codes are definitions/classification/fixtures only. The scanner
# uses syntactic scopes and expression positions, never a source directory as an exemption.
TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[0-9]+|::|=>|[{}()\[\],;:=#|!]")

def mask_rust(text):
    """Keep offsets and newlines while removing Rust strings, characters and nested comments."""
    out, i = list(text), 0
    while i < len(text):
        end = None
        if text.startswith('//', i):
            end = text.find('\n', i)
            if end < 0: end = len(text)
        elif text.startswith('/*', i):
            depth, end = 1, i + 2
            while end < len(text) and depth:
                if text.startswith('/*', end): depth += 1; end += 2
                elif text.startswith('*/', end): depth -= 1; end += 2
                else: end += 1
        else:
            raw = re.match(r'r(#+)?"', text[i:])
            char = re.match(r"'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^'\\])'", text[i:]) if text[i] == "'" else None
            if raw:
                close = '"' + (raw.group(1) or '')
                at = text.find(close, i + len(raw.group()))
                end = len(text) if at < 0 else at + len(close)
            elif text[i] == '"':
                end = i + 1
                while end < len(text):
                    if text[end] == '\\': end += 2
                    elif text[end] == '"': end += 1; break
                    else: end += 1
            elif char: end = i + len(char.group())
        if end is None: i += 1; continue
        for n in range(i, end):
            if out[n] != '\n': out[n] = ' '
        i = end
    return ''.join(out)

def runtime_auth_codes(text):
    clean = mask_rust(text)
    ts = [(m.group(), m.start()) for m in TOKEN.finditer(clean)]
    vals = [t for t, _ in ts]
    ends, opened = {}, []
    for i, t in enumerate(vals):
        if t in ['{', '(', '[']: opened.append(i)
        elif t in ['}', ')', ']'] and opened:
            a = opened.pop(); ends[a] = i
    funcs, code_impls, enums, impls, imports = [], [], [], [], []
    for i, t in enumerate(vals):
        if t == 'use':
            j = i + 1
            while j < len(ts) and vals[j] != ';': j += 1
            imports.append((i, j))
        # rdm_core::protocol_variants! writes its body only into an `impl NodeProtocol` block: its
        # reserved constructors are that impl's, exempt by the same kind.
        if t == 'macro_rules' and vals[i + 1:i + 3] == ['!', 'protocol_variants'] and i + 3 in ends:
            impls.append((i + 3, ends[i + 3], 'NodeProtocol'))
        if t not in ['fn', 'impl', 'enum'] or i + 1 >= len(ts): continue
        j = i + 2
        while j < len(ts) and vals[j] not in ['{', ';']: j += 1
        if j not in ends or vals[j] != '{': continue
        if t == 'fn':
            # Only the innermost function's own test attribute is a fixture exemption.
            a = i - 1
            while a >= 0 and vals[a] not in ['{', '}', ';']: a -= 1
            prefix = clean[ts[a + 1][1]:ts[i][1]]
            fixture = bool(re.search(r'#\s*\[\s*(?:[A-Za-z_]+\s*::\s*)?test(?:\s*\]|\s*\()', prefix))
            funcs.append((j, ends[j], vals[i + 1], fixture))
        elif t == 'impl':
            impls.append((j, ends[j], 'NodeProtocol' if 'NodeProtocol' in vals[i:j] and 'for' in vals[i:j] else vals[i + 1]))
            if vals[i + 1] in ['Code', 'ReplyKind']: code_impls.append((j, ends[j]))
        elif t == 'enum': enums.append((j, ends[j]))
    aliases = {'PermissionDenied': 'PermissionDenied', 'Unauthenticated': 'Unauthenticated', 'Unauthorized': 'PermissionDenied', 'SenderNotSubject': 'PermissionDenied', 'CallerRefused': 'PermissionDenied'}
    for a, b in imports:
        for q in range(a, b - 2):
            if vals[q] in aliases and vals[q + 1] == 'as': aliases[vals[q + 2]] = aliases[vals[q]]
    found = []
    for i, (token, at) in enumerate(ts):
        name = aliases.get(token)
        if token == 'Unauthorized' and i >= 2 and vals[i - 2:i] == ['ReplyKind', '::']: name = 'PermissionDenied'
        if token == 'Code' and vals[i:i + 4] == ['Code', '::', 'from_number', '(']:
            arg = vals[i + 4] if i + 4 < len(vals) else ''
            if arg in ['7', '16'] or not arg.isdigit(): name = 'PermissionDenied/Unauthenticated via numeric factory'
        if token == 'Code' and vals[i:i + 3] == ['Code', '::', 'ALL']: name = 'PermissionDenied/Unauthenticated via ALL'
        if token == 'unauthorized' and i + 1 < len(vals) and vals[i + 1] == '(' and (i == 0 or vals[i - 1] != 'fn'):
            name = 'PermissionDenied via unauthorized(..)'
        if token in ['Unauthorized', 'SenderNotSubject'] and i + 1 < len(vals) and vals[i + 1] == '{':
            name = 'PermissionDenied via ' + token
        if name is None: continue
        if any(a < i < b for a, b in enums): continue
        # An import names a variant but constructs no value.
        if any(a <= i <= b for a, b in imports): continue
        a = i - 1
        while a >= 0 and vals[a] not in [';', '{', '}']: a -= 1
        if 'use' in vals[a + 1:i]: continue
        enclosing = [f for f in funcs if f[0] < i < f[1]]
        function = max(enclosing, default=None, key=lambda f: f[0])
        if function and (function[3] or function[2] == 'classify_reply'): continue
        if function and any(a < i < b and owner == 'Code' for a, b, owner in impls) and function[2] in ['from_number', 'number', 'name']: continue
        if function and function[2] == 'unauthorized' and any(a < i < b and owner == 'NodeProtocol' for a, b, owner in impls): continue
        if function and function[2] == 'code' and any(a < i < b and owner.endswith('Reply') for a, b, owner in impls): continue
        # Assertion macros compare codes; only their own expression range is exempt.
        if any(a < i < b and a >= 2 and vals[a - 1] == '!' and vals[a - 2] in ['assert', 'assert_eq', 'assert_ne', 'debug_assert', 'debug_assert_eq', 'debug_assert_ne'] for a, b in ends.items()): continue
        if not function and any(a < i < b for a, b in code_impls): continue
        # A pattern reads an existing code. An arm's expression constructs one.
        j = i + 1
        if j in ends and vals[j] == '{': j = ends[j] + 1
        while j < len(vals) and vals[j] in [')', '}', '|', 'Code', 'Self', '::', 'PermissionDenied', 'Unauthenticated']: j += 1
        if j < len(vals) and vals[j] == '=>':
            # Nested struct patterns read a value too. A guard may construct one, so only
            # exempt the pattern before its `if`, never a guard's expression.
            blocks = [a for a, b in ends.items() if vals[a] == '{' and a < i < j < b]
            start = max(blocks, default=-1) + 1
            q = start
            while q < i:
                if vals[q] == ',': start = q + 1
                q = ends[q] + 1 if q in ends and ends[q] < i else q + 1
            if 'if' not in vals[start:i]: continue
        if j < len(vals) and vals[j] == '=' and 'let' in vals[a + 1:i] and '=' not in vals[a + 1:i]: continue
        # matches!(value, PATTERN) — exempt only the pattern after its comma.
        in_pattern = False
        for a, b in ends.items():
            if a < i < b and vals[a] == '(' and a >= 2 and vals[a - 2:a] == ['matches', '!']:
                depth = 0
                for q in range(a + 1, i):
                    if vals[q] in ['(', '[', '{']: depth += 1
                    elif vals[q] in [')', ']', '}']: depth -= 1
                    elif vals[q] == ',' and depth == 0: in_pattern = True
        if in_pattern: continue
        found.append((clean.count('\n', 0, at) + 1, name))
    return found

for path in git("diff", "--name-only", f"{base}..{head}", "--", "*.rs").splitlines():
    try: text = git("show", f"{head}:{path}")
    except subprocess.CalledProcessError: continue
    added_lines, n = set(), None
    for line in git("diff", "-U0", f"{base}..{head}", "--", path).splitlines():
        m = re.match(r'@@ -\S+ \+(\d+)(?:,\d+)? @@', line)
        if m: n = int(m.group(1)); continue
        if n is None or line.startswith(('+++', '---')): continue
        if line.startswith('+'): added_lines.add(n); n += 1
        elif not line.startswith('-'): n += 1
    for n, code in runtime_auth_codes(text):
        if n in added_lines: hits.append(f"{path}:{n}: runtime construction of Code::{code}")
# Whole-tree mode: a word that reached zero in the tree stays at zero. Each entry is a case-insensitive
# extended regex over the whole tree at <head> (this script and its test name the words, so they are exempt).
TREE_BANNED = [
    # Q176 (Luke 2026-10-28): "there is no ready-for-traffic, there is ready", no alias, no decode, no mention.
    ("ready-for-traffic", r"ready-for-traffic|ReadyForTraffic|ready_for_traffic|ready for traffic", ["."]),
    # rdm#88: the page's word is re-born (same path.name, new node.id); a mesh is never reborn. The plans keep it
    # in quotations as written, so the check holds the code and the pages.
    ("reborn", r"reborn", ["crates", "docs/architecture", "docs/reference"]),
    # rdm#88: the word is revision; the Unix epoch (a time origin) is another sense and stays.
    ("epoch", r"(?<!unix[ _])(?<!_unix_)(?<!UNIX_)epoch", ["crates", "docs/architecture", "docs/reference"]),
    # rdm#88: the state is connections-deleting (RDM_CONNECTIONS_DELETE_DEADLINE_MS). The operation key drain-node: (Reserved6/7, the wire bytes) stays; Vec::drain is a method call and stays.
    ("drain", r"(?<![.A-Za-z])drain(s|ed|ing)?(?!-node:|[A-Za-z(])", ["crates", "tools", "docs/architecture", "docs/reference"]),
    # rdm#88: a deleted birth (node.deleted); the investigation's exited verdict is the proof.
    ("departed", r"departed", ["crates", "docs/architecture", "docs/reference"]),
    # rdm#88: target, never fence (CLAUDE.md). tools/ is not scanned (docgen's summary.rs toggles a markdown code fence); docs/architecture waits on the page edits.
    ("fence", r"fenc", ["crates", "docs/reference"]),
    # rdm#88: the investigation's exited verdict (CLAUDE.md: exit proof / proven exit). gossip.md still says "runtime-exit
    # proof", a page edit that waits on the lead, so docs/architecture is not scanned for it yet.
    ("exit proof", r"exit[ _-]?proof|proven[ _-]exit\b", ["crates", "docs/reference"]),
    # rdm#88 stage 2: the small words are at zero in crates/. The iroh log target `transports::relay` and the std call
    # `thread::park(` are not RDM's words and stay; the pages' own hits (gossip.md) wait on the lead's text.
    ("relay", r"(?<![:A-Za-z_])relay(?![A-Za-z_])", ["crates"]),
    ("park", r"(?<![:A-Za-z_])park(s|ed|ing)?(?![A-Za-z_(])", ["crates"]),
    ("tree", r"(?<![A-Za-z_-])trees?(?![A-Za-z_-])", ["crates"]),
    ("small words", r"\b(relaunch|operator|sticky|authorize|authoritative)\b", ["crates"]),
]
for label, pattern, scope in TREE_BANNED:
    out = subprocess.run(["git", "grep", "-n", "-I", "-i", "-P", pattern, head, "--", *scope,
                          ":!scripts/name-check.py", ":!scripts/test_name_check.py"],
                         capture_output=True, text=True).stdout
    for l in out.splitlines():
        m = re.match(r'^[^:]*:([^:]+:\d+:.*)$', l)  # <head>:<path>:<line>:<text>
        if m: hits.append(f"tree {label}: {m.group(1)[:160]}")
print("\n".join(hits))
