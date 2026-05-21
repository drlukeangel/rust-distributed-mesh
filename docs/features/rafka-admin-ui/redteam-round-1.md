# rafka-v2-mesh-ui — Red Team Round 1

Audit by `rafka-redteam-pa` (Sonnet) against Phase A (background Jaeger
cache). 2026-05-20. **8 findings, all fixed + verified.**

## Findings

### A#3 — CRITICAL: Bootstrap flood deadlocks server permanently
**Reproducer**: 5 concurrent `POST /api/bootstrap` calls → spawn 90 nodes
(5 × 18) → tokio runtime stops issuing HTTP responses; TCP connections
accepted but hang forever; server never recovers without restart.

**Fix** (`handle_bootstrap` + `AppState.bootstrap_mutex`):
- Take a `tokio::sync::Mutex` lock FIRST so concurrent callers queue.
- Then check `state.spawned_meta.len() + 18 ≤ POOL_CAP` (50). Otherwise 429.
- Second caller sees the actual post-first-bootstrap count → either spawns
  on top OR gets rejected with a 429 explaining the cap.

**Verified**: 5 parallel POSTs → 2 succeed (36 total), 3 capped. Server stays
healthy.

### A#1 — HIGH: Uppercase mesh_id bypasses validator
**Reproducer**: `POST /api/nodes/spawn {"mesh_id":"UPPERCASE"}` → 201
(should be 400). Validator used `is_ascii_alphanumeric` which matches both
cases.

**Fix** (`is_safe_mesh_id`):
- Replaced `is_ascii_alphanumeric` with explicit
  `is_ascii_lowercase || is_ascii_digit`. First char must be one of those;
  subsequent chars may also include `-`.

**Verified**: UPPERCASE → 400, Mesh-A → 400, mesh-a → 201.

### A#2 — HIGH: chaos/start silently ignores cadence_ms body field
**Reproducer**: `POST /api/chaos/start {"cadence_ms":100}` → state shows
`cadence_ms: 30000`. Body was parsed but not honored.

**Fix** (`handle_chaos_start` + new `ChaosStartRequest`):
- Parse `cadence_ms` from optional body, clamp to `[500, 600_000]`, store
  via `state.chaos.cadence_ms.store()`.

**Verified**: `{"cadence_ms":2000}` → response `cadence_ms: 2000`.

### A#4 — MEDIUM: `/api/alerts` exceeds 4 s budget
**Reproducer**: 3.0–5.4 s measured against Jaeger-down state. Previous fix
set 3 s timeout; connection setup overhead pushed it past the budget.

**Fix** (`handle_alerts`):
- Tightened timeout to 2 s so total wall stays under 4 s even with retries.

### A#5 — MEDIUM: Secret keys persist in spawn dirs
`node-identity.json` contains `{secret_key_hex:"..."}` per spawned node.
Files persisted after node death.

**Fix**: combined with A#6.

### A#6 — MEDIUM: Dead node directories leak (209 dirs for 20 live nodes)
**Reproducer**: Spawn 200, kill all → 200 dirs remain under
`E:/tmp/rafka-ui-nodes/`. Each dir contains identity key + state.

**Fix** (`reaper_loop`):
- Reaper now removes `spawned_meta[name]` AND deletes
  `E:/tmp/rafka-ui-nodes/{name}` when a process is detected exited.
- Second pass each cycle: scan the spawn-dir, delete any dir whose name is
  in neither `processes` nor `spawned_meta` (catches pre-reaper crashes).
- Closes A#5 since dirs containing identity keys are now reaped.

### A#7 — LOW: Double DELETE returns 404 instead of idempotent 200
**Reproducer**: Second DELETE → `{"error":"no subprocess named ..."}` 404.
Operator retry loops break.

**Fix** (`handle_kill`): returns 200 with `reason: "already_gone"` on the
Err path instead of 404.

### A#8 — LOW: Concurrent /api/tests/run silently returns stale report
**Reproducer**: 3 parallel calls to `POST /api/tests/run` for same test →
all "succeed" but return identical timestamps (same cached report file).

**Fix** (`handle_test_run` + `AppState.running_tests` DashMap):
- Insert `(name, ())` at start; if already present, return 409.
- `RunGuard` Drop impl removes the entry on completion / panic / early
  return so the same test can be re-run later.

## Summary

| sev | findings | fixed | verified |
|-----|----------|-------|----------|
| critical | 1 | 1 | ✓ |
| high     | 2 | 2 | ✓ |
| medium   | 3 | 3 | ✓ (A#5 closed by A#6 fix) |
| low      | 2 | 2 | ✓ |
| **total** | **8** | **8** | **✓** |

Defended successfully (no findings): empty/65-char/slash/dot mesh_id,
node_type fuzzing, path traversal via URL, 20-parallel pre-flood spawn,
JSON prototype pollution, 50-key extra_env bomb, empty body, missing
Content-Type.

---

## Round-1 second pass — static analysis findings

Same agent ran a static review of the source after the round-1 fixes
shipped, found 4 additional bugs the dynamic audit missed.

### F#1 — HIGH: Path traversal via DELETE /api/nodes/{node_name}
`node_name` from the URL path went directly into
`format!("E:/tmp/rafka-ui-nodes/{}", node_name)` then `remove_dir_all`.
DashMap guard rejects unknown names so the actual exploit window was
narrow, but defense-in-depth was missing.

**Fix** (`handle_kill`): validate against
`^(gateway|broker|compute|registry|bridge)-[0-9a-f]{8}$` at handler entry,
return 400 on mismatch. New helper `is_valid_node_name()`.

### F#2 — HIGH: Arbitrary env var injection via extra_env
No allow-list. A caller could set `PATH`, `LD_PRELOAD`, `RAFKA_DATA_DIR`,
etc. to hijack the spawned child.

**Fix** (`validate_extra_env` + `ALLOWED_EXTRA_ENV_KEYS`): allow only
`RAFKA_MESH_ID`, `RAFKA_LINK_SLOW_MS`, `RAFKA_LINK_LOSS_PCT`,
`RAFKA_CLOCK_SKEW_MS`, `RAFKA_NODE_BIND_ADDR`,
`RAFKA_BRIDGE_TARGET_MESHES`, `RAFKA_AUTO_SHUTDOWN_SECS`, `RUST_LOG`.
Validated at both `handle_spawn` AND inside `spawn_one` for
defense-in-depth (covers bootstrap + chaos respawn paths).

### F#3 — MEDIUM: Test name not validated in /api/tests/run
`body.name` used as both CLI arg AND file-path component
(`E:/tmp/rafka-tests/{name}-{seed}.json`). Crafted name like
`../../../Windows/System32/evil` escapes the tests dir.

**Fix** (`handle_test_run`): validate `^[a-z0-9][a-z0-9-]*$` with len ≤ 64
at handler entry. tokio Command::args is shell-safe but file paths aren't.

### F#4 — MEDIUM: Cache staleness window not surfaced
`TopologySnapshot` carries `computed_at_ms` but `/api/topology` +
`/api/heartbeats` didn't include it in responses. Clients can't detect
when Jaeger is slow and the cache is stale.

**Fix**: include `computed_at_ms` in both response bodies. UI can compute
`age = Date.now() - computed_at_ms` and display a "stale data" warning if
> 10s.

### F#6 — LOW: Chaos loop respawn bypasses POOL_CAP
Pool cap was enforced only in `handle_bootstrap`. The chaos loop calls
`spawn_one` directly, so under crash-storm conditions it could grow the
pool past 50.

**Fix**: moved `POOL_CAP` check into `spawn_one` itself. Bootstrap still
checks separately (so it can return 429 instead of partial-success). Now
both paths obey the cap.

(F#5 was a non-finding — informational only — the agent confirmed the
mutex pattern is correct.)

## Updated summary

| sev | found | fixed | verified |
|-----|-------|-------|----------|
| critical | 1 | 1 | ✓ |
| high     | 4 | 4 | ✓ (2 from dynamic, 2 from static) |
| medium   | 5 | 5 | ✓ (3 from dynamic, 2 from static) |
| low      | 3 | 3 | ✓ (2 from dynamic, 1 from static) |
| **total** | **13** | **13** | **✓** |
