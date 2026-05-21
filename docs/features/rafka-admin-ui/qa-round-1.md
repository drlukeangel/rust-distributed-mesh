# rafka-v2-mesh-ui — QA Round 1 Findings

Audit by adversarial-qa agent `rafka-ui-qa` (Sonnet). Tested 2026-05-20
against port 19105 with Jaeger DOWN (stress-test for "never hang"
requirement). Some findings reverified on port 19106 with Jaeger UP — noted
inline.

## Findings (priority order)

### F#1 — CRITICAL: `mesh_id` body field on `/api/nodes/spawn` is silently ignored
**Severity**: critical · **Status**: fixed (see commit)

`SpawnRequest` only accepts `extra_env.RAFKA_MESH_ID`. Sending a top-level
`body.mesh_id` field falls through to "default". UI workaround was using
the env-nested form, but the API contract was undocumented and confusing.

**Fix**: Add `mesh_id: Option<String>` field to `SpawnRequest`. In
`handle_spawn`, if present, inject it into `extra_env.RAFKA_MESH_ID` before
calling `spawn_one`.

### F#2 — CRITICAL: `/api/nodes` had no per-request timeout
**Severity**: critical · **Status**: fixed

`handle_nodes` made a bare `state.http.get(url).send()` call. The global
client timeout (4 s) DOES cover this, so the actual hang was bounded — but
QA tested against an older binary that pre-dated the global timeout.

**Verification on port 19106**: returned in 2.4 s (within 4 s budget). No
hang reproduced.

**Fix**: explicitly add `.timeout(Duration::from_secs(4))` per-call so the
budget is documented at the call site, not the client level.

### F#3 — HIGH: Local endpoint latency claims in PRD are unrealistic
**Severity**: high · **Status**: doc fix shipped

PRD claimed `<10 ms` for `/api/health`, `/api/chaos/state`,
`/api/chaos/start`. Reality measured ~2000 ms cold, ~25 ms warm (PowerShell
loopback overhead + tracing instrumentation). The 10 ms claim was
aspirational, not measured.

**Fix**: Updated PRD API table to honest ranges (`<50 ms warm, up to ~2 s
cold`).

### F#4 — HIGH: `/api/alerts` exceeds 4 s budget when Jaeger down
**Severity**: high · **Status**: fixed

With Jaeger unreachable, `/api/alerts` took 5.3 s on the QA's measurement.
The reqwest call has the global 4 s client timeout, but connection setup
adds overhead.

**Fix**: per-call `.timeout(Duration::from_secs(3))` in `handle_alerts`
keeps total wall time under 4 s even with TCP overhead.

### F#5 — HIGH: `remove-resilience` test counts the ambient pool, not its own spawned set
**Severity**: high · **Status**: fixed in `cli/rfa/src/main.rs`

The test spawns 6 nodes, kills 3, then queries `/api/heartbeats` and counts
ALL nodes with fresh age_ms. Against a warm server with 30+ pre-existing
nodes, the pass criterion (`fresh >= 3`) is met vacuously regardless of
whether any of the 6 test nodes survived.

**Fix**: Track the 6 spawned node_names explicitly, filter heartbeats to
ONLY those names when counting survivors.

### F#6 — MEDIUM: `framer-roundtrip` baseline drift (cargo overhead)
**Severity**: medium · **Status**: doc fix shipped

Baseline says 0.5 s; actual is 2.1 s due to cargo invocation overhead. The
underlying test runs in 30 ms but cargo spends 2 s on dependency-graph
check.

**Fix**: Updated test-registry.md to reflect cold-cargo timing
(~2 s for `framer-*` and `traced-*` and `unknown-tag-*`).

### F#7 — MEDIUM: `chaos-soak` event count is variable, presented as fixed
**Severity**: medium · **Status**: doc fix shipped

Baseline table shows `8 events`; actual report shows `7 events`. Event
count depends on chaos cadence timing.

**Fix**: Replaced fixed counts with `≥N events` ranges.

### F#8 — MEDIUM: `mesh_id` not sanitized — slashes, spaces, unicode silently accepted
**Severity**: medium · **Status**: fixed

`spawn_one` accepts any string in `RAFKA_MESH_ID`. Slashes break Jaeger
query filtering; spaces break CSS class lookup on the React side. Note:
this was masked by F#1 (mesh_id was ignored anyway).

**Fix**: Validate mesh_id against `^[a-z0-9][a-z0-9-]{0,63}$` in
`spawn_one`. Reject with 400 on violation.

### F#9 — LOW: Bootstrap is additive, not idempotent
**Severity**: low · **Status**: documented (no code change)

`POST /api/bootstrap` always spawns 18 fresh nodes regardless of current
state. Calling it twice yields 36 nodes.

**Decision**: This is intentional — operators may want to expand the pool
during testing. Updated PRD + how-to.md to explicitly state "additive,
clear with kill buttons or restart UI if you want a fresh slate".
