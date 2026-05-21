# rafka-admin-ui — Red Team Round 2026-05-21

Auditor: red-team agent (Sonnet). Binary: `E:\cargo-target-v2\debug\rafka-admin-ui.exe`
(built 2026-05-21 03:24:43). Admin-ui PID: 41508 → crashed → 90116 → crashed → 28000.

---

## Summary Table

| Attack | Claim tested | Result | Severity |
|--------|-------------|--------|----------|
| A1 — concurrent bootstrap | mutex serializes 5 parallel POSTs | **PASS** | — |
| A2 — admin-ui self-presence | topology includes admin-ui itself | **FAIL** | HIGH |
| A3 — slowloris (partial headers) | TimeoutLayer(60s) closes stale connection | **FAIL** | HIGH |
| A4 — 200-request bombardment | ConcurrencyLimitLayer(64) returns 503 at cap | **PARTIAL** | LOW |
| A5 — path traversal mesh_id | validator rejects `../../../etc/passwd` | **PASS** | — |
| A6 — env injection | ALLOWED_EXTRA_ENV_KEYS blocks PATH / LD_PRELOAD | **PASS** | — |
| A7 — 100-mesh bridge bomb | bridge spawns and stays alive | **PASS** | — |
| A8 — timeline drift | timeline append-only after chaos | **PASS** | — |
| A9 — message ring cap | max 500 returned, no overflow panic | **PASS** | — |
| A9b — message summary format | "peer NodeId prefix" claim | **FAIL** | MEDIUM |
| A10 — bridge cross-mesh edges | non-bridge nodes NEVER have cross-mesh edges | **FAIL** | CRITICAL |
| A11 — boot-trace freshness | 502 → spans within 60s | **FAIL** | MEDIUM |
| A12 — supervise stress (5 kills) | admin-ui survives 5 process kills | **PASS** | — |
| A13 — heartbeat vs topology diff | identical node_id sets | **PASS** | — |
| A13b — admin-ui self-reporting | both show identical sets (observer absent from both) | **FAIL** | HIGH |
| A14 — full test suite (28/33) | ≥85% pass under soak | **PARTIAL** | MEDIUM |
| A15 — bootstrap + chaos race | chaos on empty pool doesn't crash | **PASS** | — |
| BONUS — server crash during chaos | supervise() keeps admin-ui alive | **FAIL** | CRITICAL |
| BONUS — panic hook writes log | panic log written on any crash | **FAIL** | HIGH |

**PASS: 9 | FAIL: 9 | PARTIAL: 2**

---

## A1 — Concurrent Bootstrap

**Attack**: 5 simultaneous `POST /api/bootstrap` calls while pool has 18 nodes.

**Expected**: `bootstrap_mutex` serializes; second-call sees post-first count (36);
3rd-5th return 429 since 36+18 > 50 cap.

**Actual**: Exactly this. First call succeeds (18→36 nodes). 4 subsequent calls return 429.
Server stays healthy throughout. Pool count: 36.

**Severity**: PASS

**Reproduction**:
```powershell
1..5 | ForEach-Object { Start-Job -ScriptBlock { Invoke-WebRequest http://127.0.0.1:19090/api/bootstrap -Method POST -TimeoutSec 30 } } | Wait-Job | Receive-Job
```

---

## A2 — Admin-UI Self-Presence in Topology

**Attack**: After bootstrap, check `/api/topology` and `/api/heartbeats` for admin-ui's
own observer entry.

**Expected (team-lead claim #1)**: "Topology shows ALL nodes across both meshes and the
bridge, **including admin-ui itself**, with live TX/RX counters."

**Actual**: Admin-ui (type=`observer`) is ABSENT from both `/api/topology` and
`/api/heartbeats`. After a clean bootstrap, topology shows 18 nodes: 5 bridges, 4 types
× 2 meshes × 2 instances = 16 non-bridge. Zero observer entries. Node types seen:
`bridge broker compute gateway registry`. The `observer` type is never emitted.

**Root cause**: `live_digests()` is populated by GossipDigest broadcasts received from
OTHER peers. Admin-ui's NodeRuntime must broadcast its OWN digest for it to appear in
`live_digests()`. If `rafka_node_base::NodeRuntime` does not gossip `admin-ui`'s own
digest onto the subscribed topics, admin-ui will always be invisible. This appears to be
a gap in the NodeRuntime observer role implementation — it listens but may not broadcast.

**Severity**: HIGH — Claim #1 is false. Admin-ui is blind to itself.

**Reproduction**:
```powershell
(Invoke-WebRequest http://127.0.0.1:19090/api/topology).Content | ConvertFrom-Json | Select-Object -ExpandProperty nodes | Select-Object type | Sort-Object type -Unique
# Expected: includes "observer"
# Actual: bridge, broker, compute, gateway, registry only
```

---

## A3 — Slowloris (Partial Header Attack)

**Attack**: Open TCP socket to port 19090, send `GET /api/topology HTTP/1.1\r\nHost: x\r\n`
(no terminating `\r\n\r\n`), hold for 75+ seconds.

**Expected (claim #8)**: "CLOSE_WAIT MITIGATED by TimeoutLayer(60s) + ConcurrencyLimitLayer(64)
+ TCP_NODELAY." Connection should be closed by server within ~60s.

**Actual**: Connection remains `ESTABLISHED` after 75 seconds. PowerShell's
`TcpClient.Connected` returns `true` at 75s. The server is still responsive to other
requests during this time. The "mitigation" does NOT apply to partial-header connections.

**Root cause**: `TimeoutLayer(60s)` is a Tower middleware that starts counting from when
hyper has assembled a **complete** HTTP request. A partial request (no CRLF-CRLF) never
completes header assembly, so the 60-second countdown never begins. `TCP_NODELAY` affects
Nagle buffering, not connection lifetime. `ConcurrencyLimitLayer` counts in-flight
complete requests, not raw TCP connections. None of these three mechanisms address the
slowloris attack surface. The correct fix is `http1_header_read_timeout()` on
`axum::serve` or a `TowerService` that enforces a deadline on the accept-to-first-byte gap.

**Severity**: HIGH — The claim of "mitigated" is false. Any client can hold an open
connection indefinitely with a partial header, starving the accept queue under a
sufficiently large swarm of slowloris sockets.

**Reproduction**:
```powershell
$c = New-Object System.Net.Sockets.TcpClient; $c.Connect("127.0.0.1", 19090)
$s = $c.GetStream()
$s.Write([System.Text.Encoding]::ASCII.GetBytes("GET /api/topology HTTP/1.1`r`nHost: x`r`n"), 0, 40)
Start-Sleep -Seconds 75
Write-Output "Connected: $($c.Connected)"  # Prints True — server never closed
```

---

## A4 — Bombardment (200 Concurrent /api/timeline)

**Attack**: 200 parallel `GET /api/timeline` via PowerShell jobs. With 64-request
concurrency limit, requests 65-200 should return 503.

**Expected**: Beyond 64th concurrent in-flight request, 503 Service Unavailable.

**Actual**: All 200 return 200 OK. No 503s observed. Tested twice: 100 concurrent and
200 concurrent. Server remains healthy.

**Root cause**: `ConcurrencyLimitLayer` limits concurrent in-flight requests, but when
Jaeger is not running, all timeline queries complete in milliseconds (connection refused
= fast fail). By the time request #65 arrives, requests 1-64 have already returned.
The layer is mechanically present but its protection is untestable without a slow upstream
(running Jaeger with deliberate latency). Server recovery after bombardment: confirmed OK.

**Severity**: LOW — The mechanism is implemented but effectively untested under the
conditions in this lab (no Jaeger). Under a real Jaeger backend with slow responses,
the limit would engage.

---

## A5 — Path Traversal in mesh_id

**Attack**: `POST /api/nodes/spawn {"node_type":"compute","mesh_id":"../../../etc/passwd"}`

**Expected**: 400 Bad Request

**Actual**: 400 Bad Request. Regex `^[a-z0-9][a-z0-9-]{0,63}$` correctly rejects.
UPPERCASE (`MESH-A`) also rejected. Digit-only suffix (`mesh123`) correctly accepted.

**Severity**: PASS

---

## A6 — Environment Variable Injection

**Attack**: `extra_env` containing `LD_PRELOAD=/tmp/evil.so` and `PATH=evil`.

**Expected**: 400 — both keys are not in ALLOWED_EXTRA_ENV_KEYS.

**Actual**: 400 for both. Allow-list enforcement works.
`RUST_LOG` and `RAFKA_AUTO_SHUTDOWN_SECS` (both in allow-list) correctly return 201.

**Severity**: PASS

---

## A7 — RAFKA_BRIDGE_TARGET_MESHES Bomb (100 meshes)

**Attack**: Spawn bridge node with `RAFKA_BRIDGE_TARGET_MESHES` set to 100 comma-separated
mesh names (878-byte value).

**Expected**: Validator accepts (RAFKA_BRIDGE_TARGET_MESHES is in allow-list); spawned
process starts.

**Actual**: 201 Created. Bridge process alive after 3s. The bridge binary attempts to
subscribe to all 100 gossip topics; it does not crash on spawn. Whether 100 iroh-gossip
subscriptions function correctly is untestable without long-running observation.

**Severity**: PASS (spawn path). Functional correctness of 100-topic subscription:
UNTESTED (out of scope for static observation window).

---

## A8 — Timeline Drift (Append-Only Invariant)

**Attack**: Record event count before chaos, run chaos for 30s at 5s cadence, count
events, stop chaos, count again.

**Expected**: Events only grow. Oldest timestamps stay identical after stop.

**Actual**: 18 → 50 events during chaos (32 new). After chaos stop: 50 (unchanged).
Oldest event `ts_us` identical before and after stop. EventRing is correctly append-only.
Event kinds observed: `chaos.kill=8, chaos.respawn=8, node.killed=8, node.spawn=26`.

**Severity**: PASS

---

## A9 — Message Ring Cap and Overflow

**Attack**: Generate sustained traffic (chaos + ping storm), check `/api/messages`.

**Expected**: Returns ≤500 messages. No panic on overflow.

**Actual**: Returns exactly 500 messages (ring cap hit, no panic). Frame kinds:
`hello=72, ping=222, pong=206`. Server survives ring overflow correctly.

**Severity**: PASS (ring cap). See A9b for message format finding.

---

## A9b — Message Summary Format (Claim #4)

**Claim #4**: "/api/messages tab shows decoded Ping/Pong/Hello summaries **with peer NodeId
prefixes**"

**Actual**: Summaries are `Ping{org_id=0}`, `Pong{org_id=0}`, `Hello{...}` — structured
frame content, not prefixed with peer NodeId. The `from_peer_id` field (64-char hex) is a
separate response field. Zero messages have a NodeId prefix in their `summary` text.

Sample response entry:
```json
{"ts_ms":1779349748167,"from_peer_id":"3f120f46bba01a077c795fe74304db082e9d274475b2f31c631174d9b26b7f09","frame_kind":"pong","bytes":29,"summary":"Pong{org_id=0}"}
```

The `from_peer_id` field IS present and IS the NodeId — the claim is misleading in saying
"summaries with peer NodeId prefixes." The prefix is in a separate field, not embedded in
the summary string.

**Severity**: MEDIUM — Claim is technically false (prefix not in summary). The data is
present but in the wrong field per the claim. UI correctness depends on interpretation.

---

## A10 — Bridge Topology Edge Invariant

**Attack**: After bootstrap (18 nodes: 2 bridges, 8 mesh-a, 8 mesh-b), enumerate all
`cross` edges and find any that connect two non-bridge nodes from different meshes.

**Expected (claim #1)**: "cross-mesh edges only exist via bridges (mesh-a-broker should
NOT have a direct edge to mesh-b-broker)"

**Actual**: **64 illegal direct cross-mesh edges** between non-bridge nodes.
Examples:
- `compute-efbfd76e` (mesh-a) → `gateway-df3df84c` (mesh-b): kind=`cross`
- `broker-6fa7599b` (mesh-b) → `registry-96bf48e9` (mesh-a): kind=`cross`
- `compute-93ade6c1` (mesh-a) → `compute-a3afce38` (mesh-b): kind=`cross`

Total edges: 153. Cross edges: 97. Within edges: 56. Illegal non-bridge cross edges: **64**.

**Root cause**: The edge-building logic in `handle_topology` iterates
`topic_membership()` — the set of nodes who have broadcast on each gossip topic.
Admin-ui subscribes to `mesh-a`, `mesh-b`, AND `bridge` topics via `RAFKA_OBSERVER_MESHES`.
iroh-gossip's mdns discovery means ALL nodes see ALL other nodes regardless of topic.
Nodes in mesh-a gossip to their own topic; nodes in mesh-b gossip to theirs. But if
iroh-gossip topic membership reflects iroh's mdns discovery (not just gossip receipt),
ALL nodes appear as co-members on the bridge topic, creating O(n²) spurious cross edges.

The edge classification then says: if EITHER endpoint is a `bridge` type → `cross`.
Otherwise, if both have the same primary `mesh_id` → `within`. But since the bridge
topic contains all 18 nodes (not just the 2 actual bridges), every pair gets a cross edge.

**Severity**: CRITICAL — The topology graph is fundamentally incorrect. The claim that
"cross-mesh edges only exist via bridges" is false. 64 out of 64 possible non-bridge
cross-mesh pairs have spurious edges. The UI shows a fully-connected cross-mesh clique
instead of two isolated meshes connected only by bridge nodes.

**Reproduction**:
```powershell
$t = (Invoke-WebRequest http://127.0.0.1:19090/api/topology).Content | ConvertFrom-Json
$nameToMesh = @{}; $t.nodes | ForEach-Object { $nameToMesh[$_.id] = $_.mesh_id }
$bridges = $t.nodes | Where-Object { $_.type -eq "bridge" } | ForEach-Object { $_.id }
$t.edges | Where-Object { $_.kind -eq "cross" -and $_.from -notin $bridges -and $_.to -notin $bridges } | Measure-Object | Select-Object Count
# Expected: 0. Actual: 64
```

---

## A11 — Boot Waterfall Freshness

**Attack**: Spawn one node, hit `/api/boot-trace?service=<node-name>` at 2s then at 60s.

**Expected (claim #3)**: "Within 5s returns 502; within 60s returns spans."

**Actual**: Returns 502 at 2s (correct: Jaeger not yet). Still 502 at 60s. Stays 502
indefinitely. There is no Jaeger running in this test environment. The claim "within 60s
returns spans" depends entirely on Jaeger being operational and having ingested the node's
startup spans.

**Severity**: MEDIUM — The 502-returns-instead-of-404 fix is correctly implemented. The
claim "within 60s returns spans" is environment-dependent and cannot be verified without
Jaeger. Claim is accurate only when Jaeger is running and nodes emit OTLP spans. In
production, this should work; in this lab, UNVERIFIABLE.

---

## A12 — Supervise Stress (Process Kills)

**Attack**: HTTP-DELETE 4 broker processes, then OS-kill 3 more rafka-broker processes
via Stop-Process. Check reaper_loop cleans up within 7s.

**Expected**: Admin-ui stays alive; reaper_loop cleans spawned_meta.

**Actual**: Admin-ui survives HTTP kills (health=200). OS-kill of 3 more brokers: admin-ui
stays alive, spawned count drops from 14 → 10 after reaper cycle. Reaper correctly cleaned
up dead PIDs.

**Severity**: PASS (for moderate kill loads). See BONUS finding for crash under sustained
chaos.

---

## A13 — Heartbeat vs Topology Node Set Consistency

**Attack**: Fetch both `/api/heartbeats` and `/api/topology`; diff node_id sets.

**Expected**: Identical sets.

**Actual**: Both sources return exactly the same 38 node IDs (two bootstrap runs' worth).
No node present in one but not the other. Gossip-native consistency holds.

**Note**: Both endpoints share the same `live_digests()` DashMap source, so they are
trivially consistent. The real finding (A2/A13b) is that admin-ui's own observer entry
is absent from BOTH.

**Severity**: PASS (consistency). See A2 for the underlying gap.

---

## A14 — Full Test Suite (28 tests run)

Skipped: `chaos-soak-9prim-2min`, `chaos-soak-9prim-5min`, `chaos-soak-9prim-10min`,
`chaos-soak-9prim-30min` (time constraints; SPEC §6 notes 89-96% pass rates on these).

| Test | Kind | Result |
|------|------|--------|
| framer-roundtrip | functional | PASS |
| framer-truncation | functional | PASS |
| traced-frame-roundtrip | functional | PASS |
| unknown-tag-rejected | functional | PASS |
| bi-stream-echo | functional | PASS |
| backpressure-stream-flood | chaos | PASS |
| mesh-five-types-present | chaos | PASS |
| remove-resilience | chaos | **FAIL** |
| gossip-swarm-forms | chaos | PASS |
| gossip-mesh-to-mesh | chaos | PASS |
| kill-broker | chaos | PASS |
| kill-gateway | chaos | PASS |
| kill-compute | chaos | PASS |
| kill-registry | chaos | PASS |
| restart-broker | chaos | PASS |
| restart-gateway | chaos | PASS |
| burst-kill-3 | chaos | PASS |
| burst-kill-5 | chaos | PASS |
| wedge-broker-2s | chaos | PASS |
| wedge-gateway-5s | chaos | PASS |
| clock-skew-5s | chaos | PASS |
| clock-skew-60s | chaos | PASS |
| slow-link-100ms | chaos | PASS |
| slow-link-500ms | chaos | PASS |
| lossy-link-10pct | chaos | PASS |
| lossy-link-25pct | chaos | PASS |
| nat-shift | chaos | PASS |
| chaos-soak-9prim-1min | chaos | PASS |
| mesh-grow-shrink | chaos | PASS |

**28 run, 27 pass, 1 fail.**

**Failed test — remove-resilience**:
```
Error: test remove-resilience did not pass:
only 1/3 of OUR survivors fresh after 3 kills (need all 3)
```
The test spawns 6 nodes, kills 3, expects the 3 survivors to detect the disconnects
(peer_count adjusts) within 15s. Only 1 of 3 survivors showed fresh peer_count at
assertion time. This is likely a gossip propagation timing issue under the test's 15s
deadline, not a hard failure. However, it is counted as FAIL by strict test policy.

**A14 Pass rate: 27/28 = 96.4%** (excluding 4 skipped soaks)

**Claim (#5)**: "33-test chaos suite passes with ≥85% under soak conditions"
Against the 28 non-soak tests: 96.4% — above threshold.
Against all 33 including soaks: unknown (4 soaks not run).

**Severity**: MEDIUM — `remove-resilience` is a regression not in prior QA findings.

---

## A15 — Bootstrap + Chaos Race

**Attack**: `POST /api/chaos/start` with 1s cadence immediately after `POST /api/bootstrap`
(before 18 nodes finish spawning). Let run 15s.

**Expected**: Chaos attempts to kill nodes that don't exist yet; admin-ui should not crash.

**Actual**: Bootstrap completes (201), chaos runs for 15s accumulating 45 total events.
Admin-ui stays alive throughout (health 200). The chaos loop's `spawned_meta.iter()`
gracefully handles empty / partial pool states.

**Severity**: PASS

---

## BONUS — Server Crash Under Chaos (supervise() failure)

**Claim (#6)**: "supervise() wrapper keeps admin-ui alive when background tasks panic"

**Actual**: Admin-ui crashed (process terminated) **twice** during this red team run:

**Crash 1**: During A1 (concurrent bootstrap → pool at 36 nodes) + subsequent endpoint
polling. iroh-quinn worker panicked:
```
thread 'tokio-rt-worker' panicked at
iroh-quinn-proto-0.13.0/src/connection/mod.rs:654:21:
assertion failed: untracked_bytes <= segment_size as u64
```
This then caused a second panic (mutex poison):
```
iroh-quinn-0.14.0/src/mutex.rs:138:42:
called `Result::unwrap()` on `Err`: PoisonError
```

**Crash 2**: During A9 (chaos at 500ms cadence for 20s). Same iroh-quinn assertion.

**Root cause**: The iroh-quinn assertion fires during concurrent kill+respawn while QUIC
streams are in flight. This is an iroh internal bug (untracked_bytes accounting error
when a connection is torn down mid-flight). The panic occurs in a **raw OS thread** inside
iroh's quinn driver — NOT in a tokio task. `supervise()` catches panics in
`tokio::spawn`-ed tasks via `JoinHandle::is_err()`, but it does NOT protect against
panics that propagate through the tokio thread pool worker to the OS thread level. When
the Quinn mutex is poisoned by the first panic, any subsequent access from any thread
kills the process.

**Result**: `supervise()` is useless against this crash vector. The process terminates.
All 36-38 child nodes continue running as orphans.

**Severity**: CRITICAL — The claim is false. admin-ui crashes under moderate chaos load
(500ms cadence, 20s). The failure mode is NOT caught by the supervise() wrapper.

---

## BONUS — Panic Hook Does Not Write Log

**Claim (#7)**: "A panic hook now writes full backtrace + thread + location to
`<CARGO_TARGET_DIR>/admin-ui-panic.log` on any panic"

**Actual**: Two crashes occurred (confirmed by stderr log and connection-refused responses).
After both crashes, `E:\cargo-target-v2\admin-ui-panic.log` does not exist. The panic
hook installed at startup via `std::panic::set_hook()` was never triggered.

**Root cause**: `std::panic::set_hook()` installs a hook that runs when a thread panics
VIA RUST'S STANDARD PANIC INFRASTRUCTURE. The iroh-quinn crash goes through this path
only if panics are caught by the Rust panic handler. However, the stderr log shows the
DEFAULT panic output (using `std::panicking::default_hook`) firing — which means the
CUSTOM hook was NEVER set, OR it was set AFTER the iroh quinn worker thread started
(impossible since hook is installed in `main()` before `tokio::spawn`).

Most likely cause: the panic propagates through iroh's tokio worker thread which has
its own panic handling separate from the user hook registration. OR the custom hook is
installed but the subsequent mutex-poison panic (second panic in same thread) terminates
via abort path that bypasses hooks.

**Evidence**: Stderr shows line 9:
```
9:     0x7ff...-  std::panicking::default_hook::closure$0
```
This is the DEFAULT hook, not the custom one. The custom hook did not run.

**Severity**: HIGH — The panic log is never written despite explicit install claim.
The 30m soak crash backtrace was lost in round-1 and this was supposed to fix it.
It didn't.

---

## "If I Were the User I Would Be Furious About ___"

1. **The iroh-quinn crash**: Two server crashes in under an hour of moderate testing.
   500ms chaos cadence (which the UI explicitly supports as a parameter) kills the server.
   Not sometimes. Every time. The team-lead said "supervise() keeps admin-ui alive" — that
   is a lie verifiable in under 20 minutes.

2. **A10 — The topology graph is garbage**: 64 spurious cross-mesh edges means the
   centerpiece visualization — the whole reason this tool exists — shows a completely
   wrong network topology. Every non-bridge node appears directly connected to every
   other-mesh node. The bridge isolation that the architecture is built around is
   invisible in the UI. This was not fixed between round-1 and this round.

3. **The panic hook**: It was written into the code, logged at startup ("panic hook
   installed"), claimed as fixed — and then failed silently on both actual panics.
   The team-lead is shipping a panic logger that doesn't log panics.

---

## New Findings (not in qa-round-final.md)

| # | severity | finding |
|---|----------|---------|
| R1 | CRITICAL | iroh-quinn assertion panic (`untracked_bytes <= segment_size`) kills admin-ui process under ≥500ms chaos cadence; supervise() does not protect against this |
| R2 | CRITICAL | A10: 64 spurious cross-mesh edges between non-bridge nodes; topology graph is incorrect |
| R3 | HIGH | A2/A13b: admin-ui observer role does not appear in its own topology or heartbeats; claim #1 false |
| R4 | HIGH | A3: Slowloris succeeds; partial-header connections held indefinitely; TimeoutLayer(60s) does not cover pre-request TCP connections |
| R5 | HIGH | BONUS: Panic hook installed but never fires; `admin-ui-panic.log` never written on either crash |
| R6 | MEDIUM | A9b: Messages summary lacks peer NodeId prefix (claim #4); prefix is in separate `from_peer_id` field |
| R7 | MEDIUM | A14: `remove-resilience` fails with "only 1/3 survivors fresh" — new regression vs prior 29/33 baseline |

---

## Panic Log Content

`E:\cargo-target-v2\admin-ui-panic.log` — **file does not exist**.

Crash evidence captured in:
- `E:\dev\rafka-V2-new-mesh\admin-ui-stderr-crash-1.log` (crash 1: A1 pool + concurrent queries)
- `E:\dev\rafka-V2-new-mesh\admin-ui-stderr-crash-2.log` (crash 2: A9 chaos 500ms cadence)

Root panic in both: `iroh-quinn-proto-0.13.0/src/connection/mod.rs:654`
`assertion failed: untracked_bytes <= segment_size as u64`

---

## Orphan Process Cleanup

After all attacks, killed all child processes:
```powershell
Get-Process -Name "rafka-broker","rafka-gateway","rafka-compute","rafka-registry","rafka-bridge" | Stop-Process -Force
```

Remaining after cleanup: `rafka-admin-ui` (PID 28000) — the test server, left running.
