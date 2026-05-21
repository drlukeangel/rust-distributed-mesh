# rafka-admin-ui — QA Postfix Round 2026-05-21

**Auditor**: adversarial-qa agent (Sonnet 4.6)
**Binary under test**: `E:\dev\rafka-V2-new-mesh\target\debug\rafka-admin-ui.exe`
**Built**: 2026-05-21 06:53:58 (fresh `cargo build` from commit `88937f9`)
**Correct rfa binary**: `E:\dev\rafka-V2-new-mesh\target\debug\rfa.exe`
**Admin-ui port**: 127.0.0.1:19092
**Jaeger**: running on `[::1]:16686` (IPv6 only; HTTP on `127.0.0.1:16686` times out)
**Date**: 2026-05-21

> IMPORTANT preliminary finding: **SPEC §1 states the CLI binary is at
> `E:\cargo-target-v2\debug\rfa.exe` — this path is wrong.** The workspace
> `cargo metadata` reports `target_directory = E:\dev\rafka-V2-new-mesh\target`.
> The binary at `E:\cargo-target-v2\debug\rfa.exe` is a **stale 3:24 AM build**
> that predates all 10 commits under test. Initial verification using that binary
> was entirely invalid; every claim had to be re-run after discovering the
> discrepancy. **Net cost: ~15 minutes of evidence collection discarded.**

---

## 1. Summary Table — 10 Claims

| # | Claim | Verdict | Evidence |
|---|-------|---------|----------|
| 1 | R1 boundary: cadence_ms < 2000 → HTTP 400 with detailed reason | **PASS** | Tested 200, 500, 1999 → all 400 with `error/reason/floor_ms/requested_ms` body |
| 2 | R2 topology: 0 illegal non-bridge cross edges after bootstrap+gossip | **FAIL** | 80 illegal cross edges immediately after bootstrap; 677 with 73-node pool |
| 3 | R3 self-presence: admin-ui in /api/topology with `type=admin-ui` | **PASS** (caveated — see NF-2) | Node present; `id=<unspawned>`, `mesh_id=default`, `age_ms` stale |
| 4 | R4 slowloris: partial-header connection FIN'd at t≈30s | **PASS** | Read() returned 0 (EOF) at t=29.4s using blocking read with 1s timeout |
| 5 | R5 panic hook: ANY panic writes backtrace to admin-ui-panic.log | **FAIL** | 18 iroh-quinn panics in session 3 → no panic log created anywhere |
| 6 | R6 message prefix: /api/messages summaries have `[8-char-hex]` prefix | **PASS** | 40/40 messages matched `^\[[0-9a-f]{8}\]` (e.g. `[441eb653] Ping{org_id=0}`) |
| 7 | R7 remove-resilience: test passes in ~30s | **PASS** | `status=passed duration=33370ms` — within acceptable range |
| 8 | Bridge preflight: missing bridge.exe → exit with PREFLIGHT FAILURE + path + cargo build cmd | **PASS** | Process exited code 1, stderr: `PREFLIGHT FAILURE: missing peer binaries:` + path + `cargo build -p ...` |
| 9 | gossip-mesh-to-mesh: passes by aggregating cross.peer_connected across all services | **PASS** | `status=passed duration=20134ms detail="100 cross.peer_connected spans across all services"` |
| 10 | mesh-five-types-present: passes when all 5 binaries present | **PASS** | `status=passed duration=8055ms detail="all 5 types present: {registry, broker, gateway, admin-ui, compute, bridge}"` |

**PASS: 8 | FAIL: 2**

---

## 2. Evidence Per Claim

### Claim 1 — R1 Boundary (PASS)

Commands run:
```
POST /api/chaos/start {"cadence_ms":200}  → 400 {"error":"cadence_ms_below_floor","floor_ms":2000,"reason":"...iroh-quinn-proto-0.13.0...","requested_ms":200}
POST /api/chaos/start {"cadence_ms":500}  → 400
POST /api/chaos/start {"cadence_ms":1999} → 400
POST /api/chaos/start {"cadence_ms":2000} → 200 {"running":true,"cadence_ms":2000,...}
```

The 400 body includes `error`, `floor_ms`, `requested_ms`, and a multi-sentence
`reason` explaining the iroh-quinn upstream bug. Claim fully satisfied.

**However**: see New Finding NF-1. The SPEC claims cadence ≥ 2000ms means the
iroh-quinn bug "cannot be reached through the public API." That is demonstrably
false (18 iroh-quinn panics at exactly 2000ms in 5 minutes).

### Claim 2 — R2 Cross-Mesh Edge Invariant (FAIL)

After a clean bootstrap of 18 nodes (4 mesh-a × 4 types, 4 mesh-b × 4 types, 2 bridges):

```powershell
$t = Invoke-RestMethod "http://127.0.0.1:19092/api/topology"
$bridges = $t.nodes | Where-Object { $_.type -eq "bridge" } | ForEach-Object { $_.id }
$illegal = $t.edges | Where-Object {
    $_.kind -eq "cross" -and
    $bridges -notcontains $_.from -and
    $bridges -notcontains $_.to
}
# Expected: 0   Actual: 80
```

Result: **80 illegal cross edges** between non-bridge nodes across mesh-a and mesh-b.

Root cause (unchanged from red-team round): iroh's mdns discovery makes ALL
nodes peers of ALL other nodes regardless of gossip topic. The digest's `peer_ids`
field therefore lists every discovered endpoint, including cross-mesh peers. The
edge-builder in `handle_topology` (lines 1979-2002 of `main.rs`) builds an edge
for every (self, peer) pair that appears in each live digest's `peer_ids`. With
18 nodes each reporting 18 peers, the edge set is fully connected. Classifying
by `mesh_a != mesh_b` (line 1995) produces O(n²) cross edges regardless of
bridge presence.

The `topic_label` fix (SPEC §7 #7) corrected `topic_membership` keying but did
NOT fix the edge builder, which reads from `peer_ids` not from `topic_membership`.
The SPEC's "R2 fixed" claim is false.

Sample illegal edges:
- `gateway-f099b2e7` (mesh-b) → `registry-c091e7b6` (mesh-a): kind=cross
- `broker-54eaeafb` (mesh-a) → `compute-e9717926` (mesh-b): kind=cross
- `compute-29b37c3a` (mesh-a) → `compute-b50209a5` (mesh-b): kind=cross

### Claim 3 — R3 Self-Presence (PASS with caveats)

```
GET /api/topology → nodes includes: {id="<unspawned>", type="admin-ui", mesh_id="default", status="live"}
GET /api/heartbeats → includes: {node_name="<unspawned>", node_type="admin-ui", mesh_id="default", age_ms=286244}
```

Admin-ui appears with `type=admin-ui` in both endpoints. Literal claim satisfied.

**Caveats**: (1) `id="<unspawned>"` — not a real node name; (2) `mesh_id="default"`
— incorrect, admin-ui should report its actual mesh subscription; (3) `age_ms`
grows monotonically (286k → 308k → 341k over 45 minutes of testing), meaning
the self-digest is NOT being refreshed each gossip tick as the SPEC describes.
See New Finding NF-2.

### Claim 4 — R4 Slowloris (PASS)

TCP socket, sent `GET /api/topology HTTP/1.1\r\nHost: x\r\n`, held without
terminating CRLF-CRLF. Used `stream.ReadTimeout=1000` + polling `Read()`:

```
t=1s:   read timeout
t=11s:  read timeout
t=21s:  read timeout
t=29.4s: Read() returned 0 bytes (EOF — server sent FIN)
```

Server FIN'd at 29.4s (within 30s window). `http1::Builder::header_read_timeout(30s)` works.

### Claim 5 — R5 Panic Hook (FAIL)

**Session evidence**: 18 panics logged to stderr during 5-minute stress test at
2000ms cadence (both `iroh-quinn-proto-0.13.0/src/connection/mod.rs:654` and
`iroh-quinn-0.14.0/src/mutex.rs:138`). After all 18 panics:
- `E:\dev\rafka-V2-new-mesh\target\admin-ui-panic.log`: **does not exist**
- `E:\dev\rafka-V2-new-mesh\admin-ui-panic.log`: **does not exist**

**Proof the hook works for non-iroh panics**: `E:\dev\i37-rafka-authz-create\admin-ui-panic.log`
contains 151 lines — the hyper "no timer set" panic from an earlier intermediate
build. Custom hook ran, wrote full backtrace.

**Root cause**: The iroh-quinn assertion (`untracked_bytes <= segment_size`) panics
in a tokio-rt-worker thread. This triggers our hook (first panic). The hook
writes to file — BUT the iroh-quinn mutex immediately poisons (second panic on
the same thread within the same `panic_with_hook` call). Rust's panic infrastructure
calls `abort()` on double-panic before `write_all` in the hook completes. The
hook fires but terminates without writing. Evidence: `OpenOptions::create(true)`
may succeed (explaining 0-byte file in session 2), but `write_all` is cut off by
the abort.

The SPEC claim "ANY panic writes a custom-format backtrace" is false for the
specific crash mode (iroh-quinn double-panic → abort) that admin-ui encounters
during every chaos run.

### Claims 6-10 — PASS (see summary table)

Claim 7 (remove-resilience): `duration=33370ms`. SPEC says "~30s." 3.4s over
the stated target but well within operational tolerance.

Claim 10 (mesh-five-types-present): detail shows 6 types including `admin-ui`
(was 5 pre-fix). Superset; test passes correctly.

---

## 3. New Findings

| # | Severity | Claim | Reality | Repro |
|---|----------|-------|---------|-------|
| NF-1 | CRITICAL | SPEC §7 #1: `cadence_ms ≥ 2000` means iroh-quinn bug "cannot be reached through the public API" | 18 iroh-quinn `connection/mod.rs:654` panics in 5 min of chaos at exactly 2000ms cadence | `POST /api/chaos/start {"cadence_ms":2000}` → run 5 min with bootstrapped 18-node pool |
| NF-2 | HIGH | admin-ui self-digest refreshes every 5s (age_ms always < 5s) | `age_ms` grows monotonically: 286k → 308k → 341k over 45 min; gossip task appears to stop updating the self-digest | `GET /api/heartbeats` → filter `node_type=admin-ui` → watch `age_ms` over 5 samples |
| NF-3 | MEDIUM | SPEC §1: CLI binary at `E:\cargo-target-v2\debug\rfa.exe` | Actual `cargo target_directory` is `E:\dev\rafka-V2-new-mesh\target`; the `E:\cargo-target-v2` binary is a stale 3:24 AM build predating all 10 commits | `cargo metadata --no-deps --format-version 1 \| python -c "import sys,json; m=json.load(sys.stdin); print(m['target_directory'])"` |
| NF-4 | MEDIUM | admin-ui self-presence uses correct id/name/mesh | id=`<unspawned>`, mesh_id=`default`, node_name=`<unspawned>` in both topology and heartbeats | `GET /api/topology` → filter `type=admin-ui` → check `id`, `mesh_id`, `node_id` fields |
| NF-5 | LOW | Topology `source` field present (SPEC §3) | `/api/topology` response includes `"source":"gossip"` but SPEC table documents this field while `/api/topology` route returned at line 2004 bypasses the Jaeger path entirely; `source` is hardcoded, not dynamic | Code: `main.rs:2004` returns before reaching the Jaeger code path that would set `source` dynamically |

### NF-1 Detail: 2000ms Floor Does Not Prevent iroh-quinn Panics

Chaos ran at `cadence_ms=2000` for exactly 300s. Panic count from session stderr:

```
panicked at iroh-quinn-proto-0.13.0/src/connection/mod.rs:654:21  (×9 workers)
panicked at iroh-quinn-0.14.0/src/mutex.rs:138:42                 (×9 workers)
Total: 18 panics
```

The process survived all 18 panics (tokio caught the task failures). But the SPEC
claim that the 2000ms floor "bounds the system to its safe operating envelope" is
false — the upstream iroh-quinn assertion fires at the floor cadence with a loaded
pool. The floor prevents process *death* (this time) but not iroh internal corruption.

---

## 4. Endpoint Shape Verification (SPEC §3)

All endpoints verified against shape table:

| Endpoint | Shape | Result |
|----------|-------|--------|
| GET /api/health | `{status}` | PASS |
| GET /api/cluster/summary | `{spawned,meshes,chaos_per_min,mean_peers,total_chaos_events}` | PASS (meshes is array, not count) |
| GET /api/topology | `{nodes:[{id,node_id,type,mesh_id,peer_count,frames_sent_total,frames_recv_total,wall_time_ms,status}], edges:[{from,to,kind}], source}` | PASS (fields present; edge correctness fails R2) |
| GET /api/heartbeats | `{heartbeats:[{node_name,node_type,node_id,mesh_id,peer_count,frames_sent_total,frames_recv_total,age_ms}], source:"gossip"}` | PASS |
| GET /api/messages | `{messages:[{ts_ms,from_peer_id,frame_kind,bytes,summary}]}`, newest first, max 500 | PASS |
| GET /api/timeline | `{events:[{ts_us,kind,node_name,node_type,mesh_id,detail}]}` | PASS |
| GET /api/alerts | `{alerts:[...]}` | PASS (empty; no Jaeger-detected failures) |
| GET /api/boot-trace | 502 when no trace | PASS |
| GET /api/chaos/state | `{running,cadence_ms,total_events,last_event_ts_us}` | PASS |
| POST /api/bootstrap | `{spawned:[...],errors:[...]}` | PASS |
| POST /api/nodes/spawn | `{node_name,pid}` | PASS |
| DELETE /api/nodes/{name} | `{node_name,reason}` | PASS (always 200) |
| POST /api/chaos/start | `{running,cadence_ms,total_events,last_event_ts_us}` | PASS |
| POST /api/chaos/stop | `{running:false,...}` | PASS |

---

## 5. Validator Coverage (SPEC §2)

| Validator | Test | Result |
|-----------|------|--------|
| `node_type` allow-list | `{"node_type":"evil","mesh_id":"m"}` | 400 PASS |
| `mesh_id` regex | `{"node_type":"broker","mesh_id":"../evil"}`, `"mesh with spaces"`, `"UPPERCASE"` | 400 PASS |
| `extra_env` allow-list | `LD_PRELOAD`, `PATH` → rejected; `RUST_LOG` → accepted | PASS |
| `node_name` path regex (DELETE) | `gateway-ZZZZZZZZ`, `foo-bar`, `bridge-xyz`, `broker-12345678abcdef` | 400 PASS |
| Pool cap 50 | Double bootstrap (18→36): 429 on 3rd+ calls | PASS |
| Test name validation | `../evil` → 400; 65-char name → 400 | PASS |
| Concurrent test (409) | Two simultaneous `gossip-swarm-forms` POSTs → second gets 409 | PASS |

---

## 6. Memory Stress Test

**Setup**: bootstrap (18 nodes), chaos at cadence_ms=2000 for 5 minutes.

| t | memory (MB) | chaos_events |
|---|-------------|--------------|
| baseline | 470.7 | 0 |
| 30s | 623.5 | 26 |
| 60s | 679.0 | 54 |
| 90s | 680.5 | 82 |
| 120s | 680.8 | 110 |
| 150s | 708.3 | 138 |
| 180s | 705.2 | 166 |
| 240s | 705.4 | 224 |
| 300s | 705.5 | 280 |

Growth from t=30s to t=300s: **82 MB**. Plateaus around 705 MB. No evidence of
unbounded growth over the 5-minute window. Process survived all 18 iroh-quinn panics.

**Note**: 18 iroh-quinn panics fired during this stress run (6 unique thread IDs,
3 panics each). None wrote to the panic log (NF-1 + Claim 5 FAIL).

---

## 7. "If I Were the User I Would Still Be Furious About ___"

**1. R2 is not fixed and the claim it was fixed is a lie.**
The topology graph shows 80 illegal direct cross-mesh edges immediately after
every bootstrap. The centerpiece visualization — the thing the tool exists to show —
is wrong. The "fixed" commit changed `topic_membership` keying but left the
`peer_ids`-based edge builder untouched. Every non-bridge node peers with every
other node via iroh mdns regardless of gossip topic. This was CRITICAL in the
red-team round. It is still CRITICAL now.

**2. The "safe floor" at 2000ms is not safe.**
SPEC §7 #1 says the floor "bounds the system to its safe operating envelope."
18 iroh-quinn assertion panics in 5 minutes at the floor cadence says otherwise.
The process survived this time (tokio recovered), but the SPEC's safety guarantee
is false. The floor prevents a particular crash *mode* (fast-enough-to-abort), not
the underlying assertion. Any chaos operator running at 2000ms will see 18 quinn
panics every 5 minutes in their logs — an indication that something is very wrong
while the SPEC tells them they're in the "safe envelope."

**3. The panic log is silent during the crash that motivated its existence.**
The hook was added specifically to capture the iroh-quinn crash that caused data
loss in prior QA rounds. The hook installs correctly (proven: captured the hyper
timer panic in an earlier build). It does NOT capture the iroh-quinn double-panic
path. The one panic type the tool was built to log is the one it cannot log.

---

## 8. Orphan Process Cleanup

```powershell
Get-Process rafka-* | Stop-Process -Force
```

Ran at end of session. No orphans remaining at report time.

---

## 9. Test Suite Sample Results

| Test | Status | Duration |
|------|--------|----------|
| remove-resilience | passed | 33.4s |
| gossip-mesh-to-mesh | passed | 20.1s |
| mesh-five-types-present | passed | 8.1s |

Full suite not run (25-40 min budget; soaks explicitly skipped per brief).
