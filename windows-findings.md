# Substrate performance findings — release vs debug, OS comparison, and a memory leak

Investigation date: 2026-05-27 → 2026-05-28. Platform: Windows 11 (10.0.26200),
80 logical cores, 128 GB RAM. Workload: 18-node 2-mesh bootstrap topology
(4×gateway + 4×broker + 4×compute + 4×registry + 2×bridge across mesh-a/mesh-b),
continuous chaos loop (kill-and-respawn, 30s cadence). iroh 0.98.2.

All measurements are **steady-state empty-shell** — there is no application-layer
payload yet; 100% of the CPU/RAM is substrate (iroh gossip + QUIC + telemetry).

> _This document plus `linux-findings.md` will be reworked into a public blog post.
> The narrative value is the debugging arc — confident wrong turns documented honestly,
> theory repeatedly losing to the measured curve (see Finding 4), and the payoff of an
> isolating minimal-revert experiment. Keep that arc when adapting._

---

## Finding 1 — Debug builds are 5–20× more expensive than release; the OS barely matters

Steady-state per-broker CPU, 18-node mesh, measured three ways:

| Build | OS | mean cores / broker | range |
|---|---|---|---|
| Debug | Windows | **0.12** | 0.07–0.16 |
| Release | Windows | **0.020** | 0.02–0.03 |
| Release | Linux (WSL2) | **0.017** | 0.013–0.020 |

**Release on Windows ≈ release on Linux.** The ~6× gap between Windows-debug and
Windows-release is almost entirely `iroh-quinn-proto` running unoptimized — the
admin-ui source already warns of this at `admin-ui/src/main.rs` (RAFKA_CHILD_BUILD_PROFILE
comment: "debug iroh-quinn-proto runs 5-20× slower than release").

**Takeaway:** never reason about capacity from debug numbers. An early hypothesis
that "Linux is 7× cheaper than Windows" was wrong — it was almost entirely the
debug-vs-release axis, not the OS.

### CPU scaling model (release)

Per substrate node: **~0.015 cores fixed + ~0.002 cores per peer-connection-per-topic.**

- Same-type nodes vary in CPU purely because peer counts vary.
- Gateways look "cheaper per peer" only because they aggregate more in-topic peers,
  amortizing the fixed cost — it's an averaging artifact, not an architectural
  efficiency. All 5 node binaries are identical thin wrappers around
  `rafka-node-base::run_node`; only `node_type` (a string) differs.
- Bridges cost ~2× a broker because they subscribe to **two** gossip topics
  (mesh-a + mesh-b), so they process roughly double the gossip volume.

---

## Finding 2 — Where the CPU actually goes (flamegraph)

Profiled a release-built broker for 60s with samply (ETW, Administrator). 77,940
stack samples. Category attribution:

| category | % of samples |
|---|---|
| System Libraries (ntdll, KernelBase, mio, parking_lot wrappers) | 99% |
| User (rafka + iroh + tokio + mio user-side) | 0.6% |
| Kernel | 0.3% |

**89% of all samples land in `NtWaitForAlertByThreadId`** — tokio workers parked in
`WaitOnAddress` via `parking_lot::Condvar`, waiting for wakeups. That is *wait time*,
not CPU burn (ETW context-switch attribution catches parked threads). The genuinely
on-CPU work is the remaining ~11%, spread diffusely across:

- `mio::sys::windows::iocp::Selector` → `GetQueuedCompletionStatusEx` → `NtRemoveIoCompletionEx`
  (Windows IOCP polling for UDP completions)
- tokio scheduler park/unpark machinery
- small atomic ops, `RtlQueryPerformanceCounter`, ntoskrnl leaves

**Takeaway:** the empty-shell broker's ~0.02 cores is the per-packet wake/sleep tax
of tokio + mio + IOCP reacting to gossip traffic. It is NOT concentrated in any one
hot function, and it is NOT in rafka's own code (0.6% user). There is no single hot
spot to optimize — only the aggregate gossip-packet rate (tunable via
`RAFKA_GOSSIP_INTERVAL_MS`).

### Earlier wrong turns (documented honestly)

- First guess "iroh-gossip is hot" was based on **counting log events** attached to
  spans — event count ≠ CPU. Retracted.
- Sample-distribution math (77,940 samples ≈ 0.87 cores) did not reconcile with the
  measured 0.11 cores — because the bulk of samples are *wait* attribution, not CPU.
  Use the category breakdown (User vs System vs Kernel), not raw sample counts.

---

## Finding 3 — Memory leak: monotonic RAM growth under sustained chaos (THE OPEN ISSUE)

During a ~3.5-hour Windows release soak with continuous chaos, **RAM grew
monotonically** — most severe on the two node types that subscribe to multiple
gossip topics:

| node type | RAM @ ~10 min | RAM @ ~3.5 hr | growth |
|---|---|---|---|
| **bridge** (2 topics) | 0.052 GB | **0.604 GB** | **~12×** |
| **admin-ui** (all topics) | 0.076 GB | **0.426 GB** | ~5.6× |
| broker | 0.047 GB | 0.071 GB | 1.5× |
| compute | 0.046 GB | 0.061 GB | 1.3× |
| gateway | 0.049 GB | 0.057 GB | 1.2× |
| registry | 0.046 GB | 0.036 GB | ~flat (chaos-respawned, fresh) |

Bridge RAM climb was smooth and relentless — ~180 MB/hr, no plateau:

```
23:03  0.501 GB
23:09  0.519
23:18  0.550
23:30  0.591
23:35  0.604
```

CPU crept up alongside RAM (bridge spikes to 0.11–0.13 cores), consistent with
"more cached state → more work per gossip tick."

### Ruled out: our own bookkeeping maps

`rafka-node-base` keeps three process-global maps — `live_digests`, `last_seen_ms`,
`topic_membership`. `run_staleness_pruner` (lib.rs:690) sweeps every 5s and removes
node_ids older than `RAFKA_STALENESS_MS` (default 30s) from all three. Verified the
pruner logic is correct and prunes all three maps by the same stale set. **These maps
are bounded.** The leak is below our code.

### Remaining suspects (in priority order)

1. **iroh-quinn / iroh-gossip retaining per-peer state for churned peers.** Chaos
   creates a fresh `node_id` every 30s; if iroh does not fully release connection /
   membership state for the killed peer, it accumulates. Bridges churn through 2×
   the peers (2 topics) → ~2× the leak rate. This fits the data precisely.
2. tokio task handles from churned connections not being dropped.
3. (verify) `message_ring` bounded-ness.

This lands on Golden Principle #1 (iroh owns the mesh). If the leak is iroh-side
state retention, the fix is an iroh config knob (connection eviction / idle timeout)
or an upstream report — **not** a hand-rolled cleanup in rafka.

### A/B result — the leak is CROSS-PLATFORM (not Windows-specific)

Re-ran the identical release soak under Linux (WSL2). The leak reproduces at
essentially the same rate:

| node type | Windows @ ~2 hr | Linux @ ~2.8 hr (338 chaos events) |
|---|---|---|
| bridge | 0.288 GB | **0.291 GB** |
| admin-ui | 0.426 GB | **0.401 GB** |
| broker | ~0.07 GB | 0.061 GB |

Linux bridge trajectory: 0.043 (T0) → 0.074 (22 min) → 0.291 GB (~2.8 hr) — same
monotonic climb, same magnitude as Windows. Both platforms leak to ~0.29 GB on
bridges over a couple hours.

**Verdict:** the leak is **platform-independent iroh logic**, not Windows IOCP
socket-state retention. The fix is iroh connection lifecycle under peer churn
(suspect #1), independent of OS. WSL2 uses Linux epoll, not Windows IOCP, yet leaks
identically — conclusive.

---

## Finding 4 — Two fix attempts, and what the experiments actually proved

This is the most instructive part of the investigation: a plausible root-cause theory,
a fix built on it, and an empirical test that demolished both. It's a clean case study
in *theory loses to the curve*.

### Attempt 1 — "remove keep-alive, add app-level pings" (REGRESSED)

The first hypothesis: rafka-mesh-transport configured iroh-quinn with
`keep_alive_interval(15s)`. QUIC keep-alive sends ack-eliciting PING frames that reset
the local idle timer. When a peer is abruptly killed (chaos), it never sends a clean
CONNECTION_CLOSE; meanwhile the local node keeps firing keep-alives, resetting its own
idle timer, so `max_idle_timeout(30s)` never fires. The connection lingers forever,
`accept_uni().await` in the frame-reader task blocks indefinitely, and the per-peer
tokio tasks + iroh-gossip state are never released. Plausible, and it matches the
"connection never closes → state orphaned" shape.

The fix: remove `keep_alive_interval`, and instead spawn the existing `run_ping_sender`
task (application-layer Ping frame every 10s) — the idea being that app pings only keep
a connection alive if the peer is actually responding.

**Result: the leak got ~3× WORSE.** Normalized per chaos event (30s cadence, fresh T0):

| build | bridge RAM @ ~106 events | leak rate |
|---|---|---|
| original (keep-alive 15s) | ~0.12 GB | ~0.85 MB/event |
| attempt-1 (app-pings 10s) | **0.28 GB** | **~2.6 MB/event** |

Worse, the leak **spread to every node type** — brokers went from ~0.07 GB to 0.246 GB,
because `run_ping_sender` was previously disabled and the fix enabled it on *all 18
nodes*. `run_ping_sender` (lib.rs:1576) blindly iterates the entire `PeerRegistry` and
`open_uni()`-pings every entry — including dead peers — every 10s, never removing them.
That is structurally the *same* keep-warm behavior as the keep-alive it replaced (10s
ping < 30s timeout), plus new per-ping allocation (orphaned uni-streams + span contexts)
to dead peers. Net regression.

### Attempt 2 — "minimal revert": remove ALL rafka keepalive (DECISIVE)

To isolate the variable, we ran the cleanest possible experiment: keep the keep-alive
removal, but turn `run_ping_sender` back OFF, leave `max_idle_timeout` at 30s. Net: zero
rafka-layer keepalive of any kind.

**Result: identical leak rate to the original keep-alive build.**

| build | keepalive mechanism | bridge RAM @ ~57 min (~114 events) |
|---|---|---|
| original | quinn keep-alive 15s | ~0.125 GB (interpolated) |
| attempt-1 | app-pings 10s | 0.28 GB |
| **minimal-revert** | **none** | **0.126 GB** |

Minimal-revert at 68 min / 136 events: bridge 0.14 GB, admin-ui 0.27 GB — still climbing,
tracking the original curve.

**This refutes the keep-alive theory.** If keep-alive (or any keepalive) were the cause,
removing it entirely would have fixed the leak. It made zero difference. And crucially,
**peer counts stayed healthy** throughout the minimal-revert run (gateways ~4.2, computes
~3.2) — the mesh was *not* flapping, connections *were* being established and used. So
the leak is not a connection-lifecycle problem at all.

### Corrected root cause

The leak is **iroh state keyed by `node_id`, accumulating for every peer ever observed,
independent of connection keepalive or even connection close.** Each chaos cycle mints a
brand-new node_id; something in iroh retains it and never evicts. Narrowed suspects:

1. **iroh-gossip HyParView passive view** — accumulating dead node_ids it learned via
   SHUFFLE but never expires.
2. **iroh endpoint `remote_map` / `RemoteState`** — per-node_id state retained after the
   connection is gone.

Bridges and admin-ui leak fastest because multi-topic subscription = more unique
node_ids observed per unit time. The leak rate tracks *node_ids seen*, not *connections
held*.

### Architectural note (separate from the leak): `run_ping_sender` violates Golden Principle #1

Independent of the leak, the experiment surfaced that `run_ping_sender` is rafka
**hand-rolling a liveness/keepalive heartbeat** — exactly what CLAUDE.md Golden Principle
#1 forbids ("no hand-rolled gossip protocol", iroh owns mesh liveness). iroh-gossip's
HyParView already does neighbor failure detection; iroh-quinn already does transport
keepalive. rafka should not own a redundant ping/pong.

The task `run_ping_sender` was *also* conflated with two legitimate, non-liveness jobs:
emitting `frame.sent` spans to weight the topology-UI edges, and serving as the injection
point for the `lossy_link` / `slow_link` chaos primitives. Those belong elsewhere:
UI edge-weighting should derive from real `rafka.mesh.gossip.received` traffic spans we
already emit; chaos fault-injection should hook the actual gossip/frame send path. The
keepalive role should be deleted outright.

**Bottom line:** removing rafka's ping/pong is correct on principle, but it does NOT fix
the leak — the leak is iroh's, to be fixed via an eviction config knob or upstream patch.

---

## Tooling notes (for reproduction)

- **Profiler:** `samply record -p <pid> --reuse-threads` (needs Administrator for ETW
  on Windows). `--all` mode works without pid-attach but the 15k-track profile can
  choke the local viewer; upload to profiler.firefox.com or use `--reuse-threads` +
  single-pid for a compact file. Symbol resolution needs the matching PDB.
- **Linux build:** rustc 1.95.0 ICEs in `check_mod_deathness` on `admin-ui/src/main.rs`;
  worked around with a crate-level `#![allow(dead_code)]` (committed). Cross-platform
  binary spawn now uses `std::env::consts::EXE_SUFFIX`.
- **Per-node CPU/RAM** are exposed live (no Jaeger) at `/api/topology` (`cpu_used`,
  `ram_used` fields from each node's `GossipDigest`).
- **Alerting:** `/api/alerts` emits warn-severity entries for any node exceeding
  `RAFKA_CPU_ALERT_THRESHOLD` (default 0.10 cores) or `RAFKA_RAM_ALERT_THRESHOLD_GB`
  (default 0.5 GB). admin-ui itself is excluded (does extra HTTP/orchestration work).

## Recommendations

1. **Measure release, always.** Debug CPU/RAM numbers are not representative.
2. **Triage the memory leak as the top substrate issue** — it caps multi-hour
   viability far more than the per-node CPU floor does. Start with iroh connection
   lifecycle under peer churn.
3. Consider raising `RAFKA_GOSSIP_INTERVAL_MS` (currently 2000) to cut the IOCP
   completion rate — the cheapest lever on the CPU floor, though it won't touch the
   leak.
