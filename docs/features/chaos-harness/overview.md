# chaos-harness — overview

> **Source:** Chaos engineering substrate per PRD `docs/plans/mesh-v1/04-chaos-harness-prd.md`. Implements the catalog of primitives + soak runner.

## What it is

`crates/rafka-chaos/` defines the `ChaosPrimitive` trait + concrete primitives + the soak loop. `rfa mesh chaos *` is the operator/CI surface; topology-ui's spawn/kill endpoints are the dirty-work backend.

Goal: every sprint after sprint-11 inherits the chaos-pass acceptance criterion — tests pass under random kill/restart/network-disturbance, not just steady-state.

## Phase 1 (shipped) — process primitives

- `kill_node` — DELETE on topology-ui; detection: target gone from `/api/nodes/spawned` within deadline
- `restart_node` — kill + immediate re-spawn same node_type; detection: new node_name appears in `/api/spawned`
- `burst_kill` — N back-to-back kills against random spawned subprocesses; substrate-race exercise
- `disk_full` — fill spawn data dir until writes fail (cap on filler size for safety)
- `wedge_node` — Windows `NtSuspendProcess` via PowerShell; revert with `NtResumeProcess`

## Phase 2 (shipped) — network + clock primitives

- `partition_pair{a, b, duration_ms}` — Windows `New-NetFirewallRule` blocking outbound UDP for two named programs. **Requires elevated shell** (admin); fails clean with surfaced stderr otherwise. Revert removes the tagged rules.
- `clock_skew{target, skew_ms}` — restart the target node with `RAFKA_CLOCK_SKEW_MS` env injected via topology-ui's `extra_env` field. `rafka-node-base` reads the var at boot and emits `clock_skew_ms` + `wall_time_ms` attributes on every `rafka.mesh.heartbeat` span. Detection verifies new subprocess appears; substrate detection (Jaeger query for skewed `wall_time_ms`) is a follow-up.
- `slow_link{target, latency_ms}` — kill + respawn target with `RAFKA_LINK_SLOW_MS` env. `rafka-node-base.run_ping_sender` reads at boot and sleeps that many ms before each outbound `open_uni` so the link appears slow at the app layer.
- `lossy_link{target, loss_pct}` — kill + respawn with `RAFKA_LINK_LOSS_PCT` env (0-100). Per outbound ping, node-base rolls a u8%100; if < loss_pct, emits a `rafka.mesh.frame.dropped_by_fault_inject` span and skips the write. Telemetry-visible drop signal.

## Phase 3 (queued) — extended network primitives

- `partition_subset` — partition multiple node-pairs forming disjoint subsets
- `flap_link` — repeatedly disconnect+reconnect via firewall rule toggle
- `nat_shift` — force iroh endpoint rebind to new port (mid-run reconfigure)
- `firewall_inbound` — block all inbound to a target

## Locked spans

- `rafka.chaos.primitive.executed{name, target, otel.kind="internal"}` — emit per execute()
- `rafka.chaos.primitive.detected{name, target, result, waited_ms, otel.kind="internal"}` — emit per detect()
- `rafka.chaos.primitive.reverted{name, otel.kind="internal"}` — emit per revert() (no-op for kill, real for network primitives)

## Soak runner

`crates/rafka-chaos/src/soak.rs::run_soak`:
- Loops `while elapsed < duration`
- Picks random primitive from registered pool (via seeded `ChaCha20Rng`)
- Execute → detect (with 30s deadline) → record SoakEvent
- Sleeps `interval` between events
- Outputs `SoakReport` JSON: per-event timestamp + primitive + targets + detection result + waited_ms; aggregate pass/fail counts

Exit code 0 only if all events passed. Non-zero on any timeout or assertion failure. CI gates on this.

## CLI surface

```bash
rfa mesh chaos kill [--target X] [--deadline-ms 30000]
rfa mesh chaos restart [--target X] [--deadline-ms 30000]
rfa mesh chaos soak --duration <d> [--interval <d>] [--seed <n>]
```

Smoke mode: `--duration 5m --interval 30s` → ~10 events; for PR checks.
Soak mode: `--duration 24h --interval 30s` → ~2880 events; for nightly CI.

## Invariants

1. **Reproducibility via seed.** Same seed = same primitive sequence + targets (modulo registry state at run time).
2. **Failures are recorded, not raised mid-loop.** Soak continues past individual primitive failures; final report aggregates.
3. **Detection is bounded.** Every primitive has a `deadline_ms`; timeouts count as `FailedTimeout`, not infinite hang.
4. **Targets default to random spawned subprocess.** Operators can override with `--target` for repro.

## Cross-references

* Sibling: [`subprocess-control`](../subprocess-control/overview.md) (provides spawn/kill backend), [`rfa-cli`](../rfa-cli/overview.md) (CLI surface).
* Code: `crates/rafka-chaos/src/{lib,primitives,soak}.rs`, `cli/rfa/src/main.rs::cmd_chaos_*`.
* PRD: `docs/plans/mesh-v1/04-chaos-harness-prd.md` (the 12-primitive catalog + 24h soak gate).
