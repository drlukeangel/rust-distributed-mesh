# chaos-harness — overview

> **Source:** Chaos engineering substrate per PRD `docs/plans/mesh-v1/04-chaos-harness-prd.md`. Implements the catalog of primitives + soak runner.

## What it is

`crates/rafka-chaos/` defines the `ChaosPrimitive` trait + concrete primitives + the soak loop. `rfa mesh chaos *` is the operator/CI surface; topology-ui's spawn/kill endpoints are the dirty-work backend.

Goal: every sprint after sprint-11 inherits the chaos-pass acceptance criterion — tests pass under random kill/restart/network-disturbance, not just steady-state.

## Phase 1 (shipped) — process primitives

- `kill_node` — DELETE on topology-ui; detection: target gone from `/api/nodes/spawned` within deadline
- `restart_node` — kill + immediate re-spawn same node_type; detection: new node_name appears in `/api/spawned`

## Phase 2 (queued) — network primitives

- `partition_pair` — Windows firewall rule blocking specific NodeId↔IP
- `partition_subset` — partition multiple node-pairs forming disjoint subsets
- `flap_link` — repeatedly disconnect+reconnect via firewall rule toggle
- `nat_shift` — force iroh endpoint rebind to new port (mid-run reconfigure)
- `slow_link` — Windows QoS / packet-delay injection per peer
- `lossy_link` — Windows QoS / packet-drop injection per peer
- `firewall_inbound` — block all inbound to a target

## Phase 3 (queued) — system primitives

- `clock_skew` — env var injection at next node restart (gateway/broker/compute/registry read `RAFKA_CLOCK_SKEW_MS` at boot)
- `wedge_node` — Suspend-Process equivalent (SIGSTOP)
- `disk_full` — fill spawn data dir to 100%

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
