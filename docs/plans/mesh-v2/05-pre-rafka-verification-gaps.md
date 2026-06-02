# Pre-rafka verification gaps (mesh-v2 → app layer handoff)

**Date:** 2026-06-01
**Question:** before we build rafka (the topic/partition/produce-consume app layer) on top of the
mesh-v2 substrate, what must still be verified?

This is grounded in the closed sprint configs (11–21), the mesh-v2 PRDs (00–04), and a read of the live
chaos loop + data-plane code — not assumption.

---

## ⚠️ The caveat that reframes every green number

Every "RSS flat / CPU ~0.02 cores / cross-mesh held / 0 panics" result was measured against **brokers
that do nothing** — they receive a `Write`, emit a span, and ack in microseconds. No disk, no
processing. So those numbers are **floors for the empty substrate, not predictions for
rafka-with-work.** The moment a broker writes to a log, ack latency, in-flight stream backlog, and RSS
all change. The load-bearing measurements below must be **re-run on release builds once the broker does
real work.**

---

## A. Substrate-verification gaps (the actual "verify" list)

| # | Gap | Status | What to do |
|---|---|---|---|
| 1 | **Network-fault resilience** — PRD 02 §2.2/§2.3 claim the cross-mesh write recovers across `partition_pair`/`flap_link` and the relay tolerates `slow_link`/`lossy_link`. | **Unverified.** Confirmed: the authoritative sprint-18 UI-path soak (`/api/chaos/start` → `chaos_loop`) only kill+respawns processes. The network primitives exist in `crates/rafka-chaos` but were never fired in the closed proof. They also require Windows-firewall rules (admin rights) and target **binary names** (coarse on loopback where all brokers share `rafka-broker.exe`). | Run a soak that drives the network primitives (elevated, via `rfa`/`ChaosContext`); confirm the cross-mesh write degrades + recovers + RSS stays flat. |
| 2 | **Relay carries real frames end-to-end** | Proven only in an isolated `test_utils` test (sprint-16). On loopback it's correctly idle (direct wins). | **Conditional:** fine for a single-host first build. **Mandatory before any cross-host/NAT deploy** — prove a real `Write`/`Ack` traverses the relay live. |
| 3 | **Produce/ack under concurrency + load** | Single-shot `Write→Ack` only; never load-tested. | Load-test many concurrent producers. Note the **2s chaos-cadence floor**: sub-2s concurrent kill+respawn panics on an upstream `iroh-quinn-proto-0.13.0` assertion — a real churn ceiling to clear on the next iroh bump. |
| 4 | **Scale ceiling** | ~40 empty nodes, flat. | Measure gossip fanout cost, backbone summary size, and directory growth at 100s of nodes. |
| 5 | **Backbone partition / soft-lease failover** under a real cross-mesh network split | Only publisher death/expiry was reasoned through; a live split was never injected. | Inject a mesh-to-mesh partition; confirm lease failover + directory staleness behave. |

---

## B. What rafka *adds* — decisions, not substrate holes (don't conflate)

- **Durability / the log** — zero today. The broker acks but stores nothing. This is the core of what
  rafka *is*.
- **Delivery semantics** — offsets, ordering across reconnect, at-least-once/dedup, redelivery on
  failure. Today: one `Write→Ack`, no retry, no ordering guarantee.
- **Backpressure policy** — *positive*: the substrate already **emits** the load signal
  (`cpu_used`/budget, `Degraded` state). The data exists; only the producer-throttle /
  avoid-a-Degraded-broker routing **policy** is app-layer.
- **Multi-tenancy + control-op authz** — CLAUDE.md says "multi-tenant from day 1 via gossip org
  boundary," but today **any console can kill or set-state any node** with no authz and no org
  isolation. Decide the tenancy + who-can-control model before the app layer leans on it.

---

## C. What's solid — build on it with confidence

Membership/discovery (gossip directory), self-naming, cross-mesh visibility (backbone + per-node
metrics/aggregate), control-plane kill/set-state (by message, not ownership), `NodeState` lifecycle,
telemetry-as-substrate, **RSS-flat under process-kill churn**, the spawn-identity race fix, and the
cross-mesh neighborship fix.

---

## Recommended next step

Gap #1 — the network-fault recovery — is the highest-value item: it closes the one PRD claim marked
done-but-unproven and is the resilience bar the substrate rests on.

### Gap #1 — partial result (run 2026-06-01, two-console fleet on :19100 / :19101)

The firewall-based link partition/flap (`partition_pair`, `flap_link`, `slow_link`, `lossy_link`)
**could not run** — they require elevated `New-NetFirewallRule` and this session is not elevated, and on
loopback they target shared binary names (coarse). So the **process-fault half** of PRD 02 §2.2 was run
instead — kill the cross-mesh write targets, observe graceful degradation, restart, observe resume —
proven via Jaeger spans + the live consoles:

| Phase | Observation | Evidence |
|---|---|---|
| Baseline | cross-mesh write flowing `mesh1.gateway → mesh2.broker` | Jaeger: `mesh2.broker` 24× `produce.handle` / 120s; `mesh1.gateway` 48× `frame.sent` |
| Kill both `mesh2.broker` (control-op) | **graceful**: consoles stay 200, gateway **same pid**, RSS flat (44.2→44.1 MB), CPU ~0.09 cores (**no retry storm**), **0 panics** | proc count 8→6; `netfault-1-killed` |
| During outage | cross-mesh write **quiesces**, intra-mesh **unaffected** | Jaeger 35s window: `mesh2.broker` `produce.handle` **0**, `mesh1.broker` **7** |
| Eviction | both brokers gone from the **cross-mesh backbone view** within ~30s | console1 `/api/topology`; `netfault-1-killed` (mesh2 → 2 nodes) |
| Restart one `mesh2.broker` | rejoins backbone; **cross-mesh write resumes** | new `mesh2.broker.00b3af` in console1 backbone; Jaeger `produce.handle` **5** / 25s; `netfault-2-recovered` (mesh2 → 3 nodes) |

Screenshots: `docs/plans/mesh-v2/verify/screenshots/netfault-{0-before,1-killed,2-recovered}/`.

**Still open under #1:** the firewall-level *link cut while the process stays alive* (true
partition/flap/slow/lossy) — needs an **elevated** run. Process-fault recovery (target dies and returns)
is now proven; packet-level fault tolerance is not.
