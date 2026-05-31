# PRD — Chaos + resilience round (mesh-v2)

**Status:** Open (do NOT start until 00 + 01 are complete + verified)
**Branch:** off `mesh-v2` after 01 lands
**Builds on:** the topology cache + base UI (00) and the forced-relay path (01).

## 0. What this is

Golden Principle #5: "Chaos-pass replaces 'tests pass.'" Steady-state passing is insufficient — the
cache + cross-mesh write must hold up while the fleet is being hit. This round proves the mesh-v2 model
**survives churn**: nodes die and come back, links flap, the relay degrades — and the topology cache
stays correct, the write-sim recovers, the admin-ui stays live, and RSS stays flat over a 10-minute
soak. No new capability; this is the validation rung the whole substrate rests on.

This reuses the **existing chaos harness** (`crates/rafka-chaos`) and the **UI-path soak**
(`/api/bootstrap` + `/api/chaos/start`) — the operator's real path, not a CLI side-door.

## 1. Chaos primitives in play (all already exist — `crates/rafka-chaos`)

- **Process:** `kill_node` (DELETE /api/nodes/{name}), `restart_node` (kill + re-spawn), `burst_kill`
  (N rapid kills), `wedge_node` (SIGSTOP/suspend), `DiskFull`.
- **Network:** `partition_pair`, `partition_subset`, `flap_link`, `slow_link`, `lossy_link`.
- Driven via `ChaosContext` against the topology-ui at `:19090` (+ Jaeger for detection). Do NOT add new
  chaos primitives for this round — compose the existing ones.

## 2. What must hold under chaos (the resilience claims)

1. **Cache self-heals.** Kill a node (`kill_node` / `burst_kill`) → it is **evicted** from the topology
   cache via the staleness pruner (`run_staleness_pruner`, threshold `RAFKA_STALENESS_MS` default 30s,
   sweeps every 5s) → it disappears from `/api/topology-cache`, Topology, and Nodes — **no ghost, no
   `<unspawned>`**. `restart_node` → the revived node re-appears in the cache within a couple gossip
   cycles.
2. **Cross-mesh write recovers.** Kill `mesh2.broker1` → the write-sim from `mesh1.gateway1` fails
   **gracefully** (no panic, no unbounded retry storm, the failure is a span/log not a crash) and, once
   the target is gone from the cache, the gateway stops dialing a dead name. `restart_node` → once the
   replacement re-appears in the cache, delivery resumes. `partition_pair` / `flap_link` between gateway
   and broker → the write recovers when the link heals.
3. **Relay path tolerates degradation (01 on).** With `RAFKA_FORCE_RELAY=1`, apply `slow_link` /
   `lossy_link` / `flap_link` to the relay path → the cross-mesh write degrades but recovers; no leak,
   no wedged connection.
4. **RSS stays flat.** Over the soak, the admin-ui and fleet RSS must be **flat** (no monotonic growth)
   — this is the bar the substrate memory-leak fixes established (Windows + WSL/Linux, heap ↓81%). A
   growing RSS under churn is a regression and fails the round.

## 3. The soak (the operator's path)

Per the locked soak definition: `POST /api/bootstrap` (the two-mesh fleet) → `POST /api/chaos/start`
→ watch for **10 minutes** → `POST /api/chaos/stop`. Sample each ~30s: heartbeat count, alert count,
admin-ui RSS, fleet RSS, total chaos events. (This is the same shape as the M1 baseline soak that ran
clean — 40 events, RAM flat ~81 MB admin-ui / ~840 MB fleet, 0 alerts.)

**Build-freshness gate (mandatory):** before judging the soak, confirm every node binary's build mtime
is newer than the patched sources — presence-in-source ≠ compiled-in. A soak on a stale binary proves
nothing.

## 4. Admin-ui proof (it stays live the whole time)

The admin-ui must remain responsive and correct **throughout** the chaos, not just at the endpoints:
- **Topology / Nodes:** killed nodes drop out promptly; restarted nodes re-appear; counts track reality.
- **Cache:** `/api/topology-cache` view evicts dead names and re-adds revived ones — visibly self-heals.
- **Messages:** the write-sim traffic continues (with gaps during a target's death), resuming on recovery.
- **Alerts:** CPU/RAM alerts fire only on genuine threshold breaches, not chaos noise.

## 5. Verification — Playwright captures + the soak log

Harness at `docs/plans/mesh-v2/verify/` (`screenshot.js <phase>`):
- **`phase5-chaos-start`:** full fleet + chaos running (Topology, Nodes, Messages, Cache).
- **`phase5-mid-kill`:** immediately after a `kill_node` — the killed node **absent** from Cache + Nodes
  (proves self-heal, not a stale view).
- **`phase5-recovery`:** after `restart_node` — the revived node back in Cache + Topology; Messages
  resumed.
- Plus the **soak log** (elapsed / heartbeats / alerts / adminMB / fleetMB / events) showing 10 minutes
  with flat RSS and bounded alerts, saved under `verify/screenshots/phase5-chaos-*/` or a sibling
  `verify/soak/` text artifact.

## 6. Acceptance criteria
- A 10-minute UI-path soak (`/api/bootstrap` + `/api/chaos/start`) runs clean: **flat RSS**, bounded
  alerts, admin-ui responsive throughout, no panics in any node.
- Cache self-heals: a killed node is gone from `/api/topology-cache` + Nodes within ~`RAFKA_STALENESS_MS`
  + one sweep; a restarted node re-appears. Proven by the mid-kill + recovery screenshots.
- Cross-mesh write (direct AND `RAFKA_FORCE_RELAY=1`) recovers after `kill_node` + `restart_node` and
  after `flap_link`.
- Build-freshness gate passed (binaries newer than patched sources) before the soak was judged.
- No new chaos primitives, no new product surfaces, no app-layer concepts introduced.

## 7. Critical files
- `crates/rafka-chaos/src/{primitives.rs,soak.rs,lib.rs}` — reuse; compose existing primitives into the
  soak battery. Add a battery entry only if a needed composition is missing (no new primitive kinds).
- `crates/rafka-node-base/src/lib.rs` — confirm the staleness pruner evicts cache entries on node death;
  confirm the gateway's write-sim handles a vanished target gracefully (no retry storm).
- `admin-ui/src/main.rs` + `admin-ui/web/src/` — confirm Topology/Nodes/Cache/Messages stay correct
  under churn (no ghosts, no duplicate node_ids).

## 8. Out of scope
Real multi-host failure injection, OS-level network namespace chaos, NAT/firewall scenarios, partition
tolerance proofs beyond link flap, any app-layer/durability/replication concept. This round proves the
simple model survives the existing chaos battery, visible live, with flat memory.
