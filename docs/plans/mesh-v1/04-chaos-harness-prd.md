# PRD — Chaos Test Harness

**Status:** Open
**Companion to:** `00-mesh-rebuild-prd.md`
**Ships in:** Sprint 1 (Sprint 0 substrate must be alive first)

---

## 1. The mandate

Substrate without chaos testing is unverified substrate. Every Sprint after Sprint 1 inherits the chaos-pass acceptance criterion: "tests pass under chaos" replaces "tests pass" as the bar.

This is the SECOND load-bearing investment of the rebuild (first being iroh-substrate selection). Without it, we have no way to PROVE the substrate is rock-hard — we just have a substrate that worked when we last looked.

## 2. The chaos primitives catalog

Each primitive is an operation that disturbs the mesh in a specific, reproducible way. Sprint 1 must ship every primitive in this table.

| Primitive | What it does | Detection criterion (substrate passes if...) |
|---|---|---|
| `kill_node` | Terminates a node's process abruptly (SIGKILL, no graceful shutdown) | Survivors detect within 4×gossip_interval; topology converges within 10s after kill |
| `restart_node` | Kill + relaunch with SAME `EndpointId` | Reconnects within 5s; no duplicate node entries in survivors' membership |
| `partition_pair` | Blocks all traffic between two specific nodes | Both nodes still see ALL OTHER peers; the blocked pair appears as stale in each other's view |
| `partition_subset` | Splits the mesh into two disjoint subsets that can each communicate internally but not with the other side | Each subset converges to its own view; on heal, full mesh reconverges within 30s |
| `flap_link` | Repeatedly disconnect+reconnect an edge every N seconds | No "ghost peer" accumulation; final state matches expected after flapping stops |
| `nat_shift` | Force a node to rebind its endpoint to a new port | Peers re-discover via iroh-relay or DNS; old connection is replaced, not duplicated |
| `clock_skew` | Inject ±60s clock offset on a node | Gossip timeouts still work; no false-positive staleness eviction |
| `slow_link` | Add 500ms latency to all traffic from a node | Gossip still completes; no timeouts; throughput degrades gracefully |
| `lossy_link` | Drop 10% of packets from a node | Same as above; iroh's QUIC handles loss |
| `wedge_node` | Suspend a node's process (SIGSTOP) — process exists but doesn't respond | Survivors detect via gossip timeout; treat as failure equivalent to kill |
| `firewall_inbound` | Block inbound connections to a node (Windows-firewall-pattern reproduction) | Peers reach via relay if available; node remains visible in topology even if direct-peer-only nodes can't reach it |
| `disk_full` | Fill node's data dir to 100% (mid-boot or steady-state) | Boot fails cleanly with clear error; steady-state node continues without writing new state until disk has space |

## 3. The soak run

**Sprint 1 acceptance test:** a single 24-hour run that:

1. Spawns 7 nodes (2 gateways, 3 brokers, 2 compute)
2. Every 30 seconds, picks a random primitive from the table + random target(s)
3. Executes the primitive
4. Waits for the primitive's "detection criterion" — fails the soak if not met within 30s
5. After 24h:
   - Final node count = expected node count
   - Final topology graph (modulo any chaos primitives still active) = a fully-connected mesh
   - Zero permanent splits
   - Zero unrecovered membership drift
   - Zero process panics in any node's logs
   - All OTLP spans accounted for (no gaps in trace_id sequences from continuously-active nodes)

The soak run is `rfa mesh chaos soak --duration 24h`. It can be invoked from CI nightly. Outputs structured JSON pass/fail with per-primitive stats.

## 4. Harness shape

```rust
// crates/rafka-chaos/src/lib.rs

pub trait ChaosPrimitive: Send + Sync {
    fn name(&self) -> &str;
    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError>;
    async fn detect(&self, ctx: &ChaosContext, outcome: &ChaosOutcome) -> Result<DetectionResult, ChaosError>;
    async fn revert(&self, ctx: &ChaosContext, outcome: &ChaosOutcome) -> Result<(), ChaosError>;
}

pub struct ChaosContext {
    pub mesh_client: IrohMeshClient,        // joins as a chaos-controller view-node
    pub node_registry: NodeRegistry,        // managed via topology-ui's subprocess control
    pub random: ChaCha20Rng,                 // seeded for reproducibility
}

pub enum DetectionResult {
    Passed,
    FailedTimeout { waited_ms: u64 },
    FailedAssertion { msg: String },
}
```

Each primitive in the table is a struct implementing `ChaosPrimitive`. The soak loop picks one + targets + execute → detect → (if not auto-reverting) revert after a TTL.

## 5. Integration with topology UI

The UI shows chaos events in the span timeline:
- Color-coded chaos primitive name
- Click to see target(s) + detection result
- "Active chaos" panel listing currently-running primitives + their TTL

Chaos primitives are first-class spans:
- `rafka.chaos.primitive.executed{name, targets, seed}`
- `rafka.chaos.primitive.detected{name, result, waited_ms}`
- `rafka.chaos.primitive.reverted{name}`

## 6. CI integration

Two run modes:

**Smoke (per-PR):**
- 5-minute run
- 10 chaos events
- All standard primitives, no extreme combinations
- Acceptance: same as soak but in 5 min

**Nightly (scheduled):**
- 24-hour soak as defined in §3
- Failure → page on-call (when on-call exists)
- Pass → publish chaos-stability badge

Both modes run via `rfa mesh chaos soak --duration <X> --seed <Y>` so they're identical local-vs-CI.

## 7. Reproducibility

Every soak run records:
- The seed (`--seed`)
- The full sequence of (timestamp, primitive, targets) it executed
- The final topology
- Per-primitive detection latencies

This goes to `tests/artifacts/chaos-soak/<run-id>/manifest.json`. A failing soak can be replayed exactly via `rfa mesh chaos soak --replay <run-id>`.

## 8. Acceptance criteria (Sprint 1)

1. All 12 primitives in §2 implemented and individually testable: `rfa mesh chaos <primitive> <targets>` exits 0 on success, non-zero on detection failure
2. Soak run for 1 hour (smoke version of the 24h test) passes without intervention
3. UI shows live chaos events in the span timeline
4. CI integration: smoke chaos run added to PR check
5. 24-hour soak run passes (this is the Sprint 1 close gate)
6. All chaos primitives have OTLP spans landing in `tests/artifacts/chaos-soak/`

## 9. What this catches that current testing doesn't

Current rafka tests:
- Run on a single Windows machine
- No process kills mid-test
- No network failures
- No clock skew
- No disk pressure
- Pass/fail based on "did the assertion match"

Chaos harness:
- Forces failure paths to execute on every run
- Verifies recovery, not just steady-state
- Reveals "works when nothing goes wrong" code that hides catastrophic bugs
- Surfaces resource leaks (file handles, ephemeral ports, gossip state) over 24h that 60-second tests never see

The substrate-debug tax we've been paying per sprint is largely tax for bugs the chaos harness would have caught in week 1. Investment now → return forever.

## 10. Non-goals (Sprint 1)

- **No multi-mesh chaos** in Sprint 1 (that's Sprint 2's deliverable with the multi-mesh substrate). Sprint 1 covers single-mesh chaos only.
- **No customer-data chaos** (no topics, no records yet). Substrate-layer chaos only.
- **No Jepsen-style linearizability testing.** That's a separate initiative if needed; Sprint 1's job is "substrate doesn't shit the bed under adversarial conditions."
