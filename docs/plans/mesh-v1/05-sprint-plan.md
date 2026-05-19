# Sprint Plan — Mesh Substrate Rebuild

**Status:** Open
**Companion to:** all PRDs in this folder (`00`–`04`)
**Total duration:** ~6 weeks for the substrate rebuild before feature initiatives resume

---

## Sprint summary

| # | Name | Duration | Acceptance gate |
|---|---|---|---|
| 0 | Substrate + UI + CLI | 2 weeks | 3 nodes spawn via UI + CLI; topology updates in real-time; OTLP spans land |
| 1 | Chaos harness + soak | 2 weeks | 12 primitives shipped; 24h soak passes |
| 2 | Multi-mesh + relay | 2 weeks | 2 meshes peered via iroh-relay; cross-mesh chaos passes |
| 3+ | Feature initiatives resume | TBD | Each gated by chaos-pass on substrate |

---

## Sprint 0 — Substrate spike + day-1 control surfaces

**Branch:** `sprint-mesh-0-substrate`
**Duration:** 2 weeks
**Goal:** Boot 4 node types on iroh. Topology UI + `rfa` CLI exist from the start.

### Deliverables

1. **iroh-based `MeshTransport` impl** (`crates/rafka-mesh-transport/src/iroh.rs`)
   - Implements existing trait from `rafka-mesh-ops`
   - Preserves `InternalMeshFrame` shape unchanged
   - Old custom-QUIC impl deleted (no fallback maintained)

2. **4 node binaries minimally functional**
   - `cargo run -p rafka-gateway` boots, mints identity, joins mesh
   - Same for `rafka-broker`, `rafka-compute`, `rafka-registry`
   - All emit the 14 substrate spans defined in `01-substrate-prd.md §9`

3. **`rafka-topology-ui` binary**
   - Boots on `http://localhost:19090`
   - Joins mesh as view-only node (ALPN `rafka-topology-v1`)
   - Serves single-page HTML with live topology graph
   - Subprocess spawn/kill for node lifecycle ops

4. **`rfa` CLI** (`crates/rfa/`)
   - Commands: `mesh node add/remove/list/describe/logs/spans`, `mesh topology show/watch`, `mesh status`, `mesh wait-converged`
   - Talks to topology-ui's REST API
   - Every command supports `--format json`

5. **OTLP collector wiring**
   - All substrate spans land in `tests/artifacts/mesh-substrate/*.spans.jsonl`
   - Topology UI subscribes to span stream + renders in timeline

### Workspace structure changes

```
crates/
├── rafka-mesh-transport/      # iroh impl of MeshTransport trait
├── rafka-mesh-ops/            # unchanged — codec crate
├── rafka-chaos/               # added Sprint 1
├── rfa/                       # CLI binary
gateway/                       # bare gateway (no Kafka wire, no REST, no authz)
broker/                        # bare broker (no SingleWal yet, no coordinators)
compute/                       # bare compute (no job_tailer, no RSQL, no WASM)
schema/                        # NEW — bare schema registry
topology-ui/                   # NEW — UI binary
```

Everything app-layer is GONE from these binaries. Each binary is a mesh participant + nothing else.

### Acceptance gate (must all pass)

1. `cargo run -p rafka-topology-ui` starts UI on `http://localhost:19090`
2. Click "Spawn gateway" in UI: blue node appears within 5s
3. Click "Spawn broker": green node appears + edge to gateway within 5s
4. `rfa mesh node add --type compute --name cp-1`: orange node appears in UI
5. `rfa mesh node remove cp-1`: orange node disappears within 10s
6. Kill node via UI: span timeline shows `rafka.mesh.peer.staleness_timeout`
7. Restart same node: reconnects within 5s, same `EndpointId`
8. `cargo check --workspace --tests --no-default-features` = 0 errors, 0 new warnings
9. `tests/artifacts/mesh-substrate/` has non-empty JSONLs for every span name

### What's explicitly DEFERRED

- All chaos primitives (Sprint 1)
- Multi-mesh / relay (Sprint 2)
- Kafka wire protocol (post-substrate)
- Authz / authn (post-substrate)
- Topics / records / WAL writes (post-substrate)
- All entity types beyond "mesh node"

---

## Sprint 1 — Chaos harness + 24h soak

**Branch:** `sprint-mesh-1-chaos`
**Duration:** 2 weeks
**Goal:** Substrate is provably hard.

### Deliverables

1. **`rafka-chaos` crate** (`crates/rafka-chaos/`)
   - `ChaosPrimitive` trait + 12 primitives from `04-chaos-harness-prd.md §2`
   - Each primitive is independently testable: `rfa mesh chaos <primitive> <targets>`

2. **Soak runner**
   - `rfa mesh chaos soak --duration <X> --seed <Y>`
   - Records every (timestamp, primitive, targets) to a replayable manifest
   - Outputs pass/fail with per-primitive stats

3. **UI chaos panel** (Sprint 0 UI extended)
   - "Inject chaos" button per primitive
   - "Active chaos" panel showing currently-running primitives
   - Span timeline color-codes chaos events

4. **CI integration**
   - Per-PR: 5-minute smoke chaos run
   - Nightly: 1-hour soak run on PR-branch, 24h on main

### Acceptance gate

1. All 12 primitives ship + each pass when run individually
2. 1-hour smoke soak passes (intermediate validation)
3. **24-hour soak run passes** — zero permanent splits, zero membership drift, zero process panics
4. CI: smoke chaos added to PR check; nightly soak added to scheduled jobs
5. UI shows chaos events in real-time
6. All chaos primitives have OTLP spans
7. Replay verified: a failing soak's manifest replays to same failure point

### Migration step

After 24h soak passes, the OLD custom QUIC mesh implementation is fully deleted from the codebase. No `#[cfg(feature = "legacy-mesh")]` paths. `git rm` the old code.

---

## Sprint 2 — Multi-mesh via iroh-relay

**Branch:** `sprint-mesh-2-multi-mesh`
**Duration:** 2 weeks
**Goal:** Cross-mesh peering is a first-class substrate capability.

### Deliverables

1. **iroh-relay deployment**
   - Helm chart for `rafka-relay` (lightweight; just `iroh-relay` server)
   - Local dev: relay runs as a subprocess of topology-ui

2. **Multi-mesh topology**
   - `rfa mesh add --name <mesh-id>` creates a logical mesh
   - `rfa mesh peer --from <a> --to <b> --via <relay-url>` establishes cross-mesh routing
   - Each mesh = isolated gossip overlay; relay bridges traffic

3. **UI multi-mesh view**
   - Graph splits into per-mesh panels
   - Relays drawn as distinct node-type tier
   - Cross-mesh edges visually distinct

4. **Cross-mesh chaos**
   - Sprint 1's 12 primitives extended with multi-mesh variants:
     - `partition_meshes` — block all relay traffic between two meshes
     - `kill_relay` — terminate a relay server
     - `wan_latency_inject` — add 200ms latency on cross-mesh traffic
   - 24h soak with multi-mesh chaos enabled

### Acceptance gate

1. 2 meshes (mesh-a, mesh-b) connected via iroh-relay
2. Node in mesh-a sees node in mesh-b via topology UI (cross-mesh edge drawn)
3. `partition_meshes` chaos: both meshes operate standalone; cross-mesh edges marked "stale"; on heal, edges return within 30s
4. `kill_relay` chaos: cross-mesh traffic stops; intra-mesh traffic UNAFFECTED; topology UI shows relay as dead
5. 24h multi-mesh soak passes
6. All cross-mesh spans land in `tests/artifacts/mesh-multi/`

---

## Sprint 3+ — Feature initiatives resume

Each subsequent feature sprint (topics, jobs, RSQL, authz, etc.) inherits:

- **Chaos-pass acceptance criterion:** the feature's test suite runs under the smoke chaos battery; must pass
- **OTLP artifact mandate:** every new span emit site must have evidence in `tests/artifacts/<feature>/`
- **No new mesh primitives:** if a feature needs cross-node coordination, it uses `InternalMeshFrame` over the substrate, never invents new transport

The feature sprint sequence is RE-DERIVED at end of Sprint 2 from the locked spec docs in `_migrationv2/architecture/`. Initial candidate order (subject to revision):

| Order | Initiative | What it adds |
|---|---|---|
| 1 | Topics + SingleWal + entity catalog backbone | Persistent storage, slug↔id registry |
| 2 | Identity + role-bindings + compile pipeline | Authz from spec |
| 3 | Kafka wire protocol | Customer-facing surface |
| 4 | Consumer groups + share groups | Coordination layer |
| 5 | Jobs + compute job_tailer | Background work |
| 6 | RSQL + DQ + alerts + WASM | Compute features |
| 7 | Multi-region semantics on top of multi-mesh | Mirroring, leaf-node WAL, etc. |
| 8 | Schema registry | The 4th node type gets its real surface |
| 9 | Cascade + entity-map + audit | Operational completeness |
| 10+ | Connectors, RSQL surfaces, alert definitions, inference endpoints, WASM hooks, DQ rules | Per `02-entity-catalog §7` |

Each is its own sprint or multi-sprint initiative. Each ships with chaos as a hard gate.

---

## Cross-sprint discipline

All sprints under this initiative:

- **Sonnet for every subagent dispatch** (CLAUDE.md).
- **No defer language** in plans or briefs.
- **OTLP artifact evidence** before sprint closure.
- **stash → pull --rebase main → commit → push** between fix batches.
- **No `cargo clean`** as a debugging shortcut.
- **No Claude attribution** in commit messages.
- **Workspace gate** (`cargo check --workspace --tests --no-default-features`) zero/zero before every commit.
- **Trust-but-verify (80/20 rule)** for all agent reports.
- **One task per agent** dispatched at a time.

---

## What this plan DELIBERATELY doesn't do

- **No "ship Kafka wire alongside substrate rebuild."** Wire-protocol work resumes AFTER Sprint 2.
- **No "fallback to custom QUIC if iroh fails."** If iroh fails Sprint 0, libp2p spike replaces it. Custom QUIC is permanently retired.
- **No "preserve current ThreeProcessHarness."** Replaced by chaos-first harness in Sprint 1.
- **No "audit existing migration claims."** Wasted effort under the rebuild — rebuild against the spec docs, not against the half-truth code.
- **No customer commitments during the 6-week window.** There are no customers; no opportunity cost.

---

## Pre-Sprint-0 cleanup

Before Sprint 0 starts, two cleanup PRs land on main:

1. **Land i34-phase5 as-is** — compute REST removal code shape is correct, substrate-verify is moot under the rebuild
2. **Freeze all in-flight mesh-touching sprints** — i35-multi-mesh, anything proposing new mesh ops. Mark their sprint-config status as `superseded by docs/plans/mesh/`.

Both happen in one PR. After that lands, Sprint 0 starts on the rebuild branch.
