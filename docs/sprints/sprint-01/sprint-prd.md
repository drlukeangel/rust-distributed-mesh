# PRD — Mesh Substrate Rebuild

**Status:** Open (decision locked 2026-05-19)
**Owner:** Team Lead
**Initiative number:** TBA (probably i38; replaces i35-multi-mesh entirely)

---

## 1. The decision

Rafka's custom QUIC mesh is permanently retired. The substrate is rebuilt on an off-the-shelf mesh framework (default candidate: **iroh**). Every sprint after this initiative starts ships on the new substrate or doesn't ship at all.

This is the new Golden Principle #13 — *Rafka Does Not Own Mesh Infrastructure*. Mesh transport, peer discovery, NAT traversal, gossip, relay, and connection migration are off-the-shelf. Code that reintroduces hand-rolled mesh primitives is rejected at review.

## 2. Why

For the past 4+ sprints, every feature ship has hit a class of bugs that has nothing to do with the feature being shipped:

- Windows-specific inbound QUIC handshake hangs
- Custom gossip drift between gateways
- Ephemeral-port back-connect races
- Stale-rlib substrate contention across worktrees
- Cross-mesh peering not converging
- Harness scaffold drift between subprocess3 tests

These are not bad luck. They are the signature of *mesh treated as a feature when it should be infrastructure.* Rafka has been paying a per-sprint debugging tax to maintain its own QUIC mesh, while delivering zero competitive advantage from owning that layer.

Concurrent discovery: claimed-done migrations (e.g. "compute migrated to mesh") were not actually done — the codebase carries fictional `DONE` markers across many subsystems. Auditing every claim to ground truth costs roughly the same as rebuilding on a verified substrate. Rebuild is the better trade because the result sits on solid ground from day one.

There are no customers today. The opportunity-cost argument against the pivot is zero.

## 3. North-star outcomes

By the end of this initiative:

1. **Substrate is provably hard.** A 24-hour chaos run on N nodes with random kill / partition / NAT / flap injection produces zero permanent splits and zero unrecovered membership drift.
2. **Day-1 operability.** From Sprint 0, operators can SEE the mesh (topology UI) and DRIVE it (add/remove nodes via UI and via `rfa` CLI). Substrate is never a black box.
3. **Multi-mesh is native, not bolted on.** Cross-mesh peering via iroh-relay (or equivalent) is part of Sprint 2, not deferred to Phase N+5.
4. **All future feature sprints inherit the chaos-pass acceptance criterion.** "Tests pass under chaos" replaces "tests pass" as the bar.

## 4. Non-goals

- **Kafka wire-protocol compatibility is OUT until substrate is hard.** The existing wire surface is preserved as a spec target, not implemented during this initiative.
- **App-layer features (topics, jobs, RSQL, compute) are OUT.** They land in subsequent initiatives, each verified against the substrate's chaos suite.
- **Migration of existing data is OUT.** Zero customers means zero data to migrate.
- **Backwards compatibility with current mesh on-wire formats is OUT.** Custom QUIC mesh is retired; nothing speaks its wire.

## 5. Scope

In scope:

- Iroh-backed mesh substrate (or libp2p / quinn+chitchat if iroh fails the chaos battery)
- 4 node types as bare processes: `rafka-gateway`, `rafka-broker`, `rafka-compute`, `rafka-schema`
- Single shared codec via existing `rafka-mesh-ops` crate (`InternalMeshFrame` shape preserved)
- Topology UI (web-based, real-time mesh view) — Sprint 0 deliverable
- `rfa` CLI for node lifecycle ops — Sprint 0 deliverable
- Multi-mesh / cross-mesh bridging via iroh-relay — Sprint 2 deliverable
- Chaos harness with built-in failure-injection catalog — Sprint 1 deliverable
- Observability: every substrate operation emits OTLP spans + structured logs from day 1

Out of scope (for THIS initiative):

- Kafka wire protocol
- Authz / authn
- Topic records, WAL writes, schema registration
- REST management plane beyond node-lifecycle ops
- Org tenancy semantics

## 6. Acceptance criteria

This initiative is complete when ALL of the following are true:

1. `cargo run -p rafka-gateway` (and broker, compute, schema) boots a node that joins the mesh via iroh
2. Topology UI at `http://localhost:19090` shows the live node graph, updating in real-time as nodes join/leave
3. `rfa mesh node add --type broker` spawns a new broker process, it joins, UI shows it within 5s
4. `rfa mesh node remove <node-id>` terminates the process, UI shows it disappearing within gossip TTL (≤10s)
5. Chaos battery (Sprint 1 deliverable) runs for 24 consecutive hours with zero permanent splits, zero membership drift, zero deadlocks
6. Two meshes peered via iroh-relay (Sprint 2 deliverable) survive WAN partition + heal cycles without state corruption
7. Every substrate operation emits OTLP spans landing in `tests/artifacts/mesh/*.spans.jsonl`
8. Workspace gate `cargo check --workspace --tests --no-default-features` = 0 errors, 0 new warnings throughout the initiative
9. Zero hand-rolled mesh primitives (gossip, NAT traversal, peer discovery) in the codebase. `grep -rn "fn.*gossip\|fn.*discover_peer\|fn.*nat_traverse" --include="*.rs"` returns zero matches outside the iroh dependency

## 7. Out-of-band constraints

- **Sonnet for every subagent dispatch** (CLAUDE.md mandate).
- **No defer language** in plans or sprint configs.
- **OTLP artifact evidence** before any sprint closure (CLAUDE.md mandate).
- **No Claude attribution** in commit messages.
- **No `cargo clean`** as a debugging shortcut.
- **Stash → pull --rebase main → commit → push** between every fix batch.

## 8. Companion documents in this folder

| Doc | Purpose |
|---|---|
| `00-mesh-rebuild-prd.md` | This document. North-star PRD. |
| `01-substrate-prd.md` | Technical PRD for the iroh substrate selection + integration |
| `02-topology-ui-prd.md` | Day-1 topology UI requirements + design |
| `03-rfa-cli-prd.md` | Day-1 `rfa` CLI requirements for node lifecycle |
| `04-chaos-harness-prd.md` | Chaos test framework + acceptance battery |
| `05-sprint-plan.md` | Sprint-by-sprint execution sequence |
| `06-decisions.md` | Locked decisions log (one entry per binding decision) |

## 9. Timeline (aspirational, not contractual)

- **Sprint 0:** 2 weeks. Iroh substrate spike + day-1 UI + day-1 rfa CLI.
- **Sprint 1:** 2 weeks. Chaos harness + 24h soak pass.
- **Sprint 2:** 2 weeks. Multi-mesh via iroh-relay + cross-mesh chaos.
- **Sprint 3+:** Feature initiatives resume, each gated by chaos-pass on substrate.

End-to-end: **~6 weeks of focused substrate work** before feature initiatives resume.

## 10. What this replaces

- `i35-multi-mesh` — fully replaced by Sprint 2 of this initiative
- `i34-phase5-compute-rest-removal` — substrate-verification deferred; code shape lands now; the rebuild substrate verifies the migration semantics
- All "harness fragmentation" debugging effort — gone, replaced by chaos-test discipline
- Per-sprint substrate-debug tax — paid down to zero
