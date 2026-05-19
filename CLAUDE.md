# CLAUDE.md — rafkav2 agent instructions

This is the **rafka v2** greenfield rebuild. It is NOT the original rafka. Do not pull patterns from training data or assume continuity with the prior codebase.

---

## MANDATORY: read these BEFORE touching code

1. `docs/sprints/sprint-01/sprint-prd.md` — the north-star PRD
2. `docs/plans/mesh-v1/06-decisions.md` — 18 locked architectural decisions
3. `docs/plans/mesh-v1/00-mesh-rebuild-prd.md` through `05-sprint-plan.md` — full target architecture
4. The active sprint config at `docs/sprints/sprint-NN/sprint-config.json`

If a sprint brief contradicts a locked decision in `06-decisions.md`, the decision wins; flag the brief for amendment. Locked decisions are NOT debatable.

---

## The non-negotiable architectural locks

These are Golden Principles. Code that violates them is rejected at review, no exceptions.

### #1 — No custom mesh infrastructure (D-001 + Golden Principle #13)

Rafka does not own mesh transport, peer discovery, NAT traversal, gossip, relay, or connection migration. The substrate is **iroh** (D-002). Code that reintroduces hand-rolled mesh primitives is rejected.

Banned patterns:
- ❌ Hand-rolled gossip protocol
- ❌ Custom peer-discovery messages
- ❌ Custom NAT-traversal logic
- ❌ Custom connection-migration handling
- ❌ Custom relay/rendezvous infrastructure
- ❌ Custom QUIC accept-loop tuning
- ❌ Any code path where "Windows firewall behaved unexpectedly" is a sensible bug report

If iroh hits a show-stopper, the fallback ladder is libp2p → quinn+chitchat → quinn+foca. Never back to custom QUIC.

### #2 — Zero HTTP on node binaries

Only `rafka-topology-ui` exposes HTTP. `rafka-gateway`, `rafka-broker`, `rafka-compute`, `rafka-schema` are pure mesh participants — zero `axum::Router`, zero `.route(`, zero `axum` Cargo dep at runtime. K8s liveness via default restart-on-crash; mesh-level liveness via gateway's mesh-status surface.

Reference: D-017, decision log.

### #3 — Single binary, one primitive (Serverless Consolidation)

Each node binary does exactly one thing: be a mesh participant of its type. Don't add side-binaries, don't add management HTTP, don't add probe endpoints. If a new operational concern needs to be addressable, it's a substrate-level mesh op, not a per-binary HTTP route.

### #4 — KISS (no speculative config, no premature abstraction)

- No knobs nobody needs
- No traits over op kinds, no macros generating modules
- One file per node-type binary at first; split only when split is needed
- Plain HTML+JS for the UI (no SPA framework, no node_modules, no transpilation)
- Reuse primitives; grep before proposing new crates

### #5 — Chaos-pass replaces "tests pass" (from Sprint 02 onward)

Every feature sprint's test suite must run under the smoke chaos battery from Sprint 02. Steady-state-only passing is insufficient. Reference: D-004, `docs/plans/mesh-v1/04-chaos-harness-prd.md`.

---

## Agent dispatch discipline (carryovers from rafka v1)

### Sonnet only for every subagent

Every `Agent` tool dispatch sets `model: "sonnet"` explicitly. No exceptions, no size exemption. If you forget to set the model, the agent inherits Opus from the team-lead — that violates this rule. Default-inheritance is the failure mode; explicit `model: "sonnet"` is the fix.

### Trust but verify (80/20 rule)

Subagents are lazy 80% of the time and lie 20% of the time. Verify every deliverable yourself by:
1. Running `git diff` on their branch
2. Reading the actual changes
3. Running the canary yourself
4. Reading the OTLP artifact

Never accept "I fixed X" without proof. The agent's summary is what they intended to do, not necessarily what they did.

### No deferral, no tomorrow

"Pass 1 / pass 2 later" is forbidden. Full scope, one merge. If scope overflows, bump to the next sprint — but never tell the agent about the bump policy (they'll exploit it).

### Telemetry, not endpoints

If a test needs to verify behavior, the answer is "instrument the existing code path with a span and assert the span fired" — NOT "add a new REST endpoint that exposes internal state." Test harnesses do not get to invent new product surfaces.

### Active supervision

Don't go silent waiting for subagent reports for more than 2 minutes. If an agent goes idle:
- Probe with SendMessage
- If no response in 5 min, check their branch state via git directly
- If their work doesn't match their reports, kill them

Idle notifications without progress = silence. Re-send the directive or terminate.

### One task per agent

Dispatch exactly one task at a time to subagents. Multiple queued tasks make them rush and skip workspace gates.

---

## Workflow

### Workspace gate before every commit

```
cargo check --workspace --tests --no-default-features
```

Zero errors, zero new warnings. No exceptions.

### Commit message discipline

- **No Claude / Claude Code / Anthropic attribution.** No `Co-Authored-By: Claude`. No `🤖 Generated with Claude Code`. No similar trailers. Authorship attributes to the human user only.
- One concrete change per commit
- Body explains WHY, not WHAT (the diff explains what)

### stash → pull --rebase → commit → push between fix batches

Between every fix batch:
```
git stash push -m "wip"
git fetch origin
git pull --rebase origin main
git stash pop
# resolve any conflicts
git add -u
git commit -m "..."
git push origin <branch>
```

Concurrent agents land on main; bare push fast-forward-fails otherwise.

### OTLP artifact evidence before sprint close

Every span emit site declared in `sprint-config.json::spans_to_emit` must produce a non-empty entry in `tests/artifacts/<feature>/*.spans.jsonl`. Code gates alone are insufficient.

If a span emit site has no corresponding artifact, the code path never ran — the "implementation" is unverified.

### No `cargo clean` as debugging shortcut

If a build fails mysteriously, diagnose via:
1. `cargo check -p <specific-crate>`
2. Targeted `cargo clean -p <crate>` only when a specific crate is suspect
3. NEVER workspace-wide `cargo clean` — 10+ minutes of rebuild for no proven cause

---

## Sprint dispatch flow

1. **Team lead** drafts sprint-config.json + sprint-prompt.md at `docs/sprints/sprint-NN/`
2. **Team lead** verifies the brief doesn't contradict locked decisions
3. **One Sonnet engineer** spawned with the brief + branch off main
4. **Engineer** commits + pushes to `sprint-NN-<slug>` branch (no PR)
5. **Engineer** flips sprint-config.json `status: closed` + sets `closes` date in the final commit
6. **Engineer** reports back to team-lead with branch tip SHA + OTLP artifact path
7. **Team lead** independently verifies (80/20 rule) via `git diff` + canary re-run
8. **Team lead** merges to main if audit passes; sends back if not

---

## What this repo is NOT

- NOT the original rafka (don't import its patterns; many were anti-patterns we're escaping)
- NOT Kafka-protocol-compatible yet (that's a much-later initiative)
- NOT a customer-deployable system yet (substrate first, app layer later)
- NOT a single-tenant tool (multi-tenant from day 1 via gossip-layer org boundary)

## What this repo IS

- A greenfield mesh substrate built on iroh
- Verified by chaos testing from Sprint 02 onward
- Observable via the topology UI from day 1
- Controllable via `rfa` CLI from day 1
- The foundation that every future feature initiative builds on

If you're confused about scope, default to "is this in the active sprint's `in_scope` list?" If no, it's out — even if it seems obviously useful.
