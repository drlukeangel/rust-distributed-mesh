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

### #6 — Telemetry IS the substrate, not a feature

Telemetry is built FIRST in every sprint, and EVERY code path emits spans. Code that does work without leaving a telemetry trail is rejected at review.

- Every async `fn` is `#[instrument]`-decorated
- Every state transition is a span
- Every error is a span attribute or a child error-span
- Every boot sequence is a chain of nested spans under one parent
- Every binary calls `rafka_telemetry::init_telemetry(service_name)` in `main()` BEFORE any other work
- OTLP collector + Jaeger run from day 0 — `deployment/dev/docker-compose.otlp.yml` is the first deliverable of any sprint that needs verification

The pilot phase (now) skips formal test coverage, but telemetry coverage is non-negotiable. You prove behavior via Jaeger queries, not via test assertions.

### #7 — Every sprint closes with a Jaeger URL

The engineer's final SendMessage to team-lead MUST include a clickable Jaeger URL pre-filtered to the sprint's spans. Without the URL, the sprint is not closed. The URL is the user-facing proof of work.

Format: `http://localhost:16686/search?service=<service-name>&operation=<root-span>&lookback=1h`

If multiple services are involved, include one URL per service. If a specific span chain proves the sprint's exit criterion, link directly to a trace ID.

### #10 — Span + metric vocabulary is locked once, not invented per-sprint

The names, attributes, and units of OTLP spans/metrics across the substrate are decided ONCE in CLAUDE.md and treated as a stable contract. Sprints emit against the locked vocabulary; they do not invent new attribute names or rename existing ones.

**Why:** the dynamic throughput viz, the topology UI, the topology log, the OTLP heartbeat panel — all of them consume spans/metrics by name + attribute. If Sprint 04 emits `src=...` and Sprint 11 expects `src_endpoint_id=...`, the viz silently shows zero data. Retro-renaming spans across already-merged sprints is a permanent tax we don't pay.

**Substrate span attribute contract (locked):**

| Span | Required attributes |
|---|---|
| `rafka.mesh.node.ready` (root boot) | `node_id`, `node_type`, `bind_addr`, `version` |
| `rafka.mesh.boot.identity_loaded` | `node_id`, `path` |
| `rafka.mesh.boot.identity_minted` | `node_id`, `path` |
| `rafka.mesh.boot.endpoint_created` | `node_id`, `bind_addr` |
| `rafka.mesh.boot.alpn_registered` | `node_id`, `alpn` (e.g. `"rafka-mesh-v1"`) |
| `rafka.mesh.boot.gossip_started` | `node_id` |
| `rafka.mesh.boot.accept_loop_started` | `node_id` |
| `rafka.mesh.heartbeat` | `node_id`, `peer_count` |
| `rafka.mesh.node.stopping` | `node_id`, `reason` |
| `rafka.mesh.peer.discovered` | `node_id` (local), `peer_id` (remote), `peer_node_type` |
| `rafka.mesh.peer.connected` | `node_id`, `peer_id`, `peer_node_type` |
| `rafka.mesh.peer.disconnected` | `node_id`, `peer_id`, `reason` |
| `rafka.mesh.peer.staleness_timeout` | `node_id`, `peer_id`, `last_seen_ms_ago` |
| `rafka.mesh.frame.sent` | `node_id` (src), `peer_id` (dst), `op_kind`, `bytes`, `trace_id` |
| `rafka.mesh.frame.received` | `node_id` (dst), `peer_id` (src), `op_kind`, `bytes`, `trace_id` |
| `rafka.mesh.frame.decode_failed` | `node_id`, `peer_id`, `bytes`, `error` |

**`op_kind` enum (locked):** `"produce"`, `"fetch"`, `"replication"`, `"schema_lookup"`, `"ping"`, `"pong"`, `"control"`. Future op classes append; never reuse a string for a different meaning.

**`node_type` enum (locked):** `"data-gateway"`, `"broker"`, `"compute"`, `"schema"`. Future node types append.

**Substrate metric contract (locked):**

| Metric | Unit | Labels |
|---|---|---|
| `rafka.mesh.bytes_sent_per_sec` | bytes/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.bytes_received_per_sec` | bytes/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.frames_sent_per_sec` | frames/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.frames_received_per_sec` | frames/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.frame.decode_error_rate` | errors/sec (gauge) | `src_node_id`, `dst_node_id` |
| `rafka.mesh.peer.rtt_ms` | milliseconds (histogram) | `node_id`, `peer_id` |

Aggregation window: **5-second sliding** for every per-sec gauge. Locked so the dynamic-throughput viz can divide consistently.

**How to extend:**
- New span: propose the addition + attribute list in the sprint's `sprint-config.json::spans_to_emit` AND append to this table in the same commit. CLAUDE.md update lands before the emit code.
- New attribute on an existing span: propose in the sprint config, then update this table. NEVER add silently.
- Rename: not allowed. Add a new name, deprecate the old in this table, give two sprints of co-emit before removal.
- New `op_kind` or `node_type` enum value: append-only. Old values are immortal.

**Banned patterns:**
- ❌ Inventing attribute names mid-sprint (`src` vs `src_id` vs `source_endpoint_id` — pick once, pick `node_id`)
- ❌ Reusing an existing span name for a different event (use a new name)
- ❌ Per-sprint metric name drift (`rafka.bytes_per_sec` vs `rafka.bytes/s` vs `rafka.byte_rate`)
- ❌ Inconsistent units across similar metrics (one in bytes, one in KB — always SI base units)

### #9 — Latest stable version of every dependency. Always.

Every `Cargo.toml` dep starts at the latest stable version published on crates.io. Every external tool (iroh, opentelemetry-otlp, tracing-opentelemetry, libp2p, axum, quinn, etc.) gets the latest stable on the day the sprint opens. No "let's use 0.35 because that's what an old example showed."

**Why:** rafkav2 is greenfield. There is zero installed-base inertia, zero customer-pinned versions, zero data-format compatibility to preserve. Starting on an old version pays the cost of EVERY bug between that version and current — bugs that the upstream team already fixed. The Sprint 01 iroh 0.35 → 0.91 saga is the cautionary tale: a 30-second version bump would have skipped 4+ hours of WMI-COM-init debugging.

**How to apply:**
- When a sprint adds a new dep, run `cargo add <crate>` (no version pin) — cargo picks latest
- When a sprint inherits an existing dep, check `cargo outdated -w` before starting; bump if behind
- When a sprint's pre-reads point at version-specific docs, ALWAYS cross-check the current docs.rs page first
- When the latest version has an API change vs. older docs, USE THE NEW API; don't pin to old
- Version pins (`= "X.Y.Z"`) are reserved for two cases: (a) actual external constraint we can prove (rare, document it), (b) workaround for a regression in latest (open the upstream issue + link it in the Cargo.toml comment)

**Banned patterns:**
- ❌ "I'll just use the version the docs example shows" → docs lag the latest crate by months
- ❌ "Let's pin to a known-good version for stability" → in a greenfield, the latest IS the known-good
- ❌ Copy-paste a version number from an older sister project (rafka v1 patterns do NOT carry over)
- ❌ Compatibility-range pins like `"^0.35"` that silently keep us on old majors — use `cargo add` which writes the current major

If a sprint engineer hits a blocker, ALWAYS test "bump to latest" as the first 15-minute experiment before deeper diagnosis. Saves hours.

### #8 — All configuration via environment variables. Zero config files for substrate.

No TOML, YAML, or JSON config files for substrate-layer settings (transport, identity, telemetry, peer discovery, gossip, bind addrs, ports). Env vars only, every var has a sane default, every override documented in CLAUDE.md.

**Why:** rafka v1 accumulated 4 different config patterns (env + toml + env-pointing-to-toml + hardcoded magic numbers like port 4315/4316/16686). The result: nobody knew what port the collector was on without reading the running container. v2 doesn't repeat this.

**OpenTelemetry standard env vars** (use these for telemetry; do NOT invent rafka-specific shims):
- `OTEL_EXPORTER_OTLP_ENDPOINT` — collector URL (e.g. `http://localhost:4316`)
- `OTEL_SERVICE_NAME` — what shows in Jaeger left-rail filter
- `OTEL_TRACES_SAMPLER_ARG` — sampling ratio
- `OTEL_RESOURCE_ATTRIBUTES` — extra k=v pairs

**Rafka-specific env vars** (prefix `RAFKA_*`, every one with a default):
- `RAFKA_NODE_TYPE` — data-gateway / broker / compute / schema
- `RAFKA_DATA_DIR` — where identity + state lives (default `./data/node-${random}`)
- `RAFKA_NODE_BIND_ADDR` — iroh endpoint bind (default `0.0.0.0:0` ephemeral)
- `RAFKA_SEED_NODES` — CSV of `<endpoint_id>@<host>:<port>` for bootstrap discovery
- `RAFKA_GOSSIP_INTERVAL_MS` — heartbeat cadence (default `500`)

Every new env var added in a sprint MUST be documented in CLAUDE.md as part of the close-out commit. Config files are reserved for app-layer customer-facing policy (when the app layer exists in a much-later initiative); never substrate.

**Banned patterns:**
- ❌ Magic-number ports anywhere in code (`9092`, `4317`, etc.) — env var with default
- ❌ TOML/YAML/JSON files for node config
- ❌ Env vars pointing to config-file paths (the `RAFKA_GATEWAY_CONFIG=path/to/toml` pattern from v1)
- ❌ Hardcoded paths to data dirs / log dirs / cert dirs

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

## Env vars (all node binaries)

All env vars recognized by node binaries (`data-gateway`, `broker`, etc.). No other configuration mechanism exists.

| Env var | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4316` | OTLP gRPC collector URL. Port 4316 maps to `rafka-test-jaeger` container's OTLP/gRPC port. Override for any other collector. |
| `OTEL_SERVICE_NAME` | `data-gateway` | Service name shown in Jaeger's left-rail filter. |
| `RAFKA_DATA_DIR` | `./data/node-<random-hex>` | Directory where `node-identity.json` is stored. Set this to a stable path across restarts to preserve node identity (same `node_id` across reboots). |
| `RAFKA_NODE_BIND_ADDR` | `0.0.0.0:0` | IPv4 socket address iroh binds the QUIC endpoint to. Port 0 = ephemeral OS-assigned. Override to pin to a specific port for firewall rules. |
| `RAFKA_GOSSIP_INTERVAL_MS` | `500` | Gossip heartbeat interval in milliseconds. Stub in Sprint 01 — logged as a span attribute but not yet wired to real gossip scheduling. |
| `RAFKA_SEED_NODES` | _(empty)_ | Comma-separated list of `<node_id_hex>@<host>:<port>` entries to dial on boot. Each seed triggers `rafka.mesh.peer.discovered` + `rafka.mesh.peer.connected` spans. Example: `abc123...@127.0.0.1:14820`. Added Sprint 03. |
| `RAFKA_AUTO_SHUTDOWN_SECS` | _(unset = wait for signal)_ | If set, node shuts down cleanly after this many seconds. Verification hook only — used to produce a clean process exit (and thus flush OTLP spans) in environments where Ctrl+C delivery is unreliable (e.g. Windows child process). |

**Infrastructure context (Sprint 01):** The shared `rafka-test-otel-collector` receives spans on `localhost:4317` (gRPC). The `rafka-test-jaeger` instance also accepts OTLP/gRPC directly on `localhost:4316` (host → container 4317). Sprint 01 uses port 4316 (direct to Jaeger, skips collector). Jaeger UI: `http://localhost:16686`.

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
