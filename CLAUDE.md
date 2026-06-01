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
- UI: the admin-ui is a **React + react-flow SPA** built by Vite (`admin-ui/web/`, served from
  `web/dist`). The earlier "plain HTML+JS only, no SPA, no node_modules, no transpilation" rule was
  **dropped 2026-05-31** — build UI in `admin-ui/web/src/` (TSX) and rebuild with `npm run build`.
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
| `rafka.mesh.heartbeat` | `node_id`, `peer_count`, `cpu_used`, `cpu_budget`, `ram_used`, `ram_budget` |
| `rafka.mesh.node.stopping` | `node_id`, `reason` |
| `rafka.mesh.peer.discovered` | `node_id` (local), `peer_id` (remote), `peer_node_type` |
| `rafka.mesh.peer.connected` | `node_id`, `peer_id`, `peer_node_type` |
| `rafka.mesh.peer.disconnected` | `node_id`, `peer_id`, `reason` |
| `rafka.mesh.peer.staleness_timeout` | `node_id`, `peer_id`, `last_seen_ms_ago` |
| `rafka.mesh.frame.sent` | `node_id` (src), `peer_id` (dst), `op_kind`, `bytes`, `trace_id` |
| `rafka.mesh.frame.received` | `node_id` (dst), `peer_id` (src), `op_kind`, `bytes`, `trace_id` |
| `rafka.mesh.frame.decode_failed` | `node_id`, `peer_id`, `bytes`, `error` |
| `rafka.mesh.produce.handle` (sprint-13 B5) | `node_id`, `peer_id`, `op_kind` (`"produce"`), `write_from`, `write_to`, `seq` — broker's own work, parented to the gateway's propagated W3C context |
| `rafka.mesh.produce.ack` (sprint-13 B5) | `node_id`, `peer_id`, `op_kind` (`"ack"`), `seq` — broker→gateway ack; its W3C context is injected into the Ack frame so the gateway's ack-receive parents onto the broker (reverse edge) |
| `rafka.mesh.backbone.published` (sprint-14, PRD 03) | `node_id`, `mesh_id`, `publisher` (publishing gateway's node_id), `node_count`, `cpu_used`, `cpu_budget`, `ram_used`, `ram_budget` — the per-mesh aggregate rides as span attributes (spans-only stack; no separate metrics SDK). Emitted by the soft-lease holder each interval. |
| `rafka.mesh.backbone.received` (sprint-14, PRD 03) | `node_id`, `mesh_id` (of the summary), `publisher`, `node_count` — a consumer (gateway or admin-ui) received another mesh's summary off the backbone topic. |
| `rafka.mesh.backbone.tombstone_published` (sprint-18) | `node_id` (publisher), `dead_node_id`, `dead_mesh_id`, `published_by` — emitted by any backbone participant (admin-ui or gateway) that broadcasts a `BackboneMessage::Tombstone` on the backbone topic. This is the cross-mesh fast-eviction signal; it reaches gateways in every mesh (not just the killer's own mesh). `otel.kind="producer"`. |
| `rafka.mesh.backbone.tombstone_applied` (sprint-18) | `node_id` (local consumer applying the eviction), `dead_node_id`, `dead_mesh_id`, `published_by` — emitted by every backbone subscriber that receives a `BackboneMessage::Tombstone` and calls `apply_tombstone`. On the dead node's home-mesh gateway, this triggers eviction from `live_digests` → next `MeshSummary` publish omits the dead node. `source="backbone_receive"`. `otel.kind="consumer"`. |
| `rafka.mesh.tombstone.applied` (sprint-15; `source` extended sprint-20) | `node_id` (the dead node being evicted), `source` (`"local"` = killer console self-injection, `"gossip_receive"` = applied on a peer's receive path from a `Leaving` digest, `"staleness_dead"` = sprint-20, the staleness pruner evicting a node that vanished with NO `Leaving` — observer-inferred `Dead`) — emitted by `apply_tombstone` every time a node is evicted from all three process-global maps. The `source` attribute distinguishes graceful `Leaving` (`local`/`gossip_receive`) from crash-inferred `Dead` (`staleness_dead`); both evict, the source tells them apart. (`source` is an append-only value set, like the enums.) |
| `rafka.mesh.control.shutdown_sent` (sprint-19) | `node_id` (the sender, e.g. an admin-ui console), `peer_id` (target node_id), `op_kind` (`"control"`), `reason` — emitted by `send_shutdown` when a console dials a target node and writes an `InternalMeshFrame::Shutdown`. This is the control-plane KILL: any mesh participant can shut down any node it can address, NO OS-process ownership (replaces TerminateProcess-on-own-child). `otel.kind="producer"`. |
| `rafka.mesh.control.shutdown_received` (sprint-19) | `node_id` (the node shutting itself down), `peer_id` (the requester), `op_kind` (`"control"`), `reason` — emitted by a node's `run_bi_reader` when it receives a Shutdown control op; the node then broadcasts its own gossip tombstone and runs the standard graceful-shutdown path. `otel.kind="consumer"`. |
| `rafka.mesh.control.state_change_sent` (sprint-21) | `node_id` (the sender, e.g. an admin-ui console), `peer_id` (target node_id), `op_kind` (`"control"`), `state` (desired NodeState name: `"Updating"`/`"Draining"`/`"Alive"`) — emitted by `send_set_state` when a console dials a target and writes an `InternalMeshFrame::SetState`. The NON-terminal sibling of the kill: marks a node's lifecycle (rolling/draining) or resumes it, no shutdown. `otel.kind="producer"`. |
| `rafka.mesh.control.state_change_received` (sprint-21) | `node_id` (the node applying the override), `peer_id` (the requester), `op_kind` (`"control"`), `state`, `accepted` (bool — false for a non-settable/terminal state) — emitted by a node's `run_bi_reader` on a SetState op; on accept it stores the value as its self-state override (`Updating`/`Draining`) or clears it (`Alive` resume), and the next gossip digest publishes the new `state`. `otel.kind="consumer"`. |
| `rafka.mesh.node.state_changed` (sprint-20 vocab, emitted 2026-06-01) | `node_id`, `node_name`, `from` (prior `NodeState`), `to` (new `NodeState`), `source` (`"self"`) — emitted by the digest-builder on EVERY self-state transition (`Joining→Alive`, `Alive→Degraded`, `→Updating`, `→Draining`, …). One span per transition gives Jaeger the FULL per-node lifecycle even for states too fleeting to render. Terminal `Leaving`/`Dead` evictions are recorded by `rafka.mesh.tombstone.applied` (source `gossip_receive`/`staleness_dead`), not here. `otel.kind="internal"`. |

**`op_kind` enum (locked):** `"produce"`, `"fetch"`, `"replication"`, `"schema_lookup"`, `"ping"`, `"pong"`, `"control"`, `"ack"` (sprint-13 B5 — broker→gateway acknowledgement). Future op classes append; never reuse a string for a different meaning.

**`node_type` enum (locked):** `"gateway"`, `"broker"`, `"compute"`, `"registry"`, `"admin-ui"` (sprint-14 B6 — the operator console is now a normal mesh node: `RAFKA_MESH_ID=<real mesh>`, self-names `<mesh>.admin-ui.<6hex>`, joins its home mesh's gossip + consumes the backbone; NO flat/`"admin"` special case). Future node types append.

**`NodeState` enum (locked, sprint-20):** `Joining`, `Alive`, `Degraded`, `Updating`, `Draining`, `Leaving`, `Dead` — the `state` field on `GossipDigest` (replaces the sprint-15 `leaving: bool`), published as an event on every transition (generalizes the fast-delete tombstone). Self-published: `Joining/Alive/Degraded/Updating/Draining/Leaving`. Observer-inferred: `Dead` (a node that vanished with no `Leaving` — the staleness/crash fallback). Receivers evict on `Leaving`/`Dead`; other states upsert + render. APPEND-ONLY and ORDER IS LOCKED (postcard discriminant is positional — new variants go at the END, never reorder/remove/repurpose).

**Service-name contract (locked, sprint-13 B1).** Each node SELF-NAMES from its own `node_id` (the iroh public key, known after identity load at boot). `node_name = <mesh>.<type>.<first NODE_NAME_HEX_LEN hex of node_id>` (full type word, NO ordinal/abbrev/symbol; `NODE_NAME_HEX_LEN=6`). `service.name` is mesh-qualified so Jaeger's System Architecture graph renders one node per mesh+type (NOT a single collapsed `broker`):

| node_type | node_name (self-derived) | `service.name` (`OTEL_SERVICE_NAME`) | `service.namespace` | `service.instance.id` |
|---|---|---|---|---|
| gateway  | `mesh1.gateway.3a23aa`  | `mesh1.gateway`  | `mesh1` | `<full node_id>` |
| broker   | `mesh2.broker.ccff65`   | `mesh2.broker`   | `mesh2` | `<full node_id>` |
| compute  | `mesh1.compute.61c8f1`  | `mesh1.compute`  | `mesh1` | `<full node_id>` |
| registry | `mesh1.registry.cba58f` | `mesh1.registry` | `mesh1` | `<full node_id>` |
| admin-ui | `mesh1.admin-ui.a5162c` | `mesh1.admin-ui` | `mesh1` | `<full node_id>` |

- **The node owns its identity.** Before `init_telemetry`, the node loads its identity, computes `service.name`/`service.namespace`/`service.instance.id`, and sets `OTEL_SERVICE_NAME` + `OTEL_RESOURCE_ATTRIBUTES` itself. admin-ui passes only `RAFKA_MESH_ID` + the node_type (the binary) at spawn — it does NOT pass `RAFKA_NODE_NAME` / `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES`. admin-ui pre-mints the child identity (for seeding) so it can derive the identical name for its own bookkeeping.
- **The admin-ui is a normal node (sprint-14 B6 — supersedes the sprint-13 Observer exception):** it gets `RAFKA_MESH_ID=<a real mesh, e.g. mesh1>`, self-names `mesh1.admin-ui.<6hex>`, joins its home mesh's gossip for full per-node detail, and consumes the **backbone** for the cross-mesh view. `Role::Observer` now means only "does not run the data-plane write-sim" — it no longer changes naming. **Sprint-19: the admin-ui DOES publish on the backbone.** The admin-ui is a node in its mesh, so it is a backbone publisher CANDIDATE alongside gateways (the soft lease elects one publisher per mesh; all candidates aggregate the identical summary from `live_digests()`). This means a mesh advertises itself cross-mesh even when its ONLY node is its admin-ui console — both consoles show both meshes regardless of whether either mesh has a gateway. The flat `admin-ui`/`"admin"` magic is gone.
- **`RAFKA_MESH_ID` is REQUIRED** — a node with it unset/empty FAILS FAST and refuses to boot. No silent `"default"` mesh.
- `rafka-telemetry::build_resource()` merges env-detected attributes (`OTEL_RESOURCE_ATTRIBUTES`) with an explicit `service.name` via `Resource::default().merge(...)` (it must NOT use bare `Resource::new`, which replaces env detection).
- QUIC mesh hops propagate trace context via the **global W3C `TextMapPropagator`** (traceparent + tracestate) embedded in the frame carrier — the SAME mechanism as HTTP hops. No hand-rolled context struct.

**Substrate metric contract (locked):**

| Metric | Unit | Labels |
|---|---|---|
| `rafka.mesh.bytes_sent_per_sec` | bytes/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.bytes_received_per_sec` | bytes/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.frames_sent_per_sec` | frames/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.frames_received_per_sec` | frames/sec (gauge) | `src_node_id`, `dst_node_id`, `op_kind` |
| `rafka.mesh.frame.decode_error_rate` | errors/sec (gauge) | `src_node_id`, `dst_node_id` |
| `rafka.mesh.peer.rtt_ms` | milliseconds (histogram) | `node_id`, `peer_id` |
| `rafka.mesh.node.cpu_used_cores`   | cores (gauge) | `node_id`, `node_type` |
| `rafka.mesh.node.cpu_budget_cores` | cores (gauge) | `node_id`, `node_type` |
| `rafka.mesh.node.ram_used_gb`      | GB (gauge)    | `node_id`, `node_type` |
| `rafka.mesh.node.ram_budget_gb`    | GB (gauge)    | `node_id`, `node_type` |

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
- `RAFKA_NODE_TYPE` — gateway / broker / compute / registry
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

All env vars recognized by node binaries (`gateway`, `broker`, `compute`, `registry`). No other configuration mechanism exists.

| Env var | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4316` | OTLP gRPC collector URL. Port 4316 maps to `rafka-test-jaeger` container's OTLP/gRPC port. Override for any other collector. |
| `OTEL_SERVICE_NAME` | `gateway` | Service name shown in Jaeger's left-rail filter. |
| `RAFKA_DATA_DIR` | `./data/node-<random-hex>` | Directory where `node-identity.json` is stored. Set this to a stable path across restarts to preserve node identity (same `node_id` across reboots). |
| `RAFKA_NODE_SECRET_KEY` | _(unset = load/mint from `RAFKA_DATA_DIR`)_ | Hex-encoded 32-byte iroh secret key. When set, it is the **source of truth** for the node's identity — `load_or_mint_identity` uses it directly (and persists it to `RAFKA_DATA_DIR` for restart stability) instead of reading/minting a file. **DEV-SPAWN ONLY:** admin-ui passes the child's pre-minted key this way so the booted identity can never diverge from the name/seed entry admin-ui recorded — eliminating the concurrent-spawn file round-trip race (a child loading a different identity → duplicate `node_id` → seed-dial TLS `UnknownIssuer` → ghost). NOT a production identity-provisioning pattern (the key rides the child env block). Added 2026-06-01. |
| `RAFKA_NODE_BIND_ADDR` | `0.0.0.0:0` | IPv4 socket address iroh binds the QUIC endpoint to. Port 0 = ephemeral OS-assigned. Override to pin to a specific port for firewall rules. |
| `RAFKA_GOSSIP_INTERVAL_MS` | `2000` | Gossip heartbeat interval in milliseconds. Bumped from 500 → 2000 as part of the CPU-optimization pass (07-cpu-optimization-plan.md): substrate health doesn't need 2 Hz granularity, and at 18 nodes the per-tick fanout cost was non-trivial. |
| `RAFKA_STALENESS_MS` | `30000` | Background pruner threshold (milliseconds). A `GossipDigest` whose `wall_time_ms` is older than this is removed from the process-global `live_digests` + `topic_membership` maps. Sweeps every 5 seconds. At the default 2s gossip cadence, 30s = 15 missed cycles. Override to tune how aggressively stale peers are evicted from the topology view. |
| `RAFKA_SEED_NODES` | _(empty)_ | Comma-separated list of `<node_id_hex>@<host>:<port>` entries to dial on boot. Each seed triggers `rafka.mesh.peer.discovered` + `rafka.mesh.peer.connected` spans. Example: `abc123...@127.0.0.1:14820`. Added Sprint 03. |
| `RAFKA_AUTO_SHUTDOWN_SECS` | _(unset = wait for signal)_ | If set, node shuts down cleanly after this many seconds. Verification hook only — used to produce a clean process exit (and thus flush OTLP spans) in environments where Ctrl+C delivery is unreliable (e.g. Windows child process). |
| `RAFKA_TOPOLOGY_UI_BIND_ADDR` | `127.0.0.1:19090` | TCP address the `rafka-topology-ui` HTTP server binds to. Override to expose on a different interface or port. |
| `JAEGER_QUERY_URL` | `http://localhost:16686` | Base URL of the Jaeger Query API. Used by `rafka-topology-ui` (chunk 2+) to fetch trace data for the boot-waterfall panel. |
| `CARGO_TARGET_DIR` | `./target` | Read by `rafka-topology-ui` to locate node binaries for spawn. Set to `E:/cargo-target-sprint-02` in local dev so the UI can find debug builds without a separate install step. |
| `RAFKA_DEPLOYMENT` | `prod` | One of `dev`, `staging`, `prod`. Gates all `RAFKA_DEV_*` overrides. Defaults to `prod` so a production manifest gets safe behavior without explicit setting. Set to `dev` automatically by admin-ui for every spawned child. |
| `RAFKA_DEV_CPU_BUDGET` | _(measured via sysinfo)_ | Override the reported `cpu_budget` field in `GossipDigest` (cores, fractional). Honored only when `RAFKA_DEPLOYMENT != prod`. Used by per-crate `.env.dev` to give heterogeneous test brokers different CPU profiles. |
| `RAFKA_DEV_RAM_BUDGET` | _(measured via sysinfo)_ | Override reported `ram_budget` in GB. Same gating as `RAFKA_DEV_CPU_BUDGET`. |
| `RAFKA_DEV_CPU_USED` | _(measured via sysinfo)_ | Override reported `cpu_used` in cores. For deterministic routing/migration test scenarios (broker reports 95% load without actually being loaded). Same gating. |
| `RAFKA_DEV_RAM_USED` | _(measured via sysinfo)_ | Override reported `ram_used` in GB. Same gating. |
| `RAFKA_CPU_ALERT_THRESHOLD` | `0.10` | Cores. Admin-ui `/api/alerts` emits a warn-severity alert for any node whose latest `GossipDigest.cpu_used` exceeds this. Release-build empty-shell baseline is ~0.02 cores; default 0.10 = ~5× headroom. Read once per `/api/alerts` request. |
| `RAFKA_RAM_ALERT_THRESHOLD_GB` | `0.5` | GB. Same shape as `RAFKA_CPU_ALERT_THRESHOLD` but for `ram_used`. Release baseline ~0.06 GB; default 0.5 = ~8× headroom. |
| `RAFKA_SPAWN_PORT_BASE` | `15820` | Admin-ui spawn-pool starting port. Children receive sequentially assigned ports starting from this base (one per spawn). Default 15820 keeps Phase 1 away from the legacy baseline port range (16820+). Override when multiple admin-ui instances run on one host. Added mesh-v2 Phase 1. |
| `RAFKA_RELAY_URL` | _(unset = `RelayMode::Disabled`)_ | Optional iroh relay URL (e.g. `http://127.0.0.1:3340`). When set, the node's iroh endpoint registers it via `RelayMode::Custom` so the relay path EXISTS as the cross-mesh transport (replaces the bridge). **mesh-v2 Phase 2: the relay is IDLE** — all writes are direct/loopback, nothing routes through it. Plumbing only; the relay's traffic-carrying role is proven in a later forced-isolation round. Default unset = direct-only. Added mesh-v2 Phase 2. |
| `RAFKA_WRITE_SIM_INTERVAL_MS` | `5000` | Gateway-only. Interval between write-sim sends. Each tick, a gateway resolves a broker in its own mesh (intra, via `live_digests()`) and a broker in the other mesh (cross-mesh, via the **backbone directory** as of sprint-14) and sends each a `Write` over a bi-stream. Added mesh-v2 Phase 2; cross-mesh resolution moved to the backbone in sprint-14. |
| `RAFKA_BACKBONE_INTERVAL_MS` | `2000` | Gateway + admin-ui. Interval at which a gateway publishes its mesh's `MeshSummary` to the backbone topic (and at which every backbone subscriber feeds new peers to the swarm). The soft-lease TTL is 3× this interval, so one missed renew does not trigger failover but a dead publisher does within ~TTL. Added sprint-14. |

**Cross-mesh backbone (sprint-14, PRD 03 — supersedes the "gateway observes both meshes" mechanism above):** A single iroh-gossip topic `blake3("rafka.backbone")` carries per-mesh `MeshSummary` records (`directory[name→{type,node_id,location,state}]` (`state` appended sprint-20, `serde(default)`→`Alive`, so a cross-mesh console renders a remote node's `NodeState`) + `aggregate{node_count,cpu,ram,frames_per_sec}` + soft-lease `published_by`/`expires_at_ms`). The **publisher** — a gateway OR the admin-ui (both are candidates as of sprint-19, since the admin-ui is a node in its mesh) — aggregates its OWN mesh from `live_digests()` and publishes; **publisher selection is a SOFT LEASE, not an election** — a live claim is never preempted by a lower-id joiner, `min(node_id)` over live publisher candidates (gateways + admin-ui) breaks ties only for a vacant/expired seat, failover happens when the claim expires (dead-man's switch). Gateways resolve a cross-mesh write's target `location`+`node_id` from the backbone directory and NO LONGER subscribe to the other mesh's full gossip — so a gateway's `topic_membership` contains only its own mesh. admin-ui consumes the backbone for the global view + its home mesh's gossip for local detail, and ALSO publishes its own mesh's summary (sprint-19) so a mesh whose only node is its admin-ui is still visible cross-mesh. `backbone_summaries()` is a process-global map SEPARATE from `topic_membership` (receiving a remote summary does not make the node a member of the remote mesh). Directory entries carry `node_id` (deviation from PRD §2's literal `{node_name,node_type,location}`) because iroh `endpoint.connect` is identity-based and the 6-hex node_name suffix is not reconstructable.

**`InternalMeshFrame::Write` variant (mesh-v2 Phase 2):** New frame `Write { from, to, seq }` for the gateway write-sim. Maps to the locked §10 `op_kind="produce"` on its `frame.sent`/`frame.received` spans. Renders as `frame_kind="write"` in the Messages tab.

**Bridge removed entirely (mesh-v2 Phase 2, PRD §5):** `Role::Bridge`, `RAFKA_BRIDGE_TARGET_MESHES`, the `rafka-bridge` crate/binary, and all admin-ui/React bridge surfaces are gone. Cross-mesh awareness comes from the gateway observing both meshes (above); cross-mesh transport is the relay (`RAFKA_RELAY_URL`). No bridge node exists in spawn, gossip, or render.

**`GossipDigest.location` field (added mesh-v2 Phase 1):** String field `location` appended to `GossipDigest`. Value = the node's `RAFKA_NODE_BIND_ADDR` at startup (e.g. `"127.0.0.1:15820"`). If `RAFKA_NODE_BIND_ADDR` was `0.0.0.0:<port>`, location is rewritten to `127.0.0.1:<port>` so loopback-local peers can actually dial it. Consumed by `/api/topology-cache` as the directory's `location` field. Old digests (pre-Phase 1) will have `location: ""` since it's the last postcard field.

**`RAFKA_NODE_NAME` format (sprint-13 B1, supersedes Phase 1):** Admin-ui assigns deterministic names `<mesh>.<type>.<abbrev><N>` (e.g. `mesh1.gateway.gw1`, `mesh2.broker.br1`). The counter is per-`(mesh_id, node_type)` pair, in-process, 1-based; abbrev = gateway→gw, broker→br, compute→cp, registry→rg. The three dotted segments derive the three OTel fields (see the Service-name contract in §10). Supersedes the Phase-1 `mesh1.broker1` two-segment form. The name is passed as `RAFKA_NODE_NAME` to each child and appears in GossipDigest, spans, and the topology cache. The gateway write-sim resolves targets `mesh1.broker.br1` / `mesh2.broker.br1` by this name.

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
