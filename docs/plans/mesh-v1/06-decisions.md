# Decisions Log — Mesh Substrate Rebuild

Locked decisions. One entry per binding decision. When a decision is revised, the entry stays but is annotated `SUPERSEDED-BY: <date> <new-entry-id>`.

---

## D-001 — Custom QUIC mesh is permanently retired
**Date:** 2026-05-19
**Status:** Locked

Rafka does not own mesh infrastructure. The custom QUIC mesh that powered intra-cluster + cross-cluster traffic is deleted at end of Sprint 1. No `#[cfg(feature = "legacy-mesh")]` paths preserved. Code that reintroduces hand-rolled mesh primitives (gossip, NAT traversal, peer discovery, connection migration, relay) is rejected at review.

**Rationale:** 4+ sprints of per-sprint substrate-debug tax. Zero competitive advantage from owning the mesh layer. Strategic mistake to be reversed via this initiative.

**Adds:** Golden Principle #13 (`docs/eng/rafka-golden-principles.md`).

---

## D-002 — iroh is the default substrate candidate
**Date:** 2026-05-19
**Status:** Locked (pending Sprint 0 spike validation)

The Sprint 0 substrate spike uses `iroh`. If the spike fails the chaos-readiness criteria, the fallback ladder is:
1. `libp2p` (rust-libp2p) — heavier API, longer track record
2. `quinn + chitchat` (gossip framework) — lower-level composition
3. `quinn + foca` (SWIM impl) — alternative gossip

**Custom QUIC mesh is NOT on the fallback ladder.** If all 3 alternatives fail, the failure mode is "rescope the requirements," not "build our own."

**Rationale:** iroh is QUIC-native, has built-in relay tier for cross-NAT, and is actively maintained by n0-computer. Best match for rafka's requirement set.

---

## D-003 — Day-1 control surfaces are mandatory
**Date:** 2026-05-19
**Status:** Locked

The topology UI (`02-topology-ui-prd.md`) and the `rfa` CLI (`03-rfa-cli-prd.md`) ship in Sprint 0 alongside the substrate. They are NOT deferred to Sprint 2 or later.

**Rationale:** Substrate without operational visibility repeats the past mistake (mesh-as-black-box). Forcing day-1 observability means every substrate bug is debuggable from the first commit.

---

## D-004 — Chaos-pass replaces "tests pass" as the acceptance bar
**Date:** 2026-05-19
**Status:** Locked from Sprint 1 onward

Starting in Sprint 1, every feature sprint's exit criteria includes "test suite passes under smoke chaos." Steady-state-only test passing is insufficient.

**Rationale:** The current substrate-debug tax is largely tax for bugs the chaos harness would have caught in week 1. Investment now → return forever.

---

## D-005 — No app-layer work during substrate rebuild
**Date:** 2026-05-19
**Status:** Locked for Sprints 0–2

No Kafka wire protocol, no authz, no topics, no records, no jobs, no RSQL, no WASM during the 6-week substrate-rebuild window. App-layer initiatives resume in Sprint 3+ once substrate is chaos-verified.

**Rationale:** Building app layer on uncharted substrate is what got us into this trap. Don't repeat the mistake.

---

## D-006 — `InternalMeshFrame` shape is preserved
**Date:** 2026-05-19
**Status:** Locked

The `InternalMeshFrame` shape from `_migrationv2/architecture/03-wire-and-mesh.md §3.1` is correct and unchanged. The `MeshTransport` trait abstraction means only the transport implementation swaps, not the codec layer.

**Rationale:** The frame layout (org_id mandatory, type-enforced tenant isolation, single construction site per op) is the result of multiple sprints of architectural work. It's correct.

---

## D-007 — Topology UI is itself a mesh participant
**Date:** 2026-05-19
**Status:** Locked

The `rafka-topology-ui` process joins the mesh as a view-only node (ALPN `rafka-topology-v1`) rather than polling REST endpoints on each node.

**Rationale:** Polling-based UI would need its own discovery problem. Joining as a participant means discovery, span streaming, and membership-change propagation are all automatic via the substrate. If the UI can't join, the substrate failed — useful negative signal.

---

## D-008 — UI uses no SPA framework
**Date:** 2026-05-19
**Status:** Locked

The topology UI ships as plain HTML + vanilla JS + a single graph rendering library (`vis-network` or `cytoscape.js`). No React, no Vue, no Svelte, no transpilation, no `node_modules`.

**Rationale:** KISS (Golden Principle #10). The UI is a debugging surface, not a product. If/when it becomes a product, that's its own initiative.

---

## D-009 — `rfa` CLI calls the topology-ui backend, not the mesh directly
**Date:** 2026-05-19
**Status:** Locked

`rfa` is a thin REST client targeting `rafka-topology-ui`. CLI and UI share the same backend → no drift between control surfaces. Cost: if the UI process is down, CLI doesn't work — acceptable for dev/chaos-test tool.

**Rationale:** Two independent process-mgmt implementations = guaranteed "killed via CLI but UI shows alive" bugs. Single backend prevents that class.

---

## D-010 — Sprint 0 includes 4 node types, not just 3
**Date:** 2026-05-19
**Status:** Locked

The substrate spike includes `rafka-registry` as the 4th node type from day 1, even though registry app logic doesn't exist yet. The registry binary is a bare mesh participant just like the other three.

**Rationale:** Discovering "we need a 4th node type" mid-feature-work means re-running substrate validation. Better to commit to all known node types now and validate the substrate handles them.

---

## D-011 — No customer migration during rebuild
**Date:** 2026-05-19
**Status:** Locked

Zero customers exist today. The rebuild assumes no data to migrate, no API compatibility constraints, no on-wire format compatibility with the old QUIC mesh. Greenfield substrate replacement.

**Rationale:** Confirmed by user 2026-05-19. Removes the cost argument against the pivot.

---

## D-012 — Existing "claimed done" migrations are treated as specs, not deliverables
**Date:** 2026-05-19
**Status:** Locked

`_migrationv2/architecture/` docs (00, 01, 02, 03) are the SOURCE OF TRUTH for what the system should look like. The current implementation is partially fictional ("compute migrated to mesh" — was not, per i34-phase5 discovery). Sprint 3+ implementations work against the architecture docs, not against the existing code.

**Rationale:** Auditing existing code claims costs approximately the same as rebuilding against the spec. Rebuild has the advantage of landing on a verified substrate.

---

## D-013 — `cargo clean` is not a debugging shortcut
**Date:** 2026-05-19 (carryover from prior sprints)
**Status:** Locked

Stale-rlib issues are diagnosed by `cargo clean -p <specific-crate>` if a specific crate is suspect, never by `cargo clean` workspace-wide. Workspace clean costs 10+ minutes of rebuild for no proven cause.

**Rationale:** Multiple sprints lost time to nuke-and-rebuild cycles that should have been targeted invalidations.

---

## D-014 — Single-tenancy by deployment, not by code knob
**Date:** 2026-05-19 (from mesh architecture Q1 discussion)
**Status:** Locked

Premium "dedicated" tenants get separate rafka cluster deployments — not a per-org code-level isolation knob. Default rafka deployment is multi-tenant via gossip-layer org boundary + per-org quinn quotas.

**Rationale:** Snowflake / Confluent / Datadog pattern. Code stays simple, operators choose blast radius via cluster topology.

---

## D-015 — Leaves are stateless. No leaf WAL.
**Date:** 2026-05-19 (from mesh architecture Q2 discussion)
**Status:** Locked

Per Golden Principle #3 (Single WAL), only brokers own durable storage. Leaves (when introduced for multi-region) are stateless forwarders. Producer-side buffering owns intermittent-device durability (Kafka client convention).

**Rationale:** Two WALs = two sources of truth = two recovery paths = principle violation.

---

## D-016 — Mirror tailers are `MirrorTail` JobKind (Sprint 7+)
**Date:** 2026-05-19 (from mesh architecture Q3 discussion)
**Status:** Locked

When multi-region work resumes, cross-region mirroring uses a new `MirrorTail` JobKind consumed by the compute job_tailer pattern. Zero new coordination primitives — reuses the compute REST-removal architecture.

**Rationale:** Per Golden Principle #6 (Serverless Consolidation) and #10 (KISS). Mirror tailing is "yet another compute job."

---

## D-017 — Compute exposes ZERO HTTP routes (including /health)
**Date:** 2026-05-19
**Status:** Locked

Compute is a pure mesh participant. No HTTP server, no axum dep, no `/health` endpoint. K8s relies on default restart-on-crash; mesh-level liveness is observable via gateway's existing `/ready` endpoint, which can report MESH_CONNECTIONS state.

**Rationale:** Per Golden Principle #6 (one binary, one primitive). Broker pattern (zero HTTP, zero probe) is the reference. Compute follows.

---

## D-018 — Topology UI ships per-mesh in Sprint 0, multi-mesh in Sprint 2
**Date:** 2026-05-19
**Status:** Locked

Sprint 0 UI shows a single mesh. Sprint 2 extends to multi-mesh view (panels per mesh, relay servers as tier, distinct cross-mesh edges). Multi-mesh UI is NOT deferred to Sprint N+5.

**Rationale:** Multi-mesh is the second-largest validation target of the rebuild; it needs visualization from the moment it works, not added later.

---

## D-019 — Final node naming: `gateway` / `broker` / `compute` / `registry`
**Date:** 2026-05-19
**Status:** Locked

Service names, NODE_TYPE values, and directory names are: `gateway`, `broker`, `compute`, `registry`. NOT `data-gateway`, NOT `compute-gateway`, NOT `schema`. The prefix/suffix patterns explored during sprint-05 brainstorming (data-gateway, compute-gateway, schema-gateway) are formally retired. The locked `node_type` enum (CLAUDE.md Principle #10) is `"gateway"`, `"broker"`, `"compute"`, `"registry"`.

**Rationale:** Each node's name should reflect its function. `gateway` is the only true gateway (external traffic ingress + authz termination). `broker` is storage. `compute` is a worker/engine. `registry` is a state service. Calling all of them `-gateway` would empty the term of meaning. Asymmetric naming that describes function is more honest than symmetric naming that obscures it.

---

## D-020 — Frame classification: control-plane vs data-plane
**Date:** 2026-05-19
**Status:** Locked

Every frame on the gateway↔broker (and any node-pair) iroh connection belongs to one of two classes:

- **Control-plane frames** — substrate maintenance: `Heartbeat`, `Ping`, `Pong`, `MembershipUpdate`, `LeaderElection`, future gossip signals. They exist so the mesh stays alive and aware of itself. Sprint-04's ping/pong falls in this class.
- **Data-plane frames** — application operations: `Produce`, `Fetch`, `Replicate`, `SchemaLookup`, `Admin`. They carry user-triggered work (or its scheduler-triggered cousins like replication). Anything in the locked `op_kind` enum is data-plane.

This is the industry-standard control plane / data plane split used in SDN (OpenFlow), service mesh (Istio: Pilot vs Envoy), Kubernetes (etcd/api-server vs pod traffic), and QUIC literature.

**Naming downstream:**
- Crate: `rafka-mesh-ops` (unchanged — carries both classes)
- Frame enum (target shape, future-sprint refactor): `MeshFrame { Control(ControlFrame), Data(DataFrame) }`
- Span vocabulary: `rafka.mesh.control.<event>` (e.g. `rafka.mesh.control.heartbeat`) and `rafka.mesh.data.<op>.<event>` (e.g. `rafka.mesh.data.produce.sent`). Current `rafka.mesh.frame.sent/received` becomes the catch-all parent; control/data spans nest beneath.
- Wire-pattern (D-021 below) follows the split: control frames use uni streams (fire-and-forget); data frames use bidi-multiplexed-with-correlation_id (request/response, pipelined).

**Rationale:** "Mesh frame vs data frame" is ambiguous since everything goes over the mesh — both classes share the transport. Control/data is sharper because it's about FUNCTION (what the frame is for), not VENUE (how it travels). Aligns project vocabulary with every major distributed-systems textbook and SDN reference.

**Implementation:** Doc-only lock this sprint. Type-system refactor + span-vocab update is queued for a future sprint.

---

## D-021 — Operation (data-plane) frames use bidi-multiplexed-with-correlation_id
**Date:** 2026-05-19
**Status:** Locked

Data-plane frames travel on ONE persistent bidi QUIC stream per gateway↔broker iroh connection. Each request frame is tagged with a monotonically-increasing `correlation_id: u64`; each response references the same correlation_id. Many in-flight requests are pipelined on the single stream.

Control-plane frames stay on uni streams (fire-and-forget, no correlation needed). Sprint-04's ping-pong stays as uni; that's correct for substrate health.

**Rationale:** This is the Kafka wire protocol pattern (correlation_id in every request/response header) and the gRPC bidi-stream pattern. Uni-per-request would force new stream open/close per produce call — wasteful at throughput. Bidi-multiplexed amortizes one stream across thousands of requests. The connection itself (with its mutual NodeId crypto handshake) is established ONCE in sprint-03 and held by `PeerRegistry`; per-request cost is just a stream-write, not a handshake.

**Implementation:** Type-system support + actual produce/fetch land in the protocol sprint (TBD, likely sprint-09 to sprint-12 range). Current ping-pong-over-uni stays as substrate verification.

---

## D-022 — Org↔Broker mapping is N:M; gateway is the trust boundary
**Date:** 2026-05-19
**Status:** Locked

Gateway connects to all brokers (full mesh on the gateway side). An org's data is sharded across 1..n brokers (multiple partitions/topics distributed by a hash function). A broker hosts 1..n orgs (no per-org dedicated brokers in the default deployment).

**Routing path for a client request:**
1. Client → Gateway (TLS + authz, gateway extracts identity, derives `org_id`)
2. Gateway → Registry: lookup "for (org_id, topic, partition), which broker is leader?" (cluster metadata fetch, cached)
3. Gateway → Broker: write request to bidi stream with `correlation_id` (per D-021)
4. Broker trusts gateway's `org_id` claim — no per-message re-authz (gateway is the trust boundary)
5. Broker → Gateway: response with same `correlation_id` on the same bidi stream
6. Gateway → Client: response routed by correlation_id → client-connection map

**Rationale:** Standard sharded multi-tenant pattern (Kafka partition-leader model, BigQuery slot model). The N:M mapping is required for horizontal scale: a tenant whose throughput exceeds one broker must shard; a small tenant should share a broker with others to avoid waste. Gateway-as-trust-boundary is the API-gateway / service-mesh pattern — broker stays a dumb backend, no auth code in the data path.

**Open question:** Registry-mediated metadata vs. gossiped-membership broadcast. Defer to registry sprint design.

---

## D-023a — Internal mesh wire = custom binary on raw QUIC, no gRPC/HTTP layer
**Date:** 2026-05-19
**Status:** Locked

Internal mesh traffic (gateway↔broker, broker↔broker replication, any node-pair within a cluster) uses bincode-encoded `MeshFrame` types directly on raw iroh QUIC streams. **NO gRPC, NO HTTP/2, NO tonic** layered between the application code and the QUIC transport.

**Rationale:**
- iroh exposes raw QUIC streams; layering gRPC requires writing a non-trivial HTTP/2-on-iroh adapter (hundreds of lines, fragile, no prior art in iroh ecosystem)
- gRPC framing overhead (HTTP/2 headers + gRPC length-prefix + trailers ~100 bytes/msg) exceeds payload size for substrate frames and wastes 1-2% bandwidth at produce throughput
- bincode beats protobuf on size + speed for our primitive-heavy frame structs (no field tags)
- gRPC's value (cross-language interop, reflection, service discovery, deadlines/cancellation) doesn't apply internally: we're Rust↔Rust over iroh, iroh handles discovery, QUIC stream-reset gives us cancellation, Jaeger handles debug visibility
- Pattern matches Kafka (custom binary on TCP), Pulsar, Redpanda — all replace gRPC for the same reasons in hot-path internal traffic

**What the wire actually looks like:** 32-byte trace context header (per sprint-04) + bincode-encoded `MeshFrame` (Control or Data variant per D-020) on raw uni or bidi QUIC streams (per D-021).

**External client API (gateway's client-facing surface) is a SEPARATE decision** — see D-023b (deferred). The "no gRPC" rule applies only to internal mesh.

---

## D-023b — External client API protocol
**Date:** 2026-05-19
**Status:** Deferred — to be locked when produce/fetch client SDK requirements are clearer

Current external client already speaks TCP or QUIC (pre-existing). Choice between (a) continuing the existing TCP/QUIC protocol unchanged, (b) tonic gRPC + HTTP/2 for client SDK ergonomics, (c) custom binary Kafka-wire-style remains open. Not adding gRPC at this time. Revisit when produce/fetch on the new substrate is being designed.

---

## D-024 — Sprint exit criteria: telemetry artifact set must prove the full sprint scope
**Date:** 2026-05-19
**Status:** Locked

Every sprint's exit criteria include surfaced Jaeger URLs that prove the FULL sprint deliverable — not one trace that proves one corner. The team-lead (QA) clicks each URL and verifies the trace covers the claim before reporting sprint-closed.

**What "telemetry artifact" means:** whatever spans prove the sprint actually did what its PRD said. For substrate sprints, that's mostly control-plane evidence (boot, membership/peer.connected, heartbeat, control frames like ping/pong). For later sprints (produce/fetch, replication, jobs), it'll be mostly data-plane evidence (operation frames, correlation_id round-trips, end-to-end client traces). The PLANE is incidental; the requirement is "every claim has a clickable trace that demonstrates it."

**General rule for enumerating required URLs per sprint:**
- Compute the deliverable scope (N nodes touched, M operations introduced, K state transitions, etc.)
- For each unit of scope, require one URL that exhibits it via spans
- For N-node mesh-membership sprints: N×(N-1)/2 pair URLs (every pair handshakes)
- For per-node features: N URLs (every node exhibits the new behavior)
- For cross-service operations: one unified-trace_id URL per operation pair (sender → receiver → sender ack → receiver final)
- For failure-mode sprints: one URL per failure path, showing the error span fired

**Current sprint examples (substrate era — mostly control-plane):**
- Sprint-01 (telemetry): 1 boot trace URL per service (1 node × all 6 boot spans)
- Sprint-03 (peer discovery): N×(N-1)/2 peer.connected URLs + 1 peer.disconnected URL
- Sprint-04 (frame propagation): 1 unified-trace URL per gateway↔peer pair (3 for 4-node mesh)
- Sprint-06 (registry): 6 pair URLs (4-node mesh) + 3 ping/pong unified-trace URLs + 4 heartbeat search URLs

**Future sprint examples (app-layer era — mostly data-plane):**
- Produce sprint: 1 URL per acks-mode (0/1/all) showing client→gateway→broker→ack round trip via trace_id
- Replication: 1 URL per replication-pair showing primary→follower data-flow under unified trace
- Jobs: 1 URL per job state transition (claimed/running/completed/failed) with correlation_id

**Rationale:** 2026-05-19 sprint-06 close-out surfaced ONE trace URL proving ONE of six pair-connections. Team-lead (me) forwarded it to user as proof; user clicked, found only 1/6 verifiable from that URL. Result: ambiguity about whether the sprint actually delivered. Lock prevents recurrence by making the required artifact set explicit and computable per sprint scope. Telemetry IS the verification surface (per CLAUDE.md Principle #10) — every sprint must surface enough of it.

**How team-lead enforces:** Before merging engineer's branch, compute the required URL count from the sprint's scope, ensure engineer surfaced at least that count, click each one, verify spans/tags match the claim. Insufficient artifacts → send back to engineer with explicit list of missing URLs. Never accept "the deliverable works, trust me" — the deliverable is the spans plus the URLs that surface them.

---

## D-025 — Shared `rafka-node-base` crate; binaries are thin shells
**Date:** 2026-05-19
**Status:** Locked

All node-type binaries (`gateway`, `broker`, `compute`, `registry`, and future types) share a single `crates/rafka-node-base/` crate containing the entire substrate boilerplate: identity load/mint, iroh endpoint creation, mdns + seed discovery, peer registry, accept loop, frame reader, ping sender, heartbeat, shutdown. Each `<node>/src/main.rs` shrinks to ~10 lines that pass a `node_type` string + role config to the shared runtime.

**Shape:**
```rust
// gateway/src/main.rs (and equivalents)
#[tokio::main]
async fn main() -> Result<()> {
    rafka_node_base::NodeRuntime::new("gateway")
        .with_role(Role::Gateway)
        .run()
        .await
}
```

`Role` enum carries the small per-binary capability differences (e.g., gateway sends pings, others passively respond). Role-specific extensions added as new variants without touching the shared runtime.

**Rationale:** Pre-refactor, all 4 main.rs files were ~575 lines that differed in TWO string literals: `const NODE_TYPE` and `init_telemetry(name)`. ~2300 lines of duplicated code. Sprint-06 boot-chain regression (node.ready + endpoint_created missing) existed BECAUSE 4 copies shared the same bug — fixing it required 4-file edits. New node types in future sprints would compound the duplication (replication will need broker-broker handlers; jobs will need compute-specific frame routing). Extracting now contains the blast radius for every future bug fix and feature.

**Implementation:** Engineer extracts `rafka-node-base` AS PART OF the sprint-06 regression fix (a single commit). The boot-chain regression fix lands IN the new crate, propagating to all 4 binaries by definition. Per-binary main.rs becomes the thin shell shown above. Verification: workspace gate 0/0 + the 17-URL D-024 artifact set passes for all 4 services with the full 6-op boot chain visible.

**The structural intent — thin NOW, fat LATER:**

The binaries are thin shells today (~10 lines each) because they have no application logic yet. They are NOT meant to stay thin. Each binary will grow into its role's actual responsibilities:
- `gateway/src/main.rs` → TLS termination, authz, client connection management, request routing to brokers, response correlation
- `broker/src/main.rs` → log segment storage, replication, fetch service, retention policy
- `compute/src/main.rs` → topic subscription, RSQL/WASM job execution, output emission
- `registry/src/main.rs` → schema registry, cluster metadata, partition-leader assignment

The shared `rafka-node-base` crate is the SUBSTRATE (mesh transport + control plane + telemetry plumbing) — boilerplate that's identical across all node types and doesn't belong in any individual binary's application code. The binaries SHED that boilerplate so they have room to grow into their actual role.

Today: each `main.rs` ~10 lines (substrate runtime + role).
Sprint-09+: each `main.rs` grows with `serve_clients()`, `replicate()`, `dispatch_jobs()`, etc. — code that's TRULY per-role and belongs in that binary.

**Future sprints benefit:**
- Sprint-05 `#[instrument]` retrofit (queued) — touch one file instead of four
- Sprint-08+ replication, jobs, schema features — substrate stays in node-base; app logic lives in the right binary's main.rs (or sibling modules within that binary)
- New node types (mirror tailer, etc.) get the substrate for free; their main.rs only contains what's actually new

---

## D-026 — REST/HTTP interface exists ONLY on `gateway`; all other node binaries are mesh-only
**Date:** 2026-05-19
**Status:** Locked

External REST/HTTP traffic (client produce/fetch/admin API, OAuth, /metrics, /health, anything served over TCP+HTTP) lives ONLY in the `gateway` binary. `broker`, `compute`, `registry` (and any future node binaries) communicate **exclusively** over the iroh mesh — no axum, no hyper, no listener on a port.

**The split:**
- `gateway` — external boundary: client TLS termination, authz, request routing, HTTP API surface. Owns its own port(s) + REST endpoints.
- `broker`, `compute`, `registry` — internal nodes: iroh mesh in, iroh mesh out. No HTTP server bound to any port. Health/liveness observable via mesh heartbeat span emission (`rafka.mesh.heartbeat`) consumed by gateway/registry/operator telemetry.
- `topology-ui` — observability surface, NOT a node. Has its own HTTP server (per D-007/D-008) because it's a debug UI, not a mesh participant in the storage/compute/registry sense. Allowed.

**Forbidden patterns** (rejected at review for `broker`, `compute`, `registry`, and `crates/rafka-node-base`):
- **`axum` as a Cargo.toml dependency** — gateway is the only crate allowed to depend on axum
- **`hyper`, `tonic`, `actix-web`, or any HTTP server framework** as a Cargo.toml dependency on non-gateway nodes
- `tokio::net::TcpListener::bind()` calls (or any code that opens a TCP listening socket for HTTP)
- Any `/health`, `/metrics`, `/ready`, or other HTTP endpoint route definitions
- Any sidecar HTTP listener for "admin" or "debug"
- reqwest is OK (outbound HTTP for Jaeger query API in topology-ui only; not for inter-node communication on broker/compute/registry)

**Allowed (gateway only):** axum, tower, tower-http, hyper, rustls/native-tls for TLS termination. These deps land in `gateway/Cargo.toml` when sprint-09+ adds the client-facing REST surface.

**Rationale:**
- Single trust boundary: gateway is where client identity is established and authz is enforced. If brokers had their own REST surface, the trust boundary fragments — clients could potentially talk to brokers directly, bypassing gateway authz. D-022 (gateway as trust boundary) requires brokers to trust gateway's `org_id` claims; that only works if brokers are unreachable from clients.
- Single port surface per cluster: only gateway needs port forwarding / NAT / TLS cert management. Brokers/compute/registry are NAT-traversed automatically by iroh.
- Forces telemetry-as-observability discipline: there's no `/health` endpoint to fall back on — instead the mesh heartbeat span IS the liveness signal, observable in Jaeger.
- Supersedes/strengthens D-017 (compute zero HTTP) — generalizes to all non-gateway nodes.

**Implementation:** Today's state already conforms (broker/compute/registry have no axum/HTTP). When sprint-09+ adds app logic, this rule prevents accidental drift (e.g., "let me just add a /metrics endpoint to broker for debugging").

---

## D-027 — Cluster metadata broadcast uses `iroh-gossip` (HyParView + Plumtree)
**Date:** 2026-05-19
**Status:** Locked

v1's `topology-metadata-broadcast` feature (gateway↔gateway state propagation via `__system_catalog` tail pattern) is **NOT ported forward** to v2. v2 uses the **`iroh-gossip`** crate (n0-computer first-party, currently v0.99.0, based on HyParView + Plumtree epidemic broadcast tree papers) for:

- TopologyChange broadcasts (broker reachability flips, partition reassignments — when those land in sprint-09+)
- Membership signals (node-up, node-down)
- Any future cluster-wide soft state that benefits from eventual consistency

For **durable shared state** (e.g., authoritative routing table, the v2 equivalent of v1's `TOPOLOGY_MAP` — when that lands), pair with `iroh-docs` (CRDT-replicated document store, also first-party). Gossip handles real-time deltas; docs handle "what's the current truth" with replication guarantees.

**Three guardrails (must hold when sprint-09+ implements):**

1. **Thin abstraction layer.** Wrap `iroh_gossip::Gossip` behind a `MeshGossip` trait. Implementations switch in one file if v1.0 breaks the API or we decide to swap libraries. Don't sprinkle `iroh_gossip::*` calls across the codebase.

2. **Pair with `iroh-docs` for durability.** Late-joiner catchup is not explicitly documented in iroh-gossip's API. If a node misses broadcasts while offline, recovery semantics are unclear. Use iroh-docs to store authoritative metadata snapshots; rejoining nodes pull the current document state from docs, then start consuming live deltas from gossip.

3. **POC catchup before relying on gossip for critical paths.** Before any replication or quorum logic depends on gossip delivery semantics, run a 3-node POC: kill node 2, broadcast 10 messages on nodes 1+3, restart node 2, verify node 2 sees all 10. If catchup is broken, design a snapshot+replay layer (or fall back to v1's tail pattern).

4. **Backpressure tests are mandatory** (per open issue #47 — "gossip backpressure leads to unresponsive states and/or ∞ discovery cycle"). Before iroh-gossip carries any production traffic, write these tests:
   - **Sustained high-rate broadcast:** 4-node mesh, broadcast 1000 msgs/sec for 30s, verify (a) no node becomes unresponsive, (b) heartbeat span emission continues uninterrupted, (c) message delivery latency p99 stays bounded.
   - **Burst recovery:** burst 10000 msgs in 1s, then idle, verify all nodes return to normal heartbeat cadence within 10s and no nodes stuck in "∞ discovery cycle" state (observable as flapping peer.connected/peer.disconnected spans).
   - **Slow consumer:** one node deliberately throttled (e.g. `tokio::time::sleep(100ms)` between receiver.next() calls), verify slow consumer does NOT block fast publishers and doesn't trigger upstream backpressure that takes down the whole topic.
   - All three tests gate the gossip integration merge. If any fails, build flow control at OUR `MeshGossip` trait layer (drop-on-overflow with overflow span emission, or token-bucket rate limiting per publisher) before relying on gossip for production traffic.

**Rationale:**

- iroh-gossip is first-party (same maintainers as iroh transport we already use) — no glue code, no version skew risk between substrate layers
- HyParView + Plumtree are textbook epidemic broadcast protocols (Leitão et al. 2007, peer-reviewed, well-implemented across many systems) — not novel inventions
- v1's `__system_catalog` tail pattern required building app-level pub/sub on top of a topic; iroh-gossip is purpose-built for this and removes that boilerplate
- 8 days since last release (v0.99.0 as of 2026-05-19), active maintenance, 12 releases shipped, 300 commits

**Risks accepted:**

- Pre-1.0 API — wrap behind trait to absorb breaking changes at 1.0
- Late-joiner catchup unclear — POC to verify; combine with iroh-docs as backup
- No publicly cited production deployments at scale — we'll be early adopters; risk mitigated by `MeshGossip` trait abstraction (swap to v1 pattern if needed)

**Implementation strategy — start with one topic, split later:**

- **Now (sprint-09+ initial impl):** ONE gossip topic carrying topology + health (membership signals, broker reachability flips, partition reassignment notifications). All 4 node types subscribe; everyone sees everything. Simple, no per-role gating, HyParView overhead is tolerable at 4-100 nodes.
- **Later (scale-driven):** Split into multiple topics as fanout cost grows OR per-role subscription patterns emerge (e.g., compute-only job-state topic, registry-only schema-update topic). Each topic = independent HyParView swarm.

Single-topic kickoff keeps the first integration narrow + lets us prove iroh-gossip's late-joiner catchup (Risk #3) before fragmenting state across topics.

**Implementation:** Sprint-09+ when the first cluster-metadata-broadcast use case is implemented (probably broker reachability gossip during initial produce/fetch wiring).

**Sources investigated:**
- https://docs.rs/iroh-gossip/latest/iroh_gossip/
- https://crates.io/crates/iroh-gossip
- https://github.com/n0-computer/iroh-gossip

---

## D-028 — Infrastructure topology design rules (Tier 1 only — harvested from v1 post-mortem)
**Date:** 2026-05-19
**Status:** Locked
**Source:** v1 `E:/dev/rafka/docs/plans/_topology-consolidation-amendments.md` (2026-05-14)
**Scope:** This decision covers **infrastructure topology** only — the layer that tracks which node binaries are alive in the mesh (broker/gateway/compute/registry presence). It does NOT cover **data-plane routing** (which broker owns which partition); that's a separate concern deferred to a later decision when produce/fetch lands.

v1's topology consolidation produced two structural lessons for the infra layer that v2 commits to from day 0. These aren't aspirational — they're "we already paid the cost to learn this, don't pay it again."

### Rule 1 — All peer registries keyed by `NodeId`, never by address string

v1's `GATEWAY_PEERS` was originally keyed by `String` address. From v1's amendment doc: *"this is an accident of the original implementation"*. Wave 2 was a remediation sprint specifically to rekey to `u32 node_id`.

v2 uses iroh's `NodeId` (Ed25519 pubkey, 32 bytes, stable across IP/NAT changes by design — that's iroh's whole value prop). Every peer registry, every span tag referring to a remote node, every gossip recipient list keys by `NodeId`. **No `String` address as primary key, anywhere.** Addresses are routing hints attached to a NodeId; they can change while the NodeId is forever.

Forbidden patterns (rejected at review):
- `HashMap<String, Connection>` where the String is an IP:port
- `DashMap<SocketAddr, _>` for tracking peers
- Span tags like `peer_addr` used as identifier (it's fine as an attribute alongside `peer_id`, but never as the lookup key)

**Current v2 state:** the existing `PeerRegistry: Arc<DashMap<String, Connection>>` in `rafka-node-base` uses the hex-encoded NodeId as its String key — that's effectively a NodeId key but should migrate to typed `DashMap<NodeId, Connection>` for compile-time safety. Queued as a follow-up sprint task.

### Rule 2 — Infra-topology broadcast goes through iroh-gossip on the topology/health topic

The v2 equivalent of v1's `NodeAddressAnnounce` broadcast (which v1 did via `__system_catalog` tail + per-gateway publish) is an iroh-gossip event on the single topology/health topic (per D-027 single-topic strategy) carrying:

```
NodeInfraEvent {
    kind: Joined | Left | HealthChange,
    node_id: NodeId,
    node_type: Role,
    version: String,
    timestamp: u64,
}
```

All 4 node types subscribe; each maintains its own local `SystemInfraTopology` cache built from the event stream. This replaces v1's `__system_catalog`-tail pattern entirely — iroh-gossip handles fan-out, we just consume events and update local state.

### Out of scope for D-028 (explicitly)

The following are NOT covered by this decision and will be resolved when sprint-09+ designs the data plane:

- App-level routing (which broker owns which partition for which org/topic) — v1 called this `TOPOLOGY_MAP`. v2 will design separately.
- Identifier scheme for orgs/topics (slug-hash vs ULID vs other) — depends on data-plane design.
- Replication / quorum / failover policies — data-plane concern.
- Bounded LRU caching for routing — data-plane concern.

D-028 stops at "which nodes are alive in the mesh and how do we know that."

**Rationale:** v1 burned multiple sprints on infrastructure-topology rework specifically because of the address-keying mistake and the `__system_catalog`-tail-pattern coupling. Both prevented cleanly here. Cost to adopt: zero (we're designing the layer fresh, not rebuilding existing code).

---

## D-029 — E2E test harness design rules (harvested from v1 i34-phase6 catalog)
**Date:** 2026-05-19
**Status:** Locked (load-bearing when sprint-09+ chaos harness lands; no current harness in v2)
**Source:** v1 `E:/dev/rafka/docs/plans/i34-phase6-harness-consolidation.md` (2026-05-18)
**Scope:** Test/observability infrastructure only. Data-plane test concerns out of scope until a separate decision when produce/fetch lands.

v1's `BaseTreeBuilder` accumulated 16 `.with_X()` methods + 15 env-var contracts that composed "by accident." Three concurrent failure modes (cross-mesh substrate, HLC canaries, B02 subprocess hang) traced to the same root: no compositional contract. v1 spent ~6 months fixing symptoms with 7+ bandaid commits that didn't unblock the underlying compositional problem. v2's chaos harness must not repeat this.

### Rule 1 — Test harness composition is type-state, not runtime

When v2 builds its e2e harness (sprint-09+), every composition axis (process mode, telemetry mode, mesh shape, bootstrap mode, auth mode) is a phantom-type parameter on the builder. Incompatible combinations fail to **compile**, not at runtime. Pattern:

```rust
pub struct Harness<Process, Telemetry, Mesh, Bootstrap> { ... }
impl Harness<InProcess, NoTelemetry, SingleMesh, NoBootstrap> {
    pub fn new() -> Self { ... }
}
impl<M, B> Harness<InProcess, NoTelemetry, M, B> {
    pub fn with_subprocess(self) -> Harness<Subprocess, OtlpCaptureSimple, M, B> { ... }
    // .with_subprocess() FORCES telemetry to Simple — no env-var dance
}
// `.with_subprocess().with_subprocess_three()` doesn't compile (no impl for that type-state)
```

Required: a `compile_fail` (trybuild) suite that demonstrates 5+ illegal compositions are rejected at compile time.

### Rule 2 — Env vars are SET by the harness, not READ by it

The harness owns the env-var contract for subprocess children. Test authors set test identity ONCE via a typed method (`with_test_identity(name, feature, sprint)`); the harness internally exports the env vars subprocess children read. The harness itself reads at most 2 env vars: `CARGO_TARGET_DIR` (per-worktree binary location) and the OTLP collector URL.

Forbidden: harness code that calls `std::env::var(...)` for test identity (name/feature/sprint/agent/run_id). Those values come from typed builder methods, then the harness writes them into subprocess env. v1's reading the same var in two paths caused race-condition-shaped bugs.

### Rule 3 — Telemetry mode is a type, not an env-var fork

The gateway/broker/compute/registry binaries today install `BatchSpanProcessor` unconditionally via `rafka_telemetry::init_telemetry()`. **Do not** add a runtime fork that picks Simple vs Batch based on env state. If subprocess tests need synchronous flushing, add a `RAFKA_TELEMETRY_MODE=simple` env var the production binaries read AT STARTUP — but the test author sees the choice as a typed builder state (`OtlpCaptureSimple` vs `OtlpCaptureBatch`), and the harness sets the env var when spawning the subprocess. Mode is a NON-LOCAL fact only at the binary's startup; everywhere else it's a TYPE.

### Rule 4 — No shared OnceLock<Runtime>; per-test runtime ownership

v1's `HARNESS_RUNTIME: OnceLock<Runtime>` survived across tests in the same binary, causing stale-handle bugs (second-test-in-binary saw first-test's runtime state). v2's harness must spawn a fresh tokio runtime per test instance, drop on test end. Tradeoff: marginal startup overhead. Reward: no cross-test state survival.

Forbidden grep result: `OnceLock<.*Runtime>` in `tests/` should return zero hits.

### Rule 5 — Subprocess and in-process expose the SAME interface

v1's gateway URL side-channel (subprocess can't `.gateway_url()` directly) was a hack to bridge interface drift. v2: the harness struct returned from `.build()` has fields populated during build, regardless of process mode. `BaseTree.gateway_url` is set in `.build()` after subprocess spawn returns its bound address (or via in-process publisher in the in-process case). Same field, two write paths, NO global state. Topology-ui's spawn endpoint (sprint-08) already follows this — returns `{node_name, pid}` from POST.

### Rule 6 — Binary prereq is pre-flight verified, not implicit

When a typed harness state requires real binaries (subprocess modes), the harness's `.build()` MUST verify the binaries exist on disk at the resolved `CARGO_TARGET_DIR` path BEFORE attempting to spawn. Fail-fast with a clear error pointing at the build command, not a confusing runtime spawn error.

### Rule 7 — When 3+ bandaid fixes don't unblock, the problem is compositional

v1 shipped 7 commits trying to unblock B02 substrate hang (RRN format, span flush, env var precedence, gossip timeout, broadcast filter, hash_secret blocking). None worked because the underlying problem was the lack of a synchronization contract between subprocess and in-process state — a compositional issue invisible to per-symptom fixes.

v2 discipline: if a sprint-NN engineer ships 2 fixes for the same test failure without unblocking, the team-lead STOPS the per-symptom path and forces a compositional audit of the relevant axes (process × telemetry × mesh × bootstrap). Pairs with [[feedback_telemetry_validates_at_each_layer]] (after fix N, instrument target code path + re-run + verify span fired before writing fix N+1).

### Acceptance bar for the eventual harness sprint

When v2's chaos harness sprint (sprint-09+) ships, sign-off requires ALL of:

1. Type-state phantom encoding on the harness builder (rules 1, 3)
2. Trybuild compile_fail suite with 5+ illegal compositions rejected (rule 1)
3. `std::env::var(` in test infrastructure ≤ 2 hits (rule 2)
4. Zero `OnceLock<.*Runtime>` in test infrastructure (rule 4)
5. Subprocess and in-process tests use the SAME harness struct fields, populated via different write paths (rule 5)
6. Pre-flight binary existence check before subprocess spawn (rule 6)
7. Fresh-worktree + fresh-CARGO_TARGET_DIR matrix passes the full test suite on first run, no env-var setup beyond the 2 allowed (combination of rules 2 + 4 + 6)

**Rationale:** v1 paid roughly 6 months of throughput tax on harness composition issues. The structural cause (no contract, env-var-driven mode forks, runtime caching, side-channels) is preventable at design time at near-zero cost. Locking these rules now means sprint-09+ engineers reference D-029 instead of rediscovering each lesson the hard way.

---

## Decisions still open (to be locked in future PRs)

- **D-XXX:** Choice of chaos test seed-replay tooling — write our own vs. use a library (`madsim`, `loom`, etc.)
- **D-XXX:** Choice of graph rendering lib for UI (`vis-network` vs `cytoscape.js`)
- **D-XXX:** Whether topology-ui process binary lives in `topology-ui/` (sibling to gateway/broker) or `crates/rafka-topology-ui/`
- **D-XXX:** OTLP collector deployment shape for local dev (sidecar process vs in-tree library)
- **D-XXX:** Identity persistence format for `${RAFKA_DATA_DIR}/node-identity.json` (JSON, msgpack, postcard)
