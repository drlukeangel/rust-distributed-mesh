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

## Decisions still open (to be locked in future PRs)

- **D-XXX:** Choice of chaos test seed-replay tooling — write our own vs. use a library (`madsim`, `loom`, etc.)
- **D-XXX:** Choice of graph rendering lib for UI (`vis-network` vs `cytoscape.js`)
- **D-XXX:** Whether topology-ui process binary lives in `topology-ui/` (sibling to gateway/broker) or `crates/rafka-topology-ui/`
- **D-XXX:** OTLP collector deployment shape for local dev (sidecar process vs in-tree library)
- **D-XXX:** Identity persistence format for `${RAFKA_DATA_DIR}/node-identity.json` (JSON, msgpack, postcard)
