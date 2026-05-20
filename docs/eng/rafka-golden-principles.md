# The Rafka Golden Principles

This document defines the core, unshakeable architectural tenets of the Rafka streaming engine. Every new feature, pull request, and refactor must be evaluated against these principles. If a design violates one of these rules, it is fundamentally incompatible with the Rafka philosophy.

> **v2 note:** Ported from `rafka/docs/eng/rafka-golden-principles.md`. The principles apply to v2 substrate work as well — most importantly principle #11 (Serialization) which mandates `postcard` for internal mesh RPC, and principle #7 (Per-message observability) which v2 has implemented from day one via the trace-id-embedded `InternalMeshFrame`. v2 has not yet built the broker (#2), unified WAL (#3), WASM compute (#4), or election (#12) layers — those principles are forward-looking spec.

---

## 1. The Gateway is Stateless (The Edge)
The Gateway is a protocol translator, security gatekeeper, and intelligent router. **It must never store durable consumption state, offsets, or message locks in its local RAM.**
*   **Why:** In a cloud-native deployment, Gateways scale horizontally behind load balancers. If a client disconnects from Gateway A and reconnects to Gateway B, it cannot lose its state.
*   **Implication:** All Consumer Group offsets, Share Group (Queue) message locks, and heartbeat TTLs must be routed via `rpc_node_binary` to the physical Broker. The Gateway only caches globally synchronized control-plane data (like `TOPIC_REGISTRY` and `TOPOLOGY_MAP`).

## 2. The Broker is a "Pure IO Pump" (The Brawn)
The physical Broker should be as "dumb" and fast as possible. It does not speak the Apache Kafka wire protocol, it does not parse human-readable JSON on the hot path, and it does not do complex string hashing.
*   **Why:** CPU cycles on the storage nodes are precious. They should be entirely dedicated to flushing data to NVMe drives via `io_uring` and executing org WASM binaries.
*   **Implication:** The Gateway must translate standard Kafka requests into highly compact, binary `InternalMeshFrame` packets. It must hash topic strings (e.g., using Blake3) into `u64` integers *before* transmitting them over the internal QUIC mesh.

## 3. Unified Storage: The Single WAL
Traditional Kafka creates thousands of physical `.log` files (one per partition), leading to massive random I/O thrashing. Rafka writes everything from every org sequentially into a **Single Write-Ahead Log (`SingleWal`)**.
*   **Why:** Sequential NVMe writes maximize hardware throughput.
*   **Implication:** Topics, Partitions, and Virtual Queues are just **Sparse Indexes** (`RoaringBitmaps`) layered over the `SingleWal`. Filtering data (via RSQL) does not copy bytes; it just flips bits in a bitmap.

## 4. Deterministic Compute (WCC Fuel)
Rafka allows multi-org users to upload arbitrary WebAssembly (WASM) binaries to execute inline on the hot data path. To solve the Halting Problem (infinite loops crashing the broker), **every operation must cost deterministic Gas (WCC Fuel)**.
*   **Why:** A single org cannot be allowed to starve a physical CPU core or exhaust RAM, which would cause cascading failures across the mesh.
*   **Implication:**
    *   Wasmtime must run with `consume_fuel(true)`.
    *   SIMD JSON parsing charges fuel based on depth/complexity.
    *   Tombstones (deletes) incur an "Assassin Tax" to prevent compaction DDoS attacks.
    *   Stateful aggregations (L3 Cache pinning) and JIT Re-scans on historical data incur heavy WCC penalties.

## 5. Zero-Trust at the Edge
Security is not an afterthought or a plugin. It is evaluated at the very edge of the network, before any data touches the physical storage layer.
*   **Why:** Dropping unauthenticated or unauthorized traffic at the Gateway protects the internal QUIC mesh and the physical brokers from resource exhaustion attacks.
*   **Implication:** All incoming connections require Ed25519-signed JWTs (minted by an IdP). The Gateway extracts the identity, verifies the `fuel_limit`, and evaluates RRN (Rafka Resource Name) permissions strictly via an O(1) Auth Gatekeeper.

## 6. Serverless Consolidation
Rafka is not just a "faster Kafka clone." It is a unified messaging fabric.
*   **Why:** Data engineering teams shouldn't have to manage Kafka for pub/sub, RabbitMQ/SQS for task queues, and Flink for stream processing.
*   **Implication:** Rafka natively supports **Streaming** (Consumer Groups), **Queues** (KIP-932 Share Groups), and **Compute** (WASM / RSQL Materialized Views) in a single binary. Features are composable: you can attach Queue workers to a Virtual Topic that is being actively filtered by a WASM UDF.

## 7. Per-Message Observability Is Substrate Behavior
Observability is not a bolt-on product or a library operators wire in manually. **Every record carries its trace ID in the `InternalMeshFrame` wire format**, every stage boundary emits an OpenTelemetry span, and WASM guest code emits spans that parent to host stage spans via the `rafka_otel_v1` ABI. All spans land in a Rafka topic (`__system_telemetry_traces`), queryable via the same RSQL HTTP endpoint the operator uses for user data.
*   **Why:** "Why did THIS record take 3 seconds?" is the most common operator question and the one Kafka+Flink / Kafka+Arroyo cannot answer without four-tool correlation. Making trace propagation free and automatic — part of the wire format, not a bolt-on library — changes the class of debugging questions operators can answer with a single SQL query.
*   **Implication:**
    *   `InternalMeshFrame.trace_id: u128` is a load-bearing wire field; no refactor may remove or relocate it.
    *   Every new stage boundary (broker op, compute operator, gateway route) MUST emit a span parented to the inbound record's trace_id.
    *   WASM guest UDFs cannot operate as black boxes; `rafka_otel_v1` ABI is part of the contract, and guest spans parent to host stage spans.
    *   Telemetry spans flow through the same substrate as user data — no separate trace backend, no separate query surface, no separate auth surface.
    *   Self-instrumentation suppression (F-46 D1.1) is enforced via the `FLAG_IS_SYSTEM_TELEMETRY` bitflag at payload construction, never via async context tricks that break across `tokio::spawn`.
    *   Per-message observability extends transparently to connector-sourced records, batch-query operations, compute-stage emits, and derived records.
*   **v2 status:** SHIPPED. `crates/rafka-mesh-ops/src/lib.rs::InternalMeshFrame` carries the W3C trace context in its wire header (`trace_id` 16 bytes + `span_id` 8 bytes + flags). Every peer.connected, frame.sent, frame.received, heartbeat, and chaos.primitive span propagates the parent trace context across QUIC hops via `set_parent(parent_ctx)`.

## 8. Rafka IS the Substrate; Legacy Brokers Are Migration Bridges, Not Steady-State Peers
Rafka's value proposition is unified substrate behavior — broker, compute, state, storage, and observability as ONE surface. Legacy messaging brokers (Kafka, NATS, RabbitMQ, ActiveMQ, Kinesis, MQTT, Fluvio) are systems Rafka **replaces over time**, not systems Rafka **integrates with permanently**. External state stores (Redis) and alternate storage backends (direct filesystem as a connector) compete with Rafka's native primitives and ARE rejected.
*   **Why:** Strategic positioning is load-bearing. Rafka's value depends on being the single substrate for the data plane. But forcing a big-bang cutover from existing messaging stacks is a deployment anti-pattern that makes Rafka unreachable for enterprise teams. The resolution: migration-bridge connectors (one-way, source-only, time-bound) let teams migrate incrementally while the positioning remains "Rafka replaces your legacy broker" not "Rafka co-exists with it."
*   **Implication:**
    *   **YES for migration-bridge SOURCE connectors** from legacy brokers — Kafka / NATS / RabbitMQ / ActiveMQ / Kinesis / MQTT / Fluvio. These are ONE-WAY (legacy → Rafka). Docs must explicitly frame them as migration bridges with time-bound intent: "get your data OUT of your legacy broker; downstream consumers move to Rafka first; producers migrate incrementally; legacy broker decommissions when the last producer moves; remove the connector when migration completes."
    *   **NO for steady-state SINK connectors to legacy brokers** (`kafka_sink`, `rabbitmq_sink`, etc.). Writing FROM Rafka TO a legacy broker signals ongoing bidirectional integration, the anti-pattern.
    *   **NO for Redis / external KV as state store.** Rafka's compacted-topic + WASM state ABI is the state primitive; introducing Redis would regress the architecture.
    *   **NO for filesystem / S3 as a storage backend connector.** S3 is Rafka's tiered storage substrate via `object_store`, not a pluggable storage option. (Explicit `s3_batch_sink` for batch-output materialization is different — that's egress-shaped, not storage-backend-shaped.)
    *   **YES for inbound external-source connectors** where no Rafka primitive exists (HTTP / webhook / SSE / websocket).
    *   **YES for outbound non-topic sinks** where operators need explicit egress to their systems (S3 batch Parquet sink, JDBC batch sink to Snowflake / BigQuery / ClickHouse / Postgres).
    *   Connector framework scope defined in [`plans/RafkaConnectors.md`](plans/RafkaConnectors.md) — migration-bridge tier in §6.1, steady-state rejections in §6.2-6.4.
*   **The operator test:** Would this connector signal "Rafka coexists with your legacy system"? Reject. Would it signal "Rafka replaces your legacy system on a timeline"? Adopt as a migration bridge. Would it compete with a Rafka native primitive (state, storage, observability)? Reject.

## 9. Org Extensibility Is WASM; Rafka Stdlib Is Compile-Time
Org-facing custom logic in Rafka is **WASM** — running on compute-gateway workers, fuel-metered per record, sandboxed per-org, observable via `rafka_otel_v1`. There is NO org-facing DataFusion-UDF registration API. Rafka's stdlib functions (`histogram_quantile_otlp`, etc.) are compile-time additions to the RSQL crate, not extensibility points. SQL `CREATE FUNCTION` is supported only in a bounded SQL-macro form.
*   **Why:** Extensibility surfaces multiply security review, sandboxing complexity, fuel-accounting pathways, and multi-org isolation concerns. Rafka already has ONE org-facing extensibility primitive — WASM on the compute gateway — that is fuel-metered, sandboxed, billing-integrated, and observability-instrumented. Adding a SECOND extensibility path (org UDFs registered into DataFusion's scalar-evaluation loop) means bridging two runtime contexts per row, dual fuel accounting paths, per-row Wasmtime context switching from Rayon threads, and MESI cache-line storms on N-core hardware. The cure is worse than the disease.
*   **Implication:**
    *   **YES for SQL-macro `CREATE FUNCTION`** — `CREATE FUNCTION name(args) RETURNS T AS $$ <sql-expression> $$`. Body is a single SQL expression composed from built-in functions + parameters + literals. DataFusion-native scalar function registration; ZERO new primitives. Shipping as RSQL-8 (Sprint 27). Intended for the ~80% case of named expression aliases (`CREATE FUNCTION high_priority(status) RETURNS BOOLEAN AS $$ status IN ('p0', 'p1') $$`).
    *   **NO for `CREATE FUNCTION ... LANGUAGE JS|PYTHON|RUST|WASM`** — rejected at DDL parse time with a structured error pointing to this principle. Orgs wanting custom per-record logic beyond SQL expressions deploy a Virtual Topic with WASM downstream of the data source (compacted topic / connector / RSQL-produced materialization) and query the Virtual Topic's output.
    *   **NO for procedural `CREATE FUNCTION ... AS $$ BEGIN ... END $$`** — same rejection; SQL-expression bodies only, no loops / conditionals / state / I/O.
    *   **NO for connector-contributed UDFs** — the `RafkaConnector` trait does NOT carry a `register_udfs` method. Connector-specific custom logic goes through Virtual Topics downstream per the migration-bridge / connector-framework architecture. Cross-ref: `docs/plans/RafkaConnectors.md` D-C8 (reversed 2026-04-19).
    *   **YES for Rafka stdlib additions** — new built-in functions (scalar / aggregate / window / table-valued) ship as compile-time Rust code in the RSQL crate, following the RSQL-6 pattern (`histogram_quantile_otlp`). These are operator-visible but NOT org-contributable.
*   **The custom-logic test:** Does this logic need to be a single SQL expression? → RSQL-8 `CREATE FUNCTION`. Does it need state / external I/O / procedural control flow / heavy computation? → WASM in a Virtual Topic. Does it need to run inside the DataFusion query engine with access to DataFusion internals? → Compile-time Rafka stdlib addition, not org-contributed.

## 10. KISS — Keep It Simple, Stupid
Every design decision starts from "what's the simplest thing that could possibly work?" Complexity is a tax paid every time someone reads, reviews, debugs, or extends the code. If two solutions both satisfy the requirement, the simpler one wins by default — and the burden of proof sits on the more-complex one.
*   **Why:** Compound complexity is the single largest long-term cost in any sufficiently-long-lived codebase. Every unnecessary abstraction, every speculative option, every one-story epic, every "just in case" parameter multiplies review load and hides real bugs. KISS is not a style preference; it's a survival principle for a codebase that expects to live for years.
*   **Implication:**
    *   **Planning:** prefer stories in an existing epic over new epics (`feedback_keep_hierarchy_flat.md`). One-story epics are almost always a mistake. One binary that does three things beats three binaries that share 80% of their code.
    *   **Code:** prefer the fewest moving parts that satisfy the invariant. No speculative abstractions, no "in case we need it later" parameters, no parallel type hierarchies mirroring existing ones. Reuse primitives (`feedback_reuse_primitives_dont_create.md`); don't propose new crates/modules/subsystems before grepping for what's already there.
    *   **Tests:** one clear assertion beats three hedging ones. A test that only proves "it didn't crash" is weaker than a test that proves "the exact byte-for-byte expected output appeared." Kill mutants with specific-value assertions (`docs/testing-strategy-mutants.md`).
    *   **Docs:** no rhetorical padding, no three-option framing with an obvious winner (`feedback_no_padded_options.md`), no renames of perfectly-fine names. If the doc can be shorter without losing truth, shorten it.
    *   **Config:** codify logic now, hide the surface until asked (`feedback_no_speculative_config.md`). A knob nobody needs is debt nobody asked for.
    *   **APIs:** public surfaces multiply security review, sandboxing, and compatibility obligations. A new REST route needs a first-principles justification — not "it seemed like a good shape."
*   **The simplicity test:** Before shipping any design, ask: *if a new engineer joined tomorrow and read only this file, would the shape of the solution match the shape of the problem?* If the answer requires three other docs, five abstraction layers, or a 20-line commit message to explain — simplify first, then ship.
*   **Crossreference:** quality-mantra bundle at `memory/feedback_quality_mantra.md` ("No hacks, Quality > Speed > Quantity, KISS + balanced DRY").

## 11. Serialization Rule of Thumb
When choosing a serialization format for a new subsystem, evaluate who reads it and how often. Do not add unmaintained dependencies like `bincode` to the hot path when modern, zero-copy, or human-readable alternatives exist.
*   **Why:** Deserialization is a massive source of CPU overhead and memory allocation pressure in the Tokio runtime. Optimizing the wire format and parsing cost yields 2-4x smaller bytes on the wire and 5-10x faster parsing.
*   **Implication:**
    *   **Keep `serde_json` for human-read/external data:** REST APIs, Audit logs (so SREs can pipe `__rafka_audit_log` through `jq`), Theme YAMLs, and config files.
    *   **Use `postcard` for internal data read ONCE:** Gateway↔Broker trusted mesh RPC payloads, `InternalMeshFrame` auxiliary data, control-plane events, and compacted internal topics (e.g., `__system_compiled_acls`).
    *   **Use `rkyv` for internal data read MANY TIMES:** Data that gets heavily queried, enabling zero-copy memory mapping directly from disk.
*   **v2 status:** SHIPPED. `rafka-mesh-ops::InternalMeshFrame` uses `postcard 1` with `alloc` feature. Encoding via `postcard::to_allocvec` / decode via `postcard::from_bytes`. Verified end-to-end: 14 frame.sent spans (Hello + Ping + Pong) + 3 cross-process hello_received decodes with correct peer_mesh_id + node_type extraction.

## 12. Election Is QUIC-Mesh-Native; Not Raft
Rafka does not use Raft for coordinator election or replication consensus. Election is a **QUIC-mesh-native primitive** built on the existing peer-discovery + heartbeat substrate. The coordinator role rotates between gateway peers via heartbeat broadcast over the QUIC mesh; on heartbeat-timeout the lowest-ID surviving peer claims the role with a monotonically-incremented term.
*   **Why:** Raft is a strong, well-understood consensus protocol — but it imports a state machine (vote phase, candidate state, split-vote handling, leader-driven log replication, log compaction) that Rafka does not need. The coordinator role in Rafka is a routing decision, not a Raft leader that owns a replicated log. The `SingleWal` (principle §3) is the durability layer; coordination is just "which gateway routes which partition's replication writes," and that's well-served by lex-tiebreak + monotone-term broadcast.
*   **Implication:**
    *   **NO Raft labels** in code, comments, span names, struct fields, docs, or commit messages. Don't write `protocol: "raft-style-term-monotone"`. The protocol IS the QUIC mesh — name it `quic-mesh-election` or omit the protocol field entirely.
    *   **NO log replication.** The coordinator does not own a replicated log. Coordinator state is durable per-gateway via a snapshot file (`gateway-coordinator-election.json` under each gateway's data dir) AND disseminated cross-gateway via heartbeat broadcast. There is no second log.
    *   **NO leader-driven state machine.** The coordinator is a routing decision point for replication writes (sprint-80 i27.e1) and partition-active-replica election (sprint-81 i27.e2 reuses the same election substrate with a per-partition term namespace). Followers do not replay a log from the coordinator.
    *   **NO vote phase, candidate state, or split-vote handling.** Election triggers when local heartbeat-timeout exceeds `convergence_bound_ms` (default 5s); the candidate is the deterministic lex-min surviving peer (no voting). Partition-then-heal split-brain is resolved by monotone-term: lower-term holders step down on receipt of higher-term heartbeat. The lex-min tiebreak handles equal-term collisions.
    *   **The shared snapshot file is per-gateway state recovery — NOT cross-gateway consensus.** In production, each gateway has its own data dir; the snapshot is meaningful only locally. Test harness shortcuts (sharing the file via `RAFKA_GATEWAY_ELECTION_STATE_PATH` env var across multiple gateway processes) are acceptable for tests but MUST NOT bleed into production assumptions.
    *   **Heuristic check:** if a code review surfaces the word "Raft" or names a Raft-shaped concept (log replication, term-vote, leader heartbeat-as-keepalive-only), flag it. The only Raft-LIKE behavior in Rafka is monotone-term + lex-tiebreak, but those are general election primitives, not Raft-specific.
*   **Reference:** `gateway/src/mesh/election.rs` (sprint-80 i27.e1) — `ElectionManager` + heartbeat task + `MeshControlMessage::ElectionHeartbeat` envelope. Transport layer for heartbeat delivery: `rafka-mesh-transport` (`crates/rafka-mesh-transport/`) owns the QUIC endpoint and `MeshConnections` pool that carries election heartbeats; the election decision logic stays in gateway (not i34 scope).
