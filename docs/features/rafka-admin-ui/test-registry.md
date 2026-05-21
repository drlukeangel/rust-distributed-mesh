# rafka-v2-mesh-ui — Test Registry

12 named tests. All runnable from CLI (`rfa.exe mesh test run <name>`) or UI
(Tests tab → `run`). Reports land at `E:/tmp/rafka-tests/<name>-<seed>.json`.

| name | kind | runner | wall (typical) |
|------|------|--------|----------------|
| framer-roundtrip | functional | cargo test rafka-mesh-ops `framer::tests::round_trip` | ~2 s (cargo overhead dominates; underlying assert is <50 ms) |
| framer-truncation | functional | cargo test rafka-mesh-ops `framer::tests::truncation_detected` | ~2 s |
| traced-frame-roundtrip | functional | cargo test rafka-mesh-ops `tests::traced_frame_round_trip` | ~2 s |
| unknown-tag-rejected | functional | cargo test rafka-mesh-ops `tests::unknown_tag_fails_decode` | ~2 s |
| bi-stream-echo | functional | cargo test rafka-node-base `tests::bi_stream_echo_e2e` | ~6 s |
| backpressure-stream-flood | chaos | cargo test rafka-node-base `tests::backpressure_bi_stream_flood` | ~26 s |
| chaos-soak-9prim-1min | chaos | `rafka_chaos::soak::run_soak("1m","8s",seed)` | ~70 s (≥7 events) |
| chaos-soak-9prim-5min | chaos | `rafka_chaos::soak::run_soak("5m","10s",seed)` | ~310 s (≥25 events) |
| mesh-five-types-present | chaos | spawn 5 types via `/api/nodes/spawn`, query `/api/nodes/spawned` | ~10 s |
| remove-resilience | chaos | spawn 6 (own set), kill 3, verify OUR 3 survivors emit fresh heartbeats | ~25 s |
| gossip-swarm-forms | chaos | spawn 4, verify `rafka.mesh.gossip.received` spans exist | ~15 s |
| gossip-mesh-to-mesh | chaos | spawn in mesh-A + mesh-B, verify isolation + cross.peer_connected | ~30 s |

## What each test proves

### framer-roundtrip
**Asserts**: every `(tag, frame)` pair survives `encode → bytes → decode`
unchanged. Property-tested via proptest with 256 cases.
**Why it matters**: the entire data plane uses this framer. A regression
here corrupts EVERY message in flight.

### framer-truncation
**Asserts**: dropping the last byte of any encoded frame surfaces
`FramerError::Truncated`, not silent acceptance.
**Why it matters**: silent truncation would let half-frames into postcard
decode, which can panic or yield garbage.

### traced-frame-roundtrip
**Asserts**: `TracedFrame { ctx: TraceContext, inner }` preserves the
W3C trace_id (16 bytes) + span_id (8 bytes) + flags across the framer.
**Why it matters**: cross-process span linking depends on this. If trace
context is dropped, Jaeger sees disjoint traces and operator can't follow
a request through the mesh.

### unknown-tag-rejected
**Asserts**: a frame with tag ≠ 0x10 must NOT deserialize as `TracedFrame`.
**Why it matters**: tag namespace discipline. Future tags (chunked frames,
encrypted payloads) must not collide with the legacy frame format.

### bi-stream-echo
**Asserts**: two in-process iroh endpoints, A accepts via
`run_bi_echo_reader`, B opens a bi-stream and round-trips a payload tagged
0x11. Echo bytes equal sent bytes byte-for-byte after framer decode.
**Why it matters**: the bi-stream is the entire data plane — different
multiplexed ALPN from gossip. A break here means data frames don't move at
all.

### backpressure-stream-flood
**Asserts**: 32 concurrent bi-streams, each pushing 1 KiB payloads in a tight
loop for 10 s, yield zero errors AND ≥200 total round-trips.
**Why it matters**: proves the bi-stream plane back-pressures smoothly under
sustained burst load without OOM or stall. A hung accept loop would manifest
as `errors > 0` (read_to_end timeout) or `total < 200` (sender blocked).

### chaos-soak-9prim-1min / -5min
**Asserts**: continuous random chaos primitive injection for 1 or 5 minutes
with the 9-primitive pool: spawn/kill races, mesh joins/leaves, gossip
storms, payload mangling, peer churn, etc. Pass = 0 timeouts + 0 assertion
failures.
**Why it matters**: ambient stress test. Catches state-machine regressions
that single-event tests miss.

### mesh-five-types-present
**Asserts**: after spawning one of each known type (gateway, broker, compute,
registry, bridge), `/api/nodes/spawned` reports all five with type prefixes
present.
**Why it matters**: type enumeration parity between server and child binaries.

### remove-resilience
**Asserts**: spawn 6, kill 3, surviving 3 detect the disconnects within 15 s
(via peer_count adjustment in heartbeat spans).
**Why it matters**: HyParView's failure detector must actually fire within
a bounded time — without this, dead peers stay in the active view forever.

### gossip-swarm-forms
**Asserts**: after spawning 4 nodes and waiting briefly,
`rafka.mesh.gossip.received` spans exist in Jaeger.
**Why it matters**: control plane sanity. If gossip never emits "I received
a digest from peer X" spans, the swarm hasn't formed and the topology is
just isolated nodes.

### gossip-mesh-to-mesh
**Asserts**: nodes spawned with `RAFKA_MESH_ID=mesh-A` only gossip with each
other (separate topic_id derived from mesh_id), AND
`rafka.mesh.peer.connected` spans fire for cross-mesh QUIC connections
(via bridges).
**Why it matters**: tenant isolation guarantee. Two meshes must not leak
gossip into each other even though they share the underlying iroh QUIC
substrate.

## Baseline pass/fail — seed 42, run 2026-05-20

`rfa.exe mesh test all --seed 42` against UI on http://127.0.0.1:19106 with
bootstrap pool live. All 12/12 passed.

| name | duration | detail |
|------|----------|--------|
| framer-roundtrip | 0.5 s | proptest ok |
| framer-truncation | 0.5 s | proptest ok |
| traced-frame-roundtrip | 0.4 s | trace_id + span_id preserved |
| unknown-tag-rejected | 0.4 s | non-0x10 tags rejected |
| bi-stream-echo | 1.6 s | payload survived byte-for-byte |
| backpressure-stream-flood | 11.3 s | 32 streams, 1KiB × 10s, 0 errors, ≥200 RTs |
| chaos-soak-9prim-1min | 61.1 s | 8 events, all passed |
| chaos-soak-9prim-5min | 305.6 s | 29 events, all passed |
| mesh-five-types-present | 8.5 s | all 5 types visible |
| remove-resilience | 21.8 s | survivors detected within 15 s |
| gossip-swarm-forms | 30.2 s | 200 received digests across 4 nodes |
| gossip-mesh-to-mesh | 53.8 s | 10 cross.peer_connected spans, mesh isolation held |

**Total wall: ~9 min for the full suite.**
