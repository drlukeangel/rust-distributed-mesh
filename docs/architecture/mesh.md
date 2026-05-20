# Mesh Architecture

The rafka v2 mesh is the substrate every other layer rides on. Every node — gateway, broker, compute, registry, bridge — is a single binary that:

1. Holds an iroh-net `Endpoint` keyed by its `NodeId` (Ed25519 public key)
2. Discovers peers via mdns (LAN) + DERP (WAN via iroh-relay)
3. Maintains **one QUIC connection per peer pair** under a single ALPN
4. Multiplexes **two distinct planes** over that connection

This document specifies the wire contract and the plane split. For implementation details see the feature triples under `docs/features/`; for "why we made these choices" see `docs/eng/rafka-golden-principles.md`.

---

## The two planes

| Plane | Mechanism | Reliability | What rides it |
|---|---|---|---|
| **Control** | `iroh-gossip` (HyParView for membership + Plumtree for broadcast) | Internal control messages reliable-ordered (HyParView state machine requires it); application-level state digests broadcast lossily | Membership churn (join/leave), small state digests (CPU load, peer counts, mesh_id), auth deltas, routing-table updates |
| **Data** | `connection.open_bi()` (bidirectional QUIC streams) | Reliable + ordered per stream; independent flow control across streams; shared congestion control across the connection | Heavy request/response payloads (compute jobs, batch fetches, large auth pushes), anything that exceeds the safe gossip MTU or requires acknowledgement |

These ride the **same** QUIC connection, not separate connections. One ALPN = one TLS handshake per peer pair = one congestion-control context shared between control and data, which is what we want — under network saturation BBR/CUBIC throttles the connection cooperatively rather than two ALPNs fighting for the same bottleneck and inducing artificial packet loss.

Opening a new stream on an established connection is **"free"** (a local stream-ID allocation; no network handshake) because QUIC pre-grants stream quota via `MAX_STREAMS` frames. This is not 0-RTT — 0-RTT is a TLS-level connection-resumption feature; "free" is the per-stream property of an existing connection.

---

## Stream wire grammar

Every QUIC stream (bi or uni) follows the same per-stream framing:

```
stream = tag(u8) length(unsigned-varint) payload(postcard) [EOF]
```

- **tag** — 1 byte routing the stream to a handler. Lookup table below.
- **length** — LEB128 unsigned varint, the byte length of the payload that follows. Varint (not fixed `u32`) because postcard internally uses LEB128 everywhere; the framing prefix matches the format used inside the payload.
- **payload** — postcard-encoded value of the type the tag's handler expects.
- **[EOF]** — single-use streams. Sender writes the frame and calls `finish()`; receiver reads exactly `length` bytes, deserializes, drops the stream. No second message on the same stream.

The framer is property-tested. Any consumer of the wire grammar uses the same encoder/decoder from `rafka-mesh-ops::framer` — no per-handler reimplementations.

### Why postcard (not bincode, not rkyv)

Per golden principle #11:

- **postcard** for internal data **read once** (mesh frames, control messages, batch payloads). LEB128 varints give the smallest wire size; pure serde keeps domain types clean.
- **rkyv** for internal data **read many times** (WAL records replayed across consumers, cached query plans). Reserved for the future data plane and storage layer — not used in the substrate today.
- **serde_json** for **human-read external data only** (REST APIs, audit logs that operators pipe through `jq`, config files). Never the wire format.

bincode was the v2-day-one choice and was migrated out in commit `24a19ee`. The replacement (postcard) ships smaller bytes per frame and aligns with principle #11.

---

## Tag namespace

The 1-byte tag is the stream-level routing primitive. 256 values, reserved in ranges so handlers can be added without renumbering:

| Range | Class | Use |
|---|---|---|
| `0x00` | RESERVED | Sentinel / null-detection. Never assigned. |
| `0x01–0x0F` | Control plane | Pointer-gossip fetches (IHAVE/IWANT pulls), auth-state pushes, gossip-state fetches, control-plane request/response over reliable streams |
| `0x10–0x7F` | Data plane | `0x10` = legacy Ping/Pong/Hello (the substrate shipped at v2 day one). `0x11+` reserved for heavy compute, batch fetches, future RSQL query streams |
| `0x80–0xFF` | Extensions | Vendor / future / experimental. Drop on unknown tag with an `unsupported_tag` span. |

Tag selection happens **before** payload parsing — the demuxer reads exactly one byte and routes the entire stream to a handler. Each handler then reads the length varint and the postcard body. This keeps control-plane logic from leaking into data-plane modules and means a 50MB compute payload's parser is never invoked on a 64-byte pointer-pull.

---

## Pointer Gossip (oversize control deltas)

QUIC datagrams have a hard MTU ceiling around 1200 bytes (path MTU minus QUIC framing overhead). Gossip messages must respect this — anything larger silently drops at network middleboxes.

For control-plane deltas exceeding the ceiling (e.g., a broker pushing a JWT revocation list), use **Pointer Gossip** — the application-level name for Plumtree's native IHAVE/IWANT lazy-push pattern:

1. **Source** computes `hash(payload)`, caches the payload locally keyed by hash, and broadcasts a tiny `{hash, size, source_node_id}` pointer datagram over gossip.
2. **Receivers** see the pointer. If they already have the payload by hash (cache hit), done. Otherwise, **open a unidirectional QUIC stream** (tag `0x01`) to `source_node_id` carrying the hash; source responds with the payload.
3. Receiver decodes, caches, processes.

This gives sub-millisecond datagram dissemination for the routing decision ("does anyone need this?") with reliable stream fallback for the actual transfer. No fragmentation logic at the gossip layer; no oversized-datagram silent drops.

---

## QoS at scale

Two facts about QUIC stream multiplexing operators must internalize:

1. **Flow control is per-stream.** A stalled heavy-compute consumer (say its rayon queue is full) backpressures the producer on *that stream only*. A tiny pointer-pull on another stream of the same connection is unaffected. This means a slow consumer can't head-of-line block faster cousins on the same connection.

2. **Congestion control is per-connection.** If the network path itself is saturated, BBR/CUBIC throttles the connection's send rate, and all streams share the reduced capacity proportionally. They cooperate on the path's true bandwidth — they don't fight it. This is why we use **one** ALPN, not multiple — separate ALPNs would mean separate connections with independent congestion windows that compete instead of cooperate.

So: heavy compute can't HOL-block control. Network saturation throttles everything together (correct behavior). Receiver-side backpressure (full rayon queue, exhausted memory) chokes only the offending stream.

---

## What's implemented today

| Layer | Status | Notes |
|---|---|---|
| iroh-net Endpoint per node | ✅ | `crates/rafka-mesh-transport` |
| NodeId-keyed peer registry | ✅ | `crates/rafka-node-base` DashMap |
| mdns peer discovery | ✅ | iroh built-in |
| Single QUIC connection per peer pair | ✅ | iroh manages |
| Single ALPN | ✅ | `rafka-mesh-v1` (will extend per tag namespace) |
| Ping/Pong/Hello frames over QUIC streams | ✅ | `rafka-mesh-ops` — currently tagless, slated for `0x10` slot-in |
| Trace-context-embedded frame header | ✅ | 32-byte header carries W3C trace_id + span_id for cross-process span propagation (principle #7) |
| postcard wire codec | ✅ | commit `24a19ee` |
| **1-byte tag stream demux** | ⏳ Phase 1.2 | |
| **Property-tested framer crate** | ⏳ Phase 1.1 | `rafka-mesh-ops::framer` |
| **iroh-gossip wiring** | ⏳ Phase 1.3 | currently a misleading `rafka.mesh.boot.gossip_started` span over plain mdns |
| Pointer Gossip pattern | ⏳ Phase 2 | needs `0x01` handler + payload cache |
| Heavy compute data plane | ⏳ Phase 2+ | needs broker + WAL layers |
| Backpressure tests (D-027) | ⏳ Phase 1.3 gate | 1000 msg/s sustained, 10k burst, slow-consumer isolation; merge-blocking |

---

## Cross-references

- **Golden principles** — `docs/eng/rafka-golden-principles.md` (especially #2 broker design, #7 per-message observability, #11 serialization, #12 election)
- **Decisions** — `docs/plans/mesh-v1/06-decisions.md` (D-027 locks iroh-gossip + backpressure tests)
- **Frame exchange** — `docs/features/frame-exchange/` (current Ping/Pong/Hello detail)
- **Peer discovery** — `docs/features/peer-discovery/`
- **Mesh-to-mesh** — `docs/features/mesh-to-mesh/` (Hello frame, Role::Bridge, per-mesh heartbeats)
- **Chaos harness** — `docs/features/chaos-harness/` + `docs/features/chaos-timeline/` (substrate stress tests)
- **Boot chain** — `docs/features/boot-chain/`
