# frame-exchange — overview

> **Source:** Substrate feature. Demonstrates substrate-level messaging via ping/pong; trace_id propagates cross-service so the full round-trip is a single trace.

## What it is

The minimum-viable data-plane proof: gateway sends `InternalMeshFrame::Ping` every 10s over a uni stream to each peer; receivers decode, emit `frame.received{frame_kind=ping}`, reply with `Pong` via their own uni stream. Gateway receives the Pong, emits `frame.received{frame_kind=pong}`.

W3C trace context is embedded in the frame envelope (32 bytes: 16-byte trace_id + 8-byte span_id + 1-byte flags + 7-byte padding) so the receiver's span can `set_parent(extracted_ctx)` — the resulting Jaeger trace is one unified waterfall across both services.

## How it works

`crates/rafka-mesh-ops/src/lib.rs` defines `InternalMeshFrame { Ping { org_id }, Pong { org_id } }` with bincode encoding + `encode_with_context(&ctx)` / `decode_with_context(bytes)` helpers that prepend/extract the W3C context bytes.

`crates/rafka-node-base/src/lib.rs::run_ping_sender` (Gateway role only) ticks every 10s, opens a uni stream per peer, encodes Ping with current OTel context, writes + closes.

`run_frame_reader` (all roles) loops on `conn.accept_uni()`, reads the bytes, calls `decode_with_context`, creates a `rafka.mesh.frame.received` span with `set_parent(extracted_ctx)`, then for Ping replies with Pong (same trace context).

## Locked spans

- `rafka.mesh.frame.sent{node_id, peer_id, frame_kind, org_id, otel.kind="producer"}` — sender side
- `rafka.mesh.frame.received{node_id, peer_id, frame_kind, org_id, otel.kind="consumer"}` — receiver side, parented via extracted context
- `rafka.mesh.frame.sent_failed{node_id, peer_id, frame_kind, error, otel.kind="producer"}` — open_uni/write/finish error paths
- `rafka.mesh.frame.decode_failed{node_id, peer_id, error, byte_len, otel.kind="consumer"}` — bincode decode error (orphan trace, no parent extractable)

## Invariants

1. **One trace_id per round trip.** Gateway→Broker→Broker→Gateway = 4 spans, 2 services, single trace_id.
2. **Producer/consumer kinds only.** Per D-020, frames are async messages (not RPC); never `client`/`server`.
3. **Frame envelope size is fixed at 32 bytes + bincode payload.** Changing the envelope is a wire-format break.

## Cross-references

* Sibling: [`peer-discovery`](../peer-discovery/overview.md), [`cross-service-tracing`](../cross-service-tracing/overview.md).
* Code: `crates/rafka-mesh-ops/src/lib.rs`, `crates/rafka-node-base/src/lib.rs::{run_ping_sender, run_frame_reader}`.
* Decisions: D-020 (control/data plane split), D-021 (bidi+correlation_id for future ops).
