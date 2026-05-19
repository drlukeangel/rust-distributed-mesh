# boot-chain — overview

> **Source:** Substrate feature. Every node binary on boot emits a fixed chain of OTLP spans that proves identity load, endpoint creation, ALPN registration, gossip start, and accept-loop start.

## What it is

The locked 6-span sequence every `gateway`, `broker`, `compute`, `registry` (and any future node type) emits during `NodeRuntime::run()`. Rooted at `rafka.mesh.node.ready`, the chain proves the node reached steady-state without skipping a substrate step.

## How it works

`crates/rafka-node-base/src/lib.rs::NodeRuntime::run()` constructs each span via `tracing::info_span!()` in order, instruments the corresponding async section, and lets the span close on scope exit. OTel exports each span to the collector immediately.

## Span sequence (locked vocabulary, Principle #10)

1. `rafka.mesh.node.ready` — root span, attrs: `node_id`, `node_type`, `bind_addr`, `version`
2. `rafka.mesh.boot.identity_loaded` OR `rafka.mesh.boot.identity_minted` — identity step
3. `rafka.mesh.boot.endpoint_created` — iroh endpoint binds the UDP socket
4. `rafka.mesh.boot.alpn_registered` — ALPN handler installed
5. `rafka.mesh.boot.gossip_started` — mdns discovery + gossip subsystem ticking
6. `rafka.mesh.boot.accept_loop_started` — accept loop running, ready to handshake

Total wall-clock: ~0.5–1.5ms on modern hardware. Identity mint dominates if first run.

## Invariants

1. **All 6 spans always present.** Missing any one = boot regression. Verified by D-024 telemetry artifact set.
2. **Root span is `rafka.mesh.node.ready`.** Children nest beneath it (except `identity_minted` which runs before root opens — defensible per D-025 fix).
3. **`node_id` attribute is the iroh `NodeId` hex.** Stable across IP/NAT changes (D-028).

## Cross-references

* Sibling: [`peer-discovery`](../peer-discovery/overview.md), [`heartbeat`](../heartbeat/overview.md).
* Code: `crates/rafka-node-base/src/lib.rs` (`NodeRuntime::run`).
* Decisions: D-019 (naming), D-025 (node-base extraction), D-028 (NodeId-keyed).
