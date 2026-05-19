# mesh-to-mesh — runbook

> **Status:** PLANNED. This runbook documents the intended failure modes for sprint-13's eventual shipment; nothing here applies until cross-mesh code exists.

## Anticipated failure modes

### Mode 1 — Bridge gateway dies; both meshes diverge silently

**Cause:** Bridge is the only node forwarding gossip; without it, mesh-A's topology changes don't reach mesh-B.

**Detection:** Both meshes' independent gossip continues, but cross-mesh `rafka.mesh.cross.*` spans stop firing.

**Recovery:** Restart bridge gateway. On boot, it re-peers both meshes; gossip catch-up via iroh-docs snapshot (per D-027 guardrail 2) re-syncs metadata.

### Mode 2 — Nodes auto-discover across meshes via mdns (should be impossible)

**Cause:** Both meshes running on same LAN; mdns is broadcast.

**Recovery:** Either (a) put meshes on different subnets, or (b) set `RAFKA_MDNS_DISABLED=true` and rely on explicit seeds only. Bridge gateway then joins via seeds, not mdns.

### Mode 3 — Cross-mesh edge in topology-ui shows but ping/pong frames don't flow

**Cause:** Cross-mesh handshake succeeds (iroh layer) but app-layer routing doesn't forward Pings across the bridge.

**Recovery:** Confirm bridge's `run_ping_sender` iterates both PeerRegistries, not just one. Sprint-13 implementation detail.

### Mode 4 — Heartbeat shows peer_count that includes cross-mesh peers (wrong)

**Cause:** Bridge gateway's heartbeat reads a unified DashMap instead of the per-mesh DashMaps.

**Recovery:** Bridge gateway emits TWO heartbeat spans per tick — one per mesh, each with its own peer_count.

## Cross-references

* Parent: cross-mesh substrate.
* PRD: cross-mesh sprint TBD (likely sprint-13).
* Sibling: [`peer-discovery runbook`](../peer-discovery/runbook.md), [`heartbeat runbook`](../heartbeat/runbook.md).
