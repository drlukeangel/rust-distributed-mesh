# Building a multi-mesh substrate — a build log

A five-part series on building a small, observable, multi-mesh networking substrate on top of
[iroh](https://github.com/n0-computer/iroh) — gossip for membership, a relay for cross-network
transport, and OpenTelemetry traces as the source of truth. Every claim in these posts is backed by a
screenshot of the live system (the admin console and Jaeger), captured during the sprint that shipped it.

The substrate models **named nodes in named meshes** — e.g. `mesh1.broker`, `mesh2.gateway` — and a thin
**gossiped topology cache** so any node can look up another and route to it. No partitions, no quorum,
no DHT; just the smallest thing that works, proven live.

| # | Post | What it covers |
|---|---|---|
| 1 | [Watching a node boot](01-watching-a-node-boot.md) | Telemetry-first bring-up + a live topology cache for one mesh |
| 2 | [Two meshes, no bridge](02-two-meshes-no-bridge.md) | Cross-mesh delivery without a dedicated bridge node |
| 3 | [Make the trace tell the truth](03-make-the-trace-tell-the-truth.md) | Per-mesh service names, W3C context over QUIC, and killing an observability hack |
| 4 | [A gossip backbone](04-a-gossip-backbone.md) | Cross-mesh topology + aggregate metrics without a firehose |
| 5 | [The relay is a postbox, not a peer](05-the-relay-is-a-postbox.md) | The cross-mesh relay fallback, where it lives, and why it can't read your traffic |

Images live alongside each post under `../sprints/sprint-NN/screenshots/`.
