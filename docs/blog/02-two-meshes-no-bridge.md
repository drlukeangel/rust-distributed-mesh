# 2 — Two meshes, no bridge

> Part 2 of *Building a multi-mesh substrate*. Getting a write across two meshes without a dedicated
> bridge node.

The earlier design had a **bridge**: a special node that joined two meshes' gossip and shuttled
awareness between them. It's the obvious first idea, and it's the wrong one — it's a bespoke piece of
mesh infrastructure, and the substrate's whole premise is *don't build mesh infrastructure, use the
library*. So the bridge had to go.

## Delete the bridge

Out went the `bridge` node type, its spawn button, its env knobs, and the whole crate. Two meshes now
run side by side with **no** node whose only job is to connect them:

![Two-mesh topology, no bridge](../sprints/sprint-12/screenshots/ui-1-topology.png)

Two swim-lanes, `mesh1` and `mesh2`, and edges drawn directly between the nodes that actually talk —
nothing in the middle.

## Cross-mesh awareness without it

Per-mesh gossip lives on a per-mesh topic (`blake3(mesh_id)`), so a `mesh1` node doesn't see `mesh2` by
default. The interim answer: the **gateway** — the node that writes across meshes — also subscribes to
the other mesh's gossip, so its directory spans both. The cache now holds every node in both meshes:

![Cache spanning both meshes](../sprints/sprint-12/screenshots/ui-5-cache-deeplink.png)

(That this view loaded from a direct `/cache` URL is also the moment the console got real tab routing —
each tab is now a linkable path.)

## The write actually lands

A simulated writer on `mesh1.gateway` resolves `mesh2.broker` from the cache and sends to it. The proof
isn't the line in the message log — it's the **receiver's counter** climbing, and the cross-mesh trace
stitching end to end:

![Cross-mesh produce trace](../sprints/sprint-12/screenshots/jaeger-trace.png)

![Messages](../sprints/sprint-12/screenshots/ui-3-messages.png)

## The catch

"The gateway subscribes to the other mesh's entire gossip" works for two meshes on one host. It does
**not** scale: with many meshes and hundreds of gateways it's an O(meshes²) firehose, and it quietly
forces the console to special-case itself as an all-mesh subscriber. We fix that properly in
[Part 4](04-a-gossip-backbone.md) with a control-plane backbone. First, though, the traces themselves
were lying to us — Part 3.
