# 1 — Watching a node boot

> Part 1 of *Building a multi-mesh substrate*. Telemetry-first node bring-up and a live topology cache,
> for a single mesh.

The rule we set on day one: **telemetry is the substrate, not a feature.** A node that does work without
leaving a trace is a bug. So before there was any "product," there was a boot-span chain and a Jaeger
instance to read it in.

## A node names itself

When you spawn a broker into `mesh1`, it doesn't get a random hex id and it doesn't get named by some
central authority. It loads (or mints) its own identity, then **self-names** from it:

```
node_name = <mesh>.<type>.<first-6-hex-of-node-id>     e.g.  mesh1.broker.239dc9
```

The mesh is required (no default — an unlabelled node fails fast), the type is what it is, and the suffix
is the first 6 hex of the node's public key, so the friendly name is eyeball-matchable to its unique id.

## The boot is a span chain

Every boot is one trace rooted at `node.ready`, with the bring-up steps as children:

![Boot trace in Jaeger](../sprints/sprint-11/screenshots/jaeger-trace.png)

`endpoint_created → alpn_registered → gossip_started → accept_loop_started`, each a few hundred
microseconds, all under one root. If a node is misbehaving, the trace tells you exactly how far it got.

## A topology cache, built from gossip

Each node broadcasts a small digest on its mesh's gossip topic, including its reachable address. Every
node accumulates those into a process-local directory — `name → {mesh, type, location}` — surfaced as a
**Cache** view. This is the thing a node consults to answer "where do I send to reach X?"

![Topology cache](../sprints/sprint-11/screenshots/ui-6-cache.png)

No second gossip system, no database — the cache *is* the gossip digests, surfaced.

## Live, in one place

The operator console renders the same node from every angle — the graph, its resource usage, the
messages it has seen — all fed by gossip and Jaeger, not by polling the node itself:

![Topology](../sprints/sprint-11/screenshots/ui-1-topology.png)

![Messages](../sprints/sprint-11/screenshots/ui-3-messages.png)

## Takeaway

Start with the trace. If the boot chain and the gossiped directory are right and visible for **one**
node in **one** mesh, you have the spine everything else hangs off. Next post: a second mesh — and
getting a write across it without inventing a bridge.
