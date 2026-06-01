# 8 — The bug that wasn't in the mesh

> Part 8 of *Building a multi-mesh substrate*. Two cross-mesh "the mesh is broken" scares in one session.
> Neither was the mesh. The lesson is about how you find that out without burning a day.

Two admin-ui consoles, one per mesh, cross-seeded so each shows both meshes via the backbone. It worked.
Then, after a rebuild, each console showed only *its own* mesh. Classic "what did I break?"

## Scare one: the 40-second node

Spawned nodes took ~40 seconds to appear, while *removals* were instant. The asymmetry was the clue:
removal is an event (a tombstone), but appearance waited on a node actually *joining* the gossip swarm —
and with mDNS off (it cross-contaminates every rafka on localhost), a node had no way to resolve the
*address* of a peer it only learned about through gossip. iroh fell back to a discovery lookup that
failed and retried. `join_peers` takes a node id; it needs an address from somewhere.

The fix was not to invent something — it was to *stop* inventing something. I started hand-rolling a
custom `AddressLookup`, and the reviewer stopped me: iroh already ships `MemoryLookup`, a manual address
book. The gossip digest already carries each node's `location`. So: register the location into iroh's
built-in book on the receive path. Peers resolve directly, no failed lookup, no 40s. The whole fix was
*populating an existing extension point* instead of building a parallel one.

## Scare two: NeighborUp, then NeighborDown

Convergence fixed, the cross-mesh view still broke on fresh restart. I had a tidy theory — "the address
lookup races the join" — and I wrote it into the report as the root cause. It was wrong, and my own data
said so: the failing run logged **zero** address-lookup failures. There was no failed lookup to lose to.

So I stopped theorizing and turned on `iroh_gossip=debug` against the live, broken consoles. The answer
was four lines:

```
NeighborUp(peer)     — the backbone gossip neighbor forms
NeighborDown(peer)   — 30 ms later, inside a conn{peer} span, it's torn down
```

The neighbor formed and was immediately killed. Cause: I'd seeded the two consoles **bidirectionally** —
each dials the other. That makes *two* QUIC connections between the same pair, and the connection
bookkeeping had a "supersede" step that, on seeing the second connection, called
`close(CONNECTION_CLOSE)` on the first — the one iroh-gossip was using as its backbone neighbor. The
mesh wasn't broken. My *test setup* (bidirectional seeding) tripped a latent connection-management bug.

The fix: don't force-close a superseded-but-live connection. Adopt the newest for the data plane and let
the stale one idle-time-out. Critically, this can't hurt the common case — a child only ever dials its
console (one direction, one connection, nothing to supersede). With it, bidirectional cross-seed shows
both meshes again, summaries flowing both ways.

## The actual lesson

Both scares were the same mistake waiting to happen: **trusting a plausible story over the live signal.**
The first time, a reviewer caught me reaching for a custom component iroh already had. The second time, a
refuted-by-my-own-logs root cause nearly shipped — and the cure was 30 seconds of `=debug` on the
running system, not another rebuild. The five relaunch cycles I *did* burn first taught me nothing; the
one debug log taught me everything.

When the mesh "breaks" after a change, the cheapest next move is almost never another change. It's to
make the system tell you what it's actually doing — and to write your root cause as a sentence you'd be
willing to have your own logs contradict.
