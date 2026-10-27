# Proposal: a node's config, end to end (Q234, Q235, Q236)

**Status:** proposal, 2026-10-27, for the architects' review and Luke's yes on the page text in §4.
Rulings it builds on: Q166 (config is a flat node property, set by the create spec), Q203 (config is
never gossiped; the fabric-primary holds it in its Atlas), Q224 (one Atlas per node-admin, over the
meshes it is primary for), Q227 (no node reads the store), Q234 (the hydration sync carries the node's
own row), Q235 (a node's own update is pushed up), Q236 (a Build's config always supersedes). Code
unit: rafka-v2 #3236.

## 1. Why the data must get back up

A node's config lives in five places:
- the node's memory (`TopologyView::config`, `rdm-node/src/node.rs:1683`);
- its mesh-primary's Atlas;
- its row, written by its mesh-primary;
- the fabric-primary's Atlas;
- the Build's node entry.

Two flows read the upper copies, and each fails if a node's own change stays on the node:

- **Re-hydration.** A node that starts, restarts or rejoins takes its own row from its
  mesh-primary's Atlas with its topology (Q234). If the mesh-primary does not hold the node's latest
  map, the node comes back with an old one and its own update is undone.
- **Rebuild.** The reconciler "compares each node's entry with what the node's row records and
  applies every property that differs" (build.md §1). If the row does not hold the node's latest map,
  the diff is computed against a stale value. Reads answer from the atlases (`nodes.get`, query.md)
  or from the row itself (a node's point read, Q237): a submitter reading the node's config before it
  composes a Build (Q236) reads a stale value.

## 2. The flow

**A node's own update (internal), up:**

1. The application calls `node.config_update(map)` (the whole map, a PUT).
2. The node sends it to its mesh-primary: the node-admin it joined through. The message is owed
   until acknowledged, with backoff (the one delivery rule; as Q210's declarations).
3. The mesh-primary validates it (the config cap), writes the node's row and its Atlas, and answers
   the node `Applied`, or refuses it by name.
4. The mesh-primary pushes the map up to the fabric-primary, owed until acknowledged. The
   fabric-primary writes it into its Atlas. A mesh-primary that is the fabric-primary does steps 3 and
   4 as one.
5. The node holds the map on the mesh-primary's `Applied` (§5, P2), in its memory and on its own
   entry (Q234). No `ApplyNodeConfig` comes back.

**A caller's update (external), down, unchanged:**

1. The caller sends `node.config.update` to the fabric-primary, which writes its Atlas.
2. The fabric-primary sends the command to the node's mesh-primary, which writes the row and its
   Atlas.
3. A live node is sent `ApplyNodeConfig`. A stopped node is sent nothing and takes the map at its
   next hydration sync.

**A Build:** a node entry's `config` that differs from the row is applied as a caller's update is.
The Build's map supersedes the node's current one, whoever set it (Q236). Every write is a PUT, and
the submitter reads the node's current config (`nodes.get`) before it composes.

**Re-hydration:** `node.topology.get` from the node's own mesh-primary, at join, start and create,
carries the node's own whole row from the mesh-primary's Atlas (Q234). It never reads the store.

## 3. What changes in code (#3236)

- `RunningNode::config_update` (`rdm-node/src/node.rs:119`) sends to its mesh-primary as an owed
  declaration. Today it goes through the joined admin to the fabric-primary and waits for
  `ApplyNodeConfig`.
- The mesh-primary's handler writes the row and its Atlas, answers the node, and owes the push to
  the fabric-primary. The fabric-primary takes the push into its Atlas (`Atlas::write_config`,
  `rdm-admin/src/atlas.rs:356`).
- The join's config read moves off the store (`JoinDoor.row`, `rdm-admin/src/join.rs:353`) onto the
  mesh-primary's Atlas, with Q227's admin-reads unit.

## 4. Page text

**node-config-update.md, lines 5–9, old:**

> `node.config.update` replaces a node's application config, the whole map. It is a single call, not a
> Build. Config is a property bag the application stores on the node; RDM never reads it
> ([node properties](../node-rpc-envelope.md#node-properties)). A node's first config is its create spec's
> `config` in the Build ([build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine)), written to its row at the claim and
> carried in its first join; a later Build's different `config` is applied as this call applies a map.

**new:**

> `node.config.update` replaces a node's application config, the whole map: a PUT, never a patch. It is
> a single call, not a Build. Config is a property bag the application stores on the node; RDM never
> reads it ([node properties](../node-rpc-envelope.md#node-properties)). A node's first config is its create
> spec's `config` in the Build ([build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine)), written to its row at the claim and
> carried in its hydration sync ([node.topology.get](node-topology-get.md)); a later Build's different
> `config` is applied as a caller's update is, and supersedes the node's current map, whoever set it.
>
> The update runs one of two ways. A caller's goes down from the fabric-primary. A node's own goes up
> from the node, so its mesh-primary and the fabric-primary hold the map the node will be hydrated with
> and a Build is diffed against; every install, at the node and in each atlas, is ordered by the row's
> revision.

**node-config-update.md, the numbered sequence (lines 12–18), old:** steps 1–4 as on main (step 1:
"The caller sends `node.config.update` to the fabric-primary. A node sends its own through the
node-admin it joined through…").

**new:**

> ### A caller's update
>
> 1. The caller sends `node.config.update` to the fabric-primary, which sends the command to the
>    node's mesh-primary (Forward `0x1A` when not directly reachable).
> 2. The mesh-primary writes the record, stepping its revision, and its atlas.
> 3. A live node is sent `ApplyNodeConfig` with the map and its revision; it installs the map over an
>    older revision. The command completes after `Applied` or `AlreadyApplied`. A stopped node is sent
>    nothing: the command completes after the durable row write, and its next start receives the config
>    in its hydration sync.
> 4. The completion carries the row as written; the fabric-primary writes it into its atlas over an
>    older revision. Emit `node.config.updated`.
>
> ### A node's own update
>
> 1. The node holds the map and sends it to the mesh-primary its topology view names, owed until
>    answered and retargeted on a primary change; in process when that mesh-primary is this process.
> 2. The mesh-primary writes the record, stepping its revision, and its atlas, and answers the node
>    `Applied` or refuses it by name; either answer carries the row as written, which the node installs
>    over an older revision.
> 3. The mesh-primary owes the row to the fabric-primary, keyed by the node and its revision, until
>    acknowledged; the fabric-primary writes it into its atlas over an older revision. A successor
>    mesh-primary owes it again from the row. A mesh-primary that is the fabric-primary writes both at
>    step 2.
> 4. Emit `node.config.updated`.

**build.md §1, the `config` row (line 144), old:**

> | `config` | in place, as `node.config.update` applies a map: the row first, then the node |

**new:**

> | `config` | in place, as a caller's `node.config.update` applies a map: the row first, then the node. The Build's map supersedes the node's current one, whoever set it: every write is a PUT, and the submitter reads the node's current config (`nodes.get`) before it composes (Q236) |

**node-topology-get.md, lines 7–9, old:**

> An already-held member requests [node.topology.get](../node-rpc-envelope.md#topology-op-0x1e-rdm)
> from its OWN mesh-primary. It supplies its held source version under that schema, installs
> only complete snapshots, then resumes applicable deltas.

**new:**

> An already-held member requests [node.topology.get](../node-rpc-envelope.md#topology-op-0x1e-rdm)
> from its OWN mesh-primary. It supplies its held source version under that schema, installs
> only complete snapshots, then resumes applicable deltas. The answer also carries the node's own
> whole row from its mesh-primary's Atlas, never the store; the node holds it on its own entry (Q234).

**gossip.md §5, lines 665–667, old:**

> - topology is not in the join answer and not learned from gossip first: the joiner reads complete
>   source snapshots with [node.topology.get](lifecycles/node-topology-get.md), and later deltas, fulls
>   and top-ups (§3.3) keep them current;

**new:**

> - topology is not in the join answer and not learned from gossip first: the joiner reads complete
>   source snapshots and its own row with [node.topology.get](lifecycles/node-topology-get.md), and later
>   deltas, fulls and top-ups (§3.3) keep them current;

query.md (new, its draft with the architects) states the reads: the mesh-primary's Atlas for its own
mesh, the fabric-primary's for the rest, and the node's own whole row on its own entry.

## 5. Open points, decided (the lead, 2026-10-27; arch-1's seven findings folded in)

- **P1, order: the row's `revision`, minted by the row writer, governs every install.** `NodeRow`
  gains `revision: u64`, stepped by the mesh-primary on every write of the row (a durable format
  moved: the commit carries the marker). Every carrier of a map names it: `ApplyNodeConfig`, the
  reply to a node's own update, the push up, the down command's completion and the hydration sync.
  Every recipient (the node, both atlases, `Atlas::write_config`) installs a map only over an older
  revision and drops the rest, so a delayed reply or apply never overwrites a newer map. The
  fabric-primary no longer writes its atlas ahead of the row: a caller's update goes down first, and
  the fabric-primary's atlas takes the row as written, with its revision, from the mesh-primary's
  completion. No read ever answers a map the row has not committed.
- **P2, the node holds the map it sent at once (Q235, Luke); the reply carries the row.** `Applied`
  and a refusal by name both carry the row as written and its revision; the node installs that over
  an older revision, never a put-back from its own memory. There is no "config cap": the refusal is
  whichever the mesh-primary names (`node_config.rs`).
- **P3, an update at a restart or a crash: unsent is abandoned, committed is never lost.** An update
  still in the node's owed queue is abandoned under the signal stop's bound (Q210, Q206), named in the
  stop's outcome span when there is one; a crash promises no span. An update the row holds but the
  node never heard answered is not lost: the next start's hydration sync hands the node the row's map
  and revision. The mesh-primary's push up is an owed declaration keyed by the node and its revision
  (status_declare's model): a successor mesh-primary rebuilds it from its mesh's rows at the seat
  change and owes it again, so a crash between the row write and the push is recovered from the row
  (§43: the owner reconstructs owed work from durable evidence).
- **P4, a node-admin's own config: no special case.** The destination of every node's own update is
  the mesh-primary its topology view names, retargeted on a primary change with the rest of its
  queue (Q210); never "the admin it joined through", which after a hand-over may hold no atlas. The
  hop is in process only when that mesh-primary is this process.

- **P5, who writes the row on a node's own update: the mesh-primary, as today (the lead, 2026-10-27;
  Luke asked).** A node reads any row directly (Q237) but writes none. One writer mints the revision
  that orders every install; two writers on the row would need compare-and-swap at the store, which no
  backend has, and a retry for the loser. The row is mostly RDM's (status, primaries, incarnation,
  endpoint, the runtime handle, written from frames and receipts): a node writing it whole would
  overwrite them, and writing its one field is a PATCH. The mesh-primary's atlas is current because
  it is the writer; a direct write would still need the upward hop, after a store watch. A record the
  application owns outright, keyed by node with no RDM field, could be the node's own to write;
  nothing asks for one.

The freshness contract: on `Applied` the row and the mesh-primary's atlas hold the map; the
fabric-primary's atlas holds it once the push is acknowledged, bounded by the owed delivery, and a
read from it may lag by that. Q236's read-before-compose is not a lock: two PUTs are ordered by the
row's revision.

Words: pages say "atlas" in prose (Q224), "hydration sync" (Q234), and "PUT, never PATCH" (the Node
properties section's casing). The Topology op 0x1E reply's text, which Q234 also owes, is missing
from §4 and goes in before the landing.

Cells, RED first, before the landing: a reply and an apply delivered out of order leave the newer
revision installed; the mesh-primary gone after the row write and before the push, the successor
pushes the row; a hand-over with an update owed, delivered to the new mesh-primary; the local filter
self-only against mixed scope (query.md §2).
