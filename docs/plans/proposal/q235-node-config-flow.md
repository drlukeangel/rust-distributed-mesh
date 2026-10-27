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
  the diff is computed against a stale value. Reads (`nodes.get`, query.md) answer from the Atlases:
  a submitter reading the node's config before it composes a Build (Q236) reads a stale value.

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
> and a Build is diffed against.

**node-config-update.md, the numbered sequence (lines 12–18), old:** steps 1–4 as on main (step 1:
"The caller sends `node.config.update` to the fabric-primary. A node sends its own through the
node-admin it joined through…").

**new:**

> ### A caller's update
>
> 1. The caller sends `node.config.update` to the fabric-primary, which writes the map to the node in
>    its Atlas and sends the command to the node's mesh-primary (Forward `0x1A` when not directly
>    reachable).
> 2. The mesh-primary writes the record and its Atlas.
> 3. A live node is sent `ApplyNodeConfig`; it applies the map. The command completes after `Applied`
>    or `AlreadyApplied`. A stopped node is sent nothing: the command completes after the durable row
>    write, and its next start receives the config in its hydration sync.
> 4. Emit `node.config.updated`.
>
> ### A node's own update
>
> 1. The node sends its map to its mesh-primary, owed until answered. A node-admin's own goes in
>    process to the admin it joined through.
> 2. The mesh-primary writes the record and its Atlas, and answers the node `Applied` or refuses it by
>    name. On `Applied`, the node holds the map.
> 3. The mesh-primary sends the map up to the fabric-primary, owed until acknowledged; the
>    fabric-primary writes it into its Atlas. A mesh-primary that is the fabric-primary writes both at
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

## 5. Open points for the architects

- **P1, the order of two writers.** A caller's update writes the fabric-primary's Atlas first and goes
  down. A node's own writes the mesh-primary first and goes up. If both run at once, each Atlas could
  end on a different map. Dev recommendation: the mesh-primary's row write is the one order. Each
  write returns the row's version, and an Atlas takes a map only over an older version. This is the
  Atlas's existing "nothing moves backward" rule (`atlas.rs`). A caller's update completes only once
  the mesh-primary's write returns.
- **P2, when the node holds its own map.** Luke's words were "push it up". Two options: the node
  holds the map when it sends it, or on the mesh-primary's `Applied`. Dev recommendation: on
  `Applied`, because the mesh-primary can refuse it (the config cap), and a node holding a refused map
  would disagree with its row.
- **P3, a node that restarts with its update still owed.** The owed queue is in memory (Q210's), so
  an unsent update is lost at a restart, and the node is re-hydrated with the old map. Dev
  recommendation: accept it and name it in a span at the stop (as Q210's signal-stop bound). The
  application re-sends after its start if it still wants the change.
- **P4, a node-admin's own config.** A node-admin's own update goes in process to the admin it joined
  through (today's step 1). Under the up flow, a mesh-primary's own update writes its own row and its
  Atlas, then pushes up. Dev recommendation: no special case beyond the in-process hop.
