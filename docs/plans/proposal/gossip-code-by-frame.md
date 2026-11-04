# Proposal: gossip code by kind

Status: **for Luke's review. No code moves until he says yes.** Written by the lead, 2026-11-04, from
the conversation of the same day (Q374, Q375). Every claim below is read from the page, the code on
main `678f15455` or the spans of lane 4's release run at `df12ed232`; each says which.

## 1. What the page says

- **The topology is a cache on every node** (gossip.md §3, §3.3). Two channels fill it: each mesh's
  topology channel `f(FabricId, MeshId)` and the node-admin backbone `f(FabricId)` (§3.1, §4).
- **Who does what** (§3, §3.3): every node publishes its `Digest` on its own mesh channel; each
  mesh-primary publishes its mesh's full topology on the backbone and its own mesh channel; a
  receiving peer mesh-primary diffs the fulls it holds and publishes the difference on its own mesh
  channel, no loads; an ordinary node that finds a gap pulls the full fabric topology from its own
  mesh-primary with `GetTopology` (Node RPC, not gossip).
- **Four kinds** (Q374): `Digest`; the full topology; the topology difference; status, one message
  `{ type: Node | Mesh | Fabric, status }` carrying every status states.md names for that type.
- **The full-topology and difference frames lose `Mesh`** (Q375).
- **A channel is joined; a cache is not** (§5): the joiner reads its topology with
  `node.topology.get`, then joins its channels, and deltas, fulls and top-ups keep the cache current.
- **Application channels** (§8, Q362): RDM provides channels; the app's nodes join the channels its
  own code names. They carry the app's messages and fill nothing in the topology.

## 2. What the code does today (main `678f15455`)

### 2.1 One type does everything

`rdm_transport::topology::TopologyView` (topology.rs, 8,809 lines) is at once:

- the cache: `digest_view` (`DigestView`), `view`, with `StatusView` and `PrimaryView` beside it;
- the mesh channel (`mesh`), its subscribe, its rejoin, repair and refeed;
- every publisher: the `Digest` heartbeat (`publish_every`), the full topology, the difference
  (`forward`), status (`StatusPublisher`, `publish_lifecycle`);
- the receive path of every frame kind;
- the top-up (`spawn_top_up`, `top_up`);
- own config and own row (`set_config`, `set_own_row`, `set_name`);
- the application channels (`apps: Arc<gossip::Hub>`, `join_channel`).

### 2.2 The cache is created by `join`

`TopologyView::join(gossip, endpoint, fabric, mesh, mesh_id, node, clock, seeds)` is the
constructor: it creates the cache and subscribes the mesh channel in one call. `Backbone::join`
subscribes the backbone over an existing `TopologyView`.

### 2.3 Six ways to hand peers to iroh-gossip

iroh-gossip has two calls: `Gossip::subscribe(channel_id, bootstrap)` (enter a channel) and
`GossipSender::join_peers(peers)` (name more nodes on a channel already entered). RDM wraps the
second six ways:

| RDM function | adds | callers |
|---|---|---|
| `TopologyView::join_peers(peers)` | caller `"topology.join_peers"` | none in production |
| `TopologyView::join_peers_as(caller, peers)` | the caller passed | `node.mesh_join`, `admin.reenter`, `admin.session.mesh`, `admin.session.rejoin` |
| `Backbone::join_admins(admins)` | caller `"backbone.join_admins"` | admin boot; `AdminFirstStart::topology_installed` |
| the channel type's `join_peers(caller, peers)` | drops peers already asked (`rejoin`), writes the span | all of the above |
| repair and refeed (topology.rs) | `sender.join_peers` directly | the channel's own timers |
| the application-channel hub | `sender.join_peers` directly | `join_channel` on a channel it holds |

`join_peers` and `join_peers_as` are the same body with a different span caller. The same parameter
has three RDM names: `seeds` (`TopologyView::join`, `Backbone::join`, `launch.seeds`), `peers`
(`join_channel`) and iroh-gossip's `bootstrap`.

### 2.4 What is passed as the bootstrap (spans, lane 4 release run at `df12ed232`)

Estate `a_node_that_never_unblocks_is_recreated_on_the_new_provider`, 21:18,
`rdm.mesh.topology.update.via-subscribe` and `rdm.mesh.connection.update.via-join-peers`. The
`via-join-peers` span's `peers` field names only peers not asked before (2.5), so rows marked
`join_peers` show what was dialled, not what the caller named.

| time | node | channel | call | dialled | on that channel |
|---|---|---|---|---|---|
| 09.27 | mesh1.node-admin.1 (Day-0 root) | mesh1, backbone | subscribe | none | — |
| 11.48 | mesh1.node.1 | mesh1 | subscribe | mesh1.node-admin.1 | yes |
| 11.62 | mesh1.node-admin.2 | mesh1, backbone | subscribe | mesh1.node-admin.1 | yes |
| 11.65 | mesh1.node-admin.2 | mesh1 | `join_peers` `admin.session.rejoin` (named mesh1.node-admin.1, mesh1.node.1) | mesh1.node.1 | yes |
| 13.02 | mesh2.node-admin.1 | **mesh2** | subscribe | **mesh1.node-admin.1** | **no** |
| 13.02 | mesh2.node-admin.1 | backbone | subscribe | mesh1.node-admin.1 | yes |
| 14.03, 19.05, 20.05 | mesh2.node-admin.1 | **mesh2** | `topology.refeed` | **mesh1.node-admin.1** (and mesh2.node.1 from 19.05) | **no** |
| 14.56 | mesh2.node.1 | mesh2 | subscribe | mesh2.node-admin.1 | yes |
| 20.96 | mesh2.node.1 (re-created) | mesh2 | subscribe | mesh2.node-admin.1 | yes |

Where the bootstrap comes from in code:

- an ordinary node's first start: `launch.seeds`, which the launching node-admin writes as itself
  (`LaunchTemplate.seeds = vec![(key.public(), mesh_addr)]`, rdm-admin boot);
- a launched node-admin: the same `l.seeds`, handed to both `TopologyView::join` and
  `Backbone::join`;
- the Day-0 root: `cfg.seeds`, normally empty;
- a node-admin over an inherited or lost root: every birth its store records, own mesh first;
- after the topology read on a node-admin's first start (`AdminFirstStart::topology_installed`):
  every node of its own mesh it read, on the mesh channel, and every node-admin it read, on the
  backbone;
- an application channel: what the app passes.

### 2.5 Telemetry gap

`rdm.mesh.connection.update.via-join-peers` (`gossip::dial_span`) records only the fresh peers, never
the list the caller named.

### 2.6 Status in three shapes

`StatusEvent { Deleting, Stopped }` builds `NodeDeleting` / `NodeStopped`; `Frame::NodeDeleted` is
separate; `StatusScope { Mesh, Fabric }` with `StatusPublisher` builds `MeshStatus` / `FabricStatus`,
a string status, five sends one second apart, replayed on a join, heal, refeed or top-up. Node status
has none of that.

### 2.7 Names

- Wire `type`: `rdm.mesh.members`, `rdm.mesh.members.delta` for the full topology and the difference.
- `Topology` is already taken twice: rdm-admin's (query.md's observed fabric topology) and
  rdm-core's (the `GetTopology` read protocol marker).
- The span caller `admin.session.rejoin` names a first start.

### 2.8 Page against code and page against page

- `Digest` row (§3.1): `Digest { node, incarnation, endpoints with freshness, node_state, runtime,
  digest_seq, emitted_at_rafka_ms }`. The wire (`WireDigest`) carries one `endpoint_id` and one
  `transport_addr`, no freshness, and also `fabric_id`, `mesh`, `type`, `path`, `name`, `supersedes`,
  `admin_api_base`, `data_dir`, `mesh_id`, `in_flight`, `load`, `gossip`, `is_primary`,
  `is_fabric_primary`.
- §3.1: "No other channel exists." §8 (Q362) adds application channels.
- §3 (lines 108, 112, 120), §3.1 (194, 256, 332), §3.2 (498), §3.3 (577, 580), §6.1 (733), §7 (780),
  §14 (939) call the node status frames "lifecycle events"; "events" is the Build's word.

## 3. Proposed shape

- **R1. The cache is its own type.** It is created, then filled by `node.topology.get`; the receive
  rules of the four kinds install into it. No channel call creates it.
- **R2. One join per channel.** A node joins its mesh channel; a node-admin joins its mesh channel
  and the backbone as two calls; an app joins its application channels. One RDM call,
  `join(channel, peers)`, takes the caller for its span: the first call subscribes, a later one
  names more peers. `join_peers`, `join_peers_as`, `join_admins` and the direct `sender.join_peers`
  calls in repair, refeed and the hub go through it.
- **R3. One home per kind.** `Digest`, the full topology, the difference and status each in one
  place: its author, its channels, its forwarder, its dedup, its retention and the receive rule that
  installs it into the cache, as the §3.1 row states it.
- **R4. Status is one message** (Q374), `{ type: Node | Mesh | Fabric, status }`, with one publisher,
  one send rule and one recovery rule for all three types.
- **R5. Application channels on their own handle**, apart from the cache and the topology channels.
- **R6. The join span names both lists**: the peers the caller named and the peers dialled.
  Telemetry only; it lands first, on its own.

Wire and durable-format changes (R4, the Q375 renames, the wire `type` values) carry the
durable-format commit marker and come with their byte fixtures.

## 4. Open for Luke

- **O1. The bootstrap rule.** What a node passes as the bootstrap when it joins each channel. The
  code's rule today is "whoever launched me, plus whoever I hold" (2.4), and it names a mesh1
  node-admin for mesh2's channel. The lead proposed two rules (the nodes the cache holds; the
  accepting primary) and Luke refused both. No rule is written here until Luke gives one.
- **O2. The frame names** (Q375): the full topology and the difference, given the two `Topology`
  types already in the code.
- **O3. The word for the node status frames** where the page says "lifecycle event" (2.8).

## 5. Page text for Luke's yes (exact old → new)

- §3.1: "Two channels carry the topology: a Mesh's topology channel (one per MeshId) and the
  node-admin backbone (one per FabricId). No other channel exists." → "Two channels carry the
  topology: a Mesh's topology channel (one per MeshId) and the node-admin backbone (one per
  FabricId). No other channel carries the topology."
- §8: "A node joins a channel through `TopologyView::join_channel(name, peers)`
  (`rdm_transport::gossip`):" → the R5 handle's call, its name with R2.
- The `Digest` row, the "lifecycle event" lines, and the R4 and Q375 rows: after O1–O3.
