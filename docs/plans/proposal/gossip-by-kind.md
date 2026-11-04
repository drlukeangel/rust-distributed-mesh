# Proposal: the gossip crate, by kind

Status: **for Luke's review; no code moves until he says yes.** The lead, 2026-11-04. It replaces
the earlier draft of this file. It builds on Q362, Q374, Q375 and Q376. Each statement about today's
code is read from main `e57da861c` and names the symbol it read.

## 1. The goal

Gossip code that someone can read top to bottom:

1. **Readable:** one file per thing gossip.md names. A reader who opens the `Digest` row of the
   §3.1 catalog finds one file that holds its author, its channel, its receive rule and nothing else.
2. **DRY:** one way to derive a channel id, one channel type, one join, one status message, one
   word for each thing. Today each of these has two to six forms (§3).
3. **Comprehensible:** the cache, the channels that fill it and the four kinds that travel on them
   are separate types, and the call order on a node is the page's order (§2.4).

Nothing here changes what a node decides. The receive rules, the ordering keys, the retention, the
election and the Build stay as the pages state them. The changes that touch the wire or what gossip
dials are marked **wire** or **dial** and wait for Luke's yes and a flame (§6).

## 2. What the pages and rulings say (the contract this code must show)

### 2.1 Three channel kinds, one identity rule

gossip.md §4, with Q376's direction and Luke's 2026-11-04 statement that the backbone is
`(FabricId, "backbone")`:

| channel | id | who joins it |
|---|---|---|
| a mesh's topology channel | `(fabric_id, mesh_id)` | every node of that mesh |
| the backbone | `(fabric_id, "backbone")` | node-admins only |
| an application channel | `(fabric_id, <the app's channel name>)` | the app's nodes its code names (Q362) |

### 2.2 Four kinds of message (Q374)

| kind | author | channels | page |
|---|---|---|---|
| `Digest` | every node, on its heartbeat | its own mesh channel | §3.1 |
| full topology of one mesh | that mesh's mesh-primary | backbone; its own mesh channel | §3.1, §3.3 |
| topology difference | a receiving peer mesh-primary, from the two fulls it holds | its own mesh channel | §3.1, §3.3 |
| status `{ type: Node \| Mesh \| Fabric, status }` | as states.md "Who sets each state" names each | as §3.1 names each type | §3.1, states.md |

Q375: the full and difference frames lose `Mesh` from their names. An application message on an
application channel is the app's (§8); RDM carries it and decides nothing on it.

### 2.3 The topology view is a cache on every node

gossip.md §3, §3.3; CLAUDE.md: "it is a view: the topology view, `DigestView`". It is filled by the
mesh channel and, on a node-admin, the backbone; a node that finds a gap tops it up from its own
mesh-primary with `GetTopology` (Node RPC).

### 2.4 The order on a node

node-start.md: the accepted join; the mesh channel joined (and the backbone, a node-admin);
`pre_node_ready`; then "obtain topology once through node.topology.get, as the final" step. gossip.md
§9: the node "joins its gossip channels before or while it reads". A channel is joined; the cache is
created, then filled.

### 2.5 The peers a join names (Q376)

`Channel.join(id, peers)`. `peers` defaults to the node's mesh-primary, which is on every channel the
node joins. The node keeps one **seed**:

- at **create**, the node-admin that creates the node passes itself into the node's launch as its
  seed;
- at every **start** and **join**, the commanding mesh-primary passes itself as the seed now, and the
  node replaces its seed with it before it joins a channel: the seed it was created with may no
  longer exist;
- so the node joins at any point without waiting for the cache to hydrate. After the hydration the
  cache also holds the mesh-primary.

A mesh's first node-admin is its mesh's first listener: gossip.md §5, "a mesh with no live listener
has its channel created again by its first listener under the mesh's own MeshId". Its seed (the
node-admin of another mesh that launched it) is a peer for the backbone and the application
channels, never for its own mesh channel (§3.6).

## 3. What the code does today

### 3.1 One crate holds two things

`rdm-transport` holds the gossip substrate and the Node RPC runtime. build.md's package table
already lists them as two packages ("the gossip substrate on Iroh ..." and "the Node RPC runtime
over Iroh ..."). The seam is clean in one direction: no Node RPC module (`client`, `server`, `pool`,
`delivery`, `connections`, `admission`, `forward`, `live`, `route`, `stream`) imports a gossip
module. The gossip modules (`topology`, `snapshot`, `receive_order`, `envelope`, `forward_path`,
`discipline`, `gossip`, `chunking`) take from the rest only `endpoint`, `delivery::Pacing` (the
top-up's `Send`), `wire`, `clock` and `load`.

### 3.2 One type does everything

`topology::TopologyView` (topology.rs, 8,809 lines, 2,750 of them tests) is at once: what holds the
cache (`digest_view`, `view`), the mesh channel (`mesh: Channel`), the `Digest` heartbeat
(`publish_every`, `heartbeat`, `publish`), the forwarder's path (`forward`), status
(`learn_statuses`, `adopt_mesh_status`, `publish_lifecycle`), the top-up (`spawn_top_up`,
`top_up`), the node's own config, tags, row and name (`set_config`, `set_tags`, `set_own_row`,
`set_name`, `held_name`) and the application channels (`apps: Arc<gossip::Hub>`, `join_channel`).
Its constructor, `TopologyView::join`, creates the cache and subscribes the mesh channel in one
call. `Backbone` (≈600 lines) repeats the pattern for the backbone and also holds the mesh-primary's
full-topology publisher (`publish`, `own_full`, `forward_fulls`) and the status sender
(`announce_statuses`, `send_status`). 37 files in rdm-node and rdm-admin use these types; 158 lines
there name `rdm_transport::topology::`, `::gossip::` or `::snapshot::`.

### 3.3 Two channel types, three id derivations

| today | where | hashes |
|---|---|---|
| `struct Channel` (private): the mesh channel and the backbone | topology.rs | — |
| `pub struct Channel`: an application channel's handle | gossip.rs | — |
| `mesh_channel(fabric, mesh_id)` | topology.rs | `"rafka-mesh-membership:{fabric}:{mesh_id}"` |
| `backbone_channel(fabric)` | topology.rs | `"rafka-fabric-backbone:{fabric}"` |
| `channel_id(fabric, kind)` | gossip.rs | postcard of `("rdm.app", fabric, kind)` |

Two of the three hide a label in a string, one of them "rafka" in RDM code and one "membership",
a retired word.

### 3.4 Six ways to hand peers to iroh-gossip

iroh-gossip has `Gossip::subscribe(channel_id, bootstrap)` and `GossipSender::join_peers(peers)`.

| RDM function | callers |
|---|---|
| `TopologyView::join(.., seeds)`, `Backbone::join(.., seeds)` (subscribe) | rdm-node first start; rdm-admin boot |
| `TopologyView::join_peers(peers)` | none in production |
| `TopologyView::join_peers_as(caller, peers)`, the same body with another span caller | `node.mesh_join`, `admin.reenter`, `admin.session.mesh`, `admin.session.rejoin` |
| `Backbone::join_admins(admins)` | admin boot; `AdminFirstStart::topology_installed` |
| repair and refeed, `sender.join_peers` directly (skipping the `rejoin` filter on purpose) | the channel's own timers |
| the application-channel hub, `sender.join_peers` directly | `join_channel` on a held channel |

One parameter has three names: `seeds`, `peers`, iroh's `bootstrap`. The span
`rdm.mesh.connection.update.via-join-peers` records only the peers not asked before, never the list
the caller named. `admin.session.rejoin` names a first start.

### 3.5 Status in three shapes

`StatusEvent { Deleting, Stopped }` builds `NodeDeleting` / `NodeStopped`; `Frame::NodeDeleted` is
built apart; `StatusScope { Mesh, Fabric }` with `StatusPublisher` builds `MeshStatus` /
`FabricStatus` with a string status, five sends one second apart (`STATUS_SENDS`, `STATUS_EVERY`)
and a replay on a join, heal, refeed or top-up. Node status has none of the sends or the replay.

### 3.6 The bootstrap a node passes (spans, lane 4 release run at `df12ed232`)

Every subscribe names the node's launcher (`LaunchTemplate.seeds = vec![(key.public(), mesh_addr)]`
in rdm-admin boot), or, on a node-admin over an inherited or lost root, every birth its store
records. A launched node-admin hands the same list to its mesh channel and the backbone. mesh2's
first node-admin subscribed mesh2's channel with mesh1.node-admin.1 (13.02), which is never on that
channel, and `topology.refeed` dialled it there again at 14.03, 19.05 and 20.05. On a start after a
stop, `node_self` holds the commanding mesh-primary's key (`commander`) and its join goes back over
that call, but the channel's `start` rejoins through the peers it held before, never the commander.

### 3.7 The rest

- two chunk methods: `chunking::pack_in_order` (items into whole frames: topology fulls, Build facts)
  and gossip.rs's `rdm.gossip.chunk` (one encoded message split into at most 16 parts: application
  messages);
- wire `type` values `rdm.mesh.members` and `rdm.mesh.members.delta` for the full and the difference;
- the §3.1 `Digest` row lists `endpoints with freshness`; the wire (`WireDigest`) has one
  `endpoint_id` and one `transport_addr` and fourteen fields the row does not list;
- `Topology` already names two other types: rdm-admin's (query.md's observed fabric topology) and
  rdm-core's (the `GetTopology` read protocol marker).

## 4. The proposed layout

### 4.1 Crates

```text
rdm-core  ←  rdm-transport (Node RPC runtime, endpoint, delivery, wire, clock, load)
                 ←  rdm-gossip (new: channels, the four kinds, the topology view)
                        ←  rdm-node  ←  rdm-admin
```

- **`rdm-gossip`** takes every gossip module out of `rdm-transport`. It depends on `rdm-core`,
  `rdm-transport` (for the five things in §3.1), `iroh` and `iroh-gossip`. Nothing in
  `rdm-transport` depends on it.
- **`rdm-transport`** keeps the Node RPC runtime and the five shared pieces. It keeps `iroh-gossip`
  as a dependency for one thing, its ALPN, which the endpoint binds (`endpoint.rs`) and the
  connection admission names (`connections.rs`).
- The direction stays one way: `core ← transport ← gossip ← node ← admin`; `client` is unchanged.
- build.md's package table changes from two `rdm-transport` rows to one `rdm-transport` row and one
  `rdm-gossip` row, in the same landing as the move (§7).

### 4.2 Modules of `rdm-gossip`

| module | holds | comes from |
|---|---|---|
| `channel` | `ChannelId::new(fabric_id, name)`, the one derivation (§2.1); `Channel`, one subscribed channel of any kind: its sender, the peers it knows, stop and start, repair, refeed, and `Channel::join(id, peers)`, the one join (§5.1) | topology.rs `Channel`, `rejoin`, `RepairSchedule`, `refeed_backoff`, `mesh_channel`, `backbone_channel`; gossip.rs `channel_id`, `Channel`, `Hub`'s subscribe, `dial_span` |
| `envelope` | the CloudEvents binding, unchanged | envelope.rs |
| `chunk` | the one way a message larger than a frame travels (O4) | chunking.rs; gossip.rs chunk assembly |
| `receive_order` | the one out-of-order check, unchanged | receive_order.rs |
| `view` | `TopologyView`, the cache: the held digests, the held projections of every mesh, the held statuses, the held primaries; `install`, the one call every receive rule and the topology read use; its readers | topology.rs `DigestView`, `View`, `HeldMembers`, `StatusView`, `PrimaryView`, `RoutingWatch`, and the readers now on `TopologyView` (`nodes_get`, `primaries`, `mesh_status`, `fabric_status`, `held_topology_of`, `installed_snapshot`, `projected_member_at`, `mesh_primary`) |
| `digest` | the `Digest` heartbeat: its publisher and its receive rule | `publish_every`, `heartbeat`, `TopologyView::publish`, the digest arm of `View::take_with`, `discipline` |
| `topology` | the full topology and the difference (names O2): the mesh-primary's full publisher, the receiving peer mesh-primary's diff, the atomic install of a full | snapshot.rs; `Backbone::publish`, `own_full`, `forward_fulls`; `TopologyView::forward`; forward_path.rs; `View::take_snapshot`, `apply_full` |
| `status` | the one status message `{ type, status }` (§5.3) | `StatusEvent`, `StatusScope`, `StatusPublisher`, `StatusReinforcement`, `StatusFact`, `publish_lifecycle`, `learn_statuses`, `adopt_mesh_status`, `Backbone::announce_statuses`, `send_status` |
| `top_up` | the `GetTopology` pull into the view, by the injected fetch it has today | `spawn_top_up`, `top_up`, `TopUpFetch`, `TopUpDone`, `desynced_sources` |
| `location` | the peer-location view the endpoint's address lookup reads | `LocationView`, `register_location`, `LocationMove`, `gossip_addr`, `learn_addresses` |
| `app` | an application channel's message delivery: the bounded queue, the loss generation, `Event` and `Received` | gossip.rs `State`, `Publisher`, `Subscriber`, `Event`, `Received`, `deliver` |
| `frame` | the `Frame` enum and its wire positions, unchanged until §5.3 and O2 | topology.rs `Frame` |

Each module carries its own tests, moved from topology.rs's three test modules.

### 4.3 What leaves gossip

- The node's own config, tags, row and name (`set_config`, `set_tags`, `set_own_row`, `set_name`,
  `held_name`, `OwnConfig`) move to rdm-node's node record. The `Digest` publisher is handed the
  name it publishes.
- `TopologyView::join_channel`, `TopologyView::join_peers`, `join_peers_as`, `Backbone::join_admins`
  and `Backbone` itself are deleted. The mesh-primary's role flags on `Backbone`
  (`is_mesh_primary`, `set_mesh_primary`, `is_fabric_primary`, `set_fabric_primary`, `hold_primary`)
  are read from the view's held primaries.

## 5. The calls a node makes

### 5.1 One join

```rust
// Luke's form (Q376): one call for every channel, mesh, backbone and application alike
let mesh = Channel::join(ChannelId::new(&fabric_id, mesh_id.as_str()), vec![seed]).await?;
```

- `Channel::join(id, peers)`: the first call with an id subscribes with `peers` as iroh-gossip's
  bootstrap; a later call with the same id hands `peers` to `join_peers` on the channel already held.
- Its span names the channel, the peers named and the peers dialled. The caller is the span it runs
  in: every call site is already inside its own named span (`node.start`, `admin.boot`, repair,
  refeed), so no caller parameter.
- One word, **peers** (gossip.md §8's word). `seeds` and `bootstrap` leave RDM's code.
- Every caller in §3.4 becomes this call. Repair, refeed and the application channels call it too.
- Where the process's one iroh-gossip handle and its set of held channels live so `Channel::join`
  can reach them is O6.

### 5.2 The seed (Q376)

- The launch carries `seed`: the creating node-admin's endpoint id and address (today's
  `LaunchTemplate.seeds`, one entry, renamed).
- `node.start` and `node.mesh.join` carry `seed`: the commanding mesh-primary's endpoint id and
  address (**wire**: a new field in rdm-core's command bodies). The node replaces its seed with it
  before its first channel join of that start.
- Every join names `vec![seed]`, except a node that is its mesh's first listener, which names no
  peer on its own mesh channel (§2.5). The launch already says when that is: it carries the
  launcher's mesh (`rdm_core::launch::Launcher { mesh, .. }`), and a launcher of another mesh is
  the fabric-primary launching a mesh's first node-admin. On a start or a join the seed is the
  commanding mesh-primary, always of the node's own mesh. No field is added for it.

### 5.3 One status message (Q374)

One frame shape, `Frame::Status { type: Node | Mesh | Fabric, subject, status, publisher, .. }`
(**wire**), and one publisher type for all three types. What differs by type today stays as the
pages state it, carried by the one frame:

| | node status | mesh and fabric status |
|---|---|---|
| order key (§3.1 receiver rules) | `topology_version`, per publisher and subject mesh | `changed_at_rafka_ms`, per publisher |
| forwarded | by peer mesh-primaries, op preserved (`forwarded_by`) | by peer mesh-primaries, unchanged |
| recovery (§3.3) | from the full topology's `in_flight` and `deleted` | replay of the held status on a join, heal, refeed or top-up |
| sends | once | five, one second apart (`STATUS_SENDS`, `STATUS_EVERY`) |
| correlation | `(build_id, operation)` | — |

So the frame keeps both order keys, `topology_version` for a node and `changed_at_rafka_ms` for
every type, and `forwarded_by`. The receive rules, the ordering and the recovery do not change; only
the shape is one. Who sets each status stays as states.md names it. Making node status send and
replay as mesh status does would be a recovery change; this proposal does not make it.

### 5.4 The start, in the page's order

```text
create the TopologyView (empty)
Channel::join(mesh channel, [seed])           (and the backbone, a node-admin)
node.mesh.joined; the Digest heartbeat starts (node-mesh-join.md step 9: "The node's first Digest heartbeat follows.")
pre_node_ready                               (the app joins its application channels: Channel::join)
node.topology.get → view.install(...)        (the final step)
```

## 6. What changes the wire or what gossip dials

| change | wire | dial | needs |
|---|---|---|---|
| the crate move (§4.1, §4.2) | no | no | fast gate |
| one join, one span (§5.1) | no | no | fast gate |
| the join span names both lists | no | no | fast gate; lands first |
| one channel-id derivation (§2.1) | **yes**: every channel id changes; a node computing the old id never meets one computing the new | no | Luke's yes; flame; a fabric runs one RDM version |
| the seed on start and join (§5.2) | **yes**: a new field in the start and join bodies | **yes** | Luke's yes; flame |
| the first listener names no peer on its mesh channel (§5.2) | no | **yes** | Luke's yes; flame |
| dropping `rejoin`'s filter (O3) | no | **yes** | Luke's yes; flame |
| one status message (§5.3) | **yes** | no | Luke's yes; flame; byte fixtures |
| the renames (O2) | **yes** (wire `type` values) | no | Luke's yes; byte fixtures |

Every wire change carries the durable-format commit marker and its byte fixtures. iroh, iroh-base
and iroh-gossip are not touched.

## 7. Order of landings

1. The join span names the peers named and the peers dialled (telemetry only).
2. `rdm-gossip` created; the modules of §4.2 moved as they are; every caller in rdm-node, rdm-admin,
   rdm-client and rdm-test imports from it; build.md's package table in the same landing. No
   re-export from `rdm-transport`: callers change in the same landing.
3. `TopologyView` becomes the cache alone; the own record moves to rdm-node; `TopologyView::join`
   is gone and the start runs in §5.4's order.
4. `Channel`, `Channel::join` and `ChannelId`; the six wrappers and the second `Channel` type deleted.
5. The seed (§5.2), with its flame.
6. One status message (§5.3), with its flame.
7. The renames (O2), the one channel-id derivation and the chunk method (O4), each with its flame.
8. gossip.md, build.md and node-start.md text for each landing goes in that landing (§8).

Steps 2–4 change no behaviour; the fast gate is their check. Steps 5–7 are gossip changes and each
waits for Luke's yes and a flame.

## 8. Page text for Luke's yes (exact old → new)

- gossip.md §3.1: "Two channels carry the topology: a Mesh's topology channel (one per MeshId) and
  the node-admin backbone (one per FabricId). No other channel exists." → "Two channels carry the
  topology: a Mesh's topology channel (one per MeshId) and the node-admin backbone (one per
  FabricId). No other channel carries the topology."
- gossip.md §4: "mesh topology channel = f(logical FabricId, MeshId) / backbone channel =
  f(logical FabricId) / app channel = f(logical FabricId, channel name)" → "channel id =
  f(logical FabricId, name): a mesh's topology channel is named by its MeshId, the backbone is
  named `backbone`, an application channel by the name the app gives it".
- gossip.md §8: "A node joins a channel through `TopologyView::join_channel(name, peers)`
  (`rdm_transport::gossip`):" → "A node joins a channel through `Channel::join(id, peers)`
  (`rdm_gossip::channel`), the one join every channel uses:".
- build.md package table: the second `rdm-transport` row ("the gossip substrate on Iroh: ...") →
  "`rdm-gossip` | the gossip substrate on Iroh: channels, the topology view, the `Digest`, the full
  topology and its difference, status, and the gossip frame codec".
- The `Digest` row, the status rows and the frame names: with O2 and §5.3, after Luke's yes on them.

## 9. Open for Luke

- **O1.** `rdm-gossip` as its own crate (§4.1), or the same modules as files inside `rdm-transport`.
  Dev recommendation: its own crate; build.md already names it as its own package and no Node RPC
  module reads it.
- **O2.** The names of the full and difference frames (Q375), given rdm-admin's and rdm-core's
  `Topology`.
- **O3.** `rejoin`'s filter: today a join hands iroh-gossip only peers not asked before, and refeed
  skips the filter on purpose. One join either always hands every named peer (the filter goes;
  **dial**), or keeps the filter and refeed re-asks through it. Dev recommendation: the filter goes.
- **O4.** One chunk method for every message larger than a frame (the topology full's
  `snapshot_id` / `chunk_index` / `chunk_count` fields would give way to the `rdm.gossip.chunk`
  frames; **wire**), or the two kept. Dev recommendation: one, at step 7.
- **O5.** The word in gossip.md for `NodeStopped`, `NodeDeleting` and `NodeDeleted` where the page
  says "lifecycle event" (§3 lines 108, 112, 120; §3.1 194, 256, 332; §3.2 498; §3.3 577, 580; §6.1
  733; §7 780; §14 939), now that they are status (§5.3).
- **O6.** Where the process's one iroh-gossip handle and its held channels live so that
  `Channel::join(id, peers)` reaches them: (a) the node runtime holds them and hands them to
  `Channel::join` as a parameter; (b) one per process inside `rdm-gossip`, set when the node binds
  its endpoint. Dev recommendation: (b): a process has one iroh-gossip, and (b) keeps the call in Luke's form with no added parameter.
