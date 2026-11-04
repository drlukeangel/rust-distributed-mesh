# Proposal: the gossip crate, by kind

Status: **approved by Luke 2026-11-04** ("go"; Q374, Q375, Q376). The lead. This version folds in the
red team's review of `4a6419cdd` (8 HIGH, 6 MEDIUM, 5 LOW) and decides O1–O6. The decisions and their
reasons are in §9 and in `docs/plans/lead-decisions-2026-11-04.md`. Statements about today's code
are read from main `c44cdacc4`.

## 1. The goal

Gossip code that someone can read top to bottom:

1. **Readable:** one module per thing gossip.md names. A reader who opens the `Digest` row of the
   §3.1 catalog finds one module with its author, its channel, its receive rule and nothing else.
2. **DRY:** one channel type, one join, one channel-id type, one status type, one word for each
   thing. Today each of these has two to seven forms (§3).
3. **Comprehensible:** the cache, the channels that fill it and the four kinds that travel on them
   are separate types, and a node calls them in the page's order (§2.4).

**What does not change: the wire, the receive rules, the ordering keys, the retention, the election
and the Build.** build-deployment.md §3 makes mixed executables the normal upgrade path: a Build
replaces nodes one at a time (Q378) and old nodes keep running. So a change to a channel id or to a
frame's wire shape would split a mesh during its own upgrade (§9 D1). Every channel id, every frame
and every wire `type` value stays byte for byte as it is. The refactor changes where code lives and
what it is called. It makes one change to what gossip dials, the seed (§5.2), and that change is
marked **dial**.

## 2. What the pages and rulings say

### 2.1 Three channel kinds

gossip.md §4: a mesh's topology channel `f(FabricId, MeshId)`, joined by every node of that mesh; the
backbone `f(FabricId)`, joined by node-admins only; an application channel `f(FabricId, channel
name)`, joined by the app's nodes its code names (Q362).

### 2.2 Four kinds of message (Q374)

| kind | author (gossip.md §3.1) | channels |
|---|---|---|
| `Digest` | every node, on its heartbeat | its own mesh channel |
| the full topology of one mesh | that mesh's mesh-primary | backbone; its own mesh channel |
| the topology delta | a receiving peer mesh-primary from the two fulls it holds; and the source mesh-primary for its own mesh, on its own mesh channel | its own mesh channel |
| status `{ type: Node \| Mesh \| Fabric, status }` | as the §3.1 rows name each frame (`NodeStopped`: the mesh-primary; `NodeDeleting`, `NodeDeleted`: the mesh executor; `MeshStatus`: the mesh-primary; `FabricStatus`: the fabric-primary) | as the §3.1 rows name each |

Q375: the full and delta frames lose `Mesh` from their names. An application message on an
application channel is the app's (§8); RDM carries it and decides nothing on it.

### 2.3 The topology view is a cache on every node

gossip.md §3, §3.3; CLAUDE.md: "it is a view: the topology view, `DigestView`". The mesh channel
fills it, and on a node-admin so does the backbone. A node that finds a gap tops it up from its own
mesh-primary with `GetTopology` (Node RPC).

### 2.4 The order on a node

node-start.md and node-mesh-join.md:
1. the accepted join;
2. the mesh channel is joined (and the backbone, a node-admin);
3. `node.mesh.joined`, after which "the node's first `Digest` heartbeat follows" (node-mesh-join.md
   step 9);
4. `pre_node_ready`;
5. "obtain topology once through node.topology.get, as the final" step.

gossip.md §9: the node "joins its gossip channels before or while it reads". A channel is joined;
the cache is created first and filled later.

### 2.5 The peers a join names (Q376)

`Channel.join(id, peers)`. `peers` defaults to the node's mesh-primary, which is on every channel the
node joins:

- at **create**, the node-admin that creates the node passes itself into the node's launch as its
  seed;
- at every **start** and **join**, the commanding mesh-primary passes itself as the seed now, because
  the seed the node was created with may no longer exist. The node joins at any point without waiting
  for the cache to hydrate.

The commander of a node-admin's start or join can be the fabric-primary, of another mesh
(node-start.md, node-mesh-join.md). A mesh with no live listener has its channel created again by its
first listener (gossip.md §5).

## 3. What the code does today

### 3.1 One crate holds two things

`rdm-transport` holds the gossip substrate and the Node RPC runtime. build.md's package table lists
both as `rdm-transport` rows. No Node RPC module (`client`, `server`, `pool`, `delivery`,
`connections`, `admission`, `forward`, `live`, `route`, `stream`) imports a gossip module.

The gossip modules (`topology`, `snapshot`, `receive_order`, `envelope`, `forward_path`,
`discipline`, `gossip`) take from the rest:
- `endpoint`, `delivery::Pacing` (the top-up's `Send`), `wire` (`wire::take` is `pub(crate)`),
  `clock` and `load`;
- `chunking::pack_in_order`, which rdm-client's `read_frames` also uses (Q285);
- the trace-context helpers `set_remote_parent`, `current_traceparent` and `current_tracestate`;
- `test_span_capture` (`pub(crate)`) in tests.

`testkit-gossip` gates both gossip code (gossip.rs) and Node RPC code (`pool.rs`, `client.rs`).
23 of rdm-transport's 50 integration test files use gossip types, and rafka-v2 imports these paths
at its pin.

### 3.2 One type does everything

`topology::TopologyView` lives in topology.rs (8,809 lines). The tests in that file are two test
modules and one `cfg(test)` function. `TopologyView` is all of these at once:
- what holds the cache (`digest_view`, `view`);
- the mesh channel (`mesh: Channel`);
- the `Digest` heartbeat (`publish_every`, `heartbeat`, `publish`);
- the forwarder's path (`forward`);
- status (`learn_statuses`, `adopt_mesh_status`, `publish_lifecycle`);
- the top-up (`spawn_top_up`, `top_up`);
- the node's own config, tags, row and name (`set_config`, `set_tags`, `set_own_row`, `set_name`,
  `held_name`, `OwnConfig`);
- the application channels (`apps: Arc<gossip::Hub>`, `join_channel`).

Its constructor, `TopologyView::join`, creates the cache and subscribes the mesh channel in one call.

`Backbone` (≈600 lines) holds the backbone channel and the node-admin's publishing role:
- `is_mesh_primary`, `set_mesh_primary`, `is_fabric_primary`, `set_fabric_primary`, `hold_primary`;
- `published_as_primary`, `until_published_as_primary`;
- `withhold_announcements`, `publication_wake`;
- the full publisher (`publish`, `own_full`, `forward_fulls`);
- status (`announce_statuses`, `send_status`, `publish_status_event`).

Its role flags follow the election (`set_mesh_primary` from boot's election watch), and a commanded
join drops them before it joins (Q335).

### 3.3 Two channel types, three channel-id functions

| today | where | what it hashes |
|---|---|---|
| `struct Channel` (private): the mesh channel and the backbone; `Channel::join` takes 14 inputs (`on_frame`, `order`, `targets`, `replay`, `location_view`, ...) | topology.rs | — |
| `pub struct Channel`: an application channel's handle | gossip.rs | — |
| `mesh_channel(fabric, mesh_id)` | topology.rs | `"rafka-mesh-membership:{fabric}:{mesh_id}"` |
| `backbone_channel(fabric)` | topology.rs | `"rafka-fabric-backbone:{fabric}"` |
| `channel_id(fabric, kind)` | gossip.rs | postcard of `("rdm.app", fabric, kind)` |

The strings are the wire: every node of a fabric must hash the same bytes.

### 3.4 Seven places that hand peers to iroh-gossip

iroh-gossip has `Gossip::subscribe(channel_id, bootstrap)` and `GossipSender::join_peers(peers)`.

| RDM site | callers |
|---|---|
| `TopologyView::join(.., seeds)`, `Backbone::join(.., seeds)`: subscribe | rdm-node first start; rdm-admin boot |
| the channel's reopen on a start (Q88: a node back on its channel dials the members of its mesh it holds): subscribe | `Channel::start` |
| `TopologyView::join_peers(peers)` | none in production |
| `TopologyView::join_peers_as(caller, peers)`, the same body with another span caller | `node.mesh_join`, `admin.reenter`, `admin.session.mesh`, `admin.session.rejoin` |
| `Backbone::join_admins(admins)` | admin boot; `AdminFirstStart::topology_installed` |
| repair and refeed: `sender.join_peers` directly, refeed skipping the `rejoin` filter on purpose | the channel's spawned loops |
| the application-channel hub: `sender.join_peers` directly | `join_channel` on a held channel |

- One parameter has three names: `seeds`, `peers` and iroh's `bootstrap`.
- The span `rdm.mesh.connection.update.via-join-peers` records only the peers not asked before.
- `admin.session.rejoin` names a first start.
- A held but stopped channel answers a `join_peers` with `Ok(0)` and dials nothing.

### 3.5 Status in three shapes

| today | what it builds |
|---|---|
| `StatusEvent { Deleting, Stopped }` | `NodeDeleting`, `NodeStopped` |
| `Frame::NodeDeleted` | built apart |
| `StatusScope { Mesh, Fabric }` with `StatusPublisher` | `MeshStatus`, `FabricStatus`: a string status, five sends one second apart, and a replay |

Their receive rules differ by frame (receive_order.rs):
- mesh and fabric status order by the envelope's primary revision, then `changed_at_rafka_ms`;
- `NodeStopped` and `NodeDeleting` order by `topology_version`, per publisher and subject mesh;
- `NodeDeleted` carries no `publisher` or `topology_version`, and timestamps never order it.

### 3.6 The bootstrap a node passes

- **Every subscribe names the node's launcher.** It's written in rdm-admin boot as
  `LaunchTemplate.seeds = vec![(key.public(), mesh_addr)]`.
- **Day-0 and recovery starts** take `cfg.seeds` (`RDM_SEEDS`, a list; env-vars.md).
- **A node-admin over an inherited or lost root** takes every birth its store records.
- **A launched node-admin** hands the same list to its mesh channel and the backbone.

Lane 4's release run at `df12ed232` shows the defect:
- mesh2's first node-admin subscribed mesh2's channel with mesh1.node-admin.1, which is never on that
  channel;
- `topology.refeed` dialed it there again.

On a start after a stop, `node_self` holds the commanding mesh-primary's key (`commander`), and the
join goes back over that call. But the channel's reopen dials only the peers it held before, never
the commander.

### 3.7 The rest

- Two chunk methods with two jobs:
  - `chunking::pack_in_order` packs items into whole frames: topology fulls, Build facts and the Q285
    read replies;
  - gossip.rs's `rdm.gossip.chunk` splits one encoded application message into at most 16 parts.
- The §3.1 `Digest` row lists `endpoints with freshness`. The wire (`WireDigest`) carries one
  `endpoint_id` and one `transport_addr`, plus fourteen fields the row does not list.
- `Topology` already names rdm-admin's observed fabric topology and rdm-core's read-protocol marker.

## 4. The proposed layout

### 4.1 Crates

```text
rdm-core ← rdm-transport (Node RPC runtime; endpoint, delivery, wire, clock, load, chunking, telemetry)
              ← rdm-client
              ← rdm-gossip (new: channels, the topology view, the four kinds)
                     ← rdm-node ← rdm-admin
```

- **`rdm-gossip`** takes every gossip module out of `rdm-transport`. It depends on `rdm-core`,
  `rdm-transport`, `iroh` and `iroh-gossip`. Nothing in `rdm-transport` or `rdm-client` depends on
  it.
- **`rdm-transport`** keeps the Node RPC runtime and the shared pieces of §3.1. It makes `wire::take`,
  the trace-context helpers and `test_span_capture` public (`test_span_capture` behind its `testkit`
  feature) so `rdm-gossip` uses them and copies nothing. It keeps `iroh-gossip` for its ALPN (the
  endpoint binds it; `connections.rs` admits it). Its `testkit-gossip` feature splits:
  - the pool and client parts stay as `rdm-transport/testkit`;
  - the gossip parts move to `rdm-gossip/testkit`.
- build.md's package table: the gossip-substrate row names `rdm-gossip`, in the same landing as the
  move (§8).

### 4.2 Modules of `rdm-gossip`

| module | holds | comes from |
|---|---|---|
| `channel` | `ChannelId`, with `mesh(fabric, mesh_id)`, `backbone(fabric)` and `app(fabric, name)`, each hashing exactly what today's function hashes; `Channel`, one subscribed channel of any kind (sender, known peers, stop and start, repair, refeed); `Channels`, one node's iroh-gossip handle and the channels it holds, with the one join (§5.1) | topology.rs `Channel`, `rejoin`, `RepairSchedule`, `refeed_backoff`, `mesh_channel`, `backbone_channel`; gossip.rs `channel_id`, `Channel`, `Hub`'s subscribe, `dial_span` |
| `envelope` | the CloudEvents binding, unchanged | envelope.rs |
| `receive_order` | the one out-of-order check, unchanged | receive_order.rs |
| `frame` | the `Frame` enum, its wire positions and `type` values unchanged; the Rust names `MeshTopology` → `TopologyFull`, `MeshTopologyDelta` → `TopologyDelta` (Q375) | topology.rs `Frame` |
| `view` | `TopologyView`, the cache: held digests, held projections of every mesh, held statuses, held primaries; `install`, the one call every receive rule and the topology read use; its readers | `DigestView`, `View`, `HeldMembers`, `StatusView`, `PrimaryView`, `RoutingWatch`, and the readers now on `TopologyView` (`nodes_get`, `primaries`, `mesh_status`, `fabric_status`, `held_topology_of`, `installed_snapshot`, `projected_member_at`, `mesh_primary`) |
| `digest` | the `Digest` heartbeat: its publisher and its receive rule | `publish_every`, `heartbeat`, `TopologyView::publish`, the digest arm of `View::take_with`, `discipline` |
| `topology` | the full topology and its delta: the mesh-primary's full publisher, the delta (a receiving peer's and the source's own), the atomic install of a full | snapshot.rs; `Backbone::publish`, `own_full`, `forward_fulls`, `publication_wake`; `TopologyView::forward`; forward_path.rs; `View::take_snapshot`, `apply_full` |
| `status` | `Status { type: Node \| Mesh \| Fabric, status, .. }`, the one Rust type of Q374, which encodes to and decodes from today's five frames with each frame's own receive rule (§5.3) | `StatusEvent`, `StatusScope`, `StatusPublisher`, `StatusReinforcement`, `StatusFact`, `publish_lifecycle`, `learn_statuses`, `adopt_mesh_status`, `Backbone::announce_statuses`, `send_status`, `publish_status_event` |
| `backbone` | `Backbone`: the node-admin's backbone channel and its publishing role, the role flags set by the election as today | `Backbone`'s role flags, `published_as_primary`, `until_published_as_primary`, `withhold_announcements`, `hold_primary`; its publishers call `topology` and `status` |
| `top_up` | the `GetTopology` pull into the view, by the injected fetch it has today | `spawn_top_up`, `top_up`, `TopUpFetch`, `TopUpDone`, `desynced_sources` |
| `location` | the peer-location view and the address lookup it feeds | `LocationView`, `LocationMarks`, `register_location`, `LocationMove`, `gossip_addr`, `learn_addresses`, the `MemoryLookup` wiring |
| `app` | an application channel's message delivery: the bounded queue, the loss generation, `Event`, `Received`, the `rdm.gossip.chunk` assembly | gossip.rs `State`, `Publisher`, `Subscriber`, `Event`, `Received`, `deliver`, `Assembly` |

Each module carries its own tests, moved from topology.rs's test modules and from rdm-transport's 23
gossip test files.

### 4.3 What leaves gossip

- The node's own config, tags, row and name (`set_config`, `set_tags`, `set_own_row`, `set_name`,
  `held_name`, `OwnConfig`) move to rdm-node's node record. The `Digest` publisher is handed the
  name it publishes.
- Deleted:
  - `TopologyView::join`, `join_channel`, `join_peers` and `join_peers_as`;
  - `Backbone::join` and `join_admins`;
  - gossip.rs's `Channel` and `Hub`;
  - the free functions `mesh_channel`, `backbone_channel` and `channel_id`.

  Each is replaced by its §4.2 home in the same landing that adds the home.

## 5. The calls a node makes

### 5.1 One join

```rust
// one node's channels: its iroh-gossip handle, its endpoint, its view
let channels = Channels::new(gossip, endpoint, view.clone());
// one call for every channel, mesh, backbone and application alike (Q376's form)
let mesh = channels.join(ChannelId::mesh(&fabric_id, &mesh_id), peers).await?;
```

- **`Channels::join(id, peers) -> Channel`.** The first call with an id subscribes, with `peers` as
  iroh-gossip's bootstrap. A later call with the same id hands `peers` to `join_peers` on the channel
  already held. A call on a held but stopped channel starts it, so a seed handed on a start reaches
  iroh-gossip.
- **The receive path is wired by the id's kind.** `Channels` holds the view, so the topology
  receive path, the repair targets and the status replay (today's 14 inputs) come from the kind: a
  mesh or backbone id wires the topology receive path; an app id wires the app delivery. No caller
  passes them.
- **One `Channels` per node, not per process.** Test cells run several nodes in one process, each
  with its own `Channels`.
- **One span per join.** It names the channel, the peers named and the peers dialed. The caller is
  the span the call runs in. Repair, refeed and reopen open their own named span
  (`topology.repair`, `topology.refeed`, `topology.reopen`) around the call, since their loops run
  spawned.
- **The `rejoin` filter stays as it is: a peer asked while live is not asked again.** Refeed re-asks
  every known peer by clearing the asked set when the channel has no neighbour, then calling
  `join`. These are the same dials as today, through the one call.
- **One word, `peers`** (gossip.md §8's word). `seeds` and `bootstrap` leave RDM's code; `RDM_SEEDS`
  keeps its name in env-vars.md.

### 5.2 The seed (Q376), the one **dial** change

| when | the peers the join names | change |
|---|---|---|
| create, mesh channel | the launcher, when its mesh is the node's own (`rdm_core::launch::Launcher { mesh, .. }`); otherwise none: the node is its mesh's first listener | **dial**: the launcher of another mesh is no longer named on the mesh channel |
| create, backbone and application channels | the launcher | as today |
| a commanded start or join, every channel | the commander's endpoint id (`node_self`'s `commander`), plus the peers the channel already knows | **dial**: the commander is added |
| a commanded start or join, mesh channel, commander of another mesh | the peers the channel already knows | the fabric-primary is never named on a mesh channel it is not on |
| after the topology read | the nodes of its own mesh it read on the mesh channel, the node-admins it read on the backbone | as today |
| repair, refeed, reopen | the channel's known peers | as today |
| Day-0, a recovery start, an inherited or lost root, an adoption | `cfg.seeds` or the births its store records | as today |

- **No new wire field.** iroh-gossip's bootstrap takes endpoint ids, and the node already holds the
  commander's key.
- **How the node knows the commander's mesh:** from the commander's digest in the view, which holds
  every mesh's nodes from the forwarded topology. If the commander isn't in the view, it is treated
  as of another mesh.
- **A re-created mesh-primary keeps today's peers.** When the fabric-primary re-creates a lost
  mesh-primary while the mesh's ordinary nodes still listen, the new node-admin is the first listener
  on its mesh channel until those nodes dial it. Today their repair already dials the mesh-primary
  that their projection names.

### 5.3 One status type (Q374), the wire unchanged

`Status { type: Node | Mesh | Fabric, subject, status, publisher, order }` is the one Rust type that
every status publisher and receiver uses. It encodes to today's five frames and decodes from them,
and each type keeps its own rule:

| type | frame(s) | order key | recovery | sends |
|---|---|---|---|---|
| Node, stopped / deleting | `NodeStopped`, `NodeDeleting` | `topology_version`, per publisher and subject mesh | the full's `in_flight` | once |
| Node, deleted | `NodeDeleted` | none: applied once per `(type, build_id, operation)` | the full's `deleted` | once |
| Mesh | `MeshStatus` | the primary revision, then `changed_at_rafka_ms` | replay on a join, heal, refeed or top-up | five, one second apart |
| Fabric | `FabricStatus` | the primary revision, then `changed_at_rafka_ms` | replay on a join, heal, refeed or top-up | five, one second apart |

One frame on the wire for all of status would be a wire change (§9 D1). It waits for a fabric-wide
move to one RDM version, which is not part of this refactor.

### 5.4 The start, in the page's order

```text
create the TopologyView (empty)
channels.join(mesh channel, peers of §5.2)    (and the backbone, a node-admin)
node.mesh.joined; the Digest heartbeat starts
pre_node_ready                                (the app joins its application channels: channels.join)
node.topology.get → view.install(...)         (the final step)
the joins after the read (§5.2)
```

## 6. What changes the wire or what gossip dials

| change | wire | dial | proof |
|---|---|---|---|
| the crate move and the modules (§4) | no | no | fast gate |
| one join, one span (§5.1) | no | no | fast gate; the join span shows both lists |
| the seed (§5.2) | no | **yes** | fast gate; a flame of a first start, a commanded start after a stop and a mesh's first node-admin |
| one status type (§5.3) | no | no | fast gate; the status frames' byte fixtures unchanged |
| the Rust renames (Q375) | no | no | fast gate; the wire `type` values unchanged |

iroh, iroh-base and iroh-gossip are not touched.

## 7. Order of landings

1. The join span names the peers named and the peers dialed (telemetry only).
2. `rdm-gossip` is created, with the modules of §4.2 moved as they are. In the same landing:
   - every caller in rdm-node, rdm-admin, rdm-test and rdm-transport's moved tests imports from
     `rdm-gossip`;
   - the testkit features split;
   - rdm-transport's helpers become public;
   - build.md's package table changes.

   No re-export is left in `rdm-transport`.
3. `TopologyView` becomes the cache alone; the own record moves to rdm-node; `TopologyView::join` is
   gone and the start runs in §5.4's order.
4. `Channels`, `Channels::join` and `ChannelId` land. The seven join sites go through them, and
   gossip.rs's `Channel` and `Hub` are deleted.
5. The seed (§5.2), with its flame.
6. One status type (§5.3) and the Rust renames.
7. The page text of each landing goes in that landing (§8).

Steps 1–4 and 6 change no behaviour, and the fast gate is their check. Step 5 is the dial change and
lands with its flame.

## 8. Page text (approved with the change)

- **gossip.md §3.1:**
  - Old: "Two channels carry the topology: a Mesh's topology channel (one per MeshId) and the
    node-admin backbone (one per FabricId). No other channel exists."
  - New: "Two channels carry the topology: a Mesh's topology channel (one per MeshId) and the
    node-admin backbone (one per FabricId). No other channel carries the topology."
- **gossip.md §8:**
  - Old: "A node joins a channel through `TopologyView::join_channel(name, peers)`
    (`rdm_transport::gossip`): `name` is the application's,"
  - New: "A node joins a channel through `Channels::join(id, peers)` (`rdm_gossip::channel`), the one
    join every channel uses; an application channel's `id` is `ChannelId::app(fabric_id, name)`, and
    `name` is the application's,"
- **build.md package table:**
  - Old: "`rdm-transport` | the gossip substrate on Iroh: the topology digest view, versioned
    cross-mesh topology snapshots and deltas, and the gossip frame codec"
  - New: "`rdm-gossip` | the gossip substrate on Iroh: channels, the topology view, the `Digest`, the
    full topology and its delta, status, and the gossip frame codec"
- **gossip.md, "lifecycle event" / "lifecycle events":** in §3 (lines 108, 112, 120), §3.1 (194,
  256, 332), §3.2 (498), §3.3 (577, 580), §6.1 (733), §7 (780) and §14 (939), the word becomes
  "status frame" / "status frames" (Q374: they are status; §3.2 already calls a `MeshStatus` a
  status frame).
- **gossip.md §3.1 frame names:** `MeshTopology` → `TopologyFull`, `MeshTopologyDelta` →
  `TopologyDelta` (Q375). Their wire `type` values (`rdm.mesh.members`, `rdm.mesh.members.delta`)
  stay as written.

## 9. Decisions (were O1–O6)

- **D1. No wire change in this refactor.**
  - Why: mixed executables are the upgrade path (build-deployment.md §3; Q378), so a new channel id
    or frame shape would split a mesh while it upgrades.
  - Consequences: the channel ids keep their bytes; status is one Rust type over today's five frames;
    the renames are Rust names only.
  - Q374's single wire frame and a single channel-id derivation wait for a fabric-wide move to one
    RDM version.
- **D2 (O1).** `rdm-gossip` is its own crate. No Node RPC module reads gossip, and build.md describes
  the gossip substrate as its own package.
- **D3 (O2).** `TopologyFull` and `TopologyDelta`. "Full" and "delta" are gossip.md §3.3's words,
  and they collide with neither `Topology` type.
- **D4 (O3).** The `rejoin` filter stays. Refeed re-asks by clearing the asked set, so the dials are
  the same as today's.
- **D5 (O4).** The two chunk methods stay: one packs items into whole frames, the other splits one
  message. `chunking` stays in rdm-transport, because rdm-client uses it.
- **D6 (O5).** "Status frame" for `NodeStopped`, `NodeDeleting` and `NodeDeleted`: Q374 makes them
  status, and the page already uses "status frame".
- **D7 (O6).** `Channels`, one per node, holding the iroh-gossip handle, the endpoint and the view.
  Test cells run several nodes in one process, and the receive path of a channel comes from the view
  it fills.
