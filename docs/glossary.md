# Glossary

Every term below links to the architecture pages that define it.

## Hierarchy and roles

- **fabric** ([Fabric primary](architecture/fabric-primary.md)): The network root of an RDM
  estate. A fabric contains meshes, has one minted `FabricId`, and is written by one
  `fabric-primary`.
- **mesh** ([Fabric primary](architecture/fabric-primary.md),
  [Node identity and state](architecture/node-identity-and-state.md)): A named group of nodes
  inside a fabric, identified by its minted `MeshId`. Every node belongs to one mesh and
  therefore belongs to the fabric transitively (`Node.mesh_id -> Mesh.fabric`).
- **node** ([Node identity and state](architecture/node-identity-and-state.md)): One entity in the
  fabric's `nodes.storage` (`NodeRecord`) and the `rdm-node` process that runs it, of one `type`:
  `node`, `node-admin`, or a type the application declares on the Fabric (`RDM_NODE_TYPES`).
- **node-admin** ([Node-admin lifecycle](architecture/node-admin-lifecycle.md),
  [Build](architecture/build.md)): A node of type `node-admin` running `rdm-admin` on top of
  `rdm-node`. Node-admins run the control plane: topology stores, elections, Builds, lifecycle
  workflows, and deployment providers.
- **fabric-primary** ([Fabric primary](architecture/fabric-primary.md),
  [Elections](architecture/fabric-node-lifecycle-elections.md)): The one elected `node-admin` across
  the fabric that accepts Builds, writes the fabric and mesh records and every Build record,
  and commands each mesh's `mesh-primary`.
- **mesh-primary** ([Fabric primary](architecture/fabric-primary.md),
  [Elections](architecture/fabric-node-lifecycle-elections.md)): The elected primary of the
  `node-admin` nodes in one mesh. A `mesh-primary` commands its own mesh's nodes, writes their
  rows in `nodes.storage`, and announces their lifecycle status.
- **node-type primary** ([Elections](architecture/fabric-node-lifecycle-elections.md)): The
  elected primary among the `Ready` nodes of one type in one mesh. A primary stays primary
  while it is heard; when unheard past the staleness floor (or when none is recorded), the eligible
  `Ready` candidate in that mesh with the lowest complete `NodeId` wins, or the current primary
  hands over via [`node.primary.update`](architecture/lifecycles/node-primary-update.md).

## Node identity and lifecycle

- **`node.id` (`NodeId`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The node row's minted 60-bit, 12-character lowercase Crockford id. It names one node instance on
  public interfaces, logs, and wire responses, is never derived from `endpoint_id`, and is never
  reissued.
- **`path` and `name`** ([Node identity and state](architecture/node-identity-and-state.md)):
  Two of the node's properties, its logical slot in the fabric (`<mesh>.<type>` and a free-text name by
  convention), two separate fields, never joined. Durable references to a node and Build step keys name it
  by its `node.id`; `path` and `name` together are only the lookup index's key. A recovery or re-birth at
  that `path` and `name` mints a new `node.id`.
- **`endpoint_id` (`EndpointId`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The node's iroh ed25519 public key (64 hex characters) used by the transport to dial and
  authenticate peers. It is routing-only and is never stored as an application reference or exposed
  as a node's public identity.
- **incarnation (`IncarnationId`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The minted Crockford id of one runtime birth of a node. A stop and start keeps the same
  `node.id` and `incarnation`; a recovery or a re-birth at the same `path` and `name` mints a new `node.id`
  and a new `incarnation`.
- **birth** ([Node identity and state](architecture/node-identity-and-state.md)): One exact runtime
  instance of a node, identified by `(mesh, node.id, incarnation)`.
- **re-birth** ([Build deployment](architecture/build-deployment.md),
  [Node recovery](architecture/lifecycles/node-recovery.md)): A new node, with a new node.id, at the
  same `path` and `name` an earlier node held (a recovery's create, or a Build that adds a node on a
  new executable), never a running process changed in place.
- **lifecycle status (`MemberStatus`)** ([States](architecture/lifecycles/states.md)): The durable
  readiness state recorded on a node, mesh, or fabric row (`starting`, `pending`, `ready`,
  `connections-deleting`, `connections-deleted`, `stopping`, `stopped`, `deleting`, `deleted`). A
  node's status is written only by its `mesh-primary` from the node's workflow reports.
- **liveness (`NodeState`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  What peers infer locally from gossip digests (`Alive`, `PendingReconnect`, `Dead`), separate from
  the durable `status` on the node row.
- **stopped-node collector** ([Node identity and state](architecture/node-identity-and-state.md)):
  The `fabric-primary`'s sweep that submits one ordinary [`node.delete`](architecture/lifecycles/node-delete.md)
  Build for each node whose status has stayed `stopped` past `stopped_node_ttl` (`RDM_STOPPED_NODE_TTL`).
  Blank means forever: no sweep.

## Builds and deployment

- **Build** ([Build](architecture/build.md)): An external request that adds or removes nodes:
  `Create { meshes, nodes }`, `Delete { meshes, node_ids }` or `DeleteFabric`, accepted by the
  `fabric-primary` and written once in `builds.storage` (`BuildStateAdapter`). Its progress is its
  events. Node commands (stop, start, connections delete, name update) and repairs are never Builds.
- **section** ([Build](architecture/build.md)): One mesh's part of a Build: one call from the
  `fabric-primary` to that mesh's `mesh-primary`, which runs the section's steps. A step that ends
  `failed` ends its section, never the Build.
- **Build end** ([Build](architecture/build.md)): `complete` when every section finished;
  `incomplete` when it ran and not every section finished (what ran stands, nothing is reverted);
  `failed` only when it could not start. An incomplete Build is resubmitted by its `build_id`.
- **attempt** ([Node RPC §43](architecture/node-rpc.md#43-send-delivery-is-the-runtimes)): One send of
  a call under its `Send { attempts, TimeLimit }`; a call makes `attempts` sends in all (default 3).
- **step** ([Build](architecture/build.md)): One unit of work inside a Build (for example,
  `node.create:<node_id>`), assigned by the
  `fabric-primary` to a mesh executor over the [Build carrier](architecture/node-rpc-envelope.md).
- **step event** ([Build §5](architecture/build.md#5-buildstateadapter)): The `rdm.build.step` event
  a step's run writes, ending `complete` or `failed { reason }`. A mesh's new mesh-primary, or a
  resubmit, runs only the steps whose events did not end `complete`.
- **Build floor** ([Node start](architecture/lifecycles/node-start.md),
  [Node mesh join](architecture/lifecycles/node-mesh-join.md),
  [Gossip](architecture/gossip.md)): The minimum `BuildId` a joining or starting
  `node-admin` must hold in its local Build log before it can report `NodeStarted` and become
  `Ready`, named by the node-admin it reports to in its `JoinNode` (`0x1D`) answer and fetched when
  missing via `FetchCurrentBuild` or `FetchBuildFacts` (`0x1F`).
- **checklist (planned-birth checklist)** ([States](architecture/lifecycles/states.md)): The
  per-primary list of planned exact births from the accepted Build that must complete their owed
  check-in, delivered on its `Send`, for a state command before the primary reports the state's upward
  completion.
- **hand-off (mesh state hand-off)** ([States](architecture/lifecycles/states.md),
  [Fabric primary §7](architecture/fabric-primary.md#7-mesh-primary-takeover-is-hydration-not-transition)):
  The launching `node-admin`'s one `ApplyMeshState` call (`Status`, `0x1B`) to a `node-admin` it
  creates, once that birth answers `Ping`. A new mesh's first `node-admin` is handed `Pending`; one
  created for an existing mesh is handed the mesh's held state with its `changed_at`, which it adopts
  and republishes once its checklist completes.
- **deployment provider** ([Deployment provider](architecture/deployment-provider.md)): The
  environment policy (`MESH_SPAWN_TYPE=process|container`) that launches, inspects, and terminates
  node runtimes within a provider control domain (a host PID namespace for `process`, or a Docker
  daemon for `container`).
- **`RuntimeFact`** ([Deployment provider](architecture/deployment-provider.md),
  [Gossip](architecture/gossip.md)): The compact provider runtime locator (such as PID and start
  token, or container ID) bound to `(NodeId, IncarnationId)` and published by a birth in its
  topology digest so any `node-admin` in the same control domain can inspect or adopt the runtime.

## Gossip, topology, and queries

- **gossip** ([Gossip](architecture/gossip.md)): The `iroh-gossip` dissemination plane over which
  nodes publish current birth digests, lifecycle events, and primary projections. Gossip carries
  current topology facts and recovery evidence, never imperative control commands.
- **mesh channel** ([Gossip](architecture/gossip.md)): The gossip channel joined by every node of a
  single mesh (`mesh-v1:<fabric_id>:<mesh_id>`), carrying that mesh's node digests and events.
- **backbone** ([Gossip](architecture/gossip.md)): The fabric-wide gossip channel joined by
  `node-admin` nodes (`backbone-v1:<fabric_id>`), carrying mesh summaries and cross-mesh topology
  across the fabric.
- **topology (`TopologyView`)** ([Gossip](architecture/gossip.md),
  [Query](architecture/query.md)): A node's local in-memory projection of the latest gossiped
  digests, primaries, and mesh and fabric statuses it has heard, queryable locally inside the
  process via `TopologyView::nodes_get`, `meshes_get`, and `fabrics_get`.
- **atlas** ([Query](architecture/query.md)): The `fabric-primary`'s in-memory control state: its
  Builds, accepted topology and in-flight facts. Node rows and property maps are not copied into it;
  `node.get` reads the node records.
- **hydration sync** ([Node start](architecture/lifecycles/node-start.md),
  [Node topology get](architecture/lifecycles/node-topology-get.md),
  [Query](architecture/query.md)): The [`node.topology.get`](architecture/lifecycles/node-topology-get.md)
  (`0x1E`) read a node performs during [`node.start`](architecture/lifecycles/node-start.md) after
  `pre_node_ready`, installing complete mesh snapshots together with the node's own whole row, the
  recorded primaries (with lost births marked gone), and each mesh's `MeshId`.
- **staleness floor** ([Gossip](architecture/gossip.md),
  [Elections](architecture/fabric-node-lifecycle-elections.md)): The silence bound
  (`RDM_STALENESS_MS`) past which an unheard peer is inferred stale (`PendingReconnect`) or an
  unheard primary is superseded in an election.

## Node RPC, routing, and connections

- **Node RPC** ([Node RPC envelope](architecture/node-rpc-envelope.md),
  [Node RPC runtime](architecture/node-rpc.md)): RDM's exact-target RPC substrate over iroh QUIC
  streams (`rdm/node-rpc/1`), carrying unary and server-streaming calls with W3C trace context and
  typed [`RpcOutcome`](architecture/node-rpc.md) results (`Reply`, `NotSent`, `Unserved`,
  `RejectedStale`, `Indeterminate`).
- **op ledger** ([Node RPC envelope](architecture/node-rpc-envelope.md)): The table of registered
  Node RPC op codes. Each has an owner (`core`, the generic substrate; `rdm`, the mesh product;
  `rafka`, Rafka's own adapters; `testkit`, `0x70` to `0x7F`, test-only oracles that never ship in a
  product binary) and a ledger state that says whether the op may be served at all.
- **`Direct` and `Proxy` connections** ([Connections](architecture/connections.md)): The two kinds
  of data-plane connection facts recorded in `connections.storage`. `Direct` is an open connection
  from the source node to the destination node; `Proxy` routes through an intermediate carrier node
  until the destination dials the source or the Proxy is invalidated; every dial is a call on `Send`,
  and nothing re-dials on its own (Q348).
- **`forward`** ([Node RPC envelope](architecture/node-rpc-envelope.md)): The control plane's Node
  RPC op (`0x1A`) that carries a call through an intermediate node when the caller does not reach
  the destination directly. The data plane's word for the same idea is `proxy`.
- **`connections-deleting`** ([States](architecture/lifecycles/states.md),
  [Node connections delete](architecture/lifecycles/node-connections-delete.md)): The lifecycle
  state a node enters while [`node.connections.delete`](architecture/lifecycles/node-connections-delete.md)
  deletes its connections before [`node.stop`](architecture/lifecycles/node-stop.md),
  [`node.delete`](architecture/lifecycles/node-delete.md) or a
  [`node.name.update`](architecture/lifecycles/node-name-update.md), refusing new mutating work with
  `ConnectionsDeleting`.

## Hooks, certificates, storage, and recovery

- **hooks (`pre_node_ready`, `on_node_ready`, `on_mesh_ready`, `on_fabric_ready`, `on_build_started`, `on_build_ended`)**
  ([Fabric and node hooks](architecture/fabric-node-hooks.md)): The entry points where application
  work plugs into an RDM lifecycle. `pre_node_ready` gates a node's transition to `Ready`; the
  `on_*` hooks are fire-and-forget notices called after the state or Build transition is recorded.
- **`rafka-time`** ([Fabric and node hooks](architecture/fabric-node-hooks.md),
  [Gossip](architecture/gossip.md)): The fabric's clock, which says when a fact or publication
  happened and never decides liveness. RDM exposes it through one public, cloneable `RafkaTime`
  handle with `now_ms()`, from the running node and from `NodeReadyCtx`; it is the same instance
  every RDM stamp reads, and there is no second clock.
- **fabric root, fabric CA, mesh issuing cert, member cert** ([Fabric certs](architecture/fabric-certs.md)):
  The four-level certificate chain (`root -> CA -> mesh issuer -> member cert`). Each node birth receives
  a signed `member cert` at `JoinNode` and verifies the peer's `member cert` on product ALPN
  connections via [`node.connections.admit`](architecture/lifecycles/node-connections-admit.md).
- **declared secret** ([Deployment provider](architecture/deployment-provider.md),
  [Storage](architecture/storage.md)): A secret the application declares for named node types. Its
  value is set only in the environment of the types its declaration names, and is written to the
  admins that may launch those types, each keeping it beside its data dir as `declared-secrets`. It
  is never written to a row, a Build record, a receipt, the launch record or a span.
- **`storage_persistence` and `topology_storage_provider`** ([Storage](architecture/storage.md)):
  The two storage properties in RDM. `storage_persistence` (`ephemeral`,
  `persistent { disposition: preserve }` or `persistent { disposition: release }`) governs whether a node's own data directory survives across births at
  the same `path` and `name`; `topology_storage_provider` governs which backend RDM's five record stores
  (`fabric`, `mesh`, `nodes`, `connections`, `builds`) write to.
- **node investigation (`node.tickle`, `node.runtime.inspect`)**
  ([node investigation](architecture/lifecycles/node-investigation.md),
  [node.tickle](architecture/lifecycles/node-tickle.md),
  [node.runtime.inspect](architecture/lifecycles/node-runtime-inspect.md)): The workflow a
  `mesh-primary` runs when a node is unheard past the staleness floor, probing the birth with
  `node.tickle` and reading its runtime state via `node.runtime.inspect` before deciding whether to
  wait, reconnect, or recover the node via [`node.recovery`](architecture/lifecycles/node-recovery.md).
- **node investigation verdicts (`reached`, `running`, `os-stopped`, `exited`, `unproven`)**
  ([node investigation](architecture/lifecycles/node-investigation.md)): The five possible outcomes
  of [node investigation](architecture/lifecycles/node-investigation.md) on one exact birth.
  `reached` means the exact birth answered `node.tickle` (with no command left unanswered past the
  staleness floor) and is commanded back via [`node.mesh.join`](architecture/lifecycles/node-mesh-join.md);
  any other verdict comes from [`node.runtime.inspect`](architecture/lifecycles/node-runtime-inspect.md)
  and re-creates the node at the same `path` and `name` with a new `node.id` (over its predecessor's data dir when `exited` with `persistent` storage and the directory there, or a newly allocated data dir otherwise; always a new `endpoint_id`).
- **adoption launch** ([Fabric recovery](architecture/lifecycles/fabric-recovery.md#nothing-alive),
  [Node-admin lifecycle](architecture/node-admin-lifecycle.md)): A `node-admin` a person or a script
  starts at the lost `fabric-primary`'s `path` and `name`, with `RDM_FABRIC_ID`, `RDM_MESH` and
  `RDM_STORE_DIR` and no launcher, when no `node-admin` is alive. It mints its own `node.id` and no
  fabric or mesh id, and takes no lock: its first write is its recovery's.
