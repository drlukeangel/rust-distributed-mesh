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
  fabric's `nodes.storage` (`NodeRecord`) and the `rdm-node` process that runs it, of one `NodeKind`
  (`NodeAdmin`, `RpcNode`, `Broker`, `Gateway` or `Compute`).
- **node-admin** ([Node-admin lifecycle](architecture/node-admin-lifecycle.md),
  [Build](architecture/build.md)): A node of kind `NodeAdmin` running `rdm-admin` on top of
  `rdm-node`. Node-admins run the control plane: topology stores, elections, Builds, lifecycle
  workflows, and deployment providers.
- **fabric-primary** ([Fabric primary](architecture/fabric-primary.md),
  [Elections](architecture/fabric-node-lifecycle-elections.md)): The one elected `node-admin` across
  the fabric that accepts Builds, writes the fabric and mesh records, holds the fabric-wide atlas,
  and commands each mesh's `mesh-primary`.
- **mesh-primary** ([Fabric primary](architecture/fabric-primary.md),
  [Elections](architecture/fabric-node-lifecycle-elections.md)): The elected primary of the
  `node-admin` nodes in one mesh. A `mesh-primary` commands its own mesh's nodes, writes their
  rows in `nodes.storage`, holds its mesh's atlas, and announces their lifecycle status.
- **node-type primary** ([Elections](architecture/fabric-node-lifecycle-elections.md)): The
  elected primary among the `Ready` nodes of one `NodeKind` in one mesh. A primary stays primary
  while it is heard; when unheard past the staleness floor (or when none is recorded), the eligible
  `Ready` candidate in that mesh with the lowest complete `NodeId` wins, or the current primary
  hands over via [`node.primary.update`](architecture/lifecycles/node-primary-update.md).

## Node identity and lifecycle

- **`node.id` (`NodeId`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The node row's minted 60-bit, 12-character lowercase Crockford id. It names one node instance on
  public interfaces, logs, and wire responses, is never derived from `endpoint_id`, and is never
  reissued.
- **`path.name` (`PathName`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The node's logical slot in the fabric, formed from `path` and `name` (`<mesh>.<kind>.<name>` by
  convention). Durable references to a node and Build step keys name the node by `path.name`, while
  a replacement or recovery at that `path.name` mints a new `node.id` or `incarnation`.
- **`endpoint_id` (`EndpointId`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The node's iroh ed25519 public key (64 hex characters) used by the transport to dial and
  authenticate peers. It is routing-only and is never stored as an application reference or exposed
  as a node's public identity.
- **incarnation (`IncarnationId`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  The minted Crockford id of one runtime birth of a node. A stop and start keeps the same
  `node.id` and `incarnation`; a recovery or a re-birth at the same `path.name` mints a new `node.id`
  and a new `incarnation`.
- **birth** ([Node identity and state](architecture/node-identity-and-state.md)): One exact runtime
  instance of a node, identified by `(mesh, node.id, incarnation)`.
- **re-birth** ([Build deployment](architecture/build-deployment.md),
  [Node replace](architecture/lifecycles/node-replace.md)): Creating a new node birth to replace
  an existing node (such as during [`node.replace`](architecture/lifecycles/node-replace.md) when
  changing a node's executable), rather than mutating a running process in place.
- **lifecycle status (`MemberStatus`)** ([States](architecture/lifecycles/states.md)): The durable
  readiness state recorded on a node, mesh, or fabric row (`starting`, `pending`, `ready`,
  `connections-deleting`, `connections-deleted`, `stopping`, `stopped`, `deleting`, `deleted`). A
  node's status is written only by its `mesh-primary` from the node's workflow reports and
  receipts.
- **liveness (`NodeState`)** ([Node identity and state](architecture/node-identity-and-state.md)):
  What peers infer locally from gossip digests (`Alive`, `PendingReconnect`, `Dead`), separate from
  the durable `status` on the node row.
- **stopped-node collector** ([Node identity and state](architecture/node-identity-and-state.md)):
  The `fabric-primary`'s sweep that submits one ordinary [`node.delete`](architecture/lifecycles/node-delete.md)
  Build for each node whose status has stayed `stopped` past `stopped_node_ttl` (`RDM_STOPPED_NODE_TTL`).
  Blank means forever: no sweep.

## Builds and deployment

- **Build** ([Build](architecture/build.md)): A durable, multi-step control-plane operation
  accepted by the `fabric-primary` and recorded in `builds.storage` (`BuildStateAdapter`). The
  newest accepted topology Build defines the fabric's accepted topology.
- **attempt** ([Build](architecture/build.md)): One numbered execution pass of a Build (`attempt`
  starts at `1` and increments when an incomplete Build is reopened or taken over by a successor
  `fabric-primary`).
- **claim** ([Build](architecture/build.md)): The record that opens an attempt of a Build. An
  attempt lost with its executor leaves the Build `Running`, and a successor or drift pass claims
  the next attempt (`attempt + 1`).
- **step** ([Build](architecture/build.md)): One unit of work inside a Build (for example,
  `node.create:<path.name>` or `node.replace:<path.name>:cutover`), assigned by the
  `fabric-primary` to a mesh executor over the [Build carrier](architecture/node-rpc-envelope.md).
- **receipt** ([Build](architecture/build.md)): The durable record written as a Build step reaches
  an execution milestone or terminal outcome (`Complete` or `Failed`), allowing a successor
  executor to resume without repeating completed work.
- **Build floor** ([Node start](architecture/lifecycles/node-start.md),
  [Node mesh join](architecture/lifecycles/node-mesh-join.md),
  [Gossip](architecture/gossip.md)): The minimum `(BuildId, attempt)` a joining or starting
  `node-admin` must hold in its local Build log before it can report `NodeStarted` and become
  `Ready`, named by the accepting primary in its `JoinNode` (`0x1D`) answer and fetched when
  missing via `FetchCurrentBuild` or `FetchBuildFacts` (`0x1F`).
- **checklist (planned-birth checklist)** ([States](architecture/lifecycles/states.md)): The
  per-primary list of planned exact births from the accepted Build that must complete their owed
  check-in (`Budget::Owed`) for a state command before the primary reports the state's upward
  completion.
- **hand-off (mesh state hand-off)** ([States](architecture/lifecycles/states.md),
  [Node-admin lifecycle](architecture/node-admin-lifecycle.md),
  [Node RPC envelope](architecture/node-rpc-envelope.md)): Either the `ApplyMeshState(Pending)`
  call (`Status`, `0x1B`) sent by the launching `node-admin` to a new mesh's first `node-admin`
  once it answers `Ping`, or the `fabric-primary`'s next claim and `build.attempt.run` call when
  handing a Build attempt to the executor named by `executor_for`.
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
- **mesh channel** ([Gossip](architecture/gossip.md)): The gossip topic joined by every node of a
  single mesh (`mesh-v1:<fabric_id>:<mesh_id>`), carrying that mesh's node digests and events.
- **backbone** ([Gossip](architecture/gossip.md)): The fabric-wide gossip channel joined by
  `node-admin` nodes (`backbone-v1:<fabric_id>`), carrying mesh summaries and cross-mesh topology
  across the fabric.
- **topology (`TopologyView`)** ([Gossip](architecture/gossip.md),
  [Query](architecture/query.md)): A node's local in-memory projection of the latest gossiped
  digests, primaries, and mesh and fabric statuses it has heard, queryable locally inside the
  process via `TopologyView::nodes_get`, `meshes_get`, and `fabrics_get`.
- **atlas** ([Query](architecture/query.md)): The in-memory table of node, mesh, and fabric records
  held by a primary (`mesh-primary` for its own mesh; `fabric-primary` for every mesh), populated
  from the stores at start or hand-over and updated on each write to answer external `fabrics.get`,
  `meshes.get`, and `nodes.get` queries without scanning disk.
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
  while the source node's reconnect loop works to restore `Direct`.
- **`forward`** ([Node RPC envelope](architecture/node-rpc-envelope.md)): The control plane's Node
  RPC op (`0x1A`) that carries a call through an intermediate node when the caller does not reach
  the destination directly. The data plane's word for the same idea is `proxy`.
- **`connections-deleting`** ([States](architecture/lifecycles/states.md),
  [Node connections delete](architecture/lifecycles/node-connections-delete.md)): The lifecycle
  state a node enters while [`node.connections.delete`](architecture/lifecycles/node-connections-delete.md)
  deletes its connections before [`node.stop`](architecture/lifecycles/node-stop.md),
  [`node.delete`](architecture/lifecycles/node-delete.md), or [`node.replace`](architecture/lifecycles/node-replace.md)
  cutover, refusing new mutating work with `ConnectionsDeleting`.

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
  [Storage](architecture/storage.md)): A secret the application declares for named node kinds. Its
  value is set only in the environment of the kinds its declaration names, and is written to the
  admins that may launch those kinds, each keeping it beside its data dir as `declared-secrets`. It
  is never written to a row, a Build record, a receipt, the launch record or a span.
- **`storage_persistence` and `topology_storage_provider`** ([Storage](architecture/storage.md)):
  The two storage properties in RDM. `storage_persistence` (`ephemeral`,
  `persistent { disposition: preserve }` or `persistent { disposition: release }`) governs whether a node's own data directory survives across births at
  the same `path.name`; `topology_storage_provider` governs which backend RDM's five record stores
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
  and re-creates the node at the same `path.name` with a new `node.id` (over the inherited data dir
  when `exited`, or a newly allocated data dir otherwise).
- **adoption launch** ([Fabric recovery](architecture/lifecycles/fabric-recovery.md),
  [Node-admin lifecycle](architecture/node-admin-lifecycle.md)): A person- or script-started
  `node-admin` launched at the lost `fabric-primary`'s `path.name` (with `RDM_FABRIC_ID`,
  `RDM_MESH_ID`, and `RDM_STORE_DIR`, and no launcher) when every `node-admin` of every mesh is
  lost. It mints a new `node.id`, claims the `fabric-recovery` attempt of the current Build before
  any other write, refuses with `NameHeld` if any `node-admin` answers, and restores the
  `mesh-primary` and `fabric-primary` roles once `Ready`.
