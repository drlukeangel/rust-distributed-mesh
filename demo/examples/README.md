# Examples

One small program per feature an application calls or registers, each on RDM's public crates only
(`rdm-client`, `rdm-admin`, `rdm-node`, `rdm-core`, `rdm-transport`) and each building with the
consumer:

```text
cd demo && cargo build --examples          # every example, into demo/target/debug/examples/
cargo build --example node_tags            # one
```

`scripts/i143-rshape-build-consumer.sh` builds them with the consumer's binaries, so none goes stale.

There are two kinds:

- **Callers** enter at a node-admin's birth, the JSON a node-admin prints after `RDM_NODE_BIRTH=`
  (`scripts/demo-estate.sh up` keeps the Day-0 admin's in `<root>/birth.json`), and make their calls
  there:

  ```text
  scripts/demo-estate.sh up
  export RDM_NODE_ADMIN_BIRTH="$(cat "$(cat target/demo-estate.current)/birth.json")"
  demo/target/debug/examples/node_tags <mesh>.broker.1 gold
  ```

- **Executables** are a node-admin (and, for two of them, a node kind too) that registers something in
  its `Wiring`. They run in place of `rshape-node-admin`: as the Day-0 admin, and named by a Build for
  the paths of the kinds they serve.

Every program exports its spans to `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4318`).

| Example | Kind | Feature | What it shows |
|---|---|---|---|
| [on_mesh_ready.rs](on_mesh_ready.rs) | executable | `on_mesh_ready` | The notice a mesh-primary calls once its mesh is ready: registered in `FabricHooks`, fire and forget, here a counted log line. |
| [on_build_started_ended.rs](on_build_started_ended.rs) | executable | `on_build_started`, `on_build_ended` | The fabric-primary's notices of each Build: the started notice with its attempt's reason, the ended notice with `Complete` or `Failed { step, reason, code }`; the application keeps the Builds in flight. |
| [node_name_update.rs](node_name_update.rs) | caller | `node.name.update` | A live node's rename: its connections deleted and its mesh left as one Build of steps (the precondition), the call to its mesh-primary for its exact birth, then a stop under the new name. |
| [node_tags.rs](node_tags.rs) | caller | `node.tags.create`, `node.tags.delete` | Labels on a node's row, written by its mesh-primary; nodes found by label with a `tags` filter pair; the label removed. |
| [forward.rs](forward.rs) | caller | Forward `0x1A` | `node.config.update` sent `Direct` and, when that ends `NotSent`, the same call through a carrier (`RouteChoice::ViaPeer`); `--via` names the carrier. |
| [named_listeners.rs](named_listeners.rs) | executable | `RDM_LISTENERS` | The gateway's `client` listener declared in the admin's `Wiring`; the launched gateway takes its bound listener from the running node and serves on the port the operating system picked, which its row publishes. |
| [declared_secrets.rs](declared_secrets.rs) | executable | `RDM_DECLARED_SECRETS` | A secret declared for brokers only: read once from the bootstrap admin's environment, set only in brokers' environments, never written to a row, record or span; the broker reads it. |
| [build_reattach.rs](build_reattach.rs) | caller | Build reattach | A Build whose stream the caller drops after `Started`, then `BuildCarrier::reattach` by its id (the receipts, the stream again, never a second Build) and `BuildCarrier::follow` to its terminal. |
| [node_topology_get.rs](node_topology_get.rs) | caller | `node.topology.get` | The topology a node holds, mesh by mesh, and the read after a reconnect: `since` the version held answers `Unchanged`. |
| [fabric_atlas_read.rs](fabric_atlas_read.rs) | caller | `fabrics.get`, `fabrics.atlas.get` | The fabric-wide reads: the fabric, its meshes and their primaries; the fabric-primary's atlas, each node's status as the operations left it, the operation still running on it, and the gossip view's status as context only. |
| [node_primary_update.rs](node_primary_update.rs) | caller | `node.primary.update` | A planned move of a mesh's primary before maintenance: the election names the successor, which installs itself and answers with the revisions. |
| [fabric_primary_update.rs](fabric_primary_update.rs) | caller | `node.fabric-primary.update` | A planned move of the fabric-primary: its own mesh's successor first, else another mesh's mesh-primary. |
| [node_mesh_leave.rs](node_mesh_leave.rs) | caller | `node.mesh.leave` | A node taken off its mesh on its own (connections deleted, then the leave, as one Build of steps), its row read while left, then a stop and a start that bring the same birth back. |
| [executable_dirs.rs](executable_dirs.rs) | caller | `RDM_EXECUTABLE_DIRS` | A node's executable replaced by a Build naming a binding inside the directories the fabric runs with, checked and hashed before anything is sent; a refusal printed by name. |
| [fabric_recovery.rs](fabric_recovery.rs) | caller | `fabric.recovery` | The fabric-primary's process ended unexpectedly while another node-admin lives: the surviving admin re-creates the lost node-admin at its `path.name` over its inherited data directory and restores it as mesh-primary and fabric-primary. |
| [fabric_adoption_launch.rs](fabric_adoption_launch.rs) | caller | `fabric-adoption-launch` | An adoption launch on a fresh data directory (`RDM_NODE_NAME`, `RDM_FABRIC_ID`, `RDM_MESH`, `RDM_STORE_DIR` with no launcher): `AdminConfig::from_env` adopts the live fabric's identity at the lost fabric-primary's `path.name` with a fresh `node_id`, and refuses recovery flags or non-admin paths by name. |
| [fabric_primary_takeover.rs](fabric_primary_takeover.rs) | caller | `fabric-primary-takeover` | A successor fabric-primary taking over and hydrating the immutable Fabric identity, the accepted Build's required Mesh set, and the complete Mesh-status view (`expected == observed`) before publishing Fabric `Ready`, without minting a new Build. |
| [mesh_investigation.rs](mesh_investigation.rs) | caller | `mesh.investigation` | The fabric-primary's investigation of an unheard peer mesh: the ladder's rungs (`probe 1`, `mark`, `probe 2`, `decide`), probes carried through the peer mesh's live role nodes answering `carrier-edge-lost`, re-creation of the peer mesh's node-admins, and the fabric returning to `Ready`. |
| [node_recovery.rs](node_recovery.rs) | caller | `node.recovery` | A member whose runtime process exited unexpectedly: its mesh-primary proves the exit via `node.runtime.inspect`, runs `node.delete` then `node.create` at the same `path.name` over the inherited `data_dir` with a new `node_id` and `incarnation_id`, and keeps the current Build `Complete`. |
| [node_investigation.rs](node_investigation.rs) | caller | `node.investigation` | The two-step node-investigation workflow (`investigate`): probing the exact birth via `node.tickle` first (returning `Finding::Reached` without touching the OS runtime) and, when `tickle` ends `Unreached`, inspecting the node's `RuntimeFact` via `node.runtime.inspect` (`Finding::Running`). |
| [node_tickle.rs](node_tickle.rs) | caller | `node.tickle` | The reach ladder over one exact birth (`tickle`): direct probe (`Tickle::Reached`), via-peer probe (`TickleCalls::via_peer`) carried through an eligible live peer of the node's mesh, `birth_answered`, and the refusal when a node holds no `incarnation_id`. |
| [node_runtime_inspect.rs](node_runtime_inspect.rs) | caller | `node.runtime.inspect` | The runtime inspection workflow (`inspect_runtime`): verifying a live member's `RuntimeFact` (`Inspected::Running`), refusing a foreign control domain (`Inspected::Unavailable`), and inspecting an exited runtime without and with a matching `ExitRecord` (`Exited(None)` and `Exited(Some(code))`). |
| [data_dir_lost_recovery.rs](data_dir_lost_recovery.rs) | caller | `data-dir-lost-recovery` | A member whose runtime process exited and whose inherited `data_dir` is absent: after holding across `RDM_DATA_DIR_GONE_FLOORS` staleness floors (`data_dir_gone_hold_ms()`), the mesh-primary records `data-dir-lost` and recovers the member at the same `path.name` over a newly allocated `data_dir` with a newly minted `endpoint_id`, `node_id`, and `incarnation_id`. |
| [stopped_node_collection.rs](stopped_node_collection.rs) | caller | `RDM_STOPPED_NODE_TTL` | A stopped member collected after `RDM_STOPPED_NODE_TTL`: `nodes.stop` leaves the birth `Stopped` with its stop receipt in the Build log, and the fabric-primary's sweep ages the receipt and submits an ordinary `node.delete` Build once the TTL expires. |
| [on_fabric_ready.rs](on_fabric_ready.rs) | executable | `on_fabric_ready` | The notice the fabric-primary calls once the fabric's `Ready` state is recorded on the Fabric record: registered in `FabricHooks`, fire and forget, carrying `fabric_id`, `build_id`, and `attempt`. |
| [app_gossip_channels.rs](app_gossip_channels.rs) | caller | `app-gossip-channels` | Declared application event types (`types` and `held_types`) on the CloudEvents gossip envelope: `join_channel`, publishing and receiving a typed `Event` with author `Birth`, and refusing undeclared channels (`ChannelError::UndeclaredType`). |
| [topology_view_staleness.rs](topology_view_staleness.rs) | caller | `topology-view-staleness`, `cadences` | The fabric's three cadences (`staleness_floor`, `gossip_interval`, `backbone_gossip_interval`): a silent member marked `PendingReconnect` past `staleness_floor` and returning to `Ready` under the same `node_id` and `incarnation_id` on its next gossip digest. |
| [node_connections_admit.rs](node_connections_admit.rs) | caller | `node.connections.admit` | Inbound connection admission on registered application ALPNs (`validate_alpns`, `CertAdmitter::verify` against `RootSet` on `RafkaTime`, refusing `no-credential` and untrusted roots) and an admitted `demo/echo` dial (`Dialer::open` / `Acceptor::accept`). |
| [node_connections_delete.rs](node_connections_delete.rs) | caller | `node.connections.delete` | Cutting a live member's connections (`Nodes::connections_delete` with `ConnectionScope::All` at its mesh-primary and `Nodes::delete_connections` as a Build streaming `NodeEvent::ConnectionsDeleted`), refusing an empty `ConnectionScope::Connections`, while the member stays `Ready`. |
| [connections_reconnect.rs](connections_reconnect.rs) | caller | `connections-reconnect` | The source-owned Direct reconnect schedule (`reconnect_plan`, `reconnect_backoff`, `deterministic_jitter`, `next_due`, `due`, `next_failure`) reconstructed from indexed `Direct Failed` rows, dropping superseded incarnations and retiring active Proxy routes (`owed_direct_restored`) once Direct reconnects. |

The examples that change the fabric (`node_name_update`, `node_mesh_leave`, `forward`, `node_tags`,
`build_reattach`, `node_primary_update`, `fabric_primary_update`, `executable_dirs`,
`fabric_recovery`, `fabric_primary_takeover`, `mesh_investigation`, `node_recovery`,
`data_dir_lost_recovery`, `stopped_node_collection`) act on the node
they are given; run them against a demo estate, not a fabric that carries traffic.
