# i150 e6.s1: the Rafka consumer audit (rdm#264)

**Story:** i150.e6.s1 ([PRD](i150-node-writes-lifecyle-updates.md) e6; R13, R14). Read-only: nothing
in `/home/admin/rafka-v2` was edited, built or switched; its checkout was read with `git -C`, `grep` and
`cargo metadata --offline --no-deps` (written to `/tmp`, the Rafka checkout's status unchanged before and
after).
**RDM sides compared:** the consumer's pin `21272b3dbbad3cec3ee6edc5b993e3e78225aa68` against this
unit's base `a33b137e7d35e1b939ea41515c38aa72e403f142` (branch `codex/i150-node-records`). The RDM lead
branch has since moved to `33f6bf80b` (the Q322 leadership reverts `9141f55f7`..`74efeb339` and the
Q323 page text); where its shape differs from the base it is named.
**Q323 (Luke, 2026-11-01, delegated; page text `0f73e5df5` on `i150-docs-strip`):** one node record
keyed by node.id alone, append-only, latest line current; the node writes its properties, its
mesh-primary writes status and `is_primary`; the writer's birth is a field, never part of the key; a
node with no record yet has no properties yet. It supersedes rdm#258's lifecycle-only persistence:
every item below that waited on rdm#258 waits on **the Q323 unit** (PRD e3.s2 as Q323 rewrote it).
**Method:** a source read. No Rafka compile was run at a bumped pin (that would write a Rafka target
and edit its `Cargo.toml`); the compile at the bumped pin is e6.s2's first step and its error list is
the complete inventory. The tallies in §2.10 are from `grep`, not from a compiler.

## 1. The consumer baseline

| fact | value |
|---|---|
| checkout | `/home/admin/rafka-v2`, branch `main` |
| HEAD | `8efc3cd0ee4b1878af56b3ec1b7963856b5410d9` (2026-10-31 02:21, "registry: the 126 unexplained production files take their recommended owner …") |
| dirty | 93 entries: `M docs/eng/testing-lane-scripts.md`; `?? payload-audit-report.md`; 91 `?? .claude/agents/*.md`. No source file is modified |
| RDM pin, `Cargo.toml` `[workspace.dependencies]` | `rdm-core`, `rdm-transport`, `rdm-client`, `rdm-node`, `rdm-admin`, `rdm-test`, each `git = "https://github.com/drlukeangel/rust-distributed-mesh", rev = "21272b3dbbad3cec3ee6edc5b993e3e78225aa68"` |
| RDM pin, `Cargo.lock` | the same six packages, each `source = "git+…?rev=21272b3db…#21272b3dbbad3cec3ee6edc5b993e3e78225aa68"` |
| overrides | none: no `[patch]` in `Cargo.toml` or `.cargo/config.toml` |
| the pinned commit | `21272b3db` "docs(rulings): Q301 and Q302 landed (f23588ebb, 7cbbb78a6)", 2026-10-30 20:43; an ancestor of RDM `origin/main` (`a63f75756`, 44 commits later) and 201 commits behind this unit's base |
| Rafka crates importing RDM | `compute`, `gateway`, `node-admin`, `crates/rafka-entity-topology`, `rafka-gateway-core`, `rafka-gateway-mesh`, `rafka-job-door`, `rafka-mesh-ops`, `rafka-node-base`, `rafka-reader`, `rafka-test-harness`, `rafka-writer-ingress`, `rafka-writer-rpc`; 202 `.rs` files name an `rdm_*` crate |

Other worktrees of the same repository (not the baseline; recorded so a later pin bump knows what else
pins RDM):

| worktree | HEAD | branch | RDM rev |
|---|---|---|---|
| `/home/admin/rafka-warm` | `8efc3cd0ee` | detached | `21272b3d` |
| `/home/admin/lane-queue/laneG1` | `00e1d3a990` | `eng/otel-033-rdm-pin` | `21272b3d` |
| `/home/admin/lane-queue/laneG2` | `14ce0ddcc9` | `eng/remove-node-type-layer` | `21272b3d` |
| `/home/admin/lane-queue/laneG3` | `8efc3cd0ee` | `eng/coverage-owners-same-crate` | `21272b3d` |
| `/home/admin/lane-queue/laneG4` | `b37d22574b` | `docs/stub-howto-runbooks` | `90a6f531` |
| `/home/admin/lane-queue/laneG5` | `a9ca4c41e8` | `fix/i141-edge-declaration-cells` | `a213a6a2` |
| `/home/admin/lane-queue/laneG6` | `a47298a6c8` | `eng/i141-e3-s7-public-views` | `a213a6a2` |
| `/home/admin/lane-queue/laneG7` | `44fcc7ffb9` | detached | `5532aabd` |
| `/home/admin/lane-queue/laneG8` | `404f5a6f64` | `eng/i141-pin-revert` | `a213a6a2` |
| `/home/admin/lane-queue/laneG9` | `c9e04df77c` | `eng/i140-e4-read-forward` | none in `Cargo.toml` |
| `/home/admin/lane-queue/laneG12` | `ba3c8b8645` | `fix/tied-reds-2126-2333` | none in `Cargo.toml` |
| `/home/admin/lane-queue/laneGC` | `2d174a8704` | detached | `5532aabd` |
| `/home/admin/.codex/worktrees/i142-migration-definition/rafka-v2` | `6893d1adb5` | `codex/i142-migration-definition` | `a213a6a2` |
| four `/tmp/tmp.*/base` | — | detached, prunable | — |

**The consumer record differs from the pin:** `docs/plans/i141-node-rpc-substrate-prd.md` line 9 says
"**RDM pin:** `9cb7ed88339b0083e4bf0d5268e86d395f6f0857`"; `Cargo.toml` and `Cargo.lock` name
`21272b3db`. The i141 record is the one e6 coordinates (PRD e6 "Record"), so e6.s2's pin bump updates
that line.

## 2. Rafka call sites of the shapes i150 changed

Each row: the RDM change (pin → base), then every Rafka production site by file and symbol, and
whether it breaks the compile or only changes what it means at runtime.

### 2.1 The status wire: `ConfigUpdated`, `ApplyNodeConfig`, `ConfigRowRefused` (D2, D11)

RDM: `StatusReply::ConfigUpdated { node_id, incarnation, config }` (no `revision`);
`StatusRequest::ApplyNodeConfig { node_id, incarnation, config }` (no `revision`);
`StatusReply::ConfigRowRefused { node, reason }` (no `config`, no `revision`); `StatusRequest`
position 34 `NodeConfigUpdated` is gone (refused by name); `rdm_client` `config_apply` takes no
revision; `StatusRequest`'s `operation` fields are `rdm_core::OperationId`, not `String`.

| Rafka site | use | break |
|---|---|---|
| `crates/rafka-node-base/src/rdm_node.rs` `update_own_config` | matches `StatusReply::ConfigUpdated { revision, .. }`, returns `Result<u64, String>` | compile |
| `crates/rafka-gateway-core/src/boot_hydration.rs` `publish_listener_endpoints_on_own_row`, `observe_endpoint_publication` | return `Result<u64, String>` and record `revision` on `rafka.gateway.node.update.via-endpoint-config` and `…-observed` | compile (through `update_own_config`) |
| `node-admin/src/api_base.rs` `publish`, `supervise` | matches `ConfigUpdated { revision, .. }`, records `revision` on `rafka.node_admin.node.update.via-admin-api-base-config`; the module docs say "`ConfigUpdated { revision }`" | compile |
| `node-admin/src/api_base.rs` tests `updated()` | constructs `ConfigUpdated { …, revision }` | compile |

No Rafka site names `ApplyNodeConfig`, `ConfigRowRefused`, `NodeConfigUpdated`, `UpdateNodeConfig`
or `config_apply`.

### 2.2 `TopologyReply::OwnRow.revision`, `JoinControl.config` (D10)

No Rafka site. Rafka's `own_row` / `put_own_row` are `StorageBackend` methods (§2.8), not the
hydration reply.

### 2.3 `NodeView.properties` (Q318) and the `config.*` / tags filters

RDM: `NodeView` gains `placement` and `properties: PropertyRecord` (`Read` by default, or
`Unavailable { reason }`); `config` and `tags` stay. A node whose property record is missing is
listed with `properties: Unavailable` and empty `config`/`tags`, and a `config.*` or `tags` filter
pair never matches it (`rdm_core::filter`). The tags and path indexes are deleted (`b9f2eecc8`): a
`tags` pair matches the property record's tags.

| Rafka site | use | break |
|---|---|---|
| `node-admin/src/front_door.rs` `node_row` (`GET /api/nodes`) | renders `kafka_endpoint`, `rest_endpoint`, `kafka_port`, `rest_port`, `admin_api_base` from `n.config` | runtime: an `Unavailable` node renders as a node that published nothing; `properties` is never read |
| `node-admin/src/front_door.rs` `mesh_nodes_filter` | `tags` pairs; its doc says "RDM matches a `tags` pair on its active tags index" | runtime: the index is gone; a node with no property record yet matches no `tags` pair |
| `node-admin/src/front_door.rs` `node_at` | `Filter::new().eq("path", [node.path()]).eq("name", [node.name()])` | compiles; `path()` / `name()` now return the free-text path and the dot-free name |
| `node-admin/src/front_door.rs` `fabric_nodes`, `mesh_nodes`, `shape_paths`, `held_paths`, `spawn_paths`, `mesh_paths`, `delete_fabric_projections`, `delete_members`, `reset_commander`, and its tests `read`, `gateway_with_config` | read `n.name.mesh`, `n.name.kind`, `n.name.old`, `n.name.ordinal`, `n.name.to_string()` | compile (§2.5) |
| `node-admin/tests/functional/front_door.rs` | `NodeView` reads through `Nodes::get`, `.name.mesh` | compile |
| `crates/rafka-node-base/src/rdm_deletions.rs` `NodeGetEndpoint::endpoint_of` | `node.get [id]`, reads `NodeView.endpoint_id` | none (lifecycle field) |
| `crates/rafka-test-harness/src/peer_mesh.rs` `node_admin_api_bases_from_build_nodes` | reads `admin_api_base` from `GET /api/nodes` JSON | runtime, through `front_door::node_row` |

### 2.4 The own-config route (D8): `RunningNode::config_update` is the node's own writer

RDM: `rdm_client` `NodeRpc::own_config_update` is removed. `RunningNode::config_update` /
`own_config().update(map)` keeps its name and signature and now writes through the node's
`NodePropertyWriter` (span `rdm.node.record.update.via-properties`, outcome `durable`), answering
`ConfigUpdated` once the put is durable or `CallEnd::Rejected { reason: "properties-unwritten" }`;
nothing is owed to, retried at or answered by a primary. The admin's
`rdm_admin::admin::Running::config_update` keeps its signature and ends at
`StatusDoor::own_config_update`, the admin's own property writer.

| Rafka site | use | break |
|---|---|---|
| `crates/rafka-node-base/src/rdm_node.rs` `update_own_config` | `running.node.own_config()` then `own.update(config)` | compile only on `revision` (§2.1); its doc (owed until the mesh-primary answers, sent again on not-ready) describes the removed route |
| `crates/rafka-gateway-core/src/boot_hydration.rs` `publish_listener_endpoints_on_own_row` (spawned from the boot sequence) | the gateway's Kafka and REST endpoints as its whole config, `rafka.kafka-endpoint` and `rafka.rest-endpoint` (`rafka_node_base::rdm_topology::listener_endpoints_config`) | as above; its doc describes the upward route |
| `node-admin/src/main.rs` (the `put` passed to `api_base::supervise`) | `api_base::publish(…, \|config\| running.config_update(config))` | compiles; the publication is now a local durable write |
| `crates/rafka-node-base/src/listen_ports.rs` module docs | "put on the gateway's own row's config through RDM's `node.config.update`" | text only |

No Rafka site calls `own_config_update`.

### 2.5 The separate `PathName { path, name }` pair (e1) and the explicit mesh and kind

RDM: `PathName` is `{ path: String, name: String }` with serde `{path, name}`; `PathName::new(path,
name)` validates; there is no `FromStr`, no `Display`, no `mesh`, `kind`, `ordinal`, `label` or `old`
field, no `base()`, no `node_type()`; `renamed()` is `<name>-old` on the same path;
`numbered(mesh, kind, ordinal)` is the counts convenience. The mesh and kind are explicit:
`MeshNode { mesh, kind, … }`, `Launch { mesh, kind, initial_properties, … }`,
`Launcher { mesh, name, node_id, incarnation }`, `NodeSpec { path, mesh, kind, … }` with
`NodeSpec::default_for(path, mesh, kind)`, `NodeRow { mesh, kind, … }`. Launch environment:
`RDM_NODE_PATH`, `RDM_NODE_NAME` (the name half alone), `RDM_MESH`, `RDM_NODE_KIND`,
`RDM_NODE_PROPERTIES` (`InitialProperties` JSON); `RDM_LAUNCHER` is a structured JSON object.
`rdm_client`: `BuildCarrier::fabric_primary()` is gone; `carrier_following(entry, fabric_primary:
&NodeId)`; `primary_update` / `fabric_primary_update` take a `&NodeId`; `name_update` takes a
`&PathName`.

| Rafka site | use | break |
|---|---|---|
| `crates/rafka-node-base/src/rdm_topology.rs` `node_at_path_name`, `nodes_of_mesh`, `fabric_primary_path_name`, `path_name_of`, `node_type_of`, `node_type_of_name`, `rdm_path_name` / `rafka_path_name`, `digest_for_test` (builds a `MeshNode`) | parse and print a joined `path.name`, read `d.node.name.mesh`, `name.node_type()` | compile |
| `crates/rafka-node-base/src/rdm_node.rs` `prepare_node_process`, `own_mesh`, `own_node_kind`, `own_image_subdir`, `update_own_config`, its `launch_of` test | `launch.launch.name.mesh`, `.kind`, `.to_string()`, a `Launch` and a `Launcher` literal | compile |
| `crates/rafka-node-base/src/topology.rs` (the own-mesh name from `RDM_NODE_NAME`, `PathNameUnresolved`, the `require_path_name(...).parse::<rdm_core::PathName>()` site) | `mesh_of_node_name(RDM_NODE_NAME)`, falling back to `RDM_MESH` | runtime: `RDM_NODE_NAME` is the bare name, `mesh_of_node_name` answers `None`, and the mesh comes from `RDM_MESH`, which the launching admin now writes for every node; the parse is compile |
| `crates/rafka-node-base/src/launch_env.rs` | maps `RDM_NODE_NAME` to `RAFKA_NODE_NAME` through `rafka_path_name` | runtime: carries the bare name |
| `crates/rafka-telemetry/src/lib.rs` (the launch resource attributes) | `RDM_NODE_NAME` → `rdm.node.name` | runtime: the attribute is the bare name; no `RDM_NODE_PATH` attribute |
| `crates/rafka-node-base/src/lib.rs`, `broker_data_op.rs` `own_mesh_carrier`, `job_op.rs`, `job_target_op.rs`, `topic_traffic_close_op.rs`, `write_hooks.rs`, `topics.rs`, `broker_resync_step_op.rs`, `virtual_topic_read.rs`, `attribution.rs`, `connections_storage.rs` | `d.node.name.mesh`, `d.node.name.kind`, `PathName` parse | compile |
| `node-admin/src/front_door.rs` `parse_node`, `shape_paths`, `spawn_paths`, `mesh_paths`, `the_fabric` | `name.parse::<PathName>()`, `PathName::new(mesh, kind, ordinal)`, `NodeSpec::default_for(path)`, `carrier.fabric_primary()` | compile |
| `node-admin/src/build_call.rs`, `start.rs`, `system_image.rs`, `day0.rs`, `wiring.rs`, `storage.rs`, `day0_storage.rs` | `PathName` parse, `.name.mesh/.kind/.ordinal`, `ctx.birth.name.mesh` | compile |
| `mesh-node/src/main.rs` | `node_type_of_name(&launch.launch.name)`, `launch.launch.name.mesh` (the telemetry service name) | compile |
| `compute/src/job_claim_loop.rs` | `d.node.name.mesh` | compile |
| `crates/rafka-gateway-core/src/routing/peer_endpoints.rs` `resolve_peer_gateways` | `d.node.name.mesh == own_mesh` | compile |
| `crates/rafka-entity-topology/src/record.rs` `node_path_index_key`, `key_for` (mesh-members slot) | `name.to_string()`, `self.0.name.mesh` | compile |
| `crates/rafka-reader/src/lib.rs`, `crates/rafka-writer-ingress/src/heartbeat.rs`, `originate.rs`, `test_view.rs` | `PathName` parse, a `MeshNode` literal | compile |
| tests building `Launch` / `Launcher` / `MeshNode` literals | `gateway/tests/functional/support/node_launch.rs`, `node-admin/tests/entity_channels_on_rdm.rs`, `node-admin/tests/fabric_ready_applies_per_mesh.rs`, `crates/rafka-node-base/src/rdm_launch.rs`, `crates/rafka-node-base/tests/{rdm_node,rdm_one_trace,rdm_data_plane}.rs`, `crates/rafka-node-base/tests/functional/support/rdm_node_fixture.rs` | compile |

Rafka's own entity records already carry `name` and `path` apart (`rafka-entity-framework`
`identity.rs`, `Minted::from_minted(id, name, path, props)`), as the proposal says; the moves are in the
RDM-facing references above. Rafka's own `rafka_theme::node_path_name` and
`rafka_mesh_ops::node_path_name` are Rafka's types and move only where they feed an RDM `PathName`.

### 2.6 The node-records split: `NodeRow`, `node-records.jsonl`, the property record, the lifecycle record

RDM at the base: `NodeRow` (`rdm_node::nodes_storage`, the lifecycle row Rafka stores on `ops.nodes`)
has no `config`, `tags` or `revision` and gains `mesh`, `kind`, `initial_properties`,
`replacement_from`. Properties are `rdm_node::node_records::NodeProperties { birth, config, tags,
endpoint_id, transport_addr, listeners, app_types, held, applied }`, keyed by node id; the lifecycle
record is `NodeLifecycle { node_id, writer: RecordBirth, … }` keyed `(node id, writer node id, writer
incarnation)`; both are lines of `node-records.jsonl` on the file provider (`FileNodeRecords`,
`store_root::NODE_RECORDS_FILE`). The base also has `PrimaryRecordBatch` / `put_primary_records` /
`primary_records`, which the lead tip removed (`74efeb339`). **Q323 changes this shape again:**
one record per node id, no writer in the key, `lifecycle(node, writer)` and the current-writer read in
`current_record.rs` removed (PRD e3.s2 as rewritten).

`NodeRowRead::read` (what a running node reads of another node) still answers a `NodeRow`. A running
node has no read of another node's property record at the base.

| Rafka site | use | break |
|---|---|---|
| `crates/rafka-node-base/src/rdm_node.rs` `peer_config` (and `node_row_read`) | another node's config: `reader.read(id)…map(\|r\| r.config)` | compile; and at the base no node-side read returns that config. **Waits on the Q323 unit**: under Q323 the one record at the node id carries the properties; if the read a running node gets after Q323 does not return them, that is an RDM interface request back to the e2/e4 owners (PRD e6 "interface changes return to those owners"), not a Rafka workaround |
| `crates/rafka-gateway-core/src/routing/peer_endpoints.rs` `resolve_one` | the peer's `rafka.kafka-endpoint` through `peer_config` (the Kafka Metadata peer set) | follows `peer_config` |
| `node-admin/src/day0_storage.rs` (the Day-0 switch's node-record copy) | `row.tags` for the tags index copy | compile |
| `crates/rafka-entity-topology/src/store.rs`, `record.rs` | `Node(NodeRow)` stored whole on `ops.nodes`; slots `NODE_PATH_INDEX_SLOT` (2) and `NODE_TAGS_INDEX_SLOT` (4); `node_at`, `index_node_path`, `index_node_tags`, `nodes_tagged` | the trait no longer calls them (§2.8); `ops.nodes` rows written at the pin do not decode at the base (`PathName` is now an object, fields moved): the durable format moved |

### 2.7 Listener and port publication

The gateway's REST and Kafka ports are Rafka's own listeners: the gateway binds them itself, writes them
to its data dir (`listen_ports.rs` `GATEWAY_REST_PORT_FILE`, `GATEWAY_KAFKA_PORT_FILE`) and puts them on
its own config (`publish_listener_endpoints_on_own_row`, §2.4). Rafka declares no RDM listener: no
production `Launch.listeners` entry and no `RDM_LISTENERS` read (the one non-empty literal is a
`NodeRow` fixture in `node-admin/tests/functional/storage_backends.rs`). So the JoinNode `listeners`
report does not carry the gateway's ports. At the base the node's property record carries the bound
RDM listeners (`node.rs` `property_snapshot`, from the own row) and the gateway's config is written by
the node's own `OwnConfig::update`.

**A plan-vs-page difference, by line:** this unit's plan says the join writes no ports
("node-rpc-envelope.md, the JoinNode listeners sentence"). The sentence at the base, at the lead tip
`33f6bf80b`, at `0f73e5df5` and at `origin/main` ends "…on its assigned IP address with a nonzero
OS-selected port; it writes them on the birth's row." (`docs/architecture/node-rpc-envelope.md`, the
`listeners` paragraph under `JoinNode`). No text was changed here; the page text is the lead's to give.

### 2.8 An application-provided storage backend (R13, e6.s3)

RDM traits Rafka implements: `rdm_admin::storage_backend::StorageBackend` (`node-admin/src/storage.rs`
`RafkaStorageBackend`, `node-admin/src/day0_storage.rs` `Day0StorageBackend`,
`node-admin/tests/day0_file_then_topics.rs` `WrittenFile`); `rdm_admin::storage::NodesStorage`
(`RafkaNodesStorage`, `day0_storage.rs` `Forward<dyn NodesStorage>`); `FabricStorage`, `MeshStorage`,
`ConnectionsStorage` (`crates/rafka-node-base/src/connections_storage.rs` `RafkaConnectionsStorage`).

| RDM change (pin → base) | Rafka sites | break |
|---|---|---|
| `NodesStorage` loses `node_at`, `index_path`, `index_tags`, `tagged` | `RafkaNodesStorage` and `Forward<dyn NodesStorage>` implement all four; `day0_storage.rs` copy calls them; `storage_backends.rs` `nodes_storage_claims_names_and_answers_its_indexes_after_their_writes` and `day0_file_then_topics.rs` assert them | compile (methods not in the trait) |
| `StorageBoundaries` gains `node_records: Arc<dyn rdm_node::node_records::NodeRecordsStorage>` | `RafkaStorageBackend::open`, `Day0StorageBackend` | compile |
| `NodeRecordsStorage` (new): `put_properties`, `properties`, `put_lifecycle`, `lifecycle(node, writer)`, `lifecycles_of`, `lifecycles(mesh)`; at the base also `put_primary_records` / `primary_records` | none yet | new trait to bind; **its shape waits on the Q323 unit** (the writer key and `lifecycle(node, writer)` go) |
| `StorageBackend` methods unchanged (`qualify`, `own_row`, `put_own_row`, `open`, `delete_fabric`, `lines`) | as today | none |

**Where a launched node's properties land (a source fact the binding has to answer).** Rafka's
node-admin registers `RafkaStorageBackend` under RDM's `file` provider name
(`node-admin/src/start.rs` `providers`), so the admin's boundaries are Rafka's planes. Rafka's
launched nodes start on `rdm_node::storage_backend::NodeStorageProviders::file()`
(`rdm_node.rs` `prepare_node_process`), RDM's `FileNodeBackend`, whose `node_records` at the base is
`FileNodeRecords` on the launch's `RDM_STORE_DIR` store root. A gateway's own config therefore goes to
`node-records.jsonl` on that root, while the admin reads properties for `node.get` from whatever
`StorageBoundaries.node_records` Rafka supplies. Unless both are the same store, the admin answers the
gateway's properties `Unavailable` (Q321's per-process case, storage.md §2). Whether Rafka's
node-side reads (`NodeRowRead`, also on the file root) see the rows its admin writes on `ops.nodes`
today is not established by this read: no run, no span; unattributed.

### 2.9 Other RDM shapes the pin bump moves (not i150's list, found on the way)

`StatusRequest::CreateTags` / `DeleteTags` keep their shape and are served by the subject's own
property writer (`status_rpc.rs`, `node_config.rs` `own_tags`); `node-admin/src/broker_sync.rs`
`put_node_tags` compiles as is. `rdm_client` `Tracked::new` / `operation()` take and return
`OperationId`, and `overlay(node_id, action)` takes an action. A Rafka use of those is a compile site
the bump lists.

### 2.10 Tally (grep, per file, not a compile)

| category | files |
|---|---|
| `.name.mesh` / `.name.kind` / `.name.old` / `.name.ordinal` / `.name.label` | 30 (rafka-node-base 13, node-admin 9, rafka-gateway-core 3, rafka-entity-topology 2, mesh-node, rafka-reader, compute 1 each) |
| `PathName` parse (`FromStr`) | 10 |
| `PathName::new(mesh, kind, ordinal)` | 3 |
| `Launcher { name, … }` literal | 8 |
| `Launch { … }` literal | 9 |
| `NodeSpec::default_for` | 3 |
| `ConfigUpdated { revision, … }` | 2 (plus `update_own_config`'s pattern) |
| `NodesStorage` index methods | 8 |
| `BuildCarrier::fabric_primary()` | 1 |

## 3. Test targets that cover each site

Package / test target / filter, from `cargo metadata --offline --no-deps`.

| site | covering cells |
|---|---|
| §2.1, §2.4 admin publication | `rafka-node-admin` lib, `api_base::tests`: `publish_sends_the_complete_owned_map_once_and_returns_the_revision`, `publish_refused_subject_unknown_is_an_error_naming_node_operation_and_why`, `owned_config_is_the_same_key_set_for_every_base`, `supervise_a_published_put_lets_the_life_continue_until_a_stop`, `supervise_a_refused_put_ends_the_life_by_name`, `supervise_observes_each_end_while_the_put_is_outstanding` |
| §2.1, §2.4 gateway publication | `rafka-gateway-core` lib, `boot_hydration::tests`: `publication_completes_only_on_config_updated_and_rafka_never_republishes_after_a_not_ready_answer`, `a_publication_no_node_answers_is_refused_by_name_and_never_recorded_updated`, `the_boot_sequence_observes_a_completed_publication_with_its_revision`, `the_boot_sequence_names_a_refused_publication_with_its_detail`, `the_boot_sequence_names_a_publication_task_that_panicked_or_was_cancelled`, `different_kafka_and_rest_hosts_are_published_as_written`, `advertised_endpoint_takes_its_own_host_and_the_held_port_or_names_why_it_is_not_published` |
| §2.4, §2.7 config keys | `rafka-node-base` lib, `rdm_topology::tests`: `a_gateways_listener_endpoints_config_carries_its_published_endpoints_and_reads_them_back`, `the_topology_source_refuses_without_a_book_and_answers_from_the_book_installed` |
| §2.6 peer read | `rafka-gateway-core` lib, `routing::peer_endpoints::tests` (5 cells, `metadata_peer_set_*`, `a_gateway_holding_no_own_node_id_advertises_itself_alone_and_never_its_own_row`); `rafka-gateway-wire` test `functional`, `metadata_peer_gateways::` (5 cells) |
| §2.3 `GET /api/nodes` | `rafka-node-admin` lib, `front_door::tests` (`node_row` renders, `shape_paths`, `delete_*`); `rafka-node-admin` test `functional`, `front_door::` |
| §2.5 launch, digest, path pair | `rafka-node-base` tests `rdm_node`, `rdm_one_trace`, `rdm_data_plane`, `rdm_carried_read`, `rdm_carried_write`, `functional` (`support::rdm_node_fixture`); `rafka-node-base` lib `topology::tests`, `rdm_launch::tests`; `rafka-node-admin` tests `entity_channels_on_rdm`, `fabric_ready_applies_per_mesh`, `functional` (`build_call::`); `rafka-gateway` test `functional` (`support::node_launch`) |
| §2.6, §2.8 storage boundaries | `rafka-node-admin` test `functional`, `storage_backends::` (20 cells, among them `storage_backend_opens_the_five_boundaries_on_rafkas_planes`, `nodes_storage_claims_names_and_answers_its_indexes_after_their_writes`, `each_boundary_stores_the_whole_record_it_is_handed_and_a_second_write_replaces_it_with_no_read_before`, `the_own_seed_is_kept_whole_in_the_admins_data_dir`); test `day0_file_then_topics` (6 cells); test `rdm_start_on_rafka_backend` (`rdm_node_admin_on_rafkas_backend_keeps_its_fabric_and_first_build_on_rafkas_planes`); `rafka-entity-topology` lib `store::tests` (`the_path_and_tags_indexes_answer_only_what_their_own_writes_put` and 3 more) |
| real-process ports | `rafka-e2e` test `peer-kafka-addrs` (`every_gateway_publishes_its_kafka_and_rest_endpoints`, `metadata_through_one_gateway_lists_every_other_gateway_at_its_published_endpoint`); test `mesh-route-discovery` (`gateway_resolves_peer_client_endpoints_through_node_row_without_announce`, `a_peers_published_endpoint_is_visible_through_another_gateways_node_row_read`, `a_kafka_client_reconnects_through_an_advertised_peer`). None is `#[ignore]`d |

### The planned consumer cell `a_gateways_own_ports_survive_build_resumption`

It exists in neither repository today. **Exact target:** package `rafka-e2e`, test target
`node-lifecycle__replace` (`tests/e2e/tests/node-lifecycle__replace.rs`, feature `node-lifecycle`),
in a module of its own beside `replace_follows_path_name`; run through the driver that file names,
`scripts/run-estate-cells.sh <lane> node-lifecycle node-lifecycle__replace
--deployment=multi-node a_gateways_own_ports_survive_build_resumption`. The cell replaces a gateway, so
it cannot attach to a standing estate (the file's own reason); `peer-kafka-addrs` attaches to one.

What it reproduces, from the spans of the Q305 trace `3281ce5542afc8de74aff606b410ef6b` (laneG8
estate, RDM `21272b3`, Build `bid_z3g5-8csw-3m3h`; ClickHouse `otel.otel_traces` on .99), each line a
span:
- 03:19:25.874 `mesh1.node-admin` `rafka.node_admin.api.serve.via-front-door` → `rdm.node_admin.build.create.via-node-rpc`;
- 03:19:26.380 `rdm.node_admin.build.update.via-claim-decision` `won` attempt 1; `node.delete.via-build`, then 03:19:26.593 `node.create.via-build` and `node.create.via-node-row` of `mesh1.gateway.1`;
- 03:19:28.959 `mesh1.gateway` `rafka.gateway.node.update.via-endpoint-config` (the successor's own publication; matched by service and time, it is in another trace);
- 03:19:29.085 attempt 1 `deployment.update.via-step` `Complete`; 03:19:29.092 `lifecycle.update.via-transition` `mesh1.gateway.1` Pending → Ready;
- 03:19:29.112 `via-claim-decision` `won` attempt 2; 03:19:29.151 `build.update.via-reconcile` operations `node.config.update:mesh1.gateway.1`; 03:19:29.155 `node.update.via-config` `keys=0` `config-updated`; 03:19:29.163 `mesh1.gateway` `node.update.via-apply-node-config` `keys=0` `revision=9` `applied`;
- 03:19:29.197 `build.update.via-on-build-ended` `complete`.

So the second attempt is the Build's own continuation, with no crash, and its entry's empty map landed
over the successor's own endpoints. The cell posts `POST /api/build {shape, shape_bins}` with a gateway
binary of another sha256 (Q157: a held path whose row records another executable is replaced), waits
for the Build's end, and asserts Q305 B: the successor's `node.get` properties are `Read` and carry
exactly the endpoints its own `rafka.gateway.node.update.via-endpoint-config` published; no Build
property apply of the same Build lands on the successor after its own
`rdm.node.record.update.via-properties`; the predecessor's ports never appear. It runs on process
first, then container (PRD e6 estate plan). Its RDM-side twin is
`node_records::a_build_applies_properties_and_a_nodes_later_update_stands`.

**A Rafka difference found on the way:** `tests/e2e/tests/node-lifecycle__replace.rs`
`a_replaced_broker_keeps_its_path_name_and_its_topics_follow_it_with_no_rewrite` posts
`POST /api/nodes/{victim}/replace`; `node-admin/src/front_door.rs` `router` at `8efc3cd0e` routes no
`/replace` (its node routes are `/api/nodes`, `/api/nodes/spawn`, `/api/nodes/{node_name}`, and
replacement goes through `/api/build`). That cell is not `#[ignore]`d. It is Rafka's, not i150's; the
new cell drives replacement through `/api/build`.

## 4. The e6.s2 and e6.s3 dispatch plan

**One pin, one SHA.** Rafka's workspace compiles only when every site compiles at the pinned SHA, and
`node-admin` implements the storage traits, so the pin cannot move to a SHA whose `NodeRecordsStorage`
Q323 is about to change. **The pin bump waits on the Q323 unit** and names the RDM SHA after it
lands (PRD wave 6: "Pin completed RDM interfaces"). The source changes below that are marked
*stable at the base* can be written before then; none can be compiled or proven before the bump. One
Rafka owner for e6.s2, then one for e6.s3; they share `node-admin/src/storage.rs` and
`day0_storage.rs`, so they run in sequence, not at once.

### e6.s2: consumer pair/env wiring and the node's own property write

| step | files (Rafka) | what | waits on | cells |
|---|---|---|---|---|
| 0 | `Cargo.toml`, `Cargo.lock`, `docs/plans/i141-node-rpc-substrate-prd.md` (the pin line) | six `rev`s to the post-Q323 RDM SHA; `durable format moved` in the commit (`ops.nodes` rows, `PathName` object) and a fresh estate (PRD "Format changes") | **the Q323 unit** | the compile's error list is the inventory |
| 1 | §2.5 rows: `rafka-node-base` (`rdm_topology.rs`, `rdm_node.rs`, `topology.rs`, `launch_env.rs`, `lib.rs`, the op files listed), `rafka-telemetry/src/lib.rs`, `node-admin` (`front_door.rs`, `build_call.rs`, `start.rs`, `system_image.rs`, `day0.rs`, `wiring.rs`), `mesh-node/src/main.rs`, `compute/src/job_claim_loop.rs`, `rafka-gateway-core/src/routing/peer_endpoints.rs`, `rafka-entity-topology/src/record.rs`, `rafka-reader`, `rafka-writer-ingress`; the listed test fixtures | node id first; `(path, name)` carried apart; mesh and kind from `MeshNode.mesh/kind`, `Launch.mesh/kind`, `RDM_MESH`, `RDM_NODE_KIND`; `RDM_NODE_PATH` read beside `RDM_NODE_NAME` (proposal "The environment"); `NodeSpec::default_for(path, mesh, kind)`; `carrier_following(&entry, &NodeId)`; no joined-string parse or print of an RDM `PathName` | stable at the base | `rafka-node-base` tests `rdm_node`, `rdm_one_trace`, `rdm_data_plane`, `rdm_carried_read`, `rdm_carried_write`, `functional`; lib `topology::`, `rdm_launch::`, `rdm_topology::`; `rafka-node-admin` lib `front_door::`, tests `functional`, `entity_channels_on_rdm`, `fabric_ready_applies_per_mesh` |
| 2 | `rafka-node-base/src/rdm_node.rs` `update_own_config`; `rafka-gateway-core/src/boot_hydration.rs`; `node-admin/src/api_base.rs`; `rafka-node-base/src/listen_ports.rs`, `rdm_topology.rs` docs | the own write: no `revision` in the return, the spans or the docs; success is the durable local put; a `properties-unwritten` refusal ends the publication by name; nothing is owed to a primary | stable at the base | `rafka-node-admin` lib `api_base::` (the `…returns_the_revision` cell renamed with a `renamed:` line); `rafka-gateway-core` lib `boot_hydration::` (the not-ready resend cell rewritten: no primary answers it) |
| 3 | `node-admin/src/front_door.rs` `node_row`, `mesh_nodes_filter` | render `properties: Unavailable { reason }` by name, never as "published nothing"; the tags-index doc corrected | stable at the base | `rafka-node-admin` lib `front_door::` (a new `Unavailable` render cell) |
| 4 | `rafka-node-base/src/rdm_node.rs` `peer_config`; `rafka-gateway-core/src/routing/peer_endpoints.rs` `resolve_one` | another gateway's endpoints from its record at its node id; a record not yet written excludes the peer by name | **the Q323 unit** (the one record a running node reads) | `rafka-gateway-core` lib `routing::peer_endpoints::`; `rafka-gateway-wire` `functional` `metadata_peer_gateways::` |
| 5 | `node-admin/src/storage.rs` `RafkaStorageBackend::open`, `day0_storage.rs` | the compile minimum of §2.8 (remove the four index methods; supply `node_records`), with admin reads and node writes on one store (§2.8) | **the Q323 unit** (trait shape) | `rafka-node-admin` `functional` `storage_backends::`, test `day0_file_then_topics`, `rdm_start_on_rafka_backend` |
| 6 | `tests/e2e/tests/node-lifecycle__replace.rs` | `a_gateways_own_ports_survive_build_resumption` (§3), process then container | steps 0–5 | `run-estate-cells.sh … node-lifecycle node-lifecycle__replace --deployment=multi-node a_gateways_own_ports_survive_build_resumption`; then `rafka-e2e` `peer-kafka-addrs` and `mesh-route-discovery` on the same estate |

e6.s2's evidence: the green cells above, the e6.s2 cell's Jaeger trace showing the successor's own
`rdm.node.record.update.via-properties` as the last property write of the Build and no
`rdm.node_admin.config.update.via-call` from the gateway (the removed route), and the pin SHA recorded
in the i141 record.

### e6.s3: the application provider's node records and the i149 copy

All of e6.s3 **waits on the Q323 unit**: it binds `NodeRecordsStorage` on Rafka's planes, whose
key and methods Q323 changes.

| step | files (Rafka unless named) | what | cells |
|---|---|---|---|
| 1 | `node-admin/src/storage.rs` (a `NodeRecordsStorage` over a Rafka plane, opened in `RafkaStorageBackend::open`), `crates/rafka-entity-topology/src/{record,store}.rs` (the record kind and its key at node id; slots 2 and 4 out of RDM's use) | whole records stored and returned as given, a node id's latest line current (Q323), nothing decided in storage (Q109) | `rafka-node-admin` `functional` `storage_backends::` (the boundary cell; `storage_backend_opens_the_five_boundaries_on_rafkas_planes` becomes six); `rafka-entity-topology` lib `store::` |
| 2 | `crates/rafka-node-base/src/rdm_node.rs` `prepare_node_process`, `node-admin/src/start.rs` `providers` | the node side registers a `NodeStorageBackend` on the same plane under the provider name the launch carries, so a gateway's property write lands where the admin and peers read | `rafka-node-base` tests `rdm_node`, `functional`; the e6.s2 cell re-run on the Rafka provider |
| 3 | `node-admin/src/day0_storage.rs` (the Day-0 switch copy and `Forward`), `node-admin/tests/day0_file_then_topics.rs` | the switch copies `node-records.jsonl` (RDM `storage_copy::NodeRecords`) whole, properties and lifecycle; no path or tags index copy; with RDM [i149](i149-topology-storage-provider.md)'s inventory | test `day0_file_then_topics` (a node-records case in `day0_admin_writes_the_file_store_until_the_switch_and_the_topics_after_it`, `the_records_reader_the_boot_sets_reaches_the_day0_file_log`) |
| 4 | RDM: `tools/mesh-audit/i143-acceptance-jobs.json` (with the lead), `docs/plans/i149-topology-storage-provider.md` | register the consumer and migration cells; record the application-provider acceptance i149 points at i150 for | the migration full-state regression (PRD e6) on a real gateway and the Rafka provider, process then container |

The R13 RDM-side cell `node_records::all_node_fields_survive_node_restart` stays RDM's; Rafka's cells
prove only that its backend stores and returns RDM's records whole (i141 U3.2 "Rafka tests no RDM
feature").

## 5. What this read does not establish

- No Rafka compile at a bumped pin; the §2.10 tally and the "compile" column are a source read.
- No run: whether Rafka's node-side reads see its admin's `ops.nodes` writes today (§2.8) is
  unattributed.
- The proposal's Rafka-section coordinates have drifted by a few lines; they are recorded here by
  symbol: `rdm_node.rs` `update_own_config` (the `ConfigUpdated { revision }` match), `rdm_topology.rs`
  `REST_ENDPOINT_CONFIG_KEY` / `KAFKA_ENDPOINT_CONFIG_KEY`, `rafka-entity-framework` `identity.rs`
  `Minted::from_minted`, `rdm_deletions.rs` `NodeGetEndpoint`.
