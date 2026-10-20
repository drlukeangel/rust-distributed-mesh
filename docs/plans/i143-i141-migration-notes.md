# i141 migration notes — what the e11 proof-of-concept learned

Working notes kept while e11 re-bases RDM's own role binaries on the imported substrate. Each row
is a thing i141 will have to do to rafka-v2, found by doing it here first. Rows are appended as
e11 stories land; nothing here is a design, each row points at the code that proved it.

## 1. The node base is the proof node's composition, not a runtime of its own

The product base is `rafka-node-rpc-testkit::node::start` with the role's families composed in
(`crates/rafka-node-base/src/lib.rs::run`). A base that keeps its own endpoint, gossip loop,
digest publisher, peer registry and tag demux (RDM's old 2,040-line `rafka-node-base`,
rafka-v2's `crates/rafka-node-base/src/lib.rs`, ~26,000 lines) is a second substrate beside the
imported one. i141 e3/e4/e10 replace it; nothing of it is kept as a delegate except what the
PRD names (readiness/auth/handlers, i141 PRD §3).

What a role process needs, all of it from the launch node-admin hands it (`Launch::from_env`):
its path.name, node id, incarnation, transport address, seeds, data dir, mesh id. rafka-v2's
roles read `RAFKA_MESH_ID`, `RAFKA_NODE_NAME`, `RAFKA_NODE_BIND_ADDR`, `RAFKA_SEED_NODES`,
`RAFKA_DATA_DIR` and mint their own key; i141 e4 moves them onto the launch contract
(`rafka-mesh-entity::launch`).

## 2. Node kinds

A role is a `NodeKind` (`Broker`, `Gateway`, `Compute`; segments `broker`/`gateway`/`compute`),
the names RDM's old node-base `Role` and rafka-v2's node-base `Role` both already carry.
rafka-v2's `Role::Registry` and `Role::Admin` map onto `RpcNode` (a plain product node) and
`NodeAdmin`. Path names are unchanged (`mesh1.broker.1`).

## 3. One sealed catalog per process, composed where the server is built

`compose(role, b, client)` = core protocols + the role's families + the product's transitional
adapters, sealed once by `ServerBuilder::seal`. The product's own families are ledgered under its
own `TagOwner::Product` name (`families::product_ledger`); the legacy tags it still serves are
`CatalogEntry::transitional` rows naming their i142 unit. rafka-v2's node-base `catalog.rs`
(landed, e7.s1) does the adapters half; e7.s2 moves it into the server as here.

A transitional adapter seals under the owner the core ledger reserves the tag for (`rafka` for
0x10–0x19), never under the composing product's own name: `seal` refuses an owner mismatch by
tag (`OwnerMismatch`), so `rafka_node_base::LegacyAdapter` carries its ledger owner beside its
migration unit (`crates/rafka-node-base/tests/role_process.rs`). The product's own families
(`broker_data`, 0x20) are ledgered under its own `TagOwner::Product` name.

## 4. Consumers of the old base that are not nodes

The old runtime had two readers that were not nodes: `bridge` and `admin-ui`. Both read its
process globals (`live_digests`, `message_ring`, `topic_membership`, the `GossipDigest` row,
`NodeRuntime` with an observer role). Neither survives as a reader of the base:

- `bridge` is deleted (`docs/i143/e0-workspace-audit.md`, `dead-deleted`). Its one behavior,
  joining several mesh ids and recording each peer's mesh, is the fabric's own: every node holds
  every mesh's nodes by gossip and a peer mesh is reached over the backbone.
- `admin-ui` is a node-admin client. It binds no Iroh endpoint and joins no gossip topic; it
  owns its own telemetry init (`rafka_mesh_telemetry::init_telemetry`); `/api/topology`,
  `/api/heartbeats`, `/api/timeline` and `/api/cluster/summary` read node-admin's
  `GET /api/nodes` view (`rafka_node_admin_client::NodeView`: kind, mesh, node id, status, seat,
  incarnation, declared state). What the view does not carry is gone with its surface, not
  stubbed: the gossip frame ring (`/api/messages` and the Messages tab), topic-membership edges,
  per-digest frame counters and CPU/RAM budgets (`admin-ui/src/main.rs::KnownNode`).

rafka-v2 has the same class: every reader of node-base internals that is not a node process
(the topology UI, harness fixtures that decode digests, `rafka-chaos` primitives over the Admin
UI's HTTP). i141 e10 lists them; each becomes a `rafka-node-admin-client` consumer with the
view's fields, or is deleted with the surface it fed. A field the view does not carry is not
re-derived client-side from gossip.

## 5. What the role process still needs from its provider

`start_with_client` awaits the provider's runtime record in the node's data dir before it binds
(`rafka_mesh_entity::runtime::await_own_record`): the process does not describe its own runtime,
its provider does. rafka-v2's roles are born by node-admin's providers already (i138); i141 e4
keeps that, and the harness births that spawn a role by hand must write the record first or go
through node-admin.

## 6. A role is born by node-admin under its kind, and nothing else changes

node-admin resolves the executable per `NodeKind` (`crates/rafka-node-admin-core/src/admin.rs`,
`rafka-broker`/`rafka-gateway`/`rafka-compute` beside `rafka-rpc-node`), plans counts per kind
from `MeshDesired` (`build.rs::MeshDesired::counts`), elects per `(mesh, kind)` cohort
(`election.rs`) and retires a role's path by the same Build diff as an rpc node's
(`crates/rafka-test-scenario/tests/mesh_shapes__role_build.rs`). The role's boot span
(`rafka.mesh.node.create.via-deployment`) and ready span (`rafka.mesh.node.update.via-ready`)
carry `kind`. rafka-v2's node-admin equivalents (`node_type`, `display_prefix_for_node_type`,
the per-type spawn in `rafka-node-admin`) collapse onto `NodeKind` in i141 e4; the `brk_`/`gtw_`/
`cmp_` display prefixes are the REST rendering of the same kind, not a second enum.

## 7. What the role base takes from the testkit, and what stays product

- The probe (`rafka-rpc-probe`) speaks the testkit's oracles (proof store 0x70, resolve probe,
  declare probe). A role that the proof estate's cells call must serve them exactly as the
  generic rpc node does (`rafka_node_base::Oracles`); carrying a forwardable family catalogues it
  for forwarding but serves no handler (`SealedCatalog::lookup` lists served tags only), so a
  carried oracle answers 421. rafka-v2's roles serve no testkit oracle: its e2e harness calls
  product families, and i141 e9 decides which proof oracles, if any, ride a Rafka node.
- A product family's handler names its serving node from the launch (`served_by`), never from the
  environment; the launch is the one input a role process has.
- The seat a role reads is node-admin's view (`rafka-node-admin-client` `nodes()`,
  `is_primary`), one read, no comparator (`rafka_node_base::leadership`, its grep cell). rafka-v2's
  `compute_self_is_primary` and every `is_primary` computation outside node-admin go in i141 e3.
- The certainty outcomes a product sees are the client's typed `RpcOutcome` reasons; staging a
  hang or a fault for a cell is the proof family's own key convention, nothing in the substrate.

## 8. Observability and silence need nothing from the product

- One call through a carrier is one trace across three processes: the caller's span is the
  root, the carrier's inner invocation and the target's serve span descend from it, and
  `caller_system` rides the header verbatim (`node_rpc__role_context`). rafka-v2's gateway and
  broker spans join this trace by using the imported client and server; nothing product-side
  propagates context by hand (every `TRACEPARENT`/`traceparent` plumbing outside the substrate is
  i141 e6 deletion).
- A wedged role is marked, tickled, held and returned by node-admin alone
  (`mesh_runtime__role_wedge`); the role declares its readiness through the testkit's declare loop
  in `start_with_client`. rafka-v2's own offline ladder (`node-admin/src/lib.rs::OfflineTickle`)
  and its readiness declarations are what i141 e3/e6 replace with the imported ones.

## 9. The storage interfaces are awaited; a product binds async backends

i141 e3.s1 binds Rafka's durable topics to `fabric.storage`, `mesh.storage`, `nodes.storage`,
`connections.storage` and `builds.storage`. Rafka's store is async (a point read and a durable
put both await the broker), so the four interfaces are `async_trait` like `builds.storage`
(`crates/rafka-node-admin-core/src/storage.rs`, `fabric_storage.rs`); every holder that read them
synchronously is awaited too (`AcceptedStore::{record, build_id, point}`,
`ShutdownControl::{open, learn, initiate}`, `hydration_blocker`, the entry answer in
`rafka-mesh-transport::entry::EntryServer`, which carries the Fabric record). A sync sink that
must record a contact (`TopologySink::publish`) writes on its own task and names a refusal there.
rafka-v2's backends implement the awaited traits directly: no blocking shim, no second structure
fed from a read.

## 10. What binding the storage interfaces to Rafka's planes found (i141 e3.s1)

- Rafka's `Node` row holds its name as the ordinal and its path as `<mesh>.<type>`; the
  imported `PathName` is the whole `path.name`, and spells the node-admin type `admin` where
  Rafka spells it `node-admin`. One mapper (`node-admin/src/storage.rs::rdm_path_name`) is the
  only place the two spellings meet; i141 e4 decides whether Rafka adopts `NodeKind`'s segments.
- Rafka's `Node` row carries no transport address (it rides the digest); a contact to dial is a
  row plus the address this process hears for its transport identity. i141 e4 (RuntimeFact) is
  where the row learns its address.
- A Rafka plane with virtual topics (`ops.nodes.connections`) is written only through the
  classifying transport: a cell over a `TestBroker` takes the classifying wrapper
  (`test_support::connection_store_over`), as the production store does.
- Rafka's connection facts are an append log keyed by minted id; "the latest per route" is the
  reader's fold, and a route's end is a Disconnected fact: `remove_connection` is refused by name.
- Build facts and the fabric shutdown are their own planes (`ops.builds`, `ops.fabrics.shutdown`),
  each row named by its identity so the create door's claim is the insert-and-fail the imported
  contract asks for; `Fabric.build_id` is a field on the Fabric row moved through the versioned
  door (a durable format move).

## 11. What rafka-v2's node-admin does at a node's entry today, and the RDM surface each part moves onto (read 2026-10-20, at the export SHA `abc1ec15`)

Read against rafka-v2 `77fdfd4948` (`node-admin/src/{lib,main}.rs`, `crates/rafka-node-base/src/{birth,rejoin,kind_scope,write_hooks}.rs`,
`crates/rafka-entity-framework/src/raw_projection.rs`, `crates/rafka-mesh-ops/src/rafka_time.rs`) so that i141/i142 replace a
behaviour with its RDM counterpart, never with a second copy. The rows that read this section: rafka-v2 i142 PRD "Moved from RDM
i143" (the entry pull), i141 PRD decision 4 and gap G22.

### The projection cache and the pull at entry

- **Today.** node-admin "listens to every projection channel, because it refills projections at birth and restart"
  (`raw_projection.rs`): one `RawProjectionStore` per entity kind, every v2 projection frame captured raw whether or not the
  process holds a typed store (`enable_raw_projection_capture`, `capture_raw_projection`; a higher version wins, then a higher
  offset, a value beats a tombstone, tombstones kept `RAW_TOMBSTONE_RETENTION`). A newborn pulls its own kinds
  (`kind_scope::own_kind_set`, carried in its digest as `kind_set`) through `ControlFrame::ProjectionPull` (tags 16/18/19),
  node-admin answers `serve_projections(kinds)` (`birth.rs`), the node applies through `birth::apply_projections`
  (`rejoin.rs::pull_projections_from`).
- **RDM.** The set a node-admin holds is declared, never captured: a member's JoinNode carries its `types`, the admin's join
  path holds (joins) each one before it answers (`rdm-admin/src/join.rs` the `types` step → `Membership::hold_app_types_from`
  → `apps.hold`), and another admin's digest `held_hash` makes it read that admin's `node.get` and hold what it learns
  (`admin/boot.rs` `via-types-read` / `via-held-types`). The embedded app learns each newly held kind from
  `Membership::held_type_changes()` and takes that kind's `Channel` with `Membership::join_channel(kind)`; frames heard between
  the hold and the app's join are buffered by the admin's subscription (1,024 per kind, `gossip.rs` `QUEUE`; past that
  `Channel::dropped` counts). Gate cells: `app_channels.rs`
  `a_node_admin_reports_a_members_new_type_on_its_held_watch_and_its_app_receives_an_event_published_before_the_join` and
  `a_late_declaration_joins_before_the_admin_answers_and_a_peer_hash_reads_node_get_to_grow_held`; the cache shape and the
  newborn's pull inside `before_ready` are `demo/tests/projections.rs` (gossip.md §8, "Entry ends with a pull").
- **What moves.** The raw store and its apply rule stay Rafka's (the cache is the app's, RDM holds no row of it); its feed
  becomes one `Channel` subscriber per held kind, started from the held watch, and `enable_raw_projection_capture` goes. The
  pull becomes a Rafka projection op served through `Wiring::serve_app` and called from the newborn's `before_ready` with
  `BeforeReadyCtx::{authority, client, membership}` (the i142 row; its stated blocker, RDM Q62, landed `6a3f65c6`/`702ddd9e`).
  `kind_set` leaves the digest: the declaration rides JoinNode and `node.get` answers `types` and `held`; the one hash on a
  digest is `held_hash`. G22's second half ("the ordinary-node top-up must bind to the entry pull") is the order node-start.md
  fixes: the hook's pull first, the topology read (the top-up) after it, rafka-time refreshed last.
- **Two things RDM does differently, for i141 to decide, not to paper over.** (1) A node-admin holds only the kinds some live
  node or admin has declared; today it captures every frame on the wire. The same set while the projectors live; a kind no
  live node projects is not held. (2) Where a restarted node-admin refills from: today a peer admin's entry; in RDM a launched
  or restarted admin's hook has `authority` = the admin that accepted its join and pulls from it, while a fabric-primary
  recovered from its durable map has `authority: None` and completes from local state (fabric-node-hooks.md "What the context
  carries"), so its raw store starts empty and fills as it hears the channels it holds.

### The member cert

- **Today.** node-admin issues at spawn and at restart only (`main.rs::issue_member_cert`, a fixed 86,400 s, under a per-mesh
  `IssuingKey` map it mints or renews from the root), hands it in the environment (`RAFKA_NODE_CERT`, `RAFKA_CA_PUBKEY`), and
  never renews a live node's. The node's fail-closed "SVID belt" (`rafka-node-base/src/lib.rs` ~2681) verifies every inbound
  peer member → issuer → the one CA key.
- **RDM (i145).** Issued by the accepting authority at every accepted JoinNode, the first start and every start after a stop
  (`node.cert.create`, fabric-certs.md "When a cert is issued"), carried in the join answer (`JoinControl::member_cert`) with the
  fabric's policy beside it (`JoinControl::cert_policy`: `RDM_MEMBER_CERT_TTL_SECS`, `RDM_ISSUING_CERT_TTL_SECS`,
  `RDM_CERT_RENEW_AT`), read by the app from `BeforeReadyCtx::member_cert` and the running node handle, renewed by its issuer
  at two thirds of its TTL (`node.cert.renew`; the mesh issuer by `mesh.issuer.renew`), the root set a node trusts in
  `RDM_ROOTS`, the admin's material `CertChoice::Ca` (`Wiring::certs`; `NoCerts` only by name). On every inbound connection of
  an ALPN the app registered, RDM reads and verifies the dialer's member cert and admits or drops it
  (`node.connections.admit`; fabric-certs.md "On a connection").
- **What moves.** `issue_member_cert_with`, the issuing map, the env handoff and the SVID belt go; the cert stays what the app
  presents outward. i141 PRD decision 4 ("no cert is verified between mesh nodes") now reads: not on RDM's own two ALPNs
  (Node RPC, gossip), where identity is the endpoint key and admission is the JoinNode check; on every app ALPN, RDM verifies.
  Renewal is new behaviour: rafka-v2 has no counterpart, and a Rafka node that caches its cert bytes re-reads them from the
  handle after `node.cert.renew`.

### rafka-time

- **Today.** A process global (`rafka_mesh_ops::rafka_time`: `adopt_rafka_time`, `rafka_time_now_ms`, and `judgement_now_ms`,
  which answers the OS clock as `LocalBootstrap` before adoption). node-admin samples it last when it builds a served entry
  (`node-admin/src/lib.rs::build_served_entry_with_clock`: `ServedEntry::admin_time_ms`) and refuses the entry by name when it
  holds none (`rafka.mesh.entry.reject.via-rafka-time-not-adopted`, `MeshNotReady`); the node adopts `admin_time_ms` from the
  `EntrySnapshot` frame.
- **RDM.** One `rdm_transport::clock::RafkaTime` per process, composed in at start
  (`start_with_rafka_time_before_ready_alpns_and`), adopted from the join answer (`JoinControl::rafka_time_ms`; an admin that
  holds none answers `NotReady` naming why, the same refusal), kept on the authority's time by heartbeats, refreshed last in
  node.start, on `BeforeReadyCtx::rafka_time` and the running node, read with `now_ms()`; a birth's first JoinNode is judged on
  the local clock named `local-bootstrap` (fabric-certs.md "Time"). Every RDM gossip stamp reads this instance.
- **What moves.** The three globals become reads of the handle (`OrderKey.writer_timestamp_ms` from it, fabric-node-hooks.md
  "rafka-time"); `judgement_now_ms`'s `LocalBootstrap` arm is RDM's `local-bootstrap`; the entry's clock sampling and refusal
  are the join answer's. Nothing of the discipline is Rafka's to keep.

### The rest of the served entry

`ServedEntry { admin_time_ms, served_by, payload, serve_elapsed_ms }` is answered by `JoinControl`: `served_by`, the Fabric
identity, the current Build and its attempt (`BuildFloor`: the joiner's copy must reach it before Ready), the admitted
`name` and the row's `config`, the provider, rafka-time, the cert and the cert policy (`rdm-node/src/wire.rs`). The
projection payload is the one part that is not RDM's, and it is the pull above.
