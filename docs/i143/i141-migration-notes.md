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
