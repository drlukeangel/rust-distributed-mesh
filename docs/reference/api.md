# API documentation (rustdoc)

The API documentation of each crate, built by `scripts/docs-site.sh` with
`cargo doc --no-deps --workspace` beside this guide. Each crate's front page says where it sits and
gives a starter example.

| crate | what it is |
|---|---|
| [`rdm_core`](../../api/rdm_core/index.html) | ids and identity, the Fabric → Mesh → Node model, the topology view, the Node RPC protocol contract and the wire bodies |
| [`rdm_transport`](../../api/rdm_transport/index.html) | gossip on iroh, the Node RPC runtime (client, server, pool, admission), wire framing, telemetry export |
| [`rdm_node`](../../api/rdm_node/index.html) | the node runtime an application's process kinds run on, and its hooks |
| [`rdm_admin`](../../api/rdm_admin/index.html) | node-admin: Build, deployment providers, the lifecycle workflows |
| [`rdm_client`](../../api/rdm_client/index.html) | the typed objects a caller outside the fabric uses |
| [`rdm_test`](../../api/rdm_test/index.html) | the test kit: estates of real node processes, probes, proof protocols and fault doors |

The tools: [`rdm_docgen`](../../api/rdm_docgen/index.html) (writes the generated reference pages),
[`rafka_mesh_audit`](../../api/rafka_mesh_audit/index.html) (the audit gates) and
[`rdm_test_runner`](../../api/rdm_test_runner/index.html) (the test inventory and runs).

The Node RPC wire bodies, the gossip `Frame` enum and every status enum are stated on the types
themselves: each body variant's documentation names its postcard position, and each status
variant its serialized name. The op codes are on
[`rdm_core::protocol`](../../api/rdm_core/protocol/index.html).
