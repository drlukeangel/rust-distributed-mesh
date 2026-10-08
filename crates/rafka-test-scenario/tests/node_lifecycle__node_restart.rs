//! Restart canary: product=mesh, feature=node-lifecycle, subfeature=node-restart, rung=multi-node
//! (MN). The provider is `MESH_SPAWN_TYPE` (process by default); the body is
//! `rafka_test_scenario::canary::restart_canary`.

use rafka_test_scenario::canary::restart_canary;
use rafka_test_scenario::estate::Owner;

/// CONTRACT: an RPC node restarted through Build comes back as the same logical
/// node (same node id and transport identity, new process incarnation) on fresh
/// ports (fabric-node-lifecycle.md: a restart binds fresh ports), still serves the
/// value written before the restart from its own data dir, resets an unfinished
/// request with 499 (NotSent), and leaves a Build -> deployment -> node-lifecycle
/// span chain linked by ParentSpanId.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_node_restarts_same_identity_rebinds_and_recovers_state() {
    restart_canary(Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "node-restart".into(),
        rung: "multi-node".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "rpc_node_restarts_same_identity_rebinds_and_recovers_state".into(),
    })
    .await;
}
