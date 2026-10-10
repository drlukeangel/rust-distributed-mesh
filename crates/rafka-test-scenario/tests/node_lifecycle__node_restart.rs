//! Restart canary: product=mesh, feature=node-lifecycle, subfeature=node-restart, rung=multi-node
//! (MN). The provider is `MESH_SPAWN_TYPE` (process by default); the body is
//! `rafka_test_scenario::canary::restart_canary`.

use rafka_test_scenario::canary::restart_canary;
use rafka_test_scenario::estate::Owner;

/// CONTRACT: an RPC node restarted through Build is stop then start on the parked process: the same
/// node id, incarnation, endpoint key and port, the same process; it still serves the value written
/// before the restart from its own data dir, resets an unfinished request with 499 (NotSent), and
/// leaves a Build -> node operation -> start step -> start span chain linked by ParentSpanId.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state() {
    restart_canary(Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "node-restart".into(),
        rung: "multi-node".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state".into(),
    })
    .await;
}
