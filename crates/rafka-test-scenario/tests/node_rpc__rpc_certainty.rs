//! The exact seed scenario (PRD §6): product=mesh, feature=node-rpc, subfeature=rpc-certainty,
//! rung=multi-node (MN). Runs `scenarios/i143-node-rpc-seed.yaml` through the scenario runner over
//! real Node RPC, on `RDM_SCENARIO_PROVIDER` (else `MESH_SPAWN_TYPE`, else the scenario's own).

use rafka_test_scenario::{runner, scenario::Scenario};

/// CONTRACT: the PRD §6 seed, brought up as MN through node-admin Build, applies
/// its eight Put/CAS/Delete operations by path over real Node RPC and reads back
/// exactly the three asserted values.
#[tokio::test(flavor = "multi_thread")]
async fn seed_scenario_applies_every_operation_over_real_node_rpc() {
    let scenario = Scenario::parse(include_str!("../scenarios/i143-node-rpc-seed.yaml")).expect("seed parses");
    let provider = std::env::var("RDM_SCENARIO_PROVIDER").or_else(|_| std::env::var("MESH_SPAWN_TYPE")).ok();
    let report = runner::run(&scenario, provider.as_deref(), "seed_scenario_applies_every_operation_over_real_node_rpc").await;
    assert!(report.failures.is_empty(), "seed failures: {:#?}", report.failures);
}
