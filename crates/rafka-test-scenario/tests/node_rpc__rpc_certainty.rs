//! SECOND RED (i143 PRD §6, story i143.e7.s2): the exact seed scenario.
//!
//! product=mesh, feature=node-rpc, subfeature=rpc-certainty, rung=multi-node (MN),
//! provider=process. Runs `scenarios/i143-node-rpc-seed.yaml` through the scenario
//! runner over real Node RPC. Goes GREEN in i143.e7.s5.

use rafka_test_scenario::{runner, scenario::Scenario};

/// CONTRACT: the PRD §6 seed, brought up as MN through node-admin Build, applies
/// its eight Put/CAS/Delete operations by path over real Node RPC and reads back
/// exactly the three asserted values.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "SECOND RED (i143.e7.s2): goes GREEN in i143.e7.s5 once Build, the providers, the RPC node and the probe exist"]
async fn seed_scenario_applies_every_operation_over_real_node_rpc() {
    let scenario = Scenario::parse(include_str!("../scenarios/i143-node-rpc-seed.yaml")).expect("seed parses");
    let provider = std::env::var("RAFKA_SCENARIO_PROVIDER").ok();
    let report = runner::run(&scenario, provider.as_deref(), "seed_scenario_applies_every_operation_over_real_node_rpc").await;
    assert!(report.failures.is_empty(), "seed failures: {:#?}", report.failures);
}
