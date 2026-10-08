//! i143 soak: a seeded schedule of legal actions over an MM estate, judged on invariants.
//!
//! product=mesh, feature=fabric-soak, subfeature=seeded, rung=MM.
//!
//! The schedule, the continuous proof-store traffic, the invariants, the operation ledger's
//! reconciliation and the failure report (seed, executed sequence, minimized legal reproduction)
//! are the soak driver's (`rafka_test_scenario::soak`); the 30-minute acceptance cells over it are
//! `tests/i143_acceptance_2787.rs`. This stem runs the same driver for `RAFKA_SOAK_SECS` seconds
//! (default 60) of `RAFKA_SOAK_SEED` (default random; the seed is printed so a run is exactly
//! rerunnable) on the provider `MESH_SPAWN_TYPE` names.

use rafka_test_scenario::estate::{Estate, Owner};
use rafka_test_scenario::soak::{self, Config, Driver};

/// CONTRACT: for `RAFKA_SOAK_SECS` seconds of one seed, the driver's legal random actions and
/// continuous proof-store operations leave every invariant holding and every issued operation
/// accounted for by the ledger. What must NOT happen: a violated invariant, an operation without an
/// outcome, a lost applied write, a runtime left running. A failure names the seed and prints the
/// minimized reproduction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seeded_fault_schedule_holds_every_invariant() {
    let secs: u64 = std::env::var("RAFKA_SOAK_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let seed: u64 = std::env::var("RAFKA_SOAK_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or_else(|| std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1);
    eprintln!("SOAK seed={seed} secs={secs}  (rerun: RAFKA_SOAK_SEED={seed} RAFKA_SOAK_SECS={secs})");
    let provider = std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into());
    let owner = Owner { product: "mesh".into(), feature: "fabric-soak".into(), subfeature: "seeded".into(), rung: "MM".into(), provider, test: "a_seeded_fault_schedule_holds_every_invariant".into() };
    let estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let backbone = rafka_mesh_transport::membership::backbone_gossip_interval();
    let budget = match rafka_node_rpc::CallOptions::default().budget {
        rafka_node_rpc::Budget::Overall(d) => d,
        other => panic!("the default call budget is not one overall deadline: {other:?}"),
    };
    let tickle_round = budget * (1 + rafka_node_admin_core::offline::VIA_PEER_TICKLE_FANOUT as u32);
    let driver = Driver::new(estate, Config::mm(seed, secs, floor, backbone, tickle_round)).await;
    let (report, mut estate) = driver.run().await;
    eprintln!("{}", soak::summary(&report));
    estate.artifact("soak-report.json", &serde_json::to_value(&report).unwrap());
    estate.stop().await;
    let left = estate.live_runtimes();
    if let Some(repro) = &report.repro {
        eprintln!("seed {seed}: minimized reproduction of {} ({} of {} actions): {:#?}", repro.rule, repro.minimized.len(), repro.original_len, repro.minimized);
    }
    assert!(report.ok(), "seed {seed}: {:#?} / ledger {:#?}", report.violations, report.ledger.violations);
    assert!(left.is_empty(), "seed {seed}: no runtime of the estate is left running: {left:?}");
    assert_eq!(estate.live_containers(), Vec::<(String, String)>::new(), "seed {seed}: no container of the estate is left running");
}
