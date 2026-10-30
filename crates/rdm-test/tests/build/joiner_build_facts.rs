//! i143 R-G6 acceptance (Luke 2026-10-08, locked X): every Ready blocker caused by missing control
//! facts is recoverable through a request the blocked node can repeat. PROCESS layer, at the test
//! cadence (staleness 3 s, gossip 500 ms), `RDM_ARTIFACTS_DIR` set.
//!
//! The estate: bootstrap (Day 0 mesh1 with one admin), then one Build of mesh1 with three node-admins
//! and no rpc node.

use rdm_test::estate::{Estate, Owner};

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-rg6".into(),
        subfeature: "build-floor-read".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// CONTRACT: the reduced stem of the mesh-create race, with nothing withheld: bootstrap, one Build of
/// mesh1 with three node-admins and no rpc node, and the Build completes with both joining admins Ready.
/// The joiner hydrates its Build's facts from the store root, so the Build is never left waiting on an admin that
/// reports Pending. Run in a loop of thousands by the release campaign (`RG6_STEM_RUNS` is the loop's
/// own count, not read here: each run is one process).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_of_three_node_admins_completes_with_every_joiner_ready() {
    let mut estate = Estate::bootstrap(owner("a_build_of_three_node_admins_completes_with_every_joiner_ready"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 3, 0)]).await;
    estate.settled_views(&[("mesh1", 3, 0)]).await;
    estate.stop().await;
}
