//! i143 R-G6 acceptance (Luke 2026-10-08, locked X): every Ready blocker caused by missing control
//! facts is recoverable through a request the blocked node can repeat. PROCESS layer, at the test
//! cadence (staleness 3 s, gossip 500 ms), `RDM_ARTIFACTS_DIR` set.
//!
//! The estate is the testkit's faulted node-admin: bootstrap (Day 0 mesh1 with one admin), then one
//! Build of mesh1 with three node-admins and no rpc node.

use rafka_test_scenario::estate::{named, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-rg6".into(),
        subfeature: "fetch-build-facts".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// The boot file of a joining admin: it arms `spec` before the admin's first line runs.
fn arm_at_birth(estate: &Estate, admin: &str, id: &str, spec: Value) {
    let dir = estate.root.join("faults");
    std::fs::create_dir_all(&dir).unwrap();
    let mut spec = spec;
    spec["id"] = json!(id);
    std::fs::write(dir.join(format!("{admin}.boot.json")), serde_json::to_vec(&json!([spec])).unwrap()).unwrap();
}

/// CONTRACT: every admin withholds the catch-up it owes a neighbour that comes up on the Build
/// topic (the seam, nothing else). The two joining admins of the Build know the current Build from
/// the Build their join's entry answer named (blocker `no-current-build-wanted`: the Fabric row carries
/// no pointer to the newest Build, R-BI3). They hold the Build's facts from none of their neighbours'
/// catch-up: each one's own Build log cannot read that Build (the Build journal is per admin, R14
/// answer 1), so each fetches its facts with `FetchBuildFacts` (0x1F), absorbs them into its own Build
/// log and is Ready, and the Build completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joiners_whose_build_catch_up_is_lost_fetch_the_facts_and_the_build_completes() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(owner("joiners_whose_build_catch_up_is_lost_fetch_the_facts_and_the_build_completes"), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"])
        .await
        .expect("the faulted-admin binding set is accepted");
    let day0 = estate.node_view("mesh1.admin.1").await;
    Door::open(&estate.root, "mesh1.admin.1", day0.admin_api_base.as_deref().expect("the Day-0 admin advertises its door")).await.arm("catch-up-withheld", json!({"kind": "catch-up"})).await;
    for joiner in ["mesh1.admin.2", "mesh1.admin.3"] {
        arm_at_birth(&estate, joiner, "catch-up-withheld", json!({"kind": "catch-up"}));
    }

    let build = estate.reconcile_shape("fabric1", &[("mesh1", 3, 0)]).await.build_id.to_string();
    estate.stop().await;

    let spans = estate.spans();
    let withheld = named(&spans, "rdm.node_admin.build.reject.via-catch-up-withheld");
    assert!(!withheld.is_empty(), "the catch-up seam ran: no neighbour's catch-up was offered to the topic");
    // No joiner was caught up by a neighbour: each of the Build's two joining admins names the
    // blocker it fetched for (the current Build its entry answer named, absent from its own Build log) and
    // absorbed a complete stream from a node-admin.
    let fetched = named(&spans, "rdm.node_admin.build.update.via-fetch-facts");
    for joiner in ["mesh1.admin.2", "mesh1.admin.3"] {
        let mine: Vec<&&Value> = fetched.iter().filter(|sp| sp["attributes"]["node"] == joiner && sp["attributes"]["outcome"] == "absorbed").collect();
        assert!(!mine.is_empty(), "{joiner} absorbed the Build's facts from a FetchBuildFacts read: {fetched:#?}");
        assert!(
            mine.iter().all(|sp| sp["attributes"]["blocker"] == "no-current-build-wanted" && sp["attributes"]["build_id"] == build.as_str()),
            "{joiner} fetched for the current Build, named by its entry answer: {mine:#?}"
        );
        let served: Vec<Value> = named(&spans, "rdm.node_admin.build.serve.via-fetch-facts").into_iter().filter(|sp| sp["attributes"]["node"] == mine[0]["attributes"]["target"]).cloned().collect();
        assert!(served.iter().any(|sp| sp["attributes"]["complete"] == "true" && sp["attributes"]["outcome"] == "served"), "{joiner}'s responder served a complete snapshot: {served:#?}");
    }
}

/// CONTRACT: the reduced stem of the mesh-create race, with nothing withheld: bootstrap, one Build of
/// mesh1 with three node-admins and no rpc node, and the Build completes with both joining admins Ready.
/// However the Build topic's catch-up fares for a joiner (delivered, or lost to a duplicate connection
/// closing), the joiner hydrates its Build's facts, so the Build is never left waiting on an admin that
/// reports Pending. Run in a loop of thousands by the release campaign (`RG6_STEM_RUNS` is the loop's
/// own count, not read here: each run is one process).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_of_three_node_admins_completes_with_every_joiner_ready() {
    let mut estate = Estate::bootstrap(owner("a_build_of_three_node_admins_completes_with_every_joiner_ready"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 3, 0)]).await;
    estate.settled_views(&[("mesh1", 3, 0)]).await;
    estate.stop().await;
}
