//! i143.e3.s4 process E2E: live resize above and back to the floor (PRD §10).
//!
//! On running fabrics, each resize is one `POST /api/build`:
//! - MN rpc cohort 3 -> 5 -> 4 -> 7 -> 3, then node-admin cohort 2 -> 3 -> 2;
//! - MM: mesh1 and mesh2 grow and shrink independently.
//!
//! After every step, from public surfaces only:
//! - the Build completes and the view settles on exactly the desired count of
//!   each cohort, all `ready-for-traffic` (which ordinals remain depends on
//!   where the seats are: shrink retires non-primaries);
//! - every seat is the one the election computes from the view's NodeIds
//!   (one primary per cohort, the lowest ready NodeId; one fabric primary);
//!   shrink retires non-primaries, highest ordinal first;
//! - no two nodes advertise the same endpoint (no port collision);
//! - a mesh the step did not resize keeps every node's birth (incarnation).
//!
//! Evidence, by parent span id: every retired node has a
//! `rafka.node_admin.node.delete.via-build` span that descends from the
//! request that accepted its Build, with a retire pipeline under it.

use rafka_test_scenario::elections::seats_as_expected;
use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-shapes".into(),
        subfeature: "live-resize".into(),
        rung: if test.contains("mm") { "MM".into() } else { "MN".into() },
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// One resize step and what it retired.
struct Step {
    build_id: String,
    retired: Vec<String>,
}

/// Submit `meshes` as the fabric's desired shape and check the step settled.
async fn resize(estate: &Estate, label: &str, meshes: &[(&str, u32, u32)], before: &[Value]) -> (Step, Vec<Value>) {
    let desired = json!({
        "fabric": "fabric1",
        "meshes": meshes.iter().map(|(m, a, r)| json!({"name": m, "node_admin": a, "rpc_node": r})).collect::<Vec<_>>(),
    });
    let (status, accepted) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{label}: {accepted}");
    let build_id = accepted["build_id"].as_str().unwrap().to_string();
    estate.await_build(&build_id, Duration::from_secs(120)).await;
    // Exactly the desired nodes, all ready.
    let nodes = estate.settled_shape(meshes, Duration::from_secs(15)).await;
    // Every seat is the computed one.
    if let Err(e) = seats_as_expected(&nodes) {
        panic!("{label}: {e}: {nodes:#?}");
    }
    // No port collision: every advertised endpoint is unique.
    let mut seen = BTreeSet::new();
    for n in &nodes {
        for e in n["endpoints"].as_array().unwrap() {
            let addr = e["addr"].as_str().unwrap().to_string();
            assert!(seen.insert(addr.clone()), "{label}: {addr} advertised twice");
        }
    }
    // A node that survived the step kept its birth (nothing restarted).
    let births = |ns: &[Value]| -> BTreeMap<String, String> {
        ns.iter().map(|n| (n["name"].as_str().unwrap().to_string(), n["incarnation_id"].as_str().unwrap_or("").to_string())).collect()
    };
    let (old, new) = (births(before), births(&nodes));
    for (name, birth) in &new {
        if let Some(prev) = old.get(name) {
            assert_eq!(prev, birth, "{label}: {name} was restarted by a resize");
        }
    }
    let retired = old.keys().filter(|n| !new.contains_key(*n)).cloned().collect();
    (Step { build_id, retired }, nodes)
}

/// Every retired node's removal descends from the request that accepted its Build.
fn retirements_are_causal(estate: &Estate, steps: &[Step]) {
    let spans = estate.spans();
    for step in steps {
        let accepted = named(&spans, "rafka.node_admin.build.create.via-rest")
            .into_iter()
            .find(|s| s["attributes"]["build_id"] == step.build_id.as_str())
            .unwrap_or_else(|| panic!("no span accepted build {}", step.build_id))
            .clone();
        for node in &step.retired {
            let delete = named(&spans, "rafka.node_admin.node.delete.via-build")
                .into_iter()
                .find(|s| s["attributes"]["node"] == node.as_str() && s["attributes"]["build_id"] == step.build_id.as_str())
                .unwrap_or_else(|| panic!("{node}: no removal under build {}", step.build_id))
                .clone();
            assert!(descends_from(&spans, &delete, &accepted), "{node}: its removal descends from its Build");
            let retire = named(&spans, "rafka.node_admin.deployment.update.via-pipeline")
                .into_iter()
                .find(|s| s["attributes"]["node"] == node.as_str() && s["attributes"]["pipeline"] == "retire" && descends_from(&spans, s, &delete));
            assert!(retire.is_some(), "{node}: a retire pipeline ran under its removal");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mn_resizes_live_and_back_to_floor_without_flap_or_collision() {
    let mut estate = Estate::bootstrap(owner("mn_resizes_live_and_back_to_floor"), "fabric1", "mesh1").await;
    let mut steps = Vec::new();
    let mut nodes = Vec::new();
    for (label, admins, rpcs) in
        [("MN", 2, 3), ("rpc 5", 2, 5), ("rpc 4", 2, 4), ("rpc 7", 2, 7), ("rpc 3", 2, 3), ("admin 3", 3, 3), ("admin 2", 2, 3)]
    {
        let (step, now) = resize(&estate, label, &[("mesh1", admins, rpcs)], &nodes).await;
        steps.push(step);
        nodes = now;
    }
    estate.artifact("nodes.json", &json!(nodes));
    estate.stop().await;
    retirements_are_causal(&estate, &steps);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mm_meshes_grow_and_shrink_independently() {
    let mut estate = Estate::bootstrap(owner("mm_meshes_grow_and_shrink_independently"), "fabric1", "mesh1").await;
    let mut steps = Vec::new();
    let mut nodes = Vec::new();
    for (label, a, b) in [
        ("MM", (2, 3), (2, 3)),
        ("mesh2 grows", (2, 3), (3, 5)),
        ("mesh1 grows, mesh2 shrinks", (2, 5), (2, 2)),
        ("mesh1 back to floor", (2, 3), (2, 2)),
        ("mesh2 back to floor", (2, 3), (2, 3)),
    ] {
        let (step, now) = resize(&estate, label, &[("mesh1", a.0, a.1), ("mesh2", b.0, b.1)], &nodes).await;
        steps.push(step);
        nodes = now;
    }
    estate.stop().await;
    retirements_are_causal(&estate, &steps);
}
