//! i143.e1.s7 process E2E: the current desired topology outlives the Builds
//! that set it, reaches every admin without Build history, and drives a new
//! reconciliation Build when an exact runtime is proven exited
//! (rafka-v2#2851).
//!
//! From public surfaces and the estate's evidence only:
//! 1. Build A converges; `GET /api/fabric` names its desired revision.
//! 2. Forgetting A removes its history, not the desired topology.
//! 3. An admin born after A hydrates the current revision before Ready and
//!    holds none of A's history.
//! 4. A birth stopped (silent, its runtime alive) is held: no replacement.
//! 5. An admin that missed a revision while stopped catches up to it.
//! 6. A birth killed (its exact runtime exited) is proven drift: the fabric
//!    authority starts a new reconciliation Build B against the unchanged
//!    revision, never A's id, and B converges the shape back.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-desired".into(),
        subfeature: "desired-topology".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_desired_topology_outlives_its_builds_and_drives_recovery".into(),
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn signal(pid: u64, sig: &str) {
    let ok = std::process::Command::new("kill").args([sig, &pid.to_string()]).status().unwrap().success();
    assert!(ok, "kill {sig} {pid}");
}

async fn desired(estate: &Estate) -> Value {
    estate.get("/api/fabric").await.1["desired"].clone()
}

async fn build(estate: &Estate, path: &str, body: Value) -> String {
    let (status, a) = estate.post(path, &body).await;
    assert_eq!(status, 202, "{a}");
    let id = a["build_id"].as_str().unwrap().to_string();
    estate.await_build(&id, Duration::from_secs(120)).await;
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_desired_topology_outlives_its_builds_and_drives_recovery() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    assert_eq!(desired(&estate).await["revision"], 1, "Day 0 roots the desired topology");

    // 1. Build A sets the shape.
    let a = build(&estate, "/api/build", json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let d = desired(&estate).await;
    assert_eq!((d["revision"].clone(), d["source_build_id"].clone()), (json!(2), json!(a)), "{d}");
    assert_eq!(d["desired"]["meshes"], json!([{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]));

    // 2. Forgetting A's history leaves the desired topology.
    let (status, _) = estate.delete(&format!("/api/builds?id={a}")).await;
    assert_eq!(status, 204);
    assert_eq!(estate.get(&format!("/api/builds?id={a}")).await.0, 404);
    assert_eq!(desired(&estate).await, d, "forget is history administration only");

    // 3. An admin born after A: it learns revision 3 (its own Build's) at
    //    entry, never A's receipts.
    let c = build(&estate, "/api/nodes/spawn", json!({"mesh": "mesh1", "kind": "node_admin"})).await;
    let nodes = estate.settled_shape(&[("mesh1", 3, 3)], Duration::from_secs(30)).await;
    let late = nodes.iter().find(|n| n["name"] == "mesh1.admin.3").cloned().unwrap();
    let late_base = late["admin_api_base"].as_str().unwrap().to_string();
    let (_, late_fabric) = estate.http_get(&late_base, "/api/fabric").await;
    assert_eq!((late_fabric["desired"]["revision"].clone(), late_fabric["desired"]["source_build_id"].clone()), (json!(3), json!(c)));
    assert_eq!(estate.http_get(&late_base, &format!("/api/builds?id={a}")).await.0, 404, "no completed Build history reaches it");

    // 4. A held birth that falls silent is not drift.
    let held = estate.pid_of("mesh1.rpc.3").await;
    let quiet_from = now_ns();
    signal(held, "-STOP");
    wait_for("mesh1.rpc.3 is silent in the view", Duration::from_secs(15), || async {
        (estate.node("mesh1.rpc.3").await["status"] == "dead").then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    signal(held, "-CONT");
    let quiet_until = now_ns();
    wait_for("mesh1.rpc.3 is heard again", Duration::from_secs(20), || async {
        (estate.node("mesh1.rpc.3").await["status"] == "ready-for-traffic").then_some(())
    })
    .await;

    // 5. An admin that misses a revision catches up when it is back. It
    // holds no seat: pausing a seat holder moves the seat first, and a Build
    // planned after that replaces the unheard birth (the path fence).
    let quiet = estate.nodes().await.into_iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] == false && n["is_fabric_primary"] == false).expect("a mesh1 admin that holds no seat");
    let (quiet_name, quiet_base) = (quiet["name"].as_str().unwrap().to_string(), quiet["admin_api_base"].as_str().unwrap().to_string());
    let stopped = estate.pid_of(&quiet_name).await;
    signal(stopped, "-STOP");
    let r4 = build(&estate, "/api/build", json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 3, "rpc_node": 4}]})).await;
    assert_eq!(desired(&estate).await["revision"], 4);
    let resumed_at = now_ns();
    signal(stopped, "-CONT");
    wait_for("the paused admin holds revision 4", Duration::from_secs(30), || async {
        (estate.http_get(&quiet_base, "/api/fabric").await.1["desired"]["revision"] == 4).then_some(())
    })
    .await;
    estate.settled_shape(&[("mesh1", 3, 4)], Duration::from_secs(30)).await;

    // 6. A killed birth is proven drift: a new Build, the same revision.
    let lost = estate.node("mesh1.rpc.2").await;
    estate.kill_node("mesh1.rpc.2").await;
    let back = wait_for("mesh1.rpc.2 is reborn", Duration::from_secs(60), || async {
        let n = estate.node("mesh1.rpc.2").await;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != lost["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(desired(&estate).await["revision"], 4, "recovery changes no desired revision");
    estate.settled_shape(&[("mesh1", 3, 4)], Duration::from_secs(30)).await;

    estate.stop().await;
    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap();
    let drift = named(&spans, "rafka.node_admin.build.create.via-proven-drift");
    // 4: nothing while the stopped birth was only silent.
    assert!(drift.iter().all(|sp| at(sp) < quiet_from || at(sp) > quiet_until), "a silent, live birth started a recovery: {drift:?}");
    // 6: exactly one recovery, for the killed birth, against revision 4.
    assert_eq!(drift.len(), 1, "{drift:?}");
    let b = &drift[0]["attributes"];
    assert_eq!(b["desired_revision"], "4");
    assert_eq!(b["source_build_id"], r4.as_str());
    assert_eq!(b["scope"], "mesh1.rpc_node: 3 present of 4 desired (exited: mesh1.rpc.2)");
    assert_eq!(b["reason"], "proven-drift");
    let recovery = b["reconcile_build_id"].as_str().unwrap();
    assert!(![a.as_str(), c.as_str(), r4.as_str()].contains(&recovery), "a new Build, never a completed one's id");
    let fabric_primary = b["authority"].as_str().unwrap();
    // B ran under the same revision and converged; it created the birth.
    let ran = named(&spans, "rafka.node_admin.build.update.via-reconcile");
    let converged = ran
        .iter()
        .find(|sp| sp["attributes"]["build_id"] == recovery && sp["attributes"]["outcome"] == "converged")
        .unwrap_or_else(|| panic!("recovery {recovery} converged"));
    assert_eq!(converged["attributes"]["reason"], "proven-drift");
    assert_eq!(converged["attributes"]["desired_revision"], "4");
    assert!(converged["attributes"]["executor"].as_str().is_some_and(|e| !e.is_empty()));
    assert!(
        named(&spans, "rafka.node_admin.node.create.via-build").iter().any(|sp| sp["attributes"]["build_id"] == recovery && sp["attributes"]["node"] == "mesh1.rpc.2"),
        "the recovery Build created mesh1.rpc.2"
    );
    assert_ne!(back["node_id"], lost["node_id"], "a lost birth's path gets a new logical node");
    let _ = fabric_primary;
    // 3: the late admin hydrated before Ready. 5: the paused admin caught up.
    let ready = named(&spans, "rafka.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node"] == "mesh1.admin.3").cloned().expect("mesh1.admin.3 is ready");
    let hydrated = named(&spans, "rafka.node_admin.desired_topology.update.via-hydration")
        .into_iter()
        .chain(named(&spans, "rafka.node_admin.desired_topology.update.via-catch-up"))
        .filter(|sp| sp["attributes"]["node"] == "mesh1.admin.3")
        .map(|sp| (at(sp), sp["attributes"]["desired_revision"].clone()))
        .collect::<Vec<_>>();
    assert!(hydrated.iter().any(|(t, r)| *t < at(&ready) && *r == "3"), "revision 3 before Ready: {hydrated:?}");
    let caught_up = named(&spans, "rafka.node_admin.desired_topology.update.via-catch-up")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == quiet_name.as_str())
        .map(|sp| (at(sp), sp["attributes"]["desired_revision"].clone()))
        .collect::<Vec<_>>();
    assert!(caught_up.iter().any(|(t, r)| *t > resumed_at && *r == "4"), "{quiet_name} caught up revision 4 after it resumed: {caught_up:?}");
}
