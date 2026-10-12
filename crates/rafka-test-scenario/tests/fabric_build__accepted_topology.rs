//! i143 process E2E: `Fabric.build_id -> complete Build` is the one accepted topology (RDM #47).
//!
//! From public surfaces and the estate's evidence only:
//! 1. Day 0 accepts the first Build; `GET /api/fabric` names it and `GET /api/builds?id=` holds
//!    its exact topology.
//! 2. A change compiles to the next complete Build and moves the pointer; a second change while
//!    it reconciles is refused `409 build-in-progress`, naming it.
//! 3. The Build the pointer names cannot be forgotten; older Builds are history and can.
//! 4. An admin born later holds `Fabric.build_id` and the Build it names before Ready, and
//!    none of the history: the pointer is the fabric-primary's row on the fabric's store root
//!    (R14: the fabric-primary is the one writer of `fabric/`), the Build its own Build log's copy.
//! 5. A birth stopped (silent, its runtime alive) is held: nothing is reopened.
//! 6. An admin that missed a Build while stopped catches up to the pointer: it reads the row the
//!    fabric-primary wrote while it was stopped and holds the Build it names.
//! 7. A member killed (its exact runtime exited) is node recovery (node-recovery.md): its
//!    mesh-primary proves the exit and submits the recovery, which is the next attempt of the same
//!    Build, once: node.delete of the exited birth, then node.create at the same path.name with a new
//!    node.id over the inherited data dir and endpoint key. The pointer does not move; the attempt
//!    converges; the fabric-primary opens no drift attempt for the member.
//! 8. A restart changes no topology: it is an attempt of the accepted Build, under the same id.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "accepted-topology".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_build_fabric_build_id_names_is_the_one_accepted_topology".into(),
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn signal(pid: u64, sig: &str) {
    let ok = std::process::Command::new("kill").args([sig, &pid.to_string()]).status().unwrap().success();
    assert!(ok, "kill {sig} {pid}");
}

async fn pointer(estate: &Estate) -> String {
    estate.get("/api/fabric").await.1["build_id"].as_str().expect("Fabric.build_id").to_string()
}

async fn pointer_at(estate: &Estate, base: &str) -> Value {
    estate.http_get(base, "/api/fabric").await.1["build_id"].clone()
}

fn paths(build: &Value, mesh: &str) -> Vec<String> {
    build["topology"]["meshes"][mesh]["nodes"].as_array().map(|a| a.iter().filter_map(|p| p.as_str().map(String::from)).collect()).unwrap_or_default()
}

async fn build(estate: &Estate, path: &str, body: Value) -> String {
    let (status, a) = estate.post(path, &body).await;
    assert_eq!(status, 202, "{a}");
    let id = a["build_id"].as_str().unwrap().to_string();
    estate.await_build(&id, Duration::from_secs(120)).await;
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_build_fabric_build_id_names_is_the_one_accepted_topology() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;

    // 1. Day 0: the first Build, exact.
    let b0 = pointer(&estate).await;
    // The fabric seat appears one gossip interval before the executor's next pass: wait for the
    // Build itself, never for the seat that precedes it.
    estate.await_build(&b0, Duration::from_secs(60)).await;
    let (_, day0) = estate.get(&format!("/api/builds?id={b0}")).await;
    assert_eq!(paths(&day0, "mesh1"), ["mesh1.admin.1"], "{day0}");
    assert_eq!(day0["state"], "complete", "{day0}");

    // 2. A change: the next complete Build; one in flight at a time.
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    let a = a["build_id"].as_str().unwrap().to_string();
    assert_eq!(pointer(&estate).await, a, "the pointer moves on acceptance, before the Build converges");
    let (status, refused) = estate.post_raw("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 4}]})).await;
    assert_eq!((status, refused["error"].as_str(), refused["current_build_id"].as_str()), (409, Some("build-in-progress"), Some(a.as_str())), "{refused}");
    estate.await_build(&a, Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let (_, built) = estate.get(&format!("/api/builds?id={a}")).await;
    assert_eq!(paths(&built, "mesh1"), ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"], "{built}");
    assert_eq!(built["submitted_change"]["kind"], "reconcile_fabric", "the change is kept as history: {built}");

    // 3. The accepted Build is never history; Day 0's is.
    let (status, v) = estate.delete(&format!("/api/builds?id={a}")).await;
    assert_eq!(status, 409, "{v}");
    let (status, _) = estate.delete(&format!("/api/builds?id={b0}")).await;
    assert_eq!(status, 204);
    assert_eq!(estate.get(&format!("/api/builds?id={b0}")).await.0, 404);
    assert_eq!(pointer(&estate).await, a, "forget is history administration only");

    // 4. An admin born later hydrates the pointer and its Build, not the history.
    let c = build(&estate, "/api/nodes/spawn", json!({"mesh": "mesh1", "kind": "node_admin"})).await;
    let nodes = estate.settled_shape(&[("mesh1", 3, 3)], Duration::from_secs(30)).await;
    let late = nodes.iter().find(|n| n["name"] == "mesh1.admin.3").cloned().unwrap();
    let late_base = late["admin_api_base"].as_str().unwrap().to_string();
    assert_eq!(pointer_at(&estate, &late_base).await, c.as_str());
    let (status, held) = estate.http_get(&late_base, &format!("/api/builds?id={c}")).await;
    assert_eq!(status, 200, "{held}");
    assert_eq!(paths(&held, "mesh1").len(), 6, "the late admin holds the accepted Build's topology: {held}");
    assert_eq!(estate.http_get(&late_base, &format!("/api/builds?id={a}")).await.0, 404, "no completed Build history reaches it");

    // 5. A held birth that falls silent is not drift.
    let held_pid = estate.pid_of("mesh1.rpc.3").await;
    let quiet_from = now_ns();
    signal(held_pid, "-STOP");
    wait_for("mesh1.rpc.3 is silent in the view", rafka_mesh_transport::membership::staleness_floor() + Duration::from_secs(15), || async { (estate.node_opt("mesh1.rpc.3").await?["status"].as_str().is_some_and(|s| s == "dead" || s == "pending-reconnect")).then_some(()) }).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    signal(held_pid, "-CONT");
    let quiet_until = now_ns();
    wait_for("mesh1.rpc.3 is heard again", Duration::from_secs(20), || async { (estate.node_opt("mesh1.rpc.3").await?["status"] == "ready-for-traffic").then_some(()) }).await;

    // 6. An admin that misses a Build catches up to the pointer when it is back.
    let quiet = estate.nodes().await.into_iter().find(|n| n["kind"] == "node_admin" && n["name"] != "mesh1.admin.1" && n["is_primary"] == false && n["is_fabric_primary"] == false).expect("a mesh1 admin that holds no seat");
    let (quiet_name, quiet_base) = (quiet["name"].as_str().unwrap().to_string(), quiet["admin_api_base"].as_str().unwrap().to_string());
    let stopped = estate.pid_of(&quiet_name).await;
    signal(stopped, "-STOP");
    let r4 = build(&estate, "/api/build", json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 3, "rpc_node": 4}]})).await;
    assert_eq!(pointer(&estate).await, r4);
    let resumed_at = now_ns();
    signal(stopped, "-CONT");
    wait_for("the paused admin holds the pointer and the Build it names", Duration::from_secs(30), || async {
        (pointer_at(&estate, &quiet_base).await == r4.as_str() && estate.http_get(&quiet_base, &format!("/api/builds?id={r4}")).await.0 == 200).then_some(())
    })
    .await;
    estate.settled_shape(&[("mesh1", 3, 4)], Duration::from_secs(30)).await;

    // 7. A killed member is its mesh-primary's node recovery: the next attempt of the same Build, once.
    let (_, before) = estate.get(&format!("/api/builds?id={r4}")).await;
    let attempts_before = before["attempt"].as_u64().unwrap();
    let lost = estate.node("mesh1.rpc.2").await;
    let mesh_primary = estate.nodes().await.into_iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh1's primary")["name"].as_str().unwrap().to_string();
    estate.kill_node("mesh1.rpc.2").await;
    let back = wait_for("mesh1.rpc.2 is recovered", crate::node_recovery_due(), || async {
        let n = estate.node_opt("mesh1.rpc.2").await?;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != lost["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(pointer(&estate).await, r4, "recovery moves no pointer");
    estate.await_attempt(&r4, attempts_before + 1, Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 3, 4)], Duration::from_secs(30)).await;
    let (_, after) = estate.get(&format!("/api/builds?id={r4}")).await;
    assert_eq!((after["state"].as_str(), after["attempt"].as_u64()), (Some("complete"), Some(attempts_before + 1)), "one more attempt of the same Build: {after}");
    assert_eq!(after["reason"], "recovery", "the attempt is the recovery: {after}");
    assert_ne!(back["node_id"], lost["node_id"], "a lost birth's path gets a new logical node");
    assert_eq!(back["endpoint_id"], lost["endpoint_id"], "over the inherited data dir and its endpoint key");

    // 8. A restart is an attempt of the accepted Build, under the same id.
    let restarted_from = estate.node("mesh1.rpc.1").await;
    let (status, r) = estate.post("/api/nodes/mesh1.rpc.1/restart", &json!({})).await;
    assert_eq!((status, r["build_id"].as_str()), (202, Some(r4.as_str())), "{r}");
    assert_eq!(r["attempt"].as_u64(), Some(attempts_before + 2), "the restart opens the attempt after the repair's: {r}");
    estate.await_attempt(&r4, Estate::attempt_of(&r), Duration::from_secs(120)).await;
    wait_for("mesh1.rpc.1 is ready again as the same birth", Duration::from_secs(60), || async {
        let n = estate.node_opt("mesh1.rpc.1").await?;
        (n["status"] == "ready-for-traffic" && n["parked"] == false && n["incarnation_id"] == restarted_from["incarnation_id"] && n["node_id"] == restarted_from["node_id"]).then_some(())
    })
    .await;
    assert_eq!(pointer(&estate).await, r4);

    estate.stop().await;
    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap();
    let drift = named(&spans, "rdm.node_admin.build.update.via-proven-drift");
    // 5: nothing while the stopped birth was only silent.
    assert!(drift.iter().all(|sp| at(sp) < quiet_from || at(sp) > quiet_until), "a silent, live birth opened a repair: {drift:?}");
    // 7: the fabric-primary opens no drift attempt for a member: its mesh-primary recovers it.
    assert!(drift.is_empty(), "a killed member is node recovery, never the fabric-primary's drift: {drift:?}");
    let attempt = (attempts_before + 1).to_string();
    let submit = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == "mesh1.rpc.2" && sp["attributes"]["node_id"] == lost["node_id"])
        .unwrap_or_else(|| panic!("mesh1's primary submitted the recovery of the killed birth"));
    let sa = &submit["attributes"];
    assert_eq!(sa["submitter"], mesh_primary.as_str(), "the member's own mesh-primary submits: {submit}");
    assert_eq!(sa["incarnation_id"], lost["incarnation_id"], "keyed on the exact exited birth: {submit}");
    assert_eq!((sa["build_id"].as_str(), sa["attempt"].as_str()), (Some(r4.as_str()), Some(attempt.as_str())), "the recovery is the accepted Build's next attempt: {submit}");
    assert_eq!(sa["outcome"], format!("complete: attempt {attempt}"), "the submit followed the recovery to its terminal: {submit}");
    // The fabric-primary's door admitted it: one mesh, so its primary holds the fabric seat too and
    // opens the attempt in process, under the submit.
    let opened = named(&spans, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["attempt"] == attempt.as_str() && sp["attributes"]["node"] == "mesh1.rpc.2")
        .unwrap_or_else(|| panic!("the fabric-primary's door opened the recovery attempt of {r4}"));
    assert_eq!(opened["attributes"]["outcome"], "admitted", "{opened}");
    assert_eq!(opened["parent_span_id"], submit["span_id"], "the door's span is the submit's child: {opened}");
    let ran = named(&spans, "rdm.node_admin.build.update.via-reconcile");
    let repaired = ran
        .iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["reason"] == "recovery" && sp["attributes"]["outcome"] == "converged")
        .unwrap_or_else(|| panic!("the recovery attempt of {r4} converged: {ran:?}"));
    assert_eq!(repaired["attributes"]["attempt"], attempt.as_str());
    assert_eq!(repaired["attributes"]["operations"], "retire-node:mesh1.rpc.2,create-node:mesh1.rpc.2,update-node:mesh1.rpc.2", "the recovery's three operations, in order: {repaired}");
    // The recovery attempt's context is the door's span that admitted it: its reconcile is that span's child.
    assert_eq!(repaired["parent_span_id"], opened["span_id"], "the recovery's reconcile is the child of the span that admitted it: {repaired}");
    assert_eq!(repaired["trace_id"], submit["trace_id"]);
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == lost["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], lost["incarnation_id"], "named by the exact exited birth");
    assert!(
        named(&spans, "rdm.node_admin.node.create.via-build").iter().any(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["attempt"] == attempt.as_str() && sp["attributes"]["node"] == "mesh1.rpc.2"),
        "the recovery created mesh1.rpc.2 under the same Build"
    );
    estate.record_trace_url(submit["trace_id"].as_str().unwrap_or(""));
    // 8: the restart ran as an attempt of the same Build.
    let restart = ran
        .iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["reason"] == "restart" && sp["attributes"]["outcome"] == "converged")
        .unwrap_or_else(|| panic!("the restart attempt of {r4} converged: {ran:?}"));
    // The restart attempt's context is the restart call's request span, not the span that accepted the Build.
    let rest = named(&spans, "rdm.node_admin.build.update.via-rest")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == r4.as_str() && sp["attributes"]["node"] == "mesh1.rpc.1")
        .expect("the restart call's request span");
    assert_eq!(restart["parent_span_id"], rest["span_id"], "the restart's reconcile is the child of the restart call's request span: {restart}");
    assert_eq!(restart["trace_id"], rest["trace_id"]);
    // 4 and 6: the fabric-primary moves the pointer on the fabric's store root; the late admin
    // committed Ready holding it, and the paused admin caught up to the row written while it was
    // stopped (R14: the fabric-primary writes `fabric/`, the shared store is authoritative).
    let moved = named(&spans, "rdm.node_admin.fabric.update.via-build-accepted");
    let late_ready = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == "mesh1.admin.3" && sp["attributes"]["incarnation_id"] == late["incarnation_id"])
        .cloned()
        .expect("the late admin is ready");
    assert_eq!(late_ready["attributes"]["build_id"], c.as_str(), "the late admin held the pointer when it committed Ready: {late_ready}");
    assert!(
        moved.iter().any(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["build_id"] == c.as_str() && at(sp) < at(&late_ready)),
        "the fabric-primary moved the pointer to {c} before the late admin was ready: {moved:?}"
    );
    assert!(
        moved.iter().any(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["build_id"] == r4.as_str() && at(sp) < resumed_at),
        "the fabric-primary moved the pointer to {r4} while {quiet_name} was stopped: {moved:?}"
    );
    assert!(named(&spans, "rdm.node_admin.build.create.via-proven-drift").is_empty(), "no Build is minted for drift");
}
