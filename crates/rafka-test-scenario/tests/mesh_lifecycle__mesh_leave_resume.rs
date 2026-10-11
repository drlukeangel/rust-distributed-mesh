//! i143.e12.s39 (#3066) process E2E: a mesh leave resumed after the leaving mesh's primary is lost
//! mid-leave (R11; mesh-leave.md, "Resume after the primary's loss").
//!
//! desired {mesh1, mesh2} -> {mesh1}: the fabric-primary in mesh1 owns `shutdown-mesh:<mesh2 id>`.
//! mesh2's primary is held by its fault door in tier 1 (rpc.2's DrainNode receipt) and SIGKILLed
//! there by exact pid; the door dies with it. The owner's attempt ends at its hand-off wait, the
//! drift plans the shutdown again, and the resume proves and records the lost primary's exact
//! Exited, amends the roster with the new final primary, and finishes: completion covers the
//! accepted roster plus the amendment, both primary births included.
//!
//! Two forms: (a) a surviving admin of mesh2 the election seats; (b) no admin of mesh2 survives,
//! and the fabric-primary creates the one node-admin the leaving mesh may gain.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-leave-resume".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().map(str::to_string).unwrap_or_else(|| if v.is_null() { String::new() } else { v.to_string() })
}

fn attr_u(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["attributes"][k].as_u64()).unwrap_or(0)
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// What one form's run leaves to assert on.
struct Run {
    build: Value,
    spans: Vec<Value>,
    mesh2_id: String,
    lost: (String, String),
    b: String,
}

/// Build {mesh1, mesh2 with `mesh2_admins` admins}, arm mesh2's primary's door at rpc.2's DrainNode
/// receipt, submit {mesh1}, kill the held primary by exact pid, and wait for the Build to complete
/// on a later attempt.
async fn primary_lost_mid_leave(test: &str, mesh2_admins: u32) -> Run {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let root = estate.root.clone();
    let mesh1 = json!({"name": "mesh1", "node_admin": 2, "rpc_node": 0});
    let mesh2 = json!({"name": "mesh2", "node_admin": mesh2_admins, "rpc_node": 2});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh1.clone(), mesh2]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let mut want: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh2.rpc.1", "mesh2.rpc.2"].iter().map(|n| n.to_string()).collect();
    want.extend((1..=mesh2_admins).map(|i| format!("mesh2.admin.{i}")));
    let before = estate.settled(&want, Duration::from_secs(60)).await;
    let (_, m2) = estate.get("/api/meshes/mesh2").await;
    let mesh2_id = s(&m2["id"]);
    let primary = before.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().unwrap_or_else(|| panic!("mesh2 has an admin primary: {before:#?}"));
    let lost = (s(&primary["name"]), s(&primary["incarnation_id"]));
    let door = Door::open(&root, &lost.0, &s(&primary["admin_api_base"])).await;

    // Held in tier 1: rpc.2 drained and running, the other admin (form a) still ready for traffic.
    let cut = "leave:rpc2-DrainNode";
    // A member's workflow runs under `<outer>/<path>` (`mesh_leave::member_operation`): the operation prefix names rpc.2's.
    door.arm(cut, json!({"kind": "receipt", "step": "DrainNode", "operation": format!("shutdown-mesh:{mesh2_id}/mesh2.rpc.2")})).await;
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh1]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    let held = door.wait_held(cut).await;
    assert_eq!(held["hit"]["build_id"], b.as_str(), "the held receipt is the leave's: {held}");
    // The door dies with the process: it is never released.
    let pid = estate.pid_of(&lost.0).await;
    estate.kill_pid(u32::try_from(pid).unwrap());

    // Attempt 1 ends at the owner's hand-off wait (120 s, unchanged); a later attempt completes.
    let build = wait_for(&format!("build {b} complete on an attempt after the first"), Duration::from_secs(420), || async {
        let (_, v) = estate.get(&format!("/api/builds?id={b}")).await;
        (v["state"] == "complete" && v["attempt"].as_u64().unwrap_or(0) >= 2).then_some(v)
    })
    .await;
    // The view converges on mesh1 alone within gossip delay of the MeshLeft receipt.
    let mesh1_names: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2"].iter().map(|n| n.to_string()).collect();
    estate.settled(&mesh1_names, Duration::from_secs(60)).await;
    let (status, gone) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(status, 404, "mesh2 is no longer a mesh of the fabric: {gone}");
    estate.stop().await;
    let spans = estate.spans();
    Run { build, spans, mesh2_id, lost, b }
}

/// The receipts and spans both forms share: the lost primary's exit recorded before the
/// amendment, one amendment admitting `next`, and a MeshLeft covering the accepted roster plus it.
fn assert_resumed(run: &Run, next: &str, roster: usize) -> Value {
    let Run { build, spans, mesh2_id, lost, b } = run;
    let outer = format!("shutdown-mesh:{mesh2_id}");
    let steps: Vec<Value> = build["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["operation"] == outer.as_str()).collect();
    let step = |name: &str| -> Vec<&Value> { steps.iter().filter(|r| r["step"] == name).collect() };
    let amended = step("RosterAmended");
    assert_eq!(amended.len(), 1, "one RosterAmended receipt under {outer}: {build}");
    assert_eq!(s(&amended[0]["output"]["predecessor"]["node"]), lost.0, "the amendment names the lost primary: {}", amended[0]);
    assert_eq!(s(&amended[0]["output"]["predecessor"]["incarnation_id"]), lost.1, "the amendment names the lost primary's exact birth: {}", amended[0]);
    assert_eq!(s(&amended[0]["output"]["final_primary"]["node"]), next, "the amendment admits {next}: {}", amended[0]);
    let left = step("MeshLeft");
    assert_eq!(left.len(), 1, "one MeshLeft receipt under {outer}: {build}");
    assert_eq!(left[0]["outcome"], json!("complete"), "{}", left[0]);
    let births: BTreeSet<String> = left[0]["output"]["receipts"].as_array().cloned().unwrap_or_default().iter().map(|r| s(&r["node"])).collect();
    assert!(births.contains(&lost.0) && births.contains(next), "MeshLeft covers both primary births ({} and {next}): {births:?}", lost.0);
    assert_eq!(left[0]["output"]["receipts"].as_array().map(Vec::len), Some(roster), "one exact Exited per birth of the accepted roster plus the amendment: {}", left[0]);

    let one = |name: &str| -> Value {
        let v: Vec<&Value> = named(spans, name).into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str()).collect();
        assert!(!v.is_empty(), "a {name} span for {outer}");
        v[0].clone()
    };
    let recorded = one("rdm.node_admin.mesh.update.via-predecessor-exit-recorded");
    assert_eq!(s(&recorded["attributes"]["predecessor"]), format!("{} {}", lost.0, lost.1), "{recorded}");
    let applied: Vec<&Value> = named(spans, "rdm.node_admin.mesh.update.via-roster-amended").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && sp["attributes"]["outcome"] == "applied").collect();
    assert_eq!(applied.len(), 1, "the amendment was applied once: {applied:?}");
    assert!(start_ns(&recorded) <= start_ns(applied[0]), "the lost primary's exit is recorded before the amendment");
    assert!(s(&applied[0]["attributes"]["final_primary"]).starts_with(&format!("{next} ")), "{}", applied[0]);
    assert!(attr_u(applied[0], "attempt") >= 2, "the amendment belongs to a resumed attempt: {}", applied[0]);
    let mesh_left = one("rdm.node_admin.mesh.update.via-mesh-left-receipt");
    assert_eq!((attr_u(&mesh_left, "receipts"), attr_u(&mesh_left, "roster")), (roster as u64, roster as u64), "{mesh_left}");
    // A desired change, not a recovery: no drift attempt replaces or restarts a mesh2 birth.
    let drift: Vec<&Value> = named(spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains("mesh2.") && sp["attributes"]["action"] != "none").collect();
    assert!(drift.is_empty(), "no drift attempt acts on a mesh2 birth while the leave resumes: {drift:?}");
    assert!(named(spans, "rdm.node_admin.mesh.reject.via-second-amendment").is_empty());
    eprintln!("{test} trace {} build {b}", s(&applied[0]["trace_id"]), test = "mesh_leave_resume");
    applied[0].clone()
}

/// CONTRACT: mesh2's primary, killed while it holds rpc.2's drain receipt, is replaced as the
/// leave's final primary by the surviving admin the election seats: the fabric-primary records
/// the lost primary's exact Exited, then one RosterAmended naming the predecessor and the
/// survivor, and the resumed attempt finishes; MeshLeft covers the four births of the accepted
/// roster, which already holds the survivor, both primaries included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin() {
    let run = primary_lost_mid_leave("a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin", 2).await;
    let survivor = if run.lost.0 == "mesh2.admin.1" { "mesh2.admin.2" } else { "mesh2.admin.1" };
    assert_resumed(&run, survivor, 4);
    // The survivor admits the leave from the accepted roster plus the amendment, from its own
    // facts or the owner's, and runs only what has no terminal receipt.
    let outer = format!("shutdown-mesh:{}", run.mesh2_id);
    let resumed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && attr_u(sp, "attempt") >= 2).collect();
    assert!(!resumed.is_empty(), "the survivor resumed the leave's tiers on a later attempt");
    assert!(named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").is_empty(), "a surviving admin needs no created one");
}

/// CONTRACT: mesh2's only admin, killed while it holds rpc.2's drain receipt, leaves no admin of
/// the mesh: the fabric-primary proves and records its exact Exited first, creates the one
/// node-admin the leaving mesh may gain at a path no birth held, amends the roster for it, and
/// the resumed attempt finishes; MeshLeft covers the four births, both primaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates() {
    let run = primary_lost_mid_leave("a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates", 1).await;
    let created: Vec<&Value> = named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").into_iter().filter(|sp| sp["attributes"]["outcome"] == "created").collect();
    assert_eq!(created.len(), 1, "the fabric-primary created one admin for mesh2: {created:?}");
    assert_eq!(s(&created[0]["attributes"]["node"]), "mesh2.admin.2", "{}", created[0]);
    let amended = assert_resumed(&run, "mesh2.admin.2", 4);
    assert!(start_ns(created[0]) < start_ns(&amended), "the admin is created before the amendment admits it");
    let outer = format!("shutdown-mesh:{}", run.mesh2_id);
    let admitted: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-owner-admitted-roster").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str()).collect();
    assert!(admitted.iter().any(|sp| attr_u(sp, "amendments") == 1), "the created admin runs the owner's accepted roster plus the amendment: {admitted:?}");
}
