//! i143.e4.s7 process E2E: the mesh recovery contract (PRD §1.15, §12.2).
//!
//! desired {mesh1, mesh2}, observed {mesh2} -> recover mesh1. A Build B is
//! active (growing mesh1, executed by mesh1's admin) when every mesh1 process
//! is killed. From public surfaces only:
//! - control is found again through the advertised endpoints;
//! - B completes under its own id: a surviving admin takes it over and
//!   reconstructs mesh1;
//! - mesh1 is recovered as itself: the same mesh id and the same node names;
//! - every recovered node is a new birth: a new incarnation and new
//!   endpoint tokens;
//! - mesh2 was never touched (every mesh2 node keeps its incarnation).
//!
//! Evidence: a reconcile of B by a mesh2 admin names mesh1's admin primary
//! as the previous executor and descends from B's accepting request.

use rafka_test_scenario::estate::{descends_from, named, own_fabric_at, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-recover".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_lost_mesh_recovers_as_itself_under_the_same_build".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

async fn get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    r.status().is_success().then_some(())?;
    r.json().await.ok()
}

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        out.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    out
}

/// name -> incarnation
fn births(nodes: &[Value]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_mesh_recovers_as_itself_under_the_same_build() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let shape = |r1: u32| json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": r1},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (_, a) = estate.post("/api/build", &shape(3)).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let before = estate.settled(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(15)).await;
    let (_, mesh1) = estate.get("/api/meshes/mesh1").await;
    let mesh1_id = s(&mesh1["id"]);
    // mesh1's members are grown by mesh1's admin primary (the lowest NodeId of its admins).
    let mesh1_primary = before.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).unwrap();
    let (_, fabric) = estate.get("/api/fabric").await;
    let advertised: Vec<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["admin_api_base"])).collect();

    // B: grow mesh1. Once its first attempt is claimed, lose mesh1 entirely. Its births run one
    // after another (~55 ms each on the process provider), so six of them keep the first attempt
    // running across several polls; two finished inside one 100 ms poll.
    let (status, a) = estate.post("/api/build", &shape(9)).await;
    assert_eq!(status, 202, "{a}");
    let b = s(&a["build_id"]);
    // Active fabric-wide: the surviving mesh's admin already sees mesh1's
    // admin holding B's first attempt (a claim that never left the lost mesh
    // would leave the successor nothing to take over: it would claim afresh).
    let mesh2_base = advertised.iter().find(|b| **b != estate.admin).cloned().unwrap();
    wait_for("mesh2's admin sees B's first attempt held by mesh1's admin", Duration::from_secs(30), || async {
        let v = get(&mesh2_base, &format!("/api/builds?id={b}")).await?;
        (v["attempt"].as_u64() == Some(1) && v["state"] == "running" && v["executor"] == mesh1_primary.as_str()).then_some(())
    })
    .await;
    // Every mesh1 runtime, those B's first attempt launched included.
    for (dir, pid) in estate.live_runtimes() {
        if dir.file_name().unwrap().to_string_lossy().starts_with("mesh1.") {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
    estate.kill_bootstrap();

    // Control, through the advertised endpoints only.
    let base = wait_for("a surviving admin answers", Duration::from_secs(30), || {
        let advertised = advertised.clone();
        let fabric_id = estate.fabric_id.clone();
        async move {
            for base in &advertised {
                if own_fabric_at(base, &fabric_id).await.is_some() {
                    return Some(base.clone());
                }
            }
            None
        }
    })
    .await;
    estate.admin = base;

    // B completes under its own id, and mesh1 is back as itself.
    let done = estate.await_build(&b, Duration::from_secs(180)).await;
    assert!(done["attempt"].as_u64().unwrap() > 1, "a successor continued B: {done:#}");
    let after = estate.settled(&names(&[("mesh1", 2, 9), ("mesh2", 2, 3)]), Duration::from_secs(60)).await;
    let (_, mesh1) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(s(&mesh1["id"]), mesh1_id, "mesh1 recovered as itself: {mesh1:#}");
    // ...in the recovered mesh's own admins too: they carry its identity.
    for admin in after.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin") {
        let own = get(&s(&admin["admin_api_base"]), "/api/meshes/mesh1").await.unwrap();
        assert_eq!(s(&own["id"]), mesh1_id, "{} serves mesh1 under its own identity: {own:#}", admin["name"]);
    }
    let (old, new) = (births(&before), births(&after));
    for (name, incarnation) in &old {
        let now_inc = &new[name];
        if name.starts_with("mesh1.") {
            assert_ne!(now_inc, incarnation, "{name} is a new birth");
        } else {
            assert_eq!(now_inc, incarnation, "{name} was never touched");
        }
    }

    // Stop through whoever holds the fabric now.
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.artifact("nodes.json", &json!(after));
    estate.stop().await;
    let spans = estate.spans();
    let accepted = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == b.as_str()).unwrap().clone();
    let takeover = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|r| {
            let a = &r["attributes"];
            a["build_id"] == b.as_str() && a["previous_executor"] == mesh1_primary.as_str() && a["executor"].as_str().is_some_and(|e| e.starts_with("mesh2."))
        })
        .unwrap_or_else(|| panic!("no takeover of {b} by a mesh2 admin"))
        .clone();
    assert!(descends_from(&spans, &takeover, &accepted), "the takeover descends from B's request");
    // Each recovered admin held every surviving member's runtime before it
    // committed Ready (i143.e4.s16): mesh2's five births at least.
    for admin in after.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin") {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == admin["name"] && sp["attributes"]["incarnation_id"] == admin["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", admin["name"]))
            .clone();
        let held = ready["attributes"]["runtime_facts_held"].as_u64().or_else(|| ready["attributes"]["runtime_facts_held"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0);
        assert!(held >= 5, "{} held {held} runtime facts before Ready: {ready}", admin["name"]);
    }
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap_or(""));
}
