//! i143.e4.s6 process E2E: the mesh creation contract (PRD §1.15, §12.1).
//!
//! desired {mesh1} -> {mesh1, mesh2}, through one fabric Build:
//! - the fabric primary creates mesh2's node-admin cohort;
//! - mesh2's own admin primary creates mesh2's members (the same Build, a
//!   later attempt claimed by that admin);
//! - the test reads mesh2's control endpoint from the advertised topology and
//!   grows and shrinks mesh2 there; mesh2's admin primary executes those
//!   Builds.
//!
//! "Who executed" is read from each Build's public view (`executor` of the
//! attempt) and from the evidence: every `node.create.via-build` /
//! `node.delete.via-build` span sits under a `build.update.via-reconcile`
//! whose `executor` is the expected admin, by parent span id.

use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-create".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "creating_a_mesh_hands_its_members_to_its_own_primary".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The admin whose reconcile ran `span_name` for `node` under `build_id`.
fn executed_by(spans: &[Value], span_name: &str, node: &str, build_id: &str) -> String {
    let op = named(spans, span_name)
        .into_iter()
        .find(|s| s["attributes"]["node"] == node && s["attributes"]["build_id"] == build_id)
        .unwrap_or_else(|| panic!("no {span_name} for {node} under {build_id}"))
        .clone();
    let reconcile = named(spans, "rafka.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|r| r["attributes"]["build_id"] == build_id && descends_from(spans, &op, r))
        .unwrap_or_else(|| panic!("{node}: no reconcile of {build_id} above its operation"));
    s(&reconcile["attributes"]["executor"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creating_a_mesh_hands_its_members_to_its_own_primary() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let mesh = |m: &str| json!({"name": m, "node_admin": 2, "rpc_node": 3});
    let (_, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1")]})).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;

    // {mesh1} -> {mesh1, mesh2}
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let create = s(&a["build_id"]);
    let build = estate.await_build(&create, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=3).map(move |i| format!("{m}.rpc.{i}"))))
        .collect();
    let nodes = estate.settled(&want, Duration::from_secs(15)).await;
    let fabric_primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).unwrap();
    let mesh2_primary = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).unwrap();
    assert_eq!(build["executor"], mesh2_primary.as_str(), "the Build's last attempt ran on mesh2's own primary: {build:#}");

    // mesh2's control endpoint, from the advertised topology only.
    let (_, fabric) = estate.get("/api/fabric").await;
    let mesh2_base = fabric["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh2").map(|m| s(&m["admin_api_base"])).unwrap();
    let (_, mesh2_view) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&mesh2_view["admin_api_base"]), mesh2_base);
    assert_eq!(s(&mesh2_view["primary_admin"]), mesh2_primary);
    let fabric_base = estate.admin.clone();
    estate.admin = mesh2_base.clone();

    // Grow and shrink mesh2 directly.
    let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh2", "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{a}");
    let grow = s(&a["build_id"]);
    let b = estate.await_build(&grow, Duration::from_secs(120)).await;
    assert_eq!(b["executor"], mesh2_primary.as_str(), "{b:#}");
    assert_eq!(estate.node("mesh2.rpc.4").await["status"], "ready-for-traffic");
    let (status, a) = estate.delete("/api/nodes/mesh2.rpc.4").await;
    assert_eq!(status, 202, "{a}");
    let shrink = s(&a["build_id"]);
    let b = estate.await_build(&shrink, Duration::from_secs(120)).await;
    assert_eq!(b["executor"], mesh2_primary.as_str(), "{b:#}");
    assert!(!estate.nodes().await.iter().any(|n| n["name"] == "mesh2.rpc.4"), "mesh2.rpc.4 retired");

    estate.artifact("nodes.json", &json!(estate.nodes().await));
    estate.admin = fabric_base;
    estate.stop().await;
    let spans = estate.spans();
    for a in ["mesh2.admin.1", "mesh2.admin.2"] {
        assert_eq!(executed_by(&spans, "rafka.node_admin.node.create.via-build", a, &create), fabric_primary, "{a}: the fabric primary creates mesh2's admin cohort");
    }
    for r in ["mesh2.rpc.1", "mesh2.rpc.2", "mesh2.rpc.3"] {
        assert_eq!(executed_by(&spans, "rafka.node_admin.node.create.via-build", r, &create), mesh2_primary, "{r}: mesh2's primary creates its members");
    }
    assert_eq!(executed_by(&spans, "rafka.node_admin.node.create.via-build", "mesh2.rpc.4", &grow), mesh2_primary);
    assert_eq!(executed_by(&spans, "rafka.node_admin.node.delete.via-build", "mesh2.rpc.4", &shrink), mesh2_primary);
    let accepted = named(&spans, "rafka.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == create.as_str()).unwrap().clone();
    for r in named(&spans, "rafka.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == create.as_str()) {
        assert!(descends_from(&spans, r, &accepted), "every attempt of the creation Build descends from its request");
    }
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap_or(""));
}
