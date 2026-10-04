//! i143.e4.s8 process E2E: the mesh replacement contract (PRD §12.3).
//!
//! desired {mesh1, mesh2} -> {mesh2, mesh3}: mesh1 is deliberately drained
//! and retired, and mesh3 is created. Not recovery: nothing is lost, and
//! nothing of mesh1 is reconstructed. From public surfaces only:
//! - one Build, submitted through mesh2's admin, completes;
//! - the view settles on exactly mesh2 and mesh3, all ready;
//! - mesh3 has a new identity (neither mesh1's nor mesh2's id);
//! - mesh2 was never touched (every mesh2 node keeps its incarnation);
//! - no mesh1 runtime is left, and mesh1 is no longer a mesh of the fabric;
//! - control stays reachable through the advertised endpoints throughout.
//!
//! Evidence: every mesh1 node leaves through the retire pipeline under the
//! Build (`node.delete.via-build` above `deployment.update.via-pipeline`
//! with `pipeline=retire`), which descends from the accepting request; no
//! mesh1 path is created or fenced (that would be recovery).

use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-replace".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "replacing_a_mesh_retires_it_and_creates_a_new_one".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        out.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    out
}

fn incarnations(nodes: &[Value]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_mesh_retires_it_and_creates_a_new_one() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (_, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [
            {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
            {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
        ]}))
        .await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let before = estate.settled(&names(&[("mesh1", 2, 2), ("mesh2", 2, 2)]), Duration::from_secs(15)).await;
    let (_, m1) = estate.get("/api/meshes/mesh1").await;
    let (_, m2) = estate.get("/api/meshes/mesh2").await;
    let (mesh1_id, mesh2_id) = (s(&m1["id"]), s(&m2["id"]));

    // The replacement, through mesh2's admin: mesh1's admins are leaving.
    estate.admin = s(&m2["admin_api_base"]);
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [
            {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
            {"name": "mesh3", "node_admin": 2, "rpc_node": 2},
        ]}))
        .await;
    assert_eq!(status, 202, "{a}");
    let b = s(&a["build_id"]);
    let done = estate.await_build(&b, Duration::from_secs(180)).await;
    let after = estate.settled(&names(&[("mesh2", 2, 2), ("mesh3", 2, 2)]), Duration::from_secs(30)).await;

    // mesh3 is new; mesh2 untouched; mesh1 gone.
    let (_, m3) = estate.get("/api/meshes/mesh3").await;
    let mesh3_id = s(&m3["id"]);
    assert!(!mesh3_id.is_empty() && mesh3_id != mesh1_id && mesh3_id != mesh2_id, "mesh3 has a new identity: {m3:#}");
    let (old, new) = (incarnations(&before), incarnations(&after));
    for (name, inc) in old.iter().filter(|(n, _)| n.starts_with("mesh2.")) {
        assert_eq!(&new[name], inc, "{name} was never touched");
    }
    let (_, fabric) = estate.get("/api/fabric").await;
    let meshes: BTreeSet<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["name"])).collect();
    assert_eq!(meshes, ["mesh2", "mesh3"].iter().map(|m| m.to_string()).collect(), "the fabric's meshes: {fabric:#}");
    let (status, _) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(status, 404, "mesh1 is no longer a mesh of the fabric");
    let left: Vec<String> = estate
        .live_runtimes()
        .into_iter()
        .map(|(dir, _)| dir.file_name().unwrap().to_string_lossy().to_string())
        .filter(|d| d.starts_with("mesh1."))
        .collect();
    assert!(left.is_empty(), "no mesh1 runtime is left: {left:?} (Build {done:#})");

    // Stop through whoever holds the fabric now.
    estate.admin = s(&fabric["admin_api_base"]);
    estate.artifact("nodes.json", &json!(after));
    estate.stop().await;

    let spans = estate.spans();
    let accepted = named(&spans, "rafka.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == b.as_str()).unwrap().clone();
    let deletes: Vec<Value> = named(&spans, "rafka.node_admin.node.delete.via-build")
        .into_iter()
        .filter(|sp| sp["attributes"]["build_id"] == b.as_str())
        .cloned()
        .collect();
    let retires: Vec<Value> = named(&spans, "rafka.node_admin.deployment.update.via-pipeline")
        .into_iter()
        .filter(|sp| sp["attributes"]["build_id"] == b.as_str() && sp["attributes"]["pipeline"] == "retire")
        .cloned()
        .collect();
    for node in names(&[("mesh1", 2, 2)]) {
        let delete = deletes
            .iter()
            .find(|d| d["attributes"]["node"] == node.as_str())
            .unwrap_or_else(|| panic!("{node} leaves under the Build"));
        assert!(descends_from(&spans, delete, &accepted), "{node}'s retire descends from the replacement request");
        assert!(
            retires.iter().any(|r| r["attributes"]["node"] == node.as_str() && descends_from(&spans, r, delete)),
            "{node} leaves through the retire pipeline"
        );
    }
    // Not recovery: no mesh1 path is created or fenced under the Build.
    let creates: Vec<String> = named(&spans, "rafka.node_admin.node.create.via-build")
        .into_iter()
        .filter(|sp| sp["attributes"]["build_id"] == b.as_str())
        .map(|sp| s(&sp["attributes"]["node"]))
        .collect();
    assert!(creates.iter().all(|n| n.starts_with("mesh3.")), "only mesh3 is created: {creates:?}");
    let fenced: Vec<String> = named(&spans, "rafka.node_admin.deployment.delete.via-fence").into_iter().map(|sp| s(&sp["attributes"]["node"])).collect();
    assert!(fenced.iter().all(|n| !n.starts_with("mesh1.")), "no mesh1 path is fenced: {fenced:?}");
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap_or(""));
}
