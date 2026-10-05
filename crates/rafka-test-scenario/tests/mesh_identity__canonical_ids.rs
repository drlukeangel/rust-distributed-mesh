//! i143.e4.s15 process E2E: canonical Crockford60 product ids (rafka-v2 #2842).
//!
//! From the control API and the evidence only, on an MM fabric:
//! - the Fabric, Mesh and Node views carry canonical 12-char Crockford ids
//!   (no 32-char hex product id), every admin names the same Fabric id, and a
//!   node's transport identity is never its product identity;
//! - every birth's ready span names its node, mesh and fabric ids with
//!   `id_format = "crockford60"`, equal to the views';
//! - a restart keeps NodeId and FabricId under a new incarnation;
//! - an intentional replacement mints new identity: a mesh removed and
//!   created again gets a new MeshId, a node removed and created again at the
//!   same path a new NodeId; the Fabric keeps its id throughout.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const CROCKFORD: &str = "0123456789abcdefghjkmnpqrstvwxyz";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-identity".into(),
        subfeature: "canonical-ids".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn canonical(what: &str, v: &Value) -> String {
    let id = s(v);
    assert!(id.len() == 12 && id.chars().all(|c| CROCKFORD.contains(c)), "{what} {v} is not a canonical 12-char Crockford id");
    id
}

fn shape(meshes: &[&str], rpc: u32) -> Value {
    json!({"fabric": "fabric1", "meshes": meshes.iter().map(|m| json!({"name": m, "node_admin": 2, "rpc_node": rpc})).collect::<Vec<_>>()})
}

fn names(meshes: &[&str], rpc: u32) -> BTreeSet<String> {
    meshes.iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=rpc).map(move |i| format!("{m}.rpc.{i}")))).collect()
}

async fn build(estate: &Estate, body: &Value) {
    let (status, a) = estate.post("/api/build", body).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
}

async fn get(base: &str, path: &str) -> Value {
    reqwest::get(format!("{base}{path}")).await.unwrap().json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle() {
    let mut estate = Estate::bootstrap(owner("every_product_identity_is_canonical_and_kept_or_reminted_by_its_lifecycle"), "fabric1", "mesh1").await;
    build(&estate, &shape(&["mesh1", "mesh2"], 2)).await;
    let nodes = estate.settled(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await;

    // Views: canonical ids, one Fabric id, distinct mesh ids.
    let (_, fabric) = estate.get("/api/fabric").await;
    let fabric_id = canonical("fabric id", &fabric["id"]);
    let mesh_ids: BTreeMap<String, String> =
        fabric["meshes"].as_array().unwrap().iter().map(|m| (s(&m["name"]), canonical("mesh id", &m["id"]))).collect();
    assert_eq!(mesh_ids.len(), 2);
    assert_ne!(mesh_ids["mesh1"], mesh_ids["mesh2"]);
    for (mesh, id) in &mesh_ids {
        let (_, by_name) = estate.get(&format!("/api/meshes/{mesh}")).await;
        let (_, by_id) = estate.get(&format!("/api/meshes/{id}")).await;
        assert_eq!(by_name, by_id, "a mesh resolves by name or by id");
    }
    let node_ids: BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), canonical("node id", &n["node_id"]))).collect();
    assert_eq!(node_ids.values().collect::<BTreeSet<_>>().len(), node_ids.len(), "node ids are distinct");
    for n in &nodes {
        let transport = s(&n["transport_id"]);
        assert!(!transport.is_empty() && transport != s(&n["node_id"]) && transport != fabric_id, "transport identity is not product identity: {n}");
    }
    for admin in nodes.iter().filter(|n| n["kind"] == "node_admin") {
        let own = get(&s(&admin["admin_api_base"]), "/api/fabric").await;
        assert_eq!(s(&own["id"]), fabric_id, "{} names the Fabric by the same id", admin["name"]);
    }

    // Evidence: each birth's ready span names the ids the views show.
    let spans = estate.spans();
    for n in &nodes {
        let ready = named(&spans, "rafka.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == n["name"] && sp["attributes"]["incarnation_id"] == n["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", n["name"]));
        let a = &ready["attributes"];
        assert_eq!(a["id_format"], "crockford60");
        assert_eq!(s(&a["node_id"]), s(&n["node_id"]));
        assert_eq!(s(&a["fabric_id"]), fabric_id);
        assert_eq!(s(&a["mesh_id"]), mesh_ids[&s(&n["mesh"])]);
    }

    // Restart keeps NodeId and FabricId; the incarnation moves.
    let before = nodes.iter().find(|n| n["name"] == "mesh2.rpc.2").unwrap().clone();
    let (status, a) = estate.post("/api/nodes/mesh2.rpc.2/restart", &json!({})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let after = wait_for("mesh2.rpc.2 ready under a new incarnation", Duration::from_secs(30), || async {
        let n = estate.node("mesh2.rpc.2").await;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["node_id"], before["node_id"], "restart keeps the node id");
    let spans = estate.spans();
    let reborn = named(&spans, "rafka.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["incarnation_id"] == after["incarnation_id"])
        .expect("the restarted birth reports ready");
    assert_eq!(s(&reborn["attributes"]["fabric_id"]), fabric_id, "restart keeps the fabric id");
    assert_eq!(s(&reborn["attributes"]["node_id"]), s(&before["node_id"]));

    // Replacement of a node: removed, then created again at the same path.
    let replaced = node_ids["mesh1.rpc.2"].clone();
    let (status, a) = estate.delete("/api/nodes/mesh1.rpc.2").await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    build(&estate, &shape(&["mesh1", "mesh2"], 2)).await;
    let regrown = estate.settled(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await;
    let new_id = canonical("node id", &regrown.iter().find(|n| n["name"] == "mesh1.rpc.2").unwrap()["node_id"]);
    assert_ne!(new_id, replaced, "a recreated node is a new logical node");

    // Replacement of a mesh: removed, then created again.
    build(&estate, &shape(&["mesh1"], 2)).await;
    estate.settled(&names(&["mesh1"], 2), Duration::from_secs(30)).await;
    build(&estate, &shape(&["mesh1", "mesh2"], 2)).await;
    estate.settled(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await;
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id for its lifetime");
    let m2 = fabric_after["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh2").unwrap().clone();
    let m1 = fabric_after["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh1").unwrap().clone();
    assert_ne!(canonical("mesh id", &m2["id"]), mesh_ids["mesh2"], "a mesh created again is a new mesh");
    assert_eq!(s(&m1["id"]), mesh_ids["mesh1"], "an untouched mesh keeps its id");

    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}
