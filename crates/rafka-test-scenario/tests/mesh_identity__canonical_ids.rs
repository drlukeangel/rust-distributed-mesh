//! i143.e4.s15 process E2E: canonical Crockford60 product ids (rafka-v2 #2842).
//!
//! From the control API and the evidence only, on an MM fabric:
//! - the Fabric, Mesh and Node views carry canonical 12-char Crockford ids
//!   (no 32-char hex product id), every admin names the same Fabric id, and a
//!   node's transport identity is never its product identity;
//! - every birth's ready span names its node, mesh and fabric ids with
//!   `id_format = "crockford60"`, equal to the views';
//! - a restart keeps NodeId and FabricId under a new incarnation, and a second restart of the
//!   same node restarts it again;
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
        let transport = s(&n["endpoint_id"]);
        assert!(!transport.is_empty() && transport != s(&n["node_id"]) && transport != fabric_id, "transport identity is not product identity: {n}");
    }
    for admin in nodes.iter().filter(|n| n["kind"] == "node_admin") {
        let own = get(&s(&admin["admin_api_base"]), "/api/fabric").await;
        assert_eq!(s(&own["id"]), fabric_id, "{} names the Fabric by the same id", admin["name"]);
    }

    // Restart keeps NodeId, FabricId and the incarnation: the process is stopped and started.
    let before = nodes.iter().find(|n| n["name"] == "mesh2.rpc.2").unwrap().clone();
    let (status, a) = estate.post("/api/nodes/mesh2.rpc.2/restart", &json!({})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_attempt(a["build_id"].as_str().unwrap(), Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let after = wait_for("mesh2.rpc.2 ready after its restart", Duration::from_secs(30), || async {
        let n = estate.node_opt("mesh2.rpc.2").await?;
        (n["status"] == "ready-for-traffic" && n["parked"] == false).then_some(n)
    })
    .await;
    assert_eq!(after["node_id"], before["node_id"], "restart keeps the node id");
    assert_eq!(after["incarnation_id"], before["incarnation_id"], "restart keeps the incarnation");

    // A second restart of the same node is a later attempt of the same Build: the first
    // restart's finished run hands it nothing, so it restarts the node again.
    let (status, again) = estate.post("/api/nodes/mesh2.rpc.2/restart", &json!({})).await;
    assert_eq!(status, 202, "{again}");
    assert_eq!(again["build_id"], a["build_id"], "a restart is an attempt of the accepted Build: {again}");
    estate.await_attempt(again["build_id"].as_str().unwrap(), Estate::attempt_of(&again), Duration::from_secs(120)).await;
    let third = wait_for("mesh2.rpc.2 ready after its second restart", Duration::from_secs(30), || async {
        let n = estate.node_opt("mesh2.rpc.2").await?;
        (n["status"] == "ready-for-traffic" && n["parked"] == false).then_some(n)
    })
    .await;
    assert_eq!(third["node_id"], before["node_id"], "a second restart keeps the node id");
    assert_eq!(third["incarnation_id"], before["incarnation_id"], "and the incarnation");
    let restart_build = s(&again["build_id"]);

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
    let recreated = estate.settled(&names(&["mesh1", "mesh2"], 2), Duration::from_secs(30)).await;
    // R-NS1 item 1 + states.md ("A mesh id is never reissued; dead is final ... a mesh created again
    // under that name is a new mesh with a new id"): the removed mesh2 was Dead, and Dead belongs to its
    // id. No admin's view states the new mesh2 Dead.
    for n in recreated.iter().filter(|n| n["kind"] == "node_admin") {
        let m = get(&s(&n["admin_api_base"]), "/api/meshes/mesh2").await;
        assert_ne!(s(&m["status"]), "dead", "{} states the recreated mesh2 Dead: {m}", s(&n["name"]));
    }
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id for its lifetime");
    let m2 = fabric_after["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh2").unwrap().clone();
    let m1 = fabric_after["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh1").unwrap().clone();
    assert_ne!(canonical("mesh id", &m2["id"]), mesh_ids["mesh2"], "a mesh created again is a new mesh");
    assert_eq!(s(&m1["id"]), mesh_ids["mesh1"], "an untouched mesh keeps its id");

    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;

    // The span evidence is read only now: every process flushes its spans on exit (`Estate::stop`),
    // and a span read mid-run can still sit in its process's export batch.
    // Evidence: each birth's ready span names the ids the views show.
    let spans = estate.spans();
    for n in &nodes {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == n["name"] && sp["attributes"]["incarnation_id"] == n["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", n["name"]));
        let a = &ready["attributes"];
        assert_eq!(a["id_format"], "crockford60");
        assert_eq!(s(&a["node_id"]), s(&n["node_id"]));
        assert_eq!(s(&a["fabric_id"]), fabric_id);
        assert_eq!(s(&a["mesh_id"]), mesh_ids[&s(&n["mesh"])]);
    }

    // The removed mesh2's last admin published MeshStatus Dead while it lived, and it left with MeshLeft;
    // after that nothing publishes mesh2 Dead: the recreated mesh2's primary authors its new mesh's own
    // status and restates no heard status as its own (gossip.md frame catalog, MeshStatus).
    let left_at = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left-receipt").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == mesh_ids["mesh2"]).map(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0)).min().expect("the removed mesh2 left with a MeshLeft receipt");
    let dead_after: Vec<String> = named(&spans, "rdm.mesh.fabric.update.via-status-send")
        .into_iter()
        .filter(|sp| sp["attributes"]["scope"] == "mesh:mesh2" && sp["attributes"]["status"] == "dead" && sp["start_unix_nano"].as_u64().unwrap_or(0) > left_at)
        .map(|sp| format!("{}@{}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["changed_at_rafka_ms"])))
        .collect();
    assert!(dead_after.is_empty(), "MeshStatus Dead for mesh2 was sent after the removed mesh2 left: {dead_after:?}");

    let reborn = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["incarnation_id"] == after["incarnation_id"])
        .expect("the restarted birth reports ready");
    assert_eq!(s(&reborn["attributes"]["fabric_id"]), fabric_id, "restart keeps the fabric id");
    assert_eq!(s(&reborn["attributes"]["node_id"]), s(&before["node_id"]));
    // A second restart of the same node ran its own StartNode: none reused.
    let ran = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| {
            let at = &sp["attributes"];
            s(&at["build_id"]) == restart_build && at["node"] == "mesh2.rpc.2" && at["step"] == "StartNode" && at["outcome"] == "complete"
        })
        .count();
    assert!(ran >= 2, "each restart ran its own StartNode, none reused: {ran} complete StartNode steps under {restart_build}");

    // R-MN1: mesh2's removal deleted every roster birth (the final primary's row by the
    // fabric-primary at mesh-left) and wrote the mesh row Dead; mesh2 created again claimed
    // mesh2.admin.1 over the Deleted row, its config seeded from it, under a new MeshId.
    let leave = format!("shutdown-mesh:{}", mesh_ids["mesh2"]);
    let deleted = named(&spans, "rdm.node_admin.node.delete.via-node-row");
    for n in regrown.iter().filter(|n| n["mesh"] == "mesh2") {
        let row = deleted.iter().find(|sp| {
            let a = &sp["attributes"];
            s(&a["operation"]) == leave && a["subject"] == n["name"] && s(&a["node_id"]) == s(&n["node_id"])
        });
        assert!(row.is_some_and(|sp| sp["attributes"]["outcome"] == "row-deleted"), "{}'s row is written Deleted by mesh2's removal: {row:?}", n["name"]);
    }
    let dead = named(&spans, "rdm.node_admin.mesh.update.via-dead-row");
    assert!(dead.iter().any(|sp| s(&sp["attributes"]["mesh_id"]) == mesh_ids["mesh2"] && sp["attributes"]["outcome"] == "applied"), "the old mesh2's row is written Dead: {dead:?}");
    let old_admin = regrown.iter().find(|n| n["name"] == "mesh2.admin.1").unwrap();
    let new_admin = recreated.iter().find(|n| n["name"] == "mesh2.admin.1").unwrap();
    assert_ne!(new_admin["node_id"], old_admin["node_id"], "the recreated mesh's admin is a new node");
    let claim = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| {
        let a = &sp["attributes"];
        a["subject"] == "mesh2.admin.1" && a["moment"] == "claim" && s(&a["node_id"]) == s(&new_admin["node_id"])
    });
    let claim = claim.unwrap_or_else(|| panic!("the recreated mesh2.admin.1 {} claims its name", new_admin["node_id"]));
    assert_eq!(claim["attributes"]["outcome"], "row-seeded", "{claim}");
    assert_eq!(s(&claim["attributes"]["seeded_from"]), s(&old_admin["node_id"]), "seeded from the old mesh2.admin.1's Deleted row: {claim}");
    assert_eq!(s(&new_admin["mesh"]), "mesh2");
    let ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["incarnation_id"] == new_admin["incarnation_id"]).expect("the recreated mesh2.admin.1 reports ready");
    assert_eq!(s(&ready["attributes"]["mesh_id"]), s(&m2["id"]), "under the new MeshId");
    // A mesh created again is a new mesh (R-MN1): the removed mesh's births departed at mesh-left,
    // so its first admin enters from none of them, and each member is a create over the Deleted
    // rows, never a replace of a removed birth.
    let old_ids: BTreeSet<String> = regrown.iter().filter(|n| n["mesh"] == "mesh2").map(|n| s(&n["node_id"])).collect();
    let swept_old: Vec<&Value> = named(&spans, "rdm.node_admin.node.reject.via-entry-sweep-no-reply").into_iter().filter(|sp| old_ids.contains(&s(&sp["attributes"]["node_id"]))).collect();
    assert!(swept_old.is_empty(), "the recreated mesh's first admin holds no birth of the removed mesh: {swept_old:?}");
    // A create whose view holds a removed birth at its path fences it (admin.rs create -> fence.rs
    // `via-fence`, the incarnation it fenced): no create of the recreated mesh fences one.
    let old_incarnations: BTreeSet<String> = regrown.iter().filter(|n| n["mesh"] == "mesh2").map(|n| s(&n["incarnation_id"])).collect();
    let fenced_old: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.delete.via-fence").into_iter().filter(|sp| old_incarnations.contains(&s(&sp["attributes"]["incarnation"]))).collect();
    assert!(fenced_old.is_empty(), "no member of the recreated mesh is created over a removed birth: {fenced_old:?}");
}
