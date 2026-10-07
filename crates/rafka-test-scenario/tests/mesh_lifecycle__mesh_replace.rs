//! i143.e4.s8 process E2E: the mesh replacement contract (PRD §12.3).
//!
//! desired {mesh1, mesh2} -> {mesh2, mesh3} through one Build: mesh1 is drained and retired
//! through the retire pipeline, and mesh3 is created under a new identity. This is a desired
//! change, never a recovery: no proven-drift Build names mesh1, nothing recreates a mesh1 node, and
//! the scenario carries its own manifest (subfeature `mesh-replace`).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-replace".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "replacing_a_mesh_retires_the_old_one_and_creates_a_new_identity".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn mesh(name: &str) -> Value {
    json!({"name": name, "node_admin": 2, "rpc_node": 2})
}

fn names(meshes: &[&str]) -> BTreeSet<String> {
    meshes
        .iter()
        .flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")])
        .collect()
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_mesh_retires_the_old_one_and_creates_a_new_identity() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled(&names(&["mesh1", "mesh2"]), Duration::from_secs(30)).await;
    let (_, fabric) = estate.get("/api/fabric").await;
    let fabric_id = s(&fabric["id"]);
    let (_, m1) = estate.get("/api/meshes/mesh1").await;
    let (_, m2) = estate.get("/api/meshes/mesh2").await;
    let (mesh1_id, mesh2_id) = (s(&m1["id"]), s(&m2["id"]));
    let mesh1_pids: Vec<(String, u64)> = {
        let mut v = Vec::new();
        for n in before.iter().filter(|n| n["mesh"] == "mesh1") {
            let name = s(&n["name"]);
            // The bootstrap admin was adopted on day 0: its pid is the estate's own child.
            let pid = match estate.bootstrap_pid() {
                Some(p) if name == "mesh1.admin.1" => u64::from(p),
                _ => estate.pid_of(&name).await,
            };
            v.push((name, pid));
        }
        v
    };

    // Control moves to mesh2 before mesh1 goes: its admins advertise it in the topology.
    let mesh2_admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")
        .map(|n| s(&n["admin_api_base"]))
        .expect("a ready mesh2 admin");
    estate.admin = mesh2_admin.clone();

    // {mesh1, mesh2} -> {mesh2, mesh3}: one Build.
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh2"), mesh("mesh3")]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    estate.await_build(&b, Duration::from_secs(240)).await;
    let after = estate.settled(&names(&["mesh2", "mesh3"]), Duration::from_secs(60)).await;

    // Identities: the Fabric and mesh2 keep theirs; mesh3 is a new mesh; mesh1 is gone.
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id");
    let (_, m2_after) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&m2_after["id"]), mesh2_id, "an untouched mesh keeps its id");
    let (_, m3) = estate.get("/api/meshes/mesh3").await;
    let mesh3_id = s(&m3["id"]);
    assert!(!mesh3_id.is_empty() && mesh3_id != mesh1_id && mesh3_id != mesh2_id, "mesh3 is a new identity: {m3}");
    let (status, gone) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(status, 404, "mesh1 is no longer a mesh of the fabric: {gone}");
    assert!(!after.iter().any(|n| n["mesh"] == "mesh1"), "no mesh1 node remains: {after:#?}");
    for (name, pid) in &mesh1_pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }

    let (_, fabric_now) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric_now["admin_api_base"]);
    estate.stop().await;
    let spans = estate.spans();

    // mesh1 retired through the retire pipeline under B: each node drained, then terminated,
    // then removed from the topology.
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    for (name, _) in &mesh1_pids {
        let mine: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == name.as_str() && sp["attributes"]["build_id"] == b.as_str()).collect();
        let order: Vec<String> = {
            let mut v = mine.clone();
            v.sort_by_key(|sp| sp["start_unix_nano"].as_u64());
            v.iter().map(|sp| s(&sp["attributes"]["step"])).collect()
        };
        let at = |step: &str| order.iter().position(|x| x == step).unwrap_or_else(|| panic!("{name}: no {step} under {b}: {order:?}"));
        assert!(at("MarkDraining") < at("TerminateRuntime") && at("TerminateRuntime") < at("RemoveTopologyMembership"), "{name}: {order:?}");
        assert!(mine.iter().all(|sp| sp["attributes"]["outcome"] == "complete"), "{name}: every retire step completed");
    }

    // A desired change, not a recovery.
    assert!(
        !named(&spans, "rdm.node_admin.build.create.via-proven-drift").iter().any(|sp| s(&sp["attributes"]["scope"]).contains("mesh1")),
        "no recovery Build names mesh1"
    );
    let created = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!(!created.iter().any(|sp| sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.")), "B recreates nothing in mesh1");
    for n in names(&["mesh3"]) {
        assert!(created.iter().any(|sp| sp["attributes"]["build_id"] == b.as_str() && sp["attributes"]["node"] == n.as_str()), "B created {n}");
    }
}
