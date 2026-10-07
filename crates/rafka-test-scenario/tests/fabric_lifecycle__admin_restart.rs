//! i143 process E2E: node-admin storage survives an all-admin restart (RDM #43/#44/#45, and the
//! fabric.storage/builds.storage restart proof of #48).
//!
//! From public surfaces, the admins' data dirs and the estate's span evidence only. Each admin is
//! killed and started again with nothing but its data dir and the operator's environment
//! (provider, binaries, evidence): its identity, Mesh, Fabric, `Fabric.build_id` and the Build it
//! names come from its own storage.
//!
//! 1. Every admin restarts as the same logical node (same NodeId, a new incarnation superseding
//!    its last), in the same Mesh and Fabric, under the same accepted Build; the rpc nodes, which
//!    stayed up, are the same births (nothing reborn); the fabric elects one primary again and
//!    accepts the next topology change.
//! 2. A fabric shutdown held when every admin dies is reloaded by each one from its own
//!    fabric.storage: it comes up Draining with reconciliation frozen, and nothing is reborn.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-lifecycle".into(),
        subfeature: "admin-restart".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn pid_in(data_dir: &str) -> u32 {
    let d: Value = serde_json::from_slice(&std::fs::read(format!("{data_dir}/deployment.json")).unwrap()).unwrap();
    d["pid"].as_u64().unwrap_or_else(|| panic!("no pid in {data_dir}/deployment.json: {d}")) as u32
}

fn alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists() && !std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default().contains(") Z ")
}

/// Bring up mesh1 with two admins and two rpc nodes; return the settled view.
async fn mn(estate: &Estate) -> Vec<Value> {
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 2, 2)], Duration::from_secs(30)).await
}

/// Kill every admin (SIGKILL: nothing flushed), leaving the rpc nodes as they are.
fn kill_admins(estate: &mut Estate, launched_dir: &str) {
    let launched = pid_in(launched_dir);
    if alive(launched) {
        estate.kill_pid(launched);
    }
    estate.kill_bootstrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_admin_restarts_on_its_own_storage_and_the_fabric_reconverges() {
    let mut estate = Estate::bootstrap(owner("every_admin_restarts_on_its_own_storage"), "fabric1", "mesh1").await;
    let before = mn(&estate).await;
    let (_, fabric) = estate.get("/api/fabric").await;
    let (fabric_id, build_id, mesh_id) = (s(&fabric["id"]), s(&fabric["build_id"]), s(&fabric["meshes"][0]["id"]));
    let by_name = |nodes: &[Value], n: &str| nodes.iter().find(|x| x["name"] == n).cloned().unwrap_or_else(|| panic!("no {n} in {nodes:#?}"));
    let launched_dir = s(&by_name(&before, "mesh1.admin.2")["data_dir"]);
    let bootstrap_dir = estate.bootstrap_data_dir("mesh1");
    estate.artifact("nodes-before.json", &json!(before));

    kill_admins(&mut estate, &launched_dir);
    let restarted_at = now_ns();
    let base1 = estate.restart_admin(&bootstrap_dir);
    let _base2 = estate.restart_admin(&PathBuf::from(&launched_dir));
    estate.admin = base1.clone();

    // The same nodes, the admins under new incarnations, the rpc nodes untouched; one primary.
    let after = wait_for("the restarted fabric settles with the same nodes", Duration::from_secs(90), || async {
        let nodes = estate.nodes().await;
        estate.artifact("nodes-last-seen.json", &json!(nodes));
        let ready = |n: &str| nodes.iter().any(|x| x["name"] == n && x["status"] == "ready-for-traffic");
        let primaries = nodes.iter().filter(|x| x["is_fabric_primary"] == true).count();
        (["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2"].iter().all(|n| ready(n)) && primaries == 1).then_some(nodes)
    })
    .await;
    estate.artifact("nodes-after.json", &json!(after));
    for n in ["mesh1.admin.1", "mesh1.admin.2"] {
        let (b, a) = (by_name(&before, n), by_name(&after, n));
        assert_eq!(a["node_id"], b["node_id"], "{n} restarts as the same logical node");
        assert_ne!(a["incarnation_id"], b["incarnation_id"], "{n} runs a new incarnation");
    }
    for n in ["mesh1.rpc.1", "mesh1.rpc.2"] {
        assert_eq!(by_name(&after, n)["incarnation_id"], by_name(&before, n)["incarnation_id"], "{n} is the same birth: nothing reborn");
    }
    let (_, f) = estate.get("/api/fabric").await;
    assert_eq!((s(&f["id"]), s(&f["build_id"]), s(&f["meshes"][0]["id"])), (fabric_id.clone(), build_id.clone(), mesh_id.clone()), "the same Fabric, Build and Mesh: {f}");
    let (status, b) = estate.get(&format!("/api/builds?id={build_id}")).await;
    assert_eq!(status, 200, "the accepted Build is held after an all-admin restart: {b}");

    // Authority is back: the next topology change is accepted and realized.
    let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh1", "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{a}");
    let next = s(&a["build_id"]);
    estate.await_build(&next, Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;

    estate.stop().await;
    let spans = estate.spans();
    let after_restart = |sp: &&Value| sp["start_unix_nano"].as_u64().unwrap_or(0) > restarted_at;
    let restarts: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-restart").into_iter().filter(after_restart).collect();
    for n in ["mesh1.admin.1", "mesh1.admin.2"] {
        let r = restarts.iter().find(|sp| sp["attributes"]["node"] == n).unwrap_or_else(|| panic!("{n} restarted from nodes.storage: {restarts:?}"));
        assert_eq!(r["attributes"]["node_id"], by_name(&before, n)["node_id"]);
        assert_eq!(r["attributes"]["supersedes"], by_name(&before, n)["incarnation_id"], "{n} supersedes the incarnation it last ran");
    }
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(after_restart).next().is_none(), "no repair: nothing was lost");
    let created: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(after_restart)
        .map(|sp| format!("{} {}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["build_id"])))
        .collect();
    assert_eq!(created, [format!("mesh1.rpc.3 {next}")], "the only birth after the restart is the one the next Build asked for");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_shutdown_survives_an_all_admin_restart() {
    let mut estate = Estate::bootstrap(owner("a_fabric_shutdown_survives_an_all_admin_restart"), "fabric1", "mesh1").await;
    let before = mn(&estate).await;
    let (_, fabric) = estate.get("/api/fabric").await;
    let (fabric_id, primary) = (s(&fabric["id"]), s(&fabric["fabric_primary"]));
    let launched_dir = s(&before.iter().find(|n| n["name"] == "mesh1.admin.2").unwrap()["data_dir"]);
    let bootstrap_dir = estate.bootstrap_data_dir("mesh1");
    let shutdown_file = |d: &Path| d.join("fabric").join("shutdown.json");

    // Only the fabric-primary begins a shutdown: ask it at its own control API.
    let primary_base = s(&before.iter().find(|n| n["name"] == primary.as_str()).unwrap()["admin_api_base"]);
    let (status, begun) = estate.http_post(&primary_base, "/api/shutdown", &json!({})).await;
    assert_eq!(status, 202, "{begun}");
    wait_for("every admin persisted the shutdown", Duration::from_secs(15), || async {
        [bootstrap_dir.clone(), PathBuf::from(&launched_dir)].iter().all(|d| shutdown_file(d).exists()).then_some(())
    })
    .await;
    let record = std::fs::read(shutdown_file(&bootstrap_dir)).unwrap();
    assert_eq!(record, std::fs::read(shutdown_file(Path::new(&launched_dir))).unwrap(), "every admin holds the same record");

    kill_admins(&mut estate, &launched_dir);
    let restarted_at = now_ns();
    let bases = [estate.restart_admin(&bootstrap_dir), estate.restart_admin(&PathBuf::from(&launched_dir))];
    estate.admin = bases[0].clone();
    for base in &bases {
        let (status, f) = estate.http_get(base, "/api/fabric").await;
        assert_eq!(status, 200, "{f}");
        assert_eq!(s(&f["id"]), fabric_id, "the same Fabric: {f}");
        assert_eq!(s(&f["shutdown"]["initiated_by"]), primary, "the shutdown reloaded from its own storage: {f}");
    }
    for d in [bootstrap_dir.clone(), PathBuf::from(&launched_dir)] {
        assert_eq!(std::fs::read(shutdown_file(&d)).unwrap(), record, "the record is never replaced: {}", d.display());
    }
    wait_for("each restarted admin says Draining", Duration::from_secs(20), || async {
        let mut all = true;
        for base in &bases {
            let v = estate.http_get(base, "/api/nodes").await.1;
            all &= v["nodes"].as_array().unwrap().iter().any(|n| n["admin_api_base"] == base.as_str() && n["status"] == "draining");
        }
        all.then_some(())
    })
    .await;

    // No admin holds a seat after this restart, so nothing drains: the operator stops it locally.
    estate.stop_locally().await;
    let spans = estate.spans();
    let after = |name: &str| named(&spans, name).into_iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) > restarted_at).count();
    assert_eq!(after("rdm.node_admin.build.update.via-proven-drift"), 0, "no repair after the restart");
    assert_eq!(after("rdm.node_admin.build.update.via-reconcile"), 0, "no reconciliation after the restart");
    assert_eq!(after("rdm.node_admin.node.create.via-build"), 0, "no birth after the restart");
    assert_eq!(after("rdm.node_admin.fabric.update.via-shutdown-learned"), 0, "each admin held the shutdown from its own storage; it learned nothing anew");
}
