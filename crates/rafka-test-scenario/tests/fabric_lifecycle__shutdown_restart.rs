//! i143 process E2E: a fabric shutdown survives an all-admin restart
//! (fabric-mesh-lifecycle.md §11.1, FML-21..27; RDM #48).
//!
//! From public surfaces, the admins' data dirs and the estate's span evidence only:
//! 1. Only the fabric-primary begins a shutdown; every admin persists its own copy
//!    (`fabric/shutdown.json`) and gossips `Draining`.
//! 2. Every admin is killed while the shutdown is active, and an rpc node with it; each admin is
//!    restarted on its existing data dir (the bootstrap admin under its Fabric and Mesh ids, the
//!    launched admin from the launch its provider recorded).
//! 3. Each restarted admin reloads the same `FabricShutdown`, comes up `Draining` with
//!    reconciliation frozen, and nothing is reborn: no Build, no drift recovery, no new birth of
//!    the killed rpc node.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-lifecycle".into(),
        subfeature: "shutdown-restart".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_fabric_shutdown_survives_an_all_admin_restart".into(),
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn pid_in(data_dir: &str) -> u32 {
    let d: Value = serde_json::from_slice(&std::fs::read(format!("{data_dir}/deployment.json")).unwrap()).unwrap();
    d["pid"].as_u64().unwrap_or_else(|| panic!("no pid in {data_dir}/deployment.json: {d}")) as u32
}

fn shutdown_file(data_dir: &str) -> String {
    format!("{data_dir}/fabric/shutdown.json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "RDM #48: a relaunched admin blocks in FabricBuildStateAdapter::join (subscribe_and_join waits on a seed that died with the old fabric); goes GREEN with the dead-seeds ruling"]
async fn a_fabric_shutdown_survives_an_all_admin_restart() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let nodes = estate.settled_shape(&[("mesh1", 2, 2)], Duration::from_secs(30)).await;
    let (_, fabric) = estate.get("/api/fabric").await;
    let (fabric_id, mesh_id) = (fabric["id"].as_str().unwrap().to_string(), fabric["meshes"][0]["id"].as_str().unwrap().to_string());
    let primary = fabric["fabric_primary"].as_str().unwrap().to_string();
    let admins: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "node_admin").cloned().collect();
    let launched = admins.iter().find(|n| n["name"] != "mesh1.admin.1").unwrap();
    let launched_dir = launched["data_dir"].as_str().unwrap().to_string();
    let bootstrap_dir = estate.bootstrap_data_dir("mesh1").display().to_string();
    let primary_base = admins.iter().find(|n| n["name"] == primary.as_str()).unwrap()["admin_api_base"].as_str().unwrap().to_string();
    let other_base = admins.iter().find(|n| n["name"] != primary.as_str()).unwrap()["admin_api_base"].as_str().unwrap().to_string();
    estate.artifact("nodes-before.json", &json!(nodes));

    // 1. Only the fabric-primary begins it; every admin persists its own copy.
    let (status, refused) = estate.http_post(&other_base, "/api/shutdown", &json!({})).await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(refused["error"], "rejected-not-authority", "{refused}");
    assert_eq!(refused["fabric_primary"], primary.as_str(), "{refused}");
    let (status, begun) = estate.http_post(&primary_base, "/api/shutdown", &json!({})).await;
    assert_eq!(status, 202, "{begun}");
    wait_for("every admin persisted the shutdown", Duration::from_secs(15), || async {
        [&bootstrap_dir, &launched_dir].iter().all(|d| Path::new(&shutdown_file(d)).exists()).then_some(())
    })
    .await;
    let persisted: Vec<String> = [&bootstrap_dir, &launched_dir].iter().map(|d| std::fs::read_to_string(shutdown_file(d)).unwrap()).collect();
    assert_eq!(persisted[0], persisted[1], "both admins hold the same record");
    let record: Value = serde_json::from_str(&persisted[0]).unwrap();
    assert_eq!(record["format"], "fabric-shutdown/1");
    assert_eq!(record["record"]["initiated_by"], primary.as_str(), "{record}");
    wait_for("every admin says Draining", Duration::from_secs(15), || async {
        let v = estate.http_get(&primary_base, "/api/nodes").await.1;
        v["nodes"].as_array().unwrap().iter().filter(|n| n["kind"] == "node_admin").all(|n| n["status"] == "draining").then_some(())
    })
    .await;

    // 2. Kill an rpc node and every admin while the shutdown is active.
    let rpc = nodes.iter().find(|n| n["kind"] == "rpc_node").unwrap();
    let (rpc_name, rpc_dir, rpc_incarnation) =
        (rpc["name"].as_str().unwrap().to_string(), rpc["data_dir"].as_str().unwrap().to_string(), rpc["incarnation_id"].as_str().unwrap().to_string());
    estate.kill_pid(pid_in(&rpc_dir));
    estate.kill_pid(pid_in(&launched_dir));
    estate.kill_bootstrap();
    let before_restart = now_ns();
    let shutdown_bytes_before = std::fs::read(shutdown_file(&bootstrap_dir)).unwrap();

    // Restart each on its existing data dir.
    let bases = vec![estate.relaunch_bootstrap("fabric1", "mesh1", &fabric_id, &mesh_id), estate.relaunch_admin(&launched_dir)];
    estate.admin = bases[0].clone();

    // 3. Each comes up frozen under the same shutdown; nothing is reborn.
    for base in &bases {
        let (status, f) = estate.http_get(base, "/api/fabric").await;
        assert_eq!(status, 200, "{f}");
        assert_eq!(f["id"], fabric_id.as_str(), "the same Fabric: {f}");
        assert_eq!(f["shutdown"]["initiated_by"], primary.as_str(), "the shutdown reloaded: {f}");
        assert!(["frozen", "draining"].contains(&f["shutdown"]["phase"].as_str().unwrap_or("")), "{f}");
    }
    for d in [&bootstrap_dir, &launched_dir] {
        assert_eq!(std::fs::read(shutdown_file(d)).unwrap(), shutdown_bytes_before, "the record is never replaced: {d}");
    }
    wait_for("each restarted admin says Draining", Duration::from_secs(20), || async {
        let mut all = true;
        for base in &bases {
            let v = estate.http_get(base, "/api/nodes").await.1;
            let mine = v["nodes"].as_array().unwrap().iter().filter(|n| n["kind"] == "node_admin" && n["admin_api_base"] == base.as_str()).collect::<Vec<_>>();
            all &= !mine.is_empty() && mine.iter().all(|n| n["status"] == "draining");
        }
        all.then_some(())
    })
    .await;
    // Long enough for drift recovery or a Build to have acted, had anything resumed.
    tokio::time::sleep(Duration::from_secs(8)).await;
    for base in &bases {
        let v = estate.http_get(base, "/api/nodes").await.1;
        let reborn = v["nodes"].as_array().unwrap().iter().any(|n| n["name"] == rpc_name.as_str() && n["incarnation_id"] != rpc_incarnation.as_str() && n["status"] != "dead");
        assert!(!reborn, "{rpc_name} was reborn after the restart: {v}");
    }
    let spans = estate.spans();
    let after = |name: &str| named(&spans, name).into_iter().filter(|s| s["start_unix_nano"].as_u64().unwrap_or(0) > before_restart).count();
    assert_eq!(after("rafka.node_admin.build.create.via-proven-drift"), 0, "no drift recovery after the restart");
    assert_eq!(after("rafka.node_admin.build.update.via-reconcile"), 0, "no reconciliation after the restart");
    assert_eq!(after("rafka.node_admin.node.create.via-build"), 0, "no birth after the restart");
    let learned = named(&spans, "rafka.node_admin.fabric.update.via-shutdown-learned")
        .into_iter()
        .filter(|s| s["start_unix_nano"].as_u64().unwrap_or(0) > before_restart)
        .map(|s| s["attributes"]["via"].as_str().unwrap_or("").to_string())
        .collect::<Vec<_>>();
    assert!(learned.is_empty(), "a restarted admin held the shutdown from its own storage, it learned nothing anew: {learned:?}");
    estate.artifact("nodes-after-restart.json", &json!(estate.http_get(&bases[0], "/api/nodes").await.1));
    estate.stop_relaunched();
}
