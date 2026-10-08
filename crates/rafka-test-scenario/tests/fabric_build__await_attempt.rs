//! i143 process E2E: a caller waits for the attempt its own request opened (RDM fabric-build).
//!
//! A real estate whose node-admin is the testkit's faulted one. A restart opens attempt 2 of the
//! accepted Build and answers 202 naming it; a receipt cut holds that attempt in its retire half.
//! While it is held, `await_attempt(build, 2)` must not return, whatever the Build's earlier
//! attempt left behind; released, it returns the Build at attempt 2, complete.

use rafka_test_scenario::estate::{wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use rafka_node_admin_core::deployment::pipeline::RetireStep;
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "await-attempt".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "await_attempt_waits_for_the_opened_attempt_while_it_is_held".into(),
    }
}

/// CONTRACT: after a restart answers 202 with attempt 2, `await_attempt(build, 2)` stays pending
/// while that attempt is held mid-flight and returns the Build at attempt 2, `complete`, once the
/// hold is released. The 202 names the opened attempt, and the Build reports that attempt.
#[tokio::test]
async fn await_attempt_waits_for_the_opened_attempt_while_it_is_held() {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let root = estate.root.clone();

    // A lone admin hears no other member and is cut off: two admins before anything else.
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2}]})).await;
    assert_eq!((status, &a["attempt"]), (202, &json!(1)), "an accepted Build answers its first attempt: {a}");
    let shape = a["build_id"].as_str().unwrap().to_string();
    estate.await_attempt(&shape, 1, Duration::from_secs(120)).await;
    estate.settled(&["mesh1.admin.1", "mesh1.admin.2"].iter().map(|n| n.to_string()).collect(), Duration::from_secs(60)).await;

    let node = "mesh1.rpc.1";
    let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh1", "kind": "rpc_node"})).await;
    assert_eq!((status, &a["attempt"]), (202, &json!(1)), "{a}");
    let build_id = a["build_id"].as_str().unwrap().to_string();
    estate.await_attempt(&build_id, 1, Duration::from_secs(120)).await;
    wait_for(&format!("{node} ready"), Duration::from_secs(60), || async { estate.node_opt(node).await.filter(|n| n["status"] == "ready-for-traffic") }).await;

    let nodes = estate.nodes().await;
    let primary = nodes.iter().find(|n| n["kind"] == "node_admin" && n["mesh"] == "mesh1" && n["is_primary"] == true).expect("mesh1 has an admin primary");
    let api = primary["admin_api_base"].as_str().unwrap().to_string();
    let door = Door::open(&root, primary["name"].as_str().unwrap(), &api).await;

    let cut = "restart:NodeRestarting";
    door.arm(cut, json!({"kind": "receipt", "step": RetireStep::NodeRestarting.name(), "operation": "retire-node", "node": node})).await;
    let (status, r) = estate.post(&format!("/api/nodes/{node}/restart"), &Value::Null).await;
    assert_eq!(status, 202, "{r}");
    assert_eq!(r["build_id"], build_id.as_str(), "a restart opens an attempt of the accepted Build: {r}");
    assert_eq!(r["attempt"], 2, "the restart answers the attempt it opened: {r}");
    door.wait_held(cut).await;

    let waiting = tokio::time::timeout(Duration::from_secs(3), estate.await_attempt(&build_id, 2, Duration::from_secs(120))).await;
    assert!(waiting.is_err(), "await_attempt returned while attempt 2 was held: {waiting:?}");
    let (_, held) = estate.get(&format!("/api/builds?id={build_id}")).await;
    assert_ne!(held["state"], "complete", "attempt 2 is held mid-flight: {held}");

    door.release(cut).await;
    let done = estate.await_attempt(&build_id, 2, Duration::from_secs(120)).await;
    assert_eq!((done["attempt"].as_u64(), done["state"].as_str()), (Some(2), Some("complete")), "{done}");
    estate.stop().await;
}
