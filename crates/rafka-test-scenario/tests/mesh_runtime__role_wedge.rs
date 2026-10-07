//! i143.e11.s10 process E2E: status and lifecycle on a role (PRD §13.1; fabric-node-lifecycle.md
//! §7.3). `{node_admin: 1, broker: 2, gateway: 1}`; a broker is frozen in place (SIGSTOP).
//! - Born, the broker declared `ReadyForTraffic` to its mesh primary over the status op: the
//!   view shows it `declared` before anything asks.
//! - Silent, it is marked `pending-reconnect` at the staleness floor, tickled by its mesh's
//!   primary (one QUIC connect, direct then via a peer: `via-offline-tickle-failed`), the hold-down
//!   opens, and a floor later it is `dead`.
//! - Its runtime still runs, so it is held, never replaced: no Build removes it and no other birth
//!   takes its path. Thawed, the same birth is back `ready-for-traffic` (`via-offline-returned`).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-runtime".into(),
        subfeature: "role-wedge".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_wedged_broker_is_marked_then_held_and_comes_back_as_the_same_birth".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wedged_broker_is_marked_then_held_and_comes_back_as_the_same_birth() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "broker": 2, "gateway": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    let build_id = a["build_id"].as_str().unwrap().to_string();
    estate.await_build(&build_id, Duration::from_secs(120)).await;
    let want = ["mesh1.admin.1", "mesh1.broker.1", "mesh1.broker.2", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    let nodes = estate.settled(&want, Duration::from_secs(30)).await;
    let target = nodes.iter().find(|n| n["kind"] == "broker" && n["is_primary"] == false).cloned().expect("a non-primary broker");
    let (path, birth) = (s(&target["name"]), s(&target["incarnation_id"]));
    // Born ready, the broker declared its readiness to its mesh primary by itself (the status op).
    wait_for(&format!("{path}'s own ReadyForTraffic declared at the authority"), Duration::from_secs(20), || async {
        estate.nodes().await.iter().find(|n| n["name"] == path.as_str()).filter(|n| n["declared"] == "ReadyForTraffic").map(|_| ())
    })
    .await;

    // Frozen: pending-reconnect at the floor, then dead after the tickle and the hold-down.
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let pid = estate.pid_of(&path).await;
    assert!(std::process::Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success(), "SIGSTOP {pid} ({path})");
    let status_of = |nodes: &[Value]| nodes.iter().find(|n| n["name"] == path.as_str()).map(|n| s(&n["status"])).unwrap_or_default();
    wait_for(&format!("{path} marked pending-reconnect"), floor * 2 + Duration::from_secs(20), || async { (status_of(&estate.nodes().await) == "pending-reconnect").then_some(()) }).await;
    wait_for(&format!("{path} marked dead"), floor * 3 + Duration::from_secs(30), || async { (status_of(&estate.nodes().await) == "dead").then_some(()) }).await;
    let held = estate.nodes().await;
    assert_eq!(held.iter().filter(|n| n["name"] == path.as_str()).count(), 1, "the path is held by its one birth: {held:#?}");
    assert_eq!(s(&held.iter().find(|n| n["name"] == path.as_str()).unwrap()["incarnation_id"]), birth, "silence replaced nothing");
    let (_, fabric) = estate.get("/api/fabric").await;
    assert_eq!(fabric["build_id"], build_id.as_str(), "the accepted Build is unchanged: {fabric}");

    // Thawed: the same birth is back.
    let _ = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status();
    let back = wait_for(&format!("{path} back as {birth}"), floor * 2 + Duration::from_secs(30), || async {
        let nodes = estate.nodes().await;
        nodes.iter().find(|n| n["name"] == path.as_str() && n["incarnation_id"] == birth.as_str() && n["status"] == "ready-for-traffic").cloned()
    })
    .await;
    estate.artifact("held.json", &json!({"target": target, "held": held, "back": back}));
    estate.stop().await;

    let spans = estate.spans();
    let node_id = s(&target["node_id"]);
    let tickled = named(&spans, "rdm.node_admin.node.resolve.via-offline-tickle-failed").into_iter().find(|sp| sp["attributes"]["node_id"] == node_id.as_str()).cloned().unwrap_or_else(|| panic!("{path} was never tickled"));
    assert!(named(&spans, "rdm.node_admin.node.update.via-offline-hold-down-opened").iter().any(|sp| sp["attributes"]["node_id"] == node_id.as_str()), "the hold-down opened for {path}");
    assert!(named(&spans, "rdm.node_admin.node.update.via-offline-returned").iter().any(|sp| sp["attributes"]["node_id"] == node_id.as_str()), "{path} returned");
    assert!(!named(&spans, "rdm.node_admin.node.delete.via-build").iter().any(|sp| sp["attributes"]["node"] == path.as_str()), "silence removed nothing");
    estate.record_trace_url(tickled["trace_id"].as_str().unwrap_or(""));
}
