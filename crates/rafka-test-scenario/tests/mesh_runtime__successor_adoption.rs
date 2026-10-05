//! i143.e4.s16 process E2E: a successor manages births it never launched
//! (rafka-v2 #2850).
//!
//! Build A completes (mesh1.admin.1 launches three rpc nodes). Only then is
//! mesh1.admin.2 born, so it never holds A's receipts: the Build topic hands
//! a new admin only active Builds. The launching admin is killed. From public
//! surfaces only:
//! - the successor holds the mesh and the fabric, and its view advertises
//!   every rpc node's data dir (current birth metadata, from membership);
//! - it restarts one of A's births (same node id, new incarnation) and
//!   retires another (its runtime is gone), from the runtime each birth
//!   publishes with its membership, never from Build history.
//!
//! Evidence: the bootstrap admin's ready span names its own self-adopted
//! runtime (day 0); each rpc node's ready span names the runtime its provider
//! recorded; the successor's `rafka.node_admin.runtime.update.via-adopt`
//! names the same locator fingerprint, `source = self-published-membership`,
//! and the successor as adopter and executor.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-runtime".into(),
        subfeature: "successor-adoption".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_successor_manages_births_it_never_launched".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

async fn build(estate: &Estate, admins: u32, rpcs: u32) {
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": admins, "rpc_node": rpcs}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_manages_births_it_never_launched() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    // Build A: the bootstrap admin launches three rpc nodes; A completes.
    build(&estate, 1, 3).await;
    let a_births = estate.settled_shape(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    // Only now is the successor born: A is no longer active.
    build(&estate, 2, 3).await;
    let nodes = estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let successor = nodes.iter().find(|n| n["name"] == "mesh1.admin.2").cloned().unwrap();
    let base = s(&successor["admin_api_base"]);
    let rpc = |name: &str| a_births.iter().find(|n| n["name"] == name).cloned().unwrap();
    let (restarted, retired) = (rpc("mesh1.rpc.2"), rpc("mesh1.rpc.3"));
    let retired_pid = estate.pid_of("mesh1.rpc.3").await;

    // Lose the launching admin.
    estate.kill_bootstrap();
    estate.admin = base.clone();
    let view = wait_for("the successor holds the mesh and the fabric", Duration::from_secs(40), || async {
        let v = estate.nodes().await;
        let me = v.iter().find(|n| n["name"] == "mesh1.admin.2")?;
        let gone = v.iter().any(|n| n["name"] == "mesh1.admin.1" && n["status"] == "dead");
        (gone && me["is_primary"] == true && me["is_fabric_primary"] == true).then_some(v)
    })
    .await;
    for n in view.iter().filter(|n| n["kind"] == "rpc_node") {
        assert!(n["data_dir"].as_str().is_some_and(|d| !d.is_empty()), "the successor's view carries {}'s data dir: {n}", n["name"]);
    }

    // Restart one of A's births and retire another, through the successor.
    let (status, a) = estate.post("/api/nodes/mesh1.rpc.2/restart", &json!({})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let after = wait_for("mesh1.rpc.2 ready under a new incarnation", Duration::from_secs(30), || async {
        let n = estate.node("mesh1.rpc.2").await;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != restarted["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["node_id"], restarted["node_id"], "a restart keeps the node id");
    let (status, a) = estate.delete("/api/nodes/mesh1.rpc.3").await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    wait_for("mesh1.rpc.3's runtime is gone", Duration::from_secs(30), || async { (!alive(retired_pid)).then_some(()) }).await;

    estate.stop().await;
    let spans = estate.spans();
    // Day 0: the bootstrap admin published the runtime it adopted for itself.
    let boot = named(&spans, "rafka.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node"] == "mesh1.admin.1").cloned().expect("the bootstrap admin is ready");
    assert_eq!(boot["attributes"]["runtime_locator_kind"], "process-pid-start");
    assert_eq!(boot["attributes"]["source"], "self-published-membership");
    for birth in [&restarted, &retired] {
        let name = s(&birth["name"]);
        let ready = named(&spans, "rafka.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == name.as_str() && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .cloned()
            .unwrap_or_else(|| panic!("{name} reports ready"));
        let adopted = named(&spans, "rafka.node_admin.runtime.update.via-adopt")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == name.as_str() && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .cloned()
            .unwrap_or_else(|| panic!("the successor adopted {name}'s runtime"));
        let (r, a) = (&ready["attributes"], &adopted["attributes"]);
        assert_eq!(a["adopter"], "mesh1.admin.2");
        assert_eq!(a["adopter_node_id"], successor["node_id"]);
        assert_eq!(a["execution_node_id"], successor["node_id"]);
        assert_eq!(a["source"], "self-published-membership");
        for k in ["deployment_id", "provider", "provider_control_domain_fingerprint", "runtime_locator_kind", "runtime_locator_fingerprint"] {
            assert_eq!(a[k], r[k], "{name}: the adopted runtime is the one it published ({k})");
        }
    }
}
