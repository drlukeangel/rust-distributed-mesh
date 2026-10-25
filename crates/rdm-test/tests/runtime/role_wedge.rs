//! i143.e11.s10 process E2E: status and lifecycle on a role (PRD §13.1; node-identity-and-state.md
//! §7.3). `{node_admin: 1, broker: 2, gateway: 1}`; a broker is frozen in place (SIGSTOP).
//! - Born, the broker declared `ReadyForTraffic` to its mesh primary over the status op: the
//!   view shows it `declared` before anything asks.
//! - Silent, it is marked `pending-reconnect` at the staleness floor, and its mesh's primary runs node
//!   investigation on it: unreached, the provider reads its runtime suspended (`os-stopped`), and node
//!   recovery re-creates it at once at the same path.name with a new node id, over a newly allocated
//!   data dir (`inherited = false`, R-FM2; node-recovery.md step 3, Q111). Nothing holds and nothing waits
//!   for a proof of termination.
//! - The broker primary is heard throughout and stays primary (elections §5).

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeStatus};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-runtime".into(),
        subfeature: "role-wedge".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_wedged_broker_is_found_os_stopped_and_re_created_at_its_path_over_a_fresh_data_dir".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wedged_broker_is_found_os_stopped_and_re_created_at_its_path_over_a_fresh_data_dir() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let build_id = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 2), (NodeKind::Gateway, 1)])] }).await.build_id.to_string();
    let want = ["mesh1.admin.1", "mesh1.broker.1", "mesh1.broker.2", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(30)).await);
    let target = nodes.iter().find(|n| n["kind"] == "broker" && n["is_primary"] == false).cloned().expect("a non-primary broker");
    let (path, birth) = (s(&target["name"]), s(&target["incarnation_id"]));
    // Born ready, the broker declared its readiness to its mesh primary by itself (the status op). No stream
    // announces it: the admin's view carries it (`declared`).
    estate.until_node(&path, &format!("{path}'s own ReadyForTraffic declared at the authority"), Duration::from_secs(20), |n| n.filter(|n| n.declared.as_deref() == Some("ReadyForTraffic")).map(|_| ())).await;

    // Frozen: pending-reconnect at the floor, then re-created at its path.name.
    let floor = rdm_transport::topology::staleness_floor();
    let primary_before = nodes.iter().find(|n| n["kind"] == "broker" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("a broker primary");
    let pid = estate.pid_of(&path).await;
    assert!(std::process::Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success(), "SIGSTOP {pid} ({path})");
    let old_id = s(&target["node_id"]);
    let primary_now = || {
        let primary_before = primary_before.clone();
        move |views: &[rdm_client::NodeView]| {
            let ns = Estate::values(views);
            let primary = ns.iter().find(|n| n["kind"] == "broker" && n["is_primary"] == true).map(|n| s(&n["name"]));
            assert_eq!(primary.as_deref(), Some(primary_before.as_str()), "the heard broker primary stays primary: {ns:#?}");
            ns
        }
    };
    let mut check = primary_now();
    let back = estate
        .until_view(&format!("{path} re-created with a new node id and ready-for-traffic"), floor * 6 + Duration::from_secs(60), |views| {
            let ns = check(views);
            ns.into_iter().find(|n| n["name"] == path.as_str() && s(&n["node_id"]) != old_id && n["status"] == "ready-for-traffic")
        })
        .await;
    let _ = std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status();
    estate.artifact("recreated.json", &json!({"target": target, "back": back}));
    assert_eq!(estate.accepted_build_id().await, build_id, "the accepted Build is unchanged: recovery is an attempt of it");
    estate.stop().await;

    let spans = estate.spans();
    let submit = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == old_id.as_str()).cloned().unwrap_or_else(|| panic!("{path}'s recovery was never submitted"));
    assert_eq!(s(&submit["attributes"]["verdict"]), "os-stopped", "the provider read the wedged runtime suspended: {submit}");
    assert_ne!(s(&back["data_dir"]), s(&target["data_dir"]), "an os-stopped runtime's re-create runs over a newly allocated data dir (R-FM2): {back}");
    estate.record_trace_url(submit["trace_id"].as_str().unwrap_or(""));
}
