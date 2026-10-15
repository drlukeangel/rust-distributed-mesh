//! i143.e11.s10 process E2E: status and lifecycle on a role (PRD §13.1; node-identity-and-state.md
//! §7.3). `{node_admin: 1, broker: 2, gateway: 1}`; a broker is frozen in place (SIGSTOP).
//! - Born, the broker declared `ReadyForTraffic` to its mesh primary over the status op: the
//!   view shows it `declared` before anything asks.
//! - Silent, it is marked `pending-reconnect` at the staleness floor, tickled by its mesh's
//!   primary (one QUIC connect, direct then via a peer: `via-offline-tickle-failed`), the hold-down
//!   opens, and a floor later it is `dead`.
//! - Its runtime still runs, so it is held, never replaced: no Build removes it and no other birth
//!   takes its path. Thawed, the same birth is back `ready-for-traffic` (`via-offline-returned`).

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
        test: "a_wedged_broker_is_marked_then_held_and_comes_back_as_the_same_birth".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wedged_broker_is_marked_then_held_and_comes_back_as_the_same_birth() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let build_id = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 2), (NodeKind::Gateway, 1)])] }).await.build_id.to_string();
    let want = ["mesh1.admin.1", "mesh1.broker.1", "mesh1.broker.2", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(30)).await);
    let target = nodes.iter().find(|n| n["kind"] == "broker" && n["is_primary"] == false).cloned().expect("a non-primary broker");
    let (path, birth) = (s(&target["name"]), s(&target["incarnation_id"]));
    // Born ready, the broker declared its readiness to its mesh primary by itself (the status op). No stream
    // announces it: the admin's view carries it (`declared`).
    estate.until_node(&path, &format!("{path}'s own ReadyForTraffic declared at the authority"), Duration::from_secs(20), |n| n.filter(|n| n.declared.as_deref() == Some("ReadyForTraffic")).map(|_| ())).await;

    // Frozen: pending-reconnect at the floor, then dead after the tickle and the hold-down. The unheard state is
    // the admin's own detection: no call is held open to wait on, so the view is read until it holds it.
    let floor = rdm_transport::membership::staleness_floor();
    let pid = estate.pid_of(&path).await;
    assert!(std::process::Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success(), "SIGSTOP {pid} ({path})");
    estate.until_node(&path, &format!("{path} marked pending-reconnect"), floor * 2 + Duration::from_secs(20), |n| n.filter(|n| n.status == NodeStatus::PendingReconnect).map(|_| ())).await;
    estate.until_node(&path, &format!("{path} marked dead"), floor * 3 + Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Dead).map(|_| ())).await;
    let held = estate.node_values().await;
    assert_eq!(held.iter().filter(|n| n["name"] == path.as_str()).count(), 1, "the path is held by its one birth: {held:#?}");
    assert_eq!(s(&held.iter().find(|n| n["name"] == path.as_str()).unwrap()["incarnation_id"]), birth, "silence replaced nothing");
    assert_eq!(estate.accepted_build_id().await, build_id, "the accepted Build is unchanged");

    // Thawed: the same birth is back.
    let _ = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status();
    let back = estate
        .until_node(&path, &format!("{path} back as {birth}"), floor * 2 + Duration::from_secs(30), |n| {
            n.filter(|n| n.incarnation_id.as_ref().is_some_and(|i| i.to_string() == birth) && n.status == NodeStatus::ReadyForTraffic).map(|n| serde_json::to_value(n).expect("a view serializes"))
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
