//! i143 process E2E: a caller waits for the attempt its own request opened (RDM fabric-build).
//!
//! A real estate whose node-admin is the testkit's faulted one. A restart opens attempt 2 of the
//! accepted Build and answers 202 naming it; a receipt cut holds that attempt in its retire half.
//! While it is held, `await_attempt(build, 2)` must not return, whatever the Build's earlier
//! attempt left behind; released, it returns the Build at attempt 2, complete.

use crate::build_log;
use rafka_node_admin_client::{Frame, LaunchKind as NodeKind, NodeSpec, NodeStatus};
use rafka_node_admin_core::deployment::pipeline::RetireStep;
use rafka_test_scenario::estate::{Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use serde_json::json;
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

/// CONTRACT: after a restart is accepted at attempt 2, its reply stream carries no `Complete` while that
/// attempt is held mid-flight, and ends `Complete` once the hold is released, with the Build, as the
/// fabric-primary's log holds it, `complete` at attempt 2. The accepted call names the opened attempt, and
/// the Build reports that attempt.
#[tokio::test]
async fn await_attempt_waits_for_the_opened_attempt_while_it_is_held() {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let root = estate.root.clone();

    // A lone admin hears no other member and is cut off: two admins before anything else.
    let shape = estate.reconcile_shape("fabric1", &[("mesh1", 2, 0)]).await;
    assert_eq!(shape.attempt, 1, "an accepted Build answers its first attempt: {shape:?}");
    estate.settles_to(&[("mesh1", 2, 0)], Duration::from_secs(60)).await;

    let node = "mesh1.rpc.1";
    let mut spawn = estate.nodes_obj().await.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of an rpc node in mesh1: {e}"));
    let created = Estate::complete(&mut spawn).await;
    assert_eq!(created.attempt, 1, "{created:?}");
    let build_id = created.build_id.0.clone();
    estate.until_node(node, &format!("{node} ready"), Duration::from_secs(60), |n| n.filter(|n| n.status == NodeStatus::ReadyForTraffic).map(|_| ())).await;

    let primary = estate.node_views().await.into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.mesh == "mesh1" && n.is_primary).expect("mesh1 has an admin primary");
    let door = Door::open(&root, &primary.name.to_string(), primary.admin_api_base.as_deref().expect("the admin primary advertises its control API")).await;

    let cut = "restart:NodeRestarting";
    door.arm(cut, json!({"kind": "receipt", "step": RetireStep::NodeRestarting.name(), "operation": "restart-node", "node": node})).await;
    // The initial fabric refuses a node restart until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let mut restart = estate.nodes_obj().await.restart(&node.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.restart of {node}: {e}"));
    let opened = restart.accepted().clone();
    assert_eq!(opened.build_id.0, build_id, "a restart opens an attempt of the accepted Build: {opened:?}");
    assert_eq!(opened.attempt, 2, "the restart answers the attempt it opened: {opened:?}");
    door.wait_held(cut).await;

    let mut seen = Vec::new();
    let waiting = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = restart.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("the restart's reply stream broke while attempt 2 was held: {e}"));
            let complete = matches!(frame, Frame::Complete);
            seen.push(frame);
            if complete {
                return;
            }
        }
    })
    .await;
    assert!(waiting.is_err(), "the restart's reply stream completed while attempt 2 was held: {seen:?}");
    let held = build_log::build_value(&estate.carrier().await, &build_id).await;
    assert_ne!(held["state"], "complete", "attempt 2 is held mid-flight: {held}");

    door.release(cut).await;
    let finished = Estate::complete(&mut restart).await;
    assert_eq!((finished.build_id.0.as_str(), finished.attempt), (build_id.as_str(), 2), "{finished:?}");
    let done = build_log::build_fact(&estate.carrier().await, &build_id, "attempt 2 complete", Duration::from_secs(120), |b| b.attempt == 2 && b.state == rafka_node_admin_core::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    assert!(done.attempt == 2 && done.state == rafka_node_admin_core::build_state::BuildState::Complete, "{done:?}");
    estate.stop().await;
}
