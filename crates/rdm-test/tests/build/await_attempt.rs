//! i143 process E2E: a caller waits for the attempt its own request opened (RDM fabric-build).
//!
//! A real estate whose node-admin is the testkit's faulted one. A stop opens the next attempt of
//! the accepted Build and answers 202 naming it; a receipt cut holds that attempt in its stop.
//! While it is held, `await_attempt(build, attempt)` must not return, whatever the Build's earlier
//! attempts left behind; released, it returns the Build at that attempt, complete.

use crate::build_log;
use rdm_client::{Frame, LaunchKind as NodeKind, NodeStatus};
use rdm_admin::deployment::pipeline::DeleteStep;
use rdm_test::estate::{Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
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

/// CONTRACT: after a stop is accepted at the attempt after the accepted Build's Complete (the Build's
/// run, then its Complete after the fabric's change, Q154), its reply stream carries no `Complete` while that attempt is
/// held mid-flight, and ends `Complete` once the hold is released, with the Build, as the
/// fabric-primary's log holds it, `complete` at that attempt. The accepted call names the opened
/// attempt, and the Build reports that attempt.
#[tokio::test]
async fn await_attempt_waits_for_the_opened_attempt_while_it_is_held() {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let mut estate = Estate::bootstrap_on(owner(), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");

    // A lone admin hears no other member and is cut off: two admins before anything else.
    let shape = estate.reconcile_shape("fabric1", &[("mesh1", 2, 0)]).await;
    assert_eq!(shape.attempt, 1, "an accepted Build answers its first attempt: {shape:?}");
    estate.settles_to(&[("mesh1", 2, 0)], Duration::from_secs(60)).await;

    let first = "mesh1.rpc.1";
    let mut spawn = estate.create_node("mesh1", NodeKind::RpcNode).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of an rpc node in mesh1: {e}"));
    let created = Estate::complete(&mut spawn).await;
    assert_eq!(created.attempt, 1, "{created:?}");
    estate.until_node(first, &format!("{first} ready"), Duration::from_secs(60), |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;
    // A second rpc node, and the stop is of the one that is not the node-type primary: a stop
    // of the role's node moves the role first and needs a secondary to move it to
    // (node-primary-update-fabric-mesh-and-node-type.md Part C1, "no secondary"). The stop opens an
    // attempt of the second create's Build, the accepted one.
    let mut second = estate.create_node("mesh1", NodeKind::RpcNode).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of a second rpc node in mesh1: {e}"));
    let created = Estate::complete(&mut second).await;
    assert_eq!(created.attempt, 1, "{created:?}");
    let build_id = created.build_id.to_string();
    estate.until_node("mesh1.rpc.2", "mesh1.rpc.2 ready", Duration::from_secs(60), |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;
    let views = estate.node_views().await;
    let node_name = views.iter().find(|n| n.kind == NodeKind::RpcNode && n.mesh == "mesh1" && !n.is_primary).map(|n| n.name.to_string()).expect("one of mesh1's two rpc nodes does not hold the node-type primary role");
    let node = node_name.as_str();

    let primary = estate.node_views().await.into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.mesh == "mesh1" && n.is_primary).expect("mesh1 has an admin primary");
    let door = Door::at(&estate, &primary.name.to_string()).await;

    let cut = "stop:StopNode";
    door.arm(cut, json!({"kind": "receipt", "step": DeleteStep::StopNode.name(), "operation": "node.stop", "node": node})).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // The create put the fabric back to pending, so its Build is Complete at the attempt after its run,
    // claimed once the fabric's ready state was set (Q154): the stop opens the attempt after that one.
    let attempt = estate.attempt_of_build(&build_id).await as u32 + 1;
    let mut stop = estate.nodes_obj().await.stop(&node.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {node}: {e}"));
    let opened = stop.accepted().clone();
    assert_eq!(opened.build_id.to_string(), build_id, "a stop opens an attempt of the accepted Build: {opened:?}");
    assert_eq!(opened.attempt, attempt, "the stop answers the attempt it opened: {opened:?}");
    door.wait_held(cut).await;

    let mut seen = Vec::new();
    let waiting = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = stop.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("the stop's reply stream broke while attempt {attempt} was held: {e}"));
            let complete = matches!(frame, Frame::Complete);
            seen.push(frame);
            if complete {
                return;
            }
        }
    })
    .await;
    assert!(waiting.is_err(), "the stop's reply stream completed while attempt {attempt} was held: {seen:?}");
    let held = build_log::build_value(&estate.carrier().await, &build_id).await;
    assert_ne!(held["state"], "complete", "attempt {attempt} is held mid-flight: {held}");

    door.release(cut).await;
    let finished = Estate::complete(&mut stop).await;
    assert_eq!((finished.build_id.as_str(), finished.attempt), (build_id.as_str(), attempt), "{finished:?}");
    let done = build_log::build_fact(&estate.carrier().await, &build_id, &format!("attempt {attempt} complete"), Duration::from_secs(120), |b| b.attempt == attempt && b.state == rdm_admin::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    assert!(done.attempt == attempt && done.state == rdm_admin::build_state::BuildState::Complete, "{done:?}");
    estate.stop().await;
}
