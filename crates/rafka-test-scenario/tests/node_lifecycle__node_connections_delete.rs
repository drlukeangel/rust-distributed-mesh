//! product=mesh, feature=node-lifecycle, subfeature=node-connections-delete, rung=MN, provider=process.
//!
//! `node.connections.delete { All }` as a Build (node-connections-delete.md):
//! `node.connections.delete:<path>` as the next attempt of the accepted Build. Its mesh-primary sets
//! the birth connections-deleting, commands the birth, holds its `node.connections.deleted` and
//! stops there: the process keeps running.

use crate::build_log;
use rafka_node_admin_client::{Frame, NodeEvent, NodeStatus};
use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-connections-delete".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["step"]) == step)
}

/// CONTRACT: `node.connections.delete { All }` opens the next attempt of the accepted Build (reason
/// delete-connections, action delete_connections targeting the live birth, no Build minted). The
/// operation is exactly `node.connections.delete:<path>`: the one call through the birth's
/// mesh-primary, then Complete. The Build completes on node.connections.deleted; the node is
/// connections-deleting (held `Draining` until the status names land) under the SAME NodeId and
/// incarnation and its process is still running afterwards. No stop, no terminate, no NodeDeleted
/// is run for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running() {
    let test = "a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let birth_build = estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await.build_id.0;
    estate.settled_views(&[("mesh1", 2, 3)]).await;

    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node_value(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let mut deleting = estate.nodes_obj().await.delete_connections(&NODE.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.connections.delete of {NODE}: {e}"));
    let r = deleting.accepted().clone();
    assert_eq!(r.build_id.0, birth_build, "no Build is minted: {r:?}");
    let attempt_no = u64::from(r.attempt);
    let opened = build_log::build_fact(&estate.carrier().await, &birth_build, "the node.connections.delete attempt is the Build's current attempt", Duration::from_secs(30), |b| serde_json::to_value(b).is_ok_and(|b| b["reason"] == "delete-connections")).await.unwrap_or_else(|end| panic!("{end}"));
    let opened = serde_json::to_value(opened).expect("a Build view serializes");
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("delete_connections", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    let frames = Estate::frames_of(&mut deleting).await;
    let events: Vec<NodeEvent> = frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect();
    assert_eq!(events, vec![NodeEvent::ConnectionsDeleted], "the reply stream carries node.connections.deleted and nothing after it: {frames:?}");
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    build_log::attempt_complete(&estate.carrier().await, &birth_build, attempt_no, SETTLE).await;

    // The birth is connections-deleting, the same birth, and its process is still alive.
    let after = estate.until_node(NODE, &format!("{NODE} is connections-deleting"), Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Draining).map(|n| serde_json::to_value(n).expect("a view serializes"))).await;
    assert_eq!(after["node_id"], before["node_id"]);
    assert_eq!(after["incarnation_id"], before["incarnation_id"]);
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists(), "the process {pid} whose connections were deleted is still running");

    // Every process flushes its spans on exit.
    let attempt = attempt_no.to_string();
    estate.stop().await;
    let spans = estate.spans();
    let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("node.connections.delete:{NODE}"), "one node.connections.delete operation and nothing else: {reconcile}");
    let mut last = 0u64;
    for step in ["DeleteConnections", "Complete"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "a standalone node.connections.delete runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a node.connections.delete deletes no node");

    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
}
