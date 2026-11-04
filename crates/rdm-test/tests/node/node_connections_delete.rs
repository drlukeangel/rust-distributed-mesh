//! product=mesh, feature=node-lifecycle, subfeature=node-connections-delete, rung=MN, provider=process.
//!
//! `node.connection.delete { All }` as a node command (node-connections-delete.md; build.md §1, Q342):
//! the caller sends NodeCommand 0x22 `DeleteConnections` to the node's mesh-primary, which runs it,
//! never as a Build or part of one. It sets the birth connections-deleting, commands the birth, holds
//! its `node.connections.deleted` and stops there: the process keeps running.

use rdm_client::{Frame, NodeEvent, NodeStatus};
use rdm_core::status::StatusReply;
use rdm_core::{NodeId, PathName};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-connections-delete".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn step_span<'a>(spans: &'a [Value], build: &str, node: &PathName, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && sp["attributes"]["node_path"] == node.path && sp["attributes"]["node_name"] == node.name && s(&sp["attributes"]["step"]) == step)
}

/// CONTRACT: `node.connection.delete { All }` is a node command of the primary above the node, never a
/// Build or part of one (build.md §1, Q342): no Build is written. The operation is exactly
/// `node.connection.delete` keyed on the node.id: the one call through the birth's mesh-primary, then
/// Complete on node.connections.deleted; the node is connections-deleted under the SAME NodeId and
/// incarnation and its process is still running afterwards. No stop, no terminate, no NodeDeleted is
/// run for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running() {
    let test = "a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    let node = PathName::new("mesh1.node", "1").unwrap();
    let before = estate.node_value(&node).await;
    let pid = estate.pid_of(&node).await;
    let builds = estate.build_ids().await;
    let mut deleting = estate.nodes_obj().await.delete_connections(&node, &rdm_core::status::ConnectionScope::All).await.unwrap_or_else(|e| panic!("the mesh-primary refused node.connection.delete of {node:?}: {e}"));
    let frames = Estate::frames_of(&mut deleting).await;
    assert!(!frames.iter().any(|f| matches!(f, Frame::Started { .. })), "a node command opens no Build: {frames:?}");
    let events: Vec<NodeEvent> = frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect();
    assert_eq!(events, vec![NodeEvent::ConnectionsDeleted], "the reply stream carries node.connections.deleted and nothing after it: {frames:?}");
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    assert_eq!(estate.build_ids().await, builds, "a connections delete writes no Build");

    // The birth is connections-deleted (node-connections-delete.md step 4: its mesh-primary sets it from
    // the `Established` completion; a read answers the row's status, Q233), the same birth, and its
    // process is still alive.
    let after = estate.until_node(&node, &format!("{node:?} is connections-deleted"), Duration::from_secs(30), |n| n.filter(|n| n.state == NodeStatus::ConnectionsDeleted).map(|n| serde_json::to_value(n).expect("a view serializes"))).await;
    assert_eq!(after["node_id"], before["node_id"]);
    assert_eq!(after["incarnation_id"], before["incarnation_id"]);
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists(), "the process {pid} whose connections were deleted is still running");

    // Every process flushes its spans on exit.
    let birth_build = rdm_core::LifecycleOp::NO_BUILD;
    estate.stop().await;
    let spans = estate.spans();
    let mut last = 0u64;
    for step in ["DeleteConnections", "Complete"] {
        let sp = step_span(&spans, birth_build, &node, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, birth_build, &node, step).is_none(), "a standalone node.connection.delete runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a node.connection.delete deletes no node");

    let command = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().find(|sp| sp["attributes"]["subject_id"] == before["node_id"]).expect("the node command's span at the primary above the node");
    assert_eq!(s(&command["attributes"]["outcome"]), "connections-deleted", "{command}");
    estate.record_trace_url(command["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i144 R22; Luke Q6, Q7: "The node reports admission and completion on the same command's
/// ordered reply stream", on op 0x22 NodeCommand; "Remove both pre-send writes"): a standalone
/// `node.connection.delete { All }` of a member. The mesh-primary records nothing for it before its
/// command leaves: no connections-deleting record precedes its call to the node. The call is the node
/// command op 0x22 (34). The node accepts it, establishes its block on new application work and
/// reports `ConnectionsDeleting` on the call's stream; only then does the mesh-primary record the
/// birth connections-deleting; the node's completion comes after. Must NOT happen: a record before
/// the send, the command on Status 0x1B, or the record before the node's report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connections_delete_is_admitted_by_the_node_on_its_command_stream_and_nothing_is_recorded_before_the_send() {
    let test = "a_connections_delete_is_admitted_by_the_node_on_its_command_stream_and_nothing_is_recorded_before_the_send";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    let node = PathName::new("mesh1.node", "1").unwrap();
    let before = estate.node_value(&node).await;
    let mut deleting = estate.nodes_obj().await.delete_connections(&node, &rdm_core::status::ConnectionScope::All).await.unwrap_or_else(|e| panic!("the mesh-primary refused node.connection.delete of {node:?}: {e}"));
    let frames = Estate::frames_of(&mut deleting).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    // A node command belongs to no Build: its steps run under no build_id.
    let birth_build = rdm_core::LifecycleOp::NO_BUILD;
    estate.stop().await;
    let spans = estate.spans();
    // The operation's own run: its DeleteConnections step and everything in its trace.
    let step = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && sp["attributes"]["node_path"] == node.path && sp["attributes"]["node_name"] == node.name && s(&sp["attributes"]["step"]) == "DeleteConnections").cloned().expect("the DeleteConnections step");
    let trace = s(&step["trace_id"]);
    let run: Vec<&Value> = spans.iter().filter(|sp| s(&sp["trace_id"]) == trace).collect();
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let node_id = s(&before["node_id"]);
    // The mesh-primary's records of connections-deleting for this birth, in the run.
    let records: Vec<&Value> = run
        .iter()
        .copied()
        .filter(|sp| start(sp) >= start(&step))
        .filter(|sp| {
            let n = sp["name"].as_str().unwrap_or_default();
            (n == "rdm.node_admin.node.update.via-connections-deleting" && sp["attributes"]["node_id"] == before["node_id"])
                || ((n == "rdm.mesh.topology.update.from-command-admitted" || n == "rdm.mesh.topology.update.from-status-report") && s(&sp["attributes"]["node_id"]) == node_id && s(&sp["attributes"]["status"]) == "ConnectionsDeleting")
        })
        .collect();
    // The mesh-primary's call carrying the command to the node.
    let (step_start, step_end) = (start(&step), step["end_unix_nano"].as_u64().unwrap_or(0));
    let calls: Vec<&Value> = run.iter().copied().filter(|sp| sp["name"] == "rdm.node_rpc.request.update.via-call" && sp["attributes"]["target"] == format!("{:?}", rdm_transport::NodeTarget::ExactNode(serde_json::from_value::<NodeId>(before["node_id"].clone()).unwrap())) && start(sp) >= step_start && start(sp) <= step_end).collect();
    let first_call = calls.iter().map(|sp| start(sp)).min().expect("the mesh-primary's call to the node");
    let early: Vec<&&Value> = records.iter().filter(|sp| start(sp) < first_call).collect();
    assert!(early.is_empty(), "nothing is recorded for {node:?} before its command leaves: {early:#?}");
    let command = calls.iter().find(|sp| s(&sp["attributes"]["op"]) == "34").unwrap_or_else(|| panic!("the command rides NodeCommand op 0x22 (34): {calls:#?}"));
    let admitted = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.connection.delete.via-connections-deleting").expect("the node reports its admission on the stream");
    let recorded = records.iter().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleting").expect("the mesh-primary records connections-deleting");
    assert!(!s(&recorded["attributes"]["invocation_id"]).is_empty(), "the record names the invocation that reported it: {recorded}");
    assert_eq!(s(&recorded["attributes"]["seq"]), "1", "the 0x22 progress position, not a state-derived duplicate decision: {recorded}");
    // The completion as the mesh-primary takes it off the same ordered stream, after the record.
    let completed = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleted" && sp["attributes"]["node_id"] == before["node_id"]).expect("the mesh-primary holds the node's completion");
    assert_eq!(s(&completed["attributes"]["receipt"]), "established", "{completed}");
    assert!(start(command) <= start(admitted) && start(admitted) <= start(recorded) && start(recorded) <= start(completed), "the send, the node's admission, the mesh-primary's record, then the completion: {command} {admitted} {recorded} {completed}");
    println!("node.connection.delete on its command stream: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// CONTRACT (i144 R22; Luke Q9: "The mesh-primary as its own subject accepts the caller's request and
/// runs its subject-side NodeCommand execution path, recording connections-deleting and
/// connections-deleted from its own frames as for any other birth, never refusing because the request
/// names the receiving node"): a caller's `node.connection.delete` naming mesh1's primary, sent to
/// that primary. `Connections([an id it never opened])` completes naming the id not open, carried
/// in-process (`carried=in-process`: an endpoint does not dial itself, so no op 0x22 call leaves it)
/// through the birth's own command path (`via-delete-connections` on the primary). `All`, last, first
/// moves the primary's role (node-connections-delete.md step 0, D40): the new mesh-primary carries it to
/// the old primary's birth (`carried=dialled`), which reports connections-deleting, then the new
/// primary records it (`reported_by=node`), then holds the completion with its receipt.
/// Must NOT happen: a refusal because the request names the receiver, a record before its own report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_named_in_its_own_connections_delete_runs_its_own_command_path_and_records_from_its_own_frames() {
    let test = "a_mesh_primary_named_in_its_own_connections_delete_runs_its_own_command_path_and_records_from_its_own_frames";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    let nodes = estate.nodes_obj().await;
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let primary = mesh.iter().find(|n| n.r#type == "node-admin" && n.is_primary).cloned().expect("node.get names mesh1's primary");
    let successor = mesh.iter().find(|n| rdm_core::is_node_admin(&n.r#type) && !n.is_primary).cloned().expect("mesh1's other node-admin");
    let me = primary.node_id.to_string();
    // The one connections-delete call, streamed: its completion is the `NodeCommandService`'s `ConnectionsDeleted`.
    let completed = |scope: rdm_core::status::ConnectionScope| {
        let (nodes, name) = (&nodes, primary.location());
        async move {
            let mut stream = nodes.delete_connections(&name, &scope).await.unwrap_or_else(|e| panic!("node.connection.delete of {name:?} refused: {e}"));
            let frames = Estate::frames_of(&mut stream).await;
            assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
            stream.completion().cloned().expect("the `NodeCommandService`'s terminal frame")
        }
    };

    let never = rdm_core::ConnectionId::mint();
    let named_one = completed(rdm_core::status::ConnectionScope::Connections(vec![never.clone()])).await;
    assert!(
        matches!(&named_one, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { closed, not_open, receipt: None, .. } if closed.is_empty() && not_open == &vec![never.clone()]),
        "the primary completes its own Connections([…]), naming the id it never opened: {named_one:?}"
    );
    let all = completed(rdm_core::status::ConnectionScope::All).await;
    assert!(matches!(&all, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { receipt: Some(_), .. }), "the primary completes its own All with its receipt: {all:?}");
    estate.stop().await;

    let spans = estate.spans();
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    for (scope, by, how) in [("connections", &primary, "in-process"), ("all", &successor, "dialled")] {
        let carried = named(&spans, "rdm.node_admin.connection.delete.via-mesh-primary")
            .into_iter()
            .find(|sp| sp["attributes"]["node_path"] == by.path && sp["attributes"]["node_name"] == by.name && s(&sp["attributes"]["subject_id"]) == me && s(&sp["attributes"]["scope"]) == scope)
            .cloned()
            .unwrap_or_else(|| panic!("{:?} carries {me}'s {scope} node.connection.delete", by.name));
        assert_eq!((s(&carried["attributes"]["carried"]).as_str(), s(&carried["attributes"]["outcome"]).as_str()), (how, "connections-deleted"), "{carried}");
        let trace = s(&carried["trace_id"]);
        let run: Vec<&Value> = spans.iter().filter(|sp| s(&sp["trace_id"]) == trace).collect();
        let executed = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.connection.delete.via-delete-connections" && s(&sp["attributes"]["node"]) == me).unwrap_or_else(|| panic!("{me}'s own birth runs the {scope} command: trace {trace}"));
        assert_eq!(s(&executed["attributes"]["scope"]), scope, "{executed}");
        let dialled: Vec<&&Value> = run.iter().filter(|sp| sp["name"] == "rdm.node_rpc.request.update.via-call" && s(&sp["attributes"]["op"]) == "34").collect();
        if scope == "connections" {
            assert!(dialled.is_empty(), "no op 0x22 call leaves {me} for its own birth: {dialled:#?}");
        }
        if scope == "all" {
            let admitted = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.connection.delete.via-connections-deleting" && s(&sp["attributes"]["node"]) == me).expect("the primary's own birth reports its admission");
            let recorded = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleting" && s(&sp["attributes"]["node_id"]) == me).expect("the primary records its own connections-deleting");
            assert_eq!((s(&recorded["attributes"]["recorder_node_id"]), s(&recorded["attributes"]["reported_by"])), (successor.node_id.to_string(), "node".to_string()), "the new mesh-primary records it: {recorded}");
            let completed = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleted" && s(&sp["attributes"]["node_id"]) == me).expect("the new mesh-primary holds the completion");
            assert!(start(admitted) <= start(recorded) && start(recorded) <= start(completed), "its own report, then its record, then the completion: {admitted} {recorded} {completed}");
            println!("the old mesh-primary's node.connection.delete All, after its role moved: http://192.168.68.99:16687/trace/{trace}");
            estate.record_trace_url(&trace);
        }
    }
}
