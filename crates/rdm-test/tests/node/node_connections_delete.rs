//! product=mesh, feature=node-lifecycle, subfeature=node-connections-delete, rung=MN, provider=process.
//!
//! `node.connection.delete { All }` as a Build (node-connections-delete.md):
//! `node.connection.delete:<path>` as the next attempt of the accepted Build. Its mesh-primary sets
//! the birth connections-deleting, commands the birth, holds its `node.connections.deleted` and
//! stops there: the process keeps running.

use crate::build_log;
use rdm_client::{Frame, NodeEvent, NodeStatus};
use rdm_core::status::StatusReply;
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

fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["step"]) == step)
}

/// CONTRACT: `node.connection.delete { All }` opens the next attempt of the accepted Build (reason
/// delete-connections, action delete_connections targeting the live birth, no Build minted). The
/// operation is exactly `node.connection.delete:<path>`: the one call through the birth's
/// mesh-primary, then Complete. The Build completes on node.connections.deleted; the node is
/// connections-deleted under the SAME NodeId and incarnation and its process is still running
/// afterwards. No stop, no terminate, no NodeDeleted
/// is run for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running() {
    let test = "a_standalone_connections_delete_completes_on_connections_deleted_and_leaves_the_process_running";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let birth_build = estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await.build_id.to_string();
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node_value(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let mut deleting = estate.nodes_obj().await.delete_connections(&NODE.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.connection.delete of {NODE}: {e}"));
    let r = deleting.accepted().clone();
    assert_eq!(r.build_id.to_string(), birth_build, "no Build is minted: {r:?}");
    let attempt_no = u64::from(r.attempt) + u64::from(before["is_primary"] == true);
    let opened = build_log::build_fact(&estate, &birth_build, "the node.connection.delete attempt is the Build's current attempt", Duration::from_secs(30), |b| serde_json::to_value(b).is_ok_and(|b| b["reason"] == "delete-connections")).await.unwrap_or_else(|end| panic!("{end}"));
    let opened = serde_json::to_value(opened).expect("a Build view serializes");
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("delete_connections", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "targeted to the live birth: {opened}");
    let frames = Estate::frames_of(&mut deleting).await;
    let events: Vec<NodeEvent> = frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect();
    assert_eq!(events, vec![NodeEvent::ConnectionsDeleted], "the reply stream carries node.connections.deleted and nothing after it: {frames:?}");
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    build_log::attempt_complete(&estate, &birth_build, attempt_no, SETTLE).await;

    // The birth is connections-deleted (node-connections-delete.md step 4: its mesh-primary sets it from
    // the `Established` completion; a read answers the row's status, Q233), the same birth, and its
    // process is still alive.
    let after = estate.until_node(NODE, &format!("{NODE} is connections-deleted"), Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::ConnectionsDeleted).map(|n| serde_json::to_value(n).expect("a view serializes"))).await;
    assert_eq!(after["node_id"], before["node_id"]);
    assert_eq!(after["incarnation_id"], before["incarnation_id"]);
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists(), "the process {pid} whose connections were deleted is still running");

    // Every process flushes its spans on exit.
    let attempt = attempt_no.to_string();
    estate.stop().await;
    let spans = estate.spans();
    let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("node.connection.delete:{NODE}"), "one node.connection.delete operation and nothing else: {reconcile}");
    let mut last = 0u64;
    for step in ["DeleteConnections", "Complete"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "a standalone node.connection.delete runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a node.connection.delete deletes no node");

    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
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
    let birth_build = estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await.build_id.to_string();
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node_value(NODE).await;
    let mut deleting = estate.nodes_obj().await.delete_connections(&NODE.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.connection.delete of {NODE}: {e}"));
    let frames = Estate::frames_of(&mut deleting).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "{frames:?}");
    estate.stop().await;
    let spans = estate.spans();
    // The operation's own run: its DeleteConnections step and everything in its trace.
    let step = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["node"]) == NODE && s(&sp["attributes"]["step"]) == "DeleteConnections").cloned().expect("the DeleteConnections step");
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
            (n == "rdm.node_admin.node.update.via-connections-deleting" && s(&sp["attributes"]["node"]) == NODE)
                || ((n == "rdm.mesh.topology.update.from-command-admitted" || n == "rdm.mesh.topology.update.from-status-report") && s(&sp["attributes"]["node_id"]) == node_id && s(&sp["attributes"]["status"]) == "ConnectionsDeleting")
        })
        .collect();
    // The mesh-primary's call carrying the command to the node.
    let (step_start, step_end) = (start(&step), step["end_unix_nano"].as_u64().unwrap_or(0));
    let calls: Vec<&Value> = run.iter().copied().filter(|sp| sp["name"] == "rdm.node_rpc.request.update.via-call" && sp["attributes"]["target"].as_str().is_some_and(|t| t.contains(&node_id)) && start(sp) >= step_start && start(sp) <= step_end).collect();
    let first_call = calls.iter().map(|sp| start(sp)).min().expect("the mesh-primary's call to the node");
    let early: Vec<&&Value> = records.iter().filter(|sp| start(sp) < first_call).collect();
    assert!(early.is_empty(), "nothing is recorded for {NODE} before its command leaves: {early:#?}");
    let command = calls.iter().find(|sp| s(&sp["attributes"]["op"]) == "34").unwrap_or_else(|| panic!("the command rides NodeCommand op 0x22 (34): {calls:#?}"));
    let admitted = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.connection.delete.via-connections-deleting").expect("the node reports its admission on the stream");
    let recorded = records.iter().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleting").expect("the mesh-primary records connections-deleting");
    assert!(!s(&recorded["attributes"]["invocation_id"]).is_empty(), "the record names the invocation that reported it: {recorded}");
    assert_eq!(s(&recorded["attributes"]["seq"]), "1", "the 0x22 progress position, not a state-derived duplicate decision: {recorded}");
    // The completion as the mesh-primary takes it off the same ordered stream, after the record.
    let completed = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleted" && s(&sp["attributes"]["node"]) == NODE).expect("the mesh-primary holds the node's completion");
    assert_eq!(s(&completed["attributes"]["receipt"]), "established", "{completed}");
    assert!(start(command) <= start(admitted) && start(admitted) <= start(recorded) && start(recorded) <= start(completed), "the send, the node's admission, the mesh-primary's record, then the completion: {command} {admitted} {recorded} {completed}");
    println!("node.connection.delete on its command stream: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// CONTRACT (i144 R22; Luke Q9: "The mesh-primary as its own subject accepts the caller's request and
/// runs its subject-side NodeCommand execution path, recording connections-deleting and
/// connections-deleted from its own frames as for any other birth, never refusing because the request
/// names the receiving node"): a caller's `node.connection.delete` naming mesh1's primary, sent to
/// that primary. `Connections([an id it never opened])` completes naming the id not open; `All`, last,
/// completes with its receipt. Each is carried in-process (`carried=in-process`: an endpoint does not
/// dial itself, so no op 0x22 call leaves it) through the birth's own command path
/// (`via-delete-connections` on the primary). For `All` the primary's own birth reports
/// connections-deleting, then the primary records it (`reported_by=node`), then holds the completion.
/// Must NOT happen: a refusal because the request names the receiver, a record before its own report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_named_in_its_own_connections_delete_runs_its_own_command_path_and_records_from_its_own_frames() {
    let test = "a_mesh_primary_named_in_its_own_connections_delete_runs_its_own_command_path_and_records_from_its_own_frames";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    let nodes = estate.nodes_obj().await;
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    let me = primary.name.to_string();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(primary.node_id.clone()), node_id: primary.node_id.clone(), incarnation: primary.incarnation_id.clone().expect("the primary's birth") };
    let route = rdm_transport::RouteChoice::Direct;

    let never = rdm_core::ConnectionId::mint();
    let named_one = nodes.connections_delete(&birth, &primary.node_id, &route, &rdm_core::status::ConnectionScope::Connections(vec![never.clone()])).await.expect("node.connection.delete answered");
    assert!(
        matches!(&named_one, StatusReply::ConnectionsDeleted { closed, not_open, receipt: None, .. } if closed.is_empty() && not_open == &vec![never.clone()]),
        "the primary completes its own Connections([…]), naming the id it never opened: {named_one:?}"
    );
    let all = nodes.connections_delete(&birth, &primary.node_id, &route, &rdm_core::status::ConnectionScope::All).await.expect("node.connection.delete answered");
    assert!(matches!(&all, StatusReply::ConnectionsDeleted { receipt: Some(_), .. }), "the primary completes its own All with its receipt: {all:?}");
    estate.stop().await;

    let spans = estate.spans();
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    for scope in ["connections", "all"] {
        let carried = named(&spans, "rdm.node_admin.connection.delete.via-mesh-primary")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node"]) == me && s(&sp["attributes"]["subject"]) == me && s(&sp["attributes"]["scope"]) == scope)
            .cloned()
            .unwrap_or_else(|| panic!("{me} carries its own {scope} node.connection.delete"));
        assert_eq!((s(&carried["attributes"]["carried"]).as_str(), s(&carried["attributes"]["outcome"]).as_str()), ("in-process", "connections-deleted"), "{carried}");
        let trace = s(&carried["trace_id"]);
        let run: Vec<&Value> = spans.iter().filter(|sp| s(&sp["trace_id"]) == trace).collect();
        let executed = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.connection.delete.via-delete-connections" && s(&sp["attributes"]["node"]) == me).unwrap_or_else(|| panic!("{me}'s own birth runs the {scope} command: trace {trace}"));
        assert_eq!(s(&executed["attributes"]["scope"]), scope, "{executed}");
        let dialled: Vec<&&Value> = run.iter().filter(|sp| sp["name"] == "rdm.node_rpc.request.update.via-call" && s(&sp["attributes"]["op"]) == "34").collect();
        assert!(dialled.is_empty(), "no op 0x22 call leaves {me} for its own birth: {dialled:#?}");
        if scope == "all" {
            let admitted = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.connection.delete.via-connections-deleting" && s(&sp["attributes"]["node"]) == me).expect("the primary's own birth reports its admission");
            let recorded = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleting" && s(&sp["attributes"]["node"]) == me).expect("the primary records its own connections-deleting");
            assert_eq!((s(&recorded["attributes"]["recorder"]).as_str(), s(&recorded["attributes"]["reported_by"]).as_str()), (me.as_str(), "node"), "{recorded}");
            let completed = run.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleted" && s(&sp["attributes"]["node"]) == me).expect("the primary holds its own completion");
            assert!(start(admitted) <= start(recorded) && start(recorded) <= start(completed), "its own report, then its record, then the completion: {admitted} {recorded} {completed}");
            println!("the mesh-primary's own node.connection.delete All: http://192.168.68.99:16687/trace/{trace}");
            estate.record_trace_url(&trace);
        }
    }
}
