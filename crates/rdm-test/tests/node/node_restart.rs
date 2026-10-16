//! Restart canary: product=mesh, feature=node-lifecycle, subfeature=node-restart, rung=multi-node
//! (MN). The provider is `MESH_SPAWN_TYPE` (process by default); the body is
//! `rdm_test::canary::restart_canary`.

use rdm_client::{CallEnd, Frame, NodeEvent, NodeSelector, NodeStatus, NodeView, Nodes, WorkflowKind};
use rdm_core::PathName;
use rdm_test::canary::restart_canary;
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::Value;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The fabric-primary's reason, on `rdm.node_admin.node.reject.via-not-live`, for a fresh `node.restart`
/// of a stopped (parked) birth no restart is open on.
const STOPPED: &str = "a fresh restart of a stopped node is refused: node.start starts it";
/// Its reason for a fresh `node.restart` of a birth whose own restart is open (an attempt of it ended
/// before its start completed): the caller resubmits that restart's Build.
const OPEN_RESTART: &str = "a restart of this birth is open: the caller resubmits its Build, which resumes it from its receipts";

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-restart".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Whether span `sp` names Build `bare` (its bare id): step spans carry the `bid_xxxx-xxxx-xxxx`
/// rendering (`BuildId::log`), command spans the bare id.
fn same_build(sp: &Value, bare: &str) -> bool {
    let a = attr(sp, "build_id");
    a == bare || a.strip_prefix("bid_").is_some_and(|l| l.replace('-', "") == bare)
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Every frame of `stream`, in order, up to and including the terminal one.
async fn frames(mut stream: rdm_client::WorkflowStream) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.unwrap_or_else(|e| panic!("the reply stream broke: {e}"));
        let terminal = f.is_terminal();
        out.push(f);
        if terminal {
            break;
        }
    }
    out
}

fn events(frames: &[Frame]) -> Vec<NodeEvent> {
    frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect()
}

async fn view_of(nodes: &Nodes, node: &PathName) -> NodeView {
    nodes.get(&NodeSelector::Mesh(node.mesh.clone())).await.expect("node.get").into_iter().find(|n| &n.name == node).unwrap_or_else(|| panic!("node.get lists no {node}"))
}

/// mesh1 {1 node-admin, 2 rpc nodes}, settled and ready for traffic; the rpc member that holds no
/// seat (its stop or restart moves no seat first) and its exact runtime.
async fn mesh_with_a_member(test: &str) -> (Estate, PathName, ExactRuntime) {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let member = estate.node_values().await.into_iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && n["is_primary"] != true).map(|n| n["name"].as_str().unwrap_or_default().to_string()).min().expect("an rpc member that holds no seat");
    let dir = estate.data_dir(&member).await;
    let rt = ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{member}: no exact runtime published in {dir}: {r:?}"));
    (estate, member.parse().unwrap(), rt)
}

/// The `via-not-live` refusals of `node`.
fn not_live<'a>(spans: &'a [Value], node: &PathName) -> Vec<&'a Value> {
    named(spans, "rdm.node_admin.node.reject.via-not-live").into_iter().filter(|sp| attr(sp, "node") == node.to_string()).collect()
}

/// CONTRACT: an RPC node restarted through Build is stop then start on the parked process: the same
/// node id, incarnation, endpoint key and port, the same process; it still serves the value written
/// before the restart from its own data dir, resets an unfinished request with 499 (NotSent), and
/// leaves a Build -> node operation -> start step -> start span chain linked by ParentSpanId.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state() {
    restart_canary(Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "node-restart".into(),
        rung: "multi-node".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "rpc_node_restarts_as_the_same_birth_in_the_same_process_and_recovers_state".into(),
    })
    .await;
}

/// CONTRACT (R-PV1, D3 the fresh-request versus resumption rule; PRD i144 R12: "a new `node.restart`
/// against a stopped node is refused by name"): a member stopped by `node.stop` (parked, `leaving`,
/// its process alive) is not restarted by a fresh `node.restart`: the fabric-primary refuses it
/// `node-not-live` before any attempt is opened, its `via-not-live` span naming the member, its status
/// `leaving` and why (`node.start` starts a stopped node). `node.start` of the same member is then
/// served: `node.joined`, `node.started`, `Complete`, the same birth `ready-for-traffic`. Must NOT
/// happen: an attempt opened for the refused restart (any `node.restart:<member>` step), or the stopped
/// birth replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_restart_of_a_stopped_node_is_refused_by_name_and_node_start_is_served() {
    let test = "a_fresh_restart_of_a_stopped_node_is_refused_by_name_and_node_start_is_served";
    let (mut estate, member, _rt) = mesh_with_a_member(test).await;
    let nodes = estate.nodes_obj().await;
    let before = view_of(&nodes, &member).await;
    let stopped = frames(nodes.stop(&member).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let parked = view_of(&nodes, &member).await;
    assert!(parked.parked && parked.status == NodeStatus::Leaving, "node.get lists {member} parked: {parked:?}");

    match nodes.restart(&member).await {
        Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "node-not-live" && detail.contains(&member.to_string()), "the fresh restart is refused by name: {reason}: {detail}"),
        Err(other) => panic!("the fresh restart of the stopped {member} ended {other}, not node-not-live"),
        Ok(stream) => panic!("the fresh restart of the stopped {member} was accepted as attempt {} of {}", stream.accepted().attempt, stream.accepted().build_id),
    }
    let started = frames(nodes.start(&member).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    let back = view_of(&nodes, &member).await;
    assert_eq!(back.status, NodeStatus::ReadyForTraffic, "{back:?}");
    assert_eq!((&back.node_id, &back.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth starts again");

    estate.stop().await;
    let spans = estate.spans();
    let refused = not_live(&spans, &member);
    assert_eq!(refused.len(), 1, "one refusal: {refused:#?}");
    assert_eq!((attr(refused[0], "status"), attr(refused[0], "reason")), ("leaving".to_string(), STOPPED.to_string()), "{}", refused[0]);
    let restarts: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().filter(|sp| attr(sp, "pipeline") == "restart" && attr(sp, "node") == member.to_string()).collect();
    assert!(restarts.is_empty(), "no attempt ran a restart of {member}: {restarts:#?}");
    let trace = refused[0]["trace_id"].as_str().unwrap_or_default().to_string();
    println!("fresh restart refused: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// CONTRACT (R-PV1: "when the restart's stop was never admitted, the uncompleted stop-then-start
/// sequence runs again through the caller's resubmit or the lane's next attempt"; node-rpc-envelope.md
/// build.create: "existing build_id for resubmission"; PRD i144 R11, R12): a member the operating system
/// holds suspended is restarted; the restart's stop-node is never admitted and the attempt ends
/// `Failed` naming it. The member is continued. Nothing reopens the failed attempt on its own: two
/// staleness floors later the Build's attempt is the failed one, no step ran for the member, and
/// `node.get` lists the same birth `restarting` (its restart is open: commanded silence its executor
/// owns). A fresh `node.restart` of it is refused `node-not-live` naming the open restart; the caller's
/// resubmit of the same Build opens the next attempt, which runs the whole stop then the whole start:
/// the stop-node admitted (`stopped`), `node.joined`, `node.started`, `Complete`, the same birth
/// `ready-for-traffic` on the same process. The node runs its stop once (Q2: a completed stop step never
/// runs again; a parked node answers a later stop-node with the receipt of the stop it ran). Must NOT
/// happen: a step for the member between the release and the resubmit, a `node.start` sent without the
/// resumed stop, a second stop run by the node, or a new birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_whose_stop_was_never_admitted_resumes_stop_then_start_on_resubmission() {
    let test = "a_restart_whose_stop_was_never_admitted_resumes_stop_then_start_on_resubmission";
    let (mut estate, member, rt) = mesh_with_a_member(test).await;
    let nodes = estate.nodes_obj().await;
    let before = view_of(&nodes, &member).await;
    let held = rt.apply(Fault::Stop).expect("the exact runtime is suspended");
    assert_eq!(held.state_after, Some('T'), "the OS holds the runtime: {held:?}");
    let stream = nodes.restart(&member).await.expect("the restart was accepted");
    let accepted = stream.accepted().clone();
    let failed = frames(stream).await;
    assert!(matches!(failed.last(), Some(Frame::Failed { reason, .. }) if reason.contains("stop-node was not admitted")), "the restart fails naming the unadmitted stop: {failed:?}");
    rt.apply(Fault::Continue).expect("the runtime is continued");
    let continued_at = now_ns();

    // Nothing reopens the failed attempt on its own.
    tokio::time::sleep(rdm_transport::membership::staleness_floor() * 2).await;
    let build = estate.builds_obj().await.get(&accepted.build_id).await.unwrap_or_else(|e| panic!("build.get of {}: {e}", accepted.build_id));
    assert_eq!(build.attempt, accepted.attempt, "no attempt after the failed one: {build:?}");
    let open = view_of(&nodes, &member).await;
    assert_eq!((open.status, &open.node_id, &open.incarnation_id), (NodeStatus::Restarting, &before.node_id, &before.incarnation_id), "the same birth, its restart open: {open:?}");
    match nodes.restart(&member).await {
        Err(CallEnd::Rejected { reason, .. }) => assert_eq!(reason, "node-not-live", "a fresh restart of a birth whose restart is open is refused by name"),
        Err(other) => panic!("the fresh restart ended {other}, not node-not-live"),
        Ok(stream) => panic!("a fresh restart of {member} was accepted as attempt {} while its restart is open", stream.accepted().attempt),
    }
    let resubmit_at = now_ns();

    // The caller's resubmit: the next attempt runs the whole stop, then the whole start.
    let resumed = frames(nodes.resume(WorkflowKind::Restart(member.clone()), &accepted).await.expect("the resubmit was accepted")).await;
    assert!(matches!(resumed.first(), Some(Frame::Started { build_id, .. }) if *build_id == accepted.build_id), "the resubmit drives the same Build: {resumed:?}");
    assert_eq!(events(&resumed), vec![NodeEvent::Restarting, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Joined, NodeEvent::Started, NodeEvent::Restarted], "the whole stop, then the whole start: {resumed:?}");
    assert_eq!(resumed.last(), Some(&Frame::Complete), "{resumed:?}");
    let build_after = estate.builds_obj().await.get(&accepted.build_id).await.unwrap_or_else(|e| panic!("build.get of {}: {e}", accepted.build_id));
    assert!(build_after.attempt > accepted.attempt, "the resubmit ran the next attempt: {build_after:?}");
    let back = view_of(&nodes, &member).await;
    assert_eq!((back.status, &back.node_id, &back.incarnation_id), (NodeStatus::ReadyForTraffic, &before.node_id, &before.incarnation_id), "the same birth runs again: {back:?}");
    rt.check().expect("the same process runs on");

    estate.stop().await;
    let spans = estate.spans();
    let build = accepted.build_id.to_string();
    let steps = |pick: &dyn Fn(u64) -> bool| -> Vec<(String, String, String)> {
        named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| same_build(sp, &build) && attr(sp, "node") == member.to_string() && pick(start_ns(sp))).map(|sp| (attr(sp, "attempt"), attr(sp, "step"), attr(sp, "outcome"))).collect()
    };
    let between = steps(&|t| t >= continued_at && t < resubmit_at);
    assert!(between.is_empty(), "no step ran for {member} between the release and the resubmit: {between:?}");
    let after: Vec<String> = steps(&|t| t >= resubmit_at).into_iter().map(|(_, step, outcome)| format!("{step}={outcome}")).collect();
    assert_eq!(after, ["NodeRestarting=complete", "StopNode=complete", "AwaitNodeLeft=complete", "StartNode=complete", "WaitForNodeReady=complete", "Complete=complete"].map(String::from).to_vec(), "the resumed attempt runs the whole restart");
    let sent: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().filter(|sp| same_build(sp, &build) && attr(sp, "node") == member.to_string()).map(|sp| format!("{}={}", attr(sp, "command"), attr(sp, "admission"))).collect();
    assert_eq!(sent.first().map(|c| c.ends_with("=stopped")), Some(false), "the first stop-node was never admitted: {sent:?}");
    // Q2 (Luke 2026-10-16): no completed stop step runs again. The node ran its stop once, in the
    // resubmitted attempt: the first attempt's stop never reached it.
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-stop-node").into_iter().filter(|sp| same_build(sp, &build) && attr(sp, "node") == member.to_string()).collect();
    assert_eq!(ran.len(), 1, "the node ran one stop for this Build: {ran:#?}");
    assert!(start_ns(ran[0]) >= resubmit_at, "the one stop the node ran is the resubmitted attempt's: {}", ran[0]);
    let refused = not_live(&spans, &member);
    assert_eq!((refused.len(), refused.first().map(|sp| (attr(sp, "status"), attr(sp, "reason")))), (1, Some(("restarting".to_string(), OPEN_RESTART.to_string()))), "{refused:#?}");
    let trace = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| same_build(sp, &build) && attr(sp, "node") == member.to_string() && attr(sp, "step") == "StartNode").map(|sp| sp["trace_id"].as_str().unwrap_or_default().to_string()).unwrap_or_default();
    println!("resumed restart: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
