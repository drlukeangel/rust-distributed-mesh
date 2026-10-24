//! Restart canary: product=mesh, feature=node-lifecycle, subfeature=node-restart, rung=multi-node
//! (MN). The provider is `MESH_SPAWN_TYPE` (process by default); the body is
//! `rdm_test::canary::restart_canary`.

use rdm_client::{CallEnd, Frame, NodeEvent, NodeSelector, NodeStatus, NodeView, Nodes};
use rdm_core::PathName;
use rdm_test::canary::restart_canary;
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::process_faults::ExactRuntime;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

/// The fabric-primary's reason, on `rdm.node_admin.node.reject.via-not-live`, for a fresh `node.restart`
/// of a stopped (parked) birth no restart is open on.
const STOPPED: &str = "a fresh restart of a stopped node is refused: node.start starts it";

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
    member_of(estate).await
}

async fn member_of(estate: Estate) -> (Estate, PathName, ExactRuntime) {
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

/// CONTRACT (node-primary-update.md steps 3-6; node-restart.md): a mesh's primary node-admin restarted
/// through `node.restart` first moves its mesh-primary role to the mesh's other admin (its issued
/// writes finished, the row, the successor installed) and runs on as a member. Restarting that admin
/// moves the role back: the row names the first admin the successor, which installs itself and is its
/// mesh's writer from that moment, and it writes the second admin's stop rows. Both restarts complete,
/// each admin the same birth, ready for traffic. Must NOT happen: the second restart's stop not
/// admitted, or the role not back on the first admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_restarted_and_made_mesh_primary_again_writes_its_mesh_rows() {
    let test = "a_mesh_primary_restarted_and_made_mesh_primary_again_writes_its_mesh_rows";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = estate.node_values().await;
    let admin = |primary: bool| before.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && (n["is_primary"] == true) == primary).cloned().unwrap_or_else(|| panic!("mesh2 holds an admin with is_primary={primary}: {before:#?}"));
    let (first, second) = (admin(true), admin(false));
    let name = |n: &Value| n["name"].as_str().unwrap_or_default().to_string();
    let nodes = estate.nodes_obj().await;
    let mut operations = Vec::new();
    for target in [&first, &second] {
        let path: PathName = name(target).parse().unwrap();
        let mut stream = nodes.restart(&path).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.restart of {path}: {e}"));
        Estate::complete(&mut stream).await;
        operations.push(format!("node.restart:{path}"));
        let after = estate.until_node(&name(target), &format!("{path} ready again after its restart"), Duration::from_secs(60), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready-for-traffic" && n["parked"] == false).then_some(n)
        }).await;
        assert_eq!((&after["node_id"], &after["incarnation_id"]), (&target["node_id"], &target["incarnation_id"]), "{path} is the same birth after its restart: {after}");
    }
    let holder = estate.node_values().await.into_iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2 holds a primary");
    assert_eq!(holder["node_id"], first["node_id"], "the second restart moved mesh2's seat back to {}", name(&first));

    estate.stop().await;
    let spans = estate.spans();
    let back: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "operation").contains(&operations[1])).collect();
    assert_eq!(back.len(), 1, "the second restart's successor, {}, installed itself once: {back:#?}", name(&first));
    let trace = back[0]["trace_id"].as_str().unwrap_or_default().to_string();
    println!("seat given back: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
