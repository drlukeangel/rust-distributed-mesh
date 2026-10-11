//! product=mesh, feature=node-lifecycle, subfeature=node-stop, rung=MN, provider=process.
//!
//! Stop parks and start rejoins (node-stop.md, node-start.md), driven through the `node` objects:
//! `node.stop` drains the exact birth, cuts its mesh connections and parks the live process; the
//! node's `left` is the reply of the stop call, never a gossip frame; the mesh primary publishes
//! `NodeStopped` from it. `node.start` makes that same process rejoin as itself.

use rafka_mesh_entity::PathName;
use rafka_node_admin_client::{Frame, NodeAdminClient, NodeEvent, NodeMeta, NodeSelector, NodeStatus, NodeView, Nodes, Peers};
use rafka_node_rpc_contract::status::StatusReply;
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);
const NODE: &str = "mesh1.rpc.1";

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-stop".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["step"]) == step)
}

/// Every frame of `stream`, in order, up to and including the terminal one.
async fn frames(mut stream: rafka_node_admin_client::WorkflowStream) -> Vec<Frame> {
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

fn alive(pid: u64) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

async fn view_of(nodes: &Nodes, node: &PathName) -> NodeView {
    nodes.get(&NodeSelector::Mesh(node.mesh.clone())).await.expect("node.get").into_iter().find(|n| &n.name == node).unwrap_or_else(|| panic!("node.get lists no {node}"))
}

/// CONTRACT: `node.stop` of a live node answers, on its reply stream, the drain, the hard cut of its
/// mesh connections and `left`, and ends `Complete`: the process is parked, alive, with the same node
/// id, incarnation, endpoint key and port; `node.get` shows it `leaving` and `parked`. The node
/// gossips nothing for the stop: the mesh primary publishes `NodeStopped`, and every other node of the
/// mesh reports, through its own held view, that it dropped the parked birth and holds the stop
/// overlay. `node.start` makes the same process rejoin through the mesh primary: the stream carries
/// `node.joined`, `node.started` and ends `Complete`, `node.get` shows the same birth
/// `ready-for-traffic`, and every other node reports it holds that birth again with the overlay gone.
/// No runtime exits, no node is created, nothing is repaired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_parks_the_process_and_a_start_rejoins_it_as_the_same_birth() {
    stop_then_start("a_stop_parks_the_process_and_a_start_rejoins_it_as_the_same_birth", NODE).await;
}

/// CONTRACT: the same holds for a node-admin: a mesh's non-primary admin is parked by its mesh primary
/// (its tasks held, its gossip topics left, its connections cut), stays alive with its endpoint bound,
/// and a start makes the same process rejoin: the same node id, incarnation, endpoint key and port,
/// `ready-for-traffic` again, and every other node reports it held again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_admin_parks_and_a_start_rejoins_it_as_the_same_birth() {
    stop_then_start("a_stopped_node_admin_parks_and_a_start_rejoins_it_as_the_same_birth", "mesh1.admin.2").await;
}

async fn stop_then_start(test: &str, node: &str) {
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), SETTLE).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;
    let peers = Peers::of(&[]).await;
    let nodes = peers.nodes(NodeAdminClient::new(estate.admin.clone())).await.expect("the fabric-primary is named");
    let path: PathName = node.parse().unwrap();

    let mesh = nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let others: Vec<&NodeView> = mesh.iter().filter(|n| n.name != path).collect();
    let pid = estate.pid_of(node).await;
    assert!(alive(pid));

    // The stop: the reply stream carries the drain, the cut and left.
    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert!(matches!(stopped.first(), Some(Frame::Started { .. })), "{stopped:?}");
    assert_eq!(events(&stopped), vec![NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    // Every other node reports that it dropped the parked birth and holds the stop overlay.
    for peer in &others {
        peers.fact(peer, "mesh1", "NodeStopped: the parked birth is dropped and its stop overlay is held", |v| v.birth(&before.node_id).is_none() && v.overlay(&before.node_id, "stop-node:").is_some()).await;
    }
    // node.get shows it parked: the same birth, the same process, alive.
    let parked = view_of(&nodes, &path).await;
    assert!(parked.parked, "node.get lists {node} parked: {parked:?}");
    assert_eq!(parked.status, NodeStatus::Leaving, "{parked:?}");
    assert_eq!((&parked.node_id, &parked.incarnation_id, &parked.endpoint_id, &parked.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "a stop keeps the node id, incarnation, endpoint key and port");
    assert!(alive(pid), "the stopped process {pid} is alive and parked");
    assert_eq!(estate.pid_of(node).await, pid);

    // The start: the same process rejoins.
    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    for peer in &others {
        peers
            .fact(peer, "mesh1", "the rejoined birth is held again as the same incarnation and the stop overlay is gone", |v| {
                v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation_id.as_ref().unwrap()) && v.overlay(&before.node_id, "stop-node:").is_none()
            })
            .await;
    }
    let after = view_of(&nodes, &path).await;
    assert!(!after.parked && after.status == NodeStatus::ReadyForTraffic, "node.get lists {node} ready again: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "a start keeps the node id, incarnation, endpoint key and port");
    assert_eq!(estate.pid_of(node).await, pid, "the same process");

    // The trace deliverable: the story's spans, each named once.
    estate.stop().await;
    let spans = estate.spans();
    let of_node = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node"] == node).collect() };
    let stop_span = of_node("rdm.node_admin.status.update.via-stop-node");
    assert_eq!(stop_span.len(), 1, "{stop_span:?}");
    assert_eq!(of_node("rdm.node_admin.node.connections.delete.via-stop-cut").len(), 1);
    assert_eq!(of_node("rdm.node_admin.node.update.via-node-stopped").len(), 1, "the mesh primary publishes NodeStopped once");
    assert_eq!(named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == "node-left" && sp["attributes"]["subject"] == node).count(), 0, "the node gossiped no node-left");
    let served = of_node("rdm.node_admin.status.update.via-start-node");
    assert_eq!((served.len(), served[0]["attributes"]["outcome"].as_str()), (1, Some("started")), "{served:?}");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["node"] == node && sp["attributes"]["command"] == "start-node").expect("the start command was sent");
    let call = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && sp["attributes"]["reused"].is_string()).expect("the start call was made");
    assert_eq!(call["attributes"]["reused"], "false", "the start dialled afresh: the stop's connection was closed");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a parked node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R13; node-replace.md Rollback 2-3; ops-naming "A rename is the same write"): the
/// caller's `node.update` of the parked `mesh1.rpc.1` to `mesh1.rpc.1.old` reaches its mesh-primary
/// directly and ends `NodeUpdated`, the stopped node told nothing; `node.get` lists the parked birth
/// at `mesh1.rpc.1.old` and nothing at `mesh1.rpc.1`. The rename back ends `NodeUpdated` the same
/// way, and `node.start` of `mesh1.rpc.1` makes the same process rejoin: `node.joined`,
/// `node.started`, `Complete`; `node.get` lists the same birth `ready-for-traffic` at `mesh1.rpc.1`
/// and nothing at `.old`; every other node holds the birth publishing the row's name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_renamed_at_its_mesh_primary_and_back_is_started_by_the_rows_name() {
    let test = "a_stopped_node_renamed_at_its_mesh_primary_and_back_is_started_by_the_rows_name";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), SETTLE).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;
    let peers = Peers::of(&[]).await;
    let nodes = peers.nodes(NodeAdminClient::new(estate.admin.clone())).await.expect("the fabric-primary is named");
    let path: PathName = NODE.parse().unwrap();
    let renamed: PathName = path.renamed();

    let mesh = nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    let others: Vec<&NodeView> = mesh.iter().filter(|n| n.name != path).collect();
    let pid = estate.pid_of(NODE).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let parked = view_of(&nodes, &path).await;
    assert!(parked.parked && parked.status == NodeStatus::Leaving, "{parked:?}");

    // The rename: one call to the node's mesh-primary, the row moved, the stopped node told nothing.
    let held = NodeMeta {
        node_id: parked.node_id.clone(),
        incarnation: parked.incarnation_id.clone(),
        endpoint: parked.endpoint_id.clone(),
        transport_addr: parked.transport_addr,
        exe: None,
        runtime: parked.deployment_id.clone(),
        status: parked.status,
        name: parked.name.clone(),
        mesh: parked.mesh.clone(),
        kind: parked.kind,
        otel_debug: false,
    };
    let body = NodeMeta { name: renamed.clone(), ..held.clone() };
    let reply = nodes.update(&held, &body, &primary.node_id, &rafka_node_rpc::RouteChoice::Direct).await.expect("node.update answered");
    assert_eq!(reply, StatusReply::NodeUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() });
    let listed = view_of(&nodes, &renamed).await;
    assert!(listed.parked && listed.node_id == before.node_id && listed.incarnation_id == before.incarnation_id, "node.get lists the parked birth at {renamed}: {listed:?}");

    assert_eq!(nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get").into_iter().filter(|n| n.name == path).count(), 0, "node.get lists nothing at {path} while the row says {renamed}");

    // The rollback (node-replace.md Rollback 2-3): the row's name goes back while the node is
    // stopped, and the start by that name rejoins the same process.
    let back = nodes.update(&NodeMeta { name: renamed.clone(), ..held.clone() }, &held, &primary.node_id, &rafka_node_rpc::RouteChoice::Direct).await.expect("node.update answered");
    assert_eq!(back, StatusReply::NodeUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() });
    let started = frames(nodes.start(&path).await.expect("the start by the row's name was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    for peer in &others {
        peers
            .fact(peer, "mesh1", "the rejoined birth is held again as the same incarnation, publishing the row's name", |v| {
                v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation_id.as_ref().unwrap() && d.node.name == path)
            })
            .await;
    }
    let after = view_of(&nodes, &path).await;
    assert!(!after.parked && after.status == NodeStatus::ReadyForTraffic, "node.get lists {path} ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "the same birth");
    assert_eq!(nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get").into_iter().filter(|n| n.name == renamed).count(), 0, "node.get lists nothing at {renamed}");
    assert_eq!(estate.pid_of(NODE).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let update = named(&spans, "rdm.node_admin.node.update.via-update-node").into_iter().find(|sp| s(&sp["attributes"]["to"]) == renamed.to_string()).expect("the mesh-primary served the node.update");
    assert_eq!((s(&update["attributes"]["outcome"]), s(&update["attributes"]["traffic"]), s(&update["attributes"]["from"])), ("node-updated".into(), "Stopped".into(), path.to_string()), "{update:#?}");
    let back = named(&spans, "rdm.node_admin.node.update.via-update-node").into_iter().find(|sp| s(&sp["attributes"]["to"]) == path.to_string()).expect("the mesh-primary served the rename back");
    assert_eq!((s(&back["attributes"]["outcome"]), s(&back["attributes"]["from"])), ("node-updated".into(), renamed.to_string()), "{back:#?}");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the rejoin was installed");
    assert_eq!(s(&join["attributes"]["admitted_as"]), path.to_string(), "{join:#?}");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && sp["attributes"]["node"] == path.to_string()).expect("the start command was sent");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a renamed parked node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: `POST /api/nodes/{name}/stop` opens the next attempt of the accepted Build (reason
/// stop, action stop fenced to the live birth, no Build minted). The operation is exactly
/// `stop-node:<path>`: stop-node to the birth, whose reply is `left`, then Complete. The runtime is
/// not terminated and no NodeDeleted is published; the accepted topology still names the path and no
/// repair attempt follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted() {
    let test = "a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted";
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    let birth_build = s(&a["build_id"]);
    estate.await_build(&birth_build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;

    let before = estate.node(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/stop"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    assert_eq!(s(&r["build_id"]), birth_build, "no Build is minted: {r}");
    let attempt_no = Estate::attempt_of(&r);
    let opened = wait_for("the stop attempt is the Build's current attempt", Duration::from_secs(30), || async {
        let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
        (b["reason"] == "stop").then_some(b)
    })
    .await;
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("stop", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;

    let attempt = attempt_no.to_string();
    let spans = wait_for("the stop operation's last step and its reconcile span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let reconciled = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt);
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconciled).then_some(spans)
    })
    .await;
    let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("stop-node:{NODE}"), "one stop operation and nothing else: {reconcile}");
    let mut last = 0u64;
    for step in ["StopNode", "AwaitNodeLeft", "Complete"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["DrainNode", "AwaitNodeDrained", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "a standalone stop runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a stop publishes no NodeDeleted");

    assert!(alive(pid), "the stopped process {pid} is parked, alive");
    tokio::time::sleep(Duration::from_secs(15)).await;
    let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    assert_eq!(b["attempt"].as_u64(), Some(attempt_no), "no repair attempt followed the stop: {b}");
    let spans = estate.spans();
    assert!(
        named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().all(|sp| s(&sp["attributes"]["build_id"]) != birth_build || s(&sp["attributes"]["scope"]).find(NODE).is_none()),
        "the drift reconciler did not treat the stop as drift"
    );
    let n = estate.node(NODE).await;
    assert_eq!(n["incarnation_id"], before["incarnation_id"], "no new birth at {NODE}: {n}");
    assert_eq!(n["parked"], true, "{n}");
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
