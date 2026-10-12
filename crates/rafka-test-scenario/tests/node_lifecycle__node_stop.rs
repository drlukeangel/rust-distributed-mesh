//! product=mesh, feature=node-lifecycle, subfeature=node-stop, rung=MN, provider=process.
//!
//! Stop parks and start rejoins (node-stop.md, node-start.md), driven through the `node` objects:
//! `node.stop` drains the exact birth, cuts its mesh connections and parks the live process; the
//! node's `left` is the reply of the stop call, never a gossip frame; the mesh primary publishes
//! `NodeStopped` from it. `node.start` makes that same process rejoin as itself.

use rafka_mesh_entity::PathName;
use crate::build_log;
use rafka_node_admin_client::{Frame, NodeAdminClient, NodeEvent, NodeMeta, NodeSelector, NodeStatus, NodeView, Nodes, Peers};
use rafka_node_rpc_contract::status::StatusReply;
use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
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
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = node.parse().unwrap();

    let mesh = nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let others: Vec<&NodeView> = mesh.iter().filter(|n| n.name != path).collect();
    let pid = estate.pid_of(node).await;
    assert!(alive(pid));

    // The stop: the reply stream carries its node.connections.deleted and stopped.
    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert!(matches!(stopped.first(), Some(Frame::Started { .. })), "{stopped:?}");
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
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
    assert_eq!(of_node("rdm.node_admin.node.connections.delete.via-stop").len(), 1, "the stop runs its node.connections.delete {{ All }} once");
    assert_eq!(of_node("rdm.node_admin.node.update.via-stop-transport-cut").len(), 1, "then cuts its transport once");
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
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
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
        tags: parked.tags.clone(),
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
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && sp["attributes"]["node"] == path.to_string()).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the start's rejoin was installed");
    assert_eq!(s(&join["attributes"]["admitted_as"]), path.to_string(), "{join:#?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a renamed parked node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-JN1; node-start.md step 2, node-create.md step 4; R12/R13; CLAUDE.md read rule): in a
/// mesh of three node-admins, the non-primary `mesh1.admin.3` is stopped and its mesh-primary renames
/// its row to `mesh1.admin.3.old` (`node.update`, the stopped node told nothing). Every admin lists
/// the parked birth at the row's name, `mesh1.admin.2`, which performed no rename, included: its view
/// reads the name from the row on the shared store root. `node.start` of `mesh1.admin.3.old` makes the
/// same process rejoin: its join presents `mesh1.admin.3`, the name its authority last accepted for
/// this exact birth, and is admitted under the row's name (`via-join` `admitted_as`); the answer hands
/// that name, and the node installs it before it publishes (`via-join-admitted` `installed`, then
/// `via-ready` under the row's name). The stream carries `node.joined`, `node.started` and ends
/// `Complete`; every admin lists the same birth `ready-for-traffic` at `mesh1.admin.3.old`, and the rpc
/// node holds it publishing that name. What must NOT happen: an admin listing the parked birth at
/// `mesh1.admin.3`, the start refused (`UnknownNode`, `JoinMismatch("name")`), or a span of the
/// started node after its join naming `mesh1.admin.3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admin_renamed_while_stopped_starts_under_its_rows_name_on_every_admin() {
    let test = "a_node_admin_renamed_while_stopped_starts_under_its_rows_name_on_every_admin";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 3, 1)]).await;
    estate.settled_views(&[("mesh1", 3, 1)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = "mesh1.admin.3".parse().unwrap();
    let renamed: PathName = path.renamed();

    let mesh = nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    assert_ne!(primary.name, path, "the stopped admin is not mesh1's primary");
    let admins: Vec<NodeView> = mesh.iter().filter(|n| n.name != path && n.kind == rafka_mesh_entity::NodeKind::NodeAdmin).cloned().collect();
    assert!(admins.iter().any(|n| n.name != primary.name), "an admin that performs no rename: {admins:?}");
    let rpc = mesh.iter().find(|n| n.kind == rafka_mesh_entity::NodeKind::RpcNode).cloned().expect("the rpc node");
    let pid = estate.pid_of(path.to_string().as_str()).await;

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
        tags: parked.tags.clone(),
    };
    let reply = nodes.update(&held, &NodeMeta { name: renamed.clone(), ..held.clone() }, &primary.node_id, &rafka_node_rpc::RouteChoice::Direct).await.expect("node.update answered");
    assert_eq!(reply, StatusReply::NodeUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() });
    // Every admin's view names the parked birth by its row, the one that performed no rename included.
    for admin in &admins {
        let listed = estate
            .until_view_at(&admin.name, &format!("{} lists the parked birth at {renamed}", admin.name), Duration::from_secs(10), |view| {
                let at = |p: &PathName| view.iter().find(|n| &n.name == p).cloned();
                at(&renamed).filter(|n| n.node_id == before.node_id && at(&path).is_none())
            })
            .await;
        assert_eq!((listed.parked, listed.incarnation_id.clone()), (true, before.incarnation_id.clone()), "{}: {listed:?}", admin.name);
    }

    // The start by the row's name: the same process rejoins under it.
    let started = frames(nodes.start(&renamed).await.expect("the start by the row's name was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    peers
        .fact(&rpc, "mesh1", "the rejoined birth is held as the same incarnation, publishing the row's name", |v| {
            v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation_id.as_ref().unwrap() && d.node.name == renamed)
        })
        .await;
    for admin in &admins {
        estate
            .until_view_at(&admin.name, &format!("{} lists the rejoined birth ready at {renamed}", admin.name), Duration::from_secs(10), |view| {
                view.iter().find(|n| n.name == renamed && n.node_id == before.node_id && n.status == NodeStatus::ReadyForTraffic && !n.parked).map(|_| ())
            })
            .await;
    }
    assert_eq!(estate.pid_of(renamed.to_string().as_str()).await, pid, "the same process, listed at the row's name");

    estate.stop().await;
    let spans = estate.spans();
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && s(&sp["attributes"]["node"]) == renamed.to_string()).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed" && sp["trace_id"] == sent["trace_id"]).expect("the start's rejoin was installed");
    assert_eq!((s(&join["attributes"]["node"]), s(&join["attributes"]["admitted_as"])), (path.to_string(), renamed.to_string()), "the join presents the last accepted name and is admitted under the row's: {join:#?}");
    let admitted = named(&spans, "rdm.mesh.node.update.via-join-admitted").into_iter().find(|sp| s(&sp["attributes"]["installed"]) == renamed.to_string()).expect("the started node installed the row's name its join answer handed");
    let ready: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == renamed.to_string()).collect();
    assert_eq!(ready.len(), 1, "the started node reports ready under the row's name: {ready:?}");
    assert!(ready[0]["start_unix_nano"].as_u64() >= admitted["end_unix_nano"].as_u64(), "the name is installed before the node says anything: {admitted:#?} {ready:#?}");
    assert!(named(&spans, "rdm.node_admin.node.reject.via-join-mismatch").is_empty(), "no join was refused");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a renamed parked node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: `node.stop` opens the next attempt of the accepted Build (reason
/// stop, action stop fenced to the live birth, no Build minted). The operation is exactly
/// `stop-node:<path>`: stop-node to the birth, whose reply is `left`, then Complete. The runtime is
/// not terminated and no NodeDeleted is published; the accepted topology still names the path and no
/// repair attempt follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted() {
    let test = "a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let birth_build = estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await.build_id.to_string();
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    let before = estate.node_value(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let mut stop = estate.nodes_obj().await.stop(&NODE.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {NODE}: {e}"));
    let r = stop.accepted().clone();
    assert_eq!(r.build_id.to_string(), birth_build, "no Build is minted: {r:?}");
    let attempt_no = u64::from(r.attempt);
    let opened = build_log::build_fact(&estate.carrier().await, &birth_build, "the stop attempt is the Build's current attempt", Duration::from_secs(30), |b| serde_json::to_value(b).is_ok_and(|b| b["reason"] == "stop")).await.unwrap_or_else(|end| panic!("{end}"));
    let opened = serde_json::to_value(opened).expect("a Build view serializes");
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("stop", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    Estate::complete(&mut stop).await;
    build_log::attempt_complete(&estate.carrier().await, &birth_build, attempt_no, SETTLE).await;
    assert!(alive(pid), "the stopped process {pid} is parked, alive");
    // No repair attempt follows the stop: the Build's attempt is the stop's after the reconciler's rounds have passed.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let b = build_log::build_value(&estate.carrier().await, &birth_build).await;
    assert_eq!(b["attempt"].as_u64(), Some(attempt_no), "no repair attempt followed the stop: {b}");
    let n = estate.node_value(NODE).await;
    assert_eq!(n["incarnation_id"], before["incarnation_id"], "no new birth at {NODE}: {n}");
    assert_eq!(n["parked"], true, "{n}");

    // Every process flushes its spans on exit.
    let attempt = attempt_no.to_string();
    let shutdown_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.stop().await;
    let spans = estate.spans();
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
    for step in ["DeleteConnections", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "a standalone stop runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a stop publishes no NodeDeleted");

    assert!(
        named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().all(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) >= shutdown_at || s(&sp["attributes"]["build_id"]) != birth_build || s(&sp["attributes"]["scope"]).find(NODE).is_none()),
        "the drift reconciler did not treat the stop as drift"
    );
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (fabric-node-lifecycle.md §6.9, R-GC1; node-stop.md step 6): on a fabric whose
/// `RDM_STOPPED_NODE_TTL` is 3 s, `mesh1.rpc.1` is stopped and left alone. The fabric-primary's sweep
/// ages it from its stop receipt and submits one ordinary node.delete Build
/// (`rdm.node_admin.node.delete.via-stopped-ttl` with the node, the receipt's time, the ttl and the
/// Build); the delete runs as a delete does: the provider's exit proof of the parked process
/// (TerminateRuntime complete), NodeDeleted, and `node.get` lists no `mesh1.rpc.1`. The other stopped
/// period never started: nothing else is deleted. Must NOT happen: a delete before one ttl, a second
/// delete Build, a collection the sweep made itself without a Build, or a seat transfer, drain or stop
/// sent again to the stopped birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_past_the_ttl_is_deleted_by_the_fabric_primarys_sweep() {
    let test = "a_node_stopped_past_the_ttl_is_deleted_by_the_fabric_primarys_sweep";
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[("RDM_STOPPED_NODE_TTL", "3s")]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = NODE.parse().unwrap();
    peers.learn(&nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get"));
    let pid = estate.pid_of(NODE).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let stopped_at = std::time::Instant::now();
    // The sweep collects it: the delete's exit proof ends the parked process and node.get lists it no more.
    estate.until_node(NODE, "the stopped node is deleted and its process exited", Duration::from_secs(60), |n| (n.is_none() && !alive(pid)).then_some(())).await;
    assert!(stopped_at.elapsed() >= Duration::from_secs(3), "collected after one ttl, never before: {:?}", stopped_at.elapsed());

    estate.stop().await;
    let spans = estate.spans();
    let collected: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-stopped-ttl").into_iter().filter(|sp| sp["attributes"]["build_id"].is_string()).collect();
    assert_eq!(collected.len(), 1, "one node.delete Build: {collected:?}");
    let c = &collected[0]["attributes"];
    assert_eq!((c["node"].as_str(), c["ttl"].to_string().trim_matches('"')), (Some(NODE), "3000"), "{c}");
    let build = s(&c["build_id"]);
    let exit = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["node"] == NODE && sp["attributes"]["outcome"] == "complete" && s(&sp["attributes"]["build_id"]) == build);
    assert!(exit.is_some(), "the delete Build proved the parked process exited");
    // The stopped birth's stop already ran: its delete sends no seat transfer, node.connections.delete or stop again
    // (node-decommission.md 1, node-stop.md 6).
    let again: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["build_id"]) == build && ["SeatTransfer", "DeleteConnections", "StopNode", "AwaitNodeLeft"].iter().any(|st| sp["attributes"]["step"] == *st))
        .collect();
    assert!(again.is_empty(), "the delete of a stopped birth composes no stop again: {again:?}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == NODE).collect();
    assert!(!deleted.is_empty(), "NodeDeleted is published for the stopped birth");
    estate.record_trace_url(collected[0]["trace_id"].as_str().unwrap_or(""));
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end_ns(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

/// mesh1 {1 node-admin, 1 rpc node} and mesh2 {`admins` node-admins, 1 rpc node}, settled and
/// ready-for-traffic: the fabric-primary is mesh1.admin.1.
async fn two_meshes(test: &str, admins: u32) -> (Estate, String) {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}, {"name": "mesh2", "node_admin": admins, "rpc_node": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    let build = s(&a["build_id"]);
    estate.await_build(&build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", admins, 1)], Duration::from_secs(90)).await;
    // A seat move is refused until the initial fabric has first reached ready-for-traffic (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    (estate, build)
}

/// CONTRACT (R-MT1, Luke 2026-10-12, i143-rulings-2026-10-12-rx-authz.md: "NA1 > node.primary.update >
/// NA2 ---> NA2.update(is_primary = true) > node.primary.updated --> NA1 gossips MP updated NA2";
/// "only a FP should issue commands to a MP"; "this same primitive should be called on MP stop"):
/// `node.stop` of mesh2's primary P moves P's seat before the stop runs. The fabric-primary commands
/// P (`seat.update.via-primary-update`, outcome `updated`); P computes its successor NA2 (mesh2's
/// other node-admin), cuts off and drains the writes its seat authorizes, gossips that the primary
/// is updating to NA2 (`via-announce-primary-updating`, sent on the backbone and its mesh channel),
/// and calls node.primary.update on NA2; NA2 writes its own seat row (`via-seats-committed` at NA2)
/// and answers node.primary.updated; P then gossips that the primary updated
/// (`via-announce-seated-together`), and the fabric-primary installs the committed seat from P's
/// answer (`via-seats-installed` at the fabric-primary) before it dispatches the stop, which NA2
/// executes as mesh2's primary. Once the stop is complete the fabric-primary's view and NA2's view
/// both name NA2 as mesh2's primary, with no wait. No node-admin calls its mesh-primary at any step:
/// every op 0x21 call to P descends from the fabric-primary's command and every one to NA2 from P's
/// update. Must NOT happen: NA2 calling P (the retired TakeSeats direction), a stop step run before
/// the seat moved, or a view still naming P after the stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops() {
    let test = "a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops";
    let (estate, build) = two_meshes(test, 2).await;
    let before = estate.nodes().await;
    let fp = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("a fabric-primary");
    let p = before.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let na2 = before.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] != true).cloned().expect("mesh2's other node-admin");
    let (p_name, p_id, na2_name, na2_id) = (s(&p["name"]), s(&p["node_id"]), s(&na2["name"]), s(&na2["node_id"]));
    assert_ne!(fp, p_name, "mesh2's primary is not the fabric-primary");

    let (status, r) = estate.post(&format!("/api/nodes/{p_name}/stop"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    let attempt_no = Estate::attempt_of(&r);
    estate.await_attempt(&build, attempt_no, SETTLE).await;
    // Every admin of the fabric names NA2 as mesh2's primary, the fabric-primary included, at once.
    for (who, view) in [("the fabric-primary", estate.nodes().await), (na2_name.as_str(), estate.nodes_at(&s(&na2["admin_api_base"])).await)] {
        let primary: Vec<String> = view.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).collect();
        assert_eq!(primary, vec![na2_name.clone()], "{who} names {na2_name} mesh2's primary once the stop is complete: {view:#?}");
    }

    // Each process exports its own spans: the story is read once the fabric-primary's, P's and NA2's
    // parts of it are all exported.
    let spans = wait_for("the stop's last step, the fabric-primary's command, P's update and NA2's row are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let stopped = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().any(|sp| attr(sp, "build_id") == build && attr(sp, "node") == p_name && attr(sp, "step") == "Complete");
        let commanded = named(&spans, "rdm.node_admin.seat.update.via-primary-update").into_iter().any(|sp| attr(sp, "holder") == p_name);
        let updated = named(&spans, "rdm.node_admin.seat.update.via-primary-updating").into_iter().any(|sp| attr(sp, "node") == p_name);
        let committed = named(&spans, "rdm.node_admin.seat.update.via-seats-committed").into_iter().any(|sp| attr(sp, "node") == na2_name);
        // The dispatch span closes when the run's terminal reaches the fabric-primary, after the stop's last step.
        let dispatched = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().any(|sp| attr(sp, "build_id") == build && attr(sp, "executor") == na2_name);
        (stopped && commanded && updated && committed && dispatched).then_some(spans)
    })
    .await;
    let one = |name: &str, f: &dyn Fn(&Value) -> bool| -> Value {
        let found: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| f(sp)).collect();
        assert_eq!(found.len(), 1, "one {name}: {found:#?}");
        found[0].clone()
    };
    let cmd = one("rdm.node_admin.seat.update.via-primary-update", &|sp| attr(sp, "holder") == p_name);
    assert_eq!((attr(&cmd, "node"), attr(&cmd, "seat"), attr(&cmd, "outcome"), attr(&cmd, "successor")), (fp.clone(), "mesh-primary".to_string(), "updated".to_string(), na2_name.clone()), "{cmd:#?}");
    let upd = one("rdm.node_admin.seat.update.via-primary-updating", &|sp| attr(sp, "node") == p_name);
    assert_eq!((attr(&upd, "successor"), attr(&upd, "outcome")), (na2_name.clone(), "updated".to_string()), "{upd:#?}");
    assert!(descends_from(&spans, &upd, &cmd), "P's update runs under the fabric-primary's command: {upd:#?}");
    let drained = one("rdm.node_admin.seat.update.via-writes-drained", &|sp| attr(sp, "node") == p_name);
    let updating = one("rdm.mesh.seat.update.via-announce-primary-updating", &|sp| attr(sp, "node") == p_name);
    assert_eq!((attr(&updating, "successor").contains(&na2_id), attr(&updating, "backbone"), attr(&updating, "mesh_channel")), (true, "sent".to_string(), "sent".to_string()), "{updating:#?}");
    let committed = one("rdm.node_admin.seat.update.via-seats-committed", &|sp| attr(sp, "node") == na2_name && attr(sp, "replay") == "false");
    assert!(attr(&committed, "holder").contains(&p_id) && attr(&committed, "successor").contains(&na2_id) && !attr(&committed, "mesh_epoch").is_empty(), "NA2 wrote its own seat row for mesh2: {committed:#?}");
    assert!(descends_from(&spans, &committed, &upd), "NA2 commits under P's node.primary.update: {committed:#?}");
    let updated = one("rdm.mesh.seat.update.via-announce-seated-together", &|sp| attr(sp, "node") == p_name);
    let installed = one("rdm.node_admin.seat.update.via-seats-installed", &|sp| attr(sp, "node") == fp);
    assert!(attr(&installed, "successor").contains(&na2_id), "{installed:#?}");
    assert!(end_ns(&drained) <= start_ns(&updating), "P drained its writes before it said the primary is updating");
    assert!(start_ns(&updating) <= start_ns(&committed), "P said the primary is updating before NA2 committed");
    assert!(end_ns(&committed) <= start_ns(&updated), "P said the primary updated only after NA2 committed");
    // The stop ran after the seat moved, executed by NA2 as mesh2's primary.
    let dispatches: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "attempt").parse::<u64>().is_ok_and(|a| a >= attempt_no)).collect();
    let stop_run = dispatches.iter().find(|sp| attr(sp, "executor") == na2_name).unwrap_or_else(|| panic!("NA2 executed the stop: {dispatches:#?}"));
    assert!(end_ns(&installed) <= start_ns(stop_run), "the fabric-primary installed the committed seat before it dispatched the stop");
    // The stop is an attempt of the accepted Build: its steps are those of the attempts it opened, never P's birth.
    let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "node") == p_name && attr(sp, "attempt").parse::<u64>().is_ok_and(|a| a >= attempt_no)).collect();
    assert!(steps.iter().any(|sp| attr(sp, "step") == "StopNode"), "the stop's steps are read: {steps:#?}");
    assert!(steps.iter().all(|sp| start_ns(sp) >= end_ns(&committed)), "no stop step ran before the seat moved: {steps:#?}");
    assert!(steps.iter().all(|sp| attr(sp, "step") != "SeatTransfer"), "the stop takes no executor-side seat transfer: {steps:#?}");
    // Commands only go down: every 0x21 call to P is the fabric-primary's command, every one to NA2 P's.
    for call in named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| attr(sp, "op") == "33") {
        let target = attr(call, "target");
        if target.contains(&p_id) {
            assert!(descends_from(&spans, call, &cmd), "a 0x21 call to mesh2's primary that the fabric-primary did not make: {call:#?}");
        }
        if target.contains(&na2_id) {
            assert!(descends_from(&spans, call, &upd), "a 0x21 call to NA2 that P's update did not make: {call:#?}");
        }
    }
    estate.record_trace_url(cmd["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (R-MT1: "it should be rejected if it's the only NA in the mesh"): mesh2 has one node-admin
/// and an rpc node. `POST /api/nodes/mesh2.admin.1/stop` is refused by name, 422 `last-admin`, the
/// detail naming the node, its mesh and that node.primary.update has no other node-admin to take its
/// seat, under `rdm.node_admin.build.reject.via-last-admin`. No attempt opens, no seat moves and
/// every node keeps its birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name() {
    let test = "a_stop_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name";
    let (estate, build) = two_meshes(test, 1).await;
    let before = estate.nodes().await;
    let (_, b) = estate.get(&format!("/api/builds?id={build}")).await;
    let (status, r) = estate.post("/api/nodes/mesh2.admin.1/stop", &json!({})).await;
    assert_eq!((status, r["error"].as_str()), (422, Some("last-admin")), "{r}");
    let detail = s(&r["detail"]);
    assert!(detail.contains("mesh2.admin.1") && detail.contains("mesh2") && detail.contains("node.primary.update"), "the refusal names the node, its mesh and the primitive with no successor: {r}");
    let (_, after) = estate.get(&format!("/api/builds?id={build}")).await;
    assert_eq!(after["attempt"], b["attempt"], "no attempt opened: {after}");
    let spans = wait_for("the refusal is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.reject.via-last-admin").into_iter().any(|sp| attr(sp, "detail").contains("mesh2.admin.1")).then_some(spans)
    })
    .await;
    assert!(named(&spans, "rdm.node_admin.seat.update.via-primary-update").is_empty(), "no seat move was commanded");
    for n in &before {
        let now = estate.node(&s(&n["name"])).await;
        assert_eq!((now["incarnation_id"].clone(), now["is_primary"].clone()), (n["incarnation_id"].clone(), n["is_primary"].clone()), "{} is untouched", s(&n["name"]));
    }
    let refused = named(&spans, "rdm.node_admin.build.reject.via-last-admin").into_iter().find(|sp| attr(sp, "detail").contains("mesh2.admin.1")).unwrap();
    estate.record_trace_url(refused["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
