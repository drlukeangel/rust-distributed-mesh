//! product=mesh, feature=node-lifecycle, subfeature=node-stop, rung=MN, provider=process.
//!
//! Stop parks and start rejoins (node-stop.md, node-start.md), driven through the `node` objects:
//! `node.stop` drains the exact birth, cuts its mesh connections and parks the live process; the
//! node's `left` is the reply of the stop call, never a gossip frame; the mesh primary publishes
//! `NodeStopped` from it. `node.start` makes that same process rejoin as itself.

use rdm_core::PathName;
use crate::build_log;
use rdm_client::{Frame, LaunchKind as NodeKind, NodeEvent, NodeSelector, NodeStatus, NodeView, Nodes};
use rdm_core::status::StatusReply;
use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use serde_json::Value;
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
        peers.fact(peer, "mesh1", "NodeStopped: the parked birth is dropped and its stop overlay is held", |v| v.birth(&before.node_id).is_none() && v.overlay(&before.node_id, "node.stop:").is_some()).await;
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
                v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation_id.as_ref().unwrap()) && v.overlay(&before.node_id, "node.stop:").is_none()
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

/// CONTRACT (R13; node-replace.md Rollback 2-3; proposal node-properties-own-commands.md §2.3
/// "Stopped node"): the caller's `node.name.update` of the parked `mesh1.rpc.1` to the name `1.old`
/// reaches its mesh-primary directly and ends `NameUpdated`, the stopped node told nothing; `node.get`
/// lists the parked birth at `mesh1.rpc.1.old` and nothing at `mesh1.rpc.1`. The rename back to `1` ends `NameUpdated` the same
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
    let birth = rdm_client::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(parked.node_id.clone()), node_id: parked.node_id.clone(), incarnation: parked.incarnation_id.clone().expect("the parked node's birth") };
    let reply = nodes.name_update(&birth, &primary.node_id, &rafka_node_rpc::RouteChoice::Direct, &renamed.name()).await.expect("node.name.update answered");
    assert_eq!(reply, StatusReply::NameUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() });
    let listed = view_of(&nodes, &renamed).await;
    assert!(listed.parked && listed.node_id == before.node_id && listed.incarnation_id == before.incarnation_id, "node.get lists the parked birth at {renamed}: {listed:?}");

    assert_eq!(nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get").into_iter().filter(|n| n.name == path).count(), 0, "node.get lists nothing at {path} while the row says {renamed}");

    // The rollback (node-replace.md Rollback 2-3): the row's name goes back while the node is
    // stopped, and the start by that name rejoins the same process.
    let back = nodes.name_update(&birth, &primary.node_id, &rafka_node_rpc::RouteChoice::Direct, &path.name()).await.expect("node.name.update answered");
    assert_eq!(back, StatusReply::NameUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() });
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
    let update = named(&spans, "rdm.node_admin.node.name.update.via-writer").into_iter().find(|sp| s(&sp["attributes"]["to"]) == renamed.to_string()).expect("the mesh-primary served the node.name.update");
    assert_eq!((s(&update["attributes"]["outcome"]), s(&update["attributes"]["traffic"]), s(&update["attributes"]["from"])), ("name-updated".into(), "Stopped".into(), path.to_string()), "{update:#?}");
    let back = named(&spans, "rdm.node_admin.node.name.update.via-writer").into_iter().find(|sp| s(&sp["attributes"]["to"]) == path.to_string()).expect("the mesh-primary served the rename back");
    assert_eq!((s(&back["attributes"]["outcome"]), s(&back["attributes"]["from"])), ("name-updated".into(), renamed.to_string()), "{back:#?}");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && sp["attributes"]["node"] == path.to_string()).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the start's rejoin was installed");
    assert_eq!(s(&join["attributes"]["admitted_as"]), path.to_string(), "{join:#?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a renamed parked node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-rpc-envelope.md "Node properties": config "is a field of the node record ... a
/// rejoining node gets it back in its join answer"; node-config-update.md: row first, node second; the
/// stopped arm node.name.update already completes on, proposal node-properties-own-commands.md §2.3):
/// `node.config.update` of the stopped `mesh1.rpc.1` at its mesh-primary ends `node.config.updated`
/// on the row: the row at `mesh1.rpc.1` holds the whole config, and the stopped node is sent nothing
/// (the via-config span says `apply=stopped`, no `answered`; no ApplyNodeConfig reaches it). Then
/// `node.start` makes the same process rejoin: its join is handed the row's config
/// (`via-join` `config_keys=1`), and `node.get` lists the same birth `ready-for-traffic` carrying it.
/// What must NOT happen: `ConfigNotApplied` (`not-sent`) for a stopped node, or a call to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_nodes_config_update_completes_on_its_row_and_its_start_carries_it() {
    let test = "a_stopped_nodes_config_update_completes_on_its_row_and_its_start_carries_it";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = NODE.parse().unwrap();
    let mesh = nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get");
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    let pid = estate.pid_of(NODE).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let held = view_of(&nodes, &path).await;
    assert!(held.parked && held.status == NodeStatus::Leaving, "{held:?}");

    // The update: one call to the node's mesh-primary, the row written, the stopped node told nothing.
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(held.node_id.clone()), node_id: held.node_id.clone(), incarnation: held.incarnation_id.clone().expect("the stopped node's birth") };
    let carrier = estate.carrier().await;
    let reply = rdm_client::NodeRpc::new(carrier.client())
        .config_update(&primary.node_id, &rafka_node_rpc::RouteChoice::Direct, &birth, told.clone(), &rafka_node_rpc::CallOptions::default())
        .await
        .expect("node.config.update answered");
    assert_eq!(reply, StatusReply::ConfigUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() }, "a stopped node's config update completes on its row");

    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    let after = view_of(&nodes, &path).await;
    assert!(!after.parked && after.status == NodeStatus::ReadyForTraffic, "node.get lists {path} ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth");
    assert_eq!(after.config, told, "the started node carries the config its join handed it");
    assert_eq!(estate.pid_of(NODE).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let update = named(&spans, "rdm.node_admin.node.update.via-config").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).expect("the mesh-primary served the node.config.update");
    let a = &update["attributes"];
    assert_eq!((s(&a["outcome"]), s(&a["apply"]), s(&a["subject"]), s(&a["answered"])), ("config-updated".into(), "stopped".into(), path.to_string(), String::new()), "the stopped node is sent nothing: {update:#?}");
    assert!(named(&spans, "rdm.node_admin.node.update.via-apply-node-config").is_empty(), "no ApplyNodeConfig reached the stopped node");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && sp["attributes"]["node"] == path.to_string()).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the start's rejoin was installed");
    assert_eq!((s(&join["attributes"]["admitted_as"]), s(&join["attributes"]["config_keys"])), (path.to_string(), "1".to_string()), "the join answer hands the row's config: {join:#?}");
    eprintln!("trace {}: via-config outcome=config-updated apply=stopped; trace {}: via-join admitted_as={path} config_keys=1", s(&update["trace_id"]), s(&join["trace_id"]));
    estate.record_trace_url(update["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-JN1; node-start.md step 2, node-create.md step 4; R12/R13; CLAUDE.md read rule): in a
/// mesh of three node-admins, the non-primary `mesh1.admin.3` is stopped and its mesh-primary renames
/// its row to `mesh1.admin.3.old` (`node.name.update` of the name `3.old`, the stopped node told nothing). Every admin lists
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
    let admins: Vec<NodeView> = mesh.iter().filter(|n| n.name != path && n.kind == rdm_core::NodeKind::NodeAdmin).cloned().collect();
    assert!(admins.iter().any(|n| n.name != primary.name), "an admin that performs no rename: {admins:?}");
    let rpc = mesh.iter().find(|n| n.kind == rdm_core::NodeKind::RpcNode).cloned().expect("the rpc node");
    let pid = estate.pid_of(path.to_string().as_str()).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let parked = view_of(&nodes, &path).await;
    assert!(parked.parked && parked.status == NodeStatus::Leaving, "{parked:?}");

    // The rename: one call to the node's mesh-primary, the row moved, the stopped node told nothing.
    let birth = rdm_client::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(parked.node_id.clone()), node_id: parked.node_id.clone(), incarnation: parked.incarnation_id.clone().expect("the parked node's birth") };
    let reply = nodes.name_update(&birth, &primary.node_id, &rafka_node_rpc::RouteChoice::Direct, &renamed.name()).await.expect("node.name.update answered");
    assert_eq!(reply, StatusReply::NameUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() });
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
/// `node.stop:<path>`: stop-node to the birth, whose reply is `left`, then Complete. The runtime is
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
    let attempt_no = u64::from(r.attempt) + u64::from(before["is_primary"] == true);
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
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("node.stop:{NODE}"), "one stop operation and nothing else: {reconcile}");
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

/// CONTRACT (node-identity-and-state.md §6.9, R-GC1; node-stop.md step 6): on a fabric whose
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
    // (lifecycles/node-delete.md 1, node-stop.md 6).
    let again: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["build_id"]) == build && ["DeleteConnections", "StopNode", "AwaitNodeLeft"].iter().any(|st| sp["attributes"]["step"] == *st))
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
/// ready-for-traffic, through the typed objects: the fabric-primary is mesh1.admin.1.
async fn two_meshes(test: &str, admins: u32) -> (Estate, Vec<NodeView>) {
    settled_two_meshes(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, admins).await
}

/// [`two_meshes`] with every node-admin the testkit's faulted node-admin (`faults::binding_set`): its
/// fault door (testkit Node RPC op 0x75) withholds an announcement the cell arms, and nothing else of
/// it differs.
async fn faulted_two_meshes(test: &str, admins: u32) -> (Estate, Vec<NodeView>) {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    settled_two_meshes(estate, admins).await
}

async fn settled_two_meshes(estate: Estate, admins: u32) -> (Estate, Vec<NodeView>) {
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
    // A seat move is refused until the initial fabric has first reached ready-for-traffic (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    (estate, views)
}

/// CONTRACT (the approved proposal docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md
/// Part C1, Luke yes 2026-10-12: "One writer, one row, one point of no return ... written by the mesh's
/// one row writer (R-OW1, R14), never by the successor"): `node.stop` of mesh2's primary P moves P's
/// seat before the stop runs, and every node names the successor NA2.
/// 1. The fabric-primary commands P (`seat.update.via-primary-update` at the fabric-primary, outcome
///    `updated`, successor NA2).
/// 2. P pre-checks its successor NA2 (mesh2's other node-admin), publishes "primary updating"
///    (`via-announce-primary-updating` at P, sent on the backbone and its mesh channel), cuts off its
///    seat's writes with every issued one finished (`via-writes-cut-off`), and writes the ONE committed
///    row itself (`seat.update.via-seat-row-written` at P, holder P, successor NA2, the mesh seat's
///    next epoch).
/// 3. P calls node.primary.update on NA2 carrying the row; NA2 installs from the row and answers
///    node.primary.updated (`seat.update.via-primary-updated` at NA2, under P's update); P then
///    publishes "primary updated" (`via-announce-primary-updated` at P; R-MT1: "NA1 gossips that the
///    primary updated to NA2"), which every other node of both meshes hears naming NA2
///    (`rdm.mesh.seat.update.via-primary-updated`).
/// 4. The fabric-primary installs the committed seat from P's answer (`via-seats-installed`) before
///    it dispatches the stop, which NA2 executes as mesh2's primary; every admin's `node.get` names
///    NA2 mesh2's primary.
/// Must NOT happen: NA2 writing a seat row (`via-seat-row-written` at NA2) or publishing "primary
/// updated", a 0x21 call to P the
/// fabric-primary did not make or one to NA2 P did not make, a stop step before the row, or a view
/// still naming P.
///
/// Adversarial (R-NS2: "Final aggregates and leadership announcements are best-effort dissemination.
/// A stop does not wait for their delivery. Committed lifecycle facts remain recoverable after the
/// sender cuts gossip"): P's "primary updated" is withheld on both channels (the fault door's
/// `announcement` cut), and P's stop cuts its transport right behind it. Every other node still
/// holds the committed seat naming NA2: the fabric-primary from P's answer, every other node through
/// the replay or the surviving publisher, never from P's frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops() {
    let test = "a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops";
    let (mut estate, before) = faulted_two_meshes(test, 2).await;
    let fp = before.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let p = before.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2's primary");
    let na2 = before.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && !n.is_primary).cloned().expect("mesh2's other node-admin");
    let (p_name, p_id, na2_name, na2_id) = (p.name.to_string(), p.node_id.to_string(), na2.name.to_string(), na2.node_id.to_string());
    assert_ne!(fp, p_name, "mesh2's primary is not the fabric-primary");
    // P's "primary updated" never leaves it, on either channel.
    let door = Door::at(&estate, &p_name).await;
    door.arm("primary-updated", serde_json::json!({"kind": "announcement", "announcement": "primary-updated", "channel": "any"})).await;

    let nodes = estate.nodes_obj().await;
    let stopped = frames(nodes.stop(&p.name).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    // Every admin of the fabric still answering names NA2 mesh2's primary, the fabric-primary included.
    for admin in before.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.name != p.name) {
        let view = estate.node_views_at(&admin.name).await.unwrap_or_else(|| panic!("{} answers node.get", admin.name));
        let primary: Vec<String> = view.iter().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).collect();
        assert_eq!(primary, vec![na2_name.clone()], "{} names {na2_name} mesh2's primary once the stop is complete", admin.name);
    }

    estate.stop().await;
    let spans = estate.spans();
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
    // The one committed row is the holder's: the successor never writes a seat row.
    let by_successor: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").into_iter().filter(|sp| attr(sp, "node") == na2_name).collect();
    assert!(by_successor.is_empty(), "the successor wrote a seat row: {by_successor:#?}");
    let row = one("rdm.node_admin.seat.update.via-seat-row-written", &|sp| attr(sp, "node") == p_name);
    assert!(attr(&row, "holder").contains(&p_id) && attr(&row, "successor").contains(&na2_id) && !attr(&row, "mesh_revision").is_empty() && attr(&row, "fabric_revision").is_empty(), "P, mesh2's writer, wrote the one committed row moving its mesh seat to NA2: {row:#?}");
    let updating = one("rdm.mesh.seat.update.via-announce-primary-updating", &|sp| attr(sp, "node") == p_name);
    assert_eq!((attr(&updating, "seats").contains(&na2_id), attr(&updating, "backbone"), attr(&updating, "mesh_channel")), (true, "sent".to_string(), "sent".to_string()), "{updating:#?}");
    let cut_off = one("rdm.node_admin.seat.update.via-writes-cut-off", &|sp| attr(sp, "node") == p_name);
    let installed_at_na2 = one("rdm.node_admin.seat.update.via-primary-updated", &|sp| attr(sp, "node") == na2_name);
    assert_eq!((attr(&installed_at_na2, "replay"), attr(&installed_at_na2, "holder").contains(&p_id), attr(&installed_at_na2, "successor").contains(&na2_id)), ("false".to_string(), true, true), "NA2 installed from P's row and answered node.primary.updated: {installed_at_na2:#?}");
    assert!(descends_from(&spans, &installed_at_na2, &upd), "NA2 installs under P's node.primary.update call: {installed_at_na2:#?}");
    let announced = one("rdm.mesh.seat.update.via-announce-primary-updated", &|sp| attr(sp, "node") == p_name);
    assert!(attr(&announced, "seats").contains(&na2_id), "{announced:#?}");
    assert_eq!((attr(&announced, "backbone"), attr(&announced, "mesh_channel")), ("withheld".to_string(), "withheld".to_string()), "P's primary updated was withheld on both channels: {announced:#?}");
    // NA2, P's next writer, says primary updated only once P's stop completed and P can say nothing
    // more (R-NS2; C1 failure contract: "the next writer to read the row publishes 'updated'"), under
    // the span naming the stopped holder, never before P's stop cut its transport.
    let by_na2: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-announce-primary-updated").into_iter().filter(|sp| attr(sp, "node") == na2_name).collect();
    let p_cut = named(&spans, "rdm.node_admin.node.update.via-stop-transport-cut").into_iter().filter(|sp| attr(sp, "node") == p_name).map(end_ns).min();
    for said in &by_na2 {
        let after_stop = named(&spans, "rdm.mesh.seat.update.via-holder-stopped").into_iter().find(|h| attr(h, "node") == na2_name && attr(h, "holder") == p_name && descends_from(&spans, said, h)).cloned();
        let after_stop = after_stop.unwrap_or_else(|| panic!("the successor published primary updated other than after the holder's stop: {said:#?}"));
        assert!(p_cut.is_some_and(|cut| cut <= start_ns(&after_stop)), "the successor said primary updated only after P's stop cut its transport ({p_cut:?}): {after_stop:#?}");
        eprintln!("NA2 said primary updated after P's stop: trace {}", s(&after_stop["trace_id"]));
    }
    let installed = one("rdm.node_admin.seat.update.via-seats-installed", &|sp| attr(sp, "node") == fp);
    assert!(attr(&installed, "successor").contains(&na2_id), "{installed:#?}");
    // The order of the sequence (C1 steps 3-7).
    assert!(start_ns(&updating) <= start_ns(&cut_off), "P said the primary is updating before its cut-off");
    assert!(end_ns(&cut_off) <= start_ns(&row), "P cut off its writes before it wrote the row");
    assert!(end_ns(&row) <= start_ns(&installed_at_na2), "NA2 installed only after P's row");
    assert!(end_ns(&installed_at_na2) <= start_ns(&announced), "P said the primary updated only after NA2 installed and answered");
    // Every other node of both meshes holds the committed seat naming NA2, though P's frame never
    // left it: the fabric-primary from P's answer (`via-seats-installed` above), every node through
    // the replay or the surviving publisher.
    for n in before.iter().filter(|n| n.name != p.name && n.name != na2.name) {
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "node") == n.name.to_string() && attr(sp, "successor").contains(&na2_id)).collect();
        let seated: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-announcement").into_iter().filter(|sp| attr(sp, "node") == n.name.to_string() && attr(sp, "seat") == "mesh-primary" && attr(sp, "holder").contains(&na2_id)).collect();
        let held = heard.iter().chain(seated.iter()).any(|sp| matches!(attr(sp, "outcome").as_str(), "held" | "same")) || n.name.to_string() == fp;
        assert!(held, "{} holds the committed seat naming NA2 with P's frame withheld: primary updated {heard:#?}, seated {seated:#?}", n.name);
        eprintln!("seat at {}: {:?}", n.name, heard.iter().chain(seated.iter()).map(|sp| format!("{} via {} {} trace {}", s(&sp["name"]), attr(sp, "via"), attr(sp, "outcome"), s(&sp["trace_id"]))).collect::<Vec<_>>());
    }
    // The stop ran after the seat moved, executed by NA2 as mesh2's primary.
    let stop_run = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| attr(sp, "executor") == na2_name).unwrap_or_else(|| panic!("NA2 executed the stop"));
    assert!(end_ns(&installed) <= start_ns(stop_run), "the fabric-primary installed the committed seat before it dispatched the stop");
    // The stop is an attempt of the Build that birthed P: its steps are that attempt's, never P's birth's.
    assert!(attr(&cmd, "attempt") != "1" && attr(stop_run, "attempt") != "1", "neither the seat command nor the stop dispatch is P's birth attempt");
    let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "node") == p_name && attr(sp, "build_id") == attr(&cmd, "build_id") && attr(sp, "attempt") == attr(stop_run, "attempt")).collect();
    assert!(steps.iter().any(|sp| attr(sp, "step") == "StopNode"), "the stop's steps are read: {steps:#?}");
    assert!(steps.iter().all(|sp| start_ns(sp) >= end_ns(&row)), "no stop step ran before the row: {steps:#?}");
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
}

/// CONTRACT (the approved proposal Part C1 "The no-successor cases", Q7 ruled Luke 2026-10-12:
/// "`node.primary.update` commanded for a cohort with no other node of that type in the mesh is refused
/// by name, `cannot transfer: no secondary of type <node_type> in mesh <mesh>`"): mesh2 has one
/// node-admin and an rpc node. `node.stop` of mesh2.admin.1 is refused `last-admin` with exactly
/// `cannot transfer: no secondary of type node-admin in mesh mesh2`, under
/// `rdm.node_admin.build.reject.via-last-admin`. No seat move is commanded, no row is written and
/// every node keeps its birth and its seat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name() {
    let test = "a_stop_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name";
    let (mut estate, before) = two_meshes(test, 1).await;
    let nodes = estate.nodes_obj().await;
    let target: PathName = "mesh2.admin.1".parse().unwrap();
    let refused = nodes.stop(&target).await.err().expect("the stop is refused");
    let rdm_client::CallEnd::Rejected { reason, detail } = &refused else { panic!("the stop ends Rejected: {refused:?}") };
    // The typed carrier names the reason before the refusal's own text (build_stream.rs).
    assert_eq!((reason.as_str(), detail.strip_prefix("last-admin: ")), ("last-admin", Some("cannot transfer: no secondary of type node-admin in mesh mesh2")), "{refused:?}");
    for mesh in ["mesh1", "mesh2"] {
        for now in estate.mesh_node_views(mesh).await {
            let was = before.iter().find(|n| n.name == now.name).unwrap_or_else(|| panic!("{} was there before", now.name));
            assert_eq!((&now.incarnation_id, now.is_primary, now.is_fabric_primary), (&was.incarnation_id, was.is_primary, was.is_fabric_primary), "{} is untouched", now.name);
        }
    }
    estate.stop().await;
    let spans = estate.spans();
    let rejected: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-last-admin").into_iter().filter(|sp| attr(sp, "detail") == "cannot transfer: no secondary of type node-admin in mesh mesh2").collect();
    assert_eq!(rejected.len(), 1, "{rejected:#?}");
    assert!(named(&spans, "rdm.node_admin.seat.update.via-primary-update").is_empty(), "no seat move was commanded");
    assert!(named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").is_empty(), "no row was written");
    estate.record_trace_url(rejected[0]["trace_id"].as_str().unwrap_or(""));
}

/// The seat-transfer rows on the fabric's store root, `meshes/<mesh>/seat-transfers/`, each as its
/// file name and bytes: what a seat move commits, compared whole.
fn seat_transfer_rows(estate: &Estate) -> Vec<(String, Vec<u8>)> {
    let meshes = estate.fabric_store_root().join("meshes");
    let mut rows: Vec<(String, Vec<u8>)> = std::fs::read_dir(&meshes)
        .into_iter()
        .flatten()
        .flatten()
        .flat_map(|m| std::fs::read_dir(m.path().join("seat-transfers")).into_iter().flatten().flatten())
        .map(|r| (r.path().display().to_string(), std::fs::read(r.path()).unwrap_or_default()))
        .collect();
    rows.sort();
    rows
}

/// CONTRACT (R-SB1, Luke 2026-10-13: "a node.stop or node.restart of a mesh's only admin is refused
/// by name under Q7, no secondary, before anything moves. The stop path's via-seat-hand-over to=none
/// is the defect: a refused transfer never erases the incumbent's seat and never yields to nobody, and
/// the destructive stop does not continue"; the approved proposal Part C1 "The no-successor cases",
/// Q7: "`node.fabric-primary.update` with no other node-admin ReadyForTraffic in the mesh and no other
/// mesh-primary ReadyForTraffic is refused the same way, naming both searches ... a single-admin
/// single-mesh fabric's primary ... cannot be stopped or restarted until a secondary is ReadyForTraffic";
/// Part C2 step 3's text): mesh1.admin.1 is the fabric's only node-admin, holding mesh1's seat and the
/// fabric seat, beside one rpc node. Its `node.stop` and its `node.restart` are each refused
/// `last-admin` with exactly `cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh
/// mesh1, no other mesh-primary ReadyForTraffic`, one `rdm.node_admin.build.reject.via-last-admin` per
/// call. Leadership is intact: every node keeps its
/// birth and its seats (node.get), and the store root's seat-transfer rows are byte for byte what they
/// were. The stop is not run: no stop-node served, no node.connections.delete, no seat move commanded,
/// no row written. Through the whole run, the estate's teardown included, no admin hands its seat to
/// nobody (`rdm.node_admin.build.update.via-seat-hand-over` with `to = none`).
/// Must NOT happen: a stop or restart step of the only admin, a seat row written, a hand-over to none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_or_restart_of_the_fabrics_only_node_admin_is_refused_by_name_and_nothing_moves() {
    let test = "a_stop_or_restart_of_the_fabrics_only_node_admin_is_refused_by_name_and_nothing_moves";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1)]).await;
    let before = estate.settled_views(&[("mesh1", 1, 1)]).await;
    // A seat move is refused until the initial fabric has first reached ready-for-traffic (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    let rows = seat_transfer_rows(&estate);
    let nodes = estate.nodes_obj().await;
    let target: PathName = "mesh1.admin.1".parse().unwrap();
    let held = before.iter().find(|n| n.name == target).expect("node.get lists the only admin");
    assert!(held.is_primary && held.is_fabric_primary, "mesh1.admin.1 holds both seats: {held:?}");
    let q7 = "cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh mesh1, no other mesh-primary ReadyForTraffic";
    for (action, refused) in [("stop", nodes.stop(&target).await.err()), ("restart", nodes.restart(&target).await.err())] {
        let refused = refused.unwrap_or_else(|| panic!("the {action} of the fabric's only node-admin was accepted"));
        let rdm_client::CallEnd::Rejected { reason, detail } = &refused else { panic!("the {action} ends Rejected: {refused:?}") };
        assert_eq!((reason.as_str(), detail.strip_prefix("last-admin: ")), ("last-admin", Some(q7)), "{action}: {refused:?}");
    }
    for now in estate.mesh_node_views("mesh1").await {
        let was = before.iter().find(|n| n.name == now.name).unwrap_or_else(|| panic!("{} was there before", now.name));
        assert_eq!((&now.incarnation_id, now.is_primary, now.is_fabric_primary), (&was.incarnation_id, was.is_primary, was.is_fabric_primary), "{} is untouched", now.name);
    }
    assert_eq!(seat_transfer_rows(&estate), rows, "the seat-transfer rows are unchanged");
    estate.stop().await;
    let spans = estate.spans();
    let rejected: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-last-admin").into_iter().filter(|sp| attr(sp, "detail") == q7).collect();
    assert_eq!(rejected.len(), 2, "one refusal per call: {rejected:#?}");
    for name in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.node.connections.delete.via-stop", "rdm.node_admin.seat.update.via-primary-update", "rdm.node_admin.seat.update.via-seat-row-written"] {
        let ran: Vec<&Value> = named(&spans, name);
        assert!(ran.is_empty(), "the refused stop ran {name}: {ran:#?}");
    }
    let to_none: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-seat-hand-over").into_iter().filter(|sp| attr(sp, "to") == "none").collect();
    assert!(to_none.is_empty(), "an admin handed its seat to nobody: {to_none:#?}");
    estate.record_trace_url(rejected[0]["trace_id"].as_str().unwrap_or(""));
}
