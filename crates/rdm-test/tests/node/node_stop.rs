//! product=mesh, feature=node-lifecycle, subfeature=node-stop, rung=MN, provider=process.
//!
//! Stop leaves the process stopped and start rejoins (node-stop.md, node-start.md), driven through the `node` objects:
//! `node.stop` deletes the exact birth's connections, cuts its mesh connections and stops the live process; the
//! node's `left` is the reply of the stop call, never a gossip frame; the mesh primary publishes
//! `NodeStopped` from it. `node.start` makes that same process rejoin as itself.

use rdm_core::PathName;
use crate::build_log;
use rdm_client::{Frame, NodeEvent, NodeStatus, NodeView, Nodes};
use rdm_core::status::StatusReply;
use rdm_core::OperationSubject;
use rdm_test::estate::{at_location, names_location, planned_operations, plans_operation, descends_from, named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, FaultClient};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);
/// The node the cells stop and start.
fn node_at() -> PathName {
    Estate::path("mesh1.node", "1")
}

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-stop".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn step_span<'a>(spans: &'a [Value], build: &str, node: &PathName, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && at_location(sp, node) && s(&sp["attributes"]["step"]) == step)
}

/// Every frame of `stream`, in order, up to and including the terminal one.
async fn frames(mut stream: rdm_client::WorkflowStream) -> Vec<Frame> {
    let mut out = Vec::new();
    // Bounded: a stream that never reaches its terminal fails here, naming the frames it sent, while
    // the estate's evidence is still kept, never at the suite's timeout.
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let Ok(next) = tokio::time::timeout_at(deadline, stream.next()).await else {
            panic!("the reply stream sent no terminal within {SETTLE:?}: {out:?}");
        };
        let Some(f) = next else { break };
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
    nodes.get(&rdm_client::Filter::new().eq("path", [&node.path]).eq("name", [&node.name])).await.expect("node.get [mesh]").into_iter().find(|n| &n.location() == node).unwrap_or_else(|| panic!("node.get lists no {node:?}"))
}

/// CONTRACT: `node.stop` of a live node answers, on its reply stream, the connections delete, the hard cut of its
/// mesh connections and `left`, and ends `Complete`: the process is stopped, alive, with the same node
/// id, incarnation, endpoint key and port; `node.get` shows it `leaving` and `stopped`. The node
/// gossips nothing for the stop: the mesh primary publishes `NodeStopped`, and every other node of the
/// mesh reports, through its own held view, that it dropped the stopped birth and holds the stop
/// overlay. `node.start` makes the same process rejoin through the mesh primary: the stream carries
/// `node.joined`, `node.started` and ends `Complete`, `node.get` shows the same birth
/// `ready`, and every other node reports it holds that birth again with the overlay gone.
/// The mesh primary records the start's Pending once, while it accepts the start's join report and
/// before it answers it, and takes the start's `NodeJoined` as already applied (node-mesh-join.md
/// step 10). The start also declares Ready to its mesh primary as an owed message naming the start;
/// frame and declaration apply once (node-start.md step 5; Q49). Q93 supersedes Q47's line "the open
/// start sends no separate Ready declaration". No runtime exits, no node is created, nothing is repaired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_keeps_the_process_and_a_start_rejoins_it_as_the_same_birth() {
    stop_then_start("a_stop_keeps_the_process_and_a_start_rejoins_it_as_the_same_birth", &node_at()).await;
}

/// CONTRACT: the same holds for a node-admin: a mesh's non-primary admin is stopped by its mesh primary
/// (its tasks held, its gossip channels left, its connections cut), stays alive with its endpoint bound,
/// and a start makes the same process rejoin: the same node id, incarnation, endpoint key and port,
/// `ready` again, and every other node reports it held again. Its start declares Ready as an owed
/// message naming the start (node-start.md step 5); Q93 supersedes Q47's line "the open start sends no
/// separate Ready declaration".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_admin_keeps_its_process_and_a_start_rejoins_it_as_the_same_birth() {
    stop_then_start("a_stopped_node_admin_keeps_its_process_and_a_start_rejoins_it_as_the_same_birth", &Estate::path("mesh1.node-admin", "2")).await;
}

async fn stop_then_start(test: &str, node: &PathName) {
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = node.clone();

    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.location() == path).cloned().expect("node.get lists the node");
    let others: Vec<&NodeView> = mesh.iter().filter(|n| n.location() != path).collect();
    let pid = estate.pid_of(node).await;
    assert!(alive(pid));

    // The stop: the reply stream carries its node.connections.deleted and stopped.
    let builds = estate.build_ids().await;
    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    // A node command of the primary above the node, never a Build (Q342): no Build's Started frame.
    assert!(!stopped.iter().any(|f| matches!(f, Frame::Started { .. })), "{stopped:?}");
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    // Every other node reports that it dropped the stopped birth and holds the stop overlay.
    for peer in &others {
        rdm_test::typed::peer_holds(&*peers, peer, "mesh1", "NodeStopped: the stopped birth is dropped and its stop overlay is held", |v| v.birth(&before.node_id).is_none() && v.overlay(&before.node_id, "node.stop").is_some()).await;
    }
    // node.get shows it stopped: the same birth, the same process, alive.
    let stopped = view_of(&estate.nodes_obj().await, &path).await;
    assert_eq!(stopped.state, NodeStatus::Stopped, "node.get lists {node:?} stopped, the state the node wrote on its own record at its stop (Q356): {stopped:?}");
    let (writer, record) = estate.record_lines(&before.node_id).pop().expect("the node's record");
    assert_eq!((writer.as_str(), record.state), (before.node_id.as_str(), rdm_core::model::NodeStatus::Stopped), "the record's latest line is the node's own stopped: {record:?}");
    assert_eq!((&stopped.node_id, &stopped.incarnation, &stopped.endpoint_id, &stopped.transport_addr), (&before.node_id, &before.incarnation, &before.endpoint_id, &before.transport_addr), "a stop keeps the node id, incarnation, endpoint key and port");
    assert!(alive(pid), "the stopped process {pid} is alive and stopped");
    assert_eq!(estate.pid_of(node).await, pid);

    // The start: the same process rejoins.
    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    assert_eq!(estate.build_ids().await, builds, "a stop and a start write no Build");
    for peer in &others {
        rdm_test::typed::peer_holds(&*peers, peer, "mesh1", "the rejoined birth is held again as the same incarnation and the stop overlay is gone", |v| {
                v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation.as_ref().unwrap()) && v.overlay(&before.node_id, "node.stop").is_none()
            })
            .await;
    }
    let after = view_of(&estate.nodes_obj().await, &path).await;
    assert!(after.state == NodeStatus::Ready, "node.get lists {node:?} ready again: {after:?}");
    let (writer, record) = estate.record_lines(&before.node_id).pop().expect("the node's record");
    assert_eq!((writer.as_str(), record.state), (before.node_id.as_str(), rdm_core::model::NodeStatus::Ready), "the record's latest line is the node's own ready: {record:?}");
    assert_eq!((&after.node_id, &after.incarnation, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation, &before.endpoint_id, &before.transport_addr), "a start keeps the node id, incarnation, endpoint key and port");
    assert_eq!(estate.pid_of(node).await, pid, "the same process");

    // The trace deliverable: the story's spans, each named once.
    estate.stop().await;
    let spans = estate.spans();
    // The node's own spans name it by its id; the mesh-primary's spans about it by its location.
    let node_id = before.node_id.to_string();
    let names_node = |sp: &Value| at_location(sp, node) || attr(sp, "node") == node_id;
    let of_node = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| names_node(sp)).collect() };
    // The commanded stop's steps; the estate's teardown runs the same workflow on its signal, named `signal`.
    let of_stop = |name: &str| -> Vec<&Value> { of_node(name).into_iter().filter(|sp| sp["attributes"]["operation"] != "signal").collect() };
    let stop_span = of_node("rdm.node_admin.status.update.via-stop-node");
    assert_eq!(stop_span.len(), 1, "{stop_span:?}");
    assert_eq!(of_stop("rdm.node_admin.connection.delete.via-stop").len(), 1, "the stop runs its node.connection.delete {{ All }} once");
    assert_eq!(of_stop("rdm.node_admin.node.update.via-stop-transport-cut").len(), 1, "then cuts its transport once");
    assert_eq!(of_node("rdm.node_admin.node.update.via-node-stopped").len(), 1, "the mesh primary publishes NodeStopped once");
    // i146 Q36, the control: the stop answered within its bound is recorded in the birth's declared
    // row once, from whichever of its declaration naming the stop and the stop stream's stopped reached
    // the mesh primary first; the other finds it held and publishes nothing.
    let received = of_node("rdm.node_admin.node.update.via-stop-received");
    assert_eq!(received.len(), 1, "the stop stream's stopped is recorded once: {received:#?}");
    // A node's own declaration of `state` naming an operation of `kind`, whichever kind of node made it:
    // a node's `via-declare-own`, a node-admin's declarer's `via-declare` (i146 Q37: one rule for
    // every node, node-status.md "Status report").
    let declared_of = |state: &str, kind: &str| -> Vec<&Value> {
        let own = named(&spans, "rdm.node_rpc.status.update.via-declare-own").into_iter().filter(move |sp| sp["attributes"]["state"] == state);
        let admin = named(&spans, "rdm.node_admin.status.update.via-declare").into_iter().filter(move |sp| sp["attributes"]["key"].as_str().is_some_and(|k| k.starts_with("OwnState(") && k.ends_with(&format!(", {state})"))));
        own.chain(admin).filter(|sp| names_node(sp) && sp["attributes"]["operation"] == kind).collect()
    };
    let declared = declared_of("Stopped", "node.stop");
    assert!(!declared.is_empty(), "{node:?}'s stop declared its Stopped naming the stop");
    let applied = received.iter().chain(declared.iter()).filter(|sp| sp["attributes"]["outcome"] == "applied").count();
    assert_eq!(applied, 1, "exactly one of the declaration and the stop stream's stopped applied the birth's Stopped: received {received:#?}, declared {declared:#?}");
    assert_eq!(named(&spans, "rdm.mesh.topology.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == "node-left" && names_location(sp, "subject", node)).count(), 0, "the node gossiped no node-left");
    let served = of_node("rdm.node_admin.status.update.via-start-node");
    assert_eq!((served.len(), served[0]["attributes"]["outcome"].as_str()), (1, Some("started")), "{served:?}");
    // node-mesh-join.md step 10: the start's Pending is recorded and published once, by the mesh
    // primary, when it accepts the start's join report and before it answers it; the start's
    // NodeJoined frame belongs to that Pending and applies as already applied.
    let cut = start_ns(of_stop("rdm.node_admin.node.update.via-stop-transport-cut")[0]);
    let join = of_node("rdm.node_admin.node.update.via-join").into_iter().find(|sp| attr(sp, "outcome") == "installed" && start_ns(sp) > cut).unwrap_or_else(|| panic!("{node:?}: the start's join report is accepted after the stop's transport cut"));
    let admitted: Vec<&Value> = of_node("rdm.node_admin.status.update.via-mesh-join-admitted").into_iter().filter(|sp| start_ns(sp) > cut && attr(sp, "outcome") == "applied").collect();
    assert_eq!(admitted.len(), 1, "{node:?}: the start's Pending is recorded once, at its accepted join report: {admitted:#?}");
    assert!(start_ns(admitted[0]) >= start_ns(join) && end_ns(admitted[0]) <= end_ns(join), "{node:?}: the Pending is recorded while the join report is accepted, before it is answered: pending {}, join {join}", admitted[0]);
    let joined_frames: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| names_node(sp) && attr(sp, "frame") == "node-joined" && attr(sp, "operation") == "node.start").map(|sp| attr(sp, "outcome")).collect();
    assert_eq!(joined_frames, vec!["already-applied".to_string()], "{node:?}: the start's NodeJoined belongs to the join report's Pending and starts no other");
    // The start's node.started names the start (node-status.md "Status report": "a start's Pending and
    // Ready"), applied by the mesh primary.
    let ready: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| names_node(sp) && attr(sp, "frame") == "node-started" && attr(sp, "operation") == "node.start").collect();
    assert_eq!(ready.len(), 1, "{node:?}'s NodeStarted is recorded on its open start stream: {ready:#?}");
    // node-start.md step 5 (Q93): "The node also declares Ready to its mesh-primary as an owed message, as
    // it declares Stopped (Q36, Q87); frame and declaration apply once (Q49)." The start's Ready declaration
    // names the start, and is applied or answered already applied beside the NodeStarted frame.
    let ready_declared = declared_of("Ready", "node.start");
    let answered: Vec<String> = ready_declared.iter().map(|sp| attr(sp, "outcome")).filter(|o| o == "applied" || o == "already-applied").collect();
    assert!(!answered.is_empty(), "{node:?}'s start declared its Ready naming the start, answered by its mesh primary: {ready_declared:#?}");
    assert!(answered.iter().filter(|o| *o == "applied").count() <= 1, "{node:?}'s start Ready declaration applied at most once: {ready_declared:#?}");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| names_node(sp) && sp["attributes"]["command"] == "start-node").expect("the start command was sent");
    let call = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && attr(sp, "op") == "34").expect("the start call was made on 0x22");
    // The stop's transport cut closed its connection, so the start dials afresh or rides a connection
    // its caller dialled to this birth after the cut: a response the caller owed the node (node-rpc.md
    // §43) may dial the same live birth between the two (i146 e1, the read of this cell's reds).
    if attr(call, "reused") != "false" {
        // A connection between the two births dialled after the cut, by either end: the caller's own
        // dial (within the call's attempts too), or the stopped birth's dial to its mesh-primary for the
        // declaration naming its stop (node-stop.md step 4, i146 Q36).
        let cut = start_ns(of_stop("rdm.node_admin.node.update.via-stop-transport-cut")[0]);
        let identity = attr(call, "delivery_id");
        let (caller, caller_incarnation) = identity.split('#').next().unwrap_or_default().split_once('/').map(|(n, i)| (n.to_string(), i.to_string())).unwrap_or_default();
        let caller_addr = mesh.iter().find(|n| n.node_id.as_str() == caller).and_then(|n| n.transport_addr).unwrap_or_else(|| panic!("the start's caller {caller} is a node of mesh1")).to_string();
        let (birth_addr, incarnation) = (before.transport_addr.expect("the birth's transport address").to_string(), before.incarnation.as_ref().unwrap().as_str().to_string());
        let dialled = named(&spans, "rdm.node_rpc.connection.create.via-dial").into_iter().any(|sp| {
            let by_caller = attr(sp, "incarnation_id") == incarnation && attr(sp, "local").contains(&caller_addr);
            let by_birth = attr(sp, "incarnation_id") == caller_incarnation && attr(sp, "local").contains(&birth_addr);
            attr(sp, "outcome") == "connected" && (by_caller || by_birth) && (cut..=end_ns(call)).contains(&start_ns(sp))
        });
        assert!(dialled, "the start rode a connection between {caller} and {node:?}'s birth dialled after the stop's transport cut: {call}");
    }
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a stopped node is not drift");
    // node-mesh-join.md steps 5 and 7 (Q87): every join is issued a fresh member cert and the node
    // installs the one its join answered, the start's join after a stop as much as its first.
    let issued: Vec<&Value> = named(&spans, "rdm.node_admin.cert.create.via-join").into_iter().filter(|sp| names_node(sp) && attr(sp, "outcome") == "issued").collect();
    let installed = of_node("rdm.node.cert.update.via-join");
    let last = issued.iter().max_by_key(|sp| start_ns(sp)).unwrap_or_else(|| panic!("{node:?}'s joins were issued member certs: {issued:#?}"));
    assert!(issued.len() >= 2, "{node:?}'s first start and its start after the stop were each issued a cert: {issued:#?}");
    assert!(
        installed.iter().any(|sp| attr(sp, "expires_at") == attr(last, "expires_at_rafka_ms") && start_ns(sp) > start_ns(last)),
        "{node:?} installs the cert its start's join was issued, expiring {}: installed {installed:#?}",
        attr(last, "expires_at_rafka_ms")
    );
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// The mesh-primary's record of `node`'s command frames for `operation`, in the order it took them.
fn command_frames(spans: &[Value], node: &PathName, operation: &str) -> Vec<String> {
    let mut of: Vec<&Value> = named(spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| at_location(sp, node) && attr(sp, "operation") == operation).collect();
    of.sort_by_key(|sp| start_ns(sp));
    of.into_iter().map(|sp| attr(sp, "frame")).collect()
}

/// The states of the lines of `node`'s record from its stop's stopping on, each with its writer's node
/// id, in the order they were written ([`Estate::record_lines`]).
fn stop_lines(estate: &Estate, node: &rdm_core::NodeId) -> Vec<(String, String)> {
    let lines: Vec<(String, String)> = estate.record_lines(node).into_iter().map(|(w, r)| (w, serde_json::to_value(r.state).unwrap().as_str().unwrap_or_default().to_string())).collect();
    let from = lines.iter().rposition(|(_, s)| s == "stopping").unwrap_or(lines.len());
    lines[from..].to_vec()
}

/// CONTRACT (Q205; node-stop.md steps 1-5; node-rpc-envelope.md, the node.stop row; states.md "Who
/// sets each state"): a commanded stop is a workflow that calls its primitives, node.connection.delete
/// `{ All }` then node.mesh.leave, and streams each one's acknowledgement and completion on the stop's
/// own stream: the mesh-primary takes `command-accepted`, `connections-deleting`,
/// `stop-connections-deleted`, `leaving`, `stop-mesh-left`, `node-stopped`, in that order, and the node
/// writes each state on its own record before it sends the frame (Q356): stopping,
/// connections-deleting, connections-deleted, leaving, left, stopped, every line its own. The node's owed
/// `Stopped` declaration is held at the mesh-primary for the stop (the faulted admin's `declaration`
/// cut), so the stream's frames reach it first; released, the declaration finds the stop applied (step
/// 5: whichever of the two reaches it first is applied). Must NOT happen: a primitive's frame missing
/// from the stop's stream, a state the frames carry missing from the record, or a line of the stopping
/// node written by another node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commanded_stop_streams_each_primitives_acknowledgement_and_completion_and_the_node_writes_each_state() {
    let test = "a_commanded_stop_streams_each_primitives_acknowledgement_and_completion_and_the_node_writes_each_state";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary = views.iter().find(|n| n.mesh() == "mesh1" && n.r#type == "node-admin" && n.is_primary).map(|n| n.location()).expect("mesh1's primary");
    let path: PathName = node_at();
    let faults = FaultClient::at(&estate, &primary).await;
    let declared_id = views.iter().find(|node| node.location() == path).expect("the stopped node is in the view").node_id.clone();
    faults.arm("declaration", serde_json::json!({"kind": "declaration", "node_id": declared_id, "state": "Stopped"})).await;
    let stopped = frames(estate.nodes_obj().await.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let hit = faults.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node_id"], declared_id.as_str(), "the mesh-primary holds the node's Stopped declaration: {hit}");
    faults.release("declaration").await;

    estate.stop().await;
    let spans = estate.spans();
    let operation = "node.stop".to_string();
    assert_eq!(
        command_frames(&spans, &node_at(), &operation),
        ["command-accepted", "connections-deleting", "stop-connections-deleted", "leaving", "stop-mesh-left", "node-stopped"],
        "the stop's stream carries each primitive's acknowledgement and completion"
    );
    let written = stop_lines(&estate, &declared_id);
    assert_eq!(
        written.iter().map(|(_, s)| s.as_str()).collect::<Vec<_>>(),
        ["stopping", "connections-deleting", "connections-deleted", "leaving", "left", "stopped"],
        "the node writes each state of its stop on its own record: {written:?}"
    );
    assert!(written.iter().all(|(w, _)| w == declared_id.as_str()), "every line of the stop is the node's own: {written:?}");
    let declared: Vec<String> = named(&spans, "rdm.node_admin.status.update.via-declaration").into_iter().filter(|sp| attr(sp, "sender") == declared_id.as_str() && attr(sp, "state") == "Stopped").map(|sp| attr(sp, "outcome")).collect();
    assert_eq!(declared, ["already-applied"], "the released declaration finds the stop applied from its stream");
}

/// CONTRACT (Q205; node-stop.md steps 1-5; node-connections-delete.md): a node's connections deleted
/// `{ All }` as a node command, then its node.stop. The stop of the node already connections-deleted
/// answers each primitive's acknowledgement and completion and repeats no closure: its stream carries
/// the same six frames as any stop, the node closes nothing again (`via-stop` says `already-deleted`)
/// and leaves its mesh once, at the stop (one `via-leaving`, the stop's). Must NOT happen: a second
/// closure, or a leave before the stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_node_already_connections_deleted_answers_each_primitive_and_repeats_no_closure() {
    let test = "a_stop_of_a_node_already_connections_deleted_answers_each_primitive_and_repeats_no_closure";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let path: PathName = node_at();
    // Node-side spans name the node by its id.
    let node_id = estate.node_view(&path).await.node_id.to_string();
    let nodes = estate.nodes_obj().await;
    for stream in [nodes.delete_connections(&path, &rdm_core::status::ConnectionScope::All).await, nodes.stop(&path).await] {
        let got = frames(stream.expect("the node command was accepted")).await;
        assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    }

    estate.stop().await;
    let spans = estate.spans();
    let stop = "node.stop".to_string();
    assert_eq!(
        command_frames(&spans, &node_at(), &stop),
        ["command-accepted", "connections-deleting", "stop-connections-deleted", "leaving", "stop-mesh-left", "node-stopped"],
        "the stop answers each primitive's acknowledgement and completion"
    );
    let leaving: Vec<String> = named(&spans, "rdm.node.mesh.update.via-leaving").into_iter().filter(|sp| attr(sp, "node") == node_id).map(|sp| attr(sp, "operation")).collect();
    assert_eq!(leaving, [stop.clone()], "the node left its mesh once, at its stop");
    let closures: Vec<String> = named(&spans, "rdm.node_admin.connection.delete.via-stop").into_iter().filter(|sp| attr(sp, "node") == node_id && attr(sp, "operation") == stop).map(|sp| attr(sp, "outcome")).collect();
    assert_eq!(closures, ["already-deleted"], "the stop closes nothing again: it answers the receipt its connections delete established");
}

/// CONTRACT (node-name-update.md Precondition: "The node holds no connection: it is stopped, or ...";
/// Completion by case, the stopped node: `node.name.updated` after `Applied`, "its start rejoins it
/// under the name"; Q345: "the old node is stopped, and its name changed with node.name.update"): the
/// caller's `node.name.update` of the stopped `mesh1.node.1` to the name `renamed` reaches its
/// mesh-primary directly and ends `NameUpdated`; `node.get` lists the stopped birth at `mesh1.node`,
/// `renamed` and nothing at `mesh1.node.1`. `node.start` of the new name makes the same process rejoin
/// under it: `node.joined`, `node.started`, `Complete`; `node.get` lists the same birth `ready` at the
/// new name, and every other node holds it publishing that name. The mesh-primary's `via-writer` span
/// says `name-updated` with `traffic=Stopped`, `connections=stopped` and `rejoin=at-start`. What must
/// NOT happen: a refusal, a listing at `mesh1.node.1` after the rename, or the start admitting the
/// birth under another name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_nodes_rename_at_its_mesh_primary_is_applied_and_its_start_rejoins_under_the_new_name() {
    let test = "a_stopped_nodes_rename_at_its_mesh_primary_is_applied_and_its_start_rejoins_under_the_new_name";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = node_at();
    let renamed: PathName = path.with_name("renamed").unwrap();

    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.location() == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary && rdm_core::is_node_admin(&n.r#type)).cloned().expect("node.get names mesh1's primary");
    let others: Vec<&NodeView> = mesh.iter().filter(|n| n.location() != path).collect();
    let pid = estate.pid_of(&node_at()).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let stopped = view_of(&estate.nodes_obj().await, &path).await;
    assert!(stopped.state == NodeStatus::Stopped, "{stopped:?}");

    // The rename of a stopped node: one call to its mesh-primary, which checks the name and tells the
    // bound process, which writes the name on its own record (Q356).
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(stopped.node_id.clone()), node_id: stopped.node_id.clone(), incarnation: stopped.incarnation.clone().expect("the stopped node's birth") };
    let reply = nodes.name_update(&birth, &primary.node_id, &renamed.name).await.expect("node.name.update answered");
    assert!(matches!(reply, StatusReply::NameUpdated { .. }), "a stopped node holds no connection and is renamed: {reply:?}");
    let listed = view_of(&estate.nodes_obj().await, &renamed).await;
    assert!(listed.state == NodeStatus::Stopped && listed.node_id == before.node_id && listed.incarnation == before.incarnation, "node.get lists the stopped birth at {renamed:?}: {listed:?}");
    assert_eq!(estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().filter(|n| n.location() == path).count(), 0, "node.get lists nothing at {path:?}");

    // The start by its new name rejoins the same process under it.
    let started = frames(nodes.start(&renamed).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    for peer in &others {
        rdm_test::typed::peer_holds(&*peers, peer, "mesh1", "the rejoined birth is held again as the same incarnation, publishing its new name", |v| {
                v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation.as_ref().unwrap() && d.node.location() == renamed)
            })
            .await;
    }
    let after = view_of(&estate.nodes_obj().await, &renamed).await;
    assert!(after.state == NodeStatus::Ready, "node.get lists {renamed:?} ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation, &before.endpoint_id, &before.transport_addr), "the same birth");
    assert_eq!(estate.pid_of(&renamed).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let update = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).expect("the mesh-primary decided the node.name.update");
    assert_eq!(
        (s(&update["attributes"]["outcome"]), s(&update["attributes"]["traffic"]), s(&update["attributes"]["connections"]), s(&update["attributes"]["rejoin"]), (s(&update["attributes"]["from_path"]), s(&update["attributes"]["from_name"]))),
        ("name-updated".into(), "Stopped".into(), "stopped".into(), "at-start".into(), (path.path.clone(), path.name.clone())),
        "{update:#?}"
    );
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && at_location(sp, &renamed)).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the start's rejoin was installed");
    assert!(rdm_test::estate::names_location(&join, "admitted", &renamed), "{join:#?}");
    estate.record_trace_url(update["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-config-update.md, Deferred application and startup: "A stopped process remains alive
/// with its Node RPC endpoint bound"; a caller's update, step 2: "The node's property writer applies the
/// whole map to its latest owned state and issues one put"): `node.config.update` of the stopped
/// `mesh1.node.1` at its mesh-primary ends `node.config.updated`: the stopped process, still bound, is
/// sent `UpdateConfig` once and its own writer puts the map (the via-config span says
/// `apply=direct`, no `answered`; one `via-update-config` at the node). Then `node.start` makes the
/// same process rejoin (`via-join` admitted; its own record, not the join answer, holds the config),
/// and `node.get` lists the same birth `ready` carrying the config. What must NOT happen: `ConfigNotApplied` for a stopped node, or the started birth without
/// the config.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_nodes_config_update_is_applied_by_its_bound_process_and_its_start_carries_it() {
    let test = "a_stopped_nodes_config_update_is_applied_by_its_bound_process_and_its_start_carries_it";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = node_at();
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let before = mesh.iter().find(|n| n.location() == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary && rdm_core::is_node_admin(&n.r#type)).cloned().expect("node.get names mesh1's primary");
    let pid = estate.pid_of(&node_at()).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let held = view_of(&estate.nodes_obj().await, &path).await;
    assert!(held.state == NodeStatus::Stopped, "{held:?}");

    // The update: one call to the fabric-primary, here mesh1's mesh-primary too, and the stopped node,
    // still bound, applies it through its own writer.
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(held.node_id.clone()), node_id: held.node_id.clone(), incarnation: held.incarnation.clone().expect("the stopped node's birth") };
    let carrier = estate.carrier().await;
    let reply = rdm_client::NodeRpc::new(carrier.client())
        .config_update(&primary.node_id, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .expect("node.config.update answered");
    assert!(matches!(&reply, StatusReply::ConfigUpdated { node_id, incarnation, config, .. } if *node_id == before.node_id && Some(incarnation) == before.incarnation.as_ref() && *config == told), "a stopped node's config update completes once its writer put it: {reply:?}");

    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    // The config the node's record holds is what node.get answers (Q203: no digest carries it); read
    // until the started birth is ready, within a staleness floor.
    let until = std::time::Instant::now() + rdm_core::cadence::staleness_floor();
    let after = loop {
        let v = view_of(&estate.nodes_obj().await, &path).await;
        if (v.config == told && v.state == NodeStatus::Ready) || std::time::Instant::now() >= until {
            break v;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(after.state == NodeStatus::Ready, "node.get lists {path:?} ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation), (&before.node_id, &before.incarnation), "the same birth");
    assert_eq!(after.config, told, "the started node carries the config its join handed it");
    assert_eq!(estate.pid_of(&node_at()).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let update = named(&spans, "rdm.node_admin.node.update.via-config").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).expect("the mesh-primary served the node.config.update");
    let a = &update["attributes"];
    assert_eq!((s(&a["outcome"]), s(&a["apply"]), s(&a["answered"])), ("config-updated".into(), "direct".into(), String::new()), "the stopped node's own writer applied it: {update:#?}");
    let applied = named(&spans, "rdm.node_admin.node.update.via-update-config").into_iter().filter(|sp| sp["trace_id"] == update["trace_id"]).count();
    assert_eq!(applied, 1, "one UpdateConfig reached the stopped, still bound node");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && at_location(sp, &path)).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the start's rejoin was installed");
    assert!(rdm_test::estate::names_location(&join, "admitted", &path), "the start's rejoin is admitted at its path and name: {join:#?}");
    eprintln!("trace {}: via-config outcome=config-updated apply=direct; trace {}: via-join admitted at {path:?}", s(&update["trace_id"]), s(&join["trace_id"]));
    estate.record_trace_url(update["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-JN1; Q345: "the old node is stopped, and its name changed with node.name.update";
/// node-start.md step 2; R12/R13; CLAUDE.md read rule): in a mesh of three node-admins, the non-primary
/// `mesh1.node-admin.3` is stopped and then renamed `renamed` at its mesh-primary. Every admin lists the
/// stopped birth at its record's name, `mesh1.node-admin.2`, which performed no rename, included: its
/// view reads the name from the node's record on the shared store root. `node.start` of the new name makes the same
/// process rejoin: its join presents the name it took while stopped and is admitted under it (`via-join`
/// `admitted_as`); the node installs the answer's name before it publishes (`via-join-admitted`
/// `installed`, then `via-ready` under that name). The stream carries `node.joined`, `node.started`
/// and ends `Complete`; every admin lists the same birth `ready` at the new name, and the node holds it
/// publishing that name. What must NOT happen: an admin listing the stopped birth at
/// `mesh1.node-admin.3`, the start refused (`UnknownNode`, `JoinMismatch("name")`), or a span of the
/// started node after its join naming `mesh1.node-admin.3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admin_stopped_and_renamed_starts_under_the_name_it_holds_on_every_admin() {
    let test = "a_node_admin_stopped_and_renamed_starts_under_the_name_it_holds_on_every_admin";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 3, 1)]).await;
    estate.settled_views(&[("mesh1", 3, 1)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = rdm_core::PathName::new("mesh1.node-admin", "3").unwrap();
    let renamed: PathName = path.with_name("renamed").unwrap();

    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.location() == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary && rdm_core::is_node_admin(&n.r#type)).cloned().expect("node.get names mesh1's primary");
    assert_ne!(primary.location(), path, "the stopped admin is not mesh1's primary");
    let admins: Vec<NodeView> = mesh.iter().filter(|n| n.location() != path && n.r#type == "node-admin").cloned().collect();
    assert!(admins.iter().any(|n| n.location() != primary.location()), "an admin that performs no rename: {admins:?}");
    let rpc = mesh.iter().find(|n| n.r#type == "node").cloned().expect("the node");
    let pid = estate.pid_of(&path).await;

    // Its stop, then its name.
    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(before.node_id.clone()), node_id: before.node_id.clone(), incarnation: before.incarnation.clone().expect("the node's birth") };
    let reply = nodes.name_update(&birth, &primary.node_id, &renamed.name).await.expect("node.name.update answered");
    assert!(matches!(reply, StatusReply::NameUpdated { .. }), "{reply:?}");
    // Every admin's view names the stopped birth by its record, the one that performed no rename included.
    for admin in &admins {
        let listed = estate
            .until_view_at(&admin.location(), &format!("{:?} lists the stopped birth at {renamed:?}", admin.name), Duration::from_secs(10), |view| {
                let at = |p: &PathName| view.iter().find(|n| &n.location() == p).cloned();
                at(&renamed).filter(|n| n.node_id == before.node_id && at(&path).is_none())
            })
            .await;
        assert_eq!((listed.state, listed.incarnation.clone()), (NodeStatus::Stopped, before.incarnation.clone()), "{:?}: {listed:?}", admin.name);
    }

    // The start by its record's name: the same process rejoins under it.
    let started = frames(nodes.start(&renamed).await.expect("the start by the row's name was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    rdm_test::typed::peer_holds(&*peers, &rpc, "mesh1", "the rejoined birth is held as the same incarnation, publishing the row's name", |v| {
            v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation.as_ref().unwrap() && d.node.location() == renamed)
        })
        .await;
    for admin in &admins {
        estate
            .until_view_at(&admin.location(), &format!("{:?} lists the rejoined birth ready at {renamed:?}", admin.name), Duration::from_secs(10), |view| {
                view.iter().find(|n| n.location() == renamed && n.node_id == before.node_id && n.state == NodeStatus::Ready).map(|_| ())
            })
            .await;
    }
    assert_eq!(estate.pid_of(&renamed).await, pid, "the same process, listed at the row's name");

    estate.stop().await;
    let spans = estate.spans();
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && at_location(sp, &renamed)).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed" && sp["trace_id"] == sent["trace_id"]).expect("the start's rejoin was installed");
    assert!(at_location(&join, &renamed) && rdm_test::estate::names_location(&join, "admitted", &renamed), "the join presents the name the node took while stopped and is admitted under it: {join:#?}");
    let admitted = named(&spans, "rdm.mesh.node.update.via-join-admitted").into_iter().find(|sp| sp["attributes"]["node_id"] == before.node_id.as_str() && at_location(sp, &renamed)).expect("the started node installed the row's name its join answer handed");
    let ready: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|sp| sp["attributes"]["node_id"] == before.node_id.as_str() && sp["start_unix_nano"].as_u64() >= admitted["start_unix_nano"].as_u64()).collect();
    assert_eq!(ready.len(), 1, "the started node reports ready once, after it installed the row's name: {ready:?}");
    assert!(ready[0]["start_unix_nano"].as_u64() >= admitted["end_unix_nano"].as_u64(), "the name is installed before the node says anything: {admitted:#?} {ready:#?}");
    assert!(named(&spans, "rdm.node_admin.node.reject.via-join-mismatch").is_empty(), "no join was refused");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a renamed stopped node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-stop.md; build.md §1, Q342): `node.stop` is a node command of the primary above the
/// node, never a Build or part of one: no Build is written and none is reopened. The operation is
/// exactly `node.stop` keyed on the node.id: stop-node to the birth, whose reply is `left`, then
/// Complete. The runtime is not terminated and no NodeDeleted is published; no repair follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted() {
    let test = "a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    let before = estate.node_value(&node_at()).await;
    let pid = estate.pid_of(&node_at()).await;
    let builds = estate.build_ids().await;
    let stopped = frames(estate.nodes_obj().await.stop(&node_at()).await.unwrap_or_else(|e| panic!("the mesh-primary refused node.stop of {:?}: {e}", node_at()))).await;
    assert!(!stopped.iter().any(|f| matches!(f, Frame::Started { .. })), "a node command opens no Build: {stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    assert!(alive(pid), "the stopped process {pid} is stopped, alive");
    // No repair follows the stop: the birth stays, stopped, and no Build is written.
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert_eq!(estate.build_ids().await, builds, "a stop writes no Build");
    let n = estate.node_value(&node_at()).await;
    assert_eq!(n["incarnation"], before["incarnation"], "no new birth at {:?}: {n}", node_at());
    assert_eq!(n["state"], "stopped", "{n}");

    // Every process flushes its spans on exit.
    estate.stop().await;
    let spans = estate.spans();
    let none = rdm_core::LifecycleOp::NO_BUILD;
    let mut last = 0u64;
    for step in ["StopNode", "AwaitNodeLeft", "Complete"] {
        let sp = step_span(&spans, none, &node_at(), step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["DeleteConnections", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, none, &node_at(), step).is_none(), "a standalone stop runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a stop publishes no NodeDeleted");
    let command = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().find(|sp| sp["attributes"]["subject_id"] == before["node_id"] && s(&sp["attributes"]["command"]).contains("stop")).expect("the node command's span at the primary above the node");
    assert_eq!(s(&command["attributes"]["outcome"]), "node-stopped", "{command}");
    estate.record_trace_url(command["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i146 Q29, Q36; node-stop.md step 5): `node.stop` of `mesh1.node.1` runs and the birth stops;
/// the stop stream's `stopped` reaches the mesh-primary and is held there before it is recorded (the
/// faulted admin's `stopped` event cut). The stopped birth's declaration of Stopped, naming the stop's
/// operation, reaches the mesh-primary meanwhile: it is applied, and the mesh-primary publishes
/// NodeStopped from it with the stop's identity, so `node.get` lists the birth stopped while the stream's
/// `stopped` is still held. Released, the stream's `stopped` finds the birth's row held (already
/// applied) and the stop completes. Must NOT happen: NodeStopped twice, NodeStopped from the held
/// stream, a node-leaving frame sent or heard by anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_whose_stream_stopped_is_held_is_published_stopped_once_from_the_births_declaration() {
    let test = "a_stop_whose_stream_stopped_is_held_is_published_stopped_once_from_the_births_declaration";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let fp = views.iter().find(|n| n.is_fabric_primary).map(|n| n.location()).expect("a fabric-primary");
    let path: PathName = node_at();
    let before = view_of(&estate.nodes_obj().await, &path).await;

    let faults = FaultClient::at(&estate, &fp).await;
    faults.arm("stopped-held", serde_json::json!({"kind": "event", "event": "stopped", "node": before.node_id})).await;
    let stop = tokio::spawn({
        let nodes = estate.nodes_obj().await;
        let path = path.clone();
        async move { frames(nodes.stop(&path).await.expect("the stop was accepted")).await }
    });
    faults.wait_held("stopped-held").await;
    // The stream's stopped is held at the mesh-primary: the birth is listed stopped from its own
    // declaration alone.
    let until = std::time::Instant::now() + Duration::from_secs(30);
    let stopped = loop {
        let v = view_of(&estate.nodes_obj().await, &path).await;
        if v.state == NodeStatus::Stopped {
            break v;
        }
        assert!(std::time::Instant::now() < until, "the stopped birth's declaration lists it stopped within 30 s while the stream's stopped is held: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!((stopped.state, &stopped.node_id, &stopped.incarnation), (NodeStatus::Stopped, &before.node_id, &before.incarnation), "the same birth, stopped on its row from its own declaration");
    assert_eq!(faults.cut("stopped-held").await["held"], true, "the stream's stopped is still held: {}", faults.cut("stopped-held").await);
    faults.release("stopped-held").await;
    let stopped = stop.await.expect("the stop's call");
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");

    estate.stop().await;
    let spans = estate.spans();
    let operation = "node.stop".to_string();
    let declared = named(&spans, "rdm.node_rpc.status.update.via-declare-own").into_iter().find(|sp| attr(sp, "node") == before.node_id.as_str() && attr(sp, "state") == "Stopped" && attr(sp, "operation") == operation && attr(sp, "outcome") == "applied").expect("the stopped birth's declaration of Stopped naming its stop was applied");
    let node_stopped: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| at_location(sp, &node_at())).collect();
    assert_eq!(node_stopped.len(), 1, "NodeStopped is published once for the birth: {node_stopped:#?}");
    assert_eq!(attr(node_stopped[0], "operation"), operation, "NodeStopped names the stop: {}", node_stopped[0]);
    // The birth's own span of its declaration opens when its call returns, after the mesh-primary
    // applied it; the declaration is NodeStopped's source because the stream's stopped was held until
    // after NodeStopped, and was recorded as already applied.
    let _ = declared;
    let received: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-stop-received").into_iter().filter(|sp| at_location(sp, &node_at())).collect();
    assert_eq!(received.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(), vec!["already-applied".to_string()], "the released stream's stopped found the row held: {received:#?}");
    assert!(start_ns(node_stopped[0]) < start_ns(received[0]), "NodeStopped was published before the held stream's stopped was recorded");
    for kind in ["node-leaving", "node-left"] {
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-lifecycle-frame").into_iter().filter(|sp| attr(sp, "kind") == kind && names_location(sp, "subject", &node_at())).collect();
        assert!(heard.is_empty(), "no {kind} frame is published or heard: {heard:#?}");
    }
    let trace = node_stopped[0]["trace_id"].as_str().unwrap_or("").to_string();
    println!("NodeStopped from the declaration: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// CONTRACT (`docs/architecture/node-identity-and-state.md:570` — "Setting: `stopped_node_ttl`
/// (`RDM_STOPPED_NODE_TTL`), read by the fabric-primary only" and
/// `docs/architecture/node-identity-and-state.md:575` — "Action: One ordinary node.delete Build per
/// selected node, one at a time"; node-identity-and-state.md §6.9, R-GC1; node-stop.md step 6): on a fabric whose
/// `RDM_STOPPED_NODE_TTL` is 3 s, `mesh1.node.1` is stopped and left alone. The fabric-primary's sweep
/// ages it from its stop receipt and submits one ordinary node.delete Build
/// (`rdm.node_admin.node.delete.via-stopped-ttl` with the node, the receipt's time, the ttl and the
/// Build); the delete runs as a delete does: the provider's exited verdict of the stopped process
/// (TerminateRuntime complete), NodeDeleted, and `node.get` lists no `mesh1.node.1`. The other stopped
/// period never started: nothing else is deleted. Must NOT happen: a delete before one ttl, a second
/// delete Build, a collection the sweep made itself without a Build, or a role transfer, connections delete or stop
/// sent again to the stopped birth.
// @feature: stopped-node-collection
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_past_the_ttl_is_deleted_by_the_fabric_primarys_sweep() {
    let test = "a_node_stopped_past_the_ttl_is_deleted_by_the_fabric_primarys_sweep";
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[("RDM_STOPPED_NODE_TTL", "3s")]).await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = node_at();
    peers.learn(&estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]"));
    let pid = estate.pid_of(&node_at()).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let stopped_at = std::time::Instant::now();
    // The sweep collects it: the delete's exited verdict ends the stopped process and node.get lists it no more.
    estate.until_node(&node_at(), "the stopped node is deleted and its process exited", Duration::from_secs(60), |n| (n.is_none() && !alive(pid)).then_some(())).await;
    assert!(stopped_at.elapsed() >= Duration::from_secs(3), "collected after one ttl, never before: {:?}", stopped_at.elapsed());

    estate.stop().await;
    let spans = estate.spans();
    let collected: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-stopped-ttl").into_iter().filter(|sp| sp["attributes"]["build_id"].is_string()).collect();
    assert_eq!(collected.len(), 1, "one node.delete Build: {collected:?}");
    let c = &collected[0]["attributes"];
    assert!(at_location(collected[0], &node_at()) && c["ttl"].to_string().trim_matches('"') == "3000", "{c}");
    let build = s(&c["build_id"]);
    let exit = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| sp["attributes"]["step"] == "TerminateRuntime" && at_location(sp, &node_at()) && sp["attributes"]["outcome"] == "complete" && s(&sp["attributes"]["build_id"]) == build);
    assert!(exit.is_some(), "the delete Build proved the stopped process exited");
    // The stopped birth's stop already ran: its delete sends no role transfer, node.connection.delete or stop again
    // (lifecycles/node-delete.md 1, node-stop.md 6).
    let again: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["build_id"]) == build && ["DeleteConnections", "StopNode", "AwaitNodeLeft"].iter().any(|st| sp["attributes"]["step"] == *st))
        .collect();
    assert!(again.is_empty(), "the delete of a stopped birth composes no stop again: {again:?}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| at_location(sp, &node_at())).collect();
    assert!(!deleted.is_empty(), "NodeDeleted is published for the stopped birth");
    estate.record_trace_url(collected[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-stop.md step 6: "A stopped node leaves the stopped status only through node.start or
/// node.delete"; states.md "Stopped nodes": "stopped is not live and is still present"; a completed
/// operation is never planned again): in mesh2, a peer mesh of the fabric-primary mesh1.node-admin.1,
/// `node.stop` of mesh2.node.1, then `node.delete` of another node of mesh2 (the Build that removes
/// its path). The delete Build plans that path's delete and at most once the stopped path's create, whose
/// check reaches the stopped birth, holds it and receipts the create `Complete`; no plan names it
/// again and the plans are bounded; the delete's operation completes and its path is listed no more;
/// the stopped birth is still listed stopped, the same birth and the same live process. The Build then
/// ends Complete: the stopped birth holds neither its mesh's ready nor the fabric's checklist
/// (states.md:16, Q240), so the fabric's change reaches ready (flame b7f9ecec2fec32f356d022f98bdfb374
/// is this cell's shape before Q240: Blocked on the change naming the stopped birth `not-ready`).
///
/// Must NOT happen (#3233, 141's estate): every plan of the delete Build naming `node.create` at
/// the stopped path, its check holding the stopped birth and the next plan naming the create again,
/// the Build never ending (1198 plans in 240 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_deleting_another_path_after_a_stop_plans_the_stopped_paths_create_at_most_once() {
    let test = "a_build_deleting_another_path_after_a_stop_plans_the_stopped_paths_create_at_most_once";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 1, 3)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 1, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.nodes_obj().await;
    let stopped_path = Estate::path("mesh2.node", "1");
    let pid = estate.pid_of(&stopped_path).await;

    let stopped = frames(nodes.stop(&stopped_path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let before = view_of(&estate.nodes_obj().await, &stopped_path).await;
    assert!(before.state == NodeStatus::Stopped, "node.get lists {stopped_path:?} stopped: {before:?}");

    // The other path: a node of mesh2 that is not its node-type primary, so its delete moves no primary.
    let mesh2 = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.expect("node.get [mesh]");
    let other = mesh2
        .iter()
        .filter(|n| n.r#type == "node" && n.location() != stopped_path && !n.is_primary)
        .map(|n| (n.location(), n.node_id.clone()))
        .max()
        .unwrap_or_else(|| panic!("mesh2 lists another node that is not its primary: {mesh2:?}"));
    let (other, other_id) = other;
    let mut stream = nodes.delete(vec![other_id.clone()]).await.expect("the delete was accepted");
    let build_id = stream.accepted().expect("the call opened a Build").build_id.to_string();
    // Read until the Build ends, bounded and without a panic: a Build that does not end is asserted below
    // from its spans, which name the operations every plan named.
    let mut deleted = Vec::new();
    let deadline = tokio::time::Instant::now() + SETTLE;
    while let Ok(Some(f)) = tokio::time::timeout_at(deadline, stream.next()).await {
        let f = f.unwrap_or_else(|e| panic!("the delete's reply stream broke: {e}"));
        let end = f.is_terminal();
        deleted.push(f);
        if end {
            break;
        }
    }
    let settled = matches!(deleted.last(), Some(Frame::Complete));
    if settled {
        estate.until_node(&other, "the deleted path is listed no more", SETTLE, |n| n.is_none().then_some(())).await;
        let held = view_of(&estate.nodes_obj().await, &stopped_path).await;
        assert!(held.state == NodeStatus::Stopped, "{stopped_path:?} is still stopped after the delete of {other:?}: {held:?}");
        assert_eq!((&held.node_id, &held.incarnation), (&before.node_id, &before.incarnation), "the stopped birth is the same birth");
    }
    assert!(alive(pid), "the stopped process {pid} is alive");
    assert_eq!(estate.pid_of(&stopped_path).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|sp| attr(sp, "build_id") == build_id).collect();
    // A Delete creates nothing: its record names no node.id to create (D58), so no pass plans a create.
    let deletes = OperationSubject::Node(other_id.clone());
    let planned_create: Vec<String> = reconciles
        .iter()
        .filter(|sp| rdm_test::estate::planned_operations(sp).iter().any(|op| op.contains("action: \"node.create\"")))
        .map(|sp| format!("section run at {}: {} ({})", attr(sp, "executor"), attr(sp, "operations"), attr(sp, "outcome")))
        .collect();
    assert!(planned_create.len() <= 1, "the stopped {stopped_path:?}'s create is planned at most once in the delete Build {build_id}, its check holding the birth; {} of {} section runs planned it: {:#?}", planned_create.len(), reconciles.len(), &planned_create[..planned_create.len().min(5)]);
    let held: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-stopped-held").into_iter().filter(|sp| at_location(sp, &stopped_path) && attr(sp, "build_id") == build_id).collect();
    assert_eq!(held.len(), planned_create.len(), "every planned create of the stopped {stopped_path:?} is held by its check and receipted Complete: {held:#?}");
    assert!(reconciles.len() <= 3, "the delete Build {build_id} runs a bounded number of sections, never a loop: {}", reconciles.len());
    let ops: Vec<String> = reconciles.iter().flat_map(|sp| planned_operations(sp)).collect();
    assert!(reconciles.iter().any(|sp| plans_operation(sp, "node.delete", &deletes)), "a section run of the delete Build {build_id} plans the delete of {other:?}: {ops:?}");
    let delete_op = (r#"action: "node.delete""#, format!("subject: {:?}", Some(&deletes)));
    assert!(ops.iter().all(|op| op.contains(delete_op.0) && op.contains(&delete_op.1)), "the delete Build {build_id} plans the delete of {other:?} and nothing else: {ops:?}");
    let completed: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "build_id") == build_id && at_location(sp, &other) && attr(sp, "step") == "Complete" && attr(sp, "outcome") == "complete").collect();
    assert!(!completed.is_empty(), "the delete of {other:?} completes in Build {build_id}");
    assert!(settled, "the delete Build {build_id} of {other:?} ends Complete within {SETTLE:?}, the stopped {stopped_path:?} holding nothing: {deleted:?}");
    estate.record_trace_url(reconciles[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (states.md:16, Q240; "Resize and a new executable"): a stopped birth holds neither its
/// mesh's ready nor the fabric's checklist. mesh1 {1 node-admin, 1 node} and mesh2 {1 node-admin,
/// 2 nodes}, ready; `node.stop` of mesh2's node that is not its node-type primary completes;
/// then a resize grows mesh2 to 3 nodes. The resize Build ends Complete, the fabric-primary's change
/// enters ready under that Build, and the stopped birth is untouched: still stopped, the same birth
/// and the same live process. Its later `node.start` runs its gates as before the ruling: joined, started,
/// Complete, and the same birth is ready again.
///
/// Must NOT happen (flame b7f9ecec2fec32f356d022f98bdfb374): the resize Build held Blocked on the
/// fabric's change naming the stopped birth `not-ready` until it starts or is deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resize_after_a_stop_reaches_ready_with_the_stopped_birth_at_rest_and_its_start_runs_its_gates() {
    let test = "a_resize_after_a_stop_reaches_ready_with_the_stopped_birth_at_rest_and_its_start_runs_its_gates";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 1, 2)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let fabric_primary = estate.fabric_view().await.fabric_primary.expect("the ready fabric names its fabric-primary");
    let nodes = estate.nodes_obj().await;
    let mesh2 = nodes.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.expect("node.get [mesh]");
    // The node that is not mesh2's `node` type primary, so its stop moves no primary.
    let stopped_path = mesh2
        .iter()
        .find(|n| n.r#type == "node" && !n.is_primary)
        .map(|n| n.location())
        .unwrap_or_else(|| panic!("mesh2 lists a node that is not its primary: {mesh2:?}"));
    let stopped_name = stopped_path.clone();
    let pid = estate.pid_of(&stopped_name).await;

    let stopped = frames(nodes.stop(&stopped_path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    estate.until_stopped_at_the_fabric_primary(&stopped_path).await;
    let before = view_of(&estate.nodes_obj().await, &stopped_path).await;
    assert!(before.state == NodeStatus::Stopped, "node.get lists {stopped_name:?} stopped: {before:?}");

    // The resize: mesh2 grows by one node while the stopped birth rests.
    let mut stream = estate.submit_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 1, 3)]).await;
    let build_id = stream.build_id().to_string();
    let mut built = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while let Ok(Some(f)) = tokio::time::timeout_at(deadline, stream.next()).await {
        let f = f.unwrap_or_else(|e| panic!("the resize's Build stream broke: {e}"));
        let end = matches!(f, rdm_client::BuildFrame::Complete { .. } | rdm_client::BuildFrame::Failed { .. });
        built.push(f);
        if end {
            break;
        }
    }
    assert!(matches!(built.last(), Some(rdm_client::BuildFrame::Complete { .. })), "the resize Build {build_id} ends Complete with {stopped_name:?} stopped, never Blocked on the fabric's change: {built:?}");
    let fabric_primary = estate.node_values().await.iter().find(|n| n["node_id"] == fabric_primary.as_str()).map(rdm_test::estate::location).expect("the fabric-primary's row");
    estate.until_fabric_ready_at(&fabric_primary, SETTLE).await;
    let grown = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.expect("node.get [mesh]");
    let rpc: Vec<&NodeView> = grown.iter().filter(|n| n.r#type == "node").collect();
    assert_eq!(rpc.len(), 3, "mesh2 holds three nodes: {grown:?}");
    assert!(rpc.iter().filter(|n| n.location() != stopped_path).all(|n| n.state == NodeStatus::Ready), "every node of mesh2 that is not stopped is ready: {grown:?}");
    let held = view_of(&estate.nodes_obj().await, &stopped_path).await;
    assert!(held.state == NodeStatus::Stopped, "{stopped_name:?} is still stopped after the resize: {held:?}");
    assert_eq!((&held.node_id, &held.incarnation), (&before.node_id, &before.incarnation), "the stopped birth is the same birth");
    assert!(alive(pid), "the stopped process {pid} is alive");
    assert_eq!(estate.pid_of(&stopped_name).await, pid, "the same process");

    // Its later start runs its gates.
    let started = frames(estate.nodes_obj().await.start(&stopped_path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    // The answering admin learns mesh2's rows by gossip: its view takes the start a moment after the stream's end.
    let after = estate.until_node(&stopped_name, "node.get lists the started birth ready again", Duration::from_secs(60), |n| n.filter(|n| n.state == NodeStatus::Ready).cloned()).await;
    assert_eq!((&after.node_id, &after.incarnation), (&before.node_id, &before.incarnation), "a start keeps the birth");
    assert_eq!(estate.pid_of(&stopped_name).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let changes: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-change").into_iter().filter(|sp| attr(sp, "build_id") == build_id).collect();
    let entered = named(&spans, "rdm.node_admin.fabric.update.via-state-entered")
        .into_iter()
        .find(|sp| attr(sp, "to") == "ready" && changes.iter().any(|c| c["trace_id"] == sp["trace_id"]))
        .unwrap_or_else(|| panic!("the fabric entered ready under the resize Build {build_id}'s change: {changes:#?}"));
    let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().filter(|sp| attr(sp, "node") == before.node_id.as_str() && attr(sp, "outcome") == "started").count();
    assert_eq!(served, 1, "{stopped_name:?}'s start ran once after the resize");
    estate.record_trace_url(entered["trace_id"].as_str().unwrap_or(""));
}

/// A node's own span names it by its node id (`node`, or `node_id` where `node` is its name).
fn own_span(sp: &Value, node_id: &str) -> bool {
    attr(sp, "node") == node_id || attr(sp, "node_id") == node_id
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

fn unix_nanos() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("after the Unix epoch").as_nanos() as u64
}

/// mesh1 {1 node-admin, 1 node} and mesh2 {`admins` node-admins, 1 node}, settled and
/// ready, through the typed objects: the fabric-primary is mesh1.node-admin.1.
async fn two_meshes(test: &str, admins: u32) -> (Estate, Vec<NodeView>) {
    settled_two_meshes(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, admins).await
}

async fn settled_two_meshes(estate: Estate, admins: u32) -> (Estate, Vec<NodeView>) {
    estate.resize_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
    // A role move is refused until the initial fabric has first reached ready (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    (estate, views)
}

/// CONTRACT (lifecycles/node-primary-update.md, "Numbered end-to-end sequence", the `node-admin` type):
/// `node.stop` of mesh2's primary P runs node.primary.update first, and the stop continues on the
/// successor NA2's direct answer.
/// 1. The fabric-primary commands P (`primary.update.via-command` at the fabric-primary, primary P,
///    outcome `updated`).
/// 2.-3. P's election over its own view with itself excluded names NA2 (`primary.update.via-update-primary`
///    at P, successor NA2, outcome `updated`, under the command), and P finishes the writes it has
///    issued (`primary.update.via-issued-writes-finished`) before it calls NA2.
/// 4. P calls node.primary.update on NA2's exact birth; NA2 installs the election's result the call
///    carries and answers node.primary.updated (`primary.update.via-primary-updated` at NA2, outcome
///    `held`, the mesh-primary entry P -> NA2, under P's update).
/// 5. NA2 records `is_primary` on its own record (Q356; "The write records the role; it does not grant
///    it").
/// 6. The fabric-primary installs NA2's answer and hands the caller's node command on to NA2, which runs
///    it as mesh2's primary, a node command and never a Build (Q342); every admin still answering names
///    NA2 mesh2's primary (`node.get`).
/// Must NOT happen: a refusal of node.primary.update, a 0x21 call to P the fabric-primary did not make
/// or one to NA2 P did not make, a stop step before NA2 answered, or a view still naming P.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_hands_over_by_node_primary_update_before_it_stops() {
    let test = "a_stop_of_a_mesh_primary_hands_over_by_node_primary_update_before_it_stops";
    let (mut estate, before) = two_meshes(test, 2).await;
    let fp = before.iter().find(|n| n.is_fabric_primary).map(|n| n.location()).expect("a fabric-primary");
    let p = before.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).cloned().expect("mesh2's primary");
    let na2 = before.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && !n.is_primary).cloned().expect("mesh2's other node-admin");
    let (p_id, na2_id) = (p.node_id.to_string(), na2.node_id.to_string());
    assert_ne!(fp, p.location(), "mesh2's primary is not the fabric-primary");

    let nodes = estate.nodes_obj().await;
    let stopped = frames(nodes.stop(&p.location()).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    // Every admin of the fabric still answering names NA2 mesh2's primary, the fabric-primary included.
    for admin in before.iter().filter(|n| n.r#type == "node-admin" && n.location() != p.location()) {
        let view = estate.node_views_at(&admin.location()).await.unwrap_or_else(|| panic!("{:?} answers node.get", admin.name));
        let primary: Vec<PathName> = view.iter().filter(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).map(|n| n.location()).collect();
        assert_eq!(primary, vec![na2.location()], "{:?} names {:?} mesh2's primary once the stop is complete", admin.location(), na2.location());
    }
    // Step 5: NA2 records `is_primary` on its own record (Q356).
    let recorded = || -> Option<u64> { estate.node_record(&na2_id).and_then(|r| r["data"]["is_primary"].as_u64()) };
    let deadline = std::time::Instant::now() + SETTLE;
    while recorded().is_none() {
        assert!(std::time::Instant::now() < deadline, "NA2 recorded is_primary on its own record in nodes.jsonl within {SETTLE:?}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    estate.stop().await;
    let spans = estate.spans();
    let one = |name: &str, f: &dyn Fn(&Value) -> bool| -> Value {
        let found: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| f(sp)).collect();
        assert_eq!(found.len(), 1, "one {name}: {found:#?}");
        found[0].clone()
    };
    let cmd = one("rdm.node_admin.primary.update.via-command", &|sp| attr(sp, "primary_node_id") == p_id);
    assert!(at_location(&cmd, &fp), "the fabric-primary commands P: {cmd:#?}");
    assert_eq!((attr(&cmd, "type"), attr(&cmd, "to"), attr(&cmd, "outcome")), ("node-admin".to_string(), p_id.clone(), "updated".to_string()), "{cmd:#?}");
    let upd = one("rdm.node_admin.primary.update.via-update-primary", &|sp| at_location(sp, &p.location()));
    assert_eq!((attr(&upd, "moving"), attr(&upd, "successor"), attr(&upd, "outcome")), ("mesh-primary".to_string(), na2_id.clone(), "updated".to_string()), "{upd:#?}");
    assert!(descends_from(&spans, &upd, &cmd), "P's update runs under the fabric-primary's command: {upd:#?}");
    let finished = one("rdm.node_admin.primary.update.via-issued-writes-finished", &|sp| at_location(sp, &p.location()));
    let installed_at_na2 = one("rdm.node_admin.primary.update.via-primary-updated", &|sp| at_location(sp, &na2.location()) && attr(sp, "outcome") == "held");
    let entry = attr(&installed_at_na2, "primaries");
    assert!(entry.starts_with("node-admin ") && entry.contains(&p_id) && entry.contains(&format!("-> {na2_id}/")), "NA2 installed the mesh-primary entry P -> NA2 and answered node.primary.updated: {installed_at_na2:#?}");
    assert!(descends_from(&spans, &installed_at_na2, &upd), "NA2 installs under P's node.primary.update call: {installed_at_na2:#?}");
    assert!(named(&spans, "rdm.node_admin.primary.reject.via-primary-update").is_empty(), "no node.primary.update was refused");
    // The order of the sequence (steps 3, 4, 6).
    assert!(end_ns(&finished) <= start_ns(&installed_at_na2), "P finished its issued writes before NA2 installed");
    // The stop ran after NA2 answered, run by NA2 as mesh2's primary: the node command handed on to it.
    let stop_run = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().find(|sp| at_location(sp, &na2.location()) && attr(sp, "subject_id") == p_id).unwrap_or_else(|| panic!("NA2 ran the stop"));
    assert!(end_ns(&cmd) <= start_ns(stop_run), "the fabric-primary installed NA2's answer before it handed the stop on");
    // The stop belongs to no Build: its steps run under no build_id.
    let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| at_location(sp, &p.location()) && attr(sp, "build_id") == rdm_core::LifecycleOp::NO_BUILD.to_string()).collect();
    assert!(steps.iter().any(|sp| attr(sp, "step") == "StopNode"), "the stop's steps are read: {steps:#?}");
    assert!(steps.iter().all(|sp| start_ns(sp) >= end_ns(&installed_at_na2)), "no stop step ran before NA2 answered: {steps:#?}");
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

/// CONTRACT (node-primary-update.md, Operation contract, "The commander": "The fabric-primary, when its
/// `node.primary.update` of a mesh's mesh-primary is answered `node.primary.updated { node_id }`,
/// changes its atlas: it sets `is_primary` on `node_id` and removes it from the node it sent
/// `node.primary.update` to; it reads a node's mesh-primary from its atlas (Q355)"): `node.stop` of
/// mesh2's mesh-primary P moves its role to NA2 first; the moment the stop completes, with no wait for
/// gossip to reach the fabric-primary, `node.start` of P is sent where the fabric-primary's `node.get`
/// names P's mesh-primary, NA2, and NA2 runs it (`node.update.via-node-command` at NA2, `start-node`,
/// `node-started`). P rejoins as the same birth, Ready. Must NOT happen: the fabric-primary running P's
/// start itself from a view that still names P mesh2's mesh-primary (rdm#320: NA2 then refused P's
/// Ready, `RejectedInvalidNodeTransition { current: Stopped }`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_of_a_mesh_primary_stopped_just_now_is_run_by_the_new_mesh_primary() {
    let test = "a_start_of_a_mesh_primary_stopped_just_now_is_run_by_the_new_mesh_primary";
    let (mut estate, before) = two_meshes(test, 2).await;
    let fp = before.iter().find(|n| n.is_fabric_primary).map(|n| n.location()).expect("a fabric-primary");
    let p = before.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).cloned().expect("mesh2's mesh-primary");
    let na2 = before.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && !n.is_primary).cloned().expect("mesh2's other node-admin");
    assert_ne!(fp, p.location(), "mesh2's mesh-primary is not the fabric-primary");
    let p_id = p.node_id.to_string();

    let nodes = estate.nodes_obj().await;
    let stopped = frames(nodes.stop(&p.location()).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    // At once: no wait for gossip to carry NA2's is_primary to the fabric-primary.
    let started = frames(nodes.start(&p.location()).await.expect("the start was accepted")).await;
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    let after = estate
        .until_view_at(&na2.location(), "P ready again at its mesh-primary", SETTLE, |view| view.iter().find(|n| n.node_id == p.node_id && n.state == NodeStatus::Ready).cloned())
        .await;
    assert_eq!(after.incarnation, p.incarnation, "P is the same birth after its stop and start: {after:?}");

    estate.stop().await;
    let spans = estate.spans();
    let starts: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().filter(|sp| attr(sp, "command") == "start-node" && attr(sp, "subject_id") == p_id).collect();
    assert!(starts.iter().all(|sp| !at_location(sp, &fp)), "the fabric-primary never runs the start of a node whose mesh-primary is NA2: {starts:#?}");
    let run: Vec<&Value> = starts.iter().copied().filter(|sp| at_location(sp, &na2.location())).collect();
    assert_eq!(run.len(), 1, "NA2, P's mesh-primary after the update, ran P's start once: {starts:#?}");
    assert_eq!(attr(run[0], "outcome"), "node-started", "{:#?}", run[0]);
    let cmd = named(&spans, "rdm.node_admin.primary.update.via-command").into_iter().find(|sp| attr(sp, "primary_node_id") == p_id && attr(sp, "outcome") == "updated").unwrap_or_else(|| panic!("the fabric-primary commanded P's node.primary.update"));
    assert!(at_location(cmd, &fp), "the fabric-primary commanded P's update: {cmd:#?}");
    assert!(end_ns(cmd) <= start_ns(run[0]), "NA2 ran the start after the update was answered");
    estate.record_trace_url(run[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (the approved proposal Part C1 "The no-successor cases", Q7 ruled Luke 2026-10-12:
/// "`node.primary.update` commanded for a node type with no other node of that type in the mesh is refused
/// by name, `cannot transfer: no secondary of type <type> in mesh <mesh>`"; node-primary-update.md step 2:
/// "The writer pre-checks from its own view ... If it names none, the step is refused by name and nothing
/// has changed, no row"; D64): mesh2 has one node-admin and a node. The fabric-primary runs `node.stop`
/// of mesh2.node-admin.1 and commands its node.primary.update first, deciding nothing from its projection;
/// mesh2's primary pre-checks over its own view and refuses it by name
/// (`rdm.node_admin.primary.reject.via-primary-update`, `cannot transfer: no secondary of type node-admin in
/// mesh mesh2`), and the stop is refused `primary-not-updated` naming that text before the stop is sent.
/// No successor installs itself, no stop is recorded, and every node keeps its birth and its primaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name() {
    let test = "a_stop_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name";
    let (mut estate, before) = two_meshes(test, 1).await;
    let nodes = estate.nodes_obj().await;
    let target: PathName = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
    let text = "cannot transfer: no secondary of type node-admin in mesh mesh2";
    let refused = nodes.stop(&target).await.err().expect("the stop is refused");
    let rdm_client::CallEnd::Rejected { reason, detail } = &refused else { panic!("the stop ends Rejected: {refused:?}") };
    assert!(reason == "primary-not-updated" && detail.contains(text), "the stop is refused naming mesh2's primary's own refusal: {refused:?}");
    for mesh in ["mesh1", "mesh2"] {
        for now in estate.mesh_node_views(mesh).await {
            let was = before.iter().find(|n| n.location() == now.location()).unwrap_or_else(|| panic!("{:?} was there before", now.name));
            assert_eq!((&now.incarnation, now.is_primary, now.is_fabric_primary), (&was.incarnation, was.is_primary, was.is_fabric_primary), "{:?} is untouched", now.name);
        }
    }
    estate.stop().await;
    let spans = estate.spans();
    let rejected: Vec<&Value> = named(&spans, "rdm.node_admin.primary.reject.via-primary-update").into_iter().filter(|sp| attr(sp, "detail") == text).collect();
    assert_eq!(rejected.len(), 1, "mesh2's primary refused the update by name once, over its own view: {rejected:#?}");
    assert!(named(&spans, "rdm.node_admin.build.reject.via-last-admin").is_empty(), "the fabric-primary decided nothing for mesh2's primary from its projection (D64)");
    // The first primaries the creates commanded installed themselves (Q407); no successor of mesh2's
    // primary did.
    let successors: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "primaries").starts_with("node-admin ") || attr(sp, "primaries").starts_with("fabric ")).collect();
    assert!(successors.is_empty(), "no successor installed itself: {successors:#?}");
    assert!(named(&spans, "rdm.node_admin.node.update.via-node-stopped").is_empty(), "no stop was recorded");
    estate.record_trace_url(rejected[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-SB1, Luke 2026-10-13: "a node.stop or node.restart of a mesh's only admin is refused
/// by name under Q7, no secondary, before anything moves. The stop path's via-role-hand-over to=none
/// is the defect: a refused transfer never erases the incumbent's role and never yields to nobody, and
/// the destructive stop does not continue"; the approved proposal Part C1 "The no-successor cases",
/// Q7: "`node.fabric-primary.update` with no other node-admin Ready in the mesh and no other
/// mesh-primary Ready is refused the same way, naming both searches ... a single-admin
/// single-mesh fabric's primary ... cannot be stopped or restarted until a secondary is Ready";
/// Part C2 step 3's text): mesh1.node-admin.1 is the fabric's only node-admin, holding mesh1's role and the
/// fabric-primary role, beside one node. Its `node.stop` and its `node.restart` are each refused
/// `last-admin` with exactly `cannot transfer fabric-primary: no other node-admin Ready in mesh
/// mesh1, no other mesh-primary Ready`, one `rdm.node_admin.build.reject.via-last-admin` per
/// call. Every node keeps its birth and its primaries (node.get). The stop is not run: no stop-node
/// served, no node.connection.delete, no node.primary.update commanded or run. Through the whole run,
/// the estate's teardown included, no admin hands its primaries to nobody
/// (`rdm.node_admin.build.update.via-role-hand-over` with `to = none`).
/// Must NOT happen: a stop step of the only admin, a node.primary.update, a hand-over to none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_the_fabrics_only_node_admin_is_refused_by_name_and_nothing_moves() {
    let test = "a_stop_of_the_fabrics_only_node_admin_is_refused_by_name_and_nothing_moves";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 1, 1)]).await;
    let before = estate.settled_views(&[("mesh1", 1, 1)]).await;
    // A role move is refused until the initial fabric has first reached ready (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.nodes_obj().await;
    let target: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
    let held = before.iter().find(|n| n.location() == target).expect("node.get lists the only admin");
    assert!(held.is_primary && held.is_fabric_primary, "mesh1.node-admin.1 holds both roles: {held:?}");
    let q7 = "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready";
    for (action, refused) in [("stop", nodes.stop(&target).await.err())] {
        let refused = refused.unwrap_or_else(|| panic!("the {action} of the fabric's only node-admin was accepted"));
        let rdm_client::CallEnd::Rejected { reason, detail } = &refused else { panic!("the {action} ends Rejected: {refused:?}") };
        assert_eq!((reason.as_str(), detail.strip_prefix("last-admin: ")), ("last-admin", Some(q7)), "{action}: {refused:?}");
    }
    for now in estate.mesh_node_views("mesh1").await {
        let was = before.iter().find(|n| n.location() == now.location()).unwrap_or_else(|| panic!("{:?} was there before", now.name));
        assert_eq!((&now.incarnation, now.is_primary, now.is_fabric_primary), (&was.incarnation, was.is_primary, was.is_fabric_primary), "{:?} is untouched", now.name);
    }
    estate.stop().await;
    let spans = estate.spans();
    let rejected: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-last-admin").into_iter().filter(|sp| attr(sp, "detail") == q7).collect();
    assert_eq!(rejected.len(), 1, "one refusal per call: {rejected:#?}");
    for name in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.connection.delete.via-stop", "rdm.node_admin.primary.update.via-command", "rdm.node_admin.primary.update.via-update-primary"] {
        // The estate's teardown runs the stop workflow on its signal (`signal`): not the refused stop.
        let ran: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| attr(sp, "operation") != "signal").collect();
        assert!(ran.is_empty(), "the refused stop ran {name}: {ran:#?}");
    }
    let to_none: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-role-hand-over").into_iter().filter(|sp| attr(sp, "to") == "none").collect();
    assert!(to_none.is_empty(), "an admin handed its role to nobody: {to_none:#?}");
    estate.record_trace_url(rejected[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i146 Q37; node-status.md "Status report"; node-stop.md steps 4-5; node-primary-update.md
/// steps 5-8; the role taker's RoleFold, R14): a stopped birth's stop is published NodeStopped once,
/// by the mesh primary that applied its Stopped, and a move of its mesh's role publishes it no second
/// time. In a fabric of two meshes, mesh2's node that is not its node-type primary is stopped: it stops, declares its Stopped naming the
/// stop, and mesh2's primary publishes NodeStopped. Then mesh2's primary is stopped: its Build moves
/// mesh2's role to mesh2's other admin first, which folds the mesh's states from its view (the stopped
/// birth's Stopped held from its NodeStopped; no record read, Q260, Q356), and the old primary's own stop
/// declares its Stopped naming that stop to the new primary, which applies it and publishes the old
/// primary's NodeStopped once. The stopped birth hears no role move (its gossip was cut at its stop,
/// node-stop.md step 3): it re-declares nothing to the new primary, whose fold already holds it stopped. The new primary starts the stopped birth, which rejoins
/// as the same birth. Must NOT happen: NodeStopped for the stopped birth twice (once per primary), or
/// the old primary's stop declared as a state it measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_primary() {
    let test = "a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_primary";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary == primary).map(|n| n.location()).expect("mesh2 has two admins");
    let (primary, successor) = (admin_of(true), admin_of(false));
    assert!(!views.iter().any(|n| n.location() == primary && n.is_fabric_primary), "mesh2's primary is not the fabric primary: the entry admin stays put");
    // The node that is not its node type's primary: its stop moves no role of its own.
    let stopped_birth = views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node" && !n.is_primary).cloned().expect("mesh2 has a node that is not its node type's primary");
    let stopped_node = stopped_birth.location();
    let nodes = estate.nodes_obj().await;

    let stopped = frames(nodes.stop(&stopped_node).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let moved = frames(nodes.stop(&primary).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(moved.last(), Some(&Frame::Complete), "{moved:?}");
    // The fabric-primary, the stop's commander, recorded the update's answer in its atlas: the start
    // goes to mesh2's new mesh-primary with no wait for gossip (node-primary-update.md "The commander", Q355).
    let started = frames(nodes.start(&stopped_node).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    // Start completes at the mesh-primary's ready apply. This entry admin is in another mesh:
    // wait for its projection, as the other stop/start cells wait for each admin's held row.
    let entry = estate.entry().await;
    let after = estate
        .until_view_at(&entry, "the entry holds the rejoined birth ready", Duration::from_secs(10), |view| {
            view.iter().find(|n| n.location() == stopped_node && n.state == NodeStatus::Ready).cloned()
        })
        .await;
    assert_eq!((&after.node_id, &after.incarnation, after.state), (&stopped_birth.node_id, &stopped_birth.incarnation, NodeStatus::Ready), "the same birth rejoined: {after:?}");

    estate.stop().await;
    let spans = estate.spans();
    let id_at = |at: &PathName| views.iter().find(|n| &n.location() == at).map(|n| n.node_id.to_string()).expect("a settled view");
    let (stopped_s, primary_s, successor_s) = (id_at(&stopped_node), id_at(&primary), id_at(&successor));
    let stopped_of = |node: &PathName| -> Vec<&Value> { named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| at_location(sp, node)).collect() };
    let stopped_published = stopped_of(&stopped_node);
    assert_eq!(stopped_published.len(), 1, "NodeStopped for the stopped birth is published once across the role move: {stopped_published:#?}");
    let installed: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| at_location(sp, &successor)).collect();
    assert!(!installed.is_empty(), "{successor_s} installed itself as mesh2's primary");
    assert!(start_ns(stopped_published[0]) < start_ns(installed[0]), "the stopped birth's NodeStopped came from the primary before the move");
    let folded: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-successor-fold").into_iter().filter(|sp| at_location(sp, &successor) && names_location(sp, "member", &stopped_node)).collect();
    // The stopped birth's Stopped naming its stop, declared to the new primary (the estate's teardown
    // owes every node's own Stopped too, naming no operation).
    let stopped_operation = "node.stop".to_string();
    let redeclared: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        .filter(|sp| attr(sp, "node") == stopped_s && attr(sp, "state") == "Stopped" && attr(sp, "operation") == stopped_operation && attr(sp, "to") == successor_s)
        .collect();
    assert!(redeclared.is_empty(), "a stopped birth hears no role move, so it re-declares nothing to the new primary: {redeclared:#?}");
    // The new primary folds what its caches hold and reads no storage (Q227, Q260); gossip is eventual and
    // unordered (Q90), so its fold may run before or after the old primary's NodeStopped reaches it. That it
    // holds the birth stopped is proved by the start above: the start's gate admits only a stopped birth
    // (build.md §4.4), and the same birth rejoined.
    let _ = &folded;
    // The old primary's own stop: its Stopped names the stop, applied by the new primary, published once.
    let primary_stop = "node.stop".to_string();
    let primary_declared: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declare")
        .into_iter()
        .filter(|sp| at_location(sp, &primary) && attr(sp, "key").ends_with(", Stopped)") && attr(sp, "operation") == primary_stop)
        .collect();
    // The declaration rides an Owed call (i146 e3 owner 3): the new primary takes it applied, or already
    // applied when the stop stream's `stopped` reached it first (Q36: "from whichever arrives first").
    assert!(
        primary_declared.iter().any(|sp| attr(sp, "to_node_id") == successor_s && matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied")),
        "{primary_s}'s stop declared its Stopped naming the stop to {successor_s}: {primary_declared:#?}"
    );
    let primary_stopped = stopped_of(&primary);
    assert_eq!(primary_stopped.len(), 1, "NodeStopped for the old primary is published once: {primary_stopped:#?}");
    println!(
        "role move: the new primary folded {} row(s) of the stopped birth ({:?}); flame {}",
        folded.len(),
        folded.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(),
        stopped_published[0]["trace_id"].as_str().unwrap_or("")
    );
    estate.record_trace_url(primary_stopped[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i146 Q39; node-status.md "Status report"; node-primary-update.md steps 5-8): a node's
/// declaration is owed to the primary of its mesh's primary role, and the role moving is its one
/// retargeting. In a fabric of two meshes, mesh2's non-primary node is restarted while mesh2's
/// primary holds the restart's node.started unanswered at its `StatusService` (the fault service's
/// declaration cut): the restart's wait for the node's readiness fails naming the node.started its
/// primary never took, the node live and its declaration owed. Then mesh2's primary is stopped,
/// which moves mesh2's role to mesh2's other admin first. The node hears the move: its call to the
/// old primary is cancelled (`via-primary-changed`) and it declares to the new primary, which applies it
/// once; the old primary decides nothing of it (its cut holds the one call it took). The create's failed
/// WaitForNodeReady ends mesh2's section and the Build ends incomplete; what ran stands, the birth
/// among it, never deleted or created again (Q344, Q346). No drift pass repairs or removes anything
/// after it (Q342). Must NOT happen:
/// the node.started applied twice, applied by the old primary, or addressed to an admin that does not
/// hold the role; a second declaration call to the old primary (a declaration loop of the node's own,
/// which Q52 deleted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_owed_across_a_move_of_its_mesh_primary_is_delivered_once_to_the_new_mesh_primary() {
    let test = "a_declaration_owed_across_a_move_of_its_mesh_primary_is_delivered_once_to_the_new_mesh_primary";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary == primary).map(|n| n.location()).expect("mesh2 has two admins");
    let (primary, successor) = (admin_of(true), admin_of(false));
    assert!(!views.iter().any(|n| n.location() == primary && n.is_fabric_primary), "mesh2's primary is not the fabric primary: the entry admin stays put");
    let node: PathName = rdm_core::PathName::new("mesh2.node", "2").unwrap();
    let id_at = |at: &PathName| views.iter().find(|n| &n.location() == at).map(|n| n.node_id.to_string()).expect("a settled view");
    let (primary_s, successor_s) = (id_at(&primary), id_at(&successor));

    let nodes = estate.nodes_obj().await;

    // The old primary holds the node's node.started at its `StatusService`: the declaration stays owed.
    let faults = FaultClient::at(&estate, &primary).await;
    faults.arm("declaration", serde_json::json!({"kind": "declaration", "node": node, "state": "Ready"})).await;
    let request = estate.create_node_request("mesh2", "node").await;
    let restarted = frames(nodes.create(vec![request]).await.expect("the create was accepted")).await;
    assert!(
        matches!(restarted.last(), Some(Frame::Failed { reason, .. }) if reason.contains("WaitForNodeReady") && reason.contains("it took no node.started")),
        "the first start's wait for readiness fails naming the node.started its primary never took: {restarted:?}"
    );
    let hit = faults.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node"], serde_json::to_value(&node).unwrap(), "the old primary holds the node's declaration: {hit}");

    // The role moves: the old primary's stop runs node.primary.update first.
    let moved = frames(nodes.stop(&primary).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(moved.last(), Some(&Frame::Complete), "{moved:?}");
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let v = view_of(&estate.nodes_obj().await, &node).await;
        if v.state == NodeStatus::Ready {
            break;
        }
        assert!(std::time::Instant::now() < until, "the new primary sets the node ready from its declaration within 30 s: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    // The one birth at the node's path.name, by its row: the create's, which stands (Q346).
    let nodes_file = estate.fabric_store_root().join(rdm_admin::store_root::NODES_FILE);
    let births: std::collections::BTreeSet<String> = rdm_admin::store_log::read_file(&nodes_file).unwrap_or_default().into_iter().filter(|l| l.kind() == rdm_node::nodes_storage::RECORD_KIND && rdm_test::estate::is_location(&l.data, &node)).map(|l| l.subject().to_string()).collect();
    assert_eq!(births.len(), 1, "the create's one birth at {node:?}, never deleted or created again: {births:?}");
    let ready: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        // A first start names no operation; its measured Ready remains owed without an open command stream.
        .filter(|sp| births.contains(&attr(sp, "node")) && attr(sp, "state") == "Ready" && attr(sp, "operation").is_empty())
        .collect();
    let applied: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "outcome") == "applied").collect();
    assert_eq!(applied.len(), 1, "the node.started is applied once: {ready:#?}");
    assert_eq!(attr(applied[0], "to"), successor_s, "by the new primary: {:#?}", applied[0]);
    assert!(ready.iter().all(|sp| attr(sp, "to") == primary_s || attr(sp, "to") == successor_s), "addressed only to the role's primaries: {ready:#?}");
    // Before the role moved, the declaration went to its primary alone: never to an admin that did not
    // yet hold the role.
    let installed = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| at_location(sp, &successor)).map(start_ns).min().expect("the successor installed itself");
    let early: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "to") == successor_s && start_ns(sp) < installed).collect();
    assert!(early.is_empty(), "no declaration to {successor_s} before it held the primary role: {early:#?}");
    let cancelled: Vec<&Value> = named(&spans, "rdm.node_rpc.status.remove.via-primary-changed").into_iter().filter(|sp| births.contains(&attr(sp, "node"))).collect();
    // The declaration's calls to the old primary across the hold: each cancelled when the role moved
    // while it was in flight, or ended unanswered once its attempts were spent (Q311), the
    // declaration then owed until the node's queue sends it again. None was applied by the old primary.
    let to_old: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "to") == primary_s).collect();
    assert!(cancelled.len() + to_old.len() >= 1, "the declaration was sent to the old primary across the hold: cancelled {cancelled:#?}, ended {to_old:#?}");
    assert!(cancelled.len() <= 1, "at most the one call in flight at the move is cancelled: {cancelled:#?}");
    assert!(to_old.iter().all(|sp| attr(sp, "outcome").starts_with("Indeterminate")), "the old primary held the declaration unanswered, each call spent: {to_old:#?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "no drift pass runs (Q342)");
    println!(
        "declaration across a role move: applied by {} (route {}); flame http://192.168.68.99:16687/trace/{}",
        attr(applied[0], "to"),
        attr(applied[0], "route"),
        s(&applied[0]["trace_id"]),
    );
    estate.record_trace_url(applied[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-stop.md step 4: "declare `Stopped` to the mesh-primary naming the stop's operation,
/// an owed message (Node RPC §43) sent whether or not the stop call's stream is still open"; states.md
/// "node stopped": set by the node, on its own record (Q356); node-stop.md "Entry": a birth that is not
/// live is refused `node-not-live` by name, nothing written): mesh2's node is stopped while mesh2's
/// primary holds the node's `Stopped` declaration unanswered at its `StatusService` (the fault service's
/// declaration cut) and holds the stop stream's `stopped` unrecorded (its event cut), and that primary is
/// then SIGKILLed: the role moves to mesh2's other admin, and the stopped node, which left its mesh's
/// channel at its transport cut, hears neither the death nor the move. The node wrote stopped on its own
/// record before it sent the frame, so the same birth reads stopped with no answer from the dead
/// primary; the new mesh-primary holds it stopped from that record: a node.stop sent to it again is
/// refused `node-not-live`, and a node.start (a node command, never a Build, Q342) is admitted and the
/// same birth reads ready. Must NOT happen: the birth held in Stopping on its owed declaration (release
/// run 5 at 7e13f37e, rshape-authority recovery: AwaitNodeLeft "stop-node was not admitted",
/// DEADLINE_EXCEEDED), a stop sent again admitted, the start refused, a Build written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_whose_mesh_primary_dies_holding_its_stopped_declaration_reads_stopped_from_the_nodes_own_record_and_its_new_mesh_primary_starts_it() {
    let test = "a_stop_whose_mesh_primary_dies_holding_its_stopped_declaration_reads_stopped_from_the_nodes_own_record_and_its_new_mesh_primary_starts_it";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary = views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).map(|n| n.location()).expect("mesh2's primary");
    assert!(!views.iter().any(|n| n.location() == primary && n.is_fabric_primary), "mesh2's primary is not the fabric-primary: the entry admin stays put");
    let successor = views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.location() != primary).map(|n| n.location()).expect("mesh2's other admin");
    // The node of mesh2 that holds no role: a primary's stop moves its role first.
    let node: PathName = views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node" && !n.is_primary).map(|n| n.location()).expect("mesh2 has a node that holds no primary role");
    let node_s = node.clone();
    let nodes = estate.nodes_obj().await;
    let before = view_of(&estate.nodes_obj().await, &node).await;
    let node_id = before.node_id.to_string();

    // mesh2's primary holds the node's Stopped declaration unanswered and the stop stream's stopped unrecorded.
    let faults = FaultClient::at(&estate, &primary).await;
    faults.arm("declaration", serde_json::json!({"kind": "declaration", "node_id": before.node_id, "state": "Stopped"})).await;
    faults.arm("stopped-held", serde_json::json!({"kind": "event", "event": "stopped", "node": before.node_id})).await;
    let builds = estate.build_ids().await;
    let stop = nodes.stop(&node).await.unwrap_or_else(|e| panic!("the stop of {node_s:?} was accepted: {e:?}"));
    drop(stop);
    let hit = faults.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node_id"], before.node_id.as_str(), "mesh2's primary holds the node's Stopped declaration: {hit}");
    // The primary dies holding it: the role moves to mesh2's other admin.
    estate.kill_node(&primary).await;

    // The same birth reads stopped: the node's own line, with nothing from the dead primary.
    let stopped = view_of(&estate.nodes_obj().await, &node).await;
    assert_eq!((stopped.state, &stopped.node_id, &stopped.incarnation), (NodeStatus::Stopped, &before.node_id, &before.incarnation), "the same birth reads stopped from its own record: {stopped:?}");
    // The caller sends its node.stop again until mesh2's new mesh-primary answers it: refused
    // `node-not-live`, the birth stopped on its record. Never admitted.
    let until = std::time::Instant::now() + SETTLE;
    loop {
        match estate.nodes_obj().await.stop(&node).await {
            Err(rdm_client::CallEnd::Rejected { reason, .. }) if reason == "node-not-live" => break,
            Ok(_) => panic!("the stop of {node_s:?} sent again to its new mesh-primary was admitted: the birth is stopped"),
            Err(other) => assert!(std::time::Instant::now() < until, "mesh2's new mesh-primary refuses the stop of {node_s:?} sent again `node-not-live` within {SETTLE:?}: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // The new mesh-primary starts it: the same birth, ready.
    let started = frames(estate.nodes_obj().await.start(&node).await.unwrap_or_else(|e| panic!("the start of {node_s:?} at mesh2's new mesh-primary was admitted: {e:?}"))).await;
    assert_eq!(started.last(), Some(&Frame::Complete), "the start of {node_s:?} completes: {started:?}");
    // The answering admin held the stopped birth silent (PendingReconnect, observer-inferred); a live
    // birth the view holds silent is answered as the view holds it, cleared the moment it is heard
    // (node-identity-and-state.md §7.3, `Atlas::read`): read until its first digest after the start is heard.
    let until = std::time::Instant::now() + SETTLE;
    let ready = loop {
        let ready = view_of(&estate.nodes_obj().await, &node).await;
        if ready.state == NodeStatus::Ready || std::time::Instant::now() >= until {
            break ready;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!((ready.state, &ready.node_id, &ready.incarnation), (NodeStatus::Ready, &before.node_id, &before.incarnation), "the same birth, ready once it is heard: {ready:?}");
    assert_eq!(estate.build_ids().await, builds, "the stop and the start write no Build");

    estate.stop().await;
    let spans = estate.spans();
    let unadmitted: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| at_location(sp, &node) && attr(sp, "step") == "AwaitNodeLeft" && attr(sp, "code") == "DEADLINE_EXCEEDED").collect();
    assert!(unadmitted.is_empty(), "the birth held its stop in Stopping on its owed declaration: {unadmitted:#?}");
    // The birth owed its Stopped declaration naming its stop, once, and did not wait on it.
    let operation = "node.stop".to_string();
    let owed: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-owed-on-stop").into_iter().filter(|sp| attr(sp, "node") == node_id && attr(sp, "operation") == operation).collect();
    assert_eq!(owed.len(), 1, "the birth owed its Stopped declaration once, naming its stop, and did not wait on it: {owed:#?}");
    // The new mesh-primary refused the stop sent again from the node's stopped record, nothing sent to the node.
    let refused: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().filter(|sp| at_location(sp, &successor) && attr(sp, "subject_id") == node_id && attr(sp, "command") == "stop-node" && attr(sp, "outcome") == "rejected").collect();
    assert!(refused.iter().any(|sp| attr(sp, "detail").contains("a stop needs a live node")), "mesh2's new mesh-primary refused the stop sent again: the birth is stopped: {refused:#?}");
    estate.record_trace_url(&s(&owed[0]["trace_id"]));
}

/// CONTRACT (node-stop.md steps 2-4; node-mesh-leave.md; i146 Q55: "a signal stop is node.stop's own
/// steps"): a node-admin that is not its mesh's primary is sent SIGTERM. It runs node.stop's steps as
/// its own subject, as a node does: the whole `node.connection.delete { All }` under
/// `RDM_CONNECTIONS_DELETE_DEADLINE_MS` (`via-stop`, operation `signal`), its connections-deleting
/// owed to its mesh-primary; `node.mesh.leave` (`via-leaving`), its own mesh and the backbone each
/// left (`via-left` own and backbone), its Node RPC pool cut (`via-stop-transport-cut`); its `Stopped`
/// owed to its mesh-primary (`via-owed-on-signal`); then its process ends, before the estate's bound.
/// The mesh-primary admin, sent SIGTERM at the estate's stop, gossips nothing for its stop either
/// (node-stop.md step 4, R-ON1, Q95): no `via-leaving-aggregate`. Must NOT happen: a runtime the bound
/// kills, a heartbeat of the stopping admin after it left its mesh, a leave with no connections delete
/// before it, or a stopping mesh topology from a signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admins_signal_runs_node_stops_steps_and_owes_its_stopped() {
    let test = "a_node_admins_signal_runs_node_stops_steps_and_owes_its_stopped";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary_view = views.iter().find(|n| n.mesh() == "mesh1" && n.r#type == "node-admin" && n.is_primary).cloned().expect("mesh1's primary");
    let member_view = views.iter().find(|n| n.mesh() == "mesh1" && n.r#type == "node-admin" && !n.is_primary).cloned().expect("a node-admin that is not mesh1's primary");
    let (primary, member, primary_id, member_id) = (primary_view.location(), member_view.location(), primary_view.node_id.to_string(), member_view.node_id.to_string());
    let pid = estate.pid_of(&member).await;
    assert!(alive(pid));

    // The signal: the admin's own stop, then its process ends.
    assert!(std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status().unwrap().success(), "kill -TERM {pid} ({member:?})");
    wait_for(&format!("{member:?} ends by its own stop"), Duration::from_secs(60), || async { (!alive(pid)).then_some(()) }).await;
    let ended = unix_nanos();
    let killed = estate.stop().await;
    assert_eq!(killed, Vec::<u32>::new(), "every runtime ended by its own stop before the bound's SIGKILL");

    let spans = estate.spans();
    // The signalled admin's spans are its birth's, before its process ended: a recovery at the same
    // name is another birth.
    let of = |name: &str, node: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| own_span(sp, node) && start_ns(sp) <= ended).collect() };
    let deleted = of("rdm.node_admin.connection.delete.via-stop", &member_id);
    assert_eq!(deleted.len(), 1, "{member:?}'s signal ran node.connection.delete {{ All }} once as the stop's: {deleted:#?}");
    assert_eq!(attr(deleted[0], "operation"), "signal", "{:#?}", deleted[0]);
    let owed = of("rdm.node_rpc.status.update.via-owed-on-signal", &member_id);
    let owed_state = |state: &str| owed.iter().find(|sp| attr(sp, "state") == state).copied();
    let deleting = owed_state("ConnectionsDeleting").unwrap_or_else(|| panic!("{member:?} owed its connections-deleting to its mesh-primary: {owed:#?}"));
    let stopped = owed_state("Stopped").unwrap_or_else(|| panic!("{member:?} owed its stopped state to its mesh-primary: {owed:#?}"));
    let leaving = of("rdm.node.mesh.update.via-leaving", &member_id);
    assert_eq!(leaving.len(), 1, "{member:?} left its mesh once: {leaving:#?}");
    assert_eq!(attr(leaving[0], "operation"), "signal", "{member:?}'s leave is its own stop's: {leaving:#?}");
    let left = of("rdm.node.mesh.update.via-left", &member_id);
    for which in ["own", "backbone"] {
        assert!(left.iter().any(|sp| attr(sp, "which") == which), "{member:?} left its {which} channel: {left:#?}");
    }
    let cut = of("rdm.node_admin.node.update.via-stop-transport-cut", &member_id);
    assert_eq!(cut.len(), 1, "{member:?}'s stop cut its transport once: {cut:#?}");
    assert_eq!(attr(cut[0], "operation"), "signal", "{:#?}", cut[0]);
    assert!(end_ns(deleted[0]) <= start_ns(leaving[0]), "the connections are deleted before the leave (node-mesh-leave.md step 1)");
    assert!(start_ns(deleting) <= start_ns(leaving[0]) && start_ns(leaving[0]) <= start_ns(stopped), "{member:?}: connections-deleting owed, then the leave, then stopped owed");
    let last_left = left.iter().map(|sp| start_ns(sp)).max().unwrap_or(0);
    let published_after: Vec<&Value> = of("rdm.mesh.node.update.via-heartbeat", &member_id).into_iter().filter(|h| start_ns(h) > last_left).collect();
    assert!(published_after.is_empty(), "{member:?} published nothing after it left its mesh: {published_after:#?}");
    // The mesh-primary's signal, at the estate's stop, gossips nothing for the stop (Q95): the one
    // final mesh topology is R-NS1 item 2's, a deleted mesh's last admin's on its node.delete.
    let aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| attr(sp, "node") == primary_id || attr(sp, "node") == member_id).collect();
    assert!(aggregate.is_empty(), "a signal stop publishes no mesh topology (Q95): {aggregate:#?}");
    estate.artifact("admin-signal-stop.json", &serde_json::json!({"member": member, "connections_deleting_owed": deleting["trace_id"], "left": leaving[0]["trace_id"], "stopped_owed": stopped["trace_id"], "primary": primary}));
    estate.record_trace_url(leaving[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (fabric-node-hooks.md "Outcomes and the gate", Q350: "A start that ends `Failed` leaves the
/// node `Pending`, and nothing retries until someone acts"; node-start.md: a start that ends Failed at its
/// hook leaves the node rejoined and `Pending`, its process running and never stopped again): a stopped
/// node's commanded start fails at its `pre_node_ready` hook. The start is answered `StartFailed` at
/// `node.pre-node-ready`, and the node's probe then answers `StartFailed` with the hook's reason: it is
/// rejoined and Pending, not stopped. Must NOT happen: the node stopped again (a probe answering
/// `CurrentStopped`), a stop step (`via-stop`, `via-leaving`, `via-stop-transport-cut`, `Stopped` owed)
/// naming the start's operation, or the start answered Started. In its own process: the cell holds
/// process-wide state (the stop flag).
#[test]
fn a_commanded_start_whose_hook_fails_leaves_the_node_pending_and_never_stops_it_again() {
    let cell = "a_commanded_start_whose_hook_fails_leaves_the_node_pending_and_never_stops_it_again";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        use crate::rejoin_commander::{call, rig_pre_node_ready, spans_of, Rig};
        use rdm_admin::node_hooks::HookOutcome;
        use rdm_core::status::StatusRequest;
        use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
        const NODE_START: &str = "node.start";
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let Rig { primary: _primary, commander, _carried, running: _running, launch, key, dir, target, node_id, incarnation, .. } = rig_pre_node_ready("start-failed-pending", false, Some(pre_node_ready.clone())).await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k2v7m1q9t4xe".into(), operation: rdm_core::OperationId::node("node.stop", &node_id) };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k2v7m1q9t4xe".into(), operation: rdm_core::OperationId::node(NODE_START, &node_id) };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        assert!(matches!(call(&commander, &target, &stop).await, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped");
        commander.deployed_as(&launch, &key, launch.location(), Some(launch.location()));
        // The start joins the commander back over its call; its hook then fails the start.
        pre_node_ready.set_mode(Mode::Fixed(HookOutcome::Failed { reason: "the rows of orders cannot be read".into() }));
        let reply = call(&commander, &target, &start).await;
        assert!(matches!(&reply, StatusReply::StartFailed { step, .. } if step == "node.pre-node-ready"), "the start is answered Failed at its hook: {reply:?}");
        // The failed start left the node rejoined and Pending: its probe answers StartFailed, never CurrentStopped.
        let probed = call(&commander, &target, &probe).await;
        assert!(matches!(&probed, StatusReply::StartFailed { step, reason } if step == "node.pre-node-ready" && reason.contains("the rows of orders cannot be read")), "the node stays Pending, its probe answering StartFailed with the hook's reason: {probed:?}");

        // The flame: the start served once, failed at its hook, and no stop step naming the start.
        let of = |name: &str| -> Vec<std::collections::BTreeMap<String, String>> { spans_of(&spans, name, node_id.as_str()).into_iter().filter(|f| f.get("operation").map(String::as_str) == Some(NODE_START)).collect() };
        let served = spans_of(&spans, "rdm.node_admin.status.update.via-start-node", node_id.as_str());
        assert!(served.iter().any(|f| f.get("outcome").map(String::as_str) == Some("start-failed") && f.get("failed_step").map(String::as_str) == Some("node.pre-node-ready")), "the start is served once, failed at node.pre-node-ready: {served:#?}");
        for name in ["rdm.node_admin.connection.delete.via-stop", "rdm.node.mesh.update.via-leaving", "rdm.node_admin.node.update.via-stop-transport-cut", "rdm.node_rpc.status.update.via-owed-on-stop"] {
            assert!(of(name).is_empty(), "the failed start ran no stop step: {name} named the start: {:#?}", of(name));
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// A real estate whose mesh-primary (the Day-0 admin, `mesh1.node-admin.1`) is held (SIGSTOP, alive and silent) when
/// `mesh1.node.1` receives SIGTERM, so the two declarations the stop owes its mesh-primary
/// (`ConnectionsDeleting`, then `Stopped`) are owed and unanswered at the signal. `release_after` is how
/// long the mesh-primary stays held before it is continued (`None`: held until the estate ends). Returns the
/// stopped node's `via-stop-declarations` span and the estate's spans.
async fn signal_stop_with_primary_held(test: &str, bound_ms: &str, release_after: Option<Duration>) -> (Value, Vec<Value>, String) {
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[("RDM_CONNECTIONS_DELETE_DEADLINE_MS", bound_ms)]).await;
    estate.resize_shape("fabric1", &[("mesh1", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1)]).await;
    let node_id = views.iter().find(|n| n.location() == node_at()).map(|n| n.node_id.to_string()).expect("the node is in the settled views");
    estate.until_fabric_ready(SETTLE).await;
    let primary_pid = estate.bootstrap_pid().expect("the Day-0 admin is mesh1's primary and the estate holds its pid");
    let pid = estate.pid_of(&node_at()).await;
    assert!(alive(pid));
    let send = |sig: &str, pid: u64| assert!(std::process::Command::new("kill").args([sig, &pid.to_string()]).status().unwrap().success(), "kill {sig} {pid}");
    send("-STOP", primary_pid as u64);
    // Held only once every task of the admin reads `T`: kill(2) returns before the group stop completes,
    // and an admin task still running then answers the node's first declaration (rdm#149).
    wait_for("the Day-0 admin held: every task stopped", Duration::from_secs(10), || async { rdm_test::process_faults::held(primary_pid).then_some(()) }).await;
    let signalled = std::time::Instant::now();
    send("-TERM", pid);
    let ended = |pid: u64| std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |st| st.rsplit(')').next().is_some_and(|rest| rest.trim_start().starts_with('Z')));
    if let Some(after) = release_after {
        tokio::time::sleep(after).await;
        assert!(!ended(pid), "the node's stop is still waiting on its owed declarations {after:?} after the signal: the mesh-primary has not answered");
        send("-CONT", primary_pid as u64);
    }
    // The held mesh-primary is the node's parent and cannot reap it: an ended process stays a zombie (`Z`).
    wait_for(&format!("{:?} ends by its own stop", node_at()), Duration::from_secs(60), || async { ended(pid).then_some(()) }).await;
    let ended_in = signalled.elapsed();
    send("-KILL", primary_pid as u64);
    estate.stop().await;
    let spans = estate.spans();
    let outcome = named(&spans, "rdm.node_admin.node.update.via-stop-declarations").into_iter().find(|sp| rdm_test::estate::of_node(sp, &node_at(), &node_id)).cloned().unwrap_or_else(|| panic!("{:?}'s signal stop ended on no outcome span", node_at()));
    estate.record_trace_url(outcome["trace_id"].as_str().unwrap_or(""));
    let trace = s(&outcome["trace_id"]);
    println!("{:?} ended {ended_in:?} after its signal; outcome span: {outcome}", node_at());
    (outcome, spans, trace)
}

/// CONTRACT (Q210, node-rpc.md section 43: "a signal stop waits for the queue within Q206's bound and its
/// outcome span"; node-connections-delete.md): a node holds owed declarations to its mesh-primary (its
/// mesh-primary is held, answering nothing) when it receives SIGTERM. Its stop waits for them: its process is
/// still alive after the signal and for as long as the mesh-primary stays silent, inside
/// `RDM_CONNECTIONS_DELETE_DEADLINE_MS`. The mesh-primary continued inside the declaration call's attempts
/// (Q311: three, at 0, c and 3c; a mesh-primary silent past them spends the call), the queue empties in the
/// order owed and the stop ends on its outcome span `rdm.node_admin.node.update.via-stop-declarations`,
/// outcome `ended`, naming what was delivered (`ConnectionsDeleting,Stopped`) and leaving nothing. Must NOT
/// happen: the process ending before its declarations are answered, or an outcome span that names neither
/// what was delivered nor what was left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stop_waits_for_its_owed_declarations_delivered_within_the_bound_and_names_them() {
    // Held for two publish cadences: inside the call's three attempts (0, c, 3c), its connection alive.
    let held = rdm_core::cadence::gossip_interval() * 2;
    let (outcome, _, _) = signal_stop_with_primary_held("a_signal_stop_waits_for_its_owed_declarations_delivered_within_the_bound_and_names_them", "30000", Some(held)).await;
    assert_eq!(attr(&outcome, "outcome"), "ended", "the queue was delivered inside the bound: {outcome}");
    assert_eq!(attr(&outcome, "delivered"), "ConnectionsDeleting,Stopped", "the outcome span names what was delivered, in the order owed: {outcome}");
    assert_eq!(attr(&outcome, "left"), "", "nothing was left: {outcome}");
    let elapsed: u64 = attr(&outcome, "elapsed_ms").parse().unwrap_or(0);
    assert!(elapsed >= held.as_millis() as u64 * 3 / 4, "the stop waited for the held mesh-primary's answers ({elapsed} ms): {outcome}");
}

/// CONTRACT (Q210, Q206, Q311): the same node, its mesh-primary held and never continued within the bound
/// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS` 3000). The stop waits the bound, or until the declaration's call
/// ends unanswered with its attempts spent, whichever is first, and no longer, then the process leaves; its
/// outcome span says `deadline` or `unanswered`, delivered nothing, and names the undelivered declarations
/// (`ConnectionsDeleting,Stopped`) it left. Must NOT happen: the process waiting past the bound, a spent
/// declaration named delivered, or leaving without naming what it left undelivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stop_past_the_bound_leaves_naming_its_undelivered_declarations() {
    let (outcome, _, _) = signal_stop_with_primary_held("a_signal_stop_past_the_bound_leaves_naming_its_undelivered_declarations", "3000", None).await;
    assert!(["deadline", "unanswered"].contains(&attr(&outcome, "outcome").as_str()), "the bound passed, or the call was spent, with the mesh-primary silent: {outcome}");
    assert_eq!(attr(&outcome, "delivered"), "", "nothing was delivered: {outcome}");
    assert_eq!(attr(&outcome, "left"), "ConnectionsDeleting,Stopped", "the outcome span names the undelivered declarations in the order owed: {outcome}");
    let elapsed: u64 = attr(&outcome, "elapsed_ms").parse().unwrap_or(0);
    assert!((1_500..8_000).contains(&elapsed), "the stop waited for the call's attempts or the bound, and left ({elapsed} ms): {outcome}");
}

/// CONTRACT (rdm#118; telemetry.md §1 "Exit": the exit flush is bounded, i146 Q60): an estate's
/// processes write their span evidence on tmpfs under the estate's root, never on the disk the
/// estate's artifacts are on, so the gate's disk load holds no write of a bounded exit flush; the
/// estate keeps the evidence in its artifacts when it ends. The fabric's only node-admin is stopped by
/// its signal: the span its stop closes last (`rdm.mesh.node.delete.via-signal`) is in its evidence file
/// once it has exited, and the kept copy is that file whole once the estate ends.
/// Must NOT happen: the evidence directory on a disk filesystem or under the artifacts, the admin given
/// another, or the last span missing from either copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stops_last_span_is_in_the_evidence_on_tmpfs_and_kept_when_the_estate_ends() {
    let test = "a_signal_stops_last_span_is_in_the_evidence_on_tmpfs_and_kept_when_the_estate_ends";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let evidence = estate.evidence.clone();
    let fs = {
        let path = std::ffi::CString::new(evidence.display().to_string()).unwrap();
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::statfs(path.as_ptr(), &mut st) }, 0, "statfs {}", evidence.display());
        st.f_type
    };
    assert_eq!(fs, libc::TMPFS_MAGIC, "the estate's evidence {} is on tmpfs (TMPDIR={:?})", evidence.display(), std::env::var("TMPDIR"));
    assert!(!evidence.starts_with(rdm_test::estate::artifacts_root()), "the evidence {} is not written under the artifacts", evidence.display());
    let pid = estate.bootstrap_pid().expect("the bootstrap admin runs");
    let env = std::fs::read(format!("/proc/{pid}/environ")).unwrap();
    let given = env.split(|b| *b == 0).find_map(|v| v.strip_prefix(b"RDM_EVIDENCE_DIR=")).map(|v| String::from_utf8_lossy(v).into_owned());
    assert_eq!(given.as_deref(), Some(evidence.display().to_string().as_str()), "the bootstrap admin writes its evidence there");

    assert_eq!(estate.stop().await, Vec::<u32>::new(), "the admin ended by its own stop");
    let file = std::fs::read_dir(&evidence).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).find(|n| n.starts_with(&format!("rdm-node-admin.{pid}-")) && n.ends_with(".spans.jsonl")).expect("the admin's evidence file");
    let written = std::fs::read_to_string(evidence.join(&file)).unwrap();
    let last = written.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).filter(|sp| sp["name"] == "rdm.mesh.node.delete.via-signal").last();
    let last = last.unwrap_or_else(|| panic!("the signal stop's last span is in {file}"));
    let kept = estate.kept_evidence();
    estate.record_trace_url(last["trace_id"].as_str().unwrap_or(""));
    drop(estate);
    assert!(!evidence.exists(), "the estate's root is removed when it ends");
    assert_eq!(std::fs::read_to_string(kept.join(&file)).ok().as_deref(), Some(written.as_str()), "the kept {file} is the evidence file whole");
}

/// The signal stop of `node` its evidence names, by the closed spans of its leave: its `via-leave-tasks`
/// and `via-leave-shutdown` (each naming the node) and the `rdm.mesh.node.delete.via-signal` span both are
/// children of, which closes as the process ends; `None` until all three are written.
fn signal_stop_of<'a>(spans: &'a [Value], node_id: &str) -> Option<(&'a Value, &'a Value, &'a Value)> {
    let shutdown = named(spans, "rdm.mesh.node.update.via-leave-shutdown").into_iter().find(|sp| attr(sp, "node") == node_id)?;
    let tasks = named(spans, "rdm.mesh.node.update.via-leave-tasks").into_iter().find(|sp| attr(sp, "node") == node_id && sp["parent_span_id"] == shutdown["parent_span_id"])?;
    let signal = named(spans, "rdm.mesh.node.delete.via-signal").into_iter().find(|sp| sp["span_id"] == shutdown["parent_span_id"] && sp["trace_id"] == shutdown["trace_id"])?;
    Some((signal, tasks, shutdown))
}

/// CONTRACT (node-identity-and-state.md §4: "Shutdown is signal-driven. SIGINT and SIGTERM run the node's own
/// stop steps"; node-stop.md steps 2-4; node-mesh-leave.md Progress: "the node publishes nothing in that state
/// and nothing after it"; stop_gate.rs: work a stop deletes with the birth "ends at the stop and never
/// resumes"): mesh1's other node-admin, isolated (every datagram to or from its port dropped) while the
/// fabric-primary runs, runs fabric recovery's own-mesh arm on the unheard fabric-primary: its node
/// investigation's direct tickle, which can reach nobody, has started, so the arm is open. The isolated admin
/// then receives SIGTERM, whether that probe went out on a connection pooled at the cut or on a new dial. Its arm ends at its stop, by name, before its
/// leave aborts its tasks, and no recovery is admitted by it after its signal stop began. Must NOT happen: the
/// arm outliving the stop (its span never written), or a `via-recovery` of the fabric-primary opened by the
/// stopping admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stopped_admin_ends_its_open_fabric_recovery_arm_at_its_stop_and_admits_no_recovery() {
    use rdm_test::netfault::{udp_ports, Partition};
    let test = "a_signal_stopped_admin_ends_its_open_fabric_recovery_arm_at_its_stop_and_admits_no_recovery";
    let fp = Estate::path("mesh1.node-admin", "1");
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(rdm_test::estate::location(&primary), fp, "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let fp_id = s(&primary["node_id"]);
    let victim_row = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh1" && n["type"] == "node-admin" && rdm_test::estate::location(n) != fp).cloned().expect("mesh1's other node-admin");
    let (victim, victim_id) = (rdm_test::estate::location(&victim_row), s(&victim_row["node_id"]));
    let victim_pid = estate.pid_of(&victim).await;
    let cut = match Partition::isolate(&udp_ports(&nodes, &[victim.clone()])) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot drop {victim:?}'s UDP: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_at = unix_nanos();
    // The own-mesh arm is open: its node investigation of the fabric-primary has started its direct tickle.
    // The signal is sent at that start, while the probe is in flight: the fabric-primary's own recovery of
    // the isolated admin, a floor or two on, ends it otherwise, before a probe sent on a connection pooled
    // at the cut has ended at its time limit. The arm acts on the primary watch's verdict (Q337): it is
    // open while the watch investigates the fabric-primary, whose tickle's own span closes only when the
    // probe ends, so its start is read from the provider inspection the investigation closes right before it.
    let floor = rdm_core::cadence::staleness_floor();
    wait_for(&format!("{victim:?}'s watch starts its direct tickle of {fp:?} while its own-mesh arm waits on it"), floor * 6 + Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.node.resolve.via-provider-inspection")
            .into_iter()
            .find(|sp| start_ns(sp) >= cut_at && attr(sp, "inspector") == victim_id && attr(sp, "node_id") == fp_id)
            .map(|_| ())
    })
    .await;
    assert!(std::process::Command::new("kill").args(["-TERM", &victim_pid.to_string()]).status().unwrap().success(), "kill -TERM {victim_pid}");
    // Its own stop's three spans written: the stop ran to its end and the process left.
    let (signal, tasks, _) = wait_for(&format!("{victim:?} leaves by its own signal stop"), Duration::from_secs(60), || async {
        let spans = estate.spans();
        signal_stop_of(&spans, &victim_id).map(|(a, b, c)| (a.clone(), b.clone(), c.clone()))
    })
    .await;
    drop(cut);
    estate.stop().await;
    let spans = estate.spans();
    let stop_began = named(&spans, "rdm.node_admin.connection.delete.via-stop").into_iter().find(|sp| attr(sp, "node") == victim_id && attr(sp, "operation") == "signal" && sp["trace_id"] == signal["trace_id"]).map(start_ns).unwrap_or_else(|| panic!("{victim:?}'s signal stop began with its connections delete: {signal}"));
    let arm = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().find(|sp| at_location(sp, &victim) && start_ns(sp) >= cut_at).cloned().unwrap_or_else(|| panic!("{victim:?}'s fabric recovery arm, open at its signal, ended and was written before its process left"));
    assert!(end_ns(&arm) <= start_ns(&tasks), "{victim:?}'s arm ended at its stop, before its leave aborted its tasks: arm {arm}, leave-tasks {tasks}");
    assert!(attr(&arm, "outcome").contains("stop"), "the arm's end names the stop: {arm}");
    let after: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| attr(sp, "submitter") == victim_id && start_ns(sp) >= stop_began).collect();
    assert!(after.is_empty(), "{victim:?} opened no recovery after its signal stop began: {after:#?}");
    estate.record_trace_url(&s(&signal["trace_id"]));
}

/// CONTRACT (node-rpc.md §35: "Handlers already running ... may finish ... In-flight work finishes under
/// node.connection.delete's own rules; its receipt is ... `Deadline { last_in_flight, closed, held }` at its
/// bound (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`)"; CLAUDE.md, that env var: the bound "on a signal stop alike:
/// how long in-flight handlers may finish before the node leaves"): the fabric-primary serves a caller's
/// build.create of a Build that adds a node to mesh2, and the testkit's step-event cut at mesh2's primary
/// holds that create's first step event, so mesh2's section and the build.create the fabric-primary serves
/// never end. The fabric-primary receives SIGTERM. It leaves by its own signal stop within the bound's steps: its connections delete ends at the
/// bound, its leave waits for the unsettled call at most the bound, names it, and closes, and its
/// `via-signal` span is written as the process ends. Must NOT happen: the process held past its bound by the
/// call it serves, ended only by the estate's kill with no `via-signal` written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_serving_a_held_build_create_leaves_on_its_signal_within_the_bound() {
    let test = "a_fabric_primary_serving_a_held_build_create_leaves_on_its_signal_within_the_bound";
    let fp = Estate::path("mesh1.node-admin", "1");
    let bound = Duration::from_millis(3000);
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"], &[("RDM_CONNECTIONS_DELETE_DEADLINE_MS", "3000")]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let fp_pid = estate.bootstrap_pid().expect("the Day-0 admin is the fabric-primary and the estate holds its pid") as u64;
    let mesh2_faults = FaultClient::at(&estate, &Estate::path("mesh2.node-admin", "1")).await;
    let id_of = |rows: &[Value], at: &PathName| rows.iter().find(|n| rdm_test::estate::location(n) == *at).map(|n| s(&n["node_id"])).expect("a settled row");
    let rows = estate.node_values().await;
    let fp_id = id_of(&rows, &fp);
    mesh2_faults.arm("held-create", serde_json::json!({"kind": "step-event", "step": "AllocateIdentity", "operation": "node.create"})).await;
    // A Build adds a node to mesh2; its create's first step event is held at mesh2's primary, so the
    // caller's build.create the fabric-primary serves stays open. The caller's stream is kept open with it.
    let request = estate.create_node_request("mesh2", "node").await;
    let nodes = estate.nodes_obj().await;
    let create = tokio::spawn(async move { nodes.create(vec![request]).await.map(|_| ()) });
    let held = mesh2_faults.wait_held("held-create").await;
    assert_eq!(s(&held["hit"]["operation"]["action"]), "node.create", "the held step is the create's, inside the build.create the fabric-primary serves: {held}");
    let signalled = std::time::Instant::now();
    assert!(std::process::Command::new("kill").args(["-TERM", &fp_pid.to_string()]).status().unwrap().success(), "kill -TERM {fp_pid}");
    // The estate is the Day-0 admin's parent and reaps it only at its stop: an ended process is a zombie (`Z`).
    let ended = |pid: u64| std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |st| st.rsplit(')').next().is_some_and(|rest| rest.trim_start().starts_with('Z')));
    // The connections delete's bound, the declarations' bound and the leave's bound, and the steps between.
    let within = bound * 3 + Duration::from_secs(8);
    wait_for(&format!("{fp:?} ends by its own signal stop within {within:?} while the build.create it serves is held"), within, || async { ended(fp_pid).then_some(()) }).await;
    let ended_in = signalled.elapsed();
    create.abort();
    let killed = estate.stop().await;
    assert!(!killed.contains(&(fp_pid as u32)), "{fp:?} left by its own stop, not the estate's kill: {killed:?}");
    let spans = estate.spans();
    let (signal, _, shutdown) = signal_stop_of(&spans, &fp_id).unwrap_or_else(|| panic!("{fp:?}'s signal stop wrote its via-signal, leave-tasks and leave-shutdown spans as it ended ({ended_in:?} after its signal)"));
    assert_eq!(attr(shutdown, "outcome"), "closed", "{fp:?}'s transport closed: {shutdown}");
    assert!(!attr(shutdown, "unsettled").is_empty(), "the leave names the call it served that was still unsettled at its bound: {shutdown}");
    println!("{fp:?} ended {ended_in:?} after its signal; leave-shutdown: {shutdown}");
    estate.record_trace_url(&s(&signal["trace_id"]));
}
