//! product=mesh, feature=node-lifecycle, subfeature=node-stop, rung=MN, provider=process.
//!
//! Stop leaves the process stopped and start rejoins (node-stop.md, node-start.md), driven through the `node` objects:
//! `node.stop` deletes the exact birth's connections, cuts its mesh connections and stops the live process; the
//! node's `left` is the reply of the stop call, never a gossip frame; the mesh primary publishes
//! `NodeStopped` from it. `node.start` makes that same process rejoin as itself.

use rdm_core::PathName;
use crate::build_log;
use rdm_client::{Frame, LaunchKind as NodeKind, NodeEvent, NodeStatus, NodeView, Nodes};
use rdm_core::status::StatusReply;
use rdm_test::estate::{descends_from, named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
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
    rdm_test::typed::mesh_nodes(nodes, &node.mesh).await.expect("node.get [mesh]").into_iter().find(|n| &n.name == node).unwrap_or_else(|| panic!("node.get [mesh: {}] lists no {node}", node.mesh))
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
/// step 10). No runtime exits, no node is created, nothing is repaired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_keeps_the_process_and_a_start_rejoins_it_as_the_same_birth() {
    stop_then_start("a_stop_keeps_the_process_and_a_start_rejoins_it_as_the_same_birth", NODE).await;
}

/// CONTRACT: the same holds for a node-admin: a mesh's non-primary admin is stopped by its mesh primary
/// (its tasks held, its gossip topics left, its connections cut), stays alive with its endpoint bound,
/// and a start makes the same process rejoin: the same node id, incarnation, endpoint key and port,
/// `ready` again, and every other node reports it held again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_admin_keeps_its_process_and_a_start_rejoins_it_as_the_same_birth() {
    stop_then_start("a_stopped_node_admin_keeps_its_process_and_a_start_rejoins_it_as_the_same_birth", "mesh1.admin.2").await;
}

async fn stop_then_start(test: &str, node: &str) {
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = node.parse().unwrap();

    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
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
    // Every other node reports that it dropped the stopped birth and holds the stop overlay.
    for peer in &others {
        peers.fact(peer, "mesh1", "NodeStopped: the stopped birth is dropped and its stop overlay is held", |v| v.birth(&before.node_id).is_none() && v.overlay(&before.node_id, "node.stop:").is_some()).await;
    }
    // node.get shows it stopped: the same birth, the same process, alive.
    let stopped = view_of(&estate.nodes_obj().await, &path).await;
    assert!(stopped.stopped, "node.get lists {node} stopped: {stopped:?}");
    assert_eq!(stopped.status, NodeStatus::Stopped, "the row's status as its mesh-primary wrote it from the stopped frame (Q233): {stopped:?}");
    assert_eq!((&stopped.node_id, &stopped.incarnation_id, &stopped.endpoint_id, &stopped.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "a stop keeps the node id, incarnation, endpoint key and port");
    assert!(alive(pid), "the stopped process {pid} is alive and stopped");
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
    let after = view_of(&estate.nodes_obj().await, &path).await;
    assert!(!after.stopped && after.status == NodeStatus::Ready, "node.get lists {node} ready again: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "a start keeps the node id, incarnation, endpoint key and port");
    assert_eq!(estate.pid_of(node).await, pid, "the same process");

    // The trace deliverable: the story's spans, each named once.
    estate.stop().await;
    let spans = estate.spans();
    let of_node = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node"] == node).collect() };
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
    // an rpc node's `via-declare-own`, a node-admin's declarer's `via-declare` (i146 Q37: one rule for
    // every node, node-status.md "Status report").
    let declared_of = |state: &str, kind: &str| -> Vec<&Value> {
        let own = named(&spans, "rdm.node_rpc.status.update.via-declare-own").into_iter().filter(move |sp| sp["attributes"]["state"] == state);
        let admin = named(&spans, "rdm.node_admin.status.update.via-declare").into_iter().filter(move |sp| sp["attributes"]["key"].as_str().is_some_and(|k| k.starts_with("OwnState(") && k.ends_with(&format!(", {state})"))));
        own.chain(admin).filter(|sp| sp["attributes"]["node"] == node && sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with(kind))).collect()
    };
    let declared = declared_of("Stopped", "node.stop:");
    assert!(!declared.is_empty(), "{node}'s stop declared its Stopped naming the stop");
    let applied = received.iter().chain(declared.iter()).filter(|sp| sp["attributes"]["outcome"] == "applied").count();
    assert_eq!(applied, 1, "exactly one of the declaration and the stop stream's stopped applied the birth's Stopped: received {received:#?}, declared {declared:#?}");
    assert_eq!(named(&spans, "rdm.mesh.topology.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == "node-left" && sp["attributes"]["subject"] == node).count(), 0, "the node gossiped no node-left");
    let served = of_node("rdm.node_admin.status.update.via-start-node");
    assert_eq!((served.len(), served[0]["attributes"]["outcome"].as_str()), (1, Some("started")), "{served:?}");
    // node-mesh-join.md step 10: the start's Pending is recorded and published once, by the mesh
    // primary, when it accepts the start's join report and before it answers it; the start's
    // NodeJoined frame belongs to that Pending and applies as already applied.
    let cut = start_ns(of_stop("rdm.node_admin.node.update.via-stop-transport-cut")[0]);
    let join = of_node("rdm.node_admin.node.update.via-join").into_iter().find(|sp| attr(sp, "outcome") == "installed" && start_ns(sp) > cut).unwrap_or_else(|| panic!("{node}: the start's join report is accepted after the stop's transport cut"));
    let admitted: Vec<&Value> = of_node("rdm.node_admin.status.update.via-mesh-join-admitted").into_iter().filter(|sp| start_ns(sp) > cut && attr(sp, "outcome") == "applied").collect();
    assert_eq!(admitted.len(), 1, "{node}: the start's Pending is recorded once, at its accepted join report: {admitted:#?}");
    assert!(start_ns(admitted[0]) >= start_ns(join) && end_ns(admitted[0]) <= end_ns(join), "{node}: the Pending is recorded while the join report is accepted, before it is answered: pending {}, join {join}", admitted[0]);
    let joined_frames: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "frame") == "node-joined" && attr(sp, "operation").starts_with("node.start:")).map(|sp| attr(sp, "outcome")).collect();
    assert_eq!(joined_frames, vec!["already-applied".to_string()], "{node}: the start's NodeJoined belongs to the join report's Pending and starts no other");
    // The start's node.started names the start (node-status.md "Status report": "a start's Pending and
    // Ready"), applied by the mesh primary.
    let ready: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "frame") == "node-started" && attr(sp, "operation").starts_with("node.start:")).collect();
    assert_eq!(ready.len(), 1, "{node}'s NodeStarted is recorded on its open start stream: {ready:#?}");
    assert!(declared_of("Ready", "node.start:").is_empty(), "the open start sends no separate Ready declaration");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["node"] == node && sp["attributes"]["command"] == "start-node").expect("the start command was sent");
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
        let (birth_addr, incarnation) = (before.transport_addr.expect("the birth's transport address").to_string(), before.incarnation_id.as_ref().unwrap().as_str().to_string());
        let dialled = named(&spans, "rdm.node_rpc.connection.create.via-dial").into_iter().any(|sp| {
            let by_caller = attr(sp, "incarnation_id") == incarnation && attr(sp, "local").contains(&caller_addr);
            let by_birth = attr(sp, "incarnation_id") == caller_incarnation && attr(sp, "local").contains(&birth_addr);
            attr(sp, "outcome") == "connected" && (by_caller || by_birth) && (cut..=end_ns(call)).contains(&start_ns(sp))
        });
        assert!(dialled, "the start rode a connection between {caller} and {node}'s birth dialled after the stop's transport cut: {call}");
    }
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a stopped node is not drift");
    // node-mesh-join.md steps 5 and 7 (Q87): every join is issued a fresh member cert and the node
    // installs the one its join answered, the start's join after a stop as much as its first.
    let issued: Vec<&Value> = named(&spans, "rdm.node_admin.cert.create.via-join").into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "outcome") == "issued").collect();
    let installed = of_node("rdm.node.cert.update.via-join");
    let last = issued.iter().max_by_key(|sp| start_ns(sp)).unwrap_or_else(|| panic!("{node}'s joins were issued member certs: {issued:#?}"));
    assert!(issued.len() >= 2, "{node}'s first start and its start after the stop were each issued a cert: {issued:#?}");
    assert!(
        installed.iter().any(|sp| attr(sp, "expires_at") == attr(last, "expires_at_rafka_ms") && start_ns(sp) > start_ns(last)),
        "{node} installs the cert its start's join was issued, expiring {}: installed {installed:#?}",
        attr(last, "expires_at_rafka_ms")
    );
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// The mesh-primary's record of `node`'s command frames for `operation`, in the order it took them.
fn command_frames(spans: &[Value], node: &str, operation: &str) -> Vec<String> {
    let mut of: Vec<&Value> = named(spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "operation") == operation).collect();
    of.sort_by_key(|sp| start_ns(sp));
    of.into_iter().map(|sp| attr(sp, "frame")).collect()
}

/// Every status write the mesh-primary made on `node`'s row from `from` on, in order, with its outcome
/// (`written` or `unchanged`).
fn rows_taken(spans: &[Value], node: &str, from: u64) -> Vec<(String, String)> {
    let mut of: Vec<&Value> = named(spans, "rdm.node_admin.node.update.via-row-status").into_iter().filter(|sp| attr(sp, "subject") == node && start_ns(sp) >= from).collect();
    in_write_order(&mut of);
    of.into_iter().map(|sp| (attr(sp, "status"), attr(sp, "outcome"))).collect()
}

/// `rows`, `rdm.node_admin.node.update.via-row-status` spans of one record, in the order its writer
/// made the writes: by the record's `revision` once each write finished, a write that moved the record
/// to a revision before every write that found it there, then by start. Each span follows its write, so
/// a write made first may report last; the revision is what the write records.
pub(crate) fn in_write_order(rows: &mut [&Value]) {
    let revision = |sp: &Value| attr(sp, "revision").parse::<u64>().unwrap_or_else(|_| panic!("a held record's row-status span carries its revision: {sp}"));
    rows.sort_by_key(|sp| (revision(sp), attr(sp, "outcome") != "written", start_ns(sp)));
}

/// Every order a stop's row writes can take at its mesh-primary, each `(status, outcome)`: the stop
/// stream's frames in order, each written, until the node's Stopped is taken, which writes stopped,
/// and every frame after it finds the row stopped and leaves it unchanged. The node's declaration of
/// Stopped and the stream's own stopped frame are applied whichever first reaches the mesh-primary
/// (node-stop.md step 5), so the declaration may be taken after any of the frames; taken before the
/// stop's acknowledgement, the stop is applied already and stopping is not written at all.
pub(crate) fn stop_row_writes() -> Vec<Vec<(String, String)>> {
    let frames = ["Stopping", "ConnectionsDeleting", "ConnectionsDeleted", "Leaving", "Left"];
    let at = |status: &str, outcome: &str| (status.to_string(), outcome.to_string());
    let shape = |k: usize, from: usize| -> Vec<(String, String)> { frames[from..k].iter().map(|s| at(*s, "written")).chain([at("Stopped", "written")]).chain(frames[k..].iter().map(|s| at(*s, "unchanged"))).collect() };
    (1..=frames.len()).map(|k| shape(k, 0)).chain([shape(1, 1)]).collect()
}

/// The statuses the mesh-primary wrote on `node`'s row from `from` on, in order.
fn rows_written(spans: &[Value], node: &str, from: u64) -> Vec<String> {
    let mut of: Vec<&Value> = named(spans, "rdm.node_admin.node.update.via-row-status").into_iter().filter(|sp| attr(sp, "subject") == node && attr(sp, "outcome") == "written" && start_ns(sp) >= from).collect();
    in_write_order(&mut of);
    of.into_iter().map(|sp| attr(sp, "status")).collect()
}

/// Where `operation` began: the first span naming it, whoever recorded it. Its rows are read from here.
fn began(spans: &[Value], operation: &str) -> u64 {
    spans.iter().filter(|sp| attr(sp, "operation") == operation).map(start_ns).min().unwrap_or_else(|| panic!("no span names {operation}"))
}

/// CONTRACT (Q205; node-stop.md steps 1-5; node-rpc-envelope.md, the node.stop row; states.md "Who
/// sets each state"): a commanded stop is a workflow that calls its primitives, node.connection.delete
/// `{ All }` then node.mesh.leave, and streams each one's acknowledgement and completion on the stop's
/// own stream: the mesh-primary takes `command-accepted`, `connections-deleting`,
/// `stop-connections-deleted`, `leaving`, `stop-mesh-left`, `node-stopped`, in that order, and writes
/// the node's row from each: stopping, connections-deleting, connections-deleted, leaving, left,
/// stopped. The node's owed `Stopped` declaration is held at the mesh-primary for the stop (the
/// faulted admin's `declaration` cut), so the stream's frames reach it first; released, the declaration
/// finds the stop applied (step 5: whichever of the two reaches it first is applied). Must NOT happen: a
/// primitive's frame missing from the stop's stream, or a status the frames carry missing from the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commanded_stop_streams_each_primitives_acknowledgement_and_completion_and_its_mesh_primary_writes_each_status() {
    let test = "a_commanded_stop_streams_each_primitives_acknowledgement_and_completion_and_its_mesh_primary_writes_each_status";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary = views.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).expect("mesh1's primary");
    let path: PathName = NODE.parse().unwrap();
    let door = Door::at(&estate, &primary).await;
    door.arm("declaration", serde_json::json!({"kind": "declaration", "node": NODE, "state": "Stopped"})).await;
    let stopped = frames(estate.nodes_obj().await.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let hit = door.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node"], NODE, "the mesh-primary holds the node's Stopped declaration: {hit}");
    door.release("declaration").await;

    estate.stop().await;
    let spans = estate.spans();
    let operation = format!("node.stop:{NODE}");
    assert_eq!(
        command_frames(&spans, NODE, &operation),
        ["command-accepted", "connections-deleting", "stop-connections-deleted", "leaving", "stop-mesh-left", "node-stopped"],
        "the stop's stream carries each primitive's acknowledgement and completion"
    );
    assert_eq!(
        rows_written(&spans, NODE, began(&spans, &operation)),
        ["Stopping", "ConnectionsDeleting", "ConnectionsDeleted", "Leaving", "Left", "Stopped"],
        "the mesh-primary writes the row from each frame"
    );
    let declared: Vec<String> = named(&spans, "rdm.node_admin.status.update.via-declaration").into_iter().filter(|sp| attr(sp, "sender") == NODE && attr(sp, "state") == "Stopped").map(|sp| attr(sp, "outcome")).collect();
    assert_eq!(declared, ["already-applied"], "the released declaration finds the stop applied from its stream");
}

/// CONTRACT (Q205; node-mesh-leave.md Wire row and steps 1-5; node-replace.md steps 1-3): the
/// predecessor's half of a replace, node.connection.delete `{ All }`, node.mesh.leave, node.stop. The
/// standalone leave is a node command on 0x22: the mesh-primary takes `command-accepted` (leaving) and
/// `mesh-left`, and writes leaving then left. The stop of the node already connections-deleted and left
/// answers each primitive's acknowledgement and completion at once and repeats no work: its stream
/// carries the same six frames as any stop, the node closes nothing again (`via-stop` says
/// `already-deleted`) and leaves nothing again (one `via-leaving`, the leave's), and the mesh-primary
/// writes the row from each frame (Q205, Q233: the stop's acknowledgement sets stopping over the left
/// row), stopping, connections-deleting, connections-deleted, leaving, left, stopped; the node's Stopped
/// declaration may be taken before the stream's last frames, whichever first is applied (node-stop.md
/// step 5), and each frame after it leaves the row stopped ([`stop_row_writes`]). Must NOT
/// happen: a 0x1B `LeaveMesh`, a second `via-leaving`, or a second closure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_node_already_connections_deleted_and_left_answers_each_primitive_at_once_and_repeats_no_work() {
    let test = "a_stop_of_a_node_already_connections_deleted_and_left_answers_each_primitive_at_once_and_repeats_no_work";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let path: PathName = NODE.parse().unwrap();
    use rdm_core::build::Step;
    let steps = vec![Step::DeleteConnections { node: path.clone() }, Step::LeaveMesh { node: path.clone() }, Step::Stop { node: path.clone() }];
    let got = frames(estate.nodes_obj().await.steps(steps).await.expect("the Build was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");

    estate.stop().await;
    let spans = estate.spans();
    let (leave, stop) = (format!("node.mesh.leave:{NODE}"), format!("node.stop:{NODE}"));
    assert_eq!(command_frames(&spans, NODE, &leave), ["command-accepted", "mesh-left"], "the standalone leave is a node command: its acknowledgement, then its completion");
    let rows = rows_taken(&spans, NODE, began(&spans, &leave));
    let at = |status: &str, outcome: &str| (status.to_string(), outcome.to_string());
    let leave_rows = [at("Leaving", "written"), at("Left", "written")];
    assert!(
        stop_row_writes().iter().any(|stop| leave_rows.iter().chain(stop).eq(rows.iter())),
        "the leave writes leaving then left; the stop's acknowledgement sets stopping over the left row and each of its frames moves it on until the node's stopped is taken: {rows:?}"
    );
    assert_eq!(
        command_frames(&spans, NODE, &stop),
        ["command-accepted", "connections-deleting", "stop-connections-deleted", "leaving", "stop-mesh-left", "node-stopped"],
        "the stop answers each primitive's acknowledgement and completion"
    );
    let leaving: Vec<String> = named(&spans, "rdm.node.mesh.update.via-leaving").into_iter().filter(|sp| attr(sp, "node") == NODE).map(|sp| attr(sp, "operation")).collect();
    assert_eq!(leaving, [leave.clone()], "the node left its mesh once, at the leave; the stop leaves nothing again");
    let closures: Vec<String> = named(&spans, "rdm.node_admin.connection.delete.via-stop").into_iter().filter(|sp| attr(sp, "node") == NODE && attr(sp, "operation") == stop).map(|sp| attr(sp, "outcome")).collect();
    assert_eq!(closures, ["already-deleted"], "the stop closes nothing again: it answers the receipt its connections delete established");
}

/// CONTRACT (node-name-update.md Precondition: "A stopped node is never renamed"; Validation: "the
/// precondition holds, else `RejectedAdmittingConnections { status, held_connections }`"; Failure and
/// receipts: refused before the claim, "nothing written, nothing changed"; node-start.md): the caller's
/// `node.name.update` of the stopped `mesh1.rpc.1` to the name `1.old` reaches its mesh-primary directly
/// and is refused `RejectedAdmittingConnections { status: Leaving, held_connections: [] }`, `Leaving`
/// being the status a stopped birth is held at; `node.get` lists the stopped birth at `mesh1.rpc.1` and
/// nothing at `mesh1.rpc.1.old`. `node.start` of `mesh1.rpc.1` makes the same process rejoin under its
/// own name: `node.joined`, `node.started`, `Complete`; `node.get` lists the same birth
/// `ready` at `mesh1.rpc.1`, and every other node holds it publishing that name. The
/// mesh-primary's `via-writer` span says `rejected-admitting-connections` with `traffic=Stopped` and
/// `connections=stopped`. What must NOT happen: `NameUpdated`, a row or a listing at `mesh1.rpc.1.old`,
/// or the start admitting the birth under another name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_nodes_rename_at_its_mesh_primary_is_refused_and_its_start_rejoins_under_its_own_name() {
    let test = "a_stopped_nodes_rename_at_its_mesh_primary_is_refused_and_its_start_rejoins_under_its_own_name";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = NODE.parse().unwrap();
    let renamed: PathName = path.renamed();

    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    let others: Vec<&NodeView> = mesh.iter().filter(|n| n.name != path).collect();
    let pid = estate.pid_of(NODE).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let stopped = view_of(&estate.nodes_obj().await, &path).await;
    assert!(stopped.stopped && stopped.status == NodeStatus::Stopped, "{stopped:?}");

    // The rename of a stopped node: one call to its mesh-primary, refused before anything is claimed.
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(stopped.node_id.clone()), node_id: stopped.node_id.clone(), incarnation: stopped.incarnation_id.clone().expect("the stopped node's birth") };
    let reply = nodes.name_update(&birth, &primary.node_id, &rdm_transport::RouteChoice::Direct, &renamed.name()).await.expect("node.name.update answered");
    assert_eq!(reply, StatusReply::RejectedAdmittingConnections { status: NodeStatus::Leaving, held_connections: vec![] }, "a stopped node is never renamed");
    let listed = view_of(&estate.nodes_obj().await, &path).await;
    assert!(listed.stopped && listed.node_id == before.node_id && listed.incarnation_id == before.incarnation_id, "node.get lists the stopped birth at {path}: {listed:?}");
    assert_eq!(estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().filter(|n| n.name == renamed).count(), 0, "node.get lists nothing at {renamed}");

    // The start by its own name rejoins the same process.
    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    for peer in &others {
        peers
            .fact(peer, "mesh1", "the rejoined birth is held again as the same incarnation, publishing its own name", |v| {
                v.birth(&before.node_id).is_some_and(|d| d.node.incarnation == *before.incarnation_id.as_ref().unwrap() && d.node.name == path)
            })
            .await;
    }
    let after = view_of(&estate.nodes_obj().await, &path).await;
    assert!(!after.stopped && after.status == NodeStatus::Ready, "node.get lists {path} ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "the same birth");
    assert_eq!(estate.pid_of(NODE).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let update = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).expect("the mesh-primary decided the node.name.update");
    assert_eq!(
        (s(&update["attributes"]["outcome"]), s(&update["attributes"]["traffic"]), s(&update["attributes"]["connections"]), s(&update["attributes"]["from"])),
        ("rejected-admitting-connections".into(), "Stopped".into(), "stopped".into(), path.to_string()),
        "{update:#?}"
    );
    assert!(s(&update["attributes"]["detail"]).contains("a stopped node is never renamed"), "the refusal names the rule: {update:#?}");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && sp["attributes"]["node"] == path.to_string()).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").expect("the start's rejoin was installed");
    assert_eq!(s(&join["attributes"]["admitted_as"]), path.to_string(), "{join:#?}");
    estate.record_trace_url(update["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-rpc-envelope.md "Node properties": config "is a field of the node record ... a
/// rejoining node gets it back in its join answer"; node-config-update.md: row first, node second; the
/// stopped arm node.name.update already completes on, proposal node-properties-own-commands.md §2.3):
/// `node.config.update` of the stopped `mesh1.rpc.1` at its mesh-primary ends `node.config.updated`
/// on the row: the row at `mesh1.rpc.1` holds the whole config, and the stopped node is sent nothing
/// (the via-config span says `apply=stopped`, no `answered`; no ApplyNodeConfig reaches it). Then
/// `node.start` makes the same process rejoin: its join is handed the row's config
/// (`via-join` `config_keys=1`), and `node.get` lists the same birth `ready` carrying it.
/// What must NOT happen: `ConfigNotApplied` (`not-sent`) for a stopped node, or a call to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_nodes_config_update_completes_on_its_row_and_its_start_carries_it() {
    let test = "a_stopped_nodes_config_update_completes_on_its_row_and_its_start_carries_it";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = NODE.parse().unwrap();
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    let pid = estate.pid_of(NODE).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let held = view_of(&estate.nodes_obj().await, &path).await;
    assert!(held.stopped && held.status == NodeStatus::Stopped, "{held:?}");

    // The update: one call to the fabric-primary, here mesh1's mesh-primary too, the row written, the
    // stopped node told nothing.
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(held.node_id.clone()), node_id: held.node_id.clone(), incarnation: held.incarnation_id.clone().expect("the stopped node's birth") };
    let carrier = estate.carrier().await;
    let reply = rdm_client::NodeRpc::new(carrier.client())
        .config_update(&primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .expect("node.config.update answered");
    assert!(matches!(&reply, StatusReply::ConfigUpdated { node_id, incarnation, config, .. } if *node_id == before.node_id && Some(incarnation) == before.incarnation_id.as_ref() && *config == told), "a stopped node's config update completes on its row: {reply:?}");

    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    // The config the row holds is what node.get answers, from the fabric-primary's atlas (Q203: no
    // digest carries it); read until the started birth is ready, within a staleness floor.
    let until = std::time::Instant::now() + rdm_transport::topology::staleness_floor();
    let after = loop {
        let v = view_of(&estate.nodes_obj().await, &path).await;
        if (v.config == told && !v.stopped && v.status == NodeStatus::Ready) || std::time::Instant::now() >= until {
            break v;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(!after.stopped && after.status == NodeStatus::Ready, "node.get lists {path} ready: {after:?}");
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

/// CONTRACT (R-JN1; node-replace.md steps 1-2 and Rollback step 2; node-start.md step 2; R12/R13;
/// CLAUDE.md read rule): in a mesh of three node-admins, the non-primary `mesh1.admin.3` has its
/// connections deleted, leaves its mesh and the backbone, is renamed `mesh1.admin.3.old` live and is
/// stopped there, as the predecessor's half of the replace steps. Every admin lists the stopped birth at
/// the row's name, `mesh1.admin.2`, which performed no rename, included: its view reads the name from
/// the row on the shared store root. `node.start` of `mesh1.admin.3.old` makes the same process
/// rejoin: its join presents `mesh1.admin.3.old`, the name it took off its mesh, and is admitted under
/// it (`via-join` `admitted_as`); the node installs the answer's name before it publishes
/// (`via-join-admitted` `installed`, then `via-ready` under that name). The stream carries
/// `node.joined`, `node.started` and ends `Complete`; every admin lists the same birth
/// `ready` at `mesh1.admin.3.old`, and the rpc node holds it publishing that name. What must
/// NOT happen: an admin listing the stopped birth at `mesh1.admin.3`, the start refused (`UnknownNode`,
/// `JoinMismatch("name")`), or a span of the started node after its join naming `mesh1.admin.3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admin_renamed_off_its_mesh_and_stopped_starts_under_the_name_it_holds_on_every_admin() {
    let test = "a_node_admin_renamed_off_its_mesh_and_stopped_starts_under_the_name_it_holds_on_every_admin";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 3, 1)]).await;
    estate.settled_views(&[("mesh1", 3, 1)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = "mesh1.admin.3".parse().unwrap();
    let renamed: PathName = path.renamed();

    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    peers.learn(&mesh);
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let primary = mesh.iter().find(|n| n.is_primary).cloned().expect("node.get names mesh1's primary");
    assert_ne!(primary.name, path, "the stopped admin is not mesh1's primary");
    let admins: Vec<NodeView> = mesh.iter().filter(|n| n.name != path && n.kind == rdm_core::NodeKind::NodeAdmin).cloned().collect();
    assert!(admins.iter().any(|n| n.name != primary.name), "an admin that performs no rename: {admins:?}");
    let rpc = mesh.iter().find(|n| n.kind == rdm_core::NodeKind::RpcNode).cloned().expect("the rpc node");
    let pid = estate.pid_of(path.to_string().as_str()).await;

    // The predecessor's half of a replace: its connections, its mesh, its name live, then its stop.
    use rdm_core::build::Step;
    let steps = vec![
        Step::DeleteConnections { node: path.clone() },
        Step::LeaveMesh { node: path.clone() },
        Step::Update { node: path.clone(), name: renamed.clone() },
        Step::Stop { node: renamed.clone() },
    ];
    let half = frames(nodes.steps(steps).await.expect("the Build was accepted")).await;
    assert_eq!(half.last(), Some(&Frame::Complete), "{half:?}");
    // Every admin's view names the stopped birth by its row, the one that performed no rename included.
    for admin in &admins {
        let listed = estate
            .until_view_at(&admin.name, &format!("{} lists the stopped birth at {renamed}", admin.name), Duration::from_secs(10), |view| {
                let at = |p: &PathName| view.iter().find(|n| &n.name == p).cloned();
                at(&renamed).filter(|n| n.node_id == before.node_id && at(&path).is_none())
            })
            .await;
        assert_eq!((listed.stopped, listed.incarnation_id.clone()), (true, before.incarnation_id.clone()), "{}: {listed:?}", admin.name);
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
                view.iter().find(|n| n.name == renamed && n.node_id == before.node_id && n.status == NodeStatus::Ready && !n.stopped).map(|_| ())
            })
            .await;
    }
    assert_eq!(estate.pid_of(renamed.to_string().as_str()).await, pid, "the same process, listed at the row's name");

    estate.stop().await;
    let spans = estate.spans();
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["command"] == "start-node" && s(&sp["attributes"]["node"]) == renamed.to_string()).expect("the start command was sent");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed" && sp["trace_id"] == sent["trace_id"]).expect("the start's rejoin was installed");
    assert_eq!((s(&join["attributes"]["node"]), s(&join["attributes"]["admitted_as"])), (renamed.to_string(), renamed.to_string()), "the join presents the name the node took off its mesh and is admitted under it: {join:#?}");
    let admitted = named(&spans, "rdm.mesh.node.update.via-join-admitted").into_iter().find(|sp| s(&sp["attributes"]["installed"]) == renamed.to_string()).expect("the started node installed the row's name its join answer handed");
    let ready: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == renamed.to_string()).collect();
    assert_eq!(ready.len(), 1, "the started node reports ready under the row's name: {ready:?}");
    assert!(ready[0]["start_unix_nano"].as_u64() >= admitted["end_unix_nano"].as_u64(), "the name is installed before the node says anything: {admitted:#?} {ready:#?}");
    assert!(named(&spans, "rdm.node_admin.node.reject.via-join-mismatch").is_empty(), "no join was refused");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a renamed stopped node is not drift");
    estate.record_trace_url(sent["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: `node.stop` opens the next attempt of the accepted Build (reason
/// stop, action stop targeted to the live birth, no Build minted). The operation is exactly
/// `node.stop:<path>`: stop-node to the birth, whose reply is `left`, then Complete. The runtime is
/// not terminated and no NodeDeleted is published; the accepted topology still names the path and no
/// repair attempt follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted() {
    let test = "a_standalone_stop_ends_on_the_births_left_reply_with_no_exit_and_no_node_deleted";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let birth_build = estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await.build_id.to_string();
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
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
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "targeted to the live birth: {opened}");
    Estate::complete(&mut stop).await;
    build_log::attempt_complete(&estate.carrier().await, &birth_build, attempt_no, SETTLE).await;
    assert!(alive(pid), "the stopped process {pid} is stopped, alive");
    // No repair attempt follows the stop: the Build's attempt is the stop's after the reconciler's checklists have passed.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let b = build_log::build_value(&estate.carrier().await, &birth_build).await;
    assert_eq!(b["attempt"].as_u64(), Some(attempt_no), "no repair attempt followed the stop: {b}");
    let n = estate.node_value(NODE).await;
    assert_eq!(n["incarnation_id"], before["incarnation_id"], "no new birth at {NODE}: {n}");
    assert_eq!(n["stopped"], true, "{n}");

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

/// CONTRACT (i146 Q29, Q36; node-stop.md step 5): `node.stop` of `mesh1.rpc.1` runs and the birth stops;
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
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let fp = views.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let path: PathName = NODE.parse().unwrap();
    let before = view_of(&estate.nodes_obj().await, &path).await;

    let door = Door::at(&estate, &fp).await;
    door.arm("stopped-held", serde_json::json!({"kind": "event", "event": "stopped", "node": NODE})).await;
    let stop = tokio::spawn({
        let nodes = estate.nodes_obj().await;
        let path = path.clone();
        async move { frames(nodes.stop(&path).await.expect("the stop was accepted")).await }
    });
    door.wait_held("stopped-held").await;
    // The stream's stopped is held at the mesh-primary: the birth is listed stopped from its own
    // declaration alone.
    let until = std::time::Instant::now() + Duration::from_secs(30);
    let stopped = loop {
        let v = view_of(&estate.nodes_obj().await, &path).await;
        if v.stopped {
            break v;
        }
        assert!(std::time::Instant::now() < until, "the stopped birth's declaration lists it stopped within 30 s while the stream's stopped is held: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!((stopped.status, &stopped.node_id, &stopped.incarnation_id), (NodeStatus::Stopped, &before.node_id, &before.incarnation_id), "the same birth, stopped on its row from its own declaration");
    assert_eq!(door.cut("stopped-held").await["held"], true, "the stream's stopped is still held: {}", door.cut("stopped-held").await);
    door.release("stopped-held").await;
    let stopped = stop.await.expect("the stop's call");
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");

    estate.stop().await;
    let spans = estate.spans();
    let operation = format!("node.stop:{NODE}");
    let declared = named(&spans, "rdm.node_rpc.status.update.via-declare-own").into_iter().find(|sp| attr(sp, "node") == NODE && attr(sp, "state") == "Stopped" && attr(sp, "operation") == operation && attr(sp, "outcome") == "applied").expect("the stopped birth's declaration of Stopped naming its stop was applied");
    let node_stopped: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| attr(sp, "node") == NODE).collect();
    assert_eq!(node_stopped.len(), 1, "NodeStopped is published once for the birth: {node_stopped:#?}");
    assert_eq!(attr(node_stopped[0], "operation"), operation, "NodeStopped names the stop: {}", node_stopped[0]);
    // The birth's own span of its declaration opens when its call returns, after the mesh-primary
    // applied it; the declaration is NodeStopped's source because the stream's stopped was held until
    // after NodeStopped, and was recorded as already applied.
    let _ = declared;
    let received: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-stop-received").into_iter().filter(|sp| attr(sp, "node") == NODE).collect();
    assert_eq!(received.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(), vec!["already-applied".to_string()], "the released stream's stopped found the row held: {received:#?}");
    assert!(start_ns(node_stopped[0]) < start_ns(received[0]), "NodeStopped was published before the held stream's stopped was recorded");
    for kind in ["node-leaving", "node-left"] {
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-lifecycle-frame").into_iter().filter(|sp| attr(sp, "kind") == kind && attr(sp, "subject") == NODE).collect();
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
/// `RDM_STOPPED_NODE_TTL` is 3 s, `mesh1.rpc.1` is stopped and left alone. The fabric-primary's sweep
/// ages it from its stop receipt and submits one ordinary node.delete Build
/// (`rdm.node_admin.node.delete.via-stopped-ttl` with the node, the receipt's time, the ttl and the
/// Build); the delete runs as a delete does: the provider's exited verdict of the stopped process
/// (TerminateRuntime complete), NodeDeleted, and `node.get` lists no `mesh1.rpc.1`. The other stopped
/// period never started: nothing else is deleted. Must NOT happen: a delete before one ttl, a second
/// delete Build, a collection the sweep made itself without a Build, or a role transfer, connections delete or stop
/// sent again to the stopped birth.
// @feature: stopped-node-collection
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_past_the_ttl_is_deleted_by_the_fabric_primarys_sweep() {
    let test = "a_node_stopped_past_the_ttl_is_deleted_by_the_fabric_primarys_sweep";
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[("RDM_STOPPED_NODE_TTL", "3s")]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let peers = estate.peers().await;
    let nodes = estate.nodes_obj().await;
    let path: PathName = NODE.parse().unwrap();
    peers.learn(&estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]"));
    let pid = estate.pid_of(NODE).await;

    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let stopped_at = std::time::Instant::now();
    // The sweep collects it: the delete's exited verdict ends the stopped process and node.get lists it no more.
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
    assert!(exit.is_some(), "the delete Build proved the stopped process exited");
    // The stopped birth's stop already ran: its delete sends no role transfer, node.connection.delete or stop again
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

/// CONTRACT (node-stop.md step 6: "A stopped node leaves the stopped status only through node.start or
/// node.delete"; states.md "Stopped nodes": "stopped is not live and is still present"; a completed
/// operation is never planned again): in mesh2, a peer mesh of the fabric-primary mesh1.admin.1,
/// `node.stop` of mesh2.rpc.1, then `node.delete` of another rpc node of mesh2 (the Build that removes
/// its path). The delete Build plans that path's delete and at most once the stopped path's create, whose
/// check reaches the stopped birth, holds it and receipts the create `Complete`; no attempt plans it
/// again and the attempts are bounded; the delete's operation completes and its path is listed no more;
/// the stopped birth is still listed stopped, the same birth and the same live process. The Build then
/// ends Complete: the stopped birth holds neither its mesh's ready nor the fabric's checklist
/// (states.md:16, Q240), so the fabric's change reaches ready (flame b7f9ecec2fec32f356d022f98bdfb374
/// is this cell's shape before Q240: Blocked on the change naming the stopped birth `not-ready`).
///
/// Must NOT happen (#3233, 141's estate): every attempt of the delete Build planning `node.create` at
/// the stopped path, its check holding the stopped birth and the next attempt planning the create again,
/// the Build never ending (1198 attempts in 240 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_deleting_another_path_after_a_stop_plans_the_stopped_paths_create_at_most_once() {
    let test = "a_build_deleting_another_path_after_a_stop_plans_the_stopped_paths_create_at_most_once";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 1, 3)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 1, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.nodes_obj().await;
    const STOPPED: &str = "mesh2.rpc.1";
    let stopped_path: PathName = STOPPED.parse().unwrap();
    let pid = estate.pid_of(STOPPED).await;

    let stopped = frames(nodes.stop(&stopped_path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let before = view_of(&estate.nodes_obj().await, &stopped_path).await;
    assert!(before.stopped && before.status == NodeStatus::Stopped, "node.get lists {STOPPED} stopped: {before:?}");

    // The other path: an rpc node of mesh2 that is not its node-type primary, so its delete moves no primary.
    let mesh2 = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.expect("node.get [mesh]");
    let other = mesh2
        .iter()
        .filter(|n| n.name.to_string().starts_with("mesh2.rpc.") && n.name != stopped_path && !n.is_primary)
        .map(|n| n.name.clone())
        .max()
        .unwrap_or_else(|| panic!("mesh2 lists another rpc node that is not its primary: {mesh2:?}"));
    let other_name = other.to_string();
    let mut stream = nodes.delete(&other).await.expect("the delete was accepted");
    let build_id = stream.accepted().build_id.to_string();
    // Read until the Build ends, bounded and without a panic: a Build that does not end is asserted below
    // from its spans, which name the operations every attempt planned.
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
        estate.until_node(&other_name, "the deleted path is listed no more", SETTLE, |n| n.is_none().then_some(())).await;
        let held = view_of(&estate.nodes_obj().await, &stopped_path).await;
        assert!(held.stopped && held.status == NodeStatus::Stopped, "{STOPPED} is still stopped after the delete of {other}: {held:?}");
        assert_eq!((&held.node_id, &held.incarnation_id), (&before.node_id, &before.incarnation_id), "the stopped birth is the same birth");
    }
    assert!(alive(pid), "the stopped process {pid} is alive");
    assert_eq!(estate.pid_of(STOPPED).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|sp| attr(sp, "build_id") == build_id).collect();
    let planned_create: Vec<String> = reconciles
        .iter()
        .filter(|sp| attr(sp, "operations").split(',').any(|op| op == format!("node.create:{STOPPED}")))
        .map(|sp| format!("attempt {} at {}: {} ({})", attr(sp, "attempt"), attr(sp, "executor"), attr(sp, "operations"), attr(sp, "outcome")))
        .collect();
    assert!(planned_create.len() <= 1, "the stopped {STOPPED}'s create is planned at most once in the delete Build {build_id}, its check holding the birth; {} of {} attempts planned it: {:#?}", planned_create.len(), reconciles.len(), &planned_create[..planned_create.len().min(5)]);
    let held: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-stopped-held").into_iter().filter(|sp| attr(sp, "node") == STOPPED && attr(sp, "build_id") == build_id).collect();
    assert_eq!(held.len(), planned_create.len(), "every planned create of the stopped {STOPPED} is held by its check and receipted Complete: {held:#?}");
    assert!(reconciles.len() <= 3, "the delete Build {build_id} runs a bounded number of attempts, never a loop: {}", reconciles.len());
    let delete_op = format!("node.delete:{other}");
    let create_op = format!("node.create:{STOPPED}");
    let ops: Vec<String> = reconciles.iter().map(|sp| attr(sp, "operations")).filter(|o| !o.is_empty()).collect();
    assert!(ops.iter().any(|o| o.split(',').any(|op| op == delete_op)), "an attempt of the delete Build {build_id} plans {delete_op}: {ops:?}");
    assert!(ops.iter().all(|o| o.split(',').all(|op| op == delete_op || op == create_op)), "the delete Build {build_id} plans {delete_op} and at most the stopped path's held create: {ops:?}");
    let completed: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "build_id") == build_id && attr(sp, "node") == other_name && attr(sp, "step") == "Complete" && attr(sp, "outcome") == "complete").collect();
    assert!(!completed.is_empty(), "the delete of {other} completes in Build {build_id}");
    assert!(settled, "the delete Build {build_id} of {other} ends Complete within {SETTLE:?}, the stopped {STOPPED} holding nothing: {deleted:?}");
    estate.record_trace_url(reconciles[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (states.md:16, Q240; "Resize and executable replacement"): a stopped birth holds neither its
/// mesh's ready nor the fabric's checklist. mesh1 {1 node-admin, 1 rpc node} and mesh2 {1 node-admin,
/// 2 rpc nodes}, ready; `node.stop` of mesh2's rpc node that is not its node-type primary completes;
/// then a resize grows mesh2 to 3 rpc nodes. The resize Build ends Complete, the fabric-primary's change
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
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", 1, 2)]).await;
    estate.settled_views(&[("mesh1", 1, 1), ("mesh2", 1, 2)]).await;
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let fabric_primary = estate.fabric_view().await.fabric_primary.expect("the ready fabric names its fabric-primary");
    let nodes = estate.nodes_obj().await;
    let mesh2 = nodes.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.expect("node.get [mesh]");
    // The rpc node that is not mesh2's rpc node-type primary, so its stop moves no primary.
    let stopped_path = mesh2
        .iter()
        .find(|n| n.name.kind == NodeKind::RpcNode && !n.is_primary)
        .map(|n| n.name.clone())
        .unwrap_or_else(|| panic!("mesh2 lists an rpc node that is not its primary: {mesh2:?}"));
    let stopped_name = stopped_path.to_string();
    let pid = estate.pid_of(&stopped_name).await;

    let stopped = frames(nodes.stop(&stopped_path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    estate.until_stopped_at_the_fabric_primary(&stopped_path).await;
    let before = view_of(&estate.nodes_obj().await, &stopped_path).await;
    assert!(before.stopped && before.status == NodeStatus::Stopped, "node.get lists {stopped_name} stopped: {before:?}");

    // The resize: mesh2 grows by one rpc node while the stopped birth rests.
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
    assert!(matches!(built.last(), Some(rdm_client::BuildFrame::Complete { .. })), "the resize Build {build_id} ends Complete with {stopped_name} stopped, never Blocked on the fabric's change: {built:?}");
    estate.until_fabric_ready_at(&fabric_primary, SETTLE).await;
    let grown = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.expect("node.get [mesh]");
    let rpc: Vec<&NodeView> = grown.iter().filter(|n| n.name.kind == NodeKind::RpcNode).collect();
    assert_eq!(rpc.len(), 3, "mesh2 holds three rpc nodes: {grown:?}");
    assert!(rpc.iter().filter(|n| n.name != stopped_path).all(|n| n.status == NodeStatus::Ready), "every rpc node of mesh2 that is not stopped is ready: {grown:?}");
    let held = view_of(&estate.nodes_obj().await, &stopped_path).await;
    assert!(held.stopped && held.status == NodeStatus::Stopped, "{stopped_name} is still stopped after the resize: {held:?}");
    assert_eq!((&held.node_id, &held.incarnation_id), (&before.node_id, &before.incarnation_id), "the stopped birth is the same birth");
    assert!(alive(pid), "the stopped process {pid} is alive");
    assert_eq!(estate.pid_of(&stopped_name).await, pid, "the same process");

    // Its later start runs its gates.
    let started = frames(estate.nodes_obj().await.start(&stopped_path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    // The answering admin learns mesh2's rows by gossip: its view takes the start a moment after the stream's end.
    let after = estate.until_node(&stopped_name, "node.get lists the started birth ready again", Duration::from_secs(60), |n| n.filter(|n| !n.stopped && n.status == NodeStatus::Ready).cloned()).await;
    assert_eq!((&after.node_id, &after.incarnation_id), (&before.node_id, &before.incarnation_id), "a start keeps the birth");
    assert_eq!(estate.pid_of(&stopped_name).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let changes: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-change").into_iter().filter(|sp| attr(sp, "build_id") == build_id).collect();
    let entered = named(&spans, "rdm.node_admin.fabric.update.via-state-entered")
        .into_iter()
        .find(|sp| attr(sp, "to") == "ready" && changes.iter().any(|c| c["trace_id"] == sp["trace_id"]))
        .unwrap_or_else(|| panic!("the fabric entered ready under the resize Build {build_id}'s change: {changes:#?}"));
    let served = named(&spans, "rdm.node_admin.status.update.via-start-node").into_iter().filter(|sp| attr(sp, "node") == stopped_name && attr(sp, "outcome") == "started").count();
    assert_eq!(served, 1, "{stopped_name}'s start ran once after the resize");
    estate.record_trace_url(entered["trace_id"].as_str().unwrap_or(""));
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

/// mesh1 {1 node-admin, 1 rpc node} and mesh2 {`admins` node-admins, 1 rpc node}, settled and
/// ready, through the typed objects: the fabric-primary is mesh1.admin.1.
async fn two_meshes(test: &str, admins: u32) -> (Estate, Vec<NodeView>) {
    settled_two_meshes(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, admins).await
}

async fn settled_two_meshes(estate: Estate, admins: u32) -> (Estate, Vec<NodeView>) {
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
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
/// 5. NA2, the mesh's writer from that moment, records `is_primary` on its own row ("The write records
///    the role; it does not grant it").
/// 6. The fabric-primary installs NA2's answer and dispatches the stop, which NA2 executes as mesh2's
///    primary; every admin still answering names NA2 mesh2's primary (`node.get`).
/// Must NOT happen: a refusal of node.primary.update, a 0x21 call to P the fabric-primary did not make
/// or one to NA2 P did not make, a stop step before NA2 answered, or a view still naming P.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_hands_over_by_node_primary_update_before_it_stops() {
    let test = "a_stop_of_a_mesh_primary_hands_over_by_node_primary_update_before_it_stops";
    let (mut estate, before) = two_meshes(test, 2).await;
    let fp = before.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let p = before.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2's primary");
    let na2 = before.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && !n.is_primary).cloned().expect("mesh2's other node-admin");
    let (p_name, p_id, na2_name, na2_id) = (p.name.to_string(), p.node_id.to_string(), na2.name.to_string(), na2.node_id.to_string());
    assert_ne!(fp, p_name, "mesh2's primary is not the fabric-primary");

    let nodes = estate.nodes_obj().await;
    let stopped = frames(nodes.stop(&p.name).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    // Every admin of the fabric still answering names NA2 mesh2's primary, the fabric-primary included.
    for admin in before.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.name != p.name) {
        let view = estate.node_views_at(&admin.name).await.unwrap_or_else(|| panic!("{} answers node.get", admin.name));
        let primary: Vec<String> = view.iter().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).collect();
        assert_eq!(primary, vec![na2_name.clone()], "{} names {na2_name} mesh2's primary once the stop is complete", admin.name);
    }
    // Step 5: NA2 records `is_primary` on its own row, a local write of the mesh's writer.
    let recorded = || -> Option<u64> { estate.node_record(&na2_id).and_then(|r| r["record"]["births"].as_array().and_then(|b| b.last()).and_then(|b| b["is_primary"].as_u64())) };
    let deadline = std::time::Instant::now() + SETTLE;
    while recorded().is_none() {
        assert!(std::time::Instant::now() < deadline, "NA2 recorded is_primary on its own row in nodes.jsonl within {SETTLE:?}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    estate.stop().await;
    let spans = estate.spans();
    let one = |name: &str, f: &dyn Fn(&Value) -> bool| -> Value {
        let found: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| f(sp)).collect();
        assert_eq!(found.len(), 1, "one {name}: {found:#?}");
        found[0].clone()
    };
    let cmd = one("rdm.node_admin.primary.update.via-command", &|sp| attr(sp, "primary") == p_name);
    assert_eq!((attr(&cmd, "node"), attr(&cmd, "node_type"), attr(&cmd, "to"), attr(&cmd, "outcome")), (fp.clone(), "node-admin".to_string(), p_name.clone(), "updated".to_string()), "{cmd:#?}");
    let upd = one("rdm.node_admin.primary.update.via-update-primary", &|sp| attr(sp, "node") == p_name);
    assert_eq!((attr(&upd, "moving"), attr(&upd, "successor"), attr(&upd, "outcome")), ("mesh-primary".to_string(), na2_name.clone(), "updated".to_string()), "{upd:#?}");
    assert!(descends_from(&spans, &upd, &cmd), "P's update runs under the fabric-primary's command: {upd:#?}");
    let finished = one("rdm.node_admin.primary.update.via-issued-writes-finished", &|sp| attr(sp, "node") == p_name);
    let installed_at_na2 = one("rdm.node_admin.primary.update.via-primary-updated", &|sp| attr(sp, "node") == na2_name && attr(sp, "outcome") == "held");
    let entry = attr(&installed_at_na2, "primaries");
    assert!(entry.starts_with("node-admin ") && entry.contains(&p_id) && entry.contains(&format!("-> {na2_id}/")), "NA2 installed the mesh-primary entry P -> NA2 and answered node.primary.updated: {installed_at_na2:#?}");
    assert!(descends_from(&spans, &installed_at_na2, &upd), "NA2 installs under P's node.primary.update call: {installed_at_na2:#?}");
    assert!(named(&spans, "rdm.node_admin.primary.reject.via-primary-update").is_empty(), "no node.primary.update was refused");
    // The order of the sequence (steps 3, 4, 6).
    assert!(end_ns(&finished) <= start_ns(&installed_at_na2), "P finished its issued writes before NA2 installed");
    // The stop ran after NA2 answered, executed by NA2 as mesh2's primary.
    let stop_run = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| attr(sp, "executor") == na2_name).unwrap_or_else(|| panic!("NA2 executed the stop"));
    assert!(end_ns(&cmd) <= start_ns(stop_run), "the fabric-primary installed NA2's answer before it dispatched the stop");
    // The stop is an attempt of the Build that birthed P: its steps are that attempt's, never P's birth's.
    assert!(attr(&cmd, "attempt") != "1" && attr(stop_run, "attempt") != "1", "neither the command nor the stop dispatch is P's birth attempt");
    let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "node") == p_name && attr(sp, "build_id") == attr(&cmd, "build_id") && attr(sp, "attempt") == attr(stop_run, "attempt")).collect();
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

/// CONTRACT (the approved proposal Part C1 "The no-successor cases", Q7 ruled Luke 2026-10-12:
/// "`node.primary.update` commanded for a cohort with no other node of that type in the mesh is refused
/// by name, `cannot transfer: no secondary of type <node_type> in mesh <mesh>`"): mesh2 has one
/// node-admin and an rpc node. `node.stop` of mesh2.admin.1 is refused `last-admin` with exactly
/// `cannot transfer: no secondary of type node-admin in mesh mesh2`, under
/// `rdm.node_admin.build.reject.via-last-admin`. No node.primary.update is commanded or run, and every
/// node keeps its birth and its primaries.
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
    assert!(named(&spans, "rdm.node_admin.primary.update.via-command").is_empty(), "no node.primary.update was commanded");
    assert!(named(&spans, "rdm.node_admin.primary.update.via-update-primary").is_empty(), "no primary ran node.primary.update");
    estate.record_trace_url(rejected[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-SB1, Luke 2026-10-13: "a node.stop or node.restart of a mesh's only admin is refused
/// by name under Q7, no secondary, before anything moves. The stop path's via-role-hand-over to=none
/// is the defect: a refused transfer never erases the incumbent's role and never yields to nobody, and
/// the destructive stop does not continue"; the approved proposal Part C1 "The no-successor cases",
/// Q7: "`node.fabric-primary.update` with no other node-admin Ready in the mesh and no other
/// mesh-primary Ready is refused the same way, naming both searches ... a single-admin
/// single-mesh fabric's primary ... cannot be stopped or restarted until a secondary is Ready";
/// Part C2 step 3's text): mesh1.admin.1 is the fabric's only node-admin, holding mesh1's role and the
/// fabric-primary role, beside one rpc node. Its `node.stop` and its `node.restart` are each refused
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
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1)]).await;
    let before = estate.settled_views(&[("mesh1", 1, 1)]).await;
    // A role move is refused until the initial fabric has first reached ready (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.nodes_obj().await;
    let target: PathName = "mesh1.admin.1".parse().unwrap();
    let held = before.iter().find(|n| n.name == target).expect("node.get lists the only admin");
    assert!(held.is_primary && held.is_fabric_primary, "mesh1.admin.1 holds both roles: {held:?}");
    let q7 = "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready";
    for (action, refused) in [("stop", nodes.stop(&target).await.err())] {
        let refused = refused.unwrap_or_else(|| panic!("the {action} of the fabric's only node-admin was accepted"));
        let rdm_client::CallEnd::Rejected { reason, detail } = &refused else { panic!("the {action} ends Rejected: {refused:?}") };
        assert_eq!((reason.as_str(), detail.strip_prefix("last-admin: ")), ("last-admin", Some(q7)), "{action}: {refused:?}");
    }
    for now in estate.mesh_node_views("mesh1").await {
        let was = before.iter().find(|n| n.name == now.name).unwrap_or_else(|| panic!("{} was there before", now.name));
        assert_eq!((&now.incarnation_id, now.is_primary, now.is_fabric_primary), (&was.incarnation_id, was.is_primary, was.is_fabric_primary), "{} is untouched", now.name);
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
/// time. In a fabric of two meshes, mesh2's rpc node that is not its cohort's primary is stopped: it stops, declares its Stopped naming the
/// stop, and mesh2's primary publishes NodeStopped. Then mesh2's primary is stopped: its Build moves
/// mesh2's role to mesh2's other admin first, which folds the mesh's status rows (the stopped birth's
/// Stopped among them), and the old primary's own stop declares its Stopped naming that stop to the new
/// primary, which applies it and publishes the old primary's NodeStopped once. The stopped birth hears no
/// role move (its gossip was cut at its stop, node-stop.md step 3): it re-declares nothing to the new
/// primary, whose RoleFold already holds its row. The new primary starts the stopped birth, which rejoins
/// as the same birth. Must NOT happen: NodeStopped for the stopped birth twice (once per primary), or
/// the old primary's stop declared as a state it measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_primary() {
    let test = "a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_primary";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary == primary).map(|n| n.name.clone()).expect("mesh2 has two admins");
    let (primary, successor) = (admin_of(true), admin_of(false));
    assert!(!views.iter().any(|n| n.name == primary && n.is_fabric_primary), "mesh2's primary is not the fabric primary: the entry admin stays put");
    // The rpc node that is not its node type's primary: its stop moves no role of its own.
    let stopped_birth = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode && !n.is_primary).cloned().expect("mesh2 has an rpc node that is not its node type's primary");
    let stopped_node = stopped_birth.name.clone();
    let nodes = estate.nodes_obj().await;

    let stopped = frames(nodes.stop(&stopped_node).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let moved = frames(nodes.stop(&primary).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(moved.last(), Some(&Frame::Complete), "{moved:?}");
    let now = estate.mesh_node_views("mesh2").await;
    assert!(now.iter().any(|n| n.name == successor && n.is_primary), "{successor} holds mesh2's role: {now:?}");
    let started = frames(nodes.start(&stopped_node).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    // Start completes at the mesh-primary's ready apply. This entry admin is in another mesh:
    // wait for its projection, as the other stop/start cells wait for each admin's held row.
    let entry = estate.entry().await;
    let after = estate
        .until_view_at(&entry, "the entry holds the rejoined birth ready", Duration::from_secs(10), |view| {
            view.iter().find(|n| n.name == stopped_node && n.status == NodeStatus::Ready).cloned()
        })
        .await;
    assert_eq!((&after.node_id, &after.incarnation_id, after.status), (&stopped_birth.node_id, &stopped_birth.incarnation_id, NodeStatus::Ready), "the same birth rejoined: {after:?}");

    estate.stop().await;
    let spans = estate.spans();
    let (stopped_s, primary_s, successor_s) = (stopped_node.to_string(), primary.to_string(), successor.to_string());
    let stopped_of = |node: &str| -> Vec<&Value> { named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| attr(sp, "node") == node).collect() };
    let stopped_published = stopped_of(&stopped_s);
    assert_eq!(stopped_published.len(), 1, "NodeStopped for the stopped birth is published once across the role move: {stopped_published:#?}");
    let installed: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "node") == successor_s).collect();
    assert!(!installed.is_empty(), "{successor_s} installed itself as mesh2's primary");
    assert!(start_ns(stopped_published[0]) < start_ns(installed[0]), "the stopped birth's NodeStopped came from the primary before the move");
    let folded: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-successor-fold").into_iter().filter(|sp| attr(sp, "node") == successor_s && attr(sp, "member") == stopped_s).collect();
    // The stopped birth's Stopped naming its stop, declared to the new primary (the estate's teardown
    // owes every node's own Stopped too, naming no operation).
    let stopped_operation = format!("node.stop:{stopped_s}");
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
    let primary_stop = format!("node.stop:{primary_s}");
    let primary_declared: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declare")
        .into_iter()
        .filter(|sp| attr(sp, "node") == primary_s && attr(sp, "key").ends_with(", Stopped)") && attr(sp, "operation") == primary_stop)
        .collect();
    // The declaration rides an Owed call (i146 e3 owner 3): the new primary takes it applied, or already
    // applied when the stop stream's `stopped` reached it first (Q36: "from whichever arrives first").
    assert!(
        primary_declared.iter().any(|sp| attr(sp, "to") == successor_s && matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied")),
        "{primary_s}'s stop declared its Stopped naming the stop to {successor_s}: {primary_declared:#?}"
    );
    let primary_stopped = stopped_of(&primary_s);
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
/// retargeting. In a fabric of two meshes, mesh2's non-primary rpc node is restarted while mesh2's
/// primary holds the restart's node.started unanswered at its status door (the fault door's
/// declaration cut): the restart's wait for the node's readiness fails naming the node.started its
/// primary never took, the node live and its declaration owed. Then mesh2's primary is stopped,
/// which moves mesh2's role to mesh2's other admin first. The node hears the move: its call to the
/// old primary is cancelled (`via-primary-changed`) and it declares to the new primary, which applies it
/// once; the old primary decides nothing of it (its cut holds the one call it took). The create's next
/// attempt deletes the unready birth and creates it again (Q245, Q252), which fails too and ends the
/// create's Build failed; its birth is surplus to the accepted topology, so the fabric-primary's drift
/// pass is held at its drift seam until the declaration is delivered; released, drift's attempt deletes
/// the surplus node as a removal does: the provider's exited verdict, then NodeDeleted, one per birth. Must NOT happen: the
/// node.started applied twice, applied by the old primary, or addressed to an admin that does not hold
/// the role; a second declaration call to the old primary (a declaration loop of the node's own, which
/// Q52 deleted); a drift attempt before the release, or a second one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_owed_across_a_move_of_its_mesh_primary_is_delivered_once_to_the_new_mesh_primary() {
    let test = "a_declaration_owed_across_a_move_of_its_mesh_primary_is_delivered_once_to_the_new_mesh_primary";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary == primary).map(|n| n.name.clone()).expect("mesh2 has two admins");
    let (primary, successor) = (admin_of(true), admin_of(false));
    assert!(!views.iter().any(|n| n.name == primary && n.is_fabric_primary), "mesh2's primary is not the fabric primary: the entry admin stays put");
    let node: PathName = "mesh2.rpc.2".parse().unwrap();
    let (node_s, primary_s, successor_s) = (node.to_string(), primary.to_string(), successor.to_string());
    let fp = views.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let nodes = estate.nodes_obj().await;

    // The fabric-primary's drift pass is held: the failed create leaves its node surplus, and drift's
    // removal of it waits for the release.
    let drift = Door::at(&estate, &fp).await;
    drift.arm("drift-pass", serde_json::json!({"kind": "drift-pass", "node": fp})).await;
    // The old primary holds the node's node.started at its status door: the declaration stays owed.
    let door = Door::at(&estate, &primary_s).await;
    door.arm("declaration", serde_json::json!({"kind": "declaration", "node": node_s, "state": "Ready"})).await;
    let (based_on, request) = estate.create_node_request("mesh2", NodeKind::RpcNode).await;
    let restarted = frames(nodes.create_named(based_on, request).await.expect("the create was accepted")).await;
    assert!(
        matches!(restarted.last(), Some(Frame::Failed { reason, .. }) if reason.contains("WaitForNodeReady") && reason.contains("it took no node.started")),
        "the first start's wait for readiness fails naming the node.started its primary never took: {restarted:?}"
    );
    let hit = door.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node"], node_s.as_str(), "the old primary holds the node's declaration: {hit}");

    // The role moves: the old primary's stop runs node.primary.update first.
    let moved = frames(nodes.stop(&primary).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(moved.last(), Some(&Frame::Complete), "{moved:?}");
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let v = view_of(&estate.nodes_obj().await, &node).await;
        if v.status == NodeStatus::Ready {
            break;
        }
        assert!(std::time::Instant::now() < until, "the new primary sets the node ready from its declaration within 30 s: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Released: drift's attempt deletes the node the failed create left surplus.
    drift.wait_held("drift-pass").await;
    let released_at = unix_nanos();
    drift.release("drift-pass").await;
    estate.until_node(&node_s, "drift deletes the surplus node", SETTLE, |n| n.is_none().then_some(())).await;

    estate.stop().await;
    let spans = estate.spans();
    // The hold is the drift pass's own span on the fabric-primary, naming the cut, as long as the hold.
    let held: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-drift-held").into_iter().filter(|sp| attr(sp, "node") == fp).collect();
    assert!(!held.is_empty() && held.iter().all(|sp| attr(sp, "by") == "the testkit cut drift-pass"), "the fabric-primary's held drift pass names the cut: {held:#?}");
    let bare = |id: String| id.trim_start_matches("bid_").replace('-', "");
    let removal: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| attr(sp, "scope") == format!("surplus: {node_s}")).collect();
    assert_eq!(removal.len(), 1, "one drift attempt removes the surplus node: {removal:#?}");
    assert!(start_ns(removal[0]) >= released_at, "the drift attempt opened after the release, never before: {:#?}", removal[0]);
    let (build, attempt) = (bare(attr(removal[0], "build_id")), attr(removal[0], "attempt"));
    // The failed create leaves the birth Build accepted: the stop ran as its attempt, and so does drift's removal.
    let (Some(Frame::Started { build_id: birth, .. }), Some(Frame::Started { build_id: create, .. })) = (moved.first(), restarted.first()) else { panic!("{moved:?} {restarted:?}") };
    assert_eq!(build, birth.to_string(), "drift's attempt is of the accepted birth Build the stop ran on, never of the failed create's {create}: {:#?}", removal[0]);
    let exit =named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| bare(attr(sp, "build_id")) == build && attr(sp, "attempt") == attempt && attr(sp, "node") == node_s && attr(sp, "step") == "TerminateRuntime")
        .unwrap_or_else(|| panic!("drift's attempt {attempt} of {build} proves {node_s} exited"));
    assert_eq!(attr(exit, "outcome"), "complete", "{exit:#?}");
    // Every birth at the node's path.name, by its claim: the create's, and its next attempt's (Q245,
    // Q252: the next attempt deletes the unready birth the failed create left and creates it again).
    let births: std::collections::BTreeSet<String> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|sp| attr(sp, "subject") == node_s && attr(sp, "moment") == "claim").map(|sp| attr(sp, "node_id")).collect();
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| attr(sp, "node") == node_s).collect();
    // One NodeDeleted per exact birth (node_id, incarnation), and every birth deleted.
    let mut per_birth: std::collections::BTreeMap<(String, String), Vec<&Value>> = std::collections::BTreeMap::new();
    for sp in &deleted {
        per_birth.entry((attr(sp, "node_id"), attr(sp, "incarnation_id"))).or_default().push(sp);
    }
    assert!(per_birth.values().all(|d| d.len() == 1), "NodeDeleted once per birth of {node_s}: {per_birth:#?}");
    assert_eq!(per_birth.keys().map(|(id, _)| id.clone()).collect::<std::collections::BTreeSet<_>>(), births, "every birth the creates claimed is deleted, and no other: {per_birth:#?}");
    // Each deletion is explained: drift's attempt deletes the surplus birth; every other one is the
    // create's next attempt deleting the unready birth its failed first attempt left.
    let by_drift: Vec<&&Value> = deleted.iter().filter(|sp| (bare(attr(sp, "build_id")), attr(sp, "attempt")) == (build.clone(), attempt.clone())).collect();
    assert_eq!(by_drift.len(), 1, "drift's attempt deletes one birth, the surplus one: {deleted:#?}");
    let by_drift = by_drift[0];
    for sp in deleted.iter().filter(|sp| !std::ptr::eq(**sp, *by_drift)) {
        assert_eq!(bare(attr(sp, "build_id")), create.to_string(), "a birth drift did not delete is deleted by the create's next attempt: {sp:#?}");
        assert_eq!(attr(sp, "operation"), format!("node.delete:{node_s}"), "{sp:#?}");
        assert!(attr(sp, "attempt") != "1", "the create's first attempt deleted nothing: {sp:#?}");
    }
    assert!(end_ns(exit) <= start_ns(by_drift), "NodeDeleted follows the exited verdict: {exit:#?} vs {by_drift:#?}");
    let ready: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        // A first start names no operation; its measured Ready remains owed without an open command stream.
        .filter(|sp| attr(sp, "node") == node_s && attr(sp, "state") == "Ready" && attr(sp, "operation").is_empty())
        .collect();
    let applied: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "outcome") == "applied").collect();
    assert_eq!(applied.len(), 1, "the node.started is applied once: {ready:#?}");
    assert_eq!(attr(applied[0], "to"), successor_s, "by the new primary: {:#?}", applied[0]);
    assert!(ready.iter().all(|sp| attr(sp, "to") == primary_s || attr(sp, "to") == successor_s), "addressed only to the role's primaries: {ready:#?}");
    // Before the role moved, the declaration went to its primary alone: never to an admin that did not
    // yet hold the role.
    let installed = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "node") == successor_s).map(start_ns).min().expect("the successor installed itself");
    let early: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "to") == successor_s && start_ns(sp) < installed).collect();
    assert!(early.is_empty(), "no declaration to {successor_s} before it held the primary role: {early:#?}");
    let cancelled: Vec<&Value> = named(&spans, "rdm.node_rpc.status.remove.via-primary-changed").into_iter().filter(|sp| attr(sp, "node") == node_s).collect();
    // One call carried the declaration across the hold, in flight at the old primary's door until the
    // role moved: the node runs no declaration loop of its own (Q52).
    assert_eq!(cancelled.len(), 1, "one call to the old primary, cancelled once when the role moved: {cancelled:#?}");
    println!(
        "declaration across a role move: applied by {} (route {}); flame http://192.168.68.99:16687/trace/{}; drift held {} pass(es), then attempt {attempt} of {build} deleted {node_s}: flame http://192.168.68.99:16687/trace/{}",
        attr(applied[0], "to"),
        attr(applied[0], "route"),
        s(&applied[0]["trace_id"]),
        held.len(),
        s(&removal[0]["trace_id"])
    );
    estate.record_trace_url(applied[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-stop.md step 4: "declare `Stopped` to the mesh-primary naming the stop's operation,
/// an owed message (Node RPC §43) sent whether or not the stop call's stream is still open"; node-rpc.md
/// §43, "Owed messages: delivery is the runtime's"; step 5 and Q49: whichever of the stopped frame and the
/// declaration reaches the mesh-primary first is applied, and the other answers `AlreadyApplied`):
/// mesh2's rpc node is stopped while mesh2's primary holds the node's `Stopped` declaration unanswered at
/// its status door (the fault door's declaration cut) and holds the stop stream's `stopped` unrecorded
/// (its event cut), and that primary is then SIGKILLed: the role moves to mesh2's other admin, and the
/// stopped node, which left its mesh's channel at its transport cut, hears neither the death nor the
/// move. The stop's receipt and reply never wait for the declaration's answer: the Build's next attempt,
/// run by the new primary, sends the stop again, the birth admits it from its stopped cycle, the Build
/// completes and the node reads stopped. Must NOT happen: the birth held in Stopping on its owed
/// declaration, the next attempt's stop never admitted (release run 5 at 7e13f37e, rshape-authority
/// recovery: AwaitNodeLeft "stop-node was not admitted", DEADLINE_EXCEEDED).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_whose_mesh_primary_dies_holding_its_stopped_declaration_is_admitted_again_and_completes() {
    let test = "a_stop_whose_mesh_primary_dies_holding_its_stopped_declaration_is_admitted_again_and_completes";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).expect("mesh2's primary");
    assert!(!views.iter().any(|n| n.name.to_string() == primary && n.is_fabric_primary), "mesh2's primary is not the fabric-primary: the entry admin stays put");
    // The rpc node of mesh2 that holds no role: a primary's stop moves its role first.
    let node: PathName = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode && !n.is_primary).map(|n| n.name.clone()).expect("mesh2 has an rpc node that holds no primary role");
    let node_s = node.to_string();
    let nodes = estate.nodes_obj().await;
    let before = view_of(&estate.nodes_obj().await, &node).await;

    // mesh2's primary holds the node's Stopped declaration unanswered and the stop stream's stopped unrecorded.
    let door = Door::at(&estate, &primary).await;
    door.arm("declaration", serde_json::json!({"kind": "declaration", "node": node_s, "state": "Stopped"})).await;
    door.arm("stopped-held", serde_json::json!({"kind": "event", "event": "stopped", "node": node_s})).await;
    let stop = nodes.stop(&node).await.unwrap_or_else(|e| panic!("the stop of {node_s} was accepted: {e:?}"));
    let build = stop.accepted().build_id.clone();
    drop(stop);
    let hit = door.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node"], node_s.as_str(), "mesh2's primary holds the node's Stopped declaration: {hit}");
    // The primary dies holding it: the role moves to mesh2's other admin.
    estate.kill_node(&primary).await;

    // The Build's next attempt stops the birth again: admitted, the Build completes and the birth reads
    // stopped. A stop step that ends on its deadline ends the cell, naming it.
    let until = std::time::Instant::now() + SETTLE;
    loop {
        let unadmitted: Vec<Value> = named(&estate.spans(), "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .filter(|sp| attr(sp, "node") == node_s && attr(sp, "step") == "AwaitNodeLeft" && attr(sp, "code") == "DEADLINE_EXCEEDED")
            .cloned()
            .collect();
        assert!(unadmitted.is_empty(), "the next attempt's stop of {node_s} was never admitted: the birth held its stop in Stopping on its owed declaration: {unadmitted:#?}");
        let receipts = estate.carrier().await.build(&build).await;
        let complete = receipts.as_ref().is_ok_and(|b| b.phase == rdm_core::build::BuildPhase::Complete);
        let v = view_of(&estate.nodes_obj().await, &node).await;
        if complete && v.stopped {
            assert_eq!((v.status, &v.node_id, &v.incarnation_id), (NodeStatus::Stopped, &before.node_id, &before.incarnation_id), "the same birth, stopped: {v:?}");
            break;
        }
        assert!(std::time::Instant::now() < until, "the stop's Build {build:?} completes and {node_s} reads stopped within {SETTLE:?}: {receipts:?}, {v:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    estate.stop().await;
    let spans = estate.spans();
    // The birth owed its Stopped declaration naming its stop, and its stop step ran again under the next attempt.
    let operation = format!("node.stop:{node_s}");
    let owed: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-owed-on-stop").into_iter().filter(|sp| attr(sp, "node") == node_s && attr(sp, "operation") == operation).collect();
    assert_eq!(owed.len(), 1, "the birth owed its Stopped declaration once, naming its stop, and did not wait on it: {owed:#?}");
    let steps: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "node") == node_s && attr(sp, "step") == "StopNode").map(|sp| format!("{} attempt {}", attr(sp, "outcome"), attr(sp, "attempt"))).collect();
    assert!(steps.iter().any(|s| s.starts_with("complete")), "a stop step of {node_s} completed: {steps:?}");
    estate.record_trace_url(&s(&owed[0]["trace_id"]));
    println!("{node_s} stopped under {build:?}; its stop steps: {steps:?}");
}

/// CONTRACT (node-stop.md steps 2-4; node-mesh-leave.md; i146 Q55: "a signal stop is node.stop's own
/// steps"): a node-admin that is not its mesh's primary is sent SIGTERM. It runs node.stop's steps as
/// its own subject, as an rpc node does: the whole `node.connection.delete { All }` under
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
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary = views.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).expect("mesh1's primary");
    let member = views.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && !n.is_primary).map(|n| n.name.to_string()).expect("a node-admin that is not mesh1's primary");
    let pid = estate.pid_of(&member).await;
    assert!(alive(pid));

    // The signal: the admin's own stop, then its process ends.
    assert!(std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status().unwrap().success(), "kill -TERM {pid} ({member})");
    wait_for(&format!("{member} ends by its own stop"), Duration::from_secs(60), || async { (!alive(pid)).then_some(()) }).await;
    let ended = unix_nanos();
    let killed = estate.stop().await;
    assert_eq!(killed, Vec::<u32>::new(), "every runtime ended by its own stop before the bound's SIGKILL");

    let spans = estate.spans();
    // The signalled admin's spans are its birth's, before its process ended: a recovery at the same
    // name is another birth.
    let of = |name: &str, node: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| attr(sp, "node") == node && start_ns(sp) <= ended).collect() };
    let deleted = of("rdm.node_admin.connection.delete.via-stop", &member);
    assert_eq!(deleted.len(), 1, "{member}'s signal ran node.connection.delete {{ All }} once as the stop's: {deleted:#?}");
    assert_eq!(attr(deleted[0], "operation"), "signal", "{:#?}", deleted[0]);
    let owed = of("rdm.node_rpc.status.update.via-owed-on-signal", &member);
    let owed_state = |state: &str| owed.iter().find(|sp| attr(sp, "state") == state).copied();
    let deleting = owed_state("ConnectionsDeleting").unwrap_or_else(|| panic!("{member} owed its connections-deleting to its mesh-primary: {owed:#?}"));
    let stopped = owed_state("Stopped").unwrap_or_else(|| panic!("{member} owed its stopped state to its mesh-primary: {owed:#?}"));
    let leaving = of("rdm.node.mesh.update.via-leaving", &member);
    assert_eq!(leaving.len(), 1, "{member} left its mesh once: {leaving:#?}");
    assert_eq!(attr(leaving[0], "operation"), "signal", "{member}'s leave is its own stop's: {leaving:#?}");
    let left = of("rdm.node.mesh.update.via-left", &member);
    for which in ["own", "backbone"] {
        assert!(left.iter().any(|sp| attr(sp, "which") == which), "{member} left its {which} channel: {left:#?}");
    }
    let cut = of("rdm.node_admin.node.update.via-stop-transport-cut", &member);
    assert_eq!(cut.len(), 1, "{member}'s stop cut its transport once: {cut:#?}");
    assert_eq!(attr(cut[0], "operation"), "signal", "{:#?}", cut[0]);
    assert!(end_ns(deleted[0]) <= start_ns(leaving[0]), "the connections are deleted before the leave (node-mesh-leave.md step 1)");
    assert!(start_ns(deleting) <= start_ns(leaving[0]) && start_ns(leaving[0]) <= start_ns(stopped), "{member}: connections-deleting owed, then the leave, then stopped owed");
    let last_left = left.iter().map(|sp| start_ns(sp)).max().unwrap_or(0);
    let published_after: Vec<&Value> = of("rdm.mesh.node.update.via-heartbeat", &member).into_iter().filter(|h| start_ns(h) > last_left).collect();
    assert!(published_after.is_empty(), "{member} published nothing after it left its mesh: {published_after:#?}");
    // The mesh-primary's signal, at the estate's stop, gossips nothing for the stop (Q95): the one
    // final mesh topology is R-NS1 item 2's, a deleted mesh's last admin's on its node.delete.
    let aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| attr(sp, "node") == primary || attr(sp, "node") == member).collect();
    assert!(aggregate.is_empty(), "a signal stop publishes no mesh topology (Q95): {aggregate:#?}");
    estate.artifact("admin-signal-stop.json", &serde_json::json!({"member": member, "connections_deleting_owed": deleting["trace_id"], "left": leaving[0]["trace_id"], "stopped_owed": stopped["trace_id"], "primary": primary}));
    estate.record_trace_url(leaving[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-start.md "If join fails after start": `CommandAccepted`, then `Failed { step, reason,
/// code }`, nothing downstream; node-stop.md steps 2-4; node-mesh-leave.md): a stopped rpc node's
/// commanded start fails at a step (its `pre_node_ready` hook answers Failed). The birth is stopped again,
/// as it was, through node.stop's own steps, each naming the start's operation: the whole
/// `node.connection.delete { All }` (`via-stop`), its tasks held, `node.mesh.leave` (`via-leaving`,
/// `via-stop-transport-cut`), and `Stopped` owed to its mesh-primary (`via-owed-on-stop`); the start is
/// answered `StartFailed` at `node.pre-node-ready`, and the birth is stopped. Must NOT happen: the birth
/// left with its connections deleted and its mesh left but no `Stopped` owed, a stop step naming no
/// operation, or the start answered Started. In its own process: the cell holds process-wide state
/// (the stop flag).
#[test]
fn a_commanded_start_that_fails_stops_the_birth_again_through_node_stops_steps() {
    let cell = "a_commanded_start_that_fails_stops_the_birth_again_through_node_stops_steps";
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
        const NODE_START: &str = "node.start:mesh1.rpc.1";
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let Rig { primary: _primary, commander, _carried, running: _running, launch, key, dir, target, node_id, incarnation, .. } = rig_pre_node_ready("start-failed-stop", false, Some(pre_node_ready.clone())).await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k2v7m1q9t4xe".into(), attempt: 1, operation: format!("node.stop:{NODE}") };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k2v7m1q9t4xe".into(), attempt: 2, operation: NODE_START.into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        assert!(matches!(call(&commander, &target, &stop).await, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped");
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        // The start joins the commander back over its call; its hook then fails the start.
        pre_node_ready.set_mode(Mode::Fixed(HookOutcome::Failed { reason: "the rows of orders cannot be read".into() }));
        let reply = call(&commander, &target, &start).await;
        assert!(matches!(&reply, StatusReply::StartFailed { step, .. } if step == "node.pre-node-ready"), "the start is answered Failed at its hook: {reply:?}");
        // The failed start left the birth stopped, as it was.
        let probed = call(&commander, &target, &probe).await;
        assert!(matches!(probed, StatusReply::CurrentStopped { .. }), "the birth is stopped again: {probed:?}");

        // The flame: the stop's steps, each naming the start's operation.
        let of = |name: &str| -> Vec<std::collections::BTreeMap<String, String>> { spans_of(&spans, name, NODE).into_iter().filter(|f| f.get("operation").map(String::as_str) == Some(NODE_START)).collect() };
        let served = spans_of(&spans, "rdm.node_admin.status.update.via-start-node", NODE);
        assert!(served.iter().any(|f| f.get("outcome").map(String::as_str) == Some("start-failed") && f.get("failed_step").map(String::as_str) == Some("node.pre-node-ready")), "the start is served once, failed at node.pre-node-ready: {served:#?}");
        let deleted = of("rdm.node_admin.connection.delete.via-stop");
        assert_eq!(deleted.len(), 1, "the failed start ran node.connection.delete {{ All }} as the stop's, naming the start: {deleted:#?}");
        let leaving = of("rdm.node.mesh.update.via-leaving");
        assert_eq!(leaving.len(), 1, "the failed start left the mesh once, naming the start: {leaving:#?}");
        let cut = of("rdm.node_admin.node.update.via-stop-transport-cut");
        assert_eq!(cut.len(), 1, "the failed start cut the transport as the stop's step, naming the start: {cut:#?}");
        let owed: Vec<_> = of("rdm.node_rpc.status.update.via-owed-on-stop").into_iter().filter(|f| f.get("state").map(String::as_str) == Some("Stopped")).collect();
        assert_eq!(owed.len(), 1, "the failed start owed Stopped to the mesh-primary, naming the start: {owed:#?}");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// A real estate whose mesh-primary (the Day-0 admin, `mesh1.admin.1`) is held (SIGSTOP, alive and silent) when
/// `mesh1.rpc.1` receives SIGTERM, so the two declarations the stop owes its mesh-primary
/// (`ConnectionsDeleting`, then `Stopped`) are owed and unanswered at the signal. `release_after` is how
/// long the mesh-primary stays held before it is continued (`None`: held until the estate ends). Returns the
/// stopped node's `via-stop-declarations` span and the estate's spans.
async fn signal_stop_with_primary_held(test: &str, bound_ms: &str, release_after: Option<Duration>) -> (Value, Vec<Value>, String) {
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", &[("RDM_CONNECTIONS_DELETE_DEADLINE_MS", bound_ms)]).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await;
    estate.settled_views(&[("mesh1", 2, 1)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let primary_pid = estate.bootstrap_pid().expect("the Day-0 admin is mesh1's primary and the estate holds its pid");
    let pid = estate.pid_of(NODE).await;
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
    wait_for(&format!("{NODE} ends by its own stop"), Duration::from_secs(60), || async { ended(pid).then_some(()) }).await;
    let ended_in = signalled.elapsed();
    send("-KILL", primary_pid as u64);
    estate.stop().await;
    let spans = estate.spans();
    let outcome = named(&spans, "rdm.node_admin.node.update.via-stop-declarations").into_iter().find(|sp| attr(sp, "node") == NODE).cloned().unwrap_or_else(|| panic!("{NODE}'s signal stop ended on no outcome span"));
    estate.record_trace_url(outcome["trace_id"].as_str().unwrap_or(""));
    let trace = s(&outcome["trace_id"]);
    println!("{NODE} ended {ended_in:?} after its signal; outcome span: {outcome}");
    (outcome, spans, trace)
}

/// CONTRACT (Q210, node-rpc.md section 43: "a signal stop waits for the queue within Q206's bound and its
/// outcome span"; node-connections-delete.md): an rpc node holds owed declarations to its mesh-primary (its
/// mesh-primary is held, answering nothing) when it receives SIGTERM. Its stop waits for them: its process is
/// still alive after the signal and for as long as the mesh-primary stays silent, inside
/// `RDM_CONNECTIONS_DELETE_DEADLINE_MS`. The mesh-primary continued, the queue empties in the order owed and the
/// stop ends on its outcome span `rdm.node_admin.node.update.via-stop-declarations`, outcome `ended`, naming what
/// was delivered (`ConnectionsDeleting,Stopped`) and leaving nothing. Must NOT happen: the process ending
/// before its declarations are answered, or an outcome span that names neither what was delivered nor
/// what was left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stop_waits_for_its_owed_declarations_delivered_within_the_bound_and_names_them() {
    let (outcome, _, _) = signal_stop_with_primary_held("a_signal_stop_waits_for_its_owed_declarations_delivered_within_the_bound_and_names_them", "30000", Some(Duration::from_secs(3))).await;
    assert_eq!(attr(&outcome, "outcome"), "ended", "the queue was delivered inside the bound: {outcome}");
    assert_eq!(attr(&outcome, "delivered"), "ConnectionsDeleting,Stopped", "the outcome span names what was delivered, in the order owed: {outcome}");
    assert_eq!(attr(&outcome, "left"), "", "nothing was left: {outcome}");
    let elapsed: u64 = attr(&outcome, "elapsed_ms").parse().unwrap_or(0);
    assert!(elapsed >= 1_500, "the stop waited for the held mesh-primary's answers ({elapsed} ms): {outcome}");
}

/// CONTRACT (Q210, Q206): the same node, its mesh-primary held and never continued within the bound
/// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS` 3000). The stop waits the bound and no longer, then the process
/// leaves; its outcome span says `deadline`, delivered nothing, and names the undelivered declarations
/// (`ConnectionsDeleting,Stopped`) it left. Must NOT happen: the process waiting past the bound, or leaving
/// without naming what it left undelivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stop_past_the_bound_leaves_naming_its_undelivered_declarations() {
    let (outcome, _, _) = signal_stop_with_primary_held("a_signal_stop_past_the_bound_leaves_naming_its_undelivered_declarations", "3000", None).await;
    assert_eq!(attr(&outcome, "outcome"), "deadline", "the bound passed with the mesh-primary silent: {outcome}");
    assert_eq!(attr(&outcome, "delivered"), "", "nothing was delivered: {outcome}");
    assert_eq!(attr(&outcome, "left"), "ConnectionsDeleting,Stopped", "the outcome span names the undelivered declarations in the order owed: {outcome}");
    let elapsed: u64 = attr(&outcome, "elapsed_ms").parse().unwrap_or(0);
    assert!((3_000..8_000).contains(&elapsed), "the stop waited the bound and left ({elapsed} ms): {outcome}");
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
fn signal_stop_of<'a>(spans: &'a [Value], node: &str) -> Option<(&'a Value, &'a Value, &'a Value)> {
    let shutdown = named(spans, "rdm.mesh.node.update.via-leave-shutdown").into_iter().find(|sp| attr(sp, "node") == node)?;
    let tasks = named(spans, "rdm.mesh.node.update.via-leave-tasks").into_iter().find(|sp| attr(sp, "node") == node && sp["parent_span_id"] == shutdown["parent_span_id"])?;
    let signal = named(spans, "rdm.mesh.node.delete.via-signal").into_iter().find(|sp| sp["span_id"] == shutdown["parent_span_id"] && sp["trace_id"] == shutdown["trace_id"])?;
    Some((signal, tasks, shutdown))
}

/// CONTRACT (node-identity-and-state.md §4: "Shutdown is signal-driven. SIGINT and SIGTERM run the node's own
/// stop steps"; node-stop.md steps 2-4; node-mesh-leave.md Progress: "the node publishes nothing in that state
/// and nothing after it"; stop_gate.rs: work a stop deletes with the birth "ends at the stop and never
/// resumes"): mesh1's other node-admin, isolated (every datagram to or from its port dropped) while the
/// fabric-primary runs, runs fabric recovery's own-mesh arm on the unheard fabric-primary: its node
/// investigation's direct tickle reaches nobody, so the arm is open. The isolated admin then receives SIGTERM. Its arm ends at its stop, by name, before its
/// leave aborts its tasks, and no recovery is admitted by it after its signal stop began. Must NOT happen: the
/// arm outliving the stop (its span never written), or a `via-recovery` of the fabric-primary opened by the
/// stopping admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_stopped_admin_ends_its_open_fabric_recovery_arm_at_its_stop_and_admits_no_recovery() {
    use rdm_test::netfault::{udp_ports, Partition};
    let test = "a_signal_stopped_admin_ends_its_open_fabric_recovery_arm_at_its_stop_and_admits_no_recovery";
    let fp = "mesh1.admin.1";
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&primary["name"]), fp, "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let fp_id = s(&primary["node_id"]);
    let victim = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["name"] != fp).expect("mesh1's other node-admin")["name"]);
    let victim_pid = estate.pid_of(&victim).await;
    let cut = match Partition::isolate(&udp_ports(&nodes, &[victim.clone()])) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot drop {victim}'s UDP: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_at = unix_nanos();
    // The own-mesh arm is open: its node investigation's direct tickle of the fabric-primary is lost. The
    // signal is sent at that span: the fabric-primary's own recovery of the isolated admin, a floor or two
    // on, ends it otherwise, while its arm still tickles through its peers (Q111).
    let floor = rdm_transport::topology::staleness_floor();
    wait_for(&format!("{victim}'s own-mesh arm tickles {fp} and is not answered"), floor * 6 + Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node.tickle.resolve.via-direct").into_iter().find(|sp| start_ns(sp) >= cut_at && attr(sp, "caller") == victim && attr(sp, "node_id") == fp_id && !attr(sp, "failure").is_empty()).map(|_| ())
    })
    .await;
    assert!(std::process::Command::new("kill").args(["-TERM", &victim_pid.to_string()]).status().unwrap().success(), "kill -TERM {victim_pid}");
    // Its own stop's three spans written: the stop ran to its end and the process left.
    let (signal, tasks, _) = wait_for(&format!("{victim} leaves by its own signal stop"), Duration::from_secs(60), || async {
        let spans = estate.spans();
        signal_stop_of(&spans, &victim).map(|(a, b, c)| (a.clone(), b.clone(), c.clone()))
    })
    .await;
    drop(cut);
    estate.stop().await;
    let spans = estate.spans();
    let stop_began = named(&spans, "rdm.node_admin.connection.delete.via-stop").into_iter().find(|sp| attr(sp, "node") == victim && attr(sp, "operation") == "signal" && sp["trace_id"] == signal["trace_id"]).map(start_ns).unwrap_or_else(|| panic!("{victim}'s signal stop began with its connections delete: {signal}"));
    let arm = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().find(|sp| attr(sp, "node") == victim && start_ns(sp) >= cut_at).cloned().unwrap_or_else(|| panic!("{victim}'s fabric recovery arm, open at its signal, ended and was written before its process left"));
    assert!(end_ns(&arm) <= start_ns(&tasks), "{victim}'s arm ended at its stop, before its leave aborted its tasks: arm {arm}, leave-tasks {tasks}");
    assert!(attr(&arm, "outcome").contains("stop"), "the arm's end names the stop: {arm}");
    let after: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| attr(sp, "executor") == victim && start_ns(sp) >= stop_began).collect();
    assert!(after.is_empty(), "{victim} opened no recovery after its signal stop began: {after:#?}");
    estate.record_trace_url(&s(&signal["trace_id"]));
}

/// CONTRACT (node-rpc.md §35: "Handlers already running ... may finish ... In-flight work finishes under
/// node.connection.delete's own rules; its receipt is ... `Deadline { last_in_flight, closed, held }` at its
/// bound (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`)"; CLAUDE.md, that env var: the bound "on a signal stop alike:
/// how long in-flight handlers may finish before the node leaves"): the fabric-primary serves mesh2's
/// primary's claim of the drift attempt it opened for a killed mesh2 rpc node, and the testkit's
/// claimed-attempt cut holds that claim, so the call it serves never ends. The fabric-primary receives
/// SIGTERM. It leaves by its own signal stop within the bound's steps: its connections delete ends at the
/// bound, its leave waits for the unsettled call at most the bound, names it, and closes, and its
/// `via-signal` span is written as the process ends. Must NOT happen: the process held past its bound by the
/// call it serves, ended only by the estate's kill with no `via-signal` written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_serving_a_held_claim_leaves_on_its_signal_within_the_bound() {
    let test = "a_fabric_primary_serving_a_held_claim_leaves_on_its_signal_within_the_bound";
    let fp = "mesh1.admin.1";
    let bound = Duration::from_millis(3000);
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[("RDM_CONNECTIONS_DELETE_DEADLINE_MS", "3000")]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let fp_pid = estate.bootstrap_pid().expect("the Day-0 admin is the fabric-primary and the estate holds its pid") as u64;
    let fp_door = Door::at(&estate, fp).await;
    fp_door.arm("held-claim", serde_json::json!({"kind": "claimed-attempt"})).await;
    // mesh2's primary reads its killed rpc node exited and submits its recovery; the fabric-primary opens the
    // attempt, and mesh2's primary's claim of it is held at the fabric-primary.
    estate.kill_node("mesh2.rpc.1").await;
    let claimed = fp_door.wait_held("held-claim").await;
    assert_eq!(s(&claimed["hit"]["executor"]), "mesh2.admin.1", "the held claim is mesh2's primary's, served by the fabric-primary: {claimed}");
    let signalled = std::time::Instant::now();
    assert!(std::process::Command::new("kill").args(["-TERM", &fp_pid.to_string()]).status().unwrap().success(), "kill -TERM {fp_pid}");
    // The estate is the Day-0 admin's parent and reaps it only at its stop: an ended process is a zombie (`Z`).
    let ended = |pid: u64| std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |st| st.rsplit(')').next().is_some_and(|rest| rest.trim_start().starts_with('Z')));
    // The connections delete's bound, the declarations' bound and the leave's bound, and the steps between.
    let within = bound * 3 + Duration::from_secs(8);
    wait_for(&format!("{fp} ends by its own signal stop within {within:?} while the claim it serves is held"), within, || async { ended(fp_pid).then_some(()) }).await;
    let ended_in = signalled.elapsed();
    let killed = estate.stop().await;
    assert!(!killed.contains(&(fp_pid as u32)), "{fp} left by its own stop, not the estate's kill: {killed:?}");
    let spans = estate.spans();
    let (signal, _, shutdown) = signal_stop_of(&spans, fp).unwrap_or_else(|| panic!("{fp}'s signal stop wrote its via-signal, leave-tasks and leave-shutdown spans as it ended ({ended_in:?} after its signal)"));
    assert_eq!(attr(shutdown, "outcome"), "closed", "{fp}'s transport closed: {shutdown}");
    assert!(!attr(shutdown, "unsettled").is_empty(), "the leave names the call it served that was still unsettled at its bound: {shutdown}");
    println!("{fp} ended {ended_in:?} after its signal; leave-shutdown: {shutdown}");
    estate.record_trace_url(&s(&signal["trace_id"]));
}
