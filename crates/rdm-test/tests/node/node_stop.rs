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
/// The mesh primary records the start's Pending once, while it accepts the start's join report and
/// before it answers it, and takes the start's `NodeJoined` as already applied (node-mesh-join.md
/// step 10). No runtime exits, no node is created, nothing is repaired.
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
    // The commanded stop's steps; the estate's teardown runs the same workflow on its signal, named `signal`.
    let of_stop = |name: &str| -> Vec<&Value> { of_node(name).into_iter().filter(|sp| sp["attributes"]["operation"] != "signal").collect() };
    let stop_span = of_node("rdm.node_admin.status.update.via-stop-node");
    assert_eq!(stop_span.len(), 1, "{stop_span:?}");
    assert_eq!(of_stop("rdm.node_admin.node.connections.delete.via-stop").len(), 1, "the stop runs its node.connections.delete {{ All }} once");
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
    assert_eq!(named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == "node-left" && sp["attributes"]["subject"] == node).count(), 0, "the node gossiped no node-left");
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
    // ReadyForTraffic"), applied by the mesh primary.
    let ready: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "frame") == "node-started" && attr(sp, "operation").starts_with("node.start:")).collect();
    assert_eq!(ready.len(), 1, "{node}'s NodeStarted is recorded on its open start stream: {ready:#?}");
    assert!(declared_of("ReadyForTraffic", "node.start:").is_empty(), "the open start sends no separate Ready declaration");
    let sent = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().find(|sp| sp["attributes"]["node"] == node && sp["attributes"]["command"] == "start-node").expect("the start command was sent");
    let call = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().find(|sp| sp["trace_id"] == sent["trace_id"] && attr(sp, "op") == "34").expect("the start call was made on 0x22");
    // The stop's transport cut closed its connection, so the start dials afresh or rides a connection
    // its caller dialled to this birth after the cut: a response the caller owed the node (node-rpc.md
    // §43) may dial the same live birth between the two (i146 e1, the read of this cell's reds).
    if attr(call, "reused") != "false" {
        // A connection between the two births dialled after the cut, by either end: the caller's own
        // dial (within the call's attempts too), or the parked birth's dial to its mesh-primary for the
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
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "a parked node is not drift");
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

/// CONTRACT (node-name-update.md Precondition: "A stopped node is never renamed"; Validation: "the
/// precondition holds, else `RejectedAdmittingConnections { status, held_connections }`"; Failure and
/// receipts: refused before the claim, "nothing written, nothing changed"; node-start.md): the caller's
/// `node.name.update` of the stopped `mesh1.rpc.1` to the name `1.old` reaches its mesh-primary directly
/// and is refused `RejectedAdmittingConnections { status: Leaving, held_connections: [] }`, `Leaving`
/// being the status a stopped birth is held at; `node.get` lists the stopped birth at `mesh1.rpc.1` and
/// nothing at `mesh1.rpc.1.old`. `node.start` of `mesh1.rpc.1` makes the same process rejoin under its
/// own name: `node.joined`, `node.started`, `Complete`; `node.get` lists the same birth
/// `ready-for-traffic` at `mesh1.rpc.1`, and every other node holds it publishing that name. The
/// mesh-primary's `via-writer` span says `rejected-admitting-connections` with `traffic=Stopped` and
/// `connections=stopped`. What must NOT happen: `NameUpdated`, a row or a listing at `mesh1.rpc.1.old`,
/// or the start admitting the birth under another name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_nodes_rename_at_its_mesh_primary_is_refused_and_its_start_rejoins_under_its_own_name() {
    let test = "a_stopped_nodes_rename_at_its_mesh_primary_is_refused_and_its_start_rejoins_under_its_own_name";
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

    // The rename of a stopped node: one call to its mesh-primary, refused before anything is claimed.
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(parked.node_id.clone()), node_id: parked.node_id.clone(), incarnation: parked.incarnation_id.clone().expect("the parked node's birth") };
    let reply = nodes.name_update(&birth, &primary.node_id, &rdm_transport::RouteChoice::Direct, &renamed.name()).await.expect("node.name.update answered");
    assert_eq!(reply, StatusReply::RejectedAdmittingConnections { status: NodeStatus::Leaving, held_connections: vec![] }, "a stopped node is never renamed");
    let listed = view_of(&nodes, &path).await;
    assert!(listed.parked && listed.node_id == before.node_id && listed.incarnation_id == before.incarnation_id, "node.get lists the stopped birth at {path}: {listed:?}");
    assert_eq!(nodes.get(&NodeSelector::Mesh("mesh1".into())).await.expect("node.get").into_iter().filter(|n| n.name == renamed).count(), 0, "node.get lists nothing at {renamed}");

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
    let after = view_of(&nodes, &path).await;
    assert!(!after.parked && after.status == NodeStatus::ReadyForTraffic, "node.get lists {path} ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id, &after.endpoint_id, &after.transport_addr), (&before.node_id, &before.incarnation_id, &before.endpoint_id, &before.transport_addr), "the same birth");
    assert_eq!(estate.pid_of(NODE).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let update = named(&spans, "rdm.node_admin.node.name.update.via-writer").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).expect("the mesh-primary decided the node.name.update");
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
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(held.node_id.clone()), node_id: held.node_id.clone(), incarnation: held.incarnation_id.clone().expect("the stopped node's birth") };
    let carrier = estate.carrier().await;
    let reply = rdm_client::NodeRpc::new(carrier.client())
        .config_update(&primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .expect("node.config.update answered");
    assert_eq!(reply, StatusReply::ConfigUpdated { node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().unwrap() }, "a stopped node's config update completes on its row");

    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    // The config the join handed rides the started node's digests (R-JN1): the view carries it once the
    // first of them is heard, within a gossip interval of the start's completion.
    let until = std::time::Instant::now() + rdm_transport::membership::staleness_floor();
    let after = loop {
        let v = view_of(&nodes, &path).await;
        if v.config == told || std::time::Instant::now() >= until {
            break v;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
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

/// CONTRACT (R-JN1; node-replace.md steps 1-2 and Rollback step 2; node-start.md step 2; R12/R13;
/// CLAUDE.md read rule): in a mesh of three node-admins, the non-primary `mesh1.admin.3` has its
/// connections deleted, leaves its mesh and the backbone, is renamed `mesh1.admin.3.old` live and is
/// stopped there, as the predecessor's half of a replace recipe. Every admin lists the parked birth at
/// the row's name, `mesh1.admin.2`, which performed no rename, included: its view reads the name from
/// the row on the shared store root. `node.start` of `mesh1.admin.3.old` makes the same process
/// rejoin: its join presents `mesh1.admin.3.old`, the name it took off its mesh, and is admitted under
/// it (`via-join` `admitted_as`); the node installs the answer's name before it publishes
/// (`via-join-admitted` `installed`, then `via-ready` under that name). The stream carries
/// `node.joined`, `node.started` and ends `Complete`; every admin lists the same birth
/// `ready-for-traffic` at `mesh1.admin.3.old`, and the rpc node holds it publishing that name. What must
/// NOT happen: an admin listing the parked birth at `mesh1.admin.3`, the start refused (`UnknownNode`,
/// `JoinMismatch("name")`), or a span of the started node after its join naming `mesh1.admin.3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admin_renamed_off_its_mesh_and_stopped_starts_under_the_name_it_holds_on_every_admin() {
    let test = "a_node_admin_renamed_off_its_mesh_and_stopped_starts_under_the_name_it_holds_on_every_admin";
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

    // The predecessor's half of a replace: its connections, its mesh, its name live, then its stop.
    use rdm_core::build::RecipeStep;
    let steps = vec![
        RecipeStep::DeleteConnections { node: path.clone() },
        RecipeStep::LeaveMesh { node: path.clone() },
        RecipeStep::Update { node: path.clone(), name: renamed.clone() },
        RecipeStep::Stop { node: renamed.clone() },
    ];
    let half = frames(nodes.recipe(steps).await.expect("the recipe was accepted")).await;
    assert_eq!(half.last(), Some(&Frame::Complete), "{half:?}");
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
    assert_eq!((s(&join["attributes"]["node"]), s(&join["attributes"]["admitted_as"])), (renamed.to_string(), renamed.to_string()), "the join presents the name the node took off its mesh and is admitted under it: {join:#?}");
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

/// CONTRACT (states.md "Who sets each state": node stopping is set by the node's mesh-primary on the
/// stop call's admission; gossip.md frame catalog: `NodeLeaving` is a reserved position, never
/// published): the process of an rpc node of mesh1 that is not the rpc-node primary is held by the
/// operating system (SIGSTOP) and `node.stop` of it
/// is submitted. Its attempt is dispatched only once the mesh-primary marks the birth silent (its
/// connection to the birth has outlived the idle timeout, the staleness floor), so the stop-node call
/// never reaches the birth and the stop ends `Failed`
/// naming the unadmitted command, and the mesh-primary sets nothing: `node.get` lists the birth
/// neither stopping nor parked, and no `from-command-admitted` sets it `Leaving`. Released (SIGCONT),
/// the same stop submitted again is accepted, not refused `node-not-live`, and parks the same birth:
/// the parked birth owes its Stopped declaration naming its stop and replies `stopped` on the stream, so
/// the mesh-primary publishes NodeStopped once, from whichever reaches it first, the other answering
/// `AlreadyApplied` (i146 Q36, Q49), and the admission (`via-command-sent` admission `stopped`) sets
/// nothing. Must NOT happen: stopping set before or without admission, NodeStopped
/// twice, a node-leaving frame sent or heard by anyone, a resubmission refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth() {
    let test = "a_stop_the_birth_never_admitted_sets_no_stopping_and_its_resubmission_parks_the_birth";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(SETTLE).await;
    let fp = views.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    // A member of mesh1 that is not its type's primary: a primary's stop runs node.primary.update first
    // (lifecycles/node-primary-update.md), which a held process cannot answer.
    let node = views.iter().filter(|n| n.mesh == "mesh1" && n.kind == NodeKind::RpcNode && !n.is_primary).map(|n| n.name.to_string()).min().expect("an rpc node of mesh1 that is not the rpc-node primary");
    let node = node.as_str();
    let nodes = estate.nodes_obj().await;
    let path: PathName = node.parse().unwrap();
    let before = view_of(&nodes, &path).await;
    let pid = estate.pid_of(node).await;

    // The hold: the stop's attempt is held after its claim; the operating system suspends the exact
    // runtime; the attempt is dispatched once the mesh-primary's ladder records the birth os-stopped,
    // so no connection to it lives and the stop's call cannot reach it.
    let door = Door::at(&estate, &fp).await;
    door.arm("claimed", serde_json::json!({"kind": "claimed-attempt"})).await;
    let stop = tokio::spawn({
        let nodes = estate.nodes_obj().await;
        let path = path.clone();
        async move { frames(nodes.stop(&path).await.expect("the stop was accepted")).await }
    });
    door.wait_held("claimed").await;
    let mut held = Held { pid, held: false };
    held.signal("-STOP", true);
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        // The unheard mark alone proves no connection is gone: the idle timer runs from the birth's last
        // packet, the mark from its last digest. The ladder's os-stopped row follows its direct probe's
        // failure, past the idle timeout.
        let v = view_of(&nodes, &path).await;
        if v.os_stopped.is_some() {
            break;
        }
        assert!(std::time::Instant::now() < until, "the mesh-primary's ladder records the held birth os-stopped within 30 s: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    door.release("claimed").await;
    let refused = stop.await.expect("the stop's call");
    let Some(Frame::Started { build_id, .. }) = refused.first().cloned() else { panic!("{refused:?}") };
    assert!(matches!(refused.last(), Some(Frame::Failed { reason, .. }) if reason.contains("stop-node was not admitted")), "the stop fails naming the unadmitted command: {refused:?}");

    // Released: once its mesh-primary hears the birth again (its unheard marks gone: PendingReconnect,
    // and Dead with its os-stopped row while the offline ladder held it), it is commanded back through
    // node.mesh.join and runs its start sequence (node-recovery.md step 2, Q87): published Pending at
    // that join, then ready-for-traffic once its hook has passed (node-mesh-join.md step 10). Nothing
    // was admitted, so nothing is stopping and nothing is parked at any step of it.
    held.signal("-CONT", false);
    let until = std::time::Instant::now() + Duration::from_secs(60);
    let heard = loop {
        let v = view_of(&nodes, &path).await;
        assert!(!v.parked, "a stop the birth never admitted parks nothing: {v:?}");
        if v.status == NodeStatus::ReadyForTraffic && !matches!(v.declared.as_deref(), Some("Stopped" | "Leaving")) {
            break v;
        }
        assert!(std::time::Instant::now() < until, "the released birth is heard again and ready-for-traffic within 60 s: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(heard.status == NodeStatus::ReadyForTraffic && !heard.parked, "a stop the birth never admitted leaves it neither stopping nor parked: {heard:?}");
    assert_eq!((&heard.node_id, &heard.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth");

    // The same stop, submitted again, is accepted and parks the same birth.
    let stopped = frames(nodes.stop(&path).await.expect("the resubmitted stop was accepted")).await;
    assert!(matches!(stopped.first(), Some(Frame::Started { build_id: b, .. }) if *b == build_id), "the resubmitted stop is an attempt of the same Build: {stopped:?}");
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let parked = view_of(&nodes, &path).await;
    assert!(parked.parked && parked.status == NodeStatus::Leaving, "node.get lists {node} parked: {parked:?}");
    assert_eq!((&parked.node_id, &parked.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth");

    estate.stop().await;
    let spans = estate.spans();
    let build = build_id.to_string();
    let of = |name: &str, attempt: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "attempt") == attempt && attr(sp, "operation") == format!("node.stop:{node}")).collect() };
    // The two stop-node calls of the Build, in order: the one the held birth never received, then the admitted one.
    let sent: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-sent").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "operation") == format!("node.stop:{node}")).collect();
    assert_eq!(sent.iter().map(|sp| attr(sp, "admission")).collect::<Vec<_>>(), vec!["not-sent".to_string(), "stopped".to_string()], "the first stop-node never reached the held birth and the second was admitted: {sent:#?}");
    let (first, second) = (attr(sent[0], "attempt"), attr(sent[1], "attempt"));
    let stopping = |attempt: &str| -> Vec<&Value> { of("rdm.mesh.membership.update.from-command-admitted", attempt).into_iter().filter(|sp| attr(sp, "member") == node && attr(sp, "status") == "Leaving").collect() };
    assert!(stopping(&first).is_empty(), "nothing set the unadmitted birth stopping: {:#?}", stopping(&first));
    let admitted_sent = vec![sent[1]];
    let node_stopped = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().find(|sp| attr(sp, "node") == node && attr(sp, "attempt") == second).expect("NodeStopped is published for the admitted stop");
    // i146 Q36 (node-stop.md steps 4-5): the parked birth owes its Stopped declaration naming its stop
    // and replies `stopped` on the stream; whichever reaches the mesh-primary first publishes NodeStopped,
    // once, by the time the admission is taken, and the other answers AlreadyApplied (Q49). The admission
    // then sets nothing, since the birth is already stopped and states move only forward.
    assert_eq!(named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| attr(sp, "node") == node).count(), 1, "NodeStopped is published once for the birth");
    assert!(start_ns(node_stopped) <= end_ns(admitted_sent[0]), "NodeStopped, from the stream's stopped or the birth's declaration, is published by the time the admission is taken");
    let set = stopping(&second);
    assert!(set.iter().all(|sp| start_ns(sp) < start_ns(node_stopped)), "CommandAccepted may set stopping only before NodeStopped, never after it: {set:#?}");
    // i146 Q39: the stop's declaration goes to the holder of mesh1's primary seat, direct or carried,
    // never to an admin the released birth merely hears.
    let primary = views.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).expect("mesh1's primary");
    let leaving: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        .filter(|sp| attr(sp, "node") == node && attr(sp, "state") == "Stopped" && attr(sp, "operation") == format!("node.stop:{node}"))
        .collect();
    assert!(leaving.iter().all(|sp| attr(sp, "to") == primary), "the stop's declaration is addressed to mesh1's primary only: {leaving:#?}");
    assert!(leaving.iter().any(|sp| matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied")), "and taken by it, direct or carried: applied, or already applied when the stream's stopped reached it first (Q36, Q49): {leaving:#?}");
    for kind in ["node-leaving", "node-left"] {
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame").into_iter().filter(|sp| attr(sp, "kind") == kind && attr(sp, "subject") == node).collect();
        assert!(heard.is_empty(), "no {kind} frame is published or heard: {heard:#?}");
    }
    assert!(named(&spans, "rdm.mesh.membership.reject.via-node-leaving").is_empty(), "no node-leaving frame reached any receiver");
    assert!(named(&spans, "rdm.node_admin.node.reject.via-not-live").into_iter().all(|sp| attr(sp, "node") != node), "the resubmitted stop was not refused node-not-live");
    let trace = sent[0]["trace_id"].as_str().unwrap_or("").to_string();
    println!("unadmitted stop: http://192.168.68.99:16687/trace/{trace}");
    println!("admitted stop:   http://192.168.68.99:16687/trace/{}", admitted_sent[0]["trace_id"].as_str().unwrap_or(""));
    estate.record_trace_url(&trace);
}

/// CONTRACT (i146 Q29, Q36; node-stop.md step 5): `node.stop` of `mesh1.rpc.1` runs and the birth parks;
/// the stop stream's `stopped` reaches the mesh-primary and is held there before it is recorded (the
/// faulted admin's `stopped` event cut). The parked birth's declaration of Stopped, naming the stop's
/// operation, reaches the mesh-primary meanwhile: it is applied, and the mesh-primary publishes
/// NodeStopped from it with the stop's identity, so `node.get` lists the birth parked while the stream's
/// `stopped` is still held. Released, the stream's `stopped` finds the birth's row held (already
/// applied) and the stop completes. Must NOT happen: NodeStopped twice, NodeStopped from the held
/// stream, a node-leaving frame sent or heard by anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_whose_stream_stopped_is_held_is_published_stopped_once_from_the_births_declaration() {
    let test = "a_stop_whose_stream_stopped_is_held_is_published_stopped_once_from_the_births_declaration";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 3)]).await;
    // The initial fabric refuses a node stop until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(SETTLE).await;
    let fp = views.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let nodes = estate.nodes_obj().await;
    let path: PathName = NODE.parse().unwrap();
    let before = view_of(&nodes, &path).await;

    let door = Door::at(&estate, &fp).await;
    door.arm("stopped-held", serde_json::json!({"kind": "event", "event": "stopped", "node": NODE})).await;
    let stop = tokio::spawn({
        let nodes = estate.nodes_obj().await;
        let path = path.clone();
        async move { frames(nodes.stop(&path).await.expect("the stop was accepted")).await }
    });
    door.wait_held("stopped-held").await;
    // The stream's stopped is held at the mesh-primary: the birth is listed parked from its own
    // declaration alone.
    let until = std::time::Instant::now() + Duration::from_secs(30);
    let parked = loop {
        let v = view_of(&nodes, &path).await;
        if v.parked {
            break v;
        }
        assert!(std::time::Instant::now() < until, "the parked birth's declaration lists it parked within 30 s while the stream's stopped is held: {v:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!((parked.status, &parked.node_id, &parked.incarnation_id), (NodeStatus::Leaving, &before.node_id, &before.incarnation_id), "the same birth, parked");
    assert_eq!(door.cut("stopped-held").await["held"], true, "the stream's stopped is still held: {}", door.cut("stopped-held").await);
    door.release("stopped-held").await;
    let stopped = stop.await.expect("the stop's call");
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");

    estate.stop().await;
    let spans = estate.spans();
    let operation = format!("node.stop:{NODE}");
    let declared = named(&spans, "rdm.node_rpc.status.update.via-declare-own").into_iter().find(|sp| attr(sp, "node") == NODE && attr(sp, "state") == "Stopped" && attr(sp, "operation") == operation && attr(sp, "outcome") == "applied").expect("the parked birth's declaration of Stopped naming its stop was applied");
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
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame").into_iter().filter(|sp| attr(sp, "kind") == kind && attr(sp, "subject") == NODE).collect();
        assert!(heard.is_empty(), "no {kind} frame is published or heard: {heard:#?}");
    }
    let trace = node_stopped[0]["trace_id"].as_str().unwrap_or("").to_string();
    println!("NodeStopped from the declaration: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// SIGCONT on drop: a held node is never left stopped.
struct Held {
    pid: u64,
    held: bool,
}

impl Held {
    fn signal(&mut self, sig: &str, held: bool) {
        let ok = std::process::Command::new("kill").args([sig, &self.pid.to_string()]).status().unwrap().success();
        assert!(ok, "kill {sig} {}", self.pid);
        self.held = held;
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if self.held {
            let _ = std::process::Command::new("kill").args(["-CONT", &self.pid.to_string()]).status();
        }
    }
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

fn unix_nanos() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("after the epoch").as_nanos() as u64
}

/// mesh1 {1 node-admin, 1 rpc node} and mesh2 {`admins` node-admins, 1 rpc node}, settled and
/// ready-for-traffic, through the typed objects: the fabric-primary is mesh1.admin.1.
async fn two_meshes(test: &str, admins: u32) -> (Estate, Vec<NodeView>) {
    settled_two_meshes(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, admins).await
}

async fn settled_two_meshes(estate: Estate, admins: u32) -> (Estate, Vec<NodeView>) {
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 1, 1), ("mesh2", admins, 1)]).await;
    // A seat move is refused until the initial fabric has first reached ready-for-traffic (R-PB1).
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
///    carries and answers node.primary.updated (`seat.update.via-primary-updated` at NA2, outcome
///    `held`, the mesh-primary entry P -> NA2, under P's update).
/// 5. NA2, the mesh's writer from that moment, records `is_primary` on its own row ("The write records
///    the role; it does not grant it").
/// 6. The fabric-primary installs NA2's answer and dispatches the stop, which NA2 executes as mesh2's
///    primary; every admin still answering names NA2 mesh2's primary (`node.get`).
/// Must NOT happen: a refusal of node.primary.update, a 0x21 call to P the fabric-primary did not make
/// or one to NA2 P did not make, a stop step before NA2 answered, or a view still naming P.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops() {
    let test = "a_stop_of_a_mesh_primary_moves_its_seat_by_node_primary_update_before_it_stops";
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
    let contact = estate.fabric_store_root().join("nodes").join("mesh2").join("contacts").join(format!("{na2_id}.json"));
    let recorded = |at: &std::path::Path| -> Option<u64> { std::fs::read(at).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok()).and_then(|r| r["record"]["is_primary"].as_u64()) };
    let deadline = std::time::Instant::now() + SETTLE;
    while recorded(&contact).is_none() {
        assert!(std::time::Instant::now() < deadline, "NA2 recorded is_primary on its own row at {} within {SETTLE:?}", contact.display());
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
    let installed_at_na2 = one("rdm.node_admin.seat.update.via-primary-updated", &|sp| attr(sp, "node") == na2_name && attr(sp, "outcome") == "held");
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
/// by name under Q7, no secondary, before anything moves. The stop path's via-seat-hand-over to=none
/// is the defect: a refused transfer never erases the incumbent's seat and never yields to nobody, and
/// the destructive stop does not continue"; the approved proposal Part C1 "The no-successor cases",
/// Q7: "`node.fabric-primary.update` with no other node-admin ReadyForTraffic in the mesh and no other
/// mesh-primary ReadyForTraffic is refused the same way, naming both searches ... a single-admin
/// single-mesh fabric's primary ... cannot be stopped or restarted until a secondary is ReadyForTraffic";
/// Part C2 step 3's text): mesh1.admin.1 is the fabric's only node-admin, holding mesh1's seat and the
/// fabric seat, beside one rpc node. Its `node.stop` and its `node.restart` are each refused
/// `last-admin` with exactly `cannot transfer fabric-primary: no other node-admin ReadyForTraffic in mesh
/// mesh1, no other mesh-primary ReadyForTraffic`, one `rdm.node_admin.build.reject.via-last-admin` per
/// call. Every node keeps its birth and its primaries (node.get). The stop is not run: no stop-node
/// served, no node.connections.delete, no node.primary.update commanded or run. Through the whole run,
/// the estate's teardown included, no admin hands its primaries to nobody
/// (`rdm.node_admin.build.update.via-seat-hand-over` with `to = none`).
/// Must NOT happen: a stop or restart step of the only admin, a node.primary.update, a hand-over to none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_or_restart_of_the_fabrics_only_node_admin_is_refused_by_name_and_nothing_moves() {
    let test = "a_stop_or_restart_of_the_fabrics_only_node_admin_is_refused_by_name_and_nothing_moves";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 1)]).await;
    let before = estate.settled_views(&[("mesh1", 1, 1)]).await;
    // A seat move is refused until the initial fabric has first reached ready-for-traffic (R-PB1).
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.nodes_obj().await;
    let target: PathName = "mesh1.admin.1".parse().unwrap();
    let held = before.iter().find(|n| n.name == target).expect("node.get lists the only admin");
    assert!(held.is_primary && held.is_fabric_primary, "mesh1.admin.1 holds both seats: {held:?}");
    let q7 = "cannot transfer fabric-primary: no other node-admin ReadyForTraffic in mesh mesh1, no other mesh-primary ReadyForTraffic";
    for (action, refused) in [("stop", nodes.stop(&target).await.err()), ("restart", nodes.restart(&target).await.err())] {
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
    assert_eq!(rejected.len(), 2, "one refusal per call: {rejected:#?}");
    for name in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.node.connections.delete.via-stop", "rdm.node_admin.primary.update.via-command", "rdm.node_admin.primary.update.via-update-primary"] {
        // The estate's teardown runs the stop workflow on its signal (`signal`): not the refused stop.
        let ran: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| attr(sp, "operation") != "signal").collect();
        assert!(ran.is_empty(), "the refused stop ran {name}: {ran:#?}");
    }
    let to_none: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-seat-hand-over").into_iter().filter(|sp| attr(sp, "to") == "none").collect();
    assert!(to_none.is_empty(), "an admin handed its seat to nobody: {to_none:#?}");
    estate.record_trace_url(rejected[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i146 Q37; node-status.md "Status report"; node-stop.md steps 4-5; node-primary-update.md
/// steps 5-8; the seat taker's SeatFold, R14): a stopped birth's stop is published NodeStopped once,
/// by the mesh primary that applied its Stopped, and a move of its mesh's seat publishes it no second
/// time. In a fabric of two meshes, mesh2's rpc node that is not its cohort's primary is stopped: it stops, declares its Stopped naming the
/// stop, and mesh2's primary publishes NodeStopped. Then mesh2's primary is stopped: its Build moves
/// mesh2's seat to mesh2's other admin first, which folds the mesh's status rows (the stopped birth's
/// Stopped among them), and the old holder's own stop declares its Stopped naming that stop to the new
/// holder, which applies it and publishes the old holder's NodeStopped once. The stopped birth hears no
/// seat move (its gossip was cut at its stop, node-stop.md step 3): it re-declares nothing to the new
/// holder, whose SeatFold already holds its row. The new holder starts the stopped birth, which rejoins
/// as the same birth. Must NOT happen: NodeStopped for the stopped birth twice (once per holder), or
/// the old holder's stop declared as a state it measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_seat() {
    let test = "a_stopped_births_stop_is_published_once_across_a_move_of_its_mesh_seat";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary == primary).map(|n| n.name.clone()).expect("mesh2 has two admins");
    let (holder, successor) = (admin_of(true), admin_of(false));
    assert!(!views.iter().any(|n| n.name == holder && n.is_fabric_primary), "mesh2's primary is not the fabric primary: the entry admin stays put");
    // The rpc node that is not its cohort's primary: its stop moves no seat of its own.
    let stopped_birth = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode && !n.is_primary).cloned().expect("mesh2 has an rpc node that is not its cohort's primary");
    let stopped_node = stopped_birth.name.clone();
    let nodes = estate.nodes_obj().await;

    let stopped = frames(nodes.stop(&stopped_node).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let moved = frames(nodes.stop(&holder).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(moved.last(), Some(&Frame::Complete), "{moved:?}");
    let now = estate.mesh_node_views("mesh2").await;
    assert!(now.iter().any(|n| n.name == successor && n.is_primary), "{successor} holds mesh2's seat: {now:?}");
    let started = frames(nodes.start(&stopped_node).await.expect("the start was accepted")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    // Start completes at the mesh-primary's ready apply. This entry admin is in another mesh:
    // wait for its projection, as the other stop/start cells wait for each admin's held row.
    let entry = estate.entry().await;
    let after = estate
        .until_view_at(&entry, "the entry holds the rejoined birth ready", Duration::from_secs(10), |view| {
            view.iter().find(|n| n.name == stopped_node && n.status == NodeStatus::ReadyForTraffic).cloned()
        })
        .await;
    assert_eq!((&after.node_id, &after.incarnation_id, after.status), (&stopped_birth.node_id, &stopped_birth.incarnation_id, NodeStatus::ReadyForTraffic), "the same birth rejoined: {after:?}");

    estate.stop().await;
    let spans = estate.spans();
    let (stopped_s, holder_s, successor_s) = (stopped_node.to_string(), holder.to_string(), successor.to_string());
    let stopped_of = |node: &str| -> Vec<&Value> { named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| attr(sp, "node") == node).collect() };
    let stopped_published = stopped_of(&stopped_s);
    assert_eq!(stopped_published.len(), 1, "NodeStopped for the stopped birth is published once across the seat move: {stopped_published:#?}");
    let installed: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "node") == successor_s).collect();
    assert!(!installed.is_empty(), "{successor_s} installed itself as mesh2's primary");
    assert!(start_ns(stopped_published[0]) < start_ns(installed[0]), "the stopped birth's NodeStopped came from the holder before the move");
    let folded: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-seat-fold").into_iter().filter(|sp| attr(sp, "node") == successor_s && attr(sp, "member") == stopped_s).collect();
    // The stopped birth's Stopped naming its stop, declared to the new holder (the estate's teardown
    // owes every node's own Stopped too, naming no operation).
    let stopped_operation = format!("node.stop:{stopped_s}");
    let redeclared: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        .filter(|sp| attr(sp, "node") == stopped_s && attr(sp, "state") == "Stopped" && attr(sp, "operation") == stopped_operation && attr(sp, "to") == successor_s)
        .collect();
    assert!(redeclared.is_empty(), "a stopped birth hears no seat move, so it re-declares nothing to the new holder: {redeclared:#?}");
    assert!(folded.iter().any(|sp| attr(sp, "outcome") == "stopped"), "the new holder's SeatFold holds the stopped birth stopped: {folded:#?}");
    // The old holder's own stop: its Stopped names the stop, applied by the new holder, published once.
    let holder_stop = format!("node.stop:{holder_s}");
    let holder_declared: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declare")
        .into_iter()
        .filter(|sp| attr(sp, "node") == holder_s && attr(sp, "key").ends_with(", Stopped)") && attr(sp, "operation") == holder_stop)
        .collect();
    // The declaration rides an Owed call (i146 e3 owner 3): the new holder takes it applied, or already
    // applied when the stop stream's `stopped` reached it first (Q36: "from whichever arrives first").
    assert!(
        holder_declared.iter().any(|sp| attr(sp, "to") == successor_s && matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied")),
        "{holder_s}'s stop declared its Stopped naming the stop to {successor_s}: {holder_declared:#?}"
    );
    let holder_stopped = stopped_of(&holder_s);
    assert_eq!(holder_stopped.len(), 1, "NodeStopped for the old holder is published once: {holder_stopped:#?}");
    println!(
        "seat move: the new holder folded {} row(s) of the stopped birth ({:?}); flame {}",
        folded.len(),
        folded.iter().map(|sp| attr(sp, "outcome")).collect::<Vec<_>>(),
        stopped_published[0]["trace_id"].as_str().unwrap_or("")
    );
    estate.record_trace_url(holder_stopped[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (i146 Q39; node-status.md "Status report"; node-primary-update.md steps 5-8): a node's
/// declaration is owed to the holder of its mesh's primary seat, and the seat moving is its one
/// retargeting. In a fabric of two meshes, mesh2's non-primary rpc node is restarted while mesh2's
/// primary holds the restart's node.started unanswered at its status door (the fault door's
/// declaration cut): the restart's wait for the node's readiness fails naming the node.started its
/// primary never took, the node live and its declaration owed. Then mesh2's primary is stopped,
/// which moves mesh2's seat to mesh2's other admin first. The node hears the move: its call to the
/// old holder is cancelled (`via-seat-moved`) and it declares to the new holder, which applies it
/// once; the old holder decides nothing of it (its cut holds the one call it took). The create's
/// failure leaves the node surplus to the accepted topology, so the fabric-primary's drift pass is held
/// at its drift seam until the declaration is delivered; released, drift's attempt deletes the surplus
/// node as a removal does: the provider's exit proof, then NodeDeleted. Must NOT happen: the
/// node.started applied twice, applied by the old holder, or addressed to an admin that does not hold
/// the seat; a second declaration call to the old holder (a declaration loop of the node's own, which
/// Q52 deleted); a drift attempt before the release, or a second one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_owed_across_a_move_of_its_mesh_seat_is_delivered_once_to_the_new_holder() {
    let test = "a_declaration_owed_across_a_move_of_its_mesh_seat_is_delivered_once_to_the_new_holder";
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let admin_of = |primary: bool| views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary == primary).map(|n| n.name.clone()).expect("mesh2 has two admins");
    let (holder, successor) = (admin_of(true), admin_of(false));
    assert!(!views.iter().any(|n| n.name == holder && n.is_fabric_primary), "mesh2's primary is not the fabric primary: the entry admin stays put");
    let node: PathName = "mesh2.rpc.2".parse().unwrap();
    let (node_s, holder_s, successor_s) = (node.to_string(), holder.to_string(), successor.to_string());
    let fp = views.iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let nodes = estate.nodes_obj().await;

    // The fabric-primary's drift pass is held: the failed create leaves its node surplus, and drift's
    // removal of it waits for the release.
    let drift = Door::at(&estate, &fp).await;
    drift.arm("drift-pass", serde_json::json!({"kind": "drift-pass", "node": fp})).await;
    // The old holder holds the node's node.started at its status door: the declaration stays owed.
    let door = Door::at(&estate, &holder_s).await;
    door.arm("declaration", serde_json::json!({"kind": "declaration", "node": node_s, "state": "ReadyForTraffic"})).await;
    let restarted = frames(nodes.create(&rdm_client::NodeSpec { mesh: "mesh2".into(), kind: NodeKind::RpcNode }).await.expect("the create was accepted")).await;
    assert!(
        matches!(restarted.last(), Some(Frame::Failed { reason, .. }) if reason.contains("WaitForNodeReady") && reason.contains("it took no node.started")),
        "the first start's wait for readiness fails naming the node.started its primary never took: {restarted:?}"
    );
    let hit = door.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node"], node_s.as_str(), "the old holder holds the node's declaration: {hit}");

    // The seat moves: the old holder's stop runs node.primary.update first.
    let moved = frames(nodes.stop(&holder).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(moved.last(), Some(&Frame::Complete), "{moved:?}");
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let v = view_of(&nodes, &node).await;
        if v.status == NodeStatus::ReadyForTraffic {
            break;
        }
        assert!(std::time::Instant::now() < until, "the new holder sets the node ready from its declaration within 30 s: {v:?}");
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
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| attr(sp, "node") == node_s).collect();
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the surplus node: {deleted:#?}");
    assert_eq!((bare(attr(deleted[0], "build_id")), attr(deleted[0], "attempt")), (build.clone(), attempt.clone()), "NodeDeleted names drift's attempt: {:#?}", deleted[0]);
    assert!(end_ns(exit) <= start_ns(deleted[0]), "NodeDeleted follows the exit proof: {exit:#?} vs {:#?}", deleted[0]);
    let ready: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        // A first start names no operation; its measured Ready remains owed without an open command stream.
        .filter(|sp| attr(sp, "node") == node_s && attr(sp, "state") == "ReadyForTraffic" && attr(sp, "operation").is_empty())
        .collect();
    let applied: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "outcome") == "applied").collect();
    assert_eq!(applied.len(), 1, "the node.started is applied once: {ready:#?}");
    assert_eq!(attr(applied[0], "to"), successor_s, "by the new holder: {:#?}", applied[0]);
    assert!(ready.iter().all(|sp| attr(sp, "to") == holder_s || attr(sp, "to") == successor_s), "addressed only to the seat's holders: {ready:#?}");
    // Before the seat moved, the declaration went to its holder alone: never to an admin that did not
    // yet hold the seat.
    let installed = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "node") == successor_s).map(start_ns).min().expect("the successor installed itself");
    let early: Vec<&&Value> = ready.iter().filter(|sp| attr(sp, "to") == successor_s && start_ns(sp) < installed).collect();
    assert!(early.is_empty(), "no declaration to {successor_s} before it held the seat: {early:#?}");
    let cancelled: Vec<&Value> = named(&spans, "rdm.node_rpc.status.remove.via-seat-moved").into_iter().filter(|sp| attr(sp, "node") == node_s).collect();
    // One call carried the declaration across the hold, in flight at the old holder's door until the
    // seat moved: the node runs no declaration loop of its own (Q52).
    assert_eq!(cancelled.len(), 1, "one call to the old holder, cancelled once when the seat moved: {cancelled:#?}");
    println!(
        "declaration across a seat move: applied by {} (route {}); flame http://192.168.68.99:16687/trace/{}; drift held {} pass(es), then attempt {attempt} of {build} deleted {node_s}: flame http://192.168.68.99:16687/trace/{}",
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
/// (its event cut), and that primary is then SIGKILLed: the seat moves to mesh2's other admin, and the
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
    let mut estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 2, 1), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let holder = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).expect("mesh2's primary");
    assert!(!views.iter().any(|n| n.name.to_string() == holder && n.is_fabric_primary), "mesh2's primary is not the fabric-primary: the entry admin stays put");
    // The rpc node of mesh2 that holds no seat: a seat holder's stop moves its seat first.
    let node: PathName = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode && !n.is_primary).map(|n| n.name.clone()).expect("mesh2 has an rpc node that holds no seat");
    let node_s = node.to_string();
    let nodes = estate.nodes_obj().await;
    let before = view_of(&nodes, &node).await;

    // mesh2's primary holds the node's Stopped declaration unanswered and the stop stream's stopped unrecorded.
    let door = Door::at(&estate, &holder).await;
    door.arm("declaration", serde_json::json!({"kind": "declaration", "node": node_s, "state": "Stopped"})).await;
    door.arm("stopped-held", serde_json::json!({"kind": "event", "event": "stopped", "node": node_s})).await;
    let stop = nodes.stop(&node).await.unwrap_or_else(|e| panic!("the stop of {node_s} was accepted: {e:?}"));
    let build = stop.accepted().build_id.clone();
    drop(stop);
    let hit = door.wait_held("declaration").await;
    assert_eq!(hit["hit"]["node"], node_s.as_str(), "mesh2's primary holds the node's Stopped declaration: {hit}");
    // The holder dies holding it: the seat moves to mesh2's other admin.
    estate.kill_node(&holder).await;

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
        let receipts = estate.builds_obj().await.get(&build).await;
        let complete = receipts.as_ref().is_ok_and(|b| b.phase == rdm_core::build::BuildPhase::Complete);
        let v = view_of(&nodes, &node).await;
        if complete && v.parked {
            assert_eq!((v.status, &v.node_id, &v.incarnation_id), (NodeStatus::Leaving, &before.node_id, &before.incarnation_id), "the same birth, stopped: {v:?}");
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
/// its own subject, as an rpc node does: the whole `node.connections.delete { All }` under
/// `RDM_CONNECTIONS_DELETE_DEADLINE_MS` (`via-stop`, operation `signal`), its connections-deleting
/// owed to its mesh-primary; `node.mesh.leave` (`via-leaving`), its own mesh and the backbone each
/// left (`via-left` own and backbone), its Node RPC pool cut (`via-stop-transport-cut`); its `Stopped`
/// owed to its mesh-primary (`via-owed-on-signal`); then its process ends, before the estate's bound.
/// The mesh-primary admin, sent SIGTERM at the estate's stop, gossips nothing for its stop either
/// (node-stop.md step 4, R-ON1, Q95): no `via-leaving-aggregate`. Must NOT happen: a runtime the bound
/// kills, a heartbeat of the stopping admin after it left its mesh, a leave with no connections delete
/// before it, or a stopping aggregate from a signal.
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
    let deleted = of("rdm.node_admin.node.connections.delete.via-stop", &member);
    assert_eq!(deleted.len(), 1, "{member}'s signal ran node.connections.delete {{ All }} once as the stop's: {deleted:#?}");
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
    // final aggregate is R-NS1 item 2's, a deleted mesh's last admin's on its node.delete.
    let aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| attr(sp, "node") == primary || attr(sp, "node") == member).collect();
    assert!(aggregate.is_empty(), "a signal stop publishes no aggregate (Q95): {aggregate:#?}");
    estate.artifact("admin-signal-stop.json", &serde_json::json!({"member": member, "connections_deleting_owed": deleting["trace_id"], "left": leaving[0]["trace_id"], "stopped_owed": stopped["trace_id"], "primary": primary}));
    estate.record_trace_url(leaving[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-start.md "If join fails after start": `CommandAccepted`, then `Failed { step, reason,
/// code }`, nothing downstream; node-stop.md steps 2-4; node-mesh-leave.md): a stopped rpc node's
/// commanded start fails at a step (its `before_ready` hook answers Failed). The birth is stopped again,
/// as it was, through node.stop's own steps, each naming the start's operation: the whole
/// `node.connections.delete { All }` (`via-stop`), its tasks held, `node.mesh.leave` (`via-leaving`,
/// `via-stop-transport-cut`), and `Stopped` owed to its mesh-primary (`via-owed-on-stop`); the start is
/// answered `StartFailed` at `node.before-ready`, and the birth is parked. Must NOT happen: the birth
/// left with its connections deleted and its mesh left but no `Stopped` owed, a stop step naming no
/// operation, or the start answered Started. In its own process: the cell holds process-wide state
/// (the parking flag).
#[test]
fn a_commanded_start_that_fails_stops_the_birth_again_through_node_stops_steps() {
    let cell = "a_commanded_start_that_fails_stops_the_birth_again_through_node_stops_steps";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        use crate::rejoin_commander::{call, rig_before_ready, spans_of, Rig};
        use rdm_admin::before_ready::HookOutcome;
        use rdm_core::status::StatusRequest;
        use rdm_test::before_ready_probe::{Mode, TestBeforeReady};
        const NODE_START: &str = "node.start:mesh1.rpc.1";
        let before_ready = TestBeforeReady::new(Mode::Local);
        let Rig { primary: _primary, commander, _carried, running: _running, launch, key, dir, target, node_id, incarnation, .. } = rig_before_ready("start-failed-stop", false, Some(before_ready.clone())).await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k2v7m1q9t4xe".into(), attempt: 1, operation: format!("node.stop:{NODE}") };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k2v7m1q9t4xe".into(), attempt: 2, operation: NODE_START.into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        assert!(matches!(call(&commander, &target, &stop).await, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped");
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        // The start joins the commander back over its call; its hook then fails the start.
        before_ready.set_mode(Mode::Fixed(HookOutcome::Failed { reason: "the rows of orders cannot be read".into() }));
        let reply = call(&commander, &target, &start).await;
        assert!(matches!(&reply, StatusReply::StartFailed { step, .. } if step == "node.before-ready"), "the start is answered Failed at its hook: {reply:?}");
        // The failed start left the birth parked, as it was.
        let probed = call(&commander, &target, &probe).await;
        assert!(matches!(probed, StatusReply::CurrentParked { .. }), "the birth is parked again: {probed:?}");

        // The flame: the stop's steps, each naming the start's operation.
        let of = |name: &str| -> Vec<std::collections::BTreeMap<String, String>> { spans_of(&spans, name, NODE).into_iter().filter(|f| f.get("operation").map(String::as_str) == Some(NODE_START)).collect() };
        let served = spans_of(&spans, "rdm.node_admin.status.update.via-start-node", NODE);
        assert!(served.iter().any(|f| f.get("outcome").map(String::as_str) == Some("start-failed") && f.get("failed_step").map(String::as_str) == Some("node.before-ready")), "the start is served once, failed at node.before-ready: {served:#?}");
        let deleted = of("rdm.node_admin.node.connections.delete.via-stop");
        assert_eq!(deleted.len(), 1, "the failed start ran node.connections.delete {{ All }} as the stop's, naming the start: {deleted:#?}");
        let leaving = of("rdm.node.mesh.update.via-leaving");
        assert_eq!(leaving.len(), 1, "the failed start left the mesh once, naming the start: {leaving:#?}");
        let cut = of("rdm.node_admin.node.update.via-stop-transport-cut");
        assert_eq!(cut.len(), 1, "the failed start cut the transport as the stop's step, naming the start: {cut:#?}");
        let owed: Vec<_> = of("rdm.node_rpc.status.update.via-owed-on-stop").into_iter().filter(|f| f.get("state").map(String::as_str) == Some("Stopped")).collect();
        assert_eq!(owed.len(), 1, "the failed start owed Stopped to the mesh-primary, naming the start: {owed:#?}");
        let _ = std::fs::remove_dir_all(&dir);
    })
}
