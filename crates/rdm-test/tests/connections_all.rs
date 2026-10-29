//! `node.connection.delete { scope: All }` is just connections (R-CR5; lifecycles/node-connections-delete.md:
//! "A connection is an open Direct or Proxy entry of the node's connections log. Gossip neighbours and
//! the Node RPC pool are not connections, and neither scope touches them: a node that has had its
//! connections deleted is still heard on gossip"; lifecycles/node-name-update.md "Rename precondition").
//!
//! Its own process: the cells hold process-wide state (the stop flag).

use crate::common::{admin_side_serving, serve_ping, Spans};
use rdm_test::rig::AdminSide;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::{NotSentReason, RpcOutcome};
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, Status, StatusReply, StatusRequest, WhichMesh};
use rdm_test::pre_node_ready_probe::{PreNodeReadyPull, PullRequest, PullServer};
use rdm_test::node;
use rdm_test::originate::Seams;
use std::time::Duration;

async fn call_on(admin: &AdminSide, target: &NodeTarget, req: &StatusRequest, scope: Option<&str>) -> StatusReply {
    let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, scope: scope.map(str::to_string), ..CallOptions::default() };
    if let Some(reply) = crate::common::lifecycle_reply(&admin.observer.client, target, req, &opts, None).await { return reply; }
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

/// A live `mesh1.rpc.7` the admin side has heard Ready and resolves.
async fn live_node(admin: &AdminSide, dir: &std::path::Path, fabric: &FabricId) -> (Launch, node::RunningNode) {
    let (launch, running, _) = live_node_with(admin, dir, fabric, |b| b).await;
    (launch, running)
}

/// [`live_node`], serving what `register` adds, with its seams: the data-plane client it originates
/// its connections on and the connections writer that logs them.
async fn live_node_with(admin: &AdminSide, dir: &std::path::Path, fabric: &FabricId, register: impl FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder) -> (Launch, node::RunningNode, Seams) {
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    let key = node::load_or_mint_key(dir).unwrap();
    let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        name: "mesh1.rpc.7".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    };
    admin.deployed(&launch, &key);
    let seams: std::sync::Arc<std::sync::OnceLock<Seams>> = Default::default();
    let slot = seams.clone();
    let running = node::start_with_seams(&launch, move |b, s| {
        let _ = slot.set(s);
        register(b)
    })
    .await
    .expect("the node comes up");
    let seams = seams.get().cloned().expect("the node handed its seams");
    let digest_view = &admin.observer.topology_view.digest_view;
    let mut digests = digest_view.digest_changes();
    while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
        digests.changed().await.unwrap();
    }
    let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
    admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
    (launch, running, seams)
}

/// The node's data-plane Ping to its admin: a connection it originates, a Direct entry of its log once it opens.
async fn ping_admin(seams: &Seams, admin: &AdminSide, opts: &CallOptions) -> RpcOutcome<PingReply> {
    seams.client.call::<Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &PingRequest::Ping { payload: b"the node reaches its admin".to_vec() }, opts).await.0
}

fn fields_of(spans: &Spans, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

/// The refusal a call ended in when the node admits no new connection, or a panic naming what it ended in.
fn admission_blocked(out: &RpcOutcome<PingReply>) -> String {
    match out {
        RpcOutcome::NotSent(n) => match n.reason() {
            NotSentReason::AdmissionBlocked(why) => why.clone(),
            other => panic!("the call was not sent, for another reason than the admission block: {other:?}"),
        },
        other => panic!("a node whose connections were deleted opened a connection: the call ended {other:?}"),
    }
}

fn named(spans: &Spans, name: &str) -> usize {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).count()
}

/// CONTRACT: a live node's `node.connection.delete { All }` closes every open Direct or Proxy entry
/// of its connections log and nothing else. It answers `ConnectionsDeleted` holding nothing, and the
/// node is still heard on gossip: the admin side receives a later digest of the same birth. What must
/// NOT happen: the gossip channel stopped (`rdm.mesh.connection.delete.via-channel-stopped`), or the
/// Node RPC pool hard-cut (`rdm.node_rpc.connection.delete.via-hard-cut`).
#[test]
fn a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip() {
    let cap = crate::common::capture("a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_standalone_all_closes_the_log_connections_and_leaves_the_node_heard_on_gossip") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
        let dir = std::env::temp_dir().join(format!("connections-all-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running) = live_node(&admin, &dir, &fabric).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let all = NodeCommandRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
        let reply = crate::common::node_command_completion(&admin.observer.client, &target, &all).await;
        assert!(matches!(&reply, NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "All closes every open entry: {reply:?}");

        // Still heard on gossip: a digest of the same birth published after the All arrives.
        let digest_view = &admin.observer.topology_view.digest_view;
        let seq_at_all = digest_view.get(launch.node_id.as_str()).unwrap().0.digest_seq;
        let mut digests = digest_view.digest_changes();
        digests.borrow_and_update();
        // A digest is published every gossip interval (500 ms at the gate's cadence): twenty intervals
        // with none heard is a node no longer on gossip.
        let heard = tokio::time::timeout(Duration::from_secs(10), async {
            while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_all) {
                digests.changed().await.unwrap();
            }
        })
        .await;
        assert!(heard.is_ok(), "the node is still heard on gossip after All: no digest of it past seq {seq_at_all} in twenty gossip intervals");
        assert_eq!(named(&spans, "rdm.mesh.connection.delete.via-channel-stopped"), 0, "All stops no gossip channel");
        assert_eq!(named(&spans, "rdm.node_rpc.connection.delete.via-hard-cut"), 0, "All hard-cuts no Node RPC pool connection");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: the rename precondition counts open Direct and Proxy entries only (lifecycles/node-name-update.md: "it
/// holds no connection (gossip neighbours and the Node RPC pool are not connections)"). After a
/// completed `All`, the commander makes a further Node RPC call to the node on a new pool connection,
/// then sends `ApplyNodeName`: the node answers `Applied`. What must NOT happen: `Busy` naming the
/// Node RPC connection the further call opened as a held direct connection.
#[test]
fn a_rename_after_all_and_a_further_call_on_a_new_connection_is_applied() {
    let cap = crate::common::capture("a_rename_after_all_and_a_further_call_on_a_new_connection_is_applied");
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_rename_after_all_and_a_further_call_on_a_new_connection_is_applied") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
        let dir = std::env::temp_dir().join(format!("connections-all-rename-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running) = live_node(&admin, &dir, &fabric).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let all = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &all).await, NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "All closes every open entry");
        // A further call on a pool connection of its own: a new dial from the commander, a new accept at the node.
        let probe = StatusRequest::Get { entity: rdm_core::filter::Entity::Connections, node: Some((node_id.clone(), incarnation.clone())), filter: rdm_core::filter::Filter::new() };
        let read = call_on(&admin, &target, &probe, Some("after-all")).await;
        assert!(matches!(read, StatusReply::Connections { .. }), "the further call is answered: {read:?}");
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let apply = StatusRequest::ApplyNodeName { node_id, incarnation, name: taken.to_string() };
        assert_eq!(call_on(&admin, &target, &apply, None).await, StatusReply::Applied, "a Node RPC connection is no connection the rename counts");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: `Reserved6` and `Reserved7` are reserved Status positions, never sent (R-CR4;
/// node-rpc-envelope.md "Node stop command pair": "a receiver refuses them by name"). A live node
/// sent `Reserved6` refuses it by name (`RejectedUnmatchedCompletion`, field `command`) and starts
/// nothing: it still answers `All` with `ConnectionsDeleted`. What must NOT happen: `Applied`, or a
/// `Reserved7` call back to the commander.
#[test]
fn a_reserved_status_position_is_refused_by_name_and_starts_nothing() {
    let cap = crate::common::capture("a_reserved_status_position_is_refused_by_name_and_starts_nothing");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_reserved_status_position_is_refused_by_name_and_starts_nothing") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
        let dir = std::env::temp_dir().join(format!("connections-all-deleted-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running) = live_node(&admin, &dir, &fabric).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let reserved = StatusRequest::Reserved6 { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "4wd8hn3tqe6c".into(), attempt: 1, operation: "reserved-6:mesh1.rpc.7".into() };
        let reply = call_on(&admin, &target, &reserved, None).await;
        assert!(matches!(&reply, StatusReply::RejectedUnmatchedCompletion { field, .. } if field == "command"), "a reserved position is refused by name: {reply:?}");
        let all = NodeCommandRequest::DeleteConnections { node_id, incarnation, scope: ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &all).await, NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "the node deletes its connections afterwards");
        assert_eq!(named(&spans, "rdm.node_admin.status.update.via-connection-delete-node"), 0, "nothing ran for the reserved position");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (Luke Q9: "dispatch the request to its implementation; if this endpoint has no
/// implementation for it, answer \"request not handled here\" ... A missing handler is not an
/// authorization decision"): a Status `DeleteConnections { All }` that reaches a node whose Status door
/// registers no handler for it performs no closure and records no state. The connection the node
/// opened stays held in its connections log, no closure or connections-deleting span runs, no block on
/// new application work is set (the node's next call to its admin is answered on the same entry), and
/// the node's own `ConnectionsDeleted` is no answer to it. The same node closes that connection when the
/// request reaches the handler that implements it, the node command op 0x22. The node answers
/// `RequestNotHandled` naming the request, on its reject span
/// `rdm.node.status.reject.via-request-not-handled` (R24, Luke Q11).
#[test]
fn a_status_delete_connections_at_a_node_with_no_handler_for_it_closes_nothing_and_records_nothing() {
    let cell = "a_status_delete_connections_at_a_node_with_no_handler_for_it_closes_nothing_and_records_nothing";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, serve_ping).await;
        let dir = std::env::temp_dir().join(format!("connections-all-unhandled-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running, seams) = live_node_with(&admin, &dir, &fabric, |b| b).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let opened = ping_admin(&seams, &admin, &CallOptions::default()).await;
        assert!(matches!(opened, RpcOutcome::Reply(_)), "the node reaches its admin: {opened:?}");
        seams.connections.observations_landed().await;
        let held: Vec<_> = seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        assert_eq!(held.len(), 1, "the node holds the one connection it opened: {held:?}");

        let digest_view = &admin.observer.topology_view.digest_view;
        let seq_at_call = digest_view.get(launch.node_id.as_str()).expect("the admin side hears the node").0.digest_seq;
        let mut digests = digest_view.digest_changes();
        digests.borrow_and_update();
        let status = StatusRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
        let reply = call_on(&admin, &target, &status, None).await;
        assert!(!matches!(reply, StatusReply::ConnectionsDeleted { .. }), "no handler on the node's Status door performs a closure: {reply:?}");
        assert_eq!(reply, StatusReply::RequestNotHandled { request: "delete-connections".into() }, "the node answers a request it has no handler for not handled here (R24)");
        let rejected = fields_of(&spans, "rdm.node.status.reject.via-request-not-handled");
        assert_eq!(rejected.iter().map(|f| (f.get("request").cloned(), f.get("node").cloned())).collect::<Vec<_>>(), vec![(Some("delete-connections".to_string()), Some(launch.name.to_string()))], "the node's reject span names the request and itself: {rejected:?}");
        assert_eq!(seams.connections.held_routes().await.into_iter().map(|r| r.id).collect::<Vec<_>>(), held, "the node's connection is still held");
        let again = ping_admin(&seams, &admin, &CallOptions::default()).await;
        assert!(matches!(again, RpcOutcome::Reply(_)), "no block on new application work is set: {again:?}");
        for name in ["rdm.node_admin.connection.delete.via-delete-connections", "rdm.node_admin.connection.delete.via-connections-deleting", "rdm.node_rpc.connection.reject.via-admission-block"] {
            assert_eq!(named(&spans, name), 0, "nothing of a closure ran: {name}");
        }
        // No recorded state: the node's next digest, published after the request, still says Ready.
        tokio::time::timeout(Duration::from_secs(10), async {
            while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_call) {
                digests.changed().await.unwrap();
            }
        })
        .await
        .expect("the node publishes a digest after the request");
        assert_eq!(digest_view.get(launch.node_id.as_str()).map(|(d, _)| d.status), Some(MemberStatus::Ready), "the node's status is unchanged");

        // The request at the handler that implements it, the node command op 0x22, closes it.
        let command = NodeCommandRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
        let deleted = crate::common::node_command_completion(&admin.observer.client, &target, &command).await;
        assert!(matches!(&deleted, NodeCommandReply::ConnectionsDeleted { closed, held: still, .. } if still.is_empty() && held.iter().all(|id| closed.contains(id))), "{deleted:?}");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (R24, Luke Q11: "the same variant at every Status door that can lack a handler for a
/// request it receives: the rpc node's, the node-admin's and the test rig's"): the test rig's admin
/// side serves state check-ins and declarations only. A `fabric.get` reaching its Status door is
/// answered `RequestNotHandled` naming the request, decided by dispatch alone, on the admin side's
/// reject span `rdm.node.status.reject.via-request-not-handled` naming the request and the admin side.
/// Must NOT happen: an authority refusal or `NotReady` for a request the door has no handler for.
#[test]
fn a_status_request_the_rigs_admin_side_has_no_handler_for_is_answered_not_handled_by_name() {
    let cell = "a_status_request_the_rigs_admin_side_has_no_handler_for_is_answered_not_handled_by_name";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, serve_ping).await;
        let dir = std::env::temp_dir().join(format!("connections-all-rig-door-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (_launch, _running, seams) = live_node_with(&admin, &dir, &fabric, |b| b).await;
        let target = NodeTarget::ExactNode(admin.launcher.node_id.clone());
        let fabrics = StatusRequest::Get { entity: rdm_core::filter::Entity::Fabrics, node: None, filter: rdm_core::filter::Filter::new() };
        let reply = match seams.client.call::<Status>(&target, &fabrics, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(r) => r.into_value(),
            other => panic!("the admin side's Status door did not answer: {other:?}"),
        };
        assert_eq!(reply, StatusReply::RequestNotHandled { request: "fabric.get".into() }, "the rig's admin side answers a request it has no handler for not handled here");
        let rejected = fields_of(&spans, "rdm.node.status.reject.via-request-not-handled");
        assert_eq!(rejected.iter().map(|f| (f.get("request").cloned(), f.get("node").cloned())).collect::<Vec<_>>(), vec![(Some("fabric.get".to_string()), Some(admin.launcher.name.to_string()))], "the admin side's reject span names the request and itself: {rejected:?}");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: once `node.connection.delete { All }` has closed a live node's connections, the node opens
/// no new connection until it starts or joins its mesh again (node-name-update.md Precondition: "No new
/// connection is admitted from the row write through `ApplyNodeName`"; node-replace.md: a node is
/// renamed once "nothing is connected to it", and a rename is followed by its stop or its rejoin). After
/// `All` answers holding nothing, the node's data-plane call to its admin ends `NotSent`
/// `AdmissionBlocked` naming the node, before any dial: one
/// `rdm.node_rpc.connection.reject.via-admission-block` span, `at = dial`, names the destination. The log
/// holds nothing, and `ApplyNodeName` answers `Applied`. The block stays: the next call is refused the
/// same way. The node joins its mesh again (`JoinMesh`): the block lifts, and its next data-plane call is
/// answered, its entry naming the name it took as its source. What must NOT happen: a call answered on a
/// new connection before the join, a Direct entry written between `All` and the join,
/// `BusyHoldingConnections`, or the block cutting the Node RPC pool
/// (`rdm.node_rpc.connection.delete.via-hard-cut`).
#[test]
fn a_connections_deleted_node_opens_no_new_connection_and_takes_its_name() {
    let cap = crate::common::capture("a_connections_deleted_node_opens_no_new_connection_and_takes_its_name");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_connections_deleted_node_opens_no_new_connection_and_takes_its_name") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, serve_ping).await;
        let dir = std::env::temp_dir().join(format!("connections-all-admission-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, running, seams) = live_node_with(&admin, &dir, &fabric, |b| b).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let all = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &all).await, NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "All closes every open entry");

        // Between the row's write and the apply the node opens nothing.
        let why = admission_blocked(&ping_admin(&seams, &admin, &CallOptions::default()).await);
        assert!(why.contains(launch.node_id.as_str()), "the refusal names the node: {why}");
        assert!(seams.connections.held_routes().await.is_empty(), "the log holds no connection after All");
        let refusals = fields_of(&spans, "rdm.node_rpc.connection.reject.via-admission-block");
        assert_eq!(refusals.len(), 1, "one refusal, on its own span: {refusals:#?}");
        assert_eq!((refusals[0].get("at").map(String::as_str), refusals[0].get("destination").map(String::as_str)), (Some("dial"), Some("mesh1.admin.1")), "{refusals:#?}");

        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let apply = StatusRequest::ApplyNodeName { node_id: node_id.clone(), incarnation: incarnation.clone(), name: taken.to_string() };
        assert_eq!(call_on(&admin, &target, &apply, None).await, StatusReply::Applied, "a node that opened nothing takes its name");
        assert_eq!(running.topology_view.held_name(&launch.name), taken, "the node holds the applied name");

        // The block stays after the apply: a renamed node opens nothing until it starts or joins its mesh
        // again (node-replace.md: it is renamed once nothing is connected to it).
        let still = admission_blocked(&ping_admin(&seams, &admin, &CallOptions::default()).await);
        assert!(still.contains("joins its mesh again"), "the refusal says what lifts it: {still}");
        // It joins its mesh again under the name it took: the block lifts, and the next connection is
        // under that name.
        let key = node::load_or_mint_key(&dir).unwrap();
        admin.deployed_as(&launch, &key, taken.clone(), Some(taken.clone()));
        let join = StatusRequest::JoinMesh { node_id, incarnation, build_id: "4wd8hn3tqe6c".into(), attempt: 1, operation: "node.mesh.join:mesh1.rpc.1".into() };
        assert!(matches!(call_on(&admin, &target, &join, None).await, StatusReply::MeshJoined { .. }), "the node joins its mesh again");
        let after = ping_admin(&seams, &admin, &CallOptions::default()).await;
        assert!(matches!(after, RpcOutcome::Reply(_)), "once it joined its mesh again the node opens connections: {after:?}");
        seams.connections.observations_landed().await;
        let routes = seams.connections.held_routes().await;
        assert_eq!(routes.iter().map(|r| (r.source.name.clone(), r.destination.name.to_string())).collect::<Vec<_>>(), vec![(taken.clone(), "mesh1.admin.1".to_string())], "the next connection names the name the node took: {routes:?}");
        assert_eq!(fields_of(&spans, "rdm.node_rpc.connection.reject.via-admission-block").len(), 2, "one refusal before the apply and one after it, none after the join");
        assert!(fields_of(&spans, "rdm.node_rpc.connection.delete.via-hard-cut").is_empty(), "the block cuts no Node RPC pool");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: a dial the node's data plane began before `All`, and whose connection opens only after
/// `All` closed the node's connections, enters no pool and writes no entry (proposal
/// node-properties-own-commands.md §2.4: "Connection opened between the precondition check and the
/// apply: impossible by the admission block; a node that reports `Busy` is a defect in that block").
/// The dial stops once connected (the client's after-connect failpoint); `All` answers holding nothing;
/// released, the dial's connection is closed and the call ends `NotSent` `AdmissionBlocked`, its
/// `rdm.node_rpc.connection.reject.via-admission-block` span naming the pool (`at = pool`). The log
/// holds nothing and `ApplyNodeName` answers `Applied`. What must NOT happen: the late connection
/// pooled and answered, its Direct entry written, or `BusyHoldingConnections` naming it.
#[test]
fn a_dial_that_connects_after_all_enters_no_pool_and_the_name_is_applied() {
    let cap = crate::common::capture("a_dial_that_connects_after_all_enters_no_pool_and_the_name_is_applied");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_dial_that_connects_after_all_enters_no_pool_and_the_name_is_applied") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, serve_ping).await;
        let dir = std::env::temp_dir().join(format!("connections-all-late-dial-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running, seams) = live_node_with(&admin, &dir, &fabric, |b| b).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let all = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: ConnectionScope::All };
        let stop = std::sync::Arc::new(rdm_transport::Failpoint::default());
        let opts = CallOptions { after_connect: Some(stop.clone()), ..CallOptions::default() };
        let dial = ping_admin(&seams, &admin, &opts);
        let steps = async {
            stop.reached.notified().await;
            let deleted = crate::common::node_command_completion(&admin.observer.client, &target, &all).await;
            stop.release.notify_one();
            deleted
        };
        let (out, deleted) = tokio::join!(dial, steps);
        assert!(matches!(&deleted, NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "All holds nothing while the dial waits unpooled: {deleted:?}");
        admission_blocked(&out);
        assert!(seams.connections.held_routes().await.is_empty(), "the late connection writes no entry");
        let refusals = fields_of(&spans, "rdm.node_rpc.connection.reject.via-admission-block");
        assert_eq!(refusals.len(), 1, "one refusal, on its own span: {refusals:#?}");
        assert_eq!((refusals[0].get("at").map(String::as_str), refusals[0].get("destination").map(String::as_str)), (Some("pool"), Some("mesh1.admin.1")), "{refusals:#?}");
        let apply = StatusRequest::ApplyNodeName { node_id, incarnation, name: "mesh1.rpc.1".into() };
        assert_eq!(call_on(&admin, &target, &apply, None).await, StatusReply::Applied, "no late connection makes the node Busy");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: `All` closes every connection open when its wait for admitted work ends, those opened
/// while it waited included (node-connections-delete.md step 3: "Let admitted work finish, including
/// its downstream connections: they belong to the same closure", then "Close every open connection,
/// each open Direct or Proxy entry of the connections log"). A call held in flight at the node keeps
/// `All` waiting; meanwhile the node's data plane opens a connection to its admin, a Direct entry of its
/// log. Released, `All` answers `ConnectionsDeleted` with an `Established` receipt counting the held
/// call and closing that entry, holding nothing. What must NOT happen: `All` answered with the entry
/// still open (an open set read before the wait), so connections-deleted published over an open
/// connection.
#[test]
fn all_closes_the_connections_opened_while_it_waited_for_admitted_work() {
    let cap = crate::common::capture("all_closes_the_connections_opened_while_it_waited_for_admitted_work");
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "all_closes_the_connections_opened_while_it_waited_for_admitted_work") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, serve_ping).await;
        let dir = std::env::temp_dir().join(format!("connections-all-wait-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        // The node serves one op the cell can hold: a call into it stays in flight until released.
        let pull = PullServer::new(vec![]);
        pull.gate.open();
        let (launch, _running, seams) = live_node_with(&admin, &dir, &fabric, {
            let pull = pull.clone();
            move |b| pull.serve(b)
        })
        .await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let all = NodeCommandRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        pull.hold();
        let held_call = async { admin.observer.client.call::<PreNodeReadyPull>(&target, &PullRequest::Pull, &opts).await.0 };
        let steps = async {
            pull.entered(1).await;
            let deleting = crate::common::node_command_completion(&admin.observer.client, &target, &all);
            tokio::pin!(deleting);
            assert!(tokio::time::timeout(Duration::from_millis(300), &mut deleting).await.is_err(), "All is not answered while the call it admitted is in flight");
            let out = ping_admin(&seams, &admin, &CallOptions::default()).await;
            assert!(matches!(out, RpcOutcome::Reply(_)), "a connection opened while All waits belongs to its closure: {out:?}");
            seams.connections.observations_landed().await;
            let opened: Vec<_> = seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
            assert_eq!(opened.len(), 1, "the node holds the one connection it opened while All waited: {opened:?}");
            pull.release();
            (opened, deleting.await)
        };
        let (held_out, (opened, deleted)) = tokio::join!(held_call, steps);
        assert!(matches!(held_out, RpcOutcome::Reply(_)), "the held call answers once released: {held_out:?}");
        match &deleted {
            NodeCommandReply::ConnectionsDeleted { held, receipt: Some(ConnectionsReceipt::Established { in_flight: 1, closed }), .. } => {
                assert_eq!(closed, &opened, "All closes the connection opened while it waited: {deleted:?}");
                assert!(held.is_empty(), "All holds nothing once it answers: {deleted:?}");
            }
            other => panic!("All did not end Established counting the held call: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: `node.mesh.leave` refuses `BusyHoldingConnections` (`rdm.node.mesh.reject.via-busy-holding-connections`)
/// while an entry is open in the node's connections log; once `node.connection.delete { All }` closes
/// the log's entries, `LeaveMesh` stops the mesh channel (`rdm.node.mesh.update.via-leaving`,
/// `rdm.node.mesh.update.via-left` with `which = "own"`) and answers `MeshLeft { joined/left: [Own] }`.
/// `JoinMesh` then reports back via `JoinNode` (`rdm.node.mesh.update.via-joining`), starts the mesh
/// channel (`rdm.node.mesh.update.via-joined` with `which = "own"`), and answers `MeshJoined`. And
/// `node.stop` shares the `node.mesh.leave` step after `node.connection.delete { All }`.
#[test]
fn node_mesh_leave_and_node_mesh_join_standalone_and_inside_stop() {
    let cap = crate::common::capture("node_mesh_leave_and_node_mesh_join_standalone_and_inside_stop");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "node_mesh_leave_and_node_mesh_join_standalone_and_inside_stop") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, serve_ping).await;
        let dir = std::env::temp_dir().join(format!("connections-all-mesh-leave-join-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running, seams) = live_node_with(&admin, &dir, &fabric, |b| b).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());

        // Start joins the mesh (`via-joining` + `via-joined`).
        let join_spans = fields_of(&spans, "rdm.node.mesh.update.via-joined");
        assert!(join_spans.iter().any(|f| f.get("node_id").map(String::as_str) == Some(node_id.as_str()) && f.get("which").map(String::as_str) == Some("own")), "start emitted via-joined(own): {join_spans:#?}");

        // Open a data-plane connection so the node's connections log is non-empty.
        let out = ping_admin(&seams, &admin, &CallOptions::default()).await;
        assert!(matches!(out, RpcOutcome::Reply(_)), "ping succeeds: {out:?}");
        seams.connections.observations_landed().await;
        assert_eq!(seams.connections.held_routes().await.len(), 1, "one open route in log");

        // Standalone `LeaveMesh` while holding an open connection is refused `BusyHoldingConnections`.
        let leave_req = StatusRequest::LeaveMesh {
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            build_id: "4wd8hn3tqe6c".into(),
            attempt: 1,
            operation: "leave-mesh:busy".into(),
        };
        let busy = call_on(&admin, &target, &leave_req, None).await;
        assert!(matches!(&busy, StatusReply::BusyHoldingConnections { connections } if connections.len() == 1), "LeaveMesh refuses while holding a connection: {busy:?}");
        let rejects = fields_of(&spans, "rdm.node.mesh.reject.via-busy-holding-connections");
        assert_eq!(rejects.len(), 1, "one busy-holding-connections span: {rejects:#?}");
        assert_eq!(rejects[0].get("operation").map(String::as_str), Some("leave-mesh:busy"));

        // Close all connections, then standalone `LeaveMesh` succeeds.
        let all = NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &all).await, NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()));
        let leave_ok = StatusRequest::LeaveMesh {
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            build_id: "4wd8hn3tqe6c".into(),
            attempt: 1,
            operation: "leave-mesh:clean".into(),
        };
        let left_reply = call_on(&admin, &target, &leave_ok, None).await;
        assert_eq!(left_reply, StatusReply::MeshLeft { node_id: node_id.clone(), incarnation: incarnation.clone(), left: vec![WhichMesh::Own] });
        let leaving = fields_of(&spans, "rdm.node.mesh.update.via-leaving");
        assert_eq!(leaving.len(), 1, "one via-leaving span: {leaving:#?}");
        assert_eq!(leaving[0].get("operation").map(String::as_str), Some("leave-mesh:clean"));
        let left_spans = fields_of(&spans, "rdm.node.mesh.update.via-left");
        assert_eq!(left_spans.len(), 1, "one via-left span: {left_spans:#?}");
        assert_eq!(left_spans[0].get("which").map(String::as_str), Some("own"));

        // Standalone `JoinMesh` brings the node back onto the mesh channel.
        let join_req = StatusRequest::JoinMesh {
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            build_id: "4wd8hn3tqe6c".into(),
            attempt: 1,
            operation: "join-mesh:rejoin".into(),
        };
        let joined_reply = call_on(&admin, &target, &join_req, None).await;
        assert_eq!(joined_reply, StatusReply::MeshJoined { node_id: node_id.clone(), incarnation: incarnation.clone(), joined: vec![WhichMesh::Own] });
        let joining = fields_of(&spans, "rdm.node.mesh.update.via-joining");
        assert!(joining.iter().any(|f| f.get("operation").map(String::as_str) == Some("join-mesh:rejoin")), "standalone JoinMesh emitted via-joining: {joining:#?}");
        let joined_after = fields_of(&spans, "rdm.node.mesh.update.via-joined");
        assert!(joined_after.iter().filter(|f| f.get("node_id").map(String::as_str) == Some(node_id.as_str()) && f.get("which").map(String::as_str) == Some("own")).count() >= 2, "standalone JoinMesh emitted via-joined(own): {joined_after:#?}");

        let _ = std::fs::remove_dir_all(&dir);
    })
}

