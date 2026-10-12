//! `node.connections.delete { scope: All }` is just connections (R-CR5; lifecycles/node-connections-delete.md:
//! "A connection is an open Direct or Proxy entry of the node's connections log. Gossip neighbours and
//! the Node RPC pool are not connections, and neither scope touches them: a node that has had its
//! connections deleted is still heard on gossip"; lifecycles/node-update.md "Rename precondition").
//!
//! Its own process: the cells hold process-wide state (the parking flag).

use crate::common::{admin_side_serving, Spans};
use rafka_node_rpc_testkit::rig::AdminSide;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{ConnectionScope, Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

async fn call_on(admin: &AdminSide, target: &NodeTarget, req: &StatusRequest, scope: Option<&str>) -> StatusReply {
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, scope: scope.map(str::to_string), ..CallOptions::default() };
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

/// A live `mesh1.rpc.1.new` the admin side has heard ReadyForTraffic and resolves.
async fn live_node(admin: &AdminSide, dir: &std::path::Path, fabric: &FabricId) -> (Launch, node::RunningNode) {
    RuntimeFact::of_this_process("cell").unwrap().write_record(dir).unwrap();
    let key = node::load_or_mint_key(dir).unwrap();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        name: "mesh1.rpc.1.new".parse().unwrap(),
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
        mesh_issuer: None,
    };
    admin.deployed(&launch, &key);
    let running = node::start(&launch, |b, _| b).await.expect("the node comes up");
    let book = &admin.observer.membership.book;
    let mut digests = book.digest_changes();
    while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
        digests.changed().await.unwrap();
    }
    let (d, _) = book.get(launch.node_id.as_str()).unwrap();
    admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
    (launch, running)
}

fn named(spans: &Spans, name: &str) -> usize {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).count()
}

/// CONTRACT: a live node's `node.connections.delete { All }` closes every open Direct or Proxy entry
/// of its connections log and nothing else. It answers `ConnectionsDeleted` holding nothing, and the
/// node is still heard on gossip: the admin side receives a later digest of the same birth. What must
/// NOT happen: the gossip channel parked (`rdm.mesh.connection.delete.via-channel-parked`), or the
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
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("connections-all-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running) = live_node(&admin, &dir, &fabric).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let all = StatusRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
        let reply = call_on(&admin, &target, &all, None).await;
        assert!(matches!(&reply, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "All closes every open entry: {reply:?}");

        // Still heard on gossip: a digest of the same birth published after the All arrives.
        let book = &admin.observer.membership.book;
        let seq_at_all = book.get(launch.node_id.as_str()).unwrap().0.digest_seq;
        let mut digests = book.digest_changes();
        digests.borrow_and_update();
        // A digest is published every gossip interval (500 ms at the gate's cadence): twenty intervals
        // with none heard is a node no longer on gossip.
        let heard = tokio::time::timeout(Duration::from_secs(10), async {
            while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_all) {
                digests.changed().await.unwrap();
            }
        })
        .await;
        assert!(heard.is_ok(), "the node is still heard on gossip after All: no digest of it past seq {seq_at_all} in twenty gossip intervals");
        assert_eq!(named(&spans, "rdm.mesh.connection.delete.via-channel-parked"), 0, "All parks no gossip channel");
        assert_eq!(named(&spans, "rdm.node_rpc.connection.delete.via-hard-cut"), 0, "All hard-cuts no Node RPC pool connection");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: the rename precondition counts open Direct and Proxy entries only (node-update.md: "it
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
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("connections-all-rename-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let (launch, _running) = live_node(&admin, &dir, &fabric).await;
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let all = StatusRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: ConnectionScope::All };
        assert!(matches!(call_on(&admin, &target, &all, None).await, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "All closes every open entry");
        // A further call on a pool connection of its own: a new dial from the commander, a new accept at the node.
        let probe = StatusRequest::GetConnections { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let read = call_on(&admin, &target, &probe, Some("after-all")).await;
        assert!(matches!(read, StatusReply::Connections { .. }), "the further call is answered: {read:?}");
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let apply = StatusRequest::ApplyNodeName { node_id, incarnation, name: taken.to_string() };
        assert_eq!(call_on(&admin, &target, &apply, None).await, StatusReply::Applied, "a Node RPC connection is no connection the rename counts");
        let _ = std::fs::remove_dir_all(&dir);
    })
}
