//! A node takes another name only when it holds no connection (R-CR1, R-CR1a, R-CR3; rulings
//! 2026-10-11): `ApplyNodeName` at the node answers `Busy` naming the routes it still holds, and
//! `node.connections.delete { scope: All }` closes every one of them, writing each drop under the
//! node's current name, before the name can move.
//!
//! CONTRACT: `mesh1.rpc.1.new` joins its admin and is dialled by its peer `mesh1.rpc.2`, so its own
//! connections log holds a `connected` entry to each. Drained (`DrainNode`), it is told
//! `ApplyNodeName { mesh1.rpc.1 }` and answers `Busy` naming both routes, holding nothing. What must
//! NOT happen: a drained node that still holds connections takes another name.

use crate::common::admin_side_serving;
use rafka_mesh_entity::connections::NodeConnection;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingRequest};
use rafka_node_rpc_contract::status::{Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use rafka_node_rpc_testkit::originate::Seams;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Every entry in `name`'s kept log under the fabric's store root, in the order written.
fn kept_log(store: &std::path::Path, fabric: &FabricId, name: &PathName) -> Vec<NodeConnection> {
    let root = rafka_node_admin_core::store_root::StoreRoot::open(store, fabric).expect("the store root opens");
    let file = root.connections(name).join("history.jsonl");
    std::fs::read_to_string(&file)
        .unwrap_or_else(|e| panic!("{}: {e}", file.display()))
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{}: {e}: {l}", file.display())))
        .collect()
}

#[test]
fn a_drained_node_holding_connections_is_refused_its_name_until_its_connections_are_deleted() {
    let cap = crate::common::capture("a_drained_node_holding_connections_is_refused_its_name_until_its_connections_are_deleted");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let root = std::env::temp_dir().join(format!("rename-precondition-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.1.new".parse().unwrap();
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let peer_name: PathName = "mesh1.rpc.2".parse().unwrap();
        let launch_of = |name: &PathName| {
            let dir = root.join(name.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
            let key = node::load_or_mint_key(&dir).unwrap();
            let launch = Launch {
                fabric: "fabric1".into(),
                fabric_id: fabric.clone(),
                name: name.clone(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                listeners: vec![],
                seeds: vec![admin.seed.clone()],
                launcher: Some(admin.launcher.clone()),
                data_dir: dir,
                store_dir: store.clone(),
                mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
                mesh_issuer: None,
            };
            admin.deployed(&launch, &key);
            launch
        };
        let (renamed, peer) = (launch_of(&born), launch_of(&peer_name));
        let start = |launch: Launch| async move {
            let seams: Arc<OnceLock<Seams>> = Arc::default();
            let slot = seams.clone();
            let running = node::start_with_seams(&launch, move |b, s| {
                let _ = slot.set(s);
                b
            })
            .await
            .unwrap_or_else(|e| panic!("{} comes up: {e}", launch.name));
            (running, seams.get().cloned().expect("the node handed its seams"))
        };
        let (_renamed_node, renamed_seams) = start(renamed.clone()).await;
        let (peer_node, peer_seams) = start(peer.clone()).await;

        // Both births are held ready by the admin, and the peer holds the renamed one.
        let book = &admin.observer.membership.book;
        let mut heard = book.heard_changes();
        while [&renamed, &peer].iter().any(|l| book.get(l.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic)) {
            heard.changed().await.unwrap();
        }
        let peer_book = &peer_node.membership.book;
        let mut peer_digests = peer_book.digest_changes();
        while peer_book.get(renamed.node_id.as_str()).is_none() {
            peer_digests.changed().await.unwrap();
        }

        // The peer dials the node: the node holds a route to its admin (its join) and to its peer.
        match peer_seams.client.call::<Ping>(&NodeTarget::ExactNode(renamed.node_id.clone()), &PingRequest::Ping { payload: b"before the drain".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the peer's ping did not end in a reply: {other:?}"),
        }
        renamed_seams.connections.drain().await;
        let before = kept_log(&store, &fabric, &born);
        for dest in ["mesh1.admin.1", "mesh1.rpc.2"] {
            assert!(before.iter().any(|e| e.source.name == born && e.destination.name.to_string() == dest), "the node holds a route to {dest}: {before:#?}");
        }

        // The drain: the node is not ready for traffic, and still holds both routes.
        let (d, _) = book.get(renamed.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let target = NodeTarget::ExactNode(renamed.node_id.clone());
        let call = |req: StatusRequest| {
            let (client, target, opts) = (&admin.observer.client, &target, &opts);
            async move {
                match client.call::<Status>(target, &req, opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let drain = StatusRequest::DrainNode { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), build_id: "bld_rename".into(), attempt: 1, operation: format!("drain-node:{born}") };
        assert_eq!(call(drain).await, StatusReply::Applied, "the drain is admitted");
        while book.get(renamed.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Draining) {
            heard.changed().await.unwrap();
        }

        // Told its name while it holds both routes: Busy naming them, nothing held.
        let apply = StatusRequest::ApplyNodeName { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), name: taken.to_string() };
        let busy = call(apply.clone()).await;
        assert!(
            matches!(&busy, StatusReply::Busy { reason } if reason.contains("mesh1.admin.1") && reason.contains("mesh1.rpc.2")),
            "a drained node holding its routes keeps its name, Busy naming each route: {busy:?}"
        );
        assert_eq!(renamed_seams.connections.own().name, born, "the node holds the name it had");
        let _ = std::fs::remove_dir_all(&root);
    });
    let applies: Vec<std::collections::BTreeMap<String, String>> =
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.update.via-apply-node-name").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(applies.iter().map(|f| f.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["busy".to_string()], "{applies:#?}");
}
