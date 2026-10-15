//! A node takes another name only when it holds no connection (R-CR1, R-CR1a, R-CR3; rulings
//! 2026-10-11): `ApplyNodeName` at the node answers `Busy` naming the routes it still holds, and
//! `node.connections.delete { scope: All }` closes every one of them, writing each drop under the
//! node's current name, before the name can move.
//!
//! CONTRACT: `mesh1.rpc.1.new` originates a data-plane connection to its admin and one to its peer
//! `mesh1.rpc.2`, so its own connections log holds a `connected` entry to each. Told
//! `ApplyNodeName { mesh1.rpc.1 }`, it answers `Busy` naming both routes, holding nothing. Its admin's
//! `DeleteConnections { All }` answers `ConnectionsDeleted` closing both routes' open entries and
//! holding none, each drop written under `mesh1.rpc.1.new`; the same `ApplyNodeName` then answers
//! `Applied`. No `connected` entry under `mesh1.rpc.1.new` is the latest of its pair after the
//! rename, and the next entry the node writes (it dials its peer again) names `mesh1.rpc.1`. What must
//! NOT happen: a node that still holds connections takes another name, a route left open
//! under the name it gave up, or a drop written under another name.

use crate::common::admin_side_serving;
use rdm_core::connections::{ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::status::{ConnectionScope, Status, StatusReply, StatusRequest};
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
fn a_node_holding_connections_is_refused_its_name_until_its_connections_are_deleted() {
    let cap = crate::common::capture("a_node_holding_connections_is_refused_its_name_until_its_connections_are_deleted");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("rename-precondition-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        // The cell reads the node's kept log on the store root: its fabric is past its first state-commit.
        crate::common::store_past_state_commit(&store, &fabric).await;
        let born: PathName = "mesh1.rpc.1.new".parse().unwrap();
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let peer_name: PathName = "mesh1.rpc.2".parse().unwrap();
        let launch_of = |name: &PathName| {
            let dir = root.join(name.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
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
                connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
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
        let (renamed_node, renamed_seams) = start(renamed.clone()).await;
        let (peer_node, _peer_seams) = start(peer.clone()).await;

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
        // And the node holds the peer: the admin side holds both ready from their node.started reports,
        // which tell nothing of what either has heard of the other.
        let node_book = &renamed_node.membership.book;
        let mut node_digests = node_book.digest_changes();
        while node_book.get(peer.node_id.as_str()).is_none() {
            node_digests.changed().await.unwrap();
        }

        // The node originates a data-plane connection to its admin and one to its peer: it holds a
        // route to each.
        for (to, what) in [(admin.launcher.node_id.clone(), "the node reaches its admin"), (peer.node_id.clone(), "the node reaches its peer")] {
            match renamed_seams.client.call::<Ping>(&NodeTarget::ExactNode(to), &PingRequest::Ping { payload: what.as_bytes().to_vec() }, &CallOptions::default()).await.0 {
                RpcOutcome::Reply(_) => {}
                other => panic!("{what}: the ping did not end in a reply: {other:?}"),
            }
        }
        renamed_seams.connections.observations_landed().await;
        let before = kept_log(&store, &fabric, &born);
        for dest in ["mesh1.admin.1", "mesh1.rpc.2"] {
            assert!(before.iter().any(|e| e.source.name == born && e.destination.name.to_string() == dest), "the node holds a route to {dest}: {before:#?}");
        }

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
        // Told its name while it holds both routes: refused naming each by its entry's id, nothing held.
        let apply = StatusRequest::ApplyNodeName { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), name: taken.to_string() };
        let holding: Vec<_> = renamed_seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        assert_eq!(holding.len(), 2, "the node holds its two routes: {holding:?}");
        let busy = call(apply.clone()).await;
        assert_eq!(busy, StatusReply::BusyHoldingConnections { connections: holding }, "a node holding its routes keeps its name, naming each connection");
        assert_eq!(renamed_seams.connections.own().name, born, "the node holds the name it had");

        // node.connections.delete { All }: every route closed, each drop under the name it holds.
        let open_ids: Vec<_> = renamed_seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        let deleted = call(StatusRequest::DeleteConnections { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), scope: ConnectionScope::All }).await;
        let StatusReply::ConnectionsDeleted { closed, not_open, held, .. } = &deleted else { panic!("the cut ends node.connections.deleted: {deleted:?}") };
        assert!(open_ids.iter().all(|id| closed.contains(id)) && not_open.is_empty() && held.is_empty(), "every open entry is closed and none held: open {open_ids:?}, {deleted:?}");

        // The same apply now takes the name.
        assert_eq!(call(apply).await, StatusReply::Applied, "with no connection held the name moves");
        assert_eq!(renamed_seams.connections.own().name, taken, "the node holds its new name");

        // It dials its peer again: the node's next entry names the name it holds.
        let _ = renamed_seams.client.call::<Ping>(&NodeTarget::ExactNode(peer.node_id.clone()), &PingRequest::Ping { payload: b"after the rename".to_vec() }, &CallOptions::default()).await;
        renamed_seams.connections.observations_landed().await;
        let log = kept_log(&store, &fabric, &born);
        let mut wrong: Vec<String> = Vec::new();
        let mut latest: std::collections::BTreeMap<(String, String, String), &NodeConnection> = Default::default();
        for e in &log {
            latest.insert((e.source.name.to_string(), e.destination.name.to_string(), format!("{:?}", e.kind)), e);
        }
        for ((source, destination, kind), e) in &latest {
            if source == &born.to_string() && e.state == ConnectionState::Connected {
                wrong.push(format!("{source} -> {destination} {kind} is still connected under the name the node gave up: {e:?}"));
            }
        }
        for dest in ["mesh1.admin.1", "mesh1.rpc.2"] {
            let drop = log.iter().find(|e| e.source.name == born && e.destination.name.to_string() == dest && e.state == ConnectionState::Disconnected && e.reason.as_deref() == Some("node.connections.delete"));
            if drop.is_none() {
                wrong.push(format!("no drop of the route to {dest} under {born}"));
            }
        }
        let next = log.iter().rev().find(|e| e.destination.name == peer_name && e.state == ConnectionState::Connected);
        if !next.is_some_and(|e| e.source.name == taken) {
            wrong.push(format!("the node's next entry for its peer names {taken} as its source: {next:?}"));
        }
        assert!(wrong.is_empty(), "{} wrong after the rename:\n{}", wrong.len(), wrong.join("\n"));
        let _ = std::fs::remove_dir_all(&root);
    });
    let applies: Vec<std::collections::BTreeMap<String, String>> =
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.update.via-apply-node-name").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(applies.iter().map(|f| f.get("outcome").cloned().unwrap_or_default()).collect::<std::collections::BTreeSet<_>>(), ["applied".to_string(), "busy".to_string()].into(), "{applies:#?}");
    let cuts: Vec<std::collections::BTreeMap<String, String>> =
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.connections.delete.via-delete-connections").map(|(_, _, f)| f.clone()).collect();
    assert!(cuts.iter().any(|f| f.get("scope").map(String::as_str) == Some("all") && f.get("held").map(String::as_str) == Some("0") && f.get("outcome").map(String::as_str) == Some("connections-deleted")), "{cuts:#?}");
}
