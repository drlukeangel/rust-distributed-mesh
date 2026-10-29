//! A node takes another name only when it holds no connection (R-CR1, R-CR1a, R-CR3; rulings
//! 2026-10-11): `ApplyNodeName` at the node answers `Busy` naming the routes it still holds, and
//! `node.connection.delete { scope: All }` closes every one of them, writing each drop under the
//! node's current name, before the name can move.
//!
//! CONTRACT: `mesh1.rpc.7` originates a data-plane connection to its admin and one to its peer
//! `mesh1.rpc.2`, so its own connections log holds a `connected` entry to each. Told
//! `ApplyNodeName { mesh1.rpc.1 }`, it answers `Busy` naming both routes, holding nothing. Its admin's
//! `DeleteConnections { All }` answers `ConnectionsDeleted` closing both routes' open entries and
//! holding none, each drop written under `mesh1.rpc.7`; the same `ApplyNodeName` then answers
//! `Applied`. No `connected` entry under `mesh1.rpc.7` is the latest of its pair after the
//! rename, and once the node joins its mesh again the next entry it writes (it dials its peer again)
//! names `mesh1.rpc.1`. What must
//! NOT happen: a node that still holds connections takes another name, a route left open
//! under the name it gave up, or a drop written under another name.

use crate::common::admin_side_serving;
use rdm_core::connections::{ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
use rdm_core::status::{ConnectionScope, Status, StatusReply, StatusRequest};
use rdm_test::node;
use rdm_test::originate::Seams;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Every entry in `name`'s kept log under the fabric's store root, in the order written.
fn kept_log(store: &std::path::Path, fabric: &FabricId, name: &PathName) -> Vec<NodeConnection> {
    let root = rdm_admin::store_root::StoreRoot::open(store, fabric).expect("the store root opens");
    rdm_node::storage::connection_history(root.dir(), &name.to_string()).unwrap_or_else(|e| panic!("{name}'s kept log: {e}"))
}

#[test]
fn a_node_holding_connections_is_refused_its_name_until_its_connections_are_deleted() {
    let cap = crate::common::capture("a_node_holding_connections_is_refused_its_name_until_its_connections_are_deleted");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("rename-precondition-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.7".parse().unwrap();
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let peer_name: PathName = "mesh1.rpc.2".parse().unwrap();
        let launch_of = |name: &PathName| {
            let dir = root.join(name.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
            let key = node::load_or_mint_key(&dir).unwrap();
            let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
                issuing: None,
                roots: Vec::new(),
                connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
                declared: Default::default(),
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
        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while [&renamed, &peer].iter().any(|l| digest_view.get(l.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready)) {
            heard.changed().await.unwrap();
        }
        let peer_view = &peer_node.topology_view.digest_view;
        let mut peer_digests = peer_view.digest_changes();
        while peer_view.get(renamed.node_id.as_str()).is_none() {
            peer_digests.changed().await.unwrap();
        }
        // And the node holds the peer: the admin side holds both ready from their node.started reports,
        // which tell nothing of what either has heard of the other.
        let node_view = &renamed_node.topology_view.digest_view;
        let mut node_digests = node_view.digest_changes();
        while node_view.get(peer.node_id.as_str()).is_none() {
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

        let (d, _) = digest_view.get(renamed.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let target = NodeTarget::ExactNode(renamed.node_id.clone());
        let call = |req: StatusRequest| {
            let (client, target, opts) = (&admin.observer.client, &target, &opts);
            async move {
                if let Some(reply) = crate::common::lifecycle_reply(client, target, &req, opts, None).await { return reply; }
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

        // node.connection.delete { All }: every route closed, each drop under the name it holds.
        let open_ids: Vec<_> = renamed_seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        let deleted = crate::common::node_command_completion(&admin.observer.client, &target, &NodeCommandRequest::DeleteConnections { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), scope: ConnectionScope::All }).await;
        let NodeCommandReply::ConnectionsDeleted { closed, not_open, held, .. } = &deleted else { panic!("the cut ends node.connections.deleted: {deleted:?}") };
        assert!(open_ids.iter().all(|id| closed.contains(id)) && not_open.is_empty() && held.is_empty(), "every open entry is closed and none held: open {open_ids:?}, {deleted:?}");

        // The same apply now takes the name.
        assert_eq!(call(apply).await, StatusReply::Applied, "with no connection held the name moves");
        assert_eq!(renamed_seams.connections.own().name, taken, "the node holds its new name");

        // It joins its mesh again under the name it holds (its admission block lifts there), then dials
        // its peer again: the node's next entry names the name it holds.
        let key = node::load_or_mint_key(&root.join(born.to_string())).unwrap();
        admin.deployed_as(&renamed, &key, taken.clone(), Some(taken.clone()));
        let join = StatusRequest::JoinMesh { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), build_id: "4wd8hn3tqe6c".into(), attempt: 1, operation: format!("node.mesh.join:{taken}") };
        assert!(matches!(call(join).await, StatusReply::MeshJoined { .. }), "the node joins its mesh again");
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
            let drop = log.iter().find(|e| e.source.name == born && e.destination.name.to_string() == dest && e.state == ConnectionState::Disconnected && e.reason.as_deref() == Some("node.connection.delete"));
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
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.connection.delete.via-delete-connections").map(|(_, _, f)| f.clone()).collect();
    assert!(cuts.iter().any(|f| f.get("scope").map(String::as_str) == Some("all") && f.get("held").map(String::as_str) == Some("0") && f.get("outcome").map(String::as_str) == Some("connections-deleted")), "{cuts:#?}");
}

/// CONTRACT (R-CR1, node-rpc-envelope.md Node properties `name`: "the node admits no connection";
/// node-mesh-leave.md step 2; Q169): a node decides its rename and its leave from the connections
/// open now, never from the rows that have landed. `mesh1.rpc.7`'s connections store refuses every
/// write, then the node opens a data-plane connection to its admin: the connection is open, its
/// Connected row owed and not landed. `ApplyNodeName { mesh1.rpc.1 }` answers
/// `BusyHoldingConnections` naming that connection, and `LeaveMesh` answers the same and leaves
/// nothing; the name does not move. Once the store takes writes the row lands under the id the
/// refusals named. What must NOT happen: a rename or a leave let through while a connection is
/// open because its row is still queued.
#[test]
fn a_node_whose_connected_row_is_still_owed_is_refused_its_name_and_its_leave() {
    let cap = crate::common::capture("a_node_whose_connected_row_is_still_owed_is_refused_its_name_and_its_leave");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("rename-precondition-owed-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.7".parse().unwrap();
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let dir = root.join(born.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let renamed = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric.clone(),
            name: born.clone(),
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
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        admin.deployed(&renamed, &key);
        let seams: Arc<OnceLock<Seams>> = Arc::default();
        let slot = seams.clone();
        let _running = node::start_with_seams(&renamed, move |b, s| {
            let _ = slot.set(s);
            b
        })
        .await
        .unwrap_or_else(|e| panic!("{born} comes up: {e}"));
        let seams = seams.get().cloned().expect("the node handed its seams");
        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while digest_view.get(renamed.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            heard.changed().await.unwrap();
        }
        seams.connections.observations_landed().await;
        assert!(seams.connections.held_routes().await.is_empty(), "the node holds no connection yet");

        // Its store refuses every write; the node opens a data-plane connection to its admin: open
        // now, its Connected row owed.
        seams.fault.arm(u32::MAX, u32::MAX, 0);
        match seams.client.call::<Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &PingRequest::Ping { payload: b"the node reaches its admin".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the ping did not end in a reply: {other:?}"),
        }
        assert!(seams.connections.held_routes().await.is_empty(), "the Connected row has not landed: nothing is held");
        assert!(seams.connections.in_flight() > 0, "the Connected row is owed");

        let (d, _) = digest_view.get(renamed.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let target = NodeTarget::ExactNode(renamed.node_id.clone());
        let call = |req: StatusRequest| {
            let (client, target, opts) = (&admin.observer.client, &target, &opts);
            async move {
                if let Some(reply) = crate::common::lifecycle_reply(client, target, &req, opts, None).await { return reply; }
                match client.call::<Status>(target, &req, opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let apply = StatusRequest::ApplyNodeName { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), name: taken.to_string() };
        let busy = call(apply).await;
        let StatusReply::BusyHoldingConnections { connections } = &busy else { panic!("a node with an open connection keeps its name, its row owed or not: {busy:?}") };
        assert_eq!(connections.len(), 1, "the one open connection is named: {busy:?}");
        assert_eq!(seams.connections.own().name, born, "the node holds the name it had");
        let leave = StatusRequest::LeaveMesh { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), build_id: "evnhx9g6n82z".into(), attempt: 1, operation: format!("node.mesh.leave:{born}") };
        assert_eq!(call(leave).await, busy, "the leave is refused naming the same open connection");

        // The store takes writes: the row lands under the id the refusals named.
        seams.fault.release();
        tokio::time::timeout(Duration::from_secs(30), seams.connections.observations_landed()).await.expect("the owed row is retried until it lands");
        let held: Vec<_> = seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        assert_eq!(&held, connections, "the landed entry is the connection the refusals named");
        let _ = std::fs::remove_dir_all(&root);
    });
}
