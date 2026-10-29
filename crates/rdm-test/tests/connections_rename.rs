//! A node's own connections end follows the name it holds (node-identity-and-state.md §8: `source`,
//! `destination` are "the two ends' path.names", `source_node_id`/`destination_node_id` "the minted
//! Node.id of the instance at each end when the entry was written"; R12/R13: the node owns its name
//! and `ApplyNodeName` makes it hold and publish the renamed one).
//!
//! CONTRACT: `mesh1.rpc.1` is born at that name, and the data-plane connection it originates to its
//! admin is a connections entry naming `mesh1.rpc.1` as its source. Its connections are deleted (R-CR1a),
//! it leaves its mesh, takes `mesh1.rpc.1.old` live and is stopped (lifecycles/node-replace.md steps 1-2),
//! then started under the name it holds; its peer `mesh1.rpc.2` hears the renamed digest and resolves
//! the birth at `mesh1.rpc.1.old`, and nothing at `mesh1.rpc.1`. Each then originates a data-plane
//! connection to the other: the renamed node's entry names `mesh1.rpc.1.old` as its source with its own
//! node id and birth, and is held as its own Direct; the peer's entry names `mesh1.rpc.1.old` as its
//! destination with the same node id. The Node RPC pool writes nothing (connections.md "Two planes, two words"). The entry written before the rename stays exactly as written (the log is kept). What
//! must NOT happen: an entry after the rename naming the name the node gave up, at either end, or
//! the earlier entry rewritten.

use crate::common::admin_side_serving;
use rdm_core::connections::{ConnectionKind, ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rdm_transport::{CallOptions, NodeResolver, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingRequest};
use rdm_admin::model::NodeStatus;
use rdm_admin::storage::NodeRow;
use rdm_core::status::{Status, StatusReply, StatusRequest};
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
fn a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends() {
    // The stop leaves the process stopped (its stop flag is process-wide): the cell runs in a process of its own.
    if crate::own_process::delegated(module_path!(), "a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends") {
        return;
    }
    let cap = crate::common::capture("a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends");
    let spans = cap.spans.clone();
    let during = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("connections-rename-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
        let taken: PathName = born.renamed();
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
        let (peer_node, peer_seams) = start(peer.clone()).await;

        // Both births are held ready by the admin, and each by the other.
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

        // The entry written under the birth's name: it originated a data-plane connection to its admin.
        match renamed_seams.client.call::<Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &PingRequest::Ping { payload: b"before the rename".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the node's data-plane ping did not end in a reply: {other:?}"),
        }
        renamed_seams.connections.observations_landed().await;
        let before = kept_log(&store, &fabric, &born);
        assert!(
            before.iter().any(|e| e.source.name == born && e.source.node_id == renamed.node_id && e.destination.name.to_string() == "mesh1.admin.1" && e.state == ConnectionState::Connected),
            "the data-plane connection is an entry naming the birth's name as its source: {before:#?}"
        );

        // The rename (R-CR1a; node-replace.md steps 1-2): every connection deleted, off its mesh, the name
        // taken live, then stopped, then started under the name it holds.
        let (d, _) = digest_view.get(renamed.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let target = NodeTarget::ExactNode(renamed.node_id.clone());
        let call = |req: StatusRequest| {
            let (client, target, opts, digest_view) = (&admin.observer.client, &target, &opts, &admin.observer.topology_view.digest_view);
            async move {
                if let Some(reply) = crate::common::lifecycle_reply(client, target, &req, opts, Some(digest_view)).await { return reply; }
                match client.call::<Status>(target, &req, opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let all = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &all).await, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "every connection is deleted");
        // The trace (R-CR6): the All deletes the data-plane connection on that connection's own span, and
        // no pool is hard-cut before the whole All.
        {
            let held = during.0.lock().unwrap();
            let deleted: Vec<u64> = held.iter().filter(|(_, (n, _, f))| n == "rdm.node_admin.connection.delete.via-connection-delete" && f.get("node") == Some(&born.to_string()) && f.get("destination").map(String::as_str) == Some("mesh1.admin.1")).map(|(k, _)| *k).collect();
            assert_eq!(deleted.len(), 1, "the All deletes the one data-plane connection on its own span");
            let early: Vec<_> = held.iter().filter(|(k, (n, _, _))| n == "rdm.node_rpc.connection.delete.via-hard-cut" && **k < deleted[0]).map(|(_, (_, _, f))| f.clone()).collect();
            assert!(early.is_empty(), "no pool is hard-cut before the All has deleted its connections: {early:#?}");
        }
        let leave = StatusRequest::LeaveMesh { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), build_id: "evnhx9g6n82z".into(), attempt: 1, operation: format!("node.mesh.leave:{born}") };
        assert!(matches!(call(leave).await, StatusReply::MeshLeft { .. }), "the node leaves its mesh");
        let apply = StatusRequest::ApplyNodeName { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), name: taken.to_string() };
        assert_eq!(call(apply).await, StatusReply::Applied, "the node takes the name off its mesh");
        let stop = StatusRequest::StopNode { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), build_id: "evnhx9g6n82z".into(), attempt: 1, operation: format!("node.stop:{taken}") };
        assert!(matches!(call(stop).await, StatusReply::Stopped { .. }), "the node stops");
        admin.put_row(NodeRow { topology_storage_provider: None, name: taken.clone(), node_id: renamed.node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None });
        let key = node::load_or_mint_key(&root.join(born.to_string())).unwrap();
        admin.deployed_as(&renamed, &key, taken.clone(), Some(taken.clone()));
        let start = StatusRequest::StartNode { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), build_id: "evnhx9g6n82z".into(), attempt: 2, operation: format!("node.start:{taken}") };
        assert_eq!(call(start).await, StatusReply::Started, "the node starts under the name it holds");

        // The peer hears the renamed digest, and the feed of its own process takes it into its
        // resolver: each answer change ticks the resolver. Its view holds the digest before the
        // wait, so what is left is the in-process feed; a resolver that never takes the name never
        // ticks, and the bound names that.
        while peer_view.get(renamed.node_id.as_str()).is_none_or(|(d, _)| d.node.name != taken) {
            peer_digests.changed().await.unwrap();
        }
        let resolver = &peer_node.node_rpc.resolver;
        let mut moved = resolver.changes().expect("the live resolver ticks on every answer change");
        let took = async {
            while resolver.resolve(&NodeTarget::ExactNode(renamed.node_id.clone())).map(|n| n.name).as_ref() != Ok(&taken) {
                moved.changed().await.unwrap();
            }
        };
        if tokio::time::timeout(Duration::from_secs(10), took).await.is_err() {
            panic!("the peer's view holds {taken} for the birth, and its resolver still names it {:?}", resolver.resolve(&NodeTarget::ExactNode(renamed.node_id.clone())).map(|n| n.name));
        }

        // The renamed node names the peer too, before it dials it: a node takes a peer's birth into its
        // resolver from that peer's own digest (gossip.md §3.1: an own-mesh birth's endpoints come from
        // its Digest, its status from the projection). Its restart may come before the peer's next
        // heartbeat reaches it.
        let own_resolver = &renamed_node.node_rpc.resolver;
        let mut own_moved = own_resolver.changes().expect("the live resolver ticks on every answer change");
        let holds_peer = async {
            while own_resolver.resolve(&NodeTarget::ExactNode(peer.node_id.clone())).is_err() {
                own_moved.changed().await.unwrap();
            }
        };
        if tokio::time::timeout(Duration::from_secs(10), holds_peer).await.is_err() {
            panic!("the renamed node's resolver never named the peer {peer_name}: {:?}", own_resolver.resolve(&NodeTarget::ExactNode(peer.node_id.clone())));
        }

        // Each dials the other on its data plane: one entry at each end.
        match peer_seams.client.call::<Ping>(&NodeTarget::ExactNode(renamed.node_id.clone()), &PingRequest::Ping { payload: b"after the rename".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the peer's ping did not end in a reply: {other:?}"),
        }
        match renamed_seams.client.call::<Ping>(&NodeTarget::ExactNode(peer.node_id.clone()), &PingRequest::Ping { payload: b"after the rename".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the renamed node's ping did not end in a reply: {other:?}"),
        }
        renamed_seams.connections.observations_landed().await;
        peer_seams.connections.observations_landed().await;

        // Every way the entries can be wrong is named together, so one run shows each end.
        let mut wrong: Vec<String> = Vec::new();
        let mut expect = |holds: bool, what: String| {
            if !holds {
                wrong.push(what);
            }
        };
        let after = kept_log(&store, &fabric, &born);
        expect(after.get(..before.len()) == Some(&before[..]), format!("the entries written before the rename stay exactly as written: {after:#?}"));
        let accepted = after[before.len()..]
            .iter()
            .find(|e| e.kind == ConnectionKind::Direct && e.state == ConnectionState::Connected && e.destination.node_id == peer.node_id)
            .unwrap_or_else(|| panic!("the renamed node wrote an entry for its dial to the peer: {after:#?}"));
        expect(
            (&accepted.source.name, &accepted.source.node_id, accepted.source.incarnation.as_ref()) == (&taken, &renamed.node_id, Some(&renamed.incarnation)),
            format!("the renamed node's entry names {taken} as its source, with its own node id {} and birth: source {:?}", renamed.node_id, accepted.source),
        );
        expect(accepted.destination.name == peer_name, format!("the renamed node's entry names {peer_name} as its destination: {:?}", accepted.destination));
        let own_end = renamed_seams.connections.own().name.clone();
        expect(own_end == taken, format!("the writer's own end is {taken}, the name the node holds: {own_end}"));
        let held = renamed_seams.connections.held();
        let own: Vec<NodeConnection> = held.lock().unwrap().own_latest_directs().into_iter().cloned().collect();
        expect(own.iter().any(|e| e == accepted), format!("the entry it wrote after the rename is this node's own Direct: {own:#?}"));

        let at = resolver.resolve(&NodeTarget::ExactNode(renamed.node_id.clone())).map(|n| n.name);
        expect(at.as_ref() == Ok(&taken), format!("the peer resolves the birth by the name it holds, {taken}: {at:?}"));
        let holder = resolver.resolve(&NodeTarget::CurrentPath(taken.clone())).map(|n| n.node_id);
        expect(holder.as_ref() == Ok(&renamed.node_id), format!("{taken} resolves to the renamed birth at the peer: {holder:?}"));
        let gave_up = resolver.resolve(&NodeTarget::CurrentPath(born.clone())).map(|n| n.node_id);
        expect(gave_up.is_err(), format!("nobody holds {born} at the peer once the birth gave it up: {gave_up:?}"));
        let dialled = kept_log(&store, &fabric, &peer_name);
        let dial = dialled
            .iter()
            .rev()
            .find(|e| e.kind == ConnectionKind::Direct && e.state == ConnectionState::Connected && e.destination.node_id == renamed.node_id)
            .unwrap_or_else(|| panic!("the peer wrote an entry for its dial: {dialled:#?}"));
        expect(
            (&dial.source.name, &dial.destination.name) == (&peer_name, &taken),
            format!("the peer's entry names the renamed node by {taken}, the name it holds: {} -> {}", dial.source.name, dial.destination.name),
        );
        assert!(wrong.is_empty(), "{} wrong after the rename:\n{}", wrong.len(), wrong.join("\n"));
        let _ = std::fs::remove_dir_all(&root);
    });
    // The trace: each end's entry is one observed fact naming the held name.
    let observed: Vec<std::collections::BTreeMap<String, String>> =
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.connection.update.via-observed").map(|(_, _, f)| f.clone()).collect();
    for (source, destination, origin) in [("mesh1.rpc.1.old", "mesh1.rpc.2", "dial"), ("mesh1.rpc.2", "mesh1.rpc.1.old", "dial")] {
        assert!(
            observed.iter().any(|f| f.get("source").map(String::as_str) == Some(source) && f.get("destination").map(String::as_str) == Some(destination) && f.get("origin").map(String::as_str) == Some(origin) && f.get("outcome").map(String::as_str) == Some("landed")),
            "an observed {origin} fact {source} -> {destination} landed: {observed:#?}"
        );
    }
}

/// CONTRACT: a node renamed off its mesh and then stopped records its next connection, after its start,
/// under the name it holds (lifecycles/node-replace.md steps 1-2; node-start.md step 2: the node installs
/// the handed name before it publishes anything; node-identity-and-state.md §8: a row names the ends'
/// path.names; R12: the node publishes only the row's name). `mesh1.rpc.1` originates a data-plane
/// connection to its admin, has its connections deleted, leaves its mesh, takes `mesh1.rpc.1.old` live,
/// is stopped and starts: its start writes no entry (its join is Node RPC, which is no connection), and
/// the next data-plane connection it originates to its admin names `mesh1.rpc.1.old` as its source,
/// with its own node id and birth. What must NOT happen: an entry written after the start naming
/// `mesh1.rpc.1`, the name the node gave up, or an entry written by the join itself.
#[test]
fn a_node_renamed_off_its_mesh_then_stopped_records_its_next_connection_under_the_name_it_holds() {
    // The stop leaves the process stopped (its stop flag is process-wide): the cell runs in a process of its own.
    if crate::own_process::delegated(module_path!(), "a_node_renamed_off_its_mesh_then_stopped_records_its_next_connection_under_the_name_it_holds") {
        return;
    }
    let cap = crate::common::capture("a_node_renamed_off_its_mesh_then_stopped_records_its_next_connection_under_the_name_it_holds");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("join-row-name-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
        let taken: PathName = born.renamed();
        let dir = root.join(born.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
            data_dir: dir.clone(),
            store_dir: store.clone(),
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        admin.deployed(&launch, &key);
        let seams: Arc<OnceLock<Seams>> = Arc::default();
        let slot = seams.clone();
        let _running = node::start_with_seams(&launch, move |b, s| {
            let _ = slot.set(s);
            b
        })
        .await
        .unwrap_or_else(|e| panic!("{} comes up: {e}", launch.name));
        let seams = seams.get().cloned().expect("the node handed its seams");
        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            heard.changed().await.unwrap();
        }

        let ping_admin = |payload: &'static [u8]| {
            let (seams, admin_id) = (seams.clone(), admin.launcher.node_id.clone());
            async move {
                match seams.client.call::<Ping>(&NodeTarget::ExactNode(admin_id), &PingRequest::Ping { payload: payload.to_vec() }, &CallOptions::default()).await.0 {
                    RpcOutcome::Reply(_) => {}
                    other => panic!("the node's data-plane ping did not end in a reply: {other:?}"),
                }
            }
        };
        ping_admin(b"before the stop").await;

        // Every connection deleted, off its mesh, the name taken live, then stopped.
        let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let call = |req: StatusRequest| {
            let (client, target, opts, digest_view) = (&admin.observer.client, &target, &opts, &admin.observer.topology_view.digest_view);
            async move {
                if let Some(reply) = crate::common::lifecycle_reply(client, target, &req, opts, Some(digest_view)).await { return reply; }
                match client.call::<Status>(target, &req, opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let all = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &all).await, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "every connection is deleted");
        let leave = StatusRequest::LeaveMesh { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "s1t5pj9dvg1p".into(), attempt: 1, operation: format!("node.mesh.leave:{born}") };
        assert!(matches!(call(leave).await, StatusReply::MeshLeft { .. }), "the node leaves its mesh");
        let apply = StatusRequest::ApplyNodeName { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), name: taken.to_string() };
        assert_eq!(call(apply).await, StatusReply::Applied, "the node takes the name off its mesh");
        let stop = StatusRequest::StopNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "s1t5pj9dvg1p".into(), attempt: 1, operation: format!("node.stop:{taken}") };
        assert!(matches!(call(stop).await, StatusReply::Stopped { .. }), "the node stops");
        seams.connections.observations_landed().await;
        let at_stop = kept_log(&store, &fabric, &born).len();
        admin.put_row(NodeRow { topology_storage_provider: None, name: taken.clone(), node_id: launch.node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None });
        admin.deployed_as(&launch, &key, taken.clone(), Some(taken.clone()));
        let start = StatusRequest::StartNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "s1t5pj9dvg1p".into(), attempt: 2, operation: format!("node.start:{taken}") };
        assert_eq!(call(start).await, StatusReply::Started, "the node starts under the name it holds");
        seams.connections.observations_landed().await;
        assert_eq!(kept_log(&store, &fabric, &born).len(), at_stop, "the start's join wrote no entry: Node RPC is no connection");
        ping_admin(b"after the start").await;
        seams.connections.observations_landed().await;

        // Every entry written after the start names the name the node holds, the handed one.
        let started = kept_log(&store, &fabric, &born)[at_stop..].to_vec();
        let next = started
            .iter()
            .find(|e| e.kind == ConnectionKind::Direct && e.state == ConnectionState::Connected && e.destination.name.to_string() == "mesh1.admin.1")
            .unwrap_or_else(|| panic!("the node's data-plane connection to its admin: an entry for it: {started:#?}"));
        assert_eq!(
            (&next.source.name, &next.source.node_id, next.source.incarnation.as_ref()),
            (&taken, &launch.node_id, Some(&launch.incarnation)),
            "the entry names {taken}, the name it holds, as its source: {started:#?}"
        );
        let gave_up: Vec<&NodeConnection> = started.iter().filter(|e| e.source.name == born).collect();
        assert!(gave_up.is_empty(), "no entry written after the start names {born}, the name the node gave up: {gave_up:#?}");
        let _ = std::fs::remove_dir_all(&root);
    });
}
