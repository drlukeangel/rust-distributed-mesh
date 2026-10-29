//! `node.connection.delete { scope: Connections([id, …]) }` at the birth (R-CR3; rulings
//! 2026-10-11): it names exact connections by the minted id of their open entry, as
//! `node.connection.get` returns them, and cuts only those; an id that is unknown or already
//! closed is named, never ignored; each drop is written under the name the node holds; an empty
//! list is refused by name and cuts nothing.
//!
//! CONTRACT: `mesh1.rpc.1` holds a data-plane route to its admin and to its peer `mesh1.rpc.2`, each
//! a connection it originated on its data-plane client. Its admin's `DeleteConnections { Connections([<the peer route's open entry>,
//! <an id the node never wrote>]) }` is answered `ConnectionsDeleted` with the peer route's id in
//! `closed`, the unknown id in `not_open` and the admin route's id in `held`; the node's client no
//! longer pools a connection to the peer; its log holds a
//! `disconnected` entry for the peer under `mesh1.rpc.1`, with a new id, and its route to the admin
//! is still `connected`. The same id again is `not_open`. `Connections([])` is
//! `RejectedConnectionScope` and cuts nothing. The Node RPC pool is no connection: nothing of it is
//! cut. What must NOT happen: a cut of a connection not named,
//! a named id dropped silently, or the drop written under another name.

use crate::common::admin_side_serving;
use rdm_core::connections::{ConnectionKind, ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{ConnectionId, FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rdm_transport::{CallOptions, NodeResolver, NodeTarget, ResolvedNode};
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
fn a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id() {
    let cap = crate::common::capture("a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("connections-delete-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
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
        let (cut, peer) = (launch_of(&born), launch_of(&peer_name));
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
        let (cut_node, cut_seams) = start(cut.clone()).await;
        let (peer_node, _peer_seams) = start(peer.clone()).await;

        // Both births are held ready by the admin, and the peer holds the renamed one.
        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while [&cut, &peer].iter().any(|l| digest_view.get(l.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready)) {
            heard.changed().await.unwrap();
        }
        let peer_view = &peer_node.topology_view.digest_view;
        let mut peer_digests = peer_view.digest_changes();
        while peer_view.get(cut.node_id.as_str()).is_none() {
            peer_digests.changed().await.unwrap();
        }
        // And the node holds the peer: the admin side holds both ready from their node.started reports,
        // which tell nothing of what either has heard of the other.
        let node_view = &cut_node.topology_view.digest_view;
        let mut node_digests = node_view.digest_changes();
        while node_view.get(peer.node_id.as_str()).is_none() {
            node_digests.changed().await.unwrap();
        }
        while cut_seams.resolver.resolve(&NodeTarget::ExactNode(peer.node_id.clone())).is_err() {
            tokio::task::yield_now().await;
        }

        // The node originates a data-plane connection to its admin and one to its peer (connections.md
        // "Two planes, two words": in RDM standalone the testkit's originate door is the data plane):
        // the node holds a route to each, both pooled by its data-plane client.
        for (to, what) in [(admin.launcher.node_id.clone(), "the node reaches its admin"), (peer.node_id.clone(), "the node reaches the peer")] {
            match cut_seams.client.call::<Ping>(&NodeTarget::ExactNode(to), &PingRequest::Ping { payload: what.as_bytes().to_vec() }, &CallOptions::default()).await.0 {
                RpcOutcome::Reply(_) => {}
                other => panic!("{what}: the ping did not end in a reply: {other:?}"),
            }
        }
        cut_seams.connections.observations_landed().await;
        let before = kept_log(&store, &fabric, &born);
        for dest in ["mesh1.admin.1", "mesh1.rpc.2"] {
            assert!(before.iter().any(|e| e.source.name == born && e.destination.name.to_string() == dest), "the node holds a route to {dest}: {before:#?}");
        }

        // The open entries, as the node holds them (what node.connection.get returns).
        let open = |dest: &str| -> NodeConnection {
            let held = cut_seams.connections.held();
            let held = held.lock().unwrap();
            held.own_latest_directs().into_iter().find(|e| e.destination.name.to_string() == dest && e.state == ConnectionState::Connected).cloned().unwrap_or_else(|| panic!("the node holds an open route to {dest}"))
        };
        let to_peer = open("mesh1.rpc.2");
        let peer_key = peer.node_id.clone();
        let pooled_to_peer = || {
            let key = cut_seams.resolver.resolve(&NodeTarget::ExactNode(peer_key.clone())).map(|n| n.endpoint_id).ok();
            cut_seams.client.pooled().iter().any(|k| Some(k.peer) == key)
        };
        assert!(pooled_to_peer(), "the node pools a connection to its peer before the cut");

        let (d, _) = digest_view.get(cut.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let target = NodeTarget::ExactNode(cut.node_id.clone());
        let call = |req: StatusRequest| {
            let (client, target, opts) = (&admin.observer.client, &target, &opts);
            async move {
                match client.call::<Status>(target, &req, opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let delete = |ids: Vec<ConnectionId>| NodeCommandRequest::DeleteConnections { node_id: cut.node_id.clone(), incarnation: cut.incarnation.clone(), scope: ConnectionScope::Connections(ids) };
        let never = ConnectionId::mint();
        let to_admin = open("mesh1.admin.1");
        let deleted = crate::common::node_command_completion(&admin.observer.client, &target, &delete(vec![to_peer.id.clone(), never.clone()])).await;
        assert_eq!(deleted, NodeCommandReply::ConnectionsDeleted { closed: vec![to_peer.id.clone()], not_open: vec![never.clone()], held: vec![to_admin.id.clone()], receipt: None }, "the peer route is cut, the unknown id named, the admin route still held");

        // Every way the cut can be wrong is named together.
        let mut wrong: Vec<String> = Vec::new();
        let mut expect = |holds: bool, what: String| {
            if !holds {
                wrong.push(what);
            }
        };
        expect(!pooled_to_peer(), "the node's data-plane client no longer pools a connection to its peer".into());
        // The drop is queued and lands behind the answer (Q169): the cell reads the log once it has.
        cut_seams.connections.observations_landed().await;
        let log = kept_log(&store, &fabric, &born);
        let drop = log.iter().rev().find(|e| e.kind == ConnectionKind::Direct && e.destination.node_id == peer.node_id);
        expect(
            drop.is_some_and(|e| e.state == ConnectionState::Disconnected && e.source.name == born && e.id != to_peer.id && e.reason.as_deref() == Some("node.connection.delete")),
            format!("the latest entry for the peer route is its drop, written under {born} with its own id and the cut's reason: {drop:?}"),
        );
        let admin_now = log.iter().rev().find(|e| e.kind == ConnectionKind::Direct && e.destination.name.to_string() == "mesh1.admin.1");
        expect(admin_now.is_some_and(|e| e.state == ConnectionState::Connected), format!("the admin route was not named and is still connected: {admin_now:?}"));
        let again = crate::common::node_command_completion(&admin.observer.client, &target, &delete(vec![to_peer.id.clone()])).await;
        expect(matches!(&again, NodeCommandReply::ConnectionsDeleted { closed, not_open, .. } if closed.is_empty() && not_open == &vec![to_peer.id.clone()]), format!("the same id again is named not open: {again:?}"));
        let empty = crate::common::node_command_completion(&admin.observer.client, &target, &delete(vec![])).await;
        expect(matches!(&empty, NodeCommandReply::RejectedConnectionScope { reason } if reason.contains("at least one")), format!("an empty list is refused by name: {empty:?}"));
        let after = kept_log(&store, &fabric, &born).len();
        expect(after == log.len(), format!("the repeat and the refusal wrote nothing: {} entries, then {after}", log.len()));
        assert!(wrong.is_empty(), "{} wrong after the cut:\n{}", wrong.len(), wrong.join("\n"));
        let _ = std::fs::remove_dir_all(&root);
    });
    // The trace: one cut naming its scope, what it closed and what was not open.
    let cuts: Vec<std::collections::BTreeMap<String, String>> =
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.connection.delete.via-delete-connections").map(|(_, _, f)| f.clone()).collect();
    assert!(
        cuts.iter().any(|f| f.get("scope").map(String::as_str) == Some("connections") && f.get("closed").map(String::as_str) == Some("1") && f.get("not_open").map(String::as_str) == Some("1") && f.get("outcome").map(String::as_str) == Some("connections-deleted")),
        "one cut of the named connections: {cuts:#?}"
    );
}

/// CONTRACT (node-connections-delete.md steps 3.4 to 3.6, "Bound"; Q169): a `node.connection.delete
/// { All }` decides what to close from the connections open now, closes them, queues their drops and
/// answers: it never waits for a row. `mesh1.rpc.1` holds a route to its admin; its connections store
/// then refuses every write (a store reached over a connection the delete cuts). Its `All` answers
/// `ConnectionsDeleted` carrying `Established` well within `RDM_CONNECTIONS_DELETE_DEADLINE_MS`, the
/// admin route closed and nothing still open, though the log still holds the route open (its drop
/// has not landed); the drop stays owed and lands once the store takes writes again, and the log then
/// holds nothing. What must NOT happen: a delete that waits for a drop that cannot land, or a
/// connection left open.
#[test]
fn an_all_whose_drops_cannot_land_answers_at_once_and_closes_every_live_connection() {
    let cap = crate::common::capture("an_all_whose_drops_cannot_land_answers_at_once_and_closes_every_live_connection");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("connections-delete-bound-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
        let deadline = Duration::from_millis(1500);
        let dir = root.join(born.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let cut = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
            connections_delete_deadline: deadline,
            declared: Default::default(),
        };
        admin.deployed(&cut, &key);
        let seams: Arc<OnceLock<Seams>> = Arc::default();
        let slot = seams.clone();
        let _running = node::start_with_seams(&cut, move |b, s| {
            let _ = slot.set(s);
            b
        })
        .await
        .unwrap_or_else(|e| panic!("{born} comes up: {e}"));
        let seams = seams.get().cloned().expect("the node handed its seams");
        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while digest_view.get(cut.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            heard.changed().await.unwrap();
        }

        // The node holds a route to its admin, its Connected landed.
        match seams.client.call::<Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &PingRequest::Ping { payload: b"the node reaches its admin".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the ping did not end in a reply: {other:?}"),
        }
        seams.connections.observations_landed().await;
        let open: Vec<ConnectionId> = seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        assert_eq!(open.len(), 1, "the node holds its route to its admin: {open:?}");

        // From here its connections store refuses every write.
        seams.fault.arm(u32::MAX, u32::MAX, 0);
        let (d, _) = digest_view.get(cut.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(cut.node_id.clone());
        let began = std::time::Instant::now();
        let deleted = crate::common::node_command_completion(&admin.observer.client, &target, &NodeCommandRequest::DeleteConnections { node_id: cut.node_id.clone(), incarnation: cut.incarnation.clone(), scope: ConnectionScope::All }).await;
        let took = began.elapsed();
        let NodeCommandReply::ConnectionsDeleted { closed, not_open, held, receipt } = &deleted else { panic!("the delete ends node.connections.deleted: {deleted:?}") };
        assert!(took < deadline, "the delete answers without waiting for its drop, well within its bound ({deadline:?}): {took:?}");
        assert_eq!((closed, not_open, held), (&open, &Vec::new(), &Vec::new()), "the route is closed and nothing is still open: {deleted:?}");
        assert_eq!(receipt, &Some(rdm_core::status::ConnectionsReceipt::Established { in_flight: 0, closed: open.clone() }), "no handler was in flight: the closure is Established");
        assert!(seams.connections.live_routes(Some(seams.client.as_ref()), None).is_empty(), "the node has no connection open");
        assert!(seams.connections.in_flight() > 0, "the drop stays owed");
        let still: Vec<ConnectionId> = seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        assert_eq!(still, open, "the log still holds the route open: the answer did not wait for its drop");

        // The store takes writes again: the owed drop lands and the log holds nothing.
        seams.fault.release();
        tokio::time::timeout(Duration::from_secs(30), seams.connections.observations_landed()).await.expect("the owed drop is retried until it lands");
        assert!(seams.connections.held_routes().await.is_empty(), "the drop landed: the log holds nothing open");
        let _ = std::fs::remove_dir_all(&root);
    });
}
