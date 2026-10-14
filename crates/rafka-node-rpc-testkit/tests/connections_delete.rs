//! `node.connections.delete { scope: Connections([id, …]) }` at the birth (R-CR3; rulings
//! 2026-10-11): it names exact connections by the minted id of their open entry, as
//! `node.connections.get` returns them, and cuts only those; an id that is unknown or already
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
use rafka_mesh_entity::connections::{ConnectionKind, ConnectionState, NodeConnection};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{ConnectionId, FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeResolver, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingRequest};
use rafka_node_rpc_contract::status::{ConnectionScope, Status, StatusReply, StatusRequest};
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
fn a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id() {
    let cap = crate::common::capture("a_connections_delete_naming_exact_ids_cuts_only_those_and_names_every_id");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("connections-delete-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        // The cell reads the node's kept log on the store root: its fabric is past its first state-commit.
        crate::common::store_past_state_commit(&store, &fabric).await;
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
                let peer_name: PathName = "mesh1.rpc.2".parse().unwrap();
        let launch_of = |name: &PathName| {
            let dir = root.join(name.to_string());
            std::fs::create_dir_all(&dir).unwrap();
            RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
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
                connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
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
        let book = &admin.observer.membership.book;
        let mut heard = book.heard_changes();
        while [&cut, &peer].iter().any(|l| book.get(l.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic)) {
            heard.changed().await.unwrap();
        }
        let peer_book = &peer_node.membership.book;
        let mut peer_digests = peer_book.digest_changes();
        while peer_book.get(cut.node_id.as_str()).is_none() {
            peer_digests.changed().await.unwrap();
        }
        // And the node holds the peer: the admin side holds both ready from their node.started reports,
        // which tell nothing of what either has heard of the other.
        let node_book = &cut_node.membership.book;
        let mut node_digests = node_book.digest_changes();
        while node_book.get(peer.node_id.as_str()).is_none() {
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

        // The open entries, as the node holds them (what node.connections.get returns).
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

        let (d, _) = book.get(cut.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
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
        let delete = |ids: Vec<ConnectionId>| StatusRequest::DeleteConnections { node_id: cut.node_id.clone(), incarnation: cut.incarnation.clone(), scope: ConnectionScope::Connections(ids) };
        let never = ConnectionId::mint();
        let to_admin = open("mesh1.admin.1");
        let deleted = call(delete(vec![to_peer.id.clone(), never.clone()])).await;
        assert_eq!(deleted, StatusReply::ConnectionsDeleted { closed: vec![to_peer.id.clone()], not_open: vec![never.clone()], held: vec![to_admin.id.clone()], receipt: None }, "the peer route is cut, the unknown id named, the admin route still held");

        // Every way the cut can be wrong is named together.
        let mut wrong: Vec<String> = Vec::new();
        let mut expect = |holds: bool, what: String| {
            if !holds {
                wrong.push(what);
            }
        };
        expect(!pooled_to_peer(), "the node's data-plane client no longer pools a connection to its peer".into());
        let log = kept_log(&store, &fabric, &born);
        let drop = log.iter().rev().find(|e| e.kind == ConnectionKind::Direct && e.destination.node_id == peer.node_id);
        expect(
            drop.is_some_and(|e| e.state == ConnectionState::Disconnected && e.source.name == born && e.id != to_peer.id && e.reason.as_deref() == Some("node.connections.delete")),
            format!("the latest entry for the peer route is its drop, written under {born} with its own id and the cut's reason: {drop:?}"),
        );
        let admin_now = log.iter().rev().find(|e| e.kind == ConnectionKind::Direct && e.destination.name.to_string() == "mesh1.admin.1");
        expect(admin_now.is_some_and(|e| e.state == ConnectionState::Connected), format!("the admin route was not named and is still connected: {admin_now:?}"));
        let again = call(delete(vec![to_peer.id.clone()])).await;
        expect(matches!(&again, StatusReply::ConnectionsDeleted { closed, not_open, .. } if closed.is_empty() && not_open == &vec![to_peer.id.clone()]), format!("the same id again is named not open: {again:?}"));
        let empty = call(delete(vec![])).await;
        expect(matches!(&empty, StatusReply::RejectedConnectionScope { reason } if reason.contains("at least one")), format!("an empty list is refused by name: {empty:?}"));
        let after = kept_log(&store, &fabric, &born).len();
        expect(after == log.len(), format!("the repeat and the refusal wrote nothing: {} entries, then {after}", log.len()));
        assert!(wrong.is_empty(), "{} wrong after the cut:\n{}", wrong.len(), wrong.join("\n"));
        let _ = std::fs::remove_dir_all(&root);
    });
    // The trace: one cut naming its scope, what it closed and what was not open.
    let cuts: Vec<std::collections::BTreeMap<String, String>> =
        spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.connections.delete.via-delete-connections").map(|(_, _, f)| f.clone()).collect();
    assert!(
        cuts.iter().any(|f| f.get("scope").map(String::as_str) == Some("connections") && f.get("closed").map(String::as_str) == Some("1") && f.get("not_open").map(String::as_str) == Some("1") && f.get("outcome").map(String::as_str) == Some("connections-deleted")),
        "one cut of the named connections: {cuts:#?}"
    );
}
