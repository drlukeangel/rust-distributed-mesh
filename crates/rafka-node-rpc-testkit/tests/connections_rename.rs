//! A node's own connections end follows the name it holds (fabric-node-lifecycle.md §8: `source`,
//! `destination` are "the two ends' path.names", `source_node_id`/`destination_node_id` "the minted
//! Node.id of the instance at each end when the entry was written"; R12/R13: the node owns its name
//! and `ApplyNodeName` makes it hold and publish the renamed one).
//!
//! CONTRACT: `mesh1.rpc.1` is born `mesh1.rpc.1.new`, and its join to its admin is a connections
//! entry naming `mesh1.rpc.1.new` as its source. Told `ApplyNodeName { mesh1.rpc.1 }` with nothing
//! in flight it answers `Applied`; its peer `mesh1.rpc.2` hears the renamed digest and resolves the
//! birth at `mesh1.rpc.1`, and nothing at `mesh1.rpc.1.new`. The peer then dials it: the renamed
//! node's accept entry names `mesh1.rpc.1` as its source with its own node id and birth, and is held
//! as its own Direct; the peer's dial entry names `mesh1.rpc.1` as its destination with the same
//! node id. The entry written before the rename stays exactly as written (the log is kept). What
//! must NOT happen: an entry after the rename naming the name the node gave up, at either end, or
//! the earlier entry rewritten.

use crate::common::admin_side_serving;
use rafka_mesh_entity::connections::{ConnectionKind, ConnectionState, NodeConnection};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeResolver, NodeTarget, ResolvedNode};
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
fn a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends() {
    let cap = crate::common::capture("a_renamed_nodes_next_connections_entries_name_the_name_it_holds_at_both_ends");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let root = std::env::temp_dir().join(format!("connections-rename-{}", NodeId::mint()));
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

        // Both births are held ready by the admin, and each by the other.
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

        // The entry written under the birth's name: its join dialled the admin.
        renamed_seams.connections.drain().await;
        let before = kept_log(&store, &fabric, &born);
        assert!(
            before.iter().any(|e| e.source.name == born && e.source.node_id == renamed.node_id && e.destination.name.to_string() == "mesh1.admin.1" && e.state == ConnectionState::Connected),
            "the join is an entry naming the birth's name as its source: {before:#?}"
        );

        // The rename, with nothing in flight.
        let (d, _) = book.get(renamed.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let apply = StatusRequest::ApplyNodeName { node_id: renamed.node_id.clone(), incarnation: renamed.incarnation.clone(), name: taken.to_string() };
        match admin.observer.client.call::<Status>(&NodeTarget::ExactNode(renamed.node_id.clone()), &apply, &opts).await.0 {
            RpcOutcome::Reply(r) => assert_eq!(r.into_value(), StatusReply::Applied),
            other => panic!("ApplyNodeName did not end in a reply: {other:?}"),
        }

        // The peer hears the renamed digest; its resolver, fed from its book, holds the birth at the
        // name it took and nothing at the one it gave up.
        while peer_book.get(renamed.node_id.as_str()).is_none_or(|(d, _)| d.node.name != taken) {
            peer_digests.changed().await.unwrap();
        }
        rafka_node_rpc_testkit::node_rpc::feed_once(peer_book, &peer_node.node_rpc.resolver, &peer_name.to_string());

        // The peer dials the renamed node: one entry at each end.
        match peer_seams.client.call::<Ping>(&NodeTarget::ExactNode(renamed.node_id.clone()), &PingRequest::Ping { payload: b"after the rename".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the peer's ping did not end in a reply: {other:?}"),
        }
        renamed_seams.connections.drain().await;
        peer_seams.connections.drain().await;

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
            .unwrap_or_else(|| panic!("the renamed node wrote an entry for the peer's dial: {after:#?}"));
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

        let resolver = &peer_node.node_rpc.resolver;
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
    for (source, destination, origin) in [("mesh1.rpc.1", "mesh1.rpc.2", "accept"), ("mesh1.rpc.2", "mesh1.rpc.1", "dial")] {
        assert!(
            observed.iter().any(|f| f.get("source").map(String::as_str) == Some(source) && f.get("destination").map(String::as_str) == Some(destination) && f.get("origin").map(String::as_str) == Some(origin) && f.get("outcome").map(String::as_str) == Some("landed")),
            "an observed {origin} fact {source} -> {destination} landed: {observed:#?}"
        );
    }
}
