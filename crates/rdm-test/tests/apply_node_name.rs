//! A live node takes the name its mesh-primary's row now holds (`ApplyNodeName`; node-rpc-envelope.md
//! "Meta and config", R12/R13, R-CR1).
//!
//! CONTRACT: a node born `mesh1.rpc.7` that originated a data-plane connection to its admin
//! answers `ApplyNodeName { name: mesh1.rpc.1 }` with `BusyHoldingConnections` naming that connection
//! by the minted id of its open entry (proposal node-properties-own-commands.md §2.2 step 6), and holds nothing:
//! its digest still says `mesh1.rpc.7`. The cell runs lifecycles/node-name-update.md's order ("Rename precondition": the
//! node's `node.connection.delete { All }` has completed): `All`, held open by one call in flight
//! at the node until the cell releases it, answers `ConnectionsDeleted` with an `Established`
//! receipt counting that call and closing the route; only then is the name applied: `Applied`, and
//! the node holds `mesh1.rpc.1`; the same name again is `AlreadyApplied`, and a stale incarnation is
//! refused naming the one held. What must NOT happen: a name taken while the node holds a
//! connection, `All` answered while the call it admitted is in flight, or a digest naming the new
//! name before the apply.
//!
//! The trace (R-CR6): the route's observed fact (`rdm.node_admin.connection.update.via-observed`)
//! names its entry and the peer it was dialled to, and `All` deletes that data-plane connection on
//! the connection's own span (`rdm.node_admin.connection.delete.via-connection-delete`) naming the
//! same entry and peer. What must NOT happen: `All` hard-cutting a pool
//! (`rdm.node_rpc.connection.delete.via-hard-cut`): that is the stop's own transport cut alone.

use crate::common::admin_side_serving;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, Status, StatusReply, StatusRequest};
use rdm_test::pre_node_ready_probe::{PreNodeReadyPull, PullRequest, PullServer};
use rdm_test::node;
use std::time::Duration;

#[test]
fn a_live_node_takes_its_rows_name_only_once_its_connections_are_deleted() {
    let cap = crate::common::capture("a_live_node_takes_its_rows_name_only_once_its_connections_are_deleted");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let dir = std::env::temp_dir().join(format!("apply-node-name-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let born: PathName = "mesh1.rpc.7".parse().unwrap();
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric,
            name: born.clone(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![admin.seed.clone()],
            launcher: Some(admin.launcher.clone()),
            data_dir: dir.clone(),
            store_dir: dir.clone(),
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        admin.deployed(&launch, &key);
        // The node serves one op the cell can hold: a call into it stays in flight until released.
        let pull = PullServer::new(vec![]);
        pull.gate.open();
        let seams: std::sync::Arc<std::sync::OnceLock<rdm_test::originate::Seams>> = Default::default();
        let running = node::start_with_seams(&launch, {
            let (pull, slot) = (pull.clone(), seams.clone());
            move |b, s| {
                let _ = slot.set(s);
                pull.serve(b)
            }
        })
        .await
        .expect("the node comes up");
        let seams = seams.get().cloned().expect("the node handed its seams");

        let digest_view = &admin.observer.topology_view.digest_view;
        // Every digest the admin's view takes ticks this count: the cell awaits a published digest, never the cadence.
        let mut digests = digest_view.digest_changes();
        let mut heard = digest_view.heard_changes();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            heard.changed().await.unwrap();
        }
        let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
        assert_eq!(d.node.name, born);
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let client = &admin.observer.client;
        let call = |req: StatusRequest| {
            let (target, opts) = (target.clone(), opts.clone());
            async move {
                match client.call::<Status>(&target, &req, &opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let apply = StatusRequest::ApplyNodeName { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), name: taken.to_string() };

        // The node originates a data-plane connection to its admin: a Direct entry of its log.
        match seams.client.call::<Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &PingRequest::Ping { payload: b"the node reaches its admin".to_vec() }, &CallOptions::default()).await.0 {
            RpcOutcome::Reply(_) => {}
            other => panic!("the node's data-plane ping did not end in a reply: {other:?}"),
        }
        seams.connections.observations_landed().await;
        // Holding that route: the name is refused, naming the connection by its entry's id, and nothing is held.
        let held_ids: Vec<_> = seams.connections.held_routes().await.into_iter().map(|r| r.id).collect();
        assert_eq!(held_ids.len(), 1, "the node holds the one route it dialled: {held_ids:?}");
        match call(apply.clone()).await {
            StatusReply::BusyHoldingConnections { connections } => assert_eq!(connections, held_ids, "the refusal names each connection the node holds"),
            other => panic!("a node holding a connection answered {other:?}"),
        }
        let seq = digest_view.get(launch.node_id.as_str()).unwrap().0.digest_seq;
        digests.borrow_and_update();
        while digest_view.get(launch.node_id.as_str()).unwrap().0.digest_seq == seq {
            digests.changed().await.unwrap();
        }
        assert_eq!(digest_view.get(launch.node_id.as_str()).unwrap().0.node.name, born, "a Busy node keeps publishing its birth's name");

        // node.connection.delete { All }, held open by one call in flight at the node: it answers
        // only once that call has its reply, its receipt counting it, and the route is closed.
        pull.hold();
        let held_call = async { client.call::<PreNodeReadyPull>(&target, &PullRequest::Pull, &opts).await.0 };
        let steps = async {
            pull.entered(1).await;
            let delete = NodeCommandRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
            let deleting = crate::common::node_command_completion(&client, &target, &delete);
            tokio::pin!(deleting);
            assert!(tokio::time::timeout(Duration::from_millis(300), &mut deleting).await.is_err(), "All is not answered while the call it admitted is in flight");
            pull.release();
            deleting.await
        };
        let (held_out, deleted) = tokio::join!(held_call, steps);
        assert!(matches!(held_out, RpcOutcome::Reply(_)), "the held call answers once released: {held_out:?}");
        assert!(
            matches!(&deleted, NodeCommandReply::ConnectionsDeleted { held, receipt: Some(ConnectionsReceipt::Established { in_flight: 1, closed }), .. } if held.is_empty() && closed.len() == 1),
            "every connection is deleted, the receipt counting the call in flight: {deleted:?}"
        );
        // The trace: the closed entry's data-plane connection is deleted on its own span, naming the
        // entry and the peer its observed fact named; All hard-cuts no pool.
        let closed = match &deleted {
            NodeCommandReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Established { closed, .. }), .. } => closed[0].to_string(),
            other => panic!("checked above: {other:?}"),
        };
        let fields_of = |name: &str| -> Vec<std::collections::BTreeMap<String, String>> { spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect() };
        let deletes = fields_of("rdm.node_admin.connection.delete.via-connection-delete");
        assert_eq!(deletes.len(), 1, "All deletes the one data-plane connection, on its own span: {deletes:#?}");
        assert_eq!((deletes[0].get("entry"), deletes[0].get("node").map(String::as_str), deletes[0].get("destination").map(String::as_str)), (Some(&closed), Some("mesh1.rpc.7"), Some("mesh1.admin.1")), "{deletes:#?}");
        let observed = fields_of("rdm.node_admin.connection.update.via-observed");
        let dialled = observed.iter().find(|f| f.get("entry") == Some(&closed)).unwrap_or_else(|| panic!("the observed fact names the entry All closed ({closed}): {observed:#?}"));
        assert_eq!((dialled.get("source").map(String::as_str), dialled.get("destination").map(String::as_str), dialled.get("origin").map(String::as_str)), (Some("mesh1.rpc.7"), Some("mesh1.admin.1"), Some("dial")), "{dialled:#?}");
        assert!(dialled.get("peer").is_some_and(|p| !p.is_empty()) && dialled.get("peer") == deletes[0].get("peer"), "the observed fact and the delete name the same peer: {dialled:#?} vs {deletes:#?}");
        assert!(fields_of("rdm.node_rpc.connection.delete.via-hard-cut").is_empty(), "All hard-cuts no pool");
        assert_eq!(call(apply.clone()).await, StatusReply::Applied);
        assert_eq!(running.topology_view.held_name(&born), taken, "the node holds the applied name");

        // The same name again is done; a stale incarnation is refused naming the one held.
        assert_eq!(call(apply.clone()).await, StatusReply::AlreadyApplied);
        let stale = StatusRequest::ApplyNodeName { node_id: launch.node_id.clone(), incarnation: IncarnationId::mint(), name: taken.to_string() };
        assert_eq!(call(stale).await, StatusReply::RejectedStaleIncarnation { held: launch.incarnation.clone() });
        let _ = std::fs::remove_dir_all(&dir);
    })
}
