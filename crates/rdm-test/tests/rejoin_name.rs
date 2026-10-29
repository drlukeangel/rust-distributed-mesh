//! The start of a node renamed off its mesh and then stopped (R-JN1; lifecycles/node-replace.md
//! steps 1-2 and Rollback step 2; lifecycles/node-name-update.md; lifecycles/node-start.md step 2): a
//! node is renamed only while it is live, its connections deleted and its mesh left, and it is stopped
//! after, so it stops under the name it took and starts under the name it holds. A stopped node is
//! never renamed: its mesh-primary refuses it (the rename precondition's cells in rdm-admin's
//! status_declarations and node_stop's process cell).
//!
//! Its own process: the cell holds process-wide state (the stop flag).

use crate::common::admin_side_serving;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rdm_admin::model::NodeStatus;
use rdm_admin::storage::NodeRow;
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_test::node;
use std::time::Duration;

async fn call(admin: &crate::common::AdminSide, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    if let Some(reply) = crate::common::lifecycle_reply(&admin.observer.client, target, req, &opts, Some(&admin.observer.topology_view.digest_view)).await { return reply; }
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

/// CONTRACT (node-replace.md steps 1-2: node[1]'s `node.connection.delete { All }`, then its
/// `node.mesh.leave`, then `node.name.update` to `<name>.old`, then node[1].stop "leaves it stopped as
/// `<name>.old`"; node-mesh-leave.md: once it has left, "the node publishes nothing"; Rollback step 2;
/// R-JN1): a node born `mesh1.rpc.1` has its connections deleted, leaves its mesh (`MeshLeft`) and
/// takes `mesh1.rpc.1.old` live: `ApplyNodeName` answers `Applied`, and the node publishes nothing off
/// its mesh (its apply span says `digest=off-mesh`, and its admin hears no digest of it from the leave
/// until its start). It is then stopped: its stop span names `mesh1.rpc.1.old`. Its start presents the
/// name it holds and is admitted under it: every digest after the join names `mesh1.rpc.1.old`, and its
/// start span names it. What must NOT happen: a digest under either name while the node is off its
/// mesh, a stop span naming `mesh1.rpc.1`, or the start refused `JoinMismatch("name")`.
#[test]
fn a_node_renamed_off_its_mesh_then_stopped_stops_and_starts_under_the_name_it_holds() {
    let cap = crate::common::capture("a_node_renamed_off_its_mesh_then_stopped_stops_and_starts_under_the_name_it_holds");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_node_renamed_off_its_mesh_then_stopped_stops_and_starts_under_the_name_it_holds") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
        let dir = std::env::temp_dir().join(format!("rejoin-held-name-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
        let taken: PathName = born.renamed();
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
        let running = node::start(&launch, |b, _| b).await.expect("the node comes up");

        let digest_view = &admin.observer.topology_view.digest_view;
        let mut digests = digest_view.digest_changes();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            digests.changed().await.unwrap();
        }
        let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        // Step 1: every connection deleted, then off its mesh.
        let delete = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &delete).await, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "every connection is deleted");
        let leave = StatusRequest::LeaveMesh { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), attempt: 1, operation: "node.mesh.leave:mesh1.rpc.1".into() };
        assert!(matches!(call(&admin, &target, &leave).await, StatusReply::MeshLeft { .. }), "the node leaves its mesh");
        // What the node published is read from the node itself: its own view records every digest it
        // publishes. Its admin hears a digest eventually, so its admin's view read at the leave can lack
        // a heartbeat the node published just before it.
        let published = || running.topology_view.digest_view.get(launch.node_id.as_str()).expect("the node's own view records what it published").0.digest_seq;
        let seq_at_leave = published();
        // Step 2: the name, live and off its mesh; then the stop.
        let apply = StatusRequest::ApplyNodeName { node_id: node_id.clone(), incarnation: incarnation.clone(), name: taken.to_string() };
        assert_eq!(call(&admin, &target, &apply).await, StatusReply::Applied);
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1.old".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), attempt: 2, operation: "node.start:mesh1.rpc.1.old".into() };
        assert!(matches!(call(&admin, &target, &stop).await, StatusReply::Stopped { .. }));
        assert_eq!(published(), seq_at_leave, "the node published nothing from its leave to its stop");

        // The authority holds the birth under its row's name, and its held stopped digest names it too.
        admin.put_row(NodeRow { topology_storage_provider: None, name: taken.clone(), node_id: node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None });
        admin.deployed_as(&launch, &key, taken.clone(), Some(taken.clone()));

        assert_eq!(call(&admin, &target, &start).await, StatusReply::Started, "the start presents the name the node holds");
        digests.borrow_and_update();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_leave || d.status != MemberStatus::Ready) {
            let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
            if d.digest_seq > seq_at_leave {
                assert_eq!(d.node.name, taken, "every digest after the start's join names the name the node holds");
            }
            digests.changed().await.unwrap();
        }
        assert_eq!(digest_view.get(launch.node_id.as_str()).unwrap().0.node.name, taken, "the started node publishes the name it holds");
        // Its apply published nothing; its stop and start name the name it holds (R12).
        let applies: Vec<(String, String)> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.update.via-apply-node-name").map(|(_, _, f)| (f["to"].clone(), f["digest"].clone())).collect();
        assert_eq!(applies, vec![(taken.to_string(), "off-mesh".to_string())], "the apply off its mesh publishes nothing");
        for span in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.status.update.via-start-node"] {
            let named: Vec<String> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == span).map(|(_, _, f)| f["node"].clone()).collect();
            assert_eq!(named, vec![taken.to_string()], "{span} names the name the node holds");
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}
