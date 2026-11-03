//! The start of a node renamed and then stopped (R-JN1; lifecycles/node-name-update.md;
//! lifecycles/node-start.md step 2): a node holding no connection takes its row's name and publishes
//! it, and a stop that follows (Q347: "To keep a node aside, node.stop follows as its own primitive")
//! stops it under the name it took; its start presents the name it holds.
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
    let opts = CallOptions { send: rdm_transport::Send::with_time_limit(Duration::from_secs(30) + rdm_transport::TRANSPORT_LIMIT), ..CallOptions::default() };
    if let Some(reply) = crate::common::lifecycle_reply(&admin.observer.client, target, req, &opts, Some(&admin.observer.topology_view.digest_view)).await { return reply; }
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

/// CONTRACT (node-name-update.md step 5: the node "holds the new name and publishes it"; R-JN1): a node
/// born path `mesh1.node`, name `1` has its connections deleted and takes path `mesh1.node`, name
/// `renamed` live: `ApplyNodeName` answers `Applied` and its apply span says `digest=published`. It is
/// then stopped and started: its stop and start select the same node id, its start presents the name
/// it holds and is admitted under it, and every digest after the apply names path `mesh1.node`, name
/// `renamed`. What must NOT happen: a digest under name `1` after the apply, or the start refused
/// `JoinMismatch("name")`.
#[test]
fn a_node_renamed_then_stopped_stops_and_starts_under_the_name_it_holds() {
    let cap = crate::common::capture("a_node_renamed_then_stopped_stops_and_starts_under_the_name_it_holds");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_node_renamed_then_stopped_stops_and_starts_under_the_name_it_holds") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
        let dir = std::env::temp_dir().join(format!("rejoin-held-name-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let born: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let taken: PathName = born.with_name("renamed").unwrap();
        assert_eq!(taken.path, born.path, "rename preserves the immutable path");
        assert_eq!(taken.name, "renamed");
        let launch = Launch { initial_properties: Default::default(), topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric,
            mesh: "mesh1".into(),
            r#type: "node".into(),
            path: born.path.clone(), name: born.name.clone(),
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
        admin.observer.resolver.insert(ResolvedNode { mesh: d.node.mesh.clone(), r#type: d.node.r#type, node_id: d.node.node_id.clone(), path: d.node.path.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        // Every connection deleted.
        let delete = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        assert!(matches!(crate::common::node_command_completion(&admin.observer.client, &target, &delete).await, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { held, .. } if held.is_empty()), "every connection is deleted");
        // What the node published is read from the node itself: its own view records every digest it
        // publishes.
        let published = || running.topology_view.digest_view.get(launch.node_id.as_str()).expect("the node's own view records what it published").0.digest_seq;
        // The name, live; then the stop.
        let apply = StatusRequest::ApplyNodeName { node_id: node_id.clone(), incarnation: incarnation.clone(), name: taken.clone() };
        assert_eq!(call(&admin, &target, &apply).await, StatusReply::Applied);
        let seq_at_apply = published();
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), operation: rdm_core::OperationId::node("node.stop", &node_id) };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), operation: rdm_core::OperationId::node("node.start", &node_id) };
        assert!(matches!(call(&admin, &target, &stop).await, StatusReply::Stopped { .. }));

        // The authority holds the birth under its row's name, and its held stopped digest names it too.
        admin.put_row(NodeRow { mesh: launch.mesh.clone(), r#type: launch.r#type.clone(), initial_properties: None, topology_storage_provider: None, path: taken.path.clone(), name: taken.name.clone(), node_id: node_id.clone(), status: NodeStatus::Leaving, allocated: None, renamed_from: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, writer: None });
        admin.deployed_as(&launch, &key, taken.clone(), Some(taken.clone()));

        assert_eq!(call(&admin, &target, &start).await, StatusReply::Started, "the start presents the name the node holds");
        digests.borrow_and_update();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_apply || d.status != MemberStatus::Ready) {
            let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
            if d.digest_seq >= seq_at_apply {
                assert_eq!(d.node.location(), taken, "every digest after the apply names the name the node holds");
            }
            digests.changed().await.unwrap();
        }
        assert_eq!(digest_view.get(launch.node_id.as_str()).unwrap().0.node.location(), taken, "the started node publishes the name it holds");
        // Its apply published the name; its stop and start select the same node (R12).
        let applies: Vec<(String, String, String)> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.update.via-apply-node-name").map(|(_, _, f)| (f["to_path"].clone(), f["to_name"].clone(), f["digest"].clone())).collect();
        assert_eq!(applies, vec![(taken.path.clone(), taken.name.clone(), "published".to_string())], "the apply publishes the name");
        for span in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.status.update.via-start-node"] {
            let named: Vec<String> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == span).map(|(_, _, f)| f["node"].clone()).collect();
            assert_eq!(named, vec![node_id.to_string()], "{span} selects the same node ID after rename");
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}
