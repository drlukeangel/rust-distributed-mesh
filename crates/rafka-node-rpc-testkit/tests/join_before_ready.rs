//! A node is ready for traffic only after its exact birth was admitted by `JoinNode`
//! (`lifecycles/node-ready-for-traffic.md` step 1; `node-birth.md` step 6).

use crate::common::{admin_side_deaf_to_joins, Spans};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_rpc_testkit::node;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// CONTRACT: a node whose launching admin answers every `JoinNode` with `NotReady` (the admin
/// holds no join door) ends by name, the error naming the launching admin and the unreached cause.
/// It never emits `rdm.mesh.node.update.via-ready` and the launcher never hears it ready for
/// traffic. What must NOT happen: the node comes up on a "view fills from gossip" fallback.
#[tokio::test]
async fn node_whose_launcher_never_takes_its_join_ends_by_name_and_is_never_ready() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let dir = std::env::temp_dir().join(format!("join-before-ready-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
    let fabric = FabricId::mint();
    let admin = admin_side_deaf_to_joins("127.0.0.1".parse().unwrap(), &fabric).await;
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.clone(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
    };
    let started = node::start(&launch, |b, _| b).await;
    let outcome = match started {
        Ok(running) => {
            running.stop(Duration::ZERO).await;
            None
        }
        Err(e) => Some(e.to_string()),
    };
    let node_id = launch.node_id.to_string();
    let heard_ready = admin.observer.membership.book.get(node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::ReadyForTraffic);
    let ready_spans = spans.0.lock().unwrap().values().filter(|(name, _, _)| name == "rdm.mesh.node.update.via-ready").count();
    let _ = std::fs::remove_dir_all(&dir);
    let why = outcome.expect("a node whose join was never taken must end, not come up");
    assert!(why.contains("mesh1.admin.1") && why.contains("unreached"), "the end names the launcher and the cause: {why}");
    assert_eq!(ready_spans, 0, "no via-ready span is emitted");
    assert!(!heard_ready, "ReadyForTraffic is never published");
}

/// CONTRACT: a launch that names no launcher is refused at start, the error naming the node and
/// the missing launcher, before the node binds anything. What must NOT happen: the node comes up
/// ready with no admin to admit it.
#[tokio::test]
async fn node_launched_with_no_launcher_is_refused_at_start_by_name() {
    let dir = std::env::temp_dir().join(format!("join-before-ready-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![],
        launcher: None,
        data_dir: dir.clone(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
    };
    let started = node::start(&launch, |b, _| b).await;
    let _ = std::fs::remove_dir_all(&dir);
    let why = started.err().expect("a launch with no launcher must be refused").to_string();
    assert!(why.contains("mesh1.rpc.1") && why.contains("no launcher"), "the refusal names the node and the missing launcher: {why}");
}
