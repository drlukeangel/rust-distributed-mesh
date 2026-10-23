//! `node.get`'s reads (i144): every read is a keyed point read and none lists a store. A birth's
//! status-row facts are read by its exact key or through their `path.name` index; a birth the view
//! no longer holds is named at a path the current Build plans.

use rdm_admin::model::*;
use rdm_admin::storage::{NodeDataDirLostRow, NodeDataDirUnreadableRow, NodesStorage};
use rdm_admin::storage::MemoryNodesStorage;
use rdm_admin::topology::Topology;
use rdm_core::status::{NodeSelect, StatusReply};
use std::collections::BTreeMap;
use std::sync::Arc;

fn node(name: &str) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn path(name: &str) -> PathName {
    name.parse().unwrap()
}

async fn answer(view: &Topology, storage: &MemoryNodesStorage, status: &MemoryNodesStorage, planned: &rdm_admin::views::Planned, select: NodeSelect) -> Vec<rdm_core::views::NodeView> {
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    match rdm_admin::views::nodes_answer_with_status(&path("mesh1.admin.1"), view, &loads, storage, Some(status), planned, &select).await {
        StatusReply::Nodes { nodes } => nodes,
        other => panic!("node.get answered {other:?}"),
    }
}

/// CONTRACT (i144; a read is a point read through an index, never a scan): a birth recovered over a
/// fresh data dir at the path.name of a birth whose data dir was lost answers that loss on
/// `node.get`, read from the data-dir-lost rows' `path.name` index; a birth the view no longer holds
/// (deleted by its recovery, held with its data dir unreadable) is answered, by mesh and by its id,
/// at the path the current Build plans, read from the data-dir-unreadable rows' `path.name` index;
/// a birth with neither fact answers neither. A Build that cannot be read names no path: the births
/// the view holds are still answered. A removed unreadable fact is gone from its index too. Must NOT
/// happen: the loss answered only on the exited birth, the held birth missing while the Build plans
/// its path, a store listed (the status store has no list on this path to call).
#[tokio::test]
async fn node_get_answers_data_dir_facts_by_point_reads_at_their_path_name() {
    let (admin, plain, recovered) = (node("mesh1.admin.1"), node("mesh1.rpc.1"), node("mesh1.rpc.2"));
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![admin.clone(), plain.clone(), recovered.clone()],
    };
    let storage = MemoryNodesStorage::default();
    let status = &storage;
    // mesh1.rpc.2's predecessor lost its data dir; mesh1.rpc.3 was deleted by its recovery and is held
    // with its data dir unreadable.
    let lost = NodeDataDirLostRow { node_id: NodeId::mint(), name: path("mesh1.rpc.2"), incarnation_id: IncarnationId::mint(), path: "/data/mesh1.rpc.2".into(), since: 1_700 };
    status.put_node_data_dir_lost("mesh1", &lost).await.unwrap();
    let held = NodeDataDirUnreadableRow { node_id: NodeId::mint(), name: path("mesh1.rpc.3"), incarnation_id: IncarnationId::mint(), path: "/data/mesh1.rpc.3".into(), cause: "node-key: not hex".into() };
    status.put_node_data_dir_unreadable("mesh1", &held).await.unwrap();
    let planned: rdm_admin::views::Planned = Ok(BTreeMap::from([("mesh1".to_string(), ["mesh1.admin.1", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"].map(path).to_vec())]));

    let nodes = answer(&view, &storage, &status, &planned, NodeSelect::Mesh("mesh1".into())).await;
    let at = |name: &str| nodes.iter().find(|n| n.name == path(name)).unwrap_or_else(|| panic!("{name} answered: {nodes:#?}"));
    assert_eq!(nodes.len(), 4, "the three births the view holds and the held one: {nodes:#?}");
    let r2 = at("mesh1.rpc.2");
    assert_eq!((r2.node_id.clone(), r2.data_dir_lost.as_ref().map(|l| (l.path.clone(), l.since))), (recovered.node_id.clone(), Some((lost.path.clone(), lost.since))), "the recovered birth answers the loss it was recovered over");
    assert!(at("mesh1.rpc.1").data_dir_lost.is_none() && at("mesh1.rpc.1").data_dir_unreadable.is_none(), "a birth with neither fact answers neither");
    let r3 = at("mesh1.rpc.3");
    assert_eq!((r3.node_id.clone(), r3.status, r3.data_dir_unreadable.as_ref().map(|u| (u.path.clone(), u.cause.clone()))), (held.node_id.clone(), rdm_core::views::NodeStatus::Dead, Some((held.path.clone(), held.cause.clone()))), "the held birth is answered at its planned path");

    let by_id = answer(&view, &storage, &status, &planned, NodeSelect::Id(held.node_id.clone())).await;
    assert_eq!(by_id.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>(), vec![held.node_id.clone()], "the held birth is answered by its id");

    let unread: rdm_admin::views::Planned = Err("the current Build could not be read: refused".into());
    let without = answer(&view, &storage, &status, &unread, NodeSelect::Mesh("mesh1".into())).await;
    assert_eq!(without.len(), 3, "a Build that cannot be read names no path: the births the view holds are answered: {without:#?}");

    status.remove_node_data_dir_unreadable("mesh1", &held.node_id, &held.incarnation_id, &held.name).await.unwrap();
    let cleared = answer(&view, &storage, &status, &planned, NodeSelect::Mesh("mesh1".into())).await;
    assert!(cleared.iter().all(|n| n.name != path("mesh1.rpc.3")), "a removed unreadable fact is gone from its path.name index: {cleared:#?}");
}
