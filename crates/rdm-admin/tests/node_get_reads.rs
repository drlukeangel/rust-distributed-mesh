//! `node.get`'s reads (i144, Q203): answered from the node records the admin's atlas holds,
//! never the node store. A birth's facts are read on its node's record, by its id, or on the record
//! its `predecessor` names, or on the record held at its `path.name`; a birth the view no longer holds
//! is named at a path the current Build plans.

use rdm_admin::model::*;
use rdm_admin::storage::{BirthRef, DataDirLost, DataDirUnreadable, NodeRow};
use rdm_admin::node_rows::WriteRecord as _;
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

/// The node read over `view`, its records as an atlas filled from `status` holds them (Q204: read once).
async fn answer(view: &Topology, storage: &MemoryNodesStorage, status: &MemoryNodesStorage, planned: &rdm_admin::views::Planned, select: NodeSelect) -> Vec<rdm_core::views::NodeView> {
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    let atlas = rdm_admin::atlas::Atlas::default();
    atlas.fill(status, ["mesh1".to_string()]).await.expect("the records are read");
    match rdm_admin::views::nodes_answer_with_status(&path("mesh1.admin.1"), view, &loads, storage, &atlas, planned, &select).await {
        StatusReply::Nodes { nodes } => nodes,
        other => panic!("node.get answered {other:?}"),
    }
}

/// CONTRACT (i144, Q203: a node read answers from the atlas, never the node store;
/// storage-whole-records.md: "A birth recovered over a fresh data dir reads its predecessor's
/// `data-dir-lost` through the `predecessor` reference on its own record"): a birth recovered over a
/// fresh data dir at the path.name of a birth whose data dir was lost answers that loss on
/// `node.get`, read on the record its own record names as its predecessor; a birth the view no longer
/// holds (deleted by its recovery, held with its data dir unreadable) is answered, by mesh and by its
/// id, at the path the current Build plans, read on the record held at that `path.name`; a
/// birth with neither fact answers neither. A Build that cannot be read names no path: the births the
/// view holds are still answered. A cleared unreadable fact is answered no more. Must NOT happen: the
/// loss answered only on the exited birth, or the held birth missing while the Build plans its path.
#[tokio::test]
async fn node_get_answers_data_dir_facts_from_the_atlas_at_their_path_name() {
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
    let lost = DataDirLost { path: "/data/mesh1.rpc.2".into(), since: 1_700 };
    let (exited_id, exited_birth) = (NodeId::mint(), IncarnationId::mint());
    let mut exited = NodeRow::new(path("mesh1.rpc.2"), exited_id.clone(), NodeStatus::Deleted);
    exited.birth_mut(&exited_birth, &path("mesh1.rpc.2")).data_dir_lost = Some(lost.clone());
    status.write_record(&exited).await.unwrap();
    let mut successor = NodeRow::new(path("mesh1.rpc.2"), recovered.node_id.clone(), NodeStatus::ReadyForTraffic);
    successor.predecessor = Some(BirthRef { node_id: exited_id, incarnation_id: exited_birth });
    status.write_record(&successor).await.unwrap();
    let held = DataDirUnreadable { path: "/data/mesh1.rpc.3".into(), cause: "node-key: not hex".into() };
    let (held_id, held_birth) = (NodeId::mint(), IncarnationId::mint());
    let mut unreadable = NodeRow::new(path("mesh1.rpc.3"), held_id.clone(), NodeStatus::Deleted);
    unreadable.birth_mut(&held_birth, &path("mesh1.rpc.3")).data_dir_unreadable = Some(held.clone());
    status.write_record(&unreadable).await.unwrap();
    let planned: rdm_admin::views::Planned = Ok(BTreeMap::from([("mesh1".to_string(), ["mesh1.admin.1", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"].map(path).to_vec())]));

    let nodes = answer(&view, &storage, &status, &planned, NodeSelect::Mesh("mesh1".into())).await;
    let at = |name: &str| nodes.iter().find(|n| n.name == path(name)).unwrap_or_else(|| panic!("{name} answered: {nodes:#?}"));
    assert_eq!(nodes.len(), 4, "the three births the view holds and the held one: {nodes:#?}");
    let r2 = at("mesh1.rpc.2");
    assert_eq!((r2.node_id.clone(), r2.data_dir_lost.as_ref().map(|l| (l.path.clone(), l.since))), (recovered.node_id.clone(), Some((lost.path.clone(), lost.since))), "the recovered birth answers the loss it was recovered over");
    assert!(at("mesh1.rpc.1").data_dir_lost.is_none() && at("mesh1.rpc.1").data_dir_unreadable.is_none(), "a birth with neither fact answers neither");
    let r3 = at("mesh1.rpc.3");
    assert_eq!((r3.node_id.clone(), r3.status, r3.data_dir_unreadable.as_ref().map(|u| (u.path.clone(), u.cause.clone()))), (held_id.clone(), rdm_core::views::NodeStatus::Dead, Some((held.path.clone(), held.cause.clone()))), "the held birth is answered at its planned path");

    let by_id = answer(&view, &storage, &status, &planned, NodeSelect::Id(held_id.clone())).await;
    assert_eq!(by_id.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>(), vec![held_id.clone()], "the held birth is answered by its id");

    let unread: rdm_admin::views::Planned = Err("the current Build could not be read: refused".into());
    let without = answer(&view, &storage, &status, &unread, NodeSelect::Mesh("mesh1".into())).await;
    assert_eq!(without.len(), 3, "a Build that cannot be read names no path: the births the view holds are answered: {without:#?}");

    unreadable.birth_mut(&held_birth, &path("mesh1.rpc.3")).data_dir_unreadable = None;
    status.write_record(&unreadable).await.unwrap();
    let cleared = answer(&view, &storage, &status, &planned, NodeSelect::Mesh("mesh1".into())).await;
    assert!(cleared.iter().all(|n| n.name != path("mesh1.rpc.3")), "a cleared unreadable fact is answered no more: {cleared:#?}");
}
