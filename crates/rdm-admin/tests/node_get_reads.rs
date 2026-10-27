//! `nodes.get`'s reads (i144, Q203; Q167): answered from the node records the admin's atlas holds,
//! never the node store, each the rows matching every `(property, values)` pair. A birth's facts are
//! read on its node's record, by its id, or on the record its `predecessor` names, or on the record
//! held at its `path.name`; a birth the view no longer holds is named at a path the current Build
//! plans.

use rdm_admin::model::*;
use rdm_admin::storage::{BirthRef, DataDirLost, DataDirUnreadable, NodeRow};
use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::storage::MemoryNodesStorage;
use rdm_admin::topology::Topology;
use rdm_core::filter::Filter;
use rdm_core::status::StatusReply;
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
async fn answer(view: &Topology, status: &MemoryNodesStorage, planned: &rdm_admin::views::Planned, filter: Filter) -> Vec<rdm_core::views::NodeView> {
    match reply(view, status, planned, &filter).await {
        StatusReply::Nodes { nodes } => nodes,
        other => panic!("nodes.get {filter:?} answered {other:?}"),
    }
}

/// The reply of `nodes.get` with `filter` over `view`, its records as an atlas filled from `status`
/// holds them.
async fn reply(view: &Topology, status: &MemoryNodesStorage, planned: &rdm_admin::views::Planned, filter: &Filter) -> StatusReply {
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    let atlas = rdm_admin::atlas::Atlas::default();
    let meshes: Vec<String> = view.meshes.iter().map(|m| m.name.clone()).collect();
    atlas.fill(status, meshes).await.expect("the records are read");
    rdm_admin::views::nodes_answer_with_status(&path("mesh1.admin.1"), view, &loads, &atlas, planned, filter, &|_| {}).await
}

/// CONTRACT (i144, Q203: a node read answers from the atlas, never the node store;
/// storage-whole-records.md: "A birth recovered over a fresh data dir reads its predecessor's
/// `data-dir-lost` through the `predecessor` reference on its own record"): a birth recovered over a
/// fresh data dir at the path.name of a birth whose data dir was lost answers that loss on
/// `nodes.get`, read on the record its own record names as its predecessor; a birth the view no longer
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

    let nodes = answer(&view, status, &planned, Filter::new().eq("mesh", ["mesh1"])).await;
    let at = |name: &str| nodes.iter().find(|n| n.name == path(name)).unwrap_or_else(|| panic!("{name} answered: {nodes:#?}"));
    assert_eq!(nodes.len(), 4, "the three births the view holds and the held one: {nodes:#?}");
    let r2 = at("mesh1.rpc.2");
    assert_eq!((r2.node_id.clone(), r2.data_dir_lost.as_ref().map(|l| (l.path.clone(), l.since))), (recovered.node_id.clone(), Some((lost.path.clone(), lost.since))), "the recovered birth answers the loss it was recovered over");
    assert!(at("mesh1.rpc.1").data_dir_lost.is_none() && at("mesh1.rpc.1").data_dir_unreadable.is_none(), "a birth with neither fact answers neither");
    let r3 = at("mesh1.rpc.3");
    assert_eq!((r3.node_id.clone(), r3.status, r3.data_dir_unreadable.as_ref().map(|u| (u.path.clone(), u.cause.clone()))), (held_id.clone(), rdm_core::views::NodeStatus::Dead, Some((held.path.clone(), held.cause.clone()))), "the held birth is answered at its planned path");

    let by_id = answer(&view, status, &planned, Filter::new().eq("id", [held_id.to_string()])).await;
    assert_eq!(by_id.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>(), vec![held_id.clone()], "the held birth is answered by its id");

    let unread: rdm_admin::views::Planned = Err("the current Build could not be read: refused".into());
    let without = answer(&view, status, &unread, Filter::new().eq("mesh", ["mesh1"])).await;
    assert_eq!(without.len(), 3, "a Build that cannot be read names no path: the births the view holds are answered: {without:#?}");

    unreadable.birth_mut(&held_birth, &path("mesh1.rpc.3")).data_dir_unreadable = None;
    status.write_record(&unreadable).await.unwrap();
    let cleared = answer(&view, status, &planned, Filter::new().eq("mesh", ["mesh1"])).await;
    assert!(cleared.iter().all(|n| n.name != path("mesh1.rpc.3")), "a cleared unreadable fact is answered no more: {cleared:#?}");
}

/// CONTRACT (Q167 R-RD2, R-RD3; node-rpc-envelope.md nodes.get): over a fabric of two meshes holding
/// an admin, two gateways and a broker, their records carrying config and tags, `nodes.get` answers
/// the rows matching every pair, by `path.name`: `[config.zone: eu-1]` the two nodes whose config
/// holds that zone, never the one whose config lacks the key; `[tags: gold] [tags: ssd]` (all of) the
/// one carrying both; `[tags: gold, silver]` (any of) the three carrying either; `[mesh: mesh1]
/// [kind: gateway]` mesh1's gateway alone; `[mesh: mesh9]`, a mesh the admin does not hold, the empty
/// list; no pair, every node. A pair naming a property no node row has, a value its field cannot
/// take, or no value is `RejectedFilter` (`INVALID_ARGUMENT`) naming the pair, nothing read. What
/// must NOT happen: all-of read as any-of, an AND read as an OR, a missing config key matching, or an
/// unknown property answered as the empty list.
#[tokio::test]
async fn nodes_get_answers_the_rows_matching_every_pair_and_refuses_a_pair_its_schema_has_not() {
    let (admin, gw1, broker, gw2) = (node("mesh1.admin.1"), node("mesh1.gateway.1"), node("mesh1.broker.1"), node("mesh2.gateway.1"));
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![
            Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
            Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
        ],
        nodes: vec![admin.clone(), gw1.clone(), broker.clone(), gw2.clone()],
    };
    let status = MemoryNodesStorage::default();
    for (n, tags, config) in [
        (&admin, &[][..], &[][..]),
        (&gw1, &["gold", "ssd"][..], &[("zone", "eu-1")][..]),
        (&broker, &["silver"][..], &[("zone", "eu-1"), ("rack", "7")][..]),
        (&gw2, &["gold"][..], &[("rack", "7")][..]),
    ] {
        let mut row = NodeRow::new(n.name.clone(), n.node_id.clone(), NodeStatus::ReadyForTraffic);
        row.tags = tags.iter().map(|t| t.to_string()).collect();
        row.config = config.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        status.write_record(&row).await.unwrap();
    }
    let planned: rdm_admin::views::Planned = Ok(BTreeMap::new());
    let names = |nodes: Vec<rdm_core::views::NodeView>| nodes.into_iter().map(|n| n.name.to_string()).collect::<Vec<_>>();
    let get = |f: Filter| {
        let (view, status, planned) = (&view, &status, &planned);
        async move { names(answer(view, status, planned, f).await) }
    };

    assert_eq!(get(Filter::new()).await, ["mesh1.admin.1", "mesh1.broker.1", "mesh1.gateway.1", "mesh2.gateway.1"], "no pair: every node, by path.name");
    assert_eq!(get(Filter::new().eq("config.zone", ["eu-1"])).await, ["mesh1.broker.1", "mesh1.gateway.1"], "[config.zone: eu-1]");
    assert_eq!(get(Filter::new().eq("tags", ["gold"]).eq("tags", ["ssd"])).await, ["mesh1.gateway.1"], "[tags: gold] [tags: ssd]: all of");
    assert_eq!(get(Filter::new().eq("tags", ["gold", "silver"])).await, ["mesh1.broker.1", "mesh1.gateway.1", "mesh2.gateway.1"], "[tags: gold, silver]: any of");
    assert_eq!(get(Filter::new().eq("mesh", ["mesh1"]).eq("kind", ["gateway"])).await, ["mesh1.gateway.1"], "[mesh: mesh1] [kind: gateway]");
    assert_eq!(get(Filter::new().eq("kind", ["gateway"]).eq("config.rack", ["7"])).await, ["mesh2.gateway.1"], "[kind: gateway] [config.rack: 7]");
    assert!(get(Filter::new().eq("mesh", ["mesh9"])).await.is_empty(), "[mesh: mesh9]: a mesh the admin does not hold is the empty list");
    assert!(get(Filter::new().eq("id", [NodeId::mint().to_string()])).await.is_empty(), "[id: an id not held] is the empty list");

    for (filter, pair) in [
        (Filter::new().eq("mesh", ["mesh1"]).eq("zone", ["eu-1"]), "the pair (zone, [eu-1]) names no property of a node row"),
        (Filter::new().eq("kind", ["gatway"]), "the pair (kind, [gatway])"),
        (Filter::new().eq("is_primary", ["yes"]), "the pair (is_primary, [yes])"),
        (Filter::new().eq("tags", Vec::<String>::new()), "the pair (tags, []) names no value"),
    ] {
        match reply(&view, &status, &planned, &filter).await {
            r @ StatusReply::RejectedFilter { .. } => {
                let StatusReply::RejectedFilter { reason } = &r else { unreachable!() };
                assert!(reason.contains(pair), "{reason:?} names {pair:?}");
                assert_eq!(<rdm_core::status::Status as rdm_core::protocol::NodeProtocol>::classify_reply(&r), rdm_core::outcome::ReplyKind::new(rdm_core::outcome::Code::InvalidArgument));
            }
            other => panic!("{filter:?} is refused naming the pair, not {other:?}"),
        }
    }
}
