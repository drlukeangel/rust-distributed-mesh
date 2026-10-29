//! `node.get`'s reads (i144, Q203; Q167): answered from the node records the admin's atlas holds,
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
    n.status = NodeStatus::Ready;
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
        other => panic!("node.get {filter:?} answered {other:?}"),
    }
}

/// The reply of `node.get` with `filter` over `view`, its records as an atlas filled from `status`
/// holds them.
async fn reply(view: &Topology, status: &MemoryNodesStorage, planned: &rdm_admin::views::Planned, filter: &Filter) -> StatusReply {
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    let atlas = rdm_admin::atlas::Atlas::default();
    let meshes: Vec<String> = view.meshes.iter().map(|m| m.name.clone()).collect();
    atlas.fill(status, meshes).await.expect("the records are read");
    rdm_admin::views::nodes_answer_with_status(&path("mesh1.admin.1"), view, &loads, &atlas, planned, filter, &|_| {}).await
}

/// CONTRACT (`docs/architecture/lifecycles/node-recovery.md:152` — "It writes the birth's durable
/// `data-dir-lost` fact to the mesh's `nodes.storage` at `<mesh>/<node.id>/<incarnation.id>/data-dir-lost`,
/// then deletes the unreached birth (`node.delete` with `skip_stop`)" and
/// `docs/architecture/lifecycles/node-recovery.md:161` — "It writes the birth's durable
/// `data-dir-unreadable` fact to `<mesh>/<node.id>/<incarnation.id>/data-dir-unreadable` (the path and
/// the failure cause), deletes the unreached birth (`node.delete` with `skip_stop`), and **does not**
/// run `node.create`."; `docs/architecture/lifecycles/node-recovery.md:107` — "On recovery (`node.delete`
/// + `node.create` at the same `path.name`), the `Deleted` row written by `node.delete` retains the
/// exited node's `config` and `tags`; the subsequent `node.create`'s name claim reads that `Deleted`
/// row and copies its `config` and `tags` into the new node's row before writing.";
/// storage-whole-records.md: "A birth recovered over a fresh data dir reads its predecessor's
/// `data-dir-lost` through the `predecessor` reference on its own record"): a birth recovered over a
/// fresh data dir at the path.name of a birth whose data dir was lost answers that loss on
/// `node.get`, read on the record its own record names as its predecessor; a birth the view no longer
/// holds (deleted by its recovery, held with its data dir unreadable) is answered, by mesh and by its
/// id, at the path the current Build plans, read on the record held at that `path.name`; a
/// birth with neither fact answers neither. A Build that cannot be read names no path: the births the
/// view holds are still answered. A cleared unreadable fact is answered no more. Must NOT happen: the
/// loss answered only on the exited birth, or the held birth missing while the Build plans its path.
// @feature: data-dir-lost-recovery
#[tokio::test]
async fn node_get_answers_data_dir_facts_from_the_atlas_at_their_path_name() {
    let mut admin = node("mesh1.admin.1");
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let (plain, recovered) = (node("mesh1.rpc.1"), node("mesh1.rpc.2"));
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), plain.clone(), recovered.clone()],
    };
    let storage = Arc::new(MemoryNodesStorage::default());
    let status: &MemoryNodesStorage = &storage;
    let rows = rdm_admin::node_rows::NodeRows::new(
        admin.name.clone(),
        admin.node_id.clone(),
        Arc::new(tokio::sync::RwLock::new(view.clone())),
        storage.clone(),
    );
    // mesh1.rpc.2's predecessor lost its data dir, was deleted by recovery, and recovery's claim over
    // the Deleted row links predecessor and inherits config and tags; mesh1.rpc.3 was deleted by its
    // recovery and is held with its data dir unreadable.
    let lost = DataDirLost { path: "/data/mesh1.rpc.2".into(), since: 1_700 };
    let (exited_id, exited_birth) = (NodeId::mint(), IncarnationId::mint());
    let mut exited_node = Node::allocated(path("mesh1.rpc.2"));
    exited_node.node_id = exited_id.clone();
    exited_node.incarnation_id = Some(exited_birth.clone());
    let mut exited = NodeRow::new(path("mesh1.rpc.2"), exited_id.clone(), NodeStatus::Ready);
    exited.config = BTreeMap::from([("region".into(), "eu-west".into())]);
    exited.tags = std::collections::BTreeSet::from(["tier-a".to_string()]);
    exited.birth_mut(&exited_birth, &path("mesh1.rpc.2")).data_dir_lost = Some(lost.clone());
    status.write_record(&exited).await.unwrap();
    rows.deleted_unreached(&exited_node, "node.delete#recovery").await.unwrap();
    let claimed = rows.claim(&path("mesh1.rpc.2"), &recovered.node_id, "node.create#recovery", None, None, rdm_admin::node_rows::ClaimFor::Recovery).await.unwrap();
    match claimed {
        rdm_admin::node_rows::Claimed::Written { row, seeded_from } => {
            assert_eq!(seeded_from, Some(exited_id.clone()));
            assert_eq!(row.predecessor, Some(BirthRef { node_id: exited_id.clone(), incarnation_id: exited_birth.clone() }));
            assert_eq!(row.config.get("region").map(String::as_str), Some("eu-west"));
            assert!(row.tags.contains("tier-a"));
        }
        other => panic!("expected Written claim for recovery, got {other:?}"),
    }
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
    assert_eq!(r2.config.get("region").map(String::as_str), Some("eu-west"), "the recovered birth inherits its predecessor's config");
    assert!(r2.tags.iter().any(|t| t == "tier-a"), "the recovered birth inherits its predecessor's tags");
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

/// CONTRACT (Q167 R-RD2, R-RD3; node-rpc-envelope.md node.get): over a fabric of two meshes holding
/// an admin, two gateways and a broker, their records carrying config and tags, `node.get` answers
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
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![
            Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
            Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
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
        let mut row = NodeRow::new(n.name.clone(), n.node_id.clone(), NodeStatus::Ready);
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

/// CONTRACT (lifecycles/node-replace.md "Inherited": "The successor's tag-index entries use its own id.
/// Retained predecessor labels are removed from the active index, not erased from its row"; step 3: the
/// retained `<name>.old` row "whose labels leave the active tags index"; rollback step 3: the
/// predecessor's "tag-index entries restored under its" id; query.md §1: a set field (`tags`) matches
/// when it contains the value): the successor at `mesh1.rpc.1` and the retained predecessor at
/// `mesh1.rpc.1.old`, marked `inherited_by` the successor, both carry `rabbit` on their rows.
/// `node.get [tags: rabbit]` names the successor alone, while `node.get [mesh: mesh1]` answers the
/// retained row with its `rabbit` kept. Once the mark is cleared (the predecessor takes its name back at a
/// rollback), `[tags: rabbit]` names it again. Must NOT happen: the retained predecessor named under its
/// label, or its row's labels erased from what `node.get` answers.
#[tokio::test]
async fn a_retained_predecessors_labels_leave_the_tags_match_and_stay_on_its_row() {
    let (admin, successor, predecessor) = (node("mesh1.admin.1"), node("mesh1.rpc.1"), node("mesh1.rpc.1.old"));
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), successor.clone(), predecessor.clone()],
    };
    let rabbit: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    let write = |inherited: bool| {
        let (successor, predecessor, admin, rabbit) = (&successor, &predecessor, &admin, &rabbit);
        async move {
            let status = MemoryNodesStorage::default();
            status.write_record(&NodeRow::new(admin.name.clone(), admin.node_id.clone(), NodeStatus::Ready)).await.unwrap();
            let mut s = NodeRow::new(successor.name.clone(), successor.node_id.clone(), NodeStatus::Ready);
            s.tags = rabbit.clone();
            status.write_record(&s).await.unwrap();
            let mut p = NodeRow::new(predecessor.name.clone(), predecessor.node_id.clone(), NodeStatus::Ready);
            p.tags = rabbit.clone();
            p.inherited_by = inherited.then(|| successor.node_id.clone());
            status.write_record(&p).await.unwrap();
            status
        }
    };
    let planned: rdm_admin::views::Planned = Ok(BTreeMap::new());
    let ids = |nodes: Vec<rdm_core::views::NodeView>| nodes.into_iter().map(|n| n.node_id).collect::<Vec<_>>();

    let marked = write(true).await;
    assert_eq!(ids(answer(&view, &marked, &planned, Filter::new().eq("tags", ["rabbit"])).await), [successor.node_id.clone()], "[tags: rabbit] names the successor, not the retained predecessor");
    let listed = answer(&view, &marked, &planned, Filter::new().eq("mesh", ["mesh1"])).await;
    let retained = listed.iter().find(|n| n.node_id == predecessor.node_id).unwrap_or_else(|| panic!("the retained predecessor is answered at mesh1.rpc.1.old: {listed:#?}"));
    assert_eq!(retained.tags, rabbit, "the retained row keeps its labels: {retained:?}");

    let restored = write(false).await;
    let mut both = ids(answer(&view, &restored, &planned, Filter::new().eq("tags", ["rabbit"])).await);
    both.sort();
    let mut want = vec![successor.node_id.clone(), predecessor.node_id.clone()];
    want.sort();
    assert_eq!(both, want, "with no inherited_by mark the predecessor's labels are in the match again");
}

/// A nodes store that fails every call, read or write, and counts them: a read answered while it is
/// the admin's store read nothing of it.
#[derive(Default)]
struct Unreadable {
    calls: std::sync::atomic::AtomicUsize,
}

impl Unreadable {
    fn fail<T>(&self, what: &str) -> Result<T, rdm_admin::storage_error::StorageError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/{what}"), reason: "the nodes store fails every read".into() })
    }
}

#[async_trait::async_trait]
impl rdm_admin::storage::NodesStorage for Unreadable {
    async fn put_runtime(&self, _: &rdm_admin::storage::RuntimeRow) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fail("runtimes")
    }
    async fn runtimes(&self) -> Result<Vec<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.fail("runtimes")
    }
    async fn runtime(&self, _: &NodeId, _: &IncarnationId) -> Result<Option<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.fail("runtimes")
    }
    async fn node(&self, _: &NodeId) -> Result<Option<NodeRow>, rdm_admin::storage_error::StorageError> {
        self.fail("records")
    }
    async fn node_at(&self, _: &PathName) -> Result<Option<NodeRow>, rdm_admin::storage_error::StorageError> {
        self.fail("records")
    }
    async fn nodes(&self, _: &str) -> Result<Vec<NodeRow>, rdm_admin::storage_error::StorageError> {
        self.fail("records")
    }
    async fn put_node(&self, _: &NodeRow) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fail("records")
    }
    async fn index_path(&self, _: &PathName, _: &NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fail("paths")
    }
    async fn index_tags(&self, _: &str, _: &NodeId, _: &std::collections::BTreeSet<String>, _: &std::collections::BTreeSet<String>) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fail("tags")
    }
    async fn tagged(&self, _: &str) -> Result<std::collections::BTreeSet<NodeId>, rdm_admin::storage_error::StorageError> {
        self.fail("tags")
    }
    async fn name_claim(&self, _: &PathName) -> Result<Option<NodeId>, rdm_admin::storage_error::StorageError> {
        self.fail("names")
    }
    async fn claim_node_name(&self, _: &PathName, _: &NodeId) -> Result<Result<(), rdm_admin::storage::NameHeld>, rdm_admin::storage_error::StorageError> {
        self.fail("names")
    }
    async fn release_node_name(&self, _: &PathName, _: &NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fail("names")
    }
}

/// The fabric-primary `me`'s Status door over `view`, its atlas `atlas` and its nodes store
/// `nodes_storage`; every other store in memory.
fn fabric_primary_at(me: &Node, view: Topology, atlas: Arc<rdm_admin::atlas::Atlas>, nodes_storage: Arc<dyn rdm_admin::storage::NodesStorage>) -> rdm_admin::status_rpc::StatusDoor {
    let slot = Arc::new(std::sync::OnceLock::new());
    let _ = slot.set(atlas);
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    rdm_admin::status_rpc::StatusDoor {
        certs: None,
        app_channels: None,
        declaration_seam: None,
        command_frame_seam: None,
        operation_effects: Default::default(),
        stopped: Default::default(),
        me: me.name.clone(),
        node_id: me.node_id.clone(),
        fabric_id: view.fabric.id.clone(),
        topology: Arc::new(tokio::sync::RwLock::new(view)),
        declared: Arc::new(std::sync::Mutex::new(rdm_admin::status_rpc::Declared::default())),
        meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
        fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
        nodes_storage,
        caches: Default::default(),
        mesh_ids: Arc::new(BTreeMap::new),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        mesh_deleter: Arc::new(std::sync::OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(),
        atlas: slot,
        fabric_recovery: Default::default(),
        configs: Default::default(),
        republish: Default::default(),
        configure: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(),
        loads: Some(loads),
        digest_view: None,
        builds: Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    }
}

fn get(entity: rdm_core::filter::Entity, filter: Filter) -> rdm_core::status::StatusRequest {
    rdm_core::status::StatusRequest::Get { entity, node: None, filter }
}

/// CONTRACT (query.md §1; node-rpc-envelope.md node.get: "answered by the fabric-primary from its
/// atlas, never from the node store"; fabric-certs.md §Tracking): at a fabric-primary whose atlas was
/// filled from its node records, with its nodes store failing every call from then on, `node.get`
/// answers every node matching the filter, config and tags and cert included, from the atlas, and
/// `fabric.get` answers the fabric with its `next_expiry`, the soonest member cert expiry the atlas's
/// records hold; neither makes one call to the nodes store. Must NOT happen: a read refused or answered
/// short because the store failed, the cert summary read from the store (and silently dropped when the
/// store fails), or any call to the store at all.
#[tokio::test]
async fn nodes_get_and_fabrics_get_answer_from_the_atlas_while_the_nodes_store_fails_every_read() {
    let (mut admin, gw) = (node("mesh1.admin.1"), node("mesh1.gateway.1"));
    admin.kind = rdm_core::NodeKind::NodeAdmin;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), gw.clone()],
    };
    let records = MemoryNodesStorage::default();
    let expiries = [(&admin, 9_000_000u64), (&gw, 7_000_000u64)];
    for (n, expires_at) in expiries {
        let mut row = NodeRow::new(n.name.clone(), n.node_id.clone(), NodeStatus::Ready);
        row.config = BTreeMap::from([("zone".to_string(), "eu-1".to_string())]);
        row.tags = ["gold".to_string()].into();
        row.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.name).cert = Some(rdm_admin::storage::CertDates { issued_at: 1_000_000, expires_at, issuer_expires_at: 20_000_000, renew_at: expires_at - 1_000 });
        records.write_record(&row).await.unwrap();
    }
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    atlas.scope(&records, ["mesh1".to_string()].into()).await.expect("the fabric-primary takes mesh1: its atlas is filled from the node records, once");
    let store = Arc::new(Unreadable::default());
    let fp = fabric_primary_at(&admin, view, atlas, store.clone());

    match fp.apply(None, &get(rdm_core::filter::Entity::Nodes, Filter::new().eq("config.zone", ["eu-1"]).eq("tags", ["gold"]))).await {
        StatusReply::Nodes { nodes } => {
            assert_eq!(nodes.iter().map(|n| n.name.to_string()).collect::<Vec<_>>(), ["mesh1.admin.1", "mesh1.gateway.1"], "{nodes:#?}");
            assert_eq!(nodes[1].cert.as_ref().map(|c| c.expires_at), Some(7_000_000), "the cert is the atlas record's");
        }
        other => panic!("node.get answers from the atlas while the store fails every read, not {other:?}"),
    }
    match fp.apply(None, &get(rdm_core::filter::Entity::Fabrics, Filter::new())).await {
        StatusReply::Fabrics { fabrics } => {
            assert_eq!(fabrics.len(), 1, "{fabrics:#?}");
            assert_eq!(fabrics[0].next_expiry, Some(7_000_000), "the soonest cert expiry the atlas's records hold: {fabrics:#?}");
        }
        other => panic!("fabric.get answers from the atlas while the nodes store fails every read, not {other:?}"),
    }
    assert_eq!(store.calls.load(std::sync::atomic::Ordering::SeqCst), 0, "neither read called the nodes store");
}

/// CONTRACT (Q233, states.md "Who sets each state": "A query answers the state the row last took from
/// the node's frame or the mesh-primary's receipt; a command the node has not acknowledged moves the
/// row nowhere"): a stop the mesh-primary admitted and the node has not acknowledged leaves its row
/// ready, and `node.get` answers ready, not stopped, though the admitting
/// mesh-primary's view already holds the birth leaving; the node's acknowledgement, written on the row
/// as stopping, answers stopping; the stopped frame, written as stopped, answers stopped. Silence is the
/// observer's: a birth the view holds pending-reconnect reads so, and a silence mark the row still
/// carries is not answered once the birth is heard. Must NOT happen: the gossip view's lifecycle
/// status, or a lifecycle effect laid over it, answered in place of the row's.
#[tokio::test]
async fn an_unacknowledged_stop_reads_ready_and_the_row_answers_each_state_it_takes() {
    let (admin, mut rpc, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"), node("mesh2.rpc.1"));
    // The stop's admission at the mesh-primary set the birth leaving in its view, with no frame of the node.
    rpc.status = NodeStatus::Leaving;
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), rpc.clone(), peer.clone()],
    };
    let records = MemoryNodesStorage::default();
    let mut row = NodeRow::new(rpc.name.clone(), rpc.node_id.clone(), NodeStatus::Ready);
    row.birth_mut(rpc.incarnation_id.as_ref().unwrap(), &rpc.name).declared.insert(rdm_core::status::NodeState::Ready);
    records.write_record(&row).await.unwrap();
    // mesh2's row as the fabric-primary's fill read it, before mesh2's primary moved it on.
    let mut peer_row = NodeRow::new(peer.name.clone(), peer.node_id.clone(), NodeStatus::Pending);
    peer_row.birth_mut(peer.incarnation_id.as_ref().unwrap(), &peer.name);
    records.write_record(&peer_row).await.unwrap();
    // The fabric-primary, mesh1's primary too: every mesh taken, mesh1's rows its writer's.
    let atlas = rdm_admin::atlas::Atlas::default();
    atlas.scope(&records, ["mesh1".to_string(), "mesh2".to_string()].into()).await.unwrap();
    atlas.writes("mesh1");
    use rdm_core::views::NodeStatus as Read;
    assert_eq!(status_of(&view, &atlas, &rpc.node_id).await, (Read::Ready, false), "a stop the node has not acknowledged moves its row nowhere: ready");
    let ready: Vec<String> = match rdm_admin::views::nodes_answer(&path("mesh1.admin.1"), &view, &(Arc::new(BTreeMap::new) as rdm_admin::views::Loads), &atlas, &Filter::new().eq("status", ["ready"])).await {
        StatusReply::Nodes { nodes } => nodes.into_iter().map(|n| n.name.to_string()).collect(),
        other => panic!("{other:?}"),
    };
    assert_eq!(ready, ["mesh1.admin.1", "mesh1.rpc.1", "mesh2.rpc.1"], "[status: ready] matches the birth whose stop is unacknowledged");
    // Another mesh's create-time progress is held only as a fill read it: the view's answers until that
    // mesh's primary feeds this atlas; an operation's state, which reaches this atlas with the operation
    // (the stopped its receipt sets), answers itself.
    assert_eq!(status_of(&view, &atlas, &peer.node_id).await, (Read::Ready, false), "mesh2's birth: the view's ready, not the pending its row held at the fill");
    peer_row.status = NodeStatus::Stopped;
    atlas.write_record(&peer_row);
    assert_eq!(status_of(&view, &atlas, &peer.node_id).await, (Read::Stopped, true), "mesh2's birth: the stopped this atlas's receipt set");
    peer_row.status = NodeStatus::ConnectionsDeleted;
    atlas.write_record(&peer_row);
    assert_eq!(status_of(&view, &atlas, &peer.node_id).await, (Read::ConnectionsDeleted, false), "mesh2's birth: the connections-deleted its attempt's fill read");

    row.status = NodeStatus::Stopping;
    atlas.write_record(&row);
    assert_eq!(status_of(&view, &atlas, &rpc.node_id).await, (Read::Stopping, false), "the node's acknowledgement written on the row: stopping");

    row.status = NodeStatus::Stopped;
    atlas.write_record(&row);
    assert_eq!(status_of(&view, &atlas, &rpc.node_id).await, (Read::Stopped, true), "the stopped frame written on the row: stopped");

    // Silence is the observer's (node-identity-and-state.md §7.3), not a state a command sets: a birth
    // the view holds pending-reconnect reads it whatever its row says, and a silence mark its row still
    // carries is not the answer once the view hears the birth.
    row.status = NodeStatus::Ready;
    row.birth_mut(rpc.incarnation_id.as_ref().unwrap(), &rpc.name).silence = Some(NodeStatus::PendingReconnect);
    atlas.write_record(&row);
    let mut silent = view.clone();
    silent.nodes.iter_mut().filter(|n| n.node_id == rpc.node_id).for_each(|n| n.status = NodeStatus::PendingReconnect);
    assert_eq!(status_of(&silent, &atlas, &rpc.node_id).await, (Read::PendingReconnect, false), "the view holds the birth silent: pending-reconnect");
    let mut heard = view.clone();
    heard.nodes.iter_mut().filter(|n| n.node_id == rpc.node_id).for_each(|n| n.status = NodeStatus::Ready);
    assert_eq!(status_of(&heard, &atlas, &rpc.node_id).await, (Read::Ready, false), "heard again: the row's ready, not the silence mark the row still carries");
}

/// CONTRACT (query.md §2, Q232: "the same pair list answers the same nodes from both sources on a
/// settled fabric"): a gateway of mesh1 holding the digests of mesh1's admin (its primary), two rpc
/// nodes (one ready, one pending) and a broker of mesh2, and the fabric-primary's atlas
/// holding the same births, answer the same nodes for the same gossiped pairs: `[kind: rpc] [status:
/// ready]`, `[mesh: mesh1]`, `[is_primary: true]`, `[mesh: mesh2] [kind: broker]` and no
/// pair; the local query makes no call. A row-only pair (`[tags: gold]`, `[config.zone: eu-1]`) is
/// answered by `node.get` and refused locally naming the pair and `node.get`. Must NOT happen: the
/// two sources disagreeing on a gossiped pair, or a row-only pair answered locally as no match.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_pairs_answer_the_same_nodes_from_the_atlas_and_from_the_topology_a_node_holds() {
    use rdm_core::{MemberStatus, MeshDigest, MeshNode};
    let fabric = FabricId::mint();
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let gateway = rdm_transport::topology::TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.gateway.1", Arc::new(time.clone()), vec![], &[]).await.unwrap();

    // (name, ready, primary of its node type)
    let births = [("mesh1.admin.1", true, true), ("mesh1.rpc.1", true, false), ("mesh1.rpc.2", false, false), ("mesh2.broker.1", true, false)];
    let mut nodes = Vec::new();
    let records = MemoryNodesStorage::default();
    for (i, (name, ready, primary)) in births.into_iter().enumerate() {
        let mut n = node(name);
        n.mesh = path(name).mesh.clone();
        n.kind = path(name).kind;
        n.status = if ready { NodeStatus::Ready } else { NodeStatus::Pending };
        n.is_primary = primary;
        n.endpoint_id = Some(EndpointId(format!("key-{name}")));
        n.transport_addr = Some(format!("127.0.0.1:{}", 21_000 + i).parse().unwrap());
        let mut row = NodeRow::new(n.name.clone(), n.node_id.clone(), n.status);
        row.tags = ["gold".to_string()].into();
        row.config = BTreeMap::from([("zone".to_string(), "eu-1".to_string())]);
        records.write_record(&row).await.unwrap();
        let d = MeshDigest {
            held_hash: Default::default(),
            fabric_id: fabric.clone(),
            node: MeshNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: n.endpoint_id.clone().unwrap(), transport_addr: n.transport_addr.unwrap(), incarnation: n.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: if ready { MemberStatus::Ready } else { MemberStatus::Pending },
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 10,
            emitted_at_rafka_ms: 10,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: primary.then_some(1),
            is_fabric_primary: None,
        };
        assert!(gateway.digest_view.record(d.clone()), "{name}'s heartbeat is held");
        if ready && n.mesh == "mesh1" {
            assert!(gateway.digest_view.started(&d.node.node_id, &d.node.incarnation, &d.node.name), "{name}'s node.started, as its mesh-primary's projection carries it");
        }
        nodes.push(n);
    }
    let view = Topology {
        fabric: Fabric { id: fabric.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
        nodes,
    };
    let local = |f: &Filter| gateway.nodes_get(f).map(|ds| ds.into_iter().map(|d| d.node.name.to_string()).collect::<Vec<_>>());
    let planned: rdm_admin::views::Planned = Ok(BTreeMap::new());
    for f in [
        Filter::new().eq("kind", ["rpc"]).eq("status", ["ready"]),
        Filter::new().eq("mesh", ["mesh1"]),
        Filter::new().eq("is_primary", ["true"]),
        Filter::new().eq("mesh", ["mesh2"]).eq("kind", ["broker"]),
        Filter::new(),
    ] {
        let atlas: Vec<String> = answer(&view, &records, &planned, f.clone()).await.into_iter().map(|n| n.name.to_string()).collect();
        assert_eq!(local(&f), Ok(atlas.clone()), "{f:?}: the topology the gateway holds answers what the atlas answers");
        assert!(!atlas.is_empty(), "{f:?} matches a node");
    }
    assert_eq!(local(&Filter::new().eq("kind", ["rpc"]).eq("status", ["ready"])), Ok(vec!["mesh1.rpc.1".to_string()]), "the local query filters kind and status");
    for (f, pair) in [(Filter::new().eq("tags", ["gold"]), "(tags, [gold])"), (Filter::new().eq("config.zone", ["eu-1"]), "(config.zone, [eu-1])")] {
        assert_eq!(answer(&view, &records, &planned, f.clone()).await.len(), 4, "node.get answers the row-only pair {f:?}");
        let why = local(&f).expect_err("a row-only pair is refused locally");
        assert!(why.contains(pair) && why.contains("read it with node.get"), "{why:?} names {pair} and node.get");
    }
}

/// The status and `stopped` of the birth `id`, as `node.get [id: id]` over `view` and `atlas` answers it.
async fn status_of(view: &Topology, atlas: &rdm_admin::atlas::Atlas, id: &NodeId) -> (rdm_core::views::NodeStatus, bool) {
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    match rdm_admin::views::nodes_answer(&path("mesh1.admin.1"), view, &loads, atlas, &Filter::new().eq("id", [id.to_string()])).await {
        StatusReply::Nodes { nodes } => {
            let n = nodes.into_iter().next().expect("the birth is answered");
            (n.status, n.stopped)
        }
        other => panic!("node.get answers the birth, not {other:?}"),
    }
}
