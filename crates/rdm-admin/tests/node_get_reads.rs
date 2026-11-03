//! `node.get`'s reads (i144; Q167; query.md §1): each node's lifecycle record read under its mesh's
//! current writer and its properties at its own node ID, never a copy an atlas holds, each the rows
//! matching every `(property, values)` pair. A birth's facts are
//! read on its node's record, by its id, or on the record its `predecessor` names, or on the record
//! held at its `(path, name)`; a birth the view no longer holds is named at a path the current Build
//! plans.

use rdm_admin::model::*;
use rdm_admin::storage::NodeRow;
use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::storage::MemoryNodesStorage;
use rdm_admin::topology::Topology;
use rdm_core::filter::Filter;
use rdm_core::status::StatusReply;
use std::collections::BTreeMap;
use std::sync::Arc;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// The node property records the reads below answer config and tags from (node-config-update.md step 4:
/// "A reader that needs config reads the node's property record at its node ID"): one store for every
/// cell of this file, each node's record at its own minted id.
fn properties() -> &'static MemoryNodesStorage {
    static RECORDS: std::sync::OnceLock<MemoryNodesStorage> = std::sync::OnceLock::new();
    RECORDS.get_or_init(Default::default)
}

/// `node_id`'s own property record carrying `config` and `tags`, as its own writer put it.
async fn own(node_id: &NodeId, config: BTreeMap<String, String>, tags: std::collections::BTreeSet<String>) {
    use rdm_admin::storage::NodesStorage as _;
    properties().put_properties(&rdm_node::node_records::NodeProperties {
        birth: rdm_node::node_records::RecordBirth { node_id: node_id.clone(), incarnation_id: IncarnationId::mint() },
        config, tags, endpoint_id: None, transport_addr: None, listeners: Vec::new(), app_types: None, held: Default::default(),
    }).await.unwrap();
}

fn path(path: &str, name: &str) -> PathName {
    PathName::new(path, name).unwrap()
}

/// The node read over `view`, each record read through the reader (query.md §1) over `status`'s name
/// claims and the lifecycle records `status`'s records are written as under their mesh's writer in `view`.
async fn answer(view: &Topology, status: &MemoryNodesStorage, filter: Filter) -> Vec<rdm_core::views::NodeView> {
    match reply(view, status, &filter).await {
        StatusReply::Nodes { nodes } => nodes,
        other => panic!("node.get {filter:?} answered {other:?}"),
    }
}

/// The reply of `node.get` with `filter` over `view`, each record read through the reader: every record
/// `status` holds is read at its node id, each mesh in `view` given its Mesh record
/// ([`crate::write_meshes`]), its properties the node's own ([`properties`]).
async fn reply(view: &Topology, status: &MemoryNodesStorage, filter: &Filter) -> StatusReply {
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    let meshes = rdm_admin::storage::MemoryMeshStorage::default();
    crate::write_meshes(view, &meshes).await;
    // Each node's own part of its record beside its mesh-primary's in the one nodes store (Q333).
    {
        use rdm_admin::storage::NodesStorage as _;
        let meshes: std::collections::BTreeSet<String> = view.nodes.iter().map(|n| n.mesh.clone()).chain(view.meshes.iter().map(|m| m.name.clone())).collect();
        for mesh in meshes {
            for row in status.nodes(&mesh).await.unwrap() {
                if let Some(own) = properties().properties(&row.node_id).await.unwrap() {
                    status.put_properties(&own).await.unwrap();
                }
            }
        }
        for n in &view.nodes {
            if let Some(own) = properties().properties(&n.node_id).await.unwrap() {
                status.put_properties(&own).await.unwrap();
            }
        }
    }
    let current = rdm_admin::current_record::CurrentRecords { nodes: status };
    rdm_admin::views::nodes_answer_with_status(&path("mesh1.node-admin", "1"), view, &loads, &atlas, filter, &|_| {}, Some(current)).await
}

/// CONTRACT (Q167 R-RD2, R-RD3; node-rpc-envelope.md node.get): over a fabric of two meshes holding
/// an admin, two gateways and a broker, their records carrying config and tags, `node.get` answers
/// the rows matching every pair, by `(path, name)`: `[config.zone: eu-1]` the two nodes whose config
/// holds that zone, never the one whose config lacks the key; `[tags: gold] [tags: ssd]` (all of) the
/// one carrying both; `[tags: gold, silver]` (any of) the three carrying either; `[mesh: mesh1]
/// [type: gateway]` mesh1's gateway alone; `[mesh: mesh9]`, a mesh the admin does not hold, the empty
/// list; no pair, every node. A pair naming a property no node row has, a value its field cannot
/// take, or no value is `RejectedFilter` (`INVALID_ARGUMENT`) naming the pair, nothing read. What
/// must NOT happen: all-of read as any-of, an AND read as an OR, a missing config key matching, or an
/// unknown property answered as the empty list.
#[tokio::test]
async fn nodes_get_answers_the_rows_matching_every_pair_and_refuses_a_pair_its_schema_has_not() {
    let (admin, gw1, broker, gw2) = (node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), node(rdm_core::PathName::new("mesh1.gateway", "1").unwrap(), "mesh1", "gateway"), node(rdm_core::PathName::new("mesh1.broker", "1").unwrap(), "mesh1", "broker"), node(rdm_core::PathName::new("mesh2.gateway", "1").unwrap(), "mesh2", "gateway"));
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
        let row = NodeRow::new(n.location(), n.mesh.clone(), &n.r#type, n.node_id.clone(), NodeStatus::Ready);
        status.write_record(&row).await.unwrap();
        own(&n.node_id, config.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), tags.iter().map(|t| t.to_string()).collect()).await;
    }
    let names = |nodes: Vec<rdm_core::views::NodeView>| nodes.into_iter().map(|n| n.location()).collect::<Vec<_>>();
    let get = |f: Filter| {
        let (view, status) = (&view, &status);
        async move { names(answer(view, status, f).await) }
    };

    assert_eq!(get(Filter::new()).await, [path("mesh1.broker", "1"), path("mesh1.gateway", "1"), path("mesh1.node-admin", "1"), path("mesh2.gateway", "1")], "no pair: every node, by (path, name)");
    assert_eq!(get(Filter::new().eq("config.zone", ["eu-1"])).await, [path("mesh1.broker", "1"), path("mesh1.gateway", "1")], "[config.zone: eu-1]");
    assert_eq!(get(Filter::new().eq("tags", ["gold"]).eq("tags", ["ssd"])).await, [path("mesh1.gateway", "1")], "[tags: gold] [tags: ssd]: all of");
    assert_eq!(get(Filter::new().eq("tags", ["gold", "silver"])).await, [path("mesh1.broker", "1"), path("mesh1.gateway", "1"), path("mesh2.gateway", "1")], "[tags: gold, silver]: any of");
    assert_eq!(get(Filter::new().eq("mesh", ["mesh1"]).eq("type", ["gateway"])).await, [path("mesh1.gateway", "1")], "[mesh: mesh1] [type: gateway]");
    assert_eq!(get(Filter::new().eq("type", ["gateway"]).eq("config.rack", ["7"])).await, [path("mesh2.gateway", "1")], "[type: gateway] [config.rack: 7]");
    assert!(get(Filter::new().eq("mesh", ["mesh9"])).await.is_empty(), "[mesh: mesh9]: a mesh the admin does not hold is the empty list");
    assert!(get(Filter::new().eq("id", [NodeId::mint().to_string()])).await.is_empty(), "[id: an id not held] is the empty list");

    for (filter, pair) in [
        (Filter::new().eq("mesh", ["mesh1"]).eq("zone", ["eu-1"]), "the pair (zone, [eu-1]) names no property of a node row"),
        (Filter::new().eq("status", ["redy"]), "the pair (status, [redy])"),
        (Filter::new().eq("is_primary", ["yes"]), "the pair (is_primary, [yes])"),
        (Filter::new().eq("tags", Vec::<String>::new()), "the pair (tags, []) names no value"),
    ] {
        match reply(&view, &status, &filter).await {
            r @ StatusReply::RejectedFilter { .. } => {
                let StatusReply::RejectedFilter { reason } = &r else { unreachable!() };
                assert!(reason.contains(pair), "{reason:?} names {pair:?}");
                assert_eq!(<rdm_core::status::Status as rdm_core::protocol::NodeProtocol>::classify_reply(&r), rdm_core::outcome::ReplyKind::new(rdm_core::outcome::Code::InvalidArgument));
            }
            other => panic!("{filter:?} is refused naming the pair, not {other:?}"),
        }
    }
}

/// The fabric-primary `me`'s `StatusService` over `view`, its atlas `atlas`, its nodes store
/// `nodes_storage` (its config door bound to it, as boot binds it) and its mesh store `meshes`; every
/// other store in memory.
async fn fabric_primary_at(me: &Node, view: Topology, atlas: Arc<rdm_admin::atlas::Atlas>, nodes_storage: Arc<dyn rdm_admin::storage::NodesStorage>, meshes: Arc<dyn rdm_admin::storage::MeshStorage>) -> rdm_admin::status_rpc::StatusService {
    let records = nodes_storage.clone();
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
    let configs: rdm_admin::node_config::NodeConfigsSlot = Default::default();
    let _ = configs.set(Arc::new(crate::bound_configs_over(records, rdm_admin::node_config::NodeConfigs::new(client, Default::default())).await));
    let slot = Arc::new(std::sync::OnceLock::new());
    let _ = slot.set(atlas);
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    rdm_admin::status_rpc::StatusService {
        certs: None,
        primary_commands: Default::default(),
        app_channels: None,
        declaration_seam: None,
        command_frame_seam: None,
        operation_effects: Default::default(),
        stopped: Default::default(),
        me: me.location(),
        node_id: me.node_id.clone(),
        fabric_id: view.fabric.id.clone(),
        topology: Arc::new(tokio::sync::RwLock::new(view)),
        declared: Arc::new(std::sync::Mutex::new(rdm_admin::status_rpc::Declared::default())),
        meshes_storage: meshes,
        fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
        nodes_storage,
        caches: Default::default(),
        mesh_ids: Arc::new(BTreeMap::new),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        mesh_deleter: Arc::new(std::sync::OnceLock::new()),
        storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(),
        atlas: slot,
        fabric_recovery: Default::default(),
        configs,
        republish: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(), rejoin: Default::default(),
        loads: Some(loads),
        digest_view: None,
        unreachable: Default::default(),
        builds: Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    }
}

/// A plural read as the `StatusService` answers it on op 0x23 (Q285).
fn get(entity: rdm_core::filter::Entity, filter: Filter) -> rdm_core::status::Read {
    rdm_core::status::Read { entity, node: None, filter }
}

/// CONTRACT (query.md §1: "Node rows and property maps are not copied into an atlas to serve
/// `node.get` or acceptance"; Q331: one node store, each node's record at its node id in it;
/// fabric-certs.md §Tracking): at a fabric-primary, `node.get` answers every node matching the filter,
/// its cert included, from each node's record in the nodes store (a birth the view holds is read by its
/// node ID, never through a name claim), and `fabric.get` answers the fabric with its `next_expiry`, the
/// soonest member cert expiry the records hold. Must NOT happen: a read answered short, or a cert taken
/// from anywhere but the node's record.
#[tokio::test]
async fn nodes_get_and_fabrics_get_answer_each_nodes_cert_from_its_record() {
    let (mut admin, gw) = (node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), node(rdm_core::PathName::new("mesh1.gateway", "1").unwrap(), "mesh1", "gateway"));
    admin.r#type = "node-admin".to_string();
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), gw.clone()],
    };
    let records = Arc::new(MemoryNodesStorage::default());
    let expiries = [(&admin, 9_000_000u64), (&gw, 7_000_000u64)];
    for (n, expires_at) in expiries {
        let mut row = NodeRow::new(n.location(), n.mesh.clone(), &n.r#type, n.node_id.clone(), NodeStatus::Ready);
        row.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.location()).cert = Some(rdm_admin::storage::CertDates { issued_at: 1_000_000, expires_at, issuer_expires_at: 20_000_000, renew_at: expires_at - 1_000 });
        records.write_record(&row).await.unwrap();
    }
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    atlas.scope(["mesh1".to_string()].into());
    // Each record read at its node id in the nodes store (query.md §1).
    let meshes = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
    crate::write_meshes(&view, &*meshes).await;
    for n in [&admin, &gw] {
        own(&n.node_id, BTreeMap::new(), Default::default()).await;
        use rdm_admin::storage::NodesStorage as _;
        let own = properties().properties(&n.node_id).await.unwrap().expect("the node's own part of its record");
        records.put_properties(&own).await.unwrap();
    }
    let fp = fabric_primary_at(&admin, view, atlas, records.clone(), meshes).await;

    match fp.read_rows(&get(rdm_core::filter::Entity::Nodes, Filter::new().eq("mesh", ["mesh1"]))).await {
        Ok(rdm_core::read::Rows::Nodes(nodes)) => {
            assert_eq!(nodes.iter().map(|n| n.location()).collect::<Vec<_>>(), [path("mesh1.gateway", "1"), path("mesh1.node-admin", "1")], "in path order: {nodes:#?}");
            assert_eq!(nodes[0].cert.as_ref().map(|c| c.expires_at), Some(7_000_000), "the cert is the node's record's");
        }
        other => panic!("node.get answers from the nodes' records, not {other:?}"),
    }
    match fp.read_rows(&get(rdm_core::filter::Entity::Fabrics, Filter::new())).await {
        Ok(rdm_core::read::Rows::Fabrics(fabrics)) => {
            assert_eq!(fabrics.len(), 1, "{fabrics:#?}");
            assert_eq!(fabrics[0].next_expiry, Some(7_000_000), "the soonest cert expiry the atlas's records hold: {fabrics:#?}");
        }
        other => panic!("fabric.get answers from the nodes' records, not {other:?}"),
    }
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
    let (mut admin, mut rpc, peer) = (node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node"));
    // The fabric-primary, mesh1's primary too: the writer of mesh1's records and, mesh2 holding no
    // primary, the acting writer of mesh2's (query.md §1).
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    // The stop's admission at the mesh-primary set the birth leaving in its view, with no frame of the node.
    rpc.status = NodeStatus::Leaving;
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), rpc.clone(), peer.clone()],
    };
    let records = MemoryNodesStorage::default();
    // Each node's own property record: a read answers its properties beside its lifecycle record.
    for n in [&admin, &rpc, &peer] {
        own(&n.node_id, BTreeMap::new(), Default::default()).await;
    }
    let mut row = NodeRow::new(rpc.location(), rpc.mesh.clone(), &rpc.r#type, rpc.node_id.clone(), NodeStatus::Ready);
    row.birth_mut(rpc.incarnation_id.as_ref().unwrap(), &rpc.location()).declared.insert(rdm_core::status::NodeState::Ready);
    records.write_record(&row).await.unwrap();
    // mesh2's record as its current writer holds it.
    let mut peer_row = NodeRow::new(peer.location(), peer.mesh.clone(), &peer.r#type, peer.node_id.clone(), NodeStatus::Pending);
    peer_row.birth_mut(peer.incarnation_id.as_ref().unwrap(), &peer.location());
    records.write_record(&peer_row).await.unwrap();
    use rdm_core::views::NodeStatus as Read;
    assert_eq!(status_of(&view, &records, &rpc.node_id).await, (Read::Ready, false), "a stop the node has not acknowledged moves its row nowhere: ready");
    let ready: Vec<PathName> = answer(&view, &records, Filter::new().eq("status", ["ready"])).await.into_iter().map(|n| n.location()).collect();
    assert_eq!(ready, [path("mesh1.node", "1"), path("mesh1.node-admin", "1")], "[status: ready] matches the birth whose stop is unacknowledged, never mesh2's pending record");
    // Another mesh's record is read under its current writer as every record is (query.md §1): the
    // state it took answers, never the view's.
    assert_eq!(status_of(&view, &records, &peer.node_id).await, (Read::Pending, false), "mesh2's birth: the pending its current record holds, not the view's ready");
    peer_row.status = NodeStatus::Stopped;
    records.write_record(&peer_row).await.unwrap();
    assert_eq!(status_of(&view, &records, &peer.node_id).await, (Read::Stopped, true), "mesh2's birth: the stopped its record took");
    peer_row.status = NodeStatus::ConnectionsDeleted;
    records.write_record(&peer_row).await.unwrap();
    assert_eq!(status_of(&view, &records, &peer.node_id).await, (Read::ConnectionsDeleted, false), "mesh2's birth: the connections-deleted its record took");

    row.status = NodeStatus::Stopping;
    records.write_record(&row).await.unwrap();
    assert_eq!(status_of(&view, &records, &rpc.node_id).await, (Read::Stopping, false), "the node's acknowledgement written on the row: stopping");

    row.status = NodeStatus::Stopped;
    records.write_record(&row).await.unwrap();
    assert_eq!(status_of(&view, &records, &rpc.node_id).await, (Read::Stopped, true), "the stopped frame written on the row: stopped");

    // Silence is the observer's (node-identity-and-state.md §7.3), not a state a command sets: a birth
    // the view holds pending-reconnect reads it whatever its row says, and a silence mark its row still
    // carries is not the answer once the view hears the birth.
    row.status = NodeStatus::Ready;
    row.birth_mut(rpc.incarnation_id.as_ref().unwrap(), &rpc.location()).silence = Some(NodeStatus::PendingReconnect);
    records.write_record(&row).await.unwrap();
    let mut silent = view.clone();
    silent.nodes.iter_mut().filter(|n| n.node_id == rpc.node_id).for_each(|n| n.status = NodeStatus::PendingReconnect);
    assert_eq!(status_of(&silent, &records, &rpc.node_id).await, (Read::PendingReconnect, false), "the view holds the birth silent: pending-reconnect");
    let mut heard = view.clone();
    heard.nodes.iter_mut().filter(|n| n.node_id == rpc.node_id).for_each(|n| n.status = NodeStatus::Ready);
    assert_eq!(status_of(&heard, &records, &rpc.node_id).await, (Read::Ready, false), "heard again: the row's ready, not the silence mark the row still carries");
}

/// CONTRACT (query.md §2, Q232: "the same pair list answers the same nodes from both sources on a
/// settled fabric"): a gateway of mesh1 holding the digests of mesh1's admin (its primary), two rpc
/// nodes (one ready, one pending) and a broker of mesh2, and the fabric-primary's atlas
/// holding the same births, answer the same nodes for the same gossiped pairs: `[type: node] [status:
/// ready]`, `[mesh: mesh1]`, `[is_primary: true]`, `[mesh: mesh2] [type: broker]` and no
/// pair; the local query makes no call. A row-only pair (`[tags: gold]`, `[config.zone: eu-1]`) is
/// answered by `node.get` and refused locally naming the pair and `node.get`. Must NOT happen: the
/// two sources disagreeing on a gossiped pair, or a row-only pair answered locally as no match.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_pairs_answer_the_same_nodes_from_the_records_and_from_the_topology_a_node_holds() {
    use rdm_core::{MemberStatus, MeshDigest, MeshNode};
    let fabric = FabricId::mint();
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let gateway = rdm_transport::topology::TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), NodeId::mint().as_str(), Arc::new(time.clone()), vec![], &[]).await.unwrap();

    // (name, ready, primary of its node type)
    let births = [(path("mesh1.node-admin", "1"), "mesh1", "node-admin", true, true), (path("mesh1.node", "1"), "mesh1", "node", true, false), (path("mesh1.node", "2"), "mesh1", "node", false, false), (path("mesh2.broker", "1"), "mesh2", "broker", true, false)];
    let mut nodes = Vec::new();
    let records = MemoryNodesStorage::default();
    for (i, (name, mesh, kind, ready, primary)) in births.into_iter().enumerate() {
        let mut n = node(name.clone(), mesh, kind);
        n.status = if ready { NodeStatus::Ready } else { NodeStatus::Pending };
        n.is_primary = primary;
        n.endpoint_id = Some(EndpointId(format!("key-{}", n.node_id)));
        n.transport_addr = Some(format!("127.0.0.1:{}", 21_000 + i).parse().unwrap());
        let row = NodeRow::new(n.location(), n.mesh.clone(), &n.r#type, n.node_id.clone(), n.status);
        records.write_record(&row).await.unwrap();
        own(&n.node_id, BTreeMap::from([("zone".to_string(), "eu-1".to_string())]), ["gold".to_string()].into()).await;
        let d = MeshDigest {
            held_hash: Default::default(),
            fabric_id: fabric.clone(),
            node: MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(), node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: n.endpoint_id.clone().unwrap(), transport_addr: n.transport_addr.unwrap(), incarnation: n.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
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
        assert!(gateway.digest_view.record(d.clone()), "{name:?}'s heartbeat is held");
        if ready && n.mesh == "mesh1" {
            assert!(gateway.digest_view.started(&d.node.node_id, &d.node.incarnation, &d.node.location(), &d.node.mesh), "{name:?}'s node.started, as its mesh-primary's projection carries it");
        }
        nodes.push(n);
    }
    let view = Topology {
        fabric: Fabric { id: fabric.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
        nodes,
    };
    let local = |f: &Filter| gateway.nodes_get(f).map(|ds| ds.into_iter().map(|d| d.node.location()).collect::<Vec<_>>());
    for f in [
        Filter::new().eq("type", ["node"]).eq("status", ["ready"]),
        Filter::new().eq("mesh", ["mesh1"]),
        Filter::new().eq("is_primary", ["true"]),
        Filter::new().eq("mesh", ["mesh2"]).eq("type", ["broker"]),
        Filter::new(),
    ] {
        let atlas: Vec<PathName> = answer(&view, &records, f.clone()).await.into_iter().map(|n| n.location()).collect();
        assert_eq!(local(&f), Ok(atlas.clone()), "{f:?}: the topology the gateway holds answers what the atlas answers");
        assert!(!atlas.is_empty(), "{f:?} matches a node");
    }
    assert_eq!(local(&Filter::new().eq("type", ["node"]).eq("status", ["ready"])), Ok(vec![path("mesh1.node", "1")]), "the local query filters type and status");
    for (f, pair) in [(Filter::new().eq("tags", ["gold"]), "(tags, [gold])"), (Filter::new().eq("config.zone", ["eu-1"]), "(config.zone, [eu-1])")] {
        assert_eq!(answer(&view, &records, f.clone()).await.len(), 4, "node.get answers the row-only pair {f:?}");
        let why = local(&f).expect_err("a row-only pair is refused locally");
        assert!(why.contains(pair) && why.contains("read it with node.get"), "{why:?} names {pair} and node.get");
    }
}

/// The status and `stopped` of the birth `id`, as `node.get [id: id]` over `view` answers it, each record read through the reader over `records` ([`reply`]).
async fn status_of(view: &Topology, records: &MemoryNodesStorage, id: &NodeId) -> (rdm_core::views::NodeStatus, bool) {
    let n = answer(view, records, Filter::new().eq("id", [id.to_string()])).await.into_iter().next().expect("the birth is answered");
    (n.status, n.stopped)
}

/// CONTRACT (query.md §1: "Properties are read separately at the subject node ID. A missing
/// current-writer record is unavailable"; "`node.get` materializes records"; Q318): a node whose own
/// property record is not written yet is listed by `node.get` with its lifecycle facts and
/// `properties: Unavailable` naming it, its config and tags empty; a pair on its tags or config does not
/// match it, negated or not; a node whose record is read answers `Read` and its own tags. Must NOT
/// happen: the whole read refused for one node's missing record, or the unavailable node matched by
/// a negated property pair.
#[tokio::test]
async fn a_node_with_no_property_record_is_listed_unavailable_and_no_property_pair_matches_it() {
    let mut admin = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let (owned, bare) = (node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"));
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin.clone(), owned.clone(), bare.clone()],
    };
    let records = MemoryNodesStorage::default();
    for n in [&admin, &owned, &bare] {
        let mut row = NodeRow::new(n.location(), n.mesh.clone(), &n.r#type, n.node_id.clone(), NodeStatus::Ready);
        row.birth_mut(n.incarnation_id.as_ref().unwrap(), &n.location());
        records.write_record(&row).await.unwrap();
    }
    // The admin and mesh1.node.1 wrote their own property records; mesh1.node.2 has not yet.
    own(&admin.node_id, BTreeMap::new(), Default::default()).await;
    own(&owned.node_id, BTreeMap::from([("zone".to_string(), "eu-1".to_string())]), ["gold".to_string()].into()).await;

    let all = answer(&view, &records, Filter::new().eq("mesh", ["mesh1"])).await;
    assert_eq!(all.len(), 3, "every node is listed, the one with no property record included: {all:#?}");
    let of = |id: &NodeId| all.iter().find(|n| &n.node_id == id).unwrap_or_else(|| panic!("{id} listed: {all:#?}")).clone();
    let b = of(&bare.node_id);
    assert!(matches!(&b.properties, rdm_core::views::PropertyRecord::Unavailable { reason } if reason.contains(bare.node_id.as_str())), "its properties are unavailable, naming it: {b:?}");
    assert_eq!((b.status, b.tags.len(), b.config.len()), (rdm_core::views::NodeStatus::Ready, 0, 0), "its lifecycle facts stand, its properties empty: {b:?}");
    let o = of(&owned.node_id);
    assert_eq!((o.properties.clone(), o.tags.clone()), (rdm_core::views::PropertyRecord::Read, ["gold".to_string()].into()), "a node whose record is read answers its own tags");

    let ids = |nodes: Vec<rdm_core::views::NodeView>| nodes.into_iter().map(|n| n.node_id).collect::<Vec<_>>();
    assert_eq!(ids(answer(&view, &records, Filter::new().eq("tags", ["gold"])).await), [owned.node_id.clone()], "[tags: gold] matches the node that carries it");
    assert_eq!(ids(answer(&view, &records, Filter::new().not("config.zone", ["eu-1"])).await), [admin.node_id.clone()], "a negated config pair matches the node whose record lacks the value, never the node whose record is unavailable");
}
