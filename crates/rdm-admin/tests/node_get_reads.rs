//! `node.get`'s reads (i144; Q167; query.md §1): each node's lifecycle record read under its mesh's
//! current writer and its properties at its own node ID, never a copy an atlas holds, each the rows
//! matching every `(property, values)` pair. A birth's facts are
//! read on its node's record, by its id, or on the record its `predecessor` names, or on the record
//! held at its `(path, name)`; a birth the view no longer holds is named at a path the current Build
//! plans.

use rdm_admin::model::*;
use rdm_admin::storage::NodeRecord;
use rdm_admin::storage::MemoryNodesStorage;
use rdm_admin::storage::NodesStorage as _;
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

/// The config and tags each node of these cells wrote on its own record (node-config-update.md step 4),
/// held apart from the cell's store until [`reply`] lays them on each node's record there: one store
/// for every cell of this file, each node's record at its own minted id.
fn properties() -> &'static MemoryNodesStorage {
    static RECORDS: std::sync::OnceLock<MemoryNodesStorage> = std::sync::OnceLock::new();
    RECORDS.get_or_init(Default::default)
}

/// `node_id`'s own `config` and `tags`, as its own writer put them on its record.
async fn own(node_id: &NodeId, config: BTreeMap<String, String>, tags: std::collections::BTreeSet<String>) {
    let mut r = rdm_node::nodes_storage::fixture_record(&path("mesh1.node", "own"), "node", node_id.clone(), IncarnationId::mint(), NodeStatus::Ready);
    (r.config, r.tags) = (config, tags);
    properties().put_node(&r).await.unwrap();
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
    // Each node's own config and tags on its record in the one nodes store (Q356).
    {
        let meshes: std::collections::BTreeSet<String> = view.nodes.iter().map(|n| n.mesh.clone()).chain(view.meshes.iter().map(|m| m.name.clone())).collect();
        for mesh in meshes {
            for row in status.nodes(&mesh).await.unwrap() {
                if let Some(own) = properties().node(&row.node_id).await.unwrap() {
                    status.put_node(&NodeRecord { config: own.record.config, tags: own.record.tags, ..row }).await.unwrap();
                }
            }
        }
    }
    let current = rdm_admin::current_record::CurrentRecords { nodes: status };
    rdm_admin::views::nodes_answer_with_status(&path("mesh1.node-admin", "1"), view, &loads, &atlas, filter, Some(current)).await
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
        let row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Ready);
        status.put_node(&row).await.unwrap();
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
        (Filter::new().eq("state", ["redy"]), "the pair (state, [redy])"),
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
/// `nodes_storage` (its `NodeConfigs` bound to it, as boot binds it) and its mesh store `meshes`; every
/// other store in memory.
async fn fabric_primary_at(me: &Node, view: Topology, atlas: Arc<rdm_admin::atlas::Atlas>, nodes_storage: Arc<dyn rdm_admin::storage::NodesStorage>, meshes: Arc<dyn rdm_admin::storage::MeshStorage>) -> rdm_admin::status_rpc::StatusService {
    let records = nodes_storage.clone();
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
    let configs: rdm_admin::node_config::NodeConfigsSlot = Default::default();
    let _ = configs.set(Arc::new(crate::bound_configs_over(records, rdm_admin::node_config::NodeConfigs::new(client)).await));
    let slot = Arc::new(std::sync::OnceLock::new());
    let _ = slot.set(atlas);
    let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
    rdm_admin::status_rpc::StatusService {
        certs: None,
        primary_commands: Default::default(),
        topology_view: None,
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
        storage_changes: Default::default(),
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
        let mut row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), n.incarnation_id.clone().unwrap(), NodeStatus::Ready);
        row.cert = Some(rdm_admin::storage::CertDates { issued_at: 1_000_000, expires_at, issuer_expires_at: 20_000_000, renew_at: expires_at - 1_000 });
        records.put_node(&row).await.unwrap();
    }
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    atlas.scope(["mesh1".to_string()].into());
    // Each record read at its node id in the nodes store (query.md §1).
    let meshes = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
    crate::write_meshes(&view, &*meshes).await;
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

/// CONTRACT (Q233; states.md "Who sets each state": each state from starting to stopped is set by the
/// node, on its own record, Q356): a stop the mesh-primary admitted and the node has not acknowledged
/// leaves its record ready, and `node.get` answers ready, not stopped, though the admitting
/// mesh-primary's view already holds the birth leaving; the node's stopping, written on its record,
/// answers stopping; its stopped answers stopped. Silence is the observer's: a birth the view holds
/// pending-reconnect reads so, and reads its record's ready once heard. Must NOT happen: the gossip
/// view's lifecycle status, or a lifecycle effect laid over it, answered in place of the record's.
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
    let mut row = rdm_node::nodes_storage::fixture_record(&rpc.location(), &rpc.r#type, rpc.node_id.clone(), rpc.incarnation_id.clone().unwrap(), NodeStatus::Ready);
    records.put_node(&row).await.unwrap();
    // mesh2's record as its current writer holds it.
    let mut peer_row = rdm_node::nodes_storage::fixture_record(&peer.location(), &peer.r#type, peer.node_id.clone(), peer.incarnation_id.clone().unwrap(), NodeStatus::Pending);
    records.put_node(&peer_row).await.unwrap();
    use rdm_core::views::NodeStatus as Read;
    assert_eq!(status_of(&view, &records, &rpc.node_id).await, Read::Ready, "a stop the node has not acknowledged moves its record nowhere: ready");
    let ready: Vec<PathName> = answer(&view, &records, Filter::new().eq("state", ["ready"])).await.into_iter().map(|n| n.location()).collect();
    assert_eq!(ready, [path("mesh1.node", "1"), path("mesh1.node-admin", "1")], "[status: ready] matches the birth whose stop is unacknowledged, never mesh2's pending record");
    // Another mesh's record is read under its current writer as every record is (query.md §1): the
    // state it took answers, never the view's.
    assert_eq!(status_of(&view, &records, &peer.node_id).await, Read::Pending, "mesh2's birth: the pending its current record holds, not the view's ready");
    peer_row.state = NodeStatus::Stopped;
    records.put_node(&peer_row).await.unwrap();
    assert_eq!(status_of(&view, &records, &peer.node_id).await, Read::Stopped, "mesh2's birth: the stopped its record took");
    peer_row.state = NodeStatus::ConnectionsDeleted;
    records.put_node(&peer_row).await.unwrap();
    assert_eq!(status_of(&view, &records, &peer.node_id).await, Read::ConnectionsDeleted, "mesh2's birth: the connections-deleted its record took");

    row.state = NodeStatus::Stopping;
    records.put_node(&row).await.unwrap();
    assert_eq!(status_of(&view, &records, &rpc.node_id).await, Read::Stopping, "the node's stopping, written on its record");

    row.state = NodeStatus::Stopped;
    records.put_node(&row).await.unwrap();
    assert_eq!(status_of(&view, &records, &rpc.node_id).await, Read::Stopped, "the node's stopped, written on its record");

    // Silence is the observer's (node-identity-and-state.md §7.3), not a state a command sets: a birth
    // the view holds pending-reconnect reads it whatever its record says.
    row.state = NodeStatus::Ready;
    records.put_node(&row).await.unwrap();
    let mut silent = view.clone();
    silent.nodes.iter_mut().filter(|n| n.node_id == rpc.node_id).for_each(|n| n.status = NodeStatus::PendingReconnect);
    assert_eq!(status_of(&silent, &records, &rpc.node_id).await, Read::PendingReconnect, "the view holds the birth silent: pending-reconnect");
    let mut heard = view.clone();
    heard.nodes.iter_mut().filter(|n| n.node_id == rpc.node_id).for_each(|n| n.status = NodeStatus::Ready);
    assert_eq!(status_of(&heard, &records, &rpc.node_id).await, Read::Ready, "heard again: the record's ready");
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
    use rdm_core::{MeshDigest, MeshNode};
    let fabric = FabricId::mint();
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let gateway = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &MeshId::mint(), NodeId::mint().as_str(), Arc::new(time.clone()), Default::default());
    let gateway_channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &gateway).unwrap();
    gateway_channels.join(rdm_gossip::channel::ChannelId::mesh(gateway.fabric_id(), gateway.mesh_id()), vec![]).await.unwrap();

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
        let row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), n.status);
        records.put_node(&row).await.unwrap();
        own(&n.node_id, BTreeMap::from([("zone".to_string(), "eu-1".to_string())]), ["gold".to_string()].into()).await;
        let d = MeshDigest {
            fabric_id: fabric.clone(),
            node: MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(), node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: n.endpoint_id.clone().unwrap(), transport_addr: n.transport_addr.unwrap(), incarnation: n.incarnation_id.clone().unwrap(), supersedes: None, },
            status: if ready { rdm_core::model::NodeStatus::Ready } else { rdm_core::model::NodeStatus::Pending },
            digest_seq: 10,
            emitted_at_rafka_ms: 10,
            primary: rdm_core::RoleRevision::of_type(&n.r#type, primary.then_some(1), None),
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
        Filter::new().eq("type", ["node"]).eq("state", ["ready"]),
        Filter::new().eq("mesh", ["mesh1"]),
        Filter::new().eq("is_primary", ["true"]),
        Filter::new().eq("mesh", ["mesh2"]).eq("type", ["broker"]),
        Filter::new(),
    ] {
        let atlas: Vec<PathName> = answer(&view, &records, f.clone()).await.into_iter().map(|n| n.location()).collect();
        assert_eq!(local(&f), Ok(atlas.clone()), "{f:?}: the topology the gateway holds answers what the atlas answers");
        assert!(!atlas.is_empty(), "{f:?} matches a node");
    }
    assert_eq!(local(&Filter::new().eq("type", ["node"]).eq("state", ["ready"])), Ok(vec![path("mesh1.node", "1")]), "the local query filters type and status");
    for (f, pair) in [(Filter::new().eq("tags", ["gold"]), "(tags, [gold])"), (Filter::new().eq("config.zone", ["eu-1"]), "(config.zone, [eu-1])")] {
        assert_eq!(answer(&view, &records, f.clone()).await.len(), 4, "node.get answers the row-only pair {f:?}");
        let why = local(&f).expect_err("a row-only pair is refused locally");
        assert!(why.contains(pair) && why.contains("read it with node.get"), "{why:?} names {pair} and node.get");
    }
}

/// The `state` of the birth `id`, as `node.get [id: id]` over `view` answers it, each record read through the reader over `records` ([`reply`]).
async fn status_of(view: &Topology, records: &MemoryNodesStorage, id: &NodeId) -> rdm_core::views::NodeStatus {
    answer(view, records, Filter::new().eq("id", [id.to_string()])).await.into_iter().next().expect("the birth is answered").state
}

/// The attribute `key` of the captured span `sp`, as its exporter holds it (text).
fn attr(sp: &serde_json::Value, key: &str) -> String {
    sp["attributes"][key].as_str().unwrap_or_default().to_string()
}

/// The store calls the captured read span `sp` names (`io_calls`).
fn io_calls(sp: &serde_json::Value) -> u64 {
    attr(sp, "io_calls").parse().unwrap_or_else(|e| panic!("the read's span names its store calls (io_calls): {e}: {sp}"))
}

/// CONTRACT (query.md §1: "A current-pair lookup resolves the path and name to a node ID from the node
/// records. The reader reads the node's record at that node ID", and "`node.get` materializes records and
/// applies its complete filter"; node-rpc-envelope.md, node properties: "path, mesh and type are
/// immutable"; fabric-certs.md §Tracking: "`fabric.get` shows the next expiry across the fabric"): at the
/// fabric-primary of two meshes of six births each, every node's record in the file nodes store, a read
/// reads the records of the nodes it can answer, and many records as one store call per mesh, never one
/// call per birth the view holds: `node.get` of one node's pair reads that node's record alone (one
/// `rdm.node_admin.node.resolve.via-lifecycle-record` span, its own) in at most two store calls (the
/// current-pair lookup and the record); `node.get` by id reads that id's record in one call;
/// `node.get [mesh: mesh2]`, `[type: node]` and no pair read their records at one call per mesh;
/// `fabric.get` reads the cert dates at one call per mesh. Each answers what it answered before.
/// A record still claiming a role the view's election gave another birth (a primary killed before it
/// cleared its record), read by its own pair, is settled against every claim as before (Q429: "a view
/// decides between two claims by the elections rule"): it answers the role false, its role the view's.
/// Must NOT happen: a read making one store call per birth the view holds.
#[test]
fn a_read_reads_the_records_of_the_nodes_it_answers_and_every_mesh_in_one_store_call() {
    use rdm_core::filter::Entity;
    use rdm_core::read::Rows;
    use tracing::Instrument as _;
    let cap = crate::mesh_delete_resume::capture();
    let dir = std::env::temp_dir().join(format!("node-get-store-calls-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let spans = cap.run(async {
        let store = rdm_admin::store_root::StoreRoot::at(&dir).unwrap();
        let records = Arc::new(rdm_admin::storage::FileNodesStorage::open(&store, &rdm_admin::store_log::LogWriter::mint()).await.unwrap());
        // Each mesh: its node-admin, its mesh-primary (mesh1's the fabric-primary), and five nodes.
        let mut births = Vec::new();
        for mesh in ["mesh1", "mesh2"] {
            let mut admin = node(path(&format!("{mesh}.node-admin"), "1"), mesh, "node-admin");
            admin.is_primary = true;
            admin.is_fabric_primary = mesh == "mesh1";
            births.push(admin);
            for i in 1..=5 {
                births.push(node(path(&format!("{mesh}.node"), &i.to_string()), mesh, "node"));
            }
        }
        for n in &births {
            let mut row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), n.incarnation_id.clone().unwrap(), NodeStatus::Ready);
            (row.is_primary, row.is_fabric_primary) = (n.is_primary.then_some(1), n.is_fabric_primary.then_some(1));
            let expires_at = if n.location() == path("mesh2.node", "3") { 5_000_000 } else { 9_000_000 };
            row.cert = Some(rdm_admin::storage::CertDates { issued_at: 1_000_000, expires_at, issuer_expires_at: 20_000_000, renew_at: expires_at - 1_000 });
            records.put_node(&row).await.unwrap();
        }
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
            nodes: births.clone(),
        };
        let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
        atlas.scope(["mesh1".to_string(), "mesh2".to_string()].into());
        let meshes = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
        crate::write_meshes(&view, &*meshes).await;
        let fp = fabric_primary_at(&births[0], view, atlas, records.clone(), meshes).await;
        let read = |entity: Entity, filter: Filter| {
            let fp = &fp;
            async move { fp.read_rows(&get(entity, filter)).instrument(tracing::info_span!("cell.read")).await.unwrap_or_else(|r| panic!("the read is answered: {r:?}")) }
        };
        let located = |rows: Rows| match rows {
            Rows::Nodes(nodes) => nodes.into_iter().map(|n| n.location()).collect::<Vec<_>>(),
            other => panic!("node.get answers nodes, not {other:?}"),
        };

        let subject = births.iter().find(|n| n.location() == path("mesh2.node", "3")).unwrap().clone();
        assert_eq!(located(read(Entity::Nodes, Filter::new().eq("path", ["mesh2.node"]).eq("name", ["3"])).await), [path("mesh2.node", "3")], "the pair's node");
        assert_eq!(located(read(Entity::Nodes, Filter::new().eq("id", [subject.node_id.to_string()])).await), [path("mesh2.node", "3")], "the id's node");
        assert_eq!(located(read(Entity::Nodes, Filter::new().eq("mesh", ["mesh2"])).await).len(), 6, "mesh2's six births");
        assert_eq!(located(read(Entity::Nodes, Filter::new().eq("type", ["node"])).await).len(), 10, "the ten nodes of type node");
        assert_eq!(located(read(Entity::Nodes, Filter::new()).await).len(), 12, "every birth");
        match read(Entity::Fabrics, Filter::new()).await {
            Rows::Fabrics(fabrics) => assert_eq!(fabrics[0].next_expiry, Some(5_000_000), "the soonest cert expiry across the fabric: {fabrics:#?}"),
            other => panic!("fabric.get answers the fabric, not {other:?}"),
        }

        // mesh2.node-admin/2, killed while it held mesh2's primary role: its record still claims it, and
        // the view's election gave the role to mesh2.node-admin/1.
        let mut stale = node(path("mesh2.node-admin", "2"), "mesh2", "node-admin");
        stale.status = NodeStatus::Dead;
        let mut row = rdm_node::nodes_storage::fixture_record(&stale.location(), &stale.r#type, stale.node_id.clone(), stale.incarnation_id.clone().unwrap(), NodeStatus::Ready);
        row.is_primary = Some(1);
        records.put_node(&row).await.unwrap();
        fp.topology.write().await.nodes.push(stale.clone());
        match read(Entity::Nodes, Filter::new().eq("path", ["mesh2.node-admin"]).eq("name", ["2"])).await {
            Rows::Nodes(nodes) => {
                assert_eq!(nodes.iter().map(|n| (n.node_id.clone(), n.is_primary)).collect::<Vec<_>>(), [(stale.node_id.clone(), false)], "the stale claim, read alone by its pair, is settled against mesh2's elected primary's: {nodes:#?}");
            }
            other => panic!("node.get answers nodes, not {other:?}"),
        }
        let primaries = located(read(Entity::Nodes, Filter::new().eq("is_primary", ["true"])).await);
        assert_eq!(primaries, [path("mesh1.node-admin", "1"), path("mesh2.node-admin", "1")], "[is_primary: true]: each mesh's elected primary, never the stale claim");
        cap.spans()
    });
    let _ = std::fs::remove_dir_all(&dir);

    let served = |pairs: &str| -> serde_json::Value {
        spans.iter().find(|sp| sp["name"] == "rdm.node_admin.node.serve.via-get" && attr(sp, "pairs") == pairs).cloned().unwrap_or_else(|| panic!("node.get {pairs:?} is served"))
    };
    let by_id = spans.iter().find(|sp| sp["name"] == "rdm.node_admin.node.serve.via-get" && attr(sp, "pairs").starts_with("(id, ")).cloned().expect("node.get by id is served");
    let resolved = |read: &serde_json::Value| -> Vec<String> {
        spans.iter().filter(|sp| sp["name"] == "rdm.node_admin.node.resolve.via-lifecycle-record" && sp["parent_span_id"] == read["span_id"]).map(|sp| attr(sp, "subject_id")).collect()
    };
    let reads: Vec<serde_json::Value> = ["(path, [mesh2.node]) (name, [3])", "(mesh, [mesh2])", "(type, [node])", ""].into_iter().map(served).chain([by_id.clone()]).collect();
    let cost: Vec<(String, u64, usize)> = reads.iter().map(|sp| (attr(sp, "pairs"), io_calls(sp), resolved(sp).len())).collect();
    let fabric = spans.iter().find(|sp| sp["name"] == "rdm.node_admin.fabric.serve.via-get").cloned().expect("fabric.get is served");
    eprintln!("each read's store calls and the records it resolved (pairs, io_calls, via-lifecycle-record spans): {cost:#?}; fabric.get io_calls {}", attr(&fabric, "io_calls"));

    let pair = served("(path, [mesh2.node]) (name, [3])");
    let subject = attr(&pair, "role_from").split(':').next().unwrap_or_default().to_string();
    assert_eq!(resolved(&pair), [subject], "node.get of one pair reads that node's record alone: {cost:#?}");
    assert!(io_calls(&pair) <= 2, "node.get of one pair: the current-pair lookup and the node's record, never a call per birth: {cost:#?}");
    assert_eq!((io_calls(&by_id), resolved(&by_id).len()), (1, 1), "node.get by id reads that id's record in one store call: {cost:#?}");
    for pairs in ["(mesh, [mesh2])", "(type, [node])", ""] {
        assert!(io_calls(&served(pairs)) <= 2, "node.get {pairs:?} reads its records at one store call per mesh, never one per birth: {cost:#?}");
    }
    assert!(io_calls(&fabric) <= 2, "fabric.get reads the cert dates at one store call per mesh, never one per birth: {fabric}");
    let stale = served("(path, [mesh2.node-admin]) (name, [2])");
    assert!(attr(&stale, "role_from").ends_with(":view"), "the stale claim's role is the view's, settled against every claim: {stale}");
}
