//! Functional cell for `node.connection.delete` (`docs/architecture/lifecycles/node-connections-delete.md`).

use iroh::SecretKey;
use rdm_admin::connections_writer::ConnectionsWriter;
use rdm_admin::fabric_storage::MemoryFabricStorage;
use rdm_admin::model::{
    EndpointId, Fabric, FabricId, IncarnationId, MeshId, Node, NodeId, NodeKind, NodeStatus,
    PathName, ProviderKind, ScopeStatus,
};
use rdm_admin::node_config::NodeConfigs;
use rdm_admin::node_self::NodeSelf;
use rdm_admin::status_rpc::{CarryEnd, Declared, StatusAuthority};
use rdm_admin::storage::{MemoryConnectionsStorage, MemoryMeshStorage, MemoryNodesStorage, NodesStorage};
use rdm_admin::topology::Topology;
use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
use rdm_core::node_command::{NodeCommand, NodeCommandRequest};
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, NotPrimary, StatusReply};
use rdm_core::{ConnectionId, MemberStatus, MeshDigest, MeshNode};
use rdm_transport::topology::TopologyView;
use rdm_transport::{
    LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder,
};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    name: String,
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn all(&self) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let spans = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(spans.iter().map(|s| s.name.as_ref()));
        spans
            .into_iter()
            .map(|s| Captured {
                name: s.name.to_string(),
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

// @feature: node-connections-delete
/// CONTRACT: docs/architecture/lifecycles/node-connections-delete.md:19
/// "`All`: Graceful node-wide closure: close every open connection of the node. Status
///  `connections-deleting` once the node reports it on the command's stream, then
///  `connections-deleted` from its `Established` completion.
///  `Connections([id, …])`: Close exactly the named connections, each by the minted id of its open
///  entry as `node.connection.get` returns it. Sets no node-wide status and does not make the node
///  non-routable. An empty list is refused by name. An id the node does not know, or finds already
///  closed, is named in the completion, never ignored."
#[test]
fn mesh_primary_carries_delete_connections_over_node_command_stream_and_records_node_status_only_for_all_scope(
) {
    let capture = Capture::new("node-connections-delete-functional");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    rt.block_on(async {
        let fabric_id = FabricId::mint();
        let mesh_id = MeshId::mint();

        let admin_name = PathName::new("mesh1", NodeKind::NodeAdmin, 1);
        let admin_id = NodeId::mint();
        let admin_inc = IncarnationId::mint();
        let admin_key = SecretKey::generate();
        let admin_ep = rdm_transport::endpoint::bind(
            admin_key.clone(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind admin ep");
        let admin_addr = admin_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .expect("ipv4");

        let subject_name = PathName::new("mesh1", NodeKind::RpcNode, 1);
        let subject_id = NodeId::mint();
        let subject_inc = IncarnationId::mint();
        let subject_key = SecretKey::generate();
        let subject_ep = rdm_transport::endpoint::bind(
            subject_key.clone(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind subject ep");
        let subject_addr = subject_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .expect("ipv4");

        let peer_name = PathName::new("mesh1", NodeKind::RpcNode, 2);
        let peer_id = NodeId::mint();
        let peer_inc = IncarnationId::mint();
        let peer_key = SecretKey::generate();
        let peer_ep = rdm_transport::endpoint::bind(
            peer_key.clone(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind peer ep");
        let peer_addr = peer_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .expect("ipv4");
        let peer_resolver = Arc::new(LiveNodeResolver::default());
        let peer_client = Arc::new(NodeRpcClient::new(peer_ep.clone(), peer_resolver));
        let peer_server = ServerBuilder::new()
            .serve_core(peer_client, None)
            .build(ServedBirth {
                node_id: peer_id.to_string(),
                incarnation: peer_inc.to_string(),
            })
            .expect("build peer server");
        let peer_router = iroh::protocol::Router::builder(peer_ep.clone())
            .accept(rdm_transport::ALPN, peer_server)
            .spawn();

        let subject_resolver = Arc::new(LiveNodeResolver::default());
        subject_resolver.apply(
            ResolvedNode {
                node_id: peer_id.clone(),
                name: peer_name.clone(),
                endpoint_id: peer_key.public(),
                transport_addr: peer_addr,
                incarnation: peer_inc.clone(),
            },
            None,
        );
        subject_resolver.apply(
            ResolvedNode {
                node_id: admin_id.clone(),
                name: admin_name.clone(),
                endpoint_id: admin_key.public(),
                transport_addr: admin_addr,
                incarnation: admin_inc.clone(),
            },
            None,
        );

        let subject_store = Arc::new(MemoryConnectionsStorage::default());
        let subject_held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let subject_writer = Arc::new(ConnectionsWriter::new(
            ConnectionEnd {
                name: subject_name.clone(),
                node_id: subject_id.clone(),
                incarnation: Some(subject_inc.clone()),
            },
            subject_store,
            subject_held,
        ));
        let subject_control = Arc::new(
            NodeRpcClient::new(subject_ep.clone(), subject_resolver.clone())
                .with_caller_system("rdm"),
        );
        let subject_data_plane = Arc::new(
            NodeRpcClient::new(subject_ep.clone(), subject_resolver.clone())
                .with_caller_system("rdm")
                .with_connection_observer(subject_writer.clone()),
        );

        let subject_own_slot: Arc<OnceLock<Arc<NodeSelf>>> = Arc::new(OnceLock::new());
        let slot_for_cmd = subject_own_slot.clone();
        let commander_id = admin_id.clone();
        let subject_server = ServerBuilder::new()
            .serve_core(subject_control.clone(), None)
            .serve_stream::<NodeCommand, _, _>(
                rdm_core::catalog::OpOwner::Product("rdm".into()),
                move |peer: rdm_transport::PeerContext, req: NodeCommandRequest, sink| {
                    let slot = slot_for_cmd.clone();
                    let commander = commander_id.clone();
                    async move {
                        let own = slot.get().expect("subject NodeSelf installed").clone();
                        Ok(own
                            .serve_node_command_on(
                                Some(&commander),
                                Some(peer.endpoint_id),
                                &req,
                                Box::new(rdm_admin::node_self::StreamFrames::new(sink)),
                            )
                            .await)
                    }
                },
            )
            .build(ServedBirth {
                node_id: subject_id.to_string(),
                incarnation: subject_inc.to_string(),
            })
            .expect("build subject server");
        let subject_router = iroh::protocol::Router::builder(subject_ep.clone())
            .accept(rdm_transport::ALPN, subject_server.clone())
            .spawn();

        let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
        clock.adopt(1_000_000);
        let gossip = iroh_gossip::net::Gossip::builder().spawn(subject_ep.clone());
        let topology_view = TopologyView::join(
            &gossip,
            &subject_ep,
            &fabric_id,
            "mesh1",
            &mesh_id,
            &subject_name.to_string(),
            clock,
            Vec::new(),
            &[],
        )
        .await
        .expect("join topology view");

        let status_cell = Arc::new(Mutex::new(MemberStatus::Ready));
        let digest_base = MeshDigest {
            held_hash: Default::default(),
            fabric_id: fabric_id.clone(),
            node: MeshNode {
                node_id: subject_id.clone(),
                name: subject_name.clone(),
                endpoint_id: EndpointId(subject_key.public().to_string()),
                incarnation: subject_inc.clone(),
                supersedes: None,
                transport_addr: subject_addr,
                runtime: None,
            },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: Some(mesh_id.clone()),
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        let status_for_set = status_cell.clone();
        let digest_for_set = digest_base.clone();
        let set_status = Arc::new(move |s: MemberStatus| {
            *status_for_set.lock().unwrap() = s;
            let mut d = digest_for_set.clone();
            d.status = s;
            d
        });
        let status_for_get = status_cell.clone();
        let get_status = Arc::new(move || *status_for_get.lock().unwrap());

        let subject_own = Arc::new(NodeSelf::new(
            subject_id.clone(),
            subject_inc.clone(),
            subject_name.clone(),
            subject_server,
            subject_control,
            subject_resolver,
            topology_view,
            set_status,
            get_status,
            None,
            Duration::from_secs(2),
        ));
        subject_own.set_connections(subject_writer.clone());
        subject_own.set_data_plane(subject_data_plane.clone());
        let _ = subject_own_slot.set(subject_own.clone());

        // Set up the mesh-primary's StatusAuthority with a client resolving `subject`.
        let admin_resolver = Arc::new(LiveNodeResolver::default());
        admin_resolver.apply(
            ResolvedNode {
                node_id: subject_id.clone(),
                name: subject_name.clone(),
                endpoint_id: subject_key.public(),
                transport_addr: subject_addr,
                incarnation: subject_inc.clone(),
            },
            None,
        );
        let admin_client = Arc::new(
            NodeRpcClient::new(admin_ep.clone(), admin_resolver).with_caller_system("rdm"),
        );
        let configs = Arc::new(NodeConfigs::new(
            admin_client,
            Default::default(),
            Default::default(),
        ));
        let configs_slot = Arc::new(OnceLock::new());
        let _ = configs_slot.set(configs);

        let mut admin_node = Node::allocated(admin_name.clone());
        admin_node.node_id = admin_id.clone();
        admin_node.incarnation_id = Some(admin_inc.clone());
        admin_node.endpoint_id = Some(EndpointId(admin_key.public().to_string()));
        admin_node.transport_addr = Some(admin_addr);
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;

        let mut subject_node = Node::allocated(subject_name.clone());
        subject_node.node_id = subject_id.clone();
        subject_node.incarnation_id = Some(subject_inc.clone());
        subject_node.endpoint_id = Some(EndpointId(subject_key.public().to_string()));
        subject_node.transport_addr = Some(subject_addr);
        subject_node.status = NodeStatus::Ready;

        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: Fabric {
                id: fabric_id.clone(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![admin_node, subject_node],
        }));

        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let mut subject_row =
            rdm_admin::storage::NodeRow::new(subject_name.clone(), subject_id.clone(), NodeStatus::Ready);
        subject_row.births.push(rdm_admin::storage::BirthFacts::new(
            subject_inc.clone(),
            subject_name.clone(),
        ));
        nodes_storage
            .put_node(&subject_row)
            .await
            .expect("put subject row");
        let mut mesh_ids = BTreeMap::new();
        mesh_ids.insert("mesh1".to_string(), mesh_id.clone());

        let authority = StatusAuthority {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin_name.clone(),
            node_id: admin_id.clone(),
            fabric_id: fabric_id.clone(),
            topology,
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(MemoryMeshStorage::default()),
            fabric_storage: Arc::new(MemoryFabricStorage::default()),
            nodes_storage: nodes_storage.clone(),
            caches: Default::default(),
            mesh_ids: Arc::new(move || mesh_ids.clone()),
            durable: Default::default(),
            commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: Default::default(),
            fabric_recovery: Default::default(),
            configs: configs_slot,
            republish: Default::default(),
            configure: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            digest_view: None,
            builds: Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
        };

        // 1. Unknown subject, stale incarnation, and empty Connections([]) scope are refused by name.
        let unknown_subject = authority
            .carry_connections_delete(&NodeId::mint(), &subject_inc, &ConnectionScope::All)
            .await;
        assert!(matches!(
            unknown_subject,
            CarryEnd::Refused(StatusReply::RejectedNotPrimary {
                why: NotPrimary::SubjectUnknown
            })
        ));

        let stale_birth = authority
            .carry_connections_delete(&subject_id, &IncarnationId::mint(), &ConnectionScope::All)
            .await;
        assert!(matches!(
            stale_birth,
            CarryEnd::Refused(StatusReply::RejectedStaleIncarnation { ref held }) if *held == subject_inc
        ));

        let empty_scope = authority
            .carry_connections_delete(
                &subject_id,
                &subject_inc,
                &ConnectionScope::Connections(Vec::new()),
            )
            .await;
        assert!(matches!(
            empty_scope,
            CarryEnd::Refused(StatusReply::RejectedConnectionScope { .. })
        ));

        // 2. Open a live Direct connection on `subject`, then delete it by id alongside an unknown id.
        let (ping1, _) = subject_data_plane
            .call::<Ping>(
                &NodeTarget::ExactNode(peer_id.clone()),
                &PingRequest::Ping {
                    payload: b"named".to_vec(),
                },
                &rdm_transport::CallOptions::default(),
            )
            .await;
        assert_eq!(ping1.name(), "Reply");
        subject_writer.observations_landed().await;
        let live_id_1 = subject_own.live_routes().expect("live routes")[0]
            .id
            .clone();
        let unknown_id = ConnectionId::mint();

        let named_end = authority
            .carry_connections_delete(
                &subject_id,
                &subject_inc,
                &ConnectionScope::Connections(vec![live_id_1.clone(), unknown_id.clone()]),
            )
            .await;
        assert!(matches!(
            named_end,
            CarryEnd::Deleted {
                ref closed,
                ref not_open,
                ref held,
                receipt: None,
            } if *closed == vec![live_id_1] && *not_open == vec![unknown_id] && held.is_empty()
        ));
        assert_eq!(
            nodes_storage
                .node(&subject_id)
                .await
                .expect("read storage")
                .expect("subject row")
                .status,
            NodeStatus::Ready,
            "Connections([id]) sets no node-wide status on the node row"
        );

        // 3. Open a second live Direct connection on `subject`, then carry ConnectionScope::All.
        let (ping2, _) = subject_data_plane
            .call::<Ping>(
                &NodeTarget::ExactNode(peer_id.clone()),
                &PingRequest::Ping {
                    payload: b"all".to_vec(),
                },
                &rdm_transport::CallOptions::default(),
            )
            .await;
        assert_eq!(ping2.name(), "Reply");
        subject_writer.observations_landed().await;
        let live_id_2 = subject_own.live_routes().expect("live routes 2")[0]
            .id
            .clone();

        let all_end = authority
            .carry_connections_delete(&subject_id, &subject_inc, &ConnectionScope::All)
            .await;
        assert!(matches!(
            all_end,
            CarryEnd::Deleted {
                ref closed,
                ref not_open,
                ref held,
                receipt: Some(ConnectionsReceipt::Established {
                    in_flight: 0,
                    closed: ref receipt_closed,
                }),
            } if *closed == vec![live_id_2.clone()]
                && not_open.is_empty()
                && held.is_empty()
                && *receipt_closed == vec![live_id_2]
        ));

        let row = nodes_storage
            .node(&subject_id)
            .await
            .expect("read storage")
            .expect("subject row written for All scope");
        assert_eq!(
            row.status,
            NodeStatus::ConnectionsDeleted,
            "All scope transitions the subject row through ConnectionsDeleting to ConnectionsDeleted"
        );

        let _ = peer_router.shutdown().await;
        let _ = subject_router.shutdown().await;
        peer_ep.close().await;
        subject_ep.close().await;
        admin_ep.close().await;
    });

    drop(rt);

    let spans = capture.all();
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_admin.connection.delete.via-mesh-primary"
                && s.attributes.get("scope").map(String::as_str) == Some("all")
                && s.attributes.get("outcome").map(String::as_str) == Some("connections-deleted")
        }),
        "expected rdm.node_admin.connection.delete.via-mesh-primary span for all scope"
    );
    assert!(
        spans
            .iter()
            .any(|s| s.name == "rdm.node_admin.node.update.via-connections-deleting"),
        "expected rdm.node_admin.node.update.via-connections-deleting span when node reports ConnectionsDeleting"
    );
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_admin.node.update.via-connections-deleted"
                && s.attributes.get("receipt").map(String::as_str) == Some("established")
        }),
        "expected rdm.node_admin.node.update.via-connections-deleted span with established receipt"
    );
}
