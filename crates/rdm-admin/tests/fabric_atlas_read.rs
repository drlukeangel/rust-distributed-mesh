//! Functional cell for `fabric-atlas-read` (`docs/architecture/query.md:11`).

use iroh::SecretKey;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::atlas::Atlas;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::fabric_storage::MemoryFabricStorage;
use rdm_admin::model::{
    EndpointId, Fabric, FabricId, IncarnationId, MeshId, Node, NodeId, NodeKind, NodeStatus,
    PathName, ProviderKind, ScopeStatus,
};
use rdm_admin::status_rpc::{Declared, StatusDoor};
use rdm_admin::storage::{MemoryMeshStorage, MemoryNodesStorage, NodeRow, NodesStorage};
use rdm_admin::topology::Topology;
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_transport::{
    CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth,
    ServerBuilder,
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

// @feature: fabric-atlas-read
/// CONTRACT: docs/architecture/query.md:11
/// "A caller outside the node sends `fabric.get`, `mesh.get` and `node.get` to the
///  fabric-primary, which answers from its atlas: every mesh's node records, held in memory, filled
///  from the node records once at its start or hand-over and written by each op after that"
#[test]
fn fabrics_atlas_get_over_status_rpc_refuses_when_atlas_unset_and_answers_planned_nodes_with_in_flight_commands(
) {
    let capture = Capture::new("fabric-atlas-read-functional");
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

        let rpc_name = PathName::new("mesh1", NodeKind::RpcNode, 1);
        let rpc_id = NodeId::mint();
        let rpc_inc = IncarnationId::mint();

        let mut admin_node = Node::allocated(admin_name.clone());
        admin_node.node_id = admin_id.clone();
        admin_node.incarnation_id = Some(admin_inc.clone());
        admin_node.endpoint_id = Some(EndpointId(admin_key.public().to_string()));
        admin_node.transport_addr = Some(admin_addr);
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;

        let mut rpc_node = Node::allocated(rpc_name.clone());
        rpc_node.node_id = rpc_id.clone();
        rpc_node.incarnation_id = Some(rpc_inc.clone());
        rpc_node.status = NodeStatus::Ready;

        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: Fabric {
                id: fabric_id.clone(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![admin_node, rpc_node],
        }));

        let deleted_name = PathName::new("mesh1", NodeKind::RpcNode, 2);
        let deleted_id = NodeId::mint();
        let deleted_inc = IncarnationId::mint();

        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let mut admin_row = NodeRow::new(admin_name.clone(), admin_id.clone(), NodeStatus::Ready);
        admin_row
            .birth_mut(&admin_inc, &admin_name)
            .declared
            .insert(rdm_core::status::NodeState::Ready);
        let mut rpc_row = NodeRow::new(rpc_name.clone(), rpc_id.clone(), NodeStatus::Ready);
        rpc_row
            .birth_mut(&rpc_inc, &rpc_name)
            .declared
            .insert(rdm_core::status::NodeState::Ready);
        let mut deleted_row =
            NodeRow::new(deleted_name.clone(), deleted_id.clone(), NodeStatus::Deleted);
        deleted_row.birth_mut(&deleted_inc, &deleted_name);
        nodes_storage.put_node(&admin_row).await.unwrap();
        nodes_storage
            .index_path(&admin_name, &admin_id)
            .await
            .unwrap();
        nodes_storage.put_node(&rpc_row).await.unwrap();
        nodes_storage.index_path(&rpc_name, &rpc_id).await.unwrap();
        nodes_storage.put_node(&deleted_row).await.unwrap();
        nodes_storage
            .index_path(&deleted_name, &deleted_id)
            .await
            .unwrap();

        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let build_id =
            rdm_admin::build::BuildId::parse(&rdm_core::fixture_crockford60("b-atlas")).unwrap();
        let mut top = FabricTopology::root("fabric1", "mesh1", mesh_id.clone());
        top.meshes
            .get_mut("mesh1")
            .unwrap()
            .nodes
            .insert(rpc_name.clone());
        top.meshes
            .get_mut("mesh1")
            .unwrap()
            .nodes
            .insert(deleted_name);
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: top,
                submitted_change: None,
                submitted_at_ms: 1_000,
            })
            .await
            .unwrap();

        let atlas_slot: Arc<OnceLock<Arc<Atlas>>> = Arc::new(OnceLock::new());
        let mut mesh_ids = BTreeMap::new();
        mesh_ids.insert("mesh1".to_string(), mesh_id);

        let door = Arc::new(StatusDoor {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin_name.clone(),
            node_id: admin_id.clone(),
            fabric_id,
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
            atlas: atlas_slot.clone(),
            fabric_recovery: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            configure: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            digest_view: None,
            builds,
        });

        let slot = Arc::new(OnceLock::new());
        let _ = slot.set(door);
        let server = rdm_admin::status_rpc::serve(ServerBuilder::new(), slot)
            .build(ServedBirth {
                node_id: admin_id.to_string(),
                incarnation: admin_inc.to_string(),
            })
            .expect("build status server");
        let router = iroh::protocol::Router::builder(admin_ep.clone())
            .accept(rdm_transport::ALPN, server)
            .spawn();

        let caller_key = SecretKey::generate();
        let caller_ep = rdm_transport::endpoint::bind(
            caller_key,
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind caller ep");
        let resolver = Arc::new(LiveNodeResolver::default());
        resolver.apply(
            ResolvedNode {
                node_id: admin_id.clone(),
                name: admin_name.clone(),
                endpoint_id: admin_key.public(),
                transport_addr: admin_addr,
                incarnation: admin_inc.clone(),
            },
            None,
        );
        let client = NodeRpcClient::new(caller_ep.clone(), resolver).with_caller_system("rdm");

        // 1. Before the fabric-primary's atlas slot is filled, GetAtlas answers NotReady.
        let (before_out, _) = client
            .call::<Status>(
                &NodeTarget::ExactNode(admin_id.clone()),
                &StatusRequest::GetAtlas,
                &CallOptions::default(),
            )
            .await;
        match before_out {
            RpcOutcome::Reply(r) => assert!(matches!(
                r.into_value(),
                StatusReply::NotReady { ref reason } if reason.contains("holds no fabric atlas yet")
            )),
            other => panic!("expected NotReady reply, got {other:?}"),
        }

        // 2. Install the atlas and verify GetAtlas answers live nodes in path.name order and omits deleted ones.
        let atlas = Arc::new(Atlas::default());
        atlas.write_record(&admin_row);
        atlas.write_record(&rpc_row);
        atlas.write_record(&deleted_row);
        let _ = atlas_slot.set(atlas);

        let (after_out, _) = client
            .call::<Status>(
                &NodeTarget::ExactNode(admin_id.clone()),
                &StatusRequest::GetAtlas,
                &CallOptions::default(),
            )
            .await;
        match after_out {
            RpcOutcome::Reply(r) => match r.into_value() {
                StatusReply::Atlas { atlas } => {
                    assert_eq!(atlas.nodes.len(), 2);
                    assert_eq!(atlas.nodes[0].name, admin_name);
                    assert_eq!(atlas.nodes[0].node_id, admin_id);
                    assert_eq!(atlas.nodes[0].incarnation_id, Some(admin_inc));
                    assert_eq!(atlas.nodes[0].status, rdm_core::views::NodeStatus::Ready);
                    assert_eq!(atlas.nodes[1].name, rpc_name);
                    assert_eq!(atlas.nodes[1].node_id, rpc_id);
                    assert_eq!(atlas.nodes[1].incarnation_id, Some(rpc_inc));
                    assert_eq!(atlas.nodes[1].status, rdm_core::views::NodeStatus::Ready);
                }
                other => panic!("expected StatusReply::Atlas, got {other:?}"),
            },
            other => panic!("expected Reply, got {other:?}"),
        }

        let _ = router.shutdown().await;
        caller_ep.close().await;
        admin_ep.close().await;
    });

    drop(rt);

    let spans = capture.all();
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_rpc.request.serve.via-direct"
                && s.attributes.get("protocol").map(String::as_str) == Some("status")
                && s.attributes.get("code").map(String::as_str) == Some("UNAVAILABLE")
                && s.attributes.get("detail").map(String::as_str) == Some("not-ready")
        }),
        "expected rdm.node_rpc.request.serve.via-direct span with detail=not-ready before atlas is set"
    );
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_rpc.request.serve.via-direct"
                && s.attributes.get("protocol").map(String::as_str) == Some("status")
                && s.attributes.get("code").map(String::as_str) == Some("OK")
                && s.attributes.get("detail").map(String::as_str) == Some("atlas")
        }),
        "expected rdm.node_rpc.request.serve.via-direct span with detail=atlas when atlas is served"
    );
}
