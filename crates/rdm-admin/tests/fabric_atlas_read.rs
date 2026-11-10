//! Functional cell for `fabric-atlas-read` (`docs/architecture/query.md:11`).

use iroh::SecretKey;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::atlas::Atlas;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::fabric_storage::MemoryFabricStorage;
use rdm_admin::model::{
    EndpointId, Fabric, FabricId, IncarnationId, MeshId, Node, NodeId, NodeStatus,
    PathName, ProviderKind, ScopeStatus,
};
use rdm_admin::status_rpc::{Declared, StatusService};
use rdm_admin::storage::{MemoryMeshStorage, MemoryNodesStorage, NodeRecord, NodesStorage};
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
/// CONTRACT: docs/architecture/query.md §1 (Q439)
/// "The atlas, `fabric.atlas.get` and `mesh.atlas.get`: the topology plus what is in flight (Q439),
///  laid out as `fabric.get` answers: a list of fabrics, each with its fields as `fabric.get` answers
///  them and the current Build and its phase, and inside it its meshes in name order, each as
///  `mesh.get` answers it, with its nodes in `(path, name)` order."
/// CONTRACT: before the admin's atlas is filled, `fabric.atlas.get` is refused `NotReady`; after it,
/// the answer is one fabric whose fields are `fabric.get`'s, under its names, with the current Build
/// and its phase, then mesh1 with `mesh.get`'s fields and its live nodes in `(path, name)` order, each
/// the topology's node row read from its record. Must NOT happen: a flat node list, a fabric or a
/// mesh spelled other than its own read spells it, or the deleted node listed.
#[test]
fn fabrics_atlas_get_over_status_rpc_refuses_when_atlas_unset_and_answers_the_fabric_then_its_meshes_and_nodes(
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

        let admin_name = PathName::numbered("mesh1", "node-admin", 1);
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

        let rpc_name = PathName::numbered("mesh1", "node", 1);
        let rpc_id = NodeId::mint();
        let rpc_inc = IncarnationId::mint();

        let mut admin_node = Node::allocated(admin_name.clone(), "mesh1", "node-admin");
        admin_node.node_id = admin_id.clone();
        admin_node.incarnation_id = Some(admin_inc.clone());
        admin_node.endpoint_id = Some(EndpointId(admin_key.public().to_string()));
        admin_node.transport_addr = Some(admin_addr);
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;

        let mut rpc_node = Node::allocated(rpc_name.clone(), "mesh1", "node");
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
            meshes: vec![rdm_admin::model::Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![admin_node, rpc_node],
        }));

        let deleted_name = PathName::numbered("mesh1", "node", 2);
        let deleted_id = NodeId::mint();
        let deleted_inc = IncarnationId::mint();

        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let admin_row = rdm_node::nodes_storage::fixture_record(&admin_name, "node-admin", admin_id.clone(), admin_inc.clone(), NodeStatus::Ready);
        let rpc_row = rdm_node::nodes_storage::fixture_record(&rpc_name, "node", rpc_id.clone(), rpc_inc.clone(), NodeStatus::Ready);
        let mut deleted_row =
            rdm_node::nodes_storage::fixture_record(&deleted_name.clone(), "node", deleted_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Deleted);
        deleted_row.incarnation = deleted_inc.clone();
        nodes_storage.put_node(&admin_row).await.unwrap();
        nodes_storage.put_node(&rpc_row).await.unwrap();
        nodes_storage.put_node(&deleted_row).await.unwrap();
        // What fabric.atlas.get reads (query.md §1): each row's lifecycle record at its node id, written by
        // mesh1's mesh-primary, mesh1's Mesh record, and the nodes store bound on the `NodeConfigs`.
        let meshes_storage = Arc::new(MemoryMeshStorage::default());
        rdm_admin::storage::MeshStorage::put_mesh(&*meshes_storage, &rdm_admin::storage::MeshRecord::new(mesh_id.clone(), "mesh1")).await.unwrap();
        let configs = crate::bound_configs_over(nodes_storage.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(admin_ep.clone(), Arc::new(LiveNodeResolver::default()))))).await;
        let configs_slot: rdm_admin::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs_slot.set(Arc::new(configs));

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
                change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                source: None, time: 1_000,
            })
            .await
            .unwrap();

        let atlas_slot: Arc<OnceLock<Arc<Atlas>>> = Arc::new(OnceLock::new());
        let mut mesh_ids = BTreeMap::new();
        mesh_ids.insert("mesh1".to_string(), mesh_id);

        let service = Arc::new(StatusService {
            certs: None,
            topology_view: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin_name.clone(),
            node_id: admin_id.clone(),
            fabric_id,
            topology,
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: meshes_storage.clone(),
            fabric_storage: Arc::new(MemoryFabricStorage::default()),
            nodes_storage: nodes_storage.clone(),
            caches: Default::default(),
            mesh_ids: Arc::new(move || mesh_ids.clone()),
            durable: Default::default(),
            commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
            own: Arc::new(OnceLock::new()),
            mesh_deleter: Arc::new(OnceLock::new()),
            primary_commands: Default::default(),
            storage_changes: Default::default(),
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: atlas_slot.clone(),
            fabric_recovery: Default::default(),
            configs: configs_slot.clone(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(), rejoin: Default::default(),
            // The fabric's own read (`fabric.get`) answers from a view with its digest view's loads.
            loads: Some(Arc::new(BTreeMap::new)),
            digest_view: None,
            unreachable: Default::default(),
            builds,
        });

        let slot = Arc::new(OnceLock::new());
        let _ = slot.set(service);
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
            ResolvedNode { r#type: "node-admin".into(), mesh: "mesh1".to_owned(),
                node_id: admin_id.clone(),
                path: admin_name.path.clone(), name: admin_name.name.clone(),
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

        // 2. Install the fabric-primary's atlas, over mesh1: GetAtlas answers the fabric as fabric.get
        // answers it, then mesh1 and its live nodes in (path, name) order; the deleted node is not listed.
        let held = Arc::new(Atlas::default());
        held.scope(["mesh1".to_string()].into());
        let _ = atlas_slot.set(held);

        let (fabric_out, _) = client
            .call::<Status>(&NodeTarget::ExactNode(admin_id.clone()), &StatusRequest::GetFabric, &CallOptions::default())
            .await;
        let fabric_get = match fabric_out {
            RpcOutcome::Reply(r) => match r.into_value() {
                StatusReply::Fabric { fabric } => serde_json::to_value(&fabric).unwrap(),
                other => panic!("expected StatusReply::Fabric, got {other:?}"),
            },
            other => panic!("expected Reply, got {other:?}"),
        };
        let (after_out, _) = client
            .call::<Status>(
                &NodeTarget::ExactNode(admin_id.clone()),
                &StatusRequest::GetAtlas,
                &CallOptions::default(),
            )
            .await;
        let atlas = match after_out {
            RpcOutcome::Reply(r) => match r.into_value() {
                StatusReply::Atlas { atlas } => serde_json::to_value(&atlas).unwrap(),
                other => panic!("expected StatusReply::Atlas, got {other:?}"),
            },
            other => panic!("expected Reply, got {other:?}"),
        };
        let fabrics = atlas["fabrics"].as_array().unwrap_or_else(|| panic!("Q439: the atlas is a list of fabrics, as fabric.get answers: {atlas}"));
        assert_eq!(fabrics.len(), 1, "one fabric: {atlas}");
        let fabric = &fabrics[0];
        // The fabric's fields are fabric.get's, under its names; its meshes are the atlas's own.
        let without = |v: &serde_json::Value, keys: &[&str]| {
            let mut v = v.clone();
            for k in keys {
                v.as_object_mut().expect("an object").remove(*k);
            }
            v
        };
        assert_eq!(without(fabric, &["build", "meshes"]), without(&fabric_get, &["meshes"]), "Q439: the atlas's fabric carries fabric.get's fields, under its names: {atlas}");
        assert_eq!(fabric["build"]["build_id"], serde_json::json!(build_id.to_string()), "the current Build: {atlas}");
        assert_eq!(fabric["build"]["phase"], serde_json::json!(rdm_core::build::BuildPhase::Pending), "the current Build's phase: {atlas}");
        let meshes = fabric["meshes"].as_array().unwrap_or_else(|| panic!("the fabric's meshes: {atlas}"));
        assert_eq!(meshes.iter().map(|m| m["name"].clone()).collect::<Vec<_>>(), vec![serde_json::json!("mesh1")], "the meshes the atlas holds: {atlas}");
        let mesh_get = fabric_get["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh1").cloned().expect("fabric.get answers mesh1");
        assert_eq!(without(&meshes[0], &["nodes"]), without(&mesh_get, &["nodes"]), "Q439: the atlas's mesh carries mesh.get's fields, under its names: {atlas}");
        let nodes = meshes[0]["nodes"].as_array().unwrap_or_else(|| panic!("mesh1's nodes: {atlas}"));
        let row = |n: &serde_json::Value| (n["node"]["path"].clone(), n["node"]["name"].clone(), n["node"]["node_id"].clone(), n["node"]["incarnation"].clone(), n["node"]["type"].clone(), n["node"]["status"].clone(), n["stopped"].clone(), n["accepted"].clone());
        let want = |at: &PathName, id: &NodeId, inc: &IncarnationId, r#type: &str| (serde_json::json!(at.path), serde_json::json!(at.name), serde_json::json!(id), serde_json::json!(inc), serde_json::json!(r#type), serde_json::json!("ready"), serde_json::json!(false), serde_json::Value::Null);
        assert_eq!(nodes.iter().map(row).collect::<Vec<_>>(), vec![want(&rpc_name, &rpc_id, &rpc_inc, "node"), want(&admin_name, &admin_id, &admin_inc, "node-admin")], "mesh1's live nodes in (path, name) order, each the topology's node row, the deleted one not listed: {atlas}");

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
